from __future__ import annotations

import json
import shutil
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterator, List

from app.modules.shared_bridge.service.shared_bridge_mailbox_store import SharedBridgeMailboxStore
from app.modules.shared_bridge.service.shared_source_cache_index_store import SharedSourceCacheIndexStore

_TERMINAL_TASK_STATUSES = {"success", "failed", "partial_failed", "cancelled", "stale"}
_BRANCH_SOURCE_ALLOWED_FAMILIES = {"branch_power_family", "branch_current_family", "branch_switch_family"}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_text_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _normalize_branch_bucket_key(value: Any) -> str:
    text = str(value or "").strip().replace("/", "-").replace("T", " ")
    if not text:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H", "%Y%m%d%H"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H")
        except ValueError:
            continue
    return ""


def _normalize_branch_requested_source_units(raw_units: Any) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, tuple[str, ...]]] = set()
    for item in raw_units if isinstance(raw_units, list) else []:
        if not isinstance(item, dict):
            continue
        building = str(item.get("building", "") or "").strip()
        source_family = str(item.get("source_family", "") or "").strip()
        if not building or source_family not in _BRANCH_SOURCE_ALLOWED_FAMILIES:
            continue
        bucket_keys = [
            _normalize_branch_bucket_key(raw_value)
            for raw_value in (item.get("target_bucket_keys") if isinstance(item.get("target_bucket_keys"), list) else [])
        ]
        bucket_keys = sorted({bucket_key for bucket_key in bucket_keys if bucket_key})
        if not bucket_keys:
            single_bucket = _normalize_branch_bucket_key(item.get("target_bucket_key", "") or item.get("bucket_key", ""))
            if single_bucket:
                bucket_keys = [single_bucket]
        if not bucket_keys:
            continue
        key = (building, source_family, tuple(bucket_keys))
        if key in seen:
            continue
        seen.add(key)
        output.append(
            {
                "building": building,
                "source_family": source_family,
                "target_bucket_keys": bucket_keys,
                "reason": str(item.get("reason", "") or "").strip(),
            }
        )
    output.sort(key=lambda unit: (str(unit.get("building", "")), str(unit.get("source_family", "")), ",".join(unit.get("target_bucket_keys", []))))
    return output


def _branch_requested_units_dedupe(units: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for unit in _normalize_branch_requested_source_units(units):
        parts.append(
            f"{unit.get('building')}|{unit.get('source_family')}|{','.join(unit.get('target_bucket_keys', []))}"
        )
    return ";".join(parts)


class SharedBridgeStore:
    def __init__(self, root_dir: str | Path, *, busy_timeout_ms: int = 15000) -> None:
        self.root_dir = Path(root_dir)
        self.db_path = self.root_dir / "bridge.db"
        self.busy_timeout_ms = max(1000, int(busy_timeout_ms or 15000))
        self._ready_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._ready = False
        self._mailbox_store = SharedBridgeMailboxStore(self.root_dir)
        self._source_cache_index_store = SharedSourceCacheIndexStore(self.root_dir)

    def ensure_ready(self) -> None:
        if self._ready:
            return
        with self._ready_lock:
            if self._ready:
                return
            self.root_dir.mkdir(parents=True, exist_ok=True)
            for name in ("artifacts", "logs", "tmp"):
                (self.root_dir / name).mkdir(parents=True, exist_ok=True)
            self._mailbox_store.ensure_ready()
            self._source_cache_index_store.ensure_ready()
            with self.connect() as conn:
                conn.executescript(
                    """
                CREATE TABLE IF NOT EXISTS bridge_settings (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    shared_schema_version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bridge_tasks (
                    task_id TEXT PRIMARY KEY,
                    feature TEXT NOT NULL DEFAULT '',
                    mode TEXT NOT NULL DEFAULT '',
                    created_by_role TEXT NOT NULL DEFAULT '',
                    created_by_node_id TEXT NOT NULL DEFAULT '',
                    requested_by TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    dedupe_key TEXT NOT NULL DEFAULT '',
                    request_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_bridge_tasks_status ON bridge_tasks(status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_bridge_tasks_feature ON bridge_tasks(feature, updated_at);
                CREATE TABLE IF NOT EXISTS bridge_stages (
                    task_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    role_target TEXT NOT NULL DEFAULT '',
                    handler TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    input_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    claimed_by_node_id TEXT NOT NULL DEFAULT '',
                    claim_token TEXT NOT NULL DEFAULT '',
                    lease_expires_at TEXT NOT NULL DEFAULT '',
                    started_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (task_id, stage_id)
                );
                CREATE INDEX IF NOT EXISTS idx_bridge_stages_role_status ON bridge_stages(role_target, status, lease_expires_at);
                CREATE TABLE IF NOT EXISTS bridge_artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL DEFAULT '',
                    artifact_kind TEXT NOT NULL DEFAULT '',
                    building TEXT NOT NULL DEFAULT '',
                    relative_path TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    size_bytes INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_bridge_artifacts_task ON bridge_artifacts(task_id, status, artifact_kind);
                CREATE INDEX IF NOT EXISTS idx_bridge_artifacts_kind_status_updated
                    ON bridge_artifacts(artifact_kind, status, updated_at, created_at);
                CREATE TABLE IF NOT EXISTS bridge_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL DEFAULT '',
                    stage_id TEXT NOT NULL DEFAULT '',
                    side TEXT NOT NULL DEFAULT '',
                    level TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_bridge_events_task ON bridge_events(task_id, event_id);
                CREATE TABLE IF NOT EXISTS bridge_nodes (
                    node_id TEXT PRIMARY KEY,
                    role_mode TEXT NOT NULL DEFAULT '',
                    node_label TEXT NOT NULL DEFAULT '',
                    host_name TEXT NOT NULL DEFAULT '',
                    version TEXT NOT NULL DEFAULT '',
                    last_seen_at TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS source_cache_entries (
                    entry_id TEXT PRIMARY KEY,
                    source_family TEXT NOT NULL DEFAULT '',
                    building TEXT NOT NULL DEFAULT '',
                    bucket_kind TEXT NOT NULL DEFAULT '',
                    bucket_key TEXT NOT NULL DEFAULT '',
                    duty_date TEXT NOT NULL DEFAULT '',
                    duty_shift TEXT NOT NULL DEFAULT '',
                    downloaded_at TEXT NOT NULL DEFAULT '',
                    relative_path TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    file_hash TEXT NOT NULL DEFAULT '',
                    size_bytes INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_source_cache_family_bucket
                    ON source_cache_entries(source_family, bucket_kind, bucket_key, building, status, downloaded_at);
                CREATE INDEX IF NOT EXISTS idx_source_cache_family_date
                    ON source_cache_entries(source_family, duty_date, duty_shift, building, status, downloaded_at);
                CREATE INDEX IF NOT EXISTS idx_source_cache_status_times
                    ON source_cache_entries(status, downloaded_at, updated_at, created_at, bucket_key);
                CREATE TABLE IF NOT EXISTS bridge_internal_issue_alerts (
                    alert_key TEXT PRIMARY KEY,
                    building TEXT NOT NULL DEFAULT '',
                    failure_kind TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    latest_detail TEXT NOT NULL DEFAULT '',
                    first_seen_at TEXT NOT NULL DEFAULT '',
                    last_seen_at TEXT NOT NULL DEFAULT '',
                    last_pushed_at TEXT NOT NULL DEFAULT '',
                    occurrence_count INTEGER NOT NULL DEFAULT 0,
                    active INTEGER NOT NULL DEFAULT 1,
                    last_task_id TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_bridge_internal_issue_alerts_due
                    ON bridge_internal_issue_alerts(active, last_seen_at, last_pushed_at, updated_at);
                CREATE TABLE IF NOT EXISTS bridge_external_alert_projection (
                    projection_key TEXT PRIMARY KEY,
                    building TEXT NOT NULL DEFAULT '',
                    failure_kind TEXT NOT NULL DEFAULT '',
                    alert_state TEXT NOT NULL DEFAULT '',
                    status_key TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    latest_detail TEXT NOT NULL DEFAULT '',
                    first_seen_at TEXT NOT NULL DEFAULT '',
                    last_seen_at TEXT NOT NULL DEFAULT '',
                    resolved_at TEXT NOT NULL DEFAULT '',
                    occurrence_count INTEGER NOT NULL DEFAULT 0,
                    still_unresolved INTEGER NOT NULL DEFAULT 1,
                    last_notified_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_bridge_external_alert_projection_building
                    ON bridge_external_alert_projection(building, still_unresolved, updated_at);
                    """
                )
                self._ensure_column(
                    conn,
                    table_name="bridge_internal_issue_alerts",
                    column_name="status_key",
                    ddl="ALTER TABLE bridge_internal_issue_alerts ADD COLUMN status_key TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_column(
                    conn,
                    table_name="bridge_internal_issue_alerts",
                    column_name="resolved_at",
                    ddl="ALTER TABLE bridge_internal_issue_alerts ADD COLUMN resolved_at TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_column(
                    conn,
                    table_name="bridge_internal_issue_alerts",
                    column_name="last_recovery_task_id",
                    ddl="ALTER TABLE bridge_internal_issue_alerts ADD COLUMN last_recovery_task_id TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_column(
                    conn,
                    table_name="bridge_internal_issue_alerts",
                    column_name="last_recovery_pushed_at",
                    ddl="ALTER TABLE bridge_internal_issue_alerts ADD COLUMN last_recovery_pushed_at TEXT NOT NULL DEFAULT ''",
                )
                row = conn.execute("SELECT COUNT(1) AS cnt FROM bridge_settings").fetchone()
                if not row or int(row["cnt"] or 0) <= 0:
                    now_text = _now_text()
                    conn.execute(
                        """
                        INSERT INTO bridge_settings(id, shared_schema_version, created_at, updated_at)
                        VALUES(1, 2, ?, ?)
                        """,
                        (now_text, now_text),
                    )
                else:
                    conn.execute("UPDATE bridge_settings SET shared_schema_version=2, updated_at=? WHERE id=1", (_now_text(),))
            self._ready = True

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, *, table_name: str, column_name: str, ddl: str) -> None:
        columns = {
            str(row["name"] or "").strip()
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if str(column_name or "").strip() not in columns:
            conn.execute(ddl)

    @contextmanager
    def connect(self, *, read_only: bool = False) -> Iterator[sqlite3.Connection]:
        conn: sqlite3.Connection | None = None
        lock = None if read_only else self._write_lock
        if lock is not None:
            lock.acquire()
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=self.busy_timeout_ms / 1000.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            if read_only:
                conn.execute("PRAGMA query_only=ON")
            else:
                conn.execute("PRAGMA journal_mode=DELETE")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA foreign_keys=ON")
            yield conn
            if conn is not None and not read_only and conn.in_transaction:
                conn.commit()
        except Exception:
            if conn is not None and conn.in_transaction:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()
            if lock is not None:
                lock.release()

    def upsert_node(
        self,
        *,
        node_id: str,
        role_mode: str,
        node_label: str,
        host_name: str,
        version: str,
        status: str = "online",
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_nodes(node_id, role_mode, node_label, host_name, version, last_seen_at, status)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    role_mode=excluded.role_mode,
                    node_label=excluded.node_label,
                    host_name=excluded.host_name,
                    version=excluded.version,
                    last_seen_at=excluded.last_seen_at,
                    status=excluded.status
                """,
                (node_id, role_mode, node_label, host_name, version, _now_text(), status),
            )

    def get_task_counts(self) -> Dict[str, int]:
        with self.connect(read_only=True) as conn:
            task_row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN status IN ('queued_for_internal', 'internal_claimed', 'internal_running') THEN 1 ELSE 0 END) AS pending_internal,
                    SUM(CASE WHEN status IN ('ready_for_external', 'external_claimed', 'external_running') THEN 1 ELSE 0 END) AS pending_external,
                    SUM(CASE WHEN status IN ('failed', 'partial_failed', 'stale') THEN 1 ELSE 0 END) AS problematic,
                    COUNT(1) AS total_count
                FROM bridge_tasks
                """
            ).fetchone()
            node_row = conn.execute("SELECT COUNT(1) AS cnt FROM bridge_nodes").fetchone()
        return {
            "pending_internal": int(task_row["pending_internal"] or 0) if task_row else 0,
            "pending_external": int(task_row["pending_external"] or 0) if task_row else 0,
            "problematic": int(task_row["problematic"] or 0) if task_row else 0,
            "total_count": int(task_row["total_count"] or 0) if task_row else 0,
            "node_count": int(node_row["cnt"] or 0) if node_row else 0,
        }

    def list_tasks(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                       status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                FROM bridge_tasks
                ORDER BY updated_at DESC, created_at DESC
                LIMIT ?
                """,
                (max(1, int(limit or 100)),),
            ).fetchall()
        return [self._row_to_task_dict(row) for row in rows]

    def get_task(self, task_id: str) -> Dict[str, Any] | None:
        task_text = str(task_id or "").strip()
        if not task_text:
            return None
        with self.connect(read_only=True) as conn:
            task_row = conn.execute(
                """
                SELECT task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                       status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                FROM bridge_tasks
                WHERE task_id=?
                """,
                (task_text,),
            ).fetchone()
            if not task_row:
                return None
            stage_rows = conn.execute(
                """
                SELECT task_id, stage_id, role_target, handler, status, input_json, result_json,
                       claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                FROM bridge_stages
                WHERE task_id=?
                ORDER BY stage_id
                """,
                (task_text,),
            ).fetchall()
            artifact_rows = conn.execute(
                """
                SELECT artifact_id, task_id, stage_id, artifact_kind, building, relative_path, status,
                       size_bytes, metadata_json, created_at, updated_at
                FROM bridge_artifacts
                WHERE task_id=?
                ORDER BY created_at
                """,
                (task_text,),
            ).fetchall()
            event_rows = conn.execute(
                """
                SELECT event_id, task_id, stage_id, side, level, event_type, payload_json, created_at
                FROM bridge_events
                WHERE task_id=?
                ORDER BY event_id DESC
                LIMIT 100
                """,
                (task_text,),
            ).fetchall()
        payload = self._row_to_task_dict(task_row)
        payload["stages"] = [self._row_to_stage_dict(row) for row in stage_rows]
        payload["artifacts"] = [self._row_to_artifact_dict(row) for row in artifact_rows]
        payload["events"] = [self._row_to_event_dict(row) for row in event_rows]
        return payload

    def find_active_task_by_dedupe_key(self, dedupe_key: str) -> Dict[str, Any] | None:
        dedupe_text = str(dedupe_key or "").strip()
        if not dedupe_text:
            return None
        try:
            mailbox_tasks = self._mailbox_store.list_tasks(limit=1000)
        except Exception:
            mailbox_tasks = []
        active_mailbox_tasks = [
            task
            for task in mailbox_tasks
            if isinstance(task, dict)
            and str(task.get("dedupe_key", "") or "").strip() == dedupe_text
            and str(task.get("status", "") or "").strip().lower() not in _TERMINAL_TASK_STATUSES
        ]
        if active_mailbox_tasks:
            active_mailbox_tasks.sort(
                key=lambda item: (
                    str(item.get("updated_at", "") or "").strip(),
                    str(item.get("created_at", "") or "").strip(),
                    str(item.get("task_id", "") or "").strip(),
                ),
                reverse=True,
            )
            return active_mailbox_tasks[0]
        placeholders = ", ".join("?" for _ in _TERMINAL_TASK_STATUSES)
        with self.connect(read_only=True) as conn:
            row = conn.execute(
                f"""
                SELECT task_id
                FROM bridge_tasks
                WHERE dedupe_key=?
                  AND status NOT IN ({placeholders})
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 1
                """,
                (dedupe_text, *_TERMINAL_TASK_STATUSES),
            ).fetchone()
        if not row:
            return None
        return self.get_task(str(row["task_id"] or "").strip())

    def upsert_internal_issue_alert(
        self,
        *,
        building: str,
        failure_kind: str,
        status_key: str,
        summary: str,
        latest_detail: str,
        observed_at: str = "",
        active: bool = True,
    ) -> Dict[str, Any]:
        building_text = str(building or "").strip()
        failure_kind_text = str(failure_kind or "").strip().lower() or "browser_issue"
        status_key_text = str(status_key or "").strip().lower() or ("healthy" if not active else "suspended")
        if not building_text:
            raise ValueError("bridge_internal_issue_alerts 缺少楼栋")
        alert_key = f"{building_text}|{failure_kind_text}"
        observed_at_text = str(observed_at or "").strip() or _now_text()
        summary_text = str(summary or "").strip()
        detail_text = str(latest_detail or "").strip() or summary_text
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT alert_key, building, failure_kind, summary, latest_detail,
                       first_seen_at, last_seen_at, last_pushed_at, occurrence_count,
                       active, last_task_id, updated_at, status_key, resolved_at,
                       last_recovery_task_id, last_recovery_pushed_at
                FROM bridge_internal_issue_alerts
                WHERE alert_key=?
                """,
                (alert_key,),
            ).fetchone()
            if row:
                previous_summary = str(row["summary"] or "").strip()
                previous_detail = str(row["latest_detail"] or "").strip()
                previous_status_key = str(row["status_key"] or "").strip().lower()
                previous_active = bool(int(row["active"] or 0))
                previous_last_seen_at = str(row["last_seen_at"] or "").strip() or observed_at_text
                occurrence_count = int(row["occurrence_count"] or 0)
                changed = (
                    summary_text != previous_summary
                    or detail_text != previous_detail
                    or status_key_text != previous_status_key
                    or active != previous_active
                )
                next_last_seen_at = observed_at_text if changed or not previous_active else previous_last_seen_at
                if active and changed:
                    occurrence_count += 1
                conn.execute(
                    """
                    UPDATE bridge_internal_issue_alerts
                    SET summary=?,
                        latest_detail=?,
                        status_key=?,
                        last_seen_at=?,
                        occurrence_count=?,
                        active=?,
                        resolved_at=?,
                        last_recovery_task_id=?,
                        last_recovery_pushed_at=?,
                        updated_at=?
                    WHERE alert_key=?
                    """,
                    (
                        summary_text,
                        detail_text,
                        status_key_text,
                        next_last_seen_at,
                        occurrence_count,
                        1 if active else 0,
                        "" if active else str(row["resolved_at"] or "").strip(),
                        "" if active else str(row["last_recovery_task_id"] or "").strip(),
                        "" if active else str(row["last_recovery_pushed_at"] or "").strip(),
                        observed_at_text,
                        alert_key,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO bridge_internal_issue_alerts(
                        alert_key, building, failure_kind, summary, latest_detail,
                        first_seen_at, last_seen_at, last_pushed_at, occurrence_count,
                        active, last_task_id, updated_at, status_key, resolved_at,
                        last_recovery_task_id, last_recovery_pushed_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, '', ?, ?, '', ?, ?, '', '', '')
                    """,
                    (
                        alert_key,
                        building_text,
                        failure_kind_text,
                        summary_text,
                        detail_text,
                        observed_at_text,
                        observed_at_text,
                        1 if active else 0,
                        1 if active else 0,
                        observed_at_text,
                        status_key_text,
                    ),
                )
        payload = self.get_internal_issue_alert(alert_key)
        if not payload:
            raise RuntimeError(f"重新加载内网问题告警失败 {alert_key}")
        return payload

    def clear_internal_issue_alert(self, building: str, failure_kind: str, *, observed_at: str = "") -> None:
        building_text = str(building or "").strip()
        failure_kind_text = str(failure_kind or "").strip().lower() or "browser_issue"
        if not building_text:
            return
        alert_key = f"{building_text}|{failure_kind_text}"
        observed_at_text = str(observed_at or "").strip() or _now_text()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE bridge_internal_issue_alerts
                SET active=0,
                    status_key='healthy',
                    last_seen_at=?,
                    resolved_at=?,
                    updated_at=?
                WHERE alert_key=?
                """,
                (observed_at_text, observed_at_text, observed_at_text, alert_key),
            )

    def get_internal_issue_alert(self, alert_key: str) -> Dict[str, Any] | None:
        alert_key_text = str(alert_key or "").strip()
        if not alert_key_text:
            return None
        with self.connect(read_only=True) as conn:
            row = conn.execute(
                """
                SELECT alert_key, building, failure_kind, summary, latest_detail,
                       first_seen_at, last_seen_at, last_pushed_at, occurrence_count,
                       active, last_task_id, updated_at, status_key, resolved_at,
                       last_recovery_task_id, last_recovery_pushed_at
                FROM bridge_internal_issue_alerts
                WHERE alert_key=?
                """,
                (alert_key_text,),
            ).fetchone()
        if not row:
            return None
        return self._row_to_internal_issue_alert_dict(row)

    def list_due_internal_issue_alerts(
        self,
        *,
        quiet_window_sec: int = 600,
        dedupe_window_sec: int = 3600,
    ) -> List[Dict[str, Any]]:
        now_dt = datetime.now()
        quiet_before = (now_dt - timedelta(seconds=max(60, int(quiet_window_sec or 600)))).strftime("%Y-%m-%d %H:%M:%S")
        dedupe_before = (now_dt - timedelta(seconds=max(60, int(dedupe_window_sec or 3600)))).strftime("%Y-%m-%d %H:%M:%S")
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT alert_key, building, failure_kind, summary, latest_detail,
                       first_seen_at, last_seen_at, last_pushed_at, occurrence_count,
                       active, last_task_id, updated_at, status_key, resolved_at,
                       last_recovery_task_id, last_recovery_pushed_at
                FROM bridge_internal_issue_alerts
                WHERE active=1
                  AND last_seen_at <= ?
                  AND (last_pushed_at='' OR last_pushed_at <= ?)
                ORDER BY last_seen_at ASC, alert_key ASC
                """,
                (quiet_before, dedupe_before),
            ).fetchall()
        return [self._row_to_internal_issue_alert_dict(row) for row in rows]

    def list_active_internal_issue_alerts(self) -> List[Dict[str, Any]]:
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT alert_key, building, failure_kind, summary, latest_detail,
                       first_seen_at, last_seen_at, last_pushed_at, occurrence_count,
                       active, last_task_id, updated_at, status_key, resolved_at,
                       last_recovery_task_id, last_recovery_pushed_at
                FROM bridge_internal_issue_alerts
                WHERE active=1
                ORDER BY updated_at DESC, alert_key ASC
                """
            ).fetchall()
        return [self._row_to_internal_issue_alert_dict(row) for row in rows]

    def list_due_internal_issue_recoveries(self) -> List[Dict[str, Any]]:
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT alert_key, building, failure_kind, summary, latest_detail,
                       first_seen_at, last_seen_at, last_pushed_at, occurrence_count,
                       active, last_task_id, updated_at, status_key, resolved_at,
                       last_recovery_task_id, last_recovery_pushed_at
                FROM bridge_internal_issue_alerts
                WHERE active=0
                  AND last_pushed_at<>''
                  AND resolved_at<>''
                  AND last_recovery_pushed_at=''
                ORDER BY resolved_at ASC, alert_key ASC
                """
            ).fetchall()
        return [self._row_to_internal_issue_alert_dict(row) for row in rows]

    def mark_internal_issue_alert_pushed(self, alert_key: str, *, task_id: str = "", pushed_at: str = "") -> None:
        alert_key_text = str(alert_key or "").strip()
        if not alert_key_text:
            return
        pushed_at_text = str(pushed_at or "").strip() or _now_text()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE bridge_internal_issue_alerts
                SET last_pushed_at=?,
                    last_task_id=COALESCE(NULLIF(?, ''), last_task_id),
                    updated_at=?
                WHERE alert_key=?
                """,
                (pushed_at_text, str(task_id or "").strip(), pushed_at_text, alert_key_text),
            )

    def mark_internal_issue_alert_recovery_pushed(
        self,
        alert_key: str,
        *,
        task_id: str = "",
        pushed_at: str = "",
    ) -> None:
        alert_key_text = str(alert_key or "").strip()
        if not alert_key_text:
            return
        pushed_at_text = str(pushed_at or "").strip() or _now_text()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE bridge_internal_issue_alerts
                SET last_recovery_task_id=COALESCE(NULLIF(?, ''), last_recovery_task_id),
                    last_recovery_pushed_at=?,
                    updated_at=?
                WHERE alert_key=?
                """,
                (str(task_id or "").strip(), pushed_at_text, pushed_at_text, alert_key_text),
            )

    def upsert_external_alert_projection(
        self,
        *,
        building: str,
        failure_kind: str,
        alert_state: str,
        status_key: str,
        summary: str,
        latest_detail: str,
        first_seen_at: str,
        last_seen_at: str,
        resolved_at: str = "",
        occurrence_count: int = 0,
        still_unresolved: bool = True,
        last_notified_at: str = "",
    ) -> Dict[str, Any]:
        building_text = str(building or "").strip()
        failure_kind_text = str(failure_kind or "").strip().lower() or "browser_issue"
        if not building_text:
            raise ValueError("bridge_external_alert_projection 缺少楼栋")
        projection_key = f"{building_text}|{failure_kind_text}"
        alert_state_text = str(alert_state or "").strip().lower() or "problem"
        status_key_text = str(status_key or "").strip().lower() or ("healthy" if alert_state_text == "recovered" else "suspended")
        notified_at_text = str(last_notified_at or "").strip() or _now_text()
        updated_at_text = _now_text()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_external_alert_projection(
                    projection_key, building, failure_kind, alert_state, status_key, summary, latest_detail,
                    first_seen_at, last_seen_at, resolved_at, occurrence_count, still_unresolved,
                    last_notified_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(projection_key) DO UPDATE SET
                    alert_state=excluded.alert_state,
                    status_key=excluded.status_key,
                    summary=excluded.summary,
                    latest_detail=excluded.latest_detail,
                    first_seen_at=excluded.first_seen_at,
                    last_seen_at=excluded.last_seen_at,
                    resolved_at=excluded.resolved_at,
                    occurrence_count=excluded.occurrence_count,
                    still_unresolved=excluded.still_unresolved,
                    last_notified_at=excluded.last_notified_at,
                    updated_at=excluded.updated_at
                """,
                (
                    projection_key,
                    building_text,
                    failure_kind_text,
                    alert_state_text,
                    status_key_text,
                    str(summary or "").strip(),
                    str(latest_detail or "").strip(),
                    str(first_seen_at or "").strip(),
                    str(last_seen_at or "").strip(),
                    str(resolved_at or "").strip(),
                    max(0, int(occurrence_count or 0)),
                    1 if still_unresolved else 0,
                    notified_at_text,
                    updated_at_text,
                ),
            )
        return {
            "projection_key": projection_key,
            "building": building_text,
            "failure_kind": failure_kind_text,
            "alert_state": alert_state_text,
            "status_key": status_key_text,
            "summary": str(summary or "").strip(),
            "latest_detail": str(latest_detail or "").strip(),
            "first_seen_at": str(first_seen_at or "").strip(),
            "last_seen_at": str(last_seen_at or "").strip(),
            "resolved_at": str(resolved_at or "").strip(),
            "occurrence_count": max(0, int(occurrence_count or 0)),
            "still_unresolved": bool(still_unresolved),
            "last_notified_at": notified_at_text,
            "updated_at": updated_at_text,
        }

    def list_external_alert_projections(self) -> List[Dict[str, Any]]:
        self.ensure_ready()
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT projection_key, building, failure_kind, alert_state, status_key, summary, latest_detail,
                       first_seen_at, last_seen_at, resolved_at, occurrence_count, still_unresolved,
                       last_notified_at, updated_at
                FROM bridge_external_alert_projection
                ORDER BY updated_at DESC, projection_key ASC
                """
            ).fetchall()
        return [self._row_to_external_alert_projection_dict(row) for row in rows]

    def cancel_task(self, task_id: str) -> bool:
        task_text = str(task_id or "").strip()
        if not task_text:
            return False
        now_text = _now_text()
        changed = False
        mailbox_payload: Dict[str, Any] | None = None
        with self.connect() as conn:
            row = conn.execute("SELECT status FROM bridge_tasks WHERE task_id=?", (task_text,)).fetchone()
            if not row:
                return False
            status = str(row["status"] or "").strip().lower()
            if status not in {"success", "failed", "partial_failed", "cancelled"}:
                changed = True
                conn.execute(
                    "UPDATE bridge_tasks SET status='cancelled', updated_at=?, revision=revision+1 WHERE task_id=?",
                    (now_text, task_text),
                )
                conn.execute(
                    """
                    UPDATE bridge_stages
                    SET status='cancelled', finished_at=?, revision=revision+1
                    WHERE task_id=? AND status NOT IN ('success', 'failed', 'cancelled')
                    """,
                    (now_text, task_text),
                )
                self._insert_event(
                    conn,
                    task_id=task_text,
                    stage_id="",
                    side="",
                    level="warning",
                    event_type="cancelled",
                    payload={"message": "任务已取消"},
                )
            if changed:
                mailbox_payload = self._sync_task_mailbox_from_conn(conn, task_text)
        if mailbox_payload:
            self._sync_task_mailbox(mailbox_payload)
        return True
    def retry_task(self, task_id: str, *, record_event: bool = True, sync_mailbox: bool = True) -> bool:
        task_text = str(task_id or "").strip()
        if not task_text:
            return False
        mailbox_payload: Dict[str, Any] | None = None
        with self.connect() as conn:
            row = conn.execute(
                "SELECT status, result_json FROM bridge_tasks WHERE task_id=?",
                (task_text,),
            ).fetchone()
            if not row:
                return False
            status = str(row["status"] or "").strip().lower()
            if status == "success":
                return False
            stage_rows = conn.execute(
                """
                SELECT stage_id, role_target, status, result_json
                FROM bridge_stages
                WHERE task_id=?
                """,
                (task_text,),
            ).fetchall()
            if not stage_rows:
                return False
            task_result = self._loads(row["result_json"])
            internal_stage = next((item for item in stage_rows if str(item["role_target"] or "").strip().lower() == "internal"), None)
            external_stage = next((item for item in stage_rows if str(item["role_target"] or "").strip().lower() == "external"), None)
            internal_stage_status = str(internal_stage["status"] or "").strip().lower() if internal_stage else ""
            retry_from_external = bool(internal_stage and external_stage and internal_stage_status == "success")
            if retry_from_external:
                reset_stage_ids = [str(external_stage["stage_id"] or "").strip()]
                next_task_status = "ready_for_external"
                internal_result = self._loads(internal_stage["result_json"]) or (
                    task_result.get("internal", {}) if isinstance(task_result, dict) else {}
                )
                next_result = {"status": "ready_for_external", "internal": internal_result}
            else:
                reset_stage_ids = [
                    str(item["stage_id"] or "").strip()
                    for item in stage_rows
                    if str(item["stage_id"] or "").strip()
                ]
                next_task_status = "queued_for_internal"
                next_result = {"status": "queued_for_internal"}
            if not reset_stage_ids:
                return False
            placeholders = ",".join("?" for _ in reset_stage_ids)
            now_text = _now_text()
            conn.execute(
                "UPDATE bridge_tasks SET status=?, result_json=?, error='', updated_at=?, revision=revision+1 WHERE task_id=?",
                (
                    next_task_status,
                    json.dumps(next_result, ensure_ascii=False),
                    now_text,
                    task_text,
                ),
            )
            conn.execute(
                f"""
                UPDATE bridge_stages
                SET status='pending', result_json='{{}}', error='', claim_token='', claimed_by_node_id='',
                    lease_expires_at='', started_at='', finished_at='', revision=revision+1
                WHERE task_id=? AND stage_id IN ({placeholders})
                """,
                [task_text, *reset_stage_ids],
            )
            if record_event:
                self._insert_event(
                    conn,
                    task_id=task_text,
                    stage_id="",
                    side="",
                    level="info",
                    event_type="retried",
                    payload={
                        "message": "任务已重新排队",
                        "retry_from": "external" if retry_from_external else "internal",
                        "next_status": next_task_status,
                    },
                )
            if sync_mailbox:
                mailbox_payload = self._sync_task_mailbox_from_conn(conn, task_text)
        if mailbox_payload:
            self._sync_task_mailbox(mailbox_payload)
        return True
    def create_handover_from_download_task(
        self,
        *,
        buildings: List[str] | None,
        end_time: str | None,
        duty_date: str | None,
        duty_shift: str | None,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        normalized_buildings = [
            str(item or "").strip()
            for item in (buildings or [])
            if str(item or "").strip()
        ]
        request_payload = {
            "buildings": normalized_buildings,
            "end_time": str(end_time or "").strip(),
            "duty_date": str(duty_date or "").strip(),
            "duty_shift": str(duty_shift or "").strip().lower(),
            "resume_job_id": str(resume_job_id or "").strip(),
            "target_bucket_key": str(target_bucket_key or "").strip(),
        }
        if request_payload["duty_date"] and request_payload["duty_shift"]:
            dedupe_key = "|".join(
                [
                    "handover_from_download",
                    "date",
                    request_payload["duty_date"],
                    request_payload["duty_shift"],
                    ",".join(normalized_buildings) or "all_enabled",
                    request_payload["end_time"] or "-",
                ]
            )
        else:
            dedupe_key = "|".join(
                [
                    "handover_from_download",
                    "latest",
                    request_payload["target_bucket_key"] or "-",
                    ",".join(normalized_buildings) or "all_enabled",
                    request_payload["end_time"] or "-",
                ]
            )
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "handover_from_download",
                    "bridge",
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "manual",
                    "queued_for_internal",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    "{}",
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.executemany(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                [
                    (
                        task_id,
                        "internal_download",
                        "internal",
                        "handover_from_download_internal",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                    (
                        task_id,
                        "external_generate_review_output",
                        "external",
                        "handover_from_shared_files_external",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                ],
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="info",
                event_type="created",
                payload={
                    "message": "已创建共享桥接任务",
                    "feature": "handover_from_download",
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载共享任务失败 {task_id}")
        return payload

    def create_day_metric_from_download_task(
        self,
        *,
        selected_dates: List[str],
        building_scope: str,
        building: str | None,
        resume_job_id: str | None = None,
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        normalized_dates = [
            str(item or "").strip()
            for item in (selected_dates or [])
            if str(item or "").strip()
        ]
        request_payload = {
            "selected_dates": normalized_dates,
            "building_scope": str(building_scope or "").strip(),
            "building": str(building or "").strip(),
            "resume_job_id": str(resume_job_id or "").strip(),
        }
        dedupe_key = "|".join(
            [
                "day_metric_from_download",
                ",".join(normalized_dates) or "-",
                request_payload["building_scope"] or "-",
                request_payload["building"] or "all_enabled",
            ]
        )
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "day_metric_from_download",
                    "bridge",
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "manual",
                    "queued_for_internal",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    "{}",
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.executemany(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                [
                    (
                        task_id,
                        "internal_download",
                        "internal",
                        "day_metric_from_download_internal",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                    (
                        task_id,
                        "external_upload",
                        "external",
                        "day_metric_from_shared_files_external",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                ],
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="info",
                event_type="created",
                payload={
                    "message": "已创建共享桥接任务",
                    "feature": "day_metric_from_download",
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载共享任务失败 {task_id}")
        return payload

    def create_wet_bulb_collection_task(
        self,
        *,
        buildings: List[str] | None,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        normalized_buildings = [
            str(item or "").strip()
            for item in (buildings or [])
            if str(item or "").strip()
        ]
        request_payload = {
            "buildings": normalized_buildings,
            "resume_job_id": str(resume_job_id or "").strip(),
            "target_bucket_key": str(target_bucket_key or "").strip(),
        }
        dedupe_key = "|".join(
            [
                "wet_bulb_collection",
                request_payload["target_bucket_key"] or now_text[:10],
                ",".join(normalized_buildings) or "all_enabled",
            ]
        )
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "wet_bulb_collection",
                    "bridge",
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "manual",
                    "queued_for_internal",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    "{}",
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.executemany(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                [
                    (
                        task_id,
                        "internal_download",
                        "internal",
                        "wet_bulb_collection_internal",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                    (
                        task_id,
                        "external_extract_and_upload",
                        "external",
                        "wet_bulb_collection_external",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                ],
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="info",
                event_type="created",
                payload={
                    "message": "已创建共享桥接任务",
                    "feature": "wet_bulb_collection",
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载共享任务失败 {task_id}")
        return payload

    def create_branch_power_upload_task(
        self,
        *,
        buildings: List[str] | None,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        target_bucket_keys: List[str] | None = None,
        range_query_start: str | None = None,
        range_query_end: str | None = None,
        requested_source_units: List[Dict[str, Any]] | None = None,
        mode: str | None = None,
        target_business_date: str | None = None,
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        normalized_buildings = [
            str(item or "").strip()
            for item in (buildings or [])
            if str(item or "").strip()
        ]
        normalized_bucket_keys = [
            str(item or "").strip()
            for item in (target_bucket_keys or [])
            if str(item or "").strip()
        ]
        normalized_requested_source_units = _normalize_branch_requested_source_units(requested_source_units)
        resolved_target_bucket_key = str(target_bucket_key or "").strip() or (normalized_bucket_keys[0] if normalized_bucket_keys else "")
        if not resolved_target_bucket_key and normalized_requested_source_units:
            first_unit_keys = normalized_requested_source_units[0].get("target_bucket_keys", [])
            if isinstance(first_unit_keys, list) and first_unit_keys:
                resolved_target_bucket_key = str(first_unit_keys[0] or "").strip()
        if not normalized_bucket_keys and normalized_requested_source_units:
            normalized_bucket_keys = sorted(
                {
                    str(bucket_key or "").strip()
                    for unit in normalized_requested_source_units
                    for bucket_key in (unit.get("target_bucket_keys", []) if isinstance(unit.get("target_bucket_keys", []), list) else [])
                    if str(bucket_key or "").strip()
                }
            )
        if not normalized_buildings and normalized_requested_source_units:
            normalized_buildings = sorted(
                {
                    str(unit.get("building", "") or "").strip()
                    for unit in normalized_requested_source_units
                    if str(unit.get("building", "") or "").strip()
                }
            )
        request_payload = {
            "buildings": normalized_buildings,
            "resume_job_id": str(resume_job_id or "").strip(),
            "target_bucket_key": resolved_target_bucket_key,
            "target_bucket_keys": normalized_bucket_keys,
            "range_query_start": str(range_query_start or "").strip(),
            "range_query_end": str(range_query_end or "").strip(),
            "requested_source_units": normalized_requested_source_units,
            "mode": str(mode or "").strip(),
            "target_business_date": str(target_business_date or "").strip(),
        }
        bucket_dedupe = ",".join(normalized_bucket_keys) or request_payload["target_bucket_key"]
        dedupe_parts = [
            "branch_power_upload",
            bucket_dedupe or now_text[:13],
            ",".join(normalized_buildings) or "all_enabled",
        ]
        unit_dedupe = _branch_requested_units_dedupe(normalized_requested_source_units)
        if unit_dedupe:
            dedupe_parts.append(unit_dedupe)
        dedupe_key = "|".join(dedupe_parts)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "branch_power_upload",
                    "bridge",
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "manual",
                    "queued_for_internal",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    "{}",
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.executemany(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                [
                    (
                        task_id,
                        "internal_download",
                        "internal",
                        "branch_power_from_download_internal",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                    (
                        task_id,
                        "external_upload",
                        "external",
                        "branch_power_from_shared_files_external",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                ],
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="info",
                event_type="created",
                payload={
                    "message": "已创建共享桥接任务",
                    "feature": "branch_power_upload",
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载共享任务失败 {task_id}")
        return payload

    def create_alarm_event_upload_task(
        self,
        *,
        mode: str,
        building: str | None,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        normalized_mode = str(mode or "").strip().lower() or "full"
        normalized_building = str(building or "").strip()
        request_payload = {
            "mode": normalized_mode,
            "building": normalized_building,
            "resume_job_id": str(resume_job_id or "").strip(),
            "target_bucket_key": str(target_bucket_key or "").strip(),
        }
        dedupe_key = "|".join(
            [
                "alarm_event_upload",
                normalized_mode,
                normalized_building or "all",
                request_payload["target_bucket_key"] or now_text[:13],
            ]
        )
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "alarm_event_upload",
                    normalized_mode,
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "manual",
                    "queued_for_internal",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    "{}",
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.executemany(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                [
                    (
                        task_id,
                        "internal_fill",
                        "internal",
                        "alarm_event_upload_internal",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                    (
                        task_id,
                        "external_upload",
                        "external",
                        "alarm_event_upload_external",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                ],
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="info",
                event_type="created",
                payload={
                    "message": "已创建告警信息上传共享桥接任务",
                    "feature": "alarm_event_upload",
                    "mode": normalized_mode,
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载共享任务失败 {task_id}")
        return payload

    def create_monthly_auto_once_task(
        self,
        *,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "manual",
        source: str = "manual",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        request_payload = {
            "source": str(source or "").strip() or "manual",
            "resume_job_id": str(resume_job_id or "").strip(),
            "target_bucket_key": str(target_bucket_key or "").strip(),
        }
        dedupe_key = "|".join(
            [
                "monthly_report_pipeline",
                "auto_once",
                request_payload["target_bucket_key"] or now_text[:10] or "-",
            ]
        )
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "monthly_report_pipeline",
                    "auto_once",
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "manual",
                    "queued_for_internal",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    "{}",
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.executemany(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                [
                    (
                        task_id,
                        "internal_download",
                        "internal",
                        "monthly_auto_once_internal",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                    (
                        task_id,
                        "external_resume",
                        "external",
                        "monthly_resume_from_shared_external",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                ],
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="info",
                event_type="created",
                payload={
                    "message": "已创建共享桥接任务",
                    "feature": "monthly_report_pipeline",
                    "mode": "auto_once",
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载共享任务失败 {task_id}")
        return payload

    def create_monthly_multi_date_task(
        self,
        *,
        selected_dates: List[str],
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        normalized_dates = [
            str(item or "").strip()
            for item in (selected_dates or [])
            if str(item or "").strip()
        ]
        request_payload = {"selected_dates": normalized_dates}
        dedupe_key = "|".join(["monthly_report_pipeline", "multi_date", ",".join(normalized_dates) or "-"])
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "monthly_report_pipeline",
                    "multi_date",
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "manual",
                    "queued_for_internal",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    "{}",
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.executemany(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                [
                    (
                        task_id,
                        "internal_download",
                        "internal",
                        "monthly_multi_date_internal",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                    (
                        task_id,
                        "external_resume",
                        "external",
                        "monthly_resume_from_shared_external",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                ],
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="info",
                event_type="created",
                payload={
                    "message": "已创建共享桥接任务",
                    "feature": "monthly_report_pipeline",
                    "mode": "multi_date",
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载共享任务失败 {task_id}")
        return payload

    def create_monthly_resume_upload_task(
        self,
        *,
        run_id: str | None,
        auto_trigger: bool,
        resume_job_id: str | None = None,
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        request_payload = {
            "run_id": str(run_id or "").strip(),
            "auto_trigger": bool(auto_trigger),
            "resume_job_id": str(resume_job_id or "").strip(),
        }
        dedupe_key = "|".join(["monthly_report_pipeline", "resume_upload", request_payload["run_id"] or "latest"])
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "monthly_report_pipeline",
                    "resume_upload",
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "manual",
                    "ready_for_external",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    json.dumps({"status": "ready_for_external"}, ensure_ascii=False),
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.execute(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                (
                    task_id,
                    "external_resume",
                    "external",
                    "monthly_resume_from_shared_external",
                    "pending",
                    json.dumps(request_payload, ensure_ascii=False),
                ),
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="info",
                event_type="created",
                payload={
                    "message": "已创建共享桥接任务",
                    "feature": "monthly_report_pipeline",
                    "mode": "resume_upload",
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载共享任务失败 {task_id}")
        return payload

    def create_handover_cache_fill_task(
        self,
        *,
        continuation_kind: str,
        buildings: List[str] | None,
        duty_date: str | None,
        duty_shift: str | None,
        selected_dates: List[str] | None,
        building_scope: str | None,
        building: str | None,
        resume_job_id: str | None = None,
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        normalized_buildings = [str(item or "").strip() for item in (buildings or []) if str(item or "").strip()]
        normalized_dates = [str(item or "").strip() for item in (selected_dates or []) if str(item or "").strip()]
        request_payload = {
            "continuation_kind": str(continuation_kind or "").strip().lower(),
            "buildings": normalized_buildings,
            "duty_date": str(duty_date or "").strip(),
            "duty_shift": str(duty_shift or "").strip().lower(),
            "selected_dates": normalized_dates,
            "building_scope": str(building_scope or "").strip(),
            "building": str(building or "").strip(),
            "resume_job_id": str(resume_job_id or "").strip(),
        }
        dedupe_key = "|".join(
            [
                "handover_cache_fill",
                request_payload["continuation_kind"] or "-",
                request_payload["duty_date"] or ",".join(normalized_dates) or "-",
                request_payload["duty_shift"] or "-",
                request_payload["building"] or ",".join(normalized_buildings) or request_payload["building_scope"] or "all_enabled",
            ]
        )
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "handover_cache_fill",
                    request_payload["continuation_kind"] or "handover",
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "manual",
                    "queued_for_internal",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    "{}",
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.executemany(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                [
                    (
                        task_id,
                        "internal_fill",
                        "internal",
                        "handover_cache_fill_internal",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                    (
                        task_id,
                        "external_continue",
                        "external",
                        "handover_cache_fill_external",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                ],
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="info",
                event_type="created",
                payload={
                    "message": "已创建共享缓存补采任务",
                    "feature": "handover_cache_fill",
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载共享任务失败 {task_id}")
        return payload

    def create_monthly_cache_fill_task(
        self,
        *,
        selected_dates: List[str] | None,
        resume_job_id: str | None = None,
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        normalized_dates = [str(item or "").strip() for item in (selected_dates or []) if str(item or "").strip()]
        request_payload = {
            "selected_dates": normalized_dates,
            "resume_job_id": str(resume_job_id or "").strip(),
        }
        dedupe_key = "|".join(["monthly_cache_fill", ",".join(normalized_dates) or "-"])
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "monthly_cache_fill",
                    "multi_date",
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "manual",
                    "queued_for_internal",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    "{}",
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.executemany(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                [
                    (
                        task_id,
                        "internal_fill",
                        "internal",
                        "monthly_cache_fill_internal",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                    (
                        task_id,
                        "external_continue",
                        "external",
                        "monthly_cache_fill_external",
                        "pending",
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                ],
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="info",
                event_type="created",
                payload={
                    "message": "已创建月报历史缓存补采任务",
                    "feature": "monthly_cache_fill",
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载共享任务失败 {task_id}")
        return payload

    def create_internal_browser_alert_task(
        self,
        *,
        building: str,
        failure_kind: str,
        alert_state: str,
        status_key: str,
        summary: str,
        latest_detail: str,
        first_seen_at: str,
        last_seen_at: str,
        resolved_at: str,
        occurrence_count: int,
        still_unresolved: bool,
        created_by_role: str,
        created_by_node_id: str,
        requested_by: str = "internal_monitor",
    ) -> Dict[str, Any]:
        task_id = uuid.uuid4().hex
        now_text = _now_text()
        payload: Dict[str, Any] | None = None
        building_text = str(building or "").strip()
        failure_kind_text = str(failure_kind or "").strip().lower() or "browser_issue"
        alert_state_text = str(alert_state or "").strip().lower() or "problem"
        status_key_text = str(status_key or "").strip().lower() or ("healthy" if alert_state_text == "recovered" else "suspended")
        request_payload = {
            "building": building_text,
            "failure_kind": failure_kind_text,
            "alert_state": alert_state_text,
            "status_key": status_key_text,
            "summary": str(summary or "").strip(),
            "latest_detail": str(latest_detail or "").strip(),
            "first_seen_at": str(first_seen_at or "").strip(),
            "last_seen_at": str(last_seen_at or "").strip(),
            "resolved_at": str(resolved_at or "").strip(),
            "occurrence_count": max(1, int(occurrence_count or 0)),
            "still_unresolved": bool(still_unresolved),
        }
        dedupe_key = "|".join(["internal_browser_alert", alert_state_text or "-", building_text or "-", failure_kind_text or "-"])
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_tasks(
                    task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                    status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    task_id,
                    "internal_browser_alert",
                    "aggregate",
                    str(created_by_role or "").strip(),
                    str(created_by_node_id or "").strip(),
                    str(requested_by or "").strip() or "internal_monitor",
                    "ready_for_external",
                    dedupe_key,
                    json.dumps(request_payload, ensure_ascii=False),
                    "{}",
                    "",
                    now_text,
                    now_text,
                ),
            )
            conn.execute(
                """
                INSERT INTO bridge_stages(
                    task_id, stage_id, role_target, handler, status, input_json, result_json,
                    claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
                ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
                """,
                (
                    task_id,
                    "external_notify",
                    "external",
                    "internal_browser_alert_external",
                    "pending",
                    json.dumps(request_payload, ensure_ascii=False),
                ),
            )
            self._insert_event(
                conn,
                task_id=task_id,
                stage_id="",
                side=str(created_by_role or "").strip(),
                level="warning",
                event_type="created",
                payload={
                    "message": "已创建内网环境告警共享任务",
                    "feature": "internal_browser_alert",
                    "dedupe_key": dedupe_key,
                    "request": request_payload,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, task_id)
        if payload:
            self._sync_task_mailbox(payload)
        if not payload:
            raise RuntimeError(f"重新加载内网环境告警任务失败 {task_id}")
        return payload

    def claim_next_task(self, *, role_target: str, node_id: str, lease_sec: int = 30) -> Dict[str, Any] | None:
        role_text = str(role_target or "").strip().lower()
        if role_text not in {"internal", "external"}:
            return None
        task_status = "queued_for_internal" if role_text == "internal" else "ready_for_external"
        running_status = "internal_running" if role_text == "internal" else "external_running"
        now_dt = datetime.now()
        now_text = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        lease_expires_at = (now_dt + timedelta(seconds=max(5, int(lease_sec or 30)))).strftime("%Y-%m-%d %H:%M:%S")
        claim_token = uuid.uuid4().hex
        claimed_task_id = ""
        claimed_stage_id = ""
        payload: Dict[str, Any] | None = None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT t.task_id, s.stage_id
                FROM bridge_tasks t
                JOIN bridge_stages s ON s.task_id = t.task_id
                WHERE t.status=?
                  AND s.role_target=?
                  AND s.status='pending'
                ORDER BY t.created_at ASC, t.task_id ASC
                LIMIT 1
                """,
                (task_status, role_text),
            ).fetchone()
            if not row:
                return None
            claimed_task_id = str(row["task_id"] or "").strip()
            claimed_stage_id = str(row["stage_id"] or "").strip()
            if not claimed_task_id or not claimed_stage_id:
                return None
            task_update = conn.execute(
                """
                UPDATE bridge_tasks
                SET status=?, updated_at=?, revision=revision+1
                WHERE task_id=? AND status=?
                """,
                (running_status, now_text, claimed_task_id, task_status),
            )
            if int(task_update.rowcount or 0) <= 0:
                return None
            stage_update = conn.execute(
                """
                UPDATE bridge_stages
                SET status='running',
                    claimed_by_node_id=?,
                    claim_token=?,
                    lease_expires_at=?,
                    started_at=CASE WHEN started_at='' THEN ? ELSE started_at END,
                    finished_at='',
                    error='',
                    revision=revision+1
                WHERE task_id=? AND stage_id=? AND role_target=? AND status='pending'
                """,
                (
                    str(node_id or "").strip(),
                    claim_token,
                    lease_expires_at,
                    now_text,
                    claimed_task_id,
                    claimed_stage_id,
                    role_text,
                ),
            )
            if int(stage_update.rowcount or 0) <= 0:
                conn.execute(
                    "UPDATE bridge_tasks SET status=?, updated_at=?, revision=revision+1 WHERE task_id=?",
                    (task_status, now_text, claimed_task_id),
                )
                return None
            self._insert_event(
                conn,
                task_id=claimed_task_id,
                stage_id=claimed_stage_id,
                side=role_text,
                level="info",
                event_type="claimed",
                payload={
                    "message": "已认领共享桥接阶段",
                    "node_id": str(node_id or "").strip(),
                    "claim_token": claim_token,
                    "lease_expires_at": lease_expires_at,
                },
            )
            payload = self._task_payload_and_sync_from_conn(conn, claimed_task_id, side_hint=role_text)
        if payload:
            self._sync_task_mailbox(payload)
        if payload:
            for item in payload.get("stages", []):
                if str(item.get("stage_id", "")).strip() == claimed_stage_id:
                    item["claim_token"] = claim_token
                    break
        return payload

    def heartbeat_claim(
        self,
        *,
        task_id: str,
        stage_id: str,
        claim_token: str,
        lease_sec: int = 30,
        sync_mailbox: bool = False,
    ) -> None:
        task_text = str(task_id or "").strip()
        stage_text = str(stage_id or "").strip()
        token_text = str(claim_token or "").strip()
        if not task_text or not stage_text or not token_text:
            return
        now_dt = datetime.now()
        mailbox_payload: Dict[str, Any] | None = None
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE bridge_stages
                SET lease_expires_at=?, revision=revision+1
                WHERE task_id=? AND stage_id=? AND claim_token=? AND status='running'
                """,
                (
                    (now_dt + timedelta(seconds=max(5, int(lease_sec or 30)))).strftime("%Y-%m-%d %H:%M:%S"),
                    task_text,
                    stage_text,
                    token_text,
                ),
            )
            if sync_mailbox:
                mailbox_payload = self._sync_task_mailbox_from_conn(conn, task_text)
        if mailbox_payload:
            self._sync_task_mailbox(mailbox_payload)

    def sweep_expired_running_tasks(self, *, stale_task_timeout_sec: int) -> int:
        now_dt = datetime.now()
        now_text = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        stale_before_text = (now_dt - timedelta(seconds=max(60, int(stale_task_timeout_sec or 1800)))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        expired_rows: List[sqlite3.Row] = []
        mailbox_payloads: List[Dict[str, Any]] = []
        with self.connect() as conn:
            expired_rows = conn.execute(
                """
                SELECT t.task_id, s.stage_id, t.feature, s.lease_expires_at
                FROM bridge_tasks t
                JOIN bridge_stages s ON s.task_id = t.task_id
                WHERE s.status='running'
                  AND s.lease_expires_at != ''
                  AND s.lease_expires_at < ?
                  AND COALESCE(NULLIF(s.started_at, ''), NULLIF(t.updated_at, ''), t.created_at) < ?
                ORDER BY t.updated_at ASC, t.task_id ASC, s.stage_id ASC
                """,
                (now_text, stale_before_text),
            ).fetchall()
            for row in expired_rows:
                task_id = str(row["task_id"] or "").strip()
                stage_id = str(row["stage_id"] or "").strip()
                if not task_id or not stage_id:
                    continue
                conn.execute(
                    """
                    UPDATE bridge_stages
                    SET status='failed',
                        result_json=?,
                        error='lease_expired',
                        claimed_by_node_id='',
                        claim_token='',
                        lease_expires_at='',
                        finished_at=?,
                        revision=revision+1
                    WHERE task_id=? AND stage_id=? AND status='running'
                    """,
                    (
                        json.dumps({"status": "failed", "error": "lease_expired"}, ensure_ascii=False),
                        now_text,
                        task_id,
                        stage_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE bridge_tasks
                    SET status='stale',
                        error='lease_expired',
                        updated_at=?,
                        revision=revision+1
                    WHERE task_id=?
                    """,
                    (now_text, task_id),
                )
                self._insert_event(
                    conn,
                    task_id=task_id,
                    stage_id=stage_id,
                    side="system",
                    level="error",
                    event_type="lease_expired",
                    payload={
                        "message": "共享桥接阶段租约已过期，任务已收口为 stale",
                        "feature": str(row["feature"] or "").strip(),
                        "lease_expires_at": str(row["lease_expires_at"] or "").strip(),
                    },
                )
                mailbox_payload = self._sync_task_mailbox_from_conn(conn, task_id, side_hint="system")
                if mailbox_payload:
                    mailbox_payloads.append(mailbox_payload)
        for mailbox_payload in mailbox_payloads:
            self._sync_task_mailbox(mailbox_payload)
        return len(expired_rows)

    def upsert_artifact(
        self,
        *,
        task_id: str,
        stage_id: str,
        artifact_kind: str,
        building: str,
        relative_path: str,
        status: str,
        size_bytes: int = 0,
        metadata: Dict[str, Any] | None = None,
        sync_mailbox: bool = True,
    ) -> None:
        task_text = str(task_id or "").strip()
        stage_text = str(stage_id or "").strip()
        kind_text = str(artifact_kind or "").strip()
        building_text = str(building or "").strip()
        rel_text = str(relative_path or "").replace("\\", "/").strip()
        if not task_text or not kind_text or not rel_text:
            return
        artifact_id = "|".join([task_text, stage_text or "-", kind_text, building_text or "-", rel_text])
        now_text = _now_text()
        mailbox_payload: Dict[str, Any] | None = None
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bridge_artifacts(
                    artifact_id, task_id, stage_id, artifact_kind, building, relative_path, status,
                    size_bytes, metadata_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(artifact_id) DO UPDATE SET
                    status=excluded.status,
                    size_bytes=excluded.size_bytes,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    artifact_id,
                    task_text,
                    stage_text,
                    kind_text,
                    building_text,
                    rel_text,
                    str(status or "").strip(),
                    int(size_bytes or 0),
                    json.dumps(metadata or {}, ensure_ascii=False),
                    now_text,
                    now_text,
                ),
            )
            if sync_mailbox:
                mailbox_payload = self._sync_task_mailbox_from_conn(conn, task_text)
        if mailbox_payload:
            self._sync_task_mailbox(mailbox_payload)

    def get_artifacts(self, task_id: str, *, artifact_kind: str = "", status: str = "") -> List[Dict[str, Any]]:
        task_text = str(task_id or "").strip()
        if not task_text:
            return []
        clauses = ["task_id=?"]
        params: List[Any] = [task_text]
        kind_text = str(artifact_kind or "").strip()
        status_text = str(status or "").strip()
        if kind_text:
            clauses.append("artifact_kind=?")
            params.append(kind_text)
        if status_text:
            clauses.append("status=?")
            params.append(status_text)
        where_sql = " AND ".join(clauses)
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                f"""
                SELECT artifact_id, task_id, stage_id, artifact_kind, building, relative_path, status,
                       size_bytes, metadata_json, created_at, updated_at
                FROM bridge_artifacts
                WHERE {where_sql}
                ORDER BY created_at ASC, artifact_id ASC
                """,
                params,
            ).fetchall()
        return [self._row_to_artifact_dict(row) for row in rows]

    def list_artifacts(
        self,
        *,
        artifact_kind: str = "",
        status: str = "",
        updated_after: str = "",
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        kind_text = str(artifact_kind or "").strip()
        status_text = str(status or "").strip()
        updated_after_text = str(updated_after or "").strip()
        if kind_text:
            clauses.append("artifact_kind=?")
            params.append(kind_text)
        if status_text:
            clauses.append("status=?")
            params.append(status_text)
        if updated_after_text:
            clauses.append("(updated_at>=? OR created_at>=?)")
            params.extend([updated_after_text, updated_after_text])
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                f"""
                SELECT artifact_id, task_id, stage_id, artifact_kind, building, relative_path, status,
                       size_bytes, metadata_json, created_at, updated_at
                FROM bridge_artifacts
                {where_sql}
                ORDER BY updated_at DESC, created_at DESC, artifact_id DESC
                LIMIT ?
                """,
                [*params, max(1, int(limit or 500))],
            ).fetchall()
        return [self._row_to_artifact_dict(row) for row in rows]

    def update_artifact_status(
        self,
        artifact_id: str,
        *,
        status: str,
        metadata_update: Dict[str, Any] | None = None,
    ) -> Dict[str, Any] | None:
        artifact_text = str(artifact_id or "").strip()
        status_text = str(status or "").strip()
        if not artifact_text or not status_text:
            return None
        mailbox_payload: Dict[str, Any] | None = None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT artifact_id, task_id, stage_id, artifact_kind, building, relative_path, status,
                       size_bytes, metadata_json, created_at, updated_at
                FROM bridge_artifacts
                WHERE artifact_id=?
                """,
                (artifact_text,),
            ).fetchone()
            if not row:
                return None
            metadata = self._loads(row["metadata_json"])
            if not isinstance(metadata, dict):
                metadata = {}
            if isinstance(metadata_update, dict):
                metadata.update(metadata_update)
            updated_at = _now_text()
            conn.execute(
                """
                UPDATE bridge_artifacts
                SET status=?,
                    metadata_json=?,
                    updated_at=?
                WHERE artifact_id=?
                """,
                (
                    status_text,
                    json.dumps(metadata, ensure_ascii=False),
                    updated_at,
                    artifact_text,
                ),
            )
            updated_row = conn.execute(
                """
                SELECT artifact_id, task_id, stage_id, artifact_kind, building, relative_path, status,
                       size_bytes, metadata_json, created_at, updated_at
                FROM bridge_artifacts
                WHERE artifact_id=?
                """,
                (artifact_text,),
            ).fetchone()
            task_id_text = str(row["task_id"] or "").strip()
            if task_id_text:
                mailbox_payload = self._sync_task_mailbox_from_conn(conn, task_id_text)
        if mailbox_payload:
            self._sync_task_mailbox(mailbox_payload)
        return self._row_to_artifact_dict(updated_row) if updated_row else None

    def delete_artifact(self, artifact_id: str) -> bool:
        artifact_text = str(artifact_id or "").strip()
        if not artifact_text:
            return False
        deleted_task_id = ""
        mailbox_payload: Dict[str, Any] | None = None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT task_id
                FROM bridge_artifacts
                WHERE artifact_id=?
                """,
                (artifact_text,),
            ).fetchone()
            if row:
                deleted_task_id = str(row["task_id"] or "").strip()
            deleted = conn.execute(
                "DELETE FROM bridge_artifacts WHERE artifact_id=?",
                (artifact_text,),
            )
            if deleted_task_id and int(deleted.rowcount or 0) > 0:
                mailbox_payload = self._sync_task_mailbox_from_conn(conn, deleted_task_id)
        if mailbox_payload:
            self._sync_task_mailbox(mailbox_payload)
        return bool(int(deleted.rowcount or 0) > 0)

    def upsert_source_cache_entry(
        self,
        *,
        source_family: str,
        building: str,
        bucket_kind: str,
        bucket_key: str,
        duty_date: str = "",
        duty_shift: str = "",
        downloaded_at: str = "",
        relative_path: str,
        status: str,
        file_hash: str = "",
        size_bytes: int = 0,
        metadata: Dict[str, Any] | None = None,
    ) -> None:
        family_text = str(source_family or "").strip().lower()
        building_text = str(building or "").strip()
        bucket_kind_text = str(bucket_kind or "").strip().lower()
        bucket_key_text = str(bucket_key or "").strip()
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        relative_path_text = str(relative_path or "").replace("\\", "/").strip()
        if not family_text or not building_text or not bucket_kind_text or not bucket_key_text or not relative_path_text:
            return
        entry_id = "|".join(
            [
                family_text,
                bucket_kind_text,
                bucket_key_text,
                duty_date_text or "-",
                duty_shift_text or "-",
                building_text,
            ]
        )
        now_text = _now_text()
        downloaded_at_text = str(downloaded_at or "").strip() or now_text
        created_at_text = now_text
        with self.connect() as conn:
            existing_row = conn.execute(
                """
                SELECT created_at
                FROM source_cache_entries
                WHERE entry_id=?
                """,
                (entry_id,),
            ).fetchone()
            if existing_row and str(existing_row["created_at"] or "").strip():
                created_at_text = str(existing_row["created_at"] or "").strip()
            conn.execute(
                """
                INSERT INTO source_cache_entries(
                    entry_id, source_family, building, bucket_kind, bucket_key, duty_date, duty_shift,
                    downloaded_at, relative_path, status, file_hash, size_bytes, metadata_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(entry_id) DO UPDATE SET
                    downloaded_at=excluded.downloaded_at,
                    relative_path=excluded.relative_path,
                    status=excluded.status,
                    file_hash=excluded.file_hash,
                    size_bytes=excluded.size_bytes,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    entry_id,
                    family_text,
                    building_text,
                    bucket_kind_text,
                    bucket_key_text,
                    duty_date_text,
                    duty_shift_text,
                    downloaded_at_text,
                    relative_path_text,
                    str(status or "").strip(),
                    str(file_hash or "").strip(),
                    int(size_bytes or 0),
                    json.dumps(metadata or {}, ensure_ascii=False),
                    created_at_text,
                    now_text,
                ),
            )
        self._source_cache_index_store.upsert_entry(
            {
                "entry_id": entry_id,
                "source_family": family_text,
                "building": building_text,
                "bucket_kind": bucket_kind_text,
                "bucket_key": bucket_key_text,
                "duty_date": duty_date_text,
                "duty_shift": duty_shift_text,
                "downloaded_at": downloaded_at_text,
                "relative_path": relative_path_text,
                "status": str(status or "").strip(),
                "file_hash": str(file_hash or "").strip(),
                "size_bytes": int(size_bytes or 0),
                "metadata": metadata or {},
                "created_at": created_at_text,
                "updated_at": now_text,
            }
        )

    def list_source_cache_entries(
        self,
        *,
        source_family: str = "",
        building: str = "",
        bucket_kind: str = "",
        bucket_key: str = "",
        duty_date: str = "",
        duty_shift: str = "",
        status: str = "",
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, int(limit or 200))
        index_rows = self._source_cache_index_store.list_entries(
            source_family=source_family,
            building=building,
            bucket_kind=bucket_kind,
            bucket_key=bucket_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
            status=status,
            limit=safe_limit,
        )
        clauses: List[str] = []
        params: List[Any] = []
        if str(source_family or "").strip():
            clauses.append("source_family=?")
            params.append(str(source_family or "").strip().lower())
        if str(building or "").strip():
            clauses.append("building=?")
            params.append(str(building or "").strip())
        if str(bucket_kind or "").strip():
            clauses.append("bucket_kind=?")
            params.append(str(bucket_kind or "").strip().lower())
        if str(bucket_key or "").strip():
            clauses.append("bucket_key=?")
            params.append(str(bucket_key or "").strip())
        if str(duty_date or "").strip():
            clauses.append("duty_date=?")
            params.append(str(duty_date or "").strip())
        if str(duty_shift or "").strip():
            clauses.append("duty_shift=?")
            params.append(str(duty_shift or "").strip().lower())
        if str(status or "").strip():
            clauses.append("status=?")
            params.append(str(status or "").strip())
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sqlite_rows: List[Dict[str, Any]] = []
        try:
            with self.connect(read_only=True) as conn:
                rows = conn.execute(
                    f"""
                    SELECT entry_id, source_family, building, bucket_kind, bucket_key, duty_date, duty_shift,
                           downloaded_at, relative_path, status, file_hash, size_bytes, metadata_json, created_at, updated_at
                    FROM source_cache_entries
                    {where_sql}
                    ORDER BY downloaded_at DESC, updated_at DESC, entry_id DESC
                    LIMIT ?
                    """,
                    [*params, safe_limit],
                ).fetchall()
            sqlite_rows = [self._row_to_source_cache_entry_dict(row) for row in rows]
        except Exception:
            if index_rows:
                return index_rows[:safe_limit]
            raise
        if not index_rows:
            return sqlite_rows

        merged: Dict[str, Dict[str, Any]] = {}
        for row in sqlite_rows:
            entry_id = str(row.get("entry_id", "") or "").strip()
            if entry_id:
                merged[entry_id] = row
        for row in index_rows:
            entry_id = str(row.get("entry_id", "") or "").strip()
            if not entry_id:
                continue
            existing = merged.get(entry_id)
            if existing is None:
                merged[entry_id] = row
                continue
            existing_updated = str(existing.get("updated_at", "") or "").strip()
            row_updated = str(row.get("updated_at", "") or "").strip()
            if row_updated > existing_updated:
                merged[entry_id] = row
        output = list(merged.values())
        output.sort(
            key=lambda row: (
                str(row.get("downloaded_at", "") or "").strip(),
                str(row.get("updated_at", "") or "").strip(),
                str(row.get("entry_id", "") or "").strip(),
            ),
            reverse=True,
        )
        return output[:safe_limit]

    def list_recent_source_cache_entries(
        self,
        *,
        status: str = "",
        since_text: str = "",
        since_bucket_key: str = "",
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        since_text = str(since_text or "").strip()
        since_bucket_key = str(since_bucket_key or "").strip()
        clauses: List[str] = []
        params: List[Any] = []
        if str(status or "").strip():
            clauses.append("status=?")
            params.append(str(status or "").strip())
        recent_clauses: List[str] = []
        if since_text:
            recent_clauses.extend(["downloaded_at>=?", "updated_at>=?", "created_at>=?"])
            params.extend([since_text, since_text, since_text])
        if since_bucket_key:
            recent_clauses.append("bucket_key>=?")
            params.append(since_bucket_key)
        if recent_clauses:
            clauses.append(f"({' OR '.join(recent_clauses)})")
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                f"""
                SELECT entry_id, source_family, building, bucket_kind, bucket_key, duty_date, duty_shift,
                       downloaded_at, relative_path, status, file_hash, size_bytes, metadata_json, created_at, updated_at
                FROM source_cache_entries
                {where_sql}
                ORDER BY downloaded_at DESC, updated_at DESC, entry_id DESC
                LIMIT ?
                """,
                [*params, max(1, int(limit or 5000))],
            ).fetchall()
        return [self._row_to_source_cache_entry_dict(row) for row in rows]

    def update_source_cache_entry_status(
        self,
        entry_id: str,
        *,
        status: str,
        metadata_update: Dict[str, Any] | None = None,
    ) -> Dict[str, Any] | None:
        entry_text = str(entry_id or "").strip()
        status_text = str(status or "").strip()
        if not entry_text or not status_text:
            return None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT entry_id, source_family, building, bucket_kind, bucket_key, duty_date, duty_shift,
                       downloaded_at, relative_path, status, file_hash, size_bytes, metadata_json, created_at, updated_at
                FROM source_cache_entries
                WHERE entry_id=?
                """,
                (entry_text,),
            ).fetchone()
            if not row:
                return None
            metadata = self._loads(row["metadata_json"])
            if not isinstance(metadata, dict):
                metadata = {}
            if isinstance(metadata_update, dict):
                metadata.update(metadata_update)
            updated_at = _now_text()
            conn.execute(
                """
                UPDATE source_cache_entries
                SET status=?,
                    metadata_json=?,
                    updated_at=?
                WHERE entry_id=?
                """,
                (
                    status_text,
                    json.dumps(metadata, ensure_ascii=False),
                    updated_at,
                    entry_text,
                ),
            )
            updated_row = conn.execute(
                """
                SELECT entry_id, source_family, building, bucket_kind, bucket_key, duty_date, duty_shift,
                       downloaded_at, relative_path, status, file_hash, size_bytes, metadata_json, created_at, updated_at
                FROM source_cache_entries
                WHERE entry_id=?
                """,
                (entry_text,),
            ).fetchone()
        payload = self._row_to_source_cache_entry_dict(updated_row) if updated_row else None
        if isinstance(payload, dict):
            self._source_cache_index_store.upsert_entry(payload)
        return payload

    def delete_source_cache_entry(self, entry_id: str) -> bool:
        entry_text = str(entry_id or "").strip()
        if not entry_text:
            return False
        deleted_entry: Dict[str, Any] | None = None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT entry_id, source_family, building, bucket_kind, bucket_key, duty_date, duty_shift,
                       downloaded_at, relative_path, status, file_hash, size_bytes, metadata_json, created_at, updated_at
                FROM source_cache_entries
                WHERE entry_id=?
                """,
                (entry_text,),
            ).fetchone()
            if row:
                deleted_entry = self._row_to_source_cache_entry_dict(row)
            deleted = conn.execute(
                "DELETE FROM source_cache_entries WHERE entry_id=?",
                (entry_text,),
            )
        success = bool(int(deleted.rowcount or 0) > 0)
        if success and isinstance(deleted_entry, dict):
            self._source_cache_index_store.delete_entry(deleted_entry)
        return success

    def list_cleanup_candidate_source_cache_entries(self, *, limit: int = 20000) -> List[Dict[str, Any]]:
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT entry_id, source_family, building, bucket_kind, bucket_key, duty_date, duty_shift,
                       downloaded_at, relative_path, status, file_hash, size_bytes, metadata_json, created_at, updated_at
                FROM source_cache_entries
                ORDER BY updated_at ASC, downloaded_at ASC, entry_id ASC
                LIMIT ?
                """,
                (max(1, int(limit or 1)),),
            ).fetchall()
        return [self._row_to_source_cache_entry_dict(row) for row in rows]

    def cleanup_terminal_history(self, *, retention_days: int = 14) -> Dict[str, Any]:
        cutoff_text = (datetime.now() - timedelta(days=max(1, int(retention_days or 14)))).strftime("%Y-%m-%d %H:%M:%S")
        deleted_task_ids: List[str] = []
        deleted_artifact_paths: List[str] = []
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT task_id
                FROM bridge_tasks
                WHERE status IN ({", ".join("?" for _ in _TERMINAL_TASK_STATUSES)})
                  AND updated_at < ?
                ORDER BY updated_at ASC, task_id ASC
                """,
                (*sorted(_TERMINAL_TASK_STATUSES), cutoff_text),
            ).fetchall()
            deleted_task_ids = [str(row["task_id"] or "").strip() for row in rows if str(row["task_id"] or "").strip()]
            if not deleted_task_ids:
                return {"deleted_tasks": 0, "artifact_relative_paths": []}
            placeholders = ", ".join("?" for _ in deleted_task_ids)
            artifact_rows = conn.execute(
                f"SELECT relative_path FROM bridge_artifacts WHERE task_id IN ({placeholders})",
                deleted_task_ids,
            ).fetchall()
            deleted_artifact_paths = [
                str(row["relative_path"] or "").strip().replace("\\", "/")
                for row in artifact_rows
                if str(row["relative_path"] or "").strip()
            ]
            conn.execute(f"DELETE FROM bridge_artifacts WHERE task_id IN ({placeholders})", deleted_task_ids)
            conn.execute(f"DELETE FROM bridge_events WHERE task_id IN ({placeholders})", deleted_task_ids)
            conn.execute(f"DELETE FROM bridge_stages WHERE task_id IN ({placeholders})", deleted_task_ids)
            conn.execute(f"DELETE FROM bridge_tasks WHERE task_id IN ({placeholders})", deleted_task_ids)
        for task_id in deleted_task_ids:
            try:
                shutil.rmtree(self._mailbox_store.task_dir(task_id), ignore_errors=True)
            except Exception:
                pass
        return {
            "deleted_tasks": len(deleted_task_ids),
            "artifact_relative_paths": deleted_artifact_paths,
        }

    def cleanup_stale_nodes(self, *, retention_days: int = 2) -> int:
        cutoff_text = (datetime.now() - timedelta(days=max(1, int(retention_days or 2)))).strftime("%Y-%m-%d %H:%M:%S")
        with self.connect() as conn:
            deleted = conn.execute(
                "DELETE FROM bridge_nodes WHERE last_seen_at != '' AND last_seen_at < ?",
                (cutoff_text,),
            )
        return int(deleted.rowcount or 0)

    def complete_stage(
        self,
        *,
        task_id: str,
        stage_id: str,
        claim_token: str,
        side: str,
        stage_result: Dict[str, Any] | None,
        stage_error: str = "",
        next_task_status: str,
        task_error: str = "",
        stage_status: str = "success",
        task_result: Dict[str, Any] | None = None,
        record_event: bool = True,
        sync_mailbox: bool = True,
    ) -> bool:
        task_text = str(task_id or "").strip()
        stage_text = str(stage_id or "").strip()
        token_text = str(claim_token or "").strip()
        if not task_text or not stage_text or not token_text:
            return False
        now_text = _now_text()
        mailbox_payload: Dict[str, Any] | None = None
        with self.connect() as conn:
            updated = conn.execute(
                """
                UPDATE bridge_stages
                SET status=?,
                    result_json=?,
                    error=?,
                    finished_at=?,
                    lease_expires_at='',
                    claim_token='',
                    revision=revision+1
                WHERE task_id=? AND stage_id=? AND claim_token=? AND status='running'
                """,
                (
                    str(stage_status or "success").strip(),
                    json.dumps(stage_result or {}, ensure_ascii=False),
                    str(stage_error or "").strip(),
                    now_text,
                    task_text,
                    stage_text,
                    token_text,
                ),
            )
            if int(updated.rowcount or 0) <= 0:
                return False
            conn.execute(
                """
                UPDATE bridge_tasks
                SET status=?, result_json=?, error=?, updated_at=?, revision=revision+1
                WHERE task_id=?
                """,
                (
                    str(next_task_status or "").strip(),
                    json.dumps(task_result or stage_result or {}, ensure_ascii=False),
                    str(task_error or "").strip(),
                    now_text,
                    task_text,
                ),
            )
            if record_event:
                self._insert_event(
                    conn,
                    task_id=task_text,
                    stage_id=stage_text,
                    side=str(side or "").strip(),
                    level="error" if str(stage_status or "").strip().lower() == "failed" else "info",
                    event_type="completed",
                    payload={
                        "message": "共享桥接阶段已完成",
                        "stage_status": str(stage_status or "").strip(),
                        "task_status": str(next_task_status or "").strip(),
                        "error": str(stage_error or "").strip(),
                    },
                )
            if sync_mailbox:
                mailbox_payload = self._sync_task_mailbox_from_conn(conn, task_text, side_hint=str(side or "").strip())
        if mailbox_payload:
            self._sync_task_mailbox(mailbox_payload)
        return True

    def append_event(
        self,
        *,
        task_id: str,
        stage_id: str,
        side: str,
        level: str,
        event_type: str,
        payload: Dict[str, Any] | None = None,
        sync_mailbox: bool = True,
    ) -> None:
        task_text = str(task_id or "").strip()
        if not task_text:
            return
        mailbox_payload: Dict[str, Any] | None = None
        with self.connect() as conn:
            self._insert_event(
                conn,
                task_id=task_text,
                stage_id=str(stage_id or "").strip(),
                side=str(side or "").strip(),
                level=str(level or "").strip() or "info",
                event_type=str(event_type or "").strip() or "log",
                payload=payload or {},
            )
            if sync_mailbox:
                mailbox_payload = self._sync_task_mailbox_from_conn(conn, task_text, side_hint=str(side or "").strip())
        if mailbox_payload:
            self._sync_task_mailbox(mailbox_payload)

    def _insert_event(
        self,
        conn: sqlite3.Connection,
        *,
        task_id: str,
        stage_id: str,
        side: str,
        level: str,
        event_type: str,
        payload: Dict[str, Any],
    ) -> None:
        conn.execute(
            """
            INSERT INTO bridge_events(task_id, stage_id, side, level, event_type, payload_json, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                stage_id,
                side,
                level,
                event_type,
                json.dumps(payload or {}, ensure_ascii=False),
                _now_text(),
            ),
        )

    @staticmethod
    def _mailbox_side_for_task(task: Dict[str, Any] | None, *, side_hint: str = "") -> str:
        hint = str(side_hint or "").strip().lower()
        if hint in {"internal", "external"}:
            return hint
        status = str((task or {}).get("status", "") or "").strip().lower()
        if status.startswith("queued_for_internal") or status.startswith("internal_"):
            return "internal"
        if status.startswith("ready_for_external") or status.startswith("external_"):
            return "external"
        if status in _TERMINAL_TASK_STATUSES:
            result = (task or {}).get("result", {}) if isinstance((task or {}).get("result", {}), dict) else {}
            if isinstance(result.get("external", {}), dict) and result.get("external"):
                return "external"
            if isinstance(result.get("internal", {}), dict) and result.get("internal"):
                return "internal"
        return ""

    def _task_payload_from_conn(self, conn: sqlite3.Connection, task_id: str) -> Dict[str, Any] | None:
        task_text = str(task_id or "").strip()
        if not task_text:
            return None
        task_row = conn.execute(
            """
            SELECT task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                   status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
            FROM bridge_tasks
            WHERE task_id=?
            """,
            (task_text,),
        ).fetchone()
        if not task_row:
            return None
        stage_rows = conn.execute(
            """
            SELECT task_id, stage_id, role_target, handler, status, input_json, result_json,
                   claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
            FROM bridge_stages
            WHERE task_id=?
            ORDER BY stage_id
            """,
            (task_text,),
        ).fetchall()
        artifact_rows = conn.execute(
            """
            SELECT artifact_id, task_id, stage_id, artifact_kind, building, relative_path, status,
                   size_bytes, metadata_json, created_at, updated_at
            FROM bridge_artifacts
            WHERE task_id=?
            ORDER BY created_at
            """,
            (task_text,),
        ).fetchall()
        event_rows = conn.execute(
            """
            SELECT event_id, task_id, stage_id, side, level, event_type, payload_json, created_at
            FROM bridge_events
            WHERE task_id=?
            ORDER BY event_id DESC
            LIMIT 100
            """,
            (task_text,),
        ).fetchall()
        payload = self._row_to_task_dict(task_row)
        payload["stages"] = [self._row_to_stage_dict(row) for row in stage_rows]
        payload["artifacts"] = [self._row_to_artifact_dict(row) for row in artifact_rows]
        payload["events"] = [self._row_to_event_dict(row) for row in event_rows]
        return payload

    def _sync_task_mailbox_from_conn(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        *,
        side_hint: str = "",
    ) -> Dict[str, Any] | None:
        payload = self._task_payload_from_conn(conn, task_id)
        if not isinstance(payload, dict):
            return None
        payload["_mailbox_side_hint"] = self._mailbox_side_for_task(payload, side_hint=side_hint)
        return payload

    def _sync_task_mailbox(self, task: Dict[str, Any] | None, *, side_hint: str = "") -> None:
        if not isinstance(task, dict):
            return
        side_override = str(task.pop("_mailbox_side_hint", "") or "").strip().lower()
        payload = dict(task)
        self._mailbox_store.write_request(payload)
        side = side_override or self._mailbox_side_for_task(payload, side_hint=side_hint)
        if side:
            self._mailbox_store.write_side_snapshot(task=payload, side=side)

    def _task_payload_and_sync_from_conn(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        *,
        side_hint: str = "",
    ) -> Dict[str, Any] | None:
        return self._sync_task_mailbox_from_conn(conn, task_id, side_hint=side_hint)

    @staticmethod
    def _loads(raw: Any) -> Any:
        try:
            return json.loads(str(raw or ""))
        except Exception:
            return {}

    def _row_to_task_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "task_id": str(row["task_id"] or ""),
            "feature": str(row["feature"] or ""),
            "mode": str(row["mode"] or ""),
            "created_by_role": str(row["created_by_role"] or ""),
            "created_by_node_id": str(row["created_by_node_id"] or ""),
            "requested_by": str(row["requested_by"] or ""),
            "status": str(row["status"] or ""),
            "dedupe_key": str(row["dedupe_key"] or ""),
            "request": self._loads(row["request_json"]),
            "result": self._loads(row["result_json"]),
            "error": str(row["error"] or ""),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
            "revision": int(row["revision"] or 0),
        }

    def _row_to_stage_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "task_id": str(row["task_id"] or ""),
            "stage_id": str(row["stage_id"] or ""),
            "role_target": str(row["role_target"] or ""),
            "handler": str(row["handler"] or ""),
            "status": str(row["status"] or ""),
            "input": self._loads(row["input_json"]),
            "result": self._loads(row["result_json"]),
            "claimed_by_node_id": str(row["claimed_by_node_id"] or ""),
            "claim_token": str(row["claim_token"] or ""),
            "lease_expires_at": str(row["lease_expires_at"] or ""),
            "started_at": str(row["started_at"] or ""),
            "finished_at": str(row["finished_at"] or ""),
            "error": str(row["error"] or ""),
            "revision": int(row["revision"] or 0),
        }

    def _row_to_artifact_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "artifact_id": str(row["artifact_id"] or ""),
            "task_id": str(row["task_id"] or ""),
            "stage_id": str(row["stage_id"] or ""),
            "artifact_kind": str(row["artifact_kind"] or ""),
            "building": str(row["building"] or ""),
            "relative_path": str(row["relative_path"] or ""),
            "status": str(row["status"] or ""),
            "size_bytes": int(row["size_bytes"] or 0),
            "metadata": self._loads(row["metadata_json"]),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _row_to_event_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "event_id": int(row["event_id"] or 0),
            "task_id": str(row["task_id"] or ""),
            "stage_id": str(row["stage_id"] or ""),
            "side": str(row["side"] or ""),
            "level": str(row["level"] or ""),
            "event_type": str(row["event_type"] or ""),
            "payload": self._loads(row["payload_json"]),
            "created_at": str(row["created_at"] or ""),
        }

    def _row_to_internal_issue_alert_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "alert_key": str(row["alert_key"] or ""),
            "building": str(row["building"] or ""),
            "failure_kind": str(row["failure_kind"] or ""),
            "summary": str(row["summary"] or ""),
            "latest_detail": str(row["latest_detail"] or ""),
            "first_seen_at": str(row["first_seen_at"] or ""),
            "last_seen_at": str(row["last_seen_at"] or ""),
            "last_pushed_at": str(row["last_pushed_at"] or ""),
            "occurrence_count": int(row["occurrence_count"] or 0),
            "active": bool(int(row["active"] or 0)),
            "last_task_id": str(row["last_task_id"] or ""),
            "status_key": str(row["status_key"] or ""),
            "resolved_at": str(row["resolved_at"] or ""),
            "last_recovery_task_id": str(row["last_recovery_task_id"] or ""),
            "last_recovery_pushed_at": str(row["last_recovery_pushed_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _row_to_external_alert_projection_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "projection_key": str(row["projection_key"] or ""),
            "building": str(row["building"] or ""),
            "failure_kind": str(row["failure_kind"] or ""),
            "alert_state": str(row["alert_state"] or ""),
            "status_key": str(row["status_key"] or ""),
            "summary": str(row["summary"] or ""),
            "latest_detail": str(row["latest_detail"] or ""),
            "first_seen_at": str(row["first_seen_at"] or ""),
            "last_seen_at": str(row["last_seen_at"] or ""),
            "resolved_at": str(row["resolved_at"] or ""),
            "occurrence_count": int(row["occurrence_count"] or 0),
            "still_unresolved": bool(int(row["still_unresolved"] or 0)),
            "last_notified_at": str(row["last_notified_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def _row_to_source_cache_entry_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "entry_id": str(row["entry_id"] or ""),
            "source_family": str(row["source_family"] or ""),
            "building": str(row["building"] or ""),
            "bucket_kind": str(row["bucket_kind"] or ""),
            "bucket_key": str(row["bucket_key"] or ""),
            "duty_date": str(row["duty_date"] or ""),
            "duty_shift": str(row["duty_shift"] or ""),
            "downloaded_at": str(row["downloaded_at"] or ""),
            "relative_path": str(row["relative_path"] or ""),
            "status": str(row["status"] or ""),
            "file_hash": str(row["file_hash"] or ""),
            "size_bytes": int(row["size_bytes"] or 0),
            "metadata": self._loads(row["metadata_json"]),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

