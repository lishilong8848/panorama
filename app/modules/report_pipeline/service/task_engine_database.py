from __future__ import annotations

import json
import queue
import sqlite3
import threading
from pathlib import Path
from typing import Any, Callable, Iterable

from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root


def _json_ready(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


class TaskEngineDatabase:
    def __init__(
        self,
        *,
        runtime_config: dict[str, Any] | None = None,
        app_dir: Path | None = None,
    ) -> None:
        self.runtime_root = resolve_runtime_state_root(runtime_config=runtime_config, app_dir=app_dir)
        self.root = self.runtime_root / "task_engine"
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "task_engine.db"
        self._writes: queue.Queue[tuple[Callable[[sqlite3.Connection], Any], threading.Event, dict[str, Any]]] = queue.Queue()
        self._init_db()
        self._writer = threading.Thread(target=self._writer_loop, name="task-engine-sqlite-writer", daemon=True)
        self._writer.start()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=5.0, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    feature TEXT NOT NULL DEFAULT '',
                    submitted_by TEXT NOT NULL DEFAULT 'manual',
                    priority TEXT NOT NULL DEFAULT 'manual',
                    resource_keys_json TEXT NOT NULL DEFAULT '[]',
                    wait_reason TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'queued',
                    created_at TEXT NOT NULL DEFAULT '',
                    started_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    result_json TEXT NOT NULL DEFAULT 'null',
                    config_snapshot_json TEXT NOT NULL DEFAULT 'null',
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS stages (
                    job_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    resource_keys_json TEXT NOT NULL DEFAULT '[]',
                    resume_policy TEXT NOT NULL DEFAULT 'manual_resume',
                    worker_handler TEXT NOT NULL DEFAULT '',
                    worker_pid INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    result_json TEXT NOT NULL DEFAULT 'null',
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (job_id, stage_id),
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS job_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL DEFAULT '',
                    stream TEXT NOT NULL DEFAULT 'job',
                    event_type TEXT NOT NULL DEFAULT 'log',
                    level TEXT NOT NULL DEFAULT 'info',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_job_events_job_event_id ON job_events(job_id, event_id);
                CREATE TABLE IF NOT EXISTS resource_leases (
                    resource_key TEXT PRIMARY KEY,
                    holder_job_id TEXT NOT NULL DEFAULT '',
                    holder_stage_id TEXT NOT NULL DEFAULT '',
                    capacity INTEGER NOT NULL DEFAULT 1,
                    state_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS workers (
                    job_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    pid INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT '',
                    command_json TEXT NOT NULL DEFAULT '[]',
                    started_at TEXT NOT NULL DEFAULT '',
                    last_heartbeat_at TEXT NOT NULL DEFAULT '',
                    exit_code INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (job_id, stage_id),
                    FOREIGN KEY (job_id, stage_id) REFERENCES stages(job_id, stage_id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS network_window (
                    singleton_key INTEGER PRIMARY KEY CHECK (singleton_key = 1),
                    current_side TEXT NOT NULL DEFAULT 'none',
                    switching INTEGER NOT NULL DEFAULT 0,
                    auto_switch_enabled INTEGER NOT NULL DEFAULT 1,
                    current_ssid TEXT NOT NULL DEFAULT '',
                    current_detected_side TEXT NOT NULL DEFAULT '',
                    ssid_side TEXT NOT NULL DEFAULT '',
                    internal_reachable INTEGER NOT NULL DEFAULT 0,
                    external_reachable INTEGER NOT NULL DEFAULT 0,
                    reachable_sides_json TEXT NOT NULL DEFAULT '[]',
                    mode TEXT NOT NULL DEFAULT 'none_reachable',
                    target_internal_ssid TEXT NOT NULL DEFAULT '',
                    target_external_ssid TEXT NOT NULL DEFAULT '',
                    last_checked_at TEXT NOT NULL DEFAULT '',
                    window_started_at TEXT NOT NULL DEFAULT '',
                    window_dispatch_count INTEGER NOT NULL DEFAULT 0,
                    window_draining INTEGER NOT NULL DEFAULT 0,
                    pending_side TEXT NOT NULL DEFAULT '',
                    queued_internal INTEGER NOT NULL DEFAULT 0,
                    queued_external INTEGER NOT NULL DEFAULT 0,
                    queued_pipeline INTEGER NOT NULL DEFAULT 0,
                    running_internal INTEGER NOT NULL DEFAULT 0,
                    running_external INTEGER NOT NULL DEFAULT 0,
                    running_pipeline INTEGER NOT NULL DEFAULT 0,
                    oldest_internal_wait_sec INTEGER NOT NULL DEFAULT 0,
                    oldest_external_wait_sec INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT ''
                );
                INSERT OR IGNORE INTO network_window(singleton_key) VALUES (1);
                """
            )
            columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
            if "resource_keys_json" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN resource_keys_json TEXT NOT NULL DEFAULT '[]'")
            if "wait_reason" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN wait_reason TEXT NOT NULL DEFAULT ''")
            network_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(network_window)").fetchall()}
            for statement, column_name in (
                ("ALTER TABLE network_window ADD COLUMN auto_switch_enabled INTEGER NOT NULL DEFAULT 1", "auto_switch_enabled"),
                ("ALTER TABLE network_window ADD COLUMN current_ssid TEXT NOT NULL DEFAULT ''", "current_ssid"),
                ("ALTER TABLE network_window ADD COLUMN current_detected_side TEXT NOT NULL DEFAULT ''", "current_detected_side"),
                ("ALTER TABLE network_window ADD COLUMN ssid_side TEXT NOT NULL DEFAULT ''", "ssid_side"),
                ("ALTER TABLE network_window ADD COLUMN internal_reachable INTEGER NOT NULL DEFAULT 0", "internal_reachable"),
                ("ALTER TABLE network_window ADD COLUMN external_reachable INTEGER NOT NULL DEFAULT 0", "external_reachable"),
                ("ALTER TABLE network_window ADD COLUMN reachable_sides_json TEXT NOT NULL DEFAULT '[]'", "reachable_sides_json"),
                ("ALTER TABLE network_window ADD COLUMN mode TEXT NOT NULL DEFAULT 'none_reachable'", "mode"),
                ("ALTER TABLE network_window ADD COLUMN target_internal_ssid TEXT NOT NULL DEFAULT ''", "target_internal_ssid"),
                ("ALTER TABLE network_window ADD COLUMN target_external_ssid TEXT NOT NULL DEFAULT ''", "target_external_ssid"),
                ("ALTER TABLE network_window ADD COLUMN last_checked_at TEXT NOT NULL DEFAULT ''", "last_checked_at"),
            ):
                if column_name not in network_columns:
                    conn.execute(statement)
        finally:
            conn.close()

    def _writer_loop(self) -> None:
        conn = self._connect()
        try:
            while True:
                callback, done, holder = self._writes.get()
                try:
                    holder["result"] = callback(conn)
                except Exception as exc:  # noqa: BLE001
                    holder["error"] = exc
                finally:
                    done.set()
        finally:
            conn.close()

    def _write(self, callback: Callable[[sqlite3.Connection], Any]) -> Any:
        done = threading.Event()
        holder: dict[str, Any] = {}
        self._writes.put((callback, done, holder))
        done.wait()
        error = holder.get("error")
        if error is not None:
            raise error
        return holder.get("result")

    @staticmethod
    def _loads(text: str | None, fallback: Any) -> Any:
        raw = str(text or "").strip()
        if not raw:
            return fallback
        try:
            return json.loads(raw)
        except Exception:  # noqa: BLE001
            return fallback

    def upsert_job(self, job_payload: dict[str, Any], *, config_snapshot: dict[str, Any] | None = None) -> None:
        payload = _json_ready(job_payload)
        snapshot = _json_ready(config_snapshot) if isinstance(config_snapshot, dict) else None

        def _op(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, name, feature, submitted_by, priority, resource_keys_json, wait_reason, status,
                    created_at, started_at, finished_at, summary, error,
                    result_json, config_snapshot_json, cancel_requested, revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(job_id) DO UPDATE SET
                    name=excluded.name,
                    feature=excluded.feature,
                    submitted_by=excluded.submitted_by,
                    priority=excluded.priority,
                    resource_keys_json=excluded.resource_keys_json,
                    wait_reason=excluded.wait_reason,
                    status=excluded.status,
                    created_at=excluded.created_at,
                    started_at=excluded.started_at,
                    finished_at=excluded.finished_at,
                    summary=excluded.summary,
                    error=excluded.error,
                    result_json=excluded.result_json,
                    config_snapshot_json=CASE
                        WHEN excluded.config_snapshot_json != 'null' THEN excluded.config_snapshot_json
                        ELSE jobs.config_snapshot_json
                    END,
                    cancel_requested=excluded.cancel_requested,
                    revision=jobs.revision + 1
                """,
                (
                    str(payload.get("job_id", "") or "").strip(),
                    str(payload.get("name", "") or "").strip(),
                    str(payload.get("feature", "") or "").strip(),
                    str(payload.get("submitted_by", "manual") or "manual").strip(),
                    str(payload.get("priority", "manual") or "manual").strip(),
                    json.dumps(payload.get("resource_keys") or [], ensure_ascii=False, default=str),
                    str(payload.get("wait_reason", "") or "").strip(),
                    str(payload.get("status", "queued") or "queued").strip(),
                    str(payload.get("created_at", "") or "").strip(),
                    str(payload.get("started_at", "") or "").strip(),
                    str(payload.get("finished_at", "") or "").strip(),
                    str(payload.get("summary", "") or "").strip(),
                    str(payload.get("error", "") or "").strip(),
                    json.dumps(payload.get("result"), ensure_ascii=False, default=str),
                    json.dumps(snapshot, ensure_ascii=False, default=str) if snapshot is not None else "null",
                    1 if bool(payload.get("cancel_requested", False)) else 0,
                ),
            )

        self._write(_op)

    def upsert_stage(self, job_id: str, stage_payload: dict[str, Any]) -> None:
        payload = _json_ready(stage_payload)
        job_key = str(job_id or "").strip()
        if not job_key:
            return

        def _op(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO stages (
                    job_id, stage_id, name, status, resource_keys_json,
                    resume_policy, worker_handler, worker_pid,
                    started_at, finished_at, summary, error, result_json,
                    cancel_requested, revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(job_id, stage_id) DO UPDATE SET
                    name=excluded.name,
                    status=excluded.status,
                    resource_keys_json=excluded.resource_keys_json,
                    resume_policy=excluded.resume_policy,
                    worker_handler=excluded.worker_handler,
                    worker_pid=excluded.worker_pid,
                    started_at=excluded.started_at,
                    finished_at=excluded.finished_at,
                    summary=excluded.summary,
                    error=excluded.error,
                    result_json=excluded.result_json,
                    cancel_requested=excluded.cancel_requested,
                    revision=stages.revision + 1
                """,
                (
                    job_key,
                    str(payload.get("stage_id", "") or "").strip(),
                    str(payload.get("name", "") or "").strip(),
                    str(payload.get("status", "pending") or "pending").strip(),
                    json.dumps(payload.get("resource_keys") or [], ensure_ascii=False, default=str),
                    str(payload.get("resume_policy", "manual_resume") or "manual_resume").strip(),
                    str(payload.get("worker_handler", "") or "").strip(),
                    int(payload.get("worker_pid") or 0),
                    str(payload.get("started_at", "") or "").strip(),
                    str(payload.get("finished_at", "") or "").strip(),
                    str(payload.get("summary", "") or "").strip(),
                    str(payload.get("error", "") or "").strip(),
                    json.dumps(payload.get("result"), ensure_ascii=False, default=str),
                    1 if bool(payload.get("cancel_requested", False)) else 0,
                ),
            )

        self._write(_op)

    def append_job_event(
        self,
        *,
        job_id: str,
        stage_id: str = "",
        stream: str = "job",
        event_type: str = "log",
        level: str = "info",
        payload: dict[str, Any] | None = None,
        created_at: str = "",
    ) -> int:
        event_payload = _json_ready(payload or {})

        def _op(conn: sqlite3.Connection) -> int:
            cursor = conn.execute(
                """
                INSERT INTO job_events(job_id, stage_id, stream, event_type, level, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(job_id or "").strip(),
                    str(stage_id or "").strip(),
                    str(stream or "job").strip(),
                    str(event_type or "log").strip(),
                    str(level or "info").strip(),
                    json.dumps(event_payload, ensure_ascii=False, default=str),
                    str(created_at or "").strip(),
                ),
            )
            return int(cursor.lastrowid or 0)

        return int(self._write(_op) or 0)

    def upsert_worker(self, *, job_id: str, stage_id: str, snapshot: dict[str, Any]) -> None:
        payload = _json_ready(snapshot)

        def _op(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO workers(job_id, stage_id, pid, status, command_json, started_at, last_heartbeat_at, exit_code, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id, stage_id) DO UPDATE SET
                    pid=excluded.pid,
                    status=excluded.status,
                    command_json=excluded.command_json,
                    started_at=excluded.started_at,
                    last_heartbeat_at=excluded.last_heartbeat_at,
                    exit_code=excluded.exit_code,
                    updated_at=excluded.updated_at
                """,
                (
                    str(job_id or "").strip(),
                    str(stage_id or "").strip(),
                    int(payload.get("pid") or 0),
                    str(payload.get("status", "") or "").strip(),
                    json.dumps(payload.get("command") or [], ensure_ascii=False, default=str),
                    str(payload.get("started_at", "") or "").strip(),
                    str(payload.get("last_heartbeat_at", "") or "").strip(),
                    int(payload.get("exit_code") or 0),
                    str(payload.get("updated_at", "") or "").strip(),
                ),
            )

        self._write(_op)

    def persist_resource_snapshot(self, snapshot: dict[str, Any]) -> None:
        payload = _json_ready(snapshot or {})
        resources = list(payload.get("resources") or [])
        network = dict(payload.get("network") or {})

        def _op(conn: sqlite3.Connection) -> None:
            conn.execute("DELETE FROM resource_leases")
            for item in resources:
                resource_key = str(item.get("resource_key", "") or "").strip()
                if not resource_key:
                    continue
                holder_ids = list(item.get("holder_job_ids") or [])
                conn.execute(
                    """
                    INSERT INTO resource_leases(resource_key, holder_job_id, holder_stage_id, capacity, state_json, updated_at)
                    VALUES (?, ?, '', ?, ?, ?)
                    """,
                    (
                        resource_key,
                        str(holder_ids[0] if holder_ids else ""),
                        int(item.get("capacity") or 1),
                        json.dumps(item, ensure_ascii=False, default=str),
                        str(payload.get("updated_at", "") or "").strip(),
                    ),
                )
            conn.execute(
                """
                UPDATE network_window SET
                    current_side=?, switching=?, auto_switch_enabled=?, current_ssid=?, current_detected_side=?, ssid_side=?,
                    internal_reachable=?, external_reachable=?, reachable_sides_json=?, mode=?, target_internal_ssid=?, target_external_ssid=?, last_checked_at=?,
                    window_started_at=?, window_dispatch_count=?, window_draining=?, pending_side=?,
                    queued_internal=?, queued_external=?, queued_pipeline=?, running_internal=?, running_external=?, running_pipeline=?,
                    oldest_internal_wait_sec=?, oldest_external_wait_sec=?, updated_at=?
                WHERE singleton_key=1
                """,
                (
                    str(network.get("current_side", "none") or "none").strip(),
                    1 if bool(network.get("switching", False)) else 0,
                    1 if bool(network.get("auto_switch_enabled", True)) else 0,
                    str(network.get("current_ssid", "") or "").strip(),
                    str(network.get("current_detected_side", "") or "").strip(),
                    str(network.get("ssid_side", "") or "").strip(),
                    1 if bool(network.get("internal_reachable", False)) else 0,
                    1 if bool(network.get("external_reachable", False)) else 0,
                    json.dumps(network.get("reachable_sides") or [], ensure_ascii=False, default=str),
                    str(network.get("mode", "none_reachable") or "none_reachable").strip(),
                    str(network.get("target_internal_ssid", "") or "").strip(),
                    str(network.get("target_external_ssid", "") or "").strip(),
                    str(network.get("last_checked_at", "") or "").strip(),
                    str(network.get("window_started_at", "") or "").strip(),
                    int(network.get("window_dispatch_count") or 0),
                    1 if bool(network.get("window_draining", False)) else 0,
                    str(network.get("pending_side", "") or "").strip(),
                    int(network.get("queued_internal") or 0),
                    int(network.get("queued_external") or 0),
                    int(network.get("queued_pipeline") or 0),
                    int(network.get("running_internal") or 0),
                    int(network.get("running_external") or 0),
                    int(network.get("running_pipeline") or 0),
                    int(network.get("oldest_internal_wait_sec") or 0),
                    int(network.get("oldest_external_wait_sec") or 0),
                    str(payload.get("updated_at", "") or "").strip(),
                ),
            )

        self._write(_op)

    def _row_to_job(self, row: sqlite3.Row, stages: list[dict[str, Any]], last_event_id: int) -> dict[str, Any]:
        return {
            "job_id": str(row["job_id"] or ""),
            "name": str(row["name"] or ""),
            "feature": str(row["feature"] or ""),
            "submitted_by": str(row["submitted_by"] or "manual"),
            "status": str(row["status"] or "queued"),
            "created_at": str(row["created_at"] or ""),
            "started_at": str(row["started_at"] or ""),
            "finished_at": str(row["finished_at"] or ""),
            "summary": str(row["summary"] or ""),
            "error": str(row["error"] or ""),
            "result": self._loads(row["result_json"], None),
            "log_count": 0,
            "priority": str(row["priority"] or "manual"),
            "resource_keys": self._loads(row["resource_keys_json"], []),
            "wait_reason": str(row["wait_reason"] or ""),
            "stages": stages,
            "cancel_requested": bool(int(row["cancel_requested"] or 0)),
            "revision": int(row["revision"] or 0),
            "last_event_id": int(last_event_id or 0),
        }

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (str(job_id or "").strip(),)).fetchone()
            if row is None:
                return None
            stage_rows = conn.execute(
                "SELECT * FROM stages WHERE job_id = ? ORDER BY stage_id ASC",
                (str(job_id or "").strip(),),
            ).fetchall()
            worker_rows = {
                (str(item["job_id"]), str(item["stage_id"])): item
                for item in conn.execute("SELECT * FROM workers WHERE job_id = ?", (str(job_id or "").strip(),)).fetchall()
            }
            last_event_id = int(
                conn.execute("SELECT COALESCE(MAX(event_id), 0) FROM job_events WHERE job_id = ?", (str(job_id or "").strip(),)).fetchone()[0] or 0
            )
            stages: list[dict[str, Any]] = []
            for item in stage_rows:
                worker = worker_rows.get((str(item["job_id"]), str(item["stage_id"])))
                stages.append(
                    {
                        "stage_id": str(item["stage_id"] or ""),
                        "name": str(item["name"] or ""),
                        "status": str(item["status"] or "pending"),
                        "resource_keys": self._loads(item["resource_keys_json"], []),
                        "resume_policy": str(item["resume_policy"] or "manual_resume"),
                        "worker_handler": str(item["worker_handler"] or ""),
                        "worker_pid": int(item["worker_pid"] or 0),
                        "started_at": str(item["started_at"] or ""),
                        "finished_at": str(item["finished_at"] or ""),
                        "summary": str(item["summary"] or ""),
                        "error": str(item["error"] or ""),
                        "result": self._loads(item["result_json"], None),
                        "cancel_requested": bool(int(item["cancel_requested"] or 0)),
                        "revision": int(item["revision"] or 0),
                        "worker_status": str((worker["status"] if worker else "") or ""),
                        "last_heartbeat_at": str((worker["last_heartbeat_at"] if worker else "") or ""),
                    }
                )
            return self._row_to_job(row, stages, last_event_id)
        finally:
            conn.close()

    def list_jobs(self, *, limit: int = 50, statuses: Iterable[str] | None = None) -> list[dict[str, Any]]:
        normalized = [str(item or "").strip().lower() for item in (statuses or []) if str(item or "").strip()]
        conn = self._connect()
        try:
            sql = "SELECT job_id FROM jobs"
            params: list[Any] = []
            if normalized:
                placeholders = ", ".join("?" for _ in normalized)
                sql += f" WHERE lower(status) IN ({placeholders})"
                params.extend(normalized)
            sql += " ORDER BY created_at DESC, rowid DESC LIMIT ?"
            params.append(max(1, int(limit or 1)))
            job_ids = [str(row[0] or "") for row in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()
        items: list[dict[str, Any]] = []
        for job_id in job_ids:
            payload = self.get_job(job_id)
            if payload:
                items.append(payload)
        return items

    def job_counts(self) -> dict[str, int]:
        conn = self._connect()
        try:
            rows = conn.execute("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status").fetchall()
            return {str(row["status"] or ""): int(row["count"] or 0) for row in rows}
        finally:
            conn.close()

    def list_job_events(self, job_id: str, *, after_event_id: int = 0, limit: int = 1000) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT event_id, stage_id, stream, event_type, level, payload_json, created_at
                FROM job_events
                WHERE job_id = ? AND event_id > ?
                ORDER BY event_id ASC
                LIMIT ?
                """,
                (str(job_id or "").strip(), max(0, int(after_event_id or 0)), max(1, int(limit or 1))),
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                result.append(
                    {
                        "event_id": int(row["event_id"] or 0),
                        "stage_id": str(row["stage_id"] or ""),
                        "stream": str(row["stream"] or "job"),
                        "event_type": str(row["event_type"] or "log"),
                        "level": str(row["level"] or "info"),
                        "payload": self._loads(row["payload_json"], {}),
                        "created_at": str(row["created_at"] or ""),
                    }
                )
            return result
        finally:
            conn.close()

    def get_resource_snapshot(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            resource_rows = conn.execute("SELECT * FROM resource_leases ORDER BY resource_key ASC").fetchall()
            resources = [self._loads(row["state_json"], {}) for row in resource_rows]
            network_row = conn.execute("SELECT * FROM network_window WHERE singleton_key = 1").fetchone()
        finally:
            conn.close()
        browser_row = next((item for item in resources if str(item.get("resource_key", "")) == "browser:controlled"), {})
        batch_rows = [item for item in resources if str(item.get("resource_key", "")).startswith("handover_batch:")]
        network = {
            "current_side": str((network_row["current_side"] if network_row else "none") or "none"),
            "switching": bool(int((network_row["switching"] if network_row else 0) or 0)),
            "auto_switch_enabled": bool(int((network_row["auto_switch_enabled"] if network_row else 1) or 0)),
            "current_ssid": str((network_row["current_ssid"] if network_row else "") or ""),
            "current_detected_side": str((network_row["current_detected_side"] if network_row else "") or ""),
            "ssid_side": str((network_row["ssid_side"] if network_row else "") or ""),
            "internal_reachable": bool(int((network_row["internal_reachable"] if network_row else 0) or 0)),
            "external_reachable": bool(int((network_row["external_reachable"] if network_row else 0) or 0)),
            "reachable_sides": self._loads(network_row["reachable_sides_json"] if network_row else "[]", []),
            "mode": str((network_row["mode"] if network_row else "none_reachable") or "none_reachable"),
            "target_internal_ssid": str((network_row["target_internal_ssid"] if network_row else "") or ""),
            "target_external_ssid": str((network_row["target_external_ssid"] if network_row else "") or ""),
            "last_checked_at": str((network_row["last_checked_at"] if network_row else "") or ""),
            "window_started_at": str((network_row["window_started_at"] if network_row else "") or ""),
            "window_dispatch_count": int((network_row["window_dispatch_count"] if network_row else 0) or 0),
            "window_draining": bool(int((network_row["window_draining"] if network_row else 0) or 0)),
            "pending_side": str((network_row["pending_side"] if network_row else "") or ""),
            "queued_internal": int((network_row["queued_internal"] if network_row else 0) or 0),
            "queued_external": int((network_row["queued_external"] if network_row else 0) or 0),
            "queued_pipeline": int((network_row["queued_pipeline"] if network_row else 0) or 0),
            "running_internal": int((network_row["running_internal"] if network_row else 0) or 0),
            "running_external": int((network_row["running_external"] if network_row else 0) or 0),
            "running_pipeline": int((network_row["running_pipeline"] if network_row else 0) or 0),
            "oldest_internal_wait_sec": int((network_row["oldest_internal_wait_sec"] if network_row else 0) or 0),
            "oldest_external_wait_sec": int((network_row["oldest_external_wait_sec"] if network_row else 0) or 0),
        }
        return {
            "network": network,
            "controlled_browser": {
                "holder_job_id": str((browser_row.get("holder_job_ids") or [""])[0] if browser_row.get("holder_job_ids") else ""),
                "queue_length": int(browser_row.get("queue_length") or 0),
            },
            "batch_locks": [
                {
                    "batch_key": str(item.get("resource_key", "")).split(":", 1)[1],
                    "holder_job_id": str((item.get("holder_job_ids") or [""])[0] if item.get("holder_job_ids") else ""),
                    "queue_length": int(item.get("queue_length") or 0),
                }
                for item in batch_rows
                if ":" in str(item.get("resource_key", ""))
            ],
            "resources": resources,
        }
