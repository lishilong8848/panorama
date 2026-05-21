from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterator

from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from pipeline_utils import get_app_dir


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


class AppStateRepository:
    """Local SQLite foundation for runtime state that must not live on UNC paths."""

    DB_FILE = "app_state.sqlite3"

    def __init__(self, *, runtime_config: Dict[str, Any] | None = None, app_dir: Path | None = None) -> None:
        self.app_dir = Path(app_dir or get_app_dir()).resolve()
        self.runtime_root = resolve_runtime_state_root(
            runtime_config=runtime_config if isinstance(runtime_config, dict) else {},
            app_dir=self.app_dir,
        )
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.runtime_root / self.DB_FILE
        self._lock = threading.RLock()
        self._ready = False

    @contextmanager
    def connect(self, *, read_only: bool = False) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = sqlite3.connect(str(self.db_path), timeout=30.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=30000")
                conn.execute("PRAGMA foreign_keys=ON")
                if read_only:
                    conn.execute("PRAGMA query_only=ON")
                yield conn
                if not read_only and conn.in_transaction:
                    conn.commit()
            except Exception:
                if conn.in_transaction:
                    conn.rollback()
                raise
            finally:
                conn.close()

    def ensure_ready(self) -> None:
        if self._ready:
            return
        with self._lock:
            if self._ready:
                return
            with self.connect() as conn:
                self._create_schema(conn)
                self._record_schema_version(conn)
            self._ready = True

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS config_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                source TEXT NOT NULL DEFAULT '',
                config_path TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_config_snapshots_created_at
                ON config_snapshots(created_at DESC);

            CREATE TABLE IF NOT EXISTS scheduler_jobs (
                scheduler_key TEXT PRIMARY KEY,
                feature TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 0,
                running INTEGER NOT NULL DEFAULT 0,
                next_run_at TEXT NOT NULL DEFAULT '',
                last_run_at TEXT NOT NULL DEFAULT '',
                last_status TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_jobs (
                job_id TEXT PRIMARY KEY,
                feature TEXT NOT NULL DEFAULT '',
                dedupe_key TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                submitted_by TEXT NOT NULL DEFAULT '',
                priority TEXT NOT NULL DEFAULT '',
                resource_keys_json TEXT NOT NULL DEFAULT '[]',
                bridge_task_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL DEFAULT '',
                finished_at TEXT NOT NULL DEFAULT '',
                summary TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_task_jobs_status_feature
                ON task_jobs(status, feature, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_task_jobs_dedupe
                ON task_jobs(dedupe_key, status, updated_at DESC);

            CREATE TABLE IF NOT EXISTS task_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                stage_id TEXT NOT NULL DEFAULT '',
                event_type TEXT NOT NULL DEFAULT '',
                level TEXT NOT NULL DEFAULT 'info',
                message TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_task_events_job
                ON task_events(job_id, event_id);

            CREATE TABLE IF NOT EXISTS generated_files (
                file_id TEXT PRIMARY KEY,
                feature TEXT NOT NULL DEFAULT '',
                building TEXT NOT NULL DEFAULT '',
                duty_date TEXT NOT NULL DEFAULT '',
                duty_shift TEXT NOT NULL DEFAULT '',
                file_kind TEXT NOT NULL DEFAULT '',
                file_path TEXT NOT NULL DEFAULT '',
                session_id TEXT NOT NULL DEFAULT '',
                batch_key TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_generated_files_lookup
                ON generated_files(building, duty_date DESC, duty_shift, file_kind);

            CREATE TABLE IF NOT EXISTS bridge_tasks (
                task_id TEXT PRIMARY KEY,
                feature TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                dedupe_key TEXT NOT NULL DEFAULT '',
                request_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT NOT NULL DEFAULT '{}',
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_bridge_tasks_status_feature
                ON bridge_tasks(status, feature, updated_at DESC);

            CREATE TABLE IF NOT EXISTS bridge_source_index (
                source_family TEXT NOT NULL,
                bucket_key TEXT NOT NULL,
                building TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '',
                file_path TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(source_family, bucket_key, building)
            );
            CREATE INDEX IF NOT EXISTS idx_bridge_source_index_updated
                ON bridge_source_index(source_family, updated_at DESC);
            """
        )

    def _record_schema_version(self, conn: sqlite3.Connection) -> None:
        now = _now_text()
        conn.execute(
            """
            INSERT OR REPLACE INTO app_meta(key, value, updated_at)
            VALUES('schema_version', '1', ?)
            """,
            (now,),
        )

    def record_config_snapshot(self, *, snapshot_id: str, source: str, config_path: str, payload: Dict[str, Any]) -> None:
        self.ensure_ready()
        now = _now_text()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO config_snapshots(
                    snapshot_id, source, config_path, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (snapshot_id, source, config_path, _json_dumps(payload), now),
            )

    def upsert_scheduler_job(self, scheduler_key: str, payload: Dict[str, Any]) -> None:
        self.ensure_ready()
        now = _now_text()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO scheduler_jobs(
                    scheduler_key, feature, enabled, running, next_run_at, last_run_at,
                    last_status, last_error, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scheduler_key,
                    str(payload.get("feature", "") or ""),
                    1 if bool(payload.get("enabled", False)) else 0,
                    1 if bool(payload.get("running", False)) else 0,
                    str(payload.get("next_run_at", "") or ""),
                    str(payload.get("last_run_at", "") or ""),
                    str(payload.get("last_status", "") or ""),
                    str(payload.get("last_error", "") or ""),
                    _json_dumps(payload),
                    now,
                ),
            )

    def upsert_task_job(self, payload: Dict[str, Any]) -> None:
        self.ensure_ready()
        if not isinstance(payload, dict):
            return
        job_id = str(payload.get("job_id", "") or "").strip()
        if not job_id:
            return
        now = _now_text()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO task_jobs(
                    job_id, feature, dedupe_key, status, submitted_by, priority,
                    resource_keys_json, bridge_task_id, created_at, started_at,
                    finished_at, summary, error, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    str(payload.get("feature", "") or ""),
                    str(payload.get("dedupe_key", "") or ""),
                    str(payload.get("status", "") or ""),
                    str(payload.get("submitted_by", "") or ""),
                    str(payload.get("priority", "") or ""),
                    _json_dumps(payload.get("resource_keys", [])),
                    str(payload.get("bridge_task_id", "") or ""),
                    str(payload.get("created_at", "") or ""),
                    str(payload.get("started_at", "") or ""),
                    str(payload.get("finished_at", "") or ""),
                    str(payload.get("summary", "") or ""),
                    str(payload.get("error", "") or ""),
                    _json_dumps(payload),
                    now,
                ),
            )

    def append_task_event(
        self,
        *,
        job_id: str,
        stage_id: str = "",
        event_type: str = "",
        level: str = "info",
        message: str = "",
        payload: Dict[str, Any] | None = None,
    ) -> int:
        self.ensure_ready()
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return 0
        created_at = _now_text()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO task_events(
                    job_id, stage_id, event_type, level, message, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_job_id,
                    str(stage_id or "").strip(),
                    str(event_type or "").strip(),
                    str(level or "info").strip() or "info",
                    str(message or "").strip(),
                    _json_dumps(payload or {}),
                    created_at,
                ),
            )
            return int(cursor.lastrowid or 0)

    def upsert_generated_file(self, payload: Dict[str, Any]) -> None:
        self.ensure_ready()
        if not isinstance(payload, dict):
            return
        file_id = str(payload.get("file_id", "") or "").strip()
        file_path = str(payload.get("file_path", "") or "").strip()
        if not file_id or not file_path:
            return
        now = _now_text()
        created_at = str(payload.get("created_at", "") or "").strip() or now
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO generated_files(
                    file_id, feature, building, duty_date, duty_shift, file_kind,
                    file_path, session_id, batch_key, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_id) DO UPDATE SET
                    feature=excluded.feature,
                    building=excluded.building,
                    duty_date=excluded.duty_date,
                    duty_shift=excluded.duty_shift,
                    file_kind=excluded.file_kind,
                    file_path=excluded.file_path,
                    session_id=excluded.session_id,
                    batch_key=excluded.batch_key,
                    updated_at=excluded.updated_at
                """,
                (
                    file_id,
                    str(payload.get("feature", "") or ""),
                    str(payload.get("building", "") or ""),
                    str(payload.get("duty_date", "") or ""),
                    str(payload.get("duty_shift", "") or ""),
                    str(payload.get("file_kind", "") or ""),
                    file_path,
                    str(payload.get("session_id", "") or ""),
                    str(payload.get("batch_key", "") or ""),
                    created_at,
                    now,
                ),
            )

    def list_handover_generated_file_sessions(
        self,
        *,
        building: str,
        days: int = 3,
        limit: int = 6,
    ) -> list[Dict[str, Any]]:
        self.ensure_ready()
        building_name = str(building or "").strip()
        if not building_name:
            return []
        safe_limit = max(1, int(limit or 6))
        safe_days = max(1, int(days or 3))
        cutoff = (datetime.now() - timedelta(days=safe_days - 1)).strftime("%Y-%m-%d")
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM generated_files
                WHERE feature='handover_log'
                  AND building=?
                  AND duty_date>=?
                ORDER BY duty_date DESC,
                    CASE duty_shift WHEN 'night' THEN 2 ELSE 1 END DESC,
                    updated_at DESC
                """,
                (building_name, cutoff),
            ).fetchall()
        grouped: Dict[str, Dict[str, Any]] = {}
        order: list[str] = []
        for row in rows:
            session_id = str(row["session_id"] or "").strip()
            if not session_id:
                session_id = f"{row['building']}|{row['duty_date']}|{row['duty_shift']}"
            if session_id not in grouped:
                grouped[session_id] = {
                    "session_id": session_id,
                    "building": str(row["building"] or "").strip(),
                    "duty_date": str(row["duty_date"] or "").strip(),
                    "duty_shift": str(row["duty_shift"] or "").strip(),
                    "batch_key": str(row["batch_key"] or "").strip(),
                    "updated_at": str(row["updated_at"] or row["created_at"] or "").strip(),
                    "output_file": "",
                    "capacity_output_file": "",
                    "confirmed": False,
                    "revision": 0,
                }
                order.append(session_id)
            item = grouped[session_id]
            file_kind = str(row["file_kind"] or "").strip()
            file_path = str(row["file_path"] or "").strip()
            if file_kind == "handover_log":
                item["output_file"] = file_path
            elif file_kind == "capacity_report":
                item["capacity_output_file"] = file_path
            if str(row["updated_at"] or "").strip() > str(item.get("updated_at", "") or ""):
                item["updated_at"] = str(row["updated_at"] or "").strip()
        return [grouped[key] for key in order[:safe_limit]]

    def snapshot(self) -> Dict[str, Any]:
        self.ensure_ready()
        with self.connect(read_only=True) as conn:
            table_counts: Dict[str, int] = {}
            for table_name in (
                "config_snapshots",
                "scheduler_jobs",
                "task_jobs",
                "task_events",
                "generated_files",
                "bridge_tasks",
                "bridge_source_index",
            ):
                row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}").fetchone()
                table_counts[table_name] = int(row["cnt"] if row else 0)
        return {
            "db_path": str(self.db_path),
            "ready": True,
            "table_counts": table_counts,
        }
