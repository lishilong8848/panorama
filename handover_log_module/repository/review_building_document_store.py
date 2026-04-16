from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager, nullcontext
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List

from app.config.handover_segment_store import building_code_from_name
from handover_log_module.repository.event_followup_cache_store import _resolve_runtime_state_root


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _json_loads(text: Any, fallback: Any) -> Any:
    try:
        payload = json.loads(str(text or ""))
        return payload
    except Exception:  # noqa: BLE001
        return fallback


_LOCKS_GUARD = threading.Lock()
_LOCKS: Dict[str, threading.RLock] = {}


def _lock_for_path(path: Path) -> threading.RLock:
    key = str(path.resolve(strict=False)).casefold()
    with _LOCKS_GUARD:
        lock = _LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _LOCKS[key] = lock
        return lock


class ReviewBuildingDocumentStore:
    """Per-building SQLite store for handover review document state."""

    def __init__(
        self,
        *,
        config: Dict[str, Any],
        building: str,
        busy_timeout_ms: int = 30000,
    ) -> None:
        self.config = config if isinstance(config, dict) else {}
        self.building = str(building or "").strip()
        try:
            code = building_code_from_name(self.building)
        except Exception:  # noqa: BLE001
            code = self.building.lower()[:1] or "unknown"
        global_paths = self.config.get("_global_paths", {})
        runtime_root = _resolve_runtime_state_root(global_paths=global_paths if isinstance(global_paths, dict) else None)
        self.db_path = runtime_root / "handover_review_sqlite" / f"{code}.sqlite3"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.busy_timeout_ms = max(1000, int(busy_timeout_ms or 30000))
        self._write_lock = _lock_for_path(self.db_path)
        self._ready = False
        self._ready_lock = threading.Lock()

    @contextmanager
    def connect(self, *, read_only: bool = False) -> Iterator[sqlite3.Connection]:
        lock_context = nullcontext() if read_only else self._write_lock
        with lock_context:
            conn = sqlite3.connect(
                str(self.db_path),
                timeout=self.busy_timeout_ms / 1000.0,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
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
        with self._ready_lock:
            if self._ready:
                return
            with self.connect() as conn:
                self._create_schema(conn)
            self._ready = True

    @staticmethod
    def _create_schema(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS review_documents (
                session_id TEXT PRIMARY KEY,
                building TEXT NOT NULL,
                duty_date TEXT NOT NULL,
                duty_shift TEXT NOT NULL,
                batch_key TEXT NOT NULL,
                revision INTEGER NOT NULL,
                document_json TEXT NOT NULL,
                dirty_regions_json TEXT NOT NULL DEFAULT '{}',
                source_excel_path TEXT NOT NULL DEFAULT '',
                imported_from_excel INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_review_documents_context
                ON review_documents(building, duty_date, duty_shift);

            CREATE TABLE IF NOT EXISTS excel_sync_state (
                session_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                synced_revision INTEGER NOT NULL DEFAULT 0,
                pending_revision INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS building_defaults (
                default_key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_jobs (
                session_id TEXT PRIMARY KEY,
                target_revision INTEGER NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sync_jobs_status
                ON sync_jobs(status, updated_at);
            """
        )

    @staticmethod
    def _row_to_document(row: sqlite3.Row | None) -> Dict[str, Any] | None:
        if row is None:
            return None
        return {
            "session_id": str(row["session_id"] or ""),
            "building": str(row["building"] or ""),
            "duty_date": str(row["duty_date"] or ""),
            "duty_shift": str(row["duty_shift"] or ""),
            "batch_key": str(row["batch_key"] or ""),
            "revision": int(row["revision"] or 0),
            "document": _json_loads(row["document_json"], {}),
            "dirty_regions": _json_loads(row["dirty_regions_json"], {}),
            "source_excel_path": str(row["source_excel_path"] or ""),
            "imported_from_excel": bool(row["imported_from_excel"]),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    @staticmethod
    def _row_to_sync(row: sqlite3.Row | None) -> Dict[str, Any]:
        if row is None:
            return {
                "status": "unknown",
                "synced_revision": 0,
                "pending_revision": 0,
                "error": "",
                "updated_at": "",
            }
        return {
            "status": str(row["status"] or "").strip().lower(),
            "synced_revision": int(row["synced_revision"] or 0),
            "pending_revision": int(row["pending_revision"] or 0),
            "error": str(row["error"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }

    def get_document(self, session_id: str) -> Dict[str, Any] | None:
        self.ensure_ready()
        with self.connect(read_only=True) as conn:
            row = conn.execute(
                "SELECT * FROM review_documents WHERE session_id=?",
                (str(session_id or "").strip(),),
            ).fetchone()
        return self._row_to_document(row)

    def upsert_imported_document(
        self,
        *,
        session: Dict[str, Any],
        document: Dict[str, Any],
        imported_from_excel: bool,
    ) -> Dict[str, Any]:
        self.ensure_ready()
        now = _now_text()
        session_id = str(session.get("session_id", "") or "").strip()
        revision = int(session.get("revision", 0) or 0)
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT * FROM review_documents WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO review_documents(
                        session_id, building, duty_date, duty_shift, batch_key,
                        revision, document_json, dirty_regions_json,
                        source_excel_path, imported_from_excel, created_at, updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        session_id,
                        str(session.get("building", "") or "").strip(),
                        str(session.get("duty_date", "") or "").strip(),
                        str(session.get("duty_shift", "") or "").strip().lower(),
                        str(session.get("batch_key", "") or "").strip(),
                        revision,
                        _json_dumps(document if isinstance(document, dict) else {}),
                        "{}",
                        str(session.get("output_file", "") or "").strip(),
                        1 if imported_from_excel else 0,
                        now,
                        now,
                    ),
                )
                self._upsert_sync_state_conn(
                    conn,
                    session_id=session_id,
                    status="synced",
                    synced_revision=revision,
                    pending_revision=0,
                    error="",
                    updated_at=now,
                )
                row = conn.execute("SELECT * FROM review_documents WHERE session_id=?", (session_id,)).fetchone()
                return self._row_to_document(row) or {}

            row = conn.execute("SELECT * FROM review_documents WHERE session_id=?", (session_id,)).fetchone()
            return self._row_to_document(row) or {}

    def save_document(
        self,
        *,
        session: Dict[str, Any],
        document: Dict[str, Any],
        base_revision: int,
        dirty_regions: Dict[str, Any],
    ) -> tuple[Dict[str, Any], Dict[str, Any] | None]:
        self.ensure_ready()
        session_id = str(session.get("session_id", "") or "").strip()
        next_revision = int(base_revision or 0) + 1
        now = _now_text()
        with self.connect() as conn:
            previous_row = conn.execute(
                "SELECT * FROM review_documents WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if previous_row is None:
                raise KeyError("review document not initialized")
            previous = self._row_to_document(previous_row)
            current_revision = int(previous_row["revision"] or 0)
            if current_revision != int(base_revision or 0):
                raise ValueError("revision_conflict")
            conn.execute(
                """
                UPDATE review_documents
                   SET revision=?,
                       document_json=?,
                       dirty_regions_json=?,
                       source_excel_path=?,
                       updated_at=?
                 WHERE session_id=?
                """,
                (
                    next_revision,
                    _json_dumps(document if isinstance(document, dict) else {}),
                    _json_dumps(dirty_regions if isinstance(dirty_regions, dict) else {}),
                    str(session.get("output_file", "") or "").strip(),
                    now,
                    session_id,
                ),
            )
            current_sync = self.get_sync_state_from_conn(conn, session_id)
            self._upsert_sync_state_conn(
                conn,
                session_id=session_id,
                status="pending",
                synced_revision=int(current_sync.get("synced_revision", 0) or 0),
                pending_revision=next_revision,
                error="",
                updated_at=now,
            )
            self._upsert_sync_job_conn(
                conn,
                session_id=session_id,
                target_revision=next_revision,
                status="pending",
                reset_attempts=True,
                error="",
                updated_at=now,
            )
            row = conn.execute("SELECT * FROM review_documents WHERE session_id=?", (session_id,)).fetchone()
        return self._row_to_document(row) or {}, previous

    def restore_document(self, previous: Dict[str, Any] | None) -> None:
        if not isinstance(previous, dict) or not previous.get("session_id"):
            return
        self.ensure_ready()
        now = _now_text()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE review_documents
                   SET revision=?,
                       document_json=?,
                       dirty_regions_json=?,
                       source_excel_path=?,
                       imported_from_excel=?,
                       updated_at=?
                 WHERE session_id=?
                """,
                (
                    int(previous.get("revision", 0) or 0),
                    _json_dumps(previous.get("document", {}) if isinstance(previous.get("document", {}), dict) else {}),
                    _json_dumps(previous.get("dirty_regions", {}) if isinstance(previous.get("dirty_regions", {}), dict) else {}),
                    str(previous.get("source_excel_path", "") or ""),
                    1 if previous.get("imported_from_excel") else 0,
                    now,
                    str(previous.get("session_id", "") or ""),
                ),
            )
            self._upsert_sync_state_conn(
                conn,
                session_id=str(previous.get("session_id", "") or ""),
                status="synced",
                synced_revision=int(previous.get("revision", 0) or 0),
                pending_revision=0,
                error="",
                updated_at=now,
            )
            conn.execute(
                "DELETE FROM sync_jobs WHERE session_id=?",
                (str(previous.get("session_id", "") or ""),),
            )

    def get_sync_state_from_conn(self, conn: sqlite3.Connection, session_id: str) -> Dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM excel_sync_state WHERE session_id=?",
            (str(session_id or "").strip(),),
        ).fetchone()
        return self._row_to_sync(row)

    def get_sync_state(self, session_id: str) -> Dict[str, Any]:
        self.ensure_ready()
        with self.connect(read_only=True) as conn:
            return self.get_sync_state_from_conn(conn, session_id)

    @staticmethod
    def _upsert_sync_state_conn(
        conn: sqlite3.Connection,
        *,
        session_id: str,
        status: str,
        synced_revision: int,
        pending_revision: int,
        error: str,
        updated_at: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO excel_sync_state(session_id, status, synced_revision, pending_revision, error, updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(session_id) DO UPDATE SET
                status=excluded.status,
                synced_revision=excluded.synced_revision,
                pending_revision=excluded.pending_revision,
                error=excluded.error,
                updated_at=excluded.updated_at
            """,
            (
                str(session_id or "").strip(),
                str(status or "").strip().lower(),
                int(synced_revision or 0),
                int(pending_revision or 0),
                str(error or "").strip(),
                updated_at or _now_text(),
            ),
        )

    def update_sync_state(
        self,
        *,
        session_id: str,
        status: str,
        synced_revision: int = 0,
        pending_revision: int = 0,
        error: str = "",
    ) -> Dict[str, Any]:
        self.ensure_ready()
        now = _now_text()
        with self.connect() as conn:
            self._upsert_sync_state_conn(
                conn,
                session_id=session_id,
                status=status,
                synced_revision=synced_revision,
                pending_revision=pending_revision,
                error=error,
                updated_at=now,
            )
        return self.get_sync_state(session_id)

    @staticmethod
    def _upsert_sync_job_conn(
        conn: sqlite3.Connection,
        *,
        session_id: str,
        target_revision: int,
        status: str,
        reset_attempts: bool,
        error: str,
        updated_at: str,
    ) -> None:
        if reset_attempts:
            conn.execute(
                """
                INSERT INTO sync_jobs(session_id, target_revision, status, attempts, error, updated_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(session_id) DO UPDATE SET
                    target_revision=excluded.target_revision,
                    status=excluded.status,
                    attempts=0,
                    error=excluded.error,
                    updated_at=excluded.updated_at
                """,
                (
                    str(session_id or "").strip(),
                    int(target_revision or 0),
                    str(status or "").strip().lower(),
                    0,
                    str(error or "").strip(),
                    updated_at or _now_text(),
                ),
            )
            return
        conn.execute(
            """
            INSERT INTO sync_jobs(session_id, target_revision, status, attempts, error, updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(session_id) DO UPDATE SET
                target_revision=excluded.target_revision,
                status=excluded.status,
                error=excluded.error,
                updated_at=excluded.updated_at
            """,
            (
                str(session_id or "").strip(),
                int(target_revision or 0),
                str(status or "").strip().lower(),
                0,
                str(error or "").strip(),
                updated_at or _now_text(),
            ),
        )

    def enqueue_sync_job(self, *, session_id: str, target_revision: int) -> None:
        self.ensure_ready()
        now = _now_text()
        with self.connect() as conn:
            self._upsert_sync_job_conn(
                conn,
                session_id=session_id,
                target_revision=target_revision,
                status="pending",
                reset_attempts=True,
                error="",
                updated_at=now,
            )

    def claim_next_job(self) -> Dict[str, Any] | None:
        self.ensure_ready()
        now = _now_text()
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM sync_jobs
                 WHERE status='pending'
                 ORDER BY updated_at ASC
                 LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            session_id = str(row["session_id"] or "")
            conn.execute(
                "UPDATE sync_jobs SET status='running', attempts=attempts+1, updated_at=? WHERE session_id=?",
                (now, session_id),
            )
            return {
                "session_id": session_id,
                "target_revision": int(row["target_revision"] or 0),
                "attempts": int(row["attempts"] or 0) + 1,
            }

    def finish_job(
        self,
        *,
        session_id: str,
        success: bool,
        claimed_target_revision: int,
        synced_revision: int = 0,
        error: str = "",
    ) -> Dict[str, Any]:
        self.ensure_ready()
        now = _now_text()
        with self.connect() as conn:
            claimed_revision = int(claimed_target_revision or 0)
            actual_synced_revision = int(synced_revision or 0)
            sync_state = self.get_sync_state_from_conn(conn, session_id)
            current_synced_revision = int(sync_state.get("synced_revision", 0) or 0)
            current_row = conn.execute(
                "SELECT * FROM sync_jobs WHERE session_id=?",
                (str(session_id or "").strip(),),
            ).fetchone()
            current_target_revision = int(current_row["target_revision"] or 0) if current_row is not None else 0
            if success:
                next_synced_revision = max(current_synced_revision, actual_synced_revision)
                if current_row is not None and current_target_revision > next_synced_revision:
                    conn.execute(
                        "UPDATE sync_jobs SET status='pending', error='', updated_at=? WHERE session_id=?",
                        (now, str(session_id or "").strip()),
                    )
                    self._upsert_sync_state_conn(
                        conn,
                        session_id=session_id,
                        status="pending",
                        synced_revision=next_synced_revision,
                        pending_revision=current_target_revision,
                        error="",
                        updated_at=now,
                    )
                else:
                    conn.execute("DELETE FROM sync_jobs WHERE session_id=?", (str(session_id or "").strip(),))
                    self._upsert_sync_state_conn(
                        conn,
                        session_id=session_id,
                        status="synced",
                        synced_revision=next_synced_revision,
                        pending_revision=0,
                        error="",
                        updated_at=now,
                    )
            else:
                if current_row is not None and current_target_revision > claimed_revision:
                    conn.execute(
                        "UPDATE sync_jobs SET status='pending', updated_at=? WHERE session_id=?",
                        (now, str(session_id or "").strip()),
                    )
                    self._upsert_sync_state_conn(
                        conn,
                        session_id=session_id,
                        status="pending",
                        synced_revision=current_synced_revision,
                        pending_revision=current_target_revision,
                        error="",
                        updated_at=now,
                    )
                else:
                    if current_row is not None:
                        conn.execute(
                            "UPDATE sync_jobs SET status='failed', error=?, updated_at=? WHERE session_id=?",
                            (str(error or "").strip(), now, str(session_id or "").strip()),
                        )
                    self._upsert_sync_state_conn(
                        conn,
                        session_id=session_id,
                        status="failed",
                        synced_revision=current_synced_revision,
                        pending_revision=current_target_revision or claimed_revision,
                        error=str(error or "").strip(),
                        updated_at=now,
                    )
            return self.get_sync_state_from_conn(conn, session_id)

    def get_default(self, key: str) -> Dict[str, Any] | List[Dict[str, Any]] | None:
        self.ensure_ready()
        with self.connect(read_only=True) as conn:
            row = conn.execute(
                "SELECT value_json FROM building_defaults WHERE default_key=?",
                (str(key or "").strip(),),
            ).fetchone()
        if row is None:
            return None
        return _json_loads(row["value_json"], None)

    def set_default(self, key: str, value: Any) -> bool:
        self.ensure_ready()
        now = _now_text()
        normalized_key = str(key or "").strip()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT value_json, revision FROM building_defaults WHERE default_key=?",
                (normalized_key,),
            ).fetchone()
            next_json = _json_dumps(value)
            if row is not None and str(row["value_json"] or "") == next_json:
                return False
            revision = int(row["revision"] or 0) + 1 if row is not None else 1
            conn.execute(
                """
                INSERT INTO building_defaults(default_key, value_json, revision, updated_at)
                VALUES(?,?,?,?)
                ON CONFLICT(default_key) DO UPDATE SET
                    value_json=excluded.value_json,
                    revision=excluded.revision,
                    updated_at=excluded.updated_at
                """,
                (normalized_key, next_json, revision, now),
            )
            return True

    def delete_default(self, key: str) -> bool:
        self.ensure_ready()
        normalized_key = str(key or "").strip()
        if not normalized_key:
            return False
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM building_defaults WHERE default_key=?",
                (normalized_key,),
            ).fetchone()
            if row is None:
                return False
            conn.execute(
                "DELETE FROM building_defaults WHERE default_key=?",
                (normalized_key,),
            )
        return True
