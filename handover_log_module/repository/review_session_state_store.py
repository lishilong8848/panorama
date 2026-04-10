from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterator

from handover_log_module.repository.event_followup_cache_store import (
    EventFollowupCacheStore,
    _resolve_runtime_state_root,
    _safe_load_json,
)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _add_seconds_text(value: str, seconds: int) -> str:
    try:
        base = datetime.strptime(str(value or "").strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        base = datetime.now()
    return (base + timedelta(seconds=max(0, int(seconds or 0)))).strftime("%Y-%m-%d %H:%M:%S")


def _default_holder_label(client_id: str) -> str:
    suffix = str(client_id or "").strip().upper()[-4:]
    return f"终端-{suffix or '----'}"


class ReviewSessionStateStore:
    DB_FILE = "handover_review_state.db"
    DEFAULT_STATE: Dict[str, Any] = {
        "review_sessions": {},
        "review_latest_by_building": {},
        "review_cloud_batches": {},
        "review_batch_status": {},
        "updated_at": "",
    }

    def __init__(
        self,
        *,
        cache_state_file: str = "",
        global_paths: Dict[str, Any] | None = None,
        db_file: str = "",
        busy_timeout_ms: int = 30000,
    ) -> None:
        runtime_root = _resolve_runtime_state_root(global_paths=global_paths)
        db_name = str(db_file or "").strip() or self.DB_FILE
        db_path = Path(db_name)
        if not db_path.is_absolute():
            db_path = runtime_root / db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path

        legacy_name = str(cache_state_file or "").strip() or EventFollowupCacheStore.SHARED_STATE_FILE
        legacy_path = Path(legacy_name)
        if not legacy_path.is_absolute():
            legacy_path = runtime_root / legacy_path
        self.legacy_state_path = legacy_path

        self.busy_timeout_ms = max(1000, int(busy_timeout_ms or 30000))
        self._ready = False
        self._ready_lock = threading.Lock()
        self._write_lock = threading.Lock()

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
                self._migrate_from_legacy_if_needed(conn)
            self._ready = True

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS review_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS review_sessions (
                session_id TEXT PRIMARY KEY,
                building TEXT NOT NULL,
                duty_date TEXT NOT NULL,
                duty_shift TEXT NOT NULL,
                batch_key TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_review_sessions_building
                ON review_sessions(building);
            CREATE INDEX IF NOT EXISTS idx_review_sessions_batch
                ON review_sessions(batch_key);

            CREATE TABLE IF NOT EXISTS review_latest_by_building (
                building TEXT PRIMARY KEY,
                session_id TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS review_cloud_batches (
                batch_key TEXT PRIMARY KEY,
                duty_date TEXT NOT NULL,
                duty_shift TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS review_session_locks (
                lock_key TEXT PRIMARY KEY,
                building TEXT NOT NULL,
                session_id TEXT NOT NULL,
                holder_client_id TEXT NOT NULL,
                holder_label TEXT NOT NULL,
                claimed_at TEXT NOT NULL,
                last_heartbeat_at TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_review_session_locks_target
                ON review_session_locks(building, session_id);
            """
        )

    def _migrate_from_legacy_if_needed(self, conn: sqlite3.Connection) -> None:
        existing_sessions = int(
            conn.execute("SELECT COUNT(*) AS cnt FROM review_sessions").fetchone()["cnt"] or 0
        )
        existing_cloud_batches = int(
            conn.execute("SELECT COUNT(*) AS cnt FROM review_cloud_batches").fetchone()["cnt"] or 0
        )
        initialized = conn.execute(
            "SELECT value FROM review_meta WHERE key = 'legacy_migration_done'"
        ).fetchone()
        if initialized is not None or existing_sessions > 0 or existing_cloud_batches > 0:
            conn.execute(
                "INSERT OR REPLACE INTO review_meta(key, value) VALUES('legacy_migration_done', ?)",
                (_now_text(),),
            )
            return

        legacy = self._load_legacy_state()
        self._write_state(conn, legacy)
        conn.execute(
            "INSERT OR REPLACE INTO review_meta(key, value) VALUES('legacy_migration_done', ?)",
            (_now_text(),),
        )

    def _load_legacy_state(self) -> Dict[str, Any]:
        raw = _safe_load_json(self.legacy_state_path, EventFollowupCacheStore.DEFAULT_STATE)
        payload = raw if isinstance(raw, dict) else {}
        return self._normalize_state(
            {
                "review_sessions": payload.get("review_sessions", {}),
                "review_latest_by_building": payload.get("review_latest_by_building", {}),
                "review_cloud_batches": payload.get("review_cloud_batches", {}),
                "updated_at": str(payload.get("updated_at", "")).strip(),
            }
        )

    def _normalize_state(self, payload: Dict[str, Any] | None) -> Dict[str, Any]:
        state = dict(self.DEFAULT_STATE)
        raw = payload if isinstance(payload, dict) else {}
        state["review_sessions"] = (
            dict(raw.get("review_sessions", {}))
            if isinstance(raw.get("review_sessions", {}), dict)
            else {}
        )
        state["review_latest_by_building"] = (
            dict(raw.get("review_latest_by_building", {}))
            if isinstance(raw.get("review_latest_by_building", {}), dict)
            else {}
        )
        state["review_cloud_batches"] = (
            dict(raw.get("review_cloud_batches", {}))
            if isinstance(raw.get("review_cloud_batches", {}), dict)
            else {}
        )
        state["review_batch_status"] = {}
        state["updated_at"] = str(raw.get("updated_at", "")).strip()
        return state

    def _write_state(self, conn: sqlite3.Connection, payload: Dict[str, Any]) -> None:
        state = self._normalize_state(payload)
        sessions = state["review_sessions"]
        latest = state["review_latest_by_building"]
        cloud_batches = state["review_cloud_batches"]

        conn.execute("DELETE FROM review_sessions")
        conn.execute("DELETE FROM review_latest_by_building")
        conn.execute("DELETE FROM review_cloud_batches")

        for session_id, raw in sessions.items():
            if not isinstance(raw, dict):
                continue
            normalized_id = str(session_id or "").strip()
            if not normalized_id:
                continue
            conn.execute(
                """
                INSERT INTO review_sessions(
                    session_id,
                    building,
                    duty_date,
                    duty_shift,
                    batch_key,
                    updated_at,
                    payload_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_id,
                    str(raw.get("building", "")).strip(),
                    str(raw.get("duty_date", "")).strip(),
                    str(raw.get("duty_shift", "")).strip().lower(),
                    str(raw.get("batch_key", "")).strip(),
                    str(raw.get("updated_at", "")).strip(),
                    json.dumps(raw, ensure_ascii=False),
                ),
            )

        for building, session_id in latest.items():
            building_name = str(building or "").strip()
            normalized_session_id = str(session_id or "").strip()
            if not building_name or not normalized_session_id:
                continue
            conn.execute(
                "INSERT INTO review_latest_by_building(building, session_id) VALUES(?, ?)",
                (building_name, normalized_session_id),
            )

        for batch_key, raw in cloud_batches.items():
            if not isinstance(raw, dict):
                continue
            normalized_batch = str(batch_key or "").strip()
            if not normalized_batch:
                continue
            conn.execute(
                """
                INSERT INTO review_cloud_batches(
                    batch_key,
                    duty_date,
                    duty_shift,
                    updated_at,
                    payload_json
                ) VALUES(?, ?, ?, ?, ?)
                """,
                (
                    normalized_batch,
                    str(raw.get("duty_date", "")).strip(),
                    str(raw.get("duty_shift", "")).strip().lower(),
                    str(raw.get("updated_at", "")).strip(),
                    json.dumps(raw, ensure_ascii=False),
                ),
            )

        conn.execute(
            "INSERT OR REPLACE INTO review_meta(key, value) VALUES('updated_at', ?)",
            (str(state.get("updated_at", "")).strip(),),
        )

    def load_state(self) -> Dict[str, Any]:
        self.ensure_ready()
        with self.connect(read_only=True) as conn:
            sessions: Dict[str, Any] = {}
            for row in conn.execute("SELECT session_id, payload_json FROM review_sessions").fetchall():
                try:
                    payload = json.loads(str(row["payload_json"] or ""))
                except Exception:  # noqa: BLE001
                    continue
                if isinstance(payload, dict):
                    sessions[str(row["session_id"] or "").strip()] = payload

            latest = {
                str(row["building"] or "").strip(): str(row["session_id"] or "").strip()
                for row in conn.execute(
                    "SELECT building, session_id FROM review_latest_by_building"
                ).fetchall()
                if str(row["building"] or "").strip() and str(row["session_id"] or "").strip()
            }

            cloud_batches: Dict[str, Any] = {}
            for row in conn.execute("SELECT batch_key, payload_json FROM review_cloud_batches").fetchall():
                try:
                    payload = json.loads(str(row["payload_json"] or ""))
                except Exception:  # noqa: BLE001
                    continue
                if isinstance(payload, dict):
                    cloud_batches[str(row["batch_key"] or "").strip()] = payload

            meta_row = conn.execute(
                "SELECT value FROM review_meta WHERE key = 'updated_at'"
            ).fetchone()
            updated_at = str(meta_row["value"] or "").strip() if meta_row is not None else ""
            return self._normalize_state(
                {
                    "review_sessions": sessions,
                    "review_latest_by_building": latest,
                    "review_cloud_batches": cloud_batches,
                    "updated_at": updated_at,
                }
            )

    def save_state(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.ensure_ready()
        state = self._normalize_state(payload)
        state["updated_at"] = _now_text()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._write_state(conn, state)
        return state

    @staticmethod
    def _lock_key(building: str, session_id: str) -> str:
        return f"{str(building or '').strip()}::{str(session_id or '').strip()}"

    def _row_to_concurrency(
        self,
        row: sqlite3.Row | None,
        *,
        current_revision: int,
        client_id: str,
    ) -> Dict[str, Any]:
        holder_client_id = str(row["holder_client_id"] or "").strip() if row is not None else ""
        holder_label = str(row["holder_label"] or "").strip() if row is not None else ""
        lease_expires_at = str(row["lease_expires_at"] or "").strip() if row is not None else ""
        claimed_at = str(row["claimed_at"] or "").strip() if row is not None else ""
        last_heartbeat_at = str(row["last_heartbeat_at"] or "").strip() if row is not None else ""
        client_holds_lock = bool(client_id and holder_client_id and holder_client_id == client_id)
        active_editor = (
            {
                "holder_label": holder_label,
                "claimed_at": claimed_at,
                "last_heartbeat_at": last_heartbeat_at,
            }
            if holder_label
            else None
        )
        return {
            "current_revision": int(current_revision or 0),
            "active_editor": active_editor,
            "lease_expires_at": lease_expires_at,
            "is_editing_elsewhere": bool(active_editor and not client_holds_lock),
            "client_holds_lock": client_holds_lock,
        }

    def get_concurrency(
        self,
        *,
        building: str,
        session_id: str,
        current_revision: int,
        client_id: str = "",
    ) -> Dict[str, Any]:
        self.ensure_ready()
        with self.connect(read_only=True) as conn:
            row = conn.execute(
                """
                SELECT *
                  FROM review_session_locks
                 WHERE lock_key = ?
                   AND lease_expires_at > ?
                """,
                (self._lock_key(building, session_id), _now_text()),
            ).fetchone()
        return self._row_to_concurrency(
            row,
            current_revision=current_revision,
            client_id=str(client_id or "").strip(),
        )

    def claim_lock(
        self,
        *,
        building: str,
        session_id: str,
        current_revision: int,
        client_id: str,
        holder_label: str = "",
        lease_ttl_sec: int = 60,
    ) -> Dict[str, Any]:
        normalized_client_id = str(client_id or "").strip()
        if not normalized_client_id:
            raise ValueError("client_id 不能为空")
        label = str(holder_label or "").strip() or _default_holder_label(normalized_client_id)
        now_text = _now_text()
        expires_at = _add_seconds_text(now_text, lease_ttl_sec)
        lock_key = self._lock_key(building, session_id)
        self.ensure_ready()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM review_session_locks WHERE lease_expires_at <= ?",
                (now_text,),
            )
            current = conn.execute(
                "SELECT * FROM review_session_locks WHERE lock_key = ?",
                (lock_key,),
            ).fetchone()
            if current is not None:
                existing_holder = str(current["holder_client_id"] or "").strip()
                if existing_holder and existing_holder != normalized_client_id:
                    return self._row_to_concurrency(
                        current,
                        current_revision=current_revision,
                        client_id=normalized_client_id,
                    ) | {"acquired": False}
            conn.execute(
                """
                INSERT INTO review_session_locks(
                    lock_key,
                    building,
                    session_id,
                    holder_client_id,
                    holder_label,
                    claimed_at,
                    last_heartbeat_at,
                    lease_expires_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(lock_key) DO UPDATE SET
                    holder_client_id = excluded.holder_client_id,
                    holder_label = excluded.holder_label,
                    claimed_at = excluded.claimed_at,
                    last_heartbeat_at = excluded.last_heartbeat_at,
                    lease_expires_at = excluded.lease_expires_at
                """,
                (
                    lock_key,
                    str(building or "").strip(),
                    str(session_id or "").strip(),
                    normalized_client_id,
                    label,
                    now_text,
                    now_text,
                    expires_at,
                ),
            )
            claimed = conn.execute(
                "SELECT * FROM review_session_locks WHERE lock_key = ?",
                (lock_key,),
            ).fetchone()
        return self._row_to_concurrency(
            claimed,
            current_revision=current_revision,
            client_id=normalized_client_id,
        ) | {"acquired": True}

    def heartbeat_lock(
        self,
        *,
        building: str,
        session_id: str,
        current_revision: int,
        client_id: str,
        lease_ttl_sec: int = 60,
    ) -> Dict[str, Any]:
        normalized_client_id = str(client_id or "").strip()
        if not normalized_client_id:
            raise ValueError("client_id 不能为空")
        now_text = _now_text()
        expires_at = _add_seconds_text(now_text, lease_ttl_sec)
        lock_key = self._lock_key(building, session_id)
        self.ensure_ready()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM review_session_locks WHERE lease_expires_at <= ?",
                (now_text,),
            )
            current = conn.execute(
                "SELECT * FROM review_session_locks WHERE lock_key = ?",
                (lock_key,),
            ).fetchone()
            if current is not None and str(current["holder_client_id"] or "").strip() == normalized_client_id:
                conn.execute(
                    """
                    UPDATE review_session_locks
                       SET last_heartbeat_at = ?,
                           lease_expires_at = ?
                     WHERE lock_key = ?
                    """,
                    (now_text, expires_at, lock_key),
                )
            current = conn.execute(
                """
                SELECT *
                  FROM review_session_locks
                 WHERE lock_key = ?
                   AND lease_expires_at > ?
                """,
                (lock_key, now_text),
            ).fetchone()
        return self._row_to_concurrency(
            current,
            current_revision=current_revision,
            client_id=normalized_client_id,
        ) | {"renewed": bool(current is not None and str(current["holder_client_id"] or "").strip() == normalized_client_id)}

    def release_lock(
        self,
        *,
        building: str,
        session_id: str,
        current_revision: int,
        client_id: str,
    ) -> Dict[str, Any]:
        normalized_client_id = str(client_id or "").strip()
        lock_key = self._lock_key(building, session_id)
        self.ensure_ready()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            released = conn.execute(
                "DELETE FROM review_session_locks WHERE lock_key = ? AND holder_client_id = ?",
                (lock_key, normalized_client_id),
            ).rowcount > 0
            current = conn.execute(
                """
                SELECT *
                  FROM review_session_locks
                 WHERE lock_key = ?
                   AND lease_expires_at > ?
                """,
                (lock_key, _now_text()),
            ).fetchone()
        return self._row_to_concurrency(
            current,
            current_revision=current_revision,
            client_id=normalized_client_id,
        ) | {"released": released}
