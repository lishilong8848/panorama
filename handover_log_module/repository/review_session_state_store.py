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
        "review_latest_batch_key": "",
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

            CREATE TABLE IF NOT EXISTS review_shared_blocks (
                block_key TEXT PRIMARY KEY,
                batch_key TEXT NOT NULL,
                block_id TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT '',
                updated_by_building TEXT NOT NULL DEFAULT '',
                updated_by_client TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_review_shared_blocks_batch
                ON review_shared_blocks(batch_key, block_id);

            CREATE TABLE IF NOT EXISTS review_shared_block_locks (
                lock_key TEXT PRIMARY KEY,
                batch_key TEXT NOT NULL,
                block_id TEXT NOT NULL,
                holder_building TEXT NOT NULL DEFAULT '',
                holder_client_id TEXT NOT NULL DEFAULT '',
                holder_label TEXT NOT NULL DEFAULT '',
                claimed_at TEXT NOT NULL DEFAULT '',
                last_heartbeat_at TEXT NOT NULL DEFAULT '',
                lease_expires_at TEXT NOT NULL DEFAULT '',
                dirty INTEGER NOT NULL DEFAULT 0,
                dirty_at TEXT NOT NULL DEFAULT '',
                dirty_by_building TEXT NOT NULL DEFAULT '',
                dirty_by_client TEXT NOT NULL DEFAULT '',
                dirty_payload_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_review_shared_block_locks_batch
                ON review_shared_block_locks(batch_key, block_id);
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
        state["review_latest_batch_key"] = str(raw.get("review_latest_batch_key", "")).strip()
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
        conn.execute(
            "INSERT OR REPLACE INTO review_meta(key, value) VALUES('review_latest_batch_key', ?)",
            (str(payload.get("review_latest_batch_key", "")).strip(),),
        )

    @staticmethod
    def _set_meta_value(conn: sqlite3.Connection, *, key: str, value: str) -> None:
        conn.execute(
            "INSERT OR REPLACE INTO review_meta(key, value) VALUES(?, ?)",
            (str(key or "").strip(), str(value or "").strip()),
        )

    @staticmethod
    def _upsert_session_row(conn: sqlite3.Connection, session: Dict[str, Any]) -> str:
        session_id = str(session.get("session_id", "") or "").strip()
        if not session_id:
            return ""
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
            ON CONFLICT(session_id) DO UPDATE SET
                building=excluded.building,
                duty_date=excluded.duty_date,
                duty_shift=excluded.duty_shift,
                batch_key=excluded.batch_key,
                updated_at=excluded.updated_at,
                payload_json=excluded.payload_json
            """,
            (
                session_id,
                str(session.get("building", "")).strip(),
                str(session.get("duty_date", "")).strip(),
                str(session.get("duty_shift", "")).strip().lower(),
                str(session.get("batch_key", "")).strip(),
                str(session.get("updated_at", "")).strip(),
                json.dumps(session, ensure_ascii=False),
            ),
        )
        return session_id

    @staticmethod
    def _upsert_cloud_batch_row(conn: sqlite3.Connection, batch: Dict[str, Any]) -> str:
        batch_key = str(batch.get("batch_key", "") or "").strip()
        if not batch_key:
            return ""
        conn.execute(
            """
            INSERT INTO review_cloud_batches(
                batch_key,
                duty_date,
                duty_shift,
                updated_at,
                payload_json
            ) VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(batch_key) DO UPDATE SET
                duty_date=excluded.duty_date,
                duty_shift=excluded.duty_shift,
                updated_at=excluded.updated_at,
                payload_json=excluded.payload_json
            """,
            (
                batch_key,
                str(batch.get("duty_date", "")).strip(),
                str(batch.get("duty_shift", "")).strip().lower(),
                str(batch.get("updated_at", "")).strip(),
                json.dumps(batch, ensure_ascii=False),
            ),
        )
        return batch_key

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
            latest_batch_row = conn.execute(
                "SELECT value FROM review_meta WHERE key = 'review_latest_batch_key'"
            ).fetchone()
            updated_at = str(meta_row["value"] or "").strip() if meta_row is not None else ""
            state = self._normalize_state(
                {
                    "review_sessions": sessions,
                    "review_latest_by_building": latest,
                    "review_cloud_batches": cloud_batches,
                    "updated_at": updated_at,
                }
            )
            state["review_latest_batch_key"] = (
                str(latest_batch_row["value"] or "").strip() if latest_batch_row is not None else ""
            )
            return state

    def save_state(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.ensure_ready()
        state = self._normalize_state(payload)
        state["updated_at"] = _now_text()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._write_state(conn, state)
        return state

    def apply_changes(
        self,
        *,
        upsert_sessions: list[Dict[str, Any]] | None = None,
        delete_session_ids: list[str] | None = None,
        latest_by_building: Dict[str, str | None] | None = None,
        upsert_cloud_batches: list[Dict[str, Any]] | None = None,
        delete_cloud_batch_keys: list[str] | None = None,
        meta_updates: Dict[str, str | None] | None = None,
    ) -> Dict[str, Any]:
        self.ensure_ready()
        updated_at = _now_text()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for session in upsert_sessions or []:
                if isinstance(session, dict):
                    self._upsert_session_row(conn, session)
            for session_id in delete_session_ids or []:
                session_text = str(session_id or "").strip()
                if session_text:
                    conn.execute("DELETE FROM review_sessions WHERE session_id=?", (session_text,))
            for building, session_id in (latest_by_building or {}).items():
                building_text = str(building or "").strip()
                if not building_text:
                    continue
                session_text = str(session_id or "").strip()
                if session_text:
                    conn.execute(
                        """
                        INSERT INTO review_latest_by_building(building, session_id)
                        VALUES(?, ?)
                        ON CONFLICT(building) DO UPDATE SET session_id=excluded.session_id
                        """,
                        (building_text, session_text),
                    )
                else:
                    conn.execute("DELETE FROM review_latest_by_building WHERE building=?", (building_text,))
            for batch in upsert_cloud_batches or []:
                if isinstance(batch, dict):
                    self._upsert_cloud_batch_row(conn, batch)
            for batch_key in delete_cloud_batch_keys or []:
                batch_text = str(batch_key or "").strip()
                if batch_text:
                    conn.execute("DELETE FROM review_cloud_batches WHERE batch_key=?", (batch_text,))
            for key, value in (meta_updates or {}).items():
                key_text = str(key or "").strip()
                if not key_text:
                    continue
                if value is None:
                    conn.execute("DELETE FROM review_meta WHERE key=?", (key_text,))
                else:
                    self._set_meta_value(conn, key=key_text, value=str(value or "").strip())
            self._set_meta_value(conn, key="updated_at", value=updated_at)
        state = self.load_state()
        state["updated_at"] = updated_at
        return state

    def upsert_session(self, session: Dict[str, Any]) -> Dict[str, Any]:
        return self.apply_changes(upsert_sessions=[session] if isinstance(session, dict) else [])

    def delete_session(self, session_id: str) -> Dict[str, Any]:
        return self.apply_changes(delete_session_ids=[session_id])

    def upsert_cloud_batch(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        return self.apply_changes(upsert_cloud_batches=[batch] if isinstance(batch, dict) else [])

    def delete_cloud_batch(self, batch_key: str) -> Dict[str, Any]:
        return self.apply_changes(delete_cloud_batch_keys=[batch_key])

    def set_latest_by_building(self, *, building: str, session_id: str | None) -> Dict[str, Any]:
        return self.apply_changes(latest_by_building={str(building or "").strip(): session_id})

    def set_latest_batch_key(self, batch_key: str | None) -> Dict[str, Any]:
        return self.apply_changes(meta_updates={"review_latest_batch_key": batch_key})

    @staticmethod
    def _lock_key(building: str, session_id: str) -> str:
        return f"{str(building or '').strip()}::{str(session_id or '').strip()}"

    @staticmethod
    def _shared_block_key(batch_key: str, block_id: str) -> str:
        return f"{str(batch_key or '').strip()}::{str(block_id or '').strip()}"

    @staticmethod
    def _loads_json_object(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        text = str(value or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:  # noqa: BLE001
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _row_to_shared_block(
        self,
        row: sqlite3.Row | None,
        *,
        batch_key: str,
        block_id: str,
    ) -> Dict[str, Any]:
        if row is None:
            return {
                "batch_key": str(batch_key or "").strip(),
                "block_id": str(block_id or "").strip(),
                "revision": 0,
                "updated_at": "",
                "updated_by_building": "",
                "updated_by_client": "",
                "payload": {},
            }
        return {
            "batch_key": str(row["batch_key"] or "").strip(),
            "block_id": str(row["block_id"] or "").strip(),
            "revision": int(row["revision"] or 0),
            "updated_at": str(row["updated_at"] or "").strip(),
            "updated_by_building": str(row["updated_by_building"] or "").strip(),
            "updated_by_client": str(row["updated_by_client"] or "").strip(),
            "payload": self._loads_json_object(row["payload_json"]),
        }

    def _row_to_shared_lock(
        self,
        row: sqlite3.Row | None,
        *,
        current_revision: int,
        client_id: str,
    ) -> Dict[str, Any]:
        holder_client_id = str(row["holder_client_id"] or "").strip() if row is not None else ""
        holder_label = str(row["holder_label"] or "").strip() if row is not None else ""
        holder_building = str(row["holder_building"] or "").strip() if row is not None else ""
        lease_expires_at = str(row["lease_expires_at"] or "").strip() if row is not None else ""
        claimed_at = str(row["claimed_at"] or "").strip() if row is not None else ""
        last_heartbeat_at = str(row["last_heartbeat_at"] or "").strip() if row is not None else ""
        client_holds_lock = bool(client_id and holder_client_id and holder_client_id == client_id)
        active_editor = (
            {
                "holder_label": holder_label,
                "holder_building": holder_building,
                "claimed_at": claimed_at,
                "last_heartbeat_at": last_heartbeat_at,
            }
            if holder_label or holder_building
            else None
        )
        dirty = bool(int(row["dirty"] or 0)) if row is not None else False
        return {
            "current_revision": int(current_revision or 0),
            "active_editor": active_editor,
            "lease_expires_at": lease_expires_at,
            "is_editing_elsewhere": bool(active_editor and not client_holds_lock),
            "client_holds_lock": client_holds_lock,
            "dirty": dirty,
            "dirty_at": str(row["dirty_at"] or "").strip() if row is not None and dirty else "",
            "dirty_by_building": str(row["dirty_by_building"] or "").strip() if row is not None and dirty else "",
            "dirty_by_client": str(row["dirty_by_client"] or "").strip() if row is not None and dirty else "",
            "dirty_payload": self._loads_json_object(row["dirty_payload_json"]) if row is not None and dirty else {},
        }

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

    def get_shared_block(
        self,
        *,
        batch_key: str,
        block_id: str,
    ) -> Dict[str, Any]:
        batch_key_text = str(batch_key or "").strip()
        block_id_text = str(block_id or "").strip()
        if not batch_key_text or not block_id_text:
            return self._row_to_shared_block(None, batch_key=batch_key_text, block_id=block_id_text)
        self.ensure_ready()
        with self.connect(read_only=True) as conn:
            row = conn.execute(
                """
                SELECT *
                  FROM review_shared_blocks
                 WHERE block_key = ?
                """,
                (self._shared_block_key(batch_key_text, block_id_text),),
            ).fetchone()
        return self._row_to_shared_block(row, batch_key=batch_key_text, block_id=block_id_text)

    def get_shared_block_lock(
        self,
        *,
        batch_key: str,
        block_id: str,
        current_revision: int,
        client_id: str = "",
    ) -> Dict[str, Any]:
        batch_key_text = str(batch_key or "").strip()
        block_id_text = str(block_id or "").strip()
        normalized_client_id = str(client_id or "").strip()
        if not batch_key_text or not block_id_text:
            return self._row_to_shared_lock(
                None,
                current_revision=current_revision,
                client_id=normalized_client_id,
            )
        self.ensure_ready()
        with self.connect(read_only=True) as conn:
            row = conn.execute(
                """
                SELECT *
                  FROM review_shared_block_locks
                 WHERE lock_key = ?
                   AND lease_expires_at > ?
                """,
                (self._shared_block_key(batch_key_text, block_id_text), _now_text()),
            ).fetchone()
        return self._row_to_shared_lock(
            row,
            current_revision=current_revision,
            client_id=normalized_client_id,
        )

    def claim_shared_block_lock(
        self,
        *,
        batch_key: str,
        block_id: str,
        building: str,
        client_id: str,
        holder_label: str = "",
        current_revision: int = 0,
        lease_ttl_sec: int = 60,
    ) -> Dict[str, Any]:
        batch_key_text = str(batch_key or "").strip()
        block_id_text = str(block_id or "").strip()
        normalized_client_id = str(client_id or "").strip()
        if not batch_key_text:
            raise ValueError("batch_key 不能为空")
        if not block_id_text:
            raise ValueError("block_id 不能为空")
        if not normalized_client_id:
            raise ValueError("client_id 不能为空")
        building_text = str(building or "").strip()
        label = str(holder_label or "").strip() or _default_holder_label(normalized_client_id)
        now_text = _now_text()
        expires_at = _add_seconds_text(now_text, lease_ttl_sec)
        lock_key = self._shared_block_key(batch_key_text, block_id_text)
        self.ensure_ready()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM review_shared_block_locks WHERE lease_expires_at <= ?",
                (now_text,),
            )
            current = conn.execute(
                "SELECT * FROM review_shared_block_locks WHERE lock_key = ?",
                (lock_key,),
            ).fetchone()
            if current is not None:
                existing_holder = str(current["holder_client_id"] or "").strip()
                if existing_holder and existing_holder != normalized_client_id:
                    return self._row_to_shared_lock(
                        current,
                        current_revision=current_revision,
                        client_id=normalized_client_id,
                    ) | {"acquired": False}
            conn.execute(
                """
                INSERT INTO review_shared_block_locks(
                    lock_key,
                    batch_key,
                    block_id,
                    holder_building,
                    holder_client_id,
                    holder_label,
                    claimed_at,
                    last_heartbeat_at,
                    lease_expires_at,
                    dirty,
                    dirty_at,
                    dirty_by_building,
                    dirty_by_client,
                    dirty_payload_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '', '', '', '{}')
                ON CONFLICT(lock_key) DO UPDATE SET
                    holder_building = excluded.holder_building,
                    holder_client_id = excluded.holder_client_id,
                    holder_label = excluded.holder_label,
                    claimed_at = excluded.claimed_at,
                    last_heartbeat_at = excluded.last_heartbeat_at,
                    lease_expires_at = excluded.lease_expires_at
                """,
                (
                    lock_key,
                    batch_key_text,
                    block_id_text,
                    building_text,
                    normalized_client_id,
                    label,
                    now_text,
                    now_text,
                    expires_at,
                ),
            )
            claimed = conn.execute(
                "SELECT * FROM review_shared_block_locks WHERE lock_key = ?",
                (lock_key,),
            ).fetchone()
        return self._row_to_shared_lock(
            claimed,
            current_revision=current_revision,
            client_id=normalized_client_id,
        ) | {"acquired": True}

    def heartbeat_shared_block_lock(
        self,
        *,
        batch_key: str,
        block_id: str,
        client_id: str,
        current_revision: int = 0,
        lease_ttl_sec: int = 60,
    ) -> Dict[str, Any]:
        batch_key_text = str(batch_key or "").strip()
        block_id_text = str(block_id or "").strip()
        normalized_client_id = str(client_id or "").strip()
        if not normalized_client_id:
            raise ValueError("client_id 不能为空")
        now_text = _now_text()
        expires_at = _add_seconds_text(now_text, lease_ttl_sec)
        lock_key = self._shared_block_key(batch_key_text, block_id_text)
        self.ensure_ready()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM review_shared_block_locks WHERE lease_expires_at <= ?",
                (now_text,),
            )
            current = conn.execute(
                "SELECT * FROM review_shared_block_locks WHERE lock_key = ?",
                (lock_key,),
            ).fetchone()
            renewed = bool(
                current is not None
                and str(current["holder_client_id"] or "").strip() == normalized_client_id
            )
            if renewed:
                conn.execute(
                    """
                    UPDATE review_shared_block_locks
                       SET last_heartbeat_at = ?,
                           lease_expires_at = ?
                     WHERE lock_key = ?
                    """,
                    (now_text, expires_at, lock_key),
                )
            current = conn.execute(
                """
                SELECT *
                  FROM review_shared_block_locks
                 WHERE lock_key = ?
                   AND lease_expires_at > ?
                """,
                (lock_key, now_text),
            ).fetchone()
        return self._row_to_shared_lock(
            current,
            current_revision=current_revision,
            client_id=normalized_client_id,
        ) | {"renewed": renewed}

    def release_shared_block_lock(
        self,
        *,
        batch_key: str,
        block_id: str,
        client_id: str,
        current_revision: int = 0,
    ) -> Dict[str, Any]:
        batch_key_text = str(batch_key or "").strip()
        block_id_text = str(block_id or "").strip()
        normalized_client_id = str(client_id or "").strip()
        lock_key = self._shared_block_key(batch_key_text, block_id_text)
        self.ensure_ready()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            released = conn.execute(
                "DELETE FROM review_shared_block_locks WHERE lock_key = ? AND holder_client_id = ?",
                (lock_key, normalized_client_id),
            ).rowcount > 0
            current = conn.execute(
                """
                SELECT *
                  FROM review_shared_block_locks
                 WHERE lock_key = ?
                   AND lease_expires_at > ?
                """,
                (lock_key, _now_text()),
            ).fetchone()
        return self._row_to_shared_lock(
            current,
            current_revision=current_revision,
            client_id=normalized_client_id,
        ) | {"released": released}

    def mark_shared_block_dirty(
        self,
        *,
        batch_key: str,
        block_id: str,
        building: str,
        client_id: str,
        payload: Dict[str, Any],
        current_revision: int = 0,
    ) -> Dict[str, Any]:
        batch_key_text = str(batch_key or "").strip()
        block_id_text = str(block_id or "").strip()
        building_text = str(building or "").strip()
        normalized_client_id = str(client_id or "").strip()
        if not normalized_client_id:
            raise ValueError("client_id 不能为空")
        now_text = _now_text()
        lock_key = self._shared_block_key(batch_key_text, block_id_text)
        self.ensure_ready()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM review_shared_block_locks WHERE lease_expires_at <= ?",
                (now_text,),
            )
            current = conn.execute(
                "SELECT * FROM review_shared_block_locks WHERE lock_key = ?",
                (lock_key,),
            ).fetchone()
            if current is None or str(current["holder_client_id"] or "").strip() != normalized_client_id:
                return self._row_to_shared_lock(
                    current,
                    current_revision=current_revision,
                    client_id=normalized_client_id,
                ) | {"dirty_marked": False}
            conn.execute(
                """
                UPDATE review_shared_block_locks
                   SET dirty = 1,
                       dirty_at = ?,
                       dirty_by_building = ?,
                       dirty_by_client = ?,
                       dirty_payload_json = ?
                 WHERE lock_key = ?
                """,
                (
                    now_text,
                    building_text,
                    normalized_client_id,
                    json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False),
                    lock_key,
                ),
            )
            current = conn.execute(
                "SELECT * FROM review_shared_block_locks WHERE lock_key = ?",
                (lock_key,),
            ).fetchone()
        return self._row_to_shared_lock(
            current,
            current_revision=current_revision,
            client_id=normalized_client_id,
        ) | {"dirty_marked": True}

    def save_shared_block(
        self,
        *,
        batch_key: str,
        block_id: str,
        building: str,
        client_id: str,
        payload: Dict[str, Any],
        base_revision: int | None = None,
    ) -> Dict[str, Any]:
        batch_key_text = str(batch_key or "").strip()
        block_id_text = str(block_id or "").strip()
        building_text = str(building or "").strip()
        normalized_client_id = str(client_id or "").strip()
        if not normalized_client_id:
            raise ValueError("client_id 不能为空")
        now_text = _now_text()
        block_key = self._shared_block_key(batch_key_text, block_id_text)
        payload_json = json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False, sort_keys=True)
        self.ensure_ready()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM review_shared_block_locks WHERE lease_expires_at <= ?",
                (now_text,),
            )
            lock_row = conn.execute(
                "SELECT * FROM review_shared_block_locks WHERE lock_key = ?",
                (block_key,),
            ).fetchone()
            if lock_row is None or str(lock_row["holder_client_id"] or "").strip() != normalized_client_id:
                raise ValueError("110KV变电站正在其他楼栋编辑，请稍后重试")
            block_row = conn.execute(
                "SELECT * FROM review_shared_blocks WHERE block_key = ?",
                (block_key,),
            ).fetchone()
            current = self._row_to_shared_block(block_row, batch_key=batch_key_text, block_id=block_id_text)
            current_revision = int(current.get("revision", 0) or 0)
            current_payload_json = json.dumps(
                current.get("payload", {}) if isinstance(current.get("payload", {}), dict) else {},
                ensure_ascii=False,
                sort_keys=True,
            )
            no_change = payload_json == current_payload_json
            if base_revision is not None and int(base_revision or 0) != current_revision and not no_change:
                raise ValueError("110KV变电站内容已被其他楼栋更新，请刷新后重试")
            if no_change:
                conn.execute(
                    """
                    UPDATE review_shared_block_locks
                       SET dirty = 0,
                           dirty_at = '',
                           dirty_by_building = '',
                           dirty_by_client = '',
                           dirty_payload_json = '{}'
                     WHERE lock_key = ?
                    """,
                    (block_key,),
                )
            else:
                next_revision = current_revision + 1
                conn.execute(
                    """
                    INSERT INTO review_shared_blocks(
                        block_key,
                        batch_key,
                        block_id,
                        revision,
                        updated_at,
                        updated_by_building,
                        updated_by_client,
                        payload_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(block_key) DO UPDATE SET
                        revision = excluded.revision,
                        updated_at = excluded.updated_at,
                        updated_by_building = excluded.updated_by_building,
                        updated_by_client = excluded.updated_by_client,
                        payload_json = excluded.payload_json
                    """,
                    (
                        block_key,
                        batch_key_text,
                        block_id_text,
                        next_revision,
                        now_text,
                        building_text,
                        normalized_client_id,
                        payload_json,
                    ),
                )
                conn.execute(
                    """
                    UPDATE review_shared_block_locks
                       SET dirty = 0,
                           dirty_at = '',
                           dirty_by_building = '',
                           dirty_by_client = '',
                           dirty_payload_json = '{}'
                     WHERE lock_key = ?
                    """,
                    (block_key,),
                )
            saved_row = conn.execute(
                "SELECT * FROM review_shared_blocks WHERE block_key = ?",
                (block_key,),
            ).fetchone()
            lock_row = conn.execute(
                """
                SELECT *
                  FROM review_shared_block_locks
                 WHERE lock_key = ?
                   AND lease_expires_at > ?
                """,
                (block_key, now_text),
            ).fetchone()
        block = self._row_to_shared_block(saved_row, batch_key=batch_key_text, block_id=block_id_text)
        lock = self._row_to_shared_lock(
            lock_row,
            current_revision=int(block.get("revision", 0) or 0),
            client_id=normalized_client_id,
        )
        return {"block": block, "lock": lock, "no_change": no_change}
