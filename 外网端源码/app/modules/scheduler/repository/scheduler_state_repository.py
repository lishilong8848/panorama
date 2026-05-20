from __future__ import annotations

import copy
import json
import sqlite3
import threading
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping


_LOCKS_GUARD = threading.Lock()
_LOCKS: dict[str, threading.RLock] = {}


def _lock_for_path(path: Path) -> threading.RLock:
    key = str(path.resolve(strict=False)).casefold()
    with _LOCKS_GUARD:
        lock = _LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _LOCKS[key] = lock
        return lock


class SchedulerStateRepository:
    """SQLite-backed scheduler state store.

    The public key is still the legacy JSON state path so callers and UI do not
    need to change. Existing JSON files are imported on first read, but new
    writes go to SQLite only.
    """

    _TABLE_NAME = "scheduler_state"

    def _db_path(self, state_path: Path) -> Path:
        return state_path.parent / "scheduler_runtime_state.sqlite3"

    def _state_key(self, state_path: Path) -> str:
        return str(state_path.resolve(strict=False)).casefold()

    def _connect(self, db_path: Path) -> sqlite3.Connection:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            pass
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._TABLE_NAME} (
                state_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        return conn

    def _read_legacy_json(self, path: Path) -> Dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            obj = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(obj, dict):
            return None
        return obj

    def _merge_default(self, payload: Mapping[str, Any] | None, default: Mapping[str, Any]) -> Dict[str, Any]:
        state = copy.deepcopy(dict(default))
        if not isinstance(payload, Mapping):
            return state
        for key, value in payload.items():
            state[key] = copy.deepcopy(value)
        return state

    def _save_with_connection(
        self,
        conn: sqlite3.Connection,
        *,
        state_key: str,
        state: Mapping[str, Any],
    ) -> None:
        conn.execute(
            f"""
            INSERT INTO {self._TABLE_NAME} (state_key, payload, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(state_key) DO UPDATE SET
                payload = excluded.payload,
                updated_at = excluded.updated_at
            """,
            (
                state_key,
                json.dumps(dict(state), ensure_ascii=False, separators=(",", ":")),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.commit()

    def load(self, path: Path, default: Mapping[str, Any] | None = None) -> Dict[str, Any]:
        defaults = dict(default or {})
        db_path = self._db_path(path)
        lock = _lock_for_path(db_path)
        with lock:
            try:
                with closing(self._connect(db_path)) as conn:
                    row = conn.execute(
                        f"SELECT payload FROM {self._TABLE_NAME} WHERE state_key = ?",
                        (self._state_key(path),),
                    ).fetchone()
                    if row:
                        try:
                            payload = json.loads(str(row[0] or "{}"))
                        except json.JSONDecodeError:
                            payload = {}
                        return self._merge_default(payload if isinstance(payload, dict) else {}, defaults)

                    legacy_payload = self._read_legacy_json(path)
                    if legacy_payload is not None:
                        migrated = self._merge_default(legacy_payload, defaults)
                        self._save_with_connection(conn, state_key=self._state_key(path), state=migrated)
                        return migrated
            except Exception:  # noqa: BLE001
                legacy_payload = self._read_legacy_json(path)
                if legacy_payload is not None:
                    return self._merge_default(legacy_payload, defaults)
        return copy.deepcopy(defaults)

    def save(self, path: Path, state: Mapping[str, Any]) -> None:
        db_path = self._db_path(path)
        lock = _lock_for_path(db_path)
        with lock:
            with closing(self._connect(db_path)) as conn:
                self._save_with_connection(conn, state_key=self._state_key(path), state=state)

    def exists(self, path: Path) -> bool:
        db_path = self._db_path(path)
        if path.exists():
            return True
        if not db_path.exists():
            return False
        lock = _lock_for_path(db_path)
        with lock:
            try:
                with closing(self._connect(db_path)) as conn:
                    row = conn.execute(
                        f"SELECT 1 FROM {self._TABLE_NAME} WHERE state_key = ? LIMIT 1",
                        (self._state_key(path),),
                    ).fetchone()
                    return bool(row)
            except Exception:  # noqa: BLE001
                return False
