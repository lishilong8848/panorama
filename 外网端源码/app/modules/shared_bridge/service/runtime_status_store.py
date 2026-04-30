from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator


def _now_text() -> str:
    from datetime import datetime

    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class RuntimeStatusStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_lock = threading.Lock()
        self._ready_event = threading.Event()
        self._initialized = False

    @contextmanager
    def connect(self, *, read_only: bool = False) -> Iterator[sqlite3.Connection]:
        if read_only:
            if not self.path.exists():
                raise FileNotFoundError(str(self.path))
        conn = sqlite3.connect(str(self.path), timeout=5.0, isolation_level=None, check_same_thread=False)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA foreign_keys=ON")
            if not read_only:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
            yield conn
        finally:
            conn.close()

    def ensure_ready(self) -> None:
        if self._initialized and self.path.exists() and self._ready_event.is_set():
            return
        with self._init_lock:
            if self._initialized and self.path.exists() and self._ready_event.is_set():
                return
            self._ready_event.clear()
            with self.connect(read_only=False) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS status_snapshots (
                        scope TEXT PRIMARY KEY,
                        payload_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        seq INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS building_status_snapshots (
                        building TEXT PRIMARY KEY,
                        payload_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        seq INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
            self._initialized = True
            self._ready_event.set()

    def _read_snapshot(self, *, key_name: str, table_name: str, key_value: str) -> Dict[str, Any] | None:
        for attempt in range(2):
            try:
                self.ensure_ready()
                with self.connect(read_only=True) as conn:
                    row = conn.execute(
                        f"SELECT {key_name}, payload_json, updated_at, seq FROM {table_name} WHERE {key_name} = ?",
                        (key_value,),
                    ).fetchone()
                return self._row_to_record(row, key_name=key_name) if row else None
            except FileNotFoundError:
                return None
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc).lower() or attempt > 0:
                    raise
                self._initialized = False
                self._ready_event.clear()
        return None

    def write_scope_snapshot(self, scope: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.ensure_ready()
        scope_text = str(scope or "").strip()
        if not scope_text:
            raise ValueError("scope is required")
        payload_text = json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False)
        updated_at = _now_text()
        with self.connect(read_only=False) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO status_snapshots(scope, payload_json, updated_at, seq)
                    VALUES (?, ?, ?, 1)
                    ON CONFLICT(scope) DO UPDATE SET
                        payload_json = excluded.payload_json,
                        updated_at = excluded.updated_at,
                        seq = status_snapshots.seq + 1
                    """,
                    (scope_text, payload_text, updated_at),
                )
                row = conn.execute(
                    "SELECT scope, payload_json, updated_at, seq FROM status_snapshots WHERE scope = ?",
                    (scope_text,),
                ).fetchone()
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self._row_to_record(row, key_name="scope")

    def write_building_snapshot(self, building: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.ensure_ready()
        building_text = str(building or "").strip()
        if not building_text:
            raise ValueError("building is required")
        payload_text = json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False)
        updated_at = _now_text()
        with self.connect(read_only=False) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO building_status_snapshots(building, payload_json, updated_at, seq)
                    VALUES (?, ?, ?, 1)
                    ON CONFLICT(building) DO UPDATE SET
                        payload_json = excluded.payload_json,
                        updated_at = excluded.updated_at,
                        seq = building_status_snapshots.seq + 1
                    """,
                    (building_text, payload_text, updated_at),
                )
                row = conn.execute(
                    "SELECT building, payload_json, updated_at, seq FROM building_status_snapshots WHERE building = ?",
                    (building_text,),
                ).fetchone()
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self._row_to_record(row, key_name="building")

    def read_scope_snapshot(self, scope: str) -> Dict[str, Any] | None:
        scope_text = str(scope or "").strip()
        if not scope_text:
            return None
        return self._read_snapshot(key_name="scope", table_name="status_snapshots", key_value=scope_text)

    def read_building_snapshot(self, building: str) -> Dict[str, Any] | None:
        building_text = str(building or "").strip()
        if not building_text:
            return None
        return self._read_snapshot(
            key_name="building",
            table_name="building_status_snapshots",
            key_value=building_text,
        )

    @staticmethod
    def _row_to_record(row: sqlite3.Row | None, *, key_name: str) -> Dict[str, Any]:
        if row is None:
            return {}
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return {
            key_name: str(row[key_name] or "").strip(),
            "payload": payload,
            "updated_at": str(row["updated_at"] or "").strip(),
            "seq": int(row["seq"] or 0),
        }
