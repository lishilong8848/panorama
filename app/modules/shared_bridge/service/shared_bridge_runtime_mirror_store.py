from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root


def _json_loads(raw: Any) -> Any:
    try:
        return json.loads(str(raw or ""))
    except Exception:
        return {}


class SharedBridgeRuntimeMirrorStore:
    def __init__(self, *, runtime_config: Dict[str, Any], role_mode: str) -> None:
        runtime_root = resolve_runtime_state_root(runtime_config=runtime_config)
        db_name = "shared_bridge_internal.db" if str(role_mode or "").strip().lower() == "internal" else "shared_bridge_external.db"
        self.db_path = runtime_root / db_name
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_ready()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=5.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _ensure_ready(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS task_snapshots (
                    task_id TEXT PRIMARY KEY,
                    updated_at TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_task_snapshots_updated_at ON task_snapshots(updated_at DESC, task_id DESC);
                CREATE TABLE IF NOT EXISTS kv_snapshots (
                    snapshot_key TEXT PRIMARY KEY,
                    updated_at TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );
                """
            )

    def upsert_task(self, task: Dict[str, Any]) -> None:
        task_id = str(task.get("task_id", "") or "").strip()
        if not task_id:
            return
        updated_at = str(task.get("updated_at", "") or "").strip() or str(task.get("created_at", "") or "").strip()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO task_snapshots(task_id, updated_at, payload_json)
                VALUES(?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    updated_at=excluded.updated_at,
                    payload_json=excluded.payload_json
                """,
                (task_id, updated_at, json.dumps(task, ensure_ascii=False)),
            )

    def delete_task(self, task_id: str) -> None:
        task_text = str(task_id or "").strip()
        if not task_text:
            return
        with self._connect() as conn:
            conn.execute("DELETE FROM task_snapshots WHERE task_id=?", (task_text,))

    def list_tasks(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM task_snapshots
                ORDER BY updated_at DESC, task_id DESC
                LIMIT ?
                """,
                (max(1, int(limit or 100)),),
            ).fetchall()
        output: List[Dict[str, Any]] = []
        for row in rows:
            payload = _json_loads(row["payload_json"])
            if isinstance(payload, dict):
                output.append(payload)
        return output

    def get_task(self, task_id: str) -> Dict[str, Any] | None:
        task_text = str(task_id or "").strip()
        if not task_text:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM task_snapshots WHERE task_id=?",
                (task_text,),
            ).fetchone()
        if not row:
            return None
        payload = _json_loads(row["payload_json"])
        return payload if isinstance(payload, dict) else None

    def set_snapshot(self, *, key: str, payload: Dict[str, Any]) -> None:
        key_text = str(key or "").strip()
        if not key_text:
            return
        updated_at = str(payload.get("updated_at", "") or "").strip()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO kv_snapshots(snapshot_key, updated_at, payload_json)
                VALUES(?, ?, ?)
                ON CONFLICT(snapshot_key) DO UPDATE SET
                    updated_at=excluded.updated_at,
                    payload_json=excluded.payload_json
                """,
                (key_text, updated_at, json.dumps(payload, ensure_ascii=False)),
            )

    def get_snapshot(self, *, key: str) -> Dict[str, Any] | None:
        key_text = str(key or "").strip()
        if not key_text:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM kv_snapshots WHERE snapshot_key=?",
                (key_text,),
            ).fetchone()
        if not row:
            return None
        payload = _json_loads(row["payload_json"])
        return payload if isinstance(payload, dict) else None
