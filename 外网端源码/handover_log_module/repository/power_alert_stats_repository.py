from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator

from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from pipeline_utils import get_app_dir


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


class PowerAlertStatsRepository:
    """Persist daily over-threshold state for cross-day run counting."""

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

    @classmethod
    def from_config(cls, config: Dict[str, Any] | None) -> "PowerAlertStatsRepository":
        runtime_config = config if isinstance(config, dict) else {}
        return cls(runtime_config=runtime_config)

    @contextmanager
    def _connect(self, *, read_only: bool = False) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = sqlite3.connect(str(self.db_path), timeout=30.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=30000")
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
            with self._connect() as conn:
                self._create_schema(conn)
            self._ready = True

    @staticmethod
    def _create_schema(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS power_alert_daily_stats (
                table_key TEXT NOT NULL,
                business_date TEXT NOT NULL,
                object_key TEXT NOT NULL,
                threshold REAL NOT NULL DEFAULT 0,
                over_mask INTEGER NOT NULL DEFAULT 0,
                duration_hours INTEGER NOT NULL DEFAULT 0,
                run_count INTEGER NOT NULL DEFAULT 0,
                max_hour INTEGER NOT NULL DEFAULT 0,
                max_value REAL NOT NULL DEFAULT 0,
                end_over INTEGER NOT NULL DEFAULT 0,
                source_hash TEXT NOT NULL DEFAULT '',
                source_file TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(table_key, business_date, object_key)
            );
            CREATE INDEX IF NOT EXISTS idx_power_alert_daily_stats_date
                ON power_alert_daily_stats(table_key, business_date);
            CREATE INDEX IF NOT EXISTS idx_power_alert_daily_stats_object
                ON power_alert_daily_stats(table_key, object_key, business_date DESC);
            """
        )

    def get_end_over(self, *, table_key: str, business_date: str, object_key: str) -> bool | None:
        self.ensure_ready()
        with self._connect(read_only=True) as conn:
            row = conn.execute(
                """
                SELECT end_over
                FROM power_alert_daily_stats
                WHERE table_key=? AND business_date=? AND object_key=?
                """,
                (str(table_key or ""), str(business_date or ""), str(object_key or "")),
            ).fetchone()
        if row is None:
            return None
        return bool(int(row["end_over"] or 0))

    def upsert_stat(
        self,
        *,
        table_key: str,
        business_date: str,
        object_key: str,
        threshold: float,
        over_mask: int,
        duration_hours: int,
        run_count: int,
        max_hour: int,
        max_value: float,
        end_over: bool,
        source_hash: str = "",
        source_file: str = "",
        payload: Dict[str, Any] | None = None,
    ) -> None:
        self.ensure_ready()
        now = _now_text()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO power_alert_daily_stats(
                    table_key, business_date, object_key, threshold, over_mask,
                    duration_hours, run_count, max_hour, max_value, end_over,
                    source_hash, source_file, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(table_key, business_date, object_key) DO UPDATE SET
                    threshold=excluded.threshold,
                    over_mask=excluded.over_mask,
                    duration_hours=excluded.duration_hours,
                    run_count=excluded.run_count,
                    max_hour=excluded.max_hour,
                    max_value=excluded.max_value,
                    end_over=excluded.end_over,
                    source_hash=excluded.source_hash,
                    source_file=excluded.source_file,
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at
                """,
                (
                    str(table_key or ""),
                    str(business_date or ""),
                    str(object_key or ""),
                    float(threshold or 0),
                    int(over_mask or 0),
                    int(duration_hours or 0),
                    int(run_count or 0),
                    int(max_hour or 0),
                    float(max_value or 0),
                    1 if end_over else 0,
                    str(source_hash or ""),
                    str(source_file or ""),
                    _json_dumps(payload if isinstance(payload, dict) else {}),
                    now,
                ),
            )
