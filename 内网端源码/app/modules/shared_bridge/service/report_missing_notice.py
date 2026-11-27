from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager

from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root


_PENDING_TTL = 86400
_DEDUPE_TTL = 30 * 86400
_LEGACY_PARTIAL_NOTICE = "【报表缺测提醒】共享源文件已就绪\n%"


def _text(value: object, limit: int = 64) -> str:
    return " ".join(value[:limit * 2].split())[:limit] if isinstance(value, str) else ""


def enqueue_empty_report_notice(
    runtime_config: dict, *, family: str, report_label: str, building: str, window: str,
    duty_shift: str = "", query_start: str = "", query_end: str = "",
) -> None:
    identity = ["all_samples_missing", family, building, window, duty_shift]
    key = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
    window_label = " ".join(filter(None, (_text(window), _text(duty_shift))))
    lines = [
        "【报表缺测提醒】本窗口所有测点均无有效实测，未发布空报表",
        f"报表：{_text(report_label)}｜楼栋：{_text(building)}｜窗口：{window_label}",
    ]
    if query_start and query_end:
        lines.append(f"查询范围：{_text(query_start)} 至 {_text(query_end)}")
    try:
        ReportMissingNoticeOutbox(runtime_config).enqueue(key, "\n".join(lines))
    except Exception:
        logging.getLogger(__name__).warning("整表无数据提醒入队失败")


class ReportMissingNoticeOutbox:
    def __init__(self, runtime_config: dict) -> None:
        self.runtime_config = runtime_config

    @contextmanager
    def _connect(self):
        path = resolve_runtime_state_root(runtime_config=self.runtime_config) / "report_missing_notices.sqlite3"
        with closing(sqlite3.connect(path, timeout=0.3)) as conn, conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS report_missing_notices (
                    notice_id TEXT PRIMARY KEY, message TEXT, created_at REAL NOT NULL,
                    lease_token TEXT, lease_until REAL NOT NULL DEFAULT 0
                )"""
            )
            conn.execute("CREATE INDEX IF NOT EXISTS report_missing_notice_age ON report_missing_notices (created_at)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS report_missing_notice_pending ON report_missing_notices (created_at)"
                " WHERE message IS NOT NULL"
            )
            yield conn

    @staticmethod
    def _prune(conn: sqlite3.Connection, now: float) -> None:
        conn.execute("DELETE FROM report_missing_notices WHERE created_at <= ?", (now - _DEDUPE_TTL,))
        # Expired messages are not replayed after an outage; their keys still dedupe refreshes.
        conn.execute(
            "UPDATE report_missing_notices SET message = NULL WHERE message IS NOT NULL AND created_at <= ?",
            (now - _PENDING_TTL,),
        )
        # Partial-gap alerts from older builds are no longer delivered.
        conn.execute(
            "UPDATE report_missing_notices SET message = NULL WHERE message LIKE ?",
            (_LEGACY_PARTIAL_NOTICE,),
        )

    def enqueue(self, notice_id: str, message: str) -> None:
        self.enqueue_many([(notice_id, message)])

    def enqueue_many(self, notices: list[tuple[str, str]]) -> None:
        now = time.time()
        with self._connect() as conn:
            self._prune(conn, now)
            conn.executemany(
                "INSERT OR IGNORE INTO report_missing_notices (notice_id, message, created_at) VALUES (?, ?, ?)",
                ((notice_id, message, now) for notice_id, message in notices),
            )

    def claim(self) -> dict | None:
        now = time.time()
        lease = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._prune(conn, now)
            row = conn.execute(
                """
                SELECT notice_id, message FROM report_missing_notices
                WHERE message IS NOT NULL AND lease_until <= ? ORDER BY created_at, rowid LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE report_missing_notices SET lease_token = ?, lease_until = ? WHERE notice_id = ?",
                    (lease, now + 300, row[0]),
                )
        return {"notice_id": row[0], "message": row[1], "lease_token": lease} if row else None

    def acknowledge(self, notice_id: str, lease_token: str) -> bool:
        with self._connect() as conn:
            # Keep the key for dedupe even if the same historical window is republished.
            return conn.execute(
                "UPDATE report_missing_notices SET message = NULL WHERE notice_id = ? AND lease_token = ?",
                (notice_id, lease_token),
            ).rowcount == 1
