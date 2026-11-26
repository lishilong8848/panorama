from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager

from app.modules.shared_bridge.service.report_api_transform import _C_UPS_LABELS
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root


MISSING_NOTICE_FIELDS = (
    "zero_filled_point_ids", "missing_sample_points", "zero_filled_sample_point_ids",
    "report_name", "query_start", "query_end",
)
_PENDING_TTL = 86400
_DEDUPE_TTL = 30 * 86400
_DETAIL_CHARS = 950


def _text(value: object, limit: int = 64) -> str:
    return " ".join(value[:limit * 2].split())[:limit] if isinstance(value, str) else ""


def _items(value: object) -> list:
    return value if isinstance(value, list) else []


def _identity(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def build_report_missing_notices(
    payload: dict, *, family: str, report_label: str, building: str, window: str, duty_shift: str,
) -> list[tuple[str, str]]:
    whole = dict.fromkeys(
        point for value in _items(payload.get("zero_filled_point_ids")) if (point := _identity(value))
    )
    samples = {}
    for item in _items(payload.get("missing_sample_points")):
        if not isinstance(item, dict):
            continue
        point, device, metric = (_identity(item.get(key)) for key in ("point_id", "device", "metric"))
        count = item.get("count")
        if (point or (device and metric)) and point not in whole and type(count) is int and count > 0:
            label = _text(" / ".join(filter(None, (point, device, metric))), 112)
            samples[(point, device, metric, count)] = f"{label} ({min(count, 999999)}条)"
    reported_ids = {point for point, _, _, _ in samples}
    for value in _items(payload.get("zero_filled_sample_point_ids")):
        point = _identity(value)
        if point and point not in whole and point not in reported_ids:
            samples[(point, "", "", 0)] = _text(point)
    if not (whole or samples):
        return []

    identity = [family, building, window, duty_shift, sorted(whole), sorted(samples)]
    key = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
    report_name = _text(payload.get("report_name")) or report_label
    window_label = " ".join(filter(None, (_text(window), _text(duty_shift))))
    heading = [
        "【报表缺测提醒】共享源文件已就绪",
        f"报表：{report_name}｜楼栋：{_text(building)}｜窗口：{window_label}",
    ]
    start, end = (_text(payload.get(key)) for key in ("query_start", "query_end"))
    if start and end:
        heading.append(f"查询范围：{start} 至 {end}")
    heading.append(f"未返回测点：{len(whole)} 个；缺读数测点：{len(samples)} 个。补零不代表真实值。")
    known_labels = _C_UPS_LABELS if building == "C楼" and family == "handover_capacity_report_family" else {}
    details = [f"未返回：{_text(point, 112)}" + (f" / {known_labels[point]}" if point in known_labels else "")
               for point in sorted(whole)]
    details.extend(f"缺读数：{samples[item]}" for item in sorted(samples))
    batches: list[list[str]] = [[]]
    length = 0
    for line in details:
        if length + len(line) + 1 > _DETAIL_CHARS:
            batches.append([])
            length = 0
        batches[-1].append(line)
        length += len(line) + 1
    return [(key if index == 1 else hashlib.sha256(f"{key}:{index}".encode()).hexdigest(),
             "\n".join([*heading, f"测点清单 {index}/{len(batches)}", *batch]))
            for index, batch in enumerate(batches, 1)]


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
