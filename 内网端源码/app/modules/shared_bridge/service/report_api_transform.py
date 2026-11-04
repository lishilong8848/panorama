from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any


_TIME_RE = re.compile(r"^\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}(?::\d{2})?$")


def _header_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not _TIME_RE.fullmatch(text):
        return None
    try:
        return datetime.fromisoformat(text.replace("/", "-"))
    except ValueError:
        return None


def normalize_report_rows(
    grid: list[list[Any]],
    row_ids: dict[int, str],
    *,
    expected_point_ids: list[str] | None = None,
    query_start: str = "",
    query_end: str = "",
    time_tolerance_sec: int = 0,
) -> list[list[Any]]:
    if not isinstance(grid, list) or not grid or any(not isinstance(row, list) for row in grid):
        raise ValueError("报表接口未返回有效数据")
    candidates: list[tuple[int, int, list[datetime]]] = []
    for index, row in enumerate(grid[:20]):
        times = [parsed for value in row if (parsed := _header_time(value)) is not None]
        if times:
            candidates.append((len(times), index, times))
    if not candidates:
        raise ValueError("报表接口缺少时间表头")
    _, header_index, timestamps = max(candidates, key=lambda item: (item[0], item[1]))
    if len(set(timestamps)) != len(timestamps):
        raise ValueError("报表接口时间表头重复")
    if query_start and query_end:
        try:
            start = datetime.strptime(query_start, "%Y-%m-%d %H:%M:%S")
            end = datetime.strptime(query_end, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError("报表接口查询时间无效") from None
        tolerance = timedelta(seconds=max(0, int(time_tolerance_sec)))
        if end < start or any(value < start - tolerance or value > end + tolerance for value in timestamps):
            raise ValueError("报表接口返回时间超出查询窗口")
    body = grid[header_index + 1:]
    if not body:
        raise ValueError("报表接口没有数据行")
    actual = [value for index, value in row_ids.items() if index > header_index and value]
    if expected_point_ids is not None:
        expected = list(expected_point_ids)
        if not expected or len(expected) != len(set(expected)):
            raise ValueError("报表配置的测点列表无效")
        if len(actual) != len(set(actual)) or set(actual) != set(expected):
            raise ValueError("报表接口返回测点不完整或存在重复")
    if not actual and not any(any(str(cell or "").strip() for cell in row) for row in body):
        raise ValueError("报表接口没有有效数据行")
    return [[], list(grid[header_index]), [], *[list(row) for row in body]]
