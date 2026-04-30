from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class ShiftIntervalWindow:
    shift_start: datetime
    shift_end: datetime
    filter_start: datetime
    filter_end: datetime


def build_shift_interval_window(
    *,
    shift_start: datetime,
    shift_end: datetime,
    offset_hours: int = 1,
) -> ShiftIntervalWindow:
    offset = timedelta(hours=int(offset_hours))
    return ShiftIntervalWindow(
        shift_start=shift_start,
        shift_end=shift_end,
        filter_start=shift_start + offset,
        filter_end=shift_end + offset,
    )


def interval_overlaps_filter_window(
    *,
    start_time: datetime,
    end_time: datetime | None,
    filter_start: datetime,
    filter_end: datetime,
) -> bool:
    if end_time is not None and end_time < start_time:
        return False
    return start_time <= filter_end and (end_time is None or end_time > filter_start)
