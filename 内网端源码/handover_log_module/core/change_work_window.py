from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import re
from typing import List

from handover_log_module.core.shift_window import normalize_duty_shift, parse_duty_date


_DATETIME_PATTERN = re.compile(r"\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}(?::\d{2})?")
_DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
)


@dataclass(frozen=True)
class ResolvedWorkWindow:
    text: str
    start_dt: datetime | None
    end_dt: datetime | None
    source_points: List[datetime]
    used_default_end: bool


def _parse_hms(text: str, default_text: str) -> tuple[int, int, int]:
    raw = str(text or default_text).strip() or default_text
    dt = datetime.strptime(raw, "%H:%M:%S")
    return dt.hour, dt.minute, dt.second


def _parse_datetime_text(text: str) -> datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except Exception:  # noqa: BLE001
            continue
    return None


def extract_datetimes(text: str) -> List[datetime]:
    raw = str(text or "")
    if not raw.strip():
        return []
    seen: set[str] = set()
    values: List[datetime] = []
    for match in _DATETIME_PATTERN.finditer(raw):
        token = str(match.group(0) or "").strip()
        if not token or token in seen:
            continue
        parsed = _parse_datetime_text(token)
        if parsed is None:
            continue
        seen.add(token)
        values.append(parsed)
    return values


def _format_hm_range(start_dt: datetime, end_dt: datetime) -> str:
    return f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"


def resolve_work_window(
    *,
    process_updates_text: str,
    duty_date: str,
    duty_shift: str,
    day_anchor: str = "08:00:00",
    day_default_end: str = "18:30:00",
    night_anchor: str = "18:00:00",
    night_default_end_next_day: str = "08:00:00",
) -> ResolvedWorkWindow:
    duty_day = parse_duty_date(duty_date)
    shift = normalize_duty_shift(duty_shift)
    points = extract_datetimes(process_updates_text)

    day_anchor_hms = _parse_hms(day_anchor, "08:00:00")
    day_default_end_hms = _parse_hms(day_default_end, "18:30:00")
    night_anchor_hms = _parse_hms(night_anchor, "18:00:00")
    night_default_end_hms = _parse_hms(night_default_end_next_day, "08:00:00")

    if shift == "day":
        anchor_dt = datetime(duty_day.year, duty_day.month, duty_day.day, *day_anchor_hms)
        default_end_dt = datetime(duty_day.year, duty_day.month, duty_day.day, *day_default_end_hms)
        valid_points = [point for point in points if point >= anchor_dt]
    else:
        next_day = duty_day + timedelta(days=1)
        anchor_dt = datetime(duty_day.year, duty_day.month, duty_day.day, *night_anchor_hms)
        default_end_dt = datetime(next_day.year, next_day.month, next_day.day, *night_default_end_hms)
        valid_points = [point for point in points if anchor_dt <= point <= default_end_dt]

    if not valid_points:
        return ResolvedWorkWindow(
            text=_format_hm_range(anchor_dt, default_end_dt),
            start_dt=anchor_dt,
            end_dt=default_end_dt,
            source_points=[],
            used_default_end=True,
        )

    start_dt = valid_points[0]
    if len(valid_points) == 1:
        end_dt = default_end_dt
        used_default_end = True
    elif len(valid_points) == 2:
        end_dt = valid_points[1]
        used_default_end = False
    else:
        end_dt = valid_points[-1]
        used_default_end = False

    return ResolvedWorkWindow(
        text=_format_hm_range(start_dt, end_dt),
        start_dt=start_dt,
        end_dt=end_dt,
        source_points=valid_points,
        used_default_end=used_default_end,
    )
