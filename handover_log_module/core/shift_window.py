from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict


VALID_DUTY_SHIFTS = {"day", "night"}


@dataclass(frozen=True)
class DutyWindow:
    duty_date: str
    duty_shift: str
    start_time: str
    end_time: str
    duty_date_for_filename: date


def normalize_duty_shift(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text not in VALID_DUTY_SHIFTS:
        raise ValueError("duty_shift 仅支持 day 或 night")
    return text


def parse_duty_date(value: str) -> date:
    text = str(value or "").strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"duty_date 格式错误: {text}, 期望 YYYY-MM-DD") from exc


def _parse_hms(text: Any, default_hms: str) -> tuple[int, int, int]:
    raw = str(text or default_hms).strip() or default_hms
    try:
        dt = datetime.strptime(raw, "%H:%M:%S")
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"班次时间格式错误: {raw}, 期望 HH:MM:SS") from exc
    return dt.hour, dt.minute, dt.second


def build_duty_window(
    *,
    duty_date: str,
    duty_shift: str,
    shift_windows: Dict[str, Any] | None,
    time_format: str = "%Y-%m-%d %H:%M:%S",
) -> DutyWindow:
    duty_day = parse_duty_date(duty_date)
    shift = normalize_duty_shift(duty_shift)
    windows = shift_windows if isinstance(shift_windows, dict) else {}

    day_cfg = windows.get("day", {}) if isinstance(windows.get("day"), dict) else {}
    night_cfg = windows.get("night", {}) if isinstance(windows.get("night"), dict) else {}

    day_start = _parse_hms(day_cfg.get("start"), "08:00:00")
    day_end = _parse_hms(day_cfg.get("end"), "17:00:00")
    night_start = _parse_hms(night_cfg.get("start"), "17:00:00")
    night_end_next_day = _parse_hms(night_cfg.get("end_next_day"), "08:00:00")

    if shift == "day":
        start_dt = datetime(
            duty_day.year,
            duty_day.month,
            duty_day.day,
            day_start[0],
            day_start[1],
            day_start[2],
        )
        end_dt = datetime(
            duty_day.year,
            duty_day.month,
            duty_day.day,
            day_end[0],
            day_end[1],
            day_end[2],
        )
    else:
        next_day = duty_day + timedelta(days=1)
        start_dt = datetime(
            duty_day.year,
            duty_day.month,
            duty_day.day,
            night_start[0],
            night_start[1],
            night_start[2],
        )
        end_dt = datetime(
            next_day.year,
            next_day.month,
            next_day.day,
            night_end_next_day[0],
            night_end_next_day[1],
            night_end_next_day[2],
        )

    if end_dt <= start_dt:
        raise ValueError(f"班次时间窗无效: shift={shift}, start={start_dt}, end={end_dt}")

    return DutyWindow(
        duty_date=duty_day.strftime("%Y-%m-%d"),
        duty_shift=shift,
        start_time=start_dt.strftime(time_format),
        end_time=end_dt.strftime(time_format),
        duty_date_for_filename=duty_day,
    )


def format_duty_date_text(duty_date: str, template: str = "{year}年{month}月{day}日") -> str:
    day = parse_duty_date(duty_date)
    fmt = str(template or "{year}年{month}月{day}日")
    try:
        return fmt.format(year=day.year, month=day.month, day=day.day).strip()
    except Exception:  # noqa: BLE001
        return f"{day.year}年{day.month}月{day.day}日"
