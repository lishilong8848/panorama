from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List


def parse_hms_text(value: Any, field_name: str) -> tuple[int, int, int]:
    text = str(value).strip()
    try:
        dt = datetime.strptime(text, "%H:%M:%S")
    except ValueError as exc:
        raise ValueError(f"{field_name} 格式错误，必须为 HH:MM:SS") from exc
    return dt.hour, dt.minute, dt.second


def combine_date_hms(day: date, hms_text: str, field_name: str) -> datetime:
    hour, minute, second = parse_hms_text(hms_text, field_name)
    return datetime(day.year, day.month, day.day, hour, minute, second)


def build_time_range(
    download_cfg: Dict[str, Any],
    *,
    get_last_month_window: Callable[[], tuple[str, str]],
    now: datetime | None = None,
    emit_log: Callable[[str], None] | None = None,
) -> tuple[str, str]:
    now_dt = now or datetime.now()
    time_format = "%Y-%m-%d %H:%M:%S"
    mode = str(download_cfg.get("time_range_mode", "")).strip()

    def _log(line: str) -> None:
        if emit_log is not None:
            emit_log(line)

    if mode == "yesterday_to_today_start":
        today_start = datetime(now_dt.year, now_dt.month, now_dt.day, 0, 0, 0)
        yesterday_start = today_start - timedelta(days=1)
        start_text = yesterday_start.strftime(time_format)
        end_text = today_start.strftime(time_format)
        _log(f"[时间窗] mode={mode}, start={start_text}, end={end_text}")
        return start_text, end_text

    if mode == "last_month_to_this_month_start":
        start_text, end_text = get_last_month_window()
        _log(f"[时间窗] mode={mode}, start={start_text}, end={end_text}")
        return start_text, end_text

    if mode != "custom":
        raise ValueError("download.time_range_mode 仅支持 yesterday_to_today_start、last_month_to_this_month_start 或 custom")

    custom_mode = str(download_cfg.get("custom_window_mode", "absolute")).strip().lower()
    if custom_mode not in {"absolute", "daily_relative"}:
        raise ValueError("download.custom_window_mode 仅支持 absolute 或 daily_relative")

    if custom_mode == "absolute":
        start_time = str(download_cfg.get("start_time", "")).strip()
        end_time = str(download_cfg.get("end_time", "")).strip()
        if not start_time or not end_time:
            raise ValueError("当 download.time_range_mode=custom 时，必须配置 start_time 和 end_time")
        try:
            start_dt = datetime.strptime(start_time, time_format)
        except ValueError as exc:
            raise ValueError(f"start_time 格式错误，必须为 {time_format}") from exc
        try:
            end_dt = datetime.strptime(end_time, time_format)
        except ValueError as exc:
            raise ValueError(f"end_time 格式错误，必须为 {time_format}") from exc
        if start_dt >= end_dt:
            raise ValueError("时间区间错误：start_time 必须早于 end_time")
        if start_dt > now_dt or end_dt > now_dt:
            raise ValueError("时间区间错误：start_time 和 end_time 都不能超过当前时间")
        _log(f"[时间窗] mode={mode}, custom_mode={custom_mode}, start={start_time}, end={end_time}")
        return start_time, end_time

    daily_cfg = download_cfg.get("daily_custom_window")
    if not isinstance(daily_cfg, dict):
        raise ValueError("download.daily_custom_window 配置错误")

    start_hms = str(daily_cfg.get("start_time", "")).strip()
    end_hms = str(daily_cfg.get("end_time", "")).strip()
    cross_day = bool(daily_cfg.get("cross_day", False))
    today = now_dt.date()

    if not cross_day:
        today_start = combine_date_hms(today, start_hms, "download.daily_custom_window.start_time")
        today_end = combine_date_hms(today, end_hms, "download.daily_custom_window.end_time")
        if today_end <= today_start:
            raise ValueError("daily_relative 配置错误：cross_day=false 时 end_time 必须晚于 start_time")
        if now_dt >= today_end:
            start_dt = today_start
            end_dt = today_end
        else:
            prev = today - timedelta(days=1)
            start_dt = combine_date_hms(prev, start_hms, "download.daily_custom_window.start_time")
            end_dt = combine_date_hms(prev, end_hms, "download.daily_custom_window.end_time")
    else:
        today_end = combine_date_hms(today, end_hms, "download.daily_custom_window.end_time")
        if now_dt >= today_end:
            start_day = today - timedelta(days=1)
            end_day = today
        else:
            start_day = today - timedelta(days=2)
            end_day = today - timedelta(days=1)
        start_dt = combine_date_hms(start_day, start_hms, "download.daily_custom_window.start_time")
        end_dt = combine_date_hms(end_day, end_hms, "download.daily_custom_window.end_time")
        if end_dt <= start_dt:
            raise ValueError("daily_relative 配置错误：cross_day=true 时无法构造有效跨天区间")

    start_text = start_dt.strftime(time_format)
    end_text = end_dt.strftime(time_format)
    _log(f"[时间窗] mode={mode}, custom_mode={custom_mode}, start={start_text}, end={end_text}")
    return start_text, end_text


def normalize_selected_dates(selected_dates: List[str], max_dates_per_run: int, *, now: datetime | None = None) -> List[str]:
    if max_dates_per_run <= 0:
        raise ValueError("download.multi_date.max_dates_per_run 必须大于0")
    if not selected_dates:
        raise ValueError("未选择日期")

    today = (now or datetime.now()).date()
    unique: Dict[str, date] = {}
    for idx, value in enumerate(selected_dates, 1):
        text = str(value).strip()
        if not text:
            continue
        try:
            day = datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"selected_dates 第{idx}项格式错误，必须为YYYY-MM-DD: {text}") from exc
        if day > today:
            raise ValueError(f"selected_dates 第{idx}项不能超过当前日期: {text}")
        unique[text] = day

    if not unique:
        raise ValueError("未选择有效日期")

    ordered = sorted(unique.items(), key=lambda item: item[1])
    if len(ordered) > max_dates_per_run:
        raise ValueError(f"selected_dates 超过上限: {len(ordered)} > {max_dates_per_run}")
    return [item[0] for item in ordered]


def daily_window_text() -> tuple[str, str, str]:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    return (
        yesterday.strftime("%Y-%m-%d"),
        yesterday.strftime("%Y-%m-%d %H:%M:%S"),
        today.strftime("%Y-%m-%d %H:%M:%S"),
    )
