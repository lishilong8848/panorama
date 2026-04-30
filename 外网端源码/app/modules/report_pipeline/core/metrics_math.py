from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional


def norm_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    text = re.sub(r"\s+", "", text)
    text = text.replace("：", ":")
    return text


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def round6(value: float) -> float:
    return round(float(value), 6)


def round_metric_value(metric: str, value: float) -> float:
    if metric == "PUE":
        return round(float(value), 3)
    return round6(value)


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def date_text_to_timestamp_ms(date_text: str, default_day: int = 1, tz_offset_hours: int = 8) -> int:
    text = str(date_text).strip()
    m_day = re.match(r"^\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*$", text)
    if m_day:
        year = int(m_day.group(1))
        month = int(m_day.group(2))
        day = int(m_day.group(3))
    else:
        m_month = re.match(r"^\s*(\d{4})-(\d{1,2})\s*$", text)
        if not m_month:
            raise ValueError(f"无效日期格式: {date_text}, 期望YYYY-MM或YYYY-MM-DD")
        year = int(m_month.group(1))
        month = int(m_month.group(2))
        day = max(1, min(int(default_day), 28))
    tz = timezone(timedelta(hours=int(tz_offset_hours)))
    dt = datetime(year, month, day, 0, 0, 0, tzinfo=tz)
    return int(dt.timestamp() * 1000)


def month_to_timestamp_ms(month_text: str, day: int = 1, tz_offset_hours: int = 8) -> int:
    return date_text_to_timestamp_ms(month_text, default_day=day, tz_offset_hours=tz_offset_hours)


def month_from_datetime_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m")
        except ValueError:
            pass
    m = re.search(r"(\d{4})[-/年](\d{1,2})", text)
    if not m:
        return ""
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"


def date_from_datetime_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})", text)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m_month = re.search(r"(\d{4})[-/年](\d{1,2})", text)
    if m_month:
        return f"{int(m_month.group(1)):04d}-{int(m_month.group(2)):02d}-01"
    return ""


def resolve_upload_date_from_runtime(config: Dict[str, Any]) -> str:
    runtime_cfg = config["_runtime"] if "_runtime" in config else None
    if isinstance(runtime_cfg, dict) and "time_range_start" in runtime_cfg:
        date_text = date_from_datetime_text(runtime_cfg["time_range_start"])
        if date_text:
            return date_text

    download_cfg = config["download"] if "download" in config else None
    if isinstance(download_cfg, dict) and "time_range_mode" in download_cfg:
        mode = str(download_cfg["time_range_mode"]).strip()
        if mode == "custom" and "start_time" in download_cfg:
            date_text = date_from_datetime_text(download_cfg["start_time"])
            if date_text:
                return date_text
    return ""
