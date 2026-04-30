from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from openpyxl.utils.datetime import from_excel

from app.modules.report_pipeline.core.metrics_math import to_float


def normalize_field_name(name: str) -> str:
    text = str(name).strip().lower()
    text = text.replace("（", "(").replace("）", ")")
    text = text.replace("｜", "|").replace("/", "|")
    text = re.sub(r"[\s\-_]+", "", text)
    text = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", "", text)
    return text


def parse_timestamp_ms(value: Any, tz_offset_hours: int = 8) -> Optional[int]:
    if value is None:
        return None
    tz = timezone(timedelta(hours=int(tz_offset_hours)))

    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=tz)
        return int(dt.timestamp() * 1000)
    if isinstance(value, date):
        dt = datetime(value.year, value.month, value.day, tzinfo=tz)
        return int(dt.timestamp() * 1000)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        num = float(value)
        if num > 1e12:
            return int(num)
        if num > 1e10:
            return int(num)
        if num > 1e9:
            return int(num * 1000)
        if 1 <= num <= 2958465:
            try:
                dt_excel = from_excel(num)
                if isinstance(dt_excel, datetime):
                    dt = dt_excel if dt_excel.tzinfo else dt_excel.replace(tzinfo=tz)
                    return int(dt.timestamp() * 1000)
                if isinstance(dt_excel, date):
                    dt = datetime(dt_excel.year, dt_excel.month, dt_excel.day, tzinfo=tz)
                    return int(dt.timestamp() * 1000)
            except Exception:
                pass
        return None

    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{13}", text):
        return int(text)
    if re.fullmatch(r"\d{10}", text):
        return int(text) * 1000

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y.%m.%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y.%m.%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=tz)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return None


def split_multi_values(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        out = [str(item).strip() for item in value if str(item).strip()]
        return out
    text = str(value).strip()
    if not text:
        return []
    parts = re.split(r"[,\n\r，；;、]+", text)
    out = [p.strip() for p in parts if p.strip()]
    if out:
        return out
    return [text]


def lookup_option_name(raw: str, option_names: List[str], option_norm_map: Dict[str, str]) -> Optional[str]:
    text = raw.strip()
    if not text:
        return None
    if text in option_names:
        return text
    norm = normalize_field_name(text)
    if norm in option_norm_map:
        return option_norm_map[norm]
    return None


def convert_value_for_field(value: Any, field_meta: Dict[str, Any], tz_offset_hours: int) -> Tuple[Any, bool]:
    field_type = int(field_meta.get("type", 0) or 0)
    options = field_meta.get("options") or []
    option_names = [str(x).strip() for x in options if str(x).strip()]
    option_norm_map = {normalize_field_name(name): name for name in option_names}

    if value is None:
        return None, True
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None, True

    if field_type in (1, 13):
        return str(value), True
    if field_type == 2:
        num = to_float(value)
        return (num, True)
    if field_type == 3:
        text = str(value).strip()
        if not text:
            return None, True
        if option_names:
            matched = lookup_option_name(text, option_names, option_norm_map)
            return (matched, True)
        return text, True
    if field_type == 4:
        values = split_multi_values(value)
        if option_names:
            mapped: List[str] = []
            for raw in values:
                matched = lookup_option_name(raw, option_names, option_norm_map)
                if matched and matched not in mapped:
                    mapped.append(matched)
            values = mapped
        return (values if values else None, True)
    if field_type == 5:
        ts = parse_timestamp_ms(value, tz_offset_hours=tz_offset_hours)
        return (ts, True)
    if field_type == 7:
        if isinstance(value, bool):
            return value, True
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "是", "已完成"}:
            return True, True
        if text in {"0", "false", "no", "n", "否", "未完成"}:
            return False, True
        num = to_float(value)
        if num is not None:
            return bool(num), True
        return None, True
    if field_type == 15:
        text = str(value).strip()
        if not text:
            return None, True
        if re.match(r"^https?://", text, re.IGNORECASE):
            return {"text": text, "link": text}, True
        return None, True

    return None, False
