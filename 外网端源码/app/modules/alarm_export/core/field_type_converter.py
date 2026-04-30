from __future__ import annotations

import re
from typing import Any, Dict, Tuple

from app.modules.sheet_import.core.field_value_converter import parse_timestamp_ms, split_multi_values


def build_field_meta_map(table_fields: list[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for item in table_fields:
        if not isinstance(item, dict):
            continue
        name = str(item.get("field_name", "")).strip()
        if not name:
            continue
        field_type = int(item.get("type", 0) or 0)
        out[name] = {
            "name": name,
            "type": field_type,
            "property": item.get("property") if isinstance(item.get("property"), dict) else {},
        }
    return out


def _to_number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "").replace("，", "")
    if text.endswith("%"):
        text = text[:-1].strip()
    if not text:
        return None
    if re.fullmatch(r"[+-]?\d+", text):
        try:
            return int(text)
        except Exception:  # noqa: BLE001
            return None
    if re.fullmatch(r"[+-]?\d+(\.\d+)?", text):
        try:
            return float(text)
        except Exception:  # noqa: BLE001
            return None
    return None


def _to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "是", "已完成"}:
        return True
    if text in {"0", "false", "no", "n", "否", "未完成"}:
        return False
    return None


def _to_link(value: Any) -> Dict[str, str] | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if re.match(r"^https?://", text, re.IGNORECASE):
        return {"text": text, "link": text}
    return None


def convert_value_by_field_meta(value: Any, field_meta: Dict[str, Any], tz_offset_hours: int = 8) -> Tuple[Any, bool]:
    field_type = int(field_meta.get("type", 0) or 0)
    if value is None:
        return None, True
    if isinstance(value, str) and value.strip() == "":
        return None, True

    if field_type in (1, 13):
        return str(value), True
    if field_type == 2:
        return _to_number(value), True
    if field_type == 3:
        return str(value).strip(), True
    if field_type == 4:
        if isinstance(value, (list, tuple, set)):
            items = [str(x).strip() for x in value if str(x).strip()]
            return (items or None), True
        items = split_multi_values(value)
        return (items or None), True
    if field_type == 5:
        return parse_timestamp_ms(value, tz_offset_hours=tz_offset_hours), True
    if field_type == 7:
        return _to_bool(value), True
    if field_type == 15:
        return _to_link(value), True

    return None, False


def convert_alarm_row_by_field_meta(
    fields: Dict[str, Any],
    field_meta_map: Dict[str, Dict[str, Any]],
    tz_offset_hours: int = 8,
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    converted: Dict[str, Any] = {}
    stats = {
        "total_fields": 0,
        "nullified_fields": 0,
        "unsupported_fields": 0,
    }
    for key, value in fields.items():
        stats["total_fields"] += 1
        meta = field_meta_map.get(key)
        if meta is None:
            converted[key] = value
            continue
        converted_value, supported = convert_value_by_field_meta(value, meta, tz_offset_hours=tz_offset_hours)
        if converted_value is None:
            if supported:
                stats["nullified_fields"] += 1
            else:
                stats["unsupported_fields"] += 1
            continue
        converted[key] = converted_value
    return converted, stats

