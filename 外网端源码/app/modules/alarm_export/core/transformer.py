from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Tuple


def to_timestamp_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        num = int(float(value))
        if abs(num) >= 10**12:
            return num
        return num * 1000

    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        num = int(text)
        if len(text) >= 13 or abs(num) >= 10**12:
            return num
        return num * 1000

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ):
        try:
            parsed = datetime.strptime(text[:19], fmt)
            return int(parsed.timestamp() * 1000)
        except Exception:  # noqa: BLE001
            continue
    return None


def split_event_source(event_source: Any) -> Tuple[str, str]:
    parts = [part.strip() for part in str(event_source or "").split("/") if str(part).strip()]
    location = parts[3] if len(parts) >= 4 else ""
    monitor = parts[-1] if parts else ""
    return location, monitor


def _normalize_level_key(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(int(float(value)))
    text = str(value).strip()
    if not text:
        return ""
    if text.replace(".", "", 1).isdigit():
        try:
            return str(int(float(text)))
        except Exception:  # noqa: BLE001
            return text
    return text


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_recover_status_text(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "已恢复" if int(float(value)) == 1 else "未恢复"

    text = str(value or "").strip()
    if not text:
        return "未恢复"
    try:
        if int(float(text)) == 1:
            return "已恢复"
    except Exception:  # noqa: BLE001
        pass
    if text in {"已恢复", "恢复"}:
        return "已恢复"
    return "未恢复"


def _to_process_status_text(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        status_num = int(float(value))
        if status_num == 0:
            return "未处理"
        if status_num == 1:
            return "已处理"
        if status_num == 2:
            return "处理中"
        return "处理中"

    text = str(value or "").strip()
    if not text:
        return "处理中"
    try:
        status_num = int(float(text))
        if status_num == 0:
            return "未处理"
        if status_num == 1:
            return "已处理"
        if status_num == 2:
            return "处理中"
        return "处理中"
    except Exception:  # noqa: BLE001
        pass
    if text in {"已处理", "处理", "完成"}:
        return "已处理"
    if text in {"未处理", "待处理"}:
        return "未处理"
    if text in {"处理中"}:
        return "处理中"
    return "处理中"


def _read_recover_value(row: Dict[str, Any], fields_cfg: Dict[str, str]) -> Any:
    primary = row.get("is_recover")
    if primary is not None and str(primary).strip() != "":
        return primary

    value = _read_source_value(row, fields_cfg, "recover_status")
    if value is not None and str(value).strip() != "":
        return value
    return None


def _read_process_status_value(row: Dict[str, Any], fields_cfg: Dict[str, str]) -> Any:
    _ = fields_cfg
    value = row.get("is_accept")
    if value is not None and str(value).strip() != "":
        return value
    return None


def _read_source_value(row: Dict[str, Any], fields_cfg: Dict[str, str], key: str) -> Any:
    source_field = str(fields_cfg.get(key, "")).strip()
    if not source_field:
        return None
    return row.get(source_field)


def transform_row_to_feishu_fields(
    *,
    row: Dict[str, Any],
    building: str,
    fields_cfg: Dict[str, str],
    level_mapping: Dict[str, str],
    skip_levels: Iterable[Any],
    alarm_category_default: str,
) -> Dict[str, Any] | None:
    normalized_skip = {_normalize_level_key(level) for level in skip_levels}

    level_raw = _read_source_value(row, fields_cfg, "event_level")
    level_key = _normalize_level_key(level_raw)
    if level_key in normalized_skip:
        return None

    level_text = str(level_mapping.get(level_key, "")).strip()
    event_source_value = _read_source_value(row, fields_cfg, "event_source")
    location_text, monitor_text = split_event_source(event_source_value)

    event_time_ms = to_timestamp_ms(_read_source_value(row, fields_cfg, "event_time"))
    accept_time_ms = to_timestamp_ms(_read_source_value(row, fields_cfg, "accept_time"))
    recover_time_ms = to_timestamp_ms(_read_source_value(row, fields_cfg, "recover_time"))
    confirm_time_ms = to_timestamp_ms(_read_source_value(row, fields_cfg, "confirm_time"))

    return {
        "等级": level_text,
        "告警内容": _to_text(_read_source_value(row, fields_cfg, "content")),
        "楼栋": str(building).strip(),
        "具体位置": location_text,
        "监控对象": monitor_text,
        "产生时间": event_time_ms if event_time_ms is not None else "",
        "受理时间": accept_time_ms if accept_time_ms is not None else "",
        "处理状态": _to_process_status_text(_read_process_status_value(row, fields_cfg)),
        "受理人": _to_text(_read_source_value(row, fields_cfg, "accept_user")),
        "受理描述": _to_text(_read_source_value(row, fields_cfg, "accept_description")),
        "恢复时间": recover_time_ms if recover_time_ms is not None else "",
        "恢复状态": _to_recover_status_text(_read_recover_value(row, fields_cfg)),
        "处理建议": _to_text(_read_source_value(row, fields_cfg, "process_suggestion")),
        "告警类型": _to_text(_read_source_value(row, fields_cfg, "alarm_type")),
        # Keep raw source value here; type conversion is handled by field_type_converter.
        "触发值": _read_source_value(row, fields_cfg, "trigger_value"),
        "告警分类": str(alarm_category_default or "").strip() or "真实告警",
        "确认时间": confirm_time_ms if confirm_time_ms is not None else "",
        "确认人": _to_text(_read_source_value(row, fields_cfg, "confirm_user")),
        "确认描述": _to_text(_read_source_value(row, fields_cfg, "confirm_description")),
    }

