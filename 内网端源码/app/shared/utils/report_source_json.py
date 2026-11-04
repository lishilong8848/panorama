from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any


REPORT_JSON_FAMILIES = frozenset({
    "handover_log_family", "handover_capacity_report_family", "monthly_report_family",
    "branch_power_family", "branch_current_family", "branch_switch_family",
    "building_full_cabinet_power_family", "air_conditioner_temperature_humidity_family",
    "chiller_mode_switch_family",
})


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("报表源 JSON 包含重复字段")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("报表源 JSON 包含无效数值")


def load_report_source_json(
    path: str | Path, *, expected_family: str = "", expected_building: str = "",
    expected_business_date: str = "",
) -> dict[str, Any]:
    source = Path(path)
    if source.stat().st_size > 128 * 1024 * 1024:
        raise ValueError("报表源 JSON 超过大小限制")
    with source.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    if not isinstance(payload, dict) or type(payload.get("schema_version")) is not int or payload["schema_version"] != 1:
        raise ValueError("报表源 JSON 版本无效")
    family = payload.get("source_family")
    if not isinstance(family, str) or family not in REPORT_JSON_FAMILIES or (expected_family and family != expected_family):
        raise ValueError("报表源 JSON 来源不匹配")
    building = payload.get("building")
    if not isinstance(building, str) or building not in {"A楼", "B楼", "C楼", "D楼", "E楼"} or (expected_building and building != expected_building):
        raise ValueError("报表源 JSON 楼栋不匹配")
    business_date = payload.get("business_date", "")
    if not isinstance(business_date, str):
        raise ValueError("报表源 JSON 业务日期无效")
    if family in {"monthly_report_family", "branch_power_family", "branch_current_family", "branch_switch_family", "building_full_cabinet_power_family", "air_conditioner_temperature_humidity_family"} and not business_date:
        raise ValueError("报表源 JSON 缺少业务日期")
    if business_date:
        try:
            parsed_date = datetime.strptime(business_date, "%Y-%m-%d")
            if parsed_date.strftime("%Y-%m-%d") != business_date:
                raise ValueError
        except ValueError:
            raise ValueError("报表源 JSON 业务日期无效") from None
    if expected_business_date and business_date != expected_business_date:
        raise ValueError("报表源 JSON 业务日期不匹配")
    try:
        start = datetime.strptime(payload["query_start"], "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(payload["query_end"], "%Y-%m-%d %H:%M:%S")
    except (KeyError, TypeError, ValueError):
        raise ValueError("报表源 JSON 查询时间无效") from None
    if end < start:
        raise ValueError("报表源 JSON 查询时间无效")
    if family == "air_conditioner_temperature_humidity_family" and end.strftime("%Y-%m-%d") != business_date:
        raise ValueError("报表源 JSON 查询时间与业务日期不匹配")
    requested = payload.get("requested_point_count")
    returned = payload.get("returned_point_count")
    if requested is not None and (type(requested) is not int or requested <= 0 or returned != requested):
        raise ValueError("报表源 JSON 测点数量不完整")
    sheets = payload.get("sheets")
    if not isinstance(sheets, list) or not sheets:
        raise ValueError("报表源 JSON 缺少数据")
    for sheet in sheets:
        if not isinstance(sheet, dict) or not isinstance(sheet.get("name"), str) or not sheet["name"].strip():
            raise ValueError("报表源 JSON 工作表标识无效")
        rows = sheet.get("rows")
        if not isinstance(rows, list) or len(rows) < 4 or len(rows) > 50000:
            raise ValueError("报表源 JSON 数据行数无效")
        for row in rows:
            if not isinstance(row, list) or len(row) > 4096:
                raise ValueError("报表源 JSON 数据行无效")
            for value in row:
                if not isinstance(value, (str, int, float, bool, type(None))):
                    raise ValueError("报表源 JSON 包含无效数据")
                if isinstance(value, float) and not math.isfinite(value):
                    raise ValueError("报表源 JSON 包含无效数值")
        time_columns = []
        for index, value in enumerate(rows[1]):
            text = str(value or "").strip()
            if " " not in text and "T" not in text:
                continue
            try:
                datetime.fromisoformat(text.replace("/", "-"))
            except ValueError:
                continue
            time_columns.append(index)
        if not time_columns or not any(
            index < len(row) and row[index] is not None and str(row[index]).strip()
            for row in rows[3:] for index in time_columns
        ):
            raise ValueError("报表源 JSON 没有有效测点值")
    return payload


def validate_report_source_json(path: str | Path, **kwargs: Any) -> None:
    load_report_source_json(path, **kwargs)
