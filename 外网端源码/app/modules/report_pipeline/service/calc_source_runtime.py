from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import openpyxl


Worksheet = openpyxl.worksheet.worksheet.Worksheet


def validate_business_date(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("业务日期格式错误，必须为YYYY-MM-DD")
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("业务日期无效，必须为有效的YYYY-MM-DD日期") from exc
    return value


def _unique_json_object(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("月报源JSON包含重复字段")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> None:
    raise ValueError("月报源JSON包含无效数字")


def load_monthly_json_source(
    file_path: str,
    *,
    expected_building: str | None,
    expected_business_date: str | None,
) -> Tuple[openpyxl.Workbook, str, str]:
    try:
        with Path(file_path).open("r", encoding="utf-8-sig") as source:
            payload = json.load(source, object_pairs_hook=_unique_json_object, parse_constant=_reject_json_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("月报源JSON格式无效") from exc

    if not isinstance(payload, dict):
        raise ValueError("月报源JSON顶层必须是对象")
    if type(payload.get("schema_version")) is not int or payload["schema_version"] != 1:
        raise ValueError("月报源JSON schema_version必须为1")
    if payload.get("source_family") != "monthly_report_family":
        raise ValueError("月报源JSON来源不匹配")

    building = payload.get("building")
    if not isinstance(building, str) or not building.strip() or building != building.strip():
        raise ValueError("月报源JSON楼栋无效")
    if expected_building is not None and building != expected_building:
        raise ValueError("月报源JSON楼栋与指定楼栋不匹配")

    business_date = validate_business_date(payload.get("business_date"))
    if expected_business_date is not None:
        if business_date != validate_business_date(expected_business_date):
            raise ValueError("月报源JSON业务日期与指定日期不匹配")

    sheets = payload.get("sheets")
    if not isinstance(sheets, list) or not sheets:
        raise ValueError("月报源JSON缺少工作表")
    for index, sheet in enumerate(sheets, 1):
        if (
            not isinstance(sheet, dict)
            or not isinstance(sheet.get("name"), str)
            or not sheet["name"].strip()
            or not isinstance(sheet.get("rows"), list)
        ):
            raise ValueError(f"月报源JSON第{index}个工作表无效")
        for row in sheet["rows"]:
            if not isinstance(row, list):
                raise ValueError(f"月报源JSON第{index}个工作表的行必须是一维数组")
            for cell in row:
                if not isinstance(cell, (str, int, float, bool, type(None))):
                    raise ValueError(f"月报源JSON第{index}个工作表包含非标量单元格")
                if isinstance(cell, (int, float)) and not isinstance(cell, bool):
                    try:
                        finite = math.isfinite(cell)
                    except OverflowError:
                        finite = False
                    if not finite:
                        raise ValueError("月报源JSON包含无效数字")

    if not sheets[0]["rows"]:
        raise ValueError("月报源JSON第一个工作表为空")
    workbook = openpyxl.Workbook()
    try:
        worksheet = workbook.active
        for row_index, row in enumerate(sheets[0]["rows"], 1):
            for col_index, value in enumerate(row, 1):
                if value is not None:
                    worksheet.cell(row_index, col_index, value)
    except Exception:
        workbook.close()
        raise
    return workbook, building, business_date


def _parse_header_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("/", "-")
    if "T" in normalized:
        try:
            parsed = datetime.fromisoformat(normalized)
            return parsed if parsed.tzinfo is None else None
        except ValueError:
            return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            pass
    return None


def _business_day_data_columns(
    ws: Worksheet,
    *,
    header_row: int,
    data_start_col: int,
    data_end_col: int,
    business_date: str | None = None,
) -> List[int]:
    columns = list(range(data_start_col, data_end_col + 1))
    parsed_by_col: Dict[int, datetime] = {}
    for col in columns:
        parsed = _parse_header_datetime(ws.cell(header_row, col).value)
        if parsed is not None:
            parsed_by_col[col] = parsed
    business_date_text = str(business_date or "").strip()
    if business_date_text:
        start_dt = datetime.strptime(validate_business_date(business_date_text), "%Y-%m-%d")
        boundary_dt = start_dt + timedelta(days=1)
        available_datetimes = set(parsed_by_col.values())
        missing_boundaries = [
            label
            for value, label in ((start_dt, "业务日00:00"), (boundary_dt, "次日00:00"))
            if value not in available_datetimes
        ]
        if missing_boundaries:
            raise ValueError(f"源文件业务日边界不完整: {business_date_text} 缺少{','.join(missing_boundaries)}")
        filtered = [col for col, parsed in parsed_by_col.items() if start_dt <= parsed <= boundary_dt]
        if not filtered:
            raise ValueError(f"源文件未找到业务日时间列: {business_date_text} 00:00~次日00:00")
        return filtered
    if len(parsed_by_col) < 2:
        return columns
    first_col = min(parsed_by_col)
    start_dt = parsed_by_col[first_col]
    boundary_dt = datetime.combine(start_dt.date() + timedelta(days=1), time.min)
    filtered = [col for col in columns if parsed_by_col.get(col) is None or parsed_by_col[col] <= boundary_dt]
    return filtered or columns


def validate_monthly_json_columns(
    ws: Worksheet,
    *,
    header_row: int,
    data_start_col: int,
    data_end_col: int,
    business_date: str,
) -> List[int]:
    columns = _business_day_data_columns(
        ws,
        header_row=header_row,
        data_start_col=data_start_col,
        data_end_col=data_end_col,
        business_date=business_date,
    )
    parsed_headers = [_parse_header_datetime(ws.cell(header_row, col).value) for col in range(data_start_col, data_end_col + 1)]
    if any(value is None for value in parsed_headers):
        raise ValueError("月报源JSON存在无法识别的时间列")
    if len(set(parsed_headers)) != len(parsed_headers):
        raise ValueError("月报源JSON存在重复时间列")
    start_dt = datetime.strptime(validate_business_date(business_date), "%Y-%m-%d")
    available_datetimes = set(parsed_headers)
    missing_hours = [start_dt + timedelta(hours=hour) for hour in range(25)
                     if start_dt + timedelta(hours=hour) not in available_datetimes]
    if missing_hours:
        missing_text = ", ".join(value.strftime("%Y-%m-%d %H:%M") for value in missing_hours)
        raise ValueError(f"月报源JSON缺少业务日整点时间列: {missing_text}")
    return columns


def apply_building_source_overrides(
    *,
    building: str,
    source_map: Dict[str, Any],
    canonical_metric_name: Callable[[Any], str],
) -> Dict[str, Any]:
    """
    按楼栋应用指标来源兼容规则。
    当前规则：
    - A楼“光伏发电量”优先采用“总_电度”行。
    """
    building_text = str(building or "").strip()
    if building_text != "A楼":
        return source_map
    pv_total_key = canonical_metric_name("总_电度")
    pv_total_row = source_map.get(pv_total_key) or source_map.get("总_电度")
    if pv_total_row is not None:
        source_map["光伏发电量"] = pv_total_row
    return source_map


def locate_stat_columns(
    ws: Worksheet,
    *,
    header_row: int,
    data_start_col: int,
    max_label: str,
    min_label: str,
    avg_label: str,
    norm_text: Callable[[Any], str],
) -> Dict[str, int]:
    result: Dict[str, int] = {}
    for col in range(data_start_col, ws.max_column + 1):
        label = norm_text(ws.cell(header_row, col).value)
        if label == max_label and "max" not in result:
            result["max"] = col
        if label == min_label and "min" not in result:
            result["min"] = col
        if label == avg_label and "avg" not in result:
            result["avg"] = col

    missing = [name for name in ("max", "min", "avg") if name not in result]
    if missing:
        raise ValueError(f"未能在第{header_row}行定位统计列: {missing}")
    return result


def extract_month(
    ws: Worksheet,
    max_col: int,
    *,
    header_row: int,
    data_start_col: int,
    month_pattern: re.Pattern[str] | None = None,
    now_factory: Callable[[], datetime] = datetime.now,
) -> str:
    pattern = month_pattern or re.compile(r"(\d{4})[-/年](\d{1,2})")
    for col in range(data_start_col, max_col):
        value = ws.cell(header_row, col).value
        if value is None:
            continue
        if isinstance(value, datetime):
            return value.strftime("%Y-%m")
        if isinstance(value, date):
            return value.strftime("%Y-%m")

        text = str(value).strip()
        match = pattern.search(text)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            return f"{year:04d}-{month:02d}"
    return now_factory().strftime("%Y-%m")


def extract_building(file_path: str) -> str:
    stem = Path(file_path).stem
    return stem.split("_")[0] if "_" in stem else stem


def extract_row_sources(
    ws: Worksheet,
    stat_cols: Dict[str, int],
    *,
    data_start_col: int,
    data_start_row: int,
    type_col: int,
    category_col: int,
    item_col: int,
    to_float: Callable[[Any], float | None],
    canonical_metric_name: Callable[[Any], str],
    row_source_factory: Callable[..., Any],
    business_date: str | None = None,
) -> Tuple[Dict[str, Any], str, str]:
    data_end_col = stat_cols["max"] - 1
    if data_end_col < data_start_col:
        raise ValueError("数据区定位异常：最大值列在 E 列之前")

    current_type = ""
    current_category = ""
    source_map: Dict[str, Any] = {}

    data_columns = _business_day_data_columns(
        ws,
        header_row=2,
        data_start_col=data_start_col,
        data_end_col=data_end_col,
        business_date=business_date,
    )
    has_excluded_boundary_columns = len(data_columns) < (data_end_col - data_start_col + 1)

    for row in range(data_start_row, ws.max_row + 1):
        type_cell = ws.cell(row, type_col).value
        category_cell = ws.cell(row, category_col).value
        item_cell = ws.cell(row, item_col).value

        if type_cell is not None and str(type_cell).strip():
            current_type = str(type_cell).strip()
        if category_cell is not None and str(category_cell).strip():
            current_category = str(category_cell).strip()

        if item_cell is None or not str(item_cell).strip():
            continue
        item_name = str(item_cell).strip()

        data_values: List[float] = []
        for col in data_columns:
            number = to_float(ws.cell(row, col).value)
            if number is not None:
                data_values.append(number)

        max_value = to_float(ws.cell(row, stat_cols["max"]).value)
        min_value = to_float(ws.cell(row, stat_cols["min"]).value)
        avg_value = to_float(ws.cell(row, stat_cols["avg"]).value)

        if (max_value is None or has_excluded_boundary_columns) and data_values:
            max_value = max(data_values)
        if (min_value is None or has_excluded_boundary_columns) and data_values:
            min_value = min(data_values)
        if (avg_value is None or has_excluded_boundary_columns) and data_values:
            avg_value = sum(data_values) / len(data_values)

        row_source = row_source_factory(
            row_index=row,
            type_name=current_type,
            category_name=current_category,
            item_name=item_name,
            max_value=max_value,
            min_value=min_value,
            avg_value=avg_value,
            data_values=data_values,
        )
        source_map[canonical_metric_name(item_name)] = row_source

    return source_map, current_type, current_category


__all__ = [
    "load_monthly_json_source",
    "validate_business_date",
    "validate_monthly_json_columns",
    "locate_stat_columns",
    "extract_month",
    "extract_building",
    "extract_row_sources",
    "apply_building_source_overrides",
]
