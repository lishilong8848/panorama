from __future__ import annotations

import json
import math
import warnings
from pathlib import Path
from typing import Any, Dict, List

import openpyxl

from handover_log_module.core.models import RawRow
from handover_log_module.core.normalizers import format_extracted_value, normalize_b, normalize_c, to_float


def _pick_sheet(
    wb: openpyxl.Workbook,
    sheet_name: str | None,
    sheet_index: int | None,
) -> openpyxl.worksheet.worksheet.Worksheet:
    if sheet_name:
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"数据表缺少工作表: {sheet_name}")
        return wb[sheet_name]
    idx = int(sheet_index or 0)
    if idx < 0 or idx >= len(wb.sheetnames):
        raise ValueError(f"sheet_index越界: {idx}")
    return wb[wb.sheetnames[idx]]


def load_workbook_quietly(path: str | Path, **kwargs: Any) -> openpyxl.Workbook:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style, apply openpyxl's default",
            category=UserWarning,
            module=r"openpyxl\.styles\.stylesheet",
        )
        warnings.filterwarnings(
            "ignore",
            message="Unknown extension is not supported and will be removed",
            category=UserWarning,
            module=r"openpyxl\.worksheet\._reader",
        )
        warnings.filterwarnings(
            "ignore",
            message="Conditional Formatting extension is not supported and will be removed",
            category=UserWarning,
            module=r"openpyxl\.worksheet\._reader",
        )
        return openpyxl.load_workbook(path, **kwargs)


def load_rows(
    data_file: str,
    parsing_cfg: Dict[str, Any],
    normalize_cfg: Dict[str, Any],
    *,
    expected_building: str = "",
    expected_source_family: str = "",
) -> List[RawRow]:
    path = Path(data_file)
    if not path.exists():
        raise FileNotFoundError(f"数据表不存在: {path}")

    start_row = int(parsing_cfg.get("start_row", 4))
    col_b = int(parsing_cfg.get("col_b", 2))
    col_c = int(parsing_cfg.get("col_c", 3))
    col_d = int(parsing_cfg.get("col_d", 4))
    col_e = int(parsing_cfg.get("col_e", 5))
    forward_fill_b = bool(parsing_cfg.get("forward_fill_b", True))
    forward_fill_c = bool(parsing_cfg.get("forward_fill_c", True))

    b_regex = str(normalize_cfg.get("b_extract_regex", "")).strip()
    c_regex = str(normalize_cfg.get("c_extract_regex", "")).strip()

    wb = None

    try:
        sheet_name = str(parsing_cfg.get("sheet_name", "")).strip() or None
        sheet_index = parsing_cfg.get("sheet_index", 0)
        if path.suffix.lower() == ".json":
            def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
                obj: dict[str, Any] = {}
                for key, value in pairs:
                    if key in obj:
                        raise ValueError("交接班源数据 JSON 存在重复字段")
                    obj[key] = value
                return obj

            def reject_constant(_value: str) -> None:
                raise ValueError("交接班源数据 JSON 包含无效数字")

            with path.open("r", encoding="utf-8-sig") as handle:
                payload = json.load(handle, object_pairs_hook=unique_object, parse_constant=reject_constant)
            if not isinstance(payload, dict) or type(payload.get("schema_version")) is not int or payload["schema_version"] != 1:
                raise ValueError("交接班源数据 JSON 版本无效")
            family = payload.get("source_family")
            if family not in {"handover_log_family", "handover_capacity_report_family"}:
                raise ValueError("交接班源数据 JSON 来源无效")
            if expected_source_family and family != expected_source_family:
                raise ValueError("交接班源数据 JSON 来源不匹配")
            building = payload.get("building")
            if not isinstance(building, str) or building not in {"A楼", "B楼", "C楼", "D楼", "E楼"}:
                raise ValueError("交接班源数据 JSON 楼栋无效")
            if expected_building and building != expected_building:
                raise ValueError("交接班源数据 JSON 楼栋不匹配")
            sheets = payload.get("sheets")
            if not isinstance(sheets, list) or not sheets:
                raise ValueError("交接班源数据 JSON 缺少工作表数据")
            for item in sheets:
                if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not item["name"].strip() or not isinstance(item.get("rows"), list):
                    raise ValueError("交接班源数据 JSON 工作表结构无效")
            if sheet_name:
                matching_sheets = [item for item in sheets if item["name"] == sheet_name]
                if len(matching_sheets) != 1:
                    raise ValueError(f"源数据缺少唯一工作表: {sheet_name}")
                source_rows = matching_sheets[0]["rows"]
            else:
                index = int(sheet_index or 0)
                if index < 0 or index >= len(sheets):
                    raise ValueError(f"sheet_index越界: {index}")
                source_rows = sheets[index]["rows"]
            for row in source_rows:
                if not isinstance(row, list):
                    raise ValueError("交接班源数据 JSON 行格式无效")
                for value in row:
                    if not isinstance(value, (str, int, float, bool, type(None))):
                        raise ValueError("交接班源数据 JSON 包含无效单元格")
                    if isinstance(value, float) and not math.isfinite(value):
                        raise ValueError("交接班源数据 JSON 包含无效数字")

            def json_cell_values():
                columns = (col_b, col_c, col_d, col_e)
                for row_idx, row in enumerate(source_rows, start=1):
                    if row_idx >= start_row:
                        yield row_idx, *(row[col - 1] if col > 0 and len(row) >= col else None for col in columns)

            source_values = json_cell_values()
        else:
            wb = load_workbook_quietly(path, data_only=True)
            ws = _pick_sheet(wb, sheet_name=sheet_name, sheet_index=sheet_index)
            source_values = (
                (row_idx, ws.cell(row_idx, col_b).value, ws.cell(row_idx, col_c).value,
                 ws.cell(row_idx, col_d).value, ws.cell(row_idx, col_e).value)
                for row_idx in range(start_row, ws.max_row + 1)
            )

        rows: List[RawRow] = []
        last_b = ""
        last_c = ""

        for row_idx, b_raw, c_raw, d_raw, e_raw in source_values:
            e_value = to_float(e_raw)
            e_display = format_extracted_value(e_raw)

            b_text = str(b_raw).strip() if b_raw is not None else ""
            c_text = str(c_raw).strip() if c_raw is not None else ""
            d_name = str(d_raw).strip() if d_raw is not None else ""
            if not d_name:
                continue

            if b_text:
                last_b = b_text
            elif forward_fill_b:
                b_text = last_b

            if c_text:
                last_c = c_text
            elif forward_fill_c:
                c_text = last_c

            rows.append(
                RawRow(
                    row_index=row_idx,
                    b_text=b_text,
                    c_text=c_text,
                    d_name=d_name,
                    e_raw=e_display,
                    value=e_value,
                    b_norm=normalize_b(b_text, b_regex),
                    c_norm=normalize_c(c_text, c_regex),
                )
            )
        return rows
    finally:
        if wb is not None:
            wb.close()
