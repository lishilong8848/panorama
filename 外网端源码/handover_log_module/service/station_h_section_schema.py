"""H楼交接班日志“业务分类”（sections）模板解析与单元格映射。

H楼没有 A–E 那样的审核文档管线：其 H_handover.xlsx 在交付时由扁平单元格字典
（``_build_station_h_cell_values`` → ``build_station_h_workbook``）基于模板生成。本模块从
``H楼交接班日志空模板.xlsx`` 解析出可填写的业务分类，提供三件事：

1. ``station_h_section_schema()``：分类 schema（名称 / 列 key+label / 模板数据行号），供前端渲染与保存；
2. ``normalize_station_h_sections()``：把前端提交的数据规范成 ``{name: [{"cells": {col_key: str}}]}``，
   行数与模板一致、列 key 合法，杜绝越界写入；
3. ``build_station_h_section_cell_values()``：把已保存数据映射为“绝对单元格 -> 值”的字典，
   **仅含非空值**——空值不写，保留模板默认（``/`` 占位、注意事项常备文本、既有自动执行人）。

关键安全约束：分类只填入模板既有数据行，**绝不插行/删行**，因此不会位移培训行(31)、
注意事项执行人(H40:H42)、页脚(H45:H48) 等既有扁平单元格。“培训管理”已由 training_rows 单独
处理（写 B31..G31），在此排除，避免重复与冲突。
"""
from __future__ import annotations

import threading
import warnings
from typing import Any, Dict, List, Optional, Tuple

import openpyxl

from handover_log_module.core.section_layout import (
    build_section_logical_columns,
    parse_category_sections,
)
from handover_log_module.service.handover_attachment_workbook_builder import (
    STATION_H_DEFAULT_SHEET_TITLE,
    default_station_h_template_path,
)

# 培训管理已由 H 审核页 training_rows 单独处理（写 B31..G31），不纳入分类填写区。
STATION_H_EXCLUDED_SECTIONS = frozenset({"培训管理"})
STATION_H_SECTION_HIDDEN_COLUMNS = ("I",)

_schema_lock = threading.Lock()
_cached_schema: Optional[List[Dict[str, Any]]] = None


def _load_station_h_sheet() -> Tuple[Any, Any]:
    template = default_station_h_template_path()
    if not template.is_file():
        return None, None
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        workbook = openpyxl.load_workbook(str(template), data_only=False)
    if STATION_H_DEFAULT_SHEET_TITLE in workbook.sheetnames:
        return workbook, workbook[STATION_H_DEFAULT_SHEET_TITLE]
    if len(workbook.sheetnames) == 1:
        return workbook, workbook.worksheets[0]
    workbook.close()
    return None, None


def _parse_schema() -> List[Dict[str, Any]]:
    workbook, worksheet = _load_station_h_sheet()
    if worksheet is None:
        return []
    try:
        schema: List[Dict[str, Any]] = []
        for section in parse_category_sections(worksheet):
            if section.name in STATION_H_EXCLUDED_SECTIONS:
                continue
            columns = build_section_logical_columns(
                worksheet, section, hidden_columns=list(STATION_H_SECTION_HIDDEN_COLUMNS),
            )
            schema.append(
                {
                    "name": section.name,
                    "columns": [
                        {"key": col.key, "label": col.label, "source_cols": list(col.source_cols)}
                        for col in columns
                    ],
                    "rows": list(range(section.start_row, section.end_row + 1)),
                }
            )
        return schema
    finally:
        workbook.close()


def station_h_section_schema() -> List[Dict[str, Any]]:
    """H楼可填写业务分类 schema（解析自模板并缓存）：[{name, columns:[{key,label,source_cols}], rows:[int]}]。"""
    global _cached_schema
    with _schema_lock:
        if _cached_schema is None:
            _cached_schema = _parse_schema()
        return _cached_schema


def station_h_section_cells() -> frozenset:
    """所有分类数据行涉及的绝对单元格锚点集合（用于云表白名单并集，与模板保持一致）。"""
    cells = set()
    for section in station_h_section_schema():
        for row in section["rows"]:
            for column in section["columns"]:
                cells.add(f"{column['key']}{row}")
    return frozenset(cells)


def normalize_station_h_sections(sections: Any) -> Dict[str, List[Dict[str, Any]]]:
    """规范前端提交的分类数据为 {name: [{"cells": {col_key: str}}]}。

    行数严格等于模板数据行数、列 key 严格取自模板；越界行/非法列一律丢弃，值统一转字符串。
    """
    schema = station_h_section_schema()
    raw = sections if isinstance(sections, dict) else {}
    result: Dict[str, List[Dict[str, Any]]] = {}
    for spec in schema:
        name = spec["name"]
        col_keys = [column["key"] for column in spec["columns"]]
        incoming = raw.get(name)
        incoming_rows = incoming if isinstance(incoming, list) else []
        rows: List[Dict[str, Any]] = []
        for index in range(len(spec["rows"])):
            source = incoming_rows[index] if index < len(incoming_rows) else {}
            source_cells = source.get("cells", {}) if isinstance(source, dict) else {}
            if not isinstance(source_cells, dict):
                source_cells = {}
            cells: Dict[str, str] = {}
            for key in col_keys:
                value = source_cells.get(key, "")
                cells[key] = "" if value is None else str(value)
            rows.append({"cells": cells})
        result[name] = rows
    return result


def build_station_h_section_cell_values(sections: Any) -> Dict[str, str]:
    """把已保存分类数据映射为“绝对单元格 -> 值”，仅含非空值（空值保留模板默认，不覆盖）。"""
    schema = {spec["name"]: spec for spec in station_h_section_schema()}
    normalized = normalize_station_h_sections(sections)
    result: Dict[str, str] = {}
    for name, rows in normalized.items():
        spec = schema.get(name)
        if not spec:
            continue
        for index, row_number in enumerate(spec["rows"]):
            if index >= len(rows):
                break
            cells = rows[index].get("cells", {})
            for column in spec["columns"]:
                value = str(cells.get(column["key"], "") or "").strip()
                if value:
                    result[f"{column['key']}{row_number}"] = value
    return result


__all__ = [
    "STATION_H_EXCLUDED_SECTIONS",
    "station_h_section_schema",
    "station_h_section_cells",
    "normalize_station_h_sections",
    "build_station_h_section_cell_values",
]
