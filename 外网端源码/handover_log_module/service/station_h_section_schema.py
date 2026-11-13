"""H楼交接班日志“业务分类”（sections）模板解析与单元格映射。

H楼没有 A–E 那样的审核文档管线：其 H_handover.xlsx 在交付时由
``_build_station_h_cell_values`` → ``build_station_h_workbook`` 基于模板生成。本模块从
``H楼交接班日志空模板.xlsx`` 解析出可填写的业务分类，提供：

1. ``station_h_section_schema()``：前端可填写分类 schema（名称 / 列 key+label / 模板数据行号，
   **排除培训管理**——培训由审核页 training_rows 单独渲染）；
2. ``station_h_build_section_specs()``：**含培训管理**的全部分类构建规格（带 title/header/数据行号），
   供附件工作簿按模板行序定位、插行与顺延锚点；
3. ``normalize_station_h_sections()``：把前端提交的数据规范成 ``{name: [{"cells": {col_key: str}}]}``；
4. ``build_station_h_section_rows()``：把已保存分类 + 培训记录整理成 ``{分类名: [ {列key: 值} ]}``，
   供附件构建器**逐行渲染**（每条记录主表各占一行）；
5. ``build_station_h_section_cell_values()`` / ``station_h_section_overflow()``：旧的“固定行 + 分类续表”
   映射，仅供**云表（在线表格）兼容路径**使用；附件（xlsx）路径已改为逐行插行，不再写分类续表。

附件路径关键行为：各分类（含培训）按记录数在主表插行/删行，其后培训行、注意事项执行人(H40:H42)、
页脚(H45:H48) 等锚点与打印区域由构建器动态顺延；“三、注意事项交接”执行人为空时自动填当班人员第一个。
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
STATION_H_ATTENTION_SECTION = "三、注意事项交接"

_schema_lock = threading.Lock()
_cached_schema: Optional[List[Dict[str, Any]]] = None
_cached_build_specs: Optional[List[Dict[str, Any]]] = None

TRAINING_SECTION_NAME = "培训管理"
STATION_H_ATTENTION_EXECUTOR_COLUMN = "H"
STATION_H_FOOTER_TITLE = "交接确认"


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


def _parse_build_specs() -> List[Dict[str, Any]]:
    """解析**全部**分类（含培训管理），供附件工作簿按记录数插行渲染。

    与 ``station_h_section_schema`` 的区别：包含培训管理、并额外携带 title_row /
    header_row / template_data_row，以便构建器按模板行序定位、插行与顺延锚点。
    """
    workbook, worksheet = _load_station_h_sheet()
    if worksheet is None:
        return []
    try:
        specs: List[Dict[str, Any]] = []
        for section in parse_category_sections(worksheet):
            columns = build_section_logical_columns(
                worksheet, section, hidden_columns=list(STATION_H_SECTION_HIDDEN_COLUMNS),
            )
            specs.append(
                {
                    "name": section.name,
                    "title_row": section.title_row,
                    "header_row": section.header_row,
                    "template_data_row": section.template_data_row,
                    "columns": [
                        {"key": col.key, "label": col.label, "source_cols": list(col.source_cols)}
                        for col in columns
                    ],
                    "rows": list(range(section.start_row, section.end_row + 1)),
                }
            )
        specs.sort(key=lambda spec: spec["template_data_row"])
        return specs
    finally:
        workbook.close()


def station_h_build_section_specs() -> List[Dict[str, Any]]:
    """全部 H楼分类（含培训管理）的构建规格，按模板数据行升序，解析自模板并缓存。"""
    global _cached_build_specs
    with _schema_lock:
        if _cached_build_specs is None:
            _cached_build_specs = _parse_build_specs()
        return _cached_build_specs


def build_station_h_section_rows(
    sections: Any,
    training_rows: Any,
) -> Dict[str, List[Dict[str, str]]]:
    """把审核页保存的分类数据 + 培训记录整理成 ``{分类名: [ {列key: 值} ]}``（按模板分类顺序）。

    规则（对应「每条记录在主表各占一行」）：
    * 只返回**已保存**的分类：``sections`` 里出现的分类名。未保存的分类不出现在结果里，
      构建器保留其模板默认（``/`` 占位、注意事项常备文本），不插行不删行。
    * 已保存的分类返回其全部记录（含空串），记录条数即该分类在主表应占的行数；
      构建器据此插行/删行，使「新增一条 = 主表新增一行」。
    * 培训管理由 ``training_rows`` 提供（列 key B/D/E/F/G），有记录才返回。
    * 注意事项执行人（H 列）的自动填充由构建器用 ``attention_executor`` 兜底，本函数不强填。
    """
    specs = station_h_build_section_specs()
    raw = sections if isinstance(sections, dict) else {}
    training = training_rows if isinstance(training_rows, list) else []
    result: Dict[str, List[Dict[str, str]]] = {}
    for spec in specs:
        name = spec["name"]
        col_keys = [column["key"] for column in spec["columns"]]
        if name == TRAINING_SECTION_NAME:
            records: List[Dict[str, str]] = []
            for row in training:
                cells = row.get("cells", {}) if isinstance(row, dict) else {}
                if not isinstance(cells, dict):
                    cells = {}
                if not any(str(cells.get(key, "") or "").strip() for key in col_keys):
                    continue
                records.append({key: str(cells.get(key, "") or "") for key in col_keys})
            if records:
                result[name] = records
            continue
        if name not in raw:
            # 未保存：保留模板默认，交由构建器不动这个分类。
            continue
        incoming = raw.get(name)
        incoming_rows = incoming if isinstance(incoming, list) else []
        records = []
        for source in incoming_rows:
            source_cells = source.get("cells", {}) if isinstance(source, dict) else {}
            if not isinstance(source_cells, dict):
                source_cells = {}
            records.append({key: str(source_cells.get(key, "") or "") for key in col_keys})
        result[name] = records
    return result


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

    未填写的分类显示模板行数；已保存的分类保留实际行数（包括空列表）。
    """
    schema = station_h_section_schema()
    raw = sections if isinstance(sections, dict) else {}
    result: Dict[str, List[Dict[str, Any]]] = {}
    for spec in schema:
        name = spec["name"]
        col_keys = [column["key"] for column in spec["columns"]]
        incoming = raw.get(name)
        incoming_rows = incoming if isinstance(incoming, list) else [{} for _ in spec["rows"]]
        rows: List[Dict[str, Any]] = []
        for source in incoming_rows:
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


def build_station_h_section_cell_values(sections: Any, *, clear_attention_defaults: bool = False) -> Dict[str, str]:
    """把分类数据映射到固定位置；已保存的注意事项可显式清除模板默认。"""
    schema = {spec["name"]: spec for spec in station_h_section_schema()}
    normalized = normalize_station_h_sections(sections)
    result: Dict[str, str] = {}
    for name, rows in normalized.items():
        spec = schema.get(name)
        if not spec:
            continue
        for index, row_number in enumerate(spec["rows"]):
            clear_default = clear_attention_defaults and name == STATION_H_ATTENTION_SECTION
            if index >= len(rows) and not clear_default:
                break
            cells = rows[index].get("cells", {}) if index < len(rows) else {}
            for column in spec["columns"]:
                value = str(cells.get(column["key"], "") or "").strip()
                if value or clear_default:
                    result[f"{column['key']}{row_number}"] = value
    return result


def station_h_section_overflow(sections: Any) -> List[Dict[str, Any]]:
    normalized = normalize_station_h_sections(sections)
    overflow = []
    for spec in station_h_section_schema():
        count = len(spec["rows"])
        rows = [
            (index, row["cells"])
            for index, row in enumerate(normalized[spec["name"]][count:], start=count + 1)
            if any(value.strip() for value in row["cells"].values())
        ]
        if rows:
            overflow.append({"name": spec["name"], "columns": spec["columns"], "rows": rows})
    return overflow


__all__ = [
    "STATION_H_EXCLUDED_SECTIONS",
    "STATION_H_ATTENTION_SECTION",
    "STATION_H_ATTENTION_EXECUTOR_COLUMN",
    "STATION_H_FOOTER_TITLE",
    "TRAINING_SECTION_NAME",
    "station_h_section_schema",
    "station_h_build_section_specs",
    "build_station_h_section_rows",
    "station_h_section_overflow",
    "station_h_section_cells",
    "normalize_station_h_sections",
    "build_station_h_section_cell_values",
]
