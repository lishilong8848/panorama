from __future__ import annotations

import copy
import re
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

import openpyxl
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

from app.shared.utils.atomic_file import atomic_save_workbook
from app.shared.utils.file_utils import fallback_missing_windows_drive_path
from pipeline_utils import get_app_dir


_ALL_BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"]
_BUILDING_CODES = {"A楼": "A", "B楼": "B", "C楼": "C", "D楼": "D", "E楼": "E"}
_SUMMARY_SHEET_NAME = "汇总信息表"
_TOP_N = 5
_HEADERS = [
    "序号",
    "楼栋",
    "变压器编号",
    "变压器功率（KW）",
    "HVDC编号",
    "HVDC功率（KW）",
    "列头柜编号编号",
    "列头柜功率（KW）",
    "UPS编号",
    "UPS功率（KW）",
]
_HEADER_FILL = PatternFill("solid", fgColor="FFFF00")
_DATA_FILL = PatternFill("solid", fgColor="C6E0B4")
_THIN_BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)
_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
_TITLE_FONT = Font(name="宋体", size=20, bold=True)
_HEADER_FONT = Font(name="宋体", size=11, bold=True)
_DATA_FONT = Font(name="宋体", size=11)
_DEVICE_PATTERNS = {
    "TRB": re.compile(r"([A-E]-\d{3}-TRB-(?:101|201))", re.IGNORECASE),
    "HVDC": re.compile(r"([A-E]-\d{3}-HVDC-\d+)", re.IGNORECASE),
    "UPS": re.compile(r"([A-E]-\d{3}-UPS-\d+)", re.IGNORECASE),
}
_DEVICE_FALLBACK_PATTERNS = {
    "HVDC": re.compile(r"(?<![A-E]-)(\d{3}-HVDC-\d+)", re.IGNORECASE),
    "UPS": re.compile(r"(?<![A-E]-)(\d{3}-UPS-\d+)", re.IGNORECASE),
}
_BRANCH_COLUMN_PATTERN = re.compile(r"([A-E]-\d{3})-([A-Z])列", re.IGNORECASE)
_BRANCH_COLUMN_FALLBACK_PATTERN = re.compile(r"(?<![A-E]-)(\d{3})-([A-Z])列", re.IGNORECASE)


@dataclass(frozen=True)
class PowerRecord:
    building: str
    identifier: str
    power_kw: float
    source_file: str = ""


@dataclass(frozen=True)
class CapacityPowerGroups:
    transformers: List[PowerRecord]
    hvdcs: List[PowerRecord]
    upss: List[PowerRecord]


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _cell_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _row_text(row: Iterable[Any]) -> str:
    return " ".join(_cell_text(item) for item in row if _cell_text(item))


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_building(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text in _ALL_BUILDINGS:
        return text
    if len(text) == 1 and text in {"A", "B", "C", "D", "E"}:
        return f"{text}楼"
    match = re.search(r"([A-E])\s*楼", text, re.IGNORECASE)
    if match:
        return f"{match.group(1).upper()}楼"
    return str(value or "").strip()


def _building_code(building: str) -> str:
    normalized = _normalize_building(building)
    return _BUILDING_CODES.get(normalized, normalized[:1].upper())


def _dedupe_records(records: Iterable[PowerRecord]) -> List[PowerRecord]:
    best: Dict[str, PowerRecord] = {}
    for record in records:
        key = record.identifier
        current = best.get(key)
        if current is None or record.power_kw > current.power_kw:
            best[key] = record
    return list(best.values())


def _top_records(records: Iterable[PowerRecord], *, building: str, group_name: str) -> List[PowerRecord]:
    sorted_records = sorted(
        _dedupe_records(records),
        key=lambda item: (-item.power_kw, item.identifier),
    )
    if len(sorted_records) < _TOP_N:
        raise RuntimeError(f"{building}{group_name}有效数据不足{_TOP_N}条，当前{len(sorted_records)}条")
    return sorted_records[:_TOP_N]


def _find_max_value_column(rows: List[List[Any]]) -> int | None:
    for row in rows[:8]:
        for index, value in enumerate(row):
            if _cell_text(value) == "最大值":
                return index
    return None


def _power_value(row: List[Any], max_value_col: int | None) -> float | None:
    if max_value_col is not None and max_value_col < len(row):
        value = _to_float(row[max_value_col])
        if value is not None:
            return value
    numeric_values = [_to_float(item) for item in row[4:]]
    numeric_values = [item for item in numeric_values if item is not None]
    if not numeric_values:
        return None
    return max(numeric_values)


def _format_power(value: float) -> float:
    return round(float(value), 2)


def _load_rows(path: Path) -> List[List[Any]]:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style, apply openpyxl's default",
            category=UserWarning,
        )
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = workbook.active
        if hasattr(sheet, "reset_dimensions"):
            sheet.reset_dimensions()
        return [list(row) for row in sheet.iter_rows(values_only=True)]
    finally:
        workbook.close()


def _extract_device_id(row: List[Any], device_kind: str, *, building: str) -> str:
    kind = str(device_kind or "").strip().upper()
    combined = _row_text(row)
    pattern = _DEVICE_PATTERNS.get(kind)
    if pattern is not None:
        match = pattern.search(combined)
        if match:
            return match.group(1).upper()
    fallback_pattern = _DEVICE_FALLBACK_PATTERNS.get(kind)
    if fallback_pattern is not None:
        match = fallback_pattern.search(combined)
        if match:
            return f"{_building_code(building)}-{match.group(1).upper()}"
    return ""


def _transformer_label(device_id: str) -> str:
    match = re.match(r"([A-E]-\d{3})-TRB-(101|201)$", str(device_id or "").strip(), re.IGNORECASE)
    if not match:
        return ""
    transformer_side = "A" if match.group(2) == "101" else "B"
    return f"{match.group(1).upper()}-{transformer_side}变压器容量"


def _branch_column_key(row: List[Any], *, building: str) -> str:
    combined = _row_text(row[:3])
    match = _BRANCH_COLUMN_PATTERN.search(combined)
    if match:
        return f"{match.group(1).upper()}-{match.group(2).upper()}列"
    fallback = _BRANCH_COLUMN_FALLBACK_PATTERN.search(combined)
    if fallback:
        return f"{_building_code(building)}-{fallback.group(1)}-{fallback.group(2).upper()}列"
    return ""


class Top5PowerReportService:
    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        self.runtime_config = runtime_config if isinstance(runtime_config, dict) else {}

    @staticmethod
    def all_buildings() -> List[str]:
        return list(_ALL_BUILDINGS)

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "template": {
                "source_path": "TOP5功率报表空模板.xlsx",
                "output_dir": r"D:\QLDownload\TOP5功率文件生成",
                "file_name_pattern": "TOP5功率文件_{timestamp}.xlsx",
            },
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        handover_cfg = self.runtime_config.get("handover_log", {})
        if not isinstance(handover_cfg, dict):
            handover_cfg = {}
        raw_cfg = handover_cfg.get("top5_power_report", {})
        cfg = _deep_merge(self._defaults(), raw_cfg if isinstance(raw_cfg, dict) else {})
        cfg["enabled"] = bool(cfg.get("enabled", True))
        template = cfg.get("template", {}) if isinstance(cfg.get("template", {}), dict) else {}
        template["source_path"] = str(template.get("source_path", "") or "").strip() or "TOP5功率报表空模板.xlsx"
        template["output_dir"] = (
            str(template.get("output_dir", "") or "").strip() or r"D:\QLDownload\TOP5功率文件生成"
        )
        template["file_name_pattern"] = (
            str(template.get("file_name_pattern", "") or "").strip() or "TOP5功率文件_{timestamp}.xlsx"
        )
        cfg["template"] = template
        return cfg

    def is_enabled(self) -> bool:
        return bool(self._normalize_cfg().get("enabled", True))

    def _app_dir(self) -> Path:
        return get_app_dir()

    def _resolve_path(self, value: str) -> Path:
        path = Path(str(value or "").strip())
        if path.is_absolute():
            return fallback_missing_windows_drive_path(path, app_dir=self._app_dir())
        return self._app_dir() / path

    def resolve_template_path(self) -> Path:
        return self._resolve_path(self._normalize_cfg()["template"]["source_path"])

    def resolve_output_dir(self) -> Path:
        output_dir = self._resolve_path(self._normalize_cfg()["template"]["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def _build_output_path(self) -> Path:
        cfg = self._normalize_cfg()["template"]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = str(cfg["file_name_pattern"]).format(
            timestamp=timestamp,
            date=datetime.now().strftime("%Y%m%d"),
        )
        if not file_name.lower().endswith(".xlsx"):
            file_name = f"{file_name}.xlsx"
        candidate = self.resolve_output_dir() / file_name
        index = 2
        while candidate.exists():
            candidate = candidate.with_name(f"{candidate.stem}_{index}{candidate.suffix}")
            index += 1
        return candidate

    @staticmethod
    def extract_capacity_records(path: Path | str, *, building: str) -> CapacityPowerGroups:
        source_path = Path(path)
        rows = _load_rows(source_path)
        max_value_col = _find_max_value_column(rows)
        transformers: List[PowerRecord] = []
        hvdcs: List[PowerRecord] = []
        upss: List[PowerRecord] = []
        current_hvdc_id = ""
        current_ups_id = ""
        normalized_building = _normalize_building(building)

        for row in rows:
            metric_name = _cell_text(row[3] if len(row) > 3 else "")
            hvdc_id = _extract_device_id(row, "HVDC", building=normalized_building)
            if hvdc_id:
                current_hvdc_id = hvdc_id
            ups_id = _extract_device_id(row, "UPS", building=normalized_building)
            if ups_id:
                current_ups_id = ups_id

            transformer_id = _extract_device_id(row, "TRB", building=normalized_building)
            if transformer_id and "有功功率" in metric_name:
                label = _transformer_label(transformer_id)
                value = _power_value(row, max_value_col)
                if label and value is not None:
                    transformers.append(PowerRecord(normalized_building, label, value, str(source_path)))
                continue

            if metric_name == "直流总功率_KW":
                value = _power_value(row, max_value_col)
                if current_hvdc_id and value is not None:
                    hvdcs.append(PowerRecord(normalized_building, current_hvdc_id, value, str(source_path)))
                continue

            if "输出总有功功率" in metric_name:
                value = _power_value(row, max_value_col)
                if current_ups_id and value is not None:
                    upss.append(PowerRecord(normalized_building, f"{current_ups_id}_UPS", value, str(source_path)))

        return CapacityPowerGroups(
            transformers=_top_records(transformers, building=normalized_building, group_name="变压器"),
            hvdcs=_top_records(hvdcs, building=normalized_building, group_name="HVDC"),
            upss=_top_records(upss, building=normalized_building, group_name="UPS"),
        )

    @staticmethod
    def extract_branch_records(path: Path | str, *, building: str) -> List[PowerRecord]:
        source_path = Path(path)
        rows = _load_rows(source_path)
        normalized_building = _normalize_building(building)
        sums_by_column: Dict[str, List[float]] = {}

        for row in rows[3:]:
            metric_name = _cell_text(row[2] if len(row) > 2 else "")
            if "支路功率" not in metric_name:
                continue
            column_key = _branch_column_key(row, building=normalized_building)
            if not column_key:
                continue
            values = [_to_float(value) or 0.0 for value in row[3:]]
            if not values:
                continue
            current = sums_by_column.setdefault(column_key, [0.0] * len(values))
            if len(current) < len(values):
                current.extend([0.0] * (len(values) - len(current)))
            for index, value in enumerate(values):
                current[index] += value

        records = [
            PowerRecord(normalized_building, f"{column_key}功率和", max(values), str(source_path))
            for column_key, values in sums_by_column.items()
            if values
        ]
        return _top_records(records, building=normalized_building, group_name="列头柜")

    @staticmethod
    def _entries_by_building(entries: List[Dict[str, Any]], *, label: str) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict):
                continue
            building = _normalize_building(entry.get("building"))
            file_path = str(entry.get("file_path", "") or "").strip()
            if not building or not file_path:
                continue
            result[building] = {**entry, "building": building, "file_path": file_path}
        missing = [building for building in _ALL_BUILDINGS if building not in result]
        if missing:
            raise RuntimeError(f"缺少{label}最新源文件: {', '.join(missing)}")
        return result

    @staticmethod
    def _style_cell(cell, *, is_header: bool = False, is_data_fill: bool = False) -> None:
        cell.border = _THIN_BORDER
        cell.alignment = _CENTER
        cell.font = _HEADER_FONT if is_header else _DATA_FONT
        if is_header:
            cell.fill = _HEADER_FILL
        elif is_data_fill:
            cell.fill = _DATA_FILL

    def _create_summary_workbook(self):
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.title = _SUMMARY_SHEET_NAME
        self._format_summary_sheet(sheet)
        return workbook

    def _load_workbook_for_output(self):
        template_path = self.resolve_template_path()
        if template_path.exists():
            workbook = openpyxl.load_workbook(template_path)
            if _SUMMARY_SHEET_NAME not in workbook.sheetnames:
                workbook.active.title = _SUMMARY_SHEET_NAME
            return workbook
        return self._create_summary_workbook()

    def _format_summary_sheet(self, sheet) -> None:
        if "A1:J1" not in [str(range_ref) for range_ref in sheet.merged_cells.ranges]:
            sheet.merge_cells("A1:J1")
        title_cell = sheet["A1"]
        title_cell.value = "高功率设备排名（TOP5）"
        title_cell.font = _TITLE_FONT
        title_cell.alignment = _CENTER
        for column, header in enumerate(_HEADERS, start=1):
            cell = sheet.cell(row=2, column=column, value=header)
            self._style_cell(cell, is_header=True)
        widths = {
            "A": 8,
            "B": 10,
            "C": 24,
            "D": 18,
            "E": 24,
            "F": 18,
            "G": 28,
            "H": 18,
            "I": 24,
            "J": 18,
        }
        for column, width in widths.items():
            sheet.column_dimensions[column].width = width
        sheet.row_dimensions[1].height = 30
        sheet.row_dimensions[2].height = 22

    def _prepare_summary_sheet(self, workbook):
        sheet = workbook[_SUMMARY_SHEET_NAME] if _SUMMARY_SHEET_NAME in workbook.sheetnames else workbook.active
        sheet.title = _SUMMARY_SHEET_NAME
        for worksheet in list(workbook.worksheets):
            if worksheet.title != _SUMMARY_SHEET_NAME:
                workbook.remove(worksheet)
        self._format_summary_sheet(sheet)
        for row_index in range(3, max(sheet.max_row, 27) + 1):
            for column_index in range(1, 11):
                sheet.cell(row=row_index, column=column_index).value = None
        return sheet

    def _write_summary_rows(
        self,
        sheet,
        *,
        capacity_by_building: Dict[str, CapacityPowerGroups],
        branch_by_building: Dict[str, List[PowerRecord]],
    ) -> None:
        row_index = 3
        sequence = 1
        for building in _ALL_BUILDINGS:
            groups = capacity_by_building[building]
            branches = branch_by_building[building]
            for index in range(_TOP_N):
                values = [
                    sequence,
                    _building_code(building),
                    groups.transformers[index].identifier,
                    _format_power(groups.transformers[index].power_kw),
                    groups.hvdcs[index].identifier,
                    _format_power(groups.hvdcs[index].power_kw),
                    branches[index].identifier,
                    _format_power(branches[index].power_kw),
                    groups.upss[index].identifier,
                    _format_power(groups.upss[index].power_kw),
                ]
                for column_index, value in enumerate(values, start=1):
                    cell = sheet.cell(row=row_index, column=column_index, value=value)
                    self._style_cell(cell, is_data_fill=column_index >= 3)
                    if column_index in {4, 6, 8, 10}:
                        cell.number_format = "0.##"
                row_index += 1
                sequence += 1

    def _append_source_sheet(self, workbook, *, sheet_name: str, source_path: Path) -> int:
        if sheet_name in workbook.sheetnames:
            workbook.remove(workbook[sheet_name])
        sheet = workbook.create_sheet(sheet_name)
        row_count = 0
        for row in _load_rows(source_path):
            sheet.append(row)
            row_count += 1
        sheet.freeze_panes = "A4"
        return row_count

    def run(
        self,
        *,
        capacity_entries: List[Dict[str, Any]],
        branch_entries: List[Dict[str, Any]],
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        if not self.is_enabled():
            raise RuntimeError("TOP5功率文件生成已禁用")

        started_at = datetime.now()
        emit_log("[TOP5功率文件生成] 开始读取共享源文件")
        capacity_entry_map = self._entries_by_building(capacity_entries, label="交接班容量报表")
        branch_entry_map = self._entries_by_building(branch_entries, label="支路功率")

        capacity_by_building: Dict[str, CapacityPowerGroups] = {}
        branch_by_building: Dict[str, List[PowerRecord]] = {}
        for building in _ALL_BUILDINGS:
            capacity_path = Path(capacity_entry_map[building]["file_path"])
            branch_path = Path(branch_entry_map[building]["file_path"])
            emit_log(f"[TOP5功率文件生成] 解析{building}: capacity={capacity_path.name}, branch={branch_path.name}")
            capacity_by_building[building] = self.extract_capacity_records(capacity_path, building=building)
            branch_by_building[building] = self.extract_branch_records(branch_path, building=building)

        output_path = self._build_output_path()
        workbook = self._load_workbook_for_output()
        try:
            summary_sheet = self._prepare_summary_sheet(workbook)
            self._write_summary_rows(
                summary_sheet,
                capacity_by_building=capacity_by_building,
                branch_by_building=branch_by_building,
            )
            source_sheet_rows: Dict[str, int] = {}
            for building in _ALL_BUILDINGS:
                code = _building_code(building)
                row_count = self._append_source_sheet(
                    workbook,
                    sheet_name=f"容量_{code}",
                    source_path=Path(capacity_entry_map[building]["file_path"]),
                )
                source_sheet_rows[f"容量_{code}"] = row_count
            for building in _ALL_BUILDINGS:
                code = _building_code(building)
                row_count = self._append_source_sheet(
                    workbook,
                    sheet_name=f"支路功率_{code}",
                    source_path=Path(branch_entry_map[building]["file_path"]),
                )
                source_sheet_rows[f"支路功率_{code}"] = row_count
            atomic_save_workbook(workbook, output_path)
        finally:
            workbook.close()

        finished_at = datetime.now()
        source_files = {
            "capacity": {
                building: {
                    "building": building,
                    "file_path": capacity_entry_map[building]["file_path"],
                    "file_name": Path(capacity_entry_map[building]["file_path"]).name,
                    "bucket_key": str(capacity_entry_map[building].get("bucket_key", "") or "").strip(),
                }
                for building in _ALL_BUILDINGS
            },
            "branch_power": {
                building: {
                    "building": building,
                    "file_path": branch_entry_map[building]["file_path"],
                    "file_name": Path(branch_entry_map[building]["file_path"]).name,
                    "bucket_key": str(branch_entry_map[building].get("bucket_key", "") or "").strip(),
                }
                for building in _ALL_BUILDINGS
            },
        }
        result = {
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": finished_at.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "ok",
            "report_type": "top5_power_report",
            "output_file": str(output_path),
            "file_name": output_path.name,
            "output_dir": str(output_path.parent),
            "summary_row_count": len(_ALL_BUILDINGS) * _TOP_N,
            "source_sheet_count": 10,
            "source_sheet_rows": source_sheet_rows,
            "source_files": source_files,
        }
        emit_log(
            "[TOP5功率文件生成] 文件生成完成: "
            f"output={output_path}, summary_rows={result['summary_row_count']}, source_sheets=10"
        )
        return result
