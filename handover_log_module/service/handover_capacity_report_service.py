from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

import openpyxl
from openpyxl.utils import get_column_letter, range_boundaries

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from app.modules.sheet_import.core.field_value_converter import parse_timestamp_ms
from app.shared.utils.atomic_file import atomic_save_workbook
from handover_log_module.core.shift_window import build_duty_window, parse_duty_date
from handover_log_module.core.normalizers import format_number
from handover_log_module.repository.excel_reader import load_rows
from handover_log_module.repository.template_writer import build_output_filename
from handover_log_module.service import capacity_report_a, capacity_report_b, capacity_report_c, capacity_report_d, capacity_report_e
from handover_log_module.service.capacity_report_common import build_capacity_template_snapshot
from handover_log_module.service.day_metric_bitable_export_service import DayMetricBitableExportService
from handover_log_module.service.handover_capacity_oil_cache_service import HandoverCapacityOilCacheService
from pipeline_utils import get_app_dir


_BUILDER_BY_BUILDING = {
    "A楼": capacity_report_a.build_capacity_cells,
    "B楼": capacity_report_b.build_capacity_cells,
    "C楼": capacity_report_c.build_capacity_cells,
    "D楼": capacity_report_d.build_capacity_cells,
    "E楼": capacity_report_e.build_capacity_cells,
}
_RUNNING_MODE_TEXTS = {"制冷", "预冷", "板换"}
_NIGHT_WATER_SOURCE = {
    "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
    "table_id": "tblz4TkZqrUJB90y",
    "page_size": 500,
    "max_records": 20000,
    "fields": {
        "date": "执行日期",
        "building": "楼栋",
        "water_total": "当日耗水总量（实际）",
    },
}


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _with_index(path: Path, idx: int) -> Path:
    if idx <= 1:
        return path
    return path.with_name(f"{path.stem}_{idx}{path.suffix}")


def _field_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("text", "name", "value", "label"):
            text = str(value.get(key, "") or "").strip()
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts = [_field_text(item) for item in value]
        return "、".join([item for item in parts if item])
    return str(value).strip()


def _field_text_with_option_map(value: Any, option_map: Dict[str, str]) -> str:
    text = _field_text(value)
    if not text:
        return ""
    return str(option_map.get(text, text)).strip()


def _parse_datetime(value: Any) -> datetime | None:
    timestamp_ms = parse_timestamp_ms(value, tz_offset_hours=8)
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000)


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if number != number or number in {float("inf"), float("-inf")}:
            return None
        return number
    text = _field_text(value).replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _extract_building_code(value: Any) -> str:
    match = re.search(r"([A-Za-z])", _text(value))
    return match.group(1).upper() if match else ""


def _normalize_building_text(value: Any) -> str:
    return _text(value).replace(" ", "").casefold()


def _build_merged_anchor_map(sheet: Any) -> Dict[str, str]:
    anchor_map: Dict[str, str] = {}
    for merged_range in getattr(sheet.merged_cells, "ranges", []):
        min_col, min_row, max_col, max_row = range_boundaries(str(merged_range))
        anchor = f"{get_column_letter(min_col)}{min_row}"
        for row_idx in range(min_row, max_row + 1):
            for col_idx in range(min_col, max_col + 1):
                coord = f"{get_column_letter(col_idx)}{row_idx}"
                anchor_map[coord] = anchor
    return anchor_map


def _write_cells_with_merged_support(sheet: Any, cell_values: Dict[str, Any]) -> None:
    merged_anchor_map = _build_merged_anchor_map(sheet)
    resolved_values: Dict[str, Any] = {}
    source_coords: Dict[str, str] = {}
    for cell_name, value in (cell_values or {}).items():
        normalized_cell = _text(cell_name).upper()
        if not normalized_cell:
            continue
        resolved_cell = merged_anchor_map.get(normalized_cell, normalized_cell)
        if resolved_cell in resolved_values and resolved_values[resolved_cell] != value:
            raise RuntimeError(
                "merged_target_conflict: "
                f"{normalized_cell}->{resolved_cell} conflicts with {source_coords.get(resolved_cell, resolved_cell)}"
            )
        resolved_values[resolved_cell] = value
        source_coords[resolved_cell] = normalized_cell
    for resolved_cell, value in resolved_values.items():
        sheet[resolved_cell] = value


class HandoverCapacityReportService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config if isinstance(config, dict) else {}
        self._oil_cache_service = HandoverCapacityOilCacheService(self.config)
        self._day_metric_export_service = DayMetricBitableExportService(self.config)

    def _capacity_cfg(self) -> Dict[str, Any]:
        raw = self.config.get("capacity_report", {})
        return raw if isinstance(raw, dict) else {}

    def _template_cfg(self) -> Dict[str, Any]:
        raw = self._capacity_cfg().get("template", {})
        return raw if isinstance(raw, dict) else {}

    def _parsing_cfg(self) -> Dict[str, Any]:
        raw = self._capacity_cfg().get("parsing", {})
        return raw if isinstance(raw, dict) else {}

    def _chiller_mode_cfg(self) -> Dict[str, Any]:
        raw = self.config.get("chiller_mode", {})
        return raw if isinstance(raw, dict) else {}

    @staticmethod
    def _resolve_template_path(raw_path: str) -> Path:
        path = Path(str(raw_path or "").strip())
        if path.is_absolute():
            return path
        return Path(__file__).resolve().parents[2] / path

    def resolve_template_path(self) -> Path:
        source_path = _text(self._template_cfg().get("source_path"))
        if not source_path:
            raise ValueError("handover_log.capacity_report.template.source_path 未配置")
        return self._resolve_template_path(source_path)

    def resolve_output_dir(self) -> Path:
        output_dir = Path(_text(self._template_cfg().get("output_dir")))
        if not str(output_dir):
            raise ValueError("handover_log.capacity_report.template.output_dir 未配置")
        if not output_dir.is_absolute():
            output_dir = get_app_dir() / output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def _build_output_path(self, *, building: str, duty_date: str) -> Path:
        template_cfg = self._template_cfg()
        pattern = _text(template_cfg.get("file_name_pattern")) or "{building}_{date}_交接班容量报表.xlsx"
        date_format = _text(template_cfg.get("date_format")) or "%Y%m%d"
        duty_day = parse_duty_date(duty_date)
        file_name = build_output_filename(
            building=building,
            file_name_pattern=pattern,
            date_format=date_format,
            date_ref=datetime(duty_day.year, duty_day.month, duty_day.day, 0, 0, 0),
        )
        return self.resolve_output_dir() / file_name

    def _next_available_output_path(self, *, building: str, duty_date: str) -> Path:
        base_path = self._build_output_path(building=building, duty_date=duty_date)
        for idx in range(1, 1000):
            candidate = _with_index(base_path, idx)
            if not candidate.exists():
                return candidate
        raise RuntimeError(f"交接班容量报表输出文件序号已用尽: {base_path}")

    @staticmethod
    def _sheet_name(workbook: openpyxl.Workbook, configured_name: str) -> str:
        sheet_name = _text(configured_name)
        if sheet_name and sheet_name in workbook.sheetnames:
            return sheet_name
        return workbook.sheetnames[0]

    @staticmethod
    def _read_handover_cells(output_file: str, cells: List[str], sheet_name: str = "") -> Dict[str, str]:
        workbook = openpyxl.load_workbook(output_file, data_only=True, read_only=True)
        try:
            ws = workbook[sheet_name] if sheet_name and sheet_name in workbook.sheetnames else workbook[workbook.sheetnames[0]]
            values: Dict[str, str] = {}
            for cell_name in cells:
                normalized = _text(cell_name).upper()
                if not normalized:
                    continue
                values[normalized] = _text(ws[normalized].value)
            return values
        finally:
            workbook.close()

    @staticmethod
    def _raw_value_text(value: Any) -> str:
        return _text(value)

    def _load_capacity_rows(self, source_file: str) -> List[Any]:
        return load_rows(
            source_file,
            self._parsing_cfg(),
            self.config.get("normalize", {}) if isinstance(self.config.get("normalize", {}), dict) else {},
        )

    def _extract_oil_values(self, rows: List[Any]) -> Dict[str, str]:
        first_candidates = ["1#油罐容积", "油罐1液位", "1#油罐体积"]
        second_candidates = ["2#油罐容积", "油罐2液位", "2#油罐体积"]

        def _find_value(candidates: List[str]) -> str:
            for candidate in candidates:
                for row in rows:
                    if _text(getattr(row, "c_text", "")) != "燃油自控系统":
                        continue
                    if _text(getattr(row, "d_name", "")) != candidate:
                        continue
                    value_text = self._raw_value_text(getattr(row, "e_raw", None))
                    if value_text:
                        return value_text
            return ""

        return {"first": _find_value(first_candidates), "second": _find_value(second_candidates)}

    def _normalize_alarm_summary(self, payload: Dict[str, Any] | None) -> Dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        return {
            "total_count": int(data.get("total_count", 0) or 0),
            "unrecovered_count": int(data.get("unrecovered_count", 0) or 0),
            "accept_description": _text(data.get("accept_description")) or "/",
        }

    @staticmethod
    def _extract_hvdc_position_code_from_text(value: Any) -> str:
        match = re.search(r"([A-E]-\d{3}-HVDC-\d{3})", _text(value), flags=re.IGNORECASE)
        return _text(match.group(1)).upper() if match else ""

    @classmethod
    def _extract_hvdc_position_code(cls, origin_payload: Dict[str, Any] | None) -> str:
        payload = origin_payload if isinstance(origin_payload, dict) else {}
        for field_name in ("d_name", "b_text", "c_text", "b_norm", "c_norm"):
            extracted = cls._extract_hvdc_position_code_from_text(payload.get(field_name))
            if extracted:
                return extracted
        return ""

    def _resolve_hvdc_text(
        self,
        *,
        resolved_values_by_id: Dict[str, Any] | None,
        metric_origin_context: Dict[str, Any] | None,
        hvdc_source_d_name: Any = None,
    ) -> Dict[str, str]:
        resolved = resolved_values_by_id if isinstance(resolved_values_by_id, dict) else {}
        raw_value = _text(resolved.get("hvdc_load_max"))
        if raw_value and "%" not in raw_value:
            raw_value = f"{raw_value}%"
        source_d_name_text = _text(hvdc_source_d_name)
        position_code = self._extract_hvdc_position_code_from_text(hvdc_source_d_name)
        origin_context = metric_origin_context if isinstance(metric_origin_context, dict) else {"by_metric_id": {}}
        if not position_code:
            origin_payload = origin_context.get("by_metric_id", {}).get("hvdc_load_max", {})
            position_code = self._extract_hvdc_position_code(origin_payload)
            if not source_d_name_text:
                source_d_name_text = _text(origin_payload.get("d_name"))
        if raw_value and position_code:
            formatted = f"{raw_value}/{position_code}"
        elif raw_value:
            formatted = f"{raw_value}/"
        else:
            formatted = position_code
        return {
            "raw_value": raw_value,
            "source_d_name": source_d_name_text,
            "position_code": position_code,
            "formatted": formatted,
        }

    @staticmethod
    def _normalize_mode_code(value: Any, value_map: Dict[str, str]) -> str:
        raw = _text(value)
        if not raw:
            return ""
        if raw in value_map:
            return raw
        try:
            number = float(raw)
        except ValueError:
            number = None
        if number is not None and int(number) == number:
            key = str(int(number))
            if key in value_map:
                return key
        lowered = raw.casefold()
        for key, text in value_map.items():
            if _text(text).casefold() == lowered:
                return _text(key)
        return ""

    def _resolve_running_units(self, resolved_values_by_id: Dict[str, Any] | None) -> Dict[str, List[Dict[str, Any]]]:
        resolved = resolved_values_by_id if isinstance(resolved_values_by_id, dict) else {}
        chiller_cfg = self._chiller_mode_cfg()
        raw_value_map = chiller_cfg.get("value_map", {"1": "制冷", "2": "预冷", "3": "板换", "4": "停机"})
        value_map = (
            {str(key).strip(): str(value).strip() for key, value in raw_value_map.items() if _text(key)}
            if isinstance(raw_value_map, dict)
            else {"1": "制冷", "2": "预冷", "3": "板换", "4": "停机"}
        )
        running: Dict[str, List[Dict[str, Any]]] = {"west": [], "east": []}
        for unit_number in range(1, 7):
            metric_key = f"chiller_mode_{unit_number}"
            mode_code = self._normalize_mode_code(resolved.get(metric_key), value_map)
            mode_text = _text(value_map.get(mode_code))
            if mode_text not in _RUNNING_MODE_TEXTS:
                continue
            zone = "west" if unit_number <= 3 else "east"
            running[zone].append(
                {
                    "unit": unit_number,
                    "metric_key": metric_key,
                    "mode_code": mode_code,
                    "mode_text": mode_text,
                }
            )
        running["west"].sort(key=lambda item: int(item.get("unit", 0) or 0))
        running["east"].sort(key=lambda item: int(item.get("unit", 0) or 0))
        return running

    @staticmethod
    def _extract_option_map_from_field(field_def: Dict[str, Any]) -> Dict[str, str]:
        if not isinstance(field_def, dict):
            return {}
        property_cfg = field_def.get("property", {})
        option_containers: List[Any] = []
        if isinstance(property_cfg, dict):
            option_containers.append(property_cfg.get("options"))
            type_cfg = property_cfg.get("type")
            if isinstance(type_cfg, dict):
                ui_property = type_cfg.get("ui_property")
                if isinstance(ui_property, dict):
                    option_containers.append(ui_property.get("options"))
        option_map: Dict[str, str] = {}
        for options in option_containers:
            if not isinstance(options, list):
                continue
            for option in options:
                if not isinstance(option, dict):
                    continue
                option_id = str(option.get("id", "") or "").strip()
                option_name = str(option.get("name", "") or "").strip()
                if option_id:
                    option_map[option_id] = option_name
        return option_map

    def _load_field_option_maps(
        self,
        *,
        client: FeishuBitableClient,
        table_id: str,
        target_fields: List[str],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Dict[str, str]]:
        field_names = [str(name or "").strip() for name in (target_fields or []) if str(name or "").strip()]
        if not field_names or not str(table_id or "").strip():
            return {}
        try:
            field_defs = client.list_fields(table_id=table_id, page_size=200)
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][容量报表][夜班耗水] 字段定义读取失败 building=全局, error={exc}")
            return {}
        output: Dict[str, Dict[str, str]] = {}
        for field_def in field_defs:
            if not isinstance(field_def, dict):
                continue
            field_name = str(
                field_def.get("field_name")
                or field_def.get("name")
                or field_def.get("title")
                or ""
            ).strip()
            if not field_name or field_name not in field_names:
                continue
            output[field_name] = self._extract_option_map_from_field(field_def)
        return output

    def _new_night_water_client(self) -> FeishuBitableClient:
        global_feishu = self.config.get("_global_feishu", {})
        if not isinstance(global_feishu, dict):
            global_feishu = {}
        app_id = str(global_feishu.get("app_id", "") or "").strip()
        app_secret = str(global_feishu.get("app_secret", "") or "").strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")
        app_token = str(_NIGHT_WATER_SOURCE.get("app_token", "") or "").strip()
        table_id = str(_NIGHT_WATER_SOURCE.get("table_id", "") or "").strip()
        if not app_token or not table_id:
            raise ValueError("夜班耗水多维配置缺失: app_token/table_id")
        return FeishuBitableClient(
            app_id=app_id,
            app_secret=app_secret,
            app_token=app_token,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
        )

    @staticmethod
    def _matches_building(record_building: Any, target_building: str) -> bool:
        record_text = _field_text(record_building)
        target_text = _text(target_building)
        if not record_text or not target_text:
            return False
        if _normalize_building_text(record_text) == _normalize_building_text(target_text):
            return True
        return bool(
            _extract_building_code(record_text)
            and _extract_building_code(record_text) == _extract_building_code(target_text)
        )

    def _query_night_water_summary(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None],
    ) -> Dict[str, str]:
        if _text(duty_shift).lower() != "night":
            return {}

        download_cfg = self.config.get("download", {})
        shift_windows = download_cfg.get("shift_windows", {}) if isinstance(download_cfg, dict) else {}
        duty_window = build_duty_window(
            duty_date=duty_date,
            duty_shift=duty_shift,
            shift_windows=shift_windows,
        )
        duty_day = parse_duty_date(duty_date)
        month_start_dt = datetime(duty_day.year, duty_day.month, 1, 0, 0, 0)
        duty_end_dt = datetime.strptime(duty_window.end_time, "%Y-%m-%d %H:%M:%S")

        fields_cfg = _NIGHT_WATER_SOURCE.get("fields", {})
        if not isinstance(fields_cfg, dict):
            fields_cfg = {}
        date_field = str(fields_cfg.get("date", "") or "").strip()
        building_field = str(fields_cfg.get("building", "") or "").strip()
        water_total_field = str(fields_cfg.get("water_total", "") or "").strip()
        table_id = str(_NIGHT_WATER_SOURCE.get("table_id", "") or "").strip()

        try:
            client = self._new_night_water_client()
            option_maps = self._load_field_option_maps(
                client=client,
                table_id=table_id,
                target_fields=[building_field],
                emit_log=emit_log,
            )
            records = client.list_records(
                table_id=table_id,
                page_size=max(1, int(_NIGHT_WATER_SOURCE.get("page_size", 500) or 500)),
                max_records=max(1, int(_NIGHT_WATER_SOURCE.get("max_records", 20000) or 20000)),
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(
                "[交接班][容量报表][夜班耗水] 查询失败 "
                f"building={building}, duty={duty_date}/{duty_shift}, error={exc}"
            )
            return {}

        month_total = 0.0
        latest_daily_total: float | None = None
        latest_record_dt: datetime | None = None
        matched_records = 0
        building_option_map = option_maps.get(building_field, {})
        for item in records:
            if not isinstance(item, dict):
                continue
            fields = item.get("fields", {})
            if not isinstance(fields, dict):
                continue
            record_dt = _parse_datetime(fields.get(date_field))
            if record_dt is None or record_dt < month_start_dt or record_dt > duty_end_dt:
                continue
            record_building = _field_text_with_option_map(fields.get(building_field), building_option_map)
            if not self._matches_building(record_building, building):
                continue
            water_total = _to_float(fields.get(water_total_field))
            if water_total is None:
                continue
            matched_records += 1
            month_total += water_total
            if latest_record_dt is None or record_dt >= latest_record_dt:
                latest_record_dt = record_dt
                latest_daily_total = water_total

        summary = {
            "month_total": format_number(month_total),
            "latest_daily_total": format_number(latest_daily_total),
        }
        emit_log(
            "[交接班][容量报表][夜班耗水] 查询完成 "
            f"building={building}, window={month_start_dt.strftime('%Y-%m-%d %H:%M:%S')}~{duty_end_dt.strftime('%Y-%m-%d %H:%M:%S')}, "
            f"matched={matched_records}, O57={summary.get('latest_daily_total', '') or '-'}, R57={summary.get('month_total', '') or '-'}"
        )
        return summary

    def generate(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        handover_output_file: str,
        capacity_source_file: str,
        roster_assignment: Any | None,
        current_alarm_summary: Dict[str, Any] | None,
        previous_alarm_summary: Dict[str, Any] | None,
        resolved_values_by_id: Dict[str, Any] | None,
        metric_origin_context: Dict[str, Any] | None,
        hvdc_source_d_name: Any = None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        building_text = _text(building)
        duty_date_text = _text(duty_date)
        duty_shift_text = _text(duty_shift).lower()
        if not building_text or not duty_date_text or duty_shift_text not in {"day", "night"}:
            raise ValueError("生成交接班容量报表缺少楼栋或班次上下文")

        output_file = self._next_available_output_path(building=building_text, duty_date=duty_date_text)
        template_path = self.resolve_template_path()
        if not template_path.exists():
            raise FileNotFoundError(f"交接班容量报表模板不存在: {template_path}")

        handover_sheet_name = _text(
            (
                self.config.get("template", {})
                if isinstance(self.config.get("template", {}), dict)
                else {}
            ).get("sheet_name", "")
        )
        handover_cells = self._read_handover_cells(
            handover_output_file,
            ["B4", "F4", "B6", "D6", "F6", "C3", "G3", "B7", "D7", "B10", "D10", "B15", "D15", "F15"],
            sheet_name=handover_sheet_name,
        )
        capacity_rows = self._load_capacity_rows(capacity_source_file)
        oil_current = self._extract_oil_values(capacity_rows)
        oil_previous = self._oil_cache_service.load_previous_values(
            building=building_text,
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
        )
        warnings: List[str] = []
        if not oil_previous.get("first") and not oil_previous.get("second"):
            warnings.append("上一班燃油自控系统缓存不存在")

        current_alarm = self._normalize_alarm_summary(current_alarm_summary)
        previous_alarm = self._normalize_alarm_summary(previous_alarm_summary)
        night_water_summary = self._query_night_water_summary(
            building=building_text,
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
            emit_log=emit_log,
        )
        builder = _BUILDER_BY_BUILDING.get(building_text)
        if builder is None:
            raise ValueError(f"不支持的容量报表楼栋: {building_text}")

        hvdc_debug = self._resolve_hvdc_text(
            resolved_values_by_id=resolved_values_by_id,
            metric_origin_context=metric_origin_context,
            hvdc_source_d_name=hvdc_source_d_name,
        )
        emit_log(
            "[交接班][容量报表][HVDC] "
            f"building={building_text}, raw_value={hvdc_debug.get('raw_value', '') or '-'}, "
            f"source_d_name={hvdc_debug.get('source_d_name', '') or '-'}, "
            f"position_code={hvdc_debug.get('position_code', '') or '-'}, "
            f"H17={hvdc_debug.get('formatted', '') or '-'}"
        )

        workbook = openpyxl.load_workbook(template_path)
        try:
            sheet_name = self._sheet_name(workbook, _text(self._template_cfg().get("sheet_name")))
            sheet = workbook[sheet_name]
            template_snapshot = build_capacity_template_snapshot(sheet, building_text)
            running_units = self._resolve_running_units(resolved_values_by_id)
            context = {
                "building": building_text,
                "duty_date": duty_date_text,
                "duty_shift": duty_shift_text,
                "handover_cells": handover_cells,
                "roster": {
                    "current_team": _text(getattr(roster_assignment, "current_team", "")),
                    "next_team": _text(getattr(roster_assignment, "next_team", "")),
                },
                "current_alarm_summary": current_alarm,
                "previous_alarm_summary": previous_alarm,
                "oil_previous": oil_previous,
                "oil_current": oil_current,
                "night_water_summary": night_water_summary,
                "hvdc_text": hvdc_debug.get("formatted", ""),
                "capacity_rows": capacity_rows,
                "running_units": running_units,
                "resolved_values_by_id": resolved_values_by_id if isinstance(resolved_values_by_id, dict) else {},
                "template_snapshot": template_snapshot,
            }
            cell_values = builder(context)
            _write_cells_with_merged_support(sheet, cell_values)
            atomic_save_workbook(workbook, output_file)
        finally:
            workbook.close()

        if oil_current.get("first") or oil_current.get("second"):
            self._oil_cache_service.save_current_values(
                building=building_text,
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                first=_text(oil_current.get("first")),
                second=_text(oil_current.get("second")),
            )

        emit_log(
            "[交接班][容量报表] 生成完成 "
            f"building={building_text}, duty_date={duty_date_text}, duty_shift={duty_shift_text}, output={output_file}"
        )
        return {
            "status": "success",
            "output_file": str(output_file),
            "warnings": warnings,
            "error": "",
        }
