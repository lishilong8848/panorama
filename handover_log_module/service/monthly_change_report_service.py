from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

import openpyxl

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from app.modules.sheet_import.core.field_value_converter import parse_timestamp_ms
from app.shared.utils.atomic_file import atomic_save_workbook, atomic_write_text
from app.shared.utils.file_utils import fallback_missing_windows_drive_path
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from handover_log_module.api.facade import load_handover_config
from handover_log_module.service.monthly_event_report_service import MonthlyEventReportService
from pipeline_utils import get_app_dir


_ALL_BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"]
_RUNTIME_LAST_RUN_FILE = "monthly_change_report_last_run.json"
_DEFAULT_MONTHLY_FIELDS = {
    "change_code": "变更编码",
    "name": "名称",
    "location": "位置",
    "change_level": "智航-变更等级",
    "status": "变更状态",
    "start_time": "变更开始时间",
    "end_time": "变更结束时间",
}


@dataclass
class MonthlyChangeRecord:
    building: str
    start_time: datetime | None
    change_code: str
    values: Dict[str, str]


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _field_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts = [_field_text(item) for item in value]
        return "，".join([item for item in parts if item])
    if isinstance(value, dict):
        for key in ("text", "name", "value", "label", "id"):
            if key not in value:
                continue
            text = _field_text(value.get(key))
            if text:
                return text
        return ""
    return str(value).strip()


def _field_text_with_option_map(value: Any, option_map: Dict[str, str]) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts = [_field_text_with_option_map(item, option_map) for item in value]
        return "，".join([item for item in parts if item])
    if isinstance(value, dict):
        option_id = str(value.get("id", "") or "").strip()
        if option_id and option_id in option_map:
            return str(option_map.get(option_id, "") or "").strip()
        for key in ("text", "name", "value", "label"):
            if key not in value:
                continue
            text = _field_text_with_option_map(value.get(key), option_map)
            if text:
                return text
        return ""
    text = str(value or "").strip()
    if not text:
        return ""
    return str(option_map.get(text, text)).strip()


def _parse_datetime(value: Any) -> datetime | None:
    timestamp_ms = parse_timestamp_ms(value, tz_offset_hours=8)
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000)


class MonthlyChangeReportService:
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
                "source_path": "月度变更统计表空模板.xlsx",
                "output_dir": r"D:\QLDownload\月度统计表输出\变更月度统计表",
                "file_name_pattern": "{building}_{month}_变更月度统计表.xlsx",
            },
            "scheduler": {
                "enabled": False,
                "auto_start_in_gui": False,
                "day_of_month": 1,
                "run_time": "01:00:00",
                "check_interval_sec": 30,
                "state_file": "monthly_change_report_scheduler_state.json",
            },
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        handover_cfg = self.runtime_config.get("handover_log", {})
        if not isinstance(handover_cfg, dict):
            handover_cfg = {}
        raw_cfg = handover_cfg.get("monthly_change_report", {})
        cfg = _deep_merge(self._defaults(), raw_cfg if isinstance(raw_cfg, dict) else {})
        cfg["enabled"] = bool(cfg.get("enabled", True))

        event_template_cfg = handover_cfg.get("monthly_event_report", {})
        if not isinstance(event_template_cfg, dict):
            event_template_cfg = {}
        event_template = event_template_cfg.get("template", {})
        if not isinstance(event_template, dict):
            event_template = {}
        legacy_change_source_path = str(event_template.get("change_source_path", "") or "").strip()

        template = cfg.get("template", {}) if isinstance(cfg.get("template", {}), dict) else {}
        template["source_path"] = (
            str(template.get("source_path", "") or "").strip()
            or legacy_change_source_path
            or "月度变更统计表空模板.xlsx"
        )
        template["output_dir"] = (
            str(template.get("output_dir", "") or "").strip()
            or r"D:\QLDownload\月度统计表输出\变更月度统计表"
        )
        template["file_name_pattern"] = (
            str(template.get("file_name_pattern", "") or "").strip()
            or "{building}_{month}_变更月度统计表.xlsx"
        )
        cfg["template"] = template

        scheduler = cfg.get("scheduler", {}) if isinstance(cfg.get("scheduler", {}), dict) else {}
        scheduler["enabled"] = bool(scheduler.get("enabled", False))
        scheduler["auto_start_in_gui"] = bool(scheduler.get("auto_start_in_gui", False))
        scheduler["day_of_month"] = max(1, min(31, int(scheduler.get("day_of_month", 1) or 1)))
        scheduler["run_time"] = str(scheduler.get("run_time", "") or "").strip() or "01:00:00"
        scheduler["check_interval_sec"] = max(1, int(scheduler.get("check_interval_sec", 30) or 30))
        scheduler["state_file"] = (
            str(scheduler.get("state_file", "") or "").strip() or "monthly_change_report_scheduler_state.json"
        )
        cfg["scheduler"] = scheduler
        return cfg

    def _normalize_source_cfg(self) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        handover_cfg = load_handover_config(self.runtime_config)
        change_cfg = handover_cfg.get("change_management_section", {})
        if not isinstance(change_cfg, dict):
            change_cfg = {}
        source_cfg = change_cfg.get("source", {})
        if not isinstance(source_cfg, dict):
            source_cfg = {}
        base_fields = change_cfg.get("fields", {})
        if not isinstance(base_fields, dict):
            base_fields = {}
        monthly_fields = change_cfg.get("monthly_report_fields", {})
        if not isinstance(monthly_fields, dict):
            monthly_fields = {}
        normalized_fields = _deep_merge(
            {
                "building": str(base_fields.get("building", "楼栋") or "楼栋").strip() or "楼栋",
                **_DEFAULT_MONTHLY_FIELDS,
            },
            monthly_fields,
        )
        for key in list(normalized_fields.keys()):
            normalized_fields[key] = str(normalized_fields.get(key, "") or "").strip()
        return handover_cfg, source_cfg, normalized_fields

    def get_config(self) -> Dict[str, Any]:
        return self._normalize_cfg()

    def is_enabled(self) -> bool:
        return bool(self._normalize_cfg().get("enabled", True))

    @staticmethod
    def normalize_scope(scope: Any, building: Any = None) -> tuple[str, str | None]:
        return MonthlyEventReportService.normalize_scope(scope, building)

    @staticmethod
    def target_month_window(now: datetime | None = None) -> tuple[datetime, datetime, str]:
        return MonthlyEventReportService.target_month_window(now)

    @staticmethod
    def job_name(scope: str, building: str | None = None) -> str:
        normalized_scope, normalized_building = MonthlyChangeReportService.normalize_scope(scope, building)
        if normalized_scope == "building" and normalized_building:
            return f"月度变更统计表处理-{normalized_building}"
        return "月度变更统计表处理-全部楼栋"

    @staticmethod
    def dedupe_key(scope: str, building: str | None = None, *, target_month: str) -> str:
        normalized_scope, normalized_building = MonthlyChangeReportService.normalize_scope(scope, building)
        month_text = str(target_month or "").strip()
        if normalized_scope == "building" and normalized_building:
            return f"monthly_change_report:building:{normalized_building}:{month_text}"
        return f"monthly_change_report:all:{month_text}"

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

    def _runtime_state_root(self) -> Path:
        return resolve_runtime_state_root(runtime_config=self.runtime_config, app_dir=self._app_dir())

    def _last_run_path(self) -> Path:
        return self._runtime_state_root() / _RUNTIME_LAST_RUN_FILE

    @staticmethod
    def _empty_last_run() -> Dict[str, Any]:
        return {
            "started_at": "",
            "finished_at": "",
            "status": "",
            "report_type": "",
            "scope": "",
            "building": "",
            "target_month": "",
            "generated_files": 0,
            "successful_buildings": [],
            "failed_buildings": [],
            "output_dir": "",
            "files_by_building": {},
            "error": "",
        }

    def get_last_run_snapshot(self) -> Dict[str, Any]:
        path = self._last_run_path()
        if not path.exists():
            return self._empty_last_run()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return self._empty_last_run()
        if not isinstance(payload, dict):
            return self._empty_last_run()
        snapshot = self._empty_last_run()
        for key in snapshot:
            snapshot[key] = payload.get(key, snapshot[key])
        snapshot["generated_files"] = int(snapshot.get("generated_files", 0) or 0)
        snapshot["successful_buildings"] = [
            str(item or "").strip()
            for item in (snapshot.get("successful_buildings", []) or [])
            if str(item or "").strip()
        ]
        snapshot["failed_buildings"] = [
            str(item or "").strip()
            for item in (snapshot.get("failed_buildings", []) or [])
            if str(item or "").strip()
        ]
        raw_files_by_building = snapshot.get("files_by_building", {})
        normalized_files_by_building: Dict[str, Dict[str, Any]] = {}
        if isinstance(raw_files_by_building, dict):
            for building, item in raw_files_by_building.items():
                building_text = str(building or "").strip()
                if not building_text or not isinstance(item, dict):
                    continue
                file_path = str(item.get("file_path", "") or "").strip()
                normalized_files_by_building[building_text] = {
                    "building": building_text,
                    "file_path": file_path,
                    "file_name": str(item.get("file_name", "") or "").strip(),
                    "exists": bool(item.get("exists", False)) and bool(file_path) and Path(file_path).exists(),
                }
        if not normalized_files_by_building:
            target_month = str(snapshot.get("target_month", "") or "").strip()
            output_dir = str(snapshot.get("output_dir", "") or "").strip()
            successful_buildings = snapshot.get("successful_buildings", [])
            file_name_pattern = self.get_config().get("template", {}).get("file_name_pattern", "")
            if target_month and output_dir and isinstance(successful_buildings, list):
                output_dir_path = Path(output_dir)
                for building in successful_buildings:
                    building_text = str(building or "").strip()
                    if not building_text:
                        continue
                    file_name = self._safe_output_name(file_name_pattern, building=building_text, month=target_month)
                    file_path = output_dir_path / file_name
                    normalized_files_by_building[building_text] = {
                        "building": building_text,
                        "file_path": str(file_path),
                        "file_name": file_name,
                        "exists": file_path.exists(),
                    }
        snapshot["files_by_building"] = normalized_files_by_building
        return snapshot

    def _save_last_run_snapshot(self, payload: Dict[str, Any]) -> None:
        snapshot = self._empty_last_run()
        snapshot.update(payload if isinstance(payload, dict) else {})
        atomic_write_text(
            self._last_run_path(),
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

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
        except Exception as exc:
            emit_log(f"[月度变更统计表] 字段定义读取失败，选项映射按空继续: {exc}")
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
        summary = ", ".join(f"{field_name}={len(output.get(field_name, {}))}" for field_name in field_names)
        emit_log(f"[月度变更统计表] 字段选项映射已加载: {summary or '-'}")
        return output

    @staticmethod
    def _extract_buildings(value: Any) -> List[str]:
        text = str(value or "").strip().upper()
        if not text:
            return []
        matches = re.findall(r"([A-E])\s*(?:楼|栋|机楼|机房)?", text)
        output: List[str] = []
        for code in matches:
            building = f"{code}楼"
            if building not in output:
                output.append(building)
        return output

    @staticmethod
    def _format_datetime(value: Any) -> str:
        parsed = _parse_datetime(value)
        if parsed is None:
            return ""
        return parsed.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _safe_output_name(pattern: str, *, building: str, month: str) -> str:
        text = str(pattern or "").strip()
        if not text:
            return f"{building}_{month}_变更月度统计表.xlsx"
        try:
            rendered = text.format(building=building, month=month)
            return str(rendered or "").strip() or f"{building}_{month}_变更月度统计表.xlsx"
        except Exception:
            return f"{building}_{month}_变更月度统计表.xlsx"

    def _new_source_client(self) -> tuple[FeishuBitableClient, Dict[str, Any], Dict[str, Any]]:
        handover_cfg, source_cfg, monthly_fields = self._normalize_source_cfg()
        global_feishu = handover_cfg.get("_global_feishu", {})
        if not isinstance(global_feishu, dict):
            global_feishu = {}

        app_id = str(global_feishu.get("app_id", "") or "").strip()
        app_secret = str(global_feishu.get("app_secret", "") or "").strip()
        app_token = str(source_cfg.get("app_token", "") or "").strip()
        table_id = str(source_cfg.get("table_id", "") or "").strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")
        if not app_token or not table_id:
            raise ValueError("变更管理源配置缺失: app_token/table_id")

        client = FeishuBitableClient(
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
        return client, source_cfg, monthly_fields

    def _load_source_records(
        self,
        *,
        selected_buildings: Iterable[str],
        window_start: datetime,
        window_end: datetime,
        emit_log: Callable[[str], None],
    ) -> Dict[str, List[MonthlyChangeRecord]]:
        client, source_cfg, monthly_fields = self._new_source_client()
        page_size = max(1, int(source_cfg.get("page_size", 500) or 500))
        max_records = max(1, int(source_cfg.get("max_records", 5000) or 5000))
        table_id = str(source_cfg.get("table_id", "") or "").strip()
        option_map_fields = [
            monthly_fields.get("building", ""),
            monthly_fields.get("change_level", ""),
            monthly_fields.get("status", ""),
        ]
        option_maps = self._load_field_option_maps(
            client=client,
            table_id=table_id,
            target_fields=option_map_fields,
            emit_log=emit_log,
        )
        records = client.list_records(table_id=table_id, page_size=page_size, max_records=max_records)
        emit_log(
            "[月度变更统计表] 变更源读取完成: "
            f"table_id={table_id or '-'}, total={len(records)}, "
            f"window={window_start.strftime('%Y-%m-%d %H:%M:%S')}~{window_end.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        selected = {str(item or "").strip() for item in selected_buildings if str(item or "").strip()}
        grouped: Dict[str, List[MonthlyChangeRecord]] = {building: [] for building in selected}

        building_field = monthly_fields["building"]
        change_code_field = monthly_fields["change_code"]
        name_field = monthly_fields["name"]
        location_field = monthly_fields["location"]
        change_level_field = monthly_fields["change_level"]
        status_field = monthly_fields["status"]
        start_time_field = monthly_fields["start_time"]
        end_time_field = monthly_fields["end_time"]

        for item in records:
            if not isinstance(item, dict):
                continue
            fields = item.get("fields", {})
            if not isinstance(fields, dict):
                continue
            start_time = _parse_datetime(fields.get(start_time_field))
            if start_time is None or start_time < window_start or start_time >= window_end:
                continue
            matched_buildings = self._extract_buildings(
                _field_text_with_option_map(fields.get(building_field), option_maps.get(building_field, {}))
            )
            if not matched_buildings:
                continue
            change_code = _field_text(fields.get(change_code_field))
            base_values = {
                "change_code": change_code,
                "name": _field_text(fields.get(name_field)),
                "location": _field_text(fields.get(location_field)),
                "change_level": _field_text_with_option_map(
                    fields.get(change_level_field),
                    option_maps.get(change_level_field, {}),
                ),
                "status": _field_text_with_option_map(
                    fields.get(status_field),
                    option_maps.get(status_field, {}),
                ),
                "start_time": self._format_datetime(fields.get(start_time_field)),
                "end_time": self._format_datetime(fields.get(end_time_field)),
            }
            for building in matched_buildings:
                if building not in grouped:
                    continue
                grouped[building].append(
                    MonthlyChangeRecord(
                        building=building,
                        start_time=start_time,
                        change_code=change_code,
                        values=dict(base_values),
                    )
                )

        for building in grouped:
            grouped[building].sort(
                key=lambda item: (
                    item.start_time or datetime.max,
                    str(item.change_code or "").strip(),
                )
            )
        return grouped

    def _write_building_workbook(
        self,
        *,
        building: str,
        month: str,
        template_path: Path,
        output_path: Path,
        rows: List[MonthlyChangeRecord],
        emit_log: Callable[[str], None],
    ) -> Path:
        workbook = openpyxl.load_workbook(template_path)
        try:
            sheet = workbook.active
            start_row = 4
            for index, row in enumerate(rows, start=1):
                row_index = start_row + index - 1
                values = [
                    index,
                    "正常的线上工单",
                    f"21V-HD8FOC-{row.values.get('change_code', '')}" if row.values.get("change_code", "") else "",
                    row.values.get("name", ""),
                    row.values.get("location", ""),
                    row.values.get("change_level", ""),
                    "计划变更",
                    "预防维护",
                    row.values.get("status", ""),
                    "成功",
                    "",
                    row.values.get("start_time", ""),
                    "",
                    row.values.get("start_time", ""),
                    row.values.get("end_time", ""),
                    "",
                    "",
                    "",
                    "",
                ]
                for column, value in enumerate(values, start=1):
                    MonthlyEventReportService._write_styled_data_cell(
                        sheet,
                        row_index=row_index,
                        column_index=column,
                        value=value,
                    )
            atomic_save_workbook(workbook, output_path)
        finally:
            workbook.close()

        emit_log(
            "[月度变更统计表] 文件生成完成: "
            f"building={building}, month={month}, rows={len(rows)}, output={output_path}"
        )
        return output_path

    def run(
        self,
        *,
        scope: str,
        building: str | None = None,
        emit_log: Callable[[str], None] = print,
        source: str = "manual",
    ) -> Dict[str, Any]:
        if not self.is_enabled():
            raise RuntimeError("月度变更统计表处理已禁用")

        normalized_scope, normalized_building = self.normalize_scope(scope, building)
        selected_buildings = [normalized_building] if normalized_scope == "building" and normalized_building else self.all_buildings()
        window_start, window_end, target_month = self.target_month_window(datetime.now())
        template_path = self.resolve_template_path()
        if not template_path.exists():
            raise FileNotFoundError(f"月度变更统计表模板不存在: {template_path}")
        output_dir = self.resolve_output_dir()
        file_name_pattern = self.get_config()["template"]["file_name_pattern"]

        started_at = datetime.now()
        emit_log(
            "[月度变更统计表] 开始处理: "
            f"source={source}, scope={normalized_scope}, building={normalized_building or 'all'}, "
            f"target_month={target_month}, window={window_start.strftime('%Y-%m-%d %H:%M:%S')}~{window_end.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        grouped_rows = self._load_source_records(
            selected_buildings=selected_buildings,
            window_start=window_start,
            window_end=window_end,
            emit_log=emit_log,
        )

        successful_buildings: List[str] = []
        failed_buildings: List[str] = []
        files_by_building: Dict[str, Dict[str, Any]] = {}
        generated_files = 0
        failure_details: List[str] = []

        for current_building in selected_buildings:
            try:
                file_name = self._safe_output_name(
                    file_name_pattern,
                    building=current_building,
                    month=target_month,
                )
                output_path = output_dir / file_name
                self._write_building_workbook(
                    building=current_building,
                    month=target_month,
                    template_path=template_path,
                    output_path=output_path,
                    rows=grouped_rows.get(current_building, []),
                    emit_log=emit_log,
                )
                successful_buildings.append(current_building)
                generated_files += 1
                files_by_building[current_building] = {
                    "building": current_building,
                    "file_path": str(output_path),
                    "file_name": output_path.name,
                    "exists": output_path.exists(),
                }
            except Exception as exc:
                failed_buildings.append(current_building)
                detail = f"{current_building}: {exc}"
                failure_details.append(detail)
                emit_log(f"[月度变更统计表] 文件生成失败: building={current_building}, error={exc}")

        if failed_buildings and successful_buildings:
            status = "partial_failed"
        elif failed_buildings:
            status = "failed"
        else:
            status = "ok"

        finished_at = datetime.now()
        error_text = "；".join(failure_details)
        result = {
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": finished_at.strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "report_type": "change",
            "scope": normalized_scope,
            "building": normalized_building or "",
            "target_month": target_month,
            "generated_files": generated_files,
            "successful_buildings": successful_buildings,
            "failed_buildings": failed_buildings,
            "output_dir": str(output_dir),
            "files_by_building": files_by_building,
            "error": error_text,
        }
        self._save_last_run_snapshot(result)
        emit_log(
            "[月度变更统计表] 处理完成: "
            f"status={status}, target_month={target_month}, generated_files={generated_files}, "
            f"successful={','.join(successful_buildings) or '-'}, failed={','.join(failed_buildings) or '-'}"
        )
        if error_text:
            emit_log(f"[月度变更统计表] 错误详情: {error_text}")
        return result
