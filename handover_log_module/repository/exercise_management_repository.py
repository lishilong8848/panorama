from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Callable, Dict, List, Tuple

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from handover_log_module.core.shift_window import build_duty_window


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _field_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("text", "name", "value", "label"):
            text = str(value.get(key, "")).strip()
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts = [_field_text(item) for item in value]
        return "，".join([part for part in parts if part])
    return str(value).strip()


def _field_text_with_option_map(value: Any, option_map: Dict[str, str]) -> str:
    text = _field_text(value)
    if not text:
        return ""
    return str(option_map.get(text, text)).strip()


def _field_texts(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        output: List[str] = []
        for item in value:
            text = _field_text(item)
            if text:
                output.append(text)
        return output
    text = _field_text(value)
    if not text:
        return []
    return [segment.strip() for segment in re.split(r"[,，;/\s]+", text) if segment and segment.strip()]


def _field_texts_with_option_map(value: Any, option_map: Dict[str, str]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        output: List[str] = []
        for item in value:
            text = _field_text_with_option_map(item, option_map)
            if text:
                output.append(text)
        return output
    text = _field_text_with_option_map(value, option_map)
    if not text:
        return []
    return [segment.strip() for segment in re.split(r"[,，;/\s]+", text) if segment and segment.strip()]


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = int(float(value))
        if abs(number) >= 10**12:
            return datetime.fromtimestamp(number / 1000)
        return datetime.fromtimestamp(number)
    if isinstance(value, list):
        for item in value:
            parsed = _parse_datetime(item)
            if parsed is not None:
                return parsed
        return None
    if isinstance(value, dict):
        for key in ("value", "text", "name", "label"):
            if key in value:
                parsed = _parse_datetime(value.get(key))
                if parsed is not None:
                    return parsed
        return None

    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        number = int(text)
        if len(text) >= 13 or abs(number) >= 10**12:
            return datetime.fromtimestamp(number / 1000)
        return datetime.fromtimestamp(number)
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(text[:19], fmt)
        except Exception:  # noqa: BLE001
            continue
    return None


def _extract_building_code(text: str) -> str:
    raw = str(text or "").strip().upper()
    if not raw:
        return ""
    match = re.search(r"([A-E])\s*(?:楼|栋)?", raw)
    if match:
        return match.group(1)
    return ""


def _building_matches(target_building: str, values: List[str]) -> bool:
    target = str(target_building or "").strip()
    if not target or not values:
        return False
    target_code = _extract_building_code(target)
    for value in values:
        current = str(value or "").strip()
        if not current:
            continue
        if current.casefold() == target.casefold():
            return True
        if target_code and target_code == _extract_building_code(current):
            return True
    return False


@dataclass
class ExerciseManagementRow:
    record_id: str
    building_values: List[str]
    start_time: datetime
    project_text: str
    raw_fields: Dict[str, Any]


ExerciseRowsByBuilding = Dict[str, List[ExerciseManagementRow]]


class ExerciseManagementRepository:
    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg
        self._field_option_maps_cache: Dict[str, Dict[str, Dict[str, str]]] = {}

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "source": {
                "app_token": "D01TwFPyXiJBY6kCBDZcMCGLnSe",
                "table_id": "tblBrALE11XCicNN",
                "page_size": 500,
                "max_records": 5000,
            },
            "fields": {
                "building": "机楼",
                "start_time": "演练开始时间",
                "project": "告警描述",
            },
            "sections": {
                "exercise_management": "演练管理",
            },
            "fixed_values": {
                "exercise_type": "计划性演练",
                "completion": "已完成",
            },
            "column_mapping": {
                "resolve_by_header": True,
                "header_alias": {
                    "exercise_type": ["演练类型"],
                    "exercise_item": ["演练项目"],
                    "completion": ["演练完成情况", "完成情况"],
                    "executor": ["执行人", "跟进人"],
                },
                "fallback_cols": {
                    "exercise_type": "B",
                    "exercise_item": "C",
                    "completion": "D",
                    "executor": "H",
                },
            },
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("exercise_management_section", {})
        cfg = _deep_merge(self._defaults(), raw if isinstance(raw, dict) else {})
        source = cfg.get("source", {})
        source["app_token"] = str(source.get("app_token", "")).strip()
        source["table_id"] = str(source.get("table_id", "")).strip()
        source["page_size"] = max(1, int(source.get("page_size", 500) or 500))
        source["max_records"] = max(1, int(source.get("max_records", 5000) or 5000))
        cfg["source"] = source
        return cfg

    def get_config(self) -> Dict[str, Any]:
        return self._normalize_cfg()

    def _new_client(self, cfg: Dict[str, Any]) -> FeishuBitableClient:
        global_feishu = self.handover_cfg.get("_global_feishu", {})
        if not isinstance(global_feishu, dict):
            global_feishu = {}
        app_id = str(global_feishu.get("app_id", "")).strip()
        app_secret = str(global_feishu.get("app_secret", "")).strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")

        source = cfg.get("source", {})
        app_token = str(source.get("app_token", "")).strip()
        table_id = str(source.get("table_id", "")).strip()
        if not app_token or not table_id:
            raise ValueError("演练管理多维配置缺失: app_token/table_id")

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
            canonical_metric_name_fn=lambda x: str(x or "").strip(),
            dimension_mapping={},
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
                option_id = str(option.get("id", "")).strip()
                option_name = str(option.get("name", "")).strip()
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
        table_key = str(table_id or "").strip()
        field_names = [str(name or "").strip() for name in target_fields if str(name or "").strip()]
        if not table_key or not field_names:
            return {}

        cached = self._field_option_maps_cache.get(table_key)
        if cached is not None:
            return {field_name: dict(cached.get(field_name, {})) for field_name in field_names}

        try:
            field_defs = client.list_fields(table_id=table_key, page_size=200)
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][演练管理] 字段定义读取失败，选项映射按空继续: {exc}")
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

        self._field_option_maps_cache[table_key] = {key: dict(value) for key, value in output.items()}
        counts = ", ".join(f"{field_name}={len(output.get(field_name, {}))}" for field_name in field_names)
        emit_log(f"[交接班][演练管理] 字段选项映射已加载: {counts}")
        return {field_name: dict(output.get(field_name, {})) for field_name in field_names}

    def _load_rows_for_shift(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None],
    ) -> Tuple[List[ExerciseManagementRow], Dict[str, Any], Dict[str, int]]:
        cfg = self._normalize_cfg()
        if not bool(cfg.get("enabled", True)):
            return [], cfg, {"total": 0, "in_shift": 0, "parse_fail": 0}

        source = cfg.get("source", {})
        fields_cfg = cfg.get("fields", {}) if isinstance(cfg.get("fields", {}), dict) else {}
        table_id = str(source.get("table_id", "")).strip()
        building_field = str(fields_cfg.get("building", "机楼")).strip() or "机楼"
        start_time_field = str(fields_cfg.get("start_time", "演练开始时间")).strip() or "演练开始时间"
        project_field = str(fields_cfg.get("project", "告警描述")).strip() or "告警描述"

        download_cfg = self.handover_cfg.get("download", {})
        shift_windows = download_cfg.get("shift_windows", {}) if isinstance(download_cfg, dict) else {}
        duty_window = build_duty_window(
            duty_date=duty_date,
            duty_shift=duty_shift,
            shift_windows=shift_windows if isinstance(shift_windows, dict) else {},
        )
        start_dt = datetime.strptime(duty_window.start_time, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(duty_window.end_time, "%Y-%m-%d %H:%M:%S")

        emit_log(
            "[交接班][演练管理] 读取飞书: "
            f"table_id={table_id}, page_size={int(source.get('page_size', 500) or 500)}, "
            f"max_records={int(source.get('max_records', 5000) or 5000)}, "
            f"window={duty_window.start_time}~{duty_window.end_time}"
        )

        client = self._new_client(cfg)
        option_maps = self._load_field_option_maps(
            client=client,
            table_id=table_id,
            target_fields=[building_field],
            emit_log=emit_log,
        )
        building_option_map = option_maps.get(building_field, {})

        records = client.list_records(
            table_id=table_id,
            page_size=int(source.get("page_size", 500) or 500),
            max_records=int(source.get("max_records", 5000) or 5000),
        )
        rows: List[ExerciseManagementRow] = []
        total = 0
        in_shift = 0
        parse_fail = 0
        blank_project_skipped = 0

        for item in records:
            if not isinstance(item, dict):
                continue
            total += 1
            record_id = str(item.get("record_id", "")).strip()
            raw_fields = item.get("fields", {})
            if not isinstance(raw_fields, dict):
                raw_fields = {}

            building_values = _field_texts_with_option_map(raw_fields.get(building_field), building_option_map)
            start_time = _parse_datetime(raw_fields.get(start_time_field))
            if start_time is None:
                parse_fail += 1
                continue
            if start_time < start_dt or start_time > end_dt:
                continue

            project_text = _field_text(raw_fields.get(project_field))
            if not project_text:
                blank_project_skipped += 1
                continue

            in_shift += 1
            rows.append(
                ExerciseManagementRow(
                    record_id=record_id,
                    building_values=building_values,
                    start_time=start_time,
                    project_text=project_text,
                    raw_fields=raw_fields,
                )
            )

        rows.sort(key=lambda item: (item.start_time, item.record_id))
        return rows, cfg, {
            "total": total,
            "in_shift": in_shift,
            "parse_fail": parse_fail,
            "blank_project_skipped": blank_project_skipped,
        }

    def list_current_shift_rows(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> Tuple[List[ExerciseManagementRow], Dict[str, Any]]:
        rows, cfg, counters = self._load_rows_for_shift(
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )
        matched_rows = [row for row in rows if _building_matches(building, row.building_values)]
        emit_log(
            "[交接班][演练管理] 读取完成: "
            f"total={counters['total']}, in_shift={counters['in_shift']}, "
            f"building_assigned={len(matched_rows)}, parse_fail={counters['parse_fail']}, "
            f"blank_project_skipped={counters.get('blank_project_skipped', 0)}"
        )
        return matched_rows, cfg

    def list_current_shift_rows_grouped(
        self,
        *,
        buildings: List[str],
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> Tuple[ExerciseRowsByBuilding, Dict[str, Any]]:
        rows, cfg, counters = self._load_rows_for_shift(
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )
        target_buildings: List[str] = []
        for raw in buildings:
            building = str(raw or "").strip()
            if building and building not in target_buildings:
                target_buildings.append(building)

        grouped: ExerciseRowsByBuilding = {building: [] for building in target_buildings}
        assigned_rows = 0
        for row in rows:
            for building in target_buildings:
                if not _building_matches(building, row.building_values):
                    continue
                grouped[building].append(row)
                assigned_rows += 1

        emit_log(
            "[交接班][演练管理] 读取完成: "
            f"total={counters['total']}, in_shift={counters['in_shift']}, "
            f"building_assigned={assigned_rows}, parse_fail={counters['parse_fail']}, "
            f"blank_project_skipped={counters.get('blank_project_skipped', 0)}"
        )
        emit_log(
            "[交接班][演练管理] 批量分桶完成: "
            f"buildings={len(target_buildings)}, total_rows={len(rows)}, assigned_rows={assigned_rows}"
        )
        return grouped, cfg
