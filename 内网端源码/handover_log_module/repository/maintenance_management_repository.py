from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import re
from typing import Any, Callable, Dict, List, Tuple

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from handover_log_module.core.shift_interval_overlap import (
    build_shift_interval_window,
    interval_overlaps_filter_window,
)
from handover_log_module.core.shift_window import build_duty_window
from handover_log_module.core.specialty_normalizer import normalize_specialty_text


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
        return "、".join([part for part in parts if part])
    return str(value).strip()


def _field_text_with_option_map(value: Any, option_map: Dict[str, str]) -> str:
    text = _field_text(value)
    if not text:
        return ""
    return str(option_map.get(text, text)).strip()


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


def _infer_duty_by_now(now: datetime | None = None) -> tuple[str, str]:
    cursor = now or datetime.now()
    second_of_day = cursor.hour * 3600 + cursor.minute * 60 + cursor.second
    if second_of_day < 9 * 3600:
        day = cursor.date() - timedelta(days=1)
        return day.strftime("%Y-%m-%d"), "night"
    if second_of_day < 18 * 3600:
        return cursor.strftime("%Y-%m-%d"), "day"
    return cursor.strftime("%Y-%m-%d"), "night"


def _is_current_duty_context(
    *,
    duty_date: str,
    duty_shift: str,
    now: datetime | None = None,
) -> bool:
    current_duty_date, current_duty_shift = _infer_duty_by_now(now=now)
    return (
        str(duty_date or "").strip() == current_duty_date
        and str(duty_shift or "").strip().lower() == current_duty_shift
    )


@dataclass
class MaintenanceManagementRow:
    record_id: str
    building_values: List[str]
    updated_time: datetime
    item_text: str
    specialty_text: str
    raw_fields: Dict[str, Any]


MaintenanceRowsByBuilding = Dict[str, List[MaintenanceManagementRow]]


class MaintenanceManagementRepository:
    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg
        self._field_option_maps_cache: Dict[str, Dict[str, Dict[str, str]]] = {}

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "source": {
                "app_token": "D01TwFPyXiJBY6kCBDZcMCGLnSe",
                "table_id": "tblk7QuEsiE4p3nZ",
                "page_size": 500,
                "max_records": 5000,
            },
            "fields": {
                "building": "楼栋",
                "start_time": "实际开始时间",
                "updated_time": "最新更新时间",
                "actual_end_time": "实际结束时间",
                "item": "名称",
                "specialty": "专业",
            },
            "sections": {
                "maintenance_management": "维护管理",
            },
            "fixed_values": {
                "vendor_internal": "自维",
                "vendor_external": "厂维",
                "completion": "已完成",
            },
            "column_mapping": {
                "resolve_by_header": True,
                "header_alias": {
                    "maintenance_item": ["维护总项"],
                    "maintenance_party": ["维护执行方"],
                    "completion": ["维护完成情况", "完成情况"],
                    "executor": ["执行人", "跟进人"],
                },
                "fallback_cols": {
                    "maintenance_item": "B",
                    "maintenance_party": "C",
                    "completion": "D",
                    "executor": "H",
                },
            },
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("maintenance_management_section", {})
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
        global_feishu = require_feishu_auth_settings(self.handover_cfg)

        source = cfg.get("source", {})
        app_token = str(source.get("app_token", "")).strip()
        table_id = str(source.get("table_id", "")).strip()
        if not app_token or not table_id:
            raise ValueError("维护管理多维配置缺失: app_token/table_id")

        return FeishuBitableClient(
            app_id=str(global_feishu.get("app_id", "") or "").strip(),
            app_secret=str(global_feishu.get("app_secret", "") or "").strip(),
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
            emit_log(f"[交接班][维护管理] 字段定义读取失败，选项映射按空继续: {exc}")
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
        emit_log(f"[交接班][维护管理] 字段选项映射已加载: {counts}")
        return {field_name: dict(output.get(field_name, {})) for field_name in field_names}

    def _load_rows_for_shift(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None],
    ) -> Tuple[List[MaintenanceManagementRow], Dict[str, Any], Dict[str, int]]:
        cfg = self._normalize_cfg()
        if not bool(cfg.get("enabled", True)):
            return [], cfg, {
                "total": 0,
                "in_shift": 0,
                "start_time_parse_fail": 0,
                "start_time_out_of_shift_skipped": 0,
                "end_time_parse_fail": 0,
                "end_time_before_shift_skipped": 0,
                "end_time_after_shift_skipped": 0,
                "end_time_before_start_skipped": 0,
                "blank_item_skipped": 0,
            }

        source = cfg.get("source", {})
        fields_cfg = cfg.get("fields", {}) if isinstance(cfg.get("fields", {}), dict) else {}
        table_id = str(source.get("table_id", "")).strip()
        building_field = str(fields_cfg.get("building", "楼栋")).strip() or "楼栋"
        configured_start_time_field = str(fields_cfg.get("start_time", "")).strip()
        legacy_updated_time_field = str(fields_cfg.get("updated_time", "")).strip()
        start_time_field = configured_start_time_field or (
            legacy_updated_time_field if legacy_updated_time_field and legacy_updated_time_field != "最新更新时间" else "实际开始时间"
        )
        actual_end_time_field = str(fields_cfg.get("actual_end_time", "实际结束时间")).strip() or "实际结束时间"
        item_field = str(fields_cfg.get("item", "名称")).strip() or "名称"
        specialty_field = str(fields_cfg.get("specialty", "专业")).strip() or "专业"

        download_cfg = self.handover_cfg.get("download", {})
        shift_windows = download_cfg.get("shift_windows", {}) if isinstance(download_cfg, dict) else {}
        duty_window = build_duty_window(
            duty_date=duty_date,
            duty_shift=duty_shift,
            shift_windows=shift_windows if isinstance(shift_windows, dict) else {},
        )
        start_dt = datetime.strptime(duty_window.start_time, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(duty_window.end_time, "%Y-%m-%d %H:%M:%S")
        filter_window = build_shift_interval_window(
            shift_start=start_dt,
            shift_end=end_dt,
            offset_hours=1,
        )
        is_current_duty = _is_current_duty_context(duty_date=duty_date, duty_shift=duty_shift)

        emit_log(
            "[交接班][维护管理] 读取飞书: "
            f"table_id={table_id}, page_size={int(source.get('page_size', 500) or 500)}, "
            f"max_records={int(source.get('max_records', 5000) or 5000)}, "
            f"window={duty_window.start_time}~{duty_window.end_time}, "
            f"filter_window={filter_window.filter_start.strftime('%Y-%m-%d %H:%M:%S')}~{filter_window.filter_end.strftime('%Y-%m-%d %H:%M:%S')}, "
            f"mode={'current' if is_current_duty else 'history'}"
        )

        client = self._new_client(cfg)
        option_maps = self._load_field_option_maps(
            client=client,
            table_id=table_id,
            target_fields=[building_field, specialty_field],
            emit_log=emit_log,
        )
        building_option_map = option_maps.get(building_field, {})
        specialty_option_map = option_maps.get(specialty_field, {})

        records = client.list_records(
            table_id=table_id,
            page_size=int(source.get("page_size", 500) or 500),
            max_records=int(source.get("max_records", 5000) or 5000),
        )
        rows: List[MaintenanceManagementRow] = []
        total = 0
        in_shift = 0
        start_time_parse_fail = 0
        start_time_out_of_shift_skipped = 0
        end_time_parse_fail = 0
        end_time_before_shift_skipped = 0
        end_time_after_shift_skipped = 0
        end_time_before_start_skipped = 0
        blank_item_skipped = 0

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
                start_time_parse_fail += 1
                continue
            raw_actual_end_time = raw_fields.get(actual_end_time_field)
            actual_end_time_text = _field_text(raw_actual_end_time)
            actual_end_time = _parse_datetime(raw_actual_end_time)
            if actual_end_time is None and actual_end_time_text:
                end_time_parse_fail += 1
                continue
            if actual_end_time is not None and actual_end_time < start_time:
                end_time_before_start_skipped += 1
                continue
            if actual_end_time is not None and actual_end_time <= filter_window.filter_start:
                end_time_before_shift_skipped += 1
                continue
            if start_time > filter_window.filter_end:
                start_time_out_of_shift_skipped += 1
                continue
            if not interval_overlaps_filter_window(
                start_time=start_time,
                end_time=actual_end_time,
                filter_start=filter_window.filter_start,
                filter_end=filter_window.filter_end,
            ):
                end_time_after_shift_skipped += 1
                continue

            item_text = _field_text(raw_fields.get(item_field))
            if not item_text:
                blank_item_skipped += 1
                continue

            in_shift += 1
            rows.append(
                MaintenanceManagementRow(
                    record_id=record_id,
                    building_values=building_values,
                    updated_time=start_time,
                    item_text=item_text,
                    specialty_text=normalize_specialty_text(
                        _field_text_with_option_map(raw_fields.get(specialty_field), specialty_option_map)
                    ),
                    raw_fields=raw_fields,
                )
            )

        rows.sort(key=lambda item: (item.updated_time, item.record_id))
        return rows, cfg, {
            "total": total,
            "in_shift": in_shift,
            "start_time_parse_fail": start_time_parse_fail,
            "start_time_out_of_shift_skipped": start_time_out_of_shift_skipped,
            "end_time_parse_fail": end_time_parse_fail,
            "end_time_before_shift_skipped": end_time_before_shift_skipped,
            "end_time_after_shift_skipped": end_time_after_shift_skipped,
            "end_time_before_start_skipped": end_time_before_start_skipped,
            "blank_item_skipped": blank_item_skipped,
        }

    def list_current_shift_rows(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> Tuple[List[MaintenanceManagementRow], Dict[str, Any]]:
        rows, cfg, counters = self._load_rows_for_shift(
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )
        matched_rows = [row for row in rows if _building_matches(building, row.building_values)]
        emit_log(
            "[交接班][维护管理] 读取完成: "
            f"total={counters['total']}, in_shift={counters['in_shift']}, "
            f"building_assigned={len(matched_rows)}, "
            f"start_time_parse_fail={counters.get('start_time_parse_fail', 0)}, "
            f"start_time_out_of_shift_skipped={counters.get('start_time_out_of_shift_skipped', 0)}, "
            f"end_time_parse_fail={counters.get('end_time_parse_fail', 0)}, "
            f"end_time_before_shift_skipped={counters.get('end_time_before_shift_skipped', 0)}, "
            f"end_time_after_shift_skipped={counters.get('end_time_after_shift_skipped', 0)}, "
            f"end_time_before_start_skipped={counters.get('end_time_before_start_skipped', 0)}, "
            f"blank_item_skipped={counters.get('blank_item_skipped', 0)}"
        )
        return matched_rows, cfg

    def list_current_shift_rows_grouped(
        self,
        *,
        buildings: List[str],
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> Tuple[MaintenanceRowsByBuilding, Dict[str, Any]]:
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

        grouped: MaintenanceRowsByBuilding = {building: [] for building in target_buildings}
        assigned_rows = 0
        for row in rows:
            for building in target_buildings:
                if not _building_matches(building, row.building_values):
                    continue
                grouped[building].append(row)
                assigned_rows += 1

        emit_log(
            "[交接班][维护管理] 读取完成: "
            f"total={counters['total']}, in_shift={counters['in_shift']}, "
            f"building_assigned={assigned_rows}, "
            f"start_time_parse_fail={counters.get('start_time_parse_fail', 0)}, "
            f"start_time_out_of_shift_skipped={counters.get('start_time_out_of_shift_skipped', 0)}, "
            f"end_time_parse_fail={counters.get('end_time_parse_fail', 0)}, "
            f"end_time_before_shift_skipped={counters.get('end_time_before_shift_skipped', 0)}, "
            f"end_time_after_shift_skipped={counters.get('end_time_after_shift_skipped', 0)}, "
            f"end_time_before_start_skipped={counters.get('end_time_before_start_skipped', 0)}, "
            f"blank_item_skipped={counters.get('blank_item_skipped', 0)}"
        )
        emit_log(
            "[交接班][维护管理] 批量分桶完成: "
            f"buildings={len(target_buildings)}, total_rows={len(rows)}, assigned_rows={assigned_rows}"
        )
        return grouped, cfg
