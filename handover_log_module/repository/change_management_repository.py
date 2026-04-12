from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Callable, Dict, List

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
    return [segment.strip() for segment in re.split(r"[,，;/／\s]+", text) if segment and segment.strip()]


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
    return [segment.strip() for segment in re.split(r"[,，;/／\s]+", text) if segment and segment.strip()]


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


def _building_exact_single_match(target_building: str, values: List[str]) -> bool:
    target = str(target_building or "").strip()
    if not target or len(values) != 1:
        return False
    current = str(values[0] or "").strip()
    if not current:
        return False
    if current.casefold() == target.casefold():
        return True
    target_code = _extract_building_code(target)
    return bool(target_code and target_code == _extract_building_code(current))


@dataclass
class ChangeManagementRow:
    record_id: str
    building_values: List[str]
    start_time: datetime
    end_time: datetime | None
    change_level: str
    process_updates_text: str
    description: str
    specialty_text: str
    raw_fields: Dict[str, Any]


ChangeRowsByBuilding = Dict[str, List[ChangeManagementRow]]


class ChangeManagementRepository:
    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg
        self._field_option_maps_cache: Dict[str, Dict[str, Dict[str, str]]] = {}

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "source": {
                "app_token": "D01TwFPyXiJBY6kCBDZcMCGLnSe",
                "table_id": "tblYodlEKeWzqogu",
                "page_size": 500,
                "max_records": 5000,
            },
            "fields": {
                "building": "楼栋",
                "start_time": "变更开始时间",
                "end_time": "变更结束时间",
                "updated_time": "更新最新的时间",
                "change_level": "阿里-变更等级",
                "process_updates": "过程更新时间",
                "description": "名称",
                "specialty": "专业",
            },
            "sections": {
                "change_management": "变更管理",
            },
            "column_mapping": {
                "resolve_by_header": True,
                "header_alias": {
                    "change_level": ["变更等级", "事件等级"],
                    "work_window": ["作业时间段"],
                    "description": ["描述", "告警描述"],
                    "executor": ["执行人", "跟进人"],
                },
                "fallback_cols": {
                    "change_level": "B",
                    "work_window": "E",
                    "description": "D",
                    "executor": "H",
                },
            },
            "work_window_text": {
                "day_anchor": "08:00:00",
                "day_default_end": "18:30:00",
                "night_anchor": "18:00:00",
                "night_default_end_next_day": "08:00:00",
            },
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("change_management_section", {})
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
            raise ValueError("变更管理多维配置缺失: app_token/table_id")

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
            emit_log(f"[交接班][变更管理] 字段定义读取失败，选项映射按空继续: {exc}")
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
        emit_log(f"[交接班][变更管理] 字段选项映射已加载: {counts}")
        return {field_name: dict(output.get(field_name, {})) for field_name in field_names}

    def _load_rows_for_shift(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> tuple[List[ChangeManagementRow], Dict[str, Any], Dict[str, int]]:
        cfg = self._normalize_cfg()
        if not bool(cfg.get("enabled", True)):
            return [], cfg, {
                "total": 0,
                "in_shift": 0,
                "single_building_rows": 0,
                "multi_building_skipped": 0,
                "blank_building_skipped": 0,
                "start_time_parse_fail": 0,
                "end_time_parse_fail": 0,
                "end_time_before_start_skipped": 0,
                "start_time_after_filter_end_skipped": 0,
                "end_time_before_filter_start_skipped": 0,
                "blank_description_skipped": 0,
            }

        source = cfg.get("source", {})
        fields_cfg = cfg.get("fields", {}) if isinstance(cfg.get("fields", {}), dict) else {}
        monthly_fields_cfg = (
            cfg.get("monthly_report_fields", {})
            if isinstance(cfg.get("monthly_report_fields", {}), dict)
            else {}
        )
        download_cfg = self.handover_cfg.get("download", {})
        shift_windows = download_cfg.get("shift_windows", {}) if isinstance(download_cfg, dict) else {}
        duty_window = build_duty_window(
            duty_date=duty_date,
            duty_shift=duty_shift,
            shift_windows=shift_windows if isinstance(shift_windows, dict) else {},
        )
        shift_start = datetime.strptime(duty_window.start_time, "%Y-%m-%d %H:%M:%S")
        shift_end = datetime.strptime(duty_window.end_time, "%Y-%m-%d %H:%M:%S")
        filter_window = build_shift_interval_window(
            shift_start=shift_start,
            shift_end=shift_end,
            offset_hours=1,
        )

        table_id = str(source.get("table_id", "")).strip()
        page_size = int(source.get("page_size", 500))
        max_records = int(source.get("max_records", 5000))
        emit_log(
            f"[交接班][变更管理] 读取飞书: table_id={table_id}, page_size={page_size}, "
            f"max_records={max_records}, window={shift_start.strftime('%Y-%m-%d %H:%M:%S')}~{shift_end.strftime('%Y-%m-%d %H:%M:%S')}, "
            f"filter_window={filter_window.filter_start.strftime('%Y-%m-%d %H:%M:%S')}~{filter_window.filter_end.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        start_time_field = str(fields_cfg.get("start_time", "")).strip() or str(monthly_fields_cfg.get("start_time", "")).strip()
        end_time_field = str(fields_cfg.get("end_time", "")).strip() or str(monthly_fields_cfg.get("end_time", "")).strip()
        updated_field = str(fields_cfg.get("updated_time", "更新最新的时间")).strip()
        building_field = str(fields_cfg.get("building", "楼栋")).strip()
        change_level_field = str(fields_cfg.get("change_level", "阿里-变更等级")).strip()
        process_updates_field = str(fields_cfg.get("process_updates", "过程更新时间")).strip()
        description_field = str(fields_cfg.get("description", "名称")).strip()
        specialty_field = str(fields_cfg.get("specialty", "专业")).strip()

        client = self._new_client(cfg)
        option_maps = self._load_field_option_maps(
            client=client,
            table_id=table_id,
            target_fields=[building_field, specialty_field, change_level_field],
            emit_log=emit_log,
        )
        records = client.list_records(table_id=table_id, page_size=page_size, max_records=max_records)

        output: List[ChangeManagementRow] = []
        multi_building_skipped = 0
        blank_building_skipped = 0
        start_time_parse_fail = 0
        end_time_parse_fail = 0
        end_time_before_start_skipped = 0
        start_time_after_filter_end_skipped = 0
        end_time_before_filter_start_skipped = 0
        blank_description_skipped = 0

        for item in records:
            if not isinstance(item, dict):
                continue
            fields = item.get("fields", {})
            if not isinstance(fields, dict):
                continue

            building_values = _field_texts_with_option_map(
                fields.get(building_field),
                option_maps.get(building_field, {}),
            )
            if len(building_values) != 1:
                if building_values:
                    multi_building_skipped += 1
                    emit_log(
                        f"[交接班][变更管理] 跳过多楼记录: record_id={str(item.get('record_id', '')).strip()}, "
                        f"buildings={'/'.join(building_values)}"
                    )
                else:
                    blank_building_skipped += 1
                continue

            raw_start_time = fields.get(start_time_field) if start_time_field else None
            start_time = _parse_datetime(raw_start_time)
            if start_time is None and updated_field:
                start_time = _parse_datetime(fields.get(updated_field))
            if start_time is None:
                start_time_parse_fail += 1
                continue
            raw_end_time = fields.get(end_time_field) if end_time_field else None
            end_time_text = _field_text(raw_end_time)
            end_time = _parse_datetime(raw_end_time)
            if end_time is None and end_time_text:
                end_time_parse_fail += 1
                continue
            if end_time is not None and end_time < start_time:
                end_time_before_start_skipped += 1
                continue
            if start_time > filter_window.filter_end:
                start_time_after_filter_end_skipped += 1
                continue
            if end_time is not None and end_time <= filter_window.filter_start:
                end_time_before_filter_start_skipped += 1
                continue
            if not interval_overlaps_filter_window(
                start_time=start_time,
                end_time=end_time,
                filter_start=filter_window.filter_start,
                filter_end=filter_window.filter_end,
            ):
                continue

            raw_specialty_text = _field_text(fields.get(specialty_field)).strip()
            specialty_text = _field_text_with_option_map(
                fields.get(specialty_field),
                option_maps.get(specialty_field, {}),
            ).strip()
            if raw_specialty_text and raw_specialty_text == specialty_text and re.fullmatch(r"opt[A-Za-z0-9]+", raw_specialty_text):
                emit_log(
                    f"[交接班][变更管理] 专业选项未命中: record_id={str(item.get('record_id', '')).strip()}, "
                    f"raw={raw_specialty_text}"
                )

            description_text = _field_text(fields.get(description_field)).strip()
            if not description_text:
                blank_description_skipped += 1
                continue

            output.append(
                ChangeManagementRow(
                    record_id=str(item.get("record_id", "")).strip(),
                    building_values=building_values,
                    start_time=start_time,
                    end_time=end_time,
                    change_level=_field_text_with_option_map(
                        fields.get(change_level_field),
                        option_maps.get(change_level_field, {}),
                    ).strip(),
                    process_updates_text=_field_text(fields.get(process_updates_field)).strip(),
                    description=description_text,
                    specialty_text=normalize_specialty_text(specialty_text),
                    raw_fields=fields,
                )
            )

        output.sort(key=lambda row: (row.start_time, row.record_id))
        return output, cfg, {
            "total": len(records),
            "in_shift": len(output),
            "single_building_rows": len(output),
            "multi_building_skipped": multi_building_skipped,
            "blank_building_skipped": blank_building_skipped,
            "start_time_parse_fail": start_time_parse_fail,
            "end_time_parse_fail": end_time_parse_fail,
            "end_time_before_start_skipped": end_time_before_start_skipped,
            "start_time_after_filter_end_skipped": start_time_after_filter_end_skipped,
            "end_time_before_filter_start_skipped": end_time_before_filter_start_skipped,
            "blank_description_skipped": blank_description_skipped,
        }

    def list_current_shift_rows(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> tuple[List[ChangeManagementRow], Dict[str, Any]]:
        rows, cfg, counters = self._load_rows_for_shift(
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )
        output = [row for row in rows if _building_exact_single_match(building, row.building_values)]
        non_exact_building_skipped = max(0, counters["single_building_rows"] - len(output))
        emit_log(
            f"[交接班][变更管理] 读取完成: total={counters['total']}, single_building_hit={len(output)}, "
            f"in_shift={len(output)}, multi_building_skipped={counters['multi_building_skipped']}, "
            f"non_exact_building_skipped={non_exact_building_skipped}, "
            f"start_time_parse_fail={counters.get('start_time_parse_fail', 0)}, "
            f"end_time_parse_fail={counters.get('end_time_parse_fail', 0)}, "
            f"end_time_before_start_skipped={counters.get('end_time_before_start_skipped', 0)}, "
            f"start_time_after_filter_end_skipped={counters.get('start_time_after_filter_end_skipped', 0)}, "
            f"end_time_before_filter_start_skipped={counters.get('end_time_before_filter_start_skipped', 0)}, "
            f"blank_description_skipped={counters['blank_description_skipped']}"
        )
        return output, cfg

    def list_current_shift_rows_grouped(
        self,
        *,
        buildings: List[str],
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> tuple[ChangeRowsByBuilding, Dict[str, Any]]:
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

        grouped: ChangeRowsByBuilding = {building: [] for building in target_buildings}
        assigned_rows = 0
        for row in rows:
            for building in target_buildings:
                if not _building_exact_single_match(building, row.building_values):
                    continue
                grouped[building].append(row)
                assigned_rows += 1
                break

        emit_log(
            f"[交接班][变更管理] 读取完成: total={counters['total']}, in_shift={counters['in_shift']}, "
            f"building_assigned={assigned_rows}, multi_building_skipped={counters['multi_building_skipped']}, "
            f"blank_building_skipped={counters['blank_building_skipped']}, "
            f"start_time_parse_fail={counters.get('start_time_parse_fail', 0)}, "
            f"end_time_parse_fail={counters.get('end_time_parse_fail', 0)}, "
            f"end_time_before_start_skipped={counters.get('end_time_before_start_skipped', 0)}, "
            f"start_time_after_filter_end_skipped={counters.get('start_time_after_filter_end_skipped', 0)}, "
            f"end_time_before_filter_start_skipped={counters.get('end_time_before_filter_start_skipped', 0)}, "
            f"blank_description_skipped={counters['blank_description_skipped']}"
        )
        emit_log(
            "[交接班][变更管理] 批量预取完成: "
            f"buildings={len(target_buildings)}, total_records={counters['total']}, assigned_rows={assigned_rows}"
        )
        return grouped, cfg
