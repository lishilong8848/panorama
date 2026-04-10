from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Callable, Dict, List, Tuple

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
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
class OtherImportantWorkRow:
    source_key: str
    source_label: str
    record_id: str
    building_values: List[str]
    actual_end_time: datetime | None
    description_text: str
    completion_text: str
    specialty_text: str
    raw_fields: Dict[str, Any]


OtherImportantWorkRowsByBuilding = Dict[str, List[OtherImportantWorkRow]]


class OtherImportantWorkRepository:
    DEVICE_ADJUSTMENT_TABLE_ID = "tbleqBZdQu1n8qqK"

    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg
        self._field_option_maps_cache: Dict[str, Dict[str, Dict[str, str]]] = {}

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "source": {
                "app_token": "D01TwFPyXiJBY6kCBDZcMCGLnSe",
                "page_size": 500,
                "max_records": 5000,
            },
            "sections": {
                "other_important_work": "其他重要工作记录",
            },
            "order": [
                "power_notice",
                "device_adjustment",
                "device_patrol",
                "device_repair",
            ],
            "column_mapping": {
                "resolve_by_header": True,
                "header_alias": {
                    "description": ["描述"],
                    "completion": ["完成情况"],
                    "executor": ["执行人", "跟进人"],
                },
                "fallback_cols": {
                    "description": "B",
                    "completion": "F",
                    "executor": "H",
                },
            },
            "sources": {
                "power_notice": {
                    "label": "上电通告",
                    "table_id": "tblf2uQrzCWw5eIV",
                    "fields": {
                        "building": "楼栋",
                        "actual_end_time": "实际结束时间",
                        "description": "名称",
                        "completion": "进度",
                        "specialty": "专业",
                    },
                },
                "device_adjustment": {
                    "label": "设备调整",
                    "table_id": self.DEVICE_ADJUSTMENT_TABLE_ID,
                    "fields": {
                        "building": "楼栋",
                        "actual_end_time": "实际结束时间",
                        "location": "位置",
                        "description": "内容",
                        "completion": "进度",
                        "specialty": "专业",
                    },
                },
                "device_patrol": {
                    "label": "设备轮巡",
                    "table_id": "tbl0XK1iQ1P6VY5Y",
                    "fields": {
                        "building": "楼栋",
                        "actual_end_time": "实际结束时间",
                        "description": "内容",
                        "completion": "进度",
                        "specialty": "专业",
                    },
                },
                "device_repair": {
                    "label": "设备检修",
                    "table_id": "tblpaHktT0mn0hwg",
                    "fields": {
                        "building": "楼栋",
                        "actual_end_time": "实际结束时间",
                        "description": "维修故障",
                        "completion": "进度（完成情况）",
                        "specialty": "专业",
                    },
                },
            },
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("other_important_work_section", {})
        cfg = _deep_merge(self._defaults(), raw if isinstance(raw, dict) else {})

        source = cfg.get("source", {})
        source["app_token"] = str(source.get("app_token", "")).strip()
        source["page_size"] = max(1, int(source.get("page_size", 500) or 500))
        source["max_records"] = max(1, int(source.get("max_records", 5000) or 5000))
        cfg["source"] = source

        raw_order = cfg.get("order", [])
        order = raw_order if isinstance(raw_order, list) else []
        cfg["order"] = [str(item or "").strip() for item in order if str(item or "").strip()]

        sources_cfg = cfg.get("sources", {})
        if not isinstance(sources_cfg, dict):
            sources_cfg = {}
        normalized_sources: Dict[str, Any] = {}
        for source_key, source_cfg in sources_cfg.items():
            if not isinstance(source_cfg, dict):
                continue
            current = dict(source_cfg)
            current["label"] = str(current.get("label", "")).strip()
            current["table_id"] = str(current.get("table_id", "")).strip()
            fields = current.get("fields", {})
            current["fields"] = fields if isinstance(fields, dict) else {}
            normalized_sources[str(source_key).strip()] = current
        cfg["sources"] = normalized_sources
        return cfg

    def get_config(self) -> Dict[str, Any]:
        return self._normalize_cfg()

    def _new_client(self, *, app_token: str, table_id: str) -> FeishuBitableClient:
        global_feishu = self.handover_cfg.get("_global_feishu", {})
        if not isinstance(global_feishu, dict):
            global_feishu = {}
        app_id = str(global_feishu.get("app_id", "")).strip()
        app_secret = str(global_feishu.get("app_secret", "")).strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")
        if not app_token or not table_id:
            raise ValueError("其他重要工作记录多维配置缺失: app_token/table_id")

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
            emit_log(f"[交接班][其他重要工作] 字段定义读取失败，选项映射按空继续: table_id={table_key}, error={exc}")
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
        emit_log(f"[交接班][其他重要工作] 字段选项映射已加载: table_id={table_key}, {counts}")
        return {field_name: dict(output.get(field_name, {})) for field_name in field_names}

    @staticmethod
    def _sort_key(row: OtherImportantWorkRow) -> tuple[int, datetime, str]:
        has_end_time = 1 if row.actual_end_time is not None else 0
        end_time = row.actual_end_time or datetime.max
        return has_end_time, end_time, row.record_id

    @staticmethod
    def _join_location_and_description(location_text: str, description_text: str) -> str:
        location = str(location_text or "").strip()
        description = str(description_text or "").strip()
        if location and description:
            return f"{location} {description}"
        return description or location

    def _load_source_rows_for_shift(
        self,
        *,
        source_key: str,
        source_cfg: Dict[str, Any],
        shared_source_cfg: Dict[str, Any],
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None],
    ) -> Tuple[List[OtherImportantWorkRow], Dict[str, int]]:
        table_id = str(source_cfg.get("table_id", "")).strip()
        label = str(source_cfg.get("label", "")).strip() or source_key
        fields_cfg = source_cfg.get("fields", {}) if isinstance(source_cfg.get("fields", {}), dict) else {}
        building_field = str(fields_cfg.get("building", "楼栋")).strip() or "楼栋"
        actual_end_time_field = str(fields_cfg.get("actual_end_time", "实际结束时间")).strip() or "实际结束时间"
        description_field = str(fields_cfg.get("description", "内容")).strip() or "内容"
        completion_field = str(fields_cfg.get("completion", "进度")).strip() or "进度"
        specialty_field = str(fields_cfg.get("specialty", "专业")).strip() or "专业"
        should_concat_location = (
            str(source_key or "").strip() == "device_adjustment"
            or table_id == self.DEVICE_ADJUSTMENT_TABLE_ID
        )
        location_field = str(fields_cfg.get("location", "位置")).strip() or "位置"

        download_cfg = self.handover_cfg.get("download", {})
        shift_windows = download_cfg.get("shift_windows", {}) if isinstance(download_cfg, dict) else {}
        duty_window = build_duty_window(
            duty_date=duty_date,
            duty_shift=duty_shift,
            shift_windows=shift_windows if isinstance(shift_windows, dict) else {},
        )
        shift_start = datetime.strptime(duty_window.start_time, "%Y-%m-%d %H:%M:%S")
        shift_end = datetime.strptime(duty_window.end_time, "%Y-%m-%d %H:%M:%S")

        emit_log(
            "[交接班][其他重要工作] 读取飞书: "
            f"source={source_key}, label={label}, table_id={table_id}, "
            f"page_size={int(shared_source_cfg.get('page_size', 500) or 500)}, "
            f"max_records={int(shared_source_cfg.get('max_records', 5000) or 5000)}, "
            f"window={duty_window.start_time}~{duty_window.end_time}"
        )

        client = self._new_client(
            app_token=str(shared_source_cfg.get("app_token", "")).strip(),
            table_id=table_id,
        )
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
            page_size=int(shared_source_cfg.get("page_size", 500) or 500),
            max_records=int(shared_source_cfg.get("max_records", 5000) or 5000),
        )

        rows: List[OtherImportantWorkRow] = []
        total = 0
        in_scope = 0
        parse_fail = 0
        blank_description_skipped = 0
        for item in records:
            if not isinstance(item, dict):
                continue
            total += 1
            record_id = str(item.get("record_id", "")).strip()
            raw_fields = item.get("fields", {})
            if not isinstance(raw_fields, dict):
                raw_fields = {}

            building_values = _field_texts_with_option_map(raw_fields.get(building_field), building_option_map)
            raw_actual_end_time = raw_fields.get(actual_end_time_field)
            actual_end_time_text = _field_text(raw_actual_end_time)
            actual_end_time = _parse_datetime(raw_actual_end_time)
            if actual_end_time is None and actual_end_time_text:
                parse_fail += 1
                continue
            if actual_end_time is not None and (actual_end_time < shift_start or actual_end_time > shift_end):
                continue
            description_text = _field_text(raw_fields.get(description_field))
            if should_concat_location:
                location_text = _field_text(raw_fields.get(location_field))
                description_text = self._join_location_and_description(location_text, description_text)
            if not str(description_text or "").strip():
                blank_description_skipped += 1
                continue

            in_scope += 1
            rows.append(
                OtherImportantWorkRow(
                    source_key=source_key,
                    source_label=label,
                    record_id=record_id,
                    building_values=building_values,
                    actual_end_time=actual_end_time,
                    description_text=description_text,
                    completion_text=_field_text(raw_fields.get(completion_field)),
                    specialty_text=normalize_specialty_text(
                        _field_text_with_option_map(raw_fields.get(specialty_field), specialty_option_map)
                    ),
                    raw_fields=raw_fields,
                )
            )

        rows.sort(key=self._sort_key)
        emit_log(
            "[交接班][其他重要工作] 读取完成: "
            f"source={source_key}, total={total}, in_scope={in_scope}, "
            f"blank_description_skipped={blank_description_skipped}, parse_fail={parse_fail}"
        )
        return rows, {"total": total, "in_scope": in_scope, "parse_fail": parse_fail}

    def _load_rows_for_shift(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None],
    ) -> Tuple[List[OtherImportantWorkRow], Dict[str, Any], Dict[str, int]]:
        cfg = self._normalize_cfg()
        if not bool(cfg.get("enabled", True)):
            return [], cfg, {"total": 0, "in_scope": 0, "parse_fail": 0}

        shared_source_cfg = cfg.get("source", {}) if isinstance(cfg.get("source", {}), dict) else {}
        sources_cfg = cfg.get("sources", {}) if isinstance(cfg.get("sources", {}), dict) else {}
        order = cfg.get("order", []) if isinstance(cfg.get("order", []), list) else []

        all_rows: List[OtherImportantWorkRow] = []
        total = 0
        in_scope = 0
        parse_fail = 0
        for source_key in order:
            current_cfg = sources_cfg.get(source_key, {})
            if not isinstance(current_cfg, dict):
                continue
            try:
                source_rows, counters = self._load_source_rows_for_shift(
                    source_key=source_key,
                    source_cfg=current_cfg,
                    shared_source_cfg=shared_source_cfg,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    emit_log=emit_log,
                )
            except Exception as exc:  # noqa: BLE001
                emit_log(
                    "[交接班][其他重要工作] 读取失败，跳过该来源继续: "
                    f"source={source_key}, error={exc}"
                )
                continue
            all_rows.extend(source_rows)
            total += int(counters.get("total", 0) or 0)
            in_scope += int(counters.get("in_scope", 0) or 0)
            parse_fail += int(counters.get("parse_fail", 0) or 0)

        return all_rows, cfg, {"total": total, "in_scope": in_scope, "parse_fail": parse_fail}

    def list_current_shift_rows(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> Tuple[List[OtherImportantWorkRow], Dict[str, Any]]:
        rows, cfg, counters = self._load_rows_for_shift(
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )
        matched_rows = [row for row in rows if _building_matches(building, row.building_values)]
        emit_log(
            "[交接班][其他重要工作] 读取完成: "
            f"total={counters['total']}, in_scope={counters['in_scope']}, "
            f"building_assigned={len(matched_rows)}, parse_fail={counters['parse_fail']}"
        )
        return matched_rows, cfg

    def list_current_shift_rows_grouped(
        self,
        *,
        buildings: List[str],
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> Tuple[OtherImportantWorkRowsByBuilding, Dict[str, Any]]:
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

        grouped: OtherImportantWorkRowsByBuilding = {building: [] for building in target_buildings}
        assigned_rows = 0
        for row in rows:
            for building in target_buildings:
                if not _building_matches(building, row.building_values):
                    continue
                grouped[building].append(row)
                assigned_rows += 1

        emit_log(
            "[交接班][其他重要工作] 读取完成: "
            f"total={counters['total']}, in_scope={counters['in_scope']}, "
            f"building_assigned={assigned_rows}, parse_fail={counters['parse_fail']}"
        )
        emit_log(
            "[交接班][其他重要工作] 批量分桶完成: "
            f"buildings={len(target_buildings)}, total_rows={len(rows)}, assigned_rows={assigned_rows}"
        )
        return grouped, cfg
