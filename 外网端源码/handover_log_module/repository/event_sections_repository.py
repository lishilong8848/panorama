from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import re
from typing import Any, Callable, Dict, List, Optional

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from handover_log_module.core.shift_window import normalize_duty_shift, parse_duty_date


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
        return "、".join([x for x in parts if x])
    return str(value).strip()


def _is_checked(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) != 0
    if isinstance(value, list):
        return len(value) > 0
    text = _field_text(value).casefold()
    if not text:
        return False
    return text in {"1", "true", "yes", "y", "on", "是", "已选中", "已勾选", "勾选"}


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        num = int(float(value))
        if abs(num) >= 10**12:
            return datetime.fromtimestamp(num / 1000)
        return datetime.fromtimestamp(num)
    if isinstance(value, list):
        for item in value:
            dt = _parse_datetime(item)
            if dt is not None:
                return dt
        return None
    if isinstance(value, dict):
        for key in ("value", "text", "name", "label"):
            if key in value:
                dt = _parse_datetime(value.get(key))
                if dt is not None:
                    return dt
        return None

    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        num = int(text)
        if len(text) >= 13 or abs(num) >= 10**12:
            return datetime.fromtimestamp(num / 1000)
        return datetime.fromtimestamp(num)
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


def _fmt_time_hm(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.strftime("%H:%M")


@dataclass
class EventRow:
    record_id: str
    event_time: datetime | None
    event_level: str
    description: str
    building_text: str
    final_status_text: str
    excluded_checked: bool
    to_maint: bool
    maint_done_time: datetime | None
    event_done_time: datetime | None
    raw_fields: Dict[str, Any]


@dataclass
class EventSectionQueryResult:
    current_rows: List[EventRow]
    outside_shift_ongoing_rows: List[EventRow]
    historical_open_rows: List[EventRow]
    shift_start: datetime
    shift_end: datetime
    cfg: Dict[str, Any]


EventQueryByBuilding = Dict[str, EventSectionQueryResult]


class EventSectionsRepository:
    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "source": {
                "app_token": "D01TwFPyXiJBY6kCBDZcMCGLnSe",
                "table_id": "tblj9XJLq5QzTAqX",
                "page_size": 500,
                "max_records": 5000,
            },
            "duty_window": {
                "day_start": "09:00:00",
                "day_end": "18:00:00",
                "night_start": "18:00:00",
                "night_end_next_day": "09:00:00",
                "boundary_mode": "left_closed_right_open",
            },
            "fields": {
                "event_time": "事件发生时间",
                "building": "机楼",
                "event_level": "事件等级",
                "description": "告警描述",
                "exclude_checked": "不计入事件",
                "final_status": "最终状态",
                "exclude_duration": "事件结束处理时长",
                "exclude_duration_value": "不计入事件",
                "to_maint": "是否转检修",
                "maint_done_time": "检修完成时间",
                "event_done_time": "事件结束时间",
            },
            "sections": {
                "new_event": "新事件处理",
                "history_followup": "历史事件跟进",
            },
            "column_mapping": {
                "resolve_by_header": True,
                "header_alias": {
                    "event_level": ["事件等级"],
                    "event_time": ["发生时间"],
                    "description": ["描述", "告警描述"],
                    "work_window": ["作业时间段"],
                    "progress": ["事件处理进展"],
                    "follower": ["跟进人"],
                },
                "fallback_cols": {
                    "event_level": "B",
                    "event_time": "C",
                    "description": "D",
                    "work_window": "E",
                    "progress": "F",
                    "follower": "G",
                },
            },
            "progress_text": {
                "done": "已完成",
                "todo": "未完成",
            },
            "cache": {
                "enabled": True,
                "state_file": "handover_shared_cache.json",
                "max_pending": 20000,
                "max_last_query_ids": 5000,
            },
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("event_sections", {})
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
            raise ValueError("交接班事件多维配置缺失: app_token/table_id")

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
    def _build_duty_window(duty_date: str, duty_shift: str, cfg: Dict[str, Any]) -> tuple[datetime, datetime]:
        duty_day = parse_duty_date(duty_date)
        shift = normalize_duty_shift(duty_shift)
        duty_window = cfg.get("duty_window", {})
        if not isinstance(duty_window, dict):
            duty_window = {}

        def _parse_hms(name: str, default_value: str) -> tuple[int, int, int]:
            text = str(duty_window.get(name, default_value) or default_value).strip() or default_value
            dt = datetime.strptime(text, "%H:%M:%S")
            return dt.hour, dt.minute, dt.second

        day_start = _parse_hms("day_start", "09:00:00")
        day_end = _parse_hms("day_end", "18:00:00")
        night_start = _parse_hms("night_start", "18:00:00")
        night_end = _parse_hms("night_end_next_day", "09:00:00")

        if shift == "day":
            start_dt = datetime(duty_day.year, duty_day.month, duty_day.day, *day_start)
            end_dt = datetime(duty_day.year, duty_day.month, duty_day.day, *day_end)
        else:
            next_day = duty_day + timedelta(days=1)
            start_dt = datetime(duty_day.year, duty_day.month, duty_day.day, *night_start)
            end_dt = datetime(next_day.year, next_day.month, next_day.day, *night_end)

        if end_dt <= start_dt:
            raise ValueError(f"事件时间窗无效: start={start_dt}, end={end_dt}")
        return start_dt, end_dt

    @staticmethod
    def _extract_building_code(text: str) -> str:
        raw = str(text or "").strip().upper()
        if not raw:
            return ""
        # 支持 A楼/A栋/EA118机房A栋 等形态，取首个楼栋编码。
        match = re.search(r"([A-E])\s*(?:楼|栋)?", raw)
        if match:
            return match.group(1)
        return ""

    @classmethod
    def _building_matches(cls, target_building: str, building_text: str) -> bool:
        target = str(target_building or "").strip()
        value = str(building_text or "").strip()
        if not target or not value:
            return False
        if target.casefold() == value.casefold():
            return True
        return cls._extract_building_code(target) and cls._extract_building_code(target) == cls._extract_building_code(value)

    @staticmethod
    def _normalize_final_status(text: str) -> str:
        raw = str(text or "").strip()
        if raw == "事件闭环转检修中":
            return "todo"
        if raw in {"事件闭环转检修完成", "事件闭环未转检修"}:
            return "done"
        return "todo"

    def load_current_shift_events(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        now: datetime | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> EventSectionQueryResult:
        cfg = self._normalize_cfg()
        start_dt, end_dt = self._build_duty_window(duty_date, duty_shift, cfg)
        if not bool(cfg.get("enabled", True)):
            return EventSectionQueryResult(
                current_rows=[],
                outside_shift_ongoing_rows=[],
                historical_open_rows=[],
                shift_start=start_dt,
                shift_end=end_dt,
                cfg=cfg,
            )

        source = cfg.get("source", {})
        fields_cfg = cfg.get("fields", {})
        if not isinstance(fields_cfg, dict):
            fields_cfg = {}

        now_dt = now or datetime.now()
        table_id = str(source.get("table_id", "")).strip()
        page_size = int(source.get("page_size", 500))
        max_records = int(source.get("max_records", 5000))
        emit_log(
            f"[交接班][事件分类] 读取飞书: table_id={table_id}, page_size={page_size}, "
            f"max_records={max_records}, window={start_dt.strftime('%Y-%m-%d %H:%M:%S')}~{end_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        client = self._new_client(cfg)
        records = client.list_records(
            table_id=table_id,
            page_size=page_size,
            max_records=max_records,
        )

        building_field = str(fields_cfg.get("building", "机楼")).strip()
        exclude_checked_field = str(fields_cfg.get("exclude_checked", "不计入事件")).strip()
        final_status_field = str(fields_cfg.get("final_status", "最终状态")).strip()
        event_time_field = str(fields_cfg.get("event_time", "事件发生时间")).strip()
        event_level_field = str(fields_cfg.get("event_level", "事件等级")).strip()
        desc_field = str(fields_cfg.get("description", "告警描述")).strip()
        to_maint_field = str(fields_cfg.get("to_maint", "是否转检修")).strip()
        maint_done_field = str(fields_cfg.get("maint_done_time", "检修完成时间")).strip()
        event_done_field = str(fields_cfg.get("event_done_time", "事件结束时间")).strip()

        in_shift_rows: List[EventRow] = []
        out_of_shift_ongoing_rows: List[EventRow] = []
        historical_open_rows: List[EventRow] = []
        building_hit_count = 0
        excluded_count = 0
        parse_fail_count = 0
        for item in records:
            if not isinstance(item, dict):
                continue
            record_id = str(item.get("record_id", "")).strip()
            fields = item.get("fields", {})
            if not record_id or not isinstance(fields, dict):
                continue

            building_text = _field_text(fields.get(building_field))
            if not self._building_matches(building, building_text):
                continue
            building_hit_count += 1

            excluded_checked = _is_checked(fields.get(exclude_checked_field))
            if excluded_checked:
                excluded_count += 1
                continue

            event_time = _parse_datetime(fields.get(event_time_field))
            if event_time is None:
                parse_fail_count += 1
                continue

            final_status_text = _field_text(fields.get(final_status_field))
            row = EventRow(
                record_id=record_id,
                event_time=event_time,
                event_level=_field_text(fields.get(event_level_field)),
                description=_field_text(fields.get(desc_field)),
                building_text=building_text,
                final_status_text=final_status_text,
                excluded_checked=excluded_checked,
                to_maint=_is_checked(fields.get(to_maint_field)),
                maint_done_time=_parse_datetime(fields.get(maint_done_field)),
                event_done_time=_parse_datetime(fields.get(event_done_field)),
                raw_fields=dict(fields),
            )
            if start_dt <= event_time < end_dt:
                in_shift_rows.append(row)
                continue
            if event_time < start_dt and (row.event_done_time is None or row.event_done_time > now_dt):
                historical_open_rows.append(row)
            if str(final_status_text or "").strip() == "事件闭环转检修中":
                out_of_shift_ongoing_rows.append(row)

        emit_log(
            "[交接班][事件分类] 读取完成: "
            f"total={len(records)}, building_hit={building_hit_count}, "
            f"in_shift={len(in_shift_rows)}, out_shift_ongoing={len(out_of_shift_ongoing_rows)}, "
            f"historical_open={len(historical_open_rows)}, "
            f"excluded={excluded_count}, parse_fail={parse_fail_count}"
        )
        return EventSectionQueryResult(
            current_rows=in_shift_rows,
            outside_shift_ongoing_rows=out_of_shift_ongoing_rows,
            historical_open_rows=historical_open_rows,
            shift_start=start_dt,
            shift_end=end_dt,
            cfg=cfg,
        )

    def load_current_shift_events_grouped(
        self,
        *,
        buildings: List[str],
        duty_date: str,
        duty_shift: str,
        now: datetime | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> EventQueryByBuilding:
        cfg = self._normalize_cfg()
        start_dt, end_dt = self._build_duty_window(duty_date, duty_shift, cfg)
        target_buildings: List[str] = []
        for raw in buildings:
            building = str(raw or "").strip()
            if building and building not in target_buildings:
                target_buildings.append(building)

        if not target_buildings:
            return {}

        if not bool(cfg.get("enabled", True)):
            return {
                building: EventSectionQueryResult(
                    current_rows=[],
                    outside_shift_ongoing_rows=[],
                    historical_open_rows=[],
                    shift_start=start_dt,
                    shift_end=end_dt,
                    cfg=cfg,
                )
                for building in target_buildings
            }

        source = cfg.get("source", {})
        fields_cfg = cfg.get("fields", {})
        if not isinstance(fields_cfg, dict):
            fields_cfg = {}

        now_dt = now or datetime.now()
        table_id = str(source.get("table_id", "")).strip()
        page_size = int(source.get("page_size", 500))
        max_records = int(source.get("max_records", 5000))
        emit_log(
            f"[交接班][事件分类] 读取飞书: table_id={table_id}, page_size={page_size}, "
            f"max_records={max_records}, window={start_dt.strftime('%Y-%m-%d %H:%M:%S')}~{end_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        client = self._new_client(cfg)
        records = client.list_records(
            table_id=table_id,
            page_size=page_size,
            max_records=max_records,
        )

        building_field = str(fields_cfg.get("building", "机楼")).strip()
        exclude_checked_field = str(fields_cfg.get("exclude_checked", "不计入事件")).strip()
        final_status_field = str(fields_cfg.get("final_status", "最终状态")).strip()
        event_time_field = str(fields_cfg.get("event_time", "事件发生时间")).strip()
        event_level_field = str(fields_cfg.get("event_level", "事件等级")).strip()
        desc_field = str(fields_cfg.get("description", "告警描述")).strip()
        to_maint_field = str(fields_cfg.get("to_maint", "是否转检修")).strip()
        maint_done_field = str(fields_cfg.get("maint_done_time", "检修完成时间")).strip()
        event_done_field = str(fields_cfg.get("event_done_time", "事件结束时间")).strip()

        grouped: EventQueryByBuilding = {
            building: EventSectionQueryResult(
                current_rows=[],
                outside_shift_ongoing_rows=[],
                historical_open_rows=[],
                shift_start=start_dt,
                shift_end=end_dt,
                cfg=cfg,
            )
            for building in target_buildings
        }
        building_hit_count = 0
        excluded_count = 0
        parse_fail_count = 0
        assigned_rows = 0
        for item in records:
            if not isinstance(item, dict):
                continue
            record_id = str(item.get("record_id", "")).strip()
            fields = item.get("fields", {})
            if not record_id or not isinstance(fields, dict):
                continue

            building_text = _field_text(fields.get(building_field))
            matched_buildings = [
                building for building in target_buildings if self._building_matches(building, building_text)
            ]
            if not matched_buildings:
                continue
            building_hit_count += len(matched_buildings)

            excluded_checked = _is_checked(fields.get(exclude_checked_field))
            if excluded_checked:
                excluded_count += len(matched_buildings)
                continue

            event_time = _parse_datetime(fields.get(event_time_field))
            if event_time is None:
                parse_fail_count += len(matched_buildings)
                continue

            final_status_text = _field_text(fields.get(final_status_field))
            row = EventRow(
                record_id=record_id,
                event_time=event_time,
                event_level=_field_text(fields.get(event_level_field)),
                description=_field_text(fields.get(desc_field)),
                building_text=building_text,
                final_status_text=final_status_text,
                excluded_checked=excluded_checked,
                to_maint=_is_checked(fields.get(to_maint_field)),
                maint_done_time=_parse_datetime(fields.get(maint_done_field)),
                event_done_time=_parse_datetime(fields.get(event_done_field)),
                raw_fields=dict(fields),
            )

            for building in matched_buildings:
                assigned = False
                result = grouped[building]
                if start_dt <= event_time < end_dt:
                    result.current_rows.append(row)
                    assigned = True
                else:
                    if event_time < start_dt and (row.event_done_time is None or row.event_done_time > now_dt):
                        result.historical_open_rows.append(row)
                        assigned = True
                    if str(final_status_text or "").strip() == "事件闭环转检修中":
                        result.outside_shift_ongoing_rows.append(row)
                        assigned = True
                if assigned:
                    assigned_rows += 1

        emit_log(
            "[交接班][事件分类] 读取完成: "
            f"total={len(records)}, building_hit={building_hit_count}, "
            f"assigned_rows={assigned_rows}, excluded={excluded_count}, parse_fail={parse_fail_count}"
        )
        emit_log(
            "[交接班][事件分类] 批量预取完成: "
            f"buildings={len(target_buildings)}, total_records={len(records)}, assigned_rows={assigned_rows}"
        )
        return grouped

    def get_record_by_id(
        self,
        *,
        record_id: str,
    ) -> Optional[EventRow]:
        cfg = self._normalize_cfg()
        if not bool(cfg.get("enabled", True)):
            return None

        source = cfg.get("source", {})
        fields_cfg = cfg.get("fields", {})
        if not isinstance(fields_cfg, dict):
            fields_cfg = {}
        table_id = str(source.get("table_id", "")).strip()
        rid = str(record_id or "").strip()
        if not rid or not table_id:
            return None

        client = self._new_client(cfg)
        payload = client.get_record_by_id(table_id=table_id, record_id=rid)
        if not isinstance(payload, dict):
            return None
        fields = payload.get("fields", {})
        if not isinstance(fields, dict):
            return None

        event_time_field = str(fields_cfg.get("event_time", "事件发生时间")).strip()
        building_field = str(fields_cfg.get("building", "机楼")).strip()
        exclude_checked_field = str(fields_cfg.get("exclude_checked", "不计入事件")).strip()
        final_status_field = str(fields_cfg.get("final_status", "最终状态")).strip()
        event_level_field = str(fields_cfg.get("event_level", "事件等级")).strip()
        desc_field = str(fields_cfg.get("description", "告警描述")).strip()
        to_maint_field = str(fields_cfg.get("to_maint", "是否转检修")).strip()
        maint_done_field = str(fields_cfg.get("maint_done_time", "检修完成时间")).strip()
        event_done_field = str(fields_cfg.get("event_done_time", "事件结束时间")).strip()

        return EventRow(
            record_id=rid,
            event_time=_parse_datetime(fields.get(event_time_field)),
            event_level=_field_text(fields.get(event_level_field)),
            description=_field_text(fields.get(desc_field)),
            building_text=_field_text(fields.get(building_field)),
            final_status_text=_field_text(fields.get(final_status_field)),
            excluded_checked=_is_checked(fields.get(exclude_checked_field)),
            to_maint=_is_checked(fields.get(to_maint_field)),
            maint_done_time=_parse_datetime(fields.get(maint_done_field)),
            event_done_time=_parse_datetime(fields.get(event_done_field)),
            raw_fields=dict(fields),
        )

    @staticmethod
    def get_progress_text(row: EventRow, progress_cfg: Dict[str, Any]) -> str:
        done_text = str(progress_cfg.get("done", "已完成")).strip() or "已完成"
        todo_text = str(progress_cfg.get("todo", "未完成")).strip() or "未完成"
        status = EventSectionsRepository._normalize_final_status(row.final_status_text)
        if status == "done":
            return done_text
        if status == "todo":
            return todo_text
        if row.to_maint:
            return done_text if row.maint_done_time is not None else todo_text
        return done_text if row.event_done_time is not None else todo_text

    @staticmethod
    def get_work_window_text_for_history(
        *,
        duty_shift: str,
        maint_done_time: datetime | None,
    ) -> str:
        if maint_done_time is None:
            return "/"
        start_hm = "09:00" if normalize_duty_shift(duty_shift) == "day" else "18:00"
        return f"{start_hm}-{_fmt_time_hm(maint_done_time)}"
