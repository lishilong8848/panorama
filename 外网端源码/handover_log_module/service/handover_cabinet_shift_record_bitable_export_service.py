from __future__ import annotations

import copy
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Set

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from handover_log_module.service.review_document_state_service import ReviewDocumentStateService


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


class HandoverCabinetShiftRecordBitableExportService:
    def __init__(self, handover_cfg: Dict[str, Any], *, log_prefix: str = "[交接班][机柜班次多维]") -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}
        self._review_document_state_service = ReviewDocumentStateService(self.handover_cfg)
        self._log_prefix = str(log_prefix or "").strip() or "[交接班][机柜班次多维]"

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "target": {
                "app_token": "G7oUwGdwaiTmimk8i2ecGTWOn4d",
                "table_id": "tblObkKSs8C3o33B",
                "page_size": 500,
                "max_records": 5000,
                "create_batch_size": 200,
                "update_batch_size": 200,
            },
            "fields": {
                "building": "楼栋",
                "date": "日期",
                "shift": "班次",
                "duty_staff": "值班人员",
                "handover_staff": "接班人员",
                "planned_cabinets": "机房总规划机柜数（个）",
                "powered_cabinets": "实际上电机柜数（个）",
                "shift_power_on_cabinets": "本班组上电机柜数（个）",
                "shift_power_off_cabinets": "本班组下电机柜数（个）",
            },
            "fixed_values": {
                "shift_text": {
                    "day": "白班",
                    "night": "夜班",
                },
            },
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("cabinet_shift_record_bitable_export", {})
        if not isinstance(raw, dict):
            raw = {}
        cfg = _deep_merge(self._defaults(), raw)
        target = cfg.get("target", {}) if isinstance(cfg.get("target", {}), dict) else {}
        fields = cfg.get("fields", {}) if isinstance(cfg.get("fields", {}), dict) else {}
        fixed_values = cfg.get("fixed_values", {}) if isinstance(cfg.get("fixed_values", {}), dict) else {}
        shift_text = fixed_values.get("shift_text", {}) if isinstance(fixed_values.get("shift_text", {}), dict) else {}

        target["app_token"] = str(target.get("app_token", "") or "").strip()
        target["table_id"] = str(target.get("table_id", "") or "").strip()
        target["page_size"] = max(1, int(target.get("page_size", 500) or 500))
        target["max_records"] = max(1, int(target.get("max_records", 5000) or 5000))
        target["create_batch_size"] = max(1, int(target.get("create_batch_size", 200) or 200))
        target["update_batch_size"] = max(1, int(target.get("update_batch_size", 200) or 200))
        cfg["target"] = target

        for key, default in self._defaults()["fields"].items():
            fields[key] = str(fields.get(key, default) or default).strip() or default
        cfg["fields"] = fields

        shift_text["day"] = str(shift_text.get("day", "白班") or "白班").strip() or "白班"
        shift_text["night"] = str(shift_text.get("night", "夜班") or "夜班").strip() or "夜班"
        fixed_values["shift_text"] = shift_text
        cfg["fixed_values"] = fixed_values
        cfg["enabled"] = bool(cfg.get("enabled", True))
        return cfg

    def _new_client(self, cfg: Dict[str, Any]) -> FeishuBitableClient:
        global_feishu = require_feishu_auth_settings(self.handover_cfg)
        target = cfg.get("target", {})
        app_token = str(target.get("app_token", "") or "").strip()
        table_id = str(target.get("table_id", "") or "").strip()
        if not app_token or not table_id:
            raise ValueError("机柜班次记录多维配置缺失: app_token/table_id")
        return FeishuBitableClient(
            app_id=str(global_feishu.get("app_id", "") or "").strip(),
            app_secret=str(global_feishu.get("app_secret", "") or "").strip(),
            app_token=app_token,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=lambda **_: 0,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
        )

    def _emit(self, emit_log: Callable[[str], None], message: str) -> None:
        emit_log(f"{self._log_prefix} {str(message or '').strip()}")

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _midnight_timestamp_ms(duty_date: str) -> int:
        dt = datetime.strptime(str(duty_date or "").strip(), "%Y-%m-%d")
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _normalize_date_to_midnight_ms(value: Any) -> int | None:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            number = int(value)
            if number <= 0:
                return None
            if number < 10**11:
                number *= 1000
            dt = datetime.fromtimestamp(number / 1000)
            midnight = datetime(dt.year, dt.month, dt.day, 0, 0, 0)
            return int(midnight.timestamp() * 1000)
        text = str(value or "").strip().replace("/", "-")
        if not text:
            return None
        try:
            dt = datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return None
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _normalize_select_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            for key in ("name", "text", "value"):
                text = str(value.get(key, "") or "").strip()
                if text:
                    return text
            return ""
        if isinstance(value, list):
            for item in value:
                text = HandoverCabinetShiftRecordBitableExportService._normalize_select_text(item)
                if text:
                    return text
            return ""
        return str(value).strip()

    @staticmethod
    def _normalize_number(value: Any) -> Any:
        if value is None:
            return None
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        try:
            number = Decimal(text)
        except (InvalidOperation, ValueError):
            return str(value).strip()
        if number == number.to_integral_value():
            return int(number)
        return float(number)

    @staticmethod
    def _normalize_number_from_text(value: Any) -> Any:
        if value is None:
            return None
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None
        return HandoverCabinetShiftRecordBitableExportService._normalize_number(match.group(0))

    @staticmethod
    def _is_meaningful_section_row(row: Dict[str, Any]) -> bool:
        if not isinstance(row, dict):
            return False
        if bool(row.get("is_placeholder_row", False)):
            return False
        cells = row.get("cells", {})
        if not isinstance(cells, dict):
            return False
        for value in cells.values():
            text = str(value or "").strip()
            if text and text != "/":
                return True
        return False

    @classmethod
    def _section_counts(cls, document: Dict[str, Any]) -> Dict[str, int]:
        counts = {
            "event_count": 0,
            "change_count": 0,
            "exercise_count": 0,
            "maintenance_count": 0,
            "construction_count": 0,
            "training_count": 0,
        }
        sections = document.get("sections", []) if isinstance(document, dict) else []
        if not isinstance(sections, list):
            return counts
        for section in sections:
            if not isinstance(section, dict):
                continue
            title = str(section.get("name", "") or "").strip()
            rows = section.get("rows", [])
            count = sum(1 for row in rows if isinstance(row, dict) and cls._is_meaningful_section_row(row))
            if count <= 0:
                continue
            if "事件" in title:
                counts["event_count"] += count
            elif "变更" in title:
                counts["change_count"] += count
            elif "演练" in title:
                counts["exercise_count"] += count
            elif "维护" in title:
                counts["maintenance_count"] += count
            elif "施工" in title:
                counts["construction_count"] += count
            elif "培训" in title:
                counts["training_count"] += count
        return counts

    @staticmethod
    def _should_skip_payload_field(name: str) -> bool:
        text = str(name or "").strip()
        return (not text) or ("提取" in text) or ("check" in text.lower())

    @classmethod
    def _coerce_payload_field_value(cls, name: str, value: Any, field_meta: Dict[str, Any] | None) -> Any:
        if field_meta is None:
            if "自动统计" in str(name or "") and value is not None:
                return str(value)
            return value
        try:
            field_type = int(field_meta.get("type", 0) or 0)
        except Exception:  # noqa: BLE001
            field_type = 0
        # 飞书文本/多行文本字段要求 string。部分目标表把“事件数量-自动统计”建成了多行文本，
        # 这里按字段类型转换，避免 TextFieldConvFail。
        if field_type == 1:
            return "" if value is None else str(value)
        if field_type == 2:
            return cls._normalize_number(value)
        return value

    @classmethod
    def _safe_payload_fields(cls, fields: Dict[str, Any], writable_field_meta: Dict[str, Dict[str, Any]] | Set[str] | None) -> Dict[str, Any]:
        output: Dict[str, Any] = {}
        for name, value in fields.items():
            field_name = str(name or "").strip()
            if cls._should_skip_payload_field(field_name):
                continue
            if writable_field_meta is None:
                output[field_name] = cls._coerce_payload_field_value(field_name, value, None)
                continue
            if isinstance(writable_field_meta, set):
                if field_name not in writable_field_meta:
                    continue
                output[field_name] = cls._coerce_payload_field_value(field_name, value, None)
                continue
            meta = writable_field_meta.get(field_name)
            if meta is None:
                continue
            output[field_name] = cls._coerce_payload_field_value(field_name, value, meta)
        return output

    @staticmethod
    def _shift_text(duty_shift: str, cfg: Dict[str, Any]) -> str:
        shift_key = str(duty_shift or "").strip().lower()
        shift_text_cfg = cfg.get("fixed_values", {}).get("shift_text", {})
        default = "白班" if shift_key == "day" else "夜班"
        return str(shift_text_cfg.get(shift_key, default) or default).strip()

    @staticmethod
    def _fixed_cell_values(document: Dict[str, Any]) -> Dict[str, Any]:
        values: Dict[str, Any] = {}
        fixed_blocks = document.get("fixed_blocks", []) if isinstance(document, dict) else []
        if not isinstance(fixed_blocks, list):
            return values
        for block in fixed_blocks:
            if not isinstance(block, dict):
                continue
            fields = block.get("fields", [])
            if not isinstance(fields, list):
                continue
            for item in fields:
                if not isinstance(item, dict):
                    continue
                cell = str(item.get("cell", "") or "").strip().upper()
                if cell:
                    values[cell] = item.get("value", "")
        return values

    def _business_extra_fields(self, row: Dict[str, Any], fixed_values: Dict[str, Any], document: Dict[str, Any]) -> Dict[str, Any]:
        counts = self._section_counts(document)
        pue = self._normalize_number_from_text(fixed_values.get("B6", ""))
        total_load = self._normalize_number_from_text(fixed_values.get("D6", ""))
        it_load = self._normalize_number_from_text(fixed_values.get("F6", ""))
        diesel_backup = self._normalize_number_from_text(fixed_values.get("H6", ""))
        dry_bulb = self._normalize_number_from_text(fixed_values.get("B7", ""))
        wet_bulb = self._normalize_number_from_text(fixed_values.get("D7", ""))
        municipal_pressure = self._normalize_number_from_text(fixed_values.get("B8", ""))
        water_tank_backup = self._normalize_number_from_text(fixed_values.get("D8", ""))
        ups_battery_backup = self._normalize_number_from_text(fixed_values.get("F10", ""))
        cold_high_temp = str(fixed_values.get("B9", "") or "").strip()
        cold_high_humidity = str(fixed_values.get("D9", "") or "").strip()
        cold_low_temp = str(fixed_values.get("F9", "") or "").strip()
        cold_low_humidity = str(fixed_values.get("H9", "") or "").strip()
        transformer_load = str(fixed_values.get("B10", "") or "").strip()
        ups_load = str(fixed_values.get("D10", "") or "").strip()
        return {
            "PUE（实时）": pue,
            "室外干球温度（℃）": dry_bulb,
            "市政（自备井)供水压力（Bar）": municipal_pressure,
            "冷通道最高温度（℃）/编号": cold_high_temp,
            "变压器最高负载率/编号": transformer_load,
            "总负荷（kW）": total_load,
            "室外湿球温度（℃）": wet_bulb,
            "蓄水池后备时间（H）": water_tank_backup,
            "冷通道最高湿度（%）/编号": cold_high_humidity,
            "UPS最高负载率/编号": ups_load,
            "IT总负荷（kW）": it_load,
            "冷源模式": str(fixed_values.get("F7", "") or "").strip(),
            "蓄冷罐后备时间（min）": str(fixed_values.get("F8", "") or "").strip(),
            "冷通道最低温度（℃）/编号": cold_low_temp,
            "UPS蓄电池最短后备时间（min）": ups_battery_backup,
            "柴油后备时间（H）": diesel_backup,
            "冷水系统供水温度（℃）": str(fixed_values.get("H7", "") or "").strip(),
            "冷通道最低湿度（%）/编号": cold_low_humidity,
            "当班告警总数（个）": self._normalize_number_from_text(fixed_values.get("B15", "")),
            "未恢复告警数（个）": self._normalize_number_from_text(fixed_values.get("D15", "")),
            "未恢复原因": str(fixed_values.get("F15", "") or "").strip(),
            "事件数量": counts["event_count"],
            "变更数量": counts["change_count"],
            "演练数量": counts["exercise_count"],
            "维护数量": counts["maintenance_count"],
            "施工数量": counts["construction_count"],
            "培训数量": counts["training_count"],
            "事件数量-自动统计": str(counts["event_count"]),
            "值班人员-自动统计": str(row.get("duty_staff", "") or "").strip(),
        }

    def build_deferred_state(self, *, duty_shift: str) -> Dict[str, Any]:
        cfg = self._normalize_cfg()
        shift_key = str(duty_shift or "").strip().lower()
        if not cfg.get("enabled", True):
            return {
                "status": "skipped",
                "reason": "disabled",
                "record_id": "",
                "uploaded_revision": 0,
                "uploaded_at": "",
                "updated_at": "",
                "error": "",
            }
        if shift_key not in {"day", "night"}:
            return {
                "status": "skipped",
                "reason": "missing_duty_context",
                "record_id": "",
                "uploaded_revision": 0,
                "uploaded_at": "",
                "updated_at": "",
                "error": "",
            }
        return {
            "status": "pending_review",
            "reason": "await_all_confirmed",
            "record_id": "",
            "uploaded_revision": 0,
            "uploaded_at": "",
            "updated_at": "",
            "error": "",
        }

    def list_existing_records(self, emit_log: Callable[[str], None] = print) -> List[Dict[str, Any]]:
        cfg = self._normalize_cfg()
        target = cfg.get("target", {})
        table_id = str(target.get("table_id", "") or "").strip()
        client = self._new_client(cfg)
        records = client.list_records(
            table_id=table_id,
            page_size=int(target.get("page_size", 500) or 500),
            max_records=int(target.get("max_records", 5000) or 5000),
        )
        self._emit(emit_log, f"旧记录读取完成: table_id={table_id}, total={len(records)}")
        return records

    def _matching_existing_record_ids(
        self,
        *,
        existing_records: List[Dict[str, Any]],
        building: str,
        duty_date: str,
        duty_shift: str,
        cfg: Dict[str, Any],
    ) -> List[str]:
        fields = cfg.get("fields", {})
        target_building = str(building or "").strip()
        target_shift = self._shift_text(duty_shift, cfg)
        target_date_ms = self._midnight_timestamp_ms(duty_date)
        matched: List[str] = []
        for item in existing_records if isinstance(existing_records, list) else []:
            if not isinstance(item, dict):
                continue
            record_id = str(item.get("record_id", "") or "").strip()
            payload_fields = item.get("fields", {})
            if not record_id or not isinstance(payload_fields, dict):
                continue
            building_text = self._normalize_select_text(payload_fields.get(fields.get("building", "楼栋")))
            shift_text = self._normalize_select_text(payload_fields.get(fields.get("shift", "班次")))
            date_ms = self._normalize_date_to_midnight_ms(payload_fields.get(fields.get("date", "日期")))
            if building_text == target_building and shift_text == target_shift and date_ms == target_date_ms:
                matched.append(record_id)
        return matched

    def _build_row(self, session: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        building = str(session.get("building", "") or "").strip()
        duty_date = str(session.get("duty_date", "") or "").strip()
        duty_shift = str(session.get("duty_shift", "") or "").strip().lower()
        if not building or not duty_date or duty_shift not in {"day", "night"}:
            raise ValueError("invalid_duty_context")
        document, synced_session = self._review_document_state_service.load_document(session)
        fixed_values = self._fixed_cell_values(document)
        revision = int(synced_session.get("revision", session.get("revision", 0)) or session.get("revision", 0) or 0)
        row = {
            "session_id": str(session.get("session_id", "") or "").strip(),
            "building": building,
            "duty_date": duty_date,
            "duty_shift": duty_shift,
            "revision": revision,
            "shift_text": self._shift_text(duty_shift, cfg),
            "duty_staff": str(fixed_values.get("C3", "") or "").strip(),
            "handover_staff": str(fixed_values.get("G3", "") or "").strip(),
            "planned_cabinets": self._normalize_number(fixed_values.get("B13", "")),
            "powered_cabinets": self._normalize_number(fixed_values.get("D13", "")),
            "shift_power_on_cabinets": self._normalize_number(fixed_values.get("F13", "")),
            "shift_power_off_cabinets": self._normalize_number(fixed_values.get("H13", "")),
        }
        row["extra_fields"] = self._business_extra_fields(row, fixed_values, document)
        return row

    def _create_fields(
        self,
        row: Dict[str, Any],
        cfg: Dict[str, Any],
        writable_field_names: Dict[str, Dict[str, Any]] | Set[str] | None = None,
    ) -> Dict[str, Any]:
        fields = cfg.get("fields", {})
        payload = {
            fields["building"]: row["building"],
            fields["date"]: self._midnight_timestamp_ms(row["duty_date"]),
            fields["shift"]: row["shift_text"],
            fields["duty_staff"]: row["duty_staff"],
            fields["handover_staff"]: row["handover_staff"],
            fields["planned_cabinets"]: row["planned_cabinets"],
            fields["powered_cabinets"]: row["powered_cabinets"],
            fields["shift_power_on_cabinets"]: row["shift_power_on_cabinets"],
            fields["shift_power_off_cabinets"]: row["shift_power_off_cabinets"],
        }
        payload.update(row.get("extra_fields", {}) if isinstance(row.get("extra_fields", {}), dict) else {})
        return self._safe_payload_fields(payload, writable_field_names)

    def _update_fields(
        self,
        row: Dict[str, Any],
        cfg: Dict[str, Any],
        writable_field_names: Dict[str, Dict[str, Any]] | Set[str] | None = None,
    ) -> Dict[str, Any]:
        fields = cfg.get("fields", {})
        payload = {
            fields["duty_staff"]: row["duty_staff"],
            fields["handover_staff"]: row["handover_staff"],
            fields["planned_cabinets"]: row["planned_cabinets"],
            fields["powered_cabinets"]: row["powered_cabinets"],
            fields["shift_power_on_cabinets"]: row["shift_power_on_cabinets"],
            fields["shift_power_off_cabinets"]: row["shift_power_off_cabinets"],
        }
        payload.update(row.get("extra_fields", {}) if isinstance(row.get("extra_fields", {}), dict) else {})
        return self._safe_payload_fields(payload, writable_field_names)

    def _writable_field_names(self, client: FeishuBitableClient, *, table_id: str, emit_log: Callable[[str], None]) -> Dict[str, Dict[str, Any]] | None:
        read_only_types = {20, 1001, 1002, 1005}
        try:
            fields_meta = client.list_fields(table_id=table_id, page_size=500)
        except Exception as exc:  # noqa: BLE001
            self._emit(emit_log, f"字段结构读取失败，将按固定字段名写入: error={exc}")
            return None
        writable: Dict[str, Dict[str, Any]] = {}
        skipped: List[str] = []
        for item in fields_meta if isinstance(fields_meta, list) else []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("field_name") or item.get("name") or "").strip()
            if not name:
                continue
            try:
                field_type = int(item.get("type", 0) or 0)
            except Exception:  # noqa: BLE001
                field_type = 0
            if field_type in read_only_types or self._should_skip_payload_field(name):
                skipped.append(name)
                continue
            writable[name] = item
        if skipped:
            self._emit(emit_log, f"只读字段已跳过: {','.join(skipped)}")
        return writable

    @staticmethod
    def _extract_created_record_ids(responses: List[Dict[str, Any]]) -> List[str]:
        record_ids: List[str] = []
        for response in responses if isinstance(responses, list) else []:
            if not isinstance(response, dict):
                continue
            payload = response.get("data", {}) if isinstance(response.get("data", {}), dict) else {}
            records = payload.get("records", [])
            if not isinstance(records, list):
                records = payload.get("items", []) if isinstance(payload.get("items", []), list) else []
            for item in records if isinstance(records, list) else []:
                if not isinstance(item, dict):
                    continue
                record_id = str(item.get("record_id", "") or "").strip()
                if record_id:
                    record_ids.append(record_id)
        return record_ids

    def export_sessions(
        self,
        *,
        sessions: List[Dict[str, Any]],
        existing_records: List[Dict[str, Any]] | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        cfg = self._normalize_cfg()
        now_text = self._now_text()
        details: Dict[str, Dict[str, Any]] = {}
        created_buildings: List[str] = []
        updated_buildings: List[str] = []
        skipped_buildings: List[Dict[str, str]] = []
        failed_buildings: List[Dict[str, str]] = []

        normalized_sessions = [item for item in sessions if isinstance(item, dict)]
        if not normalized_sessions:
            return {
                "status": "skipped",
                "reason": "missing_sessions",
                "created_buildings": [],
                "updated_buildings": [],
                "skipped_buildings": [],
                "failed_buildings": [],
                "details": {},
            }
        if not cfg.get("enabled", True):
            for session in normalized_sessions:
                building = str(session.get("building", "") or "").strip() or "-"
                details[building] = {
                    "status": "skipped",
                    "reason": "disabled",
                    "record_id": "",
                    "uploaded_revision": int(session.get("revision", 0) or 0),
                    "updated_at": now_text,
                    "error": "",
                }
                skipped_buildings.append({"building": building, "reason": "disabled"})
            return {
                "status": "skipped",
                "reason": "disabled",
                "created_buildings": [],
                "updated_buildings": [],
                "skipped_buildings": skipped_buildings,
                "failed_buildings": [],
                "details": details,
            }

        target = cfg.get("target", {})
        table_id = str(target.get("table_id", "") or "").strip()
        self._emit(emit_log, f"开始同步: sessions={len(normalized_sessions)}, table_id={table_id}")

        rows: List[Dict[str, Any]] = []
        for session in normalized_sessions:
            building = str(session.get("building", "") or "").strip() or "-"
            try:
                rows.append(self._build_row(session, cfg))
            except Exception as exc:  # noqa: BLE001
                error_text = str(exc or "").strip() or "missing_document"
                details[building] = {
                    "status": "failed",
                    "reason": "missing_document" if error_text != "invalid_duty_context" else "invalid_duty_context",
                    "record_id": "",
                    "uploaded_revision": 0,
                    "updated_at": now_text,
                    "error": error_text,
                }
                failed_buildings.append({"building": building, "error": error_text})

        if not rows:
            return {
                "status": "failed" if failed_buildings else "skipped",
                "reason": "missing_document" if failed_buildings else "missing_sessions",
                "created_buildings": [],
                "updated_buildings": [],
                "skipped_buildings": skipped_buildings,
                "failed_buildings": failed_buildings,
                "details": details,
            }

        try:
            cached_records = list(existing_records) if isinstance(existing_records, list) else self.list_existing_records(
                emit_log=emit_log
            )
            client = self._new_client(cfg)
            writable_field_names = self._writable_field_names(client, table_id=table_id, emit_log=emit_log)
            update_records: List[Dict[str, Any]] = []
            create_rows: List[Dict[str, Any]] = []
            for row in rows:
                matched_ids = self._matching_existing_record_ids(
                    existing_records=cached_records,
                    building=row["building"],
                    duty_date=row["duty_date"],
                    duty_shift=row["duty_shift"],
                    cfg=cfg,
                )
                if matched_ids:
                    update_payload = self._update_fields(row, cfg, writable_field_names)
                    for record_id in matched_ids:
                        update_records.append({"record_id": record_id, "fields": update_payload})
                    row["record_id"] = matched_ids[0]
                    row["action"] = "updated"
                    row["matched_count"] = len(matched_ids)
                else:
                    create_rows.append(row)
                    row["action"] = "created"

            if update_records:
                client.batch_update_records(
                    table_id=table_id,
                    records=update_records,
                    batch_size=int(target.get("update_batch_size", 200) or 200),
                )
            created_record_ids: List[str] = []
            if create_rows:
                responses = client.batch_create_records(
                    table_id=table_id,
                    fields_list=[self._create_fields(row, cfg, writable_field_names) for row in create_rows],
                    batch_size=int(target.get("create_batch_size", 200) or 200),
                )
                created_record_ids = self._extract_created_record_ids(responses)
            for index, row in enumerate(create_rows):
                row["record_id"] = created_record_ids[index] if index < len(created_record_ids) else ""

            for row in rows:
                building = str(row.get("building", "") or "").strip() or "-"
                action = str(row.get("action", "") or "").strip() or "updated"
                record_id = str(row.get("record_id", "") or "").strip()
                details[building] = {
                    "status": "ok",
                    "reason": action,
                    "record_id": record_id,
                    "uploaded_revision": int(row.get("revision", 0) or 0),
                    "uploaded_at": now_text,
                    "updated_at": now_text,
                    "error": "",
                }
                if action == "created":
                    created_buildings.append(building)
                else:
                    updated_buildings.append(building)
                    if int(row.get("matched_count", 1) or 1) > 1:
                        self._emit(
                            emit_log,
                            f"检测到重复旧记录并已全部更新: building={building}, count={row.get('matched_count')}",
                        )

            self._emit(
                emit_log,
                f"同步完成: created={len(created_buildings)}, updated={len(updated_buildings)}, failed={len(failed_buildings)}",
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc or "").strip() or "record_sync_failed"
            self._emit(emit_log, f"同步失败: error={error_text}")
            for row in rows:
                building = str(row.get("building", "") or "").strip() or "-"
                if building in details and details[building].get("status") == "failed":
                    continue
                details[building] = {
                    "status": "failed",
                    "reason": "record_sync_failed",
                    "record_id": str(row.get("record_id", "") or "").strip(),
                    "uploaded_revision": 0,
                    "uploaded_at": "",
                    "updated_at": now_text,
                    "error": error_text,
                }
                failed_buildings.append({"building": building, "error": error_text})

        if failed_buildings and (created_buildings or updated_buildings):
            status = "partial_failed"
        elif failed_buildings:
            status = "failed"
        elif created_buildings or updated_buildings:
            status = "ok"
        else:
            status = "skipped"
        return {
            "status": status,
            "reason": "" if status == "ok" else status,
            "created_buildings": created_buildings,
            "updated_buildings": updated_buildings,
            "skipped_buildings": skipped_buildings,
            "failed_buildings": failed_buildings,
            "details": details,
        }
