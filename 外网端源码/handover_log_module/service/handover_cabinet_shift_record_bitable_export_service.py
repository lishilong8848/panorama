from __future__ import annotations

import copy
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List

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
        return {
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

    def _create_fields(self, row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        fields = cfg.get("fields", {})
        return {
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

    def _update_fields(self, row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        fields = cfg.get("fields", {})
        return {
            fields["planned_cabinets"]: row["planned_cabinets"],
            fields["powered_cabinets"]: row["powered_cabinets"],
            fields["shift_power_on_cabinets"]: row["shift_power_on_cabinets"],
            fields["shift_power_off_cabinets"]: row["shift_power_off_cabinets"],
        }

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
                    update_payload = self._update_fields(row, cfg)
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
                    fields_list=[self._create_fields(row, cfg) for row in create_rows],
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
