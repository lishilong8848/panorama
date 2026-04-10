from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from handover_log_module.service.handover_source_file_cache_service import HandoverSourceFileCacheService


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def _attachment_reason_text(reason: Any) -> str:
    text = str(reason or "").strip().lower()
    mapping = {
        "disabled": "配置已禁用",
        "missing_duty_context": "缺少班次上下文",
        "night_shift_disabled": "夜班上传已禁用",
        "await_all_confirmed": "等待五个楼栋全部确认",
        "already_uploaded": "已按当前版本完成上传",
        "missing_source_file": "源数据文件不存在",
        "missing_source_file_cache": "源文件缓存不存在",
        "list_existing_failed": "读取旧记录失败",
        "upload_error": "上传失败",
    }
    return mapping.get(text, text or "-")


class SourceDataAttachmentBitableExportService:
    def __init__(self, handover_cfg: Dict[str, Any], *, log_prefix: str = "[交接班][源数据附件]") -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}
        self._source_file_cache_service = HandoverSourceFileCacheService(self.handover_cfg)
        self._log_prefix = str(log_prefix or "").strip() or "[交接班][源数据附件]"

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "source": {
                "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
                "table_id": "tblF13MQ10PslIdI",
                "page_size": 500,
                "max_records": 5000,
                "delete_batch_size": 200,
            },
            "fields": {
                "type": "类型",
                "building": "楼栋",
                "date": "日期",
                "shift": "班次",
                "attachment": "附件",
            },
            "fixed_values": {
                "type": "动环数据",
                "shift_text": {
                    "day": "白班",
                    "night": "夜班",
                },
            },
            "upload_night_shift": True,
            "replace_existing": True,
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("source_data_attachment_export", {})
        cfg = _deep_merge(self._defaults(), raw if isinstance(raw, dict) else {})

        source = cfg.get("source", {})
        fields = cfg.get("fields", {})
        fixed_values = cfg.get("fixed_values", {})
        shift_text = fixed_values.get("shift_text", {})

        if not isinstance(source, dict):
            source = {}
        if not isinstance(fields, dict):
            fields = {}
        if not isinstance(fixed_values, dict):
            fixed_values = {}
        if not isinstance(shift_text, dict):
            shift_text = {}

        source["app_token"] = str(source.get("app_token", "")).strip()
        source["table_id"] = str(source.get("table_id", "")).strip()
        source["page_size"] = max(1, int(source.get("page_size", 500) or 500))
        source["max_records"] = max(1, int(source.get("max_records", 5000) or 5000))
        source["delete_batch_size"] = max(1, int(source.get("delete_batch_size", 200) or 200))
        cfg["source"] = source

        for key, default in (
            ("type", "类型"),
            ("building", "楼栋"),
            ("date", "日期"),
            ("shift", "班次"),
            ("attachment", "附件"),
        ):
            fields[key] = str(fields.get(key, default)).strip() or default
        cfg["fields"] = fields

        fixed_values["type"] = str(fixed_values.get("type", "动环数据")).strip() or "动环数据"
        shift_text["day"] = str(shift_text.get("day", "白班")).strip() or "白班"
        shift_text["night"] = str(shift_text.get("night", "夜班")).strip() or "夜班"
        fixed_values["shift_text"] = shift_text
        cfg["fixed_values"] = fixed_values

        cfg["enabled"] = bool(cfg.get("enabled", True))
        cfg["upload_night_shift"] = bool(cfg.get("upload_night_shift", True))
        cfg["replace_existing"] = bool(cfg.get("replace_existing", True))
        return cfg

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
            raise ValueError("源数据附件上报多维配置缺失: app_token/table_id")

        return FeishuBitableClient(
            app_id=app_id,
            app_secret=app_secret,
            app_token=app_token,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=lambda **_: 0,
            canonical_metric_name_fn=lambda x: str(x or "").strip(),
            dimension_mapping={},
        )

    def _emit(self, emit_log: Callable[[str], None], message: str) -> None:
        emit_log(f"{self._log_prefix} {str(message or '').strip()}")

    @staticmethod
    def _midnight_timestamp_ms(duty_date: str) -> int:
        dt = datetime.strptime(str(duty_date or "").strip(), "%Y-%m-%d")
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _is_managed_source_file(self, data_file: str) -> bool:
        return self._source_file_cache_service.is_managed_path(str(data_file or "").strip())

    @staticmethod
    def _normalize_select_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            for key in ("name", "text", "value"):
                text = str(value.get(key, "")).strip()
                if text:
                    return text
            return ""
        if isinstance(value, list):
            for item in value:
                text = SourceDataAttachmentBitableExportService._normalize_select_text(item)
                if text:
                    return text
            return ""
        return str(value).strip()

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

        text = str(value).strip()
        if not text:
            return None
        candidate = text[:10].replace("/", "-")
        try:
            dt = datetime.strptime(candidate, "%Y-%m-%d")
        except ValueError:
            return None
        return int(dt.timestamp() * 1000)

    def build_deferred_state(self, *, duty_shift: str) -> Dict[str, Any]:
        cfg = self._normalize_cfg()
        shift_text = str(duty_shift or "").strip().lower()
        if not cfg.get("enabled", True):
            return {
                "status": "skipped",
                "reason": "disabled",
                "uploaded_count": 0,
                "error": "",
                "uploaded_at": "",
                "uploaded_revision": 0,
            }
        if shift_text == "night" and not cfg.get("upload_night_shift", True):
            return {
                "status": "skipped",
                "reason": "night_shift_disabled",
                "uploaded_count": 0,
                "error": "",
                "uploaded_at": "",
                "uploaded_revision": 0,
            }
        return {
            "status": "pending_review",
            "reason": "await_all_confirmed",
            "uploaded_count": 0,
            "error": "",
            "uploaded_at": "",
            "uploaded_revision": 0,
        }

    def list_existing_records(self, emit_log: Callable[[str], None] = print) -> List[Dict[str, Any]]:
        cfg = self._normalize_cfg()
        source = cfg.get("source", {})
        table_id = str(source.get("table_id", "")).strip()
        client = self._new_client(cfg)
        records = client.list_records(
            table_id=table_id,
            page_size=int(source.get("page_size", 500) or 500),
            max_records=int(source.get("max_records", 5000) or 5000),
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
        fixed_values = cfg.get("fixed_values", {})
        shift_text_cfg = fixed_values.get("shift_text", {})
        target_type = str(fixed_values.get("type", "动环数据")).strip()
        target_building = str(building or "").strip()
        target_shift = str(shift_text_cfg.get(str(duty_shift or "").strip().lower(), "")).strip()
        target_date_ms = self._midnight_timestamp_ms(duty_date)
        matched: List[str] = []
        for item in existing_records:
            if not isinstance(item, dict):
                continue
            record_id = str(item.get("record_id", "")).strip()
            payload_fields = item.get("fields", {})
            if not record_id or not isinstance(payload_fields, dict):
                continue
            type_text = self._normalize_select_text(payload_fields.get(fields.get("type", "类型")))
            building_text = self._normalize_select_text(payload_fields.get(fields.get("building", "楼栋")))
            shift_text = self._normalize_select_text(payload_fields.get(fields.get("shift", "班次")))
            date_ms = self._normalize_date_to_midnight_ms(payload_fields.get(fields.get("date", "日期")))
            if (
                type_text == target_type
                and building_text == target_building
                and shift_text == target_shift
                and date_ms == target_date_ms
            ):
                matched.append(record_id)
        return matched

    def run_from_source_file(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        data_file: str,
        existing_records: List[Dict[str, Any]] | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        cfg = self._normalize_cfg()
        shift_key = str(duty_shift or "").strip().lower()
        building_text = str(building or "").strip()
        duty_date_text = str(duty_date or "").strip()
        data_file_text = str(data_file or "").strip()
        if not cfg.get("enabled", True):
            return {"status": "skipped", "reason": "disabled", "uploaded_count": 0, "error": ""}
        if not duty_date_text or shift_key not in {"day", "night"}:
            return {"status": "skipped", "reason": "missing_duty_context", "uploaded_count": 0, "error": ""}
        if shift_key == "night" and not cfg.get("upload_night_shift", True):
            return {"status": "skipped", "reason": "night_shift_disabled", "uploaded_count": 0, "error": ""}
        if not data_file_text or not Path(data_file_text).exists():
            missing_reason = (
                "missing_source_file_cache" if self._is_managed_source_file(data_file_text) else "missing_source_file"
            )
            self._emit(
                emit_log,
                f"上传失败: building={building_text}, 原因={_attachment_reason_text(missing_reason)}, file={data_file_text or '-'}",
            )
            return {
                "status": "failed",
                "reason": missing_reason,
                "uploaded_count": 0,
                "error": f"源数据文件不存在: {data_file_text or '-'}",
            }

        source = cfg.get("source", {})
        fields = cfg.get("fields", {})
        fixed_values = cfg.get("fixed_values", {})
        shift_text_cfg = fixed_values.get("shift_text", {})
        table_id = str(source.get("table_id", "")).strip()
        client = self._new_client(cfg)
        shift_text = str(shift_text_cfg.get(shift_key, "白班" if shift_key == "day" else "夜班")).strip()
        self._emit(
            emit_log,
            f"开始上传: building={building_text}, batch={duty_date_text}|{shift_key}, data_file={data_file_text}",
        )

        try:
            cached_records = list(existing_records) if isinstance(existing_records, list) else self.list_existing_records(
                emit_log=emit_log
            )
            deleted_record_ids = self._matching_existing_record_ids(
                existing_records=cached_records,
                building=building_text,
                duty_date=duty_date_text,
                duty_shift=shift_key,
                cfg=cfg,
            )
            if deleted_record_ids and cfg.get("replace_existing", True):
                client.batch_delete_records(
                    table_id=table_id,
                    record_ids=deleted_record_ids,
                    batch_size=int(source.get("delete_batch_size", 200) or 200),
                )
                self._emit(
                    emit_log,
                    f"已删除旧记录: building={building_text}, duty_date={duty_date_text}, duty_shift={shift_key}, count={len(deleted_record_ids)}",
                )

            file_token = client.upload_attachment(data_file_text)
            row_fields = {
                str(fields.get("type", "类型")).strip(): str(fixed_values.get("type", "动环数据")).strip(),
                str(fields.get("building", "楼栋")).strip(): building_text,
                str(fields.get("date", "日期")).strip(): self._midnight_timestamp_ms(duty_date_text),
                str(fields.get("shift", "班次")).strip(): shift_text,
                str(fields.get("attachment", "附件")).strip(): [{"file_token": file_token}],
            }
            client.batch_create_records(table_id=table_id, fields_list=[row_fields], batch_size=1)
            uploaded_at = self._now_text()
            self._emit(
                emit_log,
                f"上传完成: building={building_text}, type={fixed_values.get('type', '动环数据')}, shift={shift_text}, uploaded=1",
            )
            return {
                "status": "ok",
                "reason": "",
                "uploaded_count": 1,
                "error": "",
                "uploaded_at": uploaded_at,
                "uploaded_revision": 0,
                "deleted_record_ids": deleted_record_ids,
                "file_token": file_token,
            }
        except Exception as exc:  # noqa: BLE001
            self._emit(emit_log, f"上传失败: building={building_text}, 错误={exc}")
            return {
                "status": "failed",
                "reason": "upload_error",
                "uploaded_count": 0,
                "error": str(exc),
                "uploaded_at": "",
                "uploaded_revision": 0,
                "deleted_record_ids": [],
                "file_token": "",
            }
