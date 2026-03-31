from __future__ import annotations

import ast
import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient


class DailyReportBitableExportError(RuntimeError):
    def __init__(self, *, error_code: str, user_message: str, error_detail: str) -> None:
        self.error_code = str(error_code or "").strip() or "daily_report_export_failed"
        self.user_message = str(user_message or "").strip() or "日报多维写入失败。"
        self.error_detail = str(error_detail or "").strip()
        super().__init__(self.error_detail or self.user_message)


class HandoverDailyReportBitableExportService:
    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "target": {
                "app_token": "MliKbC3fXa8PXrsndKscmxjdn1g",
                "table_id": "tblxfd0HA9kDZQ3w",
                "replace_existing": True,
                "page_size": 500,
                "max_records": 5000,
                "delete_batch_size": 200,
            },
            "fields": {
                "year": "年度",
                "date": "日期",
                "shift": "班次",
                "report_link": "交接班日报",
                "screenshots": "日报截图",
            },
            "summary_page_url": "https://vnet.feishu.cn/app/LTjUbmZsTaTFIVsuQSLcUi4Onf4?pageId=pgeZUMIpMDuIIfLA",
            "external_page_url": "https://vnet.feishu.cn/app/LTjUbmZsTaTFIVsuQSLcUi4Onf4?pageId=pgecZCUXaEtvP9Yl",
        }

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(base or {})
        for key, value in (override or {}).items():
            if isinstance(value, dict) and isinstance(out.get(key), dict):
                out[key] = HandoverDailyReportBitableExportService._deep_merge(out[key], value)
            else:
                out[key] = copy.deepcopy(value)
        return out

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("daily_report_bitable_export", {})
        if not isinstance(raw, dict):
            raw = {}
        cfg = self._deep_merge(self._defaults(), raw)
        target = cfg.get("target", {}) if isinstance(cfg.get("target", {}), dict) else {}
        fields = cfg.get("fields", {}) if isinstance(cfg.get("fields", {}), dict) else {}
        target["app_token"] = str(target.get("app_token", "") or "").strip()
        target["table_id"] = str(target.get("table_id", "") or "").strip()
        target["replace_existing"] = bool(target.get("replace_existing", True))
        target["page_size"] = max(1, int(target.get("page_size", 500) or 500))
        target["max_records"] = max(1, int(target.get("max_records", 5000) or 5000))
        target["delete_batch_size"] = max(1, int(target.get("delete_batch_size", 200) or 200))
        cfg["target"] = target
        for key, default in (
            ("year", "年度"),
            ("date", "日期"),
            ("shift", "班次"),
            ("report_link", "交接班日报"),
            ("screenshots", "日报截图"),
        ):
            fields[key] = str(fields.get(key, default) or default).strip() or default
        cfg["fields"] = fields
        cfg["enabled"] = bool(cfg.get("enabled", True))
        return cfg

    def _new_client(self, cfg: Dict[str, Any]) -> FeishuBitableClient:
        global_feishu = self.handover_cfg.get("_global_feishu", {})
        if not isinstance(global_feishu, dict):
            global_feishu = {}
        target = cfg.get("target", {})
        app_id = str(global_feishu.get("app_id", "") or "").strip()
        app_secret = str(global_feishu.get("app_secret", "") or "").strip()
        app_token = str(target.get("app_token", "") or "").strip()
        table_id = str(target.get("table_id", "") or "").strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")
        if not app_token or not table_id:
            raise ValueError("日报多维表配置缺失: app_token/table_id")
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
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
        )

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
                text = HandoverDailyReportBitableExportService._normalize_select_text(item)
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
        text = str(value or "").strip().replace("/", "-")
        try:
            dt = datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return None
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _midnight_timestamp_ms(duty_date: str) -> int:
        dt = datetime.strptime(str(duty_date or "").strip(), "%Y-%m-%d")
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _shift_text(duty_shift: str) -> str:
        return "白班" if str(duty_shift or "").strip().lower() == "day" else "夜班"

    @staticmethod
    def _year_text(duty_date: str) -> str:
        return f"{str(duty_date or '').strip()[:4]}年度"

    @staticmethod
    def _lookup_field_ui_type(fields_meta: List[Dict[str, Any]], field_name: str) -> str:
        target_name = str(field_name or "").strip()
        if not target_name:
            return ""
        for item in fields_meta if isinstance(fields_meta, list) else []:
            if not isinstance(item, dict):
                continue
            if str(item.get("field_name", "") or "").strip() != target_name:
                continue
            ui_type = item.get("ui_type")
            if ui_type is None:
                ui_type = item.get("type")
            return str(ui_type or "").strip()
        return ""

    def _build_report_link_payload(
        self,
        *,
        spreadsheet_url: str,
        fields_meta: List[Dict[str, Any]],
        cfg: Dict[str, Any],
    ) -> Any:
        url = str(spreadsheet_url or "").strip()
        report_field_name = str(cfg.get("fields", {}).get("report_link", "交接班日报") or "交接班日报").strip()
        ui_type = self._lookup_field_ui_type(fields_meta, report_field_name).lower()
        if ui_type in {"15", "url"}:
            return {"text": url, "link": url}
        return url

    @staticmethod
    def _build_report_link_object_payload(spreadsheet_url: str) -> Dict[str, str]:
        url = str(spreadsheet_url or "").strip()
        return {"text": url, "link": url}

    @staticmethod
    def _extract_feishu_error_payload(error_text: str) -> Dict[str, Any]:
        text = str(error_text or "").strip()
        if not text:
            return {}
        start = text.find("{")
        if start < 0:
            return {}
        try:
            payload = ast.literal_eval(text[start:])
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _is_url_field_conv_fail(self, exc: Exception) -> bool:
        text = str(exc or "").strip()
        payload = self._extract_feishu_error_payload(text)
        if str(payload.get("msg", "")).strip() == "URLFieldConvFail":
            return True
        if int(payload.get("code", 0) or 0) == 1254068:
            return True
        return "URLFieldConvFail" in text

    @staticmethod
    def _build_url_field_error(detail: str) -> DailyReportBitableExportError:
        return DailyReportBitableExportError(
            error_code="daily_report_url_field_invalid",
            user_message="日报链接字段写入失败，请检查飞书多维表“交接班日报”字段类型。",
            error_detail=str(detail or "").strip(),
        )

    def _match_existing_record_ids(
        self,
        *,
        existing_records: List[Dict[str, Any]],
        duty_date: str,
        duty_shift: str,
        cfg: Dict[str, Any],
    ) -> List[str]:
        fields = cfg.get("fields", {})
        target_year = self._year_text(duty_date)
        target_date_ms = self._midnight_timestamp_ms(duty_date)
        target_shift = self._shift_text(duty_shift)
        matched: List[str] = []
        for item in existing_records:
            if not isinstance(item, dict):
                continue
            record_id = str(item.get("record_id", "") or "").strip()
            payload_fields = item.get("fields", {})
            if not record_id or not isinstance(payload_fields, dict):
                continue
            year_text = self._normalize_select_text(payload_fields.get(fields.get("year", "年度")))
            shift_text = self._normalize_select_text(payload_fields.get(fields.get("shift", "班次")))
            date_ms = self._normalize_date_to_midnight_ms(payload_fields.get(fields.get("date", "日期")))
            if year_text == target_year and shift_text == target_shift and date_ms == target_date_ms:
                matched.append(record_id)
        return matched

    def export_record(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        spreadsheet_url: str,
        summary_screenshot_path: str,
        external_screenshot_path: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        cfg = self._normalize_cfg()
        if not cfg.get("enabled", True):
            return {"status": "skipped", "record_id": "", "record_url": "", "error": "disabled"}

        summary_path = Path(str(summary_screenshot_path or "").strip())
        external_path = Path(str(external_screenshot_path or "").strip())
        if not summary_path.exists() or not external_path.exists():
            missing = "summary_sheet.png" if not summary_path.exists() else "external_page.png"
            raise FileNotFoundError(missing)

        client = self._new_client(cfg)
        target = cfg.get("target", {})
        table_id = str(target.get("table_id", "") or "").strip()
        try:
            fields_meta = client.list_fields(table_id=table_id)
        except Exception as exc:  # noqa: BLE001
            fields_meta = []
            emit_log(f"[交接班][日报多维] 字段元数据读取失败，按纯文本链接回退: {exc}")
        existing_records = client.list_records(
            table_id=table_id,
            page_size=int(target.get("page_size", 500) or 500),
            max_records=int(target.get("max_records", 5000) or 5000),
        )
        matched_ids = self._match_existing_record_ids(
            existing_records=existing_records,
            duty_date=duty_date,
            duty_shift=duty_shift,
            cfg=cfg,
        )
        if matched_ids and bool(target.get("replace_existing", True)):
            client.batch_delete_records(
                table_id=table_id,
                record_ids=matched_ids,
                batch_size=int(target.get("delete_batch_size", 200) or 200),
            )
            emit_log(f"[交接班][日报多维] 删除旧记录 count={len(matched_ids)}, batch={duty_date}|{duty_shift}")

        summary_token = client.upload_attachment_bytes(
            file_name=summary_path.name,
            content=summary_path.read_bytes(),
            mime_type="image/png",
        )
        external_token = client.upload_attachment_bytes(
            file_name=external_path.name,
            content=external_path.read_bytes(),
            mime_type="image/png",
        )
        fields = cfg.get("fields", {})
        report_field_name = str(fields.get("report_link", "交接班日报") or "交接班日报").strip()
        report_link_ui_type = self._lookup_field_ui_type(fields_meta, report_field_name).lower()
        payload_fields = {
            fields["year"]: self._year_text(duty_date),
            fields["date"]: self._midnight_timestamp_ms(duty_date),
            fields["shift"]: self._shift_text(duty_shift),
            fields["report_link"]: self._build_report_link_payload(
                spreadsheet_url=spreadsheet_url,
                fields_meta=fields_meta,
                cfg=cfg,
            ),
            fields["screenshots"]: [
                {"file_token": summary_token},
                {"file_token": external_token},
            ],
        }
        try:
            responses = client.batch_create_records(table_id=table_id, fields_list=[payload_fields], batch_size=1)
        except Exception as exc:  # noqa: BLE001
            if not self._is_url_field_conv_fail(exc):
                raise
            emit_log(
                "[交接班][日报多维] URL 字段写入回退重试 "
                f"field={report_field_name}, ui_type={report_link_ui_type or '-'}, batch={duty_date}|{duty_shift}"
            )
            payload_fields[fields["report_link"]] = self._build_report_link_object_payload(spreadsheet_url)
            try:
                responses = client.batch_create_records(table_id=table_id, fields_list=[payload_fields], batch_size=1)
            except Exception as retry_exc:  # noqa: BLE001
                raise self._build_url_field_error(str(retry_exc)) from retry_exc
        record_id = ""
        if responses and isinstance(responses[0], dict):
            records = responses[0].get("data", {}).get("records", []) if isinstance(responses[0].get("data", {}), dict) else []
            if isinstance(records, list) and records and isinstance(records[0], dict):
                record_id = str(records[0].get("record_id", "") or "").strip()
        emit_log(f"[交接班][日报多维] 写入成功 batch={duty_date}|{duty_shift}, record_id={record_id or '-'}")
        return {
            "status": "success",
            "record_id": record_id,
            "record_url": "",
            "error": "",
            "error_code": "",
            "error_detail": "",
        }
