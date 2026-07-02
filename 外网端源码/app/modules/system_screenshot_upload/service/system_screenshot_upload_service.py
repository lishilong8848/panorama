from __future__ import annotations

import copy
import mimetypes
import re
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient


DEFAULT_TARGETS: List[Dict[str, str]] = [
    {
        "key": "power_distribution",
        "label": "供配电系统图",
        "table_id": "tblGIz3IElh6T8vm",
    },
    {
        "key": "hvac",
        "label": "暖通系统图",
        "table_id": "tblnnbx0tw3sFJoH",
    },
    {
        "key": "fuel",
        "label": "燃油系统图",
        "table_id": "tblvKzr7a7SuzYMZ",
    },
    {
        "key": "generator",
        "label": "柴发系统图",
        "table_id": "tblPzH8miDHPnjAl",
    },
    {
        "key": "weak_current",
        "label": "弱电系统图",
        "table_id": "tbly9D29sw0eT6FQ",
    },
]
DEFAULT_CONFIG = {
    "enabled": True,
    "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
    "trigger_internal_capture": True,
    "wait_capture_timeout_sec": 180,
    "wait_capture_poll_sec": 5,
    "date_value_format": "{date}",
    "page_size": 500,
    "max_records": 1000,
    "delete_batch_size": 200,
    "create_batch_size": 200,
    "fields": {
        "date": "日期",
        "attachment": "附件",
    },
    "targets": DEFAULT_TARGETS,
}


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _deep_merge(raw: Any, defaults: Any) -> Any:
    if isinstance(defaults, dict):
        src = raw if isinstance(raw, dict) else {}
        out: Dict[str, Any] = {}
        for key, default_value in defaults.items():
            out[key] = _deep_merge(src.get(key), default_value)
        for key, value in src.items():
            if key not in out:
                out[key] = copy.deepcopy(value)
        return out
    if isinstance(defaults, list):
        return copy.deepcopy(raw) if isinstance(raw, list) else copy.deepcopy(defaults)
    return copy.deepcopy(defaults if raw is None else raw)


def _capture_date(value: Any | None = None) -> str:
    text = str(value or "").strip()
    if not text:
        return datetime.now().strftime("%Y-%m-%d")
    if re.fullmatch(r"20\d{2}-\d{2}-\d{2}", text):
        return text
    if re.fullmatch(r"20\d{6}", text):
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    raise ValueError("截图日期必须为 YYYY-MM-DD")


def _field_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value).strip()
    if isinstance(value, list):
        return ",".join(part for part in (_field_text(item) for item in value) if part)
    if isinstance(value, dict):
        for key in ("text", "name", "value", "display_value", "link", "url"):
            text = _field_text(value.get(key))
            if text:
                return text
    return str(value).strip()


def _date_to_timestamp_ms(date_text: str) -> int:
    current = datetime.strptime(date_text, "%Y-%m-%d")
    epoch = datetime(1970, 1, 1)
    return int((current - epoch).total_seconds() * 1000) - 8 * 3600 * 1000


def _timestamp_ms_to_date_text(value: Any) -> str:
    try:
        number = int(float(str(value).strip()))
    except Exception:
        return ""
    current = datetime(1970, 1, 1) + timedelta(milliseconds=number) + timedelta(hours=8)
    return current.strftime("%Y-%m-%d")


def _field_name(item: Dict[str, Any]) -> str:
    return str(item.get("field_name") or item.get("name") or "").strip()


class SystemScreenshotUploadService:
    def __init__(
        self,
        runtime_config: Dict[str, Any],
        *,
        internal_client: InternalBridgeHttpClient | None = None,
        bitable_client_factory: Callable[[str], FeishuBitableClient] | None = None,
    ) -> None:
        self.runtime_config = runtime_config if isinstance(runtime_config, dict) else {}
        self._internal_client = internal_client
        self._bitable_client_factory = bitable_client_factory

    @staticmethod
    def normalize_capture_date(value: Any | None = None) -> str:
        return _capture_date(value)

    def _config(self) -> Dict[str, Any]:
        return _deep_merge(_dict(self.runtime_config.get("system_screenshot_upload")), DEFAULT_CONFIG)

    def _targets(self, cfg: Dict[str, Any]) -> List[Dict[str, str]]:
        output: List[Dict[str, str]] = []
        seen: set[str] = set()
        for item in _list(cfg.get("targets")):
            if not isinstance(item, dict):
                continue
            key = str(item.get("key", "") or "").strip()
            label = str(item.get("label", "") or "").strip()
            table_id = str(item.get("table_id", "") or "").strip()
            if not key or not label or not table_id or key in seen:
                continue
            seen.add(key)
            output.append({"key": key, "label": label, "table_id": table_id})
        return output

    def _date_value(self, cfg: Dict[str, Any], capture_date: str) -> str:
        fmt = str(cfg.get("date_value_format", "{date}") or "{date}")
        year, month, day = capture_date.split("-", 2)
        try:
            return fmt.format(date=capture_date, year=year, month=month, day=day)
        except Exception:
            return capture_date

    def _make_internal_client(self) -> InternalBridgeHttpClient:
        if self._internal_client is not None:
            return self._internal_client
        client = InternalBridgeHttpClient.from_runtime_config(self.runtime_config)
        if client is None:
            raise RuntimeError("内网端 HTTP 桥接 base_url 未配置，无法读取系统截图文件")
        return client

    def _make_bitable_client(self, cfg: Dict[str, Any], table_id: str) -> FeishuBitableClient:
        if self._bitable_client_factory is not None:
            return self._bitable_client_factory(table_id)
        feishu = _dict(self.runtime_config.get("feishu"))
        app_id = str(feishu.get("app_id", "") or "").strip()
        app_secret = str(feishu.get("app_secret", "") or "").strip()
        app_token = str(cfg.get("app_token", "") or "").strip()
        if not app_token or not table_id:
            raise ValueError("系统截图上传目标 app_token/table_id 未配置")
        return FeishuBitableClient(
            app_id=app_id,
            app_secret=app_secret,
            app_token=app_token,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(feishu.get("timeout", 30) or 30),
            request_retry_count=int(feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(feishu.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=lambda date_text, **_: _date_to_timestamp_ms(str(date_text)),
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
        )

    @staticmethod
    def _field_map(client: FeishuBitableClient, table_id: str) -> Dict[str, Dict[str, Any]]:
        fields = client.list_fields(table_id)
        return {_field_name(item): item for item in fields if isinstance(item, dict) and _field_name(item)}

    @staticmethod
    def _is_date_field(field: Dict[str, Any] | None) -> bool:
        if not isinstance(field, dict):
            return False
        return str(field.get("type", "") or "").strip() in {"5", "1005"}

    def _date_field_value(self, field: Dict[str, Any] | None, date_value: str) -> Any:
        if self._is_date_field(field):
            source_date = date_value if re.fullmatch(r"20\d{2}-\d{2}-\d{2}", date_value) else _capture_date(date_value)
            return _date_to_timestamp_ms(source_date)
        return date_value

    def _record_matches_date(self, fields: Dict[str, Any], date_field: str, expected: str) -> bool:
        value = fields.get(date_field)
        text = _field_text(value)
        if text == expected:
            return True
        if _timestamp_ms_to_date_text(text) == expected:
            return True
        return expected in text

    def _old_record_ids(
        self,
        client: FeishuBitableClient,
        *,
        table_id: str,
        date_field: str,
        date_value: str,
        page_size: int,
        max_records: int,
    ) -> List[str]:
        if not date_field:
            return []
        records = client.list_records(
            table_id,
            page_size=page_size,
            max_records=max_records,
            field_names=[date_field],
        )
        record_ids: List[str] = []
        for item in records:
            if not isinstance(item, dict):
                continue
            record_id = str(item.get("record_id", "") or "").strip()
            fields = _dict(item.get("fields"))
            if record_id and self._record_matches_date(fields, date_field, date_value):
                record_ids.append(record_id)
        return record_ids

    @staticmethod
    def _mime_type(file_name: str, fallback: str = "") -> str:
        guessed, _ = mimetypes.guess_type(file_name)
        return guessed or fallback or "image/png"

    def run(
        self,
        *,
        capture_date: Any | None = None,
        trigger_internal_capture: bool | None = None,
        emit_log: Callable[[str], None] | None = None,
    ) -> Dict[str, Any]:
        log = emit_log if callable(emit_log) else (lambda _msg: None)
        cfg = self._config()
        if not bool(cfg.get("enabled", True)):
            return {"status": "skipped", "reason": "disabled"}

        date_text = self.normalize_capture_date(capture_date)
        date_value = self._date_value(cfg, date_text)
        targets = self._targets(cfg)
        if not targets:
            raise RuntimeError("系统截图上传目标为空")

        internal = self._make_internal_client()
        should_trigger = bool(cfg.get("trigger_internal_capture", True)) if trigger_internal_capture is None else bool(trigger_internal_capture)
        if should_trigger:
            log(f"[系统截图上传] 触发内网端截图检查 date={date_text}")
            internal.run_system_screenshot_capture(capture_date=date_text, force=False)

        by_key: Dict[str, Dict[str, Any]] = {}
        missing: List[str] = []
        deadline = time.monotonic() + max(1.0, float(cfg.get("wait_capture_timeout_sec", 180) or 180))
        poll_sec = max(1.0, float(cfg.get("wait_capture_poll_sec", 5) or 5))
        while True:
            listing = internal.list_system_screenshot_files(capture_date=date_text)
            files = listing.get("files", []) if isinstance(listing, dict) else []
            by_key = {}
            for item in files if isinstance(files, list) else []:
                if not isinstance(item, dict):
                    continue
                key = str(item.get("target_key", "") or "").strip()
                if key and item.get("file_exists") is True:
                    by_key[key] = item
            missing = [target["label"] for target in targets if target["key"] not in by_key]
            if not missing or not should_trigger or time.monotonic() >= deadline:
                break
            log(f"[系统截图上传] 等待内网端截图完成: missing={','.join(missing)}")
            time.sleep(poll_sec)

        if missing:
            raise RuntimeError("系统截图文件缺失或为空: " + ",".join(missing))

        app_token = str(cfg.get("app_token", "") or "").strip()
        fields_cfg = _dict(cfg.get("fields"))
        configured_date_field = str(fields_cfg.get("date", "日期") or "").strip()
        configured_attachment_field = str(fields_cfg.get("attachment", "附件") or "").strip()
        if not configured_attachment_field:
            raise ValueError("系统截图上传附件字段配置为空")

        prepared: List[Dict[str, Any]] = []
        for target in targets:
            table_id = target["table_id"]
            item = by_key[target["key"]]
            file_name = str(item.get("file_name", "") or "").strip()
            content, response_name, content_type = internal.download_system_screenshot_file(
                capture_date=date_text,
                target_key=target["key"],
                file_name=file_name,
            )
            if not content:
                raise RuntimeError(f"{target['label']} 截图文件为空: {file_name}")
            final_name = str(response_name or file_name).strip() or f"{target['label']}.png"
            client = self._make_bitable_client(cfg, table_id)
            field_map = self._field_map(client, table_id)
            attachment_field = configured_attachment_field if configured_attachment_field in field_map else ""
            if not attachment_field:
                raise RuntimeError(f"{target['label']} 目标表缺少附件字段: {configured_attachment_field}")
            date_field = configured_date_field if configured_date_field in field_map else ""
            old_ids = self._old_record_ids(
                client,
                table_id=table_id,
                date_field=date_field,
                date_value=date_value,
                page_size=max(1, int(cfg.get("page_size", 500) or 500)),
                max_records=max(0, int(cfg.get("max_records", 1000) or 1000)),
            )
            prepared.append(
                {
                    "target": target,
                    "table_id": table_id,
                    "client": client,
                    "field_map": field_map,
                    "attachment_field": attachment_field,
                    "date_field": date_field,
                    "old_ids": old_ids,
                    "content": content,
                    "file_name": final_name,
                    "mime_type": content_type or self._mime_type(final_name),
                }
            )

        uploaded: List[Dict[str, Any]] = []
        deleted_total = 0
        for prepared_item in prepared:
            target = prepared_item["target"]
            table_id = prepared_item["table_id"]
            client = prepared_item["client"]
            field_map = prepared_item["field_map"]
            attachment_field = prepared_item["attachment_field"]
            date_field = prepared_item["date_field"]
            old_ids = prepared_item["old_ids"]
            content = prepared_item["content"]
            final_name = prepared_item["file_name"]
            file_token = client.upload_attachment_bytes(
                file_name=final_name,
                content=content,
                mime_type=prepared_item["mime_type"],
            )
            fields: Dict[str, Any] = {
                attachment_field: [{"file_token": file_token}],
            }
            if date_field:
                fields[date_field] = self._date_field_value(field_map.get(date_field), date_value)
            client.batch_create_records(
                table_id,
                [fields],
                batch_size=max(1, int(cfg.get("create_batch_size", 200) or 200)),
            )
            deleted_count = 0
            if old_ids:
                deleted_count = client.batch_delete_records(
                    table_id,
                    old_ids,
                    batch_size=max(1, int(cfg.get("delete_batch_size", 200) or 200)),
                )
                deleted_total += deleted_count
            uploaded.append(
                {
                    "key": target["key"],
                    "label": target["label"],
                    "table_id": table_id,
                    "file_name": final_name,
                    "size_bytes": len(content),
                    "file_token": file_token,
                    "deleted_count": deleted_count,
                    "date_field": date_field,
                    "attachment_field": attachment_field,
                }
            )
            log(
                f"[系统截图上传] 已上传: {target['label']} table={table_id}, "
                f"file={final_name}, deleted={deleted_count}"
            )

        return {
            "status": "success",
            "capture_date": date_text,
            "date_value": date_value,
            "app_token": app_token,
            "uploaded_count": len(uploaded),
            "deleted_count": deleted_total,
            "files": uploaded,
        }
