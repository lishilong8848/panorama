from __future__ import annotations

import copy
import mimetypes
import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient


DEFAULT_BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"]
DEFAULT_TARGET = {
    "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
    "table_id": "tblNyGBGSCnWhWyL",
    "page_size": 500,
    "max_records": 5000,
    "delete_batch_size": 200,
    "create_batch_size": 200,
    "fields": {
        "building": "楼栋",
        "month": "月份",
        "attachment": "附件",
    },
}
DEFAULT_CONFIG = {
    "enabled": True,
    "buildings": DEFAULT_BUILDINGS,
    "month_value_format": "{period}",
    "target": DEFAULT_TARGET,
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


def _field_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value).strip()
    if isinstance(value, list):
        parts = [_field_text(item) for item in value]
        return ",".join([part for part in parts if part])
    if isinstance(value, dict):
        for key in ("text", "name", "value", "display_value", "link", "url"):
            if key in value:
                text = _field_text(value.get(key))
                if text:
                    return text
        if "file_name" in value:
            return _field_text(value.get("file_name"))
    return str(value).strip()


class AlarmRuleExportUploadService:
    def __init__(
        self,
        runtime_config: Dict[str, Any],
        *,
        internal_client: InternalBridgeHttpClient | None = None,
        bitable_client: FeishuBitableClient | None = None,
    ) -> None:
        self.runtime_config = runtime_config if isinstance(runtime_config, dict) else {}
        self._internal_client = internal_client
        self._bitable_client = bitable_client

    @staticmethod
    def normalize_period(period: Any | None = None) -> str:
        text = str(period or "").strip()
        if not text:
            return datetime.now().strftime("%Y-%m")
        if re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", text):
            return text
        if re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", text):
            return f"{text[:4]}-{text[4:]}"
        raise ValueError("月份必须为 YYYY-MM")

    def _config(self) -> Dict[str, Any]:
        raw = _dict(self.runtime_config.get("alarm_rule_export_upload"))
        return _deep_merge(raw, DEFAULT_CONFIG)

    def _target(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        return _deep_merge(_dict(cfg.get("target")), DEFAULT_TARGET)

    def _target_fields(self, target: Dict[str, Any]) -> Tuple[str, str, str]:
        fields = _dict(target.get("fields"))
        building_field = str(fields.get("building", "楼栋") or "").strip()
        month_field = str(fields.get("month", "月份") or "").strip()
        attachment_field = str(fields.get("attachment", "附件") or "").strip()
        if not building_field or not month_field or not attachment_field:
            raise ValueError("告警规则导出上传目标字段配置不完整")
        return building_field, month_field, attachment_field

    def _buildings(self, cfg: Dict[str, Any]) -> List[str]:
        output: List[str] = []
        for item in _list(cfg.get("buildings")):
            building = str(item or "").strip()
            if building and building not in output:
                output.append(building)
        return output or list(DEFAULT_BUILDINGS)

    def _month_value(self, cfg: Dict[str, Any], period: str) -> str:
        fmt = str(cfg.get("month_value_format", "{period}") or "{period}")
        year, month = period.split("-", 1)
        try:
            return fmt.format(period=period, year=year, month=month, month_int=int(month))
        except Exception:
            return period

    def _make_internal_client(self) -> InternalBridgeHttpClient:
        if self._internal_client is not None:
            return self._internal_client
        client = InternalBridgeHttpClient.from_runtime_config(self.runtime_config)
        if client is None:
            raise RuntimeError("内网端 HTTP 桥接 base_url 未配置，无法读取告警规则导出文件")
        return client

    def _make_bitable_client(self, target: Dict[str, Any]) -> FeishuBitableClient:
        if self._bitable_client is not None:
            return self._bitable_client
        feishu = _dict(self.runtime_config.get("feishu"))
        app_id = str(feishu.get("app_id", "") or "").strip()
        app_secret = str(feishu.get("app_secret", "") or "").strip()
        app_token = str(target.get("app_token", "") or "").strip()
        table_id = str(target.get("table_id", "") or "").strip()
        if not app_token or not table_id:
            raise ValueError("告警规则导出上传目标 app_token/table_id 未配置")
        return FeishuBitableClient(
            app_id=app_id,
            app_secret=app_secret,
            app_token=app_token,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(feishu.get("timeout", 30) or 30),
            request_retry_count=int(feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(feishu.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=lambda **_: 0,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
        )

    @staticmethod
    def _entry_sort_key(item: Dict[str, Any]) -> str:
        return (
            str(item.get("downloaded_at", "") or "").strip()
            or str(item.get("file_name", "") or "").strip()
            or str(item.get("downloaded_path", "") or "").strip()
        )

    def _select_files(
        self,
        listing: Dict[str, Any],
        *,
        period: str,
        buildings: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        files = listing.get("files", []) if isinstance(listing, dict) else []
        selected: Dict[str, Dict[str, Any]] = {}
        for item in files if isinstance(files, list) else []:
            if not isinstance(item, dict):
                continue
            building = str(item.get("building", "") or "").strip()
            item_period = str(item.get("period", "") or "").strip() or period
            file_name = str(item.get("file_name", "") or "").strip()
            if building not in buildings or item_period != period or not file_name:
                continue
            if item.get("file_exists") is False:
                continue
            if "size_bytes" in item:
                try:
                    if int(item.get("size_bytes") or 0) <= 0:
                        continue
                except Exception:
                    continue
            current = selected.get(building)
            if current is None or self._entry_sort_key(item) >= self._entry_sort_key(current):
                selected[building] = {**item, "building": building, "period": item_period, "file_name": file_name}
        return selected

    def _existing_record_ids_by_building(
        self,
        client: FeishuBitableClient,
        *,
        table_id: str,
        building_field: str,
        month_field: str,
        month_value: str,
        buildings: List[str],
        page_size: int,
        max_records: int,
    ) -> Dict[str, List[str]]:
        output: Dict[str, List[str]] = {building: [] for building in buildings}
        records = client.list_records(
            table_id,
            page_size=page_size,
            max_records=max_records,
            field_names=[building_field, month_field],
        )
        for item in records:
            if not isinstance(item, dict):
                continue
            record_id = str(item.get("record_id", "") or "").strip()
            fields = _dict(item.get("fields"))
            building = _field_text(fields.get(building_field))
            month = _field_text(fields.get(month_field))
            if record_id and building in output and month == month_value:
                output[building].append(record_id)
        return output

    @staticmethod
    def _mime_type(file_name: str) -> str:
        guessed, _ = mimetypes.guess_type(file_name)
        return guessed or "application/octet-stream"

    def run(
        self,
        *,
        period: Any | None = None,
        emit_log: Callable[[str], None] | None = None,
    ) -> Dict[str, Any]:
        log = emit_log if callable(emit_log) else (lambda _msg: None)
        cfg = self._config()
        if not bool(cfg.get("enabled", True)):
            return {"status": "skipped", "reason": "disabled"}

        target = self._target(cfg)
        table_id = str(target.get("table_id", "") or "").strip()
        app_token = str(target.get("app_token", "") or "").strip()
        if not table_id or not app_token:
            raise ValueError("告警规则导出上传目标 app_token/table_id 未配置")

        period_text = self.normalize_period(period)
        month_value = self._month_value(cfg, period_text)
        buildings = self._buildings(cfg)
        building_field, month_field, attachment_field = self._target_fields(target)

        internal = self._make_internal_client()
        log(f"[告警规则导出上传] 开始读取内网端导出文件清单 period={period_text}, buildings={','.join(buildings)}")
        listing = internal.list_alarm_rule_export_files(period=period_text)
        selected = self._select_files(listing, period=period_text, buildings=buildings)
        missing = [building for building in buildings if building not in selected]
        if missing:
            raise RuntimeError(f"告警规则导出文件缺失或为空: {','.join(missing)}")

        downloads: List[Dict[str, Any]] = []
        for building in buildings:
            item = selected[building]
            file_name = str(item.get("file_name", "") or "").strip()
            content, response_name, content_type = internal.download_alarm_rule_export_file(
                period=period_text,
                building=building,
                file_name=file_name,
            )
            if not content:
                raise RuntimeError(f"{building} 告警规则导出文件为空: {file_name}")
            final_name = str(response_name or file_name).strip() or file_name
            downloads.append(
                {
                    "building": building,
                    "period": period_text,
                    "file_name": final_name,
                    "source_file_name": file_name,
                    "size_bytes": len(content),
                    "content_type": content_type or self._mime_type(final_name),
                    "content": content,
                    "downloaded_path": item.get("downloaded_path", ""),
                }
            )
        log(f"[告警规则导出上传] 内网端文件下载完成 count={len(downloads)}, period={period_text}")

        bitable = self._make_bitable_client(target)
        existing = self._existing_record_ids_by_building(
            bitable,
            table_id=table_id,
            building_field=building_field,
            month_field=month_field,
            month_value=month_value,
            buildings=buildings,
            page_size=max(1, int(target.get("page_size", 500) or 500)),
            max_records=max(0, int(target.get("max_records", 5000) or 5000)),
        )

        fields_list: List[Dict[str, Any]] = []
        uploaded_files: List[Dict[str, Any]] = []
        for item in downloads:
            file_token = bitable.upload_attachment_bytes(
                file_name=str(item.get("file_name", "") or "").strip(),
                content=item["content"],
                mime_type=str(item.get("content_type", "") or "") or self._mime_type(str(item.get("file_name", "") or "")),
            )
            uploaded_files.append(
                {
                    "building": item["building"],
                    "period": period_text,
                    "file_name": item["file_name"],
                    "source_file_name": item["source_file_name"],
                    "size_bytes": item["size_bytes"],
                    "file_token": file_token,
                    "downloaded_path": item.get("downloaded_path", ""),
                }
            )
            fields_list.append(
                {
                    building_field: item["building"],
                    month_field: month_value,
                    attachment_field: [{"file_token": file_token}],
                }
            )

        bitable.batch_create_records(
            table_id,
            fields_list,
            batch_size=max(1, int(target.get("create_batch_size", 200) or 200)),
        )

        old_record_ids: List[str] = []
        for ids in existing.values():
            old_record_ids.extend(ids)
        deleted_count = 0
        delete_warning = ""
        if old_record_ids:
            try:
                deleted_count = bitable.batch_delete_records(
                    table_id,
                    old_record_ids,
                    batch_size=max(1, int(target.get("delete_batch_size", 200) or 200)),
                )
            except Exception as exc:  # noqa: BLE001
                delete_warning = str(exc)
                log(f"[告警规则导出上传] 旧记录删除失败，已保留新记录: {delete_warning}")

        log(
            "[告警规则导出上传] 上传完成 "
            f"period={period_text}, month_field={month_value}, uploaded={len(uploaded_files)}, deleted={deleted_count}"
        )
        result = {
            "status": "success_with_delete_warning" if delete_warning else "success",
            "period": period_text,
            "month_value": month_value,
            "app_token": app_token,
            "table_id": table_id,
            "uploaded_count": len(uploaded_files),
            "deleted_count": deleted_count,
            "files": uploaded_files,
        }
        if delete_warning:
            result["delete_warning"] = delete_warning
        return result
