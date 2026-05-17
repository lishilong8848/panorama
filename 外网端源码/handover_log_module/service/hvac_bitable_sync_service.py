from __future__ import annotations

import copy
import hashlib
import json
import time
from typing import Any, Callable, Dict, Iterable, List

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from handover_log_module.vendor import hvac_bitable_sync as hvac


class _HvacFeishuAppClient:
    """Adapter from the vendored HVAC processor to the project's Feishu app client."""

    SEND_MESSAGE_URL = "https://open.feishu.cn/open-apis/im/v1/messages"
    CREATE_FIELD_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    UPDATE_FIELD_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields/{field_id}"

    def __init__(
        self,
        *,
        runtime_config: Dict[str, Any],
        app_token: str,
        default_table_id: str,
        page_size: int,
        batch_size: int,
        emit_log: Callable[[str], None],
    ) -> None:
        self.runtime_config = runtime_config if isinstance(runtime_config, dict) else {}
        self.app_token = str(app_token or "").strip()
        self.default_table_id = str(default_table_id or "").strip()
        self.page_size = max(1, int(page_size or 500))
        self.batch_size = max(1, min(500, int(batch_size or 200)))
        self.emit_log = emit_log
        auth = require_feishu_auth_settings(self.runtime_config)
        self._client = FeishuBitableClient(
            app_id=str(auth.get("app_id", "") or "").strip(),
            app_secret=str(auth.get("app_secret", "") or "").strip(),
            app_token=self.app_token,
            calc_table_id=self.default_table_id,
            attachment_table_id=self.default_table_id,
            timeout=int(auth.get("timeout", 30) or 30),
            request_retry_count=int(auth.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(auth.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=lambda **_: 0,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
            emit_log=emit_log,
        )
        self._field_cache: Dict[str, List[Dict[str, Any]]] = {}

    @staticmethod
    def _text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _external_field_type(item: Dict[str, Any]) -> str:
        raw_type = str(item.get("type", "") or "").strip().lower()
        ui_type = str(item.get("ui_type", "") or "").strip().lower()
        if raw_type in {"text", "select"}:
            return raw_type
        if ui_type in {"singleselect", "single_select"}:
            return "select"
        if ui_type in {"text", "url", "barcode"}:
            return "text"
        if raw_type in {"3", "single_select", "single-select"}:
            return "select"
        return "text" if raw_type in {"1", ""} else raw_type

    @classmethod
    def _normalize_field(cls, item: Dict[str, Any]) -> Dict[str, Any]:
        name = cls._text(item.get("field_name") or item.get("name"))
        field_id = cls._text(item.get("field_id") or item.get("id"))
        property_obj = item.get("property", {}) if isinstance(item.get("property", {}), dict) else {}
        options = item.get("options", None)
        if options is None:
            options = property_obj.get("options", [])
        return {
            **item,
            "name": name,
            "id": field_id,
            "type": cls._external_field_type(item),
            "options": options if isinstance(options, list) else [],
        }

    def _clear_field_cache(self, table_id: str) -> None:
        self._field_cache.pop(str(table_id or "").strip(), None)

    def field_list(self, table_id: str) -> List[Dict[str, Any]]:
        table = self._text(table_id)
        if not table:
            return []
        if table not in self._field_cache:
            fields = self._client.list_fields(table_id=table, page_size=500)
            self._field_cache[table] = [
                self._normalize_field(item)
                for item in fields
                if isinstance(item, dict) and self._text(item.get("field_name") or item.get("name"))
            ]
        return [dict(item) for item in self._field_cache.get(table, [])]

    @classmethod
    def _field_create_payload(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        name = cls._text(payload.get("name") or payload.get("field_name"))
        field_type = cls._text(payload.get("type")).lower()
        if not name:
            raise ValueError("字段名不能为空")
        if field_type == "select":
            options = []
            for item in payload.get("options", []) if isinstance(payload.get("options", []), list) else []:
                if not isinstance(item, dict):
                    continue
                option_name = cls._text(item.get("name"))
                if option_name:
                    options.append({"name": option_name})
            property_obj = payload.get("property", {}) if isinstance(payload.get("property", {}), dict) else {}
            for item in property_obj.get("options", []) if isinstance(property_obj.get("options", []), list) else []:
                if not isinstance(item, dict):
                    continue
                option_name = cls._text(item.get("name"))
                if option_name and {"name": option_name} not in options:
                    options.append({"name": option_name})
            return {
                "field_name": name,
                "type": "select",
                "property": {"options": options},
            }
        return {
            "field_name": name,
            "type": "text",
        }

    def field_create(self, table_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        table = self._text(table_id)
        body = self._field_create_payload(payload if isinstance(payload, dict) else {})
        url = self.CREATE_FIELD_URL.format(app_token=self.app_token, table_id=table)
        result = self._client._request_json_with_auth_retry(  # noqa: SLF001
            "POST",
            url,
            payload=body,
            content_type_json=True,
        )
        self._clear_field_cache(table)
        return result

    def field_update(self, table_id: str, field_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        table = self._text(table_id)
        field = self._text(field_id)
        body = self._field_create_payload(payload if isinstance(payload, dict) else {})
        url = self.UPDATE_FIELD_URL.format(app_token=self.app_token, table_id=table, field_id=field)
        result = self._client._request_json_with_auth_retry(  # noqa: SLF001
            "PUT",
            url,
            payload=body,
            content_type_json=True,
        )
        self._clear_field_cache(table)
        return result

    def _existing_field_names(self, table_id: str) -> set[str]:
        return {self._text(item.get("name")) for item in self.field_list(table_id) if self._text(item.get("name"))}

    def record_list(self, table_id: str, view_id: str, fields: List[str]) -> List[Dict[str, Any]]:
        requested = [self._text(item) for item in fields if self._text(item)]
        existing = self._existing_field_names(table_id)
        selected = [item for item in requested if item in existing]
        missing = [item for item in requested if item not in existing]
        if missing:
            self.emit_log(
                f"[暖通运行数据同步] 表字段不存在，读取时跳过 table={table_id}, fields={','.join(missing[:12])}"
                + (f" 等{len(missing)}项" if len(missing) > 12 else "")
            )
        return self._client.list_records(
            table_id=table_id,
            page_size=self.page_size,
            max_records=0,
            view_id=str(view_id or "").strip(),
            field_names=selected,
        )

    def batch_update(self, table_id: str, record_ids: List[str], patch: Dict[str, Any]) -> int:
        normalized_ids = [self._text(item) for item in record_ids if self._text(item)]
        fields = patch if isinstance(patch, dict) else {}
        if not normalized_ids or not fields:
            return 0
        payload = [{"record_id": record_id, "fields": dict(fields)} for record_id in normalized_ids]
        self._client.batch_update_records(table_id=table_id, records=payload, batch_size=self.batch_size)
        return len(normalized_ids)

    def batch_delete(self, table_id: str, record_ids: List[str]) -> int:
        normalized_ids = [self._text(item) for item in record_ids if self._text(item)]
        if not normalized_ids:
            return 0
        return self._client.batch_delete_records(
            table_id=table_id,
            record_ids=normalized_ids,
            batch_size=self.batch_size,
        )

    def batch_create(self, table_id: str, fields: List[str], rows: List[List[Any]]) -> int:
        field_names = [self._text(item) for item in fields if self._text(item)]
        fields_list: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, list):
                continue
            payload = {
                field_name: value
                for field_name, value in zip(field_names, row)
                if value not in (None, "")
            }
            if payload:
                fields_list.append(payload)
        if fields_list:
            self._client.batch_create_records(
                table_id=table_id,
                fields_list=fields_list,
                batch_size=self.batch_size,
            )
        return len(fields_list)

    def send_interactive_message(
        self,
        chat_id: str,
        content: Dict[str, Any],
        identity: str,
        idempotency_key: str,
    ) -> Dict[str, Any]:
        receive_id = self._text(chat_id)
        if not receive_id:
            raise ValueError("chat_id 不能为空")
        body = self._client._request_json_with_auth_retry(  # noqa: SLF001
            "POST",
            self.SEND_MESSAGE_URL,
            params={"receive_id_type": "chat_id"},
            payload={
                "receive_id": receive_id,
                "msg_type": "interactive",
                "content": json.dumps(content or {}, ensure_ascii=False),
                "uuid": self._text(idempotency_key),
            },
            content_type_json=True,
        )
        return body


class HvacBitableSyncService:
    """Run the HVAC bitable sync after chiller mode source upload."""

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        self.runtime_config = runtime_config if isinstance(runtime_config, dict) else {}

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        merged = copy.deepcopy(base if isinstance(base, dict) else {})
        for key, value in (override if isinstance(override, dict) else {}).items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = HvacBitableSyncService._deep_merge(merged[key], value)
            else:
                merged[key] = copy.deepcopy(value)
        return merged

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "required": False,
            "dry_run": False,
            "send_mode_switch_alerts": False,
            "page_size": 500,
            "batch_size": 200,
            "base_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
            "source": {
                "table_id": "tblkvVCNRbtMmjQg",
                "all_view_id": "vewtnp2Ay9",
                "running_view_id": "vewSen1ncq",
            },
            "target": {
                "table_id": "tblxOyKdyyiMTdhR",
                "view_id": "vewyJLUSVm",
            },
            "weather": copy.deepcopy(hvac.DEFAULT_CONFIG.get("weather", {})),
            "notifications": copy.deepcopy(hvac.DEFAULT_CONFIG.get("notifications", {})),
        }

    def _cfg(self, chiller_cfg: Dict[str, Any] | None = None) -> Dict[str, Any]:
        raw_chiller = chiller_cfg if isinstance(chiller_cfg, dict) else self.runtime_config.get("chiller_mode_upload", {})
        raw_chiller = raw_chiller if isinstance(raw_chiller, dict) else {}
        upload_target = raw_chiller.get("target", {}) if isinstance(raw_chiller.get("target", {}), dict) else {}
        raw = raw_chiller.get("hvac_bitable_sync", {})
        raw = raw if isinstance(raw, dict) else {}
        merged = self._deep_merge(self._defaults(), raw)
        if not str(merged.get("base_token", "") or "").strip():
            merged["base_token"] = str(upload_target.get("app_token", "") or "").strip() or "ASLxbfESPahdTKs0A9NccgbrnXc"
        source = merged.get("source", {}) if isinstance(merged.get("source", {}), dict) else {}
        if not str(source.get("table_id", "") or "").strip():
            source["table_id"] = str(upload_target.get("table_id", "") or "").strip() or "tblkvVCNRbtMmjQg"
        merged["source"] = source
        merged["page_size"] = max(1, int(merged.get("page_size", 500) or 500))
        merged["batch_size"] = max(1, min(500, int(merged.get("batch_size", 200) or 200)))
        merged["enabled"] = bool(merged.get("enabled", True))
        merged["required"] = bool(merged.get("required", False))
        merged["dry_run"] = bool(merged.get("dry_run", False))
        merged["send_mode_switch_alerts"] = bool(merged.get("send_mode_switch_alerts", False))
        return merged

    @staticmethod
    def _emit(emit_log: Callable[[str], None], message: str) -> None:
        try:
            emit_log(message)
        except Exception:  # noqa: BLE001
            pass

    @staticmethod
    def _ensure_source_patch_fields(client: _HvacFeishuAppClient, table_id: str, emit_log: Callable[[str], None]) -> Dict[str, Any]:
        existing = client._existing_field_names(table_id)
        required_text_fields = [
            hvac.F_STATUS,
            hvac.F_REMARK,
            hvac.F_PLATE_DIFF,
            hvac.F_CHILLER_TOWER_DIFF,
            hvac.F_UNIT,
        ]
        created: List[str] = []
        for field_name in required_text_fields:
            if field_name in existing:
                continue
            client.field_create(table_id, {"type": "text", "name": field_name, "style": {"type": "plain"}})
            created.append(field_name)
            existing.add(field_name)
        if created:
            HvacBitableSyncService._emit(
                emit_log,
                f"[暖通运行数据同步] 源表补建字段完成 table={table_id}, fields={','.join(created)}",
            )
        return {"created": created}

    @staticmethod
    def _raise_if_required_fields_missing(
        *,
        client: _HvacFeishuAppClient,
        table_id: str,
        field_names: Iterable[str],
        table_label: str,
    ) -> None:
        existing = client._existing_field_names(table_id)
        missing = [name for name in field_names if str(name or "").strip() and str(name or "").strip() not in existing]
        if missing:
            raise RuntimeError(f"{table_label} 缺少字段: {', '.join(missing[:30])}")

    def sync_after_chiller_upload(
        self,
        *,
        emit_log: Callable[[str], None] = print,
        chiller_cfg: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        started = time.perf_counter()
        cfg = self._cfg(chiller_cfg)
        if not cfg.get("enabled", True):
            self._emit(emit_log, "[暖通运行数据同步] 功能未启用，跳过")
            return {"ok": True, "status": "skipped", "reason": "disabled"}

        source_cfg = cfg.get("source", {}) if isinstance(cfg.get("source", {}), dict) else {}
        target_cfg = cfg.get("target", {}) if isinstance(cfg.get("target", {}), dict) else {}
        base_token = str(cfg.get("base_token", "") or "").strip()
        source_table = str(source_cfg.get("table_id", "") or "").strip()
        source_view = str(source_cfg.get("all_view_id", "") or "").strip()
        target_table = str(target_cfg.get("table_id", "") or "").strip()
        target_view = str(target_cfg.get("view_id", "") or "").strip()
        if not base_token or not source_table or not target_table:
            raise RuntimeError("暖通运行数据同步配置缺失: base_token/source.table_id/target.table_id")

        self._emit(
            emit_log,
            f"[暖通运行数据同步] 开始 source_table={source_table}, target_table={target_table}, "
            f"mode={'dry_run' if cfg.get('dry_run') else 'write'}",
        )
        client = _HvacFeishuAppClient(
            runtime_config=self.runtime_config,
            app_token=base_token,
            default_table_id=source_table,
            page_size=int(cfg.get("page_size", 500) or 500),
            batch_size=int(cfg.get("batch_size", 200) or 200),
            emit_log=emit_log,
        )

        source_field_changes: Dict[str, Any] = {"created": []}
        if not cfg.get("dry_run"):
            source_field_changes = self._ensure_source_patch_fields(client, source_table, emit_log)

        source_records = client.record_list(source_table, source_view, list(hvac.SOURCE_FIELDS))
        derived = hvac.derive_source(source_records)
        target_rows = hvac.build_target_rows(derived)
        alert_count = sum(1 for row in target_rows if row.get(hvac.T_MODE_SWITCH_HINT))

        report: Dict[str, Any] = {
            "ok": True,
            "status": "success",
            "source_table": source_table,
            "target_table": target_table,
            "source_records": len(source_records),
            "source_patch_records": len(derived.source_patches),
            "target_planned_rows": len(target_rows),
            "mode_switch_alert_count": alert_count,
            "source_field_changes": source_field_changes,
            "dry_run": bool(cfg.get("dry_run")),
        }

        if cfg.get("dry_run"):
            report["elapsed_ms"] = int((time.perf_counter() - started) * 1000)
            self._emit(emit_log, f"[暖通运行数据同步] dry-run完成 report={json.dumps(report, ensure_ascii=False)}")
            return report

        report["source_updated_records"] = hvac.apply_source_patches(client, source_table, derived.source_patches)
        report["target_field_changes"] = hvac.ensure_target_fields(client, target_table)
        weather_provider = hvac.WeatherSummaryProvider(cfg.get("weather", {}) if isinstance(cfg.get("weather", {}), dict) else {})
        report["target_write"] = hvac.upsert_target_rows(
            client,
            target_table,
            target_view,
            target_rows,
            weather_provider,
        )
        if weather_provider.error:
            report["weather_forecast_error"] = weather_provider.error
        if weather_provider.warning_error:
            report["weather_warning_error"] = weather_provider.warning_error
        if weather_provider.nearest_window_count:
            report["weather_nearest_window_count"] = weather_provider.nearest_window_count

        source_after = client.record_list(source_table, source_view, list(hvac.SOURCE_FIELDS))
        target_after = client.record_list(target_table, target_view, list(hvac.TARGET_READ_FIELDS))
        derived_after = hvac.derive_source(source_after)
        source_mismatches = hvac.verify_source(source_after, derived_after)
        target_mismatches = hvac.verify_target(target_after, target_rows)
        report["source_verify_mismatches"] = source_mismatches[:20]
        report["source_verify_mismatch_count"] = len(source_mismatches)
        report["target_verify_mismatches"] = target_mismatches[:20]
        report["target_verify_mismatch_count"] = len(target_mismatches)

        notify_config = dict((cfg.get("notifications", {}) if isinstance(cfg.get("notifications", {}), dict) else {}).get("mode_switch_alerts", {}) or {})
        should_send_alerts = bool(notify_config.get("enabled", False) or cfg.get("send_mode_switch_alerts", False))
        if should_send_alerts:
            report["mode_switch_alert_push"] = hvac.send_mode_switch_alerts(client, target_rows, notify_config)
        else:
            report["mode_switch_alert_push"] = {
                "sent": False,
                "reason": "disabled",
                "count": alert_count,
            }

        report["elapsed_ms"] = int((time.perf_counter() - started) * 1000)
        self._emit(
            emit_log,
            f"[暖通运行数据同步] 完成 source_records={report['source_records']}, "
            f"source_updated={report['source_updated_records']}, target_rows={report['target_planned_rows']}, "
            f"alerts={alert_count}, elapsed_ms={report['elapsed_ms']}",
        )
        return report

    def safe_sync_after_chiller_upload(
        self,
        *,
        emit_log: Callable[[str], None] = print,
        chiller_cfg: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        cfg = self._cfg(chiller_cfg)
        try:
            return self.sync_after_chiller_upload(emit_log=emit_log, chiller_cfg=chiller_cfg)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            self._emit(emit_log, f"[暖通运行数据同步][失败] {error}")
            if bool(cfg.get("required", False)):
                raise
            return {
                "ok": False,
                "status": "failed",
                "error": error,
                "required": False,
            }
