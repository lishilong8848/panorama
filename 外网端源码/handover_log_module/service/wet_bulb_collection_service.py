from __future__ import annotations

import copy
import re
from datetime import datetime
from typing import Any, Callable, Dict, List

from app.config.config_compat_cleanup import sanitize_wet_bulb_collection_config
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.bitable_target_resolver import BitableTargetResolver
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from handover_log_module.api.facade import load_handover_config
from handover_log_module.core.formatter import build_metric_text
from handover_log_module.core.models import MetricHit, RawRow
from handover_log_module.service.handover_download_service import HandoverDownloadService
from handover_log_module.service.handover_extract_service import HandoverExtractService


class WetBulbCollectionService:
    def __init__(self, runtime_config: Dict[str, Any], download_browser_pool: Any | None = None) -> None:
        self.runtime_config = runtime_config if isinstance(runtime_config, dict) else {}
        self._download_browser_pool = download_browser_pool

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "scheduler": {
                "enabled": True,
                "auto_start_in_gui": False,
                "interval_minutes": 60,
                "check_interval_sec": 30,
                "retry_failed_on_next_tick": True,
                "state_file": "wet_bulb_collection_scheduler_state.json",
            },
            "source": {
                "reuse_handover_download": True,
                "reuse_handover_rule_engine": True,
            },
            "target": {
                "app_token": "JbZywYBfgiltYpksj2bc1HvCnPd",
                "table_id": "tblm3MOOxKCW3ZPd",
                "page_size": 500,
                "max_records": 5000,
                "delete_batch_size": 200,
                "create_batch_size": 200,
                "replace_existing": True,
            },
            "fields": {
                "date": "日期",
                "building": "楼栋",
                "wet_bulb_temp": "天气湿球温度",
                "cooling_mode": "冷源运行模式",
                "sequence": "序号",
            },
            "cooling_mode": {
                "priority_order": ["1", "2", "3", "4"],
                "source_value_map": {
                    "1": "制冷",
                    "2": "预冷",
                    "3": "板换",
                    "4": "停机",
                },
                "upload_value_map": {
                    "制冷": "机械制冷",
                    "预冷": "预冷模式",
                    "板换": "自然冷模式",
                },
                "skip_modes": ["停机"],
            },
        }

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        merged = copy.deepcopy(base or {})
        for key, value in (override or {}).items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = WetBulbCollectionService._deep_merge(merged[key], value)
            else:
                merged[key] = copy.deepcopy(value)
        return merged

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = sanitize_wet_bulb_collection_config(self.runtime_config.get("wet_bulb_collection", {}))
        cfg = self._deep_merge(self._defaults(), raw)

        scheduler = cfg.get("scheduler", {}) if isinstance(cfg.get("scheduler", {}), dict) else {}
        source = cfg.get("source", {}) if isinstance(cfg.get("source", {}), dict) else {}
        target = cfg.get("target", {}) if isinstance(cfg.get("target", {}), dict) else {}
        fields = cfg.get("fields", {}) if isinstance(cfg.get("fields", {}), dict) else {}
        cooling_mode = cfg.get("cooling_mode", {}) if isinstance(cfg.get("cooling_mode", {}), dict) else {}

        cfg["enabled"] = bool(cfg.get("enabled", True))

        scheduler["enabled"] = bool(scheduler.get("enabled", True))
        scheduler["auto_start_in_gui"] = bool(scheduler.get("auto_start_in_gui", False))
        scheduler["interval_minutes"] = max(1, int(scheduler.get("interval_minutes", 60) or 60))
        scheduler["check_interval_sec"] = max(1, int(scheduler.get("check_interval_sec", 30) or 30))
        scheduler["retry_failed_on_next_tick"] = bool(scheduler.get("retry_failed_on_next_tick", True))
        scheduler["state_file"] = str(scheduler.get("state_file", "")).strip() or "wet_bulb_collection_scheduler_state.json"
        cfg["scheduler"] = scheduler

        source["reuse_handover_download"] = bool(source.get("reuse_handover_download", True))
        source["reuse_handover_rule_engine"] = bool(source.get("reuse_handover_rule_engine", True))
        cfg["source"] = source

        target["app_token"] = str(target.get("app_token", "")).strip()
        target["table_id"] = str(target.get("table_id", "")).strip()
        target["page_size"] = max(1, int(target.get("page_size", 500) or 500))
        target["max_records"] = max(1, int(target.get("max_records", 5000) or 5000))
        target["delete_batch_size"] = max(1, int(target.get("delete_batch_size", 200) or 200))
        target["create_batch_size"] = max(1, int(target.get("create_batch_size", 200) or 200))
        target["replace_existing"] = bool(target.get("replace_existing", True))
        cfg["target"] = target

        for key, default in self._defaults()["fields"].items():
            fields[key] = str(fields.get(key, default)).strip() or default
        cfg["fields"] = fields

        default_cooling = self._defaults()["cooling_mode"]
        priority_order = cooling_mode.get("priority_order", default_cooling["priority_order"])
        source_value_map = cooling_mode.get("source_value_map", default_cooling["source_value_map"])
        upload_value_map = cooling_mode.get("upload_value_map", default_cooling["upload_value_map"])
        skip_modes = cooling_mode.get("skip_modes", default_cooling["skip_modes"])

        cooling_mode["priority_order"] = [str(item).strip() for item in priority_order if str(item).strip()] or list(default_cooling["priority_order"])
        if isinstance(source_value_map, dict):
            cooling_mode["source_value_map"] = {
                str(key).strip(): str(value).strip()
                for key, value in source_value_map.items()
                if str(key).strip()
            }
        else:
            cooling_mode["source_value_map"] = dict(default_cooling["source_value_map"])
        if isinstance(upload_value_map, dict):
            cooling_mode["upload_value_map"] = {
                str(key).strip(): str(value).strip()
                for key, value in upload_value_map.items()
                if str(key).strip()
            }
        else:
            cooling_mode["upload_value_map"] = dict(default_cooling["upload_value_map"])
        cooling_mode["skip_modes"] = [str(item).strip() for item in skip_modes if str(item).strip()]
        cfg["cooling_mode"] = cooling_mode
        return cfg

    def _build_derived_runtime_cfg(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        runtime_cfg = copy.deepcopy(self.runtime_config)
        handover = runtime_cfg.get("handover_log") if isinstance(runtime_cfg.get("handover_log"), dict) else {}
        runtime_cfg["handover_log"] = handover
        download = handover.get("download") if isinstance(handover.get("download"), dict) else {}
        handover["download"] = download
        network_cfg = runtime_cfg.get("network") if isinstance(runtime_cfg.get("network"), dict) else {}
        runtime_cfg["network"] = network_cfg
        # 湿球温度采集固定按当前角色网络执行，不再切换网络。
        download["switch_to_internal_before_download"] = False
        return runtime_cfg

    def _new_target_resolver(self) -> BitableTargetResolver:
        global_feishu = self.runtime_config.get("feishu", {}) if isinstance(self.runtime_config.get("feishu", {}), dict) else {}
        return BitableTargetResolver(
            app_id=str(global_feishu.get("app_id", "")).strip(),
            app_secret=str(global_feishu.get("app_secret", "")).strip(),
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
        )

    def _new_client(self, cfg: Dict[str, Any], *, target_descriptor: Dict[str, Any] | None = None) -> FeishuBitableClient:
        global_feishu = require_feishu_auth_settings(self.runtime_config)

        resolved_target = dict(target_descriptor or {})
        if not resolved_target:
            resolved_target = self.build_target_descriptor(cfg, force_refresh=True)
        operation_app_token = str(resolved_target.get("operation_app_token", "") or "").strip()
        table_id = str(resolved_target.get("table_id", "") or "").strip()
        if str(resolved_target.get("target_kind", "")).strip() not in {"base_token_pair", "wiki_token_pair"}:
            raise ValueError(str(resolved_target.get("message", "")).strip() or "湿球温度目标多维表不可用")
        if not operation_app_token or not table_id:
            raise ValueError("湿球温度目标多维表缺少 operation_app_token/table_id")

        return FeishuBitableClient(
            app_id=str(global_feishu.get("app_id", "") or "").strip(),
            app_secret=str(global_feishu.get("app_secret", "") or "").strip(),
            app_token=operation_app_token,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=lambda **_: 0,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
        )

    def build_target_descriptor(self, cfg: Dict[str, Any] | None = None, *, force_refresh: bool = False) -> Dict[str, str]:
        normalized = cfg if isinstance(cfg, dict) else self._normalize_cfg()
        target = normalized.get("target", {}) if isinstance(normalized.get("target", {}), dict) else {}
        return dict(
            self._new_target_resolver().resolve_token_pair_preview(
                configured_app_token=str(target.get("app_token", "")).strip(),
                table_id=str(target.get("table_id", "")).strip(),
                force_refresh=force_refresh,
            )
        )

    @staticmethod
    def _midnight_timestamp_ms(run_date: str) -> int:
        dt = datetime.strptime(str(run_date or "").strip(), "%Y-%m-%d")
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _current_timestamp_ms() -> int:
        # Keep second precision so Feishu "日期" datetime field stores YYYY-MM-DD HH:MM:SS.
        current = datetime.now().replace(microsecond=0)
        return int(current.timestamp() * 1000)

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
                text = WetBulbCollectionService._normalize_select_text(item)
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

    @staticmethod
    def _extract_numbers(text: Any) -> List[float]:
        raw = str(text or "").strip()
        if not raw:
            return []
        values: List[float] = []
        for match in re.findall(r"[-+]?\d+(?:,\d{3})*(?:\.\d+)?", raw):
            try:
                values.append(float(match.replace(",", "")))
            except Exception:
                continue
        return values

    @staticmethod
    def _building_sequence_text(building: str) -> str:
        mapping = {
            "A楼": "1",
            "B楼": "2",
            "C楼": "3",
            "D楼": "4",
            "E楼": "5",
        }
        return mapping.get(str(building or "").strip(), "")

    @staticmethod
    def _describe_extract_error(code: Any) -> str:
        text = str(code or "").strip() or "extract_failed"
        mapping = {
            "cooling_mode_stopped_skipped": "冷源运行模式为停机，已跳过",
            "cooling_mode_ambiguous": "冷源运行模式同时命中多个值",
            "unknown_cooling_mode": "无法识别冷源运行模式",
            "invalid_wet_bulb_temp": "无法解析湿球温度",
            "unknown_building_sequence": "未识别楼栋序号",
            "page_timeout": "页面超时",
            "extract_failed": "提取失败",
        }
        return mapping.get(text, text)

    def list_existing_records(self, cfg: Dict[str, Any], emit_log: Callable[[str], None] = print) -> List[Dict[str, Any]]:
        target = cfg.get("target", {})
        target_descriptor = self.build_target_descriptor(cfg)
        table_id = str(target_descriptor.get("table_id", "")).strip()
        client = self._new_client(cfg, target_descriptor=target_descriptor)
        records = client.list_records(
            table_id=table_id,
            page_size=int(target.get("page_size", 500) or 500),
            max_records=int(target.get("max_records", 5000) or 5000),
        )
        emit_log(f"[湿球温度定时采集] 读取旧记录完成: table_id={table_id}, total={len(records)}")
        return records

    def _matching_existing_record_ids(
        self,
        *,
        existing_records: List[Dict[str, Any]],
        building: str,
        run_date: str,
        cfg: Dict[str, Any],
    ) -> List[str]:
        fields = cfg.get("fields", {})
        target_building = str(building or "").strip()
        target_date_ms = self._midnight_timestamp_ms(run_date)
        matched: List[str] = []
        for item in existing_records:
            if not isinstance(item, dict):
                continue
            record_id = str(item.get("record_id", "")).strip()
            payload_fields = item.get("fields", {})
            if not record_id or not isinstance(payload_fields, dict):
                continue
            building_text = self._normalize_select_text(payload_fields.get(fields.get("building", "楼栋")))
            date_ms = self._normalize_date_to_midnight_ms(payload_fields.get(fields.get("date", "日期")))
            if building_text == target_building and date_ms == target_date_ms:
                matched.append(record_id)
        return matched

    def _resolve_wet_bulb_value(
        self,
        *,
        hits: Dict[str, MetricHit],
        effective_config: Dict[str, Any],
        rows: List[RawRow] | None = None,
    ) -> float:
        hit = hits.get("wet_bulb")
        if hit is not None and hit.value is not None:
            try:
                return float(hit.value)
            except Exception:
                numbers = self._extract_numbers(hit.value)
                if numbers:
                    return float(numbers[0])
        templates = effective_config.get("format_templates", {}) if isinstance(effective_config.get("format_templates", {}), dict) else {}
        rendered = build_metric_text("wet_bulb", hits, templates, effective_config)
        numbers = self._extract_numbers(rendered)
        if numbers:
            return float(numbers[0])
        fallback_keywords: List[str] = []
        rule_rows = effective_config.get("rule_rows", [])
        if isinstance(rule_rows, list):
            for item in rule_rows:
                if not isinstance(item, dict):
                    continue
                if str(item.get("id", "")).strip() != "wet_bulb":
                    continue
                keywords = item.get("d_keywords", [])
                if isinstance(keywords, list):
                    fallback_keywords.extend(str(keyword).strip() for keyword in keywords if str(keyword).strip())
        fallback_keywords.extend(
            [
                "室外湿球温度",
                "室外温度1",
                "E-124-DDC-100_室外湿度1",
                "E-124-DDC-100_室外湿球温度",
            ]
        )
        seen_keywords: set[str] = set()
        normalized_keywords: List[str] = []
        for keyword in fallback_keywords:
            lowered = str(keyword).strip().casefold()
            if not lowered or lowered in seen_keywords:
                continue
            seen_keywords.add(lowered)
            normalized_keywords.append(lowered)
        for row in rows or []:
            d_name = str(getattr(row, "d_name", "") or "").strip()
            if not d_name:
                continue
            lowered_name = d_name.casefold()
            if not any(keyword in lowered_name for keyword in normalized_keywords):
                continue
            numbers = self._extract_numbers(getattr(row, "e_raw", None))
            if numbers:
                return float(numbers[0])
        raise ValueError("invalid_wet_bulb_temp")

    def _resolve_cooling_mode_value(
        self,
        *,
        hits: Dict[str, MetricHit],
        effective_config: Dict[str, Any],
        cfg: Dict[str, Any],
    ) -> Dict[str, str]:
        cooling_cfg = cfg.get("cooling_mode", {})
        source_value_map = (
            cooling_cfg.get("source_value_map", {})
            if isinstance(cooling_cfg.get("source_value_map", {}), dict)
            else {}
        )
        upload_map = (
            cooling_cfg.get("upload_value_map", {})
            if isinstance(cooling_cfg.get("upload_value_map", {}), dict)
            else {}
        )
        priority_order = [
            str(item).strip()
            for item in cooling_cfg.get("priority_order", [])
            if str(item).strip()
        ]
        skip_modes = {
            str(item).strip()
            for item in cooling_cfg.get("skip_modes", [])
            if str(item).strip()
        }

        chiller_cfg = effective_config.get("chiller_mode", {}) if isinstance(effective_config.get("chiller_mode", {}), dict) else {}
        west_keys = chiller_cfg.get("west_keys", []) if isinstance(chiller_cfg.get("west_keys", []), list) else []
        east_keys = chiller_cfg.get("east_keys", []) if isinstance(chiller_cfg.get("east_keys", []), list) else []
        ordered_keys = [str(item).strip() for item in (west_keys + east_keys) if str(item).strip()]
        if not ordered_keys:
            ordered_keys = [f"chiller_mode_{index}" for index in range(1, 7)]

        def normalize_mode_code(value: Any) -> str:
            if value is None:
                return ""
            raw = str(value).strip()
            if not raw:
                return ""
            if raw in source_value_map:
                return raw
            try:
                number = float(raw)
            except ValueError:
                number = None
            if number is not None and int(number) == number:
                key = str(int(number))
                if key in source_value_map:
                    return key
            lowered = raw.casefold()
            for code, text in source_value_map.items():
                if str(text).strip().casefold() == lowered:
                    return str(code).strip()
            return ""

        all_mode_hits: List[tuple[str, str, str]] = []
        active_modes_by_code: Dict[str, Dict[str, str]] = {}
        for metric_key in ordered_keys:
            hit = hits.get(metric_key)
            if hit is None:
                continue
            mode_code = normalize_mode_code(hit.value)
            if not mode_code:
                continue
            source_text = str(source_value_map.get(mode_code, "")).strip()
            if not source_text:
                continue
            all_mode_hits.append((source_text, mode_code, metric_key))
            if source_text in skip_modes:
                continue
            active_modes_by_code.setdefault(
                mode_code,
                {
                    "mode_code": mode_code,
                    "metric_key": metric_key,
                    "source_text": source_text,
                },
            )

        if not active_modes_by_code:
            if all_mode_hits:
                raise RuntimeError("cooling_mode_stopped_skipped")
            raise ValueError("unknown_cooling_mode")
        resolved_meta: Dict[str, str] | None = None
        if len(active_modes_by_code) > 1:
            for preferred_code in priority_order:
                candidate = active_modes_by_code.get(preferred_code)
                if candidate is not None:
                    resolved_meta = candidate
                    break
        if resolved_meta is None:
            resolved_meta = next(iter(active_modes_by_code.values()))

        source_text = str(resolved_meta.get("source_text", "")).strip()
        upload_text = str(upload_map.get(source_text, "")).strip()
        if not upload_text:
            raise ValueError("unknown_cooling_mode")
        return {
            "source_text": source_text,
            "upload_text": upload_text,
            "source_code": str(resolved_meta.get("mode_code", "")).strip(),
            "source_key": str(resolved_meta.get("metric_key", "")).strip(),
        }

    def _extract_metrics_from_source(
        self,
        *,
        extract_service: HandoverExtractService,
        building: str,
        file_path: str,
        cfg: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        result = extract_service.extract(building=building, data_file=file_path)
        hits = result.get("hits", {}) if isinstance(result.get("hits", {}), dict) else {}
        effective_config = result.get("effective_config", {}) if isinstance(result.get("effective_config", {}), dict) else {}
        rows = result.get("rows", []) if isinstance(result.get("rows", []), list) else []
        wet_bulb = self._resolve_wet_bulb_value(hits=hits, effective_config=effective_config, rows=rows)
        cooling = self._resolve_cooling_mode_value(hits=hits, effective_config=effective_config, cfg=cfg)
        emit_log(
            f"[湿球温度定时采集][{building}] 提取完成: 湿球温度={wet_bulb}, 模式={cooling['source_text']}->{cooling['upload_text']}"
        )
        return {
            "hits": hits,
            "effective_config": effective_config,
            "wet_bulb_metric_value": wet_bulb,
            "cooling_mode_source_text": cooling["source_text"],
            "cooling_mode_uploaded_text": cooling["upload_text"],
            "cooling_mode_source_code": cooling["source_code"],
            "cooling_mode_source_key": cooling["source_key"],
        }

    def download_source_units(
        self,
        *,
        buildings: List[str] | None = None,
        emit_log: Callable[[str], None] = print,
        cfg: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized_cfg = cfg if isinstance(cfg, dict) else self._normalize_cfg()
        derived_runtime = self._build_derived_runtime_cfg(normalized_cfg)
        handover_cfg = load_handover_config(derived_runtime)
        download_service = HandoverDownloadService(
            handover_cfg,
            download_browser_pool=self._download_browser_pool,
        )
        target_buildings = [str(item).strip() for item in (buildings or []) if str(item).strip()]
        reuse_download = bool(normalized_cfg.get("source", {}).get("reuse_handover_download", True))

        download_result = download_service.run(
            buildings=target_buildings or None,
            reuse_cached=reuse_download,
            emit_log=emit_log,
        )
        success_items = download_result.get("success_files", []) if isinstance(download_result.get("success_files", []), list) else []
        failed_items = download_result.get("failed", []) if isinstance(download_result.get("failed", []), list) else []

        failed_buildings: List[Dict[str, Any]] = []
        source_units: List[Dict[str, Any]] = []

        for item in failed_items:
            if not isinstance(item, dict):
                continue
            failed_buildings.append(
                {
                    "building": str(item.get("building", "")).strip() or "-",
                    "error": str(item.get("error", "")).strip() or "download_failed",
                    "code": "download_failed",
                }
            )

        for item in success_items:
            if not isinstance(item, dict):
                continue
            building = str(item.get("building", "")).strip()
            file_path = str(item.get("file_path", "")).strip()
            if not building or not file_path:
                continue
            source_units.append({"building": building, "file_path": file_path})

        emit_log("[湿球温度定时采集] 下载阶段完成，继续按当前角色网络执行后续流程")

        return {
            "source_units": source_units,
            "failed_buildings": failed_buildings,
            "download_result": {
                "success_count": len(success_items),
                "failed_count": len(failed_items),
            },
        }

    def continue_from_source_units(
        self,
        *,
        source_units: List[Dict[str, Any]],
        emit_log: Callable[[str], None] = print,
        cfg: Dict[str, Any] | None = None,
        target_descriptor: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized_cfg = cfg if isinstance(cfg, dict) else self._normalize_cfg()
        run_date = datetime.now().strftime("%Y-%m-%d")
        deleted_count = 0
        created_count = 0
        source_summary = [
            f"{str(item.get('building', '')).strip() or '-'}={str(item.get('file_path', '') or item.get('source_file', '')).strip() or '-'}"
            for item in source_units
            if isinstance(item, dict)
        ] if isinstance(source_units, list) else []
        emit_log(
            "[湿球温度定时采集] 共享源文件继续处理开始: "
            f"count={len(source_summary)}, files={'; '.join(source_summary) or '-'}"
        )
        emit_log("[湿球温度定时采集] 开始解析目标多维表")
        resolved_target = dict(target_descriptor or self.build_target_descriptor(normalized_cfg, force_refresh=True))
        emit_log(
            "[湿球温度定时采集] 目标多维表解析完成: "
            f"target_kind={str(resolved_target.get('target_kind', '') or '-').strip() or '-'}, "
            f"table_id={str(resolved_target.get('table_id', '') or '-').strip() or '-'}"
        )

        if not normalized_cfg.get("enabled", True):
            emit_log(
                "[湿球温度定时采集] 检测到旧配置 enabled=false；"
                "本次共享源文件处理已由用户/调度明确触发，继续执行"
            )
        if str(resolved_target.get("target_kind", "")).strip() not in {"base_token_pair", "wiki_token_pair"}:
            code = "target_invalid" if str(resolved_target.get("target_kind", "")).strip() == "invalid" else "target_probe_error"
            message = str(resolved_target.get("message", "")).strip() or "湿球温度目标多维表不可用"
            emit_log(f"[湿球温度定时采集] 目标多维表不可用: {message}")
            return {
                "status": "failed",
                "run_date": run_date,
                "uploaded_buildings": [],
                "failed_buildings": [{"building": "-", "error": message, "code": code}],
                "skipped_buildings": [],
                "details": {},
                "deleted_count": deleted_count,
                "created_count": created_count,
                "target": resolved_target,
            }

        handover_cfg = load_handover_config(self._build_derived_runtime_cfg(normalized_cfg))
        extract_service = HandoverExtractService(handover_cfg)
        failed_buildings: List[Dict[str, Any]] = []
        skipped_buildings: List[Dict[str, Any]] = []
        uploaded_buildings: List[str] = []
        details: Dict[str, Any] = {}
        prepared_rows: List[Dict[str, Any]] = []
        run_timestamp_ms = self._current_timestamp_ms()

        for item in source_units if isinstance(source_units, list) else []:
            if not isinstance(item, dict):
                continue
            building = str(item.get("building", "")).strip()
            file_path = str(item.get("file_path", "")).strip() or str(item.get("source_file", "")).strip()
            if not building or not file_path:
                continue
            try:
                emit_log(f"[湿球温度定时采集][{building}] 开始读取共享源文件: {file_path}")
                extracted = self._extract_metrics_from_source(
                    extract_service=extract_service,
                    building=building,
                    file_path=file_path,
                    cfg=normalized_cfg,
                    emit_log=emit_log,
                )
                sequence_text = self._building_sequence_text(building)
                if not sequence_text:
                    raise ValueError("unknown_building_sequence")
                fields = normalized_cfg.get("fields", {})
                prepared_rows.append(
                    {
                        "building": building,
                        "file_path": file_path,
                        "fields": {
                            str(fields.get("date", "日期")).strip(): run_timestamp_ms,
                            str(fields.get("building", "楼栋")).strip(): building,
                            str(fields.get("wet_bulb_temp", "天气湿球温度")).strip(): extracted["wet_bulb_metric_value"],
                            str(fields.get("cooling_mode", "冷源运行模式")).strip(): extracted["cooling_mode_uploaded_text"],
                            str(fields.get("sequence", "序号")).strip(): sequence_text,
                        },
                        "extracted": extracted,
                    }
                )
            except RuntimeError as exc:
                code = str(exc).strip() or "skipped"
                if code == "cooling_mode_stopped_skipped":
                    skipped_buildings.append(
                        {
                            "building": building,
                            "reason": "冷源运行模式为停机，已跳过",
                            "code": code,
                        }
                    )
                    emit_log(f"[湿球温度定时采集][{building}] 跳过: 冷源运行模式为停机")
                else:
                    error_text = self._describe_extract_error(code)
                    failed_buildings.append({"building": building, "error": error_text, "code": code})
                    emit_log(f"[湿球温度定时采集][{building}] 提取失败: {error_text}")
            except Exception as exc:
                code = str(exc).strip() or "extract_failed"
                error_text = self._describe_extract_error(code)
                failed_buildings.append({"building": building, "error": error_text, "code": code})
                emit_log(f"[湿球温度定时采集][{building}] 提取失败: {error_text}")

        if prepared_rows:
            client = self._new_client(normalized_cfg, target_descriptor=resolved_target)
            table_id = str(resolved_target.get("table_id", "")).strip()
            emit_log(
                f"[湿球温度定时采集] 准备整表重建: prepared={len(prepared_rows)}, "
                f"failed={len(failed_buildings)}, skipped={len(skipped_buildings)}"
            )
            try:
                deleted_count = client.clear_table(
                    table_id=table_id,
                    list_page_size=int(normalized_cfg.get("target", {}).get("page_size", 500) or 500),
                    delete_batch_size=int(normalized_cfg.get("target", {}).get("delete_batch_size", 200) or 200),
                    list_field_names=[str(normalized_cfg.get("fields", {}).get("building", "楼栋") or "楼栋").strip()],
                )
                emit_log(f"[湿球温度定时采集] 整表清空完成: deleted={deleted_count}")
            except Exception as exc:
                failed_buildings.append({"building": "-", "error": str(exc), "code": "clear_table_failed"})
                emit_log(f"[湿球温度定时采集] 整表清空失败: {exc}")
            else:
                try:
                    created_records = client.batch_create_records(
                        table_id=table_id,
                        fields_list=[item.get("fields", {}) for item in prepared_rows],
                        batch_size=int(normalized_cfg.get("target", {}).get("create_batch_size", 200) or 200),
                    )
                    created_count = len(created_records) if isinstance(created_records, list) else len(prepared_rows)
                    emit_log(f"[湿球温度定时采集] 批量写入完成: created={created_count}")
                    for item in prepared_rows:
                        building = str(item.get("building", "")).strip()
                        extracted = item.get("extracted", {})
                        file_path = str(item.get("file_path", "")).strip()
                        uploaded_buildings.append(building)
                        details[building] = {
                            "source_file": file_path,
                            "wet_bulb_metric_value": extracted.get("wet_bulb_metric_value"),
                            "cooling_mode_source_text": extracted.get("cooling_mode_source_text", ""),
                            "cooling_mode_uploaded_text": extracted.get("cooling_mode_uploaded_text", ""),
                            "uploaded_count": 1,
                        }
                        emit_log(f"[湿球温度定时采集][{building}] 上传完成")
                except Exception as exc:
                    failed_buildings.append({"building": "-", "error": str(exc), "code": "upload_failed"})
                    emit_log(f"[湿球温度定时采集] 批量写入失败: {exc}")
        else:
            emit_log("[湿球温度定时采集] 本次没有可上传结果，保留目标多维表现有数据")

        if failed_buildings and uploaded_buildings:
            status = "partial_failed"
        elif failed_buildings:
            status = "failed"
        elif uploaded_buildings:
            status = "ok"
        else:
            status = "skipped"

        return {
            "status": status,
            "run_date": run_date,
            "uploaded_buildings": uploaded_buildings,
            "failed_buildings": failed_buildings,
            "skipped_buildings": skipped_buildings,
            "details": details,
            "deleted_count": deleted_count,
            "created_count": created_count,
            "target": resolved_target,
        }

    def run(self, *, buildings: List[str] | None = None, emit_log: Callable[[str], None] = print) -> Dict[str, Any]:
        cfg = self._normalize_cfg()
        run_date = datetime.now().strftime("%Y-%m-%d")
        deleted_count = 0
        created_count = 0
        target_descriptor = self.build_target_descriptor(cfg, force_refresh=True)
        if not cfg.get("enabled", True):
            return {
                "status": "skipped",
                "run_date": run_date,
                "uploaded_buildings": [],
                "failed_buildings": [],
                "skipped_buildings": [],
                "details": {},
                "deleted_count": deleted_count,
                "created_count": created_count,
                "target": target_descriptor,
            }
        if str(target_descriptor.get("target_kind", "")).strip() not in {"base_token_pair", "wiki_token_pair"}:
            code = "target_invalid" if str(target_descriptor.get("target_kind", "")).strip() == "invalid" else "target_probe_error"
            message = str(target_descriptor.get("message", "")).strip() or "湿球温度目标多维表不可用"
            emit_log(f"[湿球温度定时采集] 目标多维表不可用: {message}")
            return {
                "status": "failed",
                "run_date": run_date,
                "uploaded_buildings": [],
                "failed_buildings": [{"building": "-", "error": message, "code": code}],
                "skipped_buildings": [],
                "details": {},
                "deleted_count": deleted_count,
                "created_count": created_count,
                "target": target_descriptor,
            }

        target_buildings = [str(item).strip() for item in (buildings or []) if str(item).strip()]
        emit_log(
            "[湿球温度定时采集] 开始执行: "
            f"network_mode=current_role, buildings={','.join(target_buildings) if target_buildings else '按交接班启用楼栋'}"
        )
        if target_descriptor.get("table_id"):
            emit_log(
                "[湿球温度定时采集] 目标多维表: "
                f"configured_app_token={target_descriptor.get('configured_app_token', '')}, "
                f"operation_app_token={target_descriptor.get('operation_app_token', '')}, "
                f"target_kind={target_descriptor.get('target_kind', '')}, "
                f"table_id={target_descriptor['table_id']}, "
                f"url={target_descriptor.get('display_url', '') or '-'}"
            )
        download_result = self.download_source_units(
            buildings=target_buildings or None,
            emit_log=emit_log,
            cfg=cfg,
        )
        source_units = download_result.get("source_units", []) if isinstance(download_result.get("source_units", []), list) else []
        continuation_result = self.continue_from_source_units(
            source_units=source_units,
            emit_log=emit_log,
            cfg=cfg,
            target_descriptor=target_descriptor,
        )

        failed_buildings = list(download_result.get("failed_buildings", [])) if isinstance(download_result.get("failed_buildings", []), list) else []
        failed_buildings.extend(
            continuation_result.get("failed_buildings", []) if isinstance(continuation_result.get("failed_buildings", []), list) else []
        )
        uploaded_buildings = list(continuation_result.get("uploaded_buildings", [])) if isinstance(continuation_result.get("uploaded_buildings", []), list) else []
        skipped_buildings = list(continuation_result.get("skipped_buildings", [])) if isinstance(continuation_result.get("skipped_buildings", []), list) else []

        if failed_buildings and uploaded_buildings:
            status = "partial_failed"
        elif failed_buildings:
            status = "failed"
        elif uploaded_buildings:
            status = "ok"
        else:
            status = str(continuation_result.get("status", "") or "skipped").strip() or "skipped"

        return {
            "status": status,
            "run_date": run_date,
            "uploaded_buildings": uploaded_buildings,
            "failed_buildings": failed_buildings,
            "skipped_buildings": skipped_buildings,
            "details": continuation_result.get("details", {}) if isinstance(continuation_result.get("details", {}), dict) else {},
            "deleted_count": int(continuation_result.get("deleted_count", 0) or 0),
            "created_count": int(continuation_result.get("created_count", 0) or 0),
            "target": continuation_result.get("target", target_descriptor),
            "download_result": download_result.get("download_result", {}),
        }
