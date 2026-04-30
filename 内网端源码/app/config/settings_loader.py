from __future__ import annotations

import copy
import json
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

from app.config.config_compat_cleanup import sanitize_day_metric_upload_config
from app.config.config_schema_v3 import DEFAULT_CONFIG_V3, deep_merge_defaults
from app.config.config_adapter import (
    adapt_runtime_config,
    ensure_v3_config,
    normalize_role_mode,
    resolve_shared_bridge_paths,
    sync_runtime_back_to_v3,
)
from app.config.handover_segment_store import (
    HANDOVER_SEGMENT_BUILDINGS,
    HandoverSegmentRevisionConflict,
    apply_handover_segment_data,
    building_name_from_segment_code,
    build_segment_document,
    build_segment_documents_from_config,
    create_pre_handover_segment_backup,
    extract_handover_building_data,
    extract_handover_common_data,
    handover_segment_aggregate_lock,
    handover_building_segment_path,
    handover_common_segment_path,
    handover_segment_write_lock,
    handover_segment_target_lock,
    has_all_handover_segment_files,
    has_any_handover_segment_file,
    read_all_segment_documents,
    read_segment_document,
    write_segment_document,
)
from handover_log_module.core.building_title_rules import (
    HANDOVER_BUILDING_TITLE_PATTERN,
    HANDOVER_TITLE_CELL,
    canonical_handover_building_title_map,
)
from handover_log_module.core.cell_rule_compiler import normalize_cell_rules
from handover_log_module.core.expression_eval import ExpressionError, get_expression_variables
from handover_log_module.service.footer_inventory_defaults_service import FooterInventoryDefaultsService
from pipeline_utils import (
    DEFAULT_CONFIG_TEMPLATE_FILENAME,
    get_app_dir,
    get_bundle_dir,
    load_download_module,
    load_pipeline_config,
    resolve_config_path,
)

_DAY_METRIC_REPAIR_BASELINE_FILENAME = "表格计算配置.backup.20260409-145808.json"


def _valid_time(value: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", str(value).strip()))


def _deployment_role_mode(cfg: Dict[str, Any]) -> str:
    common = cfg.get("common", {})
    if not isinstance(common, dict):
        return ""
    deployment = common.get("deployment", {})
    if not isinstance(deployment, dict):
        return ""
    return normalize_role_mode(deployment.get("role_mode"))


def _normalize_sheet_rules_config(raw_rules: Any) -> List[Dict[str, Any]]:
    if isinstance(raw_rules, dict):
        normalized_input: List[Any] = []
        for sheet_name, rule in raw_rules.items():
            if isinstance(rule, dict):
                normalized_input.append(
                    {
                        "sheet_name": str(sheet_name).strip(),
                        "table_id": rule.get("table_id", ""),
                        "header_row": rule.get("header_row", 1),
                    }
                )
            elif isinstance(rule, str):
                parts = [x.strip() for x in rule.split("|")]
                if not parts or not parts[0]:
                    raise ValueError(f"features.sheet_import.sheet_rules[{sheet_name}] 格式错误，应为 table_id|header_row")
                normalized_input.append(
                    {
                        "sheet_name": str(sheet_name).strip(),
                        "table_id": parts[0],
                        "header_row": parts[1] if len(parts) >= 2 and parts[1] else 1,
                    }
                )
            else:
                raise ValueError(f"features.sheet_import.sheet_rules[{sheet_name}] 必须是对象或字符串")
    elif isinstance(raw_rules, list):
        normalized_input = list(raw_rules)
    else:
        raise ValueError("features.sheet_import.sheet_rules 必须是数组或对象")

    if not normalized_input:
        raise ValueError("features.sheet_import.sheet_rules 不能为空")

    rules: List[Dict[str, Any]] = []
    seen_sheet: set[str] = set()
    for idx, item in enumerate(normalized_input, 1):
        if isinstance(item, dict):
            sheet_name = str(item.get("sheet_name", "")).strip()
            table_id = str(item.get("table_id", "")).strip()
            header_row_raw = item.get("header_row", 1)
        elif isinstance(item, str):
            parts = [x.strip() for x in item.split("|")]
            if len(parts) != 3:
                raise ValueError(f"features.sheet_import.sheet_rules 第{idx}项格式错误，应为 sheet_name|table_id|header_row")
            sheet_name, table_id, header_row_raw = parts
        else:
            raise ValueError(f"features.sheet_import.sheet_rules 第{idx}项必须是对象或字符串")

        if not sheet_name:
            raise ValueError(f"features.sheet_import.sheet_rules 第{idx}项 sheet_name 不能为空")
        if not table_id:
            raise ValueError(f"features.sheet_import.sheet_rules 第{idx}项 table_id 不能为空")

        try:
            header_row = int(header_row_raw)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"features.sheet_import.sheet_rules 第{idx}项 header_row 必须是整数") from exc
        if header_row < 1:
            raise ValueError(f"features.sheet_import.sheet_rules 第{idx}项 header_row 必须大于等于1")

        key = sheet_name.casefold()
        if key in seen_sheet:
            raise ValueError(f"features.sheet_import.sheet_rules 存在重复 sheet_name: {sheet_name}")
        seen_sheet.add(key)
        rules.append({"sheet_name": sheet_name, "table_id": table_id, "header_row": header_row})
    return rules


def ensure_defaults(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return ensure_v3_config(cfg)


def _normalize_handover_template_title_config(cfg: Dict[str, Any]) -> bool:
    if not isinstance(cfg, dict):
        return False
    features = cfg.get("features", {})
    if not isinstance(features, dict):
        return False
    handover = features.get("handover_log", {})
    if not isinstance(handover, dict):
        return False
    template = handover.get("template", {})
    if not isinstance(template, dict):
        return False

    changed = False
    canonical_map = canonical_handover_building_title_map()
    if bool(template.get("apply_building_title", True)) is not True:
        template["apply_building_title"] = True
        changed = True
    if str(template.get("title_cell", "") or "").strip().upper() != HANDOVER_TITLE_CELL:
        template["title_cell"] = HANDOVER_TITLE_CELL
        changed = True
    if str(template.get("building_title_pattern", "") or "").strip() != HANDOVER_BUILDING_TITLE_PATTERN:
        template["building_title_pattern"] = HANDOVER_BUILDING_TITLE_PATTERN
        changed = True

    raw_map = template.get("building_title_map", {})
    normalized_map = raw_map if isinstance(raw_map, dict) else {}
    normalized_map = {
        str(key or "").strip(): str(value or "").strip()
        for key, value in normalized_map.items()
        if str(key or "").strip()
    }
    if normalized_map != canonical_map:
        template["building_title_map"] = canonical_map
        changed = True

    handover["template"] = template
    features["handover_log"] = handover
    cfg["features"] = features
    return changed


def _contains_noncanonical_handover_template_title_config(cfg: Dict[str, Any]) -> bool:
    if not isinstance(cfg, dict):
        return False
    features = cfg.get("features", {})
    if not isinstance(features, dict) or "handover_log" not in features:
        return False
    handover = features.get("handover_log", {})
    if not isinstance(handover, dict) or "template" not in handover:
        return False
    template = handover.get("template", {})
    if not isinstance(template, dict):
        return False

    canonical_map = canonical_handover_building_title_map()
    if bool(template.get("apply_building_title", True)) is not True:
        return True
    if str(template.get("title_cell", "") or "").strip().upper() != HANDOVER_TITLE_CELL:
        return True
    if str(template.get("building_title_pattern", "") or "").strip() != HANDOVER_BUILDING_TITLE_PATTERN:
        return True
    raw_map = template.get("building_title_map", {})
    normalized_map = raw_map if isinstance(raw_map, dict) else {}
    normalized_map = {
        str(key or "").strip(): str(value or "").strip()
        for key, value in normalized_map.items()
        if str(key or "").strip()
    }
    return normalized_map != canonical_map


def _validate_console(cfg: Dict[str, Any]) -> None:
    console_cfg = cfg.get("common", {}).get("console", {})
    if not isinstance(console_cfg, dict):
        raise ValueError("配置错误: common.console 缺失或格式错误")
    required = [
        "enabled",
        "host",
        "port",
        "auto_open_browser",
        "open_browser_delay_sec",
        "ui_theme",
        "log_buffer_size",
        "enable_sse",
    ]
    missing = [k for k in required if k not in console_cfg]
    if missing:
        raise ValueError(f"配置错误: common.console 缺少字段 {missing}")
    port = int(console_cfg["port"])
    if port <= 0 or port > 65535:
        raise ValueError("配置错误: common.console.port 必须在 1-65535 之间")
    if int(console_cfg["open_browser_delay_sec"]) < 0:
        raise ValueError("配置错误: common.console.open_browser_delay_sec 必须大于等于0")
    if int(console_cfg["log_buffer_size"]) < 100:
        raise ValueError("配置错误: common.console.log_buffer_size 不能小于100")


def _validate_common_paths(cfg: Dict[str, Any]) -> None:
    paths_cfg = cfg.get("common", {}).get("paths", {})
    if not isinstance(paths_cfg, dict):
        raise ValueError("配置错误: common.paths 缺失或格式错误")
    if not str(paths_cfg.get("business_root_dir", "")).strip():
        raise ValueError("配置错误: common.paths.business_root_dir 不能为空")


def _validate_deployment_and_shared_bridge(cfg: Dict[str, Any]) -> None:
    common = cfg.get("common", {})
    if not isinstance(common, dict):
        raise ValueError("配置错误: common 缺失或格式错误")
    deployment = common.get("deployment", {})
    if not isinstance(deployment, dict):
        raise ValueError("配置错误: common.deployment 缺失或格式错误")
    role_mode = normalize_role_mode(deployment.get("role_mode"))
    if role_mode not in {"", "internal", "external"}:
        raise ValueError("配置错误: common.deployment.role_mode 必须是 internal / external，或留空等待启动确认")
    last_started_role_mode = normalize_role_mode(deployment.get("last_started_role_mode"))
    if last_started_role_mode not in {"", "internal", "external"}:
        raise ValueError("配置错误: common.deployment.last_started_role_mode 必须是 internal / external，或留空")
    shared_bridge = common.get("shared_bridge", {})
    if not isinstance(shared_bridge, dict):
        raise ValueError("配置错误: common.shared_bridge 缺失或格式错误")
    if role_mode in {"internal", "external"}:
        resolved_shared_bridge = resolve_shared_bridge_paths(shared_bridge, role_mode)
        if not bool(resolved_shared_bridge.get("enabled", False)):
            raise ValueError("配置错误: internal/external 角色必须启用 common.shared_bridge.enabled")
        role_root_key = "internal_root_dir" if role_mode == "internal" else "external_root_dir"
        role_root_dir = str(resolved_shared_bridge.get(role_root_key, "") or "").strip()
        active_root_dir = str(resolved_shared_bridge.get("root_dir", "") or "").strip()
        if not (role_root_dir or active_root_dir):
            raise ValueError(f"配置错误: {role_mode} 角色必须配置 common.shared_bridge.{role_root_key}")
    for key in (
        "poll_interval_sec",
        "heartbeat_interval_sec",
        "claim_lease_sec",
        "stale_task_timeout_sec",
        "artifact_retention_days",
        "sqlite_busy_timeout_ms",
    ):
        if int(shared_bridge.get(key, 0) or 0) <= 0:
            raise ValueError(f"配置错误: common.shared_bridge.{key} 必须大于0")


def _validate_scheduler(cfg: Dict[str, Any]) -> None:
    scheduler_cfg = cfg.get("common", {}).get("scheduler", {})
    if not isinstance(scheduler_cfg, dict):
        raise ValueError("配置错误: common.scheduler 缺失或格式错误")
    if int(scheduler_cfg.get("interval_minutes", 0)) <= 0:
        raise ValueError("配置错误: common.scheduler.interval_minutes 必须大于0")
    if int(scheduler_cfg.get("check_interval_sec", 0)) <= 0:
        raise ValueError("配置错误: common.scheduler.check_interval_sec 必须大于0")
    if not isinstance(scheduler_cfg.get("retry_failed_on_next_tick", True), bool):
        raise ValueError("配置错误: common.scheduler.retry_failed_on_next_tick 必须是布尔值")
    if not str(scheduler_cfg.get("state_file", "")).strip():
        raise ValueError("配置错误: common.scheduler.state_file 不能为空")


def _validate_updater(cfg: Dict[str, Any]) -> None:
    updater_cfg = cfg.get("common", {}).get("updater", {})
    if not isinstance(updater_cfg, dict):
        raise ValueError("配置错误: common.updater 缺失或格式错误")

    deployment = cfg.get("common", {}).get("deployment", {})
    role_mode = normalize_role_mode(deployment.get("role_mode"))

    required_text = ["state_file", "download_dir", "backup_dir"]
    if role_mode != "internal":
        required_text.extend(["gitee_repo", "gitee_branch", "gitee_subdir", "gitee_manifest_path"])

    for key in required_text:
        if not str(updater_cfg.get(key, "")).strip():
            raise ValueError(f"配置错误: common.updater.{key} 不能为空")

    for key in (
        "check_interval_sec",
        "request_timeout_sec",
        "download_retry_count",
        "max_backups",
    ):
        if int(updater_cfg.get(key, 0)) <= 0:
            raise ValueError(f"配置错误: common.updater.{key} 必须大于0")


def _validate_alarm_export(cfg: Dict[str, Any]) -> None:
    features = cfg.get("features", {})
    if not isinstance(features, dict):
        raise ValueError("配置错误: features 缺失或格式错误")
    alarm_export = features.get("alarm_export", {})
    if not isinstance(alarm_export, dict):
        raise ValueError("配置错误: features.alarm_export 缺失或格式错误")
    feishu = alarm_export.get("feishu", {})
    if not isinstance(feishu, dict):
        raise ValueError("配置错误: features.alarm_export.feishu 缺失或格式错误")

    app_token = str(feishu.get("app_token", "") or "").strip()
    table_id = str(feishu.get("table_id", "") or "").strip()
    if bool(app_token) != bool(table_id):
        raise ValueError("配置错误: features.alarm_export.feishu.app_token 与 table_id 必须同时填写或同时留空")

    for key in ("page_size", "delete_batch_size", "create_batch_size"):
        if int(feishu.get(key, 0) or 0) <= 0:
            raise ValueError(f"配置错误: features.alarm_export.feishu.{key} 必须大于0")
    _validate_feature_daily_scheduler(
        "features.alarm_export.scheduler",
        alarm_export.get("scheduler", {}),
    )


def _validate_feature_daily_scheduler(section_name: str, scheduler_cfg: Any) -> None:
    if not isinstance(scheduler_cfg, dict):
        raise ValueError(f"配置错误: {section_name} 缺失或格式错误")
    if not isinstance(scheduler_cfg.get("enabled", False), bool):
        raise ValueError(f"配置错误: {section_name}.enabled 必须是布尔值")
    if not isinstance(scheduler_cfg.get("auto_start_in_gui", False), bool):
        raise ValueError(f"配置错误: {section_name}.auto_start_in_gui 必须是布尔值")
    if not _valid_time(str(scheduler_cfg.get("run_time", ""))):
        raise ValueError(f"配置错误: {section_name}.run_time 必须是 HH:MM:SS")
    if int(scheduler_cfg.get("check_interval_sec", 0) or 0) <= 0:
        raise ValueError(f"配置错误: {section_name}.check_interval_sec 必须大于0")
    if not isinstance(scheduler_cfg.get("catch_up_if_missed", False), bool):
        raise ValueError(f"配置错误: {section_name}.catch_up_if_missed 必须是布尔值")
    if not isinstance(scheduler_cfg.get("retry_failed_in_same_period", True), bool):
        raise ValueError(f"配置错误: {section_name}.retry_failed_in_same_period 必须是布尔值")
    if not str(scheduler_cfg.get("state_file", "") or "").strip():
        raise ValueError(f"配置错误: {section_name}.state_file 不能为空")


def _validate_feature_interval_scheduler(section_name: str, scheduler_cfg: Any) -> None:
    if not isinstance(scheduler_cfg, dict):
        raise ValueError(f"配置错误: {section_name} 缺失或格式错误")
    if not isinstance(scheduler_cfg.get("enabled", False), bool):
        raise ValueError(f"配置错误: {section_name}.enabled 必须是布尔值")
    if not isinstance(scheduler_cfg.get("auto_start_in_gui", False), bool):
        raise ValueError(f"配置错误: {section_name}.auto_start_in_gui 必须是布尔值")
    if int(scheduler_cfg.get("interval_minutes", 0) or 0) <= 0:
        raise ValueError(f"配置错误: {section_name}.interval_minutes 必须大于0")
    if int(scheduler_cfg.get("check_interval_sec", 0) or 0) <= 0:
        raise ValueError(f"配置错误: {section_name}.check_interval_sec 必须大于0")
    if not isinstance(scheduler_cfg.get("retry_failed_on_next_tick", True), bool):
        raise ValueError(f"配置错误: {section_name}.retry_failed_on_next_tick 必须是布尔值")
    if not str(scheduler_cfg.get("state_file", "") or "").strip():
        raise ValueError(f"配置错误: {section_name}.state_file 不能为空")


def _validate_resume(cfg: Dict[str, Any]) -> None:
    resume_cfg = cfg.get("features", {}).get("monthly_report", {}).get("resume", {})
    if not isinstance(resume_cfg, dict):
        raise ValueError("配置错误: features.monthly_report.resume 缺失或格式错误")
    if int(resume_cfg.get("retention_days", 0)) <= 0:
        raise ValueError("配置错误: features.monthly_report.resume.retention_days 必须大于0")
    if int(resume_cfg.get("auto_continue_poll_sec", 0)) <= 0:
        raise ValueError("配置错误: features.monthly_report.resume.auto_continue_poll_sec 必须大于0")
    if int(resume_cfg.get("gc_every_n_items", 0)) <= 0:
        raise ValueError("配置错误: features.monthly_report.resume.gc_every_n_items 必须大于0")
    if not str(resume_cfg.get("root_dir", "")).strip():
        raise ValueError("配置错误: features.monthly_report.resume.root_dir 不能为空")
    if not str(resume_cfg.get("index_file", "")).strip():
        raise ValueError("配置错误: features.monthly_report.resume.index_file 不能为空")


def _validate_sheet_import(cfg: Dict[str, Any]) -> None:
    sheet_cfg = cfg.get("features", {}).get("sheet_import", {})
    if not isinstance(sheet_cfg, dict):
        return
    if "sheet_rules" in sheet_cfg:
        _normalize_sheet_rules_config(sheet_cfg["sheet_rules"])


def _validate_handover_scheduler(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    scheduler_cfg = handover.get("scheduler", {})
    if not isinstance(scheduler_cfg, dict):
        raise ValueError("配置错误: features.handover_log.scheduler 缺失或格式错误")
    if not _valid_time(str(scheduler_cfg.get("morning_time", ""))):
        raise ValueError("配置错误: features.handover_log.scheduler.morning_time 必须是 HH:MM:SS")
    if not _valid_time(str(scheduler_cfg.get("afternoon_time", ""))):
        raise ValueError("配置错误: features.handover_log.scheduler.afternoon_time 必须是 HH:MM:SS")
    if int(scheduler_cfg.get("check_interval_sec", 0)) <= 0:
        raise ValueError("配置错误: features.handover_log.scheduler.check_interval_sec 必须大于0")
    if not str(scheduler_cfg.get("morning_state_file", "")).strip():
        raise ValueError("配置错误: features.handover_log.scheduler.morning_state_file 不能为空")
    if not str(scheduler_cfg.get("afternoon_state_file", "")).strip():
        raise ValueError("配置错误: features.handover_log.scheduler.afternoon_state_file 不能为空")


def _validate_handover_template(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    template = handover.get("template", {})
    if not isinstance(template, dict):
        raise ValueError("配置错误: features.handover_log.template 缺失或格式错误")
    if not str(template.get("source_path", "")).strip():
        raise ValueError("配置错误: features.handover_log.template.source_path 不能为空")

    title_cell = str(template.get("title_cell", "A1")).strip().upper()
    if not title_cell:
        raise ValueError("配置错误: features.handover_log.template.title_cell 不能为空")
    if not re.fullmatch(r"[A-Z]+[1-9]\d*", title_cell):
        raise ValueError("配置错误: features.handover_log.template.title_cell 必须是合法单元格，例如 A1")

    title_map = template.get("building_title_map", {})
    if not isinstance(title_map, dict):
        raise ValueError("配置错误: features.handover_log.template.building_title_map 必须是对象")
    for key, value in title_map.items():
        building = str(key or "").strip()
        title = str(value or "").strip()
        if not building:
            raise ValueError("配置错误: features.handover_log.template.building_title_map 不能包含空楼栋键")
        if not title:
            raise ValueError(f"配置错误: features.handover_log.template.building_title_map[{building}] 不能为空")

    pattern = str(template.get("building_title_pattern", "")).strip()
    if bool(template.get("apply_building_title", True)) and not pattern and not title_map:
        raise ValueError(
            "配置错误: 启用楼栋标题注入时，building_title_pattern 与 building_title_map 不能同时为空"
        )


def _validate_handover_download(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    download = handover.get("download", {})
    if not isinstance(download, dict):
        raise ValueError("配置错误: features.handover_log.download 缺失或格式错误")

    required_text = ["template_name", "scale_label", "export_button_text"]
    for key in required_text:
        if not str(download.get(key, "")).strip():
            raise ValueError(f"配置错误: features.handover_log.download.{key} 不能为空")

    shift_windows = download.get("shift_windows", {})
    if not isinstance(shift_windows, dict):
        raise ValueError("配置错误: features.handover_log.download.shift_windows 必须是对象")
    for scope, time_key in (("day", "start"), ("day", "end"), ("night", "start"), ("night", "end_next_day")):
        window = shift_windows.get(scope, {})
        if not isinstance(window, dict):
            raise ValueError(f"配置错误: features.handover_log.download.shift_windows.{scope} 必须是对象")
        if not _valid_time(str(window.get(time_key, ""))):
            raise ValueError(
                f"配置错误: features.handover_log.download.shift_windows.{scope}.{time_key} 必须是 HH:MM:SS"
            )

    positive_keys = [
        "query_result_timeout_ms",
        "download_event_timeout_ms",
        "login_fill_timeout_ms",
        "menu_visible_timeout_ms",
        "iframe_timeout_ms",
        "start_end_visible_timeout_ms",
        "max_retries",
    ]
    for key in positive_keys:
        if int(download.get(key, 0)) <= 0:
            raise ValueError(f"配置错误: features.handover_log.download.{key} 必须大于0")

    non_negative_keys = [
        "lookback_minutes",
        "page_refresh_retry_count",
        "retry_wait_sec",
        "site_start_delay_sec",
    ]
    for key in non_negative_keys:
        if int(download.get(key, 0)) < 0:
            raise ValueError(f"配置错误: features.handover_log.download.{key} 必须大于等于0")


def _validate_handover_capacity_report_weather(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    capacity_report = handover.get("capacity_report", {})
    if not isinstance(capacity_report, dict):
        raise ValueError("配置错误: features.handover_log.capacity_report 缺失或格式错误")
    weather = capacity_report.get("weather", {})
    if not isinstance(weather, dict):
        raise ValueError("配置错误: features.handover_log.capacity_report.weather 缺失或格式错误")

    provider = str(weather.get("provider", "") or "").strip()
    location = str(weather.get("location", "") or "").strip()
    language = str(weather.get("language", "") or "").strip()
    unit = str(weather.get("unit", "") or "").strip()
    auth_mode = str(weather.get("auth_mode", "") or "").strip()
    timeout_sec = int(weather.get("timeout_sec", 0) or 0)
    public_key = str(weather.get("seniverse_public_key", "") or "").strip()
    private_key = str(weather.get("seniverse_private_key", "") or "").strip()
    fallback_locations = weather.get("fallback_locations", [])

    if provider and provider != "seniverse":
        raise ValueError("配置错误: features.handover_log.capacity_report.weather.provider 当前仅支持 seniverse")
    if not location:
        raise ValueError("配置错误: features.handover_log.capacity_report.weather.location 不能为空")
    if not language:
        raise ValueError("配置错误: features.handover_log.capacity_report.weather.language 不能为空")
    if not unit:
        raise ValueError("配置错误: features.handover_log.capacity_report.weather.unit 不能为空")
    if auth_mode and auth_mode != "signed":
        raise ValueError("配置错误: features.handover_log.capacity_report.weather.auth_mode 当前仅支持 signed")
    if timeout_sec <= 0:
        raise ValueError("配置错误: features.handover_log.capacity_report.weather.timeout_sec 必须大于0")
    if not public_key:
        raise ValueError("配置错误: features.handover_log.capacity_report.weather.seniverse_public_key 不能为空")
    if not private_key:
        raise ValueError("配置错误: features.handover_log.capacity_report.weather.seniverse_private_key 不能为空")
    if fallback_locations and (
        not isinstance(fallback_locations, list)
        or any(not str(item or "").strip() for item in fallback_locations)
    ):
        raise ValueError("配置错误: features.handover_log.capacity_report.weather.fallback_locations 必须是非空字符串数组")


def _validate_handover_shift_roster(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    roster = handover.get("shift_roster", {})
    if not isinstance(roster, dict):
        raise ValueError("配置错误: features.handover_log.shift_roster 缺失或格式错误")

    enabled = bool(roster.get("enabled", True))
    source = roster.get("source", {})
    fields = roster.get("fields", {})
    cells = roster.get("cells", {})
    match = roster.get("match", {})
    shift_alias = roster.get("shift_alias", {})

    if not isinstance(source, dict):
        raise ValueError("配置错误: features.handover_log.shift_roster.source 必须是对象")
    if not isinstance(fields, dict):
        raise ValueError("配置错误: features.handover_log.shift_roster.fields 必须是对象")
    if not isinstance(cells, dict):
        raise ValueError("配置错误: features.handover_log.shift_roster.cells 必须是对象")
    if not isinstance(match, dict):
        raise ValueError("配置错误: features.handover_log.shift_roster.match 必须是对象")
    if not isinstance(shift_alias, dict):
        raise ValueError("配置错误: features.handover_log.shift_roster.shift_alias 必须是对象")

    page_size = int(source.get("page_size", 500) or 0)
    max_records = int(source.get("max_records", 5000) or 0)
    if page_size <= 0:
        raise ValueError("配置错误: features.handover_log.shift_roster.source.page_size 必须大于0")
    if max_records <= 0:
        raise ValueError("配置错误: features.handover_log.shift_roster.source.max_records 必须大于0")

    if enabled:
        if not str(source.get("app_token", "")).strip():
            raise ValueError("配置错误: features.handover_log.shift_roster.source.app_token 不能为空")
        if not str(source.get("table_id", "")).strip():
            raise ValueError("配置错误: features.handover_log.shift_roster.source.table_id 不能为空")
        for key in ("duty_date", "building", "team", "shift", "people_text"):
            if not str(fields.get(key, "")).strip():
                raise ValueError(f"配置错误: features.handover_log.shift_roster.fields.{key} 不能为空")

        cell_pattern = re.compile(r"^[A-Z]+[1-9]\d*$")
        current_people_cell = str(cells.get("current_people", "")).strip().upper()
        next_people_cell = str(cells.get("next_people", "")).strip().upper()
        if not current_people_cell or not cell_pattern.fullmatch(current_people_cell):
            raise ValueError("配置错误: features.handover_log.shift_roster.cells.current_people 必须是合法单元格")
        if not next_people_cell or not cell_pattern.fullmatch(next_people_cell):
            raise ValueError("配置错误: features.handover_log.shift_roster.cells.next_people 必须是合法单元格")

        next_first_cells = cells.get("next_first_person_cells", [])
        if not isinstance(next_first_cells, list) or not next_first_cells:
            raise ValueError("配置错误: features.handover_log.shift_roster.cells.next_first_person_cells 不能为空")
        for idx, raw_cell in enumerate(next_first_cells, 1):
            cell = str(raw_cell or "").strip().upper()
            if not cell_pattern.fullmatch(cell):
                raise ValueError(
                    "配置错误: features.handover_log.shift_roster.cells.next_first_person_cells "
                    f"第{idx}项非法: {cell or raw_cell}"
                )

        building_mode = str(match.get("building_mode", "")).strip().lower()
        if building_mode != "exact_then_code":
            raise ValueError(
                "配置错误: features.handover_log.shift_roster.match.building_mode "
                "仅支持 exact_then_code"
            )

        day_alias = shift_alias.get("day", [])
        night_alias = shift_alias.get("night", [])
        if not isinstance(day_alias, list) or not [str(x).strip() for x in day_alias if str(x).strip()]:
            raise ValueError("配置错误: features.handover_log.shift_roster.shift_alias.day 不能为空")
        if not isinstance(night_alias, list) or not [str(x).strip() for x in night_alias if str(x).strip()]:
            raise ValueError("配置错误: features.handover_log.shift_roster.shift_alias.night 不能为空")

        split_regex = str(roster.get("people_split_regex", "")).strip()
        if not split_regex:
            raise ValueError("配置错误: features.handover_log.shift_roster.people_split_regex 不能为空")
        try:
            re.compile(split_regex)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(
                "配置错误: features.handover_log.shift_roster.people_split_regex 不是合法正则"
            ) from exc

        long_day = roster.get("long_day", {})
        if not isinstance(long_day, dict):
            raise ValueError("配置错误: features.handover_log.shift_roster.long_day 必须是对象")
        long_source = long_day.get("source", {})
        long_fields = long_day.get("fields", {})
        if not isinstance(long_source, dict):
            raise ValueError("配置错误: features.handover_log.shift_roster.long_day.source 必须是对象")
        if not isinstance(long_fields, dict):
            raise ValueError("配置错误: features.handover_log.shift_roster.long_day.fields 必须是对象")
        if bool(long_day.get("enabled", True)):
            if not str(long_source.get("table_id", "")).strip():
                raise ValueError("配置错误: features.handover_log.shift_roster.long_day.source.table_id 不能为空")
            for key in ("duty_date", "building", "people_text"):
                if not str(long_fields.get(key, "")).strip():
                    raise ValueError(f"配置错误: features.handover_log.shift_roster.long_day.fields.{key} 不能为空")
            for key in ("day_cell", "night_cell"):
                cell = str(long_day.get(key, "")).strip().upper()
                if not cell_pattern.fullmatch(cell):
                    raise ValueError(f"配置错误: features.handover_log.shift_roster.long_day.{key} 必须是合法单元格")

        engineer_dir = roster.get("engineer_directory", {})
        if not isinstance(engineer_dir, dict):
            raise ValueError("配置错误: features.handover_log.shift_roster.engineer_directory 必须是对象")
        eng_source = engineer_dir.get("source", {})
        eng_fields = engineer_dir.get("fields", {})
        eng_delivery = engineer_dir.get("delivery", {})
        if not isinstance(eng_source, dict):
            raise ValueError("配置错误: features.handover_log.shift_roster.engineer_directory.source 必须是对象")
        if not isinstance(eng_fields, dict):
            raise ValueError("配置错误: features.handover_log.shift_roster.engineer_directory.fields 必须是对象")
        if not isinstance(eng_delivery, dict):
            raise ValueError("配置错误: features.handover_log.shift_roster.engineer_directory.delivery 必须是对象")
        if bool(engineer_dir.get("enabled", True)):
            if not str(eng_source.get("table_id", "")).strip():
                raise ValueError("配置错误: features.handover_log.shift_roster.engineer_directory.source.table_id 不能为空")
            for key in ("building", "specialty", "supervisor_text", "position"):
                if not str(eng_fields.get(key, "")).strip():
                    raise ValueError(
                        f"配置错误: features.handover_log.shift_roster.engineer_directory.fields.{key} 不能为空"
                    )
            for key in ("receive_id_type", "position_keyword"):
                if not str(eng_delivery.get(key, "")).strip():
                    raise ValueError(
                        f"配置错误: features.handover_log.shift_roster.engineer_directory.delivery.{key} 不能为空"
                    )


def _validate_handover_event_sections(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    event_sections = handover.get("event_sections", {})
    if not isinstance(event_sections, dict):
        raise ValueError("配置错误: features.handover_log.event_sections 缺失或格式错误")

    source = event_sections.get("source", {})
    duty_window = event_sections.get("duty_window", {})
    fields = event_sections.get("fields", {})
    sections = event_sections.get("sections", {})
    column_mapping = event_sections.get("column_mapping", {})
    progress_text = event_sections.get("progress_text", {})
    cache = event_sections.get("cache", {})
    if not isinstance(source, dict):
        raise ValueError("配置错误: features.handover_log.event_sections.source 必须是对象")
    if not isinstance(duty_window, dict):
        raise ValueError("配置错误: features.handover_log.event_sections.duty_window 必须是对象")
    if not isinstance(fields, dict):
        raise ValueError("配置错误: features.handover_log.event_sections.fields 必须是对象")
    if not isinstance(sections, dict):
        raise ValueError("配置错误: features.handover_log.event_sections.sections 必须是对象")
    if not isinstance(column_mapping, dict):
        raise ValueError("配置错误: features.handover_log.event_sections.column_mapping 必须是对象")
    if not isinstance(progress_text, dict):
        raise ValueError("配置错误: features.handover_log.event_sections.progress_text 必须是对象")
    if not isinstance(cache, dict):
        raise ValueError("配置错误: features.handover_log.event_sections.cache 必须是对象")

    if bool(event_sections.get("enabled", True)):
        if not str(source.get("app_token", "")).strip():
            raise ValueError("配置错误: features.handover_log.event_sections.source.app_token 不能为空")
        if not str(source.get("table_id", "")).strip():
            raise ValueError("配置错误: features.handover_log.event_sections.source.table_id 不能为空")
        if int(source.get("page_size", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.event_sections.source.page_size 必须大于0")
        if int(source.get("max_records", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.event_sections.source.max_records 必须大于0")

        for key in ("day_start", "day_end", "night_start", "night_end_next_day"):
            if not _valid_time(str(duty_window.get(key, ""))):
                raise ValueError(f"配置错误: features.handover_log.event_sections.duty_window.{key} 必须是 HH:MM:SS")
        boundary_mode = str(duty_window.get("boundary_mode", "")).strip().lower()
        if boundary_mode not in {"left_closed_right_open"}:
            raise ValueError(
                "配置错误: features.handover_log.event_sections.duty_window.boundary_mode "
                "仅支持 left_closed_right_open"
            )

        for key in (
            "event_time",
            "building",
            "event_level",
            "description",
            "exclude_checked",
            "final_status",
            "to_maint",
            "maint_done_time",
            "event_done_time",
        ):
            if not str(fields.get(key, "")).strip():
                raise ValueError(f"配置错误: features.handover_log.event_sections.fields.{key} 不能为空")

        for key in ("new_event", "history_followup"):
            if not str(sections.get(key, "")).strip():
                raise ValueError(f"配置错误: features.handover_log.event_sections.sections.{key} 不能为空")

        header_alias = column_mapping.get("header_alias", {})
        fallback_cols = column_mapping.get("fallback_cols", {})
        if not isinstance(header_alias, dict):
            raise ValueError("配置错误: features.handover_log.event_sections.column_mapping.header_alias 必须是对象")
        if not isinstance(fallback_cols, dict):
            raise ValueError("配置错误: features.handover_log.event_sections.column_mapping.fallback_cols 必须是对象")
        for key in ("event_level", "event_time", "description", "work_window", "progress", "follower"):
            aliases = header_alias.get(key, [])
            if not isinstance(aliases, list) or not [str(x).strip() for x in aliases if str(x).strip()]:
                raise ValueError(
                    f"配置错误: features.handover_log.event_sections.column_mapping.header_alias.{key} 不能为空"
                )
            col = str(fallback_cols.get(key, "")).strip().upper()
            if not re.fullmatch(r"[A-Z]+", col):
                raise ValueError(
                    f"配置错误: features.handover_log.event_sections.column_mapping.fallback_cols.{key} 非法"
                )

        if not str(progress_text.get("done", "")).strip():
            raise ValueError("配置错误: features.handover_log.event_sections.progress_text.done 不能为空")
        if not str(progress_text.get("todo", "")).strip():
            raise ValueError("配置错误: features.handover_log.event_sections.progress_text.todo 不能为空")

        if not str(cache.get("state_file", "")).strip():
            raise ValueError("配置错误: features.handover_log.event_sections.cache.state_file 不能为空")
        if int(cache.get("max_pending", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.event_sections.cache.max_pending 必须大于0")
        if int(cache.get("max_last_query_ids", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.event_sections.cache.max_last_query_ids 必须大于0")


def _validate_handover_monthly_event_report(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    monthly_cfg = handover.get("monthly_event_report", {})
    if not isinstance(monthly_cfg, dict):
        raise ValueError("配置错误: features.handover_log.monthly_event_report 缺失或格式错误")

    template = monthly_cfg.get("template", {})
    scheduler = monthly_cfg.get("scheduler", {})
    test_delivery = monthly_cfg.get("test_delivery", {})
    if not isinstance(template, dict):
        raise ValueError("配置错误: features.handover_log.monthly_event_report.template 必须是对象")
    if not isinstance(scheduler, dict):
        raise ValueError("配置错误: features.handover_log.monthly_event_report.scheduler 必须是对象")
    if not isinstance(test_delivery, dict):
        raise ValueError("配置错误: features.handover_log.monthly_event_report.test_delivery 必须是对象")

    if bool(monthly_cfg.get("enabled", True)):
        if not str(template.get("source_path", "")).strip():
            raise ValueError("配置错误: features.handover_log.monthly_event_report.template.source_path 不能为空")
        if not str(template.get("output_dir", "")).strip():
            raise ValueError("配置错误: features.handover_log.monthly_event_report.template.output_dir 不能为空")
        if not str(template.get("file_name_pattern", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.monthly_event_report.template.file_name_pattern 不能为空"
            )
        day_of_month = int(scheduler.get("day_of_month", 0) or 0)
        if day_of_month < 1 or day_of_month > 31:
            raise ValueError(
                "配置错误: features.handover_log.monthly_event_report.scheduler.day_of_month 必须在1到31之间"
            )
        if not _valid_time(str(scheduler.get("run_time", ""))):
            raise ValueError(
                "配置错误: features.handover_log.monthly_event_report.scheduler.run_time 必须是 HH:MM:SS"
            )
        if int(scheduler.get("check_interval_sec", 0)) <= 0:
            raise ValueError(
                "配置错误: features.handover_log.monthly_event_report.scheduler.check_interval_sec 必须大于0"
            )
        if not str(scheduler.get("state_file", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.monthly_event_report.scheduler.state_file 不能为空"
            )
        if not str(test_delivery.get("receive_id_type", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.monthly_event_report.test_delivery.receive_id_type 不能为空"
            )
        receive_ids = test_delivery.get("receive_ids", [])
        if not isinstance(receive_ids, list):
            raise ValueError(
                "配置错误: features.handover_log.monthly_event_report.test_delivery.receive_ids 必须是数组"
            )


def _validate_handover_monthly_change_report(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    monthly_cfg = handover.get("monthly_change_report", {})
    if not isinstance(monthly_cfg, dict):
        raise ValueError("配置错误: features.handover_log.monthly_change_report 缺失或格式错误")

    template = monthly_cfg.get("template", {})
    scheduler = monthly_cfg.get("scheduler", {})
    if not isinstance(template, dict):
        raise ValueError("配置错误: features.handover_log.monthly_change_report.template 必须是对象")
    if not isinstance(scheduler, dict):
        raise ValueError("配置错误: features.handover_log.monthly_change_report.scheduler 必须是对象")

    if bool(monthly_cfg.get("enabled", True)):
        if not str(template.get("source_path", "")).strip():
            raise ValueError("配置错误: features.handover_log.monthly_change_report.template.source_path 不能为空")
        if not str(template.get("output_dir", "")).strip():
            raise ValueError("配置错误: features.handover_log.monthly_change_report.template.output_dir 不能为空")
        if not str(template.get("file_name_pattern", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.monthly_change_report.template.file_name_pattern 不能为空"
            )
        day_of_month = int(scheduler.get("day_of_month", 0) or 0)
        if day_of_month < 1 or day_of_month > 31:
            raise ValueError(
                "配置错误: features.handover_log.monthly_change_report.scheduler.day_of_month 必须在1到31之间"
            )
        if not _valid_time(str(scheduler.get("run_time", ""))):
            raise ValueError(
                "配置错误: features.handover_log.monthly_change_report.scheduler.run_time 必须是 HH:MM:SS"
            )
        if int(scheduler.get("check_interval_sec", 0)) <= 0:
            raise ValueError(
                "配置错误: features.handover_log.monthly_change_report.scheduler.check_interval_sec 必须大于0"
            )
        if not str(scheduler.get("state_file", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.monthly_change_report.scheduler.state_file 不能为空"
            )


def _validate_handover_change_management_section(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    section_cfg = handover.get("change_management_section", {})
    if not isinstance(section_cfg, dict):
        raise ValueError("配置错误: features.handover_log.change_management_section 缺失或格式错误")

    source = section_cfg.get("source", {})
    fields = section_cfg.get("fields", {})
    monthly_report_fields = section_cfg.get("monthly_report_fields", {})
    sections = section_cfg.get("sections", {})
    column_mapping = section_cfg.get("column_mapping", {})
    work_window_text = section_cfg.get("work_window_text", {})
    if not isinstance(source, dict):
        raise ValueError("配置错误: features.handover_log.change_management_section.source 必须是对象")
    if not isinstance(fields, dict):
        raise ValueError("配置错误: features.handover_log.change_management_section.fields 必须是对象")
    if not isinstance(monthly_report_fields, dict):
        raise ValueError("配置错误: features.handover_log.change_management_section.monthly_report_fields 必须是对象")
    if not isinstance(sections, dict):
        raise ValueError("配置错误: features.handover_log.change_management_section.sections 必须是对象")
    if not isinstance(column_mapping, dict):
        raise ValueError("配置错误: features.handover_log.change_management_section.column_mapping 必须是对象")
    if not isinstance(work_window_text, dict):
        raise ValueError("配置错误: features.handover_log.change_management_section.work_window_text 必须是对象")

    enabled = bool(section_cfg.get("enabled", True))
    if enabled:
        if not str(source.get("app_token", "")).strip():
            raise ValueError("配置错误: features.handover_log.change_management_section.source.app_token 不能为空")
        if not str(source.get("table_id", "")).strip():
            raise ValueError("配置错误: features.handover_log.change_management_section.source.table_id 不能为空")
        if int(source.get("page_size", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.change_management_section.source.page_size 必须大于0")
        if int(source.get("max_records", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.change_management_section.source.max_records 必须大于0")

        for key in ("building", "start_time", "end_time", "updated_time", "change_level", "process_updates", "description", "specialty"):
            if not str(fields.get(key, "")).strip():
                raise ValueError(
                    f"配置错误: features.handover_log.change_management_section.fields.{key} 不能为空"
                )
        for key in ("building", "change_code", "name", "location", "change_level", "status", "start_time", "end_time"):
            if not str(monthly_report_fields.get(key, "")).strip():
                raise ValueError(
                    f"配置错误: features.handover_log.change_management_section.monthly_report_fields.{key} 不能为空"
                )

        if not str(sections.get("change_management", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.change_management_section.sections.change_management 不能为空"
            )

        header_alias = column_mapping.get("header_alias", {})
        fallback_cols = column_mapping.get("fallback_cols", {})
        if not isinstance(header_alias, dict):
            raise ValueError(
                "配置错误: features.handover_log.change_management_section.column_mapping.header_alias 必须是对象"
            )
        if not isinstance(fallback_cols, dict):
            raise ValueError(
                "配置错误: features.handover_log.change_management_section.column_mapping.fallback_cols 必须是对象"
            )
        for key in ("change_level", "work_window", "description", "executor"):
            aliases = header_alias.get(key, [])
            if not isinstance(aliases, list) or not [str(x).strip() for x in aliases if str(x).strip()]:
                raise ValueError(
                    "配置错误: features.handover_log.change_management_section."
                    f"column_mapping.header_alias.{key} 不能为空"
                )
            col = str(fallback_cols.get(key, "")).strip().upper()
            if not re.fullmatch(r"[A-Z]+", col):
                raise ValueError(
                    "配置错误: features.handover_log.change_management_section."
                    f"column_mapping.fallback_cols.{key} 非法"
                )

        for key in ("day_anchor", "day_default_end", "night_anchor", "night_default_end_next_day"):
            if not _valid_time(str(work_window_text.get(key, ""))):
                raise ValueError(
                    "配置错误: features.handover_log.change_management_section."
                    f"work_window_text.{key} 必须是 HH:MM:SS"
                )


def _validate_handover_exercise_management_section(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    section_cfg = handover.get("exercise_management_section", {})
    if not isinstance(section_cfg, dict):
        raise ValueError("配置错误: features.handover_log.exercise_management_section 缺失或格式错误")

    source = section_cfg.get("source", {})
    fields = section_cfg.get("fields", {})
    sections = section_cfg.get("sections", {})
    fixed_values = section_cfg.get("fixed_values", {})
    column_mapping = section_cfg.get("column_mapping", {})
    if not isinstance(source, dict):
        raise ValueError("配置错误: features.handover_log.exercise_management_section.source 必须是对象")
    if not isinstance(fields, dict):
        raise ValueError("配置错误: features.handover_log.exercise_management_section.fields 必须是对象")
    if not isinstance(sections, dict):
        raise ValueError("配置错误: features.handover_log.exercise_management_section.sections 必须是对象")
    if not isinstance(fixed_values, dict):
        raise ValueError("配置错误: features.handover_log.exercise_management_section.fixed_values 必须是对象")
    if not isinstance(column_mapping, dict):
        raise ValueError("配置错误: features.handover_log.exercise_management_section.column_mapping 必须是对象")

    enabled = bool(section_cfg.get("enabled", True))
    if enabled:
        if not str(source.get("app_token", "")).strip():
            raise ValueError("配置错误: features.handover_log.exercise_management_section.source.app_token 不能为空")
        if not str(source.get("table_id", "")).strip():
            raise ValueError("配置错误: features.handover_log.exercise_management_section.source.table_id 不能为空")
        if int(source.get("page_size", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.exercise_management_section.source.page_size 必须大于0")
        if int(source.get("max_records", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.exercise_management_section.source.max_records 必须大于0")

        for key in ("building", "start_time", "project"):
            if not str(fields.get(key, "")).strip():
                raise ValueError(
                    f"配置错误: features.handover_log.exercise_management_section.fields.{key} 不能为空"
                )

        if not str(sections.get("exercise_management", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.exercise_management_section.sections.exercise_management 不能为空"
            )

        if not str(fixed_values.get("exercise_type", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.exercise_management_section.fixed_values.exercise_type 不能为空"
            )
        if not str(fixed_values.get("completion", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.exercise_management_section.fixed_values.completion 不能为空"
            )

        header_alias = column_mapping.get("header_alias", {})
        fallback_cols = column_mapping.get("fallback_cols", {})
        if not isinstance(header_alias, dict):
            raise ValueError(
                "配置错误: features.handover_log.exercise_management_section.column_mapping.header_alias 必须是对象"
            )
        if not isinstance(fallback_cols, dict):
            raise ValueError(
                "配置错误: features.handover_log.exercise_management_section.column_mapping.fallback_cols 必须是对象"
            )
        for key in ("exercise_type", "exercise_item", "completion", "executor"):
            aliases = header_alias.get(key, [])
            if not isinstance(aliases, list) or not [str(x).strip() for x in aliases if str(x).strip()]:
                raise ValueError(
                    "配置错误: features.handover_log.exercise_management_section."
                    f"column_mapping.header_alias.{key} 不能为空"
                )
            col = str(fallback_cols.get(key, "")).strip().upper()
            if not re.fullmatch(r"[A-Z]+", col):
                raise ValueError(
                    "配置错误: features.handover_log.exercise_management_section."
                    f"column_mapping.fallback_cols.{key} 非法"
                )


def _validate_handover_maintenance_management_section(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    section_cfg = handover.get("maintenance_management_section", {})
    if not isinstance(section_cfg, dict):
        raise ValueError("配置错误: features.handover_log.maintenance_management_section 缺失或格式错误")

    source = section_cfg.get("source", {})
    fields = section_cfg.get("fields", {})
    sections = section_cfg.get("sections", {})
    fixed_values = section_cfg.get("fixed_values", {})
    column_mapping = section_cfg.get("column_mapping", {})
    if not isinstance(source, dict):
        raise ValueError("配置错误: features.handover_log.maintenance_management_section.source 必须是对象")
    if not isinstance(fields, dict):
        raise ValueError("配置错误: features.handover_log.maintenance_management_section.fields 必须是对象")
    if not isinstance(sections, dict):
        raise ValueError("配置错误: features.handover_log.maintenance_management_section.sections 必须是对象")
    if not isinstance(fixed_values, dict):
        raise ValueError("配置错误: features.handover_log.maintenance_management_section.fixed_values 必须是对象")
    if not isinstance(column_mapping, dict):
        raise ValueError("配置错误: features.handover_log.maintenance_management_section.column_mapping 必须是对象")

    enabled = bool(section_cfg.get("enabled", True))
    if enabled:
        if not str(source.get("app_token", "")).strip():
            raise ValueError("配置错误: features.handover_log.maintenance_management_section.source.app_token 不能为空")
        if not str(source.get("table_id", "")).strip():
            raise ValueError("配置错误: features.handover_log.maintenance_management_section.source.table_id 不能为空")
        if int(source.get("page_size", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.maintenance_management_section.source.page_size 必须大于0")
        if int(source.get("max_records", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.maintenance_management_section.source.max_records 必须大于0")

        for key in ("building", "start_time", "updated_time", "actual_end_time", "item", "specialty"):
            if not str(fields.get(key, "")).strip():
                raise ValueError(
                    f"配置错误: features.handover_log.maintenance_management_section.fields.{key} 不能为空"
                )

        if not str(sections.get("maintenance_management", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.maintenance_management_section.sections.maintenance_management 不能为空"
            )

        for key in ("vendor_internal", "vendor_external", "completion"):
            if not str(fixed_values.get(key, "")).strip():
                raise ValueError(
                    "配置错误: features.handover_log.maintenance_management_section."
                    f"fixed_values.{key} 不能为空"
                )

        header_alias = column_mapping.get("header_alias", {})
        fallback_cols = column_mapping.get("fallback_cols", {})
        if not isinstance(header_alias, dict):
            raise ValueError(
                "配置错误: features.handover_log.maintenance_management_section.column_mapping.header_alias 必须是对象"
            )
        if not isinstance(fallback_cols, dict):
            raise ValueError(
                "配置错误: features.handover_log.maintenance_management_section.column_mapping.fallback_cols 必须是对象"
            )
        for key in ("maintenance_item", "maintenance_party", "completion", "executor"):
            aliases = header_alias.get(key, [])
            if not isinstance(aliases, list) or not [str(x).strip() for x in aliases if str(x).strip()]:
                raise ValueError(
                    "配置错误: features.handover_log.maintenance_management_section."
                    f"column_mapping.header_alias.{key} 不能为空"
                )
            col = str(fallback_cols.get(key, "")).strip().upper()
            if not re.fullmatch(r"[A-Z]+", col):
                raise ValueError(
                    "配置错误: features.handover_log.maintenance_management_section."
                    f"column_mapping.fallback_cols.{key} 非法"
                )


def _validate_handover_other_important_work_section(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    section_cfg = handover.get("other_important_work_section", {})
    if not isinstance(section_cfg, dict):
        raise ValueError("配置错误: features.handover_log.other_important_work_section 缺失或格式错误")

    source = section_cfg.get("source", {})
    sections = section_cfg.get("sections", {})
    order = section_cfg.get("order", [])
    column_mapping = section_cfg.get("column_mapping", {})
    sources_cfg = section_cfg.get("sources", {})
    if not isinstance(source, dict):
        raise ValueError("配置错误: features.handover_log.other_important_work_section.source 必须是对象")
    if not isinstance(sections, dict):
        raise ValueError("配置错误: features.handover_log.other_important_work_section.sections 必须是对象")
    if not isinstance(order, list):
        raise ValueError("配置错误: features.handover_log.other_important_work_section.order 必须是数组")
    if not isinstance(column_mapping, dict):
        raise ValueError("配置错误: features.handover_log.other_important_work_section.column_mapping 必须是对象")
    if not isinstance(sources_cfg, dict):
        raise ValueError("配置错误: features.handover_log.other_important_work_section.sources 必须是对象")

    enabled = bool(section_cfg.get("enabled", True))
    if enabled:
        if not str(source.get("app_token", "")).strip():
            raise ValueError("配置错误: features.handover_log.other_important_work_section.source.app_token 不能为空")
        if int(source.get("page_size", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.other_important_work_section.source.page_size 必须大于0")
        if int(source.get("max_records", 0)) <= 0:
            raise ValueError("配置错误: features.handover_log.other_important_work_section.source.max_records 必须大于0")
        if not str(sections.get("other_important_work", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.other_important_work_section.sections.other_important_work 不能为空"
            )

        valid_source_keys = {"power_notice", "device_adjustment", "device_patrol", "device_repair"}
        cleaned_order = [str(item or "").strip() for item in order if str(item or "").strip()]
        if cleaned_order != ["power_notice", "device_adjustment", "device_patrol", "device_repair"]:
            raise ValueError(
                "配置错误: features.handover_log.other_important_work_section.order 必须按 "
                "power_notice/device_adjustment/device_patrol/device_repair 配置"
            )

        header_alias = column_mapping.get("header_alias", {})
        fallback_cols = column_mapping.get("fallback_cols", {})
        if not isinstance(header_alias, dict):
            raise ValueError(
                "配置错误: features.handover_log.other_important_work_section.column_mapping.header_alias 必须是对象"
            )
        if not isinstance(fallback_cols, dict):
            raise ValueError(
                "配置错误: features.handover_log.other_important_work_section.column_mapping.fallback_cols 必须是对象"
            )
        for key in ("description", "completion", "executor"):
            aliases = header_alias.get(key, [])
            if not isinstance(aliases, list) or not [str(x).strip() for x in aliases if str(x).strip()]:
                raise ValueError(
                    "配置错误: features.handover_log.other_important_work_section."
                    f"column_mapping.header_alias.{key} 不能为空"
                )
            col = str(fallback_cols.get(key, "")).strip().upper()
            if not re.fullmatch(r"[A-Z]+", col):
                raise ValueError(
                    "配置错误: features.handover_log.other_important_work_section."
                    f"column_mapping.fallback_cols.{key} 非法"
                )

        for source_key in valid_source_keys:
            current = sources_cfg.get(source_key, {})
            if not isinstance(current, dict):
                raise ValueError(
                    f"配置错误: features.handover_log.other_important_work_section.sources.{source_key} 必须是对象"
                )
            if not str(current.get("table_id", "")).strip():
                raise ValueError(
                    "配置错误: features.handover_log.other_important_work_section."
                    f"sources.{source_key}.table_id 不能为空"
                )
            fields = current.get("fields", {})
            if not isinstance(fields, dict):
                raise ValueError(
                    "配置错误: features.handover_log.other_important_work_section."
                    f"sources.{source_key}.fields 必须是对象"
                )
            for field_key in ("building", "actual_start_time", "actual_end_time", "description", "completion", "specialty"):
                if not str(fields.get(field_key, "")).strip():
                    raise ValueError(
                        "配置错误: features.handover_log.other_important_work_section."
                        f"sources.{source_key}.fields.{field_key} 不能为空"
                    )


def _validate_day_metric_upload(cfg: Dict[str, Any]) -> None:
    upload_cfg = cfg.get("features", {}).get("day_metric_upload", {})
    if not isinstance(upload_cfg, dict):
        raise ValueError("配置错误: features.day_metric_upload 缺失或格式错误")

    behavior = upload_cfg.get("behavior", {})
    target = upload_cfg.get("target", {})
    if not isinstance(behavior, dict):
        raise ValueError("配置错误: features.day_metric_upload.behavior 必须是对象")
    if not isinstance(target, dict):
        raise ValueError("配置错误: features.day_metric_upload.target 必须是对象")

    for key in (
        "basic_retry_attempts",
        "basic_retry_backoff_sec",
        "network_retry_attempts",
        "network_retry_backoff_sec",
        "alert_after_attempts",
    ):
        try:
            if int(behavior.get(key, 0)) < 0:
                raise ValueError
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"配置错误: features.day_metric_upload.behavior.{key} 必须大于等于0") from exc

    source = target.get("source", {})
    fields = target.get("fields", {})
    if not isinstance(source, dict):
        raise ValueError("配置错误: features.day_metric_upload.target.source 必须是对象")
    if not isinstance(fields, dict):
        raise ValueError("配置错误: features.day_metric_upload.target.fields 必须是对象")

    missing_policy = str(target.get("missing_value_policy", "zero")).strip().lower()
    if missing_policy != "zero":
        raise ValueError("配置错误: features.day_metric_upload.target.missing_value_policy 仅支持 zero")

    if not str(source.get("app_token", "")).strip():
        raise ValueError("配置错误: features.day_metric_upload.target.source.app_token 不能为空")
    if not str(source.get("table_id", "")).strip():
        raise ValueError("配置错误: features.day_metric_upload.target.source.table_id 不能为空")
    if int(source.get("create_batch_size", 0)) <= 0:
        raise ValueError("配置错误: features.day_metric_upload.target.source.create_batch_size 必须大于0")
    for key in ("type", "building", "date", "value", "position_code"):
        if not str(fields.get(key, "")).strip():
            raise ValueError(f"配置错误: features.day_metric_upload.target.fields.{key} 不能为空")
    _validate_feature_interval_scheduler(
        "features.day_metric_upload.scheduler",
        upload_cfg.get("scheduler", {}),
    )


def _validate_handover_source_data_attachment_export(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    export_cfg = handover.get("source_data_attachment_export", {})
    if not isinstance(export_cfg, dict):
        raise ValueError("配置错误: features.handover_log.source_data_attachment_export 缺失或格式错误")

    source = export_cfg.get("source", {})
    fields = export_cfg.get("fields", {})
    fixed_values = export_cfg.get("fixed_values", {})
    shift_text = fixed_values.get("shift_text", {}) if isinstance(fixed_values, dict) else {}
    if not isinstance(source, dict):
        raise ValueError("配置错误: features.handover_log.source_data_attachment_export.source 必须是对象")
    if not isinstance(fields, dict):
        raise ValueError("配置错误: features.handover_log.source_data_attachment_export.fields 必须是对象")
    if not isinstance(fixed_values, dict):
        raise ValueError("配置错误: features.handover_log.source_data_attachment_export.fixed_values 必须是对象")
    if not isinstance(shift_text, dict):
        raise ValueError(
            "配置错误: features.handover_log.source_data_attachment_export.fixed_values.shift_text 必须是对象"
        )

    enabled = bool(export_cfg.get("enabled", True))
    if enabled:
        if not str(source.get("app_token", "")).strip():
            raise ValueError("配置错误: features.handover_log.source_data_attachment_export.source.app_token 不能为空")
        if not str(source.get("table_id", "")).strip():
            raise ValueError("配置错误: features.handover_log.source_data_attachment_export.source.table_id 不能为空")
        for key in ("page_size", "max_records", "delete_batch_size"):
            if int(source.get(key, 0)) <= 0:
                raise ValueError(
                    f"配置错误: features.handover_log.source_data_attachment_export.source.{key} 必须大于0"
                )
        for key in ("type", "building", "date", "shift", "attachment"):
            if not str(fields.get(key, "")).strip():
                raise ValueError(
                    f"配置错误: features.handover_log.source_data_attachment_export.fields.{key} 不能为空"
                )
        if not str(fixed_values.get("type", "")).strip():
            raise ValueError("配置错误: features.handover_log.source_data_attachment_export.fixed_values.type 不能为空")
        if not str(shift_text.get("day", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.source_data_attachment_export.fixed_values.shift_text.day 不能为空"
            )
        if not str(shift_text.get("night", "")).strip():
            raise ValueError(
                "配置错误: features.handover_log.source_data_attachment_export.fixed_values.shift_text.night 不能为空"
            )


def _validate_handover_cloud_sheet_sync(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    sync_cfg = handover.get("cloud_sheet_sync", {})
    if not isinstance(sync_cfg, dict):
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync 缺失或格式错误")

    if not isinstance(sync_cfg.get("enabled", True), bool):
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.enabled 必须是布尔值")

    root_wiki_url = str(sync_cfg.get("root_wiki_url", "") or "").strip()
    if not root_wiki_url:
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.root_wiki_url 不能为空")
    if not re.search(r"/wiki/([^/?#]+)", root_wiki_url, re.IGNORECASE):
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.root_wiki_url 无法解析 wiki token")

    if not str(sync_cfg.get("template_node_token", "") or "").strip():
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.template_node_token 不能为空")
    if not str(sync_cfg.get("source_sheet_name", "") or "").strip():
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.source_sheet_name 不能为空")

    sync_mode = str(sync_cfg.get("sync_mode", "overwrite_named_sheet") or "").strip().lower()
    if sync_mode == "rebuild_sheet":
        sync_mode = "overwrite_named_sheet"
    if sync_mode != "overwrite_named_sheet":
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.sync_mode 仅支持 overwrite_named_sheet")

    sheet_names = sync_cfg.get("sheet_names", {})
    if not isinstance(sheet_names, dict):
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.sheet_names 必须是对象")
    for building in ("A楼", "B楼", "C楼", "D楼", "E楼"):
        if not str(sheet_names.get(building, "") or "").strip():
            raise ValueError(f"配置错误: features.handover_log.cloud_sheet_sync.sheet_names.{building} 不能为空")

    request_cfg = sync_cfg.get("request", {})
    if not isinstance(request_cfg, dict):
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.request 必须是对象")
    if int(request_cfg.get("timeout_sec", 0)) <= 0:
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.request.timeout_sec 必须大于0")
    if int(request_cfg.get("max_retries", -1)) < 0:
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.request.max_retries 必须大于等于0")
    if float(request_cfg.get("retry_backoff_sec", -1)) < 0:
        raise ValueError("配置错误: features.handover_log.cloud_sheet_sync.request.retry_backoff_sec 必须大于等于0")


def _validate_handover_daily_report_bitable_export(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    export_cfg = handover.get("daily_report_bitable_export", {})
    if not isinstance(export_cfg, dict):
        raise ValueError("配置错误: features.handover_log.daily_report_bitable_export 缺失或格式错误")

    if not isinstance(export_cfg.get("enabled", True), bool):
        raise ValueError("配置错误: features.handover_log.daily_report_bitable_export.enabled 必须是布尔值")

    target = export_cfg.get("target", {})
    if not isinstance(target, dict):
        raise ValueError("配置错误: features.handover_log.daily_report_bitable_export.target 必须是对象")
    if not str(target.get("app_token", "") or "").strip():
        raise ValueError("配置错误: features.handover_log.daily_report_bitable_export.target.app_token 不能为空")
    if not str(target.get("table_id", "") or "").strip():
        raise ValueError("配置错误: features.handover_log.daily_report_bitable_export.target.table_id 不能为空")
    for key in ("page_size", "max_records", "delete_batch_size"):
        if int(target.get(key, 0) or 0) <= 0:
            raise ValueError(f"配置错误: features.handover_log.daily_report_bitable_export.target.{key} 必须大于0")

    fields = export_cfg.get("fields", {})
    if not isinstance(fields, dict):
        raise ValueError("配置错误: features.handover_log.daily_report_bitable_export.fields 必须是对象")
    for key in ("year", "date", "shift", "report_link", "screenshots"):
        if not str(fields.get(key, "") or "").strip():
            raise ValueError(f"配置错误: features.handover_log.daily_report_bitable_export.fields.{key} 不能为空")

    if not str(export_cfg.get("summary_page_url", "") or "").strip():
        raise ValueError("配置错误: features.handover_log.daily_report_bitable_export.summary_page_url 不能为空")
    if not str(export_cfg.get("external_page_url", "") or "").strip():
        raise ValueError("配置错误: features.handover_log.daily_report_bitable_export.external_page_url 不能为空")


def _validate_handover_review_ui(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    review_ui = handover.get("review_ui", {})
    if not isinstance(review_ui, dict):
        raise ValueError("配置错误: features.handover_log.review_ui 缺失或格式错误")

    buildings = review_ui.get("buildings", [])
    fixed_cells = review_ui.get("fixed_cells", {})
    # Deprecated compatibility field. The review UI now uses explicit saves,
    # so we keep accepting the value without making it a required runtime knob.
    autosave_debounce_ms = int(review_ui.get("autosave_debounce_ms", 0) or 0)
    poll_interval_sec = int(review_ui.get("poll_interval_sec", 0) or 0)
    hidden_columns = review_ui.get("section_hidden_columns", [])
    public_base_url = str(review_ui.get("public_base_url", "") or "").strip()
    cabinet_power_defaults = review_ui.get("cabinet_power_defaults_by_building", {})
    footer_inventory_defaults = review_ui.get("footer_inventory_defaults_by_building", {})
    review_link_recipients = review_ui.get("review_link_recipients_by_building", {})

    if not isinstance(buildings, list) or len(buildings) != 5:
        raise ValueError("配置错误: features.handover_log.review_ui.buildings 必须包含5个楼栋")
    seen_codes: set[str] = set()
    for idx, item in enumerate(buildings, 1):
        if not isinstance(item, dict):
            raise ValueError(f"配置错误: features.handover_log.review_ui.buildings 第{idx}项必须是对象")
        code = str(item.get("code", "")).strip().lower()
        name = str(item.get("name", "")).strip()
        if not code or not name:
            raise ValueError(f"配置错误: features.handover_log.review_ui.buildings 第{idx}项 code/name 不能为空")
        if code in seen_codes:
            raise ValueError(f"配置错误: features.handover_log.review_ui.buildings 存在重复 code: {code}")
        seen_codes.add(code)

    def _parse_fixed_cell_entry(block_name: str, raw_entry: Any) -> tuple[str, str]:
        if isinstance(raw_entry, dict):
            label_cell = str(
                raw_entry.get("label_cell", raw_entry.get("LABEL_CELL", "")) or ""
            ).strip().upper()
            value_cell = str(
                raw_entry.get("value_cell", raw_entry.get("VALUE_CELL", "")) or ""
            ).strip().upper()
            if not value_cell:
                raise ValueError(
                    "配置错误: features.handover_log.review_ui.fixed_cells."
                    f"{block_name} 存在缺少 value_cell 的配对条目: {raw_entry}"
                )
            return label_cell, value_cell
        return "", str(raw_entry or "").strip().upper()

    if not isinstance(fixed_cells, dict) or not fixed_cells:
        raise ValueError("配置错误: features.handover_log.review_ui.fixed_cells 不能为空")
    for block_name, cells in fixed_cells.items():
        if not isinstance(cells, list) or not cells:
            raise ValueError(f"配置错误: features.handover_log.review_ui.fixed_cells.{block_name} 不能为空")
        for raw_entry in cells:
            label_cell, value_cell = _parse_fixed_cell_entry(block_name, raw_entry)
            if not re.fullmatch(r"[A-Z]+[1-9]\d*", value_cell):
                raise ValueError(
                    "配置错误: features.handover_log.review_ui.fixed_cells."
                    f"{block_name} 存在非法 value_cell: {value_cell}"
                )
            if label_cell and not re.fullmatch(r"[A-Z]+[1-9]\d*", label_cell):
                raise ValueError(
                    "配置错误: features.handover_log.review_ui.fixed_cells."
                    f"{block_name} 存在非法 label_cell: {label_cell}"
                )

    if poll_interval_sec <= 0:
        raise ValueError("配置错误: features.handover_log.review_ui.poll_interval_sec 必须大于0")
    if not isinstance(hidden_columns, list):
        raise ValueError("配置错误: features.handover_log.review_ui.section_hidden_columns 必须是数组")
    for raw_col in hidden_columns:
        col = str(raw_col or "").strip().upper()
        if not re.fullmatch(r"[B-I]", col):
            raise ValueError(
                "配置错误: features.handover_log.review_ui.section_hidden_columns 仅支持 B-I 单列字母"
            )
    if public_base_url:
        if not re.fullmatch(r"https?://[^/\s]+", public_base_url):
            raise ValueError(
                "配置错误: features.handover_log.review_ui.public_base_url 必须是合法的 http/https 地址"
            )
    if not isinstance(cabinet_power_defaults, dict):
        raise ValueError(
            "配置错误: features.handover_log.review_ui.cabinet_power_defaults_by_building 必须是对象"
        )
    allowed_cabinet_cells = {"B13", "D13", "F13", "H13"}
    for raw_building, raw_payload in cabinet_power_defaults.items():
        building = str(raw_building or "").strip()
        if not building:
            raise ValueError(
                "配置错误: features.handover_log.review_ui.cabinet_power_defaults_by_building 不能包含空楼栋键"
            )
        if not isinstance(raw_payload, dict):
            raise ValueError(
                "配置错误: features.handover_log.review_ui.cabinet_power_defaults_by_building."
                f"{building} 必须是对象"
            )
        cells = raw_payload.get("cells", {})
        if not isinstance(cells, dict):
            raise ValueError(
                "配置错误: features.handover_log.review_ui.cabinet_power_defaults_by_building."
                f"{building}.cells 必须是对象"
            )
        for raw_cell, raw_value in cells.items():
            cell = str(raw_cell or "").strip().upper()
            if cell not in allowed_cabinet_cells:
                raise ValueError(
                    "配置错误: features.handover_log.review_ui.cabinet_power_defaults_by_building."
                    f"{building}.cells 存在非法单元格: {cell}"
                )
            if isinstance(raw_value, (dict, list)):
                raise ValueError(
                    "配置错误: features.handover_log.review_ui.cabinet_power_defaults_by_building."
                    f"{building}.cells.{cell} 必须是字符串"
                )
    if not isinstance(footer_inventory_defaults, dict):
        raise ValueError(
            "配置错误: features.handover_log.review_ui.footer_inventory_defaults_by_building 必须是对象"
        )
    if not isinstance(review_link_recipients, dict):
        raise ValueError(
            "配置错误: features.handover_log.review_ui.review_link_recipients_by_building 必须是对象"
        )
    allowed_footer_columns = {"B", "C", "E", "F", "G", "H"}
    for raw_building, raw_payload in footer_inventory_defaults.items():
        building = str(raw_building or "").strip()
        if not building:
            raise ValueError(
                "配置错误: features.handover_log.review_ui.footer_inventory_defaults_by_building 不能包含空楼栋键"
            )
        if not isinstance(raw_payload, dict):
            raise ValueError(
                "配置错误: features.handover_log.review_ui.footer_inventory_defaults_by_building."
                f"{building} 必须是对象"
            )
        rows = raw_payload.get("rows", [])
        if not isinstance(rows, list) or not rows:
            raise ValueError(
                "配置错误: features.handover_log.review_ui.footer_inventory_defaults_by_building."
                f"{building}.rows 至少保留1条"
            )
        for idx, raw_row in enumerate(rows, 1):
            if not isinstance(raw_row, dict):
                raise ValueError(
                    "配置错误: features.handover_log.review_ui.footer_inventory_defaults_by_building."
                    f"{building}.rows 第{idx}项必须是对象"
                )
            cells = raw_row.get("cells", {})
            if not isinstance(cells, dict):
                raise ValueError(
                    "配置错误: features.handover_log.review_ui.footer_inventory_defaults_by_building."
                    f"{building}.rows 第{idx}项 cells 必须是对象"
                )
            for raw_cell, raw_value in cells.items():
                cell = str(raw_cell or "").strip().upper()
                if cell not in allowed_footer_columns:
                    raise ValueError(
                        "配置错误: features.handover_log.review_ui.footer_inventory_defaults_by_building."
                        f"{building}.rows 第{idx}项存在非法列: {cell}"
                    )
                if isinstance(raw_value, (dict, list)):
                    raise ValueError(
                        "配置错误: features.handover_log.review_ui.footer_inventory_defaults_by_building."
                        f"{building}.rows 第{idx}项 {cell} 必须是字符串"
                    )
    for raw_building, raw_items in review_link_recipients.items():
        building = str(raw_building or "").strip()
        if not building:
            raise ValueError(
                "配置错误: features.handover_log.review_ui.review_link_recipients_by_building 不能包含空楼栋键"
            )
        if not isinstance(raw_items, list):
            raise ValueError(
                "配置错误: features.handover_log.review_ui.review_link_recipients_by_building."
                f"{building} 必须是数组"
            )
        seen_open_ids: set[str] = set()
        for idx, raw_item in enumerate(raw_items, 1):
            if not isinstance(raw_item, dict):
                raise ValueError(
                    "配置错误: features.handover_log.review_ui.review_link_recipients_by_building."
                    f"{building} 第{idx}项必须是对象"
                )
            open_id = str(raw_item.get("open_id", "") or "").strip()
            if not open_id:
                raise ValueError(
                    "配置错误: features.handover_log.review_ui.review_link_recipients_by_building."
                    f"{building} 第{idx}项 open_id 不能为空"
                )
            if open_id in seen_open_ids:
                raise ValueError(
                    "配置错误: features.handover_log.review_ui.review_link_recipients_by_building."
                    f"{building} 存在重复 open_id: {open_id}"
                )
            seen_open_ids.add(open_id)


def _validate_handover_cell_rules(cfg: Dict[str, Any]) -> None:
    handover = cfg.get("features", {}).get("handover_log", {})
    if not isinstance(handover, dict):
        return
    chiller_mode = handover.get("chiller_mode", {})
    if isinstance(chiller_mode, dict):
        priority_order = chiller_mode.get("priority_order", [])
        if priority_order and not isinstance(priority_order, list):
            raise ValueError("配置错误: features.handover_log.chiller_mode.priority_order 必须是数组")
        if isinstance(priority_order, list):
            cleaned_order = [str(x).strip() for x in priority_order if str(x).strip()]
            if cleaned_order:
                invalid = [x for x in cleaned_order if x not in {"1", "2", "3", "4"}]
                if invalid:
                    raise ValueError(
                        "配置错误: features.handover_log.chiller_mode.priority_order 仅允许 1/2/3/4"
                    )

    monthly = cfg.get("features", {}).get("monthly_report", {})
    buildings_raw = []
    if isinstance(monthly, dict):
        buildings_raw.extend(monthly.get("buildings", []) if isinstance(monthly.get("buildings", []), list) else [])
        sites = monthly.get("sites", [])
        if isinstance(sites, list):
            for site in sites:
                if not isinstance(site, dict):
                    continue
                buildings_raw.append(site.get("building", ""))
    buildings = [str(x).strip() for x in buildings_raw if str(x).strip()]
    handover["cell_rules"] = normalize_cell_rules(handover, buildings)
    cell_rules = handover.get("cell_rules", {})
    if not isinstance(cell_rules, dict):
        raise ValueError("配置错误: features.handover_log.cell_rules 缺失或格式错误")

    allowed_rule_types = {"direct", "aggregate", "computed"}
    allowed_agg = {"first", "max", "min"}
    seen_default_ids: set[str] = set()
    default_rows = cell_rules.get("default_rows", [])
    if not isinstance(default_rows, list):
        raise ValueError("配置错误: features.handover_log.cell_rules.default_rows 必须是数组")
    if not default_rows:
        raise ValueError("配置错误: features.handover_log.cell_rules.default_rows 不能为空")

    def _validate_rows(rows: list[Any], scope_name: str, seen_ids: set[str]) -> None:
        for idx, raw_row in enumerate(rows, 1):
            if not isinstance(raw_row, dict):
                raise ValueError(f"配置错误: {scope_name} 第{idx}项必须是对象")
            row_id = str(raw_row.get("id", "")).strip()
            if not row_id:
                raise ValueError(f"配置错误: {scope_name} 第{idx}项 id 不能为空")
            if row_id in seen_ids:
                raise ValueError(f"配置错误: {scope_name} 存在重复 id: {row_id}")
            seen_ids.add(row_id)

            target_cell = str(raw_row.get("target_cell", "")).strip().upper()
            if target_cell and not re.fullmatch(r"[A-Z]+[1-9]\d*", target_cell):
                raise ValueError(f"配置错误: {scope_name} 第{idx}项 target_cell 非法: {target_cell}")

            rule_type = str(raw_row.get("rule_type", "")).strip().lower()
            if rule_type not in allowed_rule_types:
                raise ValueError(f"配置错误: {scope_name} 第{idx}项 rule_type 非法: {rule_type}")
            agg = str(raw_row.get("agg", "first")).strip().lower()
            if agg not in allowed_agg:
                raise ValueError(f"配置错误: {scope_name} 第{idx}项 agg 非法: {agg}")

            if rule_type in {"direct", "aggregate"}:
                keywords = raw_row.get("d_keywords", [])
                if not isinstance(keywords, list):
                    raise ValueError(f"配置错误: {scope_name} 第{idx}项 d_keywords 必须是数组")
                cleaned = [str(x).strip() for x in keywords if str(x).strip()]
                if not cleaned:
                    raise ValueError(f"配置错误: {scope_name} 第{idx}项 d_keywords 不能为空")
            if rule_type == "computed":
                computed_op = str(raw_row.get("computed_op", "")).strip()
                if not computed_op:
                    raise ValueError(f"配置错误: {scope_name} 第{idx}项 computed_op 不能为空")
                if computed_op not in {"tank_backup", "ring_supply_temp", "chiller_mode_summary"}:
                    try:
                        get_expression_variables(computed_op)
                    except ExpressionError as exc:
                        raise ValueError(
                            f"配置错误: {scope_name} 第{idx}项 computed_op 表达式非法: {exc}"
                        ) from exc

    _validate_rows(default_rows, "features.handover_log.cell_rules.default_rows", seen_default_ids)

    building_rows = cell_rules.get("building_rows", {})
    if not isinstance(building_rows, dict):
        raise ValueError("配置错误: features.handover_log.cell_rules.building_rows 必须是对象")
    for building, rows in building_rows.items():
        if not isinstance(rows, list):
            raise ValueError(f"配置错误: features.handover_log.cell_rules.building_rows[{building}] 必须是数组")
        _validate_rows(rows, f"features.handover_log.cell_rules.building_rows[{building}]", set())


def _migrate_shift_roster_people_text_fields(cfg: Dict[str, Any]) -> None:
    features = cfg.get("features", {})
    if not isinstance(features, dict):
        return
    handover = features.get("handover_log", {})
    if not isinstance(handover, dict):
        return
    roster = handover.get("shift_roster", {})
    if not isinstance(roster, dict):
        return

    preferred_field = "值班人员（实际）"
    legacy_field = "人员（文本）"

    def _migrate_fields(raw_fields: Any) -> None:
        if not isinstance(raw_fields, dict):
            return
        people_field = str(raw_fields.get("people_text", "") or "").strip()
        if not people_field or people_field == legacy_field:
            raw_fields["people_text"] = preferred_field

    _migrate_fields(roster.get("fields", {}))
    long_day = roster.get("long_day", {})
    if isinstance(long_day, dict):
        _migrate_fields(long_day.get("fields", {}))


def validate_settings(cfg: Dict[str, Any]) -> Dict[str, Any]:
    normalized_v3 = ensure_v3_config(cfg)

    runtime_cfg = adapt_runtime_config(normalized_v3)
    download_module = load_download_module()
    if hasattr(download_module, "_normalize_runtime_config"):
        runtime_cfg = download_module._normalize_runtime_config(runtime_cfg)  # type: ignore[attr-defined]
    if hasattr(download_module, "_validate_runtime_config"):
        download_module._validate_runtime_config(runtime_cfg)  # type: ignore[attr-defined]

    normalized_v3 = sync_runtime_back_to_v3(normalized_v3, runtime_cfg)
    normalized_v3 = ensure_v3_config(normalized_v3)
    _migrate_shift_roster_people_text_fields(normalized_v3)
    _normalize_handover_template_title_config(normalized_v3)

    _validate_scheduler(normalized_v3)
    _validate_updater(normalized_v3)
    _validate_common_paths(normalized_v3)
    _validate_deployment_and_shared_bridge(normalized_v3)
    _validate_resume(normalized_v3)
    _validate_sheet_import(normalized_v3)
    _validate_console(normalized_v3)
    _validate_alarm_export(normalized_v3)
    _validate_handover_scheduler(normalized_v3)
    _validate_handover_template(normalized_v3)
    _validate_handover_download(normalized_v3)
    _validate_handover_capacity_report_weather(normalized_v3)
    _validate_handover_shift_roster(normalized_v3)
    _validate_handover_event_sections(normalized_v3)
    _validate_handover_monthly_event_report(normalized_v3)
    _validate_handover_monthly_change_report(normalized_v3)
    _validate_handover_change_management_section(normalized_v3)
    _validate_handover_exercise_management_section(normalized_v3)
    _validate_handover_maintenance_management_section(normalized_v3)
    _validate_handover_other_important_work_section(normalized_v3)
    _validate_day_metric_upload(normalized_v3)
    _validate_handover_source_data_attachment_export(normalized_v3)
    _validate_handover_cloud_sheet_sync(normalized_v3)
    _validate_handover_daily_report_bitable_export(normalized_v3)
    _validate_handover_review_ui(normalized_v3)
    _validate_handover_cell_rules(normalized_v3)
    return normalized_v3


def get_settings_path(config_path: str | Path | None = None) -> Path:
    return resolve_config_path(config_path)


def _settings_backup_candidates(config_path: str | Path | None = None, *, path: Path | None = None) -> List[Path]:
    target = path if path is not None else get_settings_path(config_path)
    pattern = f"{target.stem}.backup.*{target.suffix}"
    backups: List[tuple[float, str, Path]] = []
    for candidate in target.parent.glob(pattern):
        try:
            stat = candidate.stat()
        except OSError:
            continue
        backups.append((stat.st_mtime, candidate.name, candidate))
    backups.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [item[2] for item in backups]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _load_backup_settings_v3(path: Path) -> Dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            raw = json.load(handle)
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        return ensure_v3_config(raw)
    except Exception:
        return None


def _load_template_settings_v3() -> tuple[Dict[str, Any] | None, str]:
    candidates = [
        get_app_dir() / "config" / DEFAULT_CONFIG_TEMPLATE_FILENAME,
        get_bundle_dir() / "config" / DEFAULT_CONFIG_TEMPLATE_FILENAME,
    ]
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if not candidate.exists():
            continue
        loaded = _load_backup_settings_v3(candidate)
        if loaded:
            return loaded, candidate.name
    return None, ""


def _day_metric_repair_baseline_candidates(config_path: str | Path | None = None) -> List[Path]:
    candidates: List[Path] = []
    try:
        target = get_settings_path(config_path)
        candidates.append(target.parent / _DAY_METRIC_REPAIR_BASELINE_FILENAME)
    except Exception:
        pass

    candidates.extend(
        [
            get_app_dir() / _DAY_METRIC_REPAIR_BASELINE_FILENAME,
            get_bundle_dir() / _DAY_METRIC_REPAIR_BASELINE_FILENAME,
            get_app_dir() / "config" / _DAY_METRIC_REPAIR_BASELINE_FILENAME,
            get_bundle_dir() / "config" / _DAY_METRIC_REPAIR_BASELINE_FILENAME,
        ]
    )

    deduped: List[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _has_meaningful_feishu_auth(cfg: Dict[str, Any]) -> bool:
    common = cfg.get("common", {})
    if not isinstance(common, dict):
        return False
    auth = common.get("feishu_auth", {})
    if not isinstance(auth, dict):
        return False
    return bool(_text(auth.get("app_id")) and _text(auth.get("app_secret")))


def _extract_legacy_feishu_auth(cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(cfg, dict):
        return {}
    candidates: List[Dict[str, Any]] = []
    common = cfg.get("common", {})
    if isinstance(common, dict):
        common_legacy = common.get("feishu", {})
        if isinstance(common_legacy, dict):
            candidates.append(common_legacy)
    top_level_legacy = cfg.get("feishu", {})
    if isinstance(top_level_legacy, dict):
        candidates.append(top_level_legacy)

    for candidate in candidates:
        app_id = _text(candidate.get("app_id"))
        app_secret = _text(candidate.get("app_secret"))
        if not app_id or not app_secret:
            continue
        normalized = {
            "app_id": app_id,
            "app_secret": app_secret,
        }
        if "request_retry_count" in candidate:
            normalized["request_retry_count"] = candidate.get("request_retry_count")
        if "request_retry_interval_sec" in candidate:
            normalized["request_retry_interval_sec"] = candidate.get("request_retry_interval_sec")
        if "timeout" in candidate:
            normalized["timeout"] = candidate.get("timeout")
        return normalized
    return {}


def _normalized_notify_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    common = cfg.get("common", {})
    notify_cfg = common.get("notify", {}) if isinstance(common, dict) else {}
    defaults = _dict(_dict(DEFAULT_CONFIG_V3.get("common")).get("notify"))
    return deep_merge_defaults(copy.deepcopy(notify_cfg), copy.deepcopy(defaults))


def _default_notify_config() -> Dict[str, Any]:
    return copy.deepcopy(_dict(_dict(DEFAULT_CONFIG_V3.get("common")).get("notify")))


def _backup_notify_patch(current_cfg: Dict[str, Any], backup_cfg: Dict[str, Any]) -> Dict[str, Any]:
    current = _normalized_notify_config(current_cfg)
    backup = _normalized_notify_config(backup_cfg)
    default = _default_notify_config()

    patch: Dict[str, Any] = {}
    current_webhook = _text(current.get("feishu_webhook_url"))
    backup_webhook = _text(backup.get("feishu_webhook_url"))
    current_keyword = _text(current.get("keyword"))
    backup_keyword = _text(backup.get("keyword"))
    default_keyword = _text(default.get("keyword"))

    if not current_webhook and backup_webhook:
        return copy.deepcopy(backup)

    if (
        current_webhook
        and backup_webhook
        and current_webhook == backup_webhook
        and backup_keyword
        and backup_keyword != current_keyword
        and current_keyword in {"", default_keyword}
    ):
        patch["keyword"] = backup_keyword

    return patch


def _normalized_day_metric_upload(cfg: Dict[str, Any]) -> Dict[str, Any]:
    features = cfg.get("features", {})
    feature_cfg = features.get("day_metric_upload", {}) if isinstance(features, dict) else {}
    defaults = _dict(_dict(DEFAULT_CONFIG_V3.get("features")).get("day_metric_upload"))
    return sanitize_day_metric_upload_config(
        deep_merge_defaults(copy.deepcopy(feature_cfg), copy.deepcopy(defaults))
    )


def _default_day_metric_upload() -> Dict[str, Any]:
    return sanitize_day_metric_upload_config(
        copy.deepcopy(_dict(_dict(DEFAULT_CONFIG_V3.get("features")).get("day_metric_upload")))
    )


def _has_meaningful_day_metric_upload(cfg: Dict[str, Any]) -> bool:
    feature_cfg = _normalized_day_metric_upload(cfg)
    if feature_cfg == _default_day_metric_upload():
        return False
    target = feature_cfg.get("target", {})
    source = target.get("source", {}) if isinstance(target, dict) else {}
    return bool(_text(source.get("app_token")) and _text(source.get("table_id")))


def _extract_day_metric_repair_payload(cfg: Dict[str, Any]) -> Dict[str, Any]:
    feature_cfg = _normalized_day_metric_upload(cfg)
    payload = {
        "target": copy.deepcopy(feature_cfg.get("target", {})),
        "source": copy.deepcopy(feature_cfg.get("source", {})),
        "behavior": copy.deepcopy(feature_cfg.get("behavior", {})),
    }
    return payload


def _has_meaningful_day_metric_repair_payload(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    target = payload.get("target", {})
    source = target.get("source", {}) if isinstance(target, dict) else {}
    return bool(_text(_dict(source).get("app_token")) and _text(_dict(source).get("table_id")))


def _apply_day_metric_repair_payload(cfg: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    output = ensure_v3_config(copy.deepcopy(cfg))
    features = output.get("features", {})
    if not isinstance(features, dict):
        features = {}
    day_metric = _normalized_day_metric_upload(output)
    day_metric["target"] = copy.deepcopy(payload.get("target", {}))
    day_metric["source"] = copy.deepcopy(payload.get("source", {}))
    day_metric["behavior"] = copy.deepcopy(payload.get("behavior", {}))
    features["day_metric_upload"] = day_metric
    output["features"] = features
    return output


def _pick_day_metric_repair_payload_from_backups(config_path: str | Path | None = None) -> tuple[Dict[str, Any] | None, str]:
    # 优先固定修复基线，保证“修复按钮”每次收敛到同一版本，不随最近备份漂移。
    for baseline_path in _day_metric_repair_baseline_candidates(config_path):
        if not baseline_path.exists():
            continue
        baseline_cfg = _load_backup_settings_v3(baseline_path)
        if not baseline_cfg:
            continue
        payload = _extract_day_metric_repair_payload(baseline_cfg)
        if _has_meaningful_day_metric_repair_payload(payload):
            return payload, baseline_path.name

    for backup_path in _settings_backup_candidates(config_path):
        backup_cfg = _load_backup_settings_v3(backup_path)
        if not backup_cfg:
            continue
        payload = _extract_day_metric_repair_payload(backup_cfg)
        if _has_meaningful_day_metric_repair_payload(payload):
            return payload, backup_path.name
    template_cfg, template_name = _load_template_settings_v3()
    if template_cfg:
        payload = _extract_day_metric_repair_payload(template_cfg)
        if _has_meaningful_day_metric_repair_payload(payload):
            return payload, template_name
    return None, ""


def _repair_critical_settings_from_backups(
    cfg: Dict[str, Any],
    config_path: str | Path | None = None,
) -> tuple[Dict[str, Any], List[str]]:
    repaired = ensure_v3_config(copy.deepcopy(cfg))
    repaired_notes: List[str] = []
    needs_feishu_auth = not _has_meaningful_feishu_auth(repaired)
    needs_day_metric = _normalized_day_metric_upload(repaired) == _default_day_metric_upload()
    needs_notify = False
    if _text(_normalized_notify_config(repaired).get("feishu_webhook_url")):
        needs_notify = True
    if needs_feishu_auth:
        legacy_auth = _extract_legacy_feishu_auth(cfg)
        if legacy_auth:
            current_auth = _dict(_dict(repaired.get("common")).get("feishu_auth"))
            current_auth.update(copy.deepcopy(legacy_auth))
            repaired["common"]["feishu_auth"] = current_auth
            repaired_notes.append("飞书应用凭据 <- 当前配置兼容字段(feishu)")
            needs_feishu_auth = False

    if not needs_feishu_auth and not needs_day_metric and not needs_notify:
        return repaired, repaired_notes

    for backup_path in _settings_backup_candidates(config_path):
        if not needs_feishu_auth and not needs_day_metric and not needs_notify:
            break
        backup_cfg = _load_backup_settings_v3(backup_path)
        if not backup_cfg:
            continue
        if needs_feishu_auth and _has_meaningful_feishu_auth(backup_cfg):
            repaired["common"]["feishu_auth"] = copy.deepcopy(backup_cfg["common"]["feishu_auth"])
            repaired_notes.append(f"飞书应用凭据 <- {backup_path.name}")
            needs_feishu_auth = False
        if needs_notify:
            notify_patch = _backup_notify_patch(repaired, backup_cfg)
            if notify_patch:
                current_notify = _normalized_notify_config(repaired)
                current_notify.update(copy.deepcopy(notify_patch))
                repaired["common"]["notify"] = current_notify
                repaired_notes.append(f"Webhook告警配置 <- {backup_path.name}")
                needs_notify = False
        if needs_day_metric and _has_meaningful_day_metric_upload(backup_cfg):
            repaired["features"]["day_metric_upload"] = _normalized_day_metric_upload(backup_cfg)
            repaired_notes.append(f"12项独立上传配置 <- {backup_path.name}")
            needs_day_metric = False

    if needs_feishu_auth or needs_day_metric or needs_notify:
        template_cfg, template_name = _load_template_settings_v3()
        if template_cfg:
            if needs_feishu_auth and _has_meaningful_feishu_auth(template_cfg):
                repaired["common"]["feishu_auth"] = copy.deepcopy(template_cfg["common"]["feishu_auth"])
                repaired_notes.append(f"飞书应用凭据 <- {template_name}")
                needs_feishu_auth = False
            if needs_notify:
                notify_patch = _backup_notify_patch(repaired, template_cfg)
                if notify_patch:
                    current_notify = _normalized_notify_config(repaired)
                    current_notify.update(copy.deepcopy(notify_patch))
                    repaired["common"]["notify"] = current_notify
                    repaired_notes.append(f"Webhook告警配置 <- {template_name}")
                    needs_notify = False
            if needs_day_metric and _has_meaningful_day_metric_upload(template_cfg):
                repaired["features"]["day_metric_upload"] = _normalized_day_metric_upload(template_cfg)
                repaired_notes.append(f"12项独立上传配置 <- {template_name}")
                needs_day_metric = False
    return repaired, repaired_notes


def backup_settings_file(
    config_path: str | Path | None = None,
    *,
    path: Path | None = None,
    retention: int = 10,
) -> Path | None:
    target = path if path is not None else get_settings_path(config_path)
    if not target.exists():
        return None
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    backup_path = target.with_name(f"{target.stem}.backup.{timestamp}{target.suffix}")
    counter = 1
    while backup_path.exists():
        backup_path = target.with_name(f"{target.stem}.backup.{timestamp}.{counter}{target.suffix}")
        counter += 1
    shutil.copy2(target, backup_path)
    pattern = f"{target.stem}.backup.*{target.suffix}"
    backups = sorted(
        target.parent.glob(pattern),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for extra in backups[max(0, int(retention)) :]:
        try:
            extra.unlink()
        except OSError:
            pass
    return backup_path


def write_settings_atomically(
    cfg: Dict[str, Any],
    config_path: str | Path | None = None,
    *,
    path: Path | None = None,
    backup: bool = True,
    retention: int = 10,
) -> Path:
    target = path if path is not None else get_settings_path(config_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if backup:
        backup_settings_file(path=target, retention=retention)
    payload = json.dumps(cfg, ensure_ascii=False, indent=2)
    tmp_path = target.with_name(f"{target.name}.tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="\n") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, target)
    return target


def _normalize_console_host_for_lan(cfg: Dict[str, Any], config_path: str | Path | None = None) -> Dict[str, Any]:
    _ = config_path
    if not isinstance(cfg, dict):
        return cfg
    common_cfg = cfg.get("common", {})
    if not isinstance(common_cfg, dict):
        return cfg
    console_cfg = common_cfg.get("console", {})
    if not isinstance(console_cfg, dict):
        return cfg
    host = str(console_cfg.get("host", "") or "").strip().lower()
    if host not in {"127.0.0.1", "localhost", "::1"}:
        return cfg
    console_cfg["host"] = "0.0.0.0"
    common_cfg["console"] = console_cfg
    cfg["common"] = common_cfg
    return cfg


def _contains_deprecated_alarm_db_config(cfg: Dict[str, Any]) -> bool:
    if not isinstance(cfg, dict):
        return False
    common = cfg.get("common", {})
    if isinstance(common, dict) and isinstance(common.get("alarm_db"), dict):
        return True
    features = cfg.get("features", {})
    handover = features.get("handover_log", {}) if isinstance(features, dict) else {}
    if isinstance(handover, dict) and isinstance(handover.get("alarm_db"), dict):
        return True
    legacy_alarm = cfg.get("alarm_bitable_export", {})
    if isinstance(legacy_alarm, dict) and isinstance(legacy_alarm.get("db"), dict):
        return True
    return False


def _compose_handover_segmented_config(cfg: Dict[str, Any], config_path: str | Path | None = None) -> Dict[str, Any]:
    try:
        target = get_settings_path(config_path)
    except FileNotFoundError:
        return copy.deepcopy(cfg)
    if not has_any_handover_segment_file(target):
        return copy.deepcopy(cfg)
    common_doc, building_docs = read_all_segment_documents(target)
    building_payloads = {
        building: doc.get("data", {})
        for building, doc in building_docs.items()
        if isinstance(doc.get("data"), dict)
    }
    return apply_handover_segment_data(
        cfg,
        common_data=common_doc.get("data", {}) if isinstance(common_doc.get("data"), dict) else {},
        building_data_by_name=building_payloads,
    )


def _normalize_segment_review_link_recipients(payload: Any) -> dict[str, list[Dict[str, Any]]]:
    output: dict[str, list[Dict[str, Any]]] = {}
    source = payload if isinstance(payload, dict) else {}
    for raw_building, raw_items in source.items():
        building = str(raw_building or "").strip()
        if building not in HANDOVER_SEGMENT_BUILDINGS:
            continue
        rows: list[Dict[str, Any]] = []
        if isinstance(raw_items, list):
            for raw_row in raw_items:
                if not isinstance(raw_row, dict):
                    continue
                note = str(raw_row.get("note", "") or "").strip()
                open_id = str(raw_row.get("open_id", "") or "").strip()
                if not note and not open_id:
                    continue
                if not open_id:
                    continue
                rows.append(
                    {
                        "note": note,
                        "open_id": open_id,
                        "enabled": False if raw_row.get("enabled", True) is False else True,
                    }
                )
        if rows:
            output[building] = rows
    return output


def _extract_aggregate_review_link_recipients(cfg: Dict[str, Any]) -> dict[str, list[Dict[str, Any]]]:
    features = cfg.get("features", {}) if isinstance(cfg, dict) else {}
    handover = features.get("handover_log", {}) if isinstance(features, dict) else {}
    review_ui = handover.get("review_ui", {}) if isinstance(handover, dict) else {}
    recipients = review_ui.get("review_link_recipients_by_building", {}) if isinstance(review_ui, dict) else {}
    return _normalize_segment_review_link_recipients(recipients)


def _extract_aggregate_building_payloads(cfg: Dict[str, Any]) -> dict[str, Dict[str, Any]]:
    if not isinstance(cfg, dict):
        return {}
    try:
        normalized = ensure_v3_config(copy.deepcopy(cfg))
    except Exception:  # noqa: BLE001
        normalized = copy.deepcopy(cfg)
    payloads: dict[str, Dict[str, Any]] = {}
    for building in HANDOVER_SEGMENT_BUILDINGS:
        try:
            payloads[building] = extract_handover_building_data(normalized, building)
        except Exception:  # noqa: BLE001
            continue
    return payloads


def _merge_handover_segment_payload(base: Any, overlay: Any) -> Dict[str, Any]:
    output = copy.deepcopy(base if isinstance(base, dict) else {})
    if not isinstance(overlay, dict):
        return output
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(output.get(key), dict):
            output[key] = _merge_handover_segment_payload(output.get(key), value)
        else:
            output[key] = copy.deepcopy(value)
    return output


def _repair_handover_segment_required_defaults(
    config_path: str | Path,
    source_cfg: Dict[str, Any] | None = None,
) -> bool:
    changed = False
    aggregate_recipients = _extract_aggregate_review_link_recipients(source_cfg or {})
    aggregate_building_payloads = _extract_aggregate_building_payloads(source_cfg or {})
    for building in HANDOVER_SEGMENT_BUILDINGS:
        building_path = handover_building_segment_path(config_path, building)
        if not building_path.exists():
            continue
        building_changed = False
        doc = read_segment_document(building_path)
        data = copy.deepcopy(doc.get("data", {}) if isinstance(doc.get("data"), dict) else {})
        cloud_sheet_sync = data.get("cloud_sheet_sync")
        if not isinstance(cloud_sheet_sync, dict):
            cloud_sheet_sync = {}
            data["cloud_sheet_sync"] = cloud_sheet_sync
        sheet_names = cloud_sheet_sync.get("sheet_names")
        if not isinstance(sheet_names, dict):
            sheet_names = {}
            cloud_sheet_sync["sheet_names"] = sheet_names
        if not str(sheet_names.get(building, "") or "").strip():
            sheet_names[building] = building
            building_changed = True
        source_payload = aggregate_building_payloads.get(building, {})
        source_rows = (
            source_payload.get("cell_rules", {}).get("building_rows", {}).get(building, [])
            if isinstance(source_payload, dict)
            else []
        )
        cell_rules = data.get("cell_rules")
        if not isinstance(cell_rules, dict):
            cell_rules = {}
            data["cell_rules"] = cell_rules
        building_rows = cell_rules.get("building_rows")
        if not isinstance(building_rows, dict):
            building_rows = {}
            cell_rules["building_rows"] = building_rows
        current_rows = building_rows.get(building)
        if isinstance(source_rows, list) and source_rows and (not isinstance(current_rows, list) or not current_rows):
            building_rows[building] = copy.deepcopy(source_rows)
            building_changed = True
        review_ui = data.get("review_ui")
        if not isinstance(review_ui, dict):
            review_ui = {}
            data["review_ui"] = review_ui
        source_review_ui = source_payload.get("review_ui", {}) if isinstance(source_payload, dict) else {}
        for map_key in ("cabinet_power_defaults_by_building", "footer_inventory_defaults_by_building"):
            source_map = source_review_ui.get(map_key, {}) if isinstance(source_review_ui, dict) else {}
            source_value = source_map.get(building) if isinstance(source_map, dict) else None
            if not source_value:
                continue
            target_map = review_ui.get(map_key)
            if not isinstance(target_map, dict):
                target_map = {}
                review_ui[map_key] = target_map
            current_value = target_map.get(building)
            if not current_value:
                target_map[building] = copy.deepcopy(source_value)
                building_changed = True
        if aggregate_recipients.get(building):
            recipients = review_ui.get("review_link_recipients_by_building")
            if not isinstance(recipients, dict):
                recipients = {}
                review_ui["review_link_recipients_by_building"] = recipients
            if building not in recipients:
                recipients[building] = copy.deepcopy(aggregate_recipients[building])
                building_changed = True
        if building_changed:
            next_revision = max(0, int(doc.get("revision", 0) or 0)) + 1
            write_segment_document(building_path, build_segment_document(data, revision=next_revision))
            changed = True
    return changed


def _ensure_handover_segment_files(cfg: Dict[str, Any], config_path: str | Path | None = None) -> bool:
    try:
        target = get_settings_path(config_path)
    except FileNotFoundError:
        return False
    if has_all_handover_segment_files(target):
        return _repair_handover_segment_required_defaults(target, cfg)
    with handover_segment_write_lock():
        if has_all_handover_segment_files(target):
            return _repair_handover_segment_required_defaults(target, cfg)
        source_cfg = copy.deepcopy(cfg if isinstance(cfg, dict) else {})
        source_cfg = ensure_v3_config(source_cfg)
        if has_any_handover_segment_file(target):
            source_cfg = _compose_handover_segmented_config(source_cfg, target)
            source_cfg = ensure_v3_config(source_cfg)
        else:
            create_pre_handover_segment_backup(target)
        common_doc, building_docs = build_segment_documents_from_config(source_cfg)
        common_path = handover_common_segment_path(target)
        if not common_path.exists():
            write_segment_document(common_path, common_doc)
        for building in HANDOVER_SEGMENT_BUILDINGS:
            building_path = handover_building_segment_path(target, building)
            if building_path.exists():
                continue
            write_segment_document(building_path, building_docs[building])
        _repair_handover_segment_required_defaults(target, source_cfg)
    return True


def _preserve_segment_backed_handover(cfg: Dict[str, Any], config_path: str | Path | None = None) -> Dict[str, Any]:
    try:
        target = get_settings_path(config_path)
    except FileNotFoundError:
        return copy.deepcopy(cfg)
    if not has_any_handover_segment_file(target):
        return copy.deepcopy(cfg)
    return _compose_handover_segmented_config(cfg, target)


def preserve_segmented_handover_config(
    cfg: Dict[str, Any],
    config_path: str | Path | None = None,
) -> Dict[str, Any]:
    return _preserve_segment_backed_handover(cfg, config_path)


def get_handover_common_segment(config_path: str | Path | None = None) -> Dict[str, Any]:
    target = get_settings_path(config_path)
    common_path = handover_common_segment_path(target)
    if common_path.exists():
        return read_segment_document(common_path)
    common_doc, _ = build_segment_documents_from_config(load_pipeline_config(target))
    return build_segment_document(common_doc.get("data", {}), revision=0, updated_at="")


def get_handover_building_segment(building_code: str, config_path: str | Path | None = None) -> Dict[str, Any]:
    target = get_settings_path(config_path)
    building_path = handover_building_segment_path(target, building_code)
    if building_path.exists():
        return read_segment_document(building_path)
    _, building_docs = build_segment_documents_from_config(load_pipeline_config(target))
    building_doc = building_docs[building_name_from_segment_code(building_code)]
    return build_segment_document(building_doc.get("data", {}), revision=0, updated_at="")


def _refresh_handover_aggregate_view(config_path: str | Path | None = None) -> tuple[Dict[str, Any], str]:
    target = get_settings_path(config_path)
    with handover_segment_aggregate_lock(target):
        try:
            raw_existing = load_pipeline_config(target)
            common_doc, building_docs = read_all_segment_documents(target)
            building_payloads = {
                building: doc.get("data", {})
                for building, doc in building_docs.items()
                if isinstance(doc.get("data"), dict)
            }
            aggregate_payload = apply_handover_segment_data(
                raw_existing,
                common_data=common_doc.get("data", {}) if isinstance(common_doc.get("data"), dict) else {},
                building_data_by_name=building_payloads,
            )
            write_settings_atomically(_normalize_footer_defaults_for_persistence(aggregate_payload), target)
            return load_settings(target), ""
        except Exception as exc:  # noqa: BLE001
            try:
                return load_settings(target), str(exc)
            except Exception:  # noqa: BLE001
                return {}, str(exc)


def _normalize_footer_defaults_for_persistence(cfg: Dict[str, Any]) -> Dict[str, Any]:
    normalized = copy.deepcopy(cfg if isinstance(cfg, dict) else {})
    features = normalized.get("features", {})
    if not isinstance(features, dict):
        return normalized
    handover = features.get("handover_log", {})
    if not isinstance(handover, dict):
        return normalized
    review_ui = handover.get("review_ui", {})
    if not isinstance(review_ui, dict):
        return normalized
    defaults_by_building = review_ui.get("footer_inventory_defaults_by_building", {})
    if not isinstance(defaults_by_building, dict):
        return normalized
    service = FooterInventoryDefaultsService()
    review_ui["footer_inventory_defaults_by_building"] = {
        str(building or "").strip(): {"rows": service.normalize_rows(payload.get("rows", []) if isinstance(payload, dict) else [])}
        for building, payload in defaults_by_building.items()
        if str(building or "").strip()
    }
    handover["review_ui"] = review_ui
    features["handover_log"] = handover
    normalized["features"] = features
    return normalized


def _extract_all_handover_building_segments(cfg: Dict[str, Any]) -> dict[str, Dict[str, Any]]:
    return {
        building: extract_handover_building_data(cfg, building)
        for building in HANDOVER_SEGMENT_BUILDINGS
    }


def save_handover_common_segment(
    data: Dict[str, Any],
    *,
    base_revision: int,
    config_path: str | Path | None = None,
) -> tuple[Dict[str, Any], Dict[str, Any], str]:
    target = get_settings_path(config_path)
    with handover_segment_target_lock(target, "common"):
        current_full = load_settings(target)
        current_doc = read_segment_document(handover_common_segment_path(target))
        current_revision = int(current_doc.get("revision", 0) or 0)
        if current_revision != int(base_revision):
            raise HandoverSegmentRevisionConflict("公共配置已被其他人修改，请刷新后重试")
        current_common = extract_handover_common_data(current_full)
        next_common = _merge_handover_segment_payload(current_common, data)
        next_full = apply_handover_segment_data(
            current_full,
            common_data=next_common,
            building_data_by_name=_extract_all_handover_building_segments(current_full),
        )
        validated = _normalize_footer_defaults_for_persistence(validate_settings(next_full))
        next_doc = build_segment_document(
            extract_handover_common_data(validated),
            revision=current_revision + 1,
        )
        write_segment_document(handover_common_segment_path(target), next_doc)
    refreshed_config, aggregate_refresh_error = _refresh_handover_aggregate_view(target)
    return refreshed_config, next_doc, aggregate_refresh_error


def update_handover_common_segment_data(
    updater: Callable[[Dict[str, Any]], None],
    *,
    config_path: str | Path | None = None,
) -> tuple[Dict[str, Any], Dict[str, Any], str, Dict[str, Any], bool]:
    """Patch the handover common segment under its segment lock.

    Returns: refreshed_config, next_document, aggregate_refresh_error,
    previous_document, changed.
    """
    target = get_settings_path(config_path)
    with handover_segment_target_lock(target, "common"):
        current_full = load_settings(target)
        current_doc = read_segment_document(handover_common_segment_path(target))
        current_revision = int(current_doc.get("revision", 0) or 0)
        current_data = copy.deepcopy(current_doc.get("data", {}) if isinstance(current_doc.get("data", {}), dict) else {})
        next_data = copy.deepcopy(current_data)
        updater(next_data)
        changed = next_data != current_data
        if not changed:
            return current_full, current_doc, "", current_doc, False

        next_full = apply_handover_segment_data(
            current_full,
            common_data=next_data,
            building_data_by_name=_extract_all_handover_building_segments(current_full),
        )
        validated = _normalize_footer_defaults_for_persistence(validate_settings(next_full))
        next_doc = build_segment_document(
            extract_handover_common_data(validated),
            revision=current_revision + 1,
        )
        write_segment_document(handover_common_segment_path(target), next_doc)
    refreshed_config, aggregate_refresh_error = _refresh_handover_aggregate_view(target)
    return refreshed_config, next_doc, aggregate_refresh_error, current_doc, True


def save_handover_building_segment(
    building_code: str,
    data: Dict[str, Any],
    *,
    base_revision: int,
    config_path: str | Path | None = None,
) -> tuple[Dict[str, Any], Dict[str, Any], str]:
    target = get_settings_path(config_path)
    building_path = handover_building_segment_path(target, building_code)
    target_building = building_name_from_segment_code(building_code)
    with handover_segment_target_lock(target, f"building:{str(building_code or '').strip().upper()}"):
        current_full = load_settings(target)
        current_doc = read_segment_document(building_path)
        current_revision = int(current_doc.get("revision", 0) or 0)
        if current_revision != int(base_revision):
            raise HandoverSegmentRevisionConflict("当前楼配置已被其他人修改，请刷新后重试")
        current_common = extract_handover_common_data(current_full)
        building_payloads = _extract_all_handover_building_segments(current_full)
        building_payloads[target_building] = _merge_handover_segment_payload(
            building_payloads.get(target_building, {}),
            data,
        )
        next_full = apply_handover_segment_data(
            current_full,
            common_data=current_common,
            building_data_by_name=building_payloads,
        )
        validated = _normalize_footer_defaults_for_persistence(validate_settings(next_full))
        next_doc = build_segment_document(
            extract_handover_building_data(validated, target_building),
            revision=current_revision + 1,
        )
        write_segment_document(building_path, next_doc)
    refreshed_config, aggregate_refresh_error = _refresh_handover_aggregate_view(target)
    return refreshed_config, next_doc, aggregate_refresh_error


def load_settings(config_path: str | Path | None = None) -> Dict[str, Any]:
    cfg = load_pipeline_config(config_path)
    _ = _ensure_handover_segment_files(cfg, config_path)
    composed = _compose_handover_segmented_config(cfg, config_path)
    normalized = validate_settings(composed)
    return _normalize_console_host_for_lan(normalized, config_path)


def load_bootstrap_settings(config_path: str | Path | None = None) -> Dict[str, Any]:
    cfg = load_pipeline_config(config_path)
    composed = _compose_handover_segmented_config(cfg, config_path)
    return ensure_v3_config(composed)


def repair_day_metric_related_settings(
    cfg: Dict[str, Any],
    config_path: str | Path | None = None,
) -> tuple[Dict[str, Any], List[str], bool]:
    _ = config_path
    current = ensure_v3_config(copy.deepcopy(cfg))
    return current, [], False


def save_settings(
    cfg: Dict[str, Any],
    config_path: str | Path | None = None,
    *,
    preserve_segmented_handover: bool = True,
    preserve_existing_user_values: bool = True,
    clear_paths: Iterable[str] | None = None,
    force_overwrite: bool = False,
) -> Dict[str, Any]:
    payload = _preserve_segment_backed_handover(cfg, config_path) if preserve_segmented_handover else copy.deepcopy(cfg)
    normalized = _normalize_footer_defaults_for_persistence(validate_settings(payload))
    final_payload = copy.deepcopy(normalized)
    if preserve_existing_user_values:
        try:
            target = get_settings_path(config_path)
        except FileNotFoundError:
            target = None
        if target is not None and target.exists():
            from app.config.config_merge_guard import merge_user_config_payload

            existing_raw = load_pipeline_config(target)
            merge_result = merge_user_config_payload(
                final_payload,
                existing_raw,
                clear_paths=clear_paths,
                force_overwrite=force_overwrite,
            )
            final_payload = _normalize_footer_defaults_for_persistence(merge_result.merged)
    write_settings_atomically(final_payload, config_path)
    return final_payload

