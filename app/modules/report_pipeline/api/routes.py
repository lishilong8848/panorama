from __future__ import annotations

import copy
import json
import os
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import date, datetime
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlsplit, urlunsplit

from fastapi import APIRouter, Body, File, Form, HTTPException, Request, UploadFile

from app.config.config_adapter import normalize_role_mode, resolve_shared_bridge_paths
from app.config.config_merge_guard import ConfigValueLossError, merge_user_config_payload
from app.config.secret_masking import mask_settings
from app.config.settings_loader import save_settings
from app.modules.notify.service.webhook_notify_service import WebhookNotifyService
from app.modules.report_pipeline.service.resume_checkpoint_store import (
    resolve_resume_index_path as resolve_monthly_resume_index_path,
)
from app.modules.report_pipeline.service.resume_checkpoint_store import (
    resolve_resume_root_dir as resolve_monthly_resume_root_dir,
)
from app.modules.report_pipeline.service.calculation_service import CalculationService
from app.modules.report_pipeline.service.job_service import JobBusyError
from app.modules.report_pipeline.service.monthly_cache_continue_service import run_monthly_from_file_items
from app.modules.report_pipeline.service.orchestrator_service import OrchestratorService
from app.modules.shared_bridge.service.shared_source_cache_service import (
    SharedSourceCacheService,
    is_accessible_cached_file_path,
)
from app.shared.utils.runtime_temp_workspace import (
    cleanup_runtime_temp_dir,
    create_runtime_temp_dir,
    resolve_runtime_state_root,
)
from handover_log_module.api.facade import load_handover_config
from handover_log_module.repository.event_followup_cache_store import EventFollowupCacheStore
from handover_log_module.repository.shift_roster_repository import ShiftRosterRepository
from handover_log_module.service.day_metric_bitable_export_service import DayMetricBitableExportService
from handover_log_module.service.day_metric_standalone_upload_service import DayMetricStandaloneUploadService
from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService
from handover_log_module.service.review_session_service import ReviewSessionService
from handover_log_module.service.wet_bulb_collection_service import WetBulbCollectionService
from pipeline_utils import get_app_dir


router = APIRouter(tags=["pipeline"])
_REVIEW_BASE_PROBE_TTL_SEC = 30.0
_review_base_probe_cache: dict[tuple[tuple[str, ...], str], dict[str, Any]] = {}
_review_base_probe_lock = threading.Lock()
_REVIEW_ACCESS_STATE_FILE_NAME = "handover_review_access_state.json"
_IPCONFIG_IPV4_RE = re.compile(r"(?<!\d)(?:\d{1,3}\.){3}\d{1,3}(?!\d)")
_VIRTUAL_ADAPTER_KEYWORDS = (
    "virtual",
    "vmware",
    "virtualbox",
    "hyper-v",
    "vethernet",
    "loopback",
    "docker",
    "wsl",
    "tailscale",
    "zerotier",
    "vpn",
    "tap",
    "tun",
    "bluetooth",
    "host-only",
)
_PREFERRED_ADAPTER_KEYWORDS = (
    "ethernet",
    "以太网",
    "wi-fi",
    "wifi",
    "wlan",
    "wireless",
    "无线",
    "local area connection",
    "本地连接",
)
_DEFAULT_REVIEW_BUILDINGS = [
    {"code": "a", "name": "A楼"},
    {"code": "b", "name": "B楼"},
    {"code": "c", "name": "C楼"},
    {"code": "d", "name": "D楼"},
    {"code": "e", "name": "E楼"},
]
_REVIEW_BASE_STARTUP_PROBE_DELAY_SEC = 8.0
_INTERNAL_HEALTH_SHARED_BRIDGE_SLOW_THRESHOLD_SEC = 1.0
_INTERNAL_HEALTH_SHARED_BRIDGE_SLOW_LOG_INTERVAL_SEC = 60.0


def _runtime_config(container) -> Dict[str, Any]:
    return copy.deepcopy(container.runtime_config)


def _runtime_state_config(container) -> Dict[str, Any]:
    for attr in ("runtime_config", "config"):
        value = getattr(container, attr, None)
        if isinstance(value, dict):
            return value
    return {}


def _deployment_role_mode(container) -> str:
    snapshot = container.deployment_snapshot() if hasattr(container, "deployment_snapshot") else {}
    if not isinstance(snapshot, dict):
        return ""
    text = str(snapshot.get("role_mode", "") or "").strip().lower()
    if text in {"internal", "external"}:
        return text
    return ""


def _config_last_started_role_mode(cfg: Dict[str, Any]) -> str:
    common = cfg.get("common", {}) if isinstance(cfg, dict) else {}
    deployment = common.get("deployment", {}) if isinstance(common, dict) else {}
    if not isinstance(deployment, dict):
        return ""
    return normalize_role_mode(deployment.get("last_started_role_mode"))


def _config_role_mode(cfg: Dict[str, Any]) -> str:
    common = cfg.get("common", {}) if isinstance(cfg, dict) else {}
    deployment = common.get("deployment", {}) if isinstance(common, dict) else {}
    if not isinstance(deployment, dict):
        return ""
    return normalize_role_mode(deployment.get("role_mode"))


def _normalize_console_bind_host(value: Any) -> str:
    return str(value or "").strip() or "127.0.0.1"


def _is_loopback_console_host(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if text in {"127.0.0.1", "::1", "localhost"}:
        return True
    try:
        return ip_address(text).is_loopback
    except ValueError:
        return False


def _resolve_runtime_console_bind_host(container) -> str:
    env_host = str(os.environ.get("QJPT_CONSOLE_BIND_HOST", "") or "").strip()
    if env_host:
        return _normalize_console_bind_host(env_host)
    config = container.config if isinstance(getattr(container, "config", None), dict) else {}
    common_cfg = config.get("common", {}) if isinstance(config.get("common", {}), dict) else {}
    console_cfg = common_cfg.get("console", {}) if isinstance(common_cfg, dict) else {}
    host = _normalize_console_bind_host(console_cfg.get("host", "127.0.0.1"))
    role_mode = _deployment_role_mode(container)
    if role_mode == "internal":
        return "127.0.0.1"
    return host


def _resolve_runtime_console_bind_port(container) -> int:
    env_port = str(os.environ.get("QJPT_CONSOLE_BIND_PORT", "") or "").strip()
    if env_port:
        try:
            return int(env_port)
        except Exception:  # noqa: BLE001
            pass
    config = container.config if isinstance(getattr(container, "config", None), dict) else {}
    common_cfg = config.get("common", {}) if isinstance(config.get("common", {}), dict) else {}
    console_cfg = common_cfg.get("console", {}) if isinstance(common_cfg, dict) else {}
    return _resolve_console_port(console_cfg)


def _review_access_probe_guard(container) -> tuple[bool, str, str]:
    role_mode = _deployment_role_mode(container)
    if role_mode != "external":
        return False, "internal_local_only", "当前为内网端，不提供局域网审核访问地址"
    bind_host = _resolve_runtime_console_bind_host(container)
    if _is_loopback_console_host(bind_host):
        return False, "external_bind_required", "当前外网端未开放局域网监听，无法生成审核访问地址"
    return True, "", ""


def _ensure_not_internal_role(container, detail: str) -> None:
    if _deployment_role_mode(container) == "internal":
        raise HTTPException(status_code=409, detail=detail)


def _shared_bridge_is_available(container) -> bool:
    snapshot = container.shared_bridge_snapshot() if hasattr(container, "shared_bridge_snapshot") else {}
    if not isinstance(snapshot, dict):
        return False
    return bool(snapshot.get("enabled", False)) and bool(str(snapshot.get("root_dir", "") or "").strip())


def _blank_internal_download_pool_snapshot() -> Dict[str, Any]:
    return {
        "enabled": False,
        "browser_ready": False,
        "page_slots": [],
        "active_buildings": [],
        "last_error": "",
    }


def _sanitize_shared_bridge_snapshot_for_role(snapshot: Any, *, role_mode: str) -> Dict[str, Any]:
    payload = dict(snapshot) if isinstance(snapshot, dict) else {}
    if normalize_role_mode(role_mode) != "internal":
        payload["internal_download_pool"] = _blank_internal_download_pool_snapshot()
    return payload


def _shared_bridge_health_snapshot(container, request: Request, *, role_mode: str) -> Dict[str, Any]:
    snapshot_mode = "internal_light" if str(role_mode or "").strip().lower() == "internal" else "external_full"
    started_at = time.perf_counter()
    snapshot: Dict[str, Any] = {}
    if hasattr(container, "shared_bridge_snapshot"):
        try:
            snapshot = container.shared_bridge_snapshot(mode=snapshot_mode)
        except TypeError:
            snapshot = container.shared_bridge_snapshot()
    elapsed = time.perf_counter() - started_at
    if snapshot_mode == "internal_light" and elapsed >= _INTERNAL_HEALTH_SHARED_BRIDGE_SLOW_THRESHOLD_SEC:
        now_monotonic = time.monotonic()
        last_logged_at = float(
            getattr(request.app.state, "_internal_health_shared_bridge_slow_logged_at", 0.0) or 0.0
        )
        if (now_monotonic - last_logged_at) >= _INTERNAL_HEALTH_SHARED_BRIDGE_SLOW_LOG_INTERVAL_SEC:
            setattr(request.app.state, "_internal_health_shared_bridge_slow_logged_at", now_monotonic)
            add_system_log = getattr(container, "add_system_log", None)
            if callable(add_system_log):
                add_system_log(
                    f"[health] shared_bridge snapshot took {elapsed * 1000:.0f}ms (mode={snapshot_mode})",
                    source="system",
                )
    return _sanitize_shared_bridge_snapshot_for_role(snapshot, role_mode=role_mode)


def _shared_bridge_service_or_raise(container):
    if not _shared_bridge_is_available(container):
        raise HTTPException(status_code=409, detail="共享桥接未启用或共享目录未配置")
    bridge_service = getattr(container, "shared_bridge_service", None)
    if bridge_service is None:
        raise HTTPException(status_code=409, detail="共享桥接服务未初始化")
    return bridge_service


def _current_hour_bucket() -> str:
    return datetime.now().strftime("%Y-%m-%d %H")


def _cache_wait_detail(message: str) -> HTTPException:
    return HTTPException(status_code=409, detail=message)


def _filter_accessible_cached_entries(entries: Any) -> list[Dict[str, Any]]:
    output: list[Dict[str, Any]] = []
    for item in entries if isinstance(entries, list) else []:
        if not isinstance(item, dict):
            continue
        building = str(item.get("building", "") or "").strip()
        file_path = str(item.get("file_path", "") or "").strip()
        if not building or not file_path:
            continue
        if not is_accessible_cached_file_path(file_path):
            continue
        output.append(item)
    return output


def _format_bucket_age_hours_text(value: Any) -> str:
    try:
        age_hours = float(value)
    except (TypeError, ValueError):
        return ""
    if age_hours <= 0:
        return "0 小时"
    rounded = round(age_hours, 1)
    if rounded.is_integer():
        return f"{int(rounded)} 小时"
    return f"{rounded:.1f} 小时"


def _normalize_latest_cache_selection(selection: Any) -> Dict[str, Any]:
    payload = selection if isinstance(selection, dict) else {}
    best_bucket_age_hours_raw = payload.get("best_bucket_age_hours")
    try:
        best_bucket_age_hours = float(best_bucket_age_hours_raw)
    except (TypeError, ValueError):
        best_bucket_age_hours = None
    is_best_bucket_too_old = bool(payload.get("is_best_bucket_too_old", False))
    return {
        "best_bucket_key": str(payload.get("best_bucket_key", "") or "").strip(),
        "best_bucket_age_hours": best_bucket_age_hours,
        "is_best_bucket_too_old": is_best_bucket_too_old,
        "selected_entries": _filter_accessible_cached_entries(payload.get("selected_entries", [])),
        "fallback_buildings": [
            str(item or "").strip()
            for item in payload.get("fallback_buildings", [])
            if str(item or "").strip()
        ] if isinstance(payload.get("fallback_buildings", []), list) else [],
        "missing_buildings": [
            str(item or "").strip()
            for item in payload.get("missing_buildings", [])
            if str(item or "").strip()
        ] if isinstance(payload.get("missing_buildings", []), list) else [],
        "stale_buildings": [
            str(item or "").strip()
            for item in payload.get("stale_buildings", [])
            if str(item or "").strip()
        ] if isinstance(payload.get("stale_buildings", []), list) else [],
        "blocked_buildings": [
            {
                "building": str(item.get("building", "") or "").strip(),
                "reason": str(item.get("reason", "") or "").strip(),
                "failure_kind": str(item.get("failure_kind", "") or "").strip(),
                "next_probe_at": str(item.get("next_probe_at", "") or "").strip(),
            }
            for item in payload.get("blocked_buildings", [])
            if isinstance(item, dict) and str(item.get("building", "") or "").strip()
        ] if isinstance(payload.get("blocked_buildings", []), list) else [],
        "buildings": payload.get("buildings", []) if isinstance(payload.get("buildings", []), list) else [],
        "can_proceed": bool(payload.get("can_proceed", False)) and not is_best_bucket_too_old,
    }


def _build_latest_cache_wait_detail(*, feature_name: str, selection: Dict[str, Any]) -> str:
    best_bucket_key = str(selection.get("best_bucket_key", "") or "").strip() if isinstance(selection, dict) else ""
    best_bucket_age_hours = selection.get("best_bucket_age_hours") if isinstance(selection, dict) else None
    is_best_bucket_too_old = bool(selection.get("is_best_bucket_too_old", False)) if isinstance(selection, dict) else False
    missing_buildings = selection.get("missing_buildings", []) if isinstance(selection, dict) else []
    stale_buildings = selection.get("stale_buildings", []) if isinstance(selection, dict) else []
    fallback_buildings = selection.get("fallback_buildings", []) if isinstance(selection, dict) else []
    blocked_buildings = selection.get("blocked_buildings", []) if isinstance(selection, dict) else []
    if is_best_bucket_too_old:
        age_text = _format_bucket_age_hours_text(best_bucket_age_hours)
        bucket_text = best_bucket_key or "未知时间桶"
        if age_text:
            return (
                f"等待最新共享文件更新：{feature_name}源文件当前最新时间桶 {bucket_text} 距现在约 {age_text}，已超过 3 小时。"
            )
        return f"等待最新共享文件更新：{feature_name}源文件当前最新时间桶 {bucket_text} 已超过 3 小时。"
    if stale_buildings:
        return (
            f"等待过旧楼栋共享文件更新：{feature_name}源文件已有回退版本，但以下楼栋较最新时间桶落后超过 3 桶："
            + " / ".join(str(item) for item in stale_buildings)
        )
    if blocked_buildings:
        blocked_text = " / ".join(
            f"{str(item.get('building', '') or '').strip()} {str(item.get('reason', '') or '').strip()}".strip()
            for item in blocked_buildings
            if isinstance(item, dict)
        ).strip()
        if blocked_text:
            return f"等待内网恢复：{blocked_text}"
    if missing_buildings:
        return (
            f"等待缺失楼栋共享文件补齐：{feature_name}源文件尚未登记或文件不可访问，缺失楼栋："
            + " / ".join(str(item) for item in missing_buildings)
        )
    if fallback_buildings:
        return (
            f"等待共享文件就绪：{feature_name}源文件存在回退楼栋，请等待共享文件更新后自动重试。"
        )
    return f"等待共享文件就绪：{feature_name}源文件尚未登记或尚未完成下载"


def _bridge_proxy_job(task: Dict[str, Any], *, name: str, feature: str) -> Dict[str, Any]:
    task_id = str(task.get("task_id", "") or "").strip()
    status = str(task.get("status", "") or "queued_for_internal").strip() or "queued_for_internal"
    summary = "共享桥接任务已创建，等待共享文件补采或更新完成。"
    if status == "ready_for_external":
        summary = "共享桥接任务已进入外网继续处理阶段。"
    resource_key = "shared_bridge:generic"
    feature_text = str(feature or "").strip().lower()
    if "handover" in feature_text:
        resource_key = "shared_bridge:handover"
    elif "day_metric" in feature_text:
        resource_key = "shared_bridge:day_metric"
    elif "wet_bulb" in feature_text:
        resource_key = "shared_bridge:wet_bulb"
    elif (
        "monthly" in feature_text
        or "auto_once" in feature_text
        or "multi_date" in feature_text
        or "resume_upload" in feature_text
    ):
        resource_key = "shared_bridge:monthly_report"
    return {
        "job_id": f"bridge:{task_id}" if task_id else "bridge:pending",
        "name": name,
        "feature": feature,
        "submitted_by": "manual",
        "status": status,
        "priority": "manual",
        "resource_keys": [resource_key],
        "wait_reason": "waiting:shared_bridge",
        "summary": summary,
        "result": {"bridge_task_id": task_id},
        "kind": "bridge",
    }


def _accepted_bridge_task_response(task: Dict[str, Any], *, name: str, feature: str) -> Dict[str, Any]:
    return {
        "ok": True,
        "accepted": True,
        "bridge_task": task,
        "job": _bridge_proxy_job(task, name=name, feature=feature),
    }


def _get_or_create_bridge_task(
    bridge_service,
    *,
    get_or_create_name: str,
    create_name: str,
    **kwargs,
) -> Dict[str, Any]:
    get_or_create = getattr(bridge_service, get_or_create_name, None)
    if callable(get_or_create):
        return get_or_create(**kwargs)
    create = getattr(bridge_service, create_name, None)
    if callable(create):
        return create(**kwargs)
    raise AttributeError(f"共享桥接服务缺少方法: {get_or_create_name}/{create_name}")


def _role_restart_signature(cfg: Dict[str, Any], *, include_role_mode: bool = True) -> str:
    common = cfg.get("common", {}) if isinstance(cfg, dict) else {}
    deployment = common.get("deployment", {}) if isinstance(common, dict) else {}
    shared_bridge = common.get("shared_bridge", {}) if isinstance(common, dict) else {}
    role_mode = normalize_role_mode(deployment.get("role_mode"))
    resolved_shared_bridge = resolve_shared_bridge_paths(
        shared_bridge if isinstance(shared_bridge, dict) else {},
        role_mode,
    )
    payload = {
        "node_id": str(deployment.get("node_id", "") or "").strip(),
        "node_label": str(deployment.get("node_label", "") or "").strip(),
        "bridge_enabled": bool(resolved_shared_bridge.get("enabled", False)),
        "bridge_root_dir": str(resolved_shared_bridge.get("root_dir", "") or "").strip(),
        "bridge_internal_root_dir": str(resolved_shared_bridge.get("internal_root_dir", "") or "").strip(),
        "bridge_external_root_dir": str(resolved_shared_bridge.get("external_root_dir", "") or "").strip(),
    }
    if include_role_mode:
        payload["role_mode"] = role_mode
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _empty_followup_progress() -> Dict[str, Any]:
    return {
        "status": "idle",
        "can_resume_followup": False,
        "pending_count": 0,
        "failed_count": 0,
        "day_metric_pending_count": 0,
        "attachment_pending_count": 0,
        "cloud_pending_count": 0,
        "daily_report_status": "idle",
    }


def _empty_handover_review_status() -> Dict[str, Any]:
    return {
        "batch_key": "",
        "duty_date": "",
        "duty_shift": "",
        "has_any_session": False,
        "confirmed_count": 0,
        "required_count": 5,
        "all_confirmed": False,
        "ready_for_followup_upload": False,
        "buildings": [],
        "followup_progress": _empty_followup_progress(),
    }


def _empty_handover_review_access() -> Dict[str, Any]:
    return {
        "review_links": [],
        "review_base_url": "",
        "review_base_url_effective": "",
        "review_base_url_effective_source": "",
        "review_base_url_candidates": [],
        "review_base_url_status": "",
        "review_base_url_error": "",
        "review_base_url_validated_candidates": [],
        "review_base_url_candidate_results": [],
        "review_base_url_manual_available": True,
        "configured": False,
        "review_base_url_configured_at": "",
        "review_base_url_last_probe_at": "",
    }


def _job_resource_keys(*resource_keys: str, batch_key: str = "") -> list[str]:
    keys: list[str] = []
    for item in resource_keys:
        text = str(item or "").strip()
        if text and text not in keys:
            keys.append(text)
    batch_text = str(batch_key or "").strip()
    if batch_text:
        resource_key = f"handover_batch:{batch_text}"
        if resource_key not in keys:
            keys.append(resource_key)
    return keys


def _normalize_dedupe_scalar(value: Any, *, lower: bool = False) -> str:
    text = str(value or "").strip()
    return text.lower() if lower else text


def _normalize_dedupe_list(values: list[Any] | tuple[Any, ...] | None, *, lower: bool = False) -> list[str]:
    normalized: list[str] = []
    for item in values or []:
        text = _normalize_dedupe_scalar(item, lower=lower)
        if text and text not in normalized:
            normalized.append(text)
    return sorted(normalized)


def _job_dedupe_key(kind: str, **payload: Any) -> str:
    normalized_payload: Dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, (list, tuple)):
            items = _normalize_dedupe_list(list(value))
            if items:
                normalized_payload[key] = items
            continue
        text = _normalize_dedupe_scalar(value)
        if text:
            normalized_payload[key] = text
    payload_text = json.dumps(normalized_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{str(kind or '').strip()}:{payload_text}"


def _start_background_job(
    container,
    *,
    name: str,
    run_func,
    resource_keys: list[str] | tuple[str, ...] | None = None,
    priority: str = "manual",
    feature: str = "",
    dedupe_key: str = "",
    submitted_by: str = "manual",
    worker_handler: str = "",
    worker_payload: Dict[str, Any] | None = None,
):
    job_service = container.job_service
    job_kwargs = {
        "resource_keys": resource_keys,
        "priority": priority,
        "feature": feature,
        "submitted_by": submitted_by,
    }
    if str(dedupe_key or "").strip():
        job_kwargs["dedupe_key"] = str(dedupe_key or "").strip()
    if worker_handler and hasattr(job_service, "start_worker_job"):
        return job_service.start_worker_job(
            name=name,
            worker_handler=worker_handler,
            worker_payload=worker_payload or {},
            **job_kwargs,
        )
    return job_service.start_job(
        name=name,
        run_func=run_func,
        **job_kwargs,
    )


def _review_access_now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _review_access_state_template() -> Dict[str, Any]:
    return {
        "configured": False,
        "effective_base_url": "",
        "effective_source": "",
        "candidates": [],
        "validated_candidates": [],
        "candidate_results": [],
        "status": "",
        "error": "",
        "configured_at": "",
        "last_probe_at": "",
    }


def _strip_retired_wet_bulb_fields(cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(cfg, dict):
        return cfg
    wet_cfg = cfg.get("features", {}).get("wet_bulb_collection", {})
    if not isinstance(wet_cfg, dict):
        return cfg
    wet_cfg.pop("manual_button_enabled", None)
    target = wet_cfg.get("target", {})
    if isinstance(target, dict):
        target.pop("base_url", None)
        target.pop("wiki_url", None)
    return cfg


def _resolve_review_access_state_path(container) -> Path:
    runtime_root = resolve_runtime_state_root(
        runtime_config=_runtime_state_config(container),
        app_dir=get_app_dir(),
    )
    return runtime_root / _REVIEW_ACCESS_STATE_FILE_NAME


def _normalize_review_access_state(raw: Any) -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    base = _review_access_state_template()
    base["configured"] = bool(payload.get("configured", False))
    base["effective_base_url"] = _normalize_review_base_url(payload.get("effective_base_url", ""))
    source = str(payload.get("effective_source", "") or "").strip().lower()
    base["effective_source"] = source if source in {"manual", "auto"} else ""
    base["candidates"] = [str(item or "").strip() for item in payload.get("candidates", []) if str(item or "").strip()]
    base["validated_candidates"] = (
        copy.deepcopy(payload.get("validated_candidates", []))
        if isinstance(payload.get("validated_candidates", []), list)
        else []
    )
    base["candidate_results"] = (
        copy.deepcopy(payload.get("candidate_results", []))
        if isinstance(payload.get("candidate_results", []), list)
        else []
    )
    base["status"] = str(payload.get("status", "") or "").strip()
    base["error"] = str(payload.get("error", "") or "").strip()
    base["configured_at"] = str(payload.get("configured_at", "") or "").strip()
    base["last_probe_at"] = str(payload.get("last_probe_at", "") or "").strip()
    return base


def _load_review_access_state(container) -> Dict[str, Any]:
    path = _resolve_review_access_state_path(container)
    if not path.exists():
        return _review_access_state_template()
    try:
        return _normalize_review_access_state(json.loads(path.read_text(encoding="utf-8")))
    except Exception:  # noqa: BLE001
        return _review_access_state_template()


def _save_review_access_state(container, state: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_review_access_state(state)
    path = _resolve_review_access_state_path(container)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    return normalized


def _build_review_links_for_base_url(review_cfg: Dict[str, Any], effective_base_url: str) -> list[Dict[str, str]]:
    links: list[Dict[str, str]] = []
    if not effective_base_url:
        return links
    building_items = (
        review_cfg.get("buildings", [])
        if isinstance(review_cfg.get("buildings", []), list) and review_cfg.get("buildings", [])
        else _DEFAULT_REVIEW_BUILDINGS
    )
    for item in building_items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code", "") or "").strip().lower()
        name = str(item.get("name", "") or "").strip()
        if not code:
            continue
        links.append(
            {
                "building": name or code.upper(),
                "code": code,
                "url": f"{effective_base_url}/handover/review/{code}",
            }
        )
    return links


def _materialize_review_access_snapshot(
    container,
    *,
    duty_date: str = "",
    duty_shift: str = "",
    state: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    config = container.config if isinstance(getattr(container, "config", None), dict) else {}
    console_cfg = config.get("common", {}).get("console", {}) if isinstance(config.get("common", {}), dict) else {}
    review_cfg = (
        config.get("features", {}).get("handover_log", {}).get("review_ui", {})
        if isinstance(config.get("features", {}), dict)
        else {}
    )
    configured_base_url = _normalize_review_base_url(
        review_cfg.get("public_base_url", "") if isinstance(review_cfg, dict) else ""
    )
    persisted = _normalize_review_access_state(state or _load_review_access_state(container))
    probe_enabled, disabled_status, disabled_error = _review_access_probe_guard(container)
    effective_base_url = ""
    effective_source = ""
    status = "no_candidate"
    error = ""

    if not probe_enabled:
        status = disabled_status
        error = disabled_error
    elif configured_base_url:
        effective_base_url = configured_base_url
        effective_source = "manual"
        status = "manual_ok"
    elif persisted.get("effective_base_url") and persisted.get("effective_source") == "auto":
        effective_base_url = str(persisted.get("effective_base_url", "") or "").strip()
        effective_source = "auto"
        status = str(persisted.get("status", "") or "auto_ok").strip() or "auto_ok"
        error = str(persisted.get("error", "") or "").strip()
    elif bool(persisted.get("configured", False)):
        status = "manual_only"
    else:
        status = str(persisted.get("status", "") or "no_candidate").strip() or "no_candidate"
        error = str(persisted.get("error", "") or "").strip()

    return {
        "configured": bool(persisted.get("configured", False)) or bool(configured_base_url),
        "review_base_url": configured_base_url,
        "review_base_url_effective": effective_base_url,
        "review_base_url_effective_source": effective_source,
        "review_base_url_candidates": list(persisted.get("candidates", [])) if probe_enabled else [],
        "review_base_url_status": status,
        "review_base_url_error": "" if effective_base_url else error,
        "review_base_url_validated_candidates": (
            copy.deepcopy(persisted.get("validated_candidates", [])) if probe_enabled else []
        ),
        "review_base_url_candidate_results": (
            copy.deepcopy(persisted.get("candidate_results", [])) if probe_enabled else []
        ),
        "review_base_url_manual_available": True,
        "review_base_url_configured_at": str(persisted.get("configured_at", "") or "").strip(),
        "review_base_url_last_probe_at": str(persisted.get("last_probe_at", "") or "").strip(),
        "duty_date": str(duty_date or "").strip(),
        "duty_shift": str(duty_shift or "").strip().lower(),
        "review_links": _build_review_links_for_base_url(
            review_cfg if isinstance(review_cfg, dict) else {},
            effective_base_url,
        ) if probe_enabled else [],
        "console_port": _resolve_console_port(console_cfg),
    }


def _persist_manual_review_access_snapshot(container) -> Dict[str, Any]:
    config = container.config if isinstance(getattr(container, "config", None), dict) else {}
    review_cfg = (
        config.get("features", {}).get("handover_log", {}).get("review_ui", {})
        if isinstance(config.get("features", {}), dict)
        else {}
    )
    configured_base_url = _normalize_review_base_url(
        review_cfg.get("public_base_url", "") if isinstance(review_cfg, dict) else ""
    )
    previous = _load_review_access_state(container)
    now_text = _review_access_now_text()
    if not configured_base_url:
        return _materialize_review_access_snapshot(container, state=previous)
    state = _normalize_review_access_state(previous)
    state["configured"] = True
    state["effective_base_url"] = configured_base_url
    state["effective_source"] = "manual"
    state["status"] = "manual_ok"
    state["error"] = ""
    state["configured_at"] = str(state.get("configured_at", "") or "").strip() or now_text
    return _materialize_review_access_snapshot(container, state=_save_review_access_state(container, state))


def _probe_and_persist_review_access_snapshot(
    container,
    *,
    request_host: str = "",
    port: int | None = None,
    duty_date: str = "",
    duty_shift: str = "",
) -> Dict[str, Any]:
    config = container.config if isinstance(getattr(container, "config", None), dict) else {}
    console_cfg = config.get("common", {}).get("console", {}) if isinstance(config.get("common", {}), dict) else {}
    resolved_port = _resolve_runtime_console_bind_port(container) if port is None else _resolve_console_port(
        console_cfg,
        request_port=port,
    )
    review_cfg = (
        config.get("features", {}).get("handover_log", {}).get("review_ui", {})
        if isinstance(config.get("features", {}), dict)
        else {}
    )
    probe_enabled, disabled_status, disabled_error = _review_access_probe_guard(container)
    configured_base_url = _normalize_review_base_url(
        review_cfg.get("public_base_url", "") if isinstance(review_cfg, dict) else ""
    )
    previous = _load_review_access_state(container)
    now_text = _review_access_now_text()
    state = _normalize_review_access_state(previous)
    if not probe_enabled:
        state["configured"] = bool(previous.get("configured", False)) or bool(configured_base_url)
        state["effective_base_url"] = ""
        state["effective_source"] = ""
        state["candidates"] = []
        state["validated_candidates"] = []
        state["candidate_results"] = []
        state["status"] = disabled_status
        state["error"] = disabled_error
        state["last_probe_at"] = now_text
        state["configured_at"] = str(state.get("configured_at", "") or "").strip() or (
            now_text if state["configured"] else ""
        )
        persisted = _save_review_access_state(container, state)
        return _materialize_review_access_snapshot(
            container,
            duty_date=duty_date,
            duty_shift=duty_shift,
            state=persisted,
        )

    candidate_hosts = _detect_lan_ipv4s(request_host=request_host)
    candidate_base_urls = [f"http://{host}:{resolved_port}" for host in candidate_hosts]
    probe_targets = _review_probe_targets(review_cfg if isinstance(review_cfg, dict) else {})
    candidate_results = _probe_review_base_urls_cached(candidate_base_urls, probe_targets)
    validated_candidates = [copy.deepcopy(item) for item in candidate_results if bool(item.get("ok"))]
    auto_success = validated_candidates[0] if validated_candidates else None

    state["configured"] = bool(previous.get("configured", False)) or bool(configured_base_url) or bool(auto_success)
    state["candidates"] = candidate_base_urls
    state["validated_candidates"] = validated_candidates
    state["candidate_results"] = candidate_results
    state["last_probe_at"] = now_text
    state["configured_at"] = str(state.get("configured_at", "") or "").strip() or (now_text if state["configured"] else "")

    if configured_base_url:
        state["effective_base_url"] = configured_base_url
        state["effective_source"] = "manual"
        state["status"] = "manual_ok"
        state["error"] = ""
    elif auto_success:
        state["effective_base_url"] = str(auto_success.get("base_url", "") or "").strip()
        state["effective_source"] = "auto"
        state["status"] = "auto_ok"
        state["error"] = ""
    elif candidate_base_urls:
        state["effective_base_url"] = ""
        state["effective_source"] = ""
        state["status"] = "auto_unreachable"
        state["error"] = "已检测到 IPv4，但审核页面均不可访问"
    else:
        state["effective_base_url"] = ""
        state["effective_source"] = ""
        state["status"] = "no_candidate"
        state["error"] = "未检测到可用私网 IPv4 地址"

    persisted = _save_review_access_state(container, state)
    return _materialize_review_access_snapshot(
        container,
        duty_date=duty_date,
        duty_shift=duty_shift,
        state=persisted,
    )


def _build_bootstrap_health_payload(container, request: Request) -> Dict[str, Any]:
    active_job_id = str(container.job_service.active_job_id() or "").strip()
    active_job_ids = container.job_service.active_job_ids(include_waiting=True)
    get_next_offset = getattr(container, "system_log_next_offset", None)
    system_log_next_offset = int(get_next_offset() or 0) if callable(get_next_offset) else 0
    startup_time = str(getattr(request.app.state, "started_at", "") or "").strip()
    deployment_snapshot = container.deployment_snapshot() if hasattr(container, "deployment_snapshot") else {}
    if not isinstance(deployment_snapshot, dict):
        deployment_snapshot = {"role_mode": "", "node_id": "", "node_label": ""}
    role_mode = str(deployment_snapshot.get("role_mode", "") or "").strip().lower()
    last_started_role_mode = str(deployment_snapshot.get("last_started_role_mode", "") or "").strip().lower()
    if role_mode not in {"internal", "external"}:
        deployment_snapshot = {
            **deployment_snapshot,
            "role_mode": "",
            "last_started_role_mode": "",
            "node_id": str(deployment_snapshot.get("node_id", "") or "").strip(),
            "node_label": str(deployment_snapshot.get("node_label", "") or "").strip(),
        }
        role_mode = ""
        last_started_role_mode = ""
    elif last_started_role_mode not in {"internal", "external"}:
        deployment_snapshot = {
            **deployment_snapshot,
            "last_started_role_mode": "",
        }
    runtime_activated = bool(getattr(request.app.state, "runtime_services_activated", False))
    activation_phase = str(getattr(request.app.state, "runtime_activation_phase", "") or "").strip()
    activation_error = str(getattr(request.app.state, "runtime_activation_error", "") or "").strip()
    startup_role_confirmed = bool(getattr(request.app.state, "startup_role_confirmed", False))
    startup_handoff = {
        "active": False,
        "mode": "",
        "target_role_mode": "",
        "requested_at": "",
        "reason": "",
        "nonce": "",
    }
    get_startup_role_handoff = getattr(container, "get_startup_role_handoff", None)
    if callable(get_startup_role_handoff):
        try:
            handoff_payload = get_startup_role_handoff()
        except Exception:  # noqa: BLE001
            handoff_payload = {}
        if isinstance(handoff_payload, dict):
            startup_handoff = {
                "active": bool(handoff_payload.get("active", False)),
                "mode": str(handoff_payload.get("mode", "") or "").strip(),
                "target_role_mode": str(handoff_payload.get("target_role_mode", "") or "").strip(),
                "requested_at": str(handoff_payload.get("requested_at", "") or "").strip(),
                "reason": str(handoff_payload.get("reason", "") or "").strip(),
                "nonce": str(handoff_payload.get("nonce", "") or "").strip(),
            }
    role_is_valid = role_mode in {"internal", "external"}
    has_active_resume = False
    if runtime_activated and role_mode == "external":
        try:
            pending_runs = OrchestratorService(_runtime_config(container)).list_pending_resume_runs()
            has_active_resume = bool(pending_runs)
        except Exception:  # noqa: BLE001
            has_active_resume = False
    return {
        "ok": True,
        "version": str(getattr(container, "version", "") or ""),
        "frontend_mode": str(getattr(container, "frontend_mode", "") or ""),
        "active_job_id": active_job_id,
        "active_job_ids": active_job_ids,
        "job_counts": container.job_service.job_counts(),
        "current_job": active_job_id or None,
        "system_log_next_offset": system_log_next_offset,
        "has_active_resume": has_active_resume,
        "startup_time": startup_time,
        "deployment": deployment_snapshot,
        "startup_role_confirmed": startup_role_confirmed,
        "role_selection_required": (not startup_role_confirmed) or (not role_is_valid),
        "startup_handoff": startup_handoff,
        "runtime_activated": runtime_activated,
        "activation_phase": activation_phase,
        "activation_error": activation_error,
    }


def _normalize_handover_duty_filters(duty_date: str = "", duty_shift: str = "") -> tuple[str, str]:
    duty_date_text = str(duty_date or "").strip()
    duty_shift_text = str(duty_shift or "").strip().lower()
    if not duty_date_text or duty_shift_text not in {"day", "night"}:
        return "", ""
    try:
        datetime.strptime(duty_date_text, "%Y-%m-%d")
    except ValueError:
        return "", ""
    return duty_date_text, duty_shift_text


def _is_private_ipv4(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    try:
        parsed = ip_address(raw)
    except ValueError:
        return False
    return parsed.version == 4 and parsed.is_private and not parsed.is_loopback


def _decode_console_output(raw: bytes) -> str:
    data = raw if isinstance(raw, bytes) else b""
    for encoding in ("utf-8", "gbk", "cp936", "mbcs", "latin-1"):
        try:
            return data.decode(encoding, errors="ignore")
        except LookupError:
            continue
    return data.decode("utf-8", errors="ignore")


def _collect_windows_ipconfig_ipv4s() -> list[tuple[str, str]]:
    try:
        completed = subprocess.run(
            ["ipconfig"],
            capture_output=True,
            timeout=3,
            check=False,
        )
    except Exception:  # noqa: BLE001
        return []
    output = _decode_console_output(bytes(completed.stdout or b""))
    if not output.strip():
        return []
    entries: list[tuple[str, str]] = []
    current_adapter = ""
    for raw_line in output.splitlines():
        line = str(raw_line or "").rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        if line == stripped and stripped.endswith(":"):
            current_adapter = stripped[:-1].strip()
            continue
        if "ipv4" not in stripped.lower():
            continue
        match = _IPCONFIG_IPV4_RE.search(stripped)
        if not match:
            continue
        ip_text = match.group(0)
        adapter_text = str(current_adapter or "").strip().lower()
        if not adapter_text:
            continue
        if any(keyword in adapter_text for keyword in _VIRTUAL_ADAPTER_KEYWORDS):
            continue
        if not any(keyword in adapter_text for keyword in _PREFERRED_ADAPTER_KEYWORDS):
            continue
        if _is_private_ipv4(ip_text):
            entries.append((ip_text, current_adapter))
    return entries


def _lan_candidate_priority(*, source: str, adapter_name: str = "") -> int:
    source_text = str(source or "").strip().lower()
    if source_text == "request":
        return 0
    if source_text == "ipconfig":
        return 20
    return 50


def _detect_lan_ipv4s(request_host: str = "") -> list[str]:
    ordered: dict[str, tuple[int, int]] = {}
    order = 0

    def _push(value: str, *, source: str, adapter_name: str = "") -> None:
        nonlocal order
        text = str(value or "").strip()
        if not _is_private_ipv4(text):
            return
        adapter_text = str(adapter_name or "").strip().lower()
        if source == "ipconfig":
            if not adapter_text:
                return
            if any(keyword in adapter_text for keyword in _VIRTUAL_ADAPTER_KEYWORDS):
                return
            if not any(keyword in adapter_text for keyword in _PREFERRED_ADAPTER_KEYWORDS):
                return
        priority = _lan_candidate_priority(source=source, adapter_name=adapter_name)
        current = ordered.get(text)
        if current is None or (priority, order) < current:
            ordered[text] = (priority, order)
        order += 1

    _push(request_host, source="request")
    for ip_text, adapter_name in _collect_windows_ipconfig_ipv4s():
        _push(ip_text, source="ipconfig", adapter_name=adapter_name)
    return [item[0] for item in sorted(ordered.items(), key=lambda pair: pair[1])]


def _normalize_review_base_url(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"http://{raw}"
    parts = urlsplit(raw)
    netloc = str(parts.netloc or "").strip()
    if not netloc:
        return ""
    scheme = parts.scheme.lower() if str(parts.scheme or "").strip().lower() in {"http", "https"} else "http"
    return urlunsplit((scheme, netloc, "", "", "")).rstrip("/")


def _resolve_console_port(console_cfg: Dict[str, Any] | None, request_port: Any = None) -> int:
    try:
        if request_port:
            return int(request_port)
    except Exception:  # noqa: BLE001
        pass
    try:
        return int((console_cfg or {}).get("port", 18765))
    except Exception:  # noqa: BLE001
        return 18765


def _review_probe_targets(review_cfg: Dict[str, Any]) -> list[Dict[str, str]]:
    buildings = review_cfg.get("buildings", []) if isinstance(review_cfg, dict) else []
    targets: list[Dict[str, str]] = []
    for item in buildings if isinstance(buildings, list) else []:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code", "") or "").strip().lower()
        name = str(item.get("name", "") or "").strip()
        if not code:
            continue
        targets.append(
            {
                "code": code,
                "name": name or code.upper(),
                "path": f"/handover/review/{code}",
            }
        )
    if targets:
        return targets
    return [
        {
            "code": str(item.get("code", "") or "").strip().lower(),
            "name": str(item.get("name", "") or "").strip(),
            "path": f"/handover/review/{str(item.get('code', '') or '').strip().lower()}",
        }
        for item in _DEFAULT_REVIEW_BUILDINGS
    ]


def _is_review_probe_success(status_code: int, content_type: str, body_text: str) -> bool:
    content = str(content_type or "").lower()
    body_lower = str(body_text or "").lower()
    if status_code != 200:
        return False
    if "text/html" not in content:
        return False
    has_html_shell = "<html" in body_lower or "<!doctype html" in body_lower
    has_frontend_marker = "/assets/" in body_lower or "/assets-src/" in body_lower or "id=\"app\"" in body_lower
    return has_html_shell and has_frontend_marker


def _probe_review_target_url(base_url: str, probe_path: str, *, timeout_sec: float = 1.5) -> Dict[str, Any]:
    normalized_base = _normalize_review_base_url(base_url)
    result = {"base_url": normalized_base, "ok": False, "error": "", "path": str(probe_path or "").strip()}
    if not normalized_base:
        result["error"] = "empty_base_url"
        return result

    target_url = f"{normalized_base}{probe_path}"
    request = urllib.request.Request(
        target_url,
        headers={
            "Accept": "text/html,application/xhtml+xml",
            "User-Agent": "QJPT-ReviewProbe/1.0",
        },
        method="GET",
    )
    status_code = 0
    content_type = ""
    body_text = ""
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            status_code = int(getattr(response, "status", 200) or 200)
            content_type = str(response.headers.get("Content-Type", "") or "")
            body_text = response.read(4096).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        status_code = int(getattr(exc, "code", 0) or 0)
        content_type = str(exc.headers.get("Content-Type", "") or "")
        body_text = exc.read(4096).decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        result["error"] = str(exc)
        return result

    if _is_review_probe_success(status_code, content_type, body_text):
        result["ok"] = True
        return result

    result["error"] = f"http_{status_code or 'error'}"
    return result


def _probe_review_base_url(
    base_url: str,
    probe_targets: list[Dict[str, str]],
    *,
    timeout_sec: float = 1.5,
) -> Dict[str, Any]:
    normalized_base = _normalize_review_base_url(base_url)
    targets = probe_targets if isinstance(probe_targets, list) else []
    result: Dict[str, Any] = {
        "base_url": normalized_base,
        "ok": False,
        "error": "",
        "probes": [],
    }
    if not normalized_base:
        result["error"] = "empty_base_url"
        return result
    if not targets:
        result["error"] = "empty_probe_targets"
        return result

    all_ok = True
    first_error = ""
    probe_results: list[Dict[str, Any]] = []
    for target in targets:
        path = str(target.get("path", "") or "").strip()
        code = str(target.get("code", "") or "").strip().lower()
        name = str(target.get("name", "") or "").strip()
        target_result = _probe_review_target_url(normalized_base, path, timeout_sec=timeout_sec)
        target_result["code"] = code
        target_result["name"] = name or code.upper()
        probe_results.append(target_result)
        if not bool(target_result.get("ok")):
            all_ok = False
            if not first_error:
                first_error = str(target_result.get("error", "") or "").strip() or "probe_failed"
    result["ok"] = all_ok
    result["error"] = "" if all_ok else first_error or "probe_failed"
    result["probes"] = probe_results
    return result


def _probe_review_base_urls_cached(
    base_urls: list[str],
    probe_targets: list[Dict[str, str]],
    *,
    timeout_sec: float = 1.5,
) -> list[Dict[str, Any]]:
    normalized_urls = tuple(
        value for value in (_normalize_review_base_url(item) for item in base_urls) if value
    )
    if not normalized_urls:
        return []
    normalized_targets = tuple(
        (
            str(item.get("code", "") or "").strip().lower(),
            str(item.get("name", "") or "").strip(),
            str(item.get("path", "") or "").strip(),
        )
        for item in (probe_targets if isinstance(probe_targets, list) else [])
        if str(item.get("path", "") or "").strip()
    )
    if not normalized_targets:
        normalized_targets = tuple(
            (item["code"], item["name"], item["path"]) for item in _review_probe_targets({})
        )
    cache_key = (normalized_urls, normalized_targets)
    now = time.time()
    with _review_base_probe_lock:
        cached = _review_base_probe_cache.get(cache_key)
        if cached and (now - float(cached.get("checked_at", 0.0) or 0.0)) < _REVIEW_BASE_PROBE_TTL_SEC:
            return copy.deepcopy(cached.get("result", []))
    results = [
        _probe_review_base_url(
            base_url,
            [
                {"code": code, "name": name, "path": path}
                for code, name, path in cache_key[1]
            ],
            timeout_sec=timeout_sec,
        )
        for base_url in normalized_urls
    ]
    with _review_base_probe_lock:
        _review_base_probe_cache[cache_key] = {
            "checked_at": now,
            "result": copy.deepcopy(results),
        }
    return results


def _invalidate_review_base_probe_cache() -> None:
    with _review_base_probe_lock:
        _review_base_probe_cache.clear()


def _build_handover_review_access_for_context(
    container,
    *,
    request_host: str = "",
    port: int | None = None,
    duty_date: str = "",
    duty_shift: str = "",
) -> Dict[str, Any]:
    _ = (request_host, port)
    return _materialize_review_access_snapshot(
        container,
        duty_date=duty_date,
        duty_shift=duty_shift,
    )


def _build_handover_review_access(
    container,
    request: Request,
    *,
    duty_date: str = "",
    duty_shift: str = "",
) -> Dict[str, Any]:
    return _build_handover_review_access_for_context(
        container,
        request_host=str(getattr(request.url, "hostname", "") or "").strip(),
        port=getattr(request.url, "port", None),
        duty_date=duty_date,
        duty_shift=duty_shift,
    )


def schedule_handover_review_access_startup_probe(
    container,
    *,
    delay_sec: float = _REVIEW_BASE_STARTUP_PROBE_DELAY_SEC,
) -> None:
    if getattr(container, "_handover_review_access_probe_scheduled", False):
        return
    probe_enabled, _, disabled_error = _review_access_probe_guard(container)
    if not probe_enabled:
        setattr(container, "_handover_review_access_probe_scheduled", True)
        if disabled_error:
            try:
                container.add_system_log(f"[交接班审核访问] 已跳过启动探测: {disabled_error}")
            except Exception:  # noqa: BLE001
                pass
        return
    current_snapshot = _materialize_review_access_snapshot(container)
    if bool(current_snapshot.get("configured", False)):
        setattr(container, "_handover_review_access_probe_scheduled", True)
        try:
            container.add_system_log(
                "[交接班审核访问] 已加载持久化配置，启动不再自动探测 "
                f"effective={str(current_snapshot.get('review_base_url_effective', '') or '-').strip() or '-'}, "
                f"source={str(current_snapshot.get('review_base_url_effective_source', '') or '-').strip() or '-'}"
            )
        except Exception:  # noqa: BLE001
            pass
        return
    setattr(container, "_handover_review_access_probe_scheduled", True)

    def _runner() -> None:
        try:
            time.sleep(max(0.0, float(delay_sec or 0.0)))
            snapshot = _probe_and_persist_review_access_snapshot(container)
            container.add_system_log(
                "[交接班审核访问] 启动探测完成: "
                f"effective={str(snapshot.get('review_base_url_effective', '') or '-').strip() or '-'}, "
                f"source={str(snapshot.get('review_base_url_effective_source', '') or '-').strip() or '-'}, "
                f"validated={len(snapshot.get('review_base_url_validated_candidates', []) or [])}"
            )
        except Exception as exc:  # noqa: BLE001
            try:
                container.add_system_log(f"[交接班审核访问] 启动探测失败: {exc}")
            except Exception:  # noqa: BLE001
                pass

    threading.Thread(
        target=_runner,
        name="handover-review-access-probe",
        daemon=True,
    ).start()


def _collect_config_warnings(payload: Dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if not isinstance(payload, dict):
        return warnings
    legacy_roots = {
        "download",
        "network",
        "scheduler",
        "notify",
        "feishu",
        "feishu_sheet_import",
        "handover_log",
        "manual_upload_gui",
        "input",
        "output",
        "web",
    }
    hit = sorted([key for key in legacy_roots if key in payload])
    if hit:
        warnings.append(
            "检测到旧配置根字段: "
            + ", ".join(hit)
            + "；系统已自动迁移到 v3 结构(common/features)。"
        )
    if int(payload.get("version", 0) or 0) not in (0, 3):
        warnings.append("配置 version 非 3，系统已按 v3 结构进行标准化。")
    return warnings


@router.get("/api/health")
def health(
    request: Request,
    handover_duty_date: str = "",
    handover_duty_shift: str = "",
) -> Dict[str, Any]:
    container = request.app.state.container
    runtime_cfg = container.runtime_config
    role_mode = _deployment_role_mode(container)
    include_handover_runtime_context = role_mode != "internal"
    include_network_probe = role_mode != "internal"
    include_wet_bulb_target_preview = role_mode != "internal"
    include_day_metric_target_preview = role_mode != "internal"
    include_alarm_event_target_preview = role_mode != "internal"

    wifi_name = None
    interface_name = ""
    visible_targets: Dict[str, bool] = {"internal": False, "external": False}
    last_switch_report: Dict[str, Any] = {}
    last_wifi_result = ""
    last_wifi_error_type = ""
    last_wifi_error = ""
    try:
        if container.wifi_service:
            if include_network_probe:
                wifi_name = container.wifi_service.current_ssid()
                interface_name = container.wifi_service.current_interface_name()
                visible_targets = container.wifi_service.visible_targets()
            if include_network_probe:
                last_switch_report = container.wifi_service.get_last_switch_report()
                last_wifi_result = str(last_switch_report.get("result", "") or "")
                last_wifi_error_type = str(last_switch_report.get("error_type", "") or "")
                last_wifi_error = str(last_switch_report.get("error", "") or "")
    except Exception:  # noqa: BLE001
        wifi_name = None

    scheduler = container.scheduler
    scheduler_runtime = scheduler.get_runtime_snapshot() if scheduler else {}
    updater_runtime = container.updater_snapshot()
    handover_scheduler_snapshot = container.handover_scheduler_status()
    wet_bulb_cfg = runtime_cfg.get("wet_bulb_collection", {}) if isinstance(runtime_cfg, dict) else {}
    if not isinstance(wet_bulb_cfg, dict):
        wet_bulb_cfg = {}
    wet_bulb_scheduler_snapshot = container.wet_bulb_collection_scheduler_status()
    wet_bulb_target_preview = (
        WetBulbCollectionService(runtime_cfg).build_target_descriptor(force_refresh=False)
        if include_wet_bulb_target_preview
        else {}
    )
    day_metric_target_preview = (
        DayMetricBitableExportService(load_handover_config(runtime_cfg)).build_target_descriptor(force_refresh=False)
        if include_day_metric_target_preview
        else {}
    )
    alarm_event_target_preview = (
        SharedSourceCacheService(runtime_config=runtime_cfg, store=None).get_alarm_event_upload_target_preview(
            force_refresh=False
        )
        if include_alarm_event_target_preview
        else {}
    )
    handover_slots = (
        handover_scheduler_snapshot.get("slots", {})
        if isinstance(handover_scheduler_snapshot.get("slots", {}), dict)
        else {}
    )
    handover_morning = handover_slots.get("morning", {}) if isinstance(handover_slots.get("morning", {}), dict) else {}
    handover_afternoon = (
        handover_slots.get("afternoon", {})
        if isinstance(handover_slots.get("afternoon", {}), dict)
        else {}
    )
    network_cfg = runtime_cfg.get("network", {}) if isinstance(runtime_cfg, dict) else {}
    app_dir = get_app_dir()
    paths_cfg = runtime_cfg.get("paths", {}) if isinstance(runtime_cfg, dict) else {}
    runtime_state_root_text = str(paths_cfg.get("runtime_state_root", "")).strip() if isinstance(paths_cfg, dict) else ""
    runtime_state_root_path = Path(runtime_state_root_text) if runtime_state_root_text else app_dir / ".runtime"
    if not runtime_state_root_path.is_absolute():
        runtime_state_root_path = app_dir / runtime_state_root_path

    scheduler_cfg = runtime_cfg.get("scheduler", {}) if isinstance(runtime_cfg, dict) else {}
    scheduler_state_file = str(scheduler_cfg.get("state_file", "") or "").strip()
    scheduler_state_path = Path(scheduler_state_file) if scheduler_state_file else runtime_state_root_path / "daily_scheduler_state.json"
    if not scheduler_state_path.is_absolute():
        scheduler_state_path = runtime_state_root_path / scheduler_state_path

    download_cfg = runtime_cfg.get("download", {}) if isinstance(runtime_cfg, dict) else {}
    resume_cfg = download_cfg.get("resume", {}) if isinstance(download_cfg, dict) else {}
    monthly_resume_root = resolve_monthly_resume_root_dir(
        app_dir=app_dir,
        root_dir=str(resume_cfg.get("root_dir", "")).strip(),
        runtime_state_root=runtime_state_root_text,
    )
    monthly_resume_index = resolve_monthly_resume_index_path(
        app_dir=app_dir,
        root_dir=str(resume_cfg.get("root_dir", "")).strip(),
        index_file=str(resume_cfg.get("index_file", "")).strip(),
        runtime_state_root=runtime_state_root_text,
    )

    handover_cfg = runtime_cfg.get("handover_log", {}) if isinstance(runtime_cfg, dict) else {}
    handover_template = handover_cfg.get("template", {}) if isinstance(handover_cfg, dict) else {}
    handover_event_sections = (
        handover_cfg.get("event_sections", {}) if isinstance(handover_cfg.get("event_sections", {}), dict) else {}
    )
    handover_event_cache = (
        handover_event_sections.get("cache", {})
        if isinstance(handover_event_sections.get("cache", {}), dict)
        else {}
    )
    handover_source_text = str(handover_template.get("source_path", "") or "").strip()
    handover_output_text = str(handover_template.get("output_dir", "") or "").strip()
    handover_source = Path(handover_source_text) if handover_source_text else None
    handover_output = Path(handover_output_text) if handover_output_text else None
    if handover_source is not None and not handover_source.is_absolute():
        handover_source = app_dir / handover_source
    if handover_output is not None and not handover_output.is_absolute():
        handover_output = app_dir / handover_output
    handover_event_cache_name = str(
        handover_event_cache.get("state_file", "") or "handover_shared_cache.json"
    ).strip() or "handover_shared_cache.json"
    handover_event_cache_path = runtime_state_root_path / handover_event_cache_name
    handover_event_pending_count = 0
    if include_handover_runtime_context:
        event_cache_store = EventFollowupCacheStore(
            cache_state_file=handover_event_cache_name,
            global_paths={"runtime_state_root": runtime_state_root_text},
        )
        handover_event_cache_path = event_cache_store.state_path
        try:
            cache_state = event_cache_store.load_state()
            pending_map = cache_state.get("pending_by_id", {})
            if isinstance(pending_map, dict):
                handover_event_pending_count = len(pending_map)
        except Exception:  # noqa: BLE001
            handover_event_pending_count = 0

    selected_duty_date, selected_duty_shift = _normalize_handover_duty_filters(
        handover_duty_date,
        handover_duty_shift,
    )

    handover_review_status: Dict[str, Any] = _empty_handover_review_status()
    handover_review_access = _empty_handover_review_access()
    if include_handover_runtime_context:
        try:
            handover_loaded_cfg = load_handover_config(runtime_cfg)
            review_service = ReviewSessionService(handover_loaded_cfg)
            followup_service = ReviewFollowupTriggerService(handover_loaded_cfg)
            if selected_duty_date and selected_duty_shift:
                handover_review_status = review_service.get_batch_status_for_duty(
                    selected_duty_date,
                    selected_duty_shift,
                )
            else:
                handover_review_status = review_service.get_latest_batch_status()
            target_batch_key = str(handover_review_status.get("batch_key", "")).strip()
            handover_review_status["followup_progress"] = (
                followup_service.get_followup_progress(target_batch_key)
                if target_batch_key
                else _empty_followup_progress()
            )
        except Exception:  # noqa: BLE001
            handover_review_status = _empty_handover_review_status()
        handover_review_access = _build_handover_review_access(
            container,
            request,
            duty_date=str(handover_review_status.get("duty_date", "")).strip(),
            duty_shift=str(handover_review_status.get("duty_shift", "")).strip().lower(),
        )
    system_logs = list(getattr(container, "system_logs", []))[-200:]
    get_log_entries = getattr(container, "get_system_log_entries", None)
    system_log_entries = get_log_entries(limit=200) if callable(get_log_entries) else []
    alert_log_entries = (
        get_log_entries(levels={"warning", "error"}, limit=200)
        if callable(get_log_entries)
        else []
    )
    get_next_offset = getattr(container, "system_log_next_offset", None)
    system_log_next_offset = int(get_next_offset() or 0) if callable(get_next_offset) else len(system_logs)
    shared_bridge_snapshot = _shared_bridge_health_snapshot(container, request, role_mode=role_mode)
    task_engine_snapshot = (
        container.task_engine_snapshot()
        if hasattr(container, "task_engine_snapshot")
        else {"write_queue_length": 0, "last_cleanup_at": "", "closed": True}
    )
    alert_log_queue_snapshot = (
        container.alert_log_uploader_snapshot()
        if hasattr(container, "alert_log_uploader_snapshot")
        else {
            "running": False,
            "pending_lines": 0,
            "queue_file_size_bytes": 0,
            "oldest_pending_at": "",
            "last_flush_at": "",
            "last_error": "",
        }
    )

    return {
        "ok": True,
        "version": container.version,
        "config_version": int(container.config.get("version", 0) or 0),
        "config_schema_status": "ok",
        "config_migrate_hint": "使用 scripts/migrate_config_to_v3.py 可执行一次性结构迁移与备份。",
        "config_path": str(container.config_path),
        "active_job_id": container.job_service.active_job_id(),
        "active_job_ids": container.job_service.active_job_ids(include_waiting=True),
        "job_counts": container.job_service.job_counts(),
        "task_engine": task_engine_snapshot,
        "system_alert_log_queue": alert_log_queue_snapshot,
        "scheduler": {
            "enabled": bool(scheduler.enabled) if scheduler else False,
                "status": scheduler.status_text() if scheduler else "未初始化",
            "next_run_time": scheduler.next_run_text() if scheduler else "",
            "executor_bound": bool(container.is_scheduler_executor_bound()),
            "callback_name": container.scheduler_executor_name(),
            "running": bool(scheduler_runtime.get("running", False)),
            "started_at": str(scheduler_runtime.get("started_at", "")),
            "last_check_at": str(scheduler_runtime.get("last_check_at", "")),
            "last_decision": str(scheduler_runtime.get("last_decision", "")),
            "last_trigger_at": str(scheduler_runtime.get("last_trigger_at", "")),
            "last_trigger_result": str(scheduler_runtime.get("last_trigger_result", "")),
            "state_path": str(scheduler_runtime.get("state_path", "")),
            "state_exists": bool(scheduler_runtime.get("state_exists", False)),
        },
        "handover_scheduler": {
            "enabled": bool(handover_scheduler_snapshot.get("enabled", False)),
            "running": bool(handover_scheduler_snapshot.get("running", False)),
                "status": str(handover_scheduler_snapshot.get("status", "未初始化")),
            "executor_bound": bool(container.is_handover_scheduler_executor_bound()),
            "callback_name": container.handover_scheduler_executor_name(),
            "morning": {
                "next_run_time": str(handover_morning.get("next_run_time", "")),
                "last_decision": str(handover_morning.get("last_decision", "")),
                "last_trigger_at": str(handover_morning.get("last_trigger_at", "")),
                "last_trigger_result": str(handover_morning.get("last_trigger_result", "")),
                "state_path": str(handover_morning.get("state_path", "")),
                "state_exists": bool(handover_morning.get("state_exists", False)),
            },
            "afternoon": {
                "next_run_time": str(handover_afternoon.get("next_run_time", "")),
                "last_decision": str(handover_afternoon.get("last_decision", "")),
                "last_trigger_at": str(handover_afternoon.get("last_trigger_at", "")),
                "last_trigger_result": str(handover_afternoon.get("last_trigger_result", "")),
                "state_path": str(handover_afternoon.get("state_path", "")),
                "state_exists": bool(handover_afternoon.get("state_exists", False)),
            },
            "state_paths": handover_scheduler_snapshot.get("state_paths", {}),
        },
        "handover": {
            "event_sections": {
                "enabled": bool(handover_event_sections.get("enabled", True)),
                "cache_state_path": str(handover_event_cache_path),
                "pending_count": int(handover_event_pending_count),
            },
            "review_status": handover_review_status,
            "review_links": handover_review_access.get("review_links", []),
            "review_base_url": str(handover_review_access.get("review_base_url", "") or ""),
            "review_base_url_effective": str(handover_review_access.get("review_base_url_effective", "") or ""),
            "review_base_url_effective_source": str(
                handover_review_access.get("review_base_url_effective_source", "") or ""
            ),
            "review_base_url_candidates": handover_review_access.get("review_base_url_candidates", []),
            "review_base_url_status": str(handover_review_access.get("review_base_url_status", "") or ""),
            "review_base_url_error": str(handover_review_access.get("review_base_url_error", "") or ""),
            "review_base_url_validated_candidates": handover_review_access.get("review_base_url_validated_candidates", []),
            "review_base_url_candidate_results": handover_review_access.get("review_base_url_candidate_results", []),
            "review_base_url_manual_available": bool(
                handover_review_access.get("review_base_url_manual_available", False)
            ),
            "configured": bool(handover_review_access.get("configured", False)),
            "review_base_url_configured_at": str(
                handover_review_access.get("review_base_url_configured_at", "") or ""
            ),
            "review_base_url_last_probe_at": str(
                handover_review_access.get("review_base_url_last_probe_at", "") or ""
            ),
        },
        "wet_bulb_collection": {
            "enabled": bool(wet_bulb_cfg.get("enabled", True)),
            "scheduler": {
                "running": bool(wet_bulb_scheduler_snapshot.get("running", False)),
                "status": str(wet_bulb_scheduler_snapshot.get("status", "未初始化")),
                "next_run_time": str(wet_bulb_scheduler_snapshot.get("next_run_time", "")),
                "last_check_at": str(wet_bulb_scheduler_snapshot.get("last_check_at", "")),
                "last_decision": str(wet_bulb_scheduler_snapshot.get("last_decision", "")),
                "last_trigger_at": str(wet_bulb_scheduler_snapshot.get("last_trigger_at", "")),
                "last_trigger_result": str(wet_bulb_scheduler_snapshot.get("last_trigger_result", "")),
                "state_path": str(wet_bulb_scheduler_snapshot.get("state_path", "")),
                "state_exists": bool(wet_bulb_scheduler_snapshot.get("state_exists", False)),
                "executor_bound": bool(container.is_wet_bulb_collection_scheduler_executor_bound()),
                "callback_name": container.wet_bulb_collection_scheduler_executor_name(),
            },
            "target_preview": wet_bulb_target_preview,
        },
        "day_metric_upload": {
            "enabled": bool(runtime_cfg.get("day_metric_upload", {}).get("enabled", True))
            if isinstance(runtime_cfg.get("day_metric_upload", {}), dict)
            else True,
            "target_preview": day_metric_target_preview,
        },
        "alarm_event_upload": {
            "target_preview": alarm_event_target_preview,
        },
        "updater": {
            "enabled": bool(runtime_cfg.get("updater", {}).get("enabled", True))
            if isinstance(runtime_cfg.get("updater", {}), dict)
            else True,
            "running": bool(updater_runtime.get("running", False)),
            "last_check_at": str(updater_runtime.get("last_check_at", "")),
            "last_result": str(updater_runtime.get("last_result", "")),
            "last_error": str(updater_runtime.get("last_error", "")),
            "local_version": str(updater_runtime.get("local_version", "")),
            "remote_version": str(updater_runtime.get("remote_version", "")),
            "source_kind": str(updater_runtime.get("source_kind", "remote") or "remote"),
            "source_label": str(updater_runtime.get("source_label", "远端正式更新源") or "远端正式更新源"),
            "local_release_revision": int(updater_runtime.get("local_release_revision", 0) or 0),
            "remote_release_revision": int(updater_runtime.get("remote_release_revision", 0) or 0),
            "state_path": str(updater_runtime.get("state_path", "")),
            "update_available": bool(updater_runtime.get("update_available", False)),
            "force_apply_available": bool(updater_runtime.get("force_apply_available", False)),
            "restart_required": bool(updater_runtime.get("restart_required", False)),
            "dependency_sync_status": str(updater_runtime.get("dependency_sync_status", "idle")),
            "dependency_sync_error": str(updater_runtime.get("dependency_sync_error", "")),
            "dependency_sync_at": str(updater_runtime.get("dependency_sync_at", "")),
            "queued_apply": dict(updater_runtime.get("queued_apply", {})),
            "mirror_ready": bool(updater_runtime.get("mirror_ready", False)),
            "mirror_version": str(updater_runtime.get("mirror_version", "")),
            "mirror_manifest_path": str(updater_runtime.get("mirror_manifest_path", "")),
            "last_publish_at": str(updater_runtime.get("last_publish_at", "")),
            "last_publish_error": str(updater_runtime.get("last_publish_error", "")),
        },
        "frontend": {
            "mode": str(getattr(container, "frontend_mode", "")),
            "root": str(getattr(container, "frontend_root", "")),
            "assets_dir": str(getattr(container, "frontend_assets_dir", "")),
        },
        "deployment": container.deployment_snapshot(),
        "shared_bridge": shared_bridge_snapshot,
        "network": {
            "current_ssid": wifi_name,
            "interface_name": interface_name,
            "target_internal_ssid": str(network_cfg.get("internal_ssid", "") or ""),
            "target_external_ssid": str(network_cfg.get("external_ssid", "") or ""),
            "switch_strategy": "role_fixed_network" if _deployment_role_mode(container) in {"internal", "external"} else "pending_role_selection",
            "visible_targets": visible_targets,
            "last_wifi_result": last_wifi_result,
            "last_wifi_error_type": last_wifi_error_type,
            "last_wifi_error": last_wifi_error,
            "last_switch_report": last_switch_report,
        },
        "resolved_paths": {
            "runtime_state_root": str(runtime_state_root_path),
            "scheduler_state_file": str(scheduler_state_path),
            "monthly_resume_root_dir": str(monthly_resume_root),
            "monthly_resume_index_file": str(monthly_resume_index),
            "handover_template_source_path": str(handover_source) if handover_source is not None else "",
            "handover_output_dir": str(handover_output) if handover_output is not None else "",
            "handover_event_sections_cache_state_file": str(handover_event_cache_path),
        },
        "system_logs": system_logs,
        "system_log_entries": system_log_entries,
        "alert_log_entries": alert_log_entries,
        "system_log_next_offset": system_log_next_offset,
    }


@router.get("/api/health/bootstrap")
def health_bootstrap(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return _build_bootstrap_health_payload(container, request)


@router.get("/api/config")
def get_config(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return mask_settings(copy.deepcopy(container.config))


@router.put("/api/config")
def put_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")
    try:
        payload_copy = copy.deepcopy(payload)
        meta = payload_copy.pop("_meta", {}) if isinstance(payload_copy.get("_meta"), dict) else {}
        clear_paths = meta.get("clear_paths", []) if isinstance(meta, dict) else []
        force_overwrite = bool(meta.get("force_overwrite", False)) if isinstance(meta, dict) else False
        warnings = _collect_config_warnings(payload_copy)
        merge_result = merge_user_config_payload(
            payload_copy,
            container.config,
            clear_paths=clear_paths if isinstance(clear_paths, list) else [],
            force_overwrite=force_overwrite,
        )
        current_role_mode = normalize_role_mode(
            _config_last_started_role_mode(container.config) or _config_role_mode(container.config)
        )
        target_role_mode = normalize_role_mode(_config_role_mode(merge_result.merged))
        role_restart_required = current_role_mode != target_role_mode
        other_restart_required = _role_restart_signature(
            container.config,
            include_role_mode=False,
        ) != _role_restart_signature(
            merge_result.merged,
            include_role_mode=False,
        )
        restart_required = role_restart_required or other_restart_required
        merged = _strip_retired_wet_bulb_fields(merge_result.merged)
        saved = save_settings(merged, container.config_path)
        container.reload_config(saved)
        _invalidate_review_base_probe_cache()
        container.add_system_log("[配置] 已保存并重新加载")
        review_cfg = (
            saved.get("features", {}).get("handover_log", {}).get("review_ui", {})
            if isinstance(saved.get("features", {}), dict)
            else {}
        )
        configured_base_url = _normalize_review_base_url(
            review_cfg.get("public_base_url", "") if isinstance(review_cfg, dict) else ""
        )
        handover_review_access = (
            _persist_manual_review_access_snapshot(container)
            if configured_base_url
            else _materialize_review_access_snapshot(container)
        )
        return {
            "ok": True,
            "config": mask_settings(saved),
            "warnings": warnings,
            "handover_review_access": handover_review_access,
            "restart_required": restart_required,
        }
    except ConfigValueLossError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/app/restart")
def restart_app(
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")
    has_incomplete_jobs = getattr(container.job_service, "has_incomplete_jobs", None)
    if callable(has_incomplete_jobs) and bool(has_incomplete_jobs()):
        raise HTTPException(status_code=409, detail="当前仍有任务在运行，请等待全部任务结束后再重启程序")
    source = str(payload.get("source", "manual") or "manual").strip() or "manual"
    target_role_mode = normalize_role_mode(payload.get("target_role_mode")) or _config_role_mode(container.config)
    wrote_startup_handoff = False
    try:
        with container.job_service.resource_guard(name="app_restart", resource_keys=["updater:global"]):
            write_startup_role_handoff = getattr(container, "write_startup_role_handoff", None)
            if source == "startup_role_picker" and callable(write_startup_role_handoff) and target_role_mode in {"internal", "external"}:
                write_startup_role_handoff(
                    target_role_mode=target_role_mode,
                    source=source,
                    reason=str(payload.get("reason", "") or "").strip(),
                    source_startup_time=str(getattr(request.app.state, "started_at", "") or "").strip(),
                )
                wrote_startup_handoff = True
            ok, result_text = container.request_app_restart(
                {
                    **payload,
                    "source": source,
                }
            )
        if not ok:
            raise RuntimeError(result_text)
        container.add_system_log(f"[运行时] 已触发程序重启: source={source}, result={result_text}")
        return {
            "ok": True,
            "result": {
                "last_result": "restart_scheduled",
                "reason": str(result_text or "").strip(),
            },
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        if wrote_startup_handoff:
            clear_startup_role_handoff = getattr(container, "clear_startup_role_handoff", None)
            if callable(clear_startup_role_handoff):
                clear_startup_role_handoff()
        raise HTTPException(status_code=400, detail=f"触发程序重启失败: {exc}") from exc


@router.post("/api/handover/review-access/reprobe")
def reprobe_handover_review_access(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    _invalidate_review_base_probe_cache()
    snapshot = _probe_and_persist_review_access_snapshot(
        container,
        request_host=str(getattr(request.url, "hostname", "") or "").strip(),
        port=getattr(request.url, "port", None),
    )
    container.add_system_log(
        "[交接班审核访问] 手动重新探测完成: "
        f"effective={str(snapshot.get('review_base_url_effective', '') or '-').strip() or '-'}, "
        f"source={str(snapshot.get('review_base_url_effective_source', '') or '-').strip() or '-'}, "
        f"validated={len(snapshot.get('review_base_url_validated_candidates', []) or [])}, "
        f"status={str(snapshot.get('review_base_url_status', '') or '-').strip() or '-'}"
    )
    return {
        "ok": bool(str(snapshot.get("review_base_url_effective", "") or "").strip()),
        "handover_review_access": snapshot,
    }


@router.post("/api/runtime/alarm-event-upload-target/open")
def open_alarm_event_upload_target(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    runtime_cfg = _runtime_config(container)
    preview = SharedSourceCacheService(runtime_config=runtime_cfg, store=None).get_alarm_event_upload_target_preview(
        force_refresh=True
    )
    display_url = str(preview.get("display_url", "") or preview.get("bitable_url", "") or "").strip()
    target_kind = str(preview.get("target_kind", "") or "").strip()
    configured_app_token = str(preview.get("configured_app_token", "") or "").strip()
    operation_app_token = str(preview.get("operation_app_token", "") or "").strip()
    table_id = str(preview.get("table_id", "") or "").strip()
    message = str(preview.get("message", "") or "").strip()
    container.add_system_log(
        "[告警上传][目标链接] 打开多维表: "
        f"kind={target_kind or '-'}, "
        f"display_url={display_url or '-'}, "
        f"configured_app_token={configured_app_token or '-'}, "
        f"operation_app_token={operation_app_token or '-'}, "
        f"table_id={table_id or '-'}, "
        f"message={message or '-'}"
    )
    return {
        "ok": bool(display_url),
        "target_preview": preview,
    }


@router.post("/api/jobs/auto-once")
def job_auto_once(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    config = _runtime_config(container)
    role_mode = _deployment_role_mode(container)

    if role_mode == "internal":
        raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端发起月报共享桥接任务")
    if role_mode == "external":
        bridge_service = _shared_bridge_service_or_raise(container)
        target_buildings = bridge_service.get_source_cache_buildings()
        selection = _normalize_latest_cache_selection(bridge_service.get_latest_source_cache_selection(
            source_family="monthly_report_family",
            buildings=target_buildings,
        ))
        cached_entries = selection["selected_entries"]
        if not selection["can_proceed"] or len(cached_entries) < len(target_buildings):
            task = _get_or_create_bridge_task(
                bridge_service,
                get_or_create_name="get_or_create_monthly_auto_once_task",
                create_name="create_monthly_auto_once_task",
                requested_by="manual",
                source="manual",
            )
            container.add_system_log(
                "[共享桥接] 已受理月报自动流程共享桥接任务 "
                f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, "
                f"reason={_build_latest_cache_wait_detail(feature_name='全景平台月报', selection=selection)}"
            )
            return _accepted_bridge_task_response(
                task,
                name="月报自动流程-共享桥接补采",
                feature="monthly_report_pipeline",
            )

        def _run_from_cache(emit_log):
            file_items = [
                {
                    "building": str(item.get("building", "") or "").strip(),
                    "file_path": str(item.get("file_path", "") or "").strip(),
                    "upload_date": str(item.get("metadata", {}).get("upload_date", "") or item.get("duty_date", "") or _current_hour_bucket()[:10]).strip(),
                }
                for item in cached_entries
            ]
            return run_monthly_from_file_items(
                config,
                file_items=file_items,
                emit_log=emit_log,
                source_label="月报共享文件",
            )
        dedupe_key = _job_dedupe_key(
            "monthly_cache_latest",
            bucket_key=str(selection.get("best_bucket_key", "") or "").strip(),
            buildings=[str(item.get("building", "") or "").strip() for item in cached_entries],
        )

        try:
            job = _start_background_job(
                container,
                name="月报自动流程-读取共享文件",
                run_func=_run_from_cache,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys("shared_bridge:monthly_report"),
                priority="manual",
                feature="monthly_cache_latest",
                dedupe_key=dedupe_key,
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: 月报自动流程-读取共享文件 ({job.job_id})")
            return job.to_dict()
        except JobBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    def _run(emit_log):
        orchestrator = OrchestratorService(config)
        notify = WebhookNotifyService(config)
        try:
            return orchestrator.run_auto_once(emit_log)
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="立即执行自动流程", detail=str(exc), emit_log=emit_log)
            raise

    try:
        job = _start_background_job(
            container,
            name="立即执行自动流程",
            run_func=_run,
            worker_handler="auto_once",
            worker_payload={"source": "立即执行自动流程"},
            resource_keys=_job_resource_keys("network:pipeline"),
            priority="manual",
            feature="auto_once",
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: 立即执行自动流程 ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/jobs/alarm-export/run")
def job_alarm_export_run(request: Request) -> Dict[str, Any]:
    raise HTTPException(status_code=410, detail="旧告警导出入口已退役，当前主链为“内网 API 拉取 -> 共享 JSON -> 外网上传”")


@router.post("/api/jobs/wet-bulb-collection/run")
def job_wet_bulb_collection_run(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    config = _runtime_config(container)
    role_mode = _deployment_role_mode(container)

    if role_mode == "internal":
        raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端发起湿球温度共享桥接任务")
    if role_mode == "external":
        bridge_service = _shared_bridge_service_or_raise(container)
        target_buildings = bridge_service.get_source_cache_buildings()
        selection = _normalize_latest_cache_selection(bridge_service.get_latest_source_cache_selection(
            source_family="handover_log_family",
            buildings=target_buildings,
        ))
        cached_entries = selection["selected_entries"]
        if not selection["can_proceed"] or len(cached_entries) < len(target_buildings):
            task = _get_or_create_bridge_task(
                bridge_service,
                get_or_create_name="get_or_create_wet_bulb_collection_task",
                create_name="create_wet_bulb_collection_task",
                buildings=target_buildings,
                requested_by="manual",
            )
            container.add_system_log(
                "[共享桥接] 已受理湿球温度共享桥接任务 "
                f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, "
                f"reason={_build_latest_cache_wait_detail(feature_name='交接班日志', selection=selection)}"
            )
            return _accepted_bridge_task_response(
                task,
                name="湿球温度定时采集-共享桥接补采",
                feature="wet_bulb_collection",
            )

        def _run_from_cache(emit_log):
            source_units = [
                {
                    "building": str(item.get("building", "") or "").strip(),
                    "file_path": str(item.get("file_path", "") or "").strip(),
                }
                for item in cached_entries
            ]
            service = WetBulbCollectionService(config)
            return service.continue_from_source_units(source_units=source_units, emit_log=emit_log)
        dedupe_key = _job_dedupe_key(
            "wet_bulb_cache_latest",
            bucket_key=str(selection.get("best_bucket_key", "") or "").strip(),
            buildings=[str(item.get("building", "") or "").strip() for item in cached_entries],
        )

        try:
            job = _start_background_job(
                container,
                name="湿球温度定时采集-读取共享文件",
                run_func=_run_from_cache,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys("shared_bridge:wet_bulb"),
                priority="manual",
                feature="wet_bulb_cache_latest",
                dedupe_key=dedupe_key,
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: 湿球温度定时采集-读取共享文件 ({job.job_id})")
            return job.to_dict()
        except JobBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    def _run(emit_log):
        orchestrator = OrchestratorService(config)
        notify = WebhookNotifyService(config)
        try:
            return orchestrator.run_wet_bulb_collection(emit_log=emit_log, source="湿球温度定时采集")
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="湿球温度定时采集", detail=str(exc), emit_log=emit_log)
            raise

    try:
        job = _start_background_job(
            container,
            name="湿球温度定时采集",
            run_func=_run,
            worker_handler="wet_bulb_collection_run",
            worker_payload={"source": "湿球温度定时采集"},
            resource_keys=_job_resource_keys("network:pipeline"),
            priority="manual",
            feature="wet_bulb_collection_run",
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: 湿球温度定时采集 ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/jobs/multi-date")
def job_multi_date(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    role_mode = _deployment_role_mode(container)
    config = _runtime_config(container)
    raw_dates = payload.get("dates", []) if isinstance(payload, dict) else []
    selected_dates = [str(x).strip() for x in raw_dates if str(x).strip()]
    if not selected_dates:
        raise HTTPException(status_code=400, detail="请选择至少一个日期")

    today = date.today()
    for item in selected_dates:
        try:
            day = datetime.strptime(item, "%Y-%m-%d").date()
        except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail=f"日期格式错误: {item}") from exc
        if day > today:
                raise HTTPException(status_code=400, detail=f"不能选择未来日期: {item}")

    if role_mode == "internal":
            raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端发起月报共享桥接任务")
    if role_mode == "external":
        bridge_service = _shared_bridge_service_or_raise(container)
        target_buildings = bridge_service.get_source_cache_buildings()
        cached_entries = _filter_accessible_cached_entries(bridge_service.get_monthly_by_date_cache_entries(
            selected_dates=selected_dates,
            buildings=target_buildings,
        ))
        expected_count = len(selected_dates) * len(target_buildings)
        if len(cached_entries) < expected_count:
            task = _get_or_create_bridge_task(
                bridge_service,
                get_or_create_name="get_or_create_monthly_cache_fill_task",
                create_name="create_monthly_cache_fill_task",
                selected_dates=selected_dates,
                requested_by="manual",
            )
            container.add_system_log(
                "[共享缓存] 已提交月报历史日期补采任务 "
                f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, dates={','.join(selected_dates)}"
            )
            return {
                "ok": True,
                "accepted": True,
                "bridge_task": task,
                "job": _bridge_proxy_job(
                    task,
                    name="月报历史共享文件补采",
                    feature="monthly_cache_fill",
                ),
            }

        def _run_from_cache(emit_log):
            file_items = [
                {
                    "building": str(item.get("building", "") or "").strip(),
                    "file_path": str(item.get("file_path", "") or "").strip(),
                    "upload_date": str(item.get("metadata", {}).get("upload_date", "") or item.get("duty_date", "") or "").strip(),
                }
                for item in cached_entries
            ]
            return run_monthly_from_file_items(
                config,
                file_items=file_items,
                emit_log=emit_log,
                source_label="月报历史共享文件",
            )
        dedupe_key = _job_dedupe_key(
            "monthly_cache_by_date",
            selected_dates=selected_dates,
            buildings=target_buildings,
        )

        try:
            job = _start_background_job(
                container,
                name="月报多日期-读取共享文件",
                run_func=_run_from_cache,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys("shared_bridge:monthly_report"),
                priority="manual",
                feature="monthly_cache_by_date",
                dedupe_key=dedupe_key,
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: 月报多日期-读取共享文件 ({job.job_id})")
            return job.to_dict()
        except JobBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    def _run(emit_log):
        orchestrator = OrchestratorService(config)
        notify = WebhookNotifyService(config)
        try:
            return orchestrator.run_multi_date(selected_dates, emit_log)
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="多日期自动流程", detail=str(exc), emit_log=emit_log)
            raise

    try:
        job = _start_background_job(
            container,
            name="多日期自动流程",
            run_func=_run,
            worker_handler="multi_date",
            worker_payload={"selected_dates": selected_dates},
            resource_keys=_job_resource_keys("network:pipeline"),
            priority="manual",
            feature="multi_date",
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: 多日期自动流程 ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/api/jobs/resume/pending")
def list_resume_pending(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    role_mode = _deployment_role_mode(container)
    if role_mode == "internal":
        return {"runs": [], "count": 0}
    if role_mode == "external":
        bridge_service = getattr(container, "shared_bridge_service", None)
        if bridge_service is not None and _shared_bridge_is_available(container):
            runs = bridge_service.list_monthly_pending_resume_runs()
            return {"runs": runs, "count": len(runs)}
        return {"runs": [], "count": 0}
    config = _runtime_config(container)
    orchestrator = OrchestratorService(config)
    runs = orchestrator.list_pending_resume_runs()
    return {"runs": runs, "count": len(runs)}


@router.post("/api/jobs/resume-upload")
def job_resume_upload(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    config = _runtime_config(container)
    role_mode = _deployment_role_mode(container)
    run_id = ""
    auto_trigger = False
    if isinstance(payload, dict):
        run_id = str(payload.get("run_id", "")).strip()
        auto_trigger = bool(payload.get("auto", False))

    if role_mode == "internal":
            raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端发起断点续传共享桥接任务")
    if role_mode == "external":
        if not _shared_bridge_is_available(container):
            raise HTTPException(status_code=409, detail="共享桥接未启用或共享目录未配置，无法创建断点续传任务")
        bridge_service = getattr(container, "shared_bridge_service", None)
        if bridge_service is None:
            raise HTTPException(status_code=409, detail="共享桥接服务未初始化")
        task = bridge_service.create_monthly_resume_upload_task(
            run_id=run_id or None,
            auto_trigger=auto_trigger,
            requested_by="auto" if auto_trigger else "manual",
        )
        container.add_system_log(
            "[共享桥接] 已提交月报断点续传任务 "
            f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, run_id={run_id or '-'}"
        )
        return {
            "ok": True,
            "accepted": True,
            "bridge_task": task,
            "job": _bridge_proxy_job(
                task,
            name="月报断点续传-共享桥接",
                feature="monthly_report_pipeline_bridge",
            ),
        }

    def _run(emit_log):
        orchestrator = OrchestratorService(config)
        notify = WebhookNotifyService(config)
        try:
            return orchestrator.run_resume_upload(emit_log=emit_log, run_id=run_id or None, auto_trigger=auto_trigger)
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="断点续传", detail=str(exc), emit_log=emit_log)
            raise

    try:
        job = _start_background_job(
            container,
            name="断点续传上传",
            run_func=_run,
            worker_handler="resume_upload",
            worker_payload={"run_id": run_id or None, "auto_trigger": auto_trigger},
            resource_keys=_job_resource_keys("network:external"),
            priority="resume",
            feature="resume_upload",
            submitted_by="resume",
        )
        container.add_system_log(f"[任务] 已提交: 断点续传上传 ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/jobs/resume/delete")
def delete_resume_run(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    role_mode = _deployment_role_mode(container)
    config = _runtime_config(container)
    run_id = str(payload.get("run_id", "")).strip() if isinstance(payload, dict) else ""
    if not run_id:
        raise HTTPException(status_code=400, detail="run_id 不能为空")
    if role_mode == "internal":
        raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端执行续传记录删除")
    if role_mode == "external":
        if not _shared_bridge_is_available(container):
            raise HTTPException(status_code=409, detail="共享桥接未启用或共享目录未配置，无法删除续传记录")
        bridge_service = getattr(container, "shared_bridge_service", None)
        if bridge_service is None:
            raise HTTPException(status_code=409, detail="共享桥接服务未初始化")
        try:
            return bridge_service.delete_monthly_resume_run(run_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    try:
        orchestrator = OrchestratorService(config)
        result = orchestrator.delete_resume_run(run_id=run_id)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/api/jobs/manual-upload")
async def job_manual_upload(
    request: Request,
    building: str = Form(...),
    upload_date: str = Form(...),
    legacy_switch_external_before_upload: bool = Form(False, alias="switch_external_before_upload"),
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    building = str(building).strip()
    upload_date = str(upload_date).strip()
    if not building:
        raise HTTPException(status_code=400, detail="楼栋不能为空")
    if not upload_date:
        raise HTTPException(status_code=400, detail="upload_date 不能为空")
    try:
        datetime.strptime(upload_date, "%Y-%m-%d")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="upload_date 格式错误，必须为 YYYY-MM-DD") from exc
    if not file.filename:
        raise HTTPException(status_code=400, detail="请上传 xlsx 文件")

    container = request.app.state.container
    role_mode = _deployment_role_mode(container)
    _ensure_not_internal_role(container, "当前为内网端角色，请在外网端执行手动补传")
    switch_external_before_upload = bool(legacy_switch_external_before_upload)
    config = _runtime_config(container)
    suffix = Path(file.filename or "upload.xlsx").suffix or ".xlsx"
    temp_dir = create_runtime_temp_dir(
        kind="manual_upload",
        runtime_config=config,
        app_dir=get_app_dir(),
    )
    temp_path = temp_dir / f"input{suffix}"
    with temp_path.open("wb") as f:
        f.write(file.file.read())

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            service = CalculationService(config)
            return service.run_manual_upload(
                building=building,
                file_path=str(temp_path),
                upload_date=upload_date,
                switch_external_before_upload=switch_external_before_upload,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="手动补传", detail=str(exc), building=building, emit_log=emit_log)
            raise
        finally:
            cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())

    try:
        job = _start_background_job(
            container,
            name=f"手动补传-{building}",
            run_func=_run,
            worker_handler="manual_upload",
            worker_payload={
                "building": building,
                "file_path": str(temp_path),
                "upload_date": upload_date,
                "switch_external_before_upload": switch_external_before_upload,
                "cleanup_dir": str(temp_dir),
            },
            resource_keys=_job_resource_keys("network:external"),
            priority="manual",
            feature="manual_upload",
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: 手动补传-{building} 日期={upload_date} ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception:
        cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())
        raise


@router.post("/api/jobs/handover/from-file")
async def job_handover_from_file(
    request: Request,
    building: str = Form(...),
    end_time: str = Form(""),
    duty_date: str = Form(""),
    duty_shift: str = Form(""),
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    building = str(building).strip()
    if not building:
        raise HTTPException(status_code=400, detail="楼栋不能为空")
    if not file.filename:
        raise HTTPException(status_code=400, detail="请上传 xlsx 文件")

    container = request.app.state.container
    _ensure_not_internal_role(container, "当前为内网端角色，请在外网端执行交接班已有文件生成")
    config = _runtime_config(container)
    end_time_text = str(end_time or "").strip() or None
    duty_date_text = str(duty_date or "").strip() or None
    duty_shift_text = str(duty_shift or "").strip().lower() or None

    if (duty_date_text and not duty_shift_text) or (duty_shift_text and not duty_date_text):
        raise HTTPException(status_code=400, detail="duty_date 与 duty_shift 需要同时传入")
    if duty_date_text:
        try:
            datetime.strptime(duty_date_text, "%Y-%m-%d")
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"duty_date 格式错误: {duty_date_text}") from exc
    if duty_shift_text and duty_shift_text not in {"day", "night"}:
        raise HTTPException(status_code=400, detail=f"duty_shift 仅支持 day/night: {duty_shift_text}")

    suffix = Path(file.filename or "handover.xlsx").suffix or ".xlsx"
    temp_dir = create_runtime_temp_dir(
        kind="handover_from_file",
        runtime_config=config,
        app_dir=get_app_dir(),
    )
    temp_path = temp_dir / f"input{suffix}"
    with temp_path.open("wb") as f:
        f.write(file.file.read())

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            orchestrator = OrchestratorService(config)
            return orchestrator.run_handover_from_file(
                building=building,
                file_path=str(temp_path),
                end_time=end_time_text,
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="交接班日志（已有文件）", detail=str(exc), building=building, emit_log=emit_log)
            raise
        finally:
            cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())

    try:
        job = _start_background_job(
            container,
            name=f"交接班日志-已有文件-{building}",
            run_func=_run,
            worker_handler="handover_from_file",
            worker_payload={
                "building": building,
                "file_path": str(temp_path),
                "end_time": end_time_text,
                "duty_date": duty_date_text,
                "duty_shift": duty_shift_text,
                "cleanup_dir": str(temp_dir),
            },
            resource_keys=_job_resource_keys(),
            priority="manual",
            feature="handover_from_file",
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: 交接班日志-已有文件-{building} ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception:
        cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())
        raise


@router.post("/api/jobs/handover/from-files")
async def job_handover_from_files(
    request: Request,
    buildings: list[str] = Form(...),
    end_time: str = Form(""),
    duty_date: str = Form(""),
    duty_shift: str = Form(""),
    files: list[UploadFile] = File(...),
) -> Dict[str, Any]:
    building_list = [str(item or "").strip() for item in (buildings or [])]
    upload_files = list(files or [])
    if not building_list or not upload_files:
        raise HTTPException(status_code=400, detail="请选择至少一个楼的已有数据表文件")
    if len(building_list) != len(upload_files):
        raise HTTPException(status_code=400, detail="buildings 与 files 数量必须一致")
    if any(not item for item in building_list):
        raise HTTPException(status_code=400, detail="楼栋不能为空")
    if any(not upload.filename for upload in upload_files):
        raise HTTPException(status_code=400, detail="请上传 xlsx 文件")

    container = request.app.state.container
    _ensure_not_internal_role(container, "当前为内网端角色，请在外网端执行交接班已有文件批量生成")
    config = _runtime_config(container)
    end_time_text = str(end_time or "").strip() or None
    duty_date_text = str(duty_date or "").strip() or None
    duty_shift_text = str(duty_shift or "").strip().lower() or None

    if (duty_date_text and not duty_shift_text) or (duty_shift_text and not duty_date_text):
        raise HTTPException(status_code=400, detail="duty_date 与 duty_shift 需要同时传入")
    if duty_date_text:
        try:
            datetime.strptime(duty_date_text, "%Y-%m-%d")
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"duty_date 格式错误: {duty_date_text}") from exc
    if duty_shift_text and duty_shift_text not in {"day", "night"}:
        raise HTTPException(status_code=400, detail=f"duty_shift 仅支持 day/night: {duty_shift_text}")

    temp_dir = create_runtime_temp_dir(
        kind="handover_from_files",
        runtime_config=config,
        app_dir=get_app_dir(),
    )
    building_files: list[tuple[str, str]] = []
    for index, (building, upload) in enumerate(zip(building_list, upload_files, strict=False)):
        suffix = Path(upload.filename or f"{building}_{index}.xlsx").suffix or ".xlsx"
        temp_path = temp_dir / f"{index + 1}_{building}{suffix}"
        with temp_path.open("wb") as handle:
            handle.write(upload.file.read())
        building_files.append((building, str(temp_path)))

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            orchestrator = OrchestratorService(config)
            return orchestrator.run_handover_from_files(
                building_files=building_files,
                end_time=end_time_text,
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(
                stage="交接班日志（已有文件批量）",
                detail=str(exc),
                building=",".join(building_list),
                emit_log=emit_log,
            )
            raise
        finally:
            cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())

    try:
        job = _start_background_job(
            container,
            name="交接班日志-已有文件批量生成",
            run_func=_run,
            worker_handler="handover_from_files",
            worker_payload={
                "building_files": [
                    {"building": item_building, "file_path": item_path}
                    for item_building, item_path in building_files
                ],
                "end_time": end_time_text,
                "duty_date": duty_date_text,
                "duty_shift": duty_shift_text,
                "cleanup_dir": str(temp_dir),
            },
            resource_keys=_job_resource_keys(),
            priority="manual",
            feature="handover_from_files",
            submitted_by="manual",
        )
        container.add_system_log(
            "[任务] 已提交: 交接班日志-已有文件批量生成 "
            f"selected={','.join(building_list)} ({job.job_id})"
        )
        return job.to_dict()
    except JobBusyError as exc:
        cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception:
        cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())
        raise


@router.post("/api/jobs/handover/from-download")
def job_handover_from_download(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    config = _runtime_config(container)
    role_mode = _deployment_role_mode(container)

    buildings_raw = payload.get("buildings", []) if isinstance(payload, dict) else []
    buildings = [str(x).strip() for x in buildings_raw if str(x).strip()]
    end_time = str(payload.get("end_time", "")).strip() if isinstance(payload, dict) else ""
    end_time_text = end_time or None
    duty_date_raw = str(payload.get("duty_date", "")).strip() if isinstance(payload, dict) else ""
    duty_shift_raw = str(payload.get("duty_shift", "")).strip().lower() if isinstance(payload, dict) else ""
    duty_date_text = duty_date_raw or None
    duty_shift_text = duty_shift_raw or None

    if (duty_date_text and not duty_shift_text) or (duty_shift_text and not duty_date_text):
        raise HTTPException(status_code=400, detail="duty_date 与 duty_shift 需要同时传入")
    if duty_date_text:
        try:
            datetime.strptime(duty_date_text, "%Y-%m-%d")
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"duty_date 格式错误: {duty_date_text}") from exc
    if duty_shift_text and duty_shift_text not in {"day", "night"}:
        raise HTTPException(status_code=400, detail=f"duty_shift 仅支持 day/night: {duty_shift_text}")

    if role_mode == "internal":
        raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端发起交接班桥接任务")
    if role_mode == "external":
        bridge_service = _shared_bridge_service_or_raise(container)
        target_buildings = buildings or bridge_service.get_source_cache_buildings()
        if duty_date_text and duty_shift_text:
            cached_entries = _filter_accessible_cached_entries(bridge_service.get_handover_by_date_cache_entries(
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                buildings=target_buildings,
            ))
            if len(cached_entries) < len(target_buildings):
                task = _get_or_create_bridge_task(
                    bridge_service,
                    get_or_create_name="get_or_create_handover_cache_fill_task",
                    create_name="create_handover_cache_fill_task",
                    continuation_kind="handover",
                    buildings=target_buildings,
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    selected_dates=None,
                    building_scope=None,
                    building=None,
                    requested_by="manual",
                )
                container.add_system_log(
                    "[共享缓存] 已提交交接班历史缓存补采任务 "
                    f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, "
                    f"duty_date={duty_date_text}, duty_shift={duty_shift_text}"
                )
                return {
                    "ok": True,
                    "accepted": True,
                    "bridge_task": task,
                    "job": _bridge_proxy_job(
                        task,
                    name="交接班历史共享文件补采",
                        feature="handover_cache_fill",
                    ),
                }
            building_files = [(str(item.get("building", "") or "").strip(), str(item.get("file_path", "") or "").strip()) for item in cached_entries]
        else:
            selection = _normalize_latest_cache_selection(bridge_service.get_latest_source_cache_selection(
                source_family="handover_log_family",
                buildings=target_buildings,
            ))
            cached_entries = selection["selected_entries"]
            if not selection["can_proceed"] or len(cached_entries) < len(target_buildings):
                task = _get_or_create_bridge_task(
                    bridge_service,
                    get_or_create_name="get_or_create_handover_from_download_task",
                    create_name="create_handover_from_download_task",
                    buildings=target_buildings,
                    end_time=end_time_text,
                    duty_date=None,
                    duty_shift=None,
                    requested_by="manual",
                )
                container.add_system_log(
                    "[共享桥接] 已受理交接班 latest 共享桥接任务 "
                    f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, "
                    f"reason={_build_latest_cache_wait_detail(feature_name='交接班日志', selection=selection)}"
                )
                return _accepted_bridge_task_response(
                    task,
                    name="交接班日志-共享桥接补采",
                    feature="handover_from_download",
                )
            building_files = [(str(item.get("building", "") or "").strip(), str(item.get("file_path", "") or "").strip()) for item in cached_entries]

        def _run_from_cache(emit_log):
            orchestrator = OrchestratorService(config)
            return orchestrator.run_handover_from_files(
                building_files=building_files,
                end_time=end_time_text,
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                emit_log=emit_log,
            )
        dedupe_key = _job_dedupe_key(
            "handover_cache_continue",
            mode="by_date" if (duty_date_text and duty_shift_text) else "latest",
            bucket_key=str(selection.get("best_bucket_key", "") or "").strip() if not (duty_date_text and duty_shift_text) else "",
            buildings=[item[0] for item in building_files],
            duty_date=duty_date_text or "",
            duty_shift=duty_shift_text or "",
            end_time=end_time_text or "",
        )

        try:
            job = _start_background_job(
                container,
                name="交接班日志-使用共享文件生成",
                run_func=_run_from_cache,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys("shared_bridge:handover"),
                priority="manual",
                feature="handover_cache_continue",
                dedupe_key=dedupe_key,
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: 交接班日志-使用共享文件生成 ({job.job_id})")
            return job.to_dict()
        except JobBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            orchestrator = OrchestratorService(config)
            result = orchestrator.run_handover_from_download(
                buildings=buildings or None,
                end_time=end_time_text,
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                emit_log=emit_log,
            )
            failure_summary = orchestrator.build_handover_download_failure_summary(result)
            if failure_summary:
                emit_log(
                    "[交接班下载] 失败汇总告警: "
                    f"buildings={str(failure_summary.get('building', '') or '-').strip() or '-'}, "
                    f"detail={str(failure_summary.get('detail', '') or '-').strip() or '-'}"
                )
                notify.send_failure(
                    stage="交接班日志（内网下载）",
                    detail=str(failure_summary.get("detail", "") or "").strip() or "交接班内网下载存在失败楼栋",
                    building=str(failure_summary.get("building", "") or "").strip() or None,
                    emit_log=emit_log,
                )
            return result
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="交接班日志（内网下载）", detail=str(exc), emit_log=emit_log)
            raise

    try:
        job = _start_background_job(
            container,
            name="交接班日志-内网下载生成",
            run_func=_run,
            worker_handler="handover_from_download",
            worker_payload={
                "buildings": buildings or None,
                "end_time": end_time_text,
                "duty_date": duty_date_text,
                "duty_shift": duty_shift_text,
            },
            resource_keys=_job_resource_keys("network:pipeline"),
            priority="manual",
            feature="handover_from_download",
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: 交接班日志-内网下载生成 ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/jobs/day-metric/from-download")
def job_day_metric_from_download(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    config = _runtime_config(container)
    role_mode = _deployment_role_mode(container)

    raw_dates = payload.get("dates", []) if isinstance(payload, dict) else []
    selected_dates = []
    today_value = date.today()
    for item in raw_dates if isinstance(raw_dates, list) else []:
        text = str(item or "").strip()
        if not text:
            continue
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d").date()
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"dates 包含非法日期: {text}") from exc
        if parsed > today_value:
            raise HTTPException(status_code=400, detail=f"不允许未来日期: {text}")
        if text not in selected_dates:
            selected_dates.append(text)
    if not selected_dates:
        raise HTTPException(status_code=400, detail="dates 至少需要一个有效日期")

    building_scope = str(payload.get("building_scope", "")).strip() if isinstance(payload, dict) else ""
    if building_scope not in {"single", "all_enabled"}:
        raise HTTPException(status_code=400, detail="building_scope 仅支持 single/all_enabled")
    building = str(payload.get("building", "")).strip() if isinstance(payload, dict) else ""
    if building_scope == "single" and not building:
        raise HTTPException(status_code=400, detail="single 模式下 building 不能为空")

    if role_mode == "internal":
        raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端发起12项桥接任务")
    if role_mode == "external":
        bridge_service = _shared_bridge_service_or_raise(container)
        target_buildings = [building] if building_scope == "single" else bridge_service.get_source_cache_buildings()
        target_buildings = [item for item in target_buildings if item]
        cached_entries = _filter_accessible_cached_entries(bridge_service.get_day_metric_by_date_cache_entries(
            selected_dates=selected_dates,
            buildings=target_buildings,
        ))
        expected_count = len(selected_dates) * len(target_buildings)
        if len(cached_entries) < expected_count:
            task = _get_or_create_bridge_task(
                bridge_service,
                get_or_create_name="get_or_create_handover_cache_fill_task",
                create_name="create_handover_cache_fill_task",
                continuation_kind="day_metric",
                buildings=None,
                duty_date=None,
                duty_shift=None,
                selected_dates=selected_dates,
                building_scope=building_scope,
                building=building or None,
                requested_by="manual",
            )
            container.add_system_log(
                "[共享缓存] 已提交12项历史缓存补采任务 "
                f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, "
                f"dates={','.join(selected_dates)}, scope={building_scope}, building={building or '-'}"
            )
            return {
                "ok": True,
                "accepted": True,
                "bridge_task": task,
                "job": _bridge_proxy_job(
                    task,
                    name="12项历史共享文件补采",
                    feature="handover_cache_fill",
                ),
            }

        def _run_from_cache(emit_log):
            source_units = [
                {
                    "duty_date": str(item.get("duty_date", "") or "").strip(),
                    "building": str(item.get("building", "") or "").strip(),
                    "source_file": str(item.get("file_path", "") or "").strip(),
                }
                for item in cached_entries
            ]
            service = DayMetricStandaloneUploadService(config)
            return service.continue_from_source_files(
                selected_dates=selected_dates,
                buildings=target_buildings,
                source_units=source_units,
                building_scope=building_scope,
                building=building or None,
                emit_log=emit_log,
            )
        dedupe_key = _job_dedupe_key(
            "day_metric_cache_by_date",
            selected_dates=selected_dates,
            building_scope=building_scope,
            building=building or "",
            buildings=target_buildings,
        )

        try:
            job = _start_background_job(
                container,
                name="12项独立上传-使用共享文件",
                run_func=_run_from_cache,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys("shared_bridge:day_metric"),
                priority="manual",
                feature="day_metric_cache_by_date",
                dedupe_key=dedupe_key,
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: 12项独立上传-使用共享文件 ({job.job_id})")
            return job.to_dict()
        except JobBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            orchestrator = OrchestratorService(config)
            return orchestrator.run_day_metric_from_download(
                selected_dates=selected_dates,
                building_scope=building_scope,
                building=building or None,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="12项独立上传（内网下载）", detail=str(exc), building=building or None, emit_log=emit_log)
            raise

    try:
        job = _start_background_job(
            container,
            name="12项独立上传-内网下载",
            run_func=_run,
            worker_handler="day_metric_from_download",
            worker_payload={
                "selected_dates": selected_dates,
                "building_scope": building_scope,
                "building": building or None,
            },
            resource_keys=_job_resource_keys("network:pipeline"),
            priority="manual",
            feature="day_metric_from_download",
            submitted_by="manual",
        )
        container.add_system_log(
            "[任务] 已提交: 12项独立上传-内网下载 "
            f"dates={','.join(selected_dates)} scope={building_scope} ({job.job_id})"
        )
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/jobs/day-metric/from-file")
async def job_day_metric_from_file(
    request: Request,
    building: str = Form(...),
    duty_date: str = Form(...),
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    building = str(building or "").strip()
    duty_date = str(duty_date or "").strip()
    if not building:
        raise HTTPException(status_code=400, detail="楼栋不能为空")
    if not duty_date:
        raise HTTPException(status_code=400, detail="duty_date 不能为空")
    try:
        parsed = datetime.strptime(duty_date, "%Y-%m-%d").date()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"duty_date 格式错误: {duty_date}") from exc
    if parsed > date.today():
        raise HTTPException(status_code=400, detail=f"不允许未来日期: {duty_date}")
    if not file.filename:
        raise HTTPException(status_code=400, detail="请上传 xlsx 文件")

    container = request.app.state.container
    _ensure_not_internal_role(container, "当前为内网端角色，请在外网端执行12项本地文件补录")
    config = _runtime_config(container)
    suffix = Path(file.filename or "day_metric.xlsx").suffix or ".xlsx"
    temp_dir = create_runtime_temp_dir(
        kind="day_metric_from_file",
        runtime_config=config,
        app_dir=get_app_dir(),
    )
    temp_path = temp_dir / f"input{suffix}"
    with temp_path.open("wb") as handle:
        handle.write(file.file.read())

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            orchestrator = OrchestratorService(config)
            return orchestrator.run_day_metric_from_file(
                building=building,
                duty_date=duty_date,
                file_path=str(temp_path),
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="12项独立上传（本地补录）", detail=str(exc), building=building, emit_log=emit_log)
            raise

    try:
        job = _start_background_job(
            container,
            name=f"12项独立上传-本地补录-{building}",
            run_func=_run,
            worker_handler="day_metric_from_file",
            worker_payload={
                "building": building,
                "duty_date": duty_date,
                "file_path": str(temp_path),
            },
            resource_keys=_job_resource_keys("network:external"),
            priority="manual",
            feature="day_metric_from_file",
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: 12项独立上传-本地补录-{building} 日期={duty_date} ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception:
        cleanup_runtime_temp_dir(temp_dir, runtime_config=config, app_dir=get_app_dir())
        raise


@router.post("/api/jobs/day-metric/retry-unit")
def job_day_metric_retry_unit(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    _ensure_not_internal_role(container, "当前为内网端角色，请在外网端执行12项单元重试")
    config = _runtime_config(container)
    mode = str(payload.get("mode", "")).strip().lower() if isinstance(payload, dict) else ""
    duty_date = str(payload.get("duty_date", "")).strip() if isinstance(payload, dict) else ""
    building = str(payload.get("building", "")).strip() if isinstance(payload, dict) else ""
    source_file = str(payload.get("source_file", "")).strip() if isinstance(payload, dict) else ""
    stage = str(payload.get("stage", "")).strip().lower() if isinstance(payload, dict) else ""
    if mode not in {"from_download", "from_file"}:
        raise HTTPException(status_code=400, detail="mode 仅支持 from_download/from_file")
    if not duty_date or not building:
        raise HTTPException(status_code=400, detail="duty_date 与 building 不能为空")

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            orchestrator = OrchestratorService(config)
            return orchestrator.retry_day_metric_unit(
                mode=mode,
                duty_date=duty_date,
                building=building,
                source_file=source_file or None,
                stage=stage or None,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(
                stage="12项独立上传（单元重试）",
                detail=str(exc),
                building=building,
                emit_log=emit_log,
            )
            raise

    try:
        job = _start_background_job(
            container,
            name=f"12项独立上传-单元重试-{building}",
            run_func=_run,
            worker_handler="day_metric_retry_unit",
            worker_payload={
                "mode": mode,
                "duty_date": duty_date,
                "building": building,
                "source_file": source_file or None,
                "stage": stage or None,
            },
            resource_keys=_job_resource_keys(
                "network:external" if str(stage or "").strip().lower() in {"attachment", "upload"} else "network:pipeline"
            ),
            priority="manual",
            feature="day_metric_retry_unit",
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: 12项独立上传-单元重试-{building} 日期={duty_date} 模式={mode} ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/jobs/day-metric/retry-failed")
def job_day_metric_retry_failed(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    _ensure_not_internal_role(container, "当前为内网端角色，请在外网端执行12项失败重试")
    config = _runtime_config(container)
    mode = str(payload.get("mode", "")).strip().lower() if isinstance(payload, dict) else ""

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            orchestrator = OrchestratorService(config)
            return orchestrator.retry_day_metric_failed(
                mode=mode or None,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="12项独立上传（全部失败重试）", detail=str(exc), emit_log=emit_log)
            raise

    try:
        job = _start_background_job(
            container,
            name="12项独立上传-全部失败重试",
            run_func=_run,
            worker_handler="day_metric_retry_failed",
            worker_payload={"mode": mode or None},
            resource_keys=_job_resource_keys("network:pipeline"),
            priority="manual",
            feature="day_metric_retry_failed",
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: 12项独立上传-全部失败重试 模式={mode or 'from_download'} ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/jobs/handover/followup/continue")
def job_handover_followup_continue(
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    _ensure_not_internal_role(container, "当前为内网端角色，请在外网端执行交接班继续后续上传")
    config = _runtime_config(container)
    batch_key = str(payload.get("batch_key", "")).strip() if isinstance(payload, dict) else ""
    if not batch_key:
        raise HTTPException(status_code=400, detail="batch_key 不能为空")

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            orchestrator = OrchestratorService(config)
            return orchestrator.run_handover_followup_continue(
                batch_key=batch_key,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="交接班继续后续上传", detail=str(exc), emit_log=emit_log)
            raise

    try:
        job = _start_background_job(
            container,
            name=f"交接班继续后续上传-{batch_key}",
            run_func=_run,
            worker_handler="handover_followup_continue",
            worker_payload={"batch_key": batch_key},
            resource_keys=_job_resource_keys("network:external", batch_key=batch_key),
            priority="resume",
            feature="handover_followup_continue",
            submitted_by="resume",
        )
        container.add_system_log(f"[任务] 已提交: 交接班继续后续上传 batch={batch_key} ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/api/handover/engineer-directory")
def get_handover_engineer_directory(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    runtime_cfg = _runtime_config(container)
    handover_cfg = load_handover_config(runtime_cfg)
    repo = ShiftRosterRepository(handover_cfg)
    try:
        rows = repo.list_engineer_directory(emit_log=container.add_system_log)
    except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"读取工程师目录失败: {exc}") from exc
    return {
        "rows": rows,
        "count": len(rows),
    }


@router.get("/api/jobs/{job_id}")
def get_job(job_id: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        return container.job_service.get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        payload = container.job_service.cancel_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {
        "ok": True,
        "accepted": True,
        "job": payload,
    }


@router.post("/api/jobs/{job_id}/retry")
def retry_job(job_id: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        payload = container.job_service.retry_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "ok": True,
        "accepted": True,
        "job": payload,
    }


@router.get("/api/jobs")
def list_jobs(request: Request, limit: int = 50, statuses: str = "") -> Dict[str, Any]:
    container = request.app.state.container
    normalized_statuses = [
        str(item or "").strip().lower()
        for item in str(statuses or "").split(",")
        if str(item or "").strip()
    ]
    jobs = container.job_service.list_jobs(limit=max(1, min(int(limit or 50), 200)), statuses=normalized_statuses)
    return {
        "jobs": jobs,
        "count": len(jobs),
        "active_job_ids": container.job_service.active_job_ids(include_waiting=True),
        "job_counts": container.job_service.job_counts(),
    }


@router.get("/api/runtime/resources")
def get_runtime_resources(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return container.job_service.get_resource_snapshot()
