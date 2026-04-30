from __future__ import annotations

import copy
import inspect
import json
import os
import re
import socket
import subprocess
import threading
import time
from datetime import date, datetime
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Callable, Dict, List
from urllib.parse import urlsplit, urlunsplit

from fastapi import APIRouter, Body, File, Form, HTTPException, Request, UploadFile

from app.config.config_adapter import normalize_role_mode, resolve_shared_bridge_paths
from app.config.config_compat_cleanup import sanitize_wet_bulb_collection_config
from app.config.config_merge_guard import ConfigValueLossError, merge_user_config_payload
from app.config.secret_masking import mask_settings
from app.config.settings_loader import (
    HandoverSegmentRevisionConflict,
    get_handover_building_segment,
    get_handover_common_segment,
    preserve_segmented_handover_config,
    save_handover_building_segment,
    save_handover_common_segment,
    save_settings,
)
from app.modules.notify.service.webhook_notify_service import WebhookNotifyService
from app.modules.report_pipeline.service.resume_checkpoint_store import (
    resolve_resume_index_path as resolve_monthly_resume_index_path,
)
from app.modules.report_pipeline.service.resume_checkpoint_store import (
    resolve_resume_root_dir as resolve_monthly_resume_root_dir,
)
from app.modules.report_pipeline.service.calculation_service import CalculationService
from app.modules.report_pipeline.service.job_panel_presenter import (
    build_bridge_tasks_summary,
    build_job_panel_summary,
    present_bridge_task,
    present_job_item,
)
from app.modules.report_pipeline.service.scheduler_state_presenter import (
    present_scheduler_overview_items,
    present_scheduler_overview_summary,
    present_scheduler_state,
)
from app.modules.report_pipeline.service.job_service import JobBusyError, TaskEngineUnavailableError
from app.modules.report_pipeline.service.monthly_cache_continue_service import run_monthly_from_file_items
from app.modules.report_pipeline.service.orchestrator_service import OrchestratorService
from app.modules.report_pipeline.service.shared_bridge_waiting_job_helper import (
    start_waiting_bridge_job,
)
from app.modules.shared_bridge.service.shared_source_cache_service import (
    SharedSourceCacheService,
    is_accessible_cached_file_path,
)
from app.modules.shared_bridge.service.bridge_status_presenter import (
    apply_external_source_cache_backfill_overlays,
    present_external_internal_alert_overview,
    present_external_source_cache_overview,
)
from app.modules.shared_bridge.service.dashboard_display_presenter import (
    present_config_guidance_overview,
    present_feature_target_displays,
    present_external_dashboard_display,
    present_external_module_hero_overviews,
    present_external_scheduler_overview,
    present_external_system_overview,
    present_handover_review_overview,
    present_shared_root_diagnostic_overview,
    present_monthly_report_delivery_display,
    present_monthly_report_last_run_display,
    present_updater_mirror_overview,
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
from handover_log_module.service.monthly_change_report_service import MonthlyChangeReportService
from handover_log_module.service.monthly_event_report_service import MonthlyEventReportService
from handover_log_module.service.monthly_report_delivery_service import MonthlyReportDeliveryService
from handover_log_module.service.review_document_state_service import ReviewDocumentStateService
from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService
from handover_log_module.service.review_link_delivery_service import ReviewLinkDeliveryService
from handover_log_module.service.review_session_service import ReviewSessionService
from handover_log_module.service.wet_bulb_collection_service import WetBulbCollectionService
from pipeline_utils import get_app_dir


router = APIRouter(tags=["pipeline"])
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
_HEALTH_COMPONENT_CACHE_ATTR = "_health_component_cache"
_HEALTH_COMPONENT_CACHE_LOCK_ATTR = "_health_component_cache_lock"

_HEALTH_CACHE_TTL_SHARED_BRIDGE_INTERNAL_SEC = 1.5
_HEALTH_CACHE_TTL_SHARED_BRIDGE_EXTERNAL_SEC = 2.5
_HEALTH_CACHE_TTL_REVIEW_STATUS_SEC = 2.0
_HEALTH_CACHE_TTL_REVIEW_ACCESS_SEC = 3.0
_HEALTH_CACHE_TTL_REVIEW_RECIPIENTS_SEC = 8.0
_HEALTH_CACHE_TTL_TARGET_PREVIEW_SEC = 10.0
_HEALTH_CACHE_TTL_MONTHLY_DELIVERY_SEC = 12.0
_HEALTH_CACHE_TTL_SHARED_ROOT_DIAGNOSTIC_SEC = 10.0
_PENDING_RESUME_CACHE_TTL_SEC = 15.0
_PENDING_RESUME_CACHE: Dict[str, Dict[str, Any]] = {}
_PENDING_RESUME_CACHE_LOCK = threading.Lock()


def _invalidate_review_base_probe_cache() -> None:
    return None


def _mirror_handover_review_defaults_to_sqlite(
    container,
    *,
    saved_config: Dict[str, Any],
    building_codes: list[str] | None = None,
) -> None:
    state_service = ReviewDocumentStateService(load_handover_config(saved_config))
    code_filter = {
        str(code or "").strip().upper()
        for code in (building_codes if isinstance(building_codes, list) else [])
        if str(code or "").strip()
    }
    for item in _DEFAULT_REVIEW_BUILDINGS:
        code = str(item.get("code", "") or "").strip().upper()
        building = str(item.get("name", "") or "").strip()
        if code_filter and code not in code_filter:
            continue
        if not building:
            continue
        try:
            mirrored = state_service.persist_defaults_from_config(
                building=building,
                config=saved_config,
            )
        except Exception as exc:  # noqa: BLE001
            container.add_system_log(
                f"[配置] 交接班{building}默认值镜像到SQLite失败: {exc}"
            )
            continue
        if bool(mirrored.get("defaults_updated")):
            container.add_system_log(
                f"[配置] 交接班{building}默认值已镜像到SQLite: "
                f"cabinet_power_fields={mirrored.get('cabinet_power_fields', 0)}, "
                f"footer_inventory_rows={mirrored.get('footer_inventory_rows', 0)}"
            )
_THREAD_LOCK_TYPE = type(threading.Lock())


def _runtime_config(container) -> Dict[str, Any]:
    return copy.deepcopy(container.runtime_config)


def _read_runtime_status_scope_payload(container, scope: str) -> Dict[str, Any] | None:
    scope_text = str(scope or "").strip()
    if not scope_text:
        return None
    coordinator = getattr(container, "runtime_status_coordinator", None)
    if coordinator is None or not callable(getattr(coordinator, "is_running", None)) or not coordinator.is_running():
        return None
    try:
        snapshot = coordinator.read_scope_snapshot(scope_text)
        payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
        if isinstance(payload, dict):
            return payload
        coordinator.request_refresh(reason=f"scope:{scope_text}")
    except Exception:
        return None
    return None


def _health_cached_component(
    request: Request,
    *,
    key: str,
    ttl_sec: float,
    builder: Callable[[], Any],
) -> Any:
    state = request.app.state
    cache = getattr(state, _HEALTH_COMPONENT_CACHE_ATTR, None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(state, _HEALTH_COMPONENT_CACHE_ATTR, cache)
    cache_lock = getattr(state, _HEALTH_COMPONENT_CACHE_LOCK_ATTR, None)
    if not isinstance(cache_lock, _THREAD_LOCK_TYPE):
        cache_lock = threading.Lock()
        setattr(state, _HEALTH_COMPONENT_CACHE_LOCK_ATTR, cache_lock)
    ttl = max(0.0, float(ttl_sec or 0.0))
    now = time.monotonic()
    with cache_lock:
        entry = cache.get(key)
        if isinstance(entry, dict):
            age_sec = now - float(entry.get("ts", 0.0) or 0.0)
            if age_sec <= ttl and entry.get("ready", True) is not False:
                return copy.deepcopy(entry.get("value"))
    value = builder()
    with cache_lock:
        cache[key] = {"ts": time.monotonic(), "value": copy.deepcopy(value)}
    return value


def _health_cached_component_async_default(
    request: Request,
    *,
    key: str,
    ttl_sec: float,
    builder: Callable[[], Any],
    default: Any,
) -> Any:
    state = request.app.state
    cache = getattr(state, _HEALTH_COMPONENT_CACHE_ATTR, None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(state, _HEALTH_COMPONENT_CACHE_ATTR, cache)
    cache_lock = getattr(state, _HEALTH_COMPONENT_CACHE_LOCK_ATTR, None)
    if not isinstance(cache_lock, _THREAD_LOCK_TYPE):
        cache_lock = threading.Lock()
        setattr(state, _HEALTH_COMPONENT_CACHE_LOCK_ATTR, cache_lock)

    ttl = max(0.0, float(ttl_sec or 0.0))
    now = time.monotonic()
    default_value = copy.deepcopy(default)
    start_refresh = False
    return_value = default_value

    with cache_lock:
        entry = cache.get(key)
        if isinstance(entry, dict):
            age_sec = now - float(entry.get("ts", 0.0) or 0.0)
            if bool(entry.get("ready", False)):
                return_value = copy.deepcopy(entry.get("value"))
                if age_sec <= ttl:
                    return return_value
            if not bool(entry.get("refreshing", False)):
                entry["refreshing"] = True
                cache[key] = entry
                start_refresh = True
            return return_value
        cache[key] = {
            "ts": 0.0,
            "value": copy.deepcopy(default_value),
            "ready": False,
            "refreshing": True,
        }
        start_refresh = True

    if start_refresh:
        app_ref = request.app

        def _runner() -> None:
            value = copy.deepcopy(default_value)
            ready = False
            try:
                value = builder()
                ready = True
            except Exception:
                value = copy.deepcopy(default_value)
                ready = False
            finally:
                latest_cache = getattr(app_ref.state, _HEALTH_COMPONENT_CACHE_ATTR, None)
                latest_lock = getattr(app_ref.state, _HEALTH_COMPONENT_CACHE_LOCK_ATTR, None)
                if isinstance(latest_cache, dict) and isinstance(latest_lock, _THREAD_LOCK_TYPE):
                    with latest_lock:
                        latest_cache[key] = {
                            "ts": time.monotonic(),
                            "value": copy.deepcopy(value),
                            "ready": ready,
                            "refreshing": False,
                        }

        threading.Thread(
            target=_runner,
            name=f"health-cache-{key}".replace(":", "-"),
            daemon=True,
        ).start()

    return return_value


def _health_cached_component_sync_default(
    request: Request,
    *,
    key: str,
    ttl_sec: float,
    builder: Callable[[], Any],
    default: Any,
) -> Any:
    state = request.app.state
    cache = getattr(state, _HEALTH_COMPONENT_CACHE_ATTR, None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(state, _HEALTH_COMPONENT_CACHE_ATTR, cache)
    cache_lock = getattr(state, _HEALTH_COMPONENT_CACHE_LOCK_ATTR, None)
    if not isinstance(cache_lock, _THREAD_LOCK_TYPE):
        cache_lock = threading.Lock()
        setattr(state, _HEALTH_COMPONENT_CACHE_LOCK_ATTR, cache_lock)

    ttl = max(0.0, float(ttl_sec or 0.0))
    now = time.monotonic()
    default_value = copy.deepcopy(default)
    stale_value = copy.deepcopy(default_value)
    stale_ready = False

    with cache_lock:
        entry = cache.get(key)
        if isinstance(entry, dict):
            age_sec = now - float(entry.get("ts", 0.0) or 0.0)
            if bool(entry.get("ready", False)):
                stale_value = copy.deepcopy(entry.get("value"))
                stale_ready = True
                if age_sec <= ttl:
                    return stale_value
            if bool(entry.get("refreshing", False)):
                return stale_value
            entry["refreshing"] = True
            cache[key] = entry
        else:
            cache[key] = {
                "ts": 0.0,
                "value": copy.deepcopy(default_value),
                "ready": False,
                "refreshing": True,
            }

    value = copy.deepcopy(stale_value)
    ready = stale_ready
    try:
        value = builder()
        ready = True
    except Exception:
        value = copy.deepcopy(stale_value)
        ready = stale_ready
    finally:
        with cache_lock:
            cache[key] = {
                "ts": time.monotonic(),
                "value": copy.deepcopy(value),
                "ready": ready,
                "refreshing": False,
            }
    return copy.deepcopy(value)


def _empty_job_panel_summary() -> Dict[str, Any]:
    return {
        "jobs": [],
        "count": 0,
        "active_job_ids": [],
        "job_counts": {},
        "display": {
            "running_jobs": [],
            "waiting_resource_items": [],
            "recent_finished_jobs": [],
            "overview": {
                "reason_code": "pending_backend",
                "running_count": 0,
                "waiting_count": 0,
                "bridge_active_count": 0,
                "handover_generation_busy": False,
                "handover_generation_status_text": "任务面板等待后台快照。",
                "recent_failure_title": "",
                "tone": "neutral",
                "status_text": "等待后端任务状态",
                "summary_text": "任务面板状态由后端聚合后返回。",
                "detail_text": "当前等待后台任务快照。",
                "next_action_text": "",
                "focus_title": "等待后端任务状态",
                "focus_meta": "",
                "items": [],
                "actions": [],
            },
        },
    }


def _empty_runtime_resources_summary() -> Dict[str, Any]:
    return {
        "network": {},
        "controlled_browser": {"holder_job_id": "", "queue_length": 0},
        "batch_locks": [],
        "resources": [],
    }


def _shared_source_cache_overview_from_snapshot(payload: Any) -> Dict[str, Any]:
    source_cache = payload if isinstance(payload, dict) else {}
    display_overview = source_cache.get("display_overview", {})
    if isinstance(display_overview, dict) and display_overview:
        return copy.deepcopy(display_overview)
    return present_external_source_cache_overview(source_cache)


def _external_source_cache_overview_has_runtime_rows(payload: Any) -> bool:
    overview = payload if isinstance(payload, dict) else {}
    families = overview.get("families", []) if isinstance(overview.get("families", []), list) else []
    if not families:
        return False
    for family in families:
        if not isinstance(family, dict):
            continue
        bucket_text = str(
            family.get("current_bucket")
            or family.get("best_bucket_key")
            or family.get("reference_bucket_key")
            or ""
        ).strip()
        if bucket_text and bucket_text != "-":
            return True
        buildings = family.get("buildings", []) if isinstance(family.get("buildings", []), list) else []
        for row in buildings:
            if not isinstance(row, dict):
                continue
            status_key = str(row.get("status_key") or row.get("status") or "").strip().lower()
            detail_text = str(row.get("detail_text", "") or "").strip()
            if status_key and status_key != "waiting":
                return True
            if detail_text and detail_text not in {"等待后端明细", "等待共享文件就绪"}:
                return True
    return False


def _remember_external_source_cache_overview(request: Request, payload: Any) -> Dict[str, Any]:
    overview = copy.deepcopy(payload) if isinstance(payload, dict) else {}
    attr_name = "_external_source_cache_overview_last_non_empty"
    if _external_source_cache_overview_has_runtime_rows(overview):
        try:
            setattr(request.app.state, attr_name, copy.deepcopy(overview))
        except Exception:
            pass
        return overview
    try:
        cached = getattr(request.app.state, attr_name, None)
    except Exception:
        cached = None
    if isinstance(cached, dict) and _external_source_cache_overview_has_runtime_rows(cached):
        return copy.deepcopy(cached)
    return overview


def _copy_presented_keys(payload: Any, keys: tuple[str, ...]) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    return {
        key: copy.deepcopy(source[key])
        for key in keys
        if key in source
    }


_EXTERNAL_SOURCE_CACHE_BUILDING_DISPLAY_KEYS = (
    "building",
    "bucket_key",
    "status",
    "status_key",
    "reason_code",
    "ready",
    "tone",
    "status_text",
    "detail_text",
    "downloaded_at",
    "selected_downloaded_at",
    "last_error",
    "relative_path",
    "resolved_file_path",
    "started_at",
    "blocked",
    "blocked_reason",
    "next_probe_at",
    "source_family",
    "using_fallback",
    "version_gap",
    "source_kind",
    "source_kind_text",
    "selection_scope",
    "selection_scope_text",
    "meta_lines",
    "actions",
    "backfill_running",
    "backfill_text",
    "backfill_scope_text",
    "backfill_task_id",
)


_EXTERNAL_SOURCE_CACHE_FAMILY_DISPLAY_KEYS = (
    "key",
    "title",
    "display_title",
    "current_bucket",
    "best_bucket_key",
    "best_bucket_age_hours",
    "best_bucket_age_text",
    "is_best_bucket_too_old",
    "reference_label",
    "age_label",
    "building_reference_label",
    "date_semantic",
    "tone",
    "status_text",
    "summary_text",
    "detail_text",
    "reason_code",
    "display_note_text",
    "error_text",
    "items",
    "meta_lines",
    "actions",
    "can_proceed",
    "ready_count",
    "live_ready_count",
    "live_downloading_count",
    "live_failed_count",
    "live_blocked_count",
    "fallback_buildings",
    "missing_buildings",
    "stale_buildings",
    "failed_buildings",
    "blocked_buildings",
    "last_success_at",
    "has_failures",
    "has_blocked",
    "has_downloading",
    "all_ready",
    "manual_refresh",
    "backfill_running",
    "backfill_text",
    "backfill_scope_text",
    "backfill_task_id",
    "backfill_label",
    "backfill_scope_label",
    "selection_policy",
    "selection_reference_date",
    "used_previous_day_fallback",
    "missing_today_buildings",
    "missing_both_days_buildings",
    "today_selected_count",
    "upload_last_run_at",
    "upload_last_success_at",
    "upload_last_error",
    "upload_record_count",
    "upload_file_count",
    "upload_running",
    "upload_started_at",
    "upload_current_mode",
    "upload_current_scope",
    "upload_running_text",
    "upload_status",
)


_EXTERNAL_SOURCE_CACHE_OVERVIEW_DISPLAY_KEYS = (
    "tone",
    "status_text",
    "summary_text",
    "detail_text",
    "reason_code",
    "display_note_text",
    "reference_bucket_key",
    "error_text",
    "items",
    "actions",
    "can_proceed",
    "can_proceed_latest",
    "family_can_proceed",
)


def _slim_external_source_cache_building(payload: Any) -> Dict[str, Any]:
    return _copy_presented_keys(payload, _EXTERNAL_SOURCE_CACHE_BUILDING_DISPLAY_KEYS)


def _slim_external_source_cache_family(payload: Any) -> Dict[str, Any]:
    family = payload if isinstance(payload, dict) else {}
    slim = _copy_presented_keys(family, _EXTERNAL_SOURCE_CACHE_FAMILY_DISPLAY_KEYS)
    buildings = family.get("buildings", [])
    slim["buildings"] = [
        _slim_external_source_cache_building(row)
        for row in buildings
        if isinstance(row, dict)
    ] if isinstance(buildings, list) else []
    return slim


def _slim_external_source_cache_overview(payload: Any) -> Dict[str, Any]:
    overview = payload if isinstance(payload, dict) else {}
    slim = _copy_presented_keys(overview, _EXTERNAL_SOURCE_CACHE_OVERVIEW_DISPLAY_KEYS)
    families = overview.get("families", [])
    slim["families"] = [
        _slim_external_source_cache_family(family)
        for family in families
        if isinstance(family, dict)
    ] if isinstance(families, list) else []
    return slim


def _is_recoverable_resume_index_error(exc: Exception) -> bool:
    if not isinstance(exc, OSError):
        return False
    winerror = getattr(exc, "winerror", None)
    if winerror in {53, 64, 67}:
        return True
    text = f"{type(exc).__name__}: {exc}".strip().lower()
    recoverable_tokens = (
        "winerror 64",
        "network name is no longer available",
        "specified network name is no longer available",
        "指定的网络名不再可用",
        "系统找不到指定的路径",
        "the system cannot find the path specified",
        "device is not ready",
        "设备尚未就绪",
    )
    return any(token in text for token in recoverable_tokens)


def _pending_resume_cache_key(container, *, role_mode: str) -> str:
    normalized_role = normalize_role_mode(role_mode)
    if normalized_role == "external":
        bridge_service = getattr(container, "shared_bridge_service", None)
        root = str(getattr(bridge_service, "shared_bridge_root", "") or "").strip()
        return f"external:{root or '-'}"
    return f"{normalized_role or 'standalone'}:{id(container)}"


def _resolve_pending_resume_runs_now(container, *, role_mode: str) -> List[Dict[str, Any]]:
    normalized_role = normalize_role_mode(role_mode)
    if normalized_role == "internal":
        return []
    if normalized_role == "external":
        bridge_service = getattr(container, "shared_bridge_service", None)
        if bridge_service is not None and _shared_bridge_is_available(container):
            runs = bridge_service.list_monthly_pending_resume_runs()
            return runs if isinstance(runs, list) else []
        return []
    config = _runtime_config(container)
    orchestrator = OrchestratorService(config)
    runs = orchestrator.list_pending_resume_runs()
    return runs if isinstance(runs, list) else []


def _read_pending_resume_runs_cached(container, *, role_mode: str) -> Dict[str, Any]:
    normalized_role = normalize_role_mode(role_mode)
    if normalized_role == "internal":
        return {
            "runs": [],
            "count": 0,
            "cached": True,
            "refreshing": False,
            "updated_at": "",
            "last_error": "",
            "reason_code": "role_internal",
        }
    key = _pending_resume_cache_key(container, role_mode=normalized_role)
    now = time.monotonic()
    with _PENDING_RESUME_CACHE_LOCK:
        entry = _PENDING_RESUME_CACHE.get(key)
        if isinstance(entry, dict):
            age_sec = now - float(entry.get("updated_monotonic", 0.0) or 0.0)
            if age_sec <= _PENDING_RESUME_CACHE_TTL_SEC and not bool(entry.get("refreshing", False)):
                rows = entry.get("runs", []) if isinstance(entry.get("runs", []), list) else []
                return {
                    "runs": copy.deepcopy(rows),
                    "count": len(rows),
                    "cached": True,
                    "refreshing": False,
                    "updated_at": str(entry.get("updated_at", "") or "").strip(),
                    "last_error": str(entry.get("last_error", "") or "").strip(),
                    "reason_code": str(entry.get("reason_code", "") or "ready").strip(),
                }
        if isinstance(entry, dict) and bool(entry.get("refreshing", False)):
            rows = entry.get("runs", []) if isinstance(entry.get("runs", []), list) else []
            return {
                "runs": copy.deepcopy(rows),
                "count": len(rows),
                "cached": True,
                "refreshing": True,
                "updated_at": str(entry.get("updated_at", "") or "").strip(),
                "last_error": str(entry.get("last_error", "") or "").strip(),
                "reason_code": "refreshing",
            }
        stale_rows = entry.get("runs", []) if isinstance(entry, dict) and isinstance(entry.get("runs", []), list) else []
        stale_updated_at = str(entry.get("updated_at", "") or "").strip() if isinstance(entry, dict) else ""
        stale_error = str(entry.get("last_error", "") or "").strip() if isinstance(entry, dict) else ""
        _PENDING_RESUME_CACHE[key] = {
            "runs": copy.deepcopy(stale_rows),
            "updated_at": stale_updated_at,
            "updated_monotonic": float(entry.get("updated_monotonic", 0.0) or 0.0) if isinstance(entry, dict) else 0.0,
            "last_error": stale_error,
            "reason_code": "refreshing",
            "refreshing": True,
        }

    def _refresh() -> None:
        rows: List[Dict[str, Any]] = []
        last_error = ""
        reason_code = "ready"
        try:
            rows = _resolve_pending_resume_runs_now(container, role_mode=normalized_role)
        except Exception as exc:  # noqa: BLE001
            if _is_recoverable_resume_index_error(exc):
                last_error = str(exc)
                reason_code = "index_unavailable"
            else:
                last_error = str(exc)
                reason_code = "failed"
        with _PENDING_RESUME_CACHE_LOCK:
            _PENDING_RESUME_CACHE[key] = {
                "runs": copy.deepcopy(rows),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "updated_monotonic": time.monotonic(),
                "last_error": last_error,
                "reason_code": reason_code,
                "refreshing": False,
            }

    thread = threading.Thread(target=_refresh, name="pending-resume-refresh", daemon=True)
    thread.start()
    return {
        "runs": copy.deepcopy(stale_rows),
        "count": len(stale_rows),
        "cached": True,
        "refreshing": True,
        "updated_at": stale_updated_at,
        "last_error": stale_error,
        "reason_code": "refreshing",
    }


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


def _ensure_not_internal_role(container, detail: str) -> None:
    if _deployment_role_mode(container) == "internal":
        raise HTTPException(status_code=409, detail=detail)


def _shared_bridge_is_available(container) -> bool:
    snapshot = container.shared_bridge_snapshot() if hasattr(container, "shared_bridge_snapshot") else {}
    if not isinstance(snapshot, dict):
        return False
    return bool(snapshot.get("enabled", False)) and bool(str(snapshot.get("root_dir", "") or "").strip())


def _blank_internal_download_pool_snapshot() -> Dict[str, Any]:
    payload = {
        "enabled": False,
        "browser_ready": False,
        "page_slots": [],
        "active_buildings": [],
        "last_error": "",
    }
    return payload


def _sanitize_shared_bridge_snapshot_for_role(snapshot: Any, *, role_mode: str) -> Dict[str, Any]:
    payload = dict(snapshot) if isinstance(snapshot, dict) else {}
    if normalize_role_mode(role_mode) != "internal":
        payload["internal_download_pool"] = _blank_internal_download_pool_snapshot()
    return payload


def _basic_shared_bridge_status(snapshot: Any, *, role_mode: str) -> Dict[str, Any]:
    payload = dict(snapshot) if isinstance(snapshot, dict) else {}
    return {
        "enabled": bool(payload.get("enabled", False)),
        "role_mode": normalize_role_mode(payload.get("role_mode", role_mode)),
        "root_dir": str(payload.get("root_dir", "") or "").strip(),
        "internal_root_dir": str(payload.get("internal_root_dir", "") or "").strip(),
        "external_root_dir": str(payload.get("external_root_dir", "") or "").strip(),
    }


def _shared_bridge_health_snapshot(container, request: Request, *, role_mode: str) -> Dict[str, Any]:
    snapshot_mode = "internal_light" if str(role_mode or "").strip().lower() == "internal" else "external_full"
    cache_ttl_sec = (
        _HEALTH_CACHE_TTL_SHARED_BRIDGE_INTERNAL_SEC
        if snapshot_mode == "internal_light"
        else _HEALTH_CACHE_TTL_SHARED_BRIDGE_EXTERNAL_SEC
    )

    def _build() -> Dict[str, Any]:
        return _build_shared_bridge_health_snapshot(container, request, role_mode=role_mode)

    return _health_cached_component(
        request,
        key=f"shared_bridge_snapshot:{snapshot_mode}",
        ttl_sec=cache_ttl_sec,
        builder=_build,
    )


def _build_shared_bridge_health_snapshot(container, request: Request, *, role_mode: str) -> Dict[str, Any]:
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


def _shared_bridge_health_snapshot_async_default(
    container,
    request: Request,
    *,
    role_mode: str,
) -> Dict[str, Any]:
    snapshot_mode = "internal_light" if str(role_mode or "").strip().lower() == "internal" else "external_full"
    cache_ttl_sec = (
        _HEALTH_CACHE_TTL_SHARED_BRIDGE_INTERNAL_SEC
        if snapshot_mode == "internal_light"
        else _HEALTH_CACHE_TTL_SHARED_BRIDGE_EXTERNAL_SEC
    )
    return _health_cached_component_async_default(
        request,
        key=f"shared_bridge_snapshot:{snapshot_mode}",
        ttl_sec=cache_ttl_sec,
        builder=lambda: _build_shared_bridge_health_snapshot(container, request, role_mode=role_mode),
        default=_sanitize_shared_bridge_snapshot_for_role({}, role_mode=role_mode),
    )


def _shared_root_diagnostic_snapshot(
    container,
    *,
    shared_bridge_snapshot: Dict[str, Any],
    updater_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    snapshot_getter = getattr(container, "shared_root_diagnostic_snapshot", None)
    if callable(snapshot_getter):
        try:
            payload = snapshot_getter(
                shared_bridge_snapshot=shared_bridge_snapshot,
                updater_snapshot=updater_snapshot,
            )
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
    return {}


def _shared_root_diagnostic_snapshot_async_default(
    container,
    request: Request,
    *,
    role_mode: str,
    shared_bridge_snapshot: Dict[str, Any],
    updater_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    return _health_cached_component_async_default(
        request,
        key=f"shared_root_diagnostic:{normalize_role_mode(role_mode)}",
        ttl_sec=_HEALTH_CACHE_TTL_SHARED_ROOT_DIAGNOSTIC_SEC,
        builder=lambda: _shared_root_diagnostic_snapshot(
            container,
            shared_bridge_snapshot=shared_bridge_snapshot,
            updater_snapshot=updater_snapshot,
        ),
        default={},
    )


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


def _accepted_waiting_job_response(job, task: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = {
        "ok": True,
        "accepted": True,
        "job": job.to_dict() if hasattr(job, "to_dict") else dict(job or {}),
    }
    if isinstance(task, dict) and task:
        payload["bridge_task"] = task
    return payload


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


def _build_latest_handover_review_status(container) -> Dict[str, Any]:
    runtime_cfg = container.runtime_config if isinstance(getattr(container, "runtime_config", {}), dict) else container.config
    handover_cfg = load_handover_config(runtime_cfg)
    try:
        review_service = ReviewSessionService(handover_cfg)
        followup_service = ReviewFollowupTriggerService(handover_cfg)
        status_payload = review_service.get_latest_batch_status()
        target_batch_key = str(status_payload.get("batch_key", "")).strip()
        status_payload["followup_progress"] = (
            followup_service.get_followup_progress(target_batch_key)
            if target_batch_key
            else _empty_followup_progress()
        )
        return status_payload
    except Exception:
        return _empty_handover_review_status()


def _latest_handover_review_status_cached(container, request: Request) -> Dict[str, Any]:
    return _health_cached_component(
        request,
        key="handover_review_status:latest",
        ttl_sec=_HEALTH_CACHE_TTL_REVIEW_STATUS_SEC,
        builder=lambda: _build_latest_handover_review_status(container),
    )


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


def _run_external_day_metric_shared_flow(
    *,
    container,
    config: Dict[str, Any],
    selected_dates: List[str],
    building_scope: str,
    building: str,
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    bridge_service = _shared_bridge_service_or_raise(container)
    target_buildings = [building] if building_scope == "single" else bridge_service.get_source_cache_buildings()
    target_buildings = [item for item in target_buildings if item]
    emit_log(
        "[12项独立上传] 已进入后台共享文件处理: "
        f"dates={','.join(selected_dates)}, scope={building_scope}, building={building or '-'}"
    )
    cached_entries = _filter_accessible_cached_entries(bridge_service.get_day_metric_by_date_cache_entries(
        selected_dates=selected_dates,
        buildings=target_buildings,
    ))
    expected_count = len(selected_dates) * len(target_buildings)
    if len(cached_entries) < expected_count:
        emit_log("[共享缓存][12项] 外网端只读取内网端已登记索引，缺失项将交由内网端补采")
    cached_entry_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
    for item in cached_entries:
        if not isinstance(item, dict):
            continue
        duty_date = str(item.get("duty_date", "") or "").strip()
        building_name = str(item.get("building", "") or "").strip()
        if not duty_date or not building_name:
            continue
        key = (duty_date, building_name)
        if key not in cached_entry_by_key:
            cached_entry_by_key[key] = item
    ready_dates = [
        duty_date
        for duty_date in selected_dates
        if all((duty_date, building_name) in cached_entry_by_key for building_name in target_buildings)
    ]
    missing_dates = [duty_date for duty_date in selected_dates if duty_date not in ready_dates]

    waiting_payload: Dict[str, Any] | None = None
    if missing_dates:
        dedupe_key = _job_dedupe_key(
            "day_metric_wait_shared_bridge",
            selected_dates=missing_dates,
            building_scope=building_scope,
            building=building or "",
            buildings=target_buildings,
        )
        waiting_job, waiting_task = start_waiting_bridge_job(
            job_service=container.job_service,
            bridge_service=bridge_service,
            name="12项独立上传-内网下载",
            worker_handler="day_metric_from_download",
            worker_payload={
                "selected_dates": missing_dates,
                "building_scope": building_scope,
                "building": building or None,
            },
            resource_keys=_job_resource_keys("shared_bridge:day_metric"),
            priority="manual",
            feature="day_metric_from_download",
            dedupe_key=dedupe_key,
            submitted_by="manual",
            bridge_get_or_create_name="get_or_create_day_metric_from_download_task",
            bridge_create_name="create_day_metric_from_download_task",
            bridge_kwargs={
                "selected_dates": missing_dates,
                "building_scope": building_scope,
                "building": building or None,
            },
        )
        emit_log(
            "[共享桥接] 已受理12项共享桥接任务 "
            f"task_id={str(waiting_task.get('task_id', '') or '-').strip() or '-'}, "
            f"dates={','.join(missing_dates)}, scope={building_scope}, building={building or '-'}"
        )
        waiting_payload = _accepted_waiting_job_response(waiting_job, waiting_task)
        if not ready_dates:
            return {
                "ok": True,
                "mode": "waiting_shared_bridge",
                "selected_dates": list(selected_dates),
                "missing_dates": list(missing_dates),
                "waiting": waiting_payload,
            }
        emit_log(
            "[共享缓存][12项] 已命中可直接复用的日期，将先在外网端继续上传；"
            f" 仍缺日期={','.join(missing_dates)}，等待内网补采后自动续跑"
        )

    source_units = [
        {
            "duty_date": str(item.get("duty_date", "") or "").strip(),
            "building": str(item.get("building", "") or "").strip(),
            "source_file": str(item.get("file_path", "") or "").strip(),
        }
        for duty_date in ready_dates
        for building_name in target_buildings
        if (entry := cached_entry_by_key.get((duty_date, building_name)))
        for item in [entry]
    ]
    service = DayMetricStandaloneUploadService(config)
    result = service.continue_from_source_files(
        selected_dates=ready_dates,
        buildings=target_buildings,
        source_units=source_units,
        building_scope=building_scope,
        building=building or None,
        emit_log=emit_log,
    )
    if waiting_payload is not None:
        return {
            "ok": True,
            "mode": "partial_ready",
            "selected_dates": list(ready_dates),
            "missing_dates": list(missing_dates),
            "upload_result": result,
            "waiting": waiting_payload,
        }
    return result


def _run_external_monthly_auto_once_shared_flow(
    *,
    container,
    config: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    bridge_service = _shared_bridge_service_or_raise(container)
    target_buildings = bridge_service.get_source_cache_buildings()
    emit_log("[月报自动流程] 已进入后台共享文件处理")
    selection = _normalize_latest_cache_selection(bridge_service.get_latest_source_cache_selection(
        source_family="monthly_report_family",
        buildings=target_buildings,
    ))
    cached_entries = selection["selected_entries"]
    if not selection["can_proceed"] or len(cached_entries) < len(target_buildings):
        dedupe_key = _job_dedupe_key(
            "monthly_auto_once_wait_shared_bridge",
            bucket_key=str(selection.get("best_bucket_key", "") or "").strip(),
            buildings=target_buildings,
        )
        waiting_job, task = start_waiting_bridge_job(
            job_service=container.job_service,
            bridge_service=bridge_service,
            name="月报自动流程",
            worker_handler="auto_once",
            worker_payload={"source": "月报共享桥接自动恢复"},
            resource_keys=_job_resource_keys("shared_bridge:monthly_report"),
            priority="manual",
            feature="auto_once",
            dedupe_key=dedupe_key,
            submitted_by="manual",
            bridge_get_or_create_name="get_or_create_monthly_auto_once_task",
            bridge_create_name="create_monthly_auto_once_task",
            bridge_kwargs={"source": "manual"},
        )
        emit_log(
            "[共享桥接] 已受理月报自动流程共享桥接任务 "
            f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, "
            f"reason={_build_latest_cache_wait_detail(feature_name='全景平台月报', selection=selection)}"
        )
        return {
            "ok": True,
            "mode": "waiting_shared_bridge",
            "waiting": _accepted_waiting_job_response(waiting_job, task),
        }

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


def _run_external_wet_bulb_shared_flow(
    *,
    container,
    config: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    bridge_service = _shared_bridge_service_or_raise(container)
    target_buildings = bridge_service.get_source_cache_buildings()
    emit_log("[湿球温度定时采集] 已进入后台共享文件处理")
    selection = _normalize_latest_cache_selection(bridge_service.get_latest_source_cache_selection(
        source_family="handover_log_family",
        buildings=target_buildings,
    ))
    cached_entries = selection["selected_entries"]
    if not selection["can_proceed"] or len(cached_entries) < len(target_buildings):
        dedupe_key = _job_dedupe_key(
            "wet_bulb_wait_shared_bridge",
            bucket_key=str(selection.get("best_bucket_key", "") or "").strip(),
            buildings=target_buildings,
        )
        waiting_job, task = start_waiting_bridge_job(
            job_service=container.job_service,
            bridge_service=bridge_service,
            name="湿球温度定时采集",
            worker_handler="wet_bulb_collection_run",
            worker_payload={"source": "湿球温度定时采集"},
            resource_keys=_job_resource_keys("shared_bridge:wet_bulb"),
            priority="manual",
            feature="wet_bulb_collection_run",
            dedupe_key=dedupe_key,
            submitted_by="manual",
            bridge_get_or_create_name="get_or_create_wet_bulb_collection_task",
            bridge_create_name="create_wet_bulb_collection_task",
            bridge_kwargs={"buildings": target_buildings},
        )
        emit_log(
            "[共享桥接] 已受理湿球温度共享桥接任务 "
            f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, "
            f"reason={_build_latest_cache_wait_detail(feature_name='交接班日志', selection=selection)}"
        )
        return {
            "ok": True,
            "mode": "waiting_shared_bridge",
            "waiting": _accepted_waiting_job_response(waiting_job, task),
        }

    source_units = [
        {
            "building": str(item.get("building", "") or "").strip(),
            "file_path": str(item.get("file_path", "") or "").strip(),
        }
        for item in cached_entries
    ]
    service = WetBulbCollectionService(config)
    return service.continue_from_source_units(source_units=source_units, emit_log=emit_log)


def _run_external_multi_date_shared_flow(
    *,
    container,
    config: Dict[str, Any],
    selected_dates: List[str],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    bridge_service = _shared_bridge_service_or_raise(container)
    target_buildings = bridge_service.get_source_cache_buildings()
    emit_log(f"[多日期自动流程] 已进入后台共享文件处理: dates={','.join(selected_dates)}")
    cached_entries = _filter_accessible_cached_entries(bridge_service.get_monthly_by_date_cache_entries(
        selected_dates=selected_dates,
        buildings=target_buildings,
    ))
    expected_count = len(selected_dates) * len(target_buildings)
    if len(cached_entries) < expected_count:
        dedupe_key = _job_dedupe_key(
            "multi_date_wait_shared_bridge",
            selected_dates=selected_dates,
            buildings=target_buildings,
        )
        waiting_job, task = start_waiting_bridge_job(
            job_service=container.job_service,
            bridge_service=bridge_service,
            name="多日期自动流程",
            worker_handler="multi_date",
            worker_payload={"selected_dates": selected_dates},
            resource_keys=_job_resource_keys("shared_bridge:monthly_report"),
            priority="manual",
            feature="multi_date",
            dedupe_key=dedupe_key,
            submitted_by="manual",
            bridge_get_or_create_name="get_or_create_monthly_cache_fill_task",
            bridge_create_name="create_monthly_cache_fill_task",
            bridge_kwargs={"selected_dates": selected_dates},
        )
        emit_log(
            "[共享缓存] 已提交月报历史日期补采任务 "
            f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, dates={','.join(selected_dates)}"
        )
        return {
            "ok": True,
            "mode": "waiting_shared_bridge",
            "waiting": _accepted_waiting_job_response(waiting_job, task),
        }

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


def _run_external_handover_shared_flow(
    *,
    container,
    config: Dict[str, Any],
    buildings: List[str],
    end_time_text: str | None,
    duty_date_text: str | None,
    duty_shift_text: str | None,
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    bridge_service = _shared_bridge_service_or_raise(container)
    target_buildings = buildings or bridge_service.get_source_cache_buildings()
    emit_log(
        "[交接班日志] 已进入后台共享文件处理: "
        f"buildings={','.join(target_buildings) or '-'}, "
        f"duty_date={duty_date_text or '-'}, duty_shift={duty_shift_text or '-'}"
    )
    selection: Dict[str, Any] = {}
    if duty_date_text and duty_shift_text:
        cached_entries = _filter_accessible_cached_entries(bridge_service.get_handover_by_date_cache_entries(
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
            buildings=target_buildings,
        ))
        capacity_cached_entries = _filter_accessible_cached_entries(
            bridge_service.get_handover_capacity_by_date_cache_entries(
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                buildings=target_buildings,
            )
        )
        if len(cached_entries) < len(target_buildings) or len(capacity_cached_entries) < len(target_buildings):
            dedupe_key = _job_dedupe_key(
                "handover_history_wait_shared_bridge",
                duty_date=duty_date_text or "",
                duty_shift=duty_shift_text or "",
                buildings=target_buildings,
                end_time=end_time_text or "",
            )
            waiting_job, task = start_waiting_bridge_job(
                job_service=container.job_service,
                bridge_service=bridge_service,
                name="交接班日志-内网下载生成",
                worker_handler="handover_from_download",
                worker_payload={
                    "buildings": target_buildings,
                    "end_time": end_time_text,
                    "duty_date": duty_date_text,
                    "duty_shift": duty_shift_text,
                },
                resource_keys=_job_resource_keys("shared_bridge:handover"),
                priority="manual",
                feature="handover_from_download",
                dedupe_key=dedupe_key,
                submitted_by="manual",
                bridge_get_or_create_name="get_or_create_handover_cache_fill_task",
                bridge_create_name="create_handover_cache_fill_task",
                bridge_kwargs={
                    "continuation_kind": "handover",
                    "buildings": target_buildings,
                    "duty_date": duty_date_text,
                    "duty_shift": duty_shift_text,
                    "selected_dates": None,
                    "building_scope": None,
                    "building": None,
                },
            )
            emit_log(
                "[共享缓存] 已提交交接班历史缓存补采任务 "
                f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, "
                f"duty_date={duty_date_text}, duty_shift={duty_shift_text}"
            )
            return {
                "ok": True,
                "mode": "waiting_shared_bridge",
                "waiting": _accepted_waiting_job_response(waiting_job, task),
            }
        building_files = [(str(item.get("building", "") or "").strip(), str(item.get("file_path", "") or "").strip()) for item in cached_entries]
        capacity_building_files = [
            (str(item.get("building", "") or "").strip(), str(item.get("file_path", "") or "").strip())
            for item in capacity_cached_entries
        ]
    else:
        selection = _normalize_latest_cache_selection(bridge_service.get_latest_source_cache_selection(
            source_family="handover_log_family",
            buildings=target_buildings,
        ))
        cached_entries = selection["selected_entries"]
        capacity_building_files = []
        for item in cached_entries:
            building = str(item.get("building", "") or "").strip()
            duty_date_value = str(item.get("duty_date", "") or "").strip()
            duty_shift_value = str(item.get("duty_shift", "") or "").strip().lower()
            if not building or not duty_date_value or duty_shift_value not in {"day", "night"}:
                continue
            matched = _filter_accessible_cached_entries(
                bridge_service.get_handover_capacity_by_date_cache_entries(
                    duty_date=duty_date_value,
                    duty_shift=duty_shift_value,
                    buildings=[building],
                )
            )
            if not matched:
                continue
            capacity_building_files.append(
                (
                    str(matched[0].get("building", "") or "").strip(),
                    str(matched[0].get("file_path", "") or "").strip(),
                )
            )
        if (
            not selection["can_proceed"]
            or len(cached_entries) < len(target_buildings)
            or len(capacity_building_files) < len(target_buildings)
        ):
            dedupe_key = _job_dedupe_key(
                "handover_latest_wait_shared_bridge",
                bucket_key=str(selection.get("best_bucket_key", "") or "").strip(),
                buildings=target_buildings,
                end_time=end_time_text or "",
            )
            waiting_job, task = start_waiting_bridge_job(
                job_service=container.job_service,
                bridge_service=bridge_service,
                name="交接班日志-内网下载生成",
                worker_handler="handover_from_download",
                worker_payload={
                    "buildings": target_buildings,
                    "end_time": end_time_text,
                    "duty_date": None,
                    "duty_shift": None,
                },
                resource_keys=_job_resource_keys("shared_bridge:handover"),
                priority="manual",
                feature="handover_from_download",
                dedupe_key=dedupe_key,
                submitted_by="manual",
                bridge_get_or_create_name="get_or_create_handover_from_download_task",
                bridge_create_name="create_handover_from_download_task",
                bridge_kwargs={
                    "buildings": target_buildings,
                    "end_time": end_time_text,
                    "duty_date": None,
                    "duty_shift": None,
                },
            )
            emit_log(
                "[共享桥接] 已受理交接班 latest 共享桥接任务 "
                f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, "
                f"reason={_build_latest_cache_wait_detail(feature_name='交接班日志', selection=selection)}"
            )
            return {
                "ok": True,
                "mode": "waiting_shared_bridge",
                "waiting": _accepted_waiting_job_response(waiting_job, task),
            }
        building_files = [(str(item.get("building", "") or "").strip(), str(item.get("file_path", "") or "").strip()) for item in cached_entries]

    orchestrator = OrchestratorService(config)
    return orchestrator.run_handover_from_files(
        building_files=building_files,
        capacity_building_files=capacity_building_files,
        end_time=end_time_text,
        duty_date=duty_date_text,
        duty_shift=duty_shift_text,
        emit_log=emit_log,
    )


def _call_with_supported_kwargs(func: Callable[..., Any], **kwargs: Any) -> Any:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return func(**kwargs)
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()):
        return func(**kwargs)
    supported_kwargs = {
        key: value
        for key, value in kwargs.items()
        if key in signature.parameters
    }
    return func(**supported_kwargs)


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
    features = cfg.get("features", {})
    if not isinstance(features, dict):
        return cfg
    wet_cfg = features.get("wet_bulb_collection", {})
    if not isinstance(wet_cfg, dict):
        return cfg
    features["wet_bulb_collection"] = sanitize_wet_bulb_collection_config(wet_cfg)
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


def _has_saved_startup_role_bridge_config(container, role_mode: str) -> bool:
    normalized_role = str(role_mode or "").strip().lower()
    if normalized_role not in {"internal", "external"}:
        return False
    config = getattr(container, "config", None)
    if not isinstance(config, dict) or not config:
        config = getattr(container, "runtime_config", None)
    if not isinstance(config, dict):
        return False
    common_cfg = config.get("common", {}) if isinstance(config.get("common", {}), dict) else {}
    shared_bridge_cfg = common_cfg.get("shared_bridge", {})
    if not isinstance(shared_bridge_cfg, dict):
        return False
    try:
        resolved_bridge = resolve_shared_bridge_paths(shared_bridge_cfg, normalized_role)
    except Exception:
        return False
    if not bool(resolved_bridge.get("enabled", False)):
        return False
    role_root_key = "internal_root_dir" if normalized_role == "internal" else "external_root_dir"
    role_root_dir = str(resolved_bridge.get(role_root_key, "") or "").strip()
    active_root_dir = str(resolved_bridge.get("root_dir", "") or "").strip()
    return bool(role_root_dir and active_root_dir)


def _startup_shared_bridge_config_snapshot(container) -> Dict[str, Any]:
    config = getattr(container, "config", None)
    if not isinstance(config, dict) or not config:
        config = getattr(container, "runtime_config", None)
    if not isinstance(config, dict):
        return {}
    common_cfg = config.get("common", {}) if isinstance(config.get("common", {}), dict) else {}
    raw = common_cfg.get("shared_bridge", {}) if isinstance(common_cfg, dict) else {}
    if not isinstance(raw, dict):
        return {}
    keys = (
        "enabled",
        "root_dir",
        "internal_root_dir",
        "external_root_dir",
        "poll_interval_sec",
        "heartbeat_interval_sec",
        "claim_lease_sec",
        "stale_task_timeout_sec",
        "artifact_retention_days",
        "sqlite_busy_timeout_ms",
    )
    snapshot: Dict[str, Any] = {}
    for key in keys:
        value = raw.get(key)
        if key == "enabled":
            snapshot[key] = bool(value)
        elif key.endswith("_sec") or key.endswith("_days") or key.endswith("_ms"):
            try:
                snapshot[key] = int(value)
            except Exception:
                snapshot[key] = value
        else:
            snapshot[key] = str(value or "").strip()
    return snapshot


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
    effective_base_url = ""
    effective_source = ""
    status = "manual_only"
    error = ""

    if configured_base_url:
        effective_base_url = configured_base_url
        effective_source = "manual"
        status = "manual_ok"
    else:
        status = "manual_only"
        error = "请先手工填写审核页访问基地址"

    return {
        "configured": bool(configured_base_url),
        "review_base_url": configured_base_url,
        "review_base_url_effective": effective_base_url,
        "review_base_url_effective_source": effective_source,
        "review_base_url_candidates": [],
        "review_base_url_status": status,
        "review_base_url_error": "" if effective_base_url else error,
        "review_base_url_validated_candidates": [],
        "review_base_url_candidate_results": [],
        "review_base_url_manual_available": True,
        "review_base_url_configured_at": str(persisted.get("configured_at", "") or "").strip(),
        "review_base_url_last_probe_at": str(persisted.get("last_probe_at", "") or "").strip(),
        "duty_date": str(duty_date or "").strip(),
        "duty_shift": str(duty_shift or "").strip().lower(),
        "review_links": _build_review_links_for_base_url(
            review_cfg if isinstance(review_cfg, dict) else {},
            effective_base_url,
        ),
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
    _ = (request_host, port, duty_date, duty_shift)
    return _persist_manual_review_access_snapshot(container)


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
    activation_step = str(getattr(request.app.state, "runtime_activation_step", "") or "").strip()
    startup_role_confirmed = bool(getattr(request.app.state, "startup_role_confirmed", False))
    startup_role_user_exited = bool(getattr(request.app.state, "startup_role_user_exited", False))
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
    startup_handoff_role = str(startup_handoff.get("target_role_mode", "") or "").strip().lower()
    startup_handoff_resume_ready = (
        bool(startup_handoff.get("active", False))
        and startup_handoff_role in {"internal", "external"}
        and activation_phase != "failed"
    )
    saved_role_ready = role_is_valid and _has_saved_startup_role_bridge_config(container, role_mode)
    startup_role_restorable = (
        startup_handoff_resume_ready
        or (saved_role_ready and activation_phase != "failed" and not startup_role_user_exited)
    )
    has_active_resume = False
    if runtime_activated and role_mode == "external":
        try:
            pending_payload = _read_pending_resume_runs_cached(container, role_mode=role_mode)
            pending_runs = pending_payload.get("runs", []) if isinstance(pending_payload, dict) else []
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
        "startup_role_restorable": startup_role_restorable,
        "role_selection_required": (
            not startup_handoff_resume_ready
            and ((not role_is_valid) or (not saved_role_ready) or activation_phase == "failed" or startup_role_user_exited)
        ),
        "startup_role_user_exited": startup_role_user_exited,
        "startup_handoff": startup_handoff,
        "startup_shared_bridge": _startup_shared_bridge_config_snapshot(container),
        "runtime_activated": runtime_activated,
        "activation_phase": activation_phase,
        "activation_error": activation_error,
        "activation_step": activation_step,
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
    persisted_state = _load_review_access_state(container)
    if bool(persisted_state.get("configured", False)) and str(
        persisted_state.get("effective_base_url", "") or ""
    ).strip():
        try:
            container.add_system_log("[交接班审核访问] 启动不再自动探测：已存在有效审核访问配置")
        except Exception:  # noqa: BLE001
            pass
        return
    if getattr(container, "_handover_review_access_probe_scheduled", False):
        return
    setattr(container, "_handover_review_access_probe_scheduled", True)

    def _runner() -> None:
        try:
            time.sleep(max(0.0, float(delay_sec or 0.0)))
            snapshot = _persist_manual_review_access_snapshot(container)
            try:
                delivery_results = ReviewLinkDeliveryService(
                    _runtime_config(container),
                    config_path=container.config_path,
                ).dispatch_pending_review_links(
                    emit_log=container.add_system_log,
                )
            except Exception as exc:  # noqa: BLE001
                delivery_results = []
                container.add_system_log(f"[交接班][审核链接发送] 启动补发失败: {exc}")
            container.add_system_log(
                "[交接班审核访问] 启动检查完成: "
                f"effective={str(snapshot.get('review_base_url_effective', '') or '-').strip() or '-'}, "
                f"source={str(snapshot.get('review_base_url_effective_source', '') or '-').strip() or '-'}, "
                f"review_link_dispatch={len(delivery_results)}"
            )
        except Exception as exc:  # noqa: BLE001
            try:
                container.add_system_log(f"[交接班审核访问] 启动检查失败: {exc}")
            except Exception:  # noqa: BLE001
                pass

    threading.Thread(
        target=_runner,
        name="handover-review-access-bootstrap",
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


def _augment_health_scheduler_displays(payload: Dict[str, Any], *, role_mode: str) -> None:
    if not isinstance(payload, dict):
        return

    scheduler_node = payload.get("scheduler")
    if isinstance(scheduler_node, dict):
        scheduler_node["display"] = present_scheduler_state(
            scheduler_node,
            role_mode=role_mode,
            external_only=True,
        )

    handover_node = payload.get("handover_scheduler")
    if isinstance(handover_node, dict):
        handover_node["display"] = present_scheduler_state(
            handover_node,
            role_mode=role_mode,
            external_only=True,
        )
        morning_node = handover_node.get("morning")
        if isinstance(morning_node, dict):
            morning_node["display"] = present_scheduler_state(
                morning_node,
                role_mode=role_mode,
                external_only=True,
            )
        afternoon_node = handover_node.get("afternoon")
        if isinstance(afternoon_node, dict):
            afternoon_node["display"] = present_scheduler_state(
                afternoon_node,
                role_mode=role_mode,
                external_only=True,
            )

    wet_bulb_container = payload.get("wet_bulb_collection")
    if isinstance(wet_bulb_container, dict) and isinstance(wet_bulb_container.get("scheduler"), dict):
        wet_bulb_container["scheduler"]["display"] = present_scheduler_state(
            wet_bulb_container["scheduler"],
            role_mode=role_mode,
            external_only=True,
        )

    day_metric_container = payload.get("day_metric_upload")
    if isinstance(day_metric_container, dict) and isinstance(day_metric_container.get("scheduler"), dict):
        day_metric_container["scheduler"]["display"] = present_scheduler_state(
            day_metric_container["scheduler"],
            role_mode=role_mode,
            external_only=True,
        )

    alarm_container = payload.get("alarm_event_upload")
    if isinstance(alarm_container, dict) and isinstance(alarm_container.get("scheduler"), dict):
        alarm_container["scheduler"]["display"] = present_scheduler_state(
            alarm_container["scheduler"],
            role_mode=role_mode,
            external_only=True,
        )

    monthly_event_container = payload.get("monthly_event_report")
    if isinstance(monthly_event_container, dict) and isinstance(monthly_event_container.get("scheduler"), dict):
        monthly_event_container["scheduler"]["display"] = present_scheduler_state(
            monthly_event_container["scheduler"],
            role_mode=role_mode,
            external_only=True,
        )

    monthly_change_container = payload.get("monthly_change_report")
    if isinstance(monthly_change_container, dict) and isinstance(monthly_change_container.get("scheduler"), dict):
        monthly_change_container["scheduler"]["display"] = present_scheduler_state(
            monthly_change_container["scheduler"],
            role_mode=role_mode,
            external_only=True,
        )


@router.get("/api/health")
def health(
    request: Request,
    handover_duty_date: str = "",
    handover_duty_shift: str = "",
    health_mode: str = "",
) -> Dict[str, Any]:
    container = request.app.state.container
    runtime_cfg = container.runtime_config
    role_mode = _deployment_role_mode(container)

    def _live_shared_bridge_snapshot_for_role() -> Dict[str, Any]:
        snapshot_mode = "internal_light" if role_mode == "internal" else "external_full"
        if snapshot_mode == "external_full":
            scoped_payload = _read_runtime_status_scope_payload(container, "external_shared_bridge_full")
            if isinstance(scoped_payload, dict) and scoped_payload:
                return scoped_payload
        getter = getattr(container, "shared_bridge_snapshot", None)
        if not callable(getter):
            return {}
        try:
            payload = getter(mode=snapshot_mode)
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    mode_text = str(health_mode or "").strip().lower()
    is_lite_mode = mode_text in {"lite", "fast", "initial"}
    runtime_status_coordinator = getattr(container, "runtime_status_coordinator", None)
    if is_lite_mode and runtime_status_coordinator is not None:
        try:
            if callable(getattr(runtime_status_coordinator, "is_running", None)) and runtime_status_coordinator.is_running():
                snapshot = runtime_status_coordinator.read_scope_snapshot("runtime_health_lite")
                payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
                if isinstance(payload, dict) and payload:
                    payload = {
                        **payload,
                        "shared_bridge": _basic_shared_bridge_status(
                            payload.get("shared_bridge", {}),
                            role_mode=role_mode,
                        ),
                    }
                    return payload
                runtime_status_coordinator.request_refresh(reason="health_lite_route")
        except Exception:
            pass
    include_handover_runtime_context = role_mode != "internal" and not is_lite_mode
    include_network_probe = role_mode != "internal" and not is_lite_mode
    include_wet_bulb_target_preview = role_mode != "internal" and not is_lite_mode
    include_day_metric_target_preview = role_mode != "internal" and not is_lite_mode
    include_alarm_event_target_preview = role_mode != "internal" and not is_lite_mode
    include_engineer_directory_target_preview = role_mode != "internal" and not is_lite_mode

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

    updater_runtime = container.updater_snapshot()

    def _safe_scheduler_snapshot(method_name: str) -> Dict[str, Any]:
        method = getattr(container, method_name, None)
        if not callable(method):
            return {}
        try:
            snapshot = method()
            return snapshot if isinstance(snapshot, dict) else {}
        except Exception:
            return {}

    def _safe_bool_method(method_name: str) -> bool:
        method = getattr(container, method_name, None)
        if not callable(method):
            return False
        try:
            return bool(method())
        except Exception:
            return False

    def _safe_text_method(method_name: str, default: str = "-") -> str:
        method = getattr(container, method_name, None)
        if not callable(method):
            return str(default)
        try:
            value = method()
        except Exception:
            return str(default)
        text = str(value or "").strip()
        return text if text else str(default)

    scheduler_snapshot = _safe_scheduler_snapshot("scheduler_status")
    handover_scheduler_snapshot = container.handover_scheduler_status()
    wet_bulb_cfg = runtime_cfg.get("wet_bulb_collection", {}) if isinstance(runtime_cfg, dict) else {}
    if not isinstance(wet_bulb_cfg, dict):
        wet_bulb_cfg = {}
    wet_bulb_scheduler_snapshot = container.wet_bulb_collection_scheduler_status()
    handover_loaded_cfg: Dict[str, Any] = {}
    if include_engineer_directory_target_preview or include_handover_runtime_context:
        handover_loaded_cfg = load_handover_config(runtime_cfg)
    wet_bulb_target_preview = (
        _health_cached_component_async_default(
            request,
            key="target_preview:wet_bulb",
            ttl_sec=_HEALTH_CACHE_TTL_TARGET_PREVIEW_SEC,
            builder=lambda: WetBulbCollectionService(runtime_cfg).build_target_descriptor(force_refresh=False),
            default={},
        )
        if include_wet_bulb_target_preview
        else {}
    )
    monthly_event_report_service = MonthlyEventReportService(runtime_cfg)
    monthly_change_report_service = MonthlyChangeReportService(runtime_cfg)
    monthly_change_report_scheduler_snapshot = _safe_scheduler_snapshot("monthly_change_report_scheduler_status")
    monthly_change_report_last_run = monthly_change_report_service.get_last_run_snapshot()
    monthly_event_report_scheduler_snapshot = _safe_scheduler_snapshot("monthly_event_report_scheduler_status")
    monthly_event_report_last_run = monthly_event_report_service.get_last_run_snapshot()
    day_metric_upload_scheduler_snapshot = _safe_scheduler_snapshot("day_metric_upload_scheduler_status")
    alarm_event_upload_scheduler_snapshot = _safe_scheduler_snapshot("alarm_event_upload_scheduler_status")
    monthly_report_delivery_service = MonthlyReportDeliveryService(runtime_cfg)
    include_monthly_delivery = role_mode != "internal" and not is_lite_mode

    def _build_monthly_delivery_snapshot(report_type: str) -> Dict[str, Any]:
        try:
            return monthly_report_delivery_service.build_delivery_health_snapshot(
                report_type=report_type,
                emit_log=lambda _text: None,
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "last_run": monthly_report_delivery_service.get_last_run_snapshot(report_type),
                "recipient_status_by_building": [],
                "error": str(exc),
            }

    def _empty_monthly_delivery_snapshot(report_type: str) -> Dict[str, Any]:
        return {
            "last_run": monthly_report_delivery_service.get_last_run_snapshot(report_type),
            "recipient_status_by_building": [],
            "error": "",
        }

    if include_monthly_delivery:
        monthly_event_report_delivery_snapshot = _health_cached_component_async_default(
            request,
            key="monthly_delivery:event",
            ttl_sec=_HEALTH_CACHE_TTL_MONTHLY_DELIVERY_SEC,
            builder=lambda: _build_monthly_delivery_snapshot("event"),
            default=_empty_monthly_delivery_snapshot("event"),
        )
        monthly_change_report_delivery_snapshot = _health_cached_component_async_default(
            request,
            key="monthly_delivery:change",
            ttl_sec=_HEALTH_CACHE_TTL_MONTHLY_DELIVERY_SEC,
            builder=lambda: _build_monthly_delivery_snapshot("change"),
            default=_empty_monthly_delivery_snapshot("change"),
        )
    else:
        monthly_event_report_delivery_snapshot = _empty_monthly_delivery_snapshot("event")
        monthly_change_report_delivery_snapshot = _empty_monthly_delivery_snapshot("change")
    monthly_event_report_last_run_display = present_monthly_report_last_run_display(
        "event",
        monthly_event_report_last_run,
    )
    monthly_change_report_last_run_display = present_monthly_report_last_run_display(
        "change",
        monthly_change_report_last_run,
    )
    monthly_event_report_delivery_display = present_monthly_report_delivery_display(
        "event",
        monthly_event_report_last_run,
        monthly_event_report_delivery_snapshot,
    )
    monthly_change_report_delivery_display = present_monthly_report_delivery_display(
        "change",
        monthly_change_report_last_run,
        monthly_change_report_delivery_snapshot,
    )
    day_metric_target_preview = (
        _health_cached_component_async_default(
            request,
            key="target_preview:day_metric",
            ttl_sec=_HEALTH_CACHE_TTL_TARGET_PREVIEW_SEC,
            builder=lambda: DayMetricBitableExportService(runtime_cfg).build_target_descriptor(force_refresh=False),
            default={},
        )
        if include_day_metric_target_preview
        else {}
    )
    engineer_directory_target_preview = (
        _health_cached_component_async_default(
            request,
            key="target_preview:engineer_directory",
            ttl_sec=_HEALTH_CACHE_TTL_TARGET_PREVIEW_SEC,
            builder=lambda: ShiftRosterRepository(handover_loaded_cfg).build_engineer_directory_target_descriptor(
                force_refresh=False
            ),
            default={},
        )
        if include_engineer_directory_target_preview
        else {}
    )
    alarm_event_target_preview = (
        _health_cached_component_async_default(
            request,
            key="target_preview:alarm_event",
            ttl_sec=_HEALTH_CACHE_TTL_TARGET_PREVIEW_SEC,
            builder=lambda: SharedSourceCacheService(runtime_config=runtime_cfg, store=None).get_alarm_event_upload_target_preview(
                force_refresh=False
            ),
            default={},
        )
        if include_alarm_event_target_preview
        else {}
    )
    feature_target_displays = present_feature_target_displays(
        runtime_cfg,
        engineer_directory_target_preview=engineer_directory_target_preview,
        wet_bulb_target_preview=wet_bulb_target_preview,
        day_metric_target_preview=day_metric_target_preview,
        alarm_event_target_preview=alarm_event_target_preview,
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
    handover_review_recipient_status_by_building: list[Dict[str, Any]] = []
    if include_handover_runtime_context:
        review_status_cache_key = (
            f"handover_review_status:{selected_duty_date}:{selected_duty_shift}"
            if selected_duty_date and selected_duty_shift
            else "handover_review_status:latest"
        )

        def _build_handover_review_status() -> Dict[str, Any]:
            try:
                review_service = ReviewSessionService(handover_loaded_cfg)
                followup_service = ReviewFollowupTriggerService(handover_loaded_cfg)
                if selected_duty_date and selected_duty_shift:
                    status_payload = review_service.get_batch_status_for_duty(
                        selected_duty_date,
                        selected_duty_shift,
                    )
                else:
                    status_payload = review_service.get_latest_batch_status()
                target_batch_key = str(status_payload.get("batch_key", "")).strip()
                status_payload["followup_progress"] = (
                    followup_service.get_followup_progress(target_batch_key)
                    if target_batch_key
                    else _empty_followup_progress()
                )
                return status_payload
            except Exception:  # noqa: BLE001
                return _empty_handover_review_status()

        handover_review_status = _health_cached_component_async_default(
            request,
            key=review_status_cache_key,
            ttl_sec=_HEALTH_CACHE_TTL_REVIEW_STATUS_SEC,
            builder=_build_handover_review_status,
            default=_empty_handover_review_status(),
        )
        handover_review_access = _health_cached_component_async_default(
            request,
            key=(
                "handover_review_access:"
                f"{str(handover_review_status.get('duty_date', '')).strip()}:"
                f"{str(handover_review_status.get('duty_shift', '')).strip().lower()}"
            ),
            ttl_sec=_HEALTH_CACHE_TTL_REVIEW_ACCESS_SEC,
            builder=lambda: _build_handover_review_access(
                container,
                request,
                duty_date=str(handover_review_status.get("duty_date", "")).strip(),
                duty_shift=str(handover_review_status.get("duty_shift", "")).strip().lower(),
            ),
            default=_empty_handover_review_access(),
        )

        def _build_review_recipient_status_by_building() -> list[Dict[str, Any]]:
            try:
                return ReviewLinkDeliveryService(
                    runtime_cfg,
                    config_path=container.config_path,
                ).build_recipient_status_by_building()
            except Exception:
                return []

        handover_review_recipient_status_by_building = _health_cached_component_async_default(
            request,
            key="handover_review_recipient_status_by_building",
            ttl_sec=_HEALTH_CACHE_TTL_REVIEW_RECIPIENTS_SEC,
            builder=_build_review_recipient_status_by_building,
            default=[],
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
    if is_lite_mode:
        shared_bridge_cfg = runtime_cfg.get("shared_bridge", {}) if isinstance(runtime_cfg, dict) else {}
        if not isinstance(shared_bridge_cfg, dict):
            shared_bridge_cfg = {}
        shared_bridge_snapshot = _basic_shared_bridge_status(
            {
                "enabled": bool(shared_bridge_cfg.get("enabled", False)),
                "root_dir": str(shared_bridge_cfg.get("root_dir", "") or "").strip(),
                "role_mode": role_mode,
            },
            role_mode=role_mode,
        )
        shared_root_diagnostic = {}
    else:
        shared_bridge_snapshot = _basic_shared_bridge_status(
            (
                _read_runtime_status_scope_payload(container, "external_shared_bridge_full")
                if role_mode == "external"
                else None
            ) or _shared_bridge_health_snapshot(container, request, role_mode=role_mode),
            role_mode=role_mode,
        )
        shared_root_diagnostic = _shared_root_diagnostic_snapshot_async_default(
            container,
            request,
            role_mode=role_mode,
            shared_bridge_snapshot=shared_bridge_snapshot,
            updater_snapshot=updater_runtime,
        )
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
        "health_mode": "lite" if is_lite_mode else "full",
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
                "enabled": bool(scheduler_snapshot.get("enabled", False)),
                "status": str(scheduler_snapshot.get("status", "未初始化")),
                "next_run_time": str(scheduler_snapshot.get("next_run_time", "")),
                "executor_bound": _safe_bool_method("is_scheduler_executor_bound"),
                "callback_name": _safe_text_method("scheduler_executor_name"),
                "running": bool(scheduler_snapshot.get("running", False)),
                "started_at": str(scheduler_snapshot.get("started_at", "")),
                "last_check_at": str(scheduler_snapshot.get("last_check_at", "")),
                "last_decision": str(scheduler_snapshot.get("last_decision", "")),
                "last_trigger_at": str(scheduler_snapshot.get("last_trigger_at", "")),
                "last_trigger_result": str(scheduler_snapshot.get("last_trigger_result", "")),
                "state_path": str(scheduler_snapshot.get("state_path", "")),
                "state_exists": bool(scheduler_snapshot.get("state_exists", False)),
                "remembered_enabled": bool(scheduler_snapshot.get("remembered_enabled", False)),
                "effective_auto_start_in_gui": bool(
                    scheduler_snapshot.get("effective_auto_start_in_gui", False)
                ),
                "memory_source": str(scheduler_snapshot.get("memory_source", "") or ""),
            },
            "handover_scheduler": {
                "enabled": bool(handover_scheduler_snapshot.get("enabled", False)),
                "running": bool(handover_scheduler_snapshot.get("running", False)),
                "status": str(handover_scheduler_snapshot.get("status", "未初始化")),
                "executor_bound": _safe_bool_method("is_handover_scheduler_executor_bound"),
                "callback_name": _safe_text_method("handover_scheduler_executor_name"),
                "remembered_enabled": bool(handover_scheduler_snapshot.get("remembered_enabled", False)),
                "effective_auto_start_in_gui": bool(
                    handover_scheduler_snapshot.get("effective_auto_start_in_gui", False)
                ),
                "memory_source": str(handover_scheduler_snapshot.get("memory_source", "") or ""),
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
            "engineer_directory": {
                "target_preview": engineer_directory_target_preview,
                "target_display": feature_target_displays.get("engineer_directory", {}),
            },
            "review_status": handover_review_status,
            "review_recipient_status_by_building": handover_review_recipient_status_by_building,
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
                "remembered_enabled": bool(wet_bulb_scheduler_snapshot.get("remembered_enabled", False)),
                "effective_auto_start_in_gui": bool(
                    wet_bulb_scheduler_snapshot.get("effective_auto_start_in_gui", False)
                ),
                "memory_source": str(wet_bulb_scheduler_snapshot.get("memory_source", "") or ""),
                    "executor_bound": _safe_bool_method("is_wet_bulb_collection_scheduler_executor_bound"),
                    "callback_name": _safe_text_method("wet_bulb_collection_scheduler_executor_name"),
                },
                "target_preview": wet_bulb_target_preview,
                "target_display": feature_target_displays.get("wet_bulb_collection", {}),
            },
            "monthly_event_report": {
                "enabled": bool(monthly_event_report_service.is_enabled()),
            "scheduler": {
                "running": bool(monthly_event_report_scheduler_snapshot.get("running", False)),
                "status": str(monthly_event_report_scheduler_snapshot.get("status", "未初始化")),
                "next_run_time": str(monthly_event_report_scheduler_snapshot.get("next_run_time", "")),
                "last_check_at": str(monthly_event_report_scheduler_snapshot.get("last_check_at", "")),
                "last_decision": str(monthly_event_report_scheduler_snapshot.get("last_decision", "")),
                "last_trigger_at": str(monthly_event_report_scheduler_snapshot.get("last_trigger_at", "")),
                "last_trigger_result": str(monthly_event_report_scheduler_snapshot.get("last_trigger_result", "")),
                "state_path": str(monthly_event_report_scheduler_snapshot.get("state_path", "")),
                "state_exists": bool(monthly_event_report_scheduler_snapshot.get("state_exists", False)),
                "remembered_enabled": bool(monthly_event_report_scheduler_snapshot.get("remembered_enabled", False)),
                "effective_auto_start_in_gui": bool(
                    monthly_event_report_scheduler_snapshot.get("effective_auto_start_in_gui", False)
                ),
                "memory_source": str(monthly_event_report_scheduler_snapshot.get("memory_source", "") or ""),
                    "executor_bound": _safe_bool_method("is_monthly_event_report_scheduler_executor_bound"),
                    "callback_name": _safe_text_method("monthly_event_report_scheduler_executor_name"),
                },
                "last_run": {
                    **(monthly_event_report_last_run if isinstance(monthly_event_report_last_run, dict) else {}),
                    "display": monthly_event_report_last_run_display,
                },
                "delivery": {
                    **monthly_event_report_delivery_snapshot,
                    "display": monthly_event_report_delivery_display,
                },
            },
            "monthly_change_report": {
            "enabled": bool(monthly_change_report_service.is_enabled()),
            "scheduler": {
                "running": bool(monthly_change_report_scheduler_snapshot.get("running", False)),
                "status": str(monthly_change_report_scheduler_snapshot.get("status", "未初始化")),
                "next_run_time": str(monthly_change_report_scheduler_snapshot.get("next_run_time", "")),
                "last_check_at": str(monthly_change_report_scheduler_snapshot.get("last_check_at", "")),
                "last_decision": str(monthly_change_report_scheduler_snapshot.get("last_decision", "")),
                "last_trigger_at": str(monthly_change_report_scheduler_snapshot.get("last_trigger_at", "")),
                "last_trigger_result": str(monthly_change_report_scheduler_snapshot.get("last_trigger_result", "")),
                "state_path": str(monthly_change_report_scheduler_snapshot.get("state_path", "")),
                "state_exists": bool(monthly_change_report_scheduler_snapshot.get("state_exists", False)),
                "remembered_enabled": bool(monthly_change_report_scheduler_snapshot.get("remembered_enabled", False)),
                "effective_auto_start_in_gui": bool(
                    monthly_change_report_scheduler_snapshot.get("effective_auto_start_in_gui", False)
                ),
                "memory_source": str(monthly_change_report_scheduler_snapshot.get("memory_source", "") or ""),
                    "executor_bound": _safe_bool_method("is_monthly_change_report_scheduler_executor_bound"),
                    "callback_name": _safe_text_method("monthly_change_report_scheduler_executor_name"),
                },
                "last_run": {
                    **(monthly_change_report_last_run if isinstance(monthly_change_report_last_run, dict) else {}),
                    "display": monthly_change_report_last_run_display,
                },
                "delivery": {
                    **monthly_change_report_delivery_snapshot,
                    "display": monthly_change_report_delivery_display,
                },
            },
            "day_metric_upload": {
            "scheduler": {
                "enabled": bool(day_metric_upload_scheduler_snapshot.get("enabled", False)),
                "running": bool(day_metric_upload_scheduler_snapshot.get("running", False)),
                "status": str(day_metric_upload_scheduler_snapshot.get("status", "未初始化")),
                "next_run_time": str(day_metric_upload_scheduler_snapshot.get("next_run_time", "")),
                "last_check_at": str(day_metric_upload_scheduler_snapshot.get("last_check_at", "")),
                "last_decision": str(day_metric_upload_scheduler_snapshot.get("last_decision", "")),
                "last_trigger_at": str(day_metric_upload_scheduler_snapshot.get("last_trigger_at", "")),
                "last_trigger_result": str(day_metric_upload_scheduler_snapshot.get("last_trigger_result", "")),
                "state_path": str(day_metric_upload_scheduler_snapshot.get("state_path", "")),
                "state_exists": bool(day_metric_upload_scheduler_snapshot.get("state_exists", False)),
                "remembered_enabled": bool(day_metric_upload_scheduler_snapshot.get("remembered_enabled", False)),
                "effective_auto_start_in_gui": bool(
                    day_metric_upload_scheduler_snapshot.get("effective_auto_start_in_gui", False)
                ),
                "memory_source": str(day_metric_upload_scheduler_snapshot.get("memory_source", "") or ""),
                    "executor_bound": _safe_bool_method("is_day_metric_upload_scheduler_executor_bound"),
                    "callback_name": _safe_text_method("day_metric_upload_scheduler_executor_name"),
                },
                "target_preview": day_metric_target_preview,
                "target_display": feature_target_displays.get("day_metric_upload", {}),
            },
            "alarm_event_upload": {
            "enabled": bool(runtime_cfg.get("alarm_export", {}).get("enabled", True))
            if isinstance(runtime_cfg.get("alarm_export", {}), dict)
            else True,
            "scheduler": {
                "enabled": bool(alarm_event_upload_scheduler_snapshot.get("enabled", False)),
                "running": bool(alarm_event_upload_scheduler_snapshot.get("running", False)),
                "status": str(alarm_event_upload_scheduler_snapshot.get("status", "未初始化")),
                "next_run_time": str(alarm_event_upload_scheduler_snapshot.get("next_run_time", "")),
                "last_check_at": str(alarm_event_upload_scheduler_snapshot.get("last_check_at", "")),
                "last_decision": str(alarm_event_upload_scheduler_snapshot.get("last_decision", "")),
                "last_trigger_at": str(alarm_event_upload_scheduler_snapshot.get("last_trigger_at", "")),
                "last_trigger_result": str(alarm_event_upload_scheduler_snapshot.get("last_trigger_result", "")),
                "state_path": str(alarm_event_upload_scheduler_snapshot.get("state_path", "")),
                "state_exists": bool(alarm_event_upload_scheduler_snapshot.get("state_exists", False)),
                "remembered_enabled": bool(alarm_event_upload_scheduler_snapshot.get("remembered_enabled", False)),
                "effective_auto_start_in_gui": bool(
                    alarm_event_upload_scheduler_snapshot.get("effective_auto_start_in_gui", False)
                ),
                "memory_source": str(alarm_event_upload_scheduler_snapshot.get("memory_source", "") or ""),
                    "executor_bound": _safe_bool_method("is_alarm_event_upload_scheduler_executor_bound"),
                    "callback_name": _safe_text_method("alarm_event_upload_scheduler_executor_name"),
                },
                "target_preview": alarm_event_target_preview,
                "target_display": feature_target_displays.get("alarm_event_upload", {}),
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
            "internal_peer": dict(updater_runtime.get("internal_peer", {}))
            if isinstance(updater_runtime.get("internal_peer", {}), dict)
            else {},
            "display_overview": present_updater_mirror_overview(
                {
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
                    "internal_peer": dict(updater_runtime.get("internal_peer", {}))
                    if isinstance(updater_runtime.get("internal_peer", {}), dict)
                    else {},
                    "disabled_reason": str(updater_runtime.get("disabled_reason", "")),
                }
            ),
        },
        "frontend": {
            "mode": str(getattr(container, "frontend_mode", "")),
            "root": str(getattr(container, "frontend_root", "")),
            "assets_dir": str(getattr(container, "frontend_assets_dir", "")),
        },
        "deployment": container.deployment_snapshot(),
        "shared_root_diagnostic": shared_root_diagnostic,
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


@router.post("/api/config-repair/day-metric-upload")
def post_repair_day_metric_upload_config(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        saved = copy.deepcopy(container.config)
        notes = ["12项规则已内置，无需修复"]
        changed = False
        container.add_system_log("[配置修复] 12项配置修复检查完成，无需修复")

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
            "repaired": bool(changed),
            "notes": notes,
            "config": mask_settings(copy.deepcopy(saved)),
            "handover_review_access": handover_review_access,
            "restart_required": False,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _normalize_handover_segment_code(code: str) -> str:
    text = str(code or "").strip().upper()
    if text not in {"A", "B", "C", "D", "E"}:
        raise HTTPException(status_code=400, detail="仅支持 A/B/C/D/E 五个楼栋配置")
    return text


def _normalize_handover_segment_payload(payload: Dict[str, Any]) -> tuple[int, Dict[str, Any]]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")
    raw_revision = payload.get("base_revision", 0)
    try:
        base_revision = int(raw_revision)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="base_revision 必须是整数") from exc
    data = payload.get("data", {})
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="data 必须是 JSON 对象")
    return base_revision, copy.deepcopy(data)


def _apply_container_config_snapshot(container: Any, saved_config: Dict[str, Any], *, mode: str = "light") -> None:
    apply_snapshot = getattr(container, "apply_config_snapshot", None)
    if callable(apply_snapshot):
        apply_snapshot(saved_config, mode=mode)
        return
    container.reload_config(saved_config)


@router.get("/api/config-segments/handover/common")
def get_handover_common_config_segment(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    document = get_handover_common_segment(container.config_path)
    return {
        "revision": int(document.get("revision", 0) or 0),
        "updated_at": str(document.get("updated_at", "") or "").strip(),
        "data": mask_settings(copy.deepcopy(document.get("data", {}))),
    }


@router.put("/api/config-segments/handover/common")
def put_handover_common_config_segment(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    base_revision, data = _normalize_handover_segment_payload(payload)
    try:
        saved_config, document, aggregate_refresh_error = save_handover_common_segment(
            data,
            base_revision=base_revision,
            config_path=container.config_path,
        )
        _apply_container_config_snapshot(container, saved_config, mode="light")
        if aggregate_refresh_error:
            container.add_system_log(
                f"[配置] 交接班公共配置已保存，但聚合配置刷新失败: {aggregate_refresh_error}"
            )
        else:
            container.add_system_log("[配置] 交接班公共配置已保存")
        return {
            "revision": int(document.get("revision", 0) or 0),
            "updated_at": str(document.get("updated_at", "") or "").strip(),
            "data": mask_settings(copy.deepcopy(document.get("data", {}))),
            "apply_mode": "business_only",
            "reload_performed": False,
            "applied_services": ["config_snapshot", "runtime_config", "job_service_config"],
        }
    except HandoverSegmentRevisionConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/api/config-segments/handover/buildings/{code}")
def get_handover_building_config_segment(code: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    normalized_code = _normalize_handover_segment_code(code)
    document = get_handover_building_segment(normalized_code, container.config_path)
    return {
        "revision": int(document.get("revision", 0) or 0),
        "updated_at": str(document.get("updated_at", "") or "").strip(),
        "data": mask_settings(copy.deepcopy(document.get("data", {}))),
    }


@router.put("/api/config-segments/handover/buildings/{code}")
def put_handover_building_config_segment(code: str, payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    normalized_code = _normalize_handover_segment_code(code)
    base_revision, data = _normalize_handover_segment_payload(payload)
    try:
        saved_config, document, aggregate_refresh_error = save_handover_building_segment(
            normalized_code,
            data,
            base_revision=base_revision,
            config_path=container.config_path,
        )
        _apply_container_config_snapshot(container, saved_config, mode="light")
        _mirror_handover_review_defaults_to_sqlite(
            container,
            saved_config=saved_config,
            building_codes=[normalized_code],
        )
        building_text = f"{normalized_code}楼"
        if aggregate_refresh_error:
            container.add_system_log(
                f"[配置] 交接班{building_text}配置已保存，但聚合配置刷新失败: {aggregate_refresh_error}"
            )
        else:
            container.add_system_log(f"[配置] 交接班{building_text}配置已保存")
        return {
            "revision": int(document.get("revision", 0) or 0),
            "updated_at": str(document.get("updated_at", "") or "").strip(),
            "data": mask_settings(copy.deepcopy(document.get("data", {}))),
            "apply_mode": "business_only",
            "reload_performed": False,
            "applied_services": ["config_snapshot", "runtime_config", "job_service_config"],
        }
    except HandoverSegmentRevisionConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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
        response_mode = str(meta.get("response_mode", "") or "").strip().lower() if isinstance(meta, dict) else ""
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
        merged = preserve_segmented_handover_config(
            _strip_retired_wet_bulb_fields(merge_result.merged),
            container.config_path,
        )
        save_kwargs: Dict[str, Any] = {}
        if isinstance(clear_paths, list) and clear_paths:
            save_kwargs["clear_paths"] = clear_paths
        if force_overwrite:
            save_kwargs["force_overwrite"] = force_overwrite
        saved = save_settings(merged, container.config_path, **save_kwargs)
        container.reload_config(saved)
        _mirror_handover_review_defaults_to_sqlite(
            container,
            saved_config=saved,
        )
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
        response_payload = {
            "ok": True,
            "warnings": warnings,
            "handover_review_access": handover_review_access,
            "restart_required": restart_required,
        }
        if response_mode != "minimal":
            response_payload["config"] = mask_settings(saved)
        return response_payload
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
    snapshot = _persist_manual_review_access_snapshot(
        container,
    )
    try:
        delivery_results = ReviewLinkDeliveryService(
            _runtime_config(container),
            config_path=container.config_path,
        ).dispatch_pending_review_links(
            emit_log=container.add_system_log,
        )
    except Exception as exc:  # noqa: BLE001
        delivery_results = []
        container.add_system_log(f"[交接班][审核链接发送] 刷新后补发失败: {exc}")
    container.add_system_log(
        "[交接班审核访问] 手动刷新完成: "
        f"effective={str(snapshot.get('review_base_url_effective', '') or '-').strip() or '-'}, "
        f"source={str(snapshot.get('review_base_url_effective_source', '') or '-').strip() or '-'}, "
        f"status={str(snapshot.get('review_base_url_status', '') or '-').strip() or '-'}, "
        f"review_link_dispatch={len(delivery_results)}"
    )
    return {
        "ok": bool(str(snapshot.get("review_base_url_effective", "") or "").strip()),
        "handover_review_access": snapshot,
    }


@router.post("/api/jobs/handover/review-link/send")
def send_handover_review_link_job(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")
    batch_key = str(payload.get("batch_key", "") or "").strip()
    building = str(payload.get("building", "") or "").strip()
    if not building:
        raise HTTPException(status_code=400, detail="building 不能为空")
    dedupe_key = _job_dedupe_key(
        "handover_review_link_send",
        batch_key=batch_key,
        building=building,
    )
    runtime_cfg = _runtime_config(container)
    try:
        ReviewLinkDeliveryService(runtime_cfg, config_path=container.config_path).validate_manual_send_preflight(
            batch_key=batch_key,
            building=building,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    def _run(job_ctx) -> Dict[str, Any]:
        def _emit(message: str) -> None:
            text = str(message or "").strip()
            if not text:
                return
            try:
                job_ctx.emit_log(text)
            except Exception:  # noqa: BLE001
                pass
            try:
                container.add_system_log(text)
            except Exception:  # noqa: BLE001
                pass

        service = ReviewLinkDeliveryService(runtime_cfg, config_path=container.config_path)
        try:
            return service.send_manual_test(
                batch_key=batch_key,
                building=building,
                emit_log=_emit,
            )
        except Exception as exc:  # noqa: BLE001
            _emit(
                "[交接班][审核链接发送测试] 执行失败 "
                f"batch={batch_key}, building={building or '-'}, error={exc}"
            )
            raise

    try:
        job = _start_background_job(
            container,
            name=f"交接班审核链接发送测试{f' {building}' if building else ''}{f' batch={batch_key}' if batch_key else ''}",
            run_func=_run,
            resource_keys=["handover:review_link_delivery"],
            priority="manual",
            feature="handover_review_link_delivery",
            dedupe_key=dedupe_key,
            submitted_by="manual",
        )
        container.add_system_log(
            f"[任务] 已提交: 交接班审核链接发送测试{f' {building}' if building else ''}{f' batch={batch_key}' if batch_key else ''} ({job.job_id})"
        )
        return {"accepted": True, "job": job.to_dict()}
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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
        _shared_bridge_service_or_raise(container)
        dedupe_key = _job_dedupe_key("monthly_auto_once_external_dispatch", source="manual")

        def _run_external_shared(emit_log):
            return _run_external_monthly_auto_once_shared_flow(
                container=container,
                config=config,
                emit_log=emit_log,
            )

        try:
            job = _start_background_job(
                container,
                name="月报自动流程-共享文件处理",
                run_func=_run_external_shared,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys("shared_bridge:monthly_report"),
                priority="manual",
                feature="monthly_external_dispatch",
                dedupe_key=dedupe_key,
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: 月报自动流程-共享文件处理 ({job.job_id})")
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
        _shared_bridge_service_or_raise(container)
        dedupe_key = _job_dedupe_key("wet_bulb_external_dispatch", source="manual")

        def _run_external_shared(emit_log):
            return _run_external_wet_bulb_shared_flow(
                container=container,
                config=config,
                emit_log=emit_log,
            )

        try:
            job = _start_background_job(
                container,
                name="湿球温度定时采集-共享文件处理",
                run_func=_run_external_shared,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys("shared_bridge:wet_bulb"),
                priority="manual",
                feature="wet_bulb_external_dispatch",
                dedupe_key=dedupe_key,
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: 湿球温度定时采集-共享文件处理 ({job.job_id})")
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


@router.post("/api/jobs/monthly-event-report/run")
def job_monthly_event_report_run(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    config = _runtime_config(container)
    role_mode = _deployment_role_mode(container)
    if role_mode == "internal":
        raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端发起月度事件统计表处理")

    service = MonthlyEventReportService(config)
    raw_scope = payload.get("scope", "all") if isinstance(payload, dict) else "all"
    raw_building = payload.get("building", "") if isinstance(payload, dict) else ""
    try:
        scope, building = service.normalize_scope(raw_scope, raw_building)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    _, _, target_month = service.target_month_window(datetime.now())

    def _run(emit_log):
        return service.run(
            scope=scope,
            building=building,
            emit_log=emit_log,
            source="月度事件统计表处理",
        )

    try:
        job = _start_background_job(
            container,
            name=service.job_name(scope, building),
            run_func=_run,
            worker_handler="",
            worker_payload={},
            resource_keys=_job_resource_keys("monthly_event_report:global"),
            priority="manual",
            feature="monthly_event_report",
            dedupe_key=service.dedupe_key(scope, building, target_month=target_month),
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: {service.job_name(scope, building)} ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/jobs/monthly-change-report/run")
def job_monthly_change_report_run(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    config = _runtime_config(container)
    role_mode = _deployment_role_mode(container)
    if role_mode == "internal":
        raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端发起月度变更统计表处理")

    service = MonthlyChangeReportService(config)
    raw_scope = payload.get("scope", "all") if isinstance(payload, dict) else "all"
    raw_building = payload.get("building", "") if isinstance(payload, dict) else ""
    try:
        scope, building = service.normalize_scope(raw_scope, raw_building)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    _, _, target_month = service.target_month_window(datetime.now())

    def _run(emit_log):
        return service.run(
            scope=scope,
            building=building,
            emit_log=emit_log,
            source="月度变更统计表处理",
        )

    try:
        job = _start_background_job(
            container,
            name=service.job_name(scope, building),
            run_func=_run,
            worker_handler="",
            worker_payload={},
            resource_keys=_job_resource_keys("monthly_change_report:global"),
            priority="manual",
            feature="monthly_change_report",
            dedupe_key=service.dedupe_key(scope, building, target_month=target_month),
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: {service.job_name(scope, building)} ({job.job_id})")
        return job.to_dict()
    except JobBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/jobs/monthly-report/send")
def job_monthly_report_send(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    config = _runtime_config(container)
    role_mode = _deployment_role_mode(container)
    if role_mode == "internal":
        raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端发起月度统计表发送")

    service = MonthlyReportDeliveryService(config)
    raw_payload = payload if isinstance(payload, dict) else {}
    raw_report_type = raw_payload.get("report_type", "event")
    test_mode = bool(raw_payload.get("test_mode", False))
    raw_scope = raw_payload.get("scope", "all")
    raw_building = raw_payload.get("building", "")
    try:
        report_type = service.normalize_report_type(raw_report_type)
        if test_mode:
            scope, building = "test", None
        else:
            scope, building = service.normalize_scope(raw_scope, raw_building)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if report_type == "change":
        monthly_last_run = MonthlyChangeReportService(config).get_last_run_snapshot()
    else:
        monthly_last_run = MonthlyEventReportService(config).get_last_run_snapshot()
    report_resource_key = "monthly_change_report:global" if report_type == "change" else "monthly_event_report:global"
    target_month = str(monthly_last_run.get("target_month", "") or "").strip()
    if not target_month:
        raise HTTPException(status_code=409, detail="缺少最近成功生成的月度统计表文件，请先生成后再发送")

    if test_mode:
        raw_receive_ids = raw_payload.get("receive_ids", raw_payload.get("receive_id", ""))
        receive_ids = service.normalize_receive_ids(raw_receive_ids)
        if not receive_ids:
            receive_ids = [service.default_test_open_id()]
        receive_id_type = str(raw_payload.get("receive_id_type", "") or "").strip() or "open_id"

        def _run(emit_log):
            return service.run_send_test(
                report_type=report_type,
                receive_ids=receive_ids,
                receive_id_type=receive_id_type,
                emit_log=emit_log,
                source="月度统计表发送测试",
            )

        try:
            job = _start_background_job(
                container,
                name=service.test_job_name(report_type),
                run_func=_run,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys(report_resource_key),
                priority="manual",
                feature="monthly_report_send",
                dedupe_key=service.test_dedupe_key(
                    report_type,
                    target_month=target_month,
                    receive_ids=receive_ids,
                ),
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: {service.test_job_name(report_type)} ({job.job_id})")
            return job.to_dict()
        except JobBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    def _run(emit_log):
        return service.run_send(
            report_type=report_type,
            scope=scope,
            building=building,
            emit_log=emit_log,
            source="月度统计表发送",
        )

    try:
        job = _start_background_job(
            container,
            name=service.job_name(report_type, scope, building),
            run_func=_run,
            worker_handler="",
            worker_payload={},
            resource_keys=_job_resource_keys(report_resource_key),
            priority="manual",
            feature="monthly_report_send",
            dedupe_key=service.dedupe_key(report_type, scope, building, target_month=target_month),
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: {service.job_name(report_type, scope, building)} ({job.job_id})")
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
        _shared_bridge_service_or_raise(container)
        dedupe_key = _job_dedupe_key(
            "multi_date_external_dispatch",
            selected_dates=selected_dates,
        )

        def _run_external_shared(emit_log):
            return _run_external_multi_date_shared_flow(
                container=container,
                config=config,
                selected_dates=selected_dates,
                emit_log=emit_log,
            )

        try:
            job = _start_background_job(
                container,
                name="月报多日期-共享文件处理",
                run_func=_run_external_shared,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys("shared_bridge:monthly_report"),
                priority="manual",
                feature="multi_date_external_dispatch",
                dedupe_key=dedupe_key,
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: 月报多日期-共享文件处理 ({job.job_id})")
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
    return _read_pending_resume_runs_cached(container, role_mode=role_mode)


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
        dedupe_key = _job_dedupe_key("resume_upload_wait_shared_bridge", run_id=run_id or "latest", auto_trigger=auto_trigger)
        submitted_by = "auto" if auto_trigger else "manual"
        job, task = start_waiting_bridge_job(
            job_service=container.job_service,
            bridge_service=bridge_service,
            name="断点续传上传",
            worker_handler="resume_upload",
            worker_payload={"run_id": run_id or None, "auto_trigger": auto_trigger},
            resource_keys=_job_resource_keys("shared_bridge:monthly_report"),
            priority="scheduler" if auto_trigger else "manual",
            feature="resume_upload",
            dedupe_key=dedupe_key,
            submitted_by=submitted_by,
            bridge_get_or_create_name="get_or_create_monthly_resume_upload_task",
            bridge_create_name="create_monthly_resume_upload_task",
            bridge_kwargs={"run_id": run_id or None, "auto_trigger": auto_trigger},
        )
        container.add_system_log(
            "[共享桥接] 已提交月报断点续传任务 "
            f"task_id={str(task.get('task_id', '') or '-').strip() or '-'}, run_id={run_id or '-'}"
        )
        return _accepted_waiting_job_response(job, task)

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
    capacity_source_file = ""
    if duty_date_text and duty_shift_text:
        bridge_service = getattr(container, "shared_bridge_service", None)
        if bridge_service is not None:
            matched_capacity = _filter_accessible_cached_entries(
                bridge_service.get_handover_capacity_by_date_cache_entries(
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    buildings=[building],
                )
            )
            if matched_capacity:
                capacity_source_file = str(matched_capacity[0].get("file_path", "") or "").strip()

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            orchestrator = OrchestratorService(config)
            return _call_with_supported_kwargs(
                orchestrator.run_handover_from_file,
                building=building,
                file_path=str(temp_path),
                capacity_source_file=capacity_source_file or None,
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
                "capacity_source_file": capacity_source_file or None,
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
    capacity_building_files: list[tuple[str, str]] = []
    if duty_date_text and duty_shift_text:
        bridge_service = getattr(container, "shared_bridge_service", None)
        if bridge_service is not None:
            matched_capacity = _filter_accessible_cached_entries(
                bridge_service.get_handover_capacity_by_date_cache_entries(
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    buildings=building_list,
                )
            )
            capacity_building_files = [
                (str(item.get("building", "") or "").strip(), str(item.get("file_path", "") or "").strip())
                for item in matched_capacity
                if str(item.get("building", "") or "").strip() and str(item.get("file_path", "") or "").strip()
            ]

    def _run(emit_log):
        notify = WebhookNotifyService(config)
        try:
            orchestrator = OrchestratorService(config)
            return _call_with_supported_kwargs(
                orchestrator.run_handover_from_files,
                building_files=building_files,
                capacity_building_files=capacity_building_files or None,
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
                "capacity_building_files": [
                    {"building": item_building, "file_path": item_path}
                    for item_building, item_path in capacity_building_files
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
        _shared_bridge_service_or_raise(container)
        dedupe_key = _job_dedupe_key(
            "handover_external_dispatch",
            mode="by_date" if (duty_date_text and duty_shift_text) else "latest",
            buildings=buildings,
            duty_date=duty_date_text or "",
            duty_shift=duty_shift_text or "",
            end_time=end_time_text or "",
        )

        def _run_external_shared(emit_log):
            return _run_external_handover_shared_flow(
                container=container,
                config=config,
                buildings=buildings,
                end_time_text=end_time_text,
                duty_date_text=duty_date_text,
                duty_shift_text=duty_shift_text,
                emit_log=emit_log,
            )

        try:
            job = _start_background_job(
                container,
                name="交接班日志-共享文件处理",
                run_func=_run_external_shared,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys("shared_bridge:handover"),
                priority="manual",
                feature="handover_external_dispatch",
                dedupe_key=dedupe_key,
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: 交接班日志-共享文件处理 ({job.job_id})")
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
        dedupe_key = _job_dedupe_key(
            "day_metric_external_dispatch",
            selected_dates=selected_dates,
            building_scope=building_scope,
            building=building or "",
        )

        def _run_external_shared(emit_log):
            return _run_external_day_metric_shared_flow(
                container=container,
                config=config,
                selected_dates=selected_dates,
                building_scope=building_scope,
                building=building,
                emit_log=emit_log,
            )

        try:
            job = _start_background_job(
                container,
                name="12项独立上传-共享文件处理",
                run_func=_run_external_shared,
                worker_handler="",
                worker_payload={},
                resource_keys=_job_resource_keys("shared_bridge:day_metric"),
                priority="manual",
                feature="day_metric_external_dispatch",
                dedupe_key=dedupe_key,
                submitted_by="manual",
            )
            container.add_system_log(f"[任务] 已提交: 12项独立上传-共享文件处理 ({job.job_id})")
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
        target_preview = repo.build_engineer_directory_target_descriptor(force_refresh=True)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"读取工程师目录失败: {exc}") from exc
    return {
        "rows": rows,
        "count": len(rows),
        "target_preview": target_preview,
    }


@router.get("/api/jobs/{job_id}")
def get_job(job_id: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        return present_job_item(container.job_service.get_job(job_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        current_job = container.job_service.get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    bridge_task_id = str(current_job.get("bridge_task_id", "") or "").strip() if isinstance(current_job, dict) else ""
    wait_reason = str(current_job.get("wait_reason", "") or "").strip().lower() if isinstance(current_job, dict) else ""
    if bridge_task_id and wait_reason == "waiting:shared_bridge":
        bridge_service = getattr(container, "shared_bridge_service", None)
        if bridge_service is not None:
            try:
                bridge_service.cancel_task(bridge_task_id)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=503, detail=f"绑定补采任务暂时不可取消，请稍后重试：{exc}") from exc
    try:
        payload = container.job_service.cancel_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {
        "ok": True,
        "accepted": True,
        "job": present_job_item(payload) if isinstance(payload, dict) else payload,
        "job_panel_summary": build_job_panel_summary(container, limit=60),
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
        "job": present_job_item(payload) if isinstance(payload, dict) else payload,
        "job_panel_summary": build_job_panel_summary(container, limit=60),
    }


@router.get("/api/jobs")
def list_jobs(request: Request, limit: int = 50, statuses: str = "") -> Dict[str, Any]:
    container = request.app.state.container
    normalized_statuses = [
        str(item or "").strip().lower()
        for item in str(statuses or "").split(",")
        if str(item or "").strip()
    ]
    runtime_status_coordinator = getattr(container, "runtime_status_coordinator", None)
    safe_limit = max(1, min(int(limit or 50), 200))
    if (
        not normalized_statuses
        and runtime_status_coordinator is not None
        and callable(getattr(runtime_status_coordinator, "is_running", None))
        and runtime_status_coordinator.is_running()
    ):
        try:
            snapshot = runtime_status_coordinator.read_scope_snapshot("job_panel_summary")
            payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
    try:
        if normalized_statuses:
            raw_jobs = container.job_service.list_jobs(limit=safe_limit, statuses=normalized_statuses)
            jobs = [
                present_job_item(job)
                for job in (raw_jobs if isinstance(raw_jobs, list) else [])
                if isinstance(job, dict)
            ]
            job_counts = container.job_service.job_counts()
            return {
                "jobs": jobs,
                "count": len(jobs),
                "active_job_ids": container.job_service.active_job_ids(include_waiting=True),
                "job_counts": job_counts,
            }
        payload = build_job_panel_summary(container, limit=safe_limit, strict=True)
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return payload


@router.get("/api/runtime/resources")
def get_runtime_resources(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    runtime_status_coordinator = getattr(container, "runtime_status_coordinator", None)
    if (
        runtime_status_coordinator is not None
        and callable(getattr(runtime_status_coordinator, "is_running", None))
        and runtime_status_coordinator.is_running()
    ):
        try:
            snapshot = runtime_status_coordinator.read_scope_snapshot("runtime_resources_summary")
            payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
            if isinstance(payload, dict):
                return payload
            runtime_status_coordinator.request_refresh(reason="runtime_resources_route")
            return {
                "network": {},
                "controlled_browser": {"holder_job_id": "", "queue_length": 0},
                "batch_locks": [],
                "resources": [],
            }
        except Exception:
            pass
    try:
        return container.job_service.get_resource_snapshot()
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _external_updated_at() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _external_empty_role_mismatch_display(title: str = "当前不是外网端") -> Dict[str, Any]:
    return {
        "reason_code": "role_mismatch",
        "tone": "neutral",
        "status_text": title,
        "summary_text": "该状态仅在外网端运行时返回。",
        "detail_text": "",
        "actions": {},
    }


def _external_role_mismatch_response(container, **payload: Any) -> Dict[str, Any]:
    deployment = (
        container.deployment_snapshot()
        if callable(getattr(container, "deployment_snapshot", None))
        else {"role_mode": _deployment_role_mode(container) or "internal"}
    )
    return {
        "ok": True,
        "reason_code": "role_mismatch",
        "updated_at": _external_updated_at(),
        "error_text": "",
        "health_lite": {
            "ok": True,
            "health_mode": "lite",
            "deployment": deployment,
            "runtime_activated": bool(getattr(container, "runtime_activated", False)),
            "startup_role_confirmed": bool(getattr(container, "startup_role_confirmed", False)),
            "shared_bridge": _basic_shared_bridge_status({}, role_mode="internal"),
        },
        **payload,
    }


def _external_read_scope(container, scope: str, *, reason_prefix: str) -> Dict[str, Any] | None:
    coordinator = getattr(container, "runtime_status_coordinator", None)
    if (
        coordinator is None
        or not callable(getattr(coordinator, "is_running", None))
        or not coordinator.is_running()
    ):
        return None
    scope_text = str(scope or "").strip()
    if not scope_text:
        return None
    try:
        snapshot = coordinator.read_scope_snapshot(scope_text)
        payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
        if isinstance(payload, dict):
            return payload
        coordinator.request_refresh(reason=f"{reason_prefix}:{scope_text}")
    except Exception:
        return None
    return None


def _build_external_health_lite_payload(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    health_lite = _external_read_scope(
        container,
        "runtime_health_lite",
        reason_prefix="external_bootstrap",
    ) or {
        "ok": True,
        "health_mode": "lite",
        "version": str(getattr(container, "version", "") or ""),
        "config_version": int(container.config.get("version", 0) or 0)
        if isinstance(getattr(container, "config", None), dict)
        else 0,
        "active_job_id": "",
        "active_job_ids": [],
        "job_counts": {},
        "deployment": container.deployment_snapshot() if callable(getattr(container, "deployment_snapshot", None)) else {},
        "shared_bridge": {},
        "runtime_activated": False,
        "activation_phase": "",
        "activation_error": "",
        "startup_role_confirmed": False,
    }
    runtime_activated = bool(getattr(container, "runtime_activated", False))
    startup_role_confirmed = bool(getattr(container, "startup_role_confirmed", False))
    if runtime_activated or startup_role_confirmed:
        health_lite = {
            **health_lite,
            "runtime_activated": runtime_activated,
            "startup_role_confirmed": startup_role_confirmed,
            "role_selection_required": False,
            "startup_role_user_exited": False,
            "activation_phase": "activated" if runtime_activated and startup_role_confirmed else health_lite.get("activation_phase", ""),
            "activation_error": "" if runtime_activated and startup_role_confirmed else health_lite.get("activation_error", ""),
        }
    shared_bridge = _external_read_scope(
        container,
        "external_shared_bridge_full",
        reason_prefix="external_bootstrap",
    )
    if isinstance(shared_bridge, dict) and shared_bridge:
        health_lite = {**health_lite, "shared_bridge": _basic_shared_bridge_status(shared_bridge, role_mode="external")}
    else:
        health_lite = {
            **health_lite,
            "shared_bridge": _basic_shared_bridge_status(
                health_lite.get("shared_bridge", {}) if isinstance(health_lite.get("shared_bridge", {}), dict) else {},
                role_mode="external",
            ),
        }
    return health_lite


def _build_external_bridge_tasks_summary_fast(container) -> Dict[str, Any]:
    bridge_tasks_summary = _external_read_scope(
        container,
        "bridge_tasks_dashboard_summary",
        reason_prefix="external_bridge_tasks",
    )
    if isinstance(bridge_tasks_summary, dict):
        return bridge_tasks_summary
    bridge_tasks_summary_raw = _external_read_scope(
        container,
        "bridge_tasks_summary",
        reason_prefix="external_bridge_tasks",
    ) or {"tasks": [], "count": 0}
    bridge_tasks_rows = (
        bridge_tasks_summary_raw.get("tasks", [])
        if isinstance(bridge_tasks_summary_raw, dict)
        else []
    )
    return build_bridge_tasks_summary(
        [present_bridge_task(task) for task in bridge_tasks_rows if isinstance(task, dict)],
        count=int(
            (bridge_tasks_summary_raw.get("count", 0) if isinstance(bridge_tasks_summary_raw, dict) else 0)
            or len([task for task in bridge_tasks_rows if isinstance(task, dict)])
        ),
    )


def _build_external_source_cache_module(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if _deployment_role_mode(container) == "internal":
        shared_source_cache_overview = {
            "reason_code": "role_mismatch",
            "tone": "neutral",
            "status_text": "当前不是外网端",
            "summary_text": "外网共享源文件状态仅在外网端运行时返回。",
            "detail_text": "",
            "display_note_text": "",
            "reference_bucket_key": "-",
            "error_text": "",
            "items": [],
            "families": [],
            "can_proceed": False,
            "can_proceed_latest": False,
            "actions": {},
        }
        internal_alert_overview = _external_empty_role_mismatch_display("当前不是外网端")
        return _external_role_mismatch_response(
            container,
            shared_source_cache_overview=shared_source_cache_overview,
            internal_alert_overview=internal_alert_overview,
            display={
                "internal_alert_overview": internal_alert_overview,
            },
        )
    bridge_tasks_summary = _build_external_bridge_tasks_summary_fast(container)
    bridge_tasks_rows = (
        bridge_tasks_summary.get("tasks", [])
        if isinstance(bridge_tasks_summary, dict)
        else []
    )
    live_shared_bridge = _external_read_scope(
        container,
        "external_shared_bridge_full",
        reason_prefix="external_source_cache",
    ) or {}
    fast_source_cache_overview: Dict[str, Any] = {}
    bridge_service = getattr(container, "shared_bridge_service", None)
    fast_overview_getter = getattr(bridge_service, "get_external_source_cache_overview_fast", None)
    if callable(fast_overview_getter):
        try:
            last_overview = getattr(request.app.state, "_external_source_cache_overview_last_non_empty", None)
        except Exception:
            last_overview = None
        fast_payload = _health_cached_component_sync_default(
            request,
            key="external_source_cache_overview_fast",
            ttl_sec=1.0,
            builder=fast_overview_getter,
            default=copy.deepcopy(last_overview) if isinstance(last_overview, dict) else {},
        )
        if isinstance(fast_payload, dict):
            fast_source_cache_overview = fast_payload
    shared_source_cache_overview = apply_external_source_cache_backfill_overlays(
        fast_source_cache_overview
        if _external_source_cache_overview_has_runtime_rows(fast_source_cache_overview)
        else _shared_source_cache_overview_from_snapshot(
            live_shared_bridge.get("internal_source_cache", {})
            if isinstance(live_shared_bridge, dict)
            else {}
        ),
        bridge_tasks_rows,
    )
    if not _external_source_cache_overview_has_runtime_rows(shared_source_cache_overview):
        coordinator = getattr(container, "runtime_status_coordinator", None)
        if (
            coordinator is not None
            and callable(getattr(coordinator, "is_running", None))
            and coordinator.is_running()
        ):
            try:
                coordinator.request_refresh(reason="external_source_cache:empty")
            except Exception:
                pass
    shared_source_cache_overview = _remember_external_source_cache_overview(
        request,
        shared_source_cache_overview,
    )
    shared_source_cache_overview = _slim_external_source_cache_overview(shared_source_cache_overview)
    internal_alert_overview = present_external_internal_alert_overview(
        live_shared_bridge.get("internal_alert_status", {})
        if isinstance(live_shared_bridge, dict)
        else {}
    )
    return {
        "ok": True,
        "reason_code": "ok",
        "updated_at": _external_updated_at(),
        "error_text": "",
        "shared_source_cache_overview": shared_source_cache_overview,
        "internal_alert_overview": internal_alert_overview,
        "display": {
            "internal_alert_overview": internal_alert_overview,
        },
    }


def _build_external_jobs_module(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if _deployment_role_mode(container) == "internal":
        summary = _empty_job_panel_summary()
        return _external_role_mismatch_response(
            container,
            job_panel_summary=summary,
            display={
                "task_panel_overview": summary["display"]["overview"],
                "current_task_overview": summary["display"]["overview"],
            },
        )
    job_panel_summary = (
        _external_read_scope(container, "job_panel_dashboard_summary", reason_prefix="external_jobs")
        or _external_read_scope(container, "job_panel_summary", reason_prefix="external_jobs")
        or _empty_job_panel_summary()
    )
    overview = (
        job_panel_summary.get("display", {}).get("overview", {})
        if isinstance(job_panel_summary.get("display", {}), dict)
        else {}
    )
    return {
        "ok": True,
        "reason_code": "ok",
        "updated_at": _external_updated_at(),
        "error_text": "",
        "job_panel_summary": job_panel_summary,
        "display": {
            "task_panel_overview": overview,
            "current_task_overview": overview,
        },
    }


def _build_external_bridge_tasks_module(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if _deployment_role_mode(container) == "internal":
        summary = build_bridge_tasks_summary([], count=0)
        return _external_role_mismatch_response(
            container,
            bridge_tasks_summary=summary,
            display={"bridge_task_panel_overview": summary.get("display", {}).get("overview", {})},
        )
    bridge_tasks_summary = _build_external_bridge_tasks_summary_fast(container)
    return {
        "ok": True,
        "reason_code": "ok",
        "updated_at": _external_updated_at(),
        "error_text": "",
        "bridge_tasks_summary": bridge_tasks_summary,
        "display": {
            "bridge_task_panel_overview": (
                bridge_tasks_summary.get("display", {}).get("overview", {})
                if isinstance(bridge_tasks_summary.get("display", {}), dict)
                else {}
            ),
        },
    }


def _external_scheduler_status_summary(container, *, role_mode: str) -> Dict[str, Any]:
    def _safe_scheduler(method_name: str) -> Dict[str, Any]:
        method = getattr(container, method_name, None)
        if not callable(method):
            return {}
        try:
            payload = method()
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    summary = {
        "scheduler": _safe_scheduler("scheduler_status"),
        "handover_scheduler": _safe_scheduler("handover_scheduler_status"),
        "wet_bulb_collection_scheduler": _safe_scheduler("wet_bulb_collection_scheduler_status"),
        "day_metric_upload_scheduler": _safe_scheduler("day_metric_upload_scheduler_status"),
        "alarm_event_upload_scheduler": _safe_scheduler("alarm_event_upload_scheduler_status"),
        "monthly_event_report_scheduler": _safe_scheduler("monthly_event_report_scheduler_status"),
        "monthly_change_report_scheduler": _safe_scheduler("monthly_change_report_scheduler_status"),
    }
    for key, snapshot in list(summary.items()):
        if isinstance(snapshot, dict):
            summary[key] = {**snapshot, "display": present_scheduler_state(snapshot, role_mode=role_mode)}
    return summary


def _build_external_schedulers_module(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    role_mode = _deployment_role_mode(container)
    if role_mode == "internal":
        return _external_role_mismatch_response(
            container,
            scheduler_status_summary={},
            scheduler_overview_items=[],
            scheduler_overview_summary={},
            display={"scheduler_overview": _external_empty_role_mismatch_display("当前不是外网端")},
        )
    scheduler_status_summary = _external_scheduler_status_summary(container, role_mode=role_mode or "external")
    scheduler_overview_items = present_scheduler_overview_items(
        container.config,
        scheduler_status_summary,
        role_mode=role_mode or "external",
    )
    scheduler_overview_summary = present_scheduler_overview_summary(scheduler_overview_items)
    scheduler_overview = present_external_scheduler_overview(
        scheduler_overview_summary=scheduler_overview_summary,
        scheduler_overview_items=scheduler_overview_items,
    )
    return {
        "ok": True,
        "reason_code": "ok",
        "updated_at": _external_updated_at(),
        "error_text": "",
        "scheduler_status_summary": scheduler_status_summary,
        "scheduler_overview_items": scheduler_overview_items,
        "scheduler_overview_summary": scheduler_overview_summary,
        "display": {
            "scheduler_overview": scheduler_overview,
            "scheduler_overview_items": scheduler_overview_items,
            "scheduler_overview_summary": scheduler_overview_summary,
        },
    }


def _build_external_updater_module(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if _deployment_role_mode(container) == "internal":
        return _external_role_mismatch_response(
            container,
            updater_summary={},
            display={
                "updater_mirror_overview": _external_empty_role_mismatch_display("当前不是外网端"),
                "shared_root_diagnostic_overview": _external_empty_role_mismatch_display("当前不是外网端"),
            },
        )
    try:
        updater_summary = container.updater_snapshot()
        if not isinstance(updater_summary, dict):
            updater_summary = {}
    except Exception:
        updater_summary = {}
    live_shared_bridge = _external_read_scope(
        container,
        "external_shared_bridge_full",
        reason_prefix="external_updater",
    ) or {}
    health_lite = _build_external_health_lite_payload(request)
    deployment_payload = health_lite.get("deployment", {}) if isinstance(health_lite, dict) else {}
    role_mode = normalize_role_mode(
        deployment_payload.get("role_mode", "") if isinstance(deployment_payload, dict) else "external"
    ) or "external"
    shared_root_diagnostic = _shared_root_diagnostic_snapshot_async_default(
        container,
        request,
        role_mode=role_mode,
        shared_bridge_snapshot=live_shared_bridge if isinstance(live_shared_bridge, dict) else {},
        updater_snapshot=updater_summary,
    )
    updater_display_overview = present_updater_mirror_overview(updater_summary)
    updater_summary = {**updater_summary, "display_overview": updater_display_overview}
    return {
        "ok": True,
        "reason_code": "ok",
        "updated_at": _external_updated_at(),
        "error_text": "",
        "updater_summary": updater_summary,
        "shared_root_diagnostic": shared_root_diagnostic,
        "display": {
            "updater_mirror_overview": updater_display_overview,
            "shared_root_diagnostic_overview": present_shared_root_diagnostic_overview(shared_root_diagnostic),
        },
    }


def _build_external_review_module(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if _deployment_role_mode(container) == "internal":
        return _external_role_mismatch_response(
            container,
            handover_review_status=_empty_handover_review_status(),
            handover_review_access=_empty_handover_review_access(),
            handover_review_recipient_status_by_building=[],
            display={"handover_review_overview": _external_empty_role_mismatch_display("当前不是外网端")},
        )
    runtime_cfg = _runtime_config(container)
    handover_review_status = _health_cached_component_async_default(
        request,
        key="handover_review_status:latest",
        ttl_sec=_HEALTH_CACHE_TTL_REVIEW_STATUS_SEC,
        builder=lambda: _build_latest_handover_review_status(container),
        default=_empty_handover_review_status(),
    )
    request_url = getattr(request, "url", None)
    request_host = str(getattr(request_url, "hostname", "") or "").strip()
    request_port = getattr(request_url, "port", None)
    handover_review_access = _health_cached_component_async_default(
        request,
        key=(
            "handover_review_access:"
            f"{str(handover_review_status.get('duty_date', '')).strip()}:"
            f"{str(handover_review_status.get('duty_shift', '')).strip().lower()}"
        ),
        ttl_sec=_HEALTH_CACHE_TTL_REVIEW_ACCESS_SEC,
        builder=lambda: _build_handover_review_access_for_context(
            container,
            request_host=request_host,
            port=request_port,
            duty_date=str(handover_review_status.get("duty_date", "")).strip(),
            duty_shift=str(handover_review_status.get("duty_shift", "")).strip().lower(),
        ),
        default=_empty_handover_review_access(),
    )

    def _build_review_recipient_status_by_building() -> list[Dict[str, Any]]:
        try:
            return ReviewLinkDeliveryService(
                runtime_cfg,
                config_path=container.config_path,
            ).build_recipient_status_by_building()
        except Exception:
            return []

    recipients = _health_cached_component_async_default(
        request,
        key="handover_review_recipient_status_by_building",
        ttl_sec=_HEALTH_CACHE_TTL_REVIEW_RECIPIENTS_SEC,
        builder=_build_review_recipient_status_by_building,
        default=[],
    )
    overview = present_handover_review_overview(
        handover_review_status,
        review_links=handover_review_access.get("review_links", []) if isinstance(handover_review_access, dict) else [],
        recipient_status_by_building=recipients,
    )
    return {
        "ok": True,
        "reason_code": "ok",
        "updated_at": _external_updated_at(),
        "error_text": "",
        "handover_review_status": handover_review_status,
        "handover_review_access": handover_review_access,
        "handover_review_recipient_status_by_building": recipients,
        "display": {"handover_review_overview": overview},
    }


def _build_external_config_guidance_module(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if _deployment_role_mode(container) == "internal":
        return _external_role_mismatch_response(
            container,
            config_guidance_overview=_external_empty_role_mismatch_display("当前不是外网端"),
            feature_target_displays={},
            display={"config_guidance_overview": _external_empty_role_mismatch_display("当前不是外网端")},
        )
    runtime_cfg = _runtime_config(container)
    health_lite = _build_external_health_lite_payload(request)
    deployment_payload = health_lite.get("deployment", {}) if isinstance(health_lite, dict) else {}
    role_mode = normalize_role_mode(
        deployment_payload.get("role_mode", "") if isinstance(deployment_payload, dict) else ""
    ) or "external"
    config_role_mode = normalize_role_mode(
        runtime_cfg.get("deployment", {}).get("role_mode", "")
        if isinstance(runtime_cfg.get("deployment", {}), dict)
        else ""
    )
    day_metric_target_preview = _health_cached_component_async_default(
        request,
        key="target_preview:day_metric",
        ttl_sec=_HEALTH_CACHE_TTL_TARGET_PREVIEW_SEC,
        builder=lambda: DayMetricBitableExportService(runtime_cfg).build_target_descriptor(force_refresh=False),
        default={},
    )
    alarm_event_target_preview = _health_cached_component_async_default(
        request,
        key="target_preview:alarm_event",
        ttl_sec=_HEALTH_CACHE_TTL_TARGET_PREVIEW_SEC,
        builder=lambda: SharedSourceCacheService(runtime_config=runtime_cfg, store=None).get_alarm_event_upload_target_preview(
            force_refresh=False
        ),
        default={},
    )
    config_guidance_overview = present_config_guidance_overview(
        runtime_cfg,
        configured_role_mode=config_role_mode,
        running_role_mode=role_mode,
        day_metric_target_preview=day_metric_target_preview,
        alarm_event_target_preview=alarm_event_target_preview,
    )
    feature_target_displays = present_feature_target_displays(
        runtime_cfg,
        day_metric_target_preview=day_metric_target_preview,
        alarm_event_target_preview=alarm_event_target_preview,
    )
    return {
        "ok": True,
        "reason_code": "ok",
        "updated_at": _external_updated_at(),
        "error_text": "",
        "config_guidance_overview": config_guidance_overview,
        "feature_target_displays": feature_target_displays,
        "display": {"config_guidance_overview": config_guidance_overview},
    }


def _build_external_system_module(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if _deployment_role_mode(container) == "internal":
        return _external_role_mismatch_response(
            container,
            runtime_resources_summary=_empty_runtime_resources_summary(),
            display={"system_overview": _external_empty_role_mismatch_display("当前不是外网端")},
        )
    health_lite = _build_external_health_lite_payload(request)
    runtime_resources_summary = (
        _external_read_scope(container, "runtime_resources_summary", reason_prefix="external_system")
        or _empty_runtime_resources_summary()
    )
    task_overview = {}
    job_panel_summary = (
        _external_read_scope(container, "job_panel_dashboard_summary", reason_prefix="external_system")
        or _external_read_scope(container, "job_panel_summary", reason_prefix="external_system")
        or _empty_job_panel_summary()
    )
    if isinstance(job_panel_summary.get("display", {}), dict):
        task_overview = job_panel_summary.get("display", {}).get("overview", {})
    updater_summary = {}
    try:
        updater_summary = container.updater_snapshot()
        if not isinstance(updater_summary, dict):
            updater_summary = {}
    except Exception:
        updater_summary = {}
    shared_root_diagnostic = {}
    system_overview = present_external_system_overview(
        health_lite=health_lite,
        runtime_resources_summary=runtime_resources_summary,
        task_overview=task_overview,
        shared_root_diagnostic=shared_root_diagnostic,
        updater_overview=present_updater_mirror_overview(updater_summary),
    )
    return {
        "ok": True,
        "reason_code": "ok",
        "updated_at": _external_updated_at(),
        "error_text": "",
        "health_lite": health_lite,
        "runtime_resources_summary": runtime_resources_summary,
        "display": {"system_overview": system_overview},
    }


@router.get("/api/runtime/external/bootstrap")
def get_external_runtime_bootstrap(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if _deployment_role_mode(container) == "internal":
        return _external_role_mismatch_response(container, display={"bootstrap": _external_empty_role_mismatch_display("当前不是外网端")})
    health_lite = _build_external_health_lite_payload(request)
    return {
        "ok": True,
        "reason_code": "ok",
        "updated_at": _external_updated_at(),
        "error_text": "",
        "health_lite": health_lite,
        "display": {"bootstrap": {"reason_code": "ok", "status_text": "外网端基础状态已加载", "tone": "success"}},
    }


@router.get("/api/runtime/external/source-cache")
def get_external_runtime_source_cache(request: Request) -> Dict[str, Any]:
    return _build_external_source_cache_module(request)


@router.get("/api/runtime/external/jobs")
def get_external_runtime_jobs(request: Request) -> Dict[str, Any]:
    return _build_external_jobs_module(request)


@router.get("/api/runtime/external/bridge-tasks")
def get_external_runtime_bridge_tasks(request: Request) -> Dict[str, Any]:
    return _build_external_bridge_tasks_module(request)


@router.get("/api/runtime/external/schedulers")
def get_external_runtime_schedulers(request: Request) -> Dict[str, Any]:
    return _build_external_schedulers_module(request)


@router.get("/api/runtime/external/updater")
def get_external_runtime_updater(request: Request) -> Dict[str, Any]:
    return _build_external_updater_module(request)


@router.get("/api/runtime/external/review-overview")
def get_external_runtime_review_overview(request: Request) -> Dict[str, Any]:
    return _build_external_review_module(request)


@router.get("/api/runtime/external/config-guidance")
def get_external_runtime_config_guidance(request: Request) -> Dict[str, Any]:
    return _build_external_config_guidance_module(request)


@router.get("/api/runtime/external/system")
def get_external_runtime_system(request: Request) -> Dict[str, Any]:
    return _build_external_system_module(request)


@router.get("/api/runtime/external-dashboard-summary")
def get_external_dashboard_summary(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    if _deployment_role_mode(container) == "internal":
        shared_source_cache_overview = {
            "reason_code": "role_mismatch",
            "tone": "neutral",
            "status_text": "当前不是外网端",
            "summary_text": "外网业务控制台状态仅在外网端运行时返回。",
            "detail_text": "",
            "display_note_text": "",
            "reference_bucket_key": "-",
            "error_text": "",
            "items": [],
            "families": [],
            "can_proceed": False,
            "can_proceed_latest": False,
            "actions": {},
        }
        internal_alert_overview = {
            "reason_code": "role_mismatch",
            "tone": "neutral",
            "status_text": "当前不是外网端",
            "summary_text": "",
            "detail_text": "",
            "items": [],
            "buildings": [],
            "actions": {},
        }
        dashboard_display = {
            "shared_source_cache_overview": shared_source_cache_overview,
            "internal_alert_overview": internal_alert_overview,
        }
        return {
            "ok": True,
            "reason_code": "role_mismatch",
            "health_lite": {
                "ok": True,
                "health_mode": "lite",
                "deployment": container.deployment_snapshot()
                if callable(getattr(container, "deployment_snapshot", None))
                else {"role_mode": "internal"},
                "runtime_activated": bool(getattr(container, "runtime_activated", False)),
                "startup_role_confirmed": bool(getattr(container, "startup_role_confirmed", False)),
                "shared_bridge": _basic_shared_bridge_status({}, role_mode="internal"),
            },
            "scheduler_status_summary": {},
            "job_panel_summary": _empty_job_panel_summary(),
            "bridge_tasks_summary": build_bridge_tasks_summary([], count=0),
            "runtime_resources_summary": _empty_runtime_resources_summary(),
            "updater_summary": {},
            "shared_source_cache_overview": shared_source_cache_overview,
            "internal_alert_overview": internal_alert_overview,
            "display": dashboard_display,
        }
    coordinator = getattr(container, "runtime_status_coordinator", None)
    runtime_cfg = _runtime_config(container)
    coordinator_running = (
        coordinator is not None
        and callable(getattr(coordinator, "is_running", None))
        and coordinator.is_running()
    )

    def _read_scope(scope: str) -> Dict[str, Any] | None:
        if not coordinator_running:
            return None
        try:
            snapshot = coordinator.read_scope_snapshot(scope)
            payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
            if isinstance(payload, dict):
                return payload
            coordinator.request_refresh(reason=f"external_dashboard_summary:{scope}")
        except Exception:
            return None
        return None

    health_lite = _read_scope("runtime_health_lite") or {
        "ok": True,
        "health_mode": "lite",
        "version": str(getattr(container, "version", "") or ""),
        "config_version": int(container.config.get("version", 0) or 0)
        if isinstance(getattr(container, "config", None), dict)
        else 0,
        "active_job_id": "",
        "active_job_ids": [],
        "job_counts": {},
        "deployment": container.deployment_snapshot() if callable(getattr(container, "deployment_snapshot", None)) else {},
        "shared_bridge": {},
        "runtime_activated": False,
        "activation_phase": "",
        "activation_error": "",
        "startup_role_confirmed": False,
    }
    runtime_activated = bool(getattr(container, "runtime_activated", False))
    startup_role_confirmed = bool(getattr(container, "startup_role_confirmed", False))
    if runtime_activated or startup_role_confirmed:
        health_lite = {
            **health_lite,
            "runtime_activated": runtime_activated,
            "startup_role_confirmed": startup_role_confirmed,
            "role_selection_required": False,
            "startup_role_user_exited": False,
            "activation_phase": "activated" if runtime_activated and startup_role_confirmed else health_lite.get("activation_phase", ""),
            "activation_error": "" if runtime_activated and startup_role_confirmed else health_lite.get("activation_error", ""),
        }
    live_shared_bridge: Dict[str, Any] = {}
    if not live_shared_bridge:
        live_shared_bridge = _read_scope("external_shared_bridge_full") or {}
    if not live_shared_bridge:
        live_shared_bridge = _shared_bridge_health_snapshot_async_default(
            container,
            request,
            role_mode="external",
        )
    if live_shared_bridge:
        health_lite = {**health_lite, "shared_bridge": live_shared_bridge}
    handover_review_status = _health_cached_component_async_default(
        request,
        key="handover_review_status:latest",
        ttl_sec=_HEALTH_CACHE_TTL_REVIEW_STATUS_SEC,
        builder=lambda: _build_latest_handover_review_status(container),
        default=_empty_handover_review_status(),
    )
    request_url = getattr(request, "url", None)
    request_host = str(getattr(request_url, "hostname", "") or "").strip()
    request_port = getattr(request_url, "port", None)
    handover_review_access = _health_cached_component_async_default(
        request,
        key=(
            "handover_review_access:"
            f"{str(handover_review_status.get('duty_date', '')).strip()}:"
            f"{str(handover_review_status.get('duty_shift', '')).strip().lower()}"
        ),
        ttl_sec=_HEALTH_CACHE_TTL_REVIEW_ACCESS_SEC,
        builder=lambda: _build_handover_review_access_for_context(
            container,
            request_host=request_host,
            port=request_port,
            duty_date=str(handover_review_status.get("duty_date", "")).strip(),
            duty_shift=str(handover_review_status.get("duty_shift", "")).strip().lower(),
        ),
        default=_empty_handover_review_access(),
    )
    def _build_review_recipient_status_by_building() -> list[Dict[str, Any]]:
        try:
            return ReviewLinkDeliveryService(
                runtime_cfg,
                config_path=container.config_path,
            ).build_recipient_status_by_building()
        except Exception:
            return []
    handover_review_recipient_status_by_building = _health_cached_component_async_default(
        request,
        key="handover_review_recipient_status_by_building",
        ttl_sec=_HEALTH_CACHE_TTL_REVIEW_RECIPIENTS_SEC,
        builder=_build_review_recipient_status_by_building,
        default=[],
    )
    internal_alert_overview = present_external_internal_alert_overview(
        live_shared_bridge.get("internal_alert_status", {})
    )
    job_panel_summary = (
        _read_scope("job_panel_dashboard_summary")
        or _read_scope("job_panel_summary")
        or _empty_job_panel_summary()
    )
    bridge_tasks_summary = _read_scope("bridge_tasks_dashboard_summary")
    if not isinstance(bridge_tasks_summary, dict):
        bridge_tasks_summary_raw = _read_scope("bridge_tasks_summary") or {
            "tasks": [],
            "count": 0,
        }
        bridge_tasks_rows = (
            bridge_tasks_summary_raw.get("tasks", [])
            if isinstance(bridge_tasks_summary_raw, dict)
            else []
        )
        bridge_tasks_summary = build_bridge_tasks_summary(
            [present_bridge_task(task) for task in bridge_tasks_rows if isinstance(task, dict)],
            count=int(
                (bridge_tasks_summary_raw.get("count", 0) if isinstance(bridge_tasks_summary_raw, dict) else 0)
                or len([task for task in bridge_tasks_rows if isinstance(task, dict)])
            ),
        )
    bridge_tasks_rows = (
        bridge_tasks_summary.get("tasks", [])
        if isinstance(bridge_tasks_summary, dict)
        else []
    )
    fast_source_cache_overview: Dict[str, Any] = {}
    bridge_service = getattr(container, "shared_bridge_service", None)
    fast_overview_getter = getattr(bridge_service, "get_external_source_cache_overview_fast", None)
    if callable(fast_overview_getter):
        try:
            last_overview = getattr(request.app.state, "_external_source_cache_overview_last_non_empty", None)
        except Exception:
            last_overview = None
        fast_default = copy.deepcopy(last_overview) if isinstance(last_overview, dict) else {}
        fast_payload = _health_cached_component_sync_default(
            request,
            key="external_source_cache_overview_fast",
            ttl_sec=1.0,
            builder=fast_overview_getter,
            default=fast_default,
        )
        if isinstance(fast_payload, dict):
            fast_source_cache_overview = fast_payload
    shared_source_cache_overview = apply_external_source_cache_backfill_overlays(
        fast_source_cache_overview
        if _external_source_cache_overview_has_runtime_rows(fast_source_cache_overview)
        else _shared_source_cache_overview_from_snapshot(
            live_shared_bridge.get("internal_source_cache", {})
        ),
        bridge_tasks_rows,
    )
    if not _external_source_cache_overview_has_runtime_rows(shared_source_cache_overview):
        if coordinator_running:
            try:
                coordinator.request_refresh(reason="external_dashboard_summary:source_cache_empty")
            except Exception:
                pass
        direct_shared_bridge = _shared_bridge_health_snapshot_async_default(
            container,
            request,
            role_mode="external",
        )
        direct_source_overview = apply_external_source_cache_backfill_overlays(
            _shared_source_cache_overview_from_snapshot(
                direct_shared_bridge.get("internal_source_cache", {})
                if isinstance(direct_shared_bridge, dict)
                else {}
            ),
            bridge_tasks_rows,
        )
        if _external_source_cache_overview_has_runtime_rows(direct_source_overview):
            live_shared_bridge = direct_shared_bridge if isinstance(direct_shared_bridge, dict) else live_shared_bridge
            shared_source_cache_overview = direct_source_overview
            internal_alert_overview = present_external_internal_alert_overview(
                live_shared_bridge.get("internal_alert_status", {})
                if isinstance(live_shared_bridge, dict)
                else {}
            )
    shared_source_cache_overview = _remember_external_source_cache_overview(
        request,
        shared_source_cache_overview,
    )
    runtime_resources_summary = _read_scope("runtime_resources_summary") or _empty_runtime_resources_summary()
    deployment_payload = health_lite.get("deployment", {}) if isinstance(health_lite, dict) else {}
    role_mode = normalize_role_mode(
        deployment_payload.get("role_mode", "") if isinstance(deployment_payload, dict) else ""
    )

    def _safe_scheduler(method_name: str) -> Dict[str, Any]:
        method = getattr(container, method_name, None)
        if not callable(method):
            return {}
        try:
            payload = method()
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    updater_summary = {}
    try:
        updater_summary = container.updater_snapshot()
        if not isinstance(updater_summary, dict):
            updater_summary = {}
    except Exception:
        updater_summary = {}
    shared_root_diagnostic = _shared_root_diagnostic_snapshot_async_default(
        container,
        request,
        role_mode=role_mode,
        shared_bridge_snapshot=live_shared_bridge if isinstance(live_shared_bridge, dict) else {},
        updater_snapshot=updater_summary if isinstance(updater_summary, dict) else {},
    )
    updater_display_overview = present_updater_mirror_overview(updater_summary)
    if isinstance(updater_summary, dict):
        updater_summary = {
            **updater_summary,
            "display_overview": updater_display_overview,
        }

    config_role_mode = normalize_role_mode(
        runtime_cfg.get("deployment", {}).get("role_mode", "")
        if isinstance(runtime_cfg.get("deployment", {}), dict)
        else ""
    )
    day_metric_target_preview = (
        _health_cached_component_async_default(
            request,
            key="target_preview:day_metric",
            ttl_sec=_HEALTH_CACHE_TTL_TARGET_PREVIEW_SEC,
            builder=lambda: DayMetricBitableExportService(runtime_cfg).build_target_descriptor(force_refresh=False),
            default={},
        )
        if role_mode != "internal"
        else {}
    )
    alarm_event_target_preview = (
        _health_cached_component_async_default(
            request,
            key="target_preview:alarm_event",
            ttl_sec=_HEALTH_CACHE_TTL_TARGET_PREVIEW_SEC,
            builder=lambda: SharedSourceCacheService(runtime_config=runtime_cfg, store=None).get_alarm_event_upload_target_preview(
                force_refresh=False
            ),
            default={},
        )
        if role_mode != "internal"
        else {}
    )
    config_guidance_overview = present_config_guidance_overview(
        runtime_cfg,
        configured_role_mode=config_role_mode,
        running_role_mode=role_mode,
        day_metric_target_preview=day_metric_target_preview,
        alarm_event_target_preview=alarm_event_target_preview,
    )
    feature_target_displays = present_feature_target_displays(
        runtime_cfg,
        day_metric_target_preview=day_metric_target_preview,
        alarm_event_target_preview=alarm_event_target_preview,
    )

    scheduler_status_summary = {
        "scheduler": _safe_scheduler("scheduler_status"),
        "handover_scheduler": _safe_scheduler("handover_scheduler_status"),
        "wet_bulb_collection_scheduler": _safe_scheduler("wet_bulb_collection_scheduler_status"),
        "day_metric_upload_scheduler": _safe_scheduler("day_metric_upload_scheduler_status"),
        "alarm_event_upload_scheduler": _safe_scheduler("alarm_event_upload_scheduler_status"),
        "monthly_event_report_scheduler": _safe_scheduler("monthly_event_report_scheduler_status"),
        "monthly_change_report_scheduler": _safe_scheduler("monthly_change_report_scheduler_status"),
    }

    for key, snapshot in list(scheduler_status_summary.items()):
        if isinstance(snapshot, dict):
            scheduler_status_summary[key] = {
                **snapshot,
                "display": present_scheduler_state(snapshot, role_mode=role_mode),
            }

    scheduler_overview_items = present_scheduler_overview_items(
        container.config,
        scheduler_status_summary,
        role_mode=role_mode,
    )
    scheduler_overview_summary = present_scheduler_overview_summary(scheduler_overview_items)

    dashboard_display = present_external_dashboard_display(
        shared_source_cache_overview=shared_source_cache_overview,
        review_status=handover_review_status,
        review_links=handover_review_access.get("review_links", []),
        review_recipient_status_by_building=handover_review_recipient_status_by_building,
        task_overview=(
            job_panel_summary.get("display", {}).get("overview", {})
            if isinstance(job_panel_summary.get("display", {}), dict)
            else {}
        ),
        shared_root_diagnostic=shared_root_diagnostic,
    )
    dashboard_display = {
        **dashboard_display,
        "config_guidance_overview": config_guidance_overview,
        "shared_source_cache_overview": shared_source_cache_overview,
        "internal_alert_overview": internal_alert_overview,
        "system_overview": present_external_system_overview(
            health_lite=health_lite,
            runtime_resources_summary=runtime_resources_summary,
            task_overview=(
                job_panel_summary.get("display", {}).get("overview", {})
                if isinstance(job_panel_summary.get("display", {}), dict)
                else {}
            ),
            shared_root_diagnostic=shared_root_diagnostic,
            updater_overview=updater_display_overview,
        ),
        "scheduler_overview": present_external_scheduler_overview(
            scheduler_overview_summary=scheduler_overview_summary,
            scheduler_overview_items=scheduler_overview_items,
        ),
        "task_panel_overview": (
            job_panel_summary.get("display", {}).get("overview", {})
            if isinstance(job_panel_summary.get("display", {}), dict)
            else {}
        ),
        "bridge_task_panel_overview": (
            bridge_tasks_summary.get("display", {}).get("overview", {})
            if isinstance(bridge_tasks_summary.get("display", {}), dict)
            else {}
        ),
        "scheduler_overview_items": scheduler_overview_items,
        "scheduler_overview_summary": scheduler_overview_summary,
        "module_hero_overviews": present_external_module_hero_overviews(
            scheduler_overview_summary=scheduler_overview_summary,
            scheduler_status_summary=scheduler_status_summary,
            review_status=handover_review_status,
            shared_source_cache_overview=shared_source_cache_overview,
            runtime_resources_summary=runtime_resources_summary,
            job_panel_summary=job_panel_summary,
            feature_target_displays=feature_target_displays,
        ),
        "updater_mirror_overview": updater_display_overview,
    }

    return {
        "ok": True,
        "health_lite": health_lite,
        "scheduler_status_summary": scheduler_status_summary,
        "job_panel_summary": job_panel_summary,
        "bridge_tasks_summary": bridge_tasks_summary,
        "runtime_resources_summary": runtime_resources_summary,
        "updater_summary": updater_summary,
        "shared_source_cache_overview": shared_source_cache_overview,
        "internal_alert_overview": internal_alert_overview,
        "display": dashboard_display,
    }
