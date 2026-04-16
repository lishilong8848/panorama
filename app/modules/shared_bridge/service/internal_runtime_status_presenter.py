from __future__ import annotations

import copy
from datetime import datetime
from typing import Any, Dict

from app.config.config_adapter import normalize_role_mode


INTERNAL_RUNTIME_BUILDINGS: Dict[str, str] = {
    "a": "A楼",
    "b": "B楼",
    "c": "C楼",
    "d": "D楼",
    "e": "E楼",
}

_BRIDGE_EVENT_TYPE_LABELS = {
    "await_external": "等待外网继续处理",
    "claimed": "已认领",
    "completed": "已完成",
    "log": "日志",
    "waiting_source_sync": "等待内网补采同步",
    "retired_feature": "停用功能已拦截",
}

_BRIDGE_ERROR_TEXTS = {
    "internal_download_failed": "共享文件准备失败",
    "internal_query_failed": "内网查询失败",
    "external_upload_failed": "外网上传失败",
    "external_continue_failed": "外网继续处理失败",
    "missing_source_file": "缺少共享文件",
    "await_external": "等待外网继续处理",
    "shared_bridge_disabled": "共享桥接未启用",
    "shared_bridge_service_unavailable": "共享桥接服务不可用",
    "disabled_or_switching": "当前未启用共享桥接",
    "disabled_or_unselected": "当前未启用共享桥接",
    "misconfigured": "共享桥接目录未配置",
    "database is locked": "共享桥接数据库正忙，请稍后重试",
    "unable to open database file": "无法打开共享桥接数据库文件",
    "cannot operate on a closed database": "共享桥接数据库连接已关闭",
    "cannot operate on a closed database.": "共享桥接数据库连接已关闭",
}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def bridge_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if "permissionerror" in lowered or "winerror 5" in lowered:
        return "共享桥接目录无写入权限"
    if "no such table" in lowered:
        return "共享桥接数据库结构未初始化"
    return _BRIDGE_ERROR_TEXTS.get(lowered, _BRIDGE_EVENT_TYPE_LABELS.get(lowered, text))


def normalize_internal_runtime_family_summary(payload: Any, *, fallback_bucket: str = "") -> Dict[str, Any]:
    row = payload if isinstance(payload, dict) else {}
    return {
        "ready_count": int(row.get("ready_count", 0) or 0),
        "failed_buildings": [str(item or "").strip() for item in (row.get("failed_buildings", []) or []) if str(item or "").strip()],
        "blocked_buildings": [str(item or "").strip() for item in (row.get("blocked_buildings", []) or []) if str(item or "").strip()],
        "last_success_at": str(row.get("last_success_at", "") or "").strip(),
        "current_bucket": str(row.get("current_bucket", "") or "").strip() or str(fallback_bucket or "").strip(),
    }


def normalize_internal_runtime_building_row(payload: Any, *, building: str, fallback_bucket: str = "") -> Dict[str, Any]:
    row = payload if isinstance(payload, dict) else {}
    return {
        "building": str(row.get("building", "") or "").strip() or building,
        "bucket_key": str(row.get("bucket_key", "") or "").strip() or str(fallback_bucket or "").strip(),
        "status": str(row.get("status", "") or "").strip().lower() or "waiting",
        "ready": bool(row.get("ready", False)),
        "downloaded_at": str(row.get("downloaded_at", "") or "").strip(),
        "last_error": bridge_text(row.get("last_error", "")),
        "relative_path": str(row.get("relative_path", "") or "").strip(),
        "resolved_file_path": str(row.get("resolved_file_path", "") or "").strip(),
        "started_at": str(row.get("started_at", "") or "").strip(),
        "blocked": bool(row.get("blocked", False)),
        "blocked_reason": bridge_text(row.get("blocked_reason", "")),
        "next_probe_at": str(row.get("next_probe_at", "") or "").strip(),
    }


def select_internal_runtime_building_row(payload: Any, *, building: str, fallback_bucket: str = "") -> Dict[str, Any]:
    family = payload if isinstance(payload, dict) else {}
    rows = family.get("buildings", []) if isinstance(family.get("buildings", []), list) else []
    matched = next(
        (
            item
            for item in rows
            if isinstance(item, dict) and str(item.get("building", "") or "").strip() == building
        ),
        {},
    )
    return normalize_internal_runtime_building_row(
        matched,
        building=building,
        fallback_bucket=str(family.get("current_bucket", "") or "").strip() or str(fallback_bucket or "").strip(),
    )


def build_internal_runtime_summary(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    raw_snapshot = snapshot if isinstance(snapshot, dict) else {}
    raw_pool = raw_snapshot.get("internal_download_pool", {}) if isinstance(raw_snapshot.get("internal_download_pool", {}), dict) else {}
    raw_cache = raw_snapshot.get("internal_source_cache", {}) if isinstance(raw_snapshot.get("internal_source_cache", {}), dict) else {}
    current_bucket = str(raw_cache.get("current_hour_bucket", "") or "").strip()
    return {
        "updated_at": now_text(),
        "role_mode": normalize_role_mode(raw_snapshot.get("role_mode")),
        "bridge_enabled": bool(raw_snapshot.get("enabled", False)),
        "agent_status": str(raw_snapshot.get("agent_status", "") or "").strip(),
        "db_status": str(raw_snapshot.get("db_status", "") or "").strip(),
        "last_error": bridge_text(raw_snapshot.get("last_error", "")),
        "last_poll_at": str(raw_snapshot.get("last_poll_at", "") or "").strip(),
        "queue": {
            "pending_internal": int(raw_snapshot.get("pending_internal", 0) or 0),
            "pending_external": int(raw_snapshot.get("pending_external", 0) or 0),
            "problematic": int(raw_snapshot.get("problematic", 0) or 0),
            "task_count": int(raw_snapshot.get("task_count", 0) or 0),
        },
        "pool": {
            "enabled": bool(raw_pool.get("enabled", False)),
            "browser_ready": bool(raw_pool.get("browser_ready", False)),
            "active_buildings": [str(item or "").strip() for item in (raw_pool.get("active_buildings", []) or []) if str(item or "").strip()],
            "last_error": bridge_text(raw_pool.get("last_error", "")),
        },
        "source_cache": {
            "enabled": bool(raw_cache.get("enabled", False)),
            "scheduler_running": bool(raw_cache.get("scheduler_running", False)),
            "current_hour_bucket": current_bucket,
            "last_run_at": str(raw_cache.get("last_run_at", "") or "").strip(),
            "last_success_at": str(raw_cache.get("last_success_at", "") or "").strip(),
            "last_error": bridge_text(raw_cache.get("last_error", "")),
            "cache_root": str(raw_cache.get("cache_root", "") or "").strip(),
            "current_hour_refresh": copy.deepcopy(raw_cache.get("current_hour_refresh", {}) if isinstance(raw_cache.get("current_hour_refresh", {}), dict) else {}),
            "handover_log_family": normalize_internal_runtime_family_summary(
                raw_cache.get("handover_log_family", {}),
                fallback_bucket=current_bucket,
            ),
            "handover_capacity_report_family": normalize_internal_runtime_family_summary(
                raw_cache.get("handover_capacity_report_family", {}),
                fallback_bucket=current_bucket,
            ),
            "monthly_report_family": normalize_internal_runtime_family_summary(
                raw_cache.get("monthly_report_family", {}),
                fallback_bucket=current_bucket,
            ),
            "alarm_event_family": normalize_internal_runtime_family_summary(
                raw_cache.get("alarm_event_family", {}),
                fallback_bucket=str(raw_cache.get("alarm_event_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
            ),
        },
    }


def build_internal_runtime_building_status(snapshot: Dict[str, Any], *, building: str, building_code: str) -> Dict[str, Any]:
    raw_snapshot = snapshot if isinstance(snapshot, dict) else {}
    raw_pool = raw_snapshot.get("internal_download_pool", {}) if isinstance(raw_snapshot.get("internal_download_pool", {}), dict) else {}
    raw_cache = raw_snapshot.get("internal_source_cache", {}) if isinstance(raw_snapshot.get("internal_source_cache", {}), dict) else {}
    slot_rows = raw_pool.get("page_slots", []) if isinstance(raw_pool.get("page_slots", []), list) else []
    matched_slot = next(
        (
            item
            for item in slot_rows
            if isinstance(item, dict) and str(item.get("building", "") or "").strip() == building
        ),
        {"building": building},
    )
    current_bucket = str(raw_cache.get("current_hour_bucket", "") or "").strip()
    return {
        "updated_at": now_text(),
        "building": building,
        "building_code": building_code,
        "page_slot": copy.deepcopy(matched_slot if isinstance(matched_slot, dict) else {"building": building}),
        "source_families": {
            "handover_log_family": select_internal_runtime_building_row(
                raw_cache.get("handover_log_family", {}),
                building=building,
                fallback_bucket=current_bucket,
            ),
            "handover_capacity_report_family": select_internal_runtime_building_row(
                raw_cache.get("handover_capacity_report_family", {}),
                building=building,
                fallback_bucket=current_bucket,
            ),
            "monthly_report_family": select_internal_runtime_building_row(
                raw_cache.get("monthly_report_family", {}),
                building=building,
                fallback_bucket=current_bucket,
            ),
            "alarm_event_family": select_internal_runtime_building_row(
                raw_cache.get("alarm_event_family", {}),
                building=building,
                fallback_bucket=str(raw_cache.get("alarm_event_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
            ),
        },
        "pool": {
            "browser_ready": bool(raw_pool.get("browser_ready", False)),
            "last_error": bridge_text(raw_pool.get("last_error", "")),
        },
    }


def build_empty_internal_runtime_summary(*, role_mode: str = "internal") -> Dict[str, Any]:
    return build_internal_runtime_summary({"role_mode": role_mode})


def build_empty_internal_runtime_building_status(*, building: str, building_code: str) -> Dict[str, Any]:
    return build_internal_runtime_building_status(
        {"role_mode": "internal"},
        building=building,
        building_code=building_code,
    )
