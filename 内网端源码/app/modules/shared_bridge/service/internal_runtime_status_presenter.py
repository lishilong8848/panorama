from __future__ import annotations

import copy
from datetime import datetime
from typing import Any, Dict

from app.config.config_adapter import normalize_role_mode
from app.modules.shared_bridge.service.bridge_status_presenter import (
    bridge_text,
    present_current_hour_refresh_overview,
    present_internal_download_pool_overview,
    present_internal_page_slot,
    present_internal_source_cache_overview,
    present_source_cache_building_row,
    present_source_cache_family,
)


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


def normalize_internal_runtime_family_summary(
    payload: Any,
    *,
    key: str = "",
    title: str = "",
    fallback_bucket: str = "",
) -> Dict[str, Any]:
    row = payload if isinstance(payload, dict) else {}
    return present_source_cache_family(
        row,
        key=key,
        title=title,
        fallback_bucket=str(row.get("current_bucket", "") or "").strip() or str(fallback_bucket or "").strip(),
        bucket_scope_text="本小时",
    )


def normalize_internal_runtime_building_row(
    payload: Any,
    *,
    building: str,
    fallback_bucket: str = "",
    source_family: str = "",
) -> Dict[str, Any]:
    return present_source_cache_building_row(
        payload,
        building=building,
        fallback_bucket=fallback_bucket,
        source_family=source_family,
    )


def select_internal_runtime_building_row(
    payload: Any,
    *,
    building: str,
    fallback_bucket: str = "",
    source_family: str = "",
) -> Dict[str, Any]:
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
        source_family=source_family,
    )


def _complete_internal_page_slots(payload: Any) -> list[Dict[str, Any]]:
    raw_pool = payload if isinstance(payload, dict) else {}
    rows = raw_pool.get("page_slots", []) if isinstance(raw_pool.get("page_slots", []), list) else []
    row_map = {
        str(item.get("building", "") or "").strip(): item
        for item in rows
        if isinstance(item, dict) and str(item.get("building", "") or "").strip()
    }
    return [
        present_internal_page_slot(copy.deepcopy(row_map.get(building, {"building": building})))
        for building in INTERNAL_RUNTIME_BUILDINGS.values()
    ]


def _complete_internal_source_family(
    payload: Any,
    *,
    key: str = "",
    title: str = "",
    fallback_bucket: str = "",
) -> Dict[str, Any]:
    family = payload if isinstance(payload, dict) else {}
    current_bucket = str(family.get("current_bucket", "") or "").strip() or str(fallback_bucket or "").strip()
    completed_rows = [
        select_internal_runtime_building_row(
            family,
            building=building,
            fallback_bucket=current_bucket,
            source_family=key,
        )
        for building in INTERNAL_RUNTIME_BUILDINGS.values()
    ]
    presented = present_source_cache_family(
        {
            **family,
            "current_bucket": current_bucket,
            "buildings": completed_rows,
        },
        key=key,
        title=title,
        fallback_bucket=current_bucket,
        bucket_scope_text="本小时",
    )
    return {
        **presented,
        "buildings": completed_rows,
    }


def build_internal_runtime_summary(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    raw_snapshot = snapshot if isinstance(snapshot, dict) else {}
    raw_pool = raw_snapshot.get("internal_download_pool", {}) if isinstance(raw_snapshot.get("internal_download_pool", {}), dict) else {}
    raw_cache = raw_snapshot.get("internal_source_cache", {}) if isinstance(raw_snapshot.get("internal_source_cache", {}), dict) else {}
    current_bucket = str(raw_cache.get("current_hour_bucket", "") or "").strip()
    pool_payload = {
        "enabled": bool(raw_pool.get("enabled", False)),
        "browser_ready": bool(raw_pool.get("browser_ready", False)),
        "active_buildings": [str(item or "").strip() for item in (raw_pool.get("active_buildings", []) or []) if str(item or "").strip()],
        "last_error": bridge_text(raw_pool.get("last_error", "")),
        "page_slots": _complete_internal_page_slots(raw_pool),
    }
    source_cache_payload = {
        "enabled": bool(raw_cache.get("enabled", False)),
        "scheduler_running": bool(raw_cache.get("scheduler_running", False)),
        "current_hour_bucket": current_bucket,
        "last_run_at": str(raw_cache.get("last_run_at", "") or "").strip(),
        "last_success_at": str(raw_cache.get("last_success_at", "") or "").strip(),
        "last_error": bridge_text(raw_cache.get("last_error", "")),
        "cache_root": str(raw_cache.get("cache_root", "") or "").strip(),
        "current_hour_refresh": copy.deepcopy(raw_cache.get("current_hour_refresh", {}) if isinstance(raw_cache.get("current_hour_refresh", {}), dict) else {}),
        "handover_log_family": _complete_internal_source_family(
            raw_cache.get("handover_log_family", {}),
            key="handover_log_family",
            title="交接班日志源文件",
            fallback_bucket=current_bucket,
        ),
        "handover_capacity_report_family": _complete_internal_source_family(
            raw_cache.get("handover_capacity_report_family", {}),
            key="handover_capacity_report_family",
            title="交接班容量报表源文件",
            fallback_bucket=current_bucket,
        ),
        "monthly_report_family": _complete_internal_source_family(
            raw_cache.get("monthly_report_family", {}),
            key="monthly_report_family",
            title="全景平台月报源文件",
            fallback_bucket=current_bucket,
        ),
        "branch_power_family": _complete_internal_source_family(
            raw_cache.get("branch_power_family", {}),
            key="branch_power_family",
            title="支路功率源文件",
            fallback_bucket=str(raw_cache.get("branch_power_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
        ),
        "branch_current_family": _complete_internal_source_family(
            raw_cache.get("branch_current_family", {}),
            key="branch_current_family",
            title="支路电流源文件",
            fallback_bucket=str(raw_cache.get("branch_current_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
        ),
        "branch_switch_family": _complete_internal_source_family(
            raw_cache.get("branch_switch_family", {}),
            key="branch_switch_family",
            title="支路开关源文件",
            fallback_bucket=str(raw_cache.get("branch_switch_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
        ),
        "chiller_mode_switch_family": _complete_internal_source_family(
            raw_cache.get("chiller_mode_switch_family", {}),
            key="chiller_mode_switch_family",
            title="制冷单元模式切换参数源文件",
            fallback_bucket=str(raw_cache.get("chiller_mode_switch_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
        ),
        "alarm_event_family": _complete_internal_source_family(
            raw_cache.get("alarm_event_family", {}),
            key="alarm_event_family",
            title="告警信息源文件",
            fallback_bucket=str(raw_cache.get("alarm_event_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
        ),
    }
    pool_payload["overview"] = present_internal_download_pool_overview(pool_payload)
    source_cache_payload["overview"] = present_internal_source_cache_overview(source_cache_payload)
    source_cache_payload["current_hour_refresh_overview"] = present_current_hour_refresh_overview(
        source_cache_payload["current_hour_refresh"]
    )
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
        "pool": pool_payload,
        "source_cache": source_cache_payload,
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
        "page_slot": present_internal_page_slot(copy.deepcopy(matched_slot if isinstance(matched_slot, dict) else {"building": building})),
        "source_families": {
            "handover_log_family": select_internal_runtime_building_row(
                raw_cache.get("handover_log_family", {}),
                building=building,
                fallback_bucket=current_bucket,
                source_family="handover_log_family",
            ),
            "handover_capacity_report_family": select_internal_runtime_building_row(
                raw_cache.get("handover_capacity_report_family", {}),
                building=building,
                fallback_bucket=current_bucket,
                source_family="handover_capacity_report_family",
            ),
            "monthly_report_family": select_internal_runtime_building_row(
                raw_cache.get("monthly_report_family", {}),
                building=building,
                fallback_bucket=current_bucket,
                source_family="monthly_report_family",
            ),
            "branch_power_family": select_internal_runtime_building_row(
                raw_cache.get("branch_power_family", {}),
                building=building,
                fallback_bucket=str(raw_cache.get("branch_power_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
                source_family="branch_power_family",
            ),
            "branch_current_family": select_internal_runtime_building_row(
                raw_cache.get("branch_current_family", {}),
                building=building,
                fallback_bucket=str(raw_cache.get("branch_current_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
                source_family="branch_current_family",
            ),
            "branch_switch_family": select_internal_runtime_building_row(
                raw_cache.get("branch_switch_family", {}),
                building=building,
                fallback_bucket=str(raw_cache.get("branch_switch_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
                source_family="branch_switch_family",
            ),
            "chiller_mode_switch_family": select_internal_runtime_building_row(
                raw_cache.get("chiller_mode_switch_family", {}),
                building=building,
                fallback_bucket=str(raw_cache.get("chiller_mode_switch_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
                source_family="chiller_mode_switch_family",
            ),
            "alarm_event_family": select_internal_runtime_building_row(
                raw_cache.get("alarm_event_family", {}),
                building=building,
                fallback_bucket=str(raw_cache.get("alarm_event_family", {}).get("current_bucket", "") or "").strip() or current_bucket,
                source_family="alarm_event_family",
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
