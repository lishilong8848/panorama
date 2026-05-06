from __future__ import annotations

import copy
from typing import Any, Callable, Dict

from fastapi import APIRouter, HTTPException, Query, Request

from app.config.config_adapter import normalize_role_mode
from app.modules.report_pipeline.service.job_panel_presenter import (
    build_job_panel_summary,
    build_bridge_tasks_summary,
    present_bridge_task,
)
from app.modules.report_pipeline.service.shared_bridge_waiting_job_helper import (
    start_waiting_bridge_job,
)
from app.modules.shared_bridge.service.dashboard_display_presenter import (
    present_internal_runtime_display,
    present_internal_runtime_building_display,
)
from app.modules.shared_bridge.service.internal_runtime_status_presenter import (
    build_empty_internal_runtime_building_status,
    build_empty_internal_runtime_summary,
    build_internal_runtime_building_status as presenter_build_internal_runtime_building_status,
    build_internal_runtime_summary as presenter_build_internal_runtime_summary,
)


router = APIRouter(tags=["shared-bridge"])

_BRIDGE_FEATURE_LABELS = {
    "handover_from_download": "交接班使用共享文件生成",
    "day_metric_from_download": "12项使用共享文件上传",
    "wet_bulb_collection": "湿球温度采集",
    "monthly_report_pipeline": "月报主流程",
    "alarm_event_upload": "告警信息上传",
    "handover_cache_fill": "交接班历史共享文件补采",
    "monthly_cache_fill": "月报历史共享文件补采",
    "internal_browser_alert": "内网环境告警",
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

_LATEST_REFRESHABLE_SOURCE_FAMILIES = {
    "handover_log_family": "交接班日志源文件",
    "handover_capacity_report_family": "交接班容量报表源文件",
    "monthly_report_family": "全景平台月报源文件",
    "branch_power_family": "支路功率源文件",
    "alarm_event_family": "告警信息源文件",
}

_INTERNAL_RUNTIME_BUILDINGS = {
    "a": "A楼",
    "b": "B楼",
    "c": "C楼",
    "d": "D楼",
    "e": "E楼",
}


def _internal_summary_has_runtime_signal(payload: Any) -> bool:
    summary = payload if isinstance(payload, dict) else {}
    pool = summary.get("pool", {}) if isinstance(summary.get("pool", {}), dict) else {}
    cache = summary.get("source_cache", {}) if isinstance(summary.get("source_cache", {}), dict) else {}
    slots = pool.get("page_slots", []) if isinstance(pool.get("page_slots", []), list) else []
    for slot in slots:
        if not isinstance(slot, dict):
            continue
        if bool(slot.get("page_ready", False)) or str(slot.get("login_state", "") or "").strip().lower() in {
            "ready",
            "logging_in",
            "failed",
            "expired",
        }:
            return True
    for key in ("handover_log_family", "handover_capacity_report_family", "monthly_report_family", "branch_power_family", "alarm_event_family"):
        family = cache.get(key, {}) if isinstance(cache.get(key, {}), dict) else {}
        rows = family.get("buildings", []) if isinstance(family.get("buildings", []), list) else []
        if any(
            isinstance(row, dict)
            and (
                str(row.get("status", "") or row.get("status_key", "") or "").strip().lower() not in {"", "waiting"}
                or bool(row.get("ready", False))
                or bool(row.get("blocked", False))
            )
            for row in rows
        ):
            return True
    return False


def _internal_building_status_has_runtime_signal(payload: Any) -> bool:
    status = payload if isinstance(payload, dict) else {}
    slot = status.get("page_slot", {}) if isinstance(status.get("page_slot", {}), dict) else {}
    if bool(slot.get("page_ready", False)) or str(slot.get("login_state", "") or "").strip().lower() in {
        "ready",
        "logging_in",
        "failed",
        "expired",
    }:
        return True
    families = status.get("source_families", {}) if isinstance(status.get("source_families", {}), dict) else {}
    for row in families.values():
        if isinstance(row, dict) and (
            str(row.get("status", "") or row.get("status_key", "") or "").strip().lower() not in {"", "waiting"}
            or bool(row.get("ready", False))
            or bool(row.get("blocked", False))
        ):
            return True
    return False


def _wrap_internal_light_snapshot(snapshot: Any) -> Dict[str, Any]:
    raw = snapshot if isinstance(snapshot, dict) else {}
    if isinstance(raw.get("internal_source_cache"), dict):
        return raw
    family_keys = (
        "handover_log_family",
        "handover_capacity_report_family",
        "monthly_report_family",
        "branch_power_family",
        "alarm_event_family",
    )
    if not any(isinstance(raw.get(key), dict) for key in family_keys) and "current_hour_bucket" not in raw:
        return raw
    return {
        "enabled": bool(raw.get("enabled", False)),
        "role_mode": "internal",
        "agent_status": "running",
        "db_status": "ok",
        "last_error": str(raw.get("last_error", "") or "").strip(),
        "last_poll_at": str(raw.get("last_run_at", "") or "").strip(),
        "internal_source_cache": copy.deepcopy(raw),
        "internal_download_pool": {},
    }


def _read_live_internal_summary(service: Any) -> Dict[str, Any] | None:
    if service is None or not hasattr(service, "get_health_snapshot"):
        return None
    try:
        live_snapshot = service.get_health_snapshot(mode="internal_light")
    except Exception:
        return None
    return presenter_build_internal_runtime_summary(
        _wrap_internal_light_snapshot(live_snapshot),
    )


def _read_live_internal_building_status(service: Any, *, building: str, building_code: str) -> Dict[str, Any] | None:
    if service is None or not hasattr(service, "get_health_snapshot"):
        return None
    try:
        live_snapshot = service.get_health_snapshot(mode="internal_light")
    except Exception:
        return None
    return presenter_build_internal_runtime_building_status(
        _wrap_internal_light_snapshot(live_snapshot),
        building=building,
        building_code=building_code,
    )



def _deployment_role_mode(request: Request) -> str:
    container = request.app.state.container
    snapshot = container.deployment_snapshot() if hasattr(container, "deployment_snapshot") else {}
    if not isinstance(snapshot, dict):
        return ""
    return normalize_role_mode(snapshot.get("role_mode"))


def _ensure_internal_bridge_runtime_allowed(request: Request) -> None:
    if _deployment_role_mode(request) != "internal":
        raise HTTPException(status_code=409, detail="当前仅内网端提供实时浏览器与下载状态接口。")


def _normalize_internal_runtime_building(building_code: str) -> tuple[str, str]:
    raw = str(building_code or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="缺少楼栋参数")
    lowered = raw.lower()
    if lowered in _INTERNAL_RUNTIME_BUILDINGS:
        return lowered, _INTERNAL_RUNTIME_BUILDINGS[lowered]
    normalized = raw.upper()
    if normalized in _INTERNAL_RUNTIME_BUILDINGS.values():
        code = next((key for key, value in _INTERNAL_RUNTIME_BUILDINGS.items() if value == normalized), "")
        if code:
            return code, normalized
    if len(lowered) == 1 and lowered in _INTERNAL_RUNTIME_BUILDINGS:
        return lowered, _INTERNAL_RUNTIME_BUILDINGS[lowered]
    raise HTTPException(status_code=404, detail="楼栋不存在，仅支持 A楼 到 E楼")


def _ensure_bridge_write_allowed(request: Request) -> None:
    if _deployment_role_mode(request) not in {"internal", "external"}:
        raise HTTPException(
            status_code=409,
            detail="当前未进入有效端，无法操作共享任务。",
        )


def _list_visible_bridge_tasks(service, *, limit: int = 100) -> list[Dict[str, Any]]:
    try:
        tasks = service.list_tasks(limit=limit) if service else []
    except Exception as exc:  # noqa: BLE001
        if not service or not _bridge_store_read_is_recoverable(service, exc):
            raise
        cache_reader = getattr(service, "get_cached_tasks", None)
        tasks = cache_reader(limit=limit) if callable(cache_reader) else []
    rows = tasks if isinstance(tasks, list) else []
    return [task for task in rows if str(task.get("feature", "") or "").strip().lower() != "alarm_export"]


def _get_visible_bridge_task(service, task_id: str) -> Dict[str, Any] | None:
    try:
        payload = service.get_task(task_id) if service else None
    except Exception as exc:  # noqa: BLE001
        if not service or not _bridge_store_read_is_recoverable(service, exc):
            raise
        cache_reader = getattr(service, "get_cached_task", None)
        payload = cache_reader(task_id) if callable(cache_reader) else None
    if not payload or str(payload.get("feature", "") or "").strip().lower() == "alarm_export":
        return None
    return payload


def _build_bridge_tasks_summary_payload(service, *, limit: int = 60) -> Dict[str, Any]:
    visible_tasks = _list_visible_bridge_tasks(service, limit=limit)
    return build_bridge_tasks_summary(
        [_bridge_present_task(task) for task in visible_tasks],
        count=len(visible_tasks),
    )


def _start_local_background_job(
    container,
    *,
    name: str,
    feature: str,
    dedupe_key: str,
    resource_keys: list[str] | tuple[str, ...],
    run_func: Callable[[Callable[[str], None]], Any],
) -> Dict[str, Any]:
    job_service = container.job_service
    normalized_dedupe_key = str(dedupe_key or "").strip()
    if normalized_dedupe_key:
        existing = job_service.find_active_job_by_dedupe_key(normalized_dedupe_key)
        if isinstance(existing, dict):
            return {"job": existing, "reused": True}
    job = job_service.start_job(
        name=name,
        run_func=run_func,
        resource_keys=list(resource_keys),
        priority="manual",
        feature=feature,
        dedupe_key=normalized_dedupe_key,
        submitted_by="manual",
    )
    return {"job": job.to_dict(), "reused": False}


def _accepted_waiting_job_response(job, task: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = {
        "ok": True,
        "accepted": True,
        "job": job.to_dict() if hasattr(job, "to_dict") else dict(job or {}),
    }
    if isinstance(task, dict) and task:
        payload["bridge_task"] = task
    return payload


def _run_external_alarm_upload_shared_flow(
    *,
    container,
    service,
    mode: str,
    building: str,
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    normalized_mode = str(mode or "").strip().lower() or "full"
    target_buildings = [building] if normalized_mode == "single_building" and building else service.get_source_cache_buildings()
    target_buildings = [item for item in target_buildings if item]
    emit_log(
        "[告警信息上传] 已进入后台共享文件处理: "
        f"mode={normalized_mode}, building={building or '-'}"
    )
    selection = service.get_alarm_event_upload_selection(
        building=building if normalized_mode == "single_building" else "",
    )
    target_bucket_key = ""
    current_alarm_bucket = getattr(service, "current_alarm_event_bucket", None)
    if callable(current_alarm_bucket):
        try:
            target_bucket_key = str(current_alarm_bucket() or "").strip()
        except Exception:
            target_bucket_key = ""
    if not target_bucket_key:
        target_bucket_key = str(
            selection.get("target_bucket_key", "")
            or selection.get("current_bucket", "")
            or selection.get("selection_reference_date", "")
            or ""
        ).strip()
    selected_entries = [
        item
        for item in (selection.get("selected_entries", []) if isinstance(selection.get("selected_entries", []), list) else [])
        if isinstance(item, dict)
    ]
    ready_buildings = {
        str(item.get("building", "") or "").strip()
        for item in selected_entries
        if str(item.get("building", "") or "").strip()
    }
    missing_buildings = [item for item in target_buildings if item and item not in ready_buildings]
    if missing_buildings:
        waiting_job, waiting_task = start_waiting_bridge_job(
            job_service=container.job_service,
            bridge_service=service,
            name="使用共享文件上传60天-全部楼栋" if normalized_mode == "full" else f"使用共享文件上传60天-{building}",
            worker_handler="alarm_event_upload",
            worker_payload={
                "resume_kind": "shared_bridge_alarm_event_upload",
                "mode": normalized_mode,
                "building": building or None,
            },
            resource_keys=["alarm_upload:global"],
            priority="manual",
            feature="alarm_event_upload",
            dedupe_key=":".join(
                [
                    "alarm_event_upload_wait_shared_bridge",
                    normalized_mode,
                    building or "all",
                    target_bucket_key or "-",
                ]
            ),
            submitted_by="manual",
            bridge_get_or_create_name="get_or_create_alarm_event_upload_task",
            bridge_create_name="create_alarm_event_upload_task",
            bridge_kwargs={
                "mode": normalized_mode,
                "building": building or None,
                "target_bucket_key": target_bucket_key or None,
            },
        )
        emit_log(
            "[共享桥接] 已受理告警上传共享桥接任务 "
            f"task_id={str(waiting_task.get('task_id', '') or '-').strip() or '-'}, "
            f"mode={normalized_mode}, missing={','.join(missing_buildings)}"
        )
        return {
            "ok": True,
            "mode": "waiting_shared_bridge",
            "missing_buildings": list(missing_buildings),
            "waiting": _accepted_waiting_job_response(waiting_job, waiting_task),
        }
    if normalized_mode == "single_building":
        return service.upload_alarm_event_source_cache_single_building_to_bitable(
            building=building,
            emit_log=emit_log,
        )
    return service.upload_alarm_event_source_cache_full_to_bitable(emit_log=emit_log)


def _bridge_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if "permissionerror" in lowered or "winerror 5" in lowered:
        return "共享桥接目录无写入权限"
    if "no such table" in lowered:
        return "共享桥接数据库结构未初始化"
    return _BRIDGE_ERROR_TEXTS.get(lowered, _BRIDGE_EVENT_TYPE_LABELS.get(lowered, text))


def _bridge_store_read_is_recoverable(service: Any, exc: Exception) -> bool:
    checker = getattr(service, "_is_recoverable_store_error", None)
    return bool(callable(checker) and checker(exc))


def _raise_bridge_store_http_error(exc: Exception) -> None:
    raise HTTPException(status_code=503, detail=_bridge_text(exc) or "共享桥接数据库暂时不可用，请稍后重试") from exc


def _bridge_feature_label(feature: Any) -> str:
    normalized = str(feature or "").strip().lower()
    return _BRIDGE_FEATURE_LABELS.get(normalized, str(feature or "").strip() or "-")


def _bridge_role_label(role: Any) -> str:
    normalized = normalize_role_mode(role)
    if normalized == "internal":
        return "内网端"
    if normalized == "external":
        return "外网端"
    return str(role or "").strip() or "-"


def _bridge_stage_label(*, feature: Any, mode: Any, stage_id: Any, handler: Any = "") -> str:
    feature_text = str(feature or "").strip().lower()
    mode_text = str(mode or "").strip().lower()
    stage_text = str(stage_id or "").strip().lower()
    handler_text = str(handler or "").strip().lower()
    key = stage_text or handler_text
    if key == "internal_download":
        if feature_text == "handover_from_download":
            return "准备交接班共享文件"
        if feature_text == "day_metric_from_download":
            return "准备12项共享文件"
        if feature_text == "wet_bulb_collection":
            return "准备湿球共享文件"
        if feature_text == "monthly_report_pipeline":
            if mode_text == "multi_date":
                return "准备月报历史共享文件"
            return "准备月报共享文件"
        return "准备共享文件"
    if key == "internal_fill":
        if feature_text == "alarm_event_upload":
            return "补采告警共享文件"
        if feature_text == "monthly_cache_fill":
            return "补采月报历史共享文件"
        if feature_text == "handover_cache_fill" and mode_text == "day_metric":
            return "补采12项历史共享文件"
        if feature_text == "handover_cache_fill":
            return "补采交接班历史共享文件"
        return "补采历史共享文件"
    if key == "external_generate_review_output":
        return "使用共享文件生成交接班"
    if key == "external_upload":
        if feature_text == "day_metric_from_download":
            return "使用共享文件上传12项"
        if feature_text == "alarm_event_upload":
            return "使用共享文件上传告警信息"
        return "外网继续上传"
    if key == "external_extract_and_upload":
        return "使用共享文件上传湿球温度"
    if key == "external_resume":
        if feature_text == "monthly_report_pipeline" and mode_text == "resume_upload":
            return "外网断点续传月报"
        if feature_text == "monthly_report_pipeline":
            return "使用共享文件上传月报"
        return "外网继续处理"
    if key == "external_notify":
        if feature_text == "internal_browser_alert":
            return "外网发送告警"
        return "外网通知"
    if key == "external_continue":
        if feature_text == "monthly_cache_fill":
            return "使用共享文件继续处理月报"
        if feature_text == "handover_cache_fill" and mode_text == "day_metric":
            return "使用共享文件上传12项"
        if feature_text == "handover_cache_fill":
            return "使用共享文件生成交接班"
        return "外网继续处理"
    return str(stage_id or handler or "").strip() or "-"


def _bridge_artifact_label(kind: Any) -> str:
    normalized = str(kind or "").strip().lower()
    if normalized == "source_file":
        return "源文件"
    if normalized == "prepared_rows":
        return "预处理结果"
    if normalized == "output_file":
        return "输出文件"
    if normalized == "daily_report_asset":
        return "日报截图资产"
    if normalized == "resume_state":
        return "续传状态"
    if normalized == "manifest":
        return "清单"
    return str(kind or "").strip() or "-"


def _bridge_event_text(event: Dict[str, Any]) -> str:
    payload = event.get("payload", {}) if isinstance(event.get("payload", {}), dict) else {}
    return (
        _bridge_text(payload.get("message", ""))
        or _bridge_text(payload.get("error", ""))
        or _bridge_text(event.get("event_type", ""))
        or "-"
    )


def _bridge_infer_current_stage(task: Dict[str, Any]) -> Dict[str, Any]:
    stages = task.get("stages", []) if isinstance(task.get("stages", []), list) else []
    if stages:
        for candidate in stages:
            status = str(candidate.get("status", "") or "").strip().lower()
            if status in {"running", "claimed"}:
                return candidate
        for candidate in stages:
            status = str(candidate.get("status", "") or "").strip().lower()
            if status in {"pending", "waiting_next_side"}:
                return candidate
        for candidate in stages:
            if str(candidate.get("error", "") or "").strip():
                return candidate
        return stages[-1]
    feature = task.get("feature", "")
    mode = task.get("mode", "")
    status = str(task.get("status", "") or "").strip().lower()
    if status in {"queued_for_internal", "internal_claimed", "internal_running"}:
        stage_id = "internal_fill" if str(feature or "").strip().lower() in {"handover_cache_fill", "monthly_cache_fill"} else "internal_download"
        role_target = "internal"
    elif status in {"ready_for_external", "external_claimed", "external_running"}:
        feature_text = str(feature or "").strip().lower()
        if feature_text == "handover_from_download":
            stage_id = "external_generate_review_output"
        elif feature_text == "wet_bulb_collection":
            stage_id = "external_extract_and_upload"
        elif feature_text in {"handover_cache_fill", "monthly_cache_fill"}:
            stage_id = "external_continue"
        else:
            stage_id = "external_resume" if feature_text == "monthly_report_pipeline" else "external_upload"
        role_target = "external"
    else:
        stage_id = ""
        role_target = ""
    if not stage_id:
        return {}
    return {
        "stage_id": stage_id,
        "handler": "",
        "status": status,
        "role_target": role_target,
        "error": _bridge_text(task.get("error", "")),
        "stage_name": _bridge_stage_label(feature=feature, mode=mode, stage_id=stage_id),
    }


def _bridge_present_task(task: Dict[str, Any]) -> Dict[str, Any]:
    payload = copy.deepcopy(task if isinstance(task, dict) else {})
    feature = payload.get("feature", "")
    mode = payload.get("mode", "")
    payload["feature_label"] = _bridge_feature_label(feature)
    stages = payload.get("stages", []) if isinstance(payload.get("stages", []), list) else []
    payload["stages"] = [
        {
            **stage,
            "stage_name": _bridge_stage_label(
                feature=feature,
                mode=mode,
                stage_id=stage.get("stage_id", ""),
                handler=stage.get("handler", ""),
            ),
            "role_label": _bridge_role_label(stage.get("role_target", "")),
            "error_text": _bridge_text(stage.get("error", "")),
        }
        for stage in stages
        if isinstance(stage, dict)
    ]
    artifacts = payload.get("artifacts", []) if isinstance(payload.get("artifacts", []), list) else []
    payload["artifacts"] = [
        {
            **artifact,
            "artifact_kind_label": _bridge_artifact_label(artifact.get("artifact_kind", "")),
        }
        for artifact in artifacts
        if isinstance(artifact, dict)
    ]
    events = payload.get("events", []) if isinstance(payload.get("events", []), list) else []
    payload["events"] = [
        {
            **event,
            "event_text": _bridge_event_text(event),
            "side_label": _bridge_role_label(event.get("side", "")),
        }
        for event in events
        if isinstance(event, dict)
    ]
    current_stage = _bridge_infer_current_stage(payload)
    payload["current_stage"] = current_stage
    payload["current_stage_name"] = str(current_stage.get("stage_name", "") or "").strip()
    payload["current_stage_status"] = str(current_stage.get("status", "") or "").strip()
    payload["current_stage_role"] = str(current_stage.get("role_target", "") or "").strip()
    payload["current_stage_error"] = _bridge_text(current_stage.get("error", ""))
    payload["display_error"] = (
        _bridge_text(payload.get("error", ""))
        or _bridge_text(payload.get("current_stage_error", ""))
        or next(
            (
                str(event.get("event_text", "") or "").strip()
                for event in payload.get("events", [])
                if str(event.get("level", "") or "").strip().lower() == "error"
                and str(event.get("event_text", "") or "").strip()
            ),
            "",
        )
    )
    return payload


@router.get("/api/bridge/health")
def bridge_health(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return {
        "ok": True,
        "deployment": container.deployment_snapshot(),
        "shared_bridge": container.shared_bridge_snapshot(),
    }


@router.get("/api/bridge/internal-runtime-status")
def bridge_internal_runtime_status(request: Request) -> Dict[str, Any]:
    role_mode = _deployment_role_mode(request)
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    coordinator = getattr(container, "runtime_status_coordinator", None)

    def _attach_display(summary_payload: Dict[str, Any]) -> Dict[str, Any]:
        summary = copy.deepcopy(summary_payload if isinstance(summary_payload, dict) else {})
        task_summary: Dict[str, Any] = {}
        if coordinator is not None and callable(getattr(coordinator, "read_scope_snapshot", None)):
            try:
                job_snapshot = coordinator.read_scope_snapshot("job_panel_dashboard_summary")
                payload = job_snapshot.get("payload") if isinstance(job_snapshot, dict) else None
                if isinstance(payload, dict):
                    task_summary = payload
            except Exception:
                task_summary = {}
        summary["display"] = present_internal_runtime_display(
            summary,
            task_overview=(
                task_summary.get("display", {}).get("overview", {})
                if isinstance(task_summary.get("display", {}), dict)
                else {}
            ),
        )
        return summary

    if role_mode != "internal":
        summary = build_empty_internal_runtime_summary(role_mode=role_mode or "external")
        summary["runtime_available"] = False
        summary["reason_code"] = "role_mismatch"
        return {"ok": True, "summary": _attach_display(summary)}

    if coordinator is not None and callable(getattr(coordinator, "is_running", None)) and coordinator.is_running():
        live_payload = _read_live_internal_summary(service)
        if isinstance(live_payload, dict) and _internal_summary_has_runtime_signal(live_payload):
            try:
                refresher = getattr(coordinator, "request_internal_runtime_refresh", None)
                if callable(refresher):
                    refresher(reason="internal_runtime_summary_route_live")
            except Exception:
                pass
            return {"ok": True, "summary": _attach_display(live_payload)}
        snapshot = coordinator.read_scope_snapshot("internal_runtime_summary")
        payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
        if isinstance(payload, dict):
            if not _internal_summary_has_runtime_signal(payload):
                if isinstance(live_payload, dict) and _internal_summary_has_runtime_signal(live_payload):
                    try:
                        refresher = getattr(coordinator, "request_internal_runtime_refresh", None)
                        if callable(refresher):
                            refresher(reason="internal_runtime_summary_route_live_repair")
                    except Exception:
                        pass
                    return {"ok": True, "summary": _attach_display(live_payload)}
            return {"ok": True, "summary": _attach_display(payload)}
        try:
            refresher = getattr(coordinator, "request_internal_runtime_refresh", None)
            if callable(refresher):
                refresher(reason="internal_runtime_summary_route")
            else:
                coordinator.request_refresh(reason="internal_runtime_summary_route")
        except Exception:
            pass
        return {"ok": True, "summary": _attach_display(build_empty_internal_runtime_summary(role_mode="internal"))}

    live_payload = _read_live_internal_summary(service)
    if isinstance(live_payload, dict):
        try:
            if coordinator is not None and callable(getattr(coordinator, "request_internal_runtime_refresh", None)):
                coordinator.request_internal_runtime_refresh(reason="internal_runtime_summary_route")
        except Exception:
            pass
        return {"ok": True, "summary": _attach_display(live_payload)}

    if coordinator is not None and callable(getattr(coordinator, "is_running", None)) and coordinator.is_running():
        return {"ok": True, "summary": _attach_display(build_empty_internal_runtime_summary(role_mode="internal"))}

    return {"ok": True, "summary": _attach_display(build_empty_internal_runtime_summary(role_mode="internal"))}


@router.get("/api/bridge/internal-runtime-status/buildings/{building_code}")
def bridge_internal_runtime_status_building(
    building_code: str,
    request: Request,
    include_raw: bool = False,
) -> Dict[str, Any]:
    role_mode = _deployment_role_mode(request)
    normalized_code, building = _normalize_internal_runtime_building(building_code)
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    coordinator = getattr(container, "runtime_status_coordinator", None)

    def _slim_action(payload: Any, *, label: str = "") -> Dict[str, Any]:
        action = payload if isinstance(payload, dict) else {}
        return {
            "allowed": bool(action.get("allowed", False)),
            "pending": bool(action.get("pending", False)),
            "label": str(action.get("label", "") or label).strip(),
            "disabled_reason": str(action.get("disabled_reason", "") or "").strip(),
        }

    def _slim_page_slot(payload: Any) -> Dict[str, Any]:
        slot = payload if isinstance(payload, dict) else {}
        keys = (
            "building",
            "browser_ready",
            "page_ready",
            "login_state",
            "last_login_at",
            "login_error",
            "in_use",
            "last_used_at",
            "last_result",
            "last_error",
            "suspended",
            "suspend_reason",
            "next_probe_at",
            "status_key",
            "tone",
            "status_text",
            "detail_text",
            "login_tone",
            "login_text",
        )
        result = {key: slot.get(key) for key in keys if key in slot}
        result["building"] = str(result.get("building") or building).strip()
        return result

    def _slim_source_family(key: str, payload: Any) -> Dict[str, Any]:
        row = payload if isinstance(payload, dict) else {}
        keys = (
            "building",
            "bucket_key",
            "status",
            "status_key",
            "ready",
            "downloaded_at",
            "last_error",
            "started_at",
            "blocked",
            "blocked_reason",
            "next_probe_at",
            "source_family",
            "tone",
            "status_text",
            "detail_text",
            "reason_code",
            "key",
            "title",
        )
        result = {field: row.get(field) for field in keys if field in row}
        result["building"] = str(result.get("building") or building).strip()
        result["key"] = str(result.get("key") or key).strip()
        result["source_family"] = str(result.get("source_family") or key).strip()
        result["actions"] = {
            "refresh": _slim_action(
                (row.get("actions") or {}).get("refresh") if isinstance(row.get("actions"), dict) else {},
                label="重新拉取",
            )
        }
        return result

    def _slim_display(display_payload: Any) -> Dict[str, Any]:
        display = display_payload if isinstance(display_payload, dict) else {}
        source_families = display.get("source_families", {}) if isinstance(display.get("source_families", {}), dict) else {}
        slim_family_map = {
            str(key): _slim_source_family(str(key), value)
            for key, value in source_families.items()
            if isinstance(value, dict)
        }
        family_order = display.get("families", []) if isinstance(display.get("families", []), list) else []
        slim_family_items = []
        seen_keys = set()
        for item in family_order:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or item.get("source_family") or "").strip()
            if not key:
                continue
            slim_item = slim_family_map.get(key) or _slim_source_family(key, item)
            slim_family_items.append(slim_item)
            seen_keys.add(key)
        for key, item in slim_family_map.items():
            if key not in seen_keys:
                slim_family_items.append(item)
        return {
            "building": str(display.get("building") or building).strip(),
            "tone": str(display.get("tone", "") or "neutral").strip(),
            "status_text": str(display.get("status_text", "") or "").strip(),
            "summary_text": str(display.get("summary_text", "") or "").strip(),
            "reason_code": str(display.get("reason_code", "") or "").strip(),
            "page_slot": _slim_page_slot(display.get("page_slot", {})),
            "source_families": slim_family_map,
            "families": slim_family_items,
            "items": display.get("items", []) if isinstance(display.get("items", []), list) else [],
        }

    def _attach_display(status_payload: Dict[str, Any]) -> Dict[str, Any]:
        status = copy.deepcopy(status_payload if isinstance(status_payload, dict) else {})
        display = present_internal_runtime_building_display(status)
        status["display"] = display
        if include_raw:
            return status
        return {
            "updated_at": str(status.get("updated_at", "") or "").strip(),
            "building": str(status.get("building", "") or building).strip(),
            "building_code": str(status.get("building_code", "") or normalized_code).strip(),
            "runtime_available": bool(status.get("runtime_available", True)),
            "reason_code": str(status.get("reason_code", "") or "").strip(),
            "display": _slim_display(display),
        }

    if role_mode != "internal":
        status = build_empty_internal_runtime_building_status(
            building=building,
            building_code=normalized_code,
        )
        status["runtime_available"] = False
        status["reason_code"] = "role_mismatch"
        return {"ok": True, "status": _attach_display(status)}

    if coordinator is not None and callable(getattr(coordinator, "is_running", None)) and coordinator.is_running():
        live_payload = _read_live_internal_building_status(service, building=building, building_code=normalized_code)
        if isinstance(live_payload, dict) and _internal_building_status_has_runtime_signal(live_payload):
            try:
                refresher = getattr(coordinator, "request_internal_runtime_refresh", None)
                if callable(refresher):
                    refresher(reason=f"internal_runtime_building_route_live:{building}")
            except Exception:
                pass
            return {"ok": True, "status": _attach_display(live_payload)}
        snapshot = coordinator.read_building_snapshot(building)
        payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
        if isinstance(payload, dict):
            if not _internal_building_status_has_runtime_signal(payload):
                if isinstance(live_payload, dict) and _internal_building_status_has_runtime_signal(live_payload):
                    try:
                        refresher = getattr(coordinator, "request_internal_runtime_refresh", None)
                        if callable(refresher):
                            refresher(reason=f"internal_runtime_building_route_live_repair:{building}")
                    except Exception:
                        pass
                    return {"ok": True, "status": _attach_display(live_payload)}
            return {"ok": True, "status": _attach_display(payload)}
        try:
            refresher = getattr(coordinator, "request_internal_runtime_refresh", None)
            if callable(refresher):
                refresher(reason=f"internal_runtime_building_route:{building}")
            else:
                coordinator.request_refresh(reason=f"internal_runtime_building_route:{building}")
        except Exception:
            pass
        return {
            "ok": True,
            "status": _attach_display(
                build_empty_internal_runtime_building_status(
                    building=building,
                    building_code=normalized_code,
                )
            ),
        }

    live_payload = _read_live_internal_building_status(service, building=building, building_code=normalized_code)
    if isinstance(live_payload, dict):
        try:
            if coordinator is not None and callable(getattr(coordinator, "request_internal_runtime_refresh", None)):
                coordinator.request_internal_runtime_refresh(reason=f"internal_runtime_building_route:{building}")
        except Exception:
            pass
        return {"ok": True, "status": _attach_display(live_payload)}

    if coordinator is not None and callable(getattr(coordinator, "is_running", None)) and coordinator.is_running():
        return {
            "ok": True,
            "status": _attach_display(
                build_empty_internal_runtime_building_status(
                    building=building,
                    building_code=normalized_code,
                )
            ),
        }

    return {
        "ok": True,
        "status": _attach_display(
            build_empty_internal_runtime_building_status(
                building=building,
                building_code=normalized_code,
            )
        ),
    }


@router.post("/api/bridge/shared-root/self-check")
def bridge_shared_root_self_check(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    if service is None or not hasattr(service, "diagnose_shared_root"):
        raise HTTPException(status_code=409, detail="共享桥接服务未初始化")
    payload = service.diagnose_shared_root(initialize=True)
    try:
        container.add_system_log(
            "[共享桥接] 已执行共享目录自检: "
            f"角色={str(payload.get('role_label', '') or '-').strip() or '-'}, "
            f"共享目录={str(payload.get('root_dir', '') or '-').strip() or '-'}, "
            f"结果={str(payload.get('status_text', '') or '-').strip() or '-'}"
        )
    except Exception:
        pass
    return {"ok": True, **payload}


@router.post("/api/bridge/source-cache/refresh-current-hour")
def bridge_source_cache_refresh_current_hour(request: Request) -> Dict[str, Any]:
    if _deployment_role_mode(request) != "internal":
        raise HTTPException(status_code=409, detail="当前仅内网端允许手动触发当前小时下载")
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    if service is None:
        raise HTTPException(status_code=409, detail="共享桥接服务未初始化")
    result = service.start_current_hour_source_cache_refresh()
    if not bool(result.get("accepted")) and str(result.get("reason", "")).strip() == "already_running":
        return {
            "ok": True,
            "accepted": False,
            "running": True,
            "scope": "current_hour",
            "message": "当前小时下载已在执行中",
            **result,
        }
    if not bool(result.get("accepted")):
        raise HTTPException(status_code=409, detail="当前未启用内网共享缓存下载")
    container.add_system_log("[共享缓存] 已手动触发当前小时全部文件下载")
    return {
        "ok": True,
        "accepted": True,
        "running": True,
        "scope": "current_hour",
        "message": "已开始下载当前小时全部文件",
        **result,
    }


@router.post("/api/bridge/source-cache/refresh-building-latest")
def bridge_source_cache_refresh_building_latest(
    request: Request,
    source_family: str = Query("", description="共享文件 family"),
    building: str = Query("", description="楼栋名称"),
) -> Dict[str, Any]:
    if _deployment_role_mode(request) != "internal":
        raise HTTPException(status_code=409, detail="当前仅内网端允许手动触发单楼共享文件拉取")
    normalized_family = str(source_family or "").strip()
    building_text = str(building or "").strip()
    if normalized_family not in _LATEST_REFRESHABLE_SOURCE_FAMILIES:
        raise HTTPException(status_code=400, detail="不支持的共享文件类型")
    if not building_text:
        raise HTTPException(status_code=400, detail="缺少楼栋参数")
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    if service is None:
        raise HTTPException(status_code=409, detail="共享桥接服务未初始化")
    result = service.start_building_latest_source_cache_refresh(
        source_family=normalized_family,
        building=building_text,
    )
    reason = str(result.get("reason", "") or "").strip()
    family_label = _LATEST_REFRESHABLE_SOURCE_FAMILIES.get(normalized_family, normalized_family)
    if not bool(result.get("accepted")) and reason == "already_running":
        return {
            "ok": True,
            "accepted": False,
            "running": True,
            "scope": "single_building_family",
            "source_family": normalized_family,
            "building": building_text,
            "bucket_key": str(result.get("bucket_key", "") or "").strip(),
            "message": f"{building_text} {family_label}已在拉取中",
            **result,
        }
    if not bool(result.get("accepted")):
        detail_map = {
            "disabled": "当前未启用内网共享缓存下载",
            "invalid_building": "楼栋参数无效或当前楼栋未启用",
            "bucket_unavailable": "当前小时告警桶不可用，请稍后重试或使用一键拉取告警文件",
            "unsupported_family": "不支持的共享文件类型",
        }
        raise HTTPException(status_code=409, detail=detail_map.get(reason, "单楼共享文件拉取未被接受"))
    container.add_system_log(
        f"[共享缓存] 已手动触发单楼 latest 拉取 family={normalized_family} building={building_text}"
    )
    return {
        "ok": True,
        "accepted": True,
        "running": True,
        "scope": "single_building_family",
        "source_family": normalized_family,
        "building": building_text,
        "bucket_key": str(result.get("bucket_key", "") or "").strip(),
        "message": f"已开始重新拉取 {building_text} {family_label}",
        **result,
    }


@router.post("/api/bridge/source-cache/refresh-alarm-manual")
def bridge_source_cache_refresh_alarm_manual(request: Request) -> Dict[str, Any]:
    if _deployment_role_mode(request) != "internal":
        raise HTTPException(status_code=409, detail="当前仅内网端允许手动拉取告警信息文件")
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    if service is None:
        raise HTTPException(status_code=409, detail="共享桥接服务未初始化")
    result = service.start_manual_alarm_source_cache_refresh()
    if not bool(result.get("accepted")) and str(result.get("reason", "")).strip() == "already_running":
        return {
            "ok": True,
            "accepted": False,
            "running": True,
            "scope": "alarm_manual",
            "message": "手动拉取告警信息文件已在执行中",
            **result,
        }
    if not bool(result.get("accepted")):
        raise HTTPException(status_code=409, detail="当前未启用内网告警信息文件拉取")
    container.add_system_log("[共享缓存] 已手动触发告警信息文件拉取")
    return {
        "ok": True,
        "accepted": True,
        "running": True,
        "scope": "alarm_manual",
        "message": "已开始手动拉取告警信息文件",
        **result,
    }


@router.post("/api/bridge/source-cache/delete-manual-alarm-files")
def bridge_source_cache_delete_manual_alarm_files(request: Request) -> Dict[str, Any]:
    if _deployment_role_mode(request) != "internal":
        raise HTTPException(status_code=409, detail="当前仅内网端允许删除手动拉取的告警信息文件")
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    if service is None:
        raise HTTPException(status_code=409, detail="共享桥接服务未初始化")
    result = service.delete_manual_alarm_source_cache_files()
    if not bool(result.get("accepted")):
        raise HTTPException(status_code=409, detail="当前未启用内网告警信息文件管理")
    deleted_count = int(result.get("deleted_count", 0) or 0)
    container.add_system_log(f"[共享缓存] 已删除手动拉取的告警信息文件 {deleted_count} 份")
    return {
        "ok": True,
        "accepted": True,
        "deleted_count": deleted_count,
        "message": "已删除手动拉取的告警信息文件",
        **result,
    }


@router.post("/api/bridge/source-cache/alarm/upload-full")
def bridge_source_cache_alarm_upload_full(request: Request) -> Dict[str, Any]:
    if _deployment_role_mode(request) != "external":
        raise HTTPException(status_code=409, detail="当前仅外网端允许上传告警信息文件到多维表")
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    if service is None:
        raise HTTPException(status_code=409, detail="共享桥接服务未初始化")
    dedupe_key = "alarm_event_upload:full"

    def _run(emit_log) -> Dict[str, Any]:
        result = _run_external_alarm_upload_shared_flow(
            container=container,
            service=service,
            mode="full",
            building="",
            emit_log=emit_log,
        )
        if str(result.get("mode", "") or "").strip().lower() == "waiting_shared_bridge":
            return result
        accepted = bool(result.get("accepted"))
        reason = str(result.get("reason", "") or "").strip()
        if not accepted and reason == "already_running":
            return result
        if not accepted:
            error_text = str(result.get("error", "") or "").strip() or "告警信息文件上传失败"
            raise RuntimeError(error_text)
        if reason == "partial_completed":
            failed_entries = ", ".join(str(item or "").strip() for item in result.get("failed_entries", []) or [] if str(item or "").strip())
            raise RuntimeError(f"存在失败楼栋，请查看日志{f'：{failed_entries}' if failed_entries else ''}")
        return result

    job_result = _start_local_background_job(
        container,
        name="使用共享文件上传60天-全部楼栋",
        feature="alarm_event_upload",
        dedupe_key=dedupe_key,
        resource_keys=["alarm_upload:global"],
        run_func=_run,
    )
    job = job_result["job"]
    if bool(job_result.get("reused")):
        return {
            "ok": True,
            "accepted": False,
            "running": True,
            "message": "使用共享文件上传60天-全部楼栋已在执行中，已聚焦到现有任务",
            "job": job,
            "reason": "already_running",
            "mode": "full",
            "scope": "all",
            "started_at": str(job.get("started_at") or job.get("created_at") or "").strip(),
        }
    container.add_system_log(f"[任务] 已提交: 使用共享文件上传60天-全部楼栋 ({job.get('job_id', '')})")
    return {
        "ok": True,
        "accepted": True,
        "running": True,
        "message": "已提交 使用共享文件上传60天-全部楼栋",
        "job": job,
        "mode": "full",
        "scope": "all",
        "started_at": str(job.get("started_at") or job.get("created_at") or "").strip(),
    }


@router.post("/api/bridge/source-cache/alarm/upload-building")
def bridge_source_cache_alarm_upload_building(
    request: Request,
    building: str = Query(..., description="楼栋，如 A楼"),
) -> Dict[str, Any]:
    if _deployment_role_mode(request) != "external":
        raise HTTPException(status_code=409, detail="当前仅外网端允许上传告警信息文件到多维表")
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    if service is None:
        raise HTTPException(status_code=409, detail="共享桥接服务未初始化")
    building_text = str(building or "").strip()

    def _run(emit_log) -> Dict[str, Any]:
        result = _run_external_alarm_upload_shared_flow(
            container=container,
            service=service,
            mode="single_building",
            building=building_text,
            emit_log=emit_log,
        )
        if str(result.get("mode", "") or "").strip().lower() == "waiting_shared_bridge":
            return result
        accepted = bool(result.get("accepted"))
        reason = str(result.get("reason", "") or "").strip()
        if not accepted and reason == "already_running":
            return result
        if not accepted:
            error_text = str(result.get("error", "") or "").strip() or "告警信息文件上传失败"
            raise RuntimeError(error_text)
        if reason == "partial_completed":
            failed_entries = ", ".join(str(item or "").strip() for item in result.get("failed_entries", []) or [] if str(item or "").strip())
            raise RuntimeError(f"存在失败楼栋，请查看日志{f'：{failed_entries}' if failed_entries else ''}")
        return result

    job_result = _start_local_background_job(
        container,
        name=f"使用共享文件上传60天-{building_text}",
        feature="alarm_event_upload",
        dedupe_key=f"alarm_event_upload:building:{building_text}",
        resource_keys=["alarm_upload:global"],
        run_func=_run,
    )
    job = job_result["job"]
    if bool(job_result.get("reused")):
        return {
            "ok": True,
            "accepted": False,
            "running": True,
            "message": f"使用共享文件上传60天-{building_text} 已在执行中，请求已复用现有任务",
            "job": job,
            "reason": "already_running",
            "mode": "single_building",
            "scope": building_text,
            "started_at": str(job.get("started_at") or job.get("created_at") or "").strip(),
        }
    container.add_system_log(f"[任务] 已提交: 使用共享文件上传60天-{building_text} ({job.get('job_id', '')})")
    return {
        "ok": True,
        "accepted": True,
        "running": True,
        "message": f"已提交 使用共享文件上传60天-{building_text}",
        "job": job,
        "mode": "single_building",
        "scope": building_text,
        "started_at": str(job.get("started_at") or job.get("created_at") or "").strip(),
    }


@router.post("/api/bridge/source-cache/debug-alarm-page-actions")
def bridge_source_cache_debug_alarm_page_actions(
    request: Request,
    building: str = Query(..., description="楼栋，如 A楼"),
) -> Dict[str, Any]:
    raise HTTPException(status_code=410, detail="告警页面调试入口已退役，当前版本仅支持 API 拉取")


@router.post("/api/bridge/source-cache/refresh-today")
def bridge_source_cache_refresh_today(request: Request) -> Dict[str, Any]:
    raise HTTPException(status_code=410, detail="“当天全量下载”已停用，请改用“立即下载当前小时全部文件”")


@router.get("/api/bridge/tasks")
def bridge_tasks(request: Request, limit: int = Query(100, ge=1, le=500)) -> Dict[str, Any]:
    container = request.app.state.container
    coordinator = getattr(container, "runtime_status_coordinator", None)
    safe_limit = max(1, min(int(limit or 100), 500))
    if coordinator is not None and callable(getattr(coordinator, "is_running", None)) and coordinator.is_running():
        snapshot = coordinator.read_scope_snapshot("bridge_tasks_summary")
        payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
        rows = payload.get("tasks", []) if isinstance(payload, dict) else []
        if isinstance(rows, list):
            limited_tasks = rows[:safe_limit]
            summary = build_bridge_tasks_summary(
                [_bridge_present_task(task) for task in limited_tasks],
                count=min(
                    int(payload.get("count", 0) or len(rows)) if isinstance(payload, dict) else len(rows),
                    safe_limit,
                ),
            )
            return {"ok": True, **summary}
    service = getattr(container, "shared_bridge_service", None)
    summary = build_bridge_tasks_summary(
        [_bridge_present_task(task) for task in _list_visible_bridge_tasks(service, limit=safe_limit)],
    )
    return {"ok": True, **summary}


@router.get("/api/bridge/tasks/{task_id}")
def bridge_task_detail(task_id: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    payload = _get_visible_bridge_task(service, task_id)
    if not payload:
        raise HTTPException(status_code=404, detail="共享任务不存在")
    return {"ok": True, "task": present_bridge_task(_bridge_present_task(payload))}


@router.post("/api/bridge/tasks/{task_id}/cancel")
def bridge_task_cancel(task_id: str, request: Request) -> Dict[str, Any]:
    _ensure_bridge_write_allowed(request)
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    try:
        cancelled = bool(service and service.cancel_task(task_id))
    except Exception as exc:  # noqa: BLE001
        if not service or not _bridge_store_read_is_recoverable(service, exc):
            raise
        _raise_bridge_store_http_error(exc)
    if not cancelled:
        raise HTTPException(status_code=404, detail="共享任务不存在")
    task_payload = _get_visible_bridge_task(service, task_id)
    return {
        "ok": True,
        "accepted": True,
        "task": present_bridge_task(_bridge_present_task(task_payload)) if task_payload else None,
        "bridge_tasks_summary": _build_bridge_tasks_summary_payload(service, limit=60),
    }


@router.post("/api/bridge/tasks/{task_id}/retry")
def bridge_task_retry(task_id: str, request: Request) -> Dict[str, Any]:
    _ensure_bridge_write_allowed(request)
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    try:
        retried = bool(service and service.retry_task(task_id))
    except Exception as exc:  # noqa: BLE001
        if not service or not _bridge_store_read_is_recoverable(service, exc):
            raise
        _raise_bridge_store_http_error(exc)
    if not retried:
        raise HTTPException(status_code=404, detail="共享任务不存在")
    task_payload = _get_visible_bridge_task(service, task_id)
    return {
        "ok": True,
        "accepted": True,
        "task": present_bridge_task(_bridge_present_task(task_payload)) if task_payload else None,
        "bridge_tasks_summary": _build_bridge_tasks_summary_payload(service, limit=60),
    }
