from __future__ import annotations

import copy
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Query, Request


router = APIRouter(tags=["shared-bridge"])

_BRIDGE_FEATURE_LABELS = {
    "handover_from_download": "交接班使用共享文件生成",
    "day_metric_from_download": "12项使用共享文件上传",
    "wet_bulb_collection": "湿球温度采集",
    "monthly_report_pipeline": "月报主流程",
    "handover_cache_fill": "交接班历史共享文件补采",
    "monthly_cache_fill": "月报历史共享文件补采",
}

_BRIDGE_EVENT_TYPE_LABELS = {
    "await_external": "等待外网继续处理",
    "claimed": "已认领",
    "completed": "已完成",
    "log": "日志",
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
    "disabled_or_switching": "当前未启用共享桥接或处于单机切网端",
    "misconfigured": "共享桥接目录未配置",
    "database is locked": "共享桥接数据库正忙，请稍后重试",
    "unable to open database file": "无法打开共享桥接数据库文件",
    "cannot operate on a closed database": "共享桥接数据库连接已关闭",
    "cannot operate on a closed database.": "共享桥接数据库连接已关闭",
}


def _deployment_role_mode(request: Request) -> str:
    container = request.app.state.container
    snapshot = container.deployment_snapshot() if hasattr(container, "deployment_snapshot") else {}
    if not isinstance(snapshot, dict):
        return "switching"
    text = str(snapshot.get("role_mode", "") or "").strip().lower()
    if text in {"hybrid", "dual", "dual_reachable"}:
        return "switching"
    if text in {"switching", "internal", "external"}:
        return text
    return "switching"


def _ensure_bridge_write_allowed(request: Request) -> None:
    if _deployment_role_mode(request) == "internal":
        raise HTTPException(
            status_code=409,
            detail="当前为内网端，本地管理页只提供共享任务只读查看，请在外网端执行重试或取消。",
        )


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


def _bridge_feature_label(feature: Any) -> str:
    normalized = str(feature or "").strip().lower()
    return _BRIDGE_FEATURE_LABELS.get(normalized, str(feature or "").strip() or "-")


def _bridge_role_label(role: Any) -> str:
    normalized = str(role or "").strip().lower()
    if normalized == "internal":
        return "内网端"
    if normalized == "external":
        return "外网端"
    if normalized == "switching":
        return "单机切网端"
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
        return "外网继续上传"
    if key == "external_extract_and_upload":
        return "使用共享文件上传湿球温度"
    if key == "external_resume":
        if feature_text == "monthly_report_pipeline" and mode_text == "resume_upload":
            return "外网断点续传月报"
        if feature_text == "monthly_report_pipeline":
            return "使用共享文件上传月报"
        return "外网继续处理"
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
        if str(feature or "").strip().lower() in {"handover_cache_fill", "monthly_cache_fill"}:
            stage_id = "internal_fill"
        else:
            stage_id = "internal_download"
        role_target = "internal"
    elif status in {"ready_for_external", "external_claimed", "external_running"}:
        if str(feature or "").strip().lower() == "handover_from_download":
            stage_id = "external_generate_review_output"
        elif str(feature or "").strip().lower() == "wet_bulb_collection":
            stage_id = "external_extract_and_upload"
        elif str(feature or "").strip().lower() in {"handover_cache_fill", "monthly_cache_fill"}:
            stage_id = "external_continue"
        else:
            if str(feature or "").strip().lower() == "monthly_report_pipeline":
                stage_id = "external_resume"
            else:
                stage_id = "external_upload"
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


@router.post("/api/bridge/source-cache/refresh-today")
def bridge_source_cache_refresh_today(request: Request) -> Dict[str, Any]:
    raise HTTPException(status_code=410, detail="“当天全量下载”已停用，请改用“立即下载当前小时全部文件”")


@router.get("/api/bridge/tasks")
def bridge_tasks(request: Request, limit: int = Query(100, ge=1, le=500)) -> Dict[str, Any]:
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    tasks = service.list_tasks(limit=limit) if service else []
    tasks = [task for task in tasks if str(task.get("feature", "") or "").strip().lower() != "alarm_export"]
    return {"ok": True, "tasks": [_bridge_present_task(task) for task in tasks]}


@router.get("/api/bridge/tasks/{task_id}")
def bridge_task_detail(task_id: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    payload = service.get_task(task_id) if service else None
    if not payload or str(payload.get("feature", "") or "").strip().lower() == "alarm_export":
        raise HTTPException(status_code=404, detail="共享任务不存在")
    return {"ok": True, "task": _bridge_present_task(payload)}


@router.post("/api/bridge/tasks/{task_id}/cancel")
def bridge_task_cancel(task_id: str, request: Request) -> Dict[str, Any]:
    _ensure_bridge_write_allowed(request)
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    if not service or not service.cancel_task(task_id):
        raise HTTPException(status_code=404, detail="共享任务不存在")
    return {"ok": True}


@router.post("/api/bridge/tasks/{task_id}/retry")
def bridge_task_retry(task_id: str, request: Request) -> Dict[str, Any]:
    _ensure_bridge_write_allowed(request)
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    if not service or not service.retry_task(task_id):
        raise HTTPException(status_code=404, detail="共享任务不存在")
    return {"ok": True}
