from __future__ import annotations

import copy
from typing import Any, Callable, Dict

from fastapi import APIRouter, HTTPException, Query, Request

from app.config.config_adapter import normalize_role_mode


router = APIRouter(tags=["shared-bridge"])

_BRIDGE_FEATURE_LABELS = {
    "handover_from_download": "交接班使用共享文件生成",
    "day_metric_from_download": "12项使用共享文件上传",
    "wet_bulb_collection": "湿球温度采集",
    "monthly_report_pipeline": "月报主流程",
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
    "alarm_event_family": "告警信息源文件",
}


def _deployment_role_mode(request: Request) -> str:
    container = request.app.state.container
    snapshot = container.deployment_snapshot() if hasattr(container, "deployment_snapshot") else {}
    if not isinstance(snapshot, dict):
        return ""
    return normalize_role_mode(snapshot.get("role_mode"))


def _ensure_bridge_write_allowed(request: Request) -> None:
    if _deployment_role_mode(request) == "internal":
        raise HTTPException(
            status_code=409,
            detail="当前为内网端，本地管理页只提供共享任务只读查看，请在外网端执行重试或取消。",
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
        def _combined_log(line: str) -> None:
            text = str(line or "").strip()
            if not text:
                return
            emit_log(text)

        result = service.upload_alarm_event_source_cache_full_to_bitable(emit_log=_combined_log)
        accepted = bool(result.get("accepted"))
        reason = str(result.get("reason", "") or "").strip()
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
        def _combined_log(line: str) -> None:
            text = str(line or "").strip()
            if not text:
                return
            emit_log(text)

        result = service.upload_alarm_event_source_cache_single_building_to_bitable(
            building=building_text,
            emit_log=_combined_log,
        )
        accepted = bool(result.get("accepted"))
        reason = str(result.get("reason", "") or "").strip()
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
    service = getattr(container, "shared_bridge_service", None)
    try:
        tasks = service.list_tasks(limit=limit) if service else []
    except Exception as exc:  # noqa: BLE001
        if not service or not _bridge_store_read_is_recoverable(service, exc):
            raise
        cache_reader = getattr(service, "get_cached_tasks", None)
        tasks = cache_reader(limit=limit) if callable(cache_reader) else []
    tasks = [task for task in tasks if str(task.get("feature", "") or "").strip().lower() != "alarm_export"]
    return {"ok": True, "tasks": [_bridge_present_task(task) for task in tasks]}


@router.get("/api/bridge/tasks/{task_id}")
def bridge_task_detail(task_id: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    service = getattr(container, "shared_bridge_service", None)
    try:
        payload = service.get_task(task_id) if service else None
    except Exception as exc:  # noqa: BLE001
        if not service or not _bridge_store_read_is_recoverable(service, exc):
            raise
        cache_reader = getattr(service, "get_cached_task", None)
        payload = cache_reader(task_id) if callable(cache_reader) else None
    if not payload or str(payload.get("feature", "") or "").strip().lower() == "alarm_export":
        raise HTTPException(status_code=404, detail="共享任务不存在")
    return {"ok": True, "task": _bridge_present_task(payload)}


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
    return {"ok": True}


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
    return {"ok": True}
