from __future__ import annotations

from typing import Any, Callable, Dict, List

from app.modules.report_pipeline.service.job_service import TaskEngineUnavailableError

_JOB_RUNNING_STATUSES = {"running"}
_JOB_WAITING_STATUSES = {"queued", "waiting_resource"}
_JOB_INCOMPLETE_STATUSES = _JOB_RUNNING_STATUSES | _JOB_WAITING_STATUSES
_JOB_DEPENDENCY_STATUSES = {"dependency_checking", "dependency_syncing", "dependency_repairing"}
_JOB_FINISHED_STATUSES = {
    "success",
    "failed",
    "cancelled",
    "interrupted",
    "partial_failed",
    "blocked_precondition",
}
_HANDOVER_GENERATION_FEATURES = {
    "handover_from_download",
    "handover_from_file",
    "handover_from_files",
}
_BRIDGE_TERMINAL_STATUSES = {"success", "failed", "partial_failed", "cancelled", "stale"}
_BRIDGE_WAITING_TEXTS = (
    "等待最新共享文件更新",
    "等待缺失楼栋共享文件补齐",
    "等待过旧楼栋共享文件更新",
    "等待共享文件",
)
_FULL_TASK_PANEL_LIMIT = 2000
_RECENT_FINISHED_JOB_LIMIT = 20


def _format_job_status(status: Any) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "dependency_checking":
        return "检查依赖中"
    if normalized == "dependency_syncing":
        return "同步依赖中"
    if normalized == "dependency_repairing":
        return "修复依赖中"
    if normalized == "queued":
        return "排队中"
    if normalized == "waiting_resource":
        return "等待资源"
    if normalized == "running":
        return "执行中"
    if normalized == "success":
        return "成功"
    if normalized == "failed":
        return "失败"
    if normalized == "cancelled":
        return "已取消"
    if normalized == "interrupted":
        return "已中断"
    if normalized == "partial_failed":
        return "部分失败"
    if normalized == "blocked_precondition":
        return "前置条件阻塞"
    return str(status or "").strip() or "-"


def _format_job_tone(status: Any) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"dependency_checking", "dependency_syncing"}:
        return "info"
    if normalized == "dependency_repairing":
        return "warning"
    if normalized == "success":
        return "success"
    if normalized == "running":
        return "info"
    if normalized in {"queued", "waiting_resource"}:
        return "warning"
    if normalized == "cancelled":
        return "neutral"
    if normalized in {"interrupted", "failed", "partial_failed", "blocked_precondition"}:
        return "danger"
    return "neutral"


def _format_job_wait_reason(job: Dict[str, Any]) -> str:
    raw = str(job.get("wait_reason", "") or "").strip()
    if not raw:
        return str(job.get("status", "waiting_resource") or "").strip() or "waiting_resource"
    parts = [str(item or "").strip() for item in raw.split(",")]
    mapped: List[str] = []
    for item in parts:
        if not item:
            continue
        if item == "waiting:browser_controlled":
            mapped.append("等待受控浏览器")
        elif item == "waiting:handover_batch":
            mapped.append("等待交接班批次锁")
        elif item == "waiting:network_pipeline":
            mapped.append("等待网络流水线")
        elif item == "waiting:network_internal":
            mapped.append("等待内网窗口")
        elif item == "waiting:network_external":
            mapped.append("等待外网窗口")
        elif item == "waiting:network_internal_unreachable":
            mapped.append("等待内网可达")
        elif item == "waiting:network_external_unreachable":
            mapped.append("等待外网可达")
        elif item == "waiting:output_path":
            mapped.append("等待输出文件锁")
        elif item == "waiting:source_identity":
            mapped.append("等待共享源文件")
        elif item == "waiting:app_update":
            mapped.append("等待更新独占")
        elif item == "waiting:dependency_sync":
            mapped.append("正在自动补齐运行依赖")
        else:
            mapped.append(item)
    return " / ".join(mapped) if mapped else raw


def _job_dependency_display_status(job: Dict[str, Any]) -> str:
    stages = job.get("stages", []) if isinstance(job.get("stages", []), list) else []
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        worker_status = str(stage.get("worker_status", "") or "").strip().lower()
        if worker_status in _JOB_DEPENDENCY_STATUSES:
            return worker_status
    return ""


def _is_handover_generation_job(job: Dict[str, Any]) -> bool:
    if not isinstance(job, dict):
        return False
    feature = str(job.get("feature", "") or "").strip().lower()
    status = str(job.get("status", "") or "").strip().lower()
    return feature in _HANDOVER_GENERATION_FEATURES and status in _JOB_INCOMPLETE_STATUSES


def _present_job(job: Dict[str, Any]) -> Dict[str, Any]:
    status = str(job.get("status", "") or "").strip().lower()
    dependency_status = _job_dependency_display_status(job)
    display_status = dependency_status or status
    job_id = str(job.get("job_id", "") or "").strip()
    time_text = (
        str(job.get("started_at", "") or "").strip()
        or str(job.get("created_at", "") or "").strip()
    )
    error_text = str(job.get("error", "") or "").strip()
    summary_text = str(job.get("summary", "") or "").strip()
    wait_reason = str(job.get("wait_reason", "") or "").strip()
    detail_text = ""
    if error_text:
        detail_text = f"说明：{error_text}"
    elif summary_text:
        detail_text = f"说明：{summary_text}"
    elif wait_reason:
        detail_text = f"说明：{_format_job_wait_reason(job)}"
    elif dependency_status:
        detail_text = "说明：正在自动补齐运行依赖"
    meta_parts = [f"状态：{_format_job_status(display_status)}"]
    if time_text:
        meta_parts.append(f"时间：{time_text}")
    cancel_allowed = (
        bool(job_id)
        and status in _JOB_INCOMPLETE_STATUSES
        and not bool(job.get("cancel_requested"))
    )
    retry_allowed = False
    if bool(job_id) and status in {"failed", "cancelled", "interrupted"}:
        stages = job.get("stages", []) if isinstance(job.get("stages", []), list) else []
        retry_allowed = bool(
            stages
            and isinstance(stages[0], dict)
            and str(stages[0].get("worker_handler", "") or "").strip()
        )
    return {
        **job,
        "item_kind": "job",
        "__waiting_kind": "job",
        "__waiting_id": f"job:{job_id}",
        "status_text": _format_job_status(display_status),
        "tone": _format_job_tone(display_status),
        "display_title": str(job.get("name", "") or "").strip()
        or str(job.get("feature", "") or "").strip()
        or job_id
        or "-",
        "display_meta": " | ".join(meta_parts),
        "display_detail": detail_text,
        "can_cancel": cancel_allowed,
        "actions": {
            "cancel": {
                "allowed": cancel_allowed,
                "pending": bool(job.get("cancel_requested")),
                "label": "取消中..." if bool(job.get("cancel_requested")) else "取消任务",
                "disabled_reason": (
                    "取消请求已提交"
                    if bool(job.get("cancel_requested"))
                    else ("" if cancel_allowed else "当前状态不可取消")
                ),
                "reason_code": (
                    "cancel_requested"
                    if bool(job.get("cancel_requested"))
                    else ("job_cancel_available" if cancel_allowed else "job_cancel_not_allowed")
                ),
                "target_kind": "job",
                "target_id": job_id,
            },
            "retry": {
                "allowed": retry_allowed,
                "pending": False,
                "label": "重试任务",
                "disabled_reason": "" if retry_allowed else "当前状态不可重试",
                "reason_code": "job_retry_available" if retry_allowed else "job_retry_not_allowed",
                "target_kind": "job",
                "target_id": job_id,
            },
        },
    }


def present_job_item(job: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(job, dict):
        return {}
    return _present_job(job)


def _format_bridge_feature(feature: Any) -> str:
    normalized = str(feature or "").strip().lower()
    if normalized == "handover_from_download":
        return "交接班使用共享文件生成"
    if normalized == "day_metric_from_download":
        return "12项使用共享文件上传"
    if normalized == "wet_bulb_collection":
        return "湿球温度采集"
    if normalized in {"chiller_mode_upload", "chiller_mode_upload_external_dispatch", "chiller_mode_upload_cache_latest"}:
        return "制冷模式参数上传"
    if normalized == "monthly_report_pipeline":
        return "月报主流程"
    if normalized == "internal_browser_alert":
        return "内网环境告警"
    return str(feature or "").strip() or "-"


def _format_bridge_role(role: Any) -> str:
    normalized = str(role or "").strip().lower()
    if normalized == "internal":
        return "内网端"
    if normalized == "external":
        return "外网端"
    return str(role or "").strip() or "-"


def _format_bridge_error_text(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    lowered = normalized.lower()
    if lowered == "internal_download_failed":
        return "共享文件准备失败"
    if lowered == "internal_query_failed":
        return "内网查询失败"
    if lowered == "external_upload_failed":
        return "外网上传失败"
    if lowered == "external_continue_failed":
        return "外网继续处理失败"
    if lowered == "missing_source_file":
        return "缺少共享文件"
    if lowered == "await_external":
        return "等待外网继续处理"
    if lowered == "waiting_source_sync":
        return "等待内网补采同步"
    if lowered == "shared_bridge_disabled":
        return "共享桥接未启用"
    if lowered == "shared_bridge_service_unavailable":
        return "共享桥接服务不可用"
    if lowered in {"disabled_or_switching", "disabled_or_unselected"}:
        return "当前未启用共享桥接"
    if lowered == "misconfigured":
        return "共享桥接目录未配置"
    if lowered == "busy":
        return "共享桥接数据库正忙"
    if lowered == "unavailable":
        return "共享桥接数据库暂时不可用"
    if lowered == "database is locked":
        return "共享桥接数据库正忙，请稍后重试"
    if lowered == "unable to open database file":
        return "无法打开共享桥接数据库文件"
    if lowered in {
        "cannot operate on a closed database",
        "cannot operate on a closed database.",
    }:
        return "共享桥接数据库连接已关闭"
    if "permissionerror" in lowered or "winerror 5" in lowered:
        return "共享桥接目录无写入权限"
    if "no such table" in lowered:
        return "共享桥接数据库结构未初始化"
    return normalized


def _get_latest_bridge_event(task: Dict[str, Any]) -> Dict[str, Any] | None:
    events = task.get("events")
    if isinstance(events, list) and events:
        first = events[0]
        return first if isinstance(first, dict) else None
    return None


def _is_bridge_waiting_source_sync(task: Dict[str, Any]) -> bool:
    normalized_status = str(task.get("status", "") or "").strip().lower()
    if normalized_status not in {"ready_for_external", "waiting_next_side"}:
        return False
    latest_event = _get_latest_bridge_event(task) or {}
    latest_event_type = str(latest_event.get("event_type", "") or "").strip().lower()
    if latest_event_type == "waiting_source_sync":
        return True
    latest_text = str(
        latest_event.get("event_text", "")
        or ((latest_event.get("payload") or {}) if isinstance(latest_event.get("payload"), dict) else {}).get("message", "")
        or ""
    ).strip()
    return "等待内网补采同步" in latest_text


def _format_bridge_task_status(status_or_task: Any, task_like: Dict[str, Any] | None = None) -> str:
    task = status_or_task if isinstance(status_or_task, dict) else (task_like if isinstance(task_like, dict) else None)
    normalized = str((task or {}).get("status", "") if task else status_or_task or "").strip().lower()
    if normalized == "pending":
        return "待执行"
    if normalized == "claimed":
        return "已认领"
    if normalized == "running":
        return "执行中"
    if normalized == "blocked":
        return "已阻塞"
    if normalized == "expired":
        return "已过期"
    if normalized == "waiting_next_side":
        return "等待内网补采同步" if task and _is_bridge_waiting_source_sync(task) else "等待下一侧"
    if normalized == "queued_for_internal":
        return "等待共享文件"
    if normalized == "internal_claimed":
        return "共享文件已认领"
    if normalized == "internal_running":
        return "共享文件准备中"
    if normalized == "ready_for_external":
        return "等待内网补采同步" if task and _is_bridge_waiting_source_sync(task) else "等待外网继续"
    if normalized == "external_claimed":
        return "外网已认领"
    if normalized == "external_running":
        return "外网处理中"
    if normalized == "success":
        return "成功"
    if normalized == "partial_failed":
        return "部分失败"
    if normalized == "failed":
        return "失败"
    if normalized == "cancelled":
        return "已取消"
    if normalized == "stale":
        return "超时失效"
    return str(status_or_task or "").strip() or "-"


def _format_bridge_task_tone(status: Any) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "success":
        return "success"
    if normalized in {"failed", "partial_failed", "stale", "expired"}:
        return "danger"
    if normalized == "blocked":
        return "warning"
    if normalized == "cancelled":
        return "neutral"
    if normalized in {"queued_for_internal", "ready_for_external", "pending", "waiting_next_side"}:
        return "warning"
    if normalized in {
        "internal_claimed",
        "internal_running",
        "external_claimed",
        "external_running",
        "claimed",
        "running",
    }:
        return "info"
    return "neutral"


def _format_bridge_stage_name(stage: Dict[str, Any], feature: Any = "", mode: Any = "") -> str:
    explicit = str(stage.get("stage_name", "") or "").strip()
    if explicit:
        return explicit
    feature_text = str(feature or "").strip().lower()
    mode_text = str(mode or "").strip().lower()
    stage_id = str(stage.get("stage_id", "") or stage.get("handler", "") or "").strip().lower()
    if stage_id == "internal_download":
        if feature_text == "handover_from_download":
            return "准备交接班共享文件"
        if feature_text == "day_metric_from_download":
            return "准备12项共享文件"
        if feature_text == "wet_bulb_collection":
            return "准备湿球共享文件"
        if feature_text in {"chiller_mode_upload", "chiller_mode_upload_external_dispatch", "chiller_mode_upload_cache_latest"}:
            return "准备制冷模式参数共享文件"
        if feature_text == "monthly_report_pipeline":
            return "准备月报历史共享文件" if mode_text == "multi_date" else "准备月报共享文件"
        return "准备共享文件"
    if stage_id == "internal_query":
        return "内网查询告警数据"
    if stage_id == "external_generate_review_output":
        return "使用共享文件生成交接班"
    if stage_id == "external_upload":
        if feature_text == "day_metric_from_download":
            return "使用共享文件上传12项"
        return "外网继续上传"
    if stage_id == "external_extract_and_upload":
        return "使用共享文件上传湿球温度"
    if stage_id == "external_resume":
        if feature_text == "monthly_report_pipeline" and mode_text == "resume_upload":
            return "外网断点续传月报"
        if feature_text == "monthly_report_pipeline":
            return "使用共享文件上传月报"
        return "外网继续处理"
    if stage_id == "external_notify":
        if feature_text == "internal_browser_alert":
            return "外网发送告警"
        return "外网通知"
    return str(stage.get("stage_id", "") or stage.get("handler", "") or "").strip() or "-"


def _format_bridge_stage_summary(task: Dict[str, Any]) -> str:
    current_stage_name = str(task.get("current_stage_name", "") or "").strip()
    current_stage_role = str(task.get("current_stage_role", "") or "").strip()
    current_stage_status = str(task.get("current_stage_status", "") or "").strip()
    if current_stage_name:
        return (
            f"{_format_bridge_role(current_stage_role)} / {current_stage_name} / "
            f"{_format_bridge_task_status(current_stage_status or task.get('status'), task)}"
        )
    stages = task.get("stages")
    if not isinstance(stages, list) or not stages:
        return "阶段信息待同步"
    current = next(
        (
            item
            for item in stages
            if isinstance(item, dict)
            and str(item.get("status", "") or "").strip().lower()
            in {"running", "claimed", "pending", "waiting_next_side"}
        ),
        None,
    )
    if current is None:
        current = next(
            (
                item
                for item in stages
                if isinstance(item, dict) and str(item.get("error", "") or "").strip()
            ),
            None,
        )
    if current is None:
        current = stages[-1] if isinstance(stages[-1], dict) else {}
    return (
        f"{_format_bridge_role(current.get('role_target'))} / "
        f"{_format_bridge_stage_name(current, task.get('feature'), task.get('mode'))} / "
        f"{_format_bridge_task_status(current.get('status'), task)}"
    )


def _format_bridge_task_error(task: Dict[str, Any]) -> str:
    return (
        _format_bridge_error_text(task.get("display_error"))
        or _format_bridge_error_text(task.get("current_stage_error"))
        or _format_bridge_error_text(task.get("error"))
        or "-"
    )


def _format_bridge_artifact_summary(task: Dict[str, Any]) -> str:
    artifacts = task.get("artifacts", []) if isinstance(task.get("artifacts", []), list) else []
    if not artifacts:
        return "暂无产物"
    ready_count = sum(
        1
        for item in artifacts
        if isinstance(item, dict)
        and str(item.get("status", "") or "").strip().lower() == "ready"
    )
    return f"产物 {ready_count}/{len(artifacts)}"


def _is_bridge_terminal_status(task: Dict[str, Any]) -> bool:
    normalized = str(task.get("status", "") or "").strip().lower()
    return normalized in _BRIDGE_TERMINAL_STATUSES


def _is_bridge_waiting_resource_task(task: Dict[str, Any]) -> bool:
    if _is_bridge_terminal_status(task):
        return False
    normalized_status = str(task.get("status", "") or "").strip().lower()
    if normalized_status in {"queued_for_internal", "internal_claimed", "internal_running"}:
        return True
    if _is_bridge_waiting_source_sync(task):
        return True
    combined = f"{_format_bridge_task_error(task)} {_format_bridge_stage_summary(task)}".strip()
    return any(text in combined for text in _BRIDGE_WAITING_TEXTS)


def present_bridge_task(task: Dict[str, Any]) -> Dict[str, Any]:
    task_id = str(task.get("task_id", "") or "").strip()
    time_text = str(task.get("updated_at", "") or "").strip() or str(task.get("created_at", "") or "").strip()
    error_text = _format_bridge_task_error(task)
    summary_text = str(task.get("summary", "") or "").strip()
    stage_text = _format_bridge_stage_summary(task)
    detail_text = ""
    if error_text and error_text != "-":
        detail_text = f"说明：{error_text}"
    elif summary_text:
        detail_text = f"说明：{summary_text}"
    elif stage_text and stage_text != "-":
        detail_text = f"说明：{stage_text}"
    meta_parts = [f"状态：{_format_bridge_task_status(task)}"]
    if time_text:
        meta_parts.append(f"时间：{time_text}")
    cancel_allowed = bool(task_id) and not _is_bridge_terminal_status(task)
    normalized_status = str(task.get("status", "") or "").strip().lower()
    retry_allowed = bool(task_id) and normalized_status in {
        "failed",
        "partial_failed",
        "cancelled",
        "stale",
        "expired",
        "blocked",
    }
    return {
        **task,
        "item_kind": "bridge",
        "status_text": _format_bridge_task_status(task),
        "tone": _format_bridge_task_tone(task.get("status")),
        "stage_summary_text": stage_text,
        "error_text": "" if error_text == "-" else error_text,
        "artifact_summary_text": _format_bridge_artifact_summary(task),
        "display_title": str(task.get("feature_label", "") or "").strip()
        or _format_bridge_feature(task.get("feature"))
        or task_id
        or "-",
        "display_meta": " | ".join(meta_parts),
        "display_detail": detail_text,
        "can_cancel": cancel_allowed,
        "actions": {
            "cancel": {
                "allowed": cancel_allowed,
                "pending": False,
                "label": "取消任务",
                "disabled_reason": "" if cancel_allowed else "当前状态不可取消",
                "reason_code": "bridge_cancel_available" if cancel_allowed else "bridge_cancel_not_allowed",
                "target_kind": "bridge",
                "target_id": task_id,
            },
            "retry": {
                "allowed": retry_allowed,
                "pending": False,
                "label": "重试任务",
                "disabled_reason": "" if retry_allowed else "当前状态不可重试",
                "reason_code": "bridge_retry_available" if retry_allowed else "bridge_retry_not_allowed",
                "target_kind": "bridge",
                "target_id": task_id,
            },
        },
    }


def _present_bridge_waiting_item(task: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **present_bridge_task(task),
        "__waiting_kind": "bridge",
        "__waiting_id": f"bridge:{str(task.get('task_id', '') or '').strip()}",
    }


def build_bridge_tasks_summary(
    tasks: List[Dict[str, Any]] | None,
    *,
    count: int | None = None,
) -> Dict[str, Any]:
    normalized_tasks = [task for task in (tasks or []) if isinstance(task, dict)]
    presented_tasks = [present_bridge_task(task) for task in normalized_tasks]
    active_tasks = [
        item for item in presented_tasks if not _is_bridge_terminal_status(item)
    ]
    recent_finished_tasks = [
        item for item in presented_tasks if _is_bridge_terminal_status(item)
    ][:8]
    waiting_resource_items = [
        _present_bridge_waiting_item(task)
        for task in normalized_tasks
        if _is_bridge_waiting_resource_task(task)
    ]
    active_count = len(active_tasks)
    waiting_count = len(waiting_resource_items)
    finished_count = sum(
        1 for item in presented_tasks if _is_bridge_terminal_status(item)
    )
    recent_failure = next(
        (
            item
            for item in recent_finished_tasks
            if str(item.get("status", "")).strip().lower()
            in {"failed", "partial_failed", "stale"}
        ),
        None,
    )
    focus_item = active_tasks[0] if active_tasks else (
        waiting_resource_items[0] if waiting_resource_items else None
    )
    tone = "neutral"
    status_text = "当前空闲"
    summary_text = "暂无共享桥接任务。"
    next_action_text = "需要跨内外网动作时，再查看共享桥接队列。"
    if active_count > 0:
        tone = "info" if waiting_count <= 0 else "warning"
        status_text = "当前有共享桥接任务"
        summary_text = (
            f"当前有 {active_count} 个共享桥接任务在处理中"
            + (f"，其中 {waiting_count} 个处于等待资源。" if waiting_count > 0 else "。")
        )
        next_action_text = "优先看当前认领任务；等待资源的任务会在条件满足后自动继续。"
    elif waiting_count > 0:
        tone = "warning"
        status_text = "共享桥接任务等待资源"
        summary_text = f"当前有 {waiting_count} 个共享桥接任务等待共享文件或内网补采。"
        next_action_text = "先处理共享文件或内网补采阻塞，再决定是否人工重试。"
    elif recent_failure:
        tone = "danger"
        status_text = "最近有共享桥接失败任务"
        summary_text = f"最近失败任务：{recent_failure.get('display_title') or '-'}"
        next_action_text = "先看失败摘要，再决定是否重试或取消。"

    return {
        "tasks": presented_tasks,
        "count": int(count or len(normalized_tasks)),
        "display": {
            "active_tasks": active_tasks,
            "waiting_resource_items": waiting_resource_items,
            "recent_finished_tasks": recent_finished_tasks,
            "active_count": active_count,
            "waiting_count": waiting_count,
            "finished_count": finished_count,
            "overview": {
                "reason_code": (
                    "active"
                    if active_count > 0
                    else ("waiting_resource" if waiting_count > 0 else ("recent_failure" if recent_failure else "idle"))
                ),
                "active_count": active_count,
                "waiting_count": waiting_count,
                "finished_count": finished_count,
                "recent_failure_title": (
                    recent_failure.get("display_title", "")
                    if isinstance(recent_failure, dict)
                    else ""
                ),
                "tone": tone,
                "status_text": status_text,
                "summary_text": summary_text,
                "detail_text": (
                    "共享桥接的处理中、等待资源和最近完成都以后端桥接摘要为准。"
                    if active_count > 0 or waiting_count > 0 or finished_count > 0
                    else "当前没有共享桥接任务。"
                ),
                "next_action_text": next_action_text,
                "focus_title": (
                    (focus_item or {}).get("display_title", "当前没有选中共享桥接任务")
                    if isinstance(focus_item, dict)
                    else "当前没有选中共享桥接任务"
                ),
                "focus_meta": (
                    (focus_item or {}).get("display_meta", "暂无共享桥接任务")
                    if isinstance(focus_item, dict)
                    else "暂无共享桥接任务"
                ),
                "items": [
                    {
                        "label": "当前处理中",
                        "value": f"{active_count} 项",
                        "tone": "info" if active_count > 0 else "neutral",
                    },
                    {
                        "label": "等待资源",
                        "value": f"{waiting_count} 项",
                        "tone": "warning" if waiting_count > 0 else "neutral",
                    },
                    {
                        "label": "最近完成",
                        "value": f"{finished_count} 项",
                        "tone": "info" if finished_count > 0 else "neutral",
                    },
                    {
                        "label": "最近失败",
                        "value": recent_failure.get("display_title", "无")
                        if isinstance(recent_failure, dict)
                        else "无",
                        "tone": "danger" if recent_failure else "success",
                    },
                ],
                "actions": [],
            },
        },
    }


def _safe_job_counts(container: Any) -> Dict[str, Any]:
    try:
        payload = container.job_service.job_counts()
    except TaskEngineUnavailableError:
        return {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_active_job_ids(container: Any) -> List[str]:
    try:
        payload = container.job_service.active_job_ids(include_waiting=True)
    except TaskEngineUnavailableError:
        return []
    except Exception:
        return []
    return payload if isinstance(payload, list) else []


def _safe_jobs(
    container: Any,
    *,
    limit: int,
    statuses: List[str] | tuple[str, ...] | None = None,
    emit_log: Callable[[str], None] | None = None,
    strict: bool = False,
) -> List[Dict[str, Any]]:
    try:
        rows = container.job_service.list_jobs(limit=limit, statuses=statuses)
    except TaskEngineUnavailableError:
        if strict:
            raise
        return []
    except Exception as exc:  # noqa: BLE001
        if callable(emit_log):
            emit_log(f"[任务面板] 读取任务列表失败: {exc}")
        return []
    return rows if isinstance(rows, list) else []


def _safe_bridge_tasks(
    container: Any,
    *,
    limit: int,
    emit_log: Callable[[str], None] | None = None,
) -> List[Dict[str, Any]]:
    service = getattr(container, "shared_bridge_service", None)
    if service is None:
        return []
    try:
        active_reader = getattr(service, "list_active_tasks", None)
        tasks = active_reader(limit=limit) if callable(active_reader) else service.list_tasks(limit=limit)
    except Exception as exc:  # noqa: BLE001
        checker = getattr(service, "_is_recoverable_store_error", None)
        if callable(checker) and checker(exc):
            cache_reader = getattr(service, "get_cached_tasks", None)
            tasks = cache_reader(limit=limit) if callable(cache_reader) else []
        else:
            if callable(emit_log):
                emit_log(f"[任务面板] 读取共享桥接任务失败: {exc}")
            tasks = []
    if not isinstance(tasks, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for item in tasks:
        if not isinstance(item, dict):
            continue
        if str(item.get("feature", "") or "").strip().lower() == "alarm_export":
            continue
        normalized.append(item)
    return normalized


def build_job_panel_summary(
    container: Any,
    *,
    limit: int = 60,
    emit_log: Callable[[str], None] | None = None,
    strict: bool = False,
) -> Dict[str, Any]:
    safe_limit = max(1, min(max(int(limit or 0), _FULL_TASK_PANEL_LIMIT), _FULL_TASK_PANEL_LIMIT))
    incomplete_jobs = _safe_jobs(
        container,
        limit=safe_limit,
        statuses=sorted(_JOB_INCOMPLETE_STATUSES),
        emit_log=emit_log,
        strict=strict,
    )
    recent_finished_source = _safe_jobs(
        container,
        limit=_RECENT_FINISHED_JOB_LIMIT,
        statuses=sorted(_JOB_FINISHED_STATUSES),
        emit_log=emit_log,
        strict=False,
    )
    jobs_by_id: Dict[str, Dict[str, Any]] = {}
    jobs: List[Dict[str, Any]] = []
    for job in [*incomplete_jobs, *recent_finished_source]:
        if not isinstance(job, dict):
            continue
        job_id = str(job.get("job_id", "") or "").strip()
        if job_id and job_id in jobs_by_id:
            continue
        if job_id:
            jobs_by_id[job_id] = job
        jobs.append(job)
    presented_jobs = [_present_job(job) for job in jobs if isinstance(job, dict)]
    running_jobs = [item for item in presented_jobs if str(item.get("status", "")).strip().lower() in _JOB_RUNNING_STATUSES]
    waiting_jobs = [item for item in presented_jobs if str(item.get("status", "")).strip().lower() in _JOB_WAITING_STATUSES]
    recent_finished_jobs = [
        item
        for item in presented_jobs
        if str(item.get("status", "")).strip().lower() in _JOB_FINISHED_STATUSES
    ][:6]

    bridge_tasks = _safe_bridge_tasks(container, limit=safe_limit, emit_log=emit_log)
    visible_job_bridge_task_ids = {
        str(job.get("bridge_task_id", "") or "").strip()
        for job in [*running_jobs, *waiting_jobs]
        if str(job.get("bridge_task_id", "") or "").strip()
    }
    waiting_bridge_items = [
        _present_bridge_waiting_item(task)
        for task in bridge_tasks
        if isinstance(task, dict)
        and _is_bridge_waiting_resource_task(task)
        and str(task.get("task_id", "") or "").strip() not in visible_job_bridge_task_ids
    ]
    active_bridge_count = sum(
        1
        for task in bridge_tasks
        if isinstance(task, dict) and not _is_bridge_terminal_status(task)
    )

    waiting_resource_items = [*waiting_bridge_items, *waiting_jobs]
    running_count = len(running_jobs)
    waiting_count = len(waiting_resource_items)
    handover_generation_busy = any(
        _is_handover_generation_job(job)
        for job in jobs
        if isinstance(job, dict)
    )
    recent_failure = next(
        (
            item
            for item in recent_finished_jobs
            if str(item.get("status", "")).strip().lower() in {"failed", "partial_failed", "blocked_precondition", "interrupted"}
        ),
        None,
    )
    focus_item = running_jobs[0] if running_jobs else (waiting_resource_items[0] if waiting_resource_items else None)
    tone = "neutral"
    status_text = "当前空闲"
    summary_text = "暂无长耗时任务，可直接从主动作开始。"
    next_action_text = "需要细节时再展开“任务与资源”，避免先陷进状态细节。"
    if running_count > 0:
        tone = "info"
        status_text = "有任务正在执行"
        summary_text = f"当前有 {running_count} 个运行中任务" + (f"，另有 {waiting_count} 个等待资源" if waiting_count > 0 else "。")
        next_action_text = "优先盯住当前任务；长操作的结果、进度和错误都以任务区为准。"
    elif waiting_count > 0:
        tone = "warning"
        status_text = "任务正在等待资源"
        summary_text = f"当前有 {waiting_count} 个任务等待资源，可先检查网络、共享桥接或浏览器池状态。"
        next_action_text = "先处理资源阻塞，再决定是否重试任务。"
    elif active_bridge_count > 0:
        tone = "warning"
        status_text = "内外网同步仍在推进"
        summary_text = f"当前有 {active_bridge_count} 个补采同步任务仍未结束。"
        next_action_text = "优先查看补采同步任务，再执行新的跨机动作。"
    elif recent_failure:
        tone = "danger"
        status_text = "最近有失败任务"
        summary_text = f"最近失败任务：{recent_failure.get('display_title') or '-'}"
        next_action_text = "先看失败摘要，再决定是否重试。"

    return {
        "jobs": presented_jobs,
        "count": len(presented_jobs),
        "active_job_ids": _safe_active_job_ids(container),
        "job_counts": _safe_job_counts(container),
        "display": {
            "running_jobs": running_jobs,
            "waiting_resource_items": waiting_resource_items,
            "recent_finished_jobs": recent_finished_jobs,
            "overview": {
                "reason_code": (
                    "running"
                    if running_count > 0
                    else (
                        "waiting_resource"
                        if waiting_count > 0
                        else (
                            "bridge_active"
                            if active_bridge_count > 0
                            else ("recent_failure" if recent_failure else "idle")
                        )
                    )
                ),
                "running_count": running_count,
                "waiting_count": waiting_count,
                "bridge_active_count": active_bridge_count,
                "handover_generation_busy": handover_generation_busy,
                "handover_generation_status_text": (
                    "当前已有交接班日志生成任务在执行或排队，请等待完成后再发起新的交接班生成。"
                    if handover_generation_busy
                    else "当前没有交接班日志生成任务占用执行链路。"
                ),
                "recent_failure_title": (
                    recent_failure.get("display_title", "")
                    if isinstance(recent_failure, dict)
                    else ""
                ),
                "tone": tone,
                "status_text": status_text,
                "summary_text": summary_text,
                "detail_text": (
                    "运行中、等待资源、补采同步都以后端任务摘要为准。"
                    if running_count > 0 or waiting_count > 0 or active_bridge_count > 0
                    else "当前没有需要关注的长耗时任务。"
                ),
                "next_action_text": next_action_text,
                "focus_title": (focus_item or {}).get("display_title", "当前没有选中任务") if isinstance(focus_item, dict) else "当前没有选中任务",
                "focus_meta": (focus_item or {}).get("display_meta", "可以直接开始新的流程动作") if isinstance(focus_item, dict) else "可以直接开始新的流程动作",
                "items": [
                    {"label": "运行中任务", "value": f"{running_count} 个", "tone": "info" if running_count > 0 else "neutral"},
                    {"label": "等待资源", "value": f"{waiting_count} 个", "tone": "warning" if waiting_count > 0 else "neutral"},
                    {"label": "补采同步", "value": f"{active_bridge_count} 个", "tone": "warning" if active_bridge_count > 0 else "neutral"},
                    {
                        "label": "最近失败",
                        "value": recent_failure.get("display_title", "无") if isinstance(recent_failure, dict) else "无",
                        "tone": "danger" if recent_failure else "success",
                    },
                ],
                "actions": [],
            },
        },
    }
