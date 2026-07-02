from __future__ import annotations

from typing import Any, Dict, Tuple


def start_waiting_bridge_job(
    *,
    job_service,
    bridge_service,
    name: str,
    worker_handler: str,
    worker_payload: Dict[str, Any],
    resource_keys: list[str],
    priority: str,
    feature: str,
    dedupe_key: str,
    submitted_by: str,
    bridge_get_or_create_name: str,
    bridge_create_name: str,
    bridge_kwargs: Dict[str, Any],
    summary: str = "等待内网补采同步",
) -> Tuple[Any, Dict[str, Any]]:
    dispatch_payload = {
        "get_or_create_name": bridge_get_or_create_name,
        "create_name": bridge_create_name,
        "bridge_kwargs": dict(bridge_kwargs or {}),
        "requested_by": submitted_by,
    }
    persisted_payload = dict(worker_payload or {})
    persisted_payload["_bridge_dispatch"] = dispatch_payload
    job = job_service.create_waiting_worker_job(
        name=name,
        worker_handler=worker_handler,
        worker_payload=persisted_payload,
        resource_keys=resource_keys,
        priority=priority,
        feature=feature,
        dedupe_key=dedupe_key,
        submitted_by=submitted_by,
        wait_reason="waiting:shared_bridge",
        summary=summary,
    )
    task_kwargs = dict(bridge_kwargs or {})
    task_kwargs["resume_job_id"] = getattr(job, "job_id", "")
    task_kwargs.setdefault("requested_by", submitted_by)
    create_http = getattr(bridge_service, "create_http_bridge_task", None)
    job_id = str(getattr(job, "job_id", "") or "").strip()
    try:
        if callable(create_http):
            task = create_http(
                get_or_create_name=bridge_get_or_create_name,
                create_name=bridge_create_name,
                bridge_kwargs=task_kwargs,
            )
            if isinstance(task, dict):
                job_service.bind_bridge_task(job_id, str(task.get("task_id", "") or "").strip())
                return job, task
        raise RuntimeError("内网端 HTTP 桥接未配置或不可用，已移除旧共享库任务回退")
    except Exception as exc:
        error_text = str(exc or "").strip() or "内网端 HTTP 桥接派发失败"
        return job, {
            "task_id": "",
            "status": "dispatch_pending",
            "error": error_text,
            "transport": "http",
            "detail": "内网端 HTTP 桥接暂不可用，等待任务将由后台自动重派",
        }
