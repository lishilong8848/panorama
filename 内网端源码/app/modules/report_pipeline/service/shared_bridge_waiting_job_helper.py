from __future__ import annotations

from typing import Any, Dict, Tuple


def get_or_create_bridge_task(
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
    job = job_service.create_waiting_worker_job(
        name=name,
        worker_handler=worker_handler,
        worker_payload=worker_payload,
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
    task = get_or_create_bridge_task(
        bridge_service,
        get_or_create_name=bridge_get_or_create_name,
        create_name=bridge_create_name,
        **task_kwargs,
    )
    job_service.bind_bridge_task(str(getattr(job, "job_id", "") or "").strip(), str(task.get("task_id", "") or "").strip())
    return job, task
