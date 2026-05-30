from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Body, HTTPException, Request

from app.modules.report_pipeline.service.job_panel_presenter import (
    build_job_panel_summary,
    present_job_item,
)
from app.modules.report_pipeline.service.job_service import TaskEngineUnavailableError
from app.worker.task_handlers import HANDLER_REGISTRY


router = APIRouter(prefix="/api/tasks", tags=["tasks"])
_ACTIVE_STATUSES = ["queued", "waiting_resource", "running"]
_DENIED_WORKER_HANDLERS = {"test_echo_payload", "test_sleep"}
_ACTION_WORKER_ALIASES = {
    "alarm_event_upload": "alarm_event_upload",
    "branch_power_daily": "branch_power_from_download",
    "branch_power_from_download": "branch_power_from_download",
    "day_metric_from_download": "day_metric_from_download",
    "handover_cloud_retry_batch": "handover_cloud_retry_batch",
    "handover_cloud_retry_single": "handover_cloud_retry_single",
    "handover_confirm_all": "handover_confirm_all",
    "handover_generate": "handover_review_regenerate",
    "handover_review_regenerate": "handover_review_regenerate",
    "manual_upload": "manual_upload",
    "monthly_multi_date": "multi_date",
    "multi_date": "multi_date",
    "resume_upload": "resume_upload",
    "sheet_import": "sheet_import",
    "wet_bulb_collection": "wet_bulb_collection_run",
    "wet_bulb_collection_run": "wet_bulb_collection_run",
}


def _allowed_worker_handlers() -> List[str]:
    return sorted(name for name in HANDLER_REGISTRY.keys() if name not in _DENIED_WORKER_HANDLERS)


def _list_texts(value: Any) -> List[str]:
    if isinstance(value, str):
        source = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        source = value
    else:
        source = []
    output: List[str] = []
    for item in source:
        text = str(item or "").strip()
        if text and text not in output:
            output.append(text)
    return output


def _payload_dict(value: Any, *, field_name: str) -> Dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise HTTPException(status_code=400, detail=f"{field_name} 必须是对象")
    return value


def _refresh_runtime_status_after_task_mutation(container: Any, reason: str) -> None:
    coordinator = getattr(container, "runtime_status_coordinator", None)
    if coordinator is None:
        return
    try:
        refresh_now = getattr(coordinator, "refresh_now", None)
        if callable(refresh_now):
            refresh_now()
            return
    except Exception as exc:  # noqa: BLE001
        try:
            container.add_system_log(f"[统一任务] 任务状态变化后刷新运行态快照失败: {exc}")
        except Exception:  # noqa: BLE001
            pass
    try:
        request_refresh = getattr(coordinator, "request_refresh", None)
        if callable(request_refresh):
            request_refresh(reason=str(reason or "").strip() or "task_mutation")
    except Exception:  # noqa: BLE001
        pass


@router.get("/capabilities")
def task_capabilities() -> Dict[str, Any]:
    return {
        "submit_actions": ["worker", *sorted(_ACTION_WORKER_ALIASES.keys())],
        "action_worker_aliases": dict(sorted(_ACTION_WORKER_ALIASES.items())),
        "worker_handlers": _allowed_worker_handlers(),
        "active_statuses": list(_ACTIVE_STATUSES),
    }


@router.get("/state")
def task_state_snapshot(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    job_payload: Dict[str, Any] = {}
    try:
        job_payload = {
            "active_job_ids": container.job_service.active_job_ids(include_waiting=True),
            "job_counts": container.job_service.job_counts(),
        }
    except Exception as exc:  # noqa: BLE001
        job_payload = {
            "active_job_ids": [],
            "job_counts": {},
            "job_state_error": str(exc),
        }
    health_getter = getattr(container, "app_state_health_snapshot", None)
    if callable(health_getter):
        return {"ok": True, **health_getter(), **job_payload}
    repository = getattr(container, "app_state_repository", None)
    if repository is None:
        return {"ok": True, "ready": False, "reason": "app_state_not_initialized", **job_payload}
    try:
        return {"ok": True, **repository.snapshot(), **job_payload}
    except Exception as exc:  # noqa: BLE001
        try:
            container.add_system_log(f"[统一任务] 状态库快照读取失败，已降级: {exc}")
        except Exception:  # noqa: BLE001
            pass
        return {
            "ok": True,
            "ready": False,
            "reason": "app_state_snapshot_failed",
            "error": str(exc),
            **job_payload,
        }


@router.get("/{job_id}")
def get_task(job_id: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        return {"ok": True, "task": present_job_item(container.job_service.get_job(job_id))}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("")
def list_tasks(request: Request, limit: int = 200, status: str = "active") -> Dict[str, Any]:
    container = request.app.state.container
    normalized_status = str(status or "active").strip().lower()
    statuses = _ACTIVE_STATUSES if normalized_status in {"", "active"} else _list_texts(normalized_status)
    safe_limit = max(1, min(int(limit or 200), 2000))
    try:
        raw_jobs = container.job_service.list_jobs(limit=safe_limit, statuses=statuses)
        tasks = [present_job_item(job) for job in raw_jobs if isinstance(job, dict)]
        return {
            "ok": True,
            "tasks": tasks,
            "count": len(tasks),
            "active_job_ids": container.job_service.active_job_ids(include_waiting=True),
            "job_counts": container.job_service.job_counts(),
        }
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/{job_id}/cancel")
def cancel_task(job_id: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        current_job = container.job_service.get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    bridge_task_id = str(current_job.get("bridge_task_id", "") or "").strip() if isinstance(current_job, dict) else ""
    wait_reason = str(current_job.get("wait_reason", "") or "").strip().lower() if isinstance(current_job, dict) else ""
    bridge_cancel_error = ""
    try:
        payload = container.job_service.cancel_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if bridge_task_id and wait_reason == "waiting:shared_bridge":
        bridge_service = getattr(container, "shared_bridge_service", None)
        if bridge_service is not None:
            try:
                bridge_service.cancel_task(bridge_task_id)
            except Exception as exc:  # noqa: BLE001
                bridge_cancel_error = f"绑定补采任务取消失败，但原等待任务已取消：{exc}"
                try:
                    container.add_system_log(
                        f"[任务] 等待任务已取消，但绑定补采任务取消失败 "
                        f"job_id={job_id}, bridge_task_id={bridge_task_id}, error={exc}"
                    )
                except Exception:  # noqa: BLE001
                    pass
    _refresh_runtime_status_after_task_mutation(container, "task_cancel")
    return {
        "ok": True,
        "accepted": True,
        "task": present_job_item(payload) if isinstance(payload, dict) else payload,
        "job_panel_summary": build_job_panel_summary(container, limit=60),
        "bridge_cancel_error": bridge_cancel_error,
    }


@router.post("/submit")
def submit_task(request: Request, body: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    container = request.app.state.container
    payload = _payload_dict(body, field_name="body")
    action = str(payload.get("action", "") or "").strip().lower()
    if not action:
        action = "worker"
    worker_handler = str(payload.get("worker_handler", "") or "").strip()
    if action != "worker":
        worker_handler = _ACTION_WORKER_ALIASES.get(action, action if action in HANDLER_REGISTRY else "")
    if action == "worker" and not worker_handler:
        raise HTTPException(status_code=400, detail="action=worker 需要提供 worker_handler")
    if worker_handler not in HANDLER_REGISTRY or worker_handler in _DENIED_WORKER_HANDLERS:
        raise HTTPException(status_code=400, detail=f"未知或不允许的任务动作: action={action}, worker_handler={worker_handler}")

    default_feature = action if action != "worker" else worker_handler
    feature = str(payload.get("feature", "") or default_feature).strip() or worker_handler
    name = str(payload.get("name", "") or feature).strip() or feature
    worker_payload = _payload_dict(payload.get("payload", {}), field_name="payload")
    resource_keys = _list_texts(payload.get("resource_keys", []))
    priority = str(payload.get("priority", "") or "manual").strip().lower() or "manual"
    submitted_by = str(payload.get("submitted_by", "") or "manual").strip().lower() or "manual"
    dedupe_key = str(payload.get("dedupe_key", "") or "").strip()
    resume_policy = str(payload.get("resume_policy", "") or "manual_resume").strip() or "manual_resume"

    try:
        job = container.job_service.start_worker_job(
            name=name,
            worker_handler=worker_handler,
            worker_payload=worker_payload,
            resource_keys=resource_keys,
            priority=priority,
            feature=feature,
            dedupe_key=dedupe_key,
            submitted_by=submitted_by,
            resume_policy=resume_policy,
        )
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        container.add_system_log(f"[统一任务] 已提交: feature={feature}, handler={worker_handler}, job_id={job.job_id}")
    except Exception:  # noqa: BLE001
        pass
    _refresh_runtime_status_after_task_mutation(container, "task_submit")
    return {
        "ok": True,
        "accepted": True,
        "task": present_job_item(job.to_dict()),
        "job_panel_summary": build_job_panel_summary(container, limit=60),
    }
