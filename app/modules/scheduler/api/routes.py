from __future__ import annotations

import copy
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.config.settings_loader import save_settings
from app.modules.scheduler.api._config_persistence import persist_scheduler_toggle


router = APIRouter(prefix="/api/scheduler", tags=["scheduler"])


def _scheduler_cfg_from_v3(config: Dict[str, Any]) -> Dict[str, Any]:
    common = config.get("common", {}) if isinstance(config, dict) else {}
    scheduler_cfg = common.get("scheduler", {}) if isinstance(common, dict) else {}
    return scheduler_cfg if isinstance(scheduler_cfg, dict) else {}


def _build_scheduler_payload(container, action_result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    scheduler = container.scheduler
    runtime = scheduler.get_runtime_snapshot() if scheduler else {}
    return {
        "ok": True,
        "action": action_result or {},
        "status": scheduler.status_text() if scheduler else "未初始化",
        "next_run_time": scheduler.next_run_text() if scheduler else "",
        "executor_bound": bool(container.is_scheduler_executor_bound()),
        "callback_name": container.scheduler_executor_name(),
        "running": bool(runtime.get("running", False)),
        "last_decision": str(runtime.get("last_decision", "")),
        "state_path": str(runtime.get("state_path", "")),
        "state_exists": bool(runtime.get("state_exists", False)),
    }


@router.post("/start")
def scheduler_start(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("common", "scheduler"), auto_start_in_gui=True)
    action = container.start_scheduler()
    return _build_scheduler_payload(container, action_result=action)


@router.post("/stop")
def scheduler_stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("common", "scheduler"), auto_start_in_gui=False)
    action = container.stop_scheduler()
    return _build_scheduler_payload(container, action_result=action)


@router.post("/config")
def scheduler_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是JSON对象")

    container = request.app.state.container

    merged = copy.deepcopy(container.config)
    common_cfg = merged.get("common")
    if not isinstance(common_cfg, dict):
        common_cfg = {}
        merged["common"] = common_cfg
    scheduler_cfg = common_cfg.get("scheduler")
    if not isinstance(scheduler_cfg, dict):
        scheduler_cfg = {}
        common_cfg["scheduler"] = scheduler_cfg

    allowed = {
        "enabled",
        "auto_start_in_gui",
        "interval_minutes",
        "check_interval_sec",
        "retry_failed_on_next_tick",
        "state_file",
    }
    for key in allowed:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"interval_minutes", "check_interval_sec"}:
            try:
                number = int(value)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail=f"{key} 必须是整数") from exc
            if number < 1:
                raise HTTPException(status_code=400, detail=f"{key} 必须大于等于1")
            scheduler_cfg[key] = number
        elif key == "state_file":
            text = str(value or "").strip()
            if not text:
                raise HTTPException(status_code=400, detail="state_file 不能为空")
            scheduler_cfg[key] = text
        else:
            scheduler_cfg[key] = bool(value)

    try:
        saved = save_settings(merged, container.config_path)
        container.reload_config(saved)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    new_scheduler_cfg = _scheduler_cfg_from_v3(container.config)
    runtime = container.scheduler.get_runtime_snapshot() if container.scheduler else {}
    executor_bound = bool(container.is_scheduler_executor_bound())
    message = "调度配置已更新并热重载"

    return {
        "ok": True,
        "message": message,
        "run_time_changed": False,
        "state_reset": {"changed": False, "period": "", "reset_keys": [], "state_path": ""},
        "executor_bound_after_reload": executor_bound,
        "callback_name": container.scheduler_executor_name(),
        "scheduler_config": {k: new_scheduler_cfg.get(k) for k in sorted(allowed)},
        "runtime": runtime,
    }


@router.get("/diagnostics")
def scheduler_diagnostics(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    scheduler = container.scheduler
    if not scheduler:
        return {
            "ok": True,
            "config": {},
            "runtime": {"executor_bound": False, "callback_name": container.scheduler_executor_name()},
            "state": {},
            "logs": [],
        }
    details = scheduler.get_diagnostics(limit=50)
    runtime = details.get("runtime")
    if not isinstance(runtime, dict):
        runtime = {}
        details["runtime"] = runtime
    runtime["executor_bound"] = bool(container.is_scheduler_executor_bound())
    runtime["callback_name"] = container.scheduler_executor_name()
    details["ok"] = True
    return details
