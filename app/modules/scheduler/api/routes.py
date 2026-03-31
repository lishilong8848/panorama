from __future__ import annotations

import copy
import re
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.config.settings_loader import save_settings


router = APIRouter(prefix="/api/scheduler", tags=["scheduler"])



def _valid_time(value: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", str(value or "").strip()))


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
    action = container.start_scheduler()
    return _build_scheduler_payload(container, action_result=action)


@router.post("/stop")
def scheduler_stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    action = container.stop_scheduler()
    return _build_scheduler_payload(container, action_result=action)


@router.post("/config")
def scheduler_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是JSON对象")

    container = request.app.state.container
    old_scheduler_cfg = _scheduler_cfg_from_v3(container.config)
    old_run_time = str(old_scheduler_cfg.get("run_time", "")).strip()

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
        "run_time",
        "catch_up_if_missed",
        "retry_failed_in_same_period",
        "state_file",
    }
    for key in allowed:
        if key not in payload:
            continue
        value = payload.get(key)
        if key == "run_time":
            text = str(value or "").strip()
            if not _valid_time(text):
                raise HTTPException(status_code=400, detail="run_time 必须是 HH:MM:SS")
            scheduler_cfg[key] = text
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
    new_run_time = str(new_scheduler_cfg.get("run_time", "")).strip()
    run_time_changed = old_run_time != new_run_time
    state_reset: Dict[str, Any] = {"changed": False, "period": "", "reset_keys": [], "state_path": ""}
    if run_time_changed and container.scheduler:
        state_reset = container.scheduler.reset_today_state_for_run_time_change()

    runtime = container.scheduler.get_runtime_snapshot() if container.scheduler else {}
    executor_bound = bool(container.is_scheduler_executor_bound())
    if run_time_changed:
        message = "调度配置已更新；检测到每日执行时间变化，已自动重置今日调度状态"
    else:
        message = "调度配置已更新并热重载"

    return {
        "ok": True,
        "message": message,
        "run_time_changed": run_time_changed,
        "state_reset": state_reset,
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
