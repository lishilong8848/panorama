from __future__ import annotations

import copy
import re
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.config.settings_loader import save_settings
from app.modules.scheduler.api._config_persistence import (
    persist_scheduler_toggle,
    record_scheduler_config_autostart,
)


router = APIRouter(prefix="/api/scheduler", tags=["scheduler"])


ALLOWED_KEYS = {
    "enabled",
    "auto_start_in_gui",
    "run_time",
    "check_interval_sec",
    "catch_up_if_missed",
    "retry_failed_in_same_period",
    "state_file",
}


def _valid_time(value: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", str(value or "").strip()))


def _scheduler_cfg_from_v3(config: Dict[str, Any]) -> Dict[str, Any]:
    common = config.get("common", {}) if isinstance(config, dict) else {}
    scheduler_cfg = common.get("scheduler", {}) if isinstance(common, dict) else {}
    return scheduler_cfg if isinstance(scheduler_cfg, dict) else {}


def _build_scheduler_payload(container, action_result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    snapshot = container.scheduler_status() if hasattr(container, "scheduler_status") else {}
    scheduler = container.scheduler
    runtime = scheduler.get_runtime_snapshot() if scheduler else {}
    effective_runtime = snapshot if isinstance(snapshot, dict) and snapshot else runtime
    return {
        "ok": True,
        "action": action_result or {},
        "status": str(effective_runtime.get("status", scheduler.status_text() if scheduler else "未初始化")),
        "next_run_time": str(effective_runtime.get("next_run_time", scheduler.next_run_text() if scheduler else "")),
        "executor_bound": bool(container.is_scheduler_executor_bound()),
        "callback_name": container.scheduler_executor_name(),
        "running": bool(effective_runtime.get("running", False)),
        "last_decision": str(effective_runtime.get("last_decision", "")),
        "state_path": str(effective_runtime.get("state_path", "")),
        "state_exists": bool(effective_runtime.get("state_exists", False)),
        "remembered_enabled": bool(effective_runtime.get("remembered_enabled", False)),
        "effective_auto_start_in_gui": bool(effective_runtime.get("effective_auto_start_in_gui", False)),
        "memory_source": str(effective_runtime.get("memory_source", "") or ""),
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
    old_cfg = _scheduler_cfg_from_v3(container.config)
    old_run_time = str(old_cfg.get("run_time", "") or "").strip()

    merged = copy.deepcopy(container.config)
    common_cfg = merged.get("common")
    if not isinstance(common_cfg, dict):
        common_cfg = {}
        merged["common"] = common_cfg
    scheduler_cfg = common_cfg.get("scheduler")
    if not isinstance(scheduler_cfg, dict):
        scheduler_cfg = {}
        common_cfg["scheduler"] = scheduler_cfg

    for key in ALLOWED_KEYS:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"enabled", "auto_start_in_gui"}:
            scheduler_cfg[key] = bool(value)
        elif key == "check_interval_sec":
            try:
                number = int(value)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail="check_interval_sec 必须是整数") from exc
            if number < 1:
                raise HTTPException(status_code=400, detail="check_interval_sec 必须大于等于1")
            scheduler_cfg[key] = number
        elif key in {"catch_up_if_missed", "retry_failed_in_same_period"}:
            scheduler_cfg[key] = bool(value)
        elif key == "run_time":
            text = str(value or "").strip()
            if not _valid_time(text):
                raise HTTPException(status_code=400, detail="run_time 必须是 HH:MM:SS")
            scheduler_cfg[key] = text
        elif key == "state_file":
            text = str(value or "").strip()
            if not text:
                raise HTTPException(status_code=400, detail="state_file 不能为空")
            scheduler_cfg[key] = text

    try:
        saved = save_settings(merged, container.config_path)
        container.reload_config(saved)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    new_scheduler_cfg = _scheduler_cfg_from_v3(container.config)
    record_scheduler_config_autostart(
        container,
        path=("common", "scheduler"),
        scheduler_cfg=new_scheduler_cfg,
    )
    new_run_time = str(new_scheduler_cfg.get("run_time", "") or "").strip()
    run_time_changed = bool(old_run_time and new_run_time and old_run_time != new_run_time)
    reset_result: Dict[str, Any] = {"changed": False, "period": "", "reset_keys": [], "state_path": ""}
    if run_time_changed and container.scheduler:
        reset_result = container.scheduler.reset_today_state_for_run_time_change()
    runtime = container.scheduler.get_runtime_snapshot() if container.scheduler else {}
    status_payload = _build_scheduler_payload(container)
    executor_bound = bool(container.is_scheduler_executor_bound())
    message = "调度配置已更新并热重载"

    return {
        "ok": True,
        "message": message,
        "run_time_changed": run_time_changed,
        "state_reset": reset_result,
        "executor_bound_after_reload": executor_bound,
        "callback_name": container.scheduler_executor_name(),
        "scheduler_config": {k: new_scheduler_cfg.get(k) for k in sorted(ALLOWED_KEYS)},
        "scheduler_status": status_payload,
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
