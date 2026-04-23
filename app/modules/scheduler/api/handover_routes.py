from __future__ import annotations

import copy
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.modules.scheduler.api._config_persistence import (
    persist_scheduler_toggle,
    record_scheduler_config_autostart,
    save_scheduler_config_snapshot,
)
from app.modules.scheduler.api._display_payload import with_scheduler_display
from app.modules.scheduler.api._time_normalization import normalize_scheduler_time


router = APIRouter(prefix="/api/scheduler/handover", tags=["scheduler-handover"])


def _handover_scheduler_cfg_from_v3(config: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(config, dict):
        return {}
    features = config.get("features", {})
    if not isinstance(features, dict):
        return {}
    handover = features.get("handover_log", {})
    if not isinstance(handover, dict):
        return {}
    scheduler_cfg = handover.get("scheduler", {})
    return scheduler_cfg if isinstance(scheduler_cfg, dict) else {}


def _slot_payload(slot_runtime: Dict[str, Any] | None = None) -> Dict[str, Any]:
    runtime = slot_runtime if isinstance(slot_runtime, dict) else {}
    return {
        "running": bool(runtime.get("running", False)),
        "started_at": str(runtime.get("started_at", "")),
        "next_run_time": str(runtime.get("next_run_time", "")),
        "last_check_at": str(runtime.get("last_check_at", "")),
        "last_decision": str(runtime.get("last_decision", "")),
        "last_trigger_at": str(runtime.get("last_trigger_at", "")),
        "last_trigger_result": str(runtime.get("last_trigger_result", "")),
        "state_path": str(runtime.get("state_path", "")),
        "state_exists": bool(runtime.get("state_exists", False)),
    }


def _build_handover_scheduler_payload(container, action_result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    snapshot = container.handover_scheduler_status()
    slots = snapshot.get("slots", {}) if isinstance(snapshot.get("slots", {}), dict) else {}
    morning = _slot_payload(slots.get("morning"))
    afternoon = _slot_payload(slots.get("afternoon"))
    payload = {
        "ok": True,
        "action": action_result or {},
        "enabled": bool(snapshot.get("enabled", False)),
        "running": bool(snapshot.get("running", False)),
        "status": str(snapshot.get("status", "未初始化")),
        "executor_bound": bool(container.is_handover_scheduler_executor_bound()),
        "callback_name": container.handover_scheduler_executor_name(),
        "remembered_enabled": bool(snapshot.get("remembered_enabled", False)),
        "effective_auto_start_in_gui": bool(snapshot.get("effective_auto_start_in_gui", False)),
        "memory_source": str(snapshot.get("memory_source", "") or ""),
        "morning": morning,
        "afternoon": afternoon,
        "state_paths": snapshot.get("state_paths", {}),
    }
    return with_scheduler_display(payload, container)


@router.post("/start")
def handover_scheduler_start(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "handover_log", "scheduler"), auto_start_in_gui=True)
    action = container.start_handover_scheduler()
    return _build_handover_scheduler_payload(container, action_result=action)


@router.post("/stop")
def handover_scheduler_stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "handover_log", "scheduler"), auto_start_in_gui=False)
    action = container.stop_handover_scheduler()
    return _build_handover_scheduler_payload(container, action_result=action)


@router.get("/status")
def handover_scheduler_status(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return _build_handover_scheduler_payload(container)


@router.post("/config")
def handover_scheduler_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是 JSON 对象")

    container = request.app.state.container
    old_cfg = _handover_scheduler_cfg_from_v3(container.config)
    old_morning = str(old_cfg.get("morning_time", "")).strip()
    old_afternoon = str(old_cfg.get("afternoon_time", "")).strip()

    merged = copy.deepcopy(container.config)
    features = merged.get("features")
    if not isinstance(features, dict):
        features = {}
        merged["features"] = features
    handover = features.get("handover_log")
    if not isinstance(handover, dict):
        handover = {}
        features["handover_log"] = handover
    scheduler_cfg = handover.get("scheduler")
    if not isinstance(scheduler_cfg, dict):
        scheduler_cfg = {}
        handover["scheduler"] = scheduler_cfg

    allowed = {
        "enabled",
        "auto_start_in_gui",
        "morning_time",
        "afternoon_time",
        "check_interval_sec",
        "catch_up_if_missed",
        "retry_failed_in_same_period",
        "morning_state_file",
        "afternoon_state_file",
    }
    for key in allowed:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"morning_time", "afternoon_time"}:
            scheduler_cfg[key] = normalize_scheduler_time(value, field_name=key)
        elif key == "check_interval_sec":
            try:
                number = int(value)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail="check_interval_sec 必须是整数") from exc
            if number <= 0:
                raise HTTPException(status_code=400, detail="check_interval_sec 必须大于 0")
            scheduler_cfg[key] = number
        elif key in {"morning_state_file", "afternoon_state_file"}:
            text = str(value or "").strip()
            if not text:
                raise HTTPException(status_code=400, detail=f"{key} 不能为空")
            scheduler_cfg[key] = text
        else:
            scheduler_cfg[key] = bool(value)

    try:
        save_scheduler_config_snapshot(
            container,
            merged,
            path=("features", "handover_log", "scheduler"),
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    new_cfg = _handover_scheduler_cfg_from_v3(container.config)
    record_scheduler_config_autostart(
        container,
        path=("features", "handover_log", "scheduler"),
        scheduler_cfg=new_cfg,
    )
    new_morning = str(new_cfg.get("morning_time", "")).strip()
    new_afternoon = str(new_cfg.get("afternoon_time", "")).strip()
    morning_changed = old_morning != new_morning
    afternoon_changed = old_afternoon != new_afternoon
    reset_results: Dict[str, Any] = {}
    if container.handover_scheduler_manager:
        if morning_changed:
            reset_results["morning"] = container.handover_scheduler_manager.reset_today_state_for_time_change("morning")
        if afternoon_changed:
            reset_results["afternoon"] = container.handover_scheduler_manager.reset_today_state_for_time_change(
                "afternoon"
            )

    data = _build_handover_scheduler_payload(container)
    data.update(
        {
            "message": "交接班调度配置已更新并热重载",
            "morning_time_changed": morning_changed,
            "afternoon_time_changed": afternoon_changed,
            "state_reset": reset_results,
            "scheduler_config": {k: new_cfg.get(k) for k in sorted(allowed)},
        }
    )
    return data
