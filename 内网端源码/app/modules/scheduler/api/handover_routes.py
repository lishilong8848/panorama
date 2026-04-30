from __future__ import annotations

import re
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.modules.scheduler.api._config_persistence import (
    persist_scheduler_toggle,
    record_scheduler_config_autostart,
    save_handover_common_scheduler_patch,
)


router = APIRouter(prefix="/api/scheduler/handover", tags=["scheduler-handover"])


def _valid_time(value: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", str(value or "").strip()))


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
    return {
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
    scheduler_patch: Dict[str, Any] = {}
    for key in allowed:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"morning_time", "afternoon_time"}:
            text = str(value or "").strip()
            if not _valid_time(text):
                raise HTTPException(status_code=400, detail=f"{key} 必须是 HH:MM:SS")
            scheduler_patch[key] = text
        elif key == "check_interval_sec":
            try:
                number = int(value)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail="check_interval_sec 必须是整数") from exc
            if number <= 0:
                raise HTTPException(status_code=400, detail="check_interval_sec 必须大于 0")
            scheduler_patch[key] = number
        elif key in {"morning_state_file", "afternoon_state_file"}:
            text = str(value or "").strip()
            if not text:
                raise HTTPException(status_code=400, detail=f"{key} 不能为空")
            scheduler_patch[key] = text
        else:
            scheduler_patch[key] = bool(value)

    try:
        patch_result = save_handover_common_scheduler_patch(
            container,
            path=("features", "handover_log", "scheduler"),
            scheduler_patch=scheduler_patch,
            source="交接班调度配置保存",
        )
        saved = patch_result["saved_config"]
        container.reload_config(saved)
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

    status_payload = _build_handover_scheduler_payload(container)
    data = dict(status_payload)
    data.update(
        {
            "message": "交接班调度配置已更新并热重载",
            "morning_time_changed": morning_changed,
            "afternoon_time_changed": afternoon_changed,
            "state_reset": reset_results,
            "scheduler_status": status_payload,
            "scheduler_config": {k: new_cfg.get(k) for k in sorted(allowed)},
            "updated_at": str(patch_result.get("document", {}).get("updated_at", "") or ""),
            "segment_revision": int(patch_result.get("document", {}).get("revision", 0) or 0),
            "changed": bool(patch_result.get("changed", False)),
        }
    )
    return data
