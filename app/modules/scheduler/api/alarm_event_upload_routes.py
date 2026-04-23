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


router = APIRouter(prefix="/api/scheduler/alarm-event-upload", tags=["scheduler-alarm-event-upload"])


ALLOWED_KEYS = {
    "enabled",
    "auto_start_in_gui",
    "run_time",
    "state_file",
}


def _scheduler_cfg_from_v3(config: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(config, dict):
        return {}
    features = config.get("features", {})
    if not isinstance(features, dict):
        return {}
    alarm_cfg = features.get("alarm_export", {})
    if not isinstance(alarm_cfg, dict):
        return {}
    scheduler_cfg = alarm_cfg.get("scheduler", {})
    return dict(scheduler_cfg) if isinstance(scheduler_cfg, dict) else {}


def _build_payload(container, action_result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    snapshot = container.alarm_event_upload_scheduler_status()
    payload = {
        "ok": True,
        "action": action_result or {},
        "enabled": bool(snapshot.get("enabled", False)),
        "running": bool(snapshot.get("running", False)),
        "status": str(snapshot.get("status", "未初始化")),
        "next_run_time": str(snapshot.get("next_run_time", "")),
        "last_check_at": str(snapshot.get("last_check_at", "")),
        "last_decision": str(snapshot.get("last_decision", "")),
        "last_trigger_at": str(snapshot.get("last_trigger_at", "")),
        "last_trigger_result": str(snapshot.get("last_trigger_result", "")),
        "state_path": str(snapshot.get("state_path", "")),
        "state_exists": bool(snapshot.get("state_exists", False)),
        "executor_bound": bool(container.is_alarm_event_upload_scheduler_executor_bound()),
        "callback_name": container.alarm_event_upload_scheduler_executor_name(),
        "remembered_enabled": bool(snapshot.get("remembered_enabled", False)),
        "effective_auto_start_in_gui": bool(snapshot.get("effective_auto_start_in_gui", False)),
        "memory_source": str(snapshot.get("memory_source", "") or ""),
    }
    return with_scheduler_display(payload, container, slot_keys=())


@router.post("/start")
def alarm_event_upload_scheduler_start(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(
        container,
        path=("features", "alarm_export", "scheduler"),
        scheduler_key="alarm_event_upload",
        auto_start_in_gui=True,
    )
    action = container.start_alarm_event_upload_scheduler()
    return _build_payload(container, action_result=action)


@router.post("/stop")
def alarm_event_upload_scheduler_stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(
        container,
        path=("features", "alarm_export", "scheduler"),
        scheduler_key="alarm_event_upload",
        auto_start_in_gui=False,
    )
    action = container.stop_alarm_event_upload_scheduler()
    return _build_payload(container, action_result=action)


@router.get("/status")
def alarm_event_upload_scheduler_status(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return _build_payload(container)


@router.post("/config")
def alarm_event_upload_scheduler_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是JSON对象")

    container = request.app.state.container
    merged = copy.deepcopy(container.config)
    features = merged.get("features")
    if not isinstance(features, dict):
        features = {}
        merged["features"] = features
    alarm_cfg = features.get("alarm_export")
    if not isinstance(alarm_cfg, dict):
        alarm_cfg = {}
        features["alarm_export"] = alarm_cfg
    scheduler_cfg = alarm_cfg.get("scheduler")
    if not isinstance(scheduler_cfg, dict):
        scheduler_cfg = {}
        alarm_cfg["scheduler"] = scheduler_cfg

    for key in ALLOWED_KEYS:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"enabled", "auto_start_in_gui"}:
            scheduler_cfg[key] = bool(value)
        elif key == "run_time":
            scheduler_cfg[key] = normalize_scheduler_time(value)
        elif key == "state_file":
            text = str(value or "").strip()
            if not text:
                raise HTTPException(status_code=400, detail="state_file 不能为空")
            scheduler_cfg[key] = text

    restart_running = bool(container.alarm_event_upload_scheduler.is_running()) if container.alarm_event_upload_scheduler else False
    try:
        save_scheduler_config_snapshot(
            container,
            merged,
            path=("features", "alarm_export", "scheduler"),
            scheduler_key="alarm_event_upload",
            restart_running=restart_running,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    new_cfg = _scheduler_cfg_from_v3(container.config)
    record_scheduler_config_autostart(
        container,
        path=("features", "alarm_export", "scheduler"),
        scheduler_cfg=new_cfg,
    )
    data = _build_payload(container)
    data.update(
        {
            "message": "告警信息上传调度配置已更新并立即生效" if restart_running else "告警信息上传调度配置已保存",
            "scheduler_config": {key: new_cfg.get(key) for key in sorted(ALLOWED_KEYS)},
        }
    )
    return data
