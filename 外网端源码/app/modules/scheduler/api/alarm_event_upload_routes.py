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


router = APIRouter(prefix="/api/scheduler/alarm-event-upload", tags=["scheduler-alarm-event-upload"])


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
    return {
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


@router.post("/start")
def alarm_event_upload_scheduler_start(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "alarm_export", "scheduler"), auto_start_in_gui=True)
    action = container.start_alarm_event_upload_scheduler()
    return _build_payload(container, action_result=action)


@router.post("/stop")
def alarm_event_upload_scheduler_stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "alarm_export", "scheduler"), auto_start_in_gui=False)
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
    old_cfg = _scheduler_cfg_from_v3(container.config)
    old_run_time = str(old_cfg.get("run_time", "") or "").strip()

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
        elif key == "check_interval_sec":
            try:
                number = int(value)
            except Exception as exc:
                raise HTTPException(status_code=400, detail="check_interval_sec 必须是整数") from exc
            if number <= 0:
                raise HTTPException(status_code=400, detail="check_interval_sec 必须大于 0")
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
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    new_cfg = _scheduler_cfg_from_v3(container.config)
    record_scheduler_config_autostart(
        container,
        path=("features", "alarm_export", "scheduler"),
        scheduler_cfg=new_cfg,
    )
    new_run_time = str(new_cfg.get("run_time", "") or "").strip()
    run_time_changed = bool(old_run_time and new_run_time and old_run_time != new_run_time)
    reset_result: Dict[str, Any] = {}
    if run_time_changed and container.alarm_event_upload_scheduler:
        reset_result = container.alarm_event_upload_scheduler.reset_today_state_for_run_time_change()
    status_payload = _build_payload(container)
    data = dict(status_payload)
    data.update(
        {
            "message": "告警信息上传调度配置已更新并热重载",
            "run_time_changed": run_time_changed,
            "state_reset": reset_result,
            "scheduler_status": status_payload,
            "scheduler_config": {key: new_cfg.get(key) for key in sorted(ALLOWED_KEYS)},
            "updated_at": "",
        }
    )
    return data
