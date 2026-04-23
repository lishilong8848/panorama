from __future__ import annotations

import copy
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.config.settings_loader import save_settings
from app.modules.scheduler.api._config_persistence import (
    persist_scheduler_toggle,
    record_scheduler_config_autostart,
)
from app.modules.scheduler.api._display_payload import with_scheduler_display
from app.modules.scheduler.api._time_normalization import normalize_scheduler_time


router = APIRouter(prefix="/api/scheduler/monthly-event-report", tags=["scheduler-monthly-event-report"])


ALLOWED_KEYS = {
    "enabled",
    "auto_start_in_gui",
    "day_of_month",
    "run_time",
    "check_interval_sec",
    "state_file",
}


def _scheduler_cfg_from_v3(config: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(config, dict):
        return {}
    features = config.get("features", {})
    if not isinstance(features, dict):
        return {}
    handover = features.get("handover_log", {})
    if not isinstance(handover, dict):
        return {}
    monthly_cfg = handover.get("monthly_event_report", {})
    if not isinstance(monthly_cfg, dict):
        return {}
    scheduler_cfg = monthly_cfg.get("scheduler", {})
    return dict(scheduler_cfg) if isinstance(scheduler_cfg, dict) else {}


def _build_payload(container, action_result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    snapshot = container.monthly_event_report_scheduler_status()
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
        "executor_bound": bool(container.is_monthly_event_report_scheduler_executor_bound()),
        "callback_name": container.monthly_event_report_scheduler_executor_name(),
        "remembered_enabled": bool(snapshot.get("remembered_enabled", False)),
        "effective_auto_start_in_gui": bool(snapshot.get("effective_auto_start_in_gui", False)),
        "memory_source": str(snapshot.get("memory_source", "") or ""),
    }
    return with_scheduler_display(payload, container, slot_keys=())


@router.post("/start")
def monthly_event_report_scheduler_start(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "handover_log", "monthly_event_report", "scheduler"), auto_start_in_gui=True)
    action = container.start_monthly_event_report_scheduler()
    return _build_payload(container, action_result=action)


@router.post("/stop")
def monthly_event_report_scheduler_stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "handover_log", "monthly_event_report", "scheduler"), auto_start_in_gui=False)
    action = container.stop_monthly_event_report_scheduler()
    return _build_payload(container, action_result=action)


@router.get("/status")
def monthly_event_report_scheduler_status(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return _build_payload(container)


@router.post("/config")
def monthly_event_report_scheduler_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是JSON对象")

    container = request.app.state.container
    merged = copy.deepcopy(container.config)
    features = merged.get("features")
    if not isinstance(features, dict):
        features = {}
        merged["features"] = features
    handover = features.get("handover_log")
    if not isinstance(handover, dict):
        handover = {}
        features["handover_log"] = handover
    monthly_cfg = handover.get("monthly_event_report")
    if not isinstance(monthly_cfg, dict):
        monthly_cfg = {}
        handover["monthly_event_report"] = monthly_cfg
    scheduler_cfg = monthly_cfg.get("scheduler")
    if not isinstance(scheduler_cfg, dict):
        scheduler_cfg = {}
        monthly_cfg["scheduler"] = scheduler_cfg

    for key in ALLOWED_KEYS:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"enabled", "auto_start_in_gui"}:
            scheduler_cfg[key] = bool(value)
        elif key in {"day_of_month", "check_interval_sec"}:
            try:
                number = int(value)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"{key} 必须是整数") from exc
            if key == "day_of_month" and (number < 1 or number > 31):
                raise HTTPException(status_code=400, detail="day_of_month 必须在 1 到 31 之间")
            if key == "check_interval_sec" and number <= 0:
                raise HTTPException(status_code=400, detail="check_interval_sec 必须大于 0")
            scheduler_cfg[key] = number
        elif key == "run_time":
            scheduler_cfg[key] = normalize_scheduler_time(value)
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
        path=("features", "handover_log", "monthly_event_report", "scheduler"),
        scheduler_cfg=new_cfg,
    )
    data = _build_payload(container)
    data.update(
        {
            "message": "月度事件统计表调度配置已更新并热重载",
            "scheduler_config": {key: new_cfg.get(key) for key in sorted(ALLOWED_KEYS)},
        }
    )
    return data
