from __future__ import annotations

import copy
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.config.settings_loader import save_settings
from app.modules.scheduler.api._config_persistence import persist_scheduler_toggle


router = APIRouter(prefix="/api/scheduler/wet-bulb-collection", tags=["scheduler-wet-bulb-collection"])


ALLOWED_KEYS = {
    "enabled",
    "auto_start_in_gui",
    "interval_minutes",
    "check_interval_sec",
    "retry_failed_on_next_tick",
    "state_file",
}


def _scheduler_cfg_from_v3(config: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(config, dict):
        return {}
    features = config.get("features", {})
    if not isinstance(features, dict):
        return {}
    wet_cfg = features.get("wet_bulb_collection", {})
    if not isinstance(wet_cfg, dict):
        return {}
    scheduler_cfg = wet_cfg.get("scheduler", {})
    payload = dict(scheduler_cfg) if isinstance(scheduler_cfg, dict) else {}
    return payload


def _build_payload(container, action_result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    snapshot = container.wet_bulb_collection_scheduler_status()
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
        "executor_bound": bool(container.is_wet_bulb_collection_scheduler_executor_bound()),
        "callback_name": container.wet_bulb_collection_scheduler_executor_name(),
    }


@router.post("/start")
def wet_bulb_scheduler_start(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "wet_bulb_collection", "scheduler"), auto_start_in_gui=True)
    action = container.start_wet_bulb_collection_scheduler()
    return _build_payload(container, action_result=action)


@router.post("/stop")
def wet_bulb_scheduler_stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "wet_bulb_collection", "scheduler"), auto_start_in_gui=False)
    action = container.stop_wet_bulb_collection_scheduler()
    return _build_payload(container, action_result=action)


@router.get("/status")
def wet_bulb_scheduler_status(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return _build_payload(container)


@router.post("/config")
def wet_bulb_scheduler_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是JSON对象")

    container = request.app.state.container
    merged = copy.deepcopy(container.config)
    features = merged.get("features")
    if not isinstance(features, dict):
        features = {}
        merged["features"] = features
    wet_cfg = features.get("wet_bulb_collection")
    if not isinstance(wet_cfg, dict):
        wet_cfg = {}
        features["wet_bulb_collection"] = wet_cfg
    scheduler_cfg = wet_cfg.get("scheduler")
    if not isinstance(scheduler_cfg, dict):
        scheduler_cfg = {}
        wet_cfg["scheduler"] = scheduler_cfg
    for key in ALLOWED_KEYS:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"enabled", "auto_start_in_gui", "retry_failed_on_next_tick"}:
            scheduler_cfg[key] = bool(value)
        elif key in {"interval_minutes", "check_interval_sec"}:
            try:
                number = int(value)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"{key} 必须是整数") from exc
            if number <= 0:
                raise HTTPException(status_code=400, detail=f"{key} 必须大于0")
            scheduler_cfg[key] = number
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
    data = _build_payload(container)
    data.update(
        {
            "message": "湿球温度定时采集调度配置已更新并热重载",
            "scheduler_config": {key: new_cfg.get(key) for key in sorted(ALLOWED_KEYS)},
        }
    )
    return data
