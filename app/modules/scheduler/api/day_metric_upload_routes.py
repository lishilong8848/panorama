from __future__ import annotations

import copy
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.config.settings_loader import save_settings


router = APIRouter(prefix="/api/scheduler/day-metric-upload", tags=["scheduler-day-metric-upload"])


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
    upload_cfg = features.get("day_metric_upload", {})
    if not isinstance(upload_cfg, dict):
        return {}
    scheduler_cfg = upload_cfg.get("scheduler", {})
    return dict(scheduler_cfg) if isinstance(scheduler_cfg, dict) else {}


def _build_payload(container, action_result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    snapshot = container.day_metric_upload_scheduler_status()
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
        "executor_bound": bool(container.is_day_metric_upload_scheduler_executor_bound()),
        "callback_name": container.day_metric_upload_scheduler_executor_name(),
    }


@router.post("/start")
def day_metric_upload_scheduler_start(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    action = container.start_day_metric_upload_scheduler()
    return _build_payload(container, action_result=action)


@router.post("/stop")
def day_metric_upload_scheduler_stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    action = container.stop_day_metric_upload_scheduler()
    return _build_payload(container, action_result=action)


@router.get("/status")
def day_metric_upload_scheduler_status(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return _build_payload(container)


@router.post("/config")
def day_metric_upload_scheduler_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是JSON对象")

    container = request.app.state.container
    merged = copy.deepcopy(container.config)
    features = merged.get("features")
    if not isinstance(features, dict):
        features = {}
        merged["features"] = features
    upload_cfg = features.get("day_metric_upload")
    if not isinstance(upload_cfg, dict):
        upload_cfg = {}
        features["day_metric_upload"] = upload_cfg
    scheduler_cfg = upload_cfg.get("scheduler")
    if not isinstance(scheduler_cfg, dict):
        scheduler_cfg = {}
        upload_cfg["scheduler"] = scheduler_cfg

    for key in ALLOWED_KEYS:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"enabled", "auto_start_in_gui"}:
            scheduler_cfg[key] = bool(value)
        elif key == "run_time":
            text = str(value or "").strip()
            if not text:
                raise HTTPException(status_code=400, detail="run_time 不能为空")
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
    data = _build_payload(container)
    data.update(
        {
            "message": "12项独立上传调度配置已更新并热重载",
            "scheduler_config": {key: new_cfg.get(key) for key in sorted(ALLOWED_KEYS)},
        }
    )
    return data
