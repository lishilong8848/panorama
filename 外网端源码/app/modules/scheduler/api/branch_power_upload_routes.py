from __future__ import annotations

import copy
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.config.settings_loader import save_settings
from app.modules.scheduler.api._config_persistence import (
    persist_scheduler_toggle,
    record_scheduler_config_autostart,
)


router = APIRouter(prefix="/api/scheduler/branch-power-upload", tags=["scheduler-branch-power-upload"])


ALLOWED_KEYS = {
    "enabled",
    "auto_start_in_gui",
    "minute_offset",
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
    upload_cfg = features.get("branch_power_upload", {})
    if not isinstance(upload_cfg, dict):
        return {}
    scheduler_cfg = upload_cfg.get("scheduler", {})
    return dict(scheduler_cfg) if isinstance(scheduler_cfg, dict) else {}


def _build_payload(container, action_result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    snapshot = container.branch_power_upload_scheduler_status()
    return {
        "ok": True,
        "action": action_result or {},
        "enabled": bool(snapshot.get("enabled", False)),
        "running": bool(snapshot.get("running", False)),
        "status": str(snapshot.get("status", "未初始化")),
        "next_run_time": str(snapshot.get("next_run_time", "")),
        "interval_minutes": int(snapshot.get("interval_minutes", 0) or 0),
        "minute_offset": int(snapshot.get("minute_offset", 0) or 0),
        "check_interval_sec": int(snapshot.get("check_interval_sec", 0) or 0),
        "last_check_at": str(snapshot.get("last_check_at", "")),
        "last_decision": str(snapshot.get("last_decision", "")),
        "last_trigger_at": str(snapshot.get("last_trigger_at", "")),
        "last_trigger_result": str(snapshot.get("last_trigger_result", "")),
        "state_path": str(snapshot.get("state_path", "")),
        "state_exists": bool(snapshot.get("state_exists", False)),
        "executor_bound": bool(container.is_branch_power_upload_scheduler_executor_bound()),
        "callback_name": container.branch_power_upload_scheduler_executor_name(),
        "remembered_enabled": bool(snapshot.get("remembered_enabled", False)),
        "effective_auto_start_in_gui": bool(snapshot.get("effective_auto_start_in_gui", False)),
        "memory_source": str(snapshot.get("memory_source", "") or ""),
    }


@router.post("/start")
def branch_power_upload_scheduler_start(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "branch_power_upload", "scheduler"), auto_start_in_gui=True)
    action = container.start_branch_power_upload_scheduler()
    return _build_payload(container, action_result=action)


@router.post("/stop")
def branch_power_upload_scheduler_stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "branch_power_upload", "scheduler"), auto_start_in_gui=False)
    action = container.stop_branch_power_upload_scheduler()
    return _build_payload(container, action_result=action)


@router.get("/status")
def branch_power_upload_scheduler_status(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return _build_payload(container)


@router.post("/config")
def branch_power_upload_scheduler_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是JSON对象")

    container = request.app.state.container
    merged = copy.deepcopy(container.config)
    features = merged.get("features")
    if not isinstance(features, dict):
        features = {}
        merged["features"] = features
    upload_cfg = features.get("branch_power_upload")
    if not isinstance(upload_cfg, dict):
        upload_cfg = {}
        features["branch_power_upload"] = upload_cfg
    scheduler_cfg = upload_cfg.get("scheduler")
    if not isinstance(scheduler_cfg, dict):
        scheduler_cfg = {}
        upload_cfg["scheduler"] = scheduler_cfg

    for key in ALLOWED_KEYS:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"enabled", "auto_start_in_gui", "retry_failed_on_next_tick"}:
            scheduler_cfg[key] = bool(value)
        elif key in {"check_interval_sec", "minute_offset"}:
            try:
                number = int(value)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail=f"{key} 必须是整数") from exc
            if key == "minute_offset":
                if number < 0:
                    raise HTTPException(status_code=400, detail="minute_offset 必须大于等于0")
            elif number < 1:
                raise HTTPException(status_code=400, detail=f"{key} 必须大于等于1")
            scheduler_cfg[key] = number
        elif key == "state_file":
            text = str(value or "").strip()
            if not text:
                raise HTTPException(status_code=400, detail="state_file 不能为空")
            scheduler_cfg[key] = text

    scheduler_cfg["interval_minutes"] = 1440

    try:
        saved = save_settings(merged, container.config_path)
        container.reload_config(saved)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    new_cfg = _scheduler_cfg_from_v3(container.config)
    record_scheduler_config_autostart(
        container,
        path=("features", "branch_power_upload", "scheduler"),
        scheduler_cfg=new_cfg,
    )
    data = _build_payload(container)
    data.update(
        {
            "message": "自动上传支路功率调度配置已更新并热重载",
            "scheduler_config": {key: new_cfg.get(key) for key in sorted(ALLOWED_KEYS)},
        }
    )
    return data
