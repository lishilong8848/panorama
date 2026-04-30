from __future__ import annotations

import re
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.modules.scheduler.api._config_persistence import (
    persist_scheduler_toggle,
    record_scheduler_config_autostart,
    save_handover_common_scheduler_patch,
)


router = APIRouter(prefix="/api/scheduler/monthly-change-report", tags=["scheduler-monthly-change-report"])


ALLOWED_KEYS = {
    "enabled",
    "auto_start_in_gui",
    "day_of_month",
    "run_time",
    "check_interval_sec",
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
    handover = features.get("handover_log", {})
    if not isinstance(handover, dict):
        return {}
    monthly_cfg = handover.get("monthly_change_report", {})
    if not isinstance(monthly_cfg, dict):
        return {}
    scheduler_cfg = monthly_cfg.get("scheduler", {})
    return dict(scheduler_cfg) if isinstance(scheduler_cfg, dict) else {}


def _build_payload(container, action_result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    snapshot = container.monthly_change_report_scheduler_status()
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
        "executor_bound": bool(container.is_monthly_change_report_scheduler_executor_bound()),
        "callback_name": container.monthly_change_report_scheduler_executor_name(),
        "remembered_enabled": bool(snapshot.get("remembered_enabled", False)),
        "effective_auto_start_in_gui": bool(snapshot.get("effective_auto_start_in_gui", False)),
        "memory_source": str(snapshot.get("memory_source", "") or ""),
    }


@router.post("/start")
def monthly_change_report_scheduler_start(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "handover_log", "monthly_change_report", "scheduler"), auto_start_in_gui=True)
    action = container.start_monthly_change_report_scheduler()
    return _build_payload(container, action_result=action)


@router.post("/stop")
def monthly_change_report_scheduler_stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(container, path=("features", "handover_log", "monthly_change_report", "scheduler"), auto_start_in_gui=False)
    action = container.stop_monthly_change_report_scheduler()
    return _build_payload(container, action_result=action)


@router.get("/status")
def monthly_change_report_scheduler_status(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return _build_payload(container)


@router.post("/config")
def monthly_change_report_scheduler_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是JSON对象")

    container = request.app.state.container
    old_cfg = _scheduler_cfg_from_v3(container.config)
    old_day = int(old_cfg.get("day_of_month", 0) or 0)
    old_run_time = str(old_cfg.get("run_time", "") or "").strip()

    scheduler_patch: Dict[str, Any] = {}
    for key in ALLOWED_KEYS:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"enabled", "auto_start_in_gui"}:
            scheduler_patch[key] = bool(value)
        elif key in {"day_of_month", "check_interval_sec"}:
            try:
                number = int(value)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"{key} 必须是整数") from exc
            if key == "day_of_month" and (number < 1 or number > 31):
                raise HTTPException(status_code=400, detail="day_of_month 必须在 1 到 31 之间")
            if key == "check_interval_sec" and number <= 0:
                raise HTTPException(status_code=400, detail="check_interval_sec 必须大于 0")
            scheduler_patch[key] = number
        elif key == "run_time":
            text = str(value or "").strip()
            if not _valid_time(text):
                raise HTTPException(status_code=400, detail="run_time 必须是 HH:MM:SS")
            scheduler_patch[key] = text
        elif key == "state_file":
            text = str(value or "").strip()
            if not text:
                raise HTTPException(status_code=400, detail="state_file 不能为空")
            scheduler_patch[key] = text

    try:
        patch_result = save_handover_common_scheduler_patch(
            container,
            path=("features", "handover_log", "monthly_change_report", "scheduler"),
            scheduler_patch=scheduler_patch,
            source="月度变更统计表调度配置保存",
        )
        saved = patch_result["saved_config"]
        container.reload_config(saved)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    new_cfg = _scheduler_cfg_from_v3(container.config)
    record_scheduler_config_autostart(
        container,
        path=("features", "handover_log", "monthly_change_report", "scheduler"),
        scheduler_cfg=new_cfg,
    )
    new_day = int(new_cfg.get("day_of_month", 0) or 0)
    new_run_time = str(new_cfg.get("run_time", "") or "").strip()
    schedule_changed = old_day != new_day or old_run_time != new_run_time
    reset_result: Dict[str, Any] = {}
    if schedule_changed and container.monthly_change_report_scheduler:
        reset_result = container.monthly_change_report_scheduler.reset_current_month_state_for_schedule_change()
    status_payload = _build_payload(container)
    data = dict(status_payload)
    data.update(
        {
            "message": "月度变更统计表调度配置已更新并热重载",
            "schedule_changed": schedule_changed,
            "state_reset": reset_result,
            "scheduler_status": status_payload,
            "scheduler_config": {key: new_cfg.get(key) for key in sorted(ALLOWED_KEYS)},
            "updated_at": str(patch_result.get("document", {}).get("updated_at", "") or ""),
            "segment_revision": int(patch_result.get("document", {}).get("revision", 0) or 0),
            "changed": bool(patch_result.get("changed", False)),
        }
    )
    return data
