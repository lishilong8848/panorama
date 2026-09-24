from __future__ import annotations

import copy
import re
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from app.modules.scheduler.api._config_persistence import (
    persist_full_config,
    persist_scheduler_toggle,
    record_scheduler_config_autostart,
)


router = APIRouter(
    prefix="/api/scheduler/alarm-rule-export-upload",
    tags=["scheduler-alarm-rule-export-upload"],
)
ALLOWED_KEYS = {
    "enabled",
    "auto_start_in_gui",
    "day_of_month",
    "run_time",
    "check_interval_sec",
    "state_file",
}


def _scheduler_cfg(config: Dict[str, Any]) -> Dict[str, Any]:
    features = config.get("features", {}) if isinstance(config, dict) else {}
    upload = features.get("alarm_rule_export_upload", {}) if isinstance(features, dict) else {}
    scheduler = upload.get("scheduler", {}) if isinstance(upload, dict) else {}
    return dict(scheduler) if isinstance(scheduler, dict) else {}


def _payload(container, action: Dict[str, Any] | None = None) -> Dict[str, Any]:
    snapshot = container.alarm_rule_export_upload_scheduler_status()
    return {
        "ok": True,
        "action": action or {},
        **snapshot,
        "executor_bound": bool(container.is_alarm_rule_export_upload_scheduler_executor_bound()),
        "callback_name": container.alarm_rule_export_upload_scheduler_executor_name(),
    }


@router.post("/start")
def start(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(
        container,
        path=("features", "alarm_rule_export_upload", "scheduler"),
        auto_start_in_gui=True,
    )
    return _payload(container, container.start_alarm_rule_export_upload_scheduler())


@router.post("/stop")
def stop(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    persist_scheduler_toggle(
        container,
        path=("features", "alarm_rule_export_upload", "scheduler"),
        auto_start_in_gui=False,
    )
    return _payload(container, container.stop_alarm_rule_export_upload_scheduler())


@router.get("/status")
def status(request: Request) -> Dict[str, Any]:
    return _payload(request.app.state.container)


@router.post("/config")
def save_config(payload: Dict[str, Any], request: Request) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="请求体必须是JSON对象")
    container = request.app.state.container
    merged = copy.deepcopy(container.config)
    features = merged.setdefault("features", {})
    upload = features.setdefault("alarm_rule_export_upload", {})
    scheduler = upload.setdefault("scheduler", {})

    for key in ALLOWED_KEYS:
        if key not in payload:
            continue
        value = payload.get(key)
        if key in {"enabled", "auto_start_in_gui"}:
            scheduler[key] = bool(value)
        elif key in {"day_of_month", "check_interval_sec"}:
            try:
                number = int(value)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"{key} 必须是整数") from exc
            if key == "day_of_month" and not 1 <= number <= 31:
                raise HTTPException(status_code=400, detail="day_of_month 必须在 1 到 31 之间")
            if key == "check_interval_sec" and number <= 0:
                raise HTTPException(status_code=400, detail="check_interval_sec 必须大于 0")
            scheduler[key] = number
        elif key == "run_time":
            text = str(value or "").strip()
            if not re.fullmatch(r"\d{2}:\d{2}:\d{2}", text):
                raise HTTPException(status_code=400, detail="run_time 必须是 HH:MM:SS")
            scheduler[key] = text
        elif key == "state_file":
            text = str(value or "").strip()
            if not text:
                raise HTTPException(status_code=400, detail="state_file 不能为空")
            scheduler[key] = text
    scheduler["catch_up_if_missed"] = False
    scheduler["retry_failed_in_same_period"] = True

    try:
        persist_full_config(container, merged, source="告警规则附件上传调度配置保存", mode="full")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    new_cfg = _scheduler_cfg(container.config)
    record_scheduler_config_autostart(
        container,
        path=("features", "alarm_rule_export_upload", "scheduler"),
        scheduler_cfg=new_cfg,
    )
    result = _payload(container)
    result.update(
        {
            "message": "告警规则附件上传调度配置已更新并热重载",
            "scheduler_status": dict(result),
            "scheduler_config": {key: new_cfg.get(key) for key in sorted(ALLOWED_KEYS)},
        }
    )
    return result
