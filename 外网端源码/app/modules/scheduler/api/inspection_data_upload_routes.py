from __future__ import annotations

import copy

from fastapi import APIRouter, HTTPException, Request

from app.config.inspection_data_config import INSPECTION_DATA_UPLOAD_DEFAULTS, normalize_inspection_scheduler
from app.modules.scheduler.api._config_persistence import (
    persist_full_config,
    persist_scheduler_toggle,
    record_scheduler_config_autostart,
)


router = APIRouter(prefix="/api/scheduler/inspection-data-upload", tags=["scheduler-inspection-data-upload"])
CONFIG_PATH = ("features", "inspection_data_upload", "scheduler")
ALLOWED_KEYS = set(INSPECTION_DATA_UPLOAD_DEFAULTS["scheduler"])


def _payload(container, action=None):
    return {"ok": True, "action": action or {}, **container.inspection_data_upload_scheduler_status()}


@router.get("/status")
def inspection_data_upload_scheduler_status(request: Request):
    return _payload(request.app.state.container)


@router.post("/start")
def inspection_data_upload_scheduler_start(request: Request):
    container = request.app.state.container
    if container._configured_deployment_snapshot().get("role_mode") != "external":
        raise HTTPException(status_code=409, detail="请在外网端启动巡检调度")
    feature = container.runtime_config.get("inspection_data_upload", {})
    if not feature.get("enabled", True):
        raise HTTPException(status_code=409, detail="巡检数据处理模块已关闭")
    try:
        cfg = normalize_inspection_scheduler(feature.get("scheduler", {}))
        if not cfg["run_times"]:
            raise ValueError("请先配置至少一个巡检调度时间")
        persist_scheduler_toggle(container, path=CONFIG_PATH, auto_start_in_gui=True)
        container.rebuild_inspection_data_upload_scheduler(source="调度启动")
        action = container.start_inspection_data_upload_scheduler()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _payload(container, action)


@router.post("/stop")
def inspection_data_upload_scheduler_stop(request: Request):
    container = request.app.state.container
    action = container.stop_inspection_data_upload_scheduler()
    persist_scheduler_toggle(container, path=CONFIG_PATH, auto_start_in_gui=False)
    return _payload(container, action)


@router.post("/config")
def inspection_data_upload_scheduler_config(payload: dict, request: Request):
    if set(payload) - ALLOWED_KEYS:
        raise HTTPException(status_code=400, detail="巡检调度配置包含不支持的字段")
    container = request.app.state.container
    merged = copy.deepcopy(container.config)
    feature = merged.setdefault("features", {}).setdefault("inspection_data_upload", {})
    try:
        cfg = normalize_inspection_scheduler({**feature.get("scheduler", {}), **payload})
        feature["scheduler"] = cfg
        persist_full_config(container, merged, source="巡检数据处理调度配置保存", mode="light")
        record_scheduler_config_autostart(container, path=CONFIG_PATH, scheduler_cfg=cfg)
        reload_result = container.rebuild_inspection_data_upload_scheduler(source="调度配置保存")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    status = _payload(container)
    return {**status, "scheduler_status": status, "scheduler_config": cfg,
            "scheduler_reload": reload_result, "message": "巡检数据处理调度配置已更新"}
