from __future__ import annotations

import copy

from fastapi import APIRouter, HTTPException, Request

from app.config.inspection_data_config import INSPECTION_DATA_UPLOAD_DEFAULTS, INSPECTION_SOURCE_CONTRACTS, normalize_inspection_scheduler
from app.modules.inspection_data.service.inspection_data_upload_service import InspectionDataUploadService
from app.modules.inspection_data.service.point_settings import PointSettingsConflict, inspection_target_options, load_or_restore_point_settings, load_point_settings, save_point_group
from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient
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


@router.get("/points")
def inspection_point_settings(request: Request):
    return {"ok": True, "settings": _read_point_settings(request.app.state.container.runtime_config)}


def _read_point_settings(config):
    try:
        return load_or_restore_point_settings(config) or {}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"读取巡检测点配置失败：{exc}") from exc


@router.put("/points")
def update_inspection_point_settings(payload: dict, request: Request):
    container = request.app.state.container
    if container._configured_deployment_snapshot().get("role_mode") != "external":
        raise HTTPException(status_code=409, detail="请在外网端设置巡检测点")
    if not isinstance(payload.get("expected_group"), dict):
        raise HTTPException(status_code=409, detail="请重新加载测点配置后再保存")
    _read_point_settings(container.runtime_config)
    try:
        return save_point_group(container.runtime_config, building=payload.get("building"), family=payload.get("family"),
                                points=payload.get("points", {}), remove=payload.get("remove", []),
                                expected_group=payload["expected_group"], status_rules=payload.get("status_rules"),
                                explicitly_enabled=payload.get("explicitly_enabled"), explicitly_disabled=payload.get("explicitly_disabled"))
    except PointSettingsConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (ValueError, OSError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _point_building(building_code: str) -> str:
    if building_code not in {"d", "e"}:
        raise HTTPException(status_code=404, detail="仅 D、E 楼提供巡检测点管理")
    return f"{building_code.upper()}楼"


@router.get("/points/{building_code}")
def building_inspection_point_settings(building_code: str, request: Request):
    building = _point_building(building_code)
    config = request.app.state.container.runtime_config
    return {"ok": True, "building": building, "settings": _read_point_settings(config).get(building, {}),
            "target_options": inspection_target_options(building)}


@router.put("/points/{building_code}")
def update_building_inspection_point_settings(building_code: str, payload: dict, request: Request):
    building = _point_building(building_code)
    container = request.app.state.container
    if container._configured_deployment_snapshot().get("role_mode") != "external":
        raise HTTPException(status_code=409, detail="请在外网端设置巡检测点")
    if not isinstance(payload.get("expected_group"), dict) or set(payload) - {"family", "points", "remove", "expected_group", "targets", "status_rules", "explicitly_enabled", "explicitly_disabled"}:
        raise HTTPException(status_code=400, detail="巡检测点配置格式无效")
    _read_point_settings(container.runtime_config)
    try:
        result = save_point_group(container.runtime_config, building=building, family=payload.get("family"),
                                  points=payload.get("points"), remove=payload.get("remove"), expected_group=payload["expected_group"],
                                  targets=payload.get("targets"), status_rules=payload.get("status_rules"),
                                  explicitly_enabled=payload.get("explicitly_enabled"), explicitly_disabled=payload.get("explicitly_disabled"))
    except PointSettingsConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (ValueError, OSError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {**result, "settings": result["settings"].get(building, {})}


@router.get("/points/{building_code}/latest")
def latest_building_inspection_points(building_code: str, request: Request):
    building = _point_building(building_code)
    config = request.app.state.container.runtime_config
    bridge = InternalBridgeHttpClient.from_runtime_config(config)
    if bridge is None:
        raise HTTPException(status_code=503, detail="未配置内网桥接地址")
    datasets = {}
    settings = (load_point_settings(config) or {}).get(building, {})
    try:
        payload = bridge.get_latest_inspection_points(building=building)
        if not payload.get("ok") or not isinstance(payload.get("datasets"), list):
            raise ValueError("内网巡检测点响应无效")
        for doc in payload["datasets"]:
            family = doc.get("source_family") if isinstance(doc, dict) else None
            if not isinstance(family, str) or family not in INSPECTION_SOURCE_CONTRACTS or doc.get("building") != building or family in datasets:
                raise ValueError("内网巡检测点楼栋或专业不匹配")
            bucket = doc.get("bucket_key", "")
            if not isinstance(bucket, str):
                raise ValueError("内网巡检测点时段无效")
            InspectionDataUploadService._validate_dataset(doc, bucket[:10], bucket)
            rows = InspectionDataUploadService._point_detail_rows(
                doc, INSPECTION_SOURCE_CONTRACTS[family]["discipline"], {},
                status_rules=settings.get(family, {}).get("status_rules", {}))
            datasets[family] = {"sample_time": doc["value_time"], "points": [
                {"id": row["测点ID"], "name": row["测点名称"], "path": row["测点路径"], "value": row["测点值"],
                 "raw_value": row["测点原始值"], "status": row.get("状态判定", "")}
                for row in rows if row["测点ID"]
            ]}
    except (RuntimeError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"ok": True, "building": building, "datasets": datasets}


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
