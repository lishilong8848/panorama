from __future__ import annotations

import re
from ipaddress import ip_address
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from app.modules.inspection_data.service.live_requests import (
    LiveRequestConflict, LiveRequestStore, linked_mode,
)

router = APIRouter(prefix="/api/scheduler/inspection-data-upload", tags=["scheduler-inspection-data-upload"])


def _lan_address(host):
    try:
        address = ip_address(host)
    except ValueError:
        return False
    return address.is_private and not (address.is_unspecified or address.is_reserved or address.is_multicast)


def _direct_lan(request):
    return (
        request.client is not None and _lan_address(request.client.host)
        and (request.url.hostname == "localhost" or _lan_address(request.url.hostname))
        and not any(name in request.headers for name in (
            "forwarded", "x-forwarded-for", "x-forwarded-host", "x-real-ip",
        ))
    )


def _settings_container(request):
    container = request.app.state.container
    if container._configured_deployment_snapshot().get("role_mode") != "external":
        raise HTTPException(409, "请在外网端配置巡检联动")
    if not _direct_lan(request) or request.headers.get("x-inspection-console") != "1":
        raise HTTPException(403, "请从外网端控制台操作")
    return container


def _settings_response(config):
    return JSONResponse({"enabled": linked_mode(config)}, headers={"Cache-Control": "private, no-store"})


@router.get("/live-settings")
def get_live_settings(request: Request):
    return _settings_response(_settings_container(request).runtime_config)


@router.put("/live-settings")
def save_live_settings(payload: dict, request: Request):
    container = _settings_container(request)
    enabled = payload.get("enabled")
    if set(payload) != {"enabled"} or type(enabled) is not bool:
        raise HTTPException(400, "请选择是否启用巡检联动")
    patch = {"features": {"inspection_data_upload": {"lighthouse_bridge": {"enabled": enabled}}}}
    try:
        service = container.ensure_config_runtime_service()
        saved = service.update_config_patch(patch, source="巡检联动设置")
        container.apply_config_snapshot(saved, mode="light")
    except Exception:
        raise HTTPException(503, "巡检联动设置保存未确认，请重新读取") from None
    return _settings_response(container.runtime_config)


def failure(status, error, request_id=None, building=None):
    return JSONResponse(status_code=status, content={
        "ok": False, "request_id": request_id, "building": building, "state": "failed",
        "source_time": None, "uploaded_at": None, "job_id": None, "error": error,
    })


def _container(request):
    container = request.app.state.container
    if not _direct_lan(request) or "origin" in request.headers or not linked_mode(container.runtime_config):
        return None
    if (container._configured_deployment_snapshot().get("role_mode") != "external"
            or not container.runtime_config.get("inspection_data_upload", {}).get("enabled", True)):
        return None
    return container


@router.post("/live")
def post_live(payload: dict, request: Request):
    container = _container(request)
    if container is None:
        return failure(403, "巡检联动未启用或请求未通过局域网直连")
    try:
        if set(payload) != {"request_id", "building", "not_before"}:
            raise ValueError("仅接受 request_id、building、not_before")
        result, created = LiveRequestStore(container.runtime_config).submit(
            payload["request_id"], payload["building"], payload["not_before"])
    except LiveRequestConflict:
        return failure(409, "同一 request_id 的楼栋和采样要求不能变更")
    except ValueError as exc:
        return failure(400, str(exc))
    except Exception:
        return failure(503, "巡检请求暂时无法保存，请使用相同 request_id 重试")
    try:
        container.dispatch_inspection_live_requests()
    except Exception:
        pass  # Persisted request remains pending for the existing watchdog.
    return JSONResponse(status_code=202 if created else 200,
                        content=LiveRequestStore(container.runtime_config).get(payload["request_id"]))


@router.get("/live/{request_id}")
def get_live(request_id: str, request: Request):
    container = _container(request)
    if container is None:
        return failure(403, "巡检联动未启用或请求未通过局域网直连")
    if not re.fullmatch(r"[0-9a-f]{32}", request_id):
        return failure(400, "request_id 必须为小写 uuidhex")
    try:
        result = LiveRequestStore(container.runtime_config).get(request_id)
    except Exception:
        return failure(503, "巡检状态暂时不可用，请保留原 request_id 重试")
    return result if result else failure(404, "未找到该巡检请求", request_id=request_id)
