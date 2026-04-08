from __future__ import annotations

import copy
from typing import Any, Dict

from fastapi import APIRouter, Request

from app.config.config_adapter import normalize_role_mode

router = APIRouter(prefix="/api/network", tags=["network"])

def _deployment_role_mode(container) -> str:
    snapshot = container.deployment_snapshot() if hasattr(container, "deployment_snapshot") else {}
    if not isinstance(snapshot, dict):
        return ""
    return normalize_role_mode(snapshot.get("role_mode"))


def _build_payload(container) -> dict:
    service = container.wifi_service
    if not service:
        return {
            "current_ssid": None,
            "interface_name": "",
            "visible_targets": {"internal": False, "external": False},
            "switch_strategy": "role_fixed_network",
            "role_mode": _deployment_role_mode(container),
            "hard_recovery_enabled": False,
            "is_admin": False,
            "last_switch_report": {"enabled": False, "message": "网络切换功能已移除"},
        }

    return {
        "current_ssid": service.current_ssid(),
        "interface_name": service.current_interface_name(),
        "visible_targets": service.visible_targets(),
        "switch_strategy": "role_fixed_network",
        "role_mode": _deployment_role_mode(container),
        "hard_recovery_enabled": False,
        "is_admin": service.is_admin(),
        "last_switch_report": service.get_last_switch_report(),
    }


@router.get("/status")
def network_status(request: Request) -> dict:
    container = request.app.state.container
    return _build_payload(container)


@router.get("/current")
def network_current(request: Request) -> dict:
    container = request.app.state.container
    return _build_payload(container)


@router.post("/auto-switch")
def set_auto_switch(payload: dict, request: Request) -> dict:
    container = request.app.state.container
    role_mode = _deployment_role_mode(container)
    message = "当前仅保留内网端/外网端双角色，不再提供自动切网开关。"
    container.add_system_log(f"[网络配置] 自动切网开关接口已退役: 角色={role_mode}")

    return {
        "ok": True,
        "enabled": False,
        "retired": True,
        "message": message,
        "network": _build_payload(container),
    }
