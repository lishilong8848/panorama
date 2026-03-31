from __future__ import annotations

import copy
from typing import Any, Dict

from fastapi import APIRouter, Request

router = APIRouter(prefix="/api/network", tags=["network"])



def _network_cfg(container) -> Dict[str, Any]:
    common = container.config.get("common", {}) if isinstance(container.config, dict) else {}
    network_cfg = common.get("network_switch", {}) if isinstance(common, dict) else {}
    return network_cfg if isinstance(network_cfg, dict) else {}


def _deployment_role_mode(container) -> str:
    snapshot = container.deployment_snapshot() if hasattr(container, "deployment_snapshot") else {}
    if not isinstance(snapshot, dict):
        snapshot = {}
    text = str(snapshot.get("role_mode", "") or "").strip().lower()
    if text == "hybrid":
        return "switching"
    if text in {"switching", "internal", "external"}:
        return text
    return "switching"


def _build_payload(container) -> dict:
    service = container.wifi_service
    network_cfg = _network_cfg(container)
    if not service:
        return {
            "current_ssid": None,
            "interface_name": "",
            "visible_targets": {"internal": False, "external": False},
            "switch_strategy": "single_machine_switching" if _deployment_role_mode(container) == "switching" else "role_fixed_network",
            "role_mode": _deployment_role_mode(container),
            "hard_recovery_enabled": bool(network_cfg.get("hard_recovery_enabled", True)),
            "is_admin": False,
            "last_switch_report": {},
        }

    return {
        "current_ssid": service.current_ssid(),
        "interface_name": service.current_interface_name(),
        "visible_targets": service.visible_targets(),
        "switch_strategy": "single_machine_switching" if _deployment_role_mode(container) == "switching" else "role_fixed_network",
        "role_mode": _deployment_role_mode(container),
        "hard_recovery_enabled": bool(network_cfg.get("hard_recovery_enabled", True)),
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
    message = (
        "单机切网端固定按切网流程执行，已不再提供自动切网开关。"
        if role_mode == "switching"
        else "当前角色为内网端/外网端，不使用单机切网开关。"
    )
    container.add_system_log(f"[网络配置] 自动切网开关接口已退役: 角色={role_mode}")

    return {
        "ok": True,
        "enabled": role_mode == "switching",
        "retired": True,
        "message": message,
        "network": _build_payload(container),
    }
