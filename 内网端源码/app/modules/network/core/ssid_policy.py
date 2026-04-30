from __future__ import annotations

from typing import Any, Dict


def get_external_ssid(config: Dict[str, Any]) -> str:
    return str(config.get("network", {}).get("external_ssid", "")).strip()


def get_internal_ssid(config: Dict[str, Any]) -> str:
    return str(config.get("network", {}).get("internal_ssid", "")).strip()
