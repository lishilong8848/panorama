from __future__ import annotations

import copy
import os
from typing import Any, Dict

from app.config.config_adapter import normalize_role_mode


FORCE_ROLE_MODE_ENV = "QJPT_FORCE_ROLE_MODE"


def forced_role_mode_from_env() -> str:
    return normalize_role_mode(os.environ.get(FORCE_ROLE_MODE_ENV, ""))


def role_mode_label(role_mode: str) -> str:
    normalized = normalize_role_mode(role_mode)
    if normalized == "internal":
        return "内网端"
    if normalized == "external":
        return "外网端"
    return ""


def apply_forced_role_mode(cfg: Dict[str, Any], *, force_role_mode: str | None = None) -> Dict[str, Any]:
    normalized = normalize_role_mode(force_role_mode if force_role_mode is not None else forced_role_mode_from_env())
    if normalized not in {"internal", "external"}:
        return copy.deepcopy(cfg if isinstance(cfg, dict) else {})

    payload = copy.deepcopy(cfg if isinstance(cfg, dict) else {})
    common = payload.setdefault("common", {})
    if not isinstance(common, dict):
        common = {}
        payload["common"] = common
    deployment = common.setdefault("deployment", {})
    if not isinstance(deployment, dict):
        deployment = {}
        common["deployment"] = deployment
    deployment["role_mode"] = normalized
    deployment["last_started_role_mode"] = normalized
    deployment["node_label"] = role_mode_label(normalized)
    return payload
