from __future__ import annotations

from typing import Any, Dict, Iterable

from app.modules.report_pipeline.service.scheduler_state_presenter import (
    present_scheduler_snapshot_with_display,
)


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _container_role_mode(container: Any) -> str:
    runtime_config = _dict(getattr(container, "runtime_config", {}))
    deployment = _dict(runtime_config.get("deployment"))
    role_mode = str(deployment.get("role_mode", "") or "").strip().lower()
    if role_mode in {"internal", "external"}:
        return role_mode
    config = _dict(getattr(container, "config", {}))
    common = _dict(config.get("common"))
    deployment = _dict(common.get("deployment"))
    role_mode = str(deployment.get("role_mode", "") or "").strip().lower()
    return role_mode if role_mode in {"internal", "external"} else ""


def with_scheduler_display(
    payload: Dict[str, Any],
    container: Any,
    *,
    slot_keys: Iterable[str] = ("morning", "afternoon"),
) -> Dict[str, Any]:
    return present_scheduler_snapshot_with_display(
        payload,
        role_mode=_container_role_mode(container),
        slot_keys=slot_keys,
    )
