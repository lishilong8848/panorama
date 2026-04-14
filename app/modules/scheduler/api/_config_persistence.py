from __future__ import annotations

import copy
from typing import Any, Dict, Sequence

from fastapi import HTTPException

from app.config.settings_loader import save_settings


def _ensure_dict_path(root: Dict[str, Any], path: Sequence[str]) -> Dict[str, Any]:
    current = root
    for key in path:
        next_value = current.get(key)
        if not isinstance(next_value, dict):
            next_value = {}
            current[key] = next_value
        current = next_value
    return current


def persist_scheduler_toggle(container: Any, *, path: Sequence[str], auto_start_in_gui: bool) -> None:
    try:
        merged = copy.deepcopy(container.config if isinstance(container.config, dict) else {})
        scheduler_cfg = _ensure_dict_path(merged, path)
        desired_auto_start = bool(auto_start_in_gui)
        scheduler_cfg["auto_start_in_gui"] = desired_auto_start
        if desired_auto_start:
            scheduler_cfg["enabled"] = True
        saved = save_settings(merged, container.config_path)
        container.reload_config(saved)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
