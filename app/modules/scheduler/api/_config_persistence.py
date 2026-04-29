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
        recorder = getattr(container, "record_external_scheduler_toggle", None)
        if callable(recorder):
            try:
                recorder(
                    path=tuple(str(item) for item in path),
                    auto_start_in_gui=desired_auto_start,
                    source="调度开关",
                )
            except Exception:  # noqa: BLE001
                pass
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def record_scheduler_config_autostart(container: Any, *, path: Sequence[str], scheduler_cfg: Dict[str, Any]) -> None:
    if not isinstance(scheduler_cfg, dict) or "auto_start_in_gui" not in scheduler_cfg:
        return
    recorder = getattr(container, "record_external_scheduler_toggle", None)
    if not callable(recorder):
        return
    try:
        recorder(
            path=tuple(str(item) for item in path),
            auto_start_in_gui=bool(scheduler_cfg.get("auto_start_in_gui", False)),
            source="调度配置保存",
        )
    except Exception:  # noqa: BLE001
        pass
