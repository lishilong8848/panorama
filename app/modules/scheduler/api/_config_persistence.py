from __future__ import annotations

import copy
from typing import Any, Dict, Sequence

from fastapi import HTTPException

from app.config.settings_loader import (
    get_handover_common_segment,
    load_settings,
    save_handover_common_segment,
    save_settings,
)


_HANDOVER_PREFIX: tuple[str, str] = ("features", "handover_log")
_SCHEDULER_PATH_KEY_MAP: dict[tuple[str, ...], str] = {
    ("common", "scheduler"): "auto_flow",
    ("features", "handover_log", "scheduler"): "handover",
    ("features", "wet_bulb_collection", "scheduler"): "wet_bulb_collection",
    ("features", "day_metric_upload", "scheduler"): "day_metric_upload",
    ("features", "alarm_export", "scheduler"): "alarm_event_upload",
    ("features", "handover_log", "monthly_event_report", "scheduler"): "monthly_event_report",
    ("features", "handover_log", "monthly_change_report", "scheduler"): "monthly_change_report",
}


def _ensure_dict_path(root: Dict[str, Any], path: Sequence[str]) -> Dict[str, Any]:
    current = root
    for key in path:
        next_value = current.get(key)
        if not isinstance(next_value, dict):
            next_value = {}
            current[key] = next_value
        current = next_value
    return current


def _get_dict_path(root: Dict[str, Any], path: Sequence[str]) -> Dict[str, Any]:
    current: Any = root
    for key in path:
        if not isinstance(current, dict):
            return {}
        current = current.get(key)
    return copy.deepcopy(current) if isinstance(current, dict) else {}


def _is_handover_common_scheduler_path(path: Sequence[str]) -> bool:
    normalized = tuple(str(item) for item in path)
    return len(normalized) >= 3 and normalized[:2] == _HANDOVER_PREFIX and normalized[-1] == "scheduler"


def _apply_saved_scheduler_config(
    container: Any,
    saved: Dict[str, Any],
    *,
    path: Sequence[str],
    scheduler_key: str | None = None,
    restart_running: bool = False,
) -> Dict[str, Any]:
    normalized_path = tuple(str(item) for item in path)
    target_key = str(scheduler_key or _SCHEDULER_PATH_KEY_MAP.get(normalized_path, "")).strip().lower()
    refresh_runtime = getattr(container, "refresh_single_scheduler_runtime", None)
    if target_key and callable(refresh_runtime):
        refresh_runtime(target_key, saved, restart_running=restart_running)
        return saved
    if hasattr(container, "reload_config"):
        container.reload_config(saved)
        return saved
    if hasattr(container, "config"):
        container.config = copy.deepcopy(saved)
    return saved


def save_scheduler_config_snapshot(
    container: Any,
    merged: Dict[str, Any],
    *,
    path: Sequence[str],
    scheduler_key: str | None = None,
    restart_running: bool = False,
) -> Dict[str, Any]:
    """Persist scheduler config, respecting handover segmented config as the source of truth."""
    if _is_handover_common_scheduler_path(path):
        relative_path = tuple(str(item) for item in path[2:])
        scheduler_cfg = _get_dict_path(merged, path)
        common_doc = get_handover_common_segment(container.config_path)
        if int(common_doc.get("revision", 0) or 0) <= 0:
            # Ensure first-run segment files are materialized before optimistic revision writes.
            load_settings(container.config_path)
            common_doc = get_handover_common_segment(container.config_path)
        common_data = copy.deepcopy(common_doc.get("data", {}) if isinstance(common_doc.get("data"), dict) else {})
        target = _ensure_dict_path(common_data, relative_path[:-1])
        target[relative_path[-1]] = scheduler_cfg
        saved, _doc, aggregate_error = save_handover_common_segment(
            common_data,
            base_revision=int(common_doc.get("revision", 0) or 0),
            config_path=container.config_path,
        )
        if aggregate_error:
            logger = getattr(container, "add_system_log", None)
            if callable(logger):
                try:
                    logger(f"[调度配置] 交接班分段配置已保存，但聚合配置刷新失败: {aggregate_error}")
                except Exception:  # noqa: BLE001
                    pass
        return _apply_saved_scheduler_config(
            container,
            saved,
            path=path,
            scheduler_key=scheduler_key,
            restart_running=restart_running,
        )

    saved = save_settings(merged, container.config_path)
    return _apply_saved_scheduler_config(
        container,
        saved,
        path=path,
        scheduler_key=scheduler_key,
        restart_running=restart_running,
    )


def persist_scheduler_toggle(
    container: Any,
    *,
    path: Sequence[str],
    auto_start_in_gui: bool,
    scheduler_key: str | None = None,
    restart_running: bool = False,
) -> None:
    try:
        merged = copy.deepcopy(container.config if isinstance(container.config, dict) else {})
        scheduler_cfg = _ensure_dict_path(merged, path)
        desired_auto_start = bool(auto_start_in_gui)
        scheduler_cfg["auto_start_in_gui"] = desired_auto_start
        if desired_auto_start:
            scheduler_cfg["enabled"] = True
        save_scheduler_config_snapshot(
            container,
            merged,
            path=path,
            scheduler_key=scheduler_key,
            restart_running=restart_running,
        )
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
