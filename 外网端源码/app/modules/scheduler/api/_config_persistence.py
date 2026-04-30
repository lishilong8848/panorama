from __future__ import annotations

import copy
from typing import Any, Dict, Sequence

from fastapi import HTTPException

from app.config.settings_loader import save_settings, update_handover_common_segment_data


def _ensure_dict_path(root: Dict[str, Any], path: Sequence[str]) -> Dict[str, Any]:
    current = root
    for key in path:
        next_value = current.get(key)
        if not isinstance(next_value, dict):
            next_value = {}
            current[key] = next_value
        current = next_value
    return current


def _handover_common_scheduler_path(path: Sequence[str]) -> tuple[str, ...] | None:
    normalized = tuple(str(item or "").strip() for item in path)
    mapping = {
        ("features", "handover_log", "scheduler"): ("scheduler",),
        ("features", "handover_log", "monthly_event_report", "scheduler"): (
            "monthly_event_report",
            "scheduler",
        ),
        ("features", "handover_log", "monthly_change_report", "scheduler"): (
            "monthly_change_report",
            "scheduler",
        ),
    }
    return mapping.get(normalized)


def _dict_at_path(root: Dict[str, Any], path: Sequence[str]) -> Dict[str, Any]:
    current = root
    for key in path:
        next_value = current.get(key)
        if not isinstance(next_value, dict):
            next_value = {}
            current[key] = next_value
        current = next_value
    return current


def save_handover_common_scheduler_patch(
    container: Any,
    *,
    path: Sequence[str],
    scheduler_patch: Dict[str, Any],
    source: str = "调度配置保存",
) -> Dict[str, Any]:
    segment_path = _handover_common_scheduler_path(path)
    if not segment_path:
        raise ValueError("不是交接班公共分段调度路径")
    patch = copy.deepcopy(scheduler_patch if isinstance(scheduler_patch, dict) else {})
    previous_holder: Dict[str, Any] = {}

    def _updater(common_data: Dict[str, Any]) -> None:
        scheduler_cfg = _dict_at_path(common_data, segment_path)
        previous_holder["old_scheduler_config"] = copy.deepcopy(scheduler_cfg)
        scheduler_cfg.update(copy.deepcopy(patch))

    saved, document, aggregate_refresh_error, previous_document, changed = update_handover_common_segment_data(
        _updater,
        config_path=container.config_path,
    )
    new_scheduler_config = _extract_scheduler_cfg(saved, path)
    previous_scheduler_config = previous_holder.get("old_scheduler_config")
    if not isinstance(previous_scheduler_config, dict):
        previous_data = previous_document.get("data", {}) if isinstance(previous_document, dict) else {}
        previous_scheduler_config = _read_nested_dict(previous_data, segment_path)
    add_log = getattr(container, "add_system_log", None)
    if callable(add_log):
        add_log(
            "[调度配置] 已更新交接班公共分段调度 "
            f"source={source}, path={'.'.join(str(item) for item in path)}, "
            f"changed={changed}, revision={document.get('revision', '')}, "
            f"old={previous_scheduler_config}, new={new_scheduler_config}"
        )
        if aggregate_refresh_error:
            add_log(
                "[调度配置] 交接班公共分段已保存，但聚合配置刷新异常 "
                f"path={'.'.join(str(item) for item in path)}, error={aggregate_refresh_error}"
            )
    return {
        "saved_config": saved,
        "document": document,
        "aggregate_refresh_error": aggregate_refresh_error,
        "previous_document": previous_document,
        "previous_scheduler_config": copy.deepcopy(previous_scheduler_config),
        "scheduler_config": copy.deepcopy(new_scheduler_config),
        "changed": bool(changed),
    }


def _read_nested_dict(root: Any, path: Sequence[str]) -> Dict[str, Any]:
    current = root
    for key in path:
        if not isinstance(current, dict):
            return {}
        current = current.get(key)
    return copy.deepcopy(current) if isinstance(current, dict) else {}


def _extract_scheduler_cfg(config: Dict[str, Any], path: Sequence[str]) -> Dict[str, Any]:
    current: Any = config
    for key in path:
        if not isinstance(current, dict):
            return {}
        current = current.get(str(key))
    return copy.deepcopy(current) if isinstance(current, dict) else {}


def persist_scheduler_toggle(container: Any, *, path: Sequence[str], auto_start_in_gui: bool) -> None:
    try:
        desired_auto_start = bool(auto_start_in_gui)
        patch = {"auto_start_in_gui": desired_auto_start}
        if desired_auto_start:
            patch["enabled"] = True
        if _handover_common_scheduler_path(path):
            result = save_handover_common_scheduler_patch(
                container,
                path=path,
                scheduler_patch=patch,
                source="调度开关",
            )
            saved = result["saved_config"]
        else:
            merged = copy.deepcopy(container.config if isinstance(container.config, dict) else {})
            scheduler_cfg = _ensure_dict_path(merged, path)
            scheduler_cfg.update(patch)
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
