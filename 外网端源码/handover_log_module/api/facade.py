from __future__ import annotations

import copy
from typing import Any, Callable, Dict, List

from app.modules.feishu.service.feishu_auth_resolver import resolve_feishu_auth_settings
from handover_log_module.core.cell_rule_compiler import migrate_legacy_rule_structures, normalize_cell_rules
from handover_log_module.service.handover_orchestrator import HandoverOrchestrator


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _runtime_defaults() -> Dict[str, Any]:
    # Runtime defaults come from v3 schema defaults only.
    from app.config.config_adapter import adapt_runtime_config, ensure_v3_config

    return adapt_runtime_config(ensure_v3_config({}))


def _looks_like_runtime_config(cfg: Dict[str, Any]) -> bool:
    return (
        isinstance(cfg.get("download"), dict)
        and isinstance(cfg.get("network"), dict)
        and isinstance(cfg.get("handover_log"), dict)
    )


def _to_runtime_config(config: Dict[str, Any] | None) -> Dict[str, Any]:
    from app.config.config_adapter import adapt_runtime_config, ensure_v3_config

    full_cfg = config if isinstance(config, dict) else {}
    if not full_cfg:
        return _runtime_defaults()

    if _looks_like_runtime_config(full_cfg):
        return copy.deepcopy(full_cfg)

    if isinstance(full_cfg.get("common"), dict) and isinstance(full_cfg.get("features"), dict):
        return adapt_runtime_config(ensure_v3_config(full_cfg))

    # Legacy root / handover-only payloads.
    return adapt_runtime_config(ensure_v3_config(full_cfg))


def _collect_buildings(cfg: Dict[str, Any]) -> list[str]:
    output: list[str] = []

    for site in cfg.get("sites", []) if isinstance(cfg.get("sites", []), list) else []:
        if not isinstance(site, dict):
            continue
        building = str(site.get("building", "")).strip()
        if building and building not in output:
            output.append(building)

    global_download = cfg.get("_global_download", {})
    if isinstance(global_download, dict):
        for site in global_download.get("sites", []) if isinstance(global_download.get("sites", []), list) else []:
            if not isinstance(site, dict):
                continue
            building = str(site.get("building", "")).strip()
            if building and building not in output:
                output.append(building)

    return output


def _normalize_role_mode(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"internal", "external"}:
        return text
    return ""


def _validate(cfg: Dict[str, Any]) -> None:
    required_sections = [
        "download",
        "template",
        "parsing",
        "normalize",
        "chiller_mode",
        "template_fixed_fill",
        "cell_rules",
    ]
    missing = [k for k in required_sections if k not in cfg or not isinstance(cfg[k], dict)]
    if missing:
        raise ValueError(f"handover_log配置缺少或类型错误: {missing}")

    cell_rules = cfg.get("cell_rules", {})
    default_rows = cell_rules.get("default_rows", []) if isinstance(cell_rules, dict) else []
    if not isinstance(default_rows, list):
        raise ValueError("handover_log.cell_rules.default_rows 必须是数组")
    if not default_rows:
        raise ValueError("handover_log.cell_rules.default_rows 不能为空")


def load_handover_config(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    runtime_cfg = _to_runtime_config(config)
    runtime_defaults = _runtime_defaults()
    defaults = _deep_merge(runtime_defaults.get("handover_log", {}), {})
    handover_cfg = (
        runtime_cfg.get("handover_log", {})
        if isinstance(runtime_cfg.get("handover_log", {}), dict)
        else {}
    )
    base_cfg = _deep_merge(defaults, handover_cfg)

    # Reuse global download sites/network while keeping handover module isolated.
    global_download = (
        runtime_cfg.get("download", {}) if isinstance(runtime_cfg.get("download", {}), dict) else {}
    )
    global_network = runtime_cfg.get("network", {}) if isinstance(runtime_cfg.get("network", {}), dict) else {}
    global_feishu = resolve_feishu_auth_settings(runtime_cfg)
    global_paths = runtime_cfg.get("paths", {}) if isinstance(runtime_cfg.get("paths", {}), dict) else {}
    global_shared_bridge = runtime_cfg.get("shared_bridge", {}) if isinstance(runtime_cfg.get("shared_bridge", {}), dict) else {}
    if not base_cfg.get("sites") and isinstance(global_download.get("sites"), list):
        base_cfg["sites"] = copy.deepcopy(global_download["sites"])
    base_cfg["_global_download"] = copy.deepcopy(global_download)
    base_cfg["network"] = copy.deepcopy(global_network)
    base_cfg["_global_feishu"] = copy.deepcopy(global_feishu)
    base_cfg["_global_paths"] = copy.deepcopy(global_paths)
    base_cfg["_shared_bridge"] = copy.deepcopy(global_shared_bridge)
    base_cfg["_deployment_role_mode"] = _normalize_role_mode(
        (runtime_cfg.get("deployment", {}) if isinstance(runtime_cfg.get("deployment", {}), dict) else {}).get("role_mode", "")
    )

    # One-time compatibility in runtime: old rule structures are converted to cell_rules.
    base_cfg = migrate_legacy_rule_structures(base_cfg)
    buildings = _collect_buildings(base_cfg)
    base_cfg["cell_rules"] = normalize_cell_rules(base_cfg, buildings)

    _validate(base_cfg)
    return base_cfg


def run_from_existing_file(
    config: Dict[str, Any],
    building: str,
    data_file: str,
    capacity_source_file: str | None = None,
    end_time: str | None = None,
    duty_date: str | None = None,
    duty_shift: str | None = None,
    auto_send_review_link: bool = True,
    emit_log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    cfg = load_handover_config(config)
    orchestrator = HandoverOrchestrator(cfg)
    return orchestrator.run_from_existing_file(
        building=str(building).strip(),
        data_file=str(data_file).strip(),
        capacity_source_file=str(capacity_source_file or "").strip() or None,
        end_time=end_time,
        duty_date=str(duty_date or "").strip() or None,
        duty_shift=str(duty_shift or "").strip().lower() or None,
        auto_send_review_link=bool(auto_send_review_link),
        emit_log=emit_log,
    )


def run_from_existing_files(
    config: Dict[str, Any],
    building_files: List[tuple[str, str]],
    capacity_building_files: List[tuple[str, str]] | None = None,
    configured_buildings: List[str] | None = None,
    end_time: str | None = None,
    duty_date: str | None = None,
    duty_shift: str | None = None,
    auto_send_review_link: bool = True,
    emit_log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    cfg = load_handover_config(config)
    orchestrator = HandoverOrchestrator(cfg)
    target = [
        (str(building).strip(), str(data_file).strip())
        for building, data_file in (building_files or [])
        if str(building).strip() and str(data_file).strip()
    ]
    capacity_target = [
        (str(building).strip(), str(data_file).strip())
        for building, data_file in (capacity_building_files or [])
        if str(building).strip() and str(data_file).strip()
    ]
    buildings = [str(item).strip() for item in (configured_buildings or []) if str(item).strip()]
    return orchestrator.run_from_existing_files(
        building_files=target,
        capacity_building_files=capacity_target or None,
        configured_buildings=buildings or None,
        end_time=end_time,
        duty_date=str(duty_date or "").strip() or None,
        duty_shift=str(duty_shift or "").strip().lower() or None,
        auto_send_review_link=bool(auto_send_review_link),
        emit_log=emit_log,
    )


def run_from_download(
    config: Dict[str, Any],
    buildings: List[str] | None = None,
    end_time: str | None = None,
    duty_date: str | None = None,
    duty_shift: str | None = None,
    emit_log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    cfg = load_handover_config(config)
    orchestrator = HandoverOrchestrator(cfg)
    target = [str(x).strip() for x in (buildings or []) if str(x).strip()]
    return orchestrator.run_from_download(
        buildings=target or None,
        end_time=end_time,
        duty_date=str(duty_date or "").strip() or None,
        duty_shift=str(duty_shift or "").strip().lower() or None,
        emit_log=emit_log,
    )
