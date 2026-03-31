from __future__ import annotations

from typing import Any, Dict

from handover_log_module.core.models import MetricHit


DEFAULT_PRIORITY_ORDER = ["1", "2", "3", "4"]
DEFAULT_VALUE_MAP = {
    "1": "制冷",
    "2": "预冷",
    "3": "板换",
    "4": "停机",
}
DEFAULT_WEST_KEYS = ["chiller_mode_1", "chiller_mode_2", "chiller_mode_3"]
DEFAULT_EAST_KEYS = ["chiller_mode_4", "chiller_mode_5", "chiller_mode_6"]


def _normalize_mode_code(value: Any, value_map: Dict[str, str]) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    if raw in value_map:
        return raw
    try:
        number = float(raw)
    except ValueError:
        number = None
    if number is not None and int(number) == number:
        key = str(int(number))
        if key in value_map:
            return key
    lowered = raw.casefold()
    for key, text in value_map.items():
        if str(text).strip().casefold() == lowered:
            return str(key).strip()
    return ""


def _build_cfg(effective_config: Dict[str, Any] | None, override: Dict[str, Any] | None = None) -> Dict[str, Any]:
    source = {}
    if isinstance(effective_config, dict):
        raw = effective_config.get("chiller_mode", {})
        if isinstance(raw, dict):
            source = raw
    override_cfg = override if isinstance(override, dict) else {}

    west_keys = override_cfg.get("west_keys", source.get("west_keys", DEFAULT_WEST_KEYS))
    east_keys = override_cfg.get("east_keys", source.get("east_keys", DEFAULT_EAST_KEYS))
    priority_order = override_cfg.get("priority_order", source.get("priority_order", DEFAULT_PRIORITY_ORDER))
    fallback_mode_text = str(
        override_cfg.get("fallback_mode_text", source.get("fallback_mode_text", DEFAULT_VALUE_MAP["4"]))
    ).strip() or DEFAULT_VALUE_MAP["4"]

    raw_value_map = override_cfg.get("value_map", source.get("value_map", DEFAULT_VALUE_MAP))
    if isinstance(raw_value_map, dict):
        value_map = {
            str(key).strip(): str(value).strip()
            for key, value in raw_value_map.items()
            if str(key).strip()
        }
    else:
        value_map = dict(DEFAULT_VALUE_MAP)

    return {
        "west_keys": [str(item).strip() for item in west_keys if str(item).strip()],
        "east_keys": [str(item).strip() for item in east_keys if str(item).strip()],
        "priority_order": [str(item).strip() for item in priority_order if str(item).strip()] or list(DEFAULT_PRIORITY_ORDER),
        "value_map": value_map or dict(DEFAULT_VALUE_MAP),
        "fallback_mode_text": fallback_mode_text,
    }


def _pick_mode_by_priority(
    keys: list[str],
    hits: Dict[str, MetricHit],
    value_map: Dict[str, str],
    priority_order: list[str],
    fallback_mode_text: str,
) -> Dict[str, str]:
    priority_index = {code: idx for idx, code in enumerate(priority_order)}
    candidates: list[tuple[int, int, str, str, str]] = []

    for machine_order, key in enumerate(keys):
        hit = hits.get(key)
        if hit is None:
            continue
        code = _normalize_mode_code(hit.value, value_map)
        if not code:
            continue
        text = str(value_map.get(code, "")).strip()
        if not text:
            continue
        candidates.append((priority_index.get(code, len(priority_index) + 100), machine_order, key, code, text))

    if not candidates:
        return {
            "metric_key": "",
            "mode_code": "",
            "mode_text": str(fallback_mode_text or DEFAULT_VALUE_MAP["4"]).strip() or DEFAULT_VALUE_MAP["4"],
        }

    candidates.sort(key=lambda item: (item[0], item[1]))
    _, _, metric_key, mode_code, mode_text = candidates[0]
    return {
        "metric_key": metric_key,
        "mode_code": mode_code,
        "mode_text": mode_text,
    }


def resolve_zone_modes(
    hits: Dict[str, MetricHit],
    effective_config: Dict[str, Any] | None,
    *,
    override: Dict[str, Any] | None = None,
) -> Dict[str, str]:
    cfg = _build_cfg(effective_config, override)
    west = _pick_mode_by_priority(
        cfg["west_keys"],
        hits,
        cfg["value_map"],
        cfg["priority_order"],
        cfg["fallback_mode_text"],
    )
    east = _pick_mode_by_priority(
        cfg["east_keys"],
        hits,
        cfg["value_map"],
        cfg["priority_order"],
        cfg["fallback_mode_text"],
    )
    return {
        "west_key": west["metric_key"],
        "west_code": west["mode_code"],
        "west_mode": west["mode_text"],
        "east_key": east["metric_key"],
        "east_code": east["mode_code"],
        "east_mode": east["mode_text"],
    }


def resolve_building_mode_by_priority(
    hits: Dict[str, MetricHit],
    effective_config: Dict[str, Any] | None,
    *,
    override: Dict[str, Any] | None = None,
) -> Dict[str, str]:
    cfg = _build_cfg(effective_config, override)
    zones = resolve_zone_modes(hits, effective_config, override=override)
    building = _pick_mode_by_priority(
        cfg["west_keys"] + cfg["east_keys"],
        hits,
        cfg["value_map"],
        cfg["priority_order"],
        cfg["fallback_mode_text"],
    )
    return {
        **zones,
        "building_key": building["metric_key"],
        "building_code": building["mode_code"],
        "building_mode": building["mode_text"],
    }
