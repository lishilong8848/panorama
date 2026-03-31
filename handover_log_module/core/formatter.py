from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from handover_log_module.core.expression_eval import evaluate_expression, get_expression_variables
from handover_log_module.core.models import MetricHit
from handover_log_module.core.normalizers import format_number, to_float


_BUILTIN_COMPUTED_OPS = {
    "tank_backup",
    "ring_supply_temp",
    "chiller_mode_summary",
}


def _extract_th_suffix(text: Any) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    match = re.search(r"TH\s*[-_ ]?\s*(\d{2})", raw, flags=re.IGNORECASE)
    if not match:
        return ""
    return f"TH-{match.group(1)}"


def _render_b_norm(metric_key: str, hit: MetricHit) -> str:
    b_norm = str(hit.b_norm or "").strip()
    if metric_key not in {"cold_temp_max", "cold_temp_min", "cold_humi_max", "cold_humi_min"}:
        return b_norm
    if not b_norm:
        return b_norm
    if str(hit.c_norm or "").strip():
        return b_norm
    th_suffix = _extract_th_suffix(hit.c_text) or _extract_th_suffix(hit.d_name)
    if not th_suffix:
        return b_norm
    return f"{b_norm}-{th_suffix}"


def _clean_d_name_for_metric(metric_key: str, d_name: Any) -> str:
    raw = str(d_name or "").strip()
    if not raw:
        return ""
    if metric_key in {"tr_load_max", "ups_load_max"}:
        # Keep identifier, strip trailing "负载率" suffix for B10/D10 text.
        cleaned = re.sub(r"\s*负载率\s*$", "", raw, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"[-_/]+\s*$", "", cleaned).strip()
        return cleaned or raw
    return raw


def _render_template(template: str, data: Dict[str, Any]) -> str:
    try:
        text = template.format(**data)
    except Exception:
        return ""
    return str(text).strip()


def _normalize_mode_code(value: Any, value_map: Dict[str, str]) -> str:
    if value is None:
        return ""

    raw = str(value).strip()
    if not raw:
        return ""

    if raw in value_map:
        return raw

    try:
        num = float(raw)
        if int(num) == num:
            key = str(int(num))
            if key in value_map:
                return key
    except ValueError:
        pass

    for key, mode_text in value_map.items():
        if str(mode_text).strip().casefold() == raw.casefold():
            return str(key).strip()

    return ""


def _pick_mode_by_priority(
    keys: List[str],
    hits: Dict[str, MetricHit],
    value_map: Dict[str, str],
    priority_order: List[str],
    fallback_mode_text: str,
) -> Tuple[str, str]:
    normalized_priority = [str(x).strip() for x in priority_order if str(x).strip()]
    if not normalized_priority:
        normalized_priority = ["1", "2", "3", "4"]
    priority_index = {mode_code: idx for idx, mode_code in enumerate(normalized_priority)}

    mode_candidates: List[Tuple[int, int, str, str]] = []
    for machine_order, key in enumerate(keys):
        hit = hits.get(key)
        if hit is None:
            continue
        mode_code = _normalize_mode_code(hit.value, value_map)
        if not mode_code:
            continue
        mode_text = str(value_map.get(mode_code, "")).strip()
        if not mode_text:
            continue
        mode_candidates.append((priority_index.get(mode_code, len(priority_index) + 100), machine_order, key, mode_text))

    if not mode_candidates:
        fallback = str(fallback_mode_text).strip() or str(value_map.get("4", "停机")).strip() or "停机"
        return "", fallback

    mode_candidates.sort(key=lambda item: (item[0], item[1]))
    _, _, chosen_key, chosen_mode = mode_candidates[0]
    return chosen_key, chosen_mode


def _get_computed_metric_ops(effective_config: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    if not isinstance(effective_config, dict):
        return {}
    raw = effective_config.get("computed_metric_ops", {})
    if not isinstance(raw, dict):
        return {}
    output: Dict[str, Dict[str, Any]] = {}
    for metric_key, item in raw.items():
        if not isinstance(item, dict):
            continue
        op = str(item.get("op", "")).strip()
        if not op:
            continue
        params = item.get("params", {})
        output[str(metric_key)] = {"op": op, "params": params if isinstance(params, dict) else {}}
    return output


def _resolve_builtin_text(
    metric_key: str,
    op: str,
    hits: Dict[str, MetricHit],
    templates: Dict[str, str],
    effective_config: Optional[Dict[str, Any]],
) -> str:
    template = str(templates.get(metric_key, "")).strip()
    if not template:
        return ""

    if op == "tank_backup":
        west_hit = hits.get("west_tank_time")
        east_hit = hits.get("east_tank_time")
        if west_hit is None or east_hit is None:
            return ""
        return _render_template(template, {"west": format_number(west_hit.value), "east": format_number(east_hit.value)})

    if op == "ring_supply_temp":
        west_hit = hits.get("ring_124")
        east_hit = hits.get("ring_150")
        if west_hit is None or east_hit is None:
            return ""
        return _render_template(template, {"west": format_number(west_hit.value), "east": format_number(east_hit.value)})

    if op == "chiller_mode_summary":
        chiller_cfg = {}
        if isinstance(effective_config, dict):
            raw_cfg = effective_config.get("chiller_mode", {})
            if isinstance(raw_cfg, dict):
                chiller_cfg = raw_cfg

        west_keys = chiller_cfg.get("west_keys", ["chiller_mode_1", "chiller_mode_2", "chiller_mode_3"])
        east_keys = chiller_cfg.get("east_keys", ["chiller_mode_4", "chiller_mode_5", "chiller_mode_6"])
        priority_order = chiller_cfg.get("priority_order", ["1", "2", "3", "4"])
        value_map_raw = chiller_cfg.get("value_map", {"1": "制冷", "2": "预冷", "3": "板换", "4": "停机"})
        fallback_mode_text = str(chiller_cfg.get("fallback_mode_text", "停机")).strip() or "停机"

        west_keys = [str(x).strip() for x in west_keys if str(x).strip()]
        east_keys = [str(x).strip() for x in east_keys if str(x).strip()]
        priority_order = [str(x).strip() for x in priority_order if str(x).strip()]
        if isinstance(value_map_raw, dict):
            value_map = {str(k).strip(): str(v).strip() for k, v in value_map_raw.items() if str(k).strip()}
        else:
            value_map = {"1": "制冷", "2": "预冷", "3": "板换", "4": "停机"}

        west_key, west_mode = _pick_mode_by_priority(west_keys, hits, value_map, priority_order, fallback_mode_text)
        east_key, east_mode = _pick_mode_by_priority(east_keys, hits, value_map, priority_order, fallback_mode_text)
        print(f"[交接班][冷机模式] 西区={west_key or '-'}:{west_mode} 东区={east_key or '-'}:{east_mode}")
        return _render_template(template, {"west_mode": west_mode, "east_mode": east_mode})

    return ""


def _resolve_raw_value(
    metric_key: str,
    *,
    hits: Dict[str, MetricHit],
    computed_metric_ops: Dict[str, Dict[str, Any]],
    effective_config: Optional[Dict[str, Any]],
    raw_cache: Dict[str, Optional[float]],
    visiting: Set[str],
) -> Optional[float]:
    if metric_key in raw_cache:
        return raw_cache[metric_key]
    if metric_key in visiting:
        return None

    hit = hits.get(metric_key)
    if hit is not None:
        raw_cache[metric_key] = hit.value
        return hit.value

    op_cfg = computed_metric_ops.get(metric_key, {})
    op = str(op_cfg.get("op", "")).strip()
    if not op:
        raw_cache[metric_key] = None
        return None

    visiting.add(metric_key)
    try:
        if op in _BUILTIN_COMPUTED_OPS:
            raw_value = None
        else:
            variable_names = get_expression_variables(op)
            variable_values: Dict[str, Any] = {}
            for var_name in variable_names:
                variable_values[var_name] = _resolve_raw_value(
                    var_name,
                    hits=hits,
                    computed_metric_ops=computed_metric_ops,
                    effective_config=effective_config,
                    raw_cache=raw_cache,
                    visiting=visiting,
                )
            raw_value = evaluate_expression(op, variable_values)
    except Exception as exc:  # noqa: BLE001
        print(f"[交接班][表达式] metric={metric_key} 计算失败: {exc}")
        raw_value = None

    visiting.discard(metric_key)
    raw_cache[metric_key] = to_float(raw_value)
    return raw_cache[metric_key]


def _resolve_metric_text(
    metric_key: str,
    *,
    hits: Dict[str, MetricHit],
    templates: Dict[str, str],
    effective_config: Optional[Dict[str, Any]],
    computed_metric_ops: Dict[str, Dict[str, Any]],
    raw_cache: Dict[str, Optional[float]],
    text_cache: Dict[str, str],
) -> str:
    if metric_key in text_cache:
        return text_cache[metric_key]

    hit = hits.get(metric_key)
    template = str(templates.get(metric_key, "")).strip()
    if hit is not None:
        if not template:
            text_cache[metric_key] = ""
            return ""
        b_norm_render = _render_b_norm(metric_key, hit)
        d_name_clean = _clean_d_name_for_metric(metric_key, hit.d_name)
        text = _render_template(
            template,
            {
                "value": format_number(hit.value),
                "b_norm": b_norm_render,
                "c_norm": hit.c_norm,
                "d_name": d_name_clean,
                "d_name_raw": hit.d_name,
                "d_name_clean": d_name_clean,
                "b_text": hit.b_text,
                "c_text": hit.c_text,
                "b_norm_raw": hit.b_norm,
                "b_norm_render": b_norm_render,
            },
        )
        text_cache[metric_key] = text
        return text

    op_cfg = computed_metric_ops.get(metric_key, {})
    op = str(op_cfg.get("op", "")).strip()
    if not op and metric_key in _BUILTIN_COMPUTED_OPS:
        op = metric_key
    if not op:
        text_cache[metric_key] = ""
        return ""

    if op in _BUILTIN_COMPUTED_OPS:
        text = _resolve_builtin_text(
            metric_key=metric_key,
            op=op,
            hits=hits,
            templates=templates,
            effective_config=effective_config,
        )
        text_cache[metric_key] = text
        return text

    if template in {"", "无"}:
        template = "{value}"
    raw_value = _resolve_raw_value(
        metric_key,
        hits=hits,
        computed_metric_ops=computed_metric_ops,
        effective_config=effective_config,
        raw_cache=raw_cache,
        visiting=set(),
    )
    text = _render_template(template, {"value": format_number(raw_value)})
    text_cache[metric_key] = text
    return text


def build_metric_text(
    metric_key: str,
    hits: Dict[str, MetricHit],
    templates: Dict[str, str],
    effective_config: Optional[Dict[str, Any]] = None,
) -> str:
    computed_metric_ops = _get_computed_metric_ops(effective_config)
    raw_cache: Dict[str, Optional[float]] = {key: hit.value for key, hit in hits.items()}
    text_cache: Dict[str, str] = {}
    return _resolve_metric_text(
        metric_key=metric_key,
        hits=hits,
        templates=templates,
        effective_config=effective_config,
        computed_metric_ops=computed_metric_ops,
        raw_cache=raw_cache,
        text_cache=text_cache,
    )


def build_resolved_value_context(
    hits: Dict[str, MetricHit],
    effective_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Optional[float]]:
    computed_metric_ops = _get_computed_metric_ops(effective_config)
    raw_cache: Dict[str, Optional[float]] = {key: hit.value for key, hit in hits.items()}
    for metric_key in computed_metric_ops.keys():
        _resolve_raw_value(
            metric_key,
            hits=hits,
            computed_metric_ops=computed_metric_ops,
            effective_config=effective_config,
            raw_cache=raw_cache,
            visiting=set(),
        )
    return raw_cache


def build_cell_value_map(
    cell_mapping: Dict[str, str],
    templates: Dict[str, str],
    hits: Dict[str, MetricHit],
    effective_config: Optional[Dict[str, Any]] = None,
    missing_policy: str = "blank",
) -> Dict[str, str]:
    computed_metric_ops = _get_computed_metric_ops(effective_config)
    raw_cache: Dict[str, Optional[float]] = {key: hit.value for key, hit in hits.items()}
    text_cache: Dict[str, str] = {}
    result: Dict[str, str] = {}
    for metric_key, cell in cell_mapping.items():
        text = _resolve_metric_text(
            metric_key=metric_key,
            hits=hits,
            templates=templates,
            effective_config=effective_config,
            computed_metric_ops=computed_metric_ops,
            raw_cache=raw_cache,
            text_cache=text_cache,
        )
        if text:
            result[str(cell).strip()] = text
            continue

        policy = str(missing_policy).strip().lower()
        if policy == "zero":
            result[str(cell).strip()] = "0"
        elif policy == "na":
            result[str(cell).strip()] = "N/A"
    return result


def missing_metrics_for_cells(
    cell_mapping: Dict[str, str],
    hits: Dict[str, MetricHit],
    effective_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    computed_metric_ops = _get_computed_metric_ops(effective_config)
    raw_cache: Dict[str, Optional[float]] = {key: hit.value for key, hit in hits.items()}
    text_cache: Dict[str, str] = {}
    templates: Dict[str, str] = {}
    if isinstance(effective_config, dict):
        raw_templates = effective_config.get("format_templates", {})
        if isinstance(raw_templates, dict):
            templates = {str(k): str(v) for k, v in raw_templates.items()}
    mapping: Dict[str, str] = {}
    for metric_key, cell in cell_mapping.items():
        if metric_key not in templates:
            templates[metric_key] = "{value}"
        text = _resolve_metric_text(
            metric_key=metric_key,
            hits=hits,
            templates=templates,
            effective_config=effective_config,
            computed_metric_ops=computed_metric_ops,
            raw_cache=raw_cache,
            text_cache=text_cache,
        )
        if not text:
            mapping[metric_key] = str(cell)
    return mapping
