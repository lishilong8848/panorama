from __future__ import annotations

import copy
import re
from typing import Any, Dict, List, Tuple

from handover_log_module.core.fixed_cell_overrides import FORCED_FIXED_CELL_VALUES, normalize_cell_name


_DEFAULT_COMPUTED_OPS = {
    "tank_backup",
    "ring_supply_temp",
    "chiller_mode_summary",
}


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_cell(value: Any) -> str:
    text = _norm_text(value).upper()
    if not text:
        return ""
    return text


def _valid_cell(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]+[1-9]\d*", value))


def _build_default_row() -> Dict[str, Any]:
    return {
        "id": "",
        "enabled": True,
        "target_cell": "",
        "rule_type": "direct",
        "d_keywords": [],
        "match_mode": "contains_casefold",
        "agg": "first",
        "template": "{value}",
        "computed_op": "",
        "params": {},
        # advanced matching fields (backward compatibility)
        "d_equals": "",
        "d_contains": "",
        "d_regex": "",
        "group_contains": "",
        "use_b": False,
        "use_c": False,
    }


def _expand_compatible_d_keywords(row_id: str, keywords: List[str]) -> List[str]:
    expanded: List[str] = []

    def _append(value: str) -> None:
        text = _norm_text(value)
        if text and text not in expanded:
            expanded.append(text)

    for keyword in keywords:
        _append(keyword)

    if not re.fullmatch(r"chiller_mode_[1-6]", row_id):
        return expanded

    for keyword in list(expanded):
        match = re.fullmatch(r"(\d+)号冷机(运行)?模式", keyword)
        if not match:
            continue
        machine_no = match.group(1)
        _append(f"{machine_no}号冷机模式")
        _append(f"{machine_no}号冷机运行模式")
    return expanded


def normalize_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    row = _build_default_row()
    if isinstance(raw, dict):
        row.update(copy.deepcopy(raw))
    row["id"] = _norm_text(row.get("id"))
    row["enabled"] = bool(row.get("enabled", True))
    row["target_cell"] = _norm_cell(row.get("target_cell"))
    row["rule_type"] = _norm_text(row.get("rule_type")).lower() or "direct"
    if row["rule_type"] not in {"direct", "aggregate", "computed"}:
        row["rule_type"] = "direct"
    row["match_mode"] = _norm_text(row.get("match_mode")).lower() or "contains_casefold"
    row["agg"] = _norm_text(row.get("agg")).lower() or "first"
    if row["agg"] not in {"first", "max", "min"}:
        row["agg"] = "first"
    row["template"] = _norm_text(row.get("template")) or "{value}"
    row["computed_op"] = _norm_text(row.get("computed_op"))
    params = row.get("params", {})
    row["params"] = copy.deepcopy(params) if isinstance(params, dict) else {}
    row["d_equals"] = _norm_text(row.get("d_equals"))
    row["d_contains"] = _norm_text(row.get("d_contains"))
    row["d_regex"] = _norm_text(row.get("d_regex"))
    row["group_contains"] = _norm_text(row.get("group_contains"))
    row["use_b"] = bool(row.get("use_b", False))
    row["use_c"] = bool(row.get("use_c", False))
    keywords: List[str] = []
    for item in _list(row.get("d_keywords")):
        text = _norm_text(item)
        if text:
            keywords.append(text)
    row["d_keywords"] = _expand_compatible_d_keywords(row["id"], keywords)
    return row


def normalize_cell_rules(raw_cfg: Dict[str, Any], buildings: List[str]) -> Dict[str, Any]:
    cfg = _dict(raw_cfg)
    cell_rules = _dict(cfg.get("cell_rules"))
    default_rows_raw = _list(cell_rules.get("default_rows"))
    building_rows_raw = _dict(cell_rules.get("building_rows"))

    normalized_default = [normalize_row(item) for item in default_rows_raw if isinstance(item, dict)]
    normalized_building: Dict[str, List[Dict[str, Any]]] = {}
    for building in buildings:
        rows = _list(building_rows_raw.get(building))
        normalized_building[building] = [normalize_row(item) for item in rows if isinstance(item, dict)]

    # Keep unknown building keys to avoid silent data loss.
    for building, rows in building_rows_raw.items():
        if building in normalized_building:
            continue
        rows_list = _list(rows)
        normalized_building[str(building)] = [normalize_row(item) for item in rows_list if isinstance(item, dict)]

    return {
        "default_rows": normalized_default,
        "building_rows": normalized_building,
    }


def merge_effective_rows(cell_rules: Dict[str, Any], building: str) -> List[Dict[str, Any]]:
    default_rows = [normalize_row(item) for item in _list(_dict(cell_rules).get("default_rows")) if isinstance(item, dict)]
    building_rows = [
        normalize_row(item)
        for item in _list(_dict(_dict(cell_rules).get("building_rows")).get(building))
        if isinstance(item, dict)
    ]
    merged: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for row in default_rows:
        row_id = _norm_text(row.get("id"))
        if not row_id:
            continue
        merged[row_id] = row
        order.append(row_id)
    for row in building_rows:
        row_id = _norm_text(row.get("id"))
        if not row_id:
            continue
        if row_id not in merged:
            order.append(row_id)
        merged[row_id] = row
    return [merged[row_id] for row_id in order if row_id in merged]


def compile_rows_to_runtime(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rules: Dict[str, Dict[str, Any]] = {}
    cell_mapping: Dict[str, str] = {}
    format_templates: Dict[str, str] = {}
    computed_metric_ops: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        row_id = _norm_text(row.get("id"))
        if not row_id or not bool(row.get("enabled", True)):
            continue
        target_cell = _norm_cell(row.get("target_cell"))
        rule_type = _norm_text(row.get("rule_type")).lower() or "direct"
        template = _norm_text(row.get("template")) or "{value}"
        agg = _norm_text(row.get("agg")).lower() or "first"

        if target_cell and normalize_cell_name(target_cell) in FORCED_FIXED_CELL_VALUES:
            continue

        if target_cell:
            if _valid_cell(target_cell):
                cell_mapping[row_id] = target_cell
                format_templates[row_id] = template

        if rule_type == "computed":
            computed_op = _norm_text(row.get("computed_op")) or (row_id if row_id in _DEFAULT_COMPUTED_OPS else "")
            if computed_op:
                params = row.get("params", {})
                computed_metric_ops[row_id] = {
                    "op": computed_op,
                    "params": copy.deepcopy(params) if isinstance(params, dict) else {},
                }
            continue

        rule: Dict[str, Any] = {"agg": agg}
        d_keywords = [str(x).strip() for x in _list(row.get("d_keywords")) if str(x).strip()]
        if d_keywords:
            rule["d_match"] = d_keywords
        d_equals = _norm_text(row.get("d_equals"))
        d_contains = _norm_text(row.get("d_contains"))
        d_regex = _norm_text(row.get("d_regex"))
        group_contains = _norm_text(row.get("group_contains"))
        if d_equals:
            rule["d_equals"] = d_equals
        if d_contains:
            rule["d_contains"] = d_contains
        if d_regex:
            rule["d_regex"] = d_regex
        if group_contains:
            rule["group_contains"] = group_contains
        if bool(row.get("use_b", False)):
            rule["use_b"] = True
        if bool(row.get("use_c", False)):
            rule["use_c"] = True
        rules[row_id] = rule
    return {
        "rules": rules,
        "cell_mapping": cell_mapping,
        "format_templates": format_templates,
        "computed_metric_ops": computed_metric_ops,
    }


def build_effective_handover_config(base_cfg: Dict[str, Any], building: str, buildings: List[str]) -> Dict[str, Any]:
    cfg = copy.deepcopy(_dict(base_cfg))
    normalized_rules = normalize_cell_rules(cfg, buildings)
    cfg["cell_rules"] = normalized_rules
    rows = merge_effective_rows(normalized_rules, building)
    compiled = compile_rows_to_runtime(rows)
    cfg["rules"] = compiled["rules"]
    cfg["cell_mapping"] = compiled["cell_mapping"]
    cfg["format_templates"] = compiled["format_templates"]
    cfg["computed_metric_ops"] = compiled["computed_metric_ops"]
    return cfg


def migrate_legacy_rule_structures(raw_cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfg = copy.deepcopy(_dict(raw_cfg))
    if isinstance(cfg.get("cell_rules"), dict):
        return cfg

    legacy_rules = _dict(cfg.get("rules"))
    legacy_cell_mapping = _dict(cfg.get("cell_mapping"))
    legacy_templates = _dict(cfg.get("format_templates"))
    legacy_overrides = _dict(cfg.get("building_overrides"))

    union_keys: List[str] = []
    for key in list(legacy_rules.keys()) + list(legacy_cell_mapping.keys()) + list(legacy_templates.keys()):
        text = _norm_text(key)
        if text and text not in union_keys:
            union_keys.append(text)

    default_rows: List[Dict[str, Any]] = []
    for key in union_keys:
        legacy_rule = _dict(legacy_rules.get(key))
        row = normalize_row(
            {
                "id": key,
                "enabled": True,
                "target_cell": str(legacy_cell_mapping.get(key, "") or ""),
                "template": str(legacy_templates.get(key, "{value}") or "{value}"),
                "agg": str(legacy_rule.get("agg", "first") or "first"),
                "d_keywords": legacy_rule.get("d_match", []),
                "d_equals": legacy_rule.get("d_equals", ""),
                "d_contains": legacy_rule.get("d_contains", ""),
                "d_regex": legacy_rule.get("d_regex", ""),
                "group_contains": legacy_rule.get("group_contains", ""),
                "use_b": bool(legacy_rule.get("use_b", False)),
                "use_c": bool(legacy_rule.get("use_c", False)),
            }
        )
        if key in _DEFAULT_COMPUTED_OPS and key not in legacy_rules:
            row["rule_type"] = "computed"
            row["computed_op"] = key
        else:
            row["rule_type"] = "aggregate"
        default_rows.append(row)

    building_rows: Dict[str, List[Dict[str, Any]]] = {}
    for building, override in legacy_overrides.items():
        override_cfg = _dict(override)
        o_rules = _dict(override_cfg.get("rules"))
        o_cells = _dict(override_cfg.get("cell_mapping"))
        o_templates = _dict(override_cfg.get("format_templates"))
        o_keys: List[str] = []
        for key in list(o_rules.keys()) + list(o_cells.keys()) + list(o_templates.keys()):
            text = _norm_text(key)
            if text and text not in o_keys:
                o_keys.append(text)
        rows: List[Dict[str, Any]] = []
        for key in o_keys:
            o_rule = _dict(o_rules.get(key))
            row = normalize_row(
                {
                    "id": key,
                    "enabled": True,
                    "target_cell": str(o_cells.get(key, "") or ""),
                    "template": str(o_templates.get(key, "{value}") or "{value}"),
                    "agg": str(o_rule.get("agg", "first") or "first"),
                    "d_keywords": o_rule.get("d_match", []),
                    "d_equals": o_rule.get("d_equals", ""),
                    "d_contains": o_rule.get("d_contains", ""),
                    "d_regex": o_rule.get("d_regex", ""),
                    "group_contains": o_rule.get("group_contains", ""),
                    "use_b": bool(o_rule.get("use_b", False)),
                    "use_c": bool(o_rule.get("use_c", False)),
                }
            )
            if key in _DEFAULT_COMPUTED_OPS and key not in o_rules:
                row["rule_type"] = "computed"
                row["computed_op"] = key
            else:
                row["rule_type"] = "aggregate"
            rows.append(row)
        if rows:
            building_rows[str(building)] = rows

    cfg["cell_rules"] = {
        "default_rows": default_rows,
        "building_rows": building_rows,
    }
    cfg.pop("rules", None)
    cfg.pop("cell_mapping", None)
    cfg.pop("format_templates", None)
    cfg.pop("building_overrides", None)
    return cfg
