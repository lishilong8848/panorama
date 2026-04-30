from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from handover_log_module.core.constants import VALID_AGGREGATIONS
from handover_log_module.core.models import MetricHit, RawRow


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value]
    return [str(value)]


def _norm_text(value: Any) -> str:
    return str(value or "").strip().casefold()


def _row_match_rule(row: RawRow, rule: Dict[str, Any]) -> bool:
    d_name = str(row.d_name or "")
    d_name_norm = _norm_text(d_name)

    d_equals = str(rule.get("d_equals", "")).strip()
    if d_equals and _norm_text(d_equals) not in d_name_norm:
        return False

    d_contains = str(rule.get("d_contains", "")).strip()
    if d_contains and _norm_text(d_contains) not in d_name_norm:
        return False

    d_match = [str(x).strip() for x in _as_list(rule.get("d_match")) if str(x).strip()]
    if d_match and not any(_norm_text(pattern) in d_name_norm for pattern in d_match):
        return False

    d_regex = str(rule.get("d_regex", "")).strip()
    if d_regex and re.search(d_regex, d_name, flags=re.IGNORECASE) is None:
        return False

    group_contains = str(rule.get("group_contains", "")).strip()
    if group_contains and _norm_text(group_contains) not in d_name_norm:
        return False

    return True


def _pick_by_agg(metric_key: str, rows: List[RawRow], agg: str) -> MetricHit | None:
    numeric_rows = [row for row in rows if row.value is not None]
    if agg in {"max", "min"} and not numeric_rows:
        return None

    if agg == "max":
        row = max(numeric_rows, key=lambda x: float(x.value))  # type: ignore[arg-type]
    elif agg == "min":
        row = min(numeric_rows, key=lambda x: float(x.value))  # type: ignore[arg-type]
    elif agg == "first":
        if numeric_rows:
            row = numeric_rows[0]
        elif rows:
            row = rows[0]
        else:
            return None
    else:
        raise ValueError(f"不支持的聚合方式: {agg}")

    return MetricHit(
        metric_key=metric_key,
        row_index=row.row_index,
        d_name=row.d_name,
        value=row.value,
        b_norm=row.b_norm,
        c_norm=row.c_norm,
        b_text=row.b_text,
        c_text=row.c_text,
    )


def _prefer_cold_channel_rows(metric_key: str, rows: List[RawRow]) -> List[RawRow]:
    if metric_key not in {"cold_temp_max", "cold_temp_min", "cold_humi_max", "cold_humi_min"}:
        return rows
    if not rows:
        return rows
    cold_rows = [
        row
        for row in rows
        if "冷通道" in _norm_text(row.c_text) or "冷通道" in _norm_text(row.d_name)
    ]
    return cold_rows or rows


def compute_metric_hits(
    rows: List[RawRow],
    rules: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, MetricHit], List[str]]:
    hits: Dict[str, MetricHit] = {}
    missing: List[str] = []

    for metric_key, rule in rules.items():
        agg = str(rule.get("agg", "first")).strip().lower()
        if agg not in VALID_AGGREGATIONS:
            raise ValueError(f"规则配置错误: metric={metric_key}, agg={agg}")

        matched_rows = [row for row in rows if _row_match_rule(row, rule)]
        matched_rows = _prefer_cold_channel_rows(metric_key, matched_rows)
        hit = _pick_by_agg(metric_key, matched_rows, agg)
        if hit is None:
            missing.append(metric_key)
            continue
        hits[metric_key] = hit

    return hits, missing
