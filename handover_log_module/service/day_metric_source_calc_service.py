from __future__ import annotations

import copy
from typing import Any, Dict, List

from handover_log_module.core.cell_rule_compiler import build_effective_handover_config, normalize_cell_rules
from handover_log_module.core.day_metric_direct_rules import (
    DAY_METRIC_DIRECT_BUILDINGS,
    DAY_METRIC_DIRECT_BUILDING_RULE_ROWS,
    DAY_METRIC_DIRECT_DEFAULT_RULE_ROWS,
    DAY_METRIC_DIRECT_TYPE_DEFINITIONS,
)
from handover_log_module.core.formatter import build_resolved_value_context
from handover_log_module.core.models import MetricHit
from handover_log_module.core.normalizers import to_float
from handover_log_module.core.selectors import compute_metric_hits
from handover_log_module.repository.excel_reader import load_rows


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _hit_payload(metric_key: str, hit: MetricHit | None) -> Dict[str, Any]:
    if hit is None:
        return {}
    return {
        "metric_key": str(metric_key or "").strip(),
        "row_index": int(hit.row_index or 0),
        "d_name": str(hit.d_name or "").strip(),
        "b_norm": str(hit.b_norm or "").strip(),
        "c_norm": str(hit.c_norm or "").strip(),
        "b_text": str(hit.b_text or "").strip(),
        "c_text": str(hit.c_text or "").strip(),
    }


class DayMetricSourceCalcService:
    _DEFAULT_PARSING_CFG = {
        "sheet_index": 0,
        "start_row": 4,
        "col_b": 2,
        "col_c": 3,
        "col_d": 4,
        "col_e": 5,
        "forward_fill_b": True,
        "forward_fill_c": True,
    }
    _DEFAULT_NORMALIZE_CFG = {
        "b_extract_regex": r"([A-Za-z]-\d{3})",
        "c_extract_regex": r"([A-Za-z]\d-\d)",
        "fallback": "blank",
    }

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config if isinstance(config, dict) else {}

    @staticmethod
    def metric_definitions() -> List[Dict[str, str]]:
        return [copy.deepcopy(item) for item in DAY_METRIC_DIRECT_TYPE_DEFINITIONS]

    def _parsing_cfg(self) -> Dict[str, Any]:
        parsing = self.config.get("parsing", {})
        if isinstance(parsing, dict) and parsing:
            return copy.deepcopy(parsing)
        return copy.deepcopy(self._DEFAULT_PARSING_CFG)

    def _normalize_cfg(self) -> Dict[str, Any]:
        normalize_cfg = self.config.get("normalize", {})
        if isinstance(normalize_cfg, dict) and normalize_cfg:
            return copy.deepcopy(normalize_cfg)
        return copy.deepcopy(self._DEFAULT_NORMALIZE_CFG)

    def _effective_config(self, building: str) -> Dict[str, Any]:
        buildings = list(DAY_METRIC_DIRECT_BUILDINGS)
        building_text = str(building or "").strip()
        if building_text and building_text not in buildings:
            buildings.append(building_text)
        base_cfg = {
            "parsing": self._parsing_cfg(),
            "normalize": self._normalize_cfg(),
            "cell_rules": {
                "default_rows": copy.deepcopy(DAY_METRIC_DIRECT_DEFAULT_RULE_ROWS),
                "building_rows": copy.deepcopy(DAY_METRIC_DIRECT_BUILDING_RULE_ROWS),
            },
        }
        base_cfg["cell_rules"] = normalize_cell_rules(base_cfg, buildings)
        return build_effective_handover_config(base_cfg, building_text, buildings)

    @staticmethod
    def _pick_origin_from_min(
        *,
        left_metric: str,
        right_metric: str,
        hits: Dict[str, MetricHit],
        resolved_values: Dict[str, Any],
    ) -> Dict[str, Any]:
        left_value = to_float(resolved_values.get(left_metric))
        right_value = to_float(resolved_values.get(right_metric))
        if left_value is None and right_value is None:
            return {}
        if right_value is None:
            return _hit_payload(left_metric, hits.get(left_metric))
        if left_value is None:
            return _hit_payload(right_metric, hits.get(right_metric))
        if left_value <= right_value:
            return _hit_payload(left_metric, hits.get(left_metric))
        return _hit_payload(right_metric, hits.get(right_metric))

    def _build_origin_context(
        self,
        *,
        building: str,
        hits: Dict[str, MetricHit],
        resolved_values: Dict[str, Any],
    ) -> Dict[str, Any]:
        by_metric_id: Dict[str, Dict[str, Any]] = {}
        for item in self.metric_definitions():
            metric_id = str(item.get("metric_id", "")).strip()
            if not metric_id:
                continue
            if metric_id == "tank_backup_min":
                by_metric_id[metric_id] = self._pick_origin_from_min(
                    left_metric="west_tank_time",
                    right_metric="east_tank_time",
                    hits=hits,
                    resolved_values=resolved_values,
                )
                continue
            if metric_id == "water_backup_shortest":
                if str(building or "").strip() in {"A楼", "B楼"}:
                    by_metric_id[metric_id] = {}
                else:
                    by_metric_id[metric_id] = _hit_payload("water_pool_backup_time", hits.get("water_pool_backup_time"))
                continue
            by_metric_id[metric_id] = _hit_payload(metric_id, hits.get(metric_id))
        return {"by_metric_id": by_metric_id}

    @staticmethod
    def _tank_backup_min(resolved_values: Dict[str, Any]) -> float | None:
        west = to_float(resolved_values.get("west_tank_time"))
        east = to_float(resolved_values.get("east_tank_time"))
        if west is None and east is None:
            return None
        if west is None:
            return east
        if east is None:
            return west
        return min(west, east)

    @staticmethod
    def _water_backup_shortest(building: str, resolved_values: Dict[str, Any]) -> float | None:
        building_text = str(building or "").strip()
        if building_text in {"A楼", "B楼"}:
            return to_float(resolved_values.get("water_backup_shortest"))
        return to_float(resolved_values.get("water_pool_backup_time"))

    @staticmethod
    def _has_effective_e_value(rows: List[Any]) -> bool:
        for row in rows:
            raw = getattr(row, "e_raw", None)
            if raw is None:
                continue
            if isinstance(raw, str):
                if raw.strip():
                    return True
                continue
            return True
        return False

    def calculate(
        self,
        *,
        building: str,
        duty_date: str,
        data_file: str,
    ) -> Dict[str, Any]:
        effective_config = self._effective_config(building)
        rows = load_rows(
            data_file=data_file,
            parsing_cfg=_dict(effective_config.get("parsing")),
            normalize_cfg=_dict(effective_config.get("normalize")),
        )
        if not self._has_effective_e_value(rows):
            raise ValueError(
                f"交接班源文件E列无有效数据: {data_file}; 疑似命中空缓存或空源文件"
            )
        hits, missing = compute_metric_hits(rows, _dict(effective_config.get("rules")))
        resolved_values = build_resolved_value_context(hits, effective_config)
        resolved_values["tank_backup_min"] = self._tank_backup_min(resolved_values)
        resolved_values["water_backup_shortest"] = self._water_backup_shortest(building, resolved_values)

        origin_context = self._build_origin_context(
            building=building,
            hits=hits,
            resolved_values=resolved_values,
        )
        records: List[Dict[str, Any]] = []
        for item in self.metric_definitions():
            metric_id = str(item.get("metric_id", "")).strip()
            records.append(
                {
                    "metric_id": metric_id,
                    "name": str(item.get("name", "")).strip(),
                    "value": resolved_values.get(metric_id),
                    "origin_payload": copy.deepcopy(_dict(_dict(origin_context.get("by_metric_id")).get(metric_id))),
                }
            )

        return {
            "duty_date": str(duty_date or "").strip(),
            "building": str(building or "").strip(),
            "rows": rows,
            "hits": hits,
            "missing_metrics": missing,
            "effective_config": effective_config,
            "records": records,
            "resolved_metrics": resolved_values,
            "metric_origin_context": origin_context,
        }
