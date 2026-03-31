from __future__ import annotations

from handover_log_module.core.chiller_mode_resolver import resolve_building_mode_by_priority
from handover_log_module.core.models import MetricHit


def test_resolve_building_mode_by_priority_prefers_cooling_over_precool() -> None:
    hits = {
        "chiller_mode_1": MetricHit("chiller_mode_1", 1, "1号冷机模式", 2, "", ""),
        "chiller_mode_4": MetricHit("chiller_mode_4", 2, "4号冷机模式", 1, "", ""),
    }
    effective_config = {
        "chiller_mode": {
            "west_keys": ["chiller_mode_1", "chiller_mode_2", "chiller_mode_3"],
            "east_keys": ["chiller_mode_4", "chiller_mode_5", "chiller_mode_6"],
            "priority_order": ["1", "2", "3", "4"],
            "value_map": {"1": "制冷", "2": "预冷", "3": "板换", "4": "停机"},
            "fallback_mode_text": "停机",
        }
    }

    result = resolve_building_mode_by_priority(hits, effective_config)

    assert result["west_mode"] == "预冷"
    assert result["east_mode"] == "制冷"
    assert result["building_mode"] == "制冷"
    assert result["building_code"] == "1"


def test_resolve_building_mode_by_priority_falls_back_to_stop_when_no_hit() -> None:
    result = resolve_building_mode_by_priority({}, {"chiller_mode": {}})

    assert result["west_mode"] == "停机"
    assert result["east_mode"] == "停机"
    assert result["building_mode"] == "停机"
