from __future__ import annotations

from handover_log_module.core.formatter import build_cell_value_map
from handover_log_module.core.models import MetricHit


def test_build_cell_value_map_basic_metrics() -> None:
    hits = {
        "cold_temp_max": MetricHit("cold_temp_max", 4, "冷通道温度", 25.17, "E-301", "C3-2"),
        "tr_load_max": MetricHit("tr_load_max", 8, "E-317-TR201负载率", 15.09, "", ""),
        "west_tank_time": MetricHit("west_tank_time", 10, "西区蓄冷罐放冷时间", 1743.0, "", ""),
        "east_tank_time": MetricHit("east_tank_time", 11, "东区蓄冷罐放冷时间", 388.0, "", ""),
    }
    cell_mapping = {
        "cold_temp_max": "B9",
        "tr_load_max": "B10",
        "tank_backup": "F8",
        "ups_load_max": "D10",
    }
    templates = {
        "cold_temp_max": "{value}℃/{b_norm} {c_norm}",
        "tr_load_max": "{value}%/{d_name}",
        "tank_backup": "西区{west}/东区{east}",
        "ups_load_max": "{value}%/{d_name}",
    }
    result = build_cell_value_map(cell_mapping, templates, hits, missing_policy="blank")
    assert result["B9"] == "25.17℃/E-301 C3-2"
    assert result["B10"] == "15.09%/E-317-TR201"
    assert result["F8"] == "西区1743/东区388"
    assert "D10" not in result


def test_chiller_mode_summary_pick_first_non_stop() -> None:
    hits = {
        "chiller_mode_1": MetricHit("chiller_mode_1", 4, "1号冷机模式", 4, "", ""),
        "chiller_mode_2": MetricHit("chiller_mode_2", 5, "2号冷机模式", 3, "", ""),
        "chiller_mode_3": MetricHit("chiller_mode_3", 6, "3号冷机模式", 1, "", ""),
        "chiller_mode_4": MetricHit("chiller_mode_4", 7, "4号冷机模式", 1, "", ""),
        "chiller_mode_5": MetricHit("chiller_mode_5", 8, "5号冷机模式", 4, "", ""),
        "chiller_mode_6": MetricHit("chiller_mode_6", 9, "6号冷机模式", 2, "", ""),
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
    cell_mapping = {"chiller_mode_summary": "F7"}
    templates = {"chiller_mode_summary": "西区{west_mode}/东区{east_mode}"}
    result = build_cell_value_map(
        cell_mapping=cell_mapping,
        templates=templates,
        hits=hits,
        effective_config=effective_config,
        missing_policy="blank",
    )
    assert result["F7"] == "西区制冷/东区制冷"


def test_chiller_mode_summary_fallback_stop_and_mixed_values() -> None:
    hits = {
        "chiller_mode_1": MetricHit("chiller_mode_1", 4, "1号冷机模式", "4", "", ""),
        "chiller_mode_2": MetricHit("chiller_mode_2", 5, "2号冷机模式", 4.0, "", ""),
        "chiller_mode_3": MetricHit("chiller_mode_3", 6, "3号冷机模式", None, "", ""),
        "chiller_mode_4": MetricHit("chiller_mode_4", 7, "4号冷机模式", "2", "", ""),
        "chiller_mode_5": MetricHit("chiller_mode_5", 8, "5号冷机模式", "停机", "", ""),
        "chiller_mode_6": MetricHit("chiller_mode_6", 9, "6号冷机模式", "1.0", "", ""),
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
    cell_mapping = {"chiller_mode_summary": "F7"}
    templates = {"chiller_mode_summary": "西区{west_mode}/东区{east_mode}"}
    result = build_cell_value_map(
        cell_mapping=cell_mapping,
        templates=templates,
        hits=hits,
        effective_config=effective_config,
        missing_policy="blank",
    )
    assert result["F7"] == "西区停机/东区制冷"


def test_expression_computed_rule_with_intermediate_metric() -> None:
    hits = {
        "number_js": MetricHit("number_js", 12, "蓄水池液位", 6.28, "", ""),
    }
    effective_config = {
        "computed_metric_ops": {
            "number_js_cs": {"op": "number_js/3.14", "params": {}},
        }
    }
    cell_mapping = {"number_js_cs": "H10"}
    templates = {"number_js_cs": "无"}
    result = build_cell_value_map(
        cell_mapping=cell_mapping,
        templates=templates,
        hits=hits,
        effective_config=effective_config,
        missing_policy="blank",
    )
    assert result["H10"] == "2"


def test_cold_metric_render_b_norm_with_th_suffix_when_c_norm_empty() -> None:
    hits = {
        "cold_temp_max": MetricHit(
            "cold_temp_max",
            4,
            "冷通道温度",
            25.04,
            "A-401",
            "",
            "南通阿里保税A区A楼/A楼/二层/包间M1 A-401",
            "A-401-TH-15",
        ),
    }
    cell_mapping = {"cold_temp_max": "B9"}
    templates = {"cold_temp_max": "{value}℃/{b_norm} {c_norm}"}
    result = build_cell_value_map(cell_mapping, templates, hits, missing_policy="blank")
    assert result["B9"] == "25.04℃/A-401-TH-15"
