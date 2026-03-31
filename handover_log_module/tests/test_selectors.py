from __future__ import annotations

from handover_log_module.core.models import RawRow
from handover_log_module.core.selectors import compute_metric_hits


def test_compute_metric_hits():
    rows = [
        RawRow(4, "path E-301", "冷通道 C3-2", "冷通道温度", 25.17, 25.17, "E-301", "C3-2"),
        RawRow(5, "path E-402", "冷通道 C5-1", "冷通道温度", 16.6, 16.6, "E-402", "C5-1"),
        RawRow(6, "path E-402", "冷通道 C6-1", "冷通道湿度", 52.68, 52.68, "E-402", "C6-1"),
        RawRow(7, "path E-201", "冷通道 C6-1", "冷通道湿度", 37.71, 37.71, "E-201", "C6-1"),
        RawRow(8, "", "", "E-317-TR201负载率", 15.09, 15.09, "", ""),
        RawRow(9, "", "", "E-317-UPS-101负载率", 9.5, 9.5, "", ""),
    ]
    rules = {
        "cold_temp_max": {"d_match": ["冷通道温度"], "agg": "max"},
        "cold_temp_min": {"d_match": ["冷通道温度"], "agg": "min"},
        "cold_humi_max": {"d_match": ["冷通道湿度"], "agg": "max"},
        "cold_humi_min": {"d_match": ["冷通道湿度"], "agg": "min"},
        "tr_load_max": {"d_regex": ".*负载率", "group_contains": "TR", "agg": "max"},
        "ups_load_max": {"d_regex": ".*负载率", "group_contains": "UPS", "agg": "max"},
    }
    hits, missing = compute_metric_hits(rows=rows, rules=rules)
    assert not missing
    assert hits["cold_temp_max"].value == 25.17
    assert hits["cold_temp_min"].value == 16.6
    assert hits["cold_humi_max"].value == 52.68
    assert hits["cold_humi_min"].value == 37.71
    assert hits["tr_load_max"].value == 15.09
    assert hits["ups_load_max"].value == 9.5


def test_cold_metrics_prefer_rows_with_cold_channel_context() -> None:
    rows = [
        RawRow(1, "path E-301", "冷通道 C3-2", "温度", 25.0, 25.0, "E-301", "C3-2"),
        RawRow(2, "path E-402", "冷通道 C5-1", "温度", 16.0, 16.0, "E-402", "C5-1"),
        RawRow(3, "path E-124", "室外", "室外温度1", 9.5, 9.5, "", ""),
        RawRow(4, "path E-301", "冷通道 C3-2", "湿度", 51.0, 51.0, "E-301", "C3-2"),
        RawRow(5, "path E-402", "冷通道 C5-1", "湿度", 39.0, 39.0, "E-402", "C5-1"),
        RawRow(6, "path E-124", "室外", "室外湿度1", 6.17, 6.17, "", ""),
    ]
    rules = {
        "cold_temp_min": {"d_match": ["温度"], "agg": "min"},
        "cold_humi_min": {"d_match": ["湿度"], "agg": "min"},
    }
    hits, missing = compute_metric_hits(rows=rows, rules=rules)
    assert not missing
    assert hits["cold_temp_min"].value == 16.0
    assert hits["cold_humi_min"].value == 39.0
