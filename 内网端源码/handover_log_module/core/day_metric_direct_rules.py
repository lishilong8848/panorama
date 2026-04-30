from __future__ import annotations

from typing import Any, Dict, List


DAY_METRIC_DIRECT_BUILDINGS: List[str] = ["A楼", "B楼", "C楼", "D楼", "E楼"]


DAY_METRIC_DIRECT_TYPE_DEFINITIONS: List[Dict[str, str]] = [
    {"name": "总负荷（KW）", "metric_id": "city_power"},
    {"name": "IT总负荷（KW）", "metric_id": "it_power"},
    {"name": "室外湿球最高温度（℃）", "metric_id": "wet_bulb"},
    {"name": "冷水系统供水最高温度（℃）", "metric_id": "chilled_supply_temp_max"},
    {"name": "蓄水池后备最短时间（H）", "metric_id": "water_backup_shortest"},
    {"name": "蓄冷罐后备最短时间（min）", "metric_id": "tank_backup_min"},
    {"name": "供油可用时长（H）", "metric_id": "oil_backup_time"},
    {"name": "冷通道最高温度（℃）", "metric_id": "cold_temp_max"},
    {"name": "冷通道最高湿度（%）", "metric_id": "cold_humi_max"},
    {"name": "变压器负载率（MAX）", "metric_id": "tr_load_max"},
    {"name": "UPS负载率（MAX）", "metric_id": "ups_load_max"},
    {"name": "HVDC负载率（MAX）", "metric_id": "hvdc_load_max"},
]


def _rule(
    metric_id: str,
    *,
    rule_type: str = "direct",
    agg: str = "first",
    d_keywords: List[str] | None = None,
    d_regex: str = "",
    group_contains: str = "",
    computed_op: str = "",
) -> Dict[str, Any]:
    return {
        "id": metric_id,
        "enabled": True,
        "target_cell": "",
        "rule_type": rule_type,
        "d_keywords": list(d_keywords or []),
        "match_mode": "contains_casefold",
        "agg": agg,
        "template": "{value}",
        "computed_op": computed_op,
        "params": {},
        "d_equals": "",
        "d_contains": "",
        "d_regex": d_regex,
        "group_contains": group_contains,
        "use_b": False,
        "use_c": False,
    }


DAY_METRIC_DIRECT_DEFAULT_RULE_ROWS: List[Dict[str, Any]] = [
    _rule("city_power", d_keywords=["市电进线总功率", "市电总功率", "D楼总功率"]),
    _rule("it_power", d_keywords=["IT总功率", "IT功率", "二三四层功率和", "IT功率和"]),
    _rule("wet_bulb", d_keywords=["室外湿球温度"]),
    _rule("oil_backup_time", d_keywords=["油量后备时间", "燃油后备时间"]),
    _rule("cold_temp_max", rule_type="aggregate", agg="max", d_keywords=["冷通道温度", "温度"]),
    _rule("cold_humi_max", rule_type="aggregate", agg="max", d_keywords=["冷通道湿度", "湿度"]),
    _rule("tr_load_max", rule_type="aggregate", agg="max", d_keywords=["负载率"], d_regex=".*负载率", group_contains="TR"),
    _rule("ups_load_max", rule_type="aggregate", agg="max", d_keywords=["负载率"], d_regex=".*负载率", group_contains="UPS"),
    _rule(
        "hvdc_load_max",
        rule_type="aggregate",
        agg="max",
        d_keywords=["HVDC", "负载率"],
        d_regex=".*(HVDC.*负载率|负载率.*HVDC).*",
        group_contains="HVDC",
    ),
    _rule("chilled_supply_temp_max", rule_type="aggregate", agg="max", d_keywords=["冷冻水供水温度"]),
    _rule("west_tank_time", d_keywords=["西区蓄冷罐放冷时间"]),
    _rule("east_tank_time", d_keywords=["东区蓄冷罐放冷时间"]),
    _rule("water_pool_backup_time", d_keywords=["水池后备时间"]),
    _rule("ring_124", d_keywords=["124-冷冻水供水环管温度", "124-冷冻水供水温度", "西区冷冻水供水环管温度"]),
    _rule("ring_150", d_keywords=["150-冷冻水供水环管温度", "150-冷冻水供水温度", "东区冷冻水供水环管温度"]),
]


DAY_METRIC_DIRECT_BUILDING_RULE_ROWS: Dict[str, List[Dict[str, Any]]] = {
    "A楼": [
        _rule("ch_1", d_keywords=["西区水池液位m"]),
        _rule("ch_2", d_keywords=["东区水池液位m"]),
        _rule("water_backup_shortest", rule_type="computed", computed_op="(ch_1+ch_2)*150.6/264*24"),
        _rule("ring_124", d_keywords=["B区二次冷冻水供水温度4(南)"]),
        _rule("ring_150", d_keywords=["A区二次冷冻水供水温度5(南)"]),
    ],
    "B楼": [
        _rule("ch_1", d_keywords=["东区水池液位m"]),
        _rule("ch_2", d_keywords=["西区水池液位m"]),
        _rule("water_backup_shortest", rule_type="computed", computed_op="(ch_1+ch_2)*150.6/264*24"),
        _rule("ring_124", d_keywords=["制冷站冷冻水供水环管02水道温度"]),
        _rule("ring_150", d_keywords=["制冷站冷冻水供水环管02水道温度"]),
    ],
    "C楼": [],
    "D楼": [
        _rule("ring_124", d_keywords=["D-124-冷冻水供水主环路温度_1"]),
        _rule("ring_150", d_keywords=["D-150-冷冻水供水主环路温度_2"]),
    ],
    "E楼": [
        _rule("wet_bulb", d_keywords=["E-124-DDC-100_室外温度1"]),
        _rule("ring_124", d_keywords=["E-124-DDC-100_冷冻水供水环管温度_1"]),
        _rule("ring_150", d_keywords=["E-150-DDC-100_冷冻水供水环管温度_1"]),
    ],
}
