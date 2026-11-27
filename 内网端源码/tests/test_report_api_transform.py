from __future__ import annotations

import pytest

from app.modules.shared_bridge.service.report_api_transform import ReportNoSamples, is_usable_report_sample, normalize_report_rows


@pytest.mark.parametrize("value,usable", [
    (0, True), (" 0.00 ", True), ("-2.5", True), ("运行", True), ("1,234.5", True), ("1,2", False),
    (None, False), (False, False), ("--", False), ("无权限", False), ("超时", False),
    ("NaN", False), (float("inf"), False), ("1_2", False),
])
def test_handover_sample_contract_for_api_and_excel(value, usable):
    assert is_usable_report_sample(value, "handover_log_family") is usable


def test_normalizes_duplicate_report_headers_and_preserves_rows():
    grid = [
        [""],
        ["", "", "", "", "2026-11-04 09:00:00", "2026-11-04 09:05:00"],
        ["", "", "", "", "2026-11-04 09:00:00", "2026-11-04 09:05:00"],
        ["", "A-202", "TH-01", "温度", "24.65", "24.6"],
        ["", "A-202", "TH-01", "湿度", "52.73", "52.7"],
    ]
    rows = normalize_report_rows(
        grid,
        {3: "0_1_2", 4: "0_1_3"},
        expected_point_ids=["0_1_2", "0_1_3"],
        query_start="2026-11-04 09:00:00",
        query_end="2026-11-04 09:05:00",
    )
    assert rows[1] == grid[2]
    assert rows[3:] == grid[3:]


def test_rejects_missing_or_duplicate_points():
    grid = [[], ["", "", "", "", "2026-11-04 09:00:00"], ["", "A", "C", "温度", "1"]]
    with pytest.raises(ValueError, match="测点"):
        normalize_report_rows(grid, {2: "0_1_2"}, expected_point_ids=["0_1_2", "0_1_3"])
    with pytest.raises(ValueError, match="测点"):
        normalize_report_rows(grid + [grid[2]], {2: "0_1_2", 3: "0_1_2"}, expected_point_ids=["0_1_2"])


def test_rejects_time_outside_requested_window_and_empty_data():
    with pytest.raises(ValueError, match="时间"):
        normalize_report_rows(
            [[], ["", "", "", "", "2026-11-03 09:00:00"], ["", "A", "C", "温度", "1"]],
            {2: "0_1_2"}, query_start="2026-11-04 09:00:00", query_end="2026-11-04 10:00:00",
        )
    with pytest.raises(ValueError, match="数据"):
        normalize_report_rows([[], ["", "", "", "", "2026-11-04 09:00:00"]], {})


def test_capacity_fills_only_missing_sample_cells_and_never_overwrites_live_ups_value():
    times = ["2026-11-26 09:00:00", "2026-11-26 09:05:00", "平均值"]
    grid = [[], ["", "", "", "", *times], ["", "C-218", "C-218-UPS-101", "B_输出电压_V", 220, 221, 220.5]]
    ids = ["1_2", "25_0_220_1_601_0", "25_0_220_1_624_0", "25_0_220_1_1002_0", "9_9"]
    filled = []
    rows = normalize_report_rows(grid, {2:"1_2"}, expected_point_ids=ids, zero_fill_building="C楼",
                                 source_family="handover_capacity_report_family", filled_ids=filled)
    assert filled == ids[1:] and rows[3] == grid[2]
    assert [r[3] for r in rows[4:]] == ["电池_正极电压_V", ids[2], "UPS_输出总有功功率_KW", "9_9"]
    assert all(r[4:] == [0, 0, None] for r in rows[4:])
    other = normalize_report_rows(grid, {2:"1_2"}, expected_point_ids=ids, zero_fill_building="A楼",
                                  source_family="handover_capacity_report_family")
    assert all(row[1:4] == ["A楼","未返回测点",pt] for row,pt in zip(other[4:],ids[1:]))


@pytest.mark.parametrize("returned,value", [({},"1"), ({2:"4_5"},"1"), ({2:"1_2",3:"1_2"},"1"),
                                          ({2:"1_2"},None), ({2:"1_2"},"--")])
def test_partial_capacity_policy_rejects_untrusted_response(returned,value):
    row = ["","C楼","UPS","电压",value]
    grid = [[], ["","","","","2026-11-26 09:00:00"], row, row]
    with pytest.raises(ValueError):
        normalize_report_rows(grid, returned, expected_point_ids=["1_2","1_3"],zero_fill_building="C楼",
                              source_family="handover_capacity_report_family")


@pytest.mark.parametrize("ids,values", [
    ({}, ["12"]),                                 # No point IDs
    ({2: "1_2", 3: "1_9"}, ["1", "2"]),          # Extra ID despite an equal count
    ({2: "1_2", 3: "1_2"}, ["1", "2"]),          # Duplicate and missing
    ({2: "1_2"}, ["1", "9"]),                    # A real row lost its hidden ID
    ({1: "1_3", 2: "1_2"}, ["1"]),               # ID on the header
    ({2: "1_2", 3: "1_3"}, ["1"]),               # ID has no body row
    ({2: "1_2"}, [None]), ({2: "1_2"}, ["--"]), ({2: "1_2"}, ["超时"]),
    ({2: "1_2"}, ["读取失败"]), ({2: "1_2"}, ["NaN"]), ({2: "1_2"}, ["inf"]),
])
def test_missing_id_must_be_provable_and_supported_by_a_real_sample(ids, values):
    grid = [[], ["", "", "", "", "2026-11-26 09:00:00", "平均值"],
            *[["", "C楼", "UPS", "测点", value, 42] for value in values]]
    filled = []
    with pytest.raises(ValueError):
        normalize_report_rows(grid, ids, expected_point_ids=["1_2", "1_3"],
                              zero_fill_building="C楼", source_family="monthly_report_family", filled_ids=filled)
    assert filled == []


@pytest.mark.parametrize("family", ["handover_log_family", "handover_capacity_report_family"])
def test_handover_complete_ids_do_not_mask_api_returning_only_error_cells(family):
    grid = [[], ["", "", "", "", "2026-11-26 09:00:00", "平均值"],
            ["", "C楼", "UPS", "电压", "无权限", 25]]
    options = dict(expected_point_ids=["1_2"], zero_fill_building="C楼", source_family=family)
    with pytest.raises(ValueError, match="没有可用的实际采样"):
        normalize_report_rows(grid, {2: "1_2"}, **options)
    grid[2][4] = "0"
    assert normalize_report_rows(grid, {2: "1_2"}, **options)[3][4] == "0"


def test_missing_samples_preserve_live_zero_and_never_turn_discrete_or_meter_into_zero():
    grid = [[], ["", "", "", "", "2026-11-26 09:00:00", "2026-11-26 09:05:00", "平均值"],
            ["", "C楼", "设备甲", "输出功率", "0", "--", "19"],
            ["", "", "", "回风温度", "25"],
            ["", "", "", "运行状态", "", "1"],
            ["", "", "", "输入累计电度", "100", None]]
    ids = ["10_0_100_1_601_0", "10_0_100_1_602_0", "10_0_100_2_603_0", "10_0_100_1_604_0"]
    missing = []
    rows = normalize_report_rows(grid, dict(enumerate(ids, 2)), expected_point_ids=ids,
                                  source_family="handover_capacity_report_family", missing_samples=missing)
    assert [row[4:] for row in rows[3:]] == [["0", 0, "19"], ["25", 0], [None, "1"], ["100", None]]
    assert [x["point_id"] for x in missing] == ids
    assert [(x["device"], x["first_sample_missing"], x["zero_filled"]) for x in missing] == [
        ("设备甲", False, True), ("设备甲", False, True), ("设备甲", True, False), ("设备甲", False, False),
    ]


def test_sample_gaps_preserve_negative_values_and_written_state_tokens():
    grid = [[], ["", "", "", "", "2026-11-26 09:00:00", "2026-11-26 09:05:00"],
            ["", "A楼", "设备甲", "功率", "-1", "-Infinity"],
            ["", "", "", "反馈", "运行", "n/a"]]
    missing = []
    rows = normalize_report_rows(grid, {}, source_family="handover_log_family", missing_samples=missing)
    assert [row[4:] for row in rows[3:]] == [["-1", 0], ["运行", None]]
    assert [item["zero_filled"] for item in missing] == [True, False]


@pytest.mark.parametrize("family", ["branch_power_family", "branch_current_family", "branch_switch_family",
                                     "building_full_cabinet_power_family", "monthly_report_family"])
def test_full_day_requires_a_real_reading_in_business_hours(family):
    from datetime import datetime, timedelta

    day = datetime(2026, 11, 26)
    times = [(day + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S") for i in range(-1, 27)]
    before, after, midnight = 0, 27, 25
    labels = ["房间", "机列", "支路"] if family.startswith("branch_") else ["", "E楼", "设备", "功率"]
    values = ["0"] * len(times)
    for index in (before, midnight, after):
        values[index] = "--"
    mixed = list(values)
    mixed[1] = "--"  # Business date 00:00.
    gaps = []
    rows = normalize_report_rows(
        [[""] * len(labels) + times, [*labels, *values], [*labels, *mixed]],
        {1: "1_2", 2: "2_3"}, source_family=family, missing_samples=gaps, business_date="2026-11-26",
        query_start="2026-11-25 23:50:00", query_end="2026-11-27 02:20:00", time_tolerance_sec=3600,
    )
    assert [item["count"] for item in gaps] == [3, 4]
    assert rows[3][len(labels) + 1] == "0"
    assert rows[3][-1] == (None if family == "monthly_report_family" else 0)
    padding_only = ["--"] * len(times)
    padding_only[before] = padding_only[after] = "1"
    options = dict(source_family=family, missing_samples=[], business_date="2026-11-26",
                   query_start="2026-11-25 23:50:00", query_end="2026-11-27 02:20:00", time_tolerance_sec=3600)
    with pytest.raises(ReportNoSamples):
        normalize_report_rows([[""] * len(labels) + times, labels + padding_only], {1: "1_2"}, **options)
    padding_only[midnight if family == "monthly_report_family" else 1] = "0"
    normalize_report_rows([[""] * len(labels) + times, labels + padding_only], {1: "1_2"}, **options)
