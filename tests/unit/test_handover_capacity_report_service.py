from handover_log_module.core.models import RawRow
from handover_log_module.service.capacity_report_common import build_capacity_cells_with_config
from handover_log_module.service.handover_capacity_report_service import (
    HandoverCapacityReportService,
    _build_fixed_header_cells,
)


def test_build_fixed_header_cells_for_a_building() -> None:
    cells = _build_fixed_header_cells("A楼")

    assert cells["A1"] == "世纪互联南通数据中心A栋FM运维交接班重要事项"
    assert cells["E5"] == "A楼"
    assert cells["G16"] == "A楼"
    assert cells["G17"] == "A楼"
    assert cells["G18"] == "A楼"
    assert cells["S15"] == "A楼"
    assert cells["S16"] == "A楼"
    assert cells["S17"] == "A楼"
    assert cells["S18"] == "A楼"
    assert cells["A20"] == "A楼"
    assert cells["A65"] == "A楼容量一览表"
    assert cells["O55"] == "A楼能耗一览"


def test_build_capacity_cells_include_direct_source_values_and_zone_capacity_formula() -> None:
    rows = [
        RawRow(1, "", "西区 101 1号冷机", "冷机_冷冻水出水温度", "8", 8.0),
        RawRow(2, "", "西区 102 2号冷机", "冷机_冷冻水出水温度", "6", 6.0),
        RawRow(3, "", "西区", "板换冷却水进水温度", "10", 10.0),
        RawRow(4, "", "西区", "板交冷冻供水温度", "14", 14.0),
        RawRow(5, "", "西区", "一次总流量", "100", 100.0),
        RawRow(6, "", "东区", "板换冷却水进水温度", "12", 12.0),
        RawRow(7, "", "东区", "板交冷冻供水温度", "18", 18.0),
        RawRow(8, "", "东区", "一次总流量", "120", 120.0),
        RawRow(9, "", "", "蓄水池总储水量", "88.6", 88.6),
        RawRow(10, "", "", "油量", "55.2", 55.2),
    ]
    context = {
        "building": "A楼",
        "duty_shift": "day",
        "capacity_rows": rows,
        "running_units": {
            "west": [
                {"unit": 1, "mode_text": "预冷"},
                {"unit": 2, "mode_text": "制冷"},
                {"unit": 3, "mode_text": "板换"},
            ],
            "east": [
                {"unit": 4, "mode_text": "板换"},
            ],
        },
    }

    values = build_capacity_cells_with_config(context)

    assert values["AC29"] == "88.6"
    assert values["U16"] == "55.2"
    assert values["D22"] == "466.67"
    assert values["Q22"] == "840"


def test_capacity_overlay_values_include_ac24_and_track_d8(monkeypatch) -> None:
    service = HandoverCapacityReportService({})
    monkeypatch.setattr(
        service,
        "_query_capacity_water_summary",
        lambda **kwargs: {"month_total": "123.4", "latest_daily_total": "5.6"},
    )
    monkeypatch.setattr(
        service,
        "_fetch_weather_payload_for_duty_date",
        lambda **kwargs: {"text": "多云", "humidity": "96%"},
    )

    values = service._build_capacity_overlay_values(
        building="A楼",
        duty_date="2026-04-11",
        handover_cells={
            "D8": "9.8",
            "H6": "12",
            "F8": "西区30/东区40",
            "B6": "1.23",
            "D6": "2000",
            "F6": "1200",
            "B13": "1000",
            "D13": "800",
        },
        emit_log=lambda _msg: None,
    )

    assert values["AC24"] == "9.8"
    assert values["X2"] == "96%"
    assert "D8" in service.tracked_cells()


def test_d_building_cooling_tower_out_temp_alias_fills_f30() -> None:
    rows = [
        RawRow(1, "", "西区 101 1号冷机", "冷却塔出水温度", "27.5", 27.5),
    ]
    context = {
        "building": "D楼",
        "duty_shift": "day",
        "capacity_rows": rows,
        "running_units": {
            "west": [
                {"unit": 1, "mode_text": "制冷"},
            ],
            "east": [],
        },
    }

    values = build_capacity_cells_with_config(context)

    assert values["F30"] == "27.5"


def test_e_building_plate_mode_skips_chiller_fields_but_keeps_fan_and_pump_values() -> None:
    rows = [
        RawRow(1, "", "西区 101 1号冷机", "冷机_电流百分比", "45", 45.0),
        RawRow(2, "", "西区 101 1号冷机", "冷机_冷冻水出水温度", "7.2", 7.2),
        RawRow(3, "", "西区 101 1号冷机", "冷机_冷却水进水温度", "27.1", 27.1),
        RawRow(4, "", "西区 101 1号冷机", "冷凝器压力", "100", 100.0),
        RawRow(5, "", "西区 101 1号冷机", "蒸发器小温差", "2.1", 2.1),
        RawRow(6, "", "西区 101 1号冷机", "冷凝器小温差", "1.8", 1.8),
        RawRow(7, "", "西区 101 1号冷机", "冷塔1#风机频率反馈", "33", 33.0),
        RawRow(8, "", "西区 101 1号冷机", "冷塔2#风机频率反馈", "34", 34.0),
        RawRow(9, "", "西区 101 1号冷机", "冷却泵频率反馈", "35", 35.0),
        RawRow(10, "", "西区 101 1号冷机", "一次冷冻泵频率反馈", "36", 36.0),
        RawRow(11, "", "西区", "板换冷却水进水温度", "10", 10.0),
    ]
    context = {
        "building": "E楼",
        "duty_shift": "day",
        "capacity_rows": rows,
        "running_units": {
            "west": [{"unit": 1, "mode_text": "板换"}],
            "east": [],
        },
    }

    values = build_capacity_cells_with_config(context)

    assert "D25" not in values
    assert "D27" not in values
    assert "D28" not in values
    assert "D29" not in values
    assert "D30" not in values
    assert "D31" not in values
    assert values["F27"] == "33"
    assert values["G27"] == "34"
    assert values["I26"] == "35"
    assert values["J26"] == "36"


def test_hvdc_missing_r_and_u_fill_zero() -> None:
    context = {
        "capacity_rows": [],
        "running_units": {},
        "template_snapshot": {
            "hvdc_entries": [
                {"row": 67, "identifier": "E-317-HVDC-252", "search_tokens": ["E-317-HVDC-252"]},
            ],
        },
    }

    values = build_capacity_cells_with_config(context)

    assert values["R67"] == "0"
    assert values["U67"] == "0"


def test_aircon_matrix_mapping_fills_last_two_targets() -> None:
    rows = [
        RawRow(1, "南通阿里保税A区E楼/E楼/四层/空调区1 E-412", "E-412-CRAHB-A_电量仪", "总_有功功率", "2.23", 2.23),
        RawRow(2, "南通阿里保税A区E楼/E楼/四层/空调区2 E-411", "E-411-CRAHB-A_电量仪", "总_有功功率", "0.8", 0.8),
        RawRow(3, "南通阿里保税A区E楼/E楼/四层/空调区3 E-441", "E-441-CRAHB-A_电量仪", "总_有功功率", "0", 0.0),
        RawRow(4, "南通阿里保税A区E楼/E楼/四层/空调区4 E-440", "E-440-CRAHB-A_电量仪", "总_有功功率", "1.36", 1.36),
    ]
    context = {
        "capacity_rows": rows,
        "running_units": {},
        "template_snapshot": {"building_code": "E"},
    }

    values = build_capacity_cells_with_config(context)

    assert values["AE172"] == "2.23"
    assert values["AE182"] == "0.8"
    assert values["AE152"] == "0"
    assert values["AE162"] == "1.36"
