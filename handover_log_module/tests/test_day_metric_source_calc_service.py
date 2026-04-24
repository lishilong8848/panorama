from __future__ import annotations

import pytest

from handover_log_module.core.models import RawRow
from handover_log_module.service import day_metric_source_calc_service as module
from handover_log_module.service.day_metric_source_calc_service import DayMetricSourceCalcService


def _row(row_index: int, d_name: str, value: float | None, *, b_text: str = "", c_text: str = "") -> RawRow:
    return RawRow(
        row_index=row_index,
        b_text=b_text,
        c_text=c_text,
        d_name=d_name,
        e_raw=value,
        value=value,
        b_norm=b_text,
        c_norm=c_text,
    )


def test_calculate_uses_ch_formula_for_a_building(monkeypatch) -> None:
    rows = [
        _row(1, "市电进线总功率", 100.0, b_text="A-401"),
        _row(2, "IT总功率", 80.0, b_text="A-402"),
        _row(3, "室外湿球温度", 24.0),
        _row(4, "冷通道温度", 31.2, b_text="E-301", c_text="C3-2"),
        _row(5, "冷通道湿度", 55.0, b_text="E-301", c_text="C3-2"),
        _row(6, "TR-1负载率", 70.0),
        _row(7, "UPS-3负载率", 60.0),
        _row(8, "HVDC-1负载率", 50.0),
        _row(9, "油量后备时间", 36.0, b_text="燃油系统"),
        _row(10, "冷冻水供水温度", 7.5),
        _row(11, "西区蓄冷罐放冷时间", 30.0, b_text="西区蓄冷罐"),
        _row(12, "东区蓄冷罐放冷时间", 20.0, b_text="东区蓄冷罐"),
        _row(13, "西区水池液位m", 1.0),
        _row(14, "东区水池液位m", 2.0),
        _row(15, "B区二次冷冻水供水温度4(南)", 8.2),
        _row(16, "A区二次冷冻水供水温度5(南)", 8.4),
    ]
    monkeypatch.setattr(module, "load_rows", lambda **kwargs: rows)

    result = DayMetricSourceCalcService({}).calculate(
        building="A楼",
        duty_date="2026-04-12",
        data_file="demo.xlsx",
    )

    assert result["resolved_metrics"]["city_power"] == 100.0
    assert result["resolved_metrics"]["tank_backup_min"] == 20.0
    assert result["resolved_metrics"]["water_backup_shortest"] == pytest.approx((1.0 + 2.0) * 150.6 / 264 * 24)
    assert result["metric_origin_context"]["by_metric_id"]["tank_backup_min"]["metric_key"] == "east_tank_time"


def test_calculate_uses_water_pool_backup_for_c_building(monkeypatch) -> None:
    rows = [
        _row(1, "水池后备时间", 18.5, b_text="负载率"),
        _row(2, "西区蓄冷罐放冷时间", 40.0),
        _row(3, "东区蓄冷罐放冷时间", 35.0),
    ]
    monkeypatch.setattr(module, "load_rows", lambda **kwargs: rows)

    result = DayMetricSourceCalcService({}).calculate(
        building="C楼",
        duty_date="2026-04-12",
        data_file="demo.xlsx",
    )

    assert result["resolved_metrics"]["water_backup_shortest"] == 18.5
    assert result["resolved_metrics"]["tank_backup_min"] == 35.0
    assert result["metric_origin_context"]["by_metric_id"]["water_backup_shortest"]["metric_key"] == "water_pool_backup_time"


def test_calculate_uses_e_building_wet_bulb_override(monkeypatch) -> None:
    rows = [
        _row(1, "E-124-DDC-100_室外湿度1", 19.6),
        _row(2, "室外湿球温度", 25.3),
    ]
    monkeypatch.setattr(module, "load_rows", lambda **kwargs: rows)

    result = DayMetricSourceCalcService({}).calculate(
        building="E楼",
        duty_date="2026-04-12",
        data_file="demo.xlsx",
    )

    assert result["resolved_metrics"]["wet_bulb"] == 19.6
    assert result["metric_origin_context"]["by_metric_id"]["wet_bulb"]["d_name"] == "E-124-DDC-100_室外湿度1"


def test_calculate_rejects_source_when_e_column_has_no_data(monkeypatch) -> None:
    rows = [
        RawRow(
            row_index=4,
            b_text="A-401",
            c_text="",
            d_name="市电进线总功率",
            e_raw="",
            value=None,
            b_norm="A-401",
            c_norm="",
        ),
        RawRow(
            row_index=5,
            b_text="A-402",
            c_text="",
            d_name="IT总功率",
            e_raw=None,
            value=None,
            b_norm="A-402",
            c_norm="",
        ),
    ]
    monkeypatch.setattr(module, "load_rows", lambda **kwargs: rows)

    with pytest.raises(ValueError, match="E列无有效数据"):
        DayMetricSourceCalcService({}).calculate(
            building="A楼",
            duty_date="2026-04-12",
            data_file="empty.xlsx",
        )
