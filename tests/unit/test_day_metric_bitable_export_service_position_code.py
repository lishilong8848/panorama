from __future__ import annotations

from handover_log_module.service.day_metric_bitable_export_service import DayMetricBitableExportService


def test_day_metric_total_power_position_code_uses_pue_hint() -> None:
    assert (
        DayMetricBitableExportService._compose_position_code(
            {"metric_key": "city_power", "c_norm": "pue"},
            type_item={"name": "总负荷（KW）", "source": "cell", "cell": "D6"},
        )
        == "pue能耗数据计算"
    )
    assert (
        DayMetricBitableExportService._compose_position_code(
            {"metric_key": "it_power", "c_norm": "pue"},
            type_item={"name": "IT总负荷（KW）", "source": "cell", "cell": "F6"},
        )
        == "pue能耗数据计算"
    )


def test_day_metric_water_pool_position_code_falls_back_to_load_ratio() -> None:
    assert (
        DayMetricBitableExportService._compose_position_code(
            {"metric_key": "water_pool_backup_time"},
            type_item={"name": "蓄水池后备最短时间（H）", "source": "cell", "cell": "D8"},
        )
        == "负载率"
    )


def test_day_metric_oil_position_code_uses_fuel_system_label() -> None:
    assert (
        DayMetricBitableExportService._compose_position_code(
            {"metric_key": "oil_backup_time", "d_name": "燃油后备时间参数"},
            type_item={"name": "供油可用时长（H）", "source": "cell", "cell": "H6"},
        )
        == "燃油系统"
    )


def test_day_metric_cold_channel_position_code_strips_channel_suffix() -> None:
    assert (
        DayMetricBitableExportService._compose_position_code(
            {"metric_key": "cold_temp_max", "c_norm": "A-202-TH-11_冷通道"},
            type_item={"name": "冷通道最高温度（℃）", "source": "metric", "metric_id": "cold_temp_max"},
        )
        == "A-202-TH-11"
    )
    assert (
        DayMetricBitableExportService._compose_position_code(
            {"metric_key": "cold_humi_max", "b_norm": "A-202", "c_norm": "TH-23_冷通道"},
            type_item={"name": "冷通道最高湿度（%）", "source": "metric", "metric_id": "cold_humi_max"},
        )
        == "A-202 TH-23"
    )
