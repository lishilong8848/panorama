from __future__ import annotations

from app.modules.report_pipeline.core.metrics_math import (
    resolve_upload_date_from_runtime,
    round_metric_value,
)


def test_round_metric_value_for_pue_keeps_three_decimals():
    assert round_metric_value("PUE", 1.23456) == 1.235


def test_round_metric_value_for_other_metrics_keeps_six_decimals():
    assert round_metric_value("PLF", 1.23456789) == 1.234568


def test_resolve_upload_date_from_runtime_prefers_runtime_start():
    cfg = {
        "_runtime": {"time_range_start": "2026-03-08 00:00:00"},
        "download": {"time_range_mode": "custom", "start_time": "2025-01-01 00:00:00"},
    }
    assert resolve_upload_date_from_runtime(cfg) == "2026-03-08"


def test_resolve_upload_date_from_runtime_fallback_to_custom_start():
    cfg = {"download": {"time_range_mode": "custom", "start_time": "2026-01-02 12:00:00"}}
    assert resolve_upload_date_from_runtime(cfg) == "2026-01-02"
