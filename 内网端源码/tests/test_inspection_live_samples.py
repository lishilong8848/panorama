from datetime import datetime, timedelta
import pytest
from handover_log_module.repository.inspection_report_api import _first_sample


def test_live_selects_latest_actual_timestamp_preserves_legacy_first():
    start = datetime(2026, 11, 18, 10)
    end = start + timedelta(minutes=20)
    grid = [["", "位置", "设备", "测点", "2026-11-18 10:00:00", "2026-11-18 10:15:00", "2026-11-18 10:05:00"],
            ["", "机房", "设备1", "温度", "20", "23", "21"]]
    ids = []
    old, rows = _first_sample(grid, start, end)
    assert old == "2026-11-18 10:00:00" and rows[0][3] == "20"
    latest, rows = _first_sample(grid, start, end, latest=True, ids={1: "7_0_1"}, sample_ids=ids)
    assert latest == "2026-11-18 10:15:00" and rows[0][3] == "23"
    assert ids == ["7_0_1"]


def test_live_does_not_use_future_column_or_fake_sample_time():
    start = datetime(2026, 11, 18, 10)
    grid = [["", "位置", "设备", "测点", "2026-11-18 10:25:00"], ["", "机房", "设备1", "温度", "23"]]
    with pytest.raises(ValueError):
        _first_sample(grid, start, start+timedelta(minutes=20), latest=True)
