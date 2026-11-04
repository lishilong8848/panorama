from __future__ import annotations

import pytest

from app.modules.shared_bridge.service.report_api_transform import normalize_report_rows


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
