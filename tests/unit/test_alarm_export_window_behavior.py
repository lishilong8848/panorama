from __future__ import annotations

from datetime import datetime

from app.modules.alarm_export.service.alarm_export_service import AlarmExportService


def test_build_previous_and_current_month_window_mid_month():
    start_dt, end_dt = AlarmExportService._build_previous_and_current_month_window(
        datetime(2026, 3, 18, 14, 23, 11)
    )

    assert start_dt == datetime(2026, 2, 1, 0, 0, 0)
    assert end_dt == datetime(2026, 3, 18, 14, 23, 11)


def test_build_previous_and_current_month_window_year_boundary():
    start_dt, end_dt = AlarmExportService._build_previous_and_current_month_window(
        datetime(2026, 1, 5, 9, 0, 0)
    )

    assert start_dt == datetime(2025, 12, 1, 0, 0, 0)
    assert end_dt == datetime(2026, 1, 5, 9, 0, 0)


def test_resume_matches_same_window_scope_key_even_when_window_end_changes():
    state = {
        "mode": "prod_sites",
        "window_mode": "previous_and_current_month_to_now",
        "window_scope_key": "2026-03",
        "status": "failed",
        "window_start": "2026-02-01 00:00:00",
        "window_end": "2026-03-18 14:00:00",
    }

    assert AlarmExportService._resume_matches_window(
        state,
        mode="prod_sites",
        window_mode="previous_and_current_month_to_now",
        window_scope_key="2026-03",
    ) is True


def test_resume_becomes_stale_when_month_scope_changes():
    state = {
        "mode": "prod_sites",
        "window_mode": "previous_and_current_month_to_now",
        "window_scope_key": "2026-02",
        "status": "failed",
    }

    assert AlarmExportService._resume_matches_window(
        state,
        mode="prod_sites",
        window_mode="previous_and_current_month_to_now",
        window_scope_key="2026-03",
    ) is False
