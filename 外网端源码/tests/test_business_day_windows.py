from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import openpyxl
import pytest

from app.bootstrap import container as container_module
from app.bootstrap.container import AppContainer
from app.modules.report_pipeline.service.calc_io_runtime import build_results_from_file_items
from app.modules.report_pipeline.service.calc_source_runtime import _business_day_data_columns
from app.modules.scheduler.service.apscheduler_orchestrator import ApschedulerSchedulerFacade
from app.modules.shared_bridge.service import shared_bridge_runtime_service as bridge_runtime_module
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService


def test_monthly_columns_keep_business_day_and_next_midnight_only():
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    headers = [
        "2026-08-23 23:50:00",
        "2026-08-24 00:00:00",
        "2026-08-24 12:00:00",
        "2026-08-25 00:00:00",
        "2026-08-25 00:10:00",
    ]
    for col, value in enumerate(headers, start=5):
        worksheet.cell(2, col).value = value

    columns = _business_day_data_columns(
        worksheet,
        header_row=2,
        data_start_col=5,
        data_end_col=9,
        business_date="2026-08-24",
    )

    assert columns == [6, 7, 8]


def test_file_item_business_date_reaches_monthly_calculator(tmp_path):
    source_file = tmp_path / "A楼.xlsx"
    openpyxl.Workbook().save(source_file)
    calls = []

    def calculate(file_path, building, business_date):
        calls.append((file_path, building, business_date))
        return SimpleNamespace(missing_metrics=[])

    build_results_from_file_items(
        [{"building": "A楼", "file_path": str(source_file), "upload_date": "2026-08-24"}],
        calculate_monthly_report=calculate,
        emit_log=lambda _line: None,
    )

    assert calls == [(str(source_file), "A楼", "2026-08-24")]


def test_monthly_columns_reject_missing_next_midnight_boundary():
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.cell(2, 5).value = "2026-08-24 00:00:00"
    worksheet.cell(2, 6).value = "2026-08-24 23:00:00"

    with pytest.raises(ValueError, match="缺少次日00:00"):
        _business_day_data_columns(
            worksheet,
            header_row=2,
            data_start_col=5,
            data_end_col=6,
            business_date="2026-08-24",
        )


def test_daily_interval_offset_240_means_0400():
    facade = SimpleNamespace(
        cfg={"interval_minutes": 1440, "minute_offset": 240, "align_to_wall_clock": True},
        state={},
    )

    next_run = ApschedulerSchedulerFacade._next_interval_run_after(
        facade,
        datetime(2026, 8, 24, 3, 59, 59),
    )

    assert next_run == datetime(2026, 8, 24, 4, 0, 0)


def test_legacy_external_schedule_defaults_are_migrated(monkeypatch):
    captured = []

    def fake_facade(**kwargs):
        captured.append(kwargs)
        return kwargs

    monkeypatch.setattr(container_module, "ApschedulerSchedulerFacade", fake_facade)
    fake_container = SimpleNamespace(
        runtime_config={"scheduler": {"run_time": "00:10:00"}},
        config={
            "features": {
                "branch_power_upload": {
                    "scheduler": {"interval_minutes": 1440, "minute_offset": 30}
                }
            }
        },
        scheduler_callback=None,
        branch_power_upload_scheduler_callback=None,
        _scheduler_run_callback=lambda _source: (True, "ok"),
        _branch_power_upload_scheduler_run_callback=lambda _source: (True, "ok"),
        _runtime_state_root_text=lambda: "runtime_state",
        add_system_log=lambda _line: None,
        ensure_scheduler_orchestrator=lambda: object(),
        _job_busy_for_feature_prefixes=lambda *_prefixes: (lambda: False),
    )

    AppContainer._build_scheduler(fake_container)
    AppContainer._build_branch_power_upload_scheduler(fake_container)

    assert captured[0]["scheduler_cfg"]["run_time"] == "02:30:00"
    assert captured[1]["scheduler_cfg"]["minute_offset"] == 240


def test_monthly_source_selection_accepts_previous_business_day_only(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 8, 25, 2, 30, 0)

    monkeypatch.setattr(bridge_runtime_module, "datetime", FixedDateTime)

    def selection_for(bucket_key):
        entries = [
            {
                "building": building,
                "bucket_key": bucket_key,
                "status": "ready",
                "file_path": f"{building}.xlsx",
            }
            for building in ("A楼", "B楼", "C楼", "D楼", "E楼")
        ]
        service = SimpleNamespace(
            _http_source_index_entries=lambda **_kwargs: entries,
            _http_source_cache_buildings=lambda buildings: list(buildings or []),
            _http_source_bucket_dt=SharedBridgeRuntimeService._http_source_bucket_dt,
        )
        return SharedBridgeRuntimeService._http_latest_source_cache_selection(
            service,
            source_family="monthly_report_family",
            buildings=["A楼", "B楼", "C楼", "D楼", "E楼"],
            max_selection_age_hours=36.0,
        )

    assert selection_for("2026-08-24")["can_proceed"] is True
    assert selection_for("2026-08-23")["can_proceed"] is False
