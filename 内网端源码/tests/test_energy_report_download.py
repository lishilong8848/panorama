import asyncio
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import openpyxl
import pytest

from app.config.energy_report_download_config import (
    FAMILY_PUE_CONFIRMATION,
    FAMILY_UTILITY_FEEDER_DEVIATION,
    FEEDER_DEVIATION_TEMPLATES,
    PUE_CONFIRMATION_TEMPLATES,
    energy_report_period,
)
from app.modules.shared_bridge.service.internal_runtime_status_presenter import build_internal_runtime_summary
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_source_cache_service import SharedSourceCacheService
from handover_log_module.repository.download_gateway import DownloadGateway
from handover_log_module.service.handover_download_service import HandoverDownloadService


PUE = FAMILY_PUE_CONFIRMATION
FEEDER = FAMILY_UTILITY_FEEDER_DEVIATION


@pytest.mark.parametrize("family", [PUE, FEEDER])
def test_default_period_rolls_over_year(family):
    assert energy_report_period(family, now=datetime(2026, 1, 1)) == "2025-12"


@pytest.mark.parametrize(("family", "value"), [(PUE, "2026-1"), (PUE, "2026-13"), (FEEDER, "2026-10-01")])
def test_invalid_period_rejected(family, value):
    with pytest.raises(ValueError):
        energy_report_period(family, value)


@pytest.mark.parametrize(("family", "template", "period", "start", "end", "scale"), [
    (PUE, PUE_CONFIRMATION_TEMPLATES["E楼"], "2026-10", "2026-10", "2026-10", ""),
    (FEEDER, FEEDER_DEVIATION_TEMPLATES["E楼"], "2026-10", "2026-10-01 00:00:00", "2026-11-01 00:00:00", "小时"),
    (FEEDER, FEEDER_DEVIATION_TEMPLATES["E楼"], "2025-12", "2025-12-01 00:00:00", "2026-01-01 00:00:00", "小时"),
    (FEEDER, FEEDER_DEVIATION_TEMPLATES["E楼"], "2028-02", "2028-02-01 00:00:00", "2028-03-01 00:00:00", "小时"),
])
def test_download_configuration_and_query_fields(monkeypatch, family, template, period, start, end, scale):
    service = HandoverDownloadService({})
    run = Mock(return_value={})
    clone = Mock(return_value=SimpleNamespace(run=run))
    monkeypatch.setattr(service, "_clone_with_download_config", clone)
    service.run_energy_report_only(source_family=family, building="E楼", period=period)
    assert clone.call_args.args[0]["template_name"] == template
    assert clone.call_args.args[0]["scale_label"] == scale
    assert run.call_args.kwargs["start_time"] == start
    assert run.call_args.kwargs["end_time"] == end

    gateway = DownloadGateway({})
    gateway._fill_text_input_by_widget_or_label = AsyncMock()
    gateway._fill_optional_sheet_name = AsyncMock()
    gateway._select_combo_by_widget_or_label = AsyncMock()
    gateway._wait_query_loading_started = AsyncMock()
    button = SimpleNamespace(wait_for=AsyncMock(), click=AsyncMock())
    frame = SimpleNamespace(locator=Mock(return_value=button))
    asyncio.run(gateway._fill_query_conditions(
        frame, start_time=start, end_time=end, scale_label=scale,
        template_name=template, start_end_visible_timeout_ms=1000,
    ))
    assert [call.kwargs["label_text"] for call in gateway._fill_text_input_by_widget_or_label.call_args_list] == (
        ["开始时间"] if family == PUE else ["开始时间", "结束时间"]
    )
    gateway._fill_optional_sheet_name.assert_not_awaited()
    assert gateway._select_combo_by_widget_or_label.await_count == (0 if family == PUE else 1)
    button.click.assert_awaited_once()
    assert all("contains(normalize-space(@title)" not in selector for selector in gateway._report_template_selectors(template))


@pytest.mark.parametrize("family", [PUE, FEEDER])
def test_monthly_file_is_published_indexed_and_reused(monkeypatch, tmp_path, family):
    root = tmp_path / "share"
    root.mkdir()
    store = SharedBridgeStore(root)
    store.ensure_ready()
    source = tmp_path / "sample.xlsx"
    book = openpyxl.Workbook()
    book.active.append(["2026-10", "EA118-A栋"])
    book.save(source)
    book.close()
    cache = SharedSourceCacheService(runtime_config={
        "deployment": {"role_mode": "internal"},
        "shared_bridge": {"enabled": True, "root_dir": str(root)},
        "internal_source_cache": {"enabled": True},
        "internal_source_sites": [{"building": "A楼", "enabled": True}],
    }, store=store, emit_log=lambda _: None, download_browser_pool=object())
    monkeypatch.setattr(
        HandoverDownloadService, "run_energy_report_only",
        Mock(return_value={"success_files": [{"file_path": str(source)}], "start_time": "2026-10"}),
    )
    target = cache._resolve_latest_refresh_target(source_family=family, target_bucket_key="2026-10")
    target["fill_func"](building="A楼", bucket_key="2026-10", emit_log=lambda _: None)
    existing = cache._existing_latest_refresh_result(
        source_family=family, building="A楼", bucket_key="2026-10", allow_directory_scan=False,
    )
    assert existing["reason"] == "already_ready"
    assert (root / existing["relative_path"]).read_bytes() == source.read_bytes()
    assert "202610--月报" in existing["relative_path"]
    assert cache._existing_latest_refresh_result(
        source_family=family, building="A楼", bucket_key="2026-09", allow_directory_scan=False,
    ) is None
    assert set(build_internal_runtime_summary({})["source_cache"]) >= {PUE, FEEDER}


def test_monthly_schedule_runs_day_one_and_retries_incomplete(monkeypatch, tmp_path):
    root = tmp_path / "share"
    root.mkdir()
    cache = SharedSourceCacheService(
        runtime_config={"deployment": {"role_mode": "internal"},
                        "shared_bridge": {"enabled": True, "root_dir": str(root)},
                        "internal_source_cache": {"enabled": True}},
        store=SharedBridgeStore(root), emit_log=lambda _: None,
    )
    pipeline = Mock()
    ready = Mock(return_value=None)
    monkeypatch.setattr(cache, "get_enabled_buildings", lambda: ["A楼", "E楼"])
    monkeypatch.setattr(cache, "_run_latest_source_steps_by_building", pipeline)
    monkeypatch.setattr(cache, "_latest_refresh_existing_entry", ready)
    cache._run_energy_report_files_if_due(datetime(2026, 11, 1, 7, 59, 59))
    pipeline.assert_not_called()

    due = datetime(2026, 11, 1, 8)
    cache._run_energy_report_files_if_due(due)
    assert [(family, period) for family, period, _ in pipeline.call_args.kwargs["steps"]] == [
        (PUE, "2026-10"), (FEEDER, "2026-10"),
    ]
    assert cache._energy_report_ready_period == ""
    cache._run_energy_report_files_if_due(due)
    assert pipeline.call_count == 1

    ready.return_value = {"status": "ready"}
    cache._energy_report_attempt = ("", 0.0)
    cache._run_energy_report_files_if_due(due)
    cache._run_energy_report_files_if_due(due)
    assert pipeline.call_count == 2
    assert cache._energy_report_ready_period == "2026-10"


def test_force_monthly_repair_skips_existing_ready_file(monkeypatch, tmp_path):
    root = tmp_path / "share"
    root.mkdir()
    cache = SharedSourceCacheService(
        runtime_config={"deployment": {"role_mode": "internal"},
                        "shared_bridge": {"enabled": True, "root_dir": str(root)},
                        "internal_source_cache": {"enabled": True}},
        store=SharedBridgeStore(root), emit_log=lambda _: None,
    )
    existing = Mock(return_value={"reason": "already_ready"})
    refresh = Mock()
    monkeypatch.setattr(cache, "_existing_latest_refresh_result", existing)
    monkeypatch.setattr(cache, "_refresh_family_bucket", refresh)
    cache._run_building_latest_refresh_background(
        source_family=PUE, building="A楼", bucket_key="2026-10", fill_func=Mock(),
        thread_key=(PUE, "A楼", "latest", "2026-10"), force_refresh=True,
    )
    existing.assert_not_called()
    assert refresh.call_args.kwargs["force_refresh_existing"] is True
    assert refresh.call_args.kwargs["bucket_key"] == "2026-10"


@pytest.mark.parametrize("broken", [False, True])
def test_completed_month_is_rechecked_and_corruption_retried(monkeypatch, tmp_path, broken):
    root = tmp_path / "share"
    root.mkdir()
    cache = SharedSourceCacheService(
        runtime_config={"deployment": {"role_mode": "internal"},
                        "shared_bridge": {"enabled": True, "root_dir": str(root)},
                        "internal_source_cache": {"enabled": True}},
        store=SharedBridgeStore(root), emit_log=lambda _: None,
    )
    cache._energy_report_ready_period = "2026-10"
    cache._energy_report_attempt = ("", 0)
    monkeypatch.setattr(cache, "get_enabled_buildings", lambda: ["A楼"])
    monkeypatch.setattr(cache, "_latest_refresh_existing_entry", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(cache, "_resolve_entry_file_path", lambda entry: root / "source.xlsx")
    validate = Mock(side_effect=ValueError("corrupt") if broken else None)
    repair = Mock()
    pipeline = Mock()
    monkeypatch.setattr(cache, "_validate_cached_source_file", validate)
    monkeypatch.setattr(cache, "_repair_entry_to_failed", repair)
    monkeypatch.setattr(cache, "_run_latest_source_steps_by_building", pipeline)
    cache._run_energy_report_files_if_due(datetime(2026, 11, 1, 9))
    assert bool(pipeline.call_count) is broken
    assert bool(repair.call_count) is broken


def test_corrupt_download_is_not_published(tmp_path, monkeypatch):
    from zipfile import ZipFile

    source = tmp_path / "broken.xlsx"
    with ZipFile(source, "w") as z:
        z.writestr("xl/workbook.xml", "<workbook><unclosed")
    monkeypatch.setattr(HandoverDownloadService, "run_energy_report_only", Mock(return_value={
        "success_files": [{"file_path": str(source)}],
    }))
    target = SimpleNamespace(_ensure_internal_source_writer=Mock(), _stop_event=SimpleNamespace(is_set=lambda: False),
                             download_browser_pool=object(), _tmp_root=tmp_path, runtime_config={}, _store_entry=Mock())
    with pytest.raises(ValueError, match="文件损坏"):
        SharedSourceCacheService._fill_energy_report_latest(target, source_family=PUE, building="A楼",
                                                           bucket_key="2026-10", emit_log=lambda _: None)
    target._store_entry.assert_not_called()
