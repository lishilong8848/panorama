from __future__ import annotations

import asyncio
import copy
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import openpyxl
import pytest

from app.config.config_adapter import adapt_runtime_config, ensure_v3_config
from app.config.inspection_data_config import INSPECTION_FAMILY_LABELS, INSPECTION_REPORT_TEMPLATES
from app.modules.internal_bridge_http.service.internal_bridge_http_runner import InternalBridgeHttpTaskRunner
from app.modules.shared_bridge.api.routes import _LATEST_REFRESHABLE_SOURCE_FAMILIES
from app.modules.shared_bridge.service.internal_download_browser_pool import InternalDownloadBrowserPool
from app.modules.shared_bridge.service.internal_runtime_status_presenter import (
    build_internal_runtime_building_status,
    build_internal_runtime_summary,
)
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService
from app.modules.shared_bridge.service.shared_source_cache_service import (
    FAMILY_HANDOVER_LOG,
    FAMILY_ELECTRICAL_INSPECTION,
    FAMILY_HVAC_INSPECTION,
    SharedSourceCacheService,
)
from app.shared.utils.artifact_naming import build_source_artifact_path
from handover_log_module.repository import download_gateway
from handover_log_module.service.handover_download_service import HandoverDownloadService
from handover_log_module.service import handover_download_service


BUCKET = "2026-10-27 09"
ROOT = Path(__file__).resolve().parents[1]
FILL_METHODS = {
    FAMILY_ELECTRICAL_INSPECTION: "fill_electrical_inspection_latest",
    FAMILY_HVAC_INSPECTION: "fill_hvac_inspection_latest",
}


@pytest.fixture(params=list(INSPECTION_REPORT_TEMPLATES))
def family(request):
    return request.param


def inspection_fill(cache, family):
    return getattr(cache, FILL_METHODS[family])


@pytest.fixture
def cache(tmp_path):
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    return SharedSourceCacheService(
        runtime_config={
            "deployment": {"role_mode": "internal"},
            "shared_bridge": {"enabled": True, "root_dir": str(tmp_path)},
            "internal_source_cache": {"enabled": True},
            "inspection_data": {"enabled": True},
            "internal_source_sites": [{"building": f"{b}楼", "enabled": True} for b in "ABCDE"],
        },
        store=store, emit_log=lambda _: None,
    )


def workbook(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    book = openpyxl.Workbook()
    book.active.append(["巡检数据", 1])
    book.save(path)
    book.close()
    return path


def artifact(family, building="D楼", bucket=BUCKET):
    return build_source_artifact_path(
        source_family=family, building=building,
        suffix=".xlsx", bucket_kind="latest", bucket_key=bucket,
    )


def test_config_defaults_template_and_runtime_preserve_feature(family):
    normalized = ensure_v3_config({})
    feature = normalized["features"]["inspection_data"]
    assert feature["enabled"] is False
    assert feature["download"]["template_names"] == INSPECTION_REPORT_TEMPLATES
    assert "template_name" not in feature["download"]
    assert feature["download"]["skip_query_conditions"] is False
    assert feature["download"]["lookback_minutes"] == 20
    assert feature["download"]["scale_label"] == "5分钟"
    assert adapt_runtime_config(normalized)["inspection_data"] == feature
    template = json.loads((ROOT / "config" / "表格计算配置.template.json").read_text(encoding="utf-8"))
    assert template["features"]["inspection_data"] == feature
    legacy = ensure_v3_config({"inspection_data": {"enabled": False}})
    assert adapt_runtime_config(legacy)["inspection_data"]["enabled"] is False


def test_internal_config_toggle_is_preserved_and_does_not_change_other_features(family):
    from app.bootstrap.app_factory import _internal_config_payload, _merge_internal_config_payload
    current = {"version": 3, "common": {}, "features": {"inspection_data": {"enabled": False}, "other": {"enabled": True}}}
    public = _internal_config_payload(current)
    assert public["features"] == {"inspection_data": {"enabled": False}}
    public["features"]["inspection_data"]["enabled"] = True
    saved = _merge_internal_config_payload(current, public)
    assert saved["features"]["other"]["enabled"] is True
    assert _internal_config_payload(saved)["features"]["inspection_data"]["enabled"] is True
    assert current["features"]["inspection_data"]["enabled"] is False
    with pytest.raises(ValueError):
        _merge_internal_config_payload(current, {"features": {"inspection_data": {"enabled": "false"}}})


def test_hourly_name_and_api_contract(family, cache):
    label = INSPECTION_FAMILY_LABELS[family]
    assert artifact(family=family).relative_path.as_posix() == (
        f"{label}/202610/20261027--09/20261027--09--{label}--D楼.xlsx"
    )
    assert _LATEST_REFRESHABLE_SOURCE_FAMILIES[family] == INSPECTION_FAMILY_LABELS[family]
    target = cache._resolve_latest_refresh_target(source_family=family, target_bucket_key=BUCKET)
    assert target["bucket_key"] == BUCKET
    assert target["fill_func"] == inspection_fill(cache, family)
    assert cache._latest_refresh_bucket_kind(family) == "latest"
    assert cache._resolve_latest_refresh_target(
        source_family=family, target_bucket_key="2026-10-27",
    )["reason"] == "invalid_bucket"


def test_de_only_does_not_change_other_family_buildings(family, cache):
    assert cache._family_buildings(family) == ["D楼", "E楼"]
    assert cache._family_buildings(FAMILY_HANDOVER_LOG) == [f"{b}楼" for b in "ABCDE"]
    for building in ["A楼", "B楼", "C楼"]:
        assert cache.start_building_latest_refresh(source_family=family, building=building)["reason"] == "invalid_building"
    steps = cache._current_hour_source_steps(bucket_key=BUCKET, branch_bucket_key="")
    assert (family, BUCKET, inspection_fill(cache, family)) in steps[-2:]
    assert [s[0] for s in steps[-2:]] == list(INSPECTION_REPORT_TEMPLATES)
    cache.runtime_config["inspection_data"]["enabled"] = False
    assert cache._family_buildings(family) == []
    assert family not in [s[0] for s in cache._current_hour_source_steps(bucket_key=BUCKET, branch_bucket_key="")]


def test_inspection_config_overrides_scale_and_omits_sheet(family, tmp_path, monkeypatch):
    config = {"download": {"sheet_name": "配置", "scale_label": "小时"}, "inspection_data": {"enabled": True, "download": {"skip_query_conditions": True, "template_name": "巡检数据表"}}}
    before = copy.deepcopy(config)
    calls = []
    def run(service, **kwargs):
        calls.append((service.config["download"], kwargs, service._download_browser_pool))
        return {"success_files": []}
    monkeypatch.setattr(HandoverDownloadService, "run", run)
    pool = object()
    service = HandoverDownloadService(config, download_browser_pool=pool, business_root_override=tmp_path)
    service.run_inspection_data_only(building="D楼", source_family=family)
    cfg, args, actual_pool = calls[0]
    assert cfg["template_name"] == INSPECTION_REPORT_TEMPLATES[family]
    assert cfg["skip_query_conditions"] is False
    assert cfg["sheet_name"] == ""
    assert cfg["scale_label"] == "5分钟" and cfg["lookback_minutes"] == 20
    assert cfg["export_button_text"] == "原样导出"
    assert args["switch_network"] is args["reuse_cached"] is False
    assert actual_pool is pool
    assert config == before
    with pytest.raises(ValueError):
        service.run_inspection_data_only(building="A楼", source_family=family)


def test_gateway_fills_inspection_query_conditions(family, tmp_path, monkeypatch):
    gateway = download_gateway.DownloadGateway({})
    monkeypatch.setattr(download_gateway, "prepare_reusable_page", AsyncMock())
    monkeypatch.setattr(download_gateway, "apply_report_page_view", AsyncMock())
    for name in ["_login_if_needed", "_open_report_query_page", "_wait_query_ready", "_export_with_download"]:
        monkeypatch.setattr(gateway, name, AsyncMock())
    monkeypatch.setattr(gateway, "_resolve_report_frames", AsyncMock(return_value=object()))
    fill = AsyncMock()
    monkeypatch.setattr(gateway, "_fill_query_conditions", fill)
    result = asyncio.run(gateway._download_file_once(
        object(), building="D楼", site={"host": "127.0.0.1"},
        start_time="2026-10-27 08:40:00", end_time="2026-10-27 09:00:00", scale_label="5分钟", template_name=INSPECTION_REPORT_TEMPLATES[family],
        save_dir=str(tmp_path), sheet_name="", query_result_timeout_ms=20,
        download_event_timeout_ms=20, login_fill_timeout_ms=20,
        menu_visible_timeout_ms=20, iframe_timeout_ms=20, start_end_visible_timeout_ms=20,
        force_iframe_reopen_each_task=True, export_button_text="原样导出",
        menu_path=["报表报告", "数据查询", "即时报表"], debug_step_log=False,
        attempt_index=1, total_attempts=1,
    ))
    assert result["success"] is True
    fill.assert_awaited_once()
    assert fill.await_args.kwargs["start_time"] == "2026-10-27 08:40:00"
    assert fill.await_args.kwargs["end_time"] == "2026-10-27 09:00:00"
    assert fill.await_args.kwargs["scale_label"] == "5分钟"
    gateway._wait_query_ready.assert_awaited_once()
    assert gateway._wait_query_ready.await_args.kwargs["allow_initial_blank_ms"] == 20
    gateway._export_with_download.assert_awaited_once()


def test_inspection_query_does_not_touch_configuration_sheet(family, monkeypatch):
    gateway = download_gateway.DownloadGateway({})
    text_fill = AsyncMock()
    scale_fill = AsyncMock()
    sheet_fill = AsyncMock(side_effect=AssertionError("must not fill sheet"))
    monkeypatch.setattr(gateway, "_fill_text_input_by_widget_or_label", text_fill)
    monkeypatch.setattr(gateway, "_select_combo_by_widget_or_label", scale_fill)
    monkeypatch.setattr(gateway, "_fill_optional_sheet_name", sheet_fill)
    monkeypatch.setattr(gateway, "_wait_query_loading_started", AsyncMock())
    button = SimpleNamespace(count=AsyncMock(return_value=1), click=AsyncMock(), wait_for=AsyncMock())
    button.first = button
    frame = SimpleNamespace(locator=lambda _: button)
    asyncio.run(gateway._fill_query_conditions(frame, start_time="start", end_time="end",
        scale_label="5分钟", template_name=INSPECTION_REPORT_TEMPLATES[family], sheet_name="", start_end_visible_timeout_ms=100))
    assert text_fill.await_count == 2
    scale_fill.assert_awaited_once()
    sheet_fill.assert_not_called()
    button.click.assert_awaited_once()


def test_existing_canonical_file_recovered_and_other_hour_not_reused(family, cache):
    path = workbook(cache.shared_root / artifact(family=family).relative_path)
    result = inspection_fill(cache, family)(building="D楼", bucket_key=BUCKET, emit_log=lambda _: None)
    assert Path(result["file_path"]) == path
    rows = cache.store.list_source_cache_entries(source_family=family, bucket_kind="latest", duty_date="2026-10-27")
    assert len(rows) == 1 and rows[0]["bucket_key"] == BUCKET
    assert cache._existing_latest_refresh_result(source_family=family, building="D楼", bucket_key="2026-10-27 10") is None
    path.unlink()
    assert cache._latest_refresh_existing_entry(source_family=family, building="D楼", bucket_key=BUCKET) is None
    assert cache.store.list_source_cache_entries(source_family=family)[0]["status"] == "failed"


def test_fill_uses_pool_atomic_store_and_latest_index(family, cache, tmp_path, monkeypatch):
    source = workbook(tmp_path / "incoming.xlsx")
    pool = object()
    cache.download_browser_pool = pool
    seen = []
    def fake_download(service, **kwargs):
        seen.append((service._download_browser_pool, kwargs))
        return {"success_files": [{"building": "E楼", "file_path": str(source)}]}
    monkeypatch.setattr(HandoverDownloadService, "run_inspection_data_only", fake_download)
    result = inspection_fill(cache, family)(building="E楼", bucket_key=BUCKET, emit_log=lambda _: None)
    assert seen[0][0] is pool
    assert result["bucket_kind"] == "latest" and result["bucket_key"] == BUCKET
    assert result["duty_date"] == "2026-10-27"
    assert (cache.shared_root / result["relative_path"]).read_bytes() == source.read_bytes()
    inspection_fill(cache, family)(building="E楼", bucket_key=BUCKET, emit_log=lambda _: None)
    assert len(seen) == 1
    entries = cache.store._source_cache_index_store.list_entries(source_family=family, building="E楼", status="ready")
    assert len(entries) == 1 and entries[0]["bucket_key"] == BUCKET


def test_failed_download_isolated_and_retries_only_failed_family(family, cache, monkeypatch):
    calls = []
    def fail(**kwargs):
        calls.append(kwargs["building"])
        raise RuntimeError("巡检数据表尚未上线")
    after = Mock(return_value={})
    result = cache._run_building_latest_source_steps(
        building="D楼", steps=[(family, BUCKET, fail), (FAMILY_HANDOVER_LOG, BUCKET, after)],
        force_retry_failed=False,
    )
    assert result["failed_units"] == [f"D楼/{family}"]
    after.assert_called_once()
    cache._current_hour_bucket = BUCKET
    monkeypatch.setattr(cache, FILL_METHODS[family], fail)
    cache._retry_inspection_data_if_needed()
    assert calls == ["D楼"]
    row = cache.store.list_source_cache_entries(source_family=family, status="failed")[0]
    cache.store.upsert_source_cache_entry(
        source_family=family, building="D楼", bucket_kind="latest", bucket_key=BUCKET,
        duty_date=row["duty_date"],
        status="failed", relative_path=row["relative_path"], metadata=row["metadata"],
        downloaded_at=(datetime.now() - timedelta(seconds=cache.FAILED_ENTRY_RETRY_COOLDOWN_SEC + 1)).strftime("%Y-%m-%d %H:%M:%S"),
    )
    cache._retry_inspection_data_if_needed()
    assert calls == ["D楼", "D楼"]


def test_overlapping_refresh_does_not_download_twice(family, cache):
    entered, release = threading.Event(), threading.Event()
    calls = []
    def fill(**kwargs):
        calls.append(kwargs["building"])
        entered.set()
        assert release.wait(5)
        return {}
    args = dict(source_family=family, bucket_key=BUCKET, fill_func=fill, buildings=["D楼"])
    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(cache._refresh_family_bucket, **args)
        try:
            assert entered.wait(5)
            result = cache._refresh_family_bucket(**args)
            assert result["running_buildings"] == ["D楼"]
            assert calls == ["D楼"]
        finally:
            release.set()
            first.result(timeout=5)


@pytest.mark.parametrize("failure_result", [True, False])
def test_report_failure_does_not_rebuild_browser_or_hold_lock(family, monkeypatch, failure_result):
    async def run():
        pool = InternalDownloadBrowserPool({})
        pool._locks["D楼"] = asyncio.Lock()
        monkeypatch.setattr(pool, "_mark_slot_recycle_pending_if_needed", Mock())
        for name in ["_recycle_slot_if_needed", "_fail_if_login_not_ready", "_ensure_logged_in"]:
            monkeypatch.setattr(pool, name, AsyncMock())
        monkeypatch.setattr(pool, "_ensure_page", AsyncMock(return_value=object()))
        rebuild = AsyncMock()
        monkeypatch.setattr(pool, "_rebuild_slot_after_job_failure", rebuild)
        async def fail(page):
            if failure_result:
                return {"success": False, "error": "巡检数据表未上线"}
            raise RuntimeError("巡检数据表未上线")
        if failure_result:
            assert (await pool._run_building_job("D楼", fail, owner="inspection_data_source"))["success"] is False
        else:
            with pytest.raises(RuntimeError):
                await pool._run_building_job("D楼", fail, owner="inspection_data_source")
        rebuild.assert_not_called()
        assert not pool._job_tasks and not pool._slot_snapshot("D楼")["in_use"]
    asyncio.run(run())


def test_cancelled_inspection_never_downloads(family, cache, monkeypatch):
    download = Mock(side_effect=AssertionError("cancelled"))
    monkeypatch.setattr(HandoverDownloadService, "run_inspection_data_only", download)
    cache._stop_event.set()
    with pytest.raises(RuntimeError, match="取消"):
        inspection_fill(cache, family)(building="D楼", bucket_key=BUCKET, emit_log=lambda _: None)
    download.assert_not_called()


def test_unc_never_scanned(family, cache, monkeypatch):
    cache.shared_root = Path(r"\\server\share")
    monkeypatch.setattr(Path, "iterdir", Mock(side_effect=AssertionError("UNC scan")))
    monkeypatch.setattr(Path, "rglob", Mock(side_effect=AssertionError("UNC scan")))
    assert cache._scan_existing_latest_file_candidates(source_family=family, building="D楼", bucket_key=BUCKET) == []


def test_light_summary_and_details_only_show_de(family, cache):
    raw = cache.get_health_snapshot(mode="internal_light")
    assert [r["building"] for r in raw[family]["buildings"]] == ["D楼", "E楼"]
    snapshot = {"internal_source_cache": raw}
    summary = build_internal_runtime_summary(snapshot)
    assert [r["building"] for r in summary["source_cache"][family]["buildings"]] == ["D楼", "E楼"]
    for code in "abcde":
        row = build_internal_runtime_building_status(snapshot, building=f"{code.upper()}楼", building_code=code)
        assert (family in row["source_families"]) == (code in "de")


def test_http_index_date_query_only_exposes_existing_ready_file(family, cache, monkeypatch):
    path = workbook(cache.shared_root / artifact(family=family).relative_path)
    inspection_fill(cache, family)(building="D楼", bucket_key=BUCKET, emit_log=lambda _: None)
    runner = InternalBridgeHttpTaskRunner(runtime_service=SimpleNamespace(shared_bridge_root=str(cache.shared_root)))
    monkeypatch.setattr(runner, "_get_store", lambda: cache.store)
    monkeypatch.setattr(runner, "_merge_main_source_cache_entries", lambda rows, **kwargs: rows)
    monkeypatch.setattr(runner, "_start_source_index_recovery_if_needed", Mock())
    query = dict(source_family=family, building="D楼", bucket_kind="latest", bucket_or_date="2026-10-27")
    rows = runner.list_source_index(**query)
    assert len(rows) == 1 and rows[0]["file_verified"] is True
    assert rows[0]["bucket_key"] == BUCKET
    path.unlink()
    assert runner.list_source_index(**query) == []
    with pytest.raises(ValueError, match="仅支持"):
        runner.refresh_latest_source_cache(source_family=family, buildings=["A楼"])
    with pytest.raises(ValueError, match="时间桶"):
        runner.refresh_latest_source_cache(source_family=family, buildings=["D楼"], target_bucket_key="2026-10-27")


def test_date_index_recovery_keeps_actual_newest_hour(family, cache, monkeypatch):
    workbook(cache.shared_root / artifact(family=family, bucket="2026-10-27 08").relative_path)
    workbook(cache.shared_root / artifact(family=family, bucket="2026-10-27 11").relative_path)
    runner = InternalBridgeHttpTaskRunner(runtime_service=SimpleNamespace(
        shared_bridge_root=str(cache.shared_root), _source_cache_service=cache,
    ))
    monkeypatch.setattr(Path, "rglob", Mock(side_effect=AssertionError("no recursive scan")))
    rows = runner._recover_source_index_from_existing_files_if_needed(
        [], source_family=family, building="D楼",
        bucket_kind="latest", duty_date="2026-10-27",
    )
    assert len(rows) == 1
    assert rows[0]["bucket_kind"] == "latest"
    assert rows[0]["bucket_key"] == "2026-10-27 11"
    assert rows[0]["duty_date"] == "2026-10-27"
    assert cache._candidate_existing_latest_paths(
        source_family=family, building="D楼", bucket_key="2026-10-27",
    ) == []


def test_failed_atomic_save_never_publishes_ready(family, cache, tmp_path, monkeypatch):
    source = workbook(tmp_path / "incoming.xlsx")
    cache.download_browser_pool = object()
    monkeypatch.setattr(HandoverDownloadService, "run_inspection_data_only", lambda *args, **kwargs: {
        "success_files": [{"file_path": str(source)}],
    })
    cache_file = cache._cache_file_compat
    monkeypatch.setattr(cache, "_cache_file_compat", Mock(side_effect=OSError("disk full")))
    result = cache._refresh_family_bucket(
        source_family=family, bucket_key=BUCKET,
        fill_func=inspection_fill(cache, family), buildings=["D楼"],
    )
    assert result["failed_buildings"] == ["D楼"]
    assert cache.store.list_source_cache_entries(source_family=family, status="ready") == []
    assert source.is_file()
    failed = cache.store.list_source_cache_entries(source_family=family)
    assert len(failed) == 1
    monkeypatch.setattr(cache, "_cache_file_compat", cache_file)
    cache._refresh_family_bucket(
        source_family=family, bucket_key=BUCKET,
        fill_func=inspection_fill(cache, family), buildings=["D楼"], force_retry_failed=True,
    )
    recovered = cache.store.list_source_cache_entries(source_family=family)
    assert len(recovered) == 1 and recovered[0]["status"] == "ready"
    assert recovered[0]["entry_id"] == failed[0]["entry_id"]


def test_gateway_passes_inspection_owner_to_existing_pool(family, tmp_path, monkeypatch):
    from concurrent.futures import Future
    gateway = download_gateway.DownloadGateway({"handover_log": {"sites": [
        {"building": "D楼", "enabled": True, "host": "127.0.0.1"},
    ]}})
    done = Future()
    done.set_result({"success": False, "error": "not online"})
    pool = SimpleNamespace(submit_building_job=Mock(return_value=done))
    monkeypatch.setattr(download_gateway, "configure_playwright_environment", Mock())
    monkeypatch.setattr(download_gateway, "_await_ready_browser_pool", AsyncMock(return_value=pool))
    monkeypatch.setattr(download_gateway, "async_playwright", Mock(side_effect=AssertionError("no standalone browser")))
    asyncio.run(gateway._download_handover_xlsx_batch_async(
        buildings=["D楼"], start_time="", end_time="", scale_label="",
        template_name=INSPECTION_REPORT_TEMPLATES[family], save_dir=str(tmp_path), sheet_name="",
        query_result_timeout_ms=20, download_event_timeout_ms=20,
        login_fill_timeout_ms=20, menu_visible_timeout_ms=20, iframe_timeout_ms=20,
        start_end_visible_timeout_ms=20, page_refresh_retry_count=1,
        max_retries=1, retry_wait_sec=0, force_iframe_reopen_each_task=True,
        export_button_text="原样导出", menu_path=["报表报告", "数据查询", "即时报表"],
        parallel_by_building=False, site_start_delay_sec=0, debug_step_log=False,
        browser_pool=pool,
    ))
    assert pool.submit_building_job.call_args.args[0] == "D楼"
    assert pool.submit_building_job.call_args.kwargs == {"owner": "inspection_data_source"}


@pytest.mark.parametrize("startup", [True, False])
def test_scheduler_startup_and_regular_hour_include_inspection(family, cache, monkeypatch, startup):
    cache.run_on_startup = startup
    pipeline = Mock(return_value={})
    monkeypatch.setattr(cache, "_run_latest_source_steps_by_building", pipeline)
    monkeypatch.setattr(cache, "_ensure_dirs", Mock())
    for name in ["_run_monthly_report_file_if_due", "_run_top5_monthly_report_file_if_due", "_run_alarm_bucket_if_due", "_run_chiller_mode_switch_if_due", "_run_temperature_humidity_file_if_due", "_run_daily_source_files_if_due"]:
        monkeypatch.setattr(cache, name, Mock())
    monkeypatch.setattr(cache._stop_event, "wait", lambda _: cache._stop_event.set())
    cache._loop()
    pipeline.assert_called_once()
    assert family in [s[0] for s in pipeline.call_args.kwargs["steps"]]


def test_exact_report_names_and_no_fuzzy_monthly_regression(family):
    assert INSPECTION_REPORT_TEMPLATES == {
        "electrical_inspection_family": "电气巡检表",
        "hvac_inspection_family": "暖通巡检表",
    }
    template = INSPECTION_REPORT_TEMPLATES[family]
    assert download_gateway._template_name_candidates(template) == [template]
    selectors = download_gateway.DownloadGateway._report_template_selectors(template)
    assert all("contains(normalize-space(@title)" not in selector and ":has-text" not in selector for selector in selectors)
    monthly_selectors = download_gateway.DownloadGateway._report_template_selectors("全景平台月报")
    assert len(monthly_selectors) == 2
    assert all("contains(normalize-space(@title)" not in selector and ":has-text" not in selector for selector in monthly_selectors)
    assert len(download_gateway.DownloadGateway._report_template_selectors("交接班日志")) == 7


def test_actual_query_window_uses_now_not_hour_bucket(family, tmp_path, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 30, 10, 7, 23)

    monkeypatch.setattr(handover_download_service, "datetime", Clock)
    batch = Mock(return_value=[])
    monkeypatch.setattr(handover_download_service, "download_handover_xlsx_batch", batch)
    monkeypatch.setattr(handover_download_service, "set_runtime_config", Mock())
    service = HandoverDownloadService({"inspection_data": {"enabled": True}}, business_root_override=tmp_path)
    service.run_inspection_data_only(building="D楼", source_family=family, emit_log=lambda _: None)
    args = batch.call_args.kwargs
    assert args["start_time"] == "2026-10-30 09:47:23"
    assert args["end_time"] == "2026-10-30 10:07:23"
    assert args["scale_label"] == "5分钟"
    assert args["template_name"] == INSPECTION_REPORT_TEMPLATES[family]
    assert args["sheet_name"] == ""
    runtime = handover_download_service.set_runtime_config.call_args.args[0]
    assert runtime["handover_log"]["download"]["skip_query_conditions"] is False


@pytest.mark.parametrize("feature", [{}, {"enabled": False}, {"enabled": "false"}])
def test_disabled_both_families_cannot_download(family, cache, tmp_path, monkeypatch, feature):
    cache.runtime_config["inspection_data"] = feature
    cache._family_status[family]["failed_buildings"] = ["D楼"]
    batch = Mock(side_effect=AssertionError("disabled must not download"))
    monkeypatch.setattr(handover_download_service, "download_handover_xlsx_batch", batch)
    assert not cache._family_buildings(family)
    with pytest.raises(ValueError, match="未启用"):
        inspection_fill(cache, family)(building="D楼", bucket_key=BUCKET, emit_log=lambda _: None)
    direct = HandoverDownloadService({"inspection_data": feature}, business_root_override=tmp_path)
    with pytest.raises(ValueError, match="未启用"):
        direct.run_inspection_data_only(building="D楼", source_family=family)
    cache._refresh_family_bucket(source_family=family, bucket_key=BUCKET, fill_func=batch, buildings=["D楼", "E楼"])
    cache._retry_inspection_data_if_needed()
    batch.assert_not_called()
    assert not set(INSPECTION_REPORT_TEMPLATES).intersection(
        step[0] for step in cache._current_hour_source_steps(bucket_key=BUCKET, branch_bucket_key="")
    )


def test_legacy_combined_family_is_not_a_new_source(family, cache, monkeypatch):
    legacy = "inspection_data_family"
    assert legacy not in _LATEST_REFRESHABLE_SOURCE_FAMILIES
    assert legacy not in cache._family_status
    assert legacy not in cache.get_required_directory_paths()
    assert cache._family_buildings(legacy) == []
    assert cache._source_family_candidates(family) == [family]
    assert cache._resolve_latest_refresh_target(source_family=legacy)["reason"] == "unsupported_family"
    with pytest.raises(ValueError, match="已停用"):
        artifact(family=legacy)
    with pytest.raises(ValueError, match="不支持"):
        HandoverDownloadService({"inspection_data": {"enabled": True}}).run_inspection_data_only(
            building="D楼", source_family=legacy,
        )
    runner = InternalBridgeHttpTaskRunner(runtime_service=SimpleNamespace(shared_bridge_root=str(cache.shared_root)))
    monkeypatch.setattr(runner, "_get_store", Mock(side_effect=AssertionError("legacy index must not be read")))
    with pytest.raises(ValueError, match="已停用"):
        runner.list_source_index(source_family=legacy)
    with pytest.raises(ValueError, match="已停用"):
        runner.refresh_latest_source_cache(source_family=legacy, buildings=["D楼"])
    config = ensure_v3_config({"inspection_data": {"enabled": False, "download": {"template_name": "巡检数据表"}}})
    assert "template_name" not in config["features"]["inspection_data"]["download"]
    assert config["features"]["inspection_data"]["download"]["template_names"] == INSPECTION_REPORT_TEMPLATES


def test_one_ready_file_never_covers_other_family(family, cache, monkeypatch):
    other = next(item for item in INSPECTION_REPORT_TEMPLATES if item != family)
    workbook(cache.shared_root / artifact(family=family).relative_path)
    first = inspection_fill(cache, family)(building="D楼", bucket_key=BUCKET, emit_log=lambda _: None)
    query = dict(source_family=other, building="D楼", bucket_kind="latest", bucket_key=BUCKET)
    assert cache._get_ready_entry(**query) is None
    assert not cache._entry_exists_for_bucket(**query)
    assert cache._existing_latest_refresh_result(source_family=other, building="D楼", bucket_key=BUCKET) is None
    runner = InternalBridgeHttpTaskRunner(runtime_service=SimpleNamespace(
        shared_bridge_root=str(cache.shared_root), _source_cache_service=cache,
    ))
    monkeypatch.setattr(runner, "_get_store", lambda: cache.store)
    monkeypatch.setattr(runner, "_start_source_index_recovery_if_needed", Mock())
    assert runner.list_source_index(source_family=other, building="D楼", bucket_kind="latest", bucket_or_date=BUCKET) == []
    assert runner._recover_source_index_from_existing_files_if_needed(
        [], source_family=other, building="D楼", bucket_kind="latest", duty_date=BUCKET[:10],
    ) == []
    workbook(cache.shared_root / artifact(family=other).relative_path)
    second = inspection_fill(cache, other)(building="D楼", bucket_key=BUCKET, emit_log=lambda _: None)
    assert first["relative_path"] != second["relative_path"]
    entry_ids = set()
    for source_family in INSPECTION_REPORT_TEMPLATES:
        entries = runner.list_source_index(source_family=source_family, building="D楼", bucket_kind="latest", bucket_or_date=BUCKET)
        assert len(entries) == 1 and entries[0]["source_family"] == source_family
        entry_ids.add(entries[0]["entry_id"])
    assert len(entry_ids) == 2


def test_hourly_families_run_serially_per_building(cache, tmp_path, monkeypatch):
    cache.download_browser_pool = object()
    sources = {family: workbook(tmp_path / f"{family}.xlsx") for family in INSPECTION_REPORT_TEMPLATES}
    active = set()
    lock = threading.Lock()
    seen = {building: [] for building in ["D楼", "E楼"]}
    roots = {}

    def download(service, *, building, source_family, emit_log):
        with lock:
            assert building not in active
            active.add(building)
            seen[building].append(source_family)
            roots[(building, source_family)] = service._business_root_override
        try:
            time.sleep(0.02)
            return {"success_files": [{"file_path": str(sources[source_family])}]}
        finally:
            with lock:
                active.remove(building)

    monkeypatch.setattr(HandoverDownloadService, "run_inspection_data_only", download)
    steps = [step for step in cache._current_hour_source_steps(bucket_key=BUCKET, branch_bucket_key="") if step[0] in INSPECTION_REPORT_TEMPLATES]
    result = cache._run_latest_source_steps_by_building(steps=steps, force_retry_failed=False)
    assert result["failed_units"] == []
    assert len(result["completed_units"]) == 4
    assert len(set(roots.values())) == 4
    assert all(families == list(INSPECTION_REPORT_TEMPLATES) for families in seen.values())
    cache._run_latest_source_steps_by_building(steps=steps, force_retry_failed=False)
    assert all(len(families) == 2 for families in seen.values())


def test_failure_does_not_block_other_and_retry_missing_only(family, cache, tmp_path, monkeypatch):
    other = next(item for item in INSPECTION_REPORT_TEMPLATES if item != family)
    cache.download_browser_pool = object()
    cache._current_hour_bucket = BUCKET
    source = workbook(tmp_path / "incoming.xlsx")
    calls = []
    failing = True

    def download(service, *, building, source_family, emit_log):
        calls.append(source_family)
        if source_family == family and failing:
            raise RuntimeError("report unavailable")
        return {"success_files": [{"file_path": str(source)}]}

    monkeypatch.setattr(HandoverDownloadService, "run_inspection_data_only", download)
    steps = [(item, BUCKET, inspection_fill(cache, item)) for item in [family, other]]
    result = cache._run_building_latest_source_steps(building="D楼", steps=steps, force_retry_failed=False)
    assert result["failed_units"] == [f"D楼/{family}"]
    assert calls == [family, other]
    assert len(cache.store.list_source_cache_entries(source_family=other, status="ready")) == 1
    cache._retry_inspection_data_if_needed()
    assert calls == [family, other]
    failed = cache.store.list_source_cache_entries(source_family=family, status="failed")[0]
    cache.store.upsert_source_cache_entry(
        source_family=family, building="D楼", bucket_kind="latest", bucket_key=BUCKET,
        duty_date=BUCKET[:10], status="failed", relative_path=failed["relative_path"], metadata=failed["metadata"],
        downloaded_at=(datetime.now() - timedelta(seconds=cache.FAILED_ENTRY_RETRY_COOLDOWN_SEC + 1)).strftime("%Y-%m-%d %H:%M:%S"),
    )
    failing = False
    cache._retry_inspection_data_if_needed()
    assert calls == [family, other, family]
    for item in INSPECTION_REPORT_TEMPLATES:
        rows = cache.store.list_source_cache_entries(source_family=item, status="ready")
        assert len(rows) == 1
        assert rows[0]["metadata"]["template_name"] == INSPECTION_REPORT_TEMPLATES[item]
    assert not cache._family_status[family]["failed_buildings"]


def test_all_status_paths_include_independent_families(family, cache):
    full = cache._build_external_full_snapshot()
    assert [row["building"] for row in full[family]["buildings"]] == ["D楼", "E楼"]
    assert "inspection_data_family" not in full
    fast = cache.get_external_source_cache_overview_fast()
    assert family in {item["key"] for item in fast["families"]}
    paths = cache.get_required_directory_paths()
    assert paths[family] == str(cache.shared_root / INSPECTION_FAMILY_LABELS[family])
    assert cache._family_root(family) == Path(paths[family])
    cache.ensure_required_directories()
    assert Path(paths[family]).is_dir()


def test_shared_root_diagnostics_include_both_directories(family, cache):
    runtime = object.__new__(SharedBridgeRuntimeService)
    runtime.shared_bridge_root = str(cache.shared_root)
    runtime.role_mode = "internal"
    runtime._store = None
    runtime._store_root = ""
    runtime._source_cache_service = cache
    runtime._shared_bridge_db_path_text = lambda: ""
    runtime.get_source_cache_buildings = lambda: ["D楼", "E楼"]
    snapshot = runtime.diagnose_shared_root(initialize=False)
    directory = next(item for item in snapshot["directories"] if item["key"] == family)
    assert directory["path"] == str(cache.shared_root / INSPECTION_FAMILY_LABELS[family])
    assert directory["label"] == INSPECTION_FAMILY_LABELS[family]
    assert family in {item["key"] for item in snapshot["families"]}


@pytest.mark.parametrize("active", [False, True])
def test_stale_inspection_refresh_recovers_without_restarting_active_job(family, cache, tmp_path, monkeypatch, active):
    old = (datetime.now() - timedelta(seconds=cache.REFRESHING_ENTRY_TIMEOUT_SEC + 1)).strftime("%Y-%m-%d %H:%M:%S")
    cache.store.upsert_source_cache_entry(
        source_family=family, building="D楼", bucket_kind="latest", bucket_key=BUCKET,
        duty_date=BUCKET[:10], status="refreshing", relative_path=artifact(family=family).relative_path.as_posix(),
        downloaded_at=old, metadata={"refreshing_at": old},
    )
    if active:
        cache._active_latest_downloads[(family, "D楼", "latest", BUCKET)] = old
    source = workbook(tmp_path / "incoming.xlsx")
    download = Mock(return_value={"success_files": [{"file_path": str(source)}]})
    monkeypatch.setattr(HandoverDownloadService, "run_inspection_data_only", download)
    cache.download_browser_pool = object()
    cache._refresh_family_bucket(
        source_family=family, bucket_key=BUCKET, buildings=["D楼"], fill_func=inspection_fill(cache, family),
    )
    if active:
        download.assert_not_called()
    else:
        download.assert_called_once()
        assert len(cache.store.list_source_cache_entries(source_family=family, status="ready")) == 1
