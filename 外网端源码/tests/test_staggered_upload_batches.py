import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest

from app.config.config_adapter import adapt_runtime_config
from app.modules.shared_bridge.service import shared_bridge_runtime_service as bridge


def test_schedules_use_new_offsets_without_moving_business_times():
    raw = json.loads((Path(__file__).resolve().parents[1] / "表格计算配置.json").read_text(encoding="utf-8-sig"))
    cfg = adapt_runtime_config(raw)
    assert cfg["scheduler"]["run_time"] == "02:30:00"
    assert cfg["temperature_humidity_upload"]["scheduler"]["run_time"] == "01:10:00"
    assert cfg["day_metric_upload"]["scheduler"]["minute_offset"] == 15
    assert cfg["chiller_mode_upload"]["scheduler"]["minute_offset"] == 7
    assert cfg["wet_bulb_collection"]["scheduler"]["minute_offset"] == 35
    assert cfg["branch_power_upload"]["scheduler"]["minute_offset"] == 240
    assert cfg["alarm_export"]["scheduler"]["run_time"] == "08:25:00"
    assert cfg["system_screenshot_upload"]["scheduler"]["run_time"] == "06:37:00"
    assert cfg["inspection_data_upload"]["scheduler"]["run_times"] == [
        f"{hour:02d}:20:00" for hour in range(0, 24, 2)
    ]


def test_scheduled_alarm_requires_exact_daily_bucket_and_keeps_it_on_resume():
    bucket = "2026-11-13 08"
    lookup = Mock(return_value=[
        {"building": "A", "bucket_key": bucket, "bucket_kind": "latest"},
        {"building": "B", "bucket_key": "2026-11-12 08", "bucket_kind": "latest"},
        {"building": "B", "bucket_key": bucket, "bucket_kind": "manual"},
    ])
    runtime = SimpleNamespace(
        _http_source_cache_buildings=lambda _: ["A", "B"],
        get_source_cache_buildings=lambda: ["A", "B"],
        get_latest_source_cache_entries=lookup,
    )
    runtime.get_alarm_event_upload_selection = (
        lambda **kwargs: bridge.SharedBridgeRuntimeService.get_alarm_event_upload_selection(runtime, **kwargs)
    )
    task = {"requested_by": "alarm_upload_scheduler", "request": {"target_bucket_key": bucket}}
    bind = bridge.SharedBridgeRuntimeService._build_alarm_event_upload_resume_binding
    with pytest.raises(RuntimeError, match="B"):
        bind(runtime, task)
    assert lookup.call_args.kwargs["bucket_key"] == bucket
    lookup.return_value.append({"building": "B", "bucket_key": bucket, "bucket_kind": "latest"})
    assert bind(runtime, task)["worker_payload"]["required_alarm_bucket"] == bucket


def test_scheduled_day_metric_cannot_resume_with_only_one_building():
    runtime = SimpleNamespace(
        _require_accessible_cached_file=lambda path, **_: path,
        _normalize_text_list=lambda values: list(values),
        get_source_cache_buildings=lambda: ["A", "B"],
    )
    task = {
        "requested_by": "scheduler",
        "request": {"selected_dates": ["2026-11-13"]},
        "result": {"internal": {"source_units": [
            {"building": "A", "duty_date": "2026-11-13", "file_path": "a.json"},
        ]}},
    }
    with pytest.raises(RuntimeError, match="未齐全"):
        bridge.SharedBridgeRuntimeService._build_day_metric_resume_binding_from_artifacts(runtime, task)


@pytest.mark.parametrize(("returned_date", "returned_building", "count", "accepted"), [
    ("2026-11-28", "C楼", 1, True),
    ("2026-11-27", "C楼", 1, False),
    ("2026-11-28", "A楼", 1, False),
    ("2026-11-28", "C楼", 2, False),
])
def test_scheduled_day_metric_resumes_only_the_requested_building(returned_date, returned_building, count, accepted):
    runtime = SimpleNamespace(
        _require_accessible_cached_file=lambda path, **_: path,
        _normalize_text_list=lambda values: list(values),
        get_source_cache_buildings=lambda: ["A楼", "C楼"],
    )
    task = {
        "requested_by": "scheduler",
        "request": {"selected_dates": ["2026-11-28"], "building_scope": "single", "building": "C楼"},
        "result": {"internal": {"source_units": [
            {"building": returned_building, "duty_date": returned_date, "file_path": "report.json"},
        ] * count}},
    }
    bind = bridge.SharedBridgeRuntimeService._build_day_metric_resume_binding_from_artifacts
    if not accepted:
        with pytest.raises(RuntimeError, match="未齐全或批次不匹配"):
            bind(runtime, task)
        return
    payload = bind(runtime, task)["worker_payload"]
    assert payload["buildings"] == ["C楼"]
    assert payload["building_scope"] == "single"
    assert payload["selected_dates"] == ["2026-11-28"]
    assert payload["source_units"] == [{"duty_date": "2026-11-28", "building": "C楼", "source_file": "report.json"}]


@pytest.fixture
def day_metric_tick(monkeypatch, tmp_path):
    from app.bootstrap import app_factory
    from handover_log_module.api import facade

    monkeypatch.setattr(facade, "load_handover_config", lambda _: {
        "download": {"shift_windows": {"day": {"start": "08:30:00"}}},
    })
    dispatch = Mock(return_value=(SimpleNamespace(job_id="job"), {"task_id": "bridge"}))
    monkeypatch.setattr(app_factory, "start_waiting_bridge_job", dispatch)
    active = Mock(return_value=None)
    container = SimpleNamespace(runtime_config={}, job_service=SimpleNamespace(find_active_job_by_dedupe_key=active))
    lookup = Mock(return_value=[])
    bridge_service = SimpleNamespace(
        get_source_cache_buildings=lambda: ["A楼", "B楼", "C楼"],
        get_day_metric_by_date_cache_entries=lookup,
    )
    cached = Mock(return_value="已提交共享文件处理任务")

    def entry(building, date="2026-11-28", *, exists=True):
        path = tmp_path / f"{date}-{building}.json"
        if exists:
            path.touch()
        return {"duty_date": date, "building": building, "file_path": str(path)}

    def tick(clock):
        return app_factory._schedule_day_metric_upload(
            container, bridge_service, source="12项独立上传",
            now=datetime.fromisoformat(f"2026-11-28T{clock}"), submit_cached=cached,
        )

    return SimpleNamespace(tick=tick, lookup=lookup, entry=entry, cached=cached, dispatch=dispatch, active=active)


def test_day_metric_waits_before_shift_without_dispatching(day_metric_tick):
    t = day_metric_tick
    t.lookup.return_value = [t.entry("A楼", "2026-11-27"), t.entry("B楼", exists=False)]
    ok, detail = t.tick("03:15:00")
    assert not ok and detail.startswith("skip:等待源文件")
    t.cached.assert_not_called()
    t.dispatch.assert_not_called()


def test_day_metric_uploads_available_files_even_before_shift(day_metric_tick):
    t = day_metric_tick
    a = t.entry("A楼")
    t.lookup.return_value = [a]
    ok, detail = t.tick("08:29:59")
    assert ok and "等待源文件" in detail and not detail.startswith("skip:")
    t.cached.assert_called_once_with("2026-11-28", ["A楼"], [a])
    t.dispatch.assert_not_called()


def test_day_metric_uses_all_ready_files_without_consulting_shift(day_metric_tick, monkeypatch):
    from handover_log_module.api import facade

    t = day_metric_tick
    monkeypatch.setattr(facade, "load_handover_config", Mock(side_effect=AssertionError("should not check shift")))
    t.lookup.return_value = [t.entry(building) for building in ["A楼", "B楼", "C楼"]]
    assert t.tick("03:15:00")[0]
    t.cached.assert_called_once_with("2026-11-28", ["A楼", "B楼", "C楼"], t.lookup.return_value)
    t.dispatch.assert_not_called()


def test_day_metric_after_shift_dispatches_only_missing_buildings_and_reuses_waits(day_metric_tick):
    t = day_metric_tick
    a = t.entry("A楼")
    t.lookup.return_value = [a]
    assert t.tick("08:30:00")[0]
    t.cached.assert_called_once_with("2026-11-28", ["A楼"], [a])
    keys = set()
    for call, building in zip(t.dispatch.call_args_list, ["B楼", "C楼"], strict=True):
        request = {"selected_dates": ["2026-11-28"], "building_scope": "single", "building": building}
        assert call.kwargs["bridge_kwargs"] == call.kwargs["worker_payload"] == request
        assert call.kwargs["submitted_by"] == "scheduler"
        assert call.kwargs["resource_keys"] == ["shared_bridge:day_metric"]
        assert call.kwargs["defer_dispatch"] is True
        keys.add(call.kwargs["dedupe_key"])
    assert keys == {"day_metric_upload:scheduler:single:2026-11-28:B楼", "day_metric_upload:scheduler:single:2026-11-28:C楼"}
    t.active.side_effect = lambda key: {"job_id": "active"} if key in keys else None
    # B's file arrived, but its existing bridge job owns this upload.
    t.lookup.return_value = [a, t.entry("B楼")]
    assert t.tick("09:15:00")[0]
    assert t.dispatch.call_count == 2
    assert t.cached.call_args.args[1] == ["A楼"]


def test_day_metric_does_not_hide_source_query_failure(day_metric_tick):
    t = day_metric_tick
    t.lookup.side_effect = RuntimeError("source index unavailable")
    with pytest.raises(RuntimeError, match="source index unavailable"):
        t.tick("09:15:00")
    t.dispatch.assert_not_called()


def test_scheduled_single_building_wait_does_not_block_other_buildings():
    from app.bootstrap.container import AppContainer

    wait = {"feature": "day_metric_from_download", "status": "waiting_resource", "submitted_by": "scheduler",
            "wait_reason": "waiting:shared_bridge", "dedupe_key": "day_metric_upload:scheduler:single:2026-11-28:B楼"}
    list_jobs = Mock(return_value=[wait, {"feature": "handover_review", "status": "running"}])
    container = SimpleNamespace(job_service=SimpleNamespace(list_jobs=list_jobs))
    busy = AppContainer._day_metric_upload_scheduler_busy
    assert not busy(container)
    list_jobs.return_value = [{**wait, "status": "running"}]
    assert busy(container)
    list_jobs.return_value = [{**wait, "dedupe_key": "day_metric_manual"}]
    assert busy(container)


def test_day_metric_recovers_published_file_after_restart_without_dispatch(tmp_path, monkeypatch):
    from app.bootstrap.app_factory import _schedule_day_metric_upload
    from app.modules.report_pipeline.service.job_service import JobService
    from handover_log_module.api import facade

    date = "2026-11-28"
    path = tmp_path / "ready.json"
    path.touch()
    monkeypatch.setattr(facade, "load_handover_config", lambda _: {})
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    runtime = object.__new__(bridge.SharedBridgeRuntimeService)
    runtime._waiting_job_cache_probe_log_markers = {}
    runtime._require_accessible_cached_file = lambda source, **_: source
    runtime.get_source_cache_buildings = lambda: ["C楼"]
    runtime.get_day_metric_by_date_cache_entries = Mock(return_value=[])
    runtime.create_http_bridge_task = Mock()
    upload = Mock(return_value="已提交上传")

    def reopen_jobs():
        jobs = JobService()
        monkeypatch.setattr(jobs, "_launch_existing_worker_job", Mock())
        jobs.configure_task_engine(runtime_config=config, app_dir=tmp_path)
        runtime._job_service = jobs
        return SimpleNamespace(job_service=jobs, runtime_config=config)

    def tick(container):
        return _schedule_day_metric_upload(
            container, runtime, source="12项独立上传", now=datetime(2026, 11, 28, 9, 15), submit_cached=upload,
        )

    container = reopen_jobs()
    try:
        assert tick(container)[0]
        jobs = container.job_service.list_jobs(statuses=["waiting_resource"])
        assert len(jobs) == 1
        job_id = jobs[0]["job_id"]
        assert not jobs[0]["bridge_task_id"]
        runtime.create_http_bridge_task.assert_not_called()
    finally:
        container.job_service.shutdown_task_engine()

    # The source was published while the dispatch receipt was still unbound.
    runtime.get_day_metric_by_date_cache_entries.return_value = [{"duty_date": date, "building": "C楼", "file_path": str(path)}]
    container = reopen_jobs()
    try:
        assert tick(container)[1].startswith("skip:already_waiting")
        runtime._reconcile_waiting_jobs()
        jobs = container.job_service.list_jobs()
        assert len(jobs) == 1 and jobs[0]["job_id"] == job_id and jobs[0]["status"] == "queued"
        payload = container.job_service.load_job_stage_payload(job_id)
        assert payload["source_units"] == [{"duty_date": date, "building": "C楼", "source_file": str(path)}]
        container.job_service._launch_existing_worker_job.assert_called_once()
        assert runtime.get_day_metric_by_date_cache_entries.call_args.kwargs["require_fresh"] is True
        runtime.create_http_bridge_task.assert_not_called()
        upload.assert_not_called()
    finally:
        container.job_service.shutdown_task_engine()


@pytest.mark.parametrize("source_present", [False, True])
def test_day_metric_bound_wait_ignores_timeout_and_checks_source_on_404(source_present):
    from app.modules.internal_bridge_http.service.client import InternalBridgeHttpError

    waiting = {"job_id": "job", "bridge_task_id": "bridge", "feature": "day_metric_from_download",
               "submitted_by": "scheduler", "wait_reason": "waiting:shared_bridge",
               "dedupe_key": "day_metric_upload:scheduler:single:2026-11-28:C楼"}
    runtime = object.__new__(bridge.SharedBridgeRuntimeService)
    request = {"selected_dates": ["2026-11-28"], "building_scope": "single", "building": "C楼"}
    runtime._job_service = SimpleNamespace(
        list_jobs=Mock(return_value=[waiting]), fail_waiting_job=Mock(),
        load_job_stage_payload=Mock(return_value=request), resume_waiting_worker_job=Mock(),
    )
    runtime._http_bridge_should_try = lambda: True
    runtime._http_bridge_forced = lambda: True
    runtime._emit_http_bridge_issue_log = Mock()
    runtime.get_cached_task = Mock(return_value=None)
    runtime.get_source_cache_buildings = lambda: ["C楼"]
    runtime._require_accessible_cached_file = lambda source, **_: source
    runtime.get_day_metric_by_date_cache_entries = Mock(return_value=[
        {"duty_date": "2026-11-28", "building": "C楼", "file_path": "report.json"},
    ] if source_present else [])
    missing = InternalBridgeHttpError("内网端 HTTP 返回 404: 内网桥接任务不存在")
    missing.__cause__ = HTTPError("http://unused", 404, "Not Found", {}, None)
    runtime._internal_bridge_http_client = SimpleNamespace(
        get_task=Mock(side_effect=[InternalBridgeHttpError("timed out"), missing]),
    )
    runtime._reconcile_waiting_jobs()
    runtime._job_service.fail_waiting_job.assert_not_called()
    runtime.get_day_metric_by_date_cache_entries.assert_not_called()
    runtime._reconcile_waiting_jobs()
    if source_present:
        runtime._job_service.fail_waiting_job.assert_not_called()
        call = runtime._job_service.resume_waiting_worker_job.call_args
        assert call.args == ("job",)
        assert call.kwargs["worker_payload"]["bridge_task_id"] == "bridge"
    else:
        runtime._job_service.resume_waiting_worker_job.assert_not_called()
        runtime._job_service.fail_waiting_job.assert_called_once_with(
            "job", error_text="绑定补采任务丢失", summary="等待内网补采同步失败",
        )


def test_day_metric_unbound_dispatch_uses_same_job_and_no_blind_post_retry():
    runtime = object.__new__(bridge.SharedBridgeRuntimeService)
    runtime._http_bridge_should_try = lambda: True
    runtime._waiting_job_cache_probe_log_markers = {}
    runtime._cache_task_detail = Mock()
    runtime._mark_http_bridge_success = Mock()
    runtime._emit_system_log = Mock()
    runtime.get_day_metric_by_date_cache_entries = Mock(return_value=[])
    request = {"selected_dates": ["2026-11-28"], "building_scope": "single", "building": "C楼"}
    get_or_create = "get_or_create_day_metric_from_download_task"
    create = "create_day_metric_from_download_task"
    runtime._job_service = SimpleNamespace(bind_bridge_task=Mock(), load_job_stage_payload=Mock(return_value={
        "_bridge_dispatch": {"get_or_create_name": get_or_create, "create_name": create, "bridge_kwargs": request},
    }))
    runtime._internal_bridge_http_client = SimpleNamespace(create_task=Mock(return_value={"task_id": "original"}))
    snapshot = {"job_id": "job", "submitted_by": "scheduler", "feature": "day_metric_from_download",
                "dedupe_key": "day_metric_upload:scheduler:single:2026-11-28:C楼"}
    assert runtime._reissue_unbound_waiting_job(snapshot)
    runtime._internal_bridge_http_client.create_task.assert_called_once_with(
        get_or_create_name=get_or_create, create_name=create, requested_by="scheduler",
        payload={**request, "resume_job_id": "job", "requested_by": "scheduler"}, max_attempts=1,
    )
    runtime._job_service.bind_bridge_task.assert_called_once_with("job", "original")


def test_old_alarm_wait_does_not_block_new_daily_batch(monkeypatch):
    monkeypatch.setattr(bridge, "daily_scheduled_bucket_for_time", lambda: "2026-11-14 08")
    jobs = SimpleNamespace(
        list_jobs=lambda **_: [
            {"job_id": "old", "feature": "alarm_event_upload", "wait_reason": "waiting:shared_bridge"},
        ],
        load_job_stage_payload=lambda _: {"required_alarm_bucket": "2026-11-13 08"},
        cancel_job=Mock(),
    )
    runtime = SimpleNamespace(_job_service=jobs, _emit_system_log=Mock(), _reissue_unbound_waiting_job=Mock())
    bridge.SharedBridgeRuntimeService._reconcile_waiting_jobs(runtime)
    jobs.cancel_job.assert_called_once_with("old")
    runtime._reissue_unbound_waiting_job.assert_not_called()
