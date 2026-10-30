from __future__ import annotations

import copy
import sys
import threading
from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.config.config_adapter import adapt_runtime_config, ensure_v3_config, sync_runtime_back_to_v3
from app.config.inspection_data_config import INSPECTION_DATA_UPLOAD_DEFAULTS, normalize_inspection_scheduler
from app.modules.scheduler.service import inspection_data_scheduler as scheduling


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 27, 12, 0, 0)


class Orchestrator:
    timezone = "Asia/Shanghai"

    def __init__(self):
        self.jobs = {}

    def register_fire_handler(self, key, handler):
        self.handler = handler

    def add_job(self, **kwargs):
        self.jobs[kwargs["job_id"]] = SimpleNamespace(**kwargs)

    def add_date_job(self, **kwargs):
        self.jobs[kwargs["job_id"]] = SimpleNamespace(next_run_time=kwargs["run_date"], **kwargs)

    def get_job(self, key):
        return self.jobs.get(key)

    def remove_job(self, key):
        self.jobs.pop(key, None)

    def remove_jobs_by_prefix(self, prefix):
        self.jobs = {key: value for key, value in self.jobs.items() if not key.startswith(prefix)}


@pytest.fixture
def setup_scheduler(tmp_path, monkeypatch):
    monkeypatch.setattr(scheduling, "datetime", Clock)
    jobs = []

    def submit(slot):
        job = SimpleNamespace(job_id=f"job-{len(jobs)}", status="running", result=None,
                              done_event=threading.Event())
        jobs.append(job)
        return job

    scheduler = scheduling.InspectionDataScheduler(
        scheduler_key="inspection_data_upload", feature="inspection_data_upload",
        scheduler_cfg={"enabled": True, "run_times": ["01:10:00", "13:10:00"]},
        runtime_state_root=str(tmp_path), emit_log=lambda line: None, run_callback=submit,
        is_busy=lambda: False, lookup_job=lambda key: next((job for job in jobs if job.job_id == key), None),
        orchestrator=Orchestrator(), schedule_kind="daily", source_name="巡检数据处理",
    )
    completed = threading.Event()
    original_finish = scheduler._finish

    def finish(*args):
        original_finish(*args)
        completed.set()

    monkeypatch.setattr(scheduler, "_finish", finish)
    scheduler.start()
    yield scheduler, jobs, completed
    scheduler.stop()
    for job in jobs:
        job.done_event.set()


def finish_job(job, completed, status="success", result_status="success"):
    completed.clear()
    job.status = status
    job.result = {"status": result_status}
    job.done_event.set()
    assert completed.wait(3), "completion event was not observed"


def test_defaults_and_runtime_v3_roundtrip():
    v3 = ensure_v3_config({"common": {}, "features": {}})
    feature = v3["features"]["inspection_data_upload"]
    assert feature == INSPECTION_DATA_UPLOAD_DEFAULTS
    assert feature["enabled"] is True
    assert feature["buildings"] == ["D楼", "E楼"]
    assert feature["source_families"] == {"电气": "electrical_inspection_family", "暖通": "hvac_inspection_family"}
    assert "source_family" not in feature
    assert feature["scheduler"]["run_times"] == []
    assert feature["scheduler"]["enabled"] is False
    assert feature["scheduler"]["auto_start_in_gui"] is False
    assert feature["scheduler"]["catch_up_if_missed"] is False
    runtime = adapt_runtime_config(v3)
    runtime["inspection_data_upload"]["scheduler"]["run_times"] = ["13:00:00", "01:10:00", "13:00:00"]
    result = sync_runtime_back_to_v3(v3, runtime)
    assert result["features"]["inspection_data_upload"]["scheduler"]["run_times"] == ["01:10:00", "13:00:00"]
    assert ensure_v3_config({"inspection_data_upload": runtime["inspection_data_upload"]})["features"]["inspection_data_upload"]["scheduler"]["run_times"] == ["01:10:00", "13:00:00"]


@pytest.mark.parametrize("times", ["01:10:00", ["24:00:00"], ["01:60:00"], ["01:10"], [None]])
def test_invalid_times_rejected(times):
    with pytest.raises(ValueError):
        normalize_inspection_scheduler({"run_times": times})


def test_disabling_or_removing_all_times_clears_autostart():
    for patch in ({"enabled": False, "run_times": ["01:10:00"]}, {"enabled": True, "run_times": []}):
        assert normalize_inspection_scheduler({**patch, "auto_start_in_gui": True})["auto_start_in_gui"] is False


def test_slots_only_succeed_after_job_completion(setup_scheduler):
    scheduler, jobs, completed = setup_scheduler
    scheduler._fire(trigger_source="slot:01:10:00")
    assert scheduler.state["last_success_slot"] == ""
    assert scheduler.state["slots"]["2026-10-27 01:10:00"]["status"] == "submitted"
    scheduler._fire(trigger_source="slot:01:10:00")
    assert len(jobs) == 1
    finish_job(jobs[0], completed)
    assert scheduler.state["last_success_slot"] == "2026-10-27 01:10:00"
    scheduler._fire(trigger_source="slot:13:10:00")
    assert len(jobs) == 2
    finish_job(jobs[1], completed)
    assert scheduler.state["last_success_slot"] == "2026-10-27 13:10:00"
    scheduler._fire(trigger_source="slot:01:10:00")
    assert len(jobs) == 2


@pytest.mark.parametrize("result_status", ["failed", "skipped", "waiting"])
def test_non_success_results_have_only_one_retry(setup_scheduler, result_status):
    scheduler, jobs, completed = setup_scheduler
    scheduler._fire(trigger_source="slot:01:10:00")
    finish_job(jobs[0], completed, result_status=result_status)
    assert scheduler.state["last_success_slot"] == ""
    assert "inspection_data_upload:retry:2026-10-27 01:10:00" in scheduler.orchestrator.jobs
    scheduler._fire(trigger_source="retry:2026-10-27 01:10:00")
    finish_job(jobs[1], completed, result_status=result_status)
    scheduler._fire(trigger_source="retry:2026-10-27 01:10:00")
    assert len(jobs) == 2


def test_stop_cancels_retries_and_blocks_late_fire(setup_scheduler):
    scheduler, jobs, completed = setup_scheduler
    scheduler._fire(trigger_source="slot:01:10:00")
    thread = next(iter(scheduler._watchers.values()))[1]
    scheduler.stop()
    thread.join(2)
    assert not thread.is_alive()
    jobs[0].status = "failed"
    jobs[0].done_event.set()
    assert not scheduler.orchestrator.jobs
    scheduler._fire(trigger_source="slot:13:10:00")
    scheduler._fire(trigger_source="retry:2026-10-27 01:10:00")
    assert len(jobs) == 1
    assert scheduler.state["slots"]["2026-10-27 01:10:00"]["status"] == "submitted"
    scheduler.start()
    assert completed.wait(3)
    assert scheduler.state["slots"]["2026-10-27 01:10:00"]["status"] == "failed"


def test_reload_preserves_pending_and_completed_slots(setup_scheduler):
    scheduler, jobs, completed = setup_scheduler
    scheduler._fire(trigger_source="slot:01:10:00")
    scheduler.reconfigure({**scheduler.cfg, "run_times": ["01:10:00", "14:00:00"]})
    scheduler.start()
    scheduler._fire(trigger_source="slot:01:10:00")
    assert len(jobs) == 1
    finish_job(jobs[0], completed)
    scheduler.reconfigure(scheduler.cfg)
    scheduler.start()
    scheduler._fire(trigger_source="slot:01:10:00")
    assert len(jobs) == 1
    assert scheduler.state_repository.load(scheduler.state_path)["last_success_slot"] == "2026-10-27 01:10:00"


def test_busy_does_not_queue_unbounded_work(setup_scheduler):
    scheduler, jobs, completed = setup_scheduler
    scheduler.is_busy = lambda: True
    for _ in range(5):
        scheduler._fire(trigger_source="slot:01:10:00")
    assert jobs == []
    assert scheduler.state["slots"]["2026-10-27 01:10:00"]["attempts"] == 0
    assert len([key for key in scheduler.orchestrator.jobs if ":retry:" in key]) == 1
    scheduler.is_busy = lambda: False
    scheduler._fire(trigger_source="retry:2026-10-27 01:10:00")
    scheduler._fire(trigger_source="slot:13:10:00")
    assert len(jobs) == 1
    assert scheduler.state["slots"]["2026-10-27 13:10:00"]["attempts"] == 0
    finish_job(jobs[0], completed)
    scheduler._fire(trigger_source="retry:2026-10-27 13:10:00")
    assert len(jobs) == 2
    finish_job(jobs[1], completed)


def test_persistence_failure_prevents_submission(setup_scheduler, monkeypatch):
    scheduler, jobs, _ = setup_scheduler
    monkeypatch.setattr(scheduler, "_save_state", lambda: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        scheduler._fire(trigger_source="slot:01:10:00")
    assert jobs == []


def test_empty_schedule_cannot_start(setup_scheduler):
    scheduler, _, _ = setup_scheduler
    scheduler.reconfigure({"run_times": []})
    with pytest.raises(ValueError):
        scheduler.start()
    assert not scheduler.is_running()


@pytest.fixture
def fake_inspection_service(monkeypatch):
    calls = []

    class Service:
        def __init__(self, config):
            self.config = config

        @staticmethod
        def normalize_source_date(value):
            return datetime.strptime(value or "2026-10-27", "%Y-%m-%d").strftime("%Y-%m-%d")

        def run(self, **kwargs):
            calls.append(kwargs)
            return {"status": "success"}

    monkeypatch.setitem(sys.modules, "app.modules.inspection_data.service.inspection_data_upload_service",
                        SimpleNamespace(InspectionDataUploadService=Service))
    return calls


def test_worker_passes_contract_and_cancellation(fake_inspection_service):
    from app.worker.task_handlers import HANDLER_REGISTRY

    checks = []
    runtime = SimpleNamespace(raise_if_cancelled=lambda: checks.append(True))
    log = lambda line: None
    result = HANDLER_REGISTRY["inspection_data_upload"]({}, {"source_date": "2026-10-27", "dry_run": True}, log, runtime)
    assert result["status"] == "success"
    assert len(checks) == 2
    assert fake_inspection_service[0] == {
        "source_date": "2026-10-27", "dry_run": True, "log": log, "check_cancel": runtime.raise_if_cancelled,
    }


def test_manual_and_scheduler_use_global_resource_and_dedupe(fake_inspection_service, monkeypatch):
    from app.bootstrap.container import AppContainer
    from app.modules.report_pipeline.api import routes

    monkeypatch.setattr("app.bootstrap.container.datetime", Clock)
    captured = []
    job = SimpleNamespace(job_id="test", to_dict=lambda: {"job_id": "test"})
    container = SimpleNamespace(
        runtime_config={"inspection_data_upload": {"enabled": True}},
        deployment_snapshot=lambda: {"role_mode": "external"},
        _configured_deployment_snapshot=lambda: {"role_mode": "external"},
        job_service=SimpleNamespace(start_worker_job=lambda **kwargs: captured.append(kwargs) or job,
                                    get_job_state=lambda key: job),
    )
    monkeypatch.setattr(routes, "_start_background_job", lambda container, **kwargs: captured.append(kwargs) or job)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))
    for date in ["2026-10-26", "2026-10-27"]:
        routes.job_inspection_data_upload_run(request, {"source_date": date, "dry_run": True})
    AppContainer._submit_inspection_data_upload(container, "2026-10-27 01:10:00")
    for call in captured:
        assert "inspection_data_upload:global" in call["resource_keys"]
        assert call["dedupe_key"] == "inspection_data_upload"
        assert call["worker_handler"] == "inspection_data_upload"
    with pytest.raises(HTTPException) as invalid:
        routes.job_inspection_data_upload_run(request, {"source_date": "bad"})
    assert invalid.value.status_code == 400
    container.deployment_snapshot = lambda: {"role_mode": "internal"}
    with pytest.raises(HTTPException) as internal:
        routes.job_inspection_data_upload_run(request, {})
    assert internal.value.status_code == 409


def test_scheduler_config_validates_before_save_and_stop_first(monkeypatch):
    from app.modules.scheduler.api import inspection_data_upload_routes as routes

    calls = []
    feature = copy.deepcopy(INSPECTION_DATA_UPLOAD_DEFAULTS)
    container = SimpleNamespace(
        config={"features": {"inspection_data_upload": feature}},
        runtime_config={"inspection_data_upload": feature},
        _configured_deployment_snapshot=lambda: {"role_mode": "external"},
        inspection_data_upload_scheduler_status=lambda: {"running": False, "run_times": feature["scheduler"]["run_times"]},
        rebuild_inspection_data_upload_scheduler=lambda **kw: calls.append("reload") or {},
        stop_inspection_data_upload_scheduler=lambda: calls.append("stop") or {},
        start_inspection_data_upload_scheduler=lambda: calls.append("start") or {},
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))
    monkeypatch.setattr(routes, "persist_full_config", lambda *a, **kw: calls.append("save"))
    monkeypatch.setattr(routes, "record_scheduler_config_autostart", lambda *a, **kw: None)
    monkeypatch.setattr(routes, "persist_scheduler_toggle", lambda *a, **kw: calls.append("toggle"))
    with pytest.raises(HTTPException):
        routes.inspection_data_upload_scheduler_config({"run_times": ["99:00:00"]}, request)
    with pytest.raises(HTTPException):
        routes.inspection_data_upload_scheduler_start(request)
    assert not calls
    result = routes.inspection_data_upload_scheduler_config({"run_times": ["14:00:00", "01:10:00"]}, request)
    assert result["scheduler_config"]["run_times"] == ["01:10:00", "14:00:00"]
    assert calls == ["save", "reload"]
    calls.clear()
    routes.inspection_data_upload_scheduler_stop(request)
    assert calls == ["stop", "toggle"]


def test_container_reload_keeps_object_and_honors_disable(setup_scheduler):
    from app.bootstrap.container import AppContainer

    scheduler, _, _ = setup_scheduler
    container = SimpleNamespace(
        runtime_config={"inspection_data_upload": {"enabled": True, "scheduler": {**scheduler.cfg, "enabled": False}}},
        inspection_data_upload_scheduler=scheduler,
        _configured_deployment_snapshot=lambda: {"role_mode": "external"}, runtime_services_armed=True,
        start_inspection_data_upload_scheduler=lambda **kwargs: pytest.fail("disabled scheduler started"),
    )
    result = AppContainer.rebuild_inspection_data_upload_scheduler(container)
    assert result["was_running"] is True
    assert container.inspection_data_upload_scheduler is scheduler
    assert not scheduler.is_running()
    assert not scheduler.orchestrator.jobs


def test_restart_resumes_pending_job_without_resubmitting(setup_scheduler, monkeypatch):
    scheduler, jobs, _ = setup_scheduler
    monkeypatch.setattr(scheduler, "_watch_job", lambda *args: None)
    scheduler._fire(trigger_source="slot:01:10:00")
    scheduler.stop()
    restored = scheduling.InspectionDataScheduler(
        scheduler_key=scheduler.scheduler_key, feature=scheduler.feature, scheduler_cfg=scheduler.cfg,
        runtime_state_root=scheduler.runtime_state_root, emit_log=lambda line: None,
        run_callback=lambda slot: pytest.fail("pending task was submitted twice"),
        is_busy=lambda: False, lookup_job=scheduler.lookup_job, orchestrator=scheduler.orchestrator,
        schedule_kind="daily", source_name=scheduler.source_name,
    )
    completed = threading.Event()
    original_finish = restored._finish

    def finish(*args):
        original_finish(*args)
        completed.set()

    monkeypatch.setattr(restored, "_finish", finish)
    restored.start()
    restored._fire(trigger_source="slot:01:10:00")
    finish_job(jobs[0], completed)
    assert restored.state["last_success_slot"] == "2026-10-27 01:10:00"
    restored.stop()


def test_scheduler_overview_contains_multiple_times():
    from app.modules.report_pipeline.service.scheduler_state_presenter import present_scheduler_overview_items

    items = present_scheduler_overview_items(
        {"inspection_data_upload": {"scheduler": {"run_times": ["01:10:00", "13:10:00"]}}},
        {"inspection_data_upload_scheduler": {"running": True}},
        role_mode="external",
    )
    item = next(item for item in items if item["key"] == "inspection_data_upload")
    assert len(item["parts"]) == 2


@pytest.mark.parametrize("found", [False, True])
def test_dispatching_recovery_checks_existing_job(setup_scheduler, found):
    scheduler, jobs, completed = setup_scheduler
    scheduler.stop()
    slot = "2026-10-27 01:10:00"
    scheduler.state["slots"][slot] = {"status": "dispatching", "attempts": 1,
                                       "attempt_started_at": "2026-10-27 12:00:00"}
    scheduler.find_slot_job = lambda *args: {
        "job_id": "existing", "status": "success", "result": {"status": "success"},
    } if found else None
    scheduler.start()
    if found:
        assert scheduler.state["last_success_slot"] == slot
        scheduler._fire(trigger_source=f"retry:{slot}")
        assert not jobs
    else:
        assert scheduler.state["slots"][slot]["status"] == "deferred"
        assert scheduler.state["slots"][slot]["attempts"] == 0
        scheduler._fire(trigger_source=f"retry:{slot}")
        assert len(jobs) == 1
        finish_job(jobs[0], completed)


def test_restart_uses_persisted_terminal_job(setup_scheduler, monkeypatch):
    scheduler, jobs, _ = setup_scheduler
    monkeypatch.setattr(scheduler, "_watch_job", lambda *args: None)
    scheduler._fire(trigger_source="slot:01:10:00")
    scheduler.stop()
    scheduler.lookup_job = lambda key: {"job_id": key, "status": "success", "result": {"status": "success"}}
    scheduler.start()
    assert scheduler.state["last_success_slot"] == "2026-10-27 01:10:00"
    assert len(jobs) == 1


def test_old_deferred_slots_expire_without_submission(setup_scheduler):
    scheduler, jobs, _ = setup_scheduler
    old = "2026-10-26 01:10:00"
    scheduler.state["slots"][old] = {"status": "deferred", "attempts": 0}
    scheduler._schedule_date(old, "retry", Clock.now())
    scheduler._fire(trigger_source=f"retry:{old}")
    assert not jobs
    assert scheduler.state["slots"][old]["status"] == "expired"
    assert f"inspection_data_upload:retry:{old}" not in scheduler.orchestrator.jobs
    scheduler.state["slots"]["2026-10-25 01:10:00"] = {"status": "deferred", "attempts": 0}
    scheduler._expire_obsolete_slots(Clock.now())
    assert "2026-10-25 01:10:00" not in scheduler.state["slots"]


def test_removed_pending_slot_is_not_restored(setup_scheduler):
    scheduler, jobs, _ = setup_scheduler
    scheduler._fire(trigger_source="slot:01:10:00")
    thread = next(iter(scheduler._watchers.values()))[1]
    scheduler.reconfigure({**scheduler.cfg, "run_times": []})
    thread.join(2)
    assert not thread.is_alive()
    assert scheduler.state["slots"]["2026-10-27 01:10:00"]["status"] == "expired"
    scheduler.reconfigure({**scheduler.cfg, "run_times": ["14:10:00"]})
    scheduler.start()
    assert not scheduler._watchers
    assert scheduler.state["slots"]["2026-10-27 01:10:00"]["status"] == "expired"
    scheduler._fire(trigger_source="slot:14:10:00")
    assert len(jobs) == 2


def test_dry_run_is_not_upload_slot_success():
    assert scheduling.InspectionDataScheduler._job_result_status({
        "status": "success", "result": {"status": "success", "dry_run": True},
    }) == "skipped"


def test_split_manual_dry_run_keeps_default_scheduler_off(monkeypatch):
    from app.bootstrap.container import AppContainer
    from app.modules.inspection_data.service.inspection_data_upload_service import InspectionDataUploadService

    config = {"inspection_data_upload": copy.deepcopy(INSPECTION_DATA_UPLOAD_DEFAULTS)}
    before = copy.deepcopy(config)
    service = InspectionDataUploadService(config)
    def unavailable(*args):
        raise RuntimeError('mock source missing')
    monkeypatch.setattr(service, '_sources', unavailable)
    with pytest.raises(RuntimeError, match='mock source missing'):
        service.run(dry_run=True)
    assert config == before
    feature = config['inspection_data_upload']
    assert feature['scheduler']['enabled'] is False
    assert feature['scheduler']['auto_start_in_gui'] is False
    assert feature['scheduler']['run_times'] == []
    container = SimpleNamespace(
        runtime_config=config, inspection_data_upload_scheduler=None,
        _configured_deployment_snapshot=lambda: {'role_mode': 'external'}, runtime_services_armed=True,
        start_inspection_data_upload_scheduler=lambda **kwargs: pytest.fail('disabled scheduler started'),
        _build_inspection_data_upload_scheduler=lambda: SimpleNamespace(),
    )
    result = AppContainer.rebuild_inspection_data_upload_scheduler(container)
    assert result['action'] == {'started': False, 'running': False, 'reason': 'disabled'}
