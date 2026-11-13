from datetime import datetime
import io
import json
from types import SimpleNamespace
from unittest.mock import Mock

from app.bootstrap.container import AppContainer
from app.modules.report_pipeline.service import energy_report_upload_service as module
from app.modules.report_pipeline.service import job_service as jobs_module
from app.modules.report_pipeline.service.job_service import JobService, JobState
from app.worker.task_handlers import HANDLER_REGISTRY


def test_scheduler_does_not_submit_until_files_are_ready(monkeypatch, tmp_path):
    class Clock(datetime):
        current = datetime(2026, 11, 13, 9, 4, 59)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr("app.bootstrap.container.datetime", Clock)
    monkeypatch.setattr(module, "completed", lambda *_: False)
    monkeypatch.setattr(
        "app.modules.feishu.service.feishu_auth_resolver.require_feishu_auth_settings", lambda _: {},
    )
    probe = Mock(side_effect=FileNotFoundError("E楼文件未齐"))
    monkeypatch.setattr(module.EnergyReportUploadService, "source_paths", probe)
    jobs = SimpleNamespace(start_worker_job=Mock(return_value=SimpleNamespace(job_id="test-id")))
    jobs.find_active_job_by_dedupe_key = Mock(return_value=None)
    container = SimpleNamespace(
        _configured_deployment_snapshot=lambda: {"role_mode": "external"},
        runtime_config={"paths": {"runtime_state_root": str(tmp_path)}}, job_service=jobs, add_system_log=Mock(),
    )
    assert AppContainer._submit_energy_report_upload(container, "测试") == (False, "skip:before_energy_report_check")
    probe.assert_not_called()
    Clock.current = datetime(2026, 11, 13, 9, 5)
    assert AppContainer._submit_energy_report_upload(container, "测试") == (False, "skip:source_not_ready")
    jobs.start_worker_job.assert_not_called()

    probe.side_effect = None
    paths = {key: tmp_path / f"{index}.xlsx" for index, key in enumerate(module._REQUIRED_SOURCES)}
    probe.return_value = paths
    assert AppContainer._submit_energy_report_upload(container, "测试") == (True, "test-id")
    kwargs = jobs.start_worker_job.call_args.kwargs
    assert kwargs["worker_payload"] == {"period": "2026-10", "source_manifest": module.source_manifest(paths)}
    assert kwargs["worker_handler"] in HANDLER_REGISTRY
    assert kwargs["dedupe_key"] == "energy_report_upload:2026-10"
    assert kwargs["resource_keys"] == ["network:external", module.FEATURE]
    assert probe.call_args.args == ("2026-10",)
    assert probe.call_args.kwargs["request_missing"] is True

    monkeypatch.setattr(module, "completed", lambda *_: True)
    assert not AppContainer._submit_energy_report_upload(container, "测试")[0]
    jobs.start_worker_job.assert_called_once()


def test_older_unready_month_does_not_block_ready_month(monkeypatch, tmp_path):
    monkeypatch.setattr(module, "pending_periods", lambda *_: ("2026-09", "2026-10"))
    monkeypatch.setattr("app.modules.feishu.service.feishu_auth_resolver.require_feishu_auth_settings", lambda _: {})
    probe = Mock(side_effect=[FileNotFoundError("9月未齐"), {}])
    monkeypatch.setattr(module.EnergyReportUploadService, "source_paths", probe)
    jobs = SimpleNamespace(start_worker_job=Mock(return_value=SimpleNamespace(job_id="test-id")))
    jobs.find_active_job_by_dedupe_key = Mock(return_value=None)
    container = SimpleNamespace(
        _configured_deployment_snapshot=lambda: {"role_mode": "external"},
        runtime_config={"paths": {"runtime_state_root": str(tmp_path)}}, job_service=jobs, add_system_log=Mock(),
    )
    assert AppContainer._submit_energy_report_upload(container, "测试") == (True, "test-id")
    assert [call.args[0] for call in probe.call_args_list] == ["2026-09", "2026-10"]
    assert jobs.start_worker_job.call_args.kwargs["worker_payload"] == {"period": "2026-10", "source_manifest": []}


def test_energy_job_waits_for_external_network_then_can_start(monkeypatch):
    service = JobService()
    job = JobState(job_id="energy", name="月度能耗报表上传", resource_keys=["network:external", module.FEATURE])
    state = {"mode": "internal_only", "internal_reachable": True, "external_reachable": False}
    monkeypatch.setattr(service, "_get_current_network_state", lambda: dict(state))

    unavailable = service._resource_unavailable_keys(job)
    assert unavailable == ["network:external"]
    assert "waiting:network_external_unreachable" in service._wait_reason_for_job(job, unavailable)

    state.update(mode="external_only", internal_reachable=False, external_reachable=True)
    assert service._resource_unavailable_keys(job) == []


def test_monthly_source_wait_survives_restart_then_resumes_same_worker_job(monkeypatch, tmp_path):
    class Clock(datetime):
        current = datetime(2026, 11, 13, 9, 5)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr("app.bootstrap.container.datetime", Clock)
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    result = {"status": "waiting_resource", "wait_reason": module.SOURCE_WAIT_REASON, "summary": "等待内网源文件"}
    responses = iter((result, {"status": "success"}))

    def spawn(*args, **kwargs):
        payload = next(responses)
        process = Mock(pid=123, returncode=0)
        process.stdout = io.StringIO(json.dumps({"type": "result", "ok": True, "payload": payload}) + "\n")
        process.stderr = io.StringIO("")
        process.wait.return_value = 0
        process.poll.return_value = 0
        return process

    monkeypatch.setattr(jobs_module.subprocess, "Popen", spawn)
    monkeypatch.setattr("app.modules.feishu.service.feishu_auth_resolver.require_feishu_auth_settings", lambda _: {})
    paths = {key: tmp_path / f"{index}.xlsx" for index, key in enumerate(module._REQUIRED_SOURCES)}
    probe = Mock(return_value=paths)
    monkeypatch.setattr(module.EnergyReportUploadService, "source_paths", probe)

    def engine():
        service = JobService()
        monkeypatch.setattr(service, "_ensure_worker_runtime_ready", lambda *_: "python")
        monkeypatch.setattr(service, "_get_current_network_state", lambda: {"mode": "external_only", "external_reachable": True})
        service.configure_task_engine(runtime_config=config, app_dir=tmp_path)
        return service

    jobs = engine()
    container = SimpleNamespace(runtime_config=config, job_service=jobs, add_system_log=Mock(),
                                _configured_deployment_snapshot=lambda: {"role_mode": "external"})
    try:
        ok, job_id = AppContainer._submit_energy_report_upload(container, "test")
        assert ok
        job = jobs.wait_job(job_id, timeout_sec=5)
        assert job.done_event.is_set() and job.status == "waiting_resource"
        assert job.wait_reason == module.SOURCE_WAIT_REASON
        assert not jobs._job_failure_status(job)
        assert job.acquired_resources == []
        module.defer_source_retry(config, "2026-10", "bridge busy", now=Clock.current)
    finally:
        jobs.shutdown_task_engine()

    jobs = container.job_service = engine()
    try:
        assert jobs.get_job_state(job_id).wait_reason == module.SOURCE_WAIT_REASON
        assert AppContainer._submit_energy_report_upload(container, "test") == (False, "skip:source_not_ready")
        probe.assert_called_once()
        Clock.current = datetime(2026, 11, 13, 9, 10)
        assert AppContainer._submit_energy_report_upload(container, "test") == (True, job_id)
        assert jobs.wait_job(job_id, timeout_sec=5).status == "success"
        assert probe.call_count == 2
    finally:
        jobs.shutdown_task_engine()


def test_continuous_bridge_wait_alerts_only_once_per_month(monkeypatch, tmp_path):
    class Clock(datetime):
        current = datetime(2026, 11, 13, 9, 5)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr("app.bootstrap.container.datetime", Clock)
    monkeypatch.setattr("app.modules.feishu.service.feishu_auth_resolver.require_feishu_auth_settings", lambda _: {})
    probe = Mock(side_effect=module.EnergyReportSourcePending("timed out"))
    monkeypatch.setattr(module.EnergyReportUploadService, "source_paths", probe)
    jobs = JobService()
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    jobs.configure_task_engine(runtime_config=config, app_dir=tmp_path)
    notify = Mock()
    monkeypatch.setattr(jobs, "_notify_job_failure_to_feishu_async", notify)
    container = SimpleNamespace(runtime_config=config, job_service=jobs, add_system_log=Mock(),
                                _configured_deployment_snapshot=lambda: {"role_mode": "external"})
    try:
        for minute in (0, 1, 5, 15, 30, 45):
            Clock.current = datetime(2026, 11, 13, 9, 5 + minute)
            assert not AppContainer._submit_energy_report_upload(container, "test")[0]
        assert probe.call_count == 5
        notify.assert_called_once()
        assert notify.call_args.args[0].status == "failed"
        assert jobs.find_active_job_by_dedupe_key("energy_report_upload:2026-10")["status"] == "waiting_resource"
        assert module.pending_periods(config, Clock.current) == ("2026-10",)
    finally:
        jobs.shutdown_task_engine()
