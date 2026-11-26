import time
from datetime import datetime
from functools import partial
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.bootstrap import app_factory
from app.bootstrap.app_factory import (
    _HANDOVER_ATTACHMENT_CATCHUP_FEATURE,
    _resume_handover_attachment_catchup_jobs,
    _run_handover_attachment_catchup,
)
from app.modules.report_pipeline.service import job_service as jobs
from handover_log_module.service import review_followup_trigger_service as followup
from handover_log_module.service.handover_delivery_workflow_service import HandoverDeliveryWorkflowService
from handover_log_module.service.handover_generation_guard import generation_lock


BATCH = "2026-11-24|day"
RESOURCES = ["network:external", f"handover_batch:{BATCH}"]


def _engine(tmp_path, config, monkeypatch):
    engine = jobs.JobService()
    engine.configure_task_engine(runtime_config=config, app_dir=tmp_path)
    monkeypatch.setattr(engine, "_get_current_network_state", lambda: {"mode": "external_only", "external_reachable": True})
    notify = Mock()
    monkeypatch.setattr(engine, "_notify_job_failure_to_feishu_async", notify)
    return engine, notify


def _start(engine, config):
    return engine.start_job(
        "交接班定时确认并上传多维（白班）",
        partial(_run_handover_attachment_catchup, config, BATCH),
        resource_keys=RESOURCES, feature=_HANDOVER_ATTACHMENT_CATCHUP_FEATURE,
        dedupe_key=f"handover:cloud_catchup:cloud_catchup_afternoon:{BATCH}", retry_deferred=True,
    )


def _mock_upload(monkeypatch, *cloud_results):
    upload = Mock(side_effect=[{"status": result["status"], "cloud_sheet_sync": result} for result in cloud_results])
    monkeypatch.setattr(followup, "ReviewFollowupTriggerService", lambda _config: SimpleNamespace(
        upload_pending_cloud_sheets_for_batch=upload,
    ))
    return upload


def test_pending_handover_releases_resources_and_resumes_same_batch_after_restart(monkeypatch, tmp_path):
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    upload = _mock_upload(
        monkeypatch, {"status": "pending", "missing_buildings": ["C楼"]}, {"status": "ok"},
    )
    engine, notify = _engine(tmp_path, config, monkeypatch)
    try:
        job = _start(engine, config)
        deadline = time.monotonic() + 8
        while not (job.status == "waiting_resource" and job.wait_reason == "waiting:handover_artifacts"
                   and not job.acquired_resources):
            assert time.monotonic() < deadline
            time.sleep(.01)
        assert "C楼" in job.summary
        notify.assert_not_called()
        assert _start(engine, config).job_id == job.job_id
        competing = engine.start_job("manual floor", lambda _: {"status": "ok"}, resource_keys=RESOURCES)
        assert competing.done_event.wait(5) and competing.status == "success"
        upload.assert_called_once()
    finally:
        engine.shutdown_task_engine()

    restarted, _ = _engine(tmp_path, config, monkeypatch)
    try:
        resumed = restarted.get_job_state(job.job_id)
        assert resumed.status == "waiting_resource" and resumed.wait_reason == "waiting:handover_artifacts"
        resumed.result["prewrite_retry_at"] = time.time() - 1
        _resume_handover_attachment_catchup_jobs(SimpleNamespace(job_service=restarted, runtime_config=config))
        assert resumed.done_event.wait(5) and resumed.status == "success"
        assert [call.kwargs["batch_key"] for call in upload.call_args_list] == [BATCH, BATCH]
    finally:
        restarted.shutdown_task_engine()


def test_late_generation_keeps_original_upload_job_past_default_window(monkeypatch, tmp_path):
    monkeypatch.setattr(jobs, "_PREWRITE_RETRY_WINDOW", .05)
    monkeypatch.setattr(jobs, "_PREWRITE_RETRY_DELAYS", (.25,))
    cutoff = time.time() + 5
    monkeypatch.setattr(app_factory, "_handover_attachment_retry_until", lambda *_: cutoff)
    upload = _mock_upload(monkeypatch, {
        "status": "partial_failed", "failed_buildings": [{"building": "A楼", "error": "request failed"}],
        "missing_buildings": ["A楼", "C楼"], "waiting_generation_buildings": ["C楼"],
    }, {"status": "ok"})
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    engine, _ = _engine(tmp_path, config, monkeypatch)
    try:
        job = _start(engine, config)
        assert job.done_event.wait(5) and job.status == "success", job.error
        assert [call.kwargs["batch_key"] for call in upload.call_args_list] == [BATCH, BATCH]
        assert not engine._job_failure_status(job)
    finally:
        engine.shutdown_task_engine()


@pytest.mark.parametrize(("shift", "expected"), [("day", "02:30"), ("night", "16:30")])
def test_extended_upload_deadline_uses_next_generation_time(shift, expected):
    config = {"handover_log": {"scheduler": {"morning_time": "02:00", "afternoon_time": "16:00:00"}}}
    until = app_factory._handover_attachment_retry_until(config, f"2026-11-24|{shift}")
    assert datetime.fromtimestamp(until).strftime("%Y-%m-%d %H:%M") == f"2026-11-25 {expected}"


def test_pending_handover_expires_and_names_missing_floor_once(monkeypatch, tmp_path):
    monkeypatch.setattr(jobs, "_PREWRITE_RETRY_WINDOW", .15)
    upload = _mock_upload(monkeypatch, {"status": "pending", "missing_buildings": ["C楼"]})
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    engine, notify = _engine(tmp_path, config, monkeypatch)
    try:
        job = _start(engine, config)
        assert job.done_event.wait(5) and job.status == "failed"
        assert BATCH in job.error and "C楼" in job.error and "等待交接班成品超时" in job.error
        upload.assert_called_once()
        notify.assert_called_once_with(job)
    finally:
        engine.shutdown_task_engine()


@pytest.mark.parametrize("retryable", [False, True])
def test_only_retryable_preparation_errors_defer(monkeypatch, tmp_path, retryable):
    _mock_upload(monkeypatch, {"status": "partial_failed", "failed_buildings": [
        {"building": "C楼", "error": "mock error", "retryable": retryable},
    ]})
    error_type = jobs.HandoverArtifactsPending if retryable else RuntimeError
    with pytest.raises(error_type, match="C楼.*mock error") as failure:
        _run_handover_attachment_catchup({"paths": {"runtime_state_root": str(tmp_path)}}, BATCH, lambda _: None)
    assert type(failure.value) is error_type


def test_preparing_batch_does_not_replace_artifacts_that_became_ready_after_initial_read():
    workflow = object.__new__(HandoverDeliveryWorkflowService)
    old = {"building": "C楼", "session_id": f"C楼|{BATCH}", "revision": 3, "confirmed": False}
    latest = {**old, "revision": 4, "confirmed": True}
    workflow.review = SimpleNamespace(get_session_by_id=Mock(return_value=latest))
    workflow.state = SimpleNamespace(
        ACTIVE={"queued", "preparing"}, for_session=Mock(return_value={"artifacts_ready": True, "status": "ready"}),
        artifacts_exist=Mock(return_value=True), begin=Mock(),
    )
    workflow.prepare = Mock()
    assert workflow.prepare_sessions([old], lambda _: None, confirm_unconfirmed=True) == []
    workflow.state.for_session.assert_called_once_with(latest)
    workflow.state.begin.assert_not_called()
    workflow.prepare.assert_not_called()


def test_upload_prepares_other_floor_while_same_floor_generation_is_running():
    workflow = object.__new__(HandoverDeliveryWorkflowService)
    sessions = [{"building": building, "session_id": f"{building}|{BATCH}", "confirmed": True, "revision": 1}
                for building in ("B楼", "C楼")]
    workflow.review = SimpleNamespace(get_session_by_id=Mock(return_value=sessions[0]))
    workflow.state = SimpleNamespace(ACTIVE=(), for_session=Mock(return_value={}), begin=Mock())
    workflow.prepare = Mock()
    with generation_lock("C楼", "2026-11-24", "day"):
        failed = workflow.prepare_sessions(sessions, lambda _: None, confirm_unconfirmed=True)
    assert [item["building"] for item in failed if item["retryable"]] == ["C楼"]
    assert workflow.prepare.call_args.args[0] == f"B楼|{BATCH}"
