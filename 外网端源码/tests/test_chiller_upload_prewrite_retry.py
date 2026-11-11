import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.core.app_state import AppStateRepository
from app.modules.feishu.service.feishu_token_manager import FeishuTokenConnectTimeout
from app.modules.report_pipeline.api import routes
from app.modules.report_pipeline.service import job_service


def _selection(bucket):
    return {
        "best_bucket_key": bucket,
        "can_proceed": True,
        "selected_entries": [
            {"building": "A楼", "bucket_key": bucket, "file_path": f"{bucket[-5:].replace(':', '')}.json"},
        ],
    }


def _container(tmp_path, monkeypatch, selections):
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    bridge = SimpleNamespace(
        get_source_cache_buildings=lambda: ["A楼"],
        get_latest_source_cache_selection=Mock(side_effect=selections),
    )
    monkeypatch.setattr(routes, "_shared_bridge_service_or_raise", lambda _: bridge)
    monkeypatch.setattr(routes, "_deployment_role_mode", lambda _: "external")
    return SimpleNamespace(
        runtime_config=config,
        app_state_repository=AppStateRepository(runtime_config=config, app_dir=tmp_path),
        bridge=bridge,
        record_chiller_mode_upload_external_run=Mock(),
    )


def _engine(tmp_path, config, monkeypatch):
    engine = job_service.JobService()
    monkeypatch.setattr(engine, "_get_current_network_state", lambda: {
        "mode": "external_only", "internal_reachable": False, "external_reachable": True,
    })
    notify = Mock()
    monkeypatch.setattr(engine, "_notify_job_failure_to_feishu_async", notify)
    engine.configure_task_engine(runtime_config=config, app_dir=tmp_path)
    return engine, notify


def _wait_for_backoff(job):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and job.wait_reason != "waiting:feishu_auth":
        time.sleep(0.01)
    assert job.status == "waiting_resource"
    assert job.wait_reason == "waiting:feishu_auth"


def test_waiting_job_survives_restart_and_reads_latest_batch(tmp_path, monkeypatch):
    container = _container(tmp_path, monkeypatch, [
        _selection("2026-11-11 13:10"), _selection("2026-11-11 13:20"),
    ])
    uploads = []

    def run(self, *, source_units, before_write, **kwargs):
        path = source_units[0]["file_path"]
        if path == "1310.json":
            raise FeishuTokenConnectTimeout("token connect timeout")
        before_write()
        uploads.append(path)
        return {"status": "ok"}

    monkeypatch.setattr(routes.ChillerModeUploadService, "continue_from_source_units", run)
    original, notify = _engine(tmp_path, container.runtime_config, monkeypatch)
    container.job_service = original
    try:
        job = routes.start_chiller_mode_upload_job(container, priority="scheduler")
        _wait_for_backoff(job)
        assert routes.start_chiller_mode_upload_job(container, priority="manual").job_id == job.job_id
        unrelated = original.start_job("unrelated", lambda _: "ok", resource_keys=["network:external"])
        assert unrelated.done_event.wait(5) and unrelated.status == "success"
        assert uploads == []
        assert all(call.args[0].job_id != job.job_id for call in notify.call_args_list)
        # Make the recorded retry due at the next startup without waiting a real minute.
        with original._lock:
            job.result["prewrite_retry_at"] = time.time() - 1
            original._persist_job_snapshot(job)
    finally:
        original.shutdown_task_engine()

    restarted, _ = _engine(tmp_path, container.runtime_config, monkeypatch)
    container.job_service = restarted
    try:
        restored = restarted.get_job_state(job.job_id)
        assert restored.status == "waiting_resource" and restored.thread is None
        assert restored.result["prewrite_retry_count"] == 1
        routes.resume_chiller_mode_upload_jobs(container)
        assert restored.done_event.wait(5) and restored.status == "success", restored.error
        assert uploads == ["1320.json"]
        assert container.bridge.get_latest_source_cache_selection.call_count == 2
    finally:
        restarted.shutdown_task_engine()


def test_older_source_does_not_overwrite_newer_checkpoint(tmp_path, monkeypatch):
    container = _container(tmp_path, monkeypatch, [
        _selection("2026-11-11 13:20"), _selection("2026-11-11 13:10"),
    ])
    uploads = []

    def run(self, *, source_units, before_write, **kwargs):
        before_write()
        uploads.append(source_units[0]["file_path"])
        return {"status": "ok"}

    monkeypatch.setattr(routes.ChillerModeUploadService, "continue_from_source_units", run)
    assert routes._run_chiller_mode_job(container, lambda _: None, scheduled=False)["status"] == "ok"
    container.app_state_repository = AppStateRepository(runtime_config=container.runtime_config, app_dir=tmp_path)
    assert routes._run_chiller_mode_job(container, lambda _: None, scheduled=True)["status"] == "skipped"
    assert uploads == ["1320.json"]


def test_stale_building_fallback_does_not_overwrite_its_newer_data(tmp_path, monkeypatch):
    old = _selection("2026-11-11 13:20")
    old["selected_entries"].append({"building": "B楼", "bucket_key": "2026-11-11 13:20", "file_path": "B.json"})
    new = _selection("2026-11-11 13:30")
    new["selected_entries"].append({"building": "B楼", "bucket_key": "2026-11-11 13:10", "file_path": "B-old.json"})
    container = _container(tmp_path, monkeypatch, [old, new])
    container.bridge.get_source_cache_buildings = lambda: ["A楼", "B楼"]
    upload = Mock(side_effect=lambda **kwargs: kwargs["before_write"]() or {"status": "ok"})
    monkeypatch.setattr(routes.ChillerModeUploadService, "continue_from_source_units", upload)
    assert routes._run_chiller_mode_job(container, lambda _: None, scheduled=False)["status"] == "ok"
    assert routes._run_chiller_mode_job(container, lambda _: None, scheduled=False)["status"] == "skipped"
    upload.assert_called_once()


@pytest.mark.parametrize("after_write", [False, True])
def test_non_retryable_errors_remain_terminal(tmp_path, monkeypatch, after_write):
    container = _container(tmp_path, monkeypatch, [_selection("2026-11-11 13:20")])

    def run(self, *, before_write, **kwargs):
        if after_write:
            before_write()
            raise FeishuTokenConnectTimeout("token timed out after a write attempt")
        raise RuntimeError("invalid credentials")

    monkeypatch.setattr(routes.ChillerModeUploadService, "continue_from_source_units", run)
    with pytest.raises(FeishuTokenConnectTimeout if after_write else RuntimeError) as failure:
        routes._run_chiller_mode_job(container, lambda _: None, scheduled=False)
    assert not isinstance(failure.value, job_service.PrewriteJobRetry)


def test_deferred_job_expires_without_another_write_or_early_notification(tmp_path, monkeypatch):
    monkeypatch.setattr(job_service, "_PREWRITE_RETRY_WINDOW", 0.2)
    engine, notify = _engine(tmp_path, {"paths": {"runtime_state_root": str(tmp_path)}}, monkeypatch)
    run = Mock(side_effect=job_service.PrewriteJobRetry("token connect timeout"))
    try:
        job = engine.start_job("chiller", run, feature="chiller", retry_prewrite=True)
        assert job.done_event.wait(5)
        assert job.status == "failed"
        assert "未开始写入" in job.error
        run.assert_called_once()
        notify.assert_called_once_with(job)
    finally:
        engine.shutdown_task_engine()


def test_unopted_task_has_no_retry(tmp_path, monkeypatch):
    engine, _ = _engine(tmp_path, {"paths": {"runtime_state_root": str(tmp_path)}}, monkeypatch)
    run = Mock(side_effect=job_service.PrewriteJobRetry("not opted in"))
    try:
        job = engine.start_job("unrelated", run)
        assert job.done_event.wait(5) and job.status == "failed"
        run.assert_called_once()
    finally:
        engine.shutdown_task_engine()


def test_restart_never_replays_running_job_with_old_retry_metadata(tmp_path, monkeypatch):
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    engine, _ = _engine(tmp_path, config, monkeypatch)
    try:
        # Simulate a crash after dispatch, when it is unknown whether a write began.
        job = job_service.JobState("in-flight", "chiller", feature="chiller", status="running",
                                   result={"prewrite_retry_deadline": time.time() + 100})
        job.stages = [job_service.StageState("main", "chiller", status="running", resume_policy="prewrite_retry")]
        engine._persist_job_snapshot(job)
    finally:
        engine.shutdown_task_engine()
    restarted, _ = _engine(tmp_path, config, monkeypatch)
    try:
        replay = Mock()
        restarted.resume_deferred_prewrite_jobs("chiller", replay)
        assert restarted.get_job_state(job.job_id).status == "interrupted"
        replay.assert_not_called()
    finally:
        restarted.shutdown_task_engine()
