from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest
import requests

from app.modules.report_pipeline.service.job_service import JobService, JobState, StageState
from app.modules.report_pipeline.service.task_engine_database import TaskEngineDatabase
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient


def test_database_close_drains_accepted_writes(tmp_path):
    db = TaskEngineDatabase(runtime_config={"paths": {"runtime_state_root": str(tmp_path)}}, app_dir=tmp_path)
    entered, release = threading.Event(), threading.Event()
    first_done, second_done = threading.Event(), threading.Event()
    first_holder, second_holder = {}, {}
    db._writes.put((lambda conn: (entered.set(), release.wait(3)), first_done, first_holder))
    assert entered.wait(2)
    db._writes.put((lambda conn: "second", second_done, second_holder))
    closer = threading.Thread(target=db.close)
    closer.start()
    release.set()
    closer.join(4)
    assert not closer.is_alive()
    assert first_done.is_set() and second_done.is_set()
    assert second_holder == {"result": "second"}
    with pytest.raises(RuntimeError, match="已关闭"):
        db._write(lambda conn: None)


def test_config_reload_reuses_database_and_rejects_live_root_change(tmp_path):
    service = JobService()
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    service.configure_task_engine(runtime_config=config, app_dir=tmp_path)
    original = service._task_engine_db
    try:
        service.configure_task_engine(runtime_config=config, app_dir=tmp_path)
        assert service._task_engine_db is original
        assert not original._closed
        with pytest.raises(RuntimeError, match="重启"):
            service.configure_task_engine(
                runtime_config={"paths": {"runtime_state_root": str(tmp_path / "other")}}, app_dir=tmp_path
            )
        assert service._task_engine_db is original
    finally:
        service.shutdown_task_engine()


def test_shutdown_preserves_requeue_and_shared_bridge_waiting(tmp_path, monkeypatch):
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    service = JobService()
    service.configure_task_engine(runtime_config=config, app_dir=tmp_path)
    job = service.create_waiting_worker_job("test", worker_handler="test", resume_policy="requeue")
    stage = service._get_primary_stage(job)
    job.status = stage.status = "running"
    job.wait_reason = ""
    waiting = service.create_waiting_worker_job("wait", worker_handler="test", resume_policy="requeue")
    service.shutdown_task_engine()
    assert job.status == "queued"
    assert not job.cancel_requested and not stage.cancel_requested
    assert waiting.status == "waiting_resource"
    with pytest.raises(RuntimeError, match="关闭"):
        service.start_job("late", lambda emit: None)
    with pytest.raises(RuntimeError, match="关闭"):
        service.start_worker_job("late", worker_handler="test")
    restored = JobService()
    launched = []
    monkeypatch.setattr(restored, "_launch_existing_worker_job", lambda j, s, **kw: launched.append(j.job_id))
    try:
        restored.configure_task_engine(runtime_config=config, app_dir=tmp_path)
        assert launched == [job.job_id]
        assert restored._jobs[waiting.job_id].wait_reason == "waiting:shared_bridge"
    finally:
        restored.shutdown_task_engine()


def test_shutdown_stops_resource_waiter_without_user_cancel():
    service = JobService()
    service._resource_holders["test:exclusive"] = ["busy"]
    job = service.start_job("queued", lambda emit: pytest.fail("must not run"), resource_keys=["test:exclusive"])
    service.shutdown_task_engine()
    assert job.done_event.wait(2)
    assert not job.thread.is_alive()
    assert not job.cancel_requested
    assert job.status == "interrupted"


def test_running_worker_shutdown_requeues_instead_of_cancelled(tmp_path, monkeypatch):
    import io
    from app.modules.report_pipeline.service import job_service as module
    entered, exited = threading.Event(), threading.Event()
    class Process:
        pid = 123
        returncode = None
        stdout = io.StringIO("")
        stderr = io.StringIO("")
        def poll(self):
            return self.returncode
        def wait(self, timeout=None):
            assert exited.wait(3)
            return self.returncode
    process = Process()
    def popen(*args, **kwargs):
        entered.set()
        return process
    def cancel(**kwargs):
        process.returncode = 1
        exited.set()
    service = JobService()
    service.configure_task_engine(runtime_config={"paths": {"runtime_state_root": str(tmp_path)}}, app_dir=tmp_path)
    monkeypatch.setattr(module.subprocess, "Popen", popen)
    monkeypatch.setattr(service, "_ensure_worker_runtime_ready", lambda *a: "python")
    monkeypatch.setattr(service, "_send_worker_command", cancel)
    job = service.start_worker_job("test", worker_handler="test", resume_policy="requeue")
    assert entered.wait(3)
    service.shutdown_task_engine()
    assert job.done_event.wait(3)
    assert job.status == "queued"
    assert not job.cancel_requested
    assert service._task_engine_db is None


@pytest.mark.parametrize("status", ["blocked", "stale", "expired"])
def test_prune_all_terminal_statuses(status):
    service = JobService()
    service._task_engine_db = object()
    for index in range(40):
        job = JobState(job_id=str(index), name="test", status=status, sequence=index)
        job.done_event.set()
        service._jobs[job.job_id] = job
    assert service._prune_terminal_jobs_in_memory() == 8
    assert len(service._jobs) == 32

def test_pid_lookup_timeout_is_not_treated_as_exited(monkeypatch):
    from app.modules.report_pipeline.service import job_service as module
    def fail(*args, **kwargs):
        raise TimeoutError("process query unavailable")
    if module.os.name == "nt":
        monkeypatch.setattr(module.subprocess, "run", fail)
    else:
        monkeypatch.setattr(module.os, "kill", fail)
    assert JobService._pid_exists(123)


def test_configure_during_worker_shutdown_is_rejected(tmp_path):
    service = JobService()
    service._shutting_down = True
    job = JobState(job_id="closing", name="closing")
    job.thread = SimpleNamespace(is_alive=lambda: True)
    service._jobs[job.job_id] = job
    with pytest.raises(RuntimeError, match="关闭"):
        service.configure_task_engine(runtime_config={"paths": {"runtime_state_root": str(tmp_path)}}, app_dir=tmp_path)
    assert service._shutting_down
    assert job.job_id in service._jobs
    assert service._task_engine_db is None



def test_orphan_cleanup_rejects_unverified_pid(monkeypatch):
    service = JobService()
    monkeypatch.setattr(service, "_worker_identity_matches", lambda *args: False)
    monkeypatch.setattr("subprocess.run", lambda *args, **kwargs: pytest.fail("must not terminate"))
    assert not service._terminate_orphan_worker(123, job_id="old", stage_id="main")


def test_worker_identity_requires_own_job_path(monkeypatch, tmp_path):
    from app.modules.report_pipeline.service import job_service as module
    if module.os.name != "nt":
        pytest.skip("Windows command line lookup")
    service = JobService()
    service._worker_app_dir = tmp_path
    service._task_engine_store = SimpleNamespace(jobs_root=tmp_path / "jobs")
    import json
    own_command = f'python "{tmp_path / "worker_bootstrap.py"}" --job-dir "{tmp_path / "jobs" / "job1"}" --stage-id main'
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps(own_command)))
    assert service._worker_identity_matches(123, "job1", "main")
    assert not service._worker_identity_matches(123, "job2", "main")


def test_create_records_retries_use_one_idempotency_key(monkeypatch):
    monkeypatch.setattr(
        "app.modules.feishu.service.bitable_client_runtime.resolve_feishu_auth_settings",
        lambda settings: settings,
    )
    client = FeishuBitableClient(
        "test-id", "test-secret", "app", "calc", "attachment",
        request_retry_count=2, request_retry_interval_sec=0,
        date_text_to_timestamp_ms_fn=lambda **kw: 0,
        canonical_metric_name_fn=str, dimension_mapping={},
    )
    client.request_retry_interval_sec = 0
    client._tenant_access_token = "test"
    monkeypatch.setattr(client, "refresh_token", lambda **kw: "test")
    seen, server_records = [], {}
    def request(**kwargs):
        token = kwargs["params"]["client_token"]
        seen.append(token)
        server_records.setdefault(token, kwargs["json"]["records"])
        if len(seen) == 1:
            raise requests.ReadTimeout("response lost after commit")
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"code":0,"data":{}}'
        return response
    monkeypatch.setattr(requests, "request", request)
    client.batch_create_records("table", [{"value": 1}, {"value": 2}], batch_size=1)
    assert seen[0] == seen[1] and seen[2] != seen[1]
    assert len(server_records) == 2

def test_configure_after_shutdown_reopens_engine_and_restores_jobs(tmp_path, monkeypatch):
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    service = JobService()
    service.configure_task_engine(runtime_config=config, app_dir=tmp_path)
    job = service.create_waiting_worker_job("resume", worker_handler="test", resume_policy="requeue")
    stage = service._get_primary_stage(job)
    job.status = stage.status = "running"
    job.wait_reason = ""
    service.shutdown_task_engine()
    launches = []
    monkeypatch.setattr(service, "_launch_existing_worker_job", lambda j, s, **kw: launches.append(j.job_id))
    try:
        service.configure_task_engine(runtime_config=config, app_dir=tmp_path)
        assert launches == [job.job_id]
        assert not service._shutting_down
        assert service._task_engine_db is not None
    finally:
        service.shutdown_task_engine()


def test_resume_after_shutdown_does_not_change_saved_payload(tmp_path):
    service = JobService()
    service.configure_task_engine(runtime_config={"paths": {"runtime_state_root": str(tmp_path)}}, app_dir=tmp_path)
    job = service.create_waiting_worker_job("wait", worker_handler="test", worker_payload={"original": True})
    payload_path = service._task_engine_store.resolve_stage_payload_path(job.job_id, "main")
    original = payload_path.read_bytes()
    service.shutdown_task_engine()
    with pytest.raises(RuntimeError, match="关闭"):
        service.resume_waiting_worker_job(job.job_id, worker_payload={"wrong": True})
    assert payload_path.read_bytes() == original


def test_restart_does_not_launch_duplicate_when_old_worker_is_unconfirmed(tmp_path, monkeypatch):
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    service = JobService()
    service.configure_task_engine(runtime_config=config, app_dir=tmp_path)
    job = service.create_waiting_worker_job("test", worker_handler="test", resume_policy="requeue")
    stage = service._get_primary_stage(job)
    job.status = stage.status = "running"
    job.wait_reason = ""
    stage.worker_pid = 123
    service._persist_job_snapshot(job)
    service._task_engine_db.close()
    service._task_engine_db = None
    restored = JobService()
    launched = []
    monkeypatch.setattr(restored, "_pid_exists", lambda pid: True)
    monkeypatch.setattr(restored, "_terminate_orphan_worker", lambda pid, **kw: False)
    monkeypatch.setattr(restored, "_launch_existing_worker_job", lambda *a, **kw: launched.append(True))
    try:
        restored.configure_task_engine(runtime_config=config, app_dir=tmp_path)
        assert launched == []
        assert restored._jobs[job.job_id].status == "interrupted"
        assert "无法确认" in restored._jobs[job.job_id].error
    finally:
        restored.shutdown_task_engine()
