"""Production queue + subprocess + handler; only source/Feishu I/O is isolated."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.modules.inspection_data.service.live_requests import LiveRequestStore, dispatch_live_requests
from app.modules.inspection_data.service.inspection_data_upload_service import validate_inspection_batch
from app.modules.report_pipeline.service.job_service import JobService
from app.modules.scheduler.api.inspection_live_routes import router

ROOT = Path(__file__).resolve().parents[1]
BASE = "/api/scheduler/inspection-data-upload/live"
AUTH = {"Authorization": "Bearer isolated-queue-token"}


def until(check, message):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.03)
    pytest.fail(message)


@pytest.fixture
def queue(tmp_path, monkeypatch):
    output = tmp_path / "transport"
    output.mkdir()
    config = {"paths": {"runtime_state_root": str(tmp_path / "state")},
              "notify": {"task_failure_webhook_enabled": False, "task_success_webhook_enabled": False},
              "inspection_data_upload": {"wait_source_timeout_sec": 0, "lighthouse_bridge": {
                  "enabled": True, "token": "isolated-queue-token"}},
              "_isolated_inspection_output": str(output)}
    service = JobService()
    service.configure_task_engine(runtime_config=config, app_dir=ROOT, config_snapshot_getter=lambda: config)
    monkeypatch.setattr(service, "_ensure_worker_runtime_ready", lambda *args: sys.executable)
    production_command = service._build_worker_command
    def isolated_command(**kwargs):
        command = production_command(**kwargs)
        command[1] = str(ROOT / "tests" / "inspection_live_worker_fixture.py")
        return command
    monkeypatch.setattr(service, "_build_worker_command", isolated_command)
    container = SimpleNamespace(runtime_config=config, job_service=service,
                                _configured_deployment_snapshot=lambda: {"role_mode": "external"})
    container.dispatch_inspection_live_requests = lambda: dispatch_live_requests(container)
    app = FastAPI()
    app.state.container = container
    app.include_router(router)
    with TestClient(app, base_url="http://192.168.1.5", client=("192.168.1.6", 5000)) as client:
        try:
            yield client, container, LiveRequestStore(config), output
        finally:
            for name in "DE": (output / ("release-" + name)).write_text("release", encoding="utf8")
            for job in list(service._jobs.values()):
                if not job.done_event.wait(20): service.cancel_job(job.job_id)
                if job.thread: job.thread.join(timeout=10)
            service.shutdown_task_engine()


def request(building="D楼"):
    return {"request_id": uuid.uuid4().hex, "building": building, "not_before": int(time.time()*1000)-5000}


def terminal(service, job_id):
    job = service.get_job_state(job_id)
    return job if job and job.done_event.is_set() else None


def artifact(output, job_id):
    return json.loads((output / ("job-" + job_id + ".json")).read_text(encoding="utf8"))


def test_real_queue_new_same_building_request_waits_other_building_completes_and_replay_does_not_upload(queue):
    client, container, store, output = queue
    service = container.job_service
    container.runtime_config["_isolated_gate_building"] = "D楼"
    first = request()
    accepted = client.post(BASE, json=first, headers=AUTH)
    assert accepted.status_code == 202
    first_job = accepted.json()["job_id"]
    until(lambda: (output / "started-D").exists(), str(service.get_job(first_job)))
    assert store.get(first["request_id"])["state"] == "running"
    followup = request()
    assert client.post(BASE, json=followup, headers=AUTH).json()["state"] == "pending"
    other = request("E楼")
    other_job = client.post(BASE, json=other, headers=AUTH).json()["job_id"]
    e = until(lambda: terminal(service, other_job), "E worker failed to finish")
    assert e.status == "success", e.error + str(e.logs[-5:])
    assert store.get(other["request_id"])["state"] == "uploaded"
    assert not terminal(service, first_job)
    (output / "release-D").write_text("release", encoding="utf8")
    d = until(lambda: terminal(service, first_job), "D worker failed to finish")
    assert d.status == "success", d.error + str(d.logs[-5:])
    assert store.get(followup["request_id"])["state"] == "pending"
    container.dispatch_inspection_live_requests()
    followup_job = store.get(followup["request_id"])["job_id"]
    assert followup_job != first_job
    f = until(lambda: terminal(service, followup_job), "D followup worker failed to finish")
    assert f.status == "success", f.error + str(f.logs[-5:])
    for body, job_id in ((first, first_job), (other, other_job), (followup, followup_job)):
        receipt = client.get(BASE + "/" + body["request_id"], headers=AUTH).json()
        assert receipt["ok"] is True and receipt["state"] == "uploaded"
        assert body["not_before"] <= receipt["source_time"] <= receipt["uploaded_at"]
        saved = artifact(output, job_id)
        assert saved["pid"] != __import__("os").getpid() and saved["exit_code"] == 0
        assert len(saved["refreshes"]) == 2 and all(call["force_refresh"] for call in saved["refreshes"])
        rows = saved["records"]
        upload_time = rows[0]["fields"]["上传日期"]
        assert validate_inspection_batch(rows, building=body["building"], upload_time=upload_time)
        assert min(row["fields"]["源文件时间"] for row in rows) == receipt["source_time"]
        count = len(service.list_jobs(limit=100))
        assert client.post(BASE, json=body, headers=AUTH).json() == receipt
        assert len(service.list_jobs(limit=100)) == count
    # A newly opened ledger instance reads the same completed requests after jobs exit.
    reopened = LiveRequestStore(container.runtime_config)
    assert reopened.get(first["request_id"])["state"] == "uploaded"


def test_real_queue_creation_failure_recovers_same_batch_and_keeps_initial_floor(queue):
    client, container, store, output = queue
    service = container.job_service
    container.runtime_config["_isolated_fail_once"] = "D楼"
    body = request()
    first_job = client.post(BASE, json=body, headers=AUTH).json()["job_id"]
    job = until(lambda: terminal(service, first_job), "failed worker did not exit")
    assert job.status == "failed"
    assert store.get(body["request_id"])["state"] == "pending"
    assert store.get(body["request_id"])["source_time"] is None
    batch = store.next_batch("D楼")
    assert batch["attempts"] == 1 and batch["not_before"] == body["not_before"]
    with store.connect() as db: db.execute("UPDATE batches SET retry_at=0")
    replay = client.post(BASE, json=body, headers=AUTH)
    second_job = replay.json()["job_id"]
    assert second_job != first_job
    success = until(lambda: terminal(service, second_job), "recovery worker did not exit")
    assert success.status == "success", success.error + str(success.logs[-5:])
    assert store.get(body["request_id"])["state"] == "uploaded"
    with store.connect() as db:
        saved = db.execute("SELECT * FROM batches WHERE batch_id=?", (batch["batch_id"],)).fetchone()
        assert saved["state"] == "uploaded" and saved["attempts"] == 2
        assert saved["not_before"] == body["not_before"]
        assert db.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 1
    assert artifact(output, first_job)["exit_code"] == 1
    assert artifact(output, second_job)["exit_code"] == 0
