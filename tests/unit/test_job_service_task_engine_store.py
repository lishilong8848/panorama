from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

from app.modules.report_pipeline.service.job_service import JobService
from app.modules.report_pipeline.service.task_engine_database import TaskEngineDatabase


def _wait_until(predicate, timeout_sec: float = 3.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition not met before timeout")


def test_job_service_persists_job_stage_config_and_logs_to_sqlite(tmp_path: Path) -> None:
    service = JobService()
    config_snapshot = {"paths": {"business_root_dir": "D:/QLDownload"}, "execution": {"engine_mode": "thread"}}
    service.configure_task_engine(
        runtime_config=config_snapshot,
        app_dir=tmp_path,
        config_snapshot_getter=lambda: config_snapshot,
    )

    def _run(emit_log):  # noqa: ANN001
        emit_log("job started")
        return {"status": "ok"}

    job = service.start_job("demo", _run, feature="demo_feature", resource_keys=["network:external"])
    service.wait_job(job.job_id, timeout_sec=3)

    db_path = tmp_path / ".runtime" / "task_engine" / "task_engine.db"
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT feature, status, config_snapshot_json, revision FROM jobs WHERE job_id = ?", (job.job_id,)).fetchone()
        stage_row = conn.execute("SELECT status, resource_keys_json, revision FROM stages WHERE job_id = ? AND stage_id = 'main'", (job.job_id,)).fetchone()
        log_rows = conn.execute("SELECT event_type, payload_json FROM job_events WHERE job_id = ? ORDER BY event_id ASC", (job.job_id,)).fetchall()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "demo_feature"
    assert row[1] == "success"
    assert json.loads(row[2]) == config_snapshot
    assert int(row[3] or 0) >= 1
    assert stage_row is not None
    assert stage_row[0] == "success"
    assert json.loads(stage_row[1]) == ["network:external"]
    assert int(stage_row[2] or 0) >= 1
    assert any(event_type == "log" and "job started" in str(json.loads(payload_json).get("message", "")) for event_type, payload_json in log_rows)


def test_job_service_persists_resource_snapshot_to_sqlite(tmp_path: Path) -> None:
    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=tmp_path,
        config_snapshot_getter=lambda: {"paths": {}},
    )
    release = threading.Event()
    started = threading.Event()

    def _first(_emit_log):  # noqa: ANN001
        started.set()
        release.wait(timeout=3)
        return {"status": "ok-first"}

    def _second(_emit_log):  # noqa: ANN001
        return {"status": "ok-second"}

    first = service.start_job("first", _first, resource_keys=["browser:controlled"])
    started.wait(timeout=1)
    second = service.start_job("second", _second, resource_keys=["browser:controlled"])

    _wait_until(lambda: service.get_job_state(second.job_id).status == "waiting_resource")
    waiting_snapshot = service.get_resource_snapshot()
    assert waiting_snapshot["controlled_browser"]["holder_job_id"] == first.job_id
    assert waiting_snapshot["controlled_browser"]["queue_length"] >= 1

    release.set()
    service.wait_job(first.job_id, timeout_sec=3)
    service.wait_job(second.job_id, timeout_sec=3)

    final_snapshot = service.get_resource_snapshot()
    assert final_snapshot["controlled_browser"]["holder_job_id"] == ""
    assert final_snapshot["controlled_browser"]["queue_length"] == 0


def test_job_service_reuses_active_job_by_dedupe_key_from_sqlite(tmp_path: Path) -> None:
    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=tmp_path,
        config_snapshot_getter=lambda: {"paths": {}},
    )
    release = threading.Event()
    started = threading.Event()

    def _first(_emit_log):  # noqa: ANN001
        started.set()
        release.wait(timeout=3)
        return {"status": "ok-first"}

    first = service.start_job(
        "first",
        _first,
        feature="handover_cache_continue",
        dedupe_key="handover_cache_continue:{\"mode\":\"latest\"}",
    )
    started.wait(timeout=1)

    second = service.start_job(
        "second",
        lambda _emit_log: {"status": "ok-second"},
        feature="handover_cache_continue",
        dedupe_key="handover_cache_continue:{\"mode\":\"latest\"}",
    )

    assert second.job_id == first.job_id
    release.set()
    service.wait_job(first.job_id, timeout_sec=3)


def test_job_service_can_cancel_waiting_resource_job_restored_from_sqlite(tmp_path: Path) -> None:
    restored_service = JobService()
    restored_service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=tmp_path,
        config_snapshot_getter=lambda: {"paths": {}},
    )
    db = restored_service._task_engine_db  # noqa: SLF001
    assert db is not None
    db.upsert_job(
        {
            "job_id": "job-waiting-restored",
            "name": "second",
            "feature": "demo_feature",
            "status": "waiting_resource",
            "created_at": "2026-04-19 22:00:00",
            "started_at": "",
            "finished_at": "",
            "summary": "waiting",
            "error": "",
            "result": None,
            "priority": "manual",
            "resource_keys": ["browser:controlled"],
            "wait_reason": "waiting:browser_controlled",
            "bridge_task_id": "",
            "cancel_requested": False,
            "last_event_id": 0,
        },
        config_snapshot={"paths": {}},
    )
    db.upsert_stage(
        "job-waiting-restored",
        {
            "stage_id": "main",
            "name": "demo_feature",
            "status": "waiting_resource",
            "resource_keys": ["browser:controlled"],
            "resume_policy": "manual_resume",
            "worker_handler": "",
            "worker_pid": 0,
            "started_at": "",
            "finished_at": "",
            "summary": "waiting",
            "error": "",
            "result": None,
            "cancel_requested": False,
        },
    )

    payload = restored_service.cancel_job("job-waiting-restored")

    assert payload["job_id"] == "job-waiting-restored"
    assert payload["status"] == "cancelled"
    assert restored_service.get_job("job-waiting-restored")["status"] == "cancelled"


def test_job_service_preserves_shared_bridge_waiting_job_on_restore(tmp_path: Path) -> None:
    first_service = JobService()
    first_service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=tmp_path,
        config_snapshot_getter=lambda: {"paths": {}},
    )
    waiting_job = first_service.create_waiting_worker_job(
        "bridge-waiting",
        worker_handler="multi_date",
        worker_payload={"selected_dates": ["2026-04-21"]},
        resource_keys=["shared_bridge:monthly_report"],
        feature="multi_date",
        wait_reason="waiting:shared_bridge",
        summary="等待内网补采同步",
        bridge_task_id="bridge-task-1",
    )
    assert first_service.get_job_state(waiting_job.job_id).status == "waiting_resource"

    launch_calls: list[str] = []
    original_launch = JobService._launch_existing_worker_job

    def _fake_launch(self, job, stage, *, payload_path, worker_handler):  # noqa: ANN001
        launch_calls.append(str(job.job_id))

    JobService._launch_existing_worker_job = _fake_launch
    try:
        restored_service = JobService()
        restored_service.configure_task_engine(
            runtime_config={"paths": {}},
            app_dir=tmp_path,
            config_snapshot_getter=lambda: {"paths": {}},
        )
    finally:
        JobService._launch_existing_worker_job = original_launch

    restored = restored_service.get_job_state(waiting_job.job_id)
    assert restored is not None
    assert restored.status == "waiting_resource"
    assert restored.wait_reason == "waiting:shared_bridge"
    assert restored.bridge_task_id == "bridge-task-1"
    assert launch_calls == []


def test_job_service_task_engine_runtime_snapshot_and_shutdown(tmp_path: Path) -> None:
    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=tmp_path,
        config_snapshot_getter=lambda: {"paths": {}},
    )

    snapshot = service.task_engine_runtime_snapshot()

    assert snapshot["write_queue_length"] >= 0
    assert snapshot["closed"] is False

    service.shutdown_task_engine()

    closed_snapshot = service.task_engine_runtime_snapshot()
    assert closed_snapshot["closed"] is True


def test_task_engine_database_restarts_dead_writer(tmp_path: Path) -> None:
    db = TaskEngineDatabase(runtime_config={"paths": {}}, app_dir=tmp_path)
    writer = db._writer
    assert writer is not None

    db._writes.put(db._close_sentinel, timeout=1.0)
    writer.join(timeout=3.0)
    assert not writer.is_alive()

    db.upsert_job(
        {
            "job_id": "job-restarted",
            "name": "demo",
            "feature": "demo",
            "status": "queued",
            "created_at": "2026-04-15 10:00:00",
        }
    )

    row = db.get_job("job-restarted")
    assert row is not None
    assert row["job_id"] == "job-restarted"
    assert db.runtime_snapshot()["writer_alive"] is True

    db.close()
