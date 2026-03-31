from __future__ import annotations

import time
from pathlib import Path

from app.modules.report_pipeline.service.task_engine_database import TaskEngineDatabase
from app.modules.report_pipeline.service.job_service import JobService
from app.modules.report_pipeline.service.task_engine_store import TaskEngineStore


def _seed_worker_job_snapshot(
    runtime_app_dir: Path,
    *,
    job_id: str,
    job_status: str,
    stage_status: str,
    worker_handler: str,
    payload: dict,
    resume_policy: str = "manual_resume",
    worker_pid: int = 0,
) -> None:
    db = TaskEngineDatabase(runtime_config={"paths": {}}, app_dir=runtime_app_dir)
    store = TaskEngineStore(runtime_config={"paths": {}}, app_dir=runtime_app_dir)
    store.persist_stage_payload(job_id, "main", payload)
    store.persist_config_snapshot(job_id, {"paths": {"business_root_dir": "D:/QLDownload"}})
    db.upsert_job(
        {
            "job_id": job_id,
            "name": f"seed-{job_id}",
            "feature": "seed_worker",
            "submitted_by": "manual",
            "priority": "manual",
            "resource_keys": ["network:external"],
            "wait_reason": "",
            "status": job_status,
            "created_at": "2026-03-27 10:00:00",
            "started_at": "2026-03-27 10:00:01" if job_status == "running" else "",
            "finished_at": "",
            "summary": "",
            "error": "",
            "result": None,
            "cancel_requested": False,
        },
        config_snapshot={"paths": {"business_root_dir": "D:/QLDownload"}},
    )
    db.upsert_stage(
        job_id,
        {
            "stage_id": "main",
            "name": "seed_stage",
            "status": stage_status,
            "resource_keys": ["network:external"],
            "resume_policy": resume_policy,
            "worker_handler": worker_handler,
            "worker_pid": worker_pid,
            "started_at": "2026-03-27 10:00:01" if stage_status == "running" else "",
            "finished_at": "",
            "summary": "",
            "error": "",
            "result": None,
            "cancel_requested": False,
        },
    )


def test_job_service_runs_worker_stage_in_subprocess(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    runtime_app_dir = tmp_path / "sandbox_app"
    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=runtime_app_dir,
        worker_app_dir=project_root,
        config_snapshot_getter=lambda: {"paths": {"business_root_dir": "D:/QLDownload"}},
    )

    job = service.start_worker_job(
        name="worker-echo",
        worker_handler="test_echo_payload",
        worker_payload={"hello": "world"},
        resource_keys=["network:external"],
        priority="manual",
        feature="worker_echo",
        submitted_by="manual",
    )
    service.wait_job(job.job_id, timeout_sec=10)

    job_payload = service.get_job(job.job_id)
    assert job_payload["status"] == "success"
    assert job_payload["result"]["echo"] == {"hello": "world"}
    assert job_payload["stages"][0]["status"] == "success"
    assert job_payload["stages"][0]["worker_handler"] == "test_echo_payload"
    assert int(job_payload["stages"][0]["worker_pid"] or 0) == 0
    assert int(job_payload["last_event_id"] or 0) > 0

    task_engine_root = runtime_app_dir / ".runtime" / "task_engine"
    db_path = task_engine_root / "task_engine.db"
    assert db_path.exists()
    assert not (task_engine_root / "jobs" / job.job_id / "stages" / "main.result.json").exists()

    logs_payload = service.get_logs(job.job_id, after_event_id=0)
    events = list(logs_payload.get("events") or [])
    assert any(event.get("event_type") == "log" for event in events)
    assert any(event.get("event_type") == "result" for event in events)
    assert any("[worker-test]" in str((event.get("payload") or {}).get("message", "")) for event in events)


def test_job_service_worker_command_bootstraps_app_root(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    runtime_app_dir = tmp_path / "sandbox_app"
    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=runtime_app_dir,
        worker_app_dir=project_root,
        config_snapshot_getter=lambda: {"paths": {"business_root_dir": "D:/QLDownload"}},
    )

    job = service.start_worker_job(
        name="worker-command",
        worker_handler="test_echo_payload",
        worker_payload={"hello": "command"},
        resource_keys=["network:external"],
        priority="manual",
        feature="worker_echo",
        submitted_by="manual",
    )
    stage = service._get_primary_stage(job)
    payload_path = service._task_engine_store.resolve_stage_payload_path(job.job_id, stage.stage_id)
    command = service._build_worker_command(
        job_dir=service._task_engine_store.resolve_job_dir(job.job_id),
        stage=stage,
        worker_handler="test_echo_payload",
        payload_path=payload_path,
        control_port=29333,
    )

    assert "-m" not in command
    assert "-c" not in command
    assert command[1] == str(project_root / "worker_bootstrap.py")


def test_job_service_can_cancel_worker_stage(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    runtime_app_dir = tmp_path / "sandbox_app"
    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=runtime_app_dir,
        worker_app_dir=project_root,
        config_snapshot_getter=lambda: {"paths": {"business_root_dir": "D:/QLDownload"}},
    )

    job = service.start_worker_job(
        name="worker-sleep",
        worker_handler="test_sleep",
        worker_payload={"seconds": 5},
        resource_keys=["network:external"],
        priority="manual",
        feature="worker_sleep",
        submitted_by="manual",
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        payload = service.get_job(job.job_id)
        if payload["stages"][0]["worker_pid"]:
            break
        time.sleep(0.05)

    service.cancel_job(job.job_id)
    service.wait_job(job.job_id, timeout_sec=10)

    job_payload = service.get_job(job.job_id)
    assert job_payload["cancel_requested"] is True
    assert job_payload["status"] in {"cancelled", "success"}
    assert job_payload["stages"][0]["status"] in {"cancelled", "success"}
    assert job_payload["stages"][0]["summary"] in {"cancelled", "interrupted_force_killed", "success", "ok"}


def test_job_service_can_retry_worker_job(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    runtime_app_dir = tmp_path / "sandbox_app"
    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=runtime_app_dir,
        worker_app_dir=project_root,
        config_snapshot_getter=lambda: {"paths": {"business_root_dir": "D:/QLDownload"}},
    )

    job = service.start_worker_job(
        name="worker-echo",
        worker_handler="test_echo_payload",
        worker_payload={"hello": "retry"},
        resource_keys=["network:external"],
        priority="manual",
        feature="worker_echo",
        submitted_by="manual",
    )
    service.wait_job(job.job_id, timeout_sec=10)
    retried = service.retry_job(job.job_id)
    service.wait_job(retried["job_id"], timeout_sec=10)

    retried_payload = service.get_job(retried["job_id"])
    assert retried_payload["status"] == "success"
    assert retried_payload["result"]["echo"] == {"hello": "retry"}


def test_job_service_restores_queued_worker_job_on_startup(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    runtime_app_dir = tmp_path / "sandbox_app"
    _seed_worker_job_snapshot(
        runtime_app_dir,
        job_id="restore-queued",
        job_status="queued",
        stage_status="pending",
        worker_handler="test_echo_payload",
        payload={"hello": "restore-queued"},
        resume_policy="requeue",
    )

    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=runtime_app_dir,
        worker_app_dir=project_root,
        config_snapshot_getter=lambda: {"paths": {"business_root_dir": "D:/QLDownload"}},
    )

    service.wait_job("restore-queued", timeout_sec=10)
    payload = service.get_job("restore-queued")
    assert payload["status"] == "success"
    assert payload["result"]["echo"] == {"hello": "restore-queued"}


def test_job_service_requeues_running_worker_job_on_restart_when_resume_policy_is_requeue(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    runtime_app_dir = tmp_path / "sandbox_app"
    _seed_worker_job_snapshot(
        runtime_app_dir,
        job_id="restore-running-requeue",
        job_status="running",
        stage_status="running",
        worker_handler="test_echo_payload",
        payload={"hello": "restore-running"},
        resume_policy="requeue",
        worker_pid=999999,
    )

    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=runtime_app_dir,
        worker_app_dir=project_root,
        config_snapshot_getter=lambda: {"paths": {"business_root_dir": "D:/QLDownload"}},
    )

    service.wait_job("restore-running-requeue", timeout_sec=10)
    payload = service.get_job("restore-running-requeue")
    assert payload["status"] == "success"
    assert payload["result"]["echo"] == {"hello": "restore-running"}


def test_job_service_marks_running_worker_job_interrupted_on_restart_when_manual_resume(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    runtime_app_dir = tmp_path / "sandbox_app"
    _seed_worker_job_snapshot(
        runtime_app_dir,
        job_id="restore-running-manual",
        job_status="running",
        stage_status="running",
        worker_handler="test_echo_payload",
        payload={"hello": "restore-manual"},
        resume_policy="manual_resume",
        worker_pid=999999,
    )

    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=runtime_app_dir,
        worker_app_dir=project_root,
        config_snapshot_getter=lambda: {"paths": {"business_root_dir": "D:/QLDownload"}},
    )

    payload = service.get_job("restore-running-manual")
    assert payload["status"] == "interrupted"
    assert payload["summary"] == "worker_interrupted_by_main_restart"
    assert payload["stages"][0]["status"] == "interrupted"
