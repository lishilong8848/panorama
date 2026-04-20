from __future__ import annotations

import threading
import time
from pathlib import Path

from app.modules.report_pipeline.service.job_service import JobService


def _wait_until(predicate, timeout_sec: float = 3.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition not met before timeout")


def test_job_service_queues_jobs_when_resource_is_occupied() -> None:
    service = JobService()
    started = threading.Event()
    release = threading.Event()

    def first_job(emit_log):  # noqa: ANN001
        emit_log("first running")
        started.set()
        release.wait(timeout=2)
        return {"status": "ok-first"}

    def second_job(_emit_log):  # noqa: ANN001
        return {"status": "ok-second"}

    job1 = service.start_job("first", first_job, resource_keys=["browser:controlled"])
    started.wait(timeout=1)
    job2 = service.start_job("second", second_job, resource_keys=["browser:controlled"])

    _wait_until(lambda: service.get_job_state(job2.job_id).status == "waiting_resource")

    snapshot = service.get_resource_snapshot()
    assert snapshot["controlled_browser"]["holder_job_id"] == job1.job_id
    assert snapshot["controlled_browser"]["queue_length"] >= 1

    release.set()
    service.wait_job(job1.job_id, timeout_sec=2)
    service.wait_job(job2.job_id, timeout_sec=2)

    assert service.get_job(job1.job_id)["status"] == "success"
    assert service.get_job(job2.job_id)["status"] == "success"


def test_job_service_allows_parallel_jobs_on_distinct_resources() -> None:
    service = JobService()
    job1_running = threading.Event()
    job2_running = threading.Event()
    release = threading.Event()

    def internal_job(_emit_log):  # noqa: ANN001
        job1_running.set()
        release.wait(timeout=2)
        return {"status": "ok-internal"}

    def external_job(_emit_log):  # noqa: ANN001
        job2_running.set()
        release.wait(timeout=2)
        return {"status": "ok-external"}

    job1 = service.start_job("browser-job", internal_job, resource_keys=["browser:controlled"])
    job2 = service.start_job("output-job", external_job, resource_keys=["output_path:test.xlsx"])

    job1_running.wait(timeout=1)
    job2_running.wait(timeout=1)

    _wait_until(
        lambda: service.get_job_state(job1.job_id).status == "running"
        and service.get_job_state(job2.job_id).status == "running"
    )

    release.set()
    service.wait_job(job1.job_id, timeout_sec=2)
    service.wait_job(job2.job_id, timeout_sec=2)

    assert service.get_job(job1.job_id)["status"] == "success"
    assert service.get_job(job2.job_id)["status"] == "success"


def test_updater_global_blocks_new_business_jobs() -> None:
    service = JobService()
    release = threading.Event()
    updater_entered = threading.Event()

    def hold_updater() -> None:
        with service.resource_guard(name="updater", resource_keys=["updater:global"]):
            updater_entered.set()
            release.wait(timeout=2)

    threading.Thread(target=hold_updater, daemon=True).start()
    updater_entered.wait(timeout=1)

    def business_job(_emit_log):  # noqa: ANN001
        return {"status": "ok"}

    job = service.start_job("business", business_job, resource_keys=["network:external"])
    _wait_until(lambda: service.get_job_state(job.job_id).status == "waiting_resource")
    assert service.get_job_state(job.job_id).wait_reason == "waiting:app_update"

    release.set()
    service.wait_job(job.job_id, timeout_sec=2)
    assert service.get_job(job.job_id)["status"] == "success"


def test_waiting_shared_bridge_job_does_not_block_new_foreground_job(tmp_path: Path) -> None:
    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=tmp_path,
        config_snapshot_getter=lambda: {"paths": {}},
    )

    waiting_job = service.create_waiting_worker_job(
        "bridge-waiting",
        worker_handler="day_metric_from_download",
        worker_payload={"selected_dates": ["2026-04-20"]},
        resource_keys=["shared_bridge:day_metric"],
        feature="day_metric_from_download",
        wait_reason="waiting:shared_bridge",
        summary="等待内网补采同步",
    )
    assert service.get_job_state(waiting_job.job_id).status == "waiting_resource"

    started = threading.Event()

    def foreground_job(_emit_log):  # noqa: ANN001
        started.set()
        return {"status": "ok"}

    job = service.start_job(
        "foreground",
        foreground_job,
        resource_keys=["shared_bridge:day_metric"],
        feature="day_metric_external_dispatch",
    )

    _wait_until(lambda: started.is_set() or service.get_job_state(job.job_id).status == "success")
    service.wait_job(job.job_id, timeout_sec=2)

    assert service.get_job(job.job_id)["status"] == "success"
    assert service.get_job_state(waiting_job.job_id).status == "waiting_resource"
