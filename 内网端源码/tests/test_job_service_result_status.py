import threading

from app.modules.report_pipeline.service.job_service import JobService, JobState


def test_thread_job_preserves_failed_result_status():
    service = JobService()
    job = service.start_job(
        name="failed-result",
        feature="test",
        run_func=lambda _emit: {"ok": False, "status": "failed", "error": "expected failure"},
    )

    assert job.done_event.wait(5)
    assert job.status == "failed"
    assert job.error == "expected failure"


def test_completed_jobs_are_bounded_in_memory():
    service = JobService()
    service._task_engine_db = object()  # type: ignore[assignment]
    for index in range(40):
        job = JobState(job_id=f"job-{index}", name=f"job {index}", status="success", sequence=index)
        job.done_event.set()
        job.thread = threading.current_thread()
        service._jobs[job.job_id] = job

    assert service._prune_terminal_jobs_in_memory() == 8
    assert "job-0" not in service._jobs
    assert "job-39" in service._jobs


def test_job_service_stops_workers_before_closing_task_database():
    events = []

    class _Process:
        pid = 123
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            events.append("worker_waited")
            return int(self.returncode or 0)

    class _Database:
        def close(self):
            events.append("database_closed")

    process = _Process()
    service = JobService()
    service._worker_processes[("job-1", "main")] = process  # type: ignore[assignment]
    service._task_engine_db = _Database()  # type: ignore[assignment]

    def _cancel(**_kwargs):
        events.append("cancel_sent")
        process.returncode = 0
        return True

    service._send_worker_command = _cancel  # type: ignore[method-assign]
    service.shutdown_task_engine()

    assert events == ["cancel_sent", "worker_waited", "database_closed"]
