from app.modules.report_pipeline.service.job_service import JobService


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
