from __future__ import annotations

import threading

from app.modules.report_pipeline.service.job_service import JobService


def test_start_job_reuses_incomplete_job_with_same_dedupe_key() -> None:
    service = JobService()
    release = threading.Event()
    executed: list[str] = []

    def _first(_emit_log):  # noqa: ANN001
        executed.append("first")
        release.wait(timeout=2)
        return {"ok": True}

    def _second(_emit_log):  # noqa: ANN001
        executed.append("second")
        return {"ok": True}

    first = service.start_job(
        "first",
        _first,
        feature="handover_cache_continue",
        dedupe_key="handover:latest:a,b",
    )
    second = service.start_job(
        "second",
        _second,
        feature="handover_cache_continue",
        dedupe_key="handover:latest:a,b",
    )

    assert second.job_id == first.job_id
    release.set()
    finished = service.wait_job(first.job_id, timeout_sec=2)
    assert finished.status == "success"
    assert executed == ["first"]


def test_start_job_allows_new_job_after_terminal_status() -> None:
    service = JobService()

    first = service.start_job(
        "first",
        lambda _emit_log: {"ok": True},
        feature="monthly_cache_latest",
        dedupe_key="monthly:latest:2026-04-01 10",
    )
    service.wait_job(first.job_id, timeout_sec=2)

    second = service.start_job(
        "second",
        lambda _emit_log: {"ok": True},
        feature="monthly_cache_latest",
        dedupe_key="monthly:latest:2026-04-01 10",
    )
    service.wait_job(second.job_id, timeout_sec=2)

    assert second.job_id != first.job_id
