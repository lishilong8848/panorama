from __future__ import annotations

from pathlib import Path

import pytest

from app.core.app_state import AppStateRepository
from app.modules.report_pipeline.service.job_service import JobService, JobState, StageState
from app.worker.entry import _exception_detail, _invoke_handler
from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService
from handover_log_module.service.review_session_service import ReviewSessionStoreUnavailableError


def test_worker_does_not_retry_handler_after_internal_type_error() -> None:
    calls = 0

    def handler(config, payload, emit_log, runtime):  # noqa: ANN001, ARG001
        nonlocal calls
        calls += 1
        raise TypeError("internal failure")

    with pytest.raises(TypeError, match="internal failure"):
        _invoke_handler(handler, {}, {}, lambda _line: None, object())

    assert calls == 1


def test_empty_exception_message_keeps_exception_type() -> None:
    assert _exception_detail(TimeoutError()) == "TimeoutError"


def test_app_state_health_snapshot_is_cached_and_marks_large_counts_approximate(tmp_path: Path) -> None:
    repository = AppStateRepository(
        runtime_config={"paths": {"runtime_state_root": str(tmp_path / "runtime")}},
        app_dir=tmp_path,
    )

    first = repository.snapshot()
    second = repository.snapshot()

    assert first["ready"] is True
    assert first["cached"] is False
    assert second["cached"] is True
    assert set(second["table_counts_approximate"]) == {"task_events", "power_alert_daily_stats"}


def test_job_snapshot_does_not_restore_stale_worker_status() -> None:
    class FakeTaskEngineDatabase:
        def upsert_job(self, payload, config_snapshot=None):  # noqa: ANN001, ARG002
            return None

        def get_job(self, job_id):  # noqa: ANN001, ARG002
            return {
                "revision": 3,
                "last_event_id": 9,
                "stages": [
                    {
                        "stage_id": "main",
                        "revision": 2,
                        "worker_status": "running",
                        "last_heartbeat_at": "2026-08-04 17:00:00",
                    }
                ],
            }

        def upsert_stage(self, job_id, payload):  # noqa: ANN001, ARG002
            return None

    service = JobService()
    service._task_engine_db = FakeTaskEngineDatabase()  # type: ignore[assignment]
    stage = StageState(stage_id="main", name="main", status="failed", worker_status="failed")
    job = JobState(job_id="job-1", name="test", feature="test", status="failed", stages=[stage])

    service._persist_job_snapshot(job)

    assert stage.worker_status == "failed"


def test_followup_state_read_retries_transient_store_error(monkeypatch) -> None:  # noqa: ANN001
    class FakeReviewService:
        def __init__(self) -> None:
            self.calls = 0

        def list_batch_sessions(self, batch_key):  # noqa: ANN001, ARG002
            self.calls += 1
            if self.calls == 1:
                raise ReviewSessionStoreUnavailableError("temporarily unavailable")
            return [{"building": "C楼"}]

    service = object.__new__(ReviewFollowupTriggerService)
    service._review_service = FakeReviewService()
    logs: list[str] = []
    monkeypatch.setattr(
        "handover_log_module.service.review_followup_trigger_service.time.sleep",
        lambda _seconds: None,
    )

    sessions = service._list_batch_sessions_resilient("2026-08-04|day", emit_log=logs.append)

    assert sessions == [{"building": "C楼"}]
    assert service._review_service.calls == 2
    assert any("正在重试" in line for line in logs)
