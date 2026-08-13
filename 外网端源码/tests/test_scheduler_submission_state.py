from __future__ import annotations

from datetime import datetime

from app.modules.scheduler.service.apscheduler_orchestrator import ApschedulerSchedulerFacade


def test_submitted_job_is_not_recorded_as_period_success() -> None:
    facade = object.__new__(ApschedulerSchedulerFacade)
    facade.schedule_kind = "daily"
    facade.source_name = "测试任务"
    facade.state = {
        "last_success_period": "",
        "last_attempt_period": "",
        "last_run_at": "",
        "last_status": "",
        "last_error": "",
        "retry_done_period": "",
    }
    facade._save_state = lambda: None

    facade._record_fire_state(
        now=datetime(2026, 8, 13, 9, 30),
        status="submitted",
        detail="job accepted",
        duration_ms=10,
    )

    assert facade.state["last_status"] == "submitted"
    assert facade.state["last_attempt_period"] == ""
    assert facade.state["last_success_period"] == ""
