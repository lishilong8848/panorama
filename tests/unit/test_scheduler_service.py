from __future__ import annotations

from datetime import datetime

import app.modules.scheduler.service.daily_scheduler_service as scheduler_mod
from app.modules.scheduler.service.daily_scheduler_service import DailyAutoSchedulerService
from app.modules.scheduler.service.interval_scheduler_service import IntervalSchedulerService


def _build_service(tmp_path):
    scheduler_mod.get_app_dir = lambda: tmp_path  # type: ignore[assignment]
    cfg = {
        "scheduler": {
            "enabled": True,
            "auto_start_in_gui": False,
            "run_time": "00:10:00",
            "check_interval_sec": 30,
            "catch_up_if_missed": False,
            "retry_failed_in_same_period": True,
            "state_file": "daily_scheduler_state.json",
        }
    }
    return DailyAutoSchedulerService(
        config=cfg,
        emit_log=lambda _: None,
        run_callback=lambda _: (True, ""),
        is_busy=lambda: False,
    )


def _build_interval_service(tmp_path, *, interval_minutes: int = 60) -> IntervalSchedulerService:
    return IntervalSchedulerService(
        scheduler_cfg={
            "enabled": True,
            "auto_start_in_gui": False,
            "interval_minutes": interval_minutes,
            "check_interval_sec": 30,
            "state_file": "interval_scheduler_state.json",
        },
        runtime_state_root=str(tmp_path),
        emit_log=lambda _: None,
        run_callback=lambda _: (True, ""),
        is_busy=lambda: False,
    )


def test_interval_next_run_time_advances_overdue_time_to_next_interval_boundary(tmp_path):
    svc = _build_interval_service(tmp_path, interval_minutes=60)
    svc.state["last_attempt_at"] = "2026-04-22 15:34:43"
    now = datetime(2026, 4, 23, 2, 0, 0)

    assert svc.next_run_time(now) == datetime(2026, 4, 23, 2, 34, 43)


def test_interval_due_run_time_keeps_overdue_target_for_scheduler_loop(tmp_path):
    svc = _build_interval_service(tmp_path, interval_minutes=60)
    svc.state["last_attempt_at"] = "2026-04-22 15:34:43"

    assert svc.due_run_time() == datetime(2026, 4, 22, 16, 34, 43)


def test_interval_next_run_time_keeps_future_saved_next_run(tmp_path):
    svc = _build_interval_service(tmp_path, interval_minutes=60)
    svc.state["last_attempt_at"] = "2026-04-23 01:34:43"
    now = datetime(2026, 4, 23, 2, 0, 0)

    assert svc.next_run_time(now) == datetime(2026, 4, 23, 2, 34, 43)


def test_interval_next_run_time_uses_stable_started_at_without_attempt(tmp_path):
    svc = _build_interval_service(tmp_path, interval_minutes=60)
    svc.started_at = datetime(2026, 4, 23, 1, 28, 17)
    now = datetime(2026, 4, 23, 2, 28, 18)

    assert svc.next_run_time(now) == datetime(2026, 4, 23, 3, 28, 17)


def test_should_trigger_due_after_run_time(tmp_path):
    svc = _build_service(tmp_path)
    now = datetime(2026, 3, 8, 0, 10, 30)
    svc.started_at = datetime(2026, 3, 8, 0, 0, 0)

    should_run, period, reason = svc._should_trigger(now)
    assert should_run is True
    assert period == "2026-03-08"
    assert reason == "due"


def test_daily_next_run_time_is_stable_when_not_started_and_overdue(tmp_path):
    svc = _build_service(tmp_path)
    now = datetime(2026, 3, 8, 0, 10, 30)
    svc.started_at = datetime(2026, 3, 8, 0, 0, 0)

    assert svc.next_run_time(now) == datetime(2026, 3, 8, 0, 10, 0)


def test_should_skip_when_success_today(tmp_path):
    svc = _build_service(tmp_path)
    now = datetime(2026, 3, 8, 0, 10, 30)
    svc.state["last_success_period"] = "2026-03-08"

    should_run, period, reason = svc._should_trigger(now)
    assert should_run is False
    assert period == "2026-03-08"
    assert reason == "already_success_today"


def test_should_skip_missed_and_no_catchup(tmp_path):
    svc = _build_service(tmp_path)
    now = datetime(2026, 3, 8, 10, 30, 0)
    svc.started_at = datetime(2026, 3, 8, 10, 0, 0)
    svc.cfg["run_time"] = "09:00:00"

    should_run, period, reason = svc._should_trigger(now)
    assert should_run is False
    assert period == "2026-03-08"
    assert reason == "missed_and_no_catchup"


def test_should_trigger_missed_and_catchup_enabled(tmp_path):
    svc = _build_service(tmp_path)
    now = datetime(2026, 3, 8, 10, 30, 0)
    svc.started_at = datetime(2026, 3, 8, 10, 0, 0)
    svc.cfg["run_time"] = "09:00:00"
    svc.cfg["catch_up_if_missed"] = True

    should_run, period, reason = svc._should_trigger(now)
    assert should_run is True
    assert period == "2026-03-08"
    assert reason == "due"


def test_should_trigger_retry_once_when_enabled(tmp_path):
    svc = _build_service(tmp_path)
    now = datetime(2026, 3, 8, 10, 30, 0)
    svc.cfg["run_time"] = "09:00:00"
    svc.state["last_attempt_period"] = "2026-03-08"
    svc.state["last_status"] = "failed"
    svc.cfg["retry_failed_in_same_period"] = True

    should_run, period, reason = svc._should_trigger(now)
    assert should_run is True
    assert period == "2026-03-08"
    assert reason == "due_retry"

    svc.state["retry_done_period"] = "2026-03-08"
    should_run2, period2, reason2 = svc._should_trigger(now)
    assert should_run2 is False
    assert period2 == "2026-03-08"
    assert reason2 == "retry_already_done"


def test_should_not_retry_when_retry_disabled(tmp_path):
    svc = _build_service(tmp_path)
    now = datetime(2026, 3, 8, 10, 30, 0)
    svc.cfg["run_time"] = "09:00:00"
    svc.state["last_attempt_period"] = "2026-03-08"
    svc.state["last_status"] = "failed"
    svc.cfg["retry_failed_in_same_period"] = False

    should_run, period, reason = svc._should_trigger(now)
    assert should_run is False
    assert period == "2026-03-08"
    assert reason == "already_attempted_no_retry"


def test_reset_today_state_for_run_time_change(tmp_path):
    svc = _build_service(tmp_path)
    svc.state.update(
        {
            "last_success_period": "2026-03-08",
            "last_attempt_period": "2026-03-08",
            "retry_done_period": "2026-03-08",
            "last_status": "failed",
            "last_error": "x",
            "last_run_at": "2026-03-08 09:10:00",
        }
    )

    out = svc.reset_today_state_for_run_time_change(datetime(2026, 3, 8, 11, 0, 0))
    assert out["changed"] is True
    assert svc.state["last_success_period"] == ""
    assert svc.state["last_attempt_period"] == ""
    assert svc.state["retry_done_period"] == ""
    assert svc.state["last_status"] == ""
    assert svc.state["last_error"] == ""
    assert svc.state["last_run_at"] == ""
