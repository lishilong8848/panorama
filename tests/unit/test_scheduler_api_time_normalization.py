from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.modules.scheduler.api import (
    alarm_event_upload_routes,
    day_metric_upload_routes,
    handover_routes,
    monthly_change_report_routes,
    monthly_event_report_routes,
    routes,
    wet_bulb_collection_routes,
)
from app.modules.scheduler.api._time_normalization import normalize_scheduler_time


class _FakeContainer:
    def __init__(self, config: dict):
        self.config = config
        self.config_path = "settings.yaml"
        self.handover_scheduler_manager = None
        self.alarm_event_upload_scheduler = None
        self.monthly_event_report_scheduler = None
        self.monthly_change_report_scheduler = None
        self.runtime_config = {}

    def reload_config(self, config: dict) -> None:
        self.config = config

    def record_external_scheduler_toggle(self, **_kwargs) -> None:
        return None

    def handover_scheduler_status(self) -> dict:
        return {"enabled": False, "running": False, "slots": {}}

    def is_handover_scheduler_executor_bound(self) -> bool:
        return True

    def handover_scheduler_executor_name(self) -> str:
        return "handover_callback"

    def alarm_event_upload_scheduler_status(self) -> dict:
        return {"enabled": False, "running": False}

    def is_alarm_event_upload_scheduler_executor_bound(self) -> bool:
        return True

    def alarm_event_upload_scheduler_executor_name(self) -> str:
        return "alarm_callback"

    def monthly_event_report_scheduler_status(self) -> dict:
        return {"enabled": False, "running": False}

    def is_monthly_event_report_scheduler_executor_bound(self) -> bool:
        return True

    def monthly_event_report_scheduler_executor_name(self) -> str:
        return "monthly_event_callback"

    def monthly_change_report_scheduler_status(self) -> dict:
        return {"enabled": False, "running": False}

    def is_monthly_change_report_scheduler_executor_bound(self) -> bool:
        return True

    def monthly_change_report_scheduler_executor_name(self) -> str:
        return "monthly_change_callback"


def _request(container: _FakeContainer) -> SimpleNamespace:
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def _save_scheduler_config_snapshot(
    container: _FakeContainer,
    config: dict,
    *,
    path: tuple[str, ...],
    scheduler_key: str | None = None,
    restart_running: bool = False,
) -> dict:
    _ = path
    _ = scheduler_key
    _ = restart_running
    saved = copy.deepcopy(config)
    container.reload_config(saved)
    return saved


def test_normalize_scheduler_time_accepts_browser_time_without_seconds() -> None:
    assert normalize_scheduler_time("7:05") == "07:05:00"
    assert normalize_scheduler_time("07:05") == "07:05:00"
    assert normalize_scheduler_time("07:05:09") == "07:05:09"


@pytest.mark.parametrize("value", ["", "24:00", "08:60", "08:00:60", "not-a-time"])
def test_normalize_scheduler_time_rejects_invalid_values(value: str) -> None:
    with pytest.raises(HTTPException) as exc_info:
        normalize_scheduler_time(value)

    assert exc_info.value.status_code == 400


def test_handover_config_accepts_browser_time_without_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(handover_routes, "save_scheduler_config_snapshot", _save_scheduler_config_snapshot)
    container = _FakeContainer(
        {
            "features": {
                "handover_log": {
                    "scheduler": {
                        "morning_time": "07:00:00",
                        "afternoon_time": "16:00:00",
                    }
                }
            }
        }
    )

    data = handover_routes.handover_scheduler_config(
        {"morning_time": "7:05", "afternoon_time": "16:30"},
        _request(container),
    )

    scheduler = container.config["features"]["handover_log"]["scheduler"]
    assert scheduler["morning_time"] == "07:05:00"
    assert scheduler["afternoon_time"] == "16:30:00"
    assert data["scheduler_config"]["morning_time"] == "07:05:00"
    assert data["scheduler_config"]["afternoon_time"] == "16:30:00"


@pytest.mark.parametrize(
    ("route_module", "handler", "config", "path"),
    [
        (
            alarm_event_upload_routes,
            alarm_event_upload_routes.alarm_event_upload_scheduler_config,
            {"features": {"alarm_export": {"scheduler": {"run_time": "08:10:00"}}}},
            ("features", "alarm_export", "scheduler"),
        ),
        (
            monthly_event_report_routes,
            monthly_event_report_routes.monthly_event_report_scheduler_config,
            {
                "features": {
                    "handover_log": {
                        "monthly_event_report": {"scheduler": {"run_time": "01:00:00"}}
                    }
                }
            },
            ("features", "handover_log", "monthly_event_report", "scheduler"),
        ),
        (
            monthly_change_report_routes,
            monthly_change_report_routes.monthly_change_report_scheduler_config,
            {
                "features": {
                    "handover_log": {
                        "monthly_change_report": {"scheduler": {"run_time": "01:00:00"}}
                    }
                }
            },
            ("features", "handover_log", "monthly_change_report", "scheduler"),
        ),
    ],
)
def test_single_time_scheduler_config_accepts_browser_time_without_seconds(
    monkeypatch: pytest.MonkeyPatch,
    route_module,
    handler,
    config: dict,
    path: tuple[str, ...],
) -> None:
    monkeypatch.setattr(route_module, "save_scheduler_config_snapshot", _save_scheduler_config_snapshot)
    container = _FakeContainer(config)

    data = handler({"run_time": "9:15"}, _request(container))

    scheduler = container.config
    for key in path:
        scheduler = scheduler[key]
    assert scheduler["run_time"] == "09:15:00"
    assert data["scheduler_config"]["run_time"] == "09:15:00"


@pytest.mark.parametrize(
    ("route_module", "handler_name", "stop_method_name"),
    [
        (routes, "scheduler_stop", "stop_scheduler"),
        (handover_routes, "handover_scheduler_stop", "stop_handover_scheduler"),
        (wet_bulb_collection_routes, "wet_bulb_scheduler_stop", "stop_wet_bulb_collection_scheduler"),
        (day_metric_upload_routes, "day_metric_upload_scheduler_stop", "stop_day_metric_upload_scheduler"),
        (alarm_event_upload_routes, "alarm_event_upload_scheduler_stop", "stop_alarm_event_upload_scheduler"),
        (monthly_event_report_routes, "monthly_event_report_scheduler_stop", "stop_monthly_event_report_scheduler"),
        (monthly_change_report_routes, "monthly_change_report_scheduler_stop", "stop_monthly_change_report_scheduler"),
    ],
)
def test_scheduler_stop_routes_do_not_stop_before_persist_succeeds(
    monkeypatch: pytest.MonkeyPatch,
    route_module,
    handler_name: str,
    stop_method_name: str,
) -> None:
    class _StopContainer:
        config = {}
        config_path = "settings.yaml"

        def __init__(self) -> None:
            self.stop_called = False

    container = _StopContainer()

    def _stop_method():
        container.stop_called = True
        return {"stopped": True, "running": False, "reason": "stopped"}

    setattr(container, stop_method_name, _stop_method)

    def _raise_persist(*_args, **_kwargs):
        raise HTTPException(status_code=400, detail="persist failed")

    monkeypatch.setattr(route_module, "persist_scheduler_toggle", _raise_persist)

    handler = getattr(route_module, handler_name)
    with pytest.raises(HTTPException, match="persist failed"):
        handler(_request(container))

    assert container.stop_called is False
