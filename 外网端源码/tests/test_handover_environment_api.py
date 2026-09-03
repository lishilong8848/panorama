from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.bootstrap.app_factory import _is_externally_allowed_path
from app.modules.handover_review.api import routes
from handover_log_module.repository.review_session_state_store import ReviewSessionStateStore
from handover_log_module.service import handover_environment_service as environment


@pytest.fixture
def api(monkeypatch, tmp_path):
    config = {"_global_paths": {"runtime_state_root": str(tmp_path)}}
    store = ReviewSessionStateStore(global_paths=config["_global_paths"])
    for shift, dry, wet in [("day", "30.0℃", "24.0℃"), ("night", "26", "22")]:
        store.save_shared_block_unlocked(
            batch_key=f"2026-09-03|{shift}", block_id="outdoor_temperature",
            payload={"cells": {"B7": dry, "D7": wet}}, building="A楼",
        )
    hours = [datetime(2026, 9, 3) + timedelta(hours=i) for i in range(48)]
    fetch = Mock(return_value=[{"time": hour, "temperature": 28, "weather_code": 2} for hour in hours])
    monkeypatch.setattr(environment.hvac.WeatherSummaryProvider, "fetch_forecast", fetch)
    monkeypatch.setattr(environment, "_WEATHER_CACHE", {})
    monkeypatch.setattr(routes, "_handover_cfg", lambda _container: config)
    app = FastAPI()
    app.state.container = SimpleNamespace()
    app.include_router(routes.router)
    with TestClient(app) as client:
        yield client, store, fetch


@pytest.mark.parametrize("shift, dry, wet, end", [
    ("day", 30, 24, "2026-09-03 18:00:00"),
    ("night", 26, 22, "2026-09-04 09:00:00"),
])
def test_real_http_route_reads_correct_shift_and_does_not_write(api, shift, dry, wet, end):
    client, store, fetch = api
    before = store.get_shared_block(batch_key=f"2026-09-03|{shift}", block_id="outdoor_temperature")
    response = client.get("/api/handover/review/environment", params={"duty_date": "2026-09-03", "duty_shift": shift})
    body = response.json()
    assert response.status_code == 200 and body["status"] == "success"
    assert response.headers["cache-control"] == "no-store"
    assert body["data"]["weather"] == "多云"
    assert body["data"]["dry_bulb_temperature"] == dry
    assert body["data"]["wet_bulb_temperature"] == wet
    assert 0 < body["data"]["relative_humidity"] < 100
    assert body["sources"]["weather"]["window_end"] == end
    assert "session" not in body and "document" not in body
    assert store.get_shared_block(batch_key=f"2026-09-03|{shift}", block_id="outdoor_temperature") == before
    assert _is_externally_allowed_path("/api/handover/review/environment")
    fetch.assert_called_once()


def test_cached_weather_does_not_hide_new_saved_temperature(api):
    client, store, fetch = api
    params = {"duty_date": "2026-09-03", "duty_shift": "day"}
    client.get("/api/handover/review/environment", params=params)
    store.save_shared_block_unlocked(
        batch_key="2026-09-03|day", block_id="outdoor_temperature",
        payload={"cells": {"B7": "29", "D7": "23"}},
    )
    body = client.get("/api/handover/review/environment", params=params).json()
    assert body["data"]["dry_bulb_temperature"] == 29
    assert body["sources"]["temperature"]["revision"] == 2
    fetch.assert_called_once()


@pytest.mark.parametrize("params", [
    {"duty_date": "2026-09-03"}, {"duty_shift": "day"},
    {"duty_date": "2026-02-30", "duty_shift": "day"},
    {"duty_date": "2026-9-3", "duty_shift": "day"},
    {"duty_date": "2026-09-03", "duty_shift": "白班"},
    {"duty_date": "9999-12-31", "duty_shift": "night"},
])
def test_invalid_context_returns_400_without_fetching(api, params):
    client, _, fetch = api
    assert client.get("/api/handover/review/environment", params=params).status_code == 400
    fetch.assert_not_called()


def test_missing_batch_and_old_weather_do_not_fall_back_to_today(api):
    client, _, _ = api
    body = client.get("/api/handover/review/environment", params={
        "duty_date": "2026-08-03", "duty_shift": "night",
    }).json()
    assert body["status"] == "unavailable"
    assert all(value is None for value in body["data"].values())
    assert "weather_window_unavailable" in body["warnings"]


def test_weather_timeout_keeps_temperatures_and_cools_down(api):
    client, _, fetch = api
    fetch.side_effect = TimeoutError("private upstream error")
    for _ in range(2):
        response = client.get("/api/handover/review/environment", params={
            "duty_date": "2026-09-03", "duty_shift": "day",
        })
        body = response.json()
        assert body["status"] == "partial" and body["data"]["weather"] is None
        assert body["data"]["relative_humidity"] is not None
        assert "weather_unavailable" in body["warnings"]
        assert "private upstream error" not in response.text
    fetch.assert_called_once()


def test_inflight_weather_request_does_not_wait_or_start_another_fetch(api):
    client, _, fetch = api
    with environment._WEATHER_LOCK:
        body = client.get("/api/handover/review/environment", params={
            "duty_date": "2026-09-03", "duty_shift": "day",
        }).json()
    assert body["status"] == "partial"
    assert "weather_refreshing" in body["warnings"]
    fetch.assert_not_called()


def test_weather_cache_retries_after_expiry(api, monkeypatch):
    client, _, fetch = api
    fetch.side_effect = TimeoutError("offline")
    params = {"duty_date": "2026-09-03", "duty_shift": "day"}
    client.get("/api/handover/review/environment", params=params)
    monkeypatch.setitem(environment._WEATHER_CACHE, "expires_at", 0)
    fetch.side_effect = None
    body = client.get("/api/handover/review/environment", params=params).json()
    assert body["status"] == "success"
    assert fetch.call_count == 2


def test_missing_state_file_stays_missing(api, monkeypatch, tmp_path):
    client, _, _ = api
    missing_root = tmp_path / "empty"
    monkeypatch.setattr(routes, "_handover_cfg", lambda _container: {
        "_global_paths": {"runtime_state_root": str(missing_root)},
    })
    body = client.get("/api/handover/review/environment", params={
        "duty_date": "2026-09-03", "duty_shift": "day",
    }).json()
    assert body["data"]["dry_bulb_temperature"] is None
    assert not (missing_root / ReviewSessionStateStore.DB_FILE).exists()


@pytest.mark.parametrize("dry, wet, reason", [
    ("NaN", "22", "temperature_missing_or_invalid"),
    ("26", "Infinity", "temperature_missing_or_invalid"),
    ("26", "27", "wet_bulb_exceeds_dry_bulb"),
    ("", "", "temperature_missing_or_invalid"),
])
def test_invalid_temperatures_never_become_zero_or_nan_json(api, dry, wet, reason):
    client, store, _ = api
    store.save_shared_block_unlocked(
        batch_key="2026-09-03|day", block_id="outdoor_temperature",
        payload={"cells": {"B7": dry, "D7": wet}},
    )
    body = client.get("/api/handover/review/environment", params={
        "duty_date": "2026-09-03", "duty_shift": "day",
    }).json()
    assert body["status"] == "partial"
    assert body["data"]["relative_humidity"] is None
    assert reason in body["warnings"]


def test_no_parameters_uses_current_shift(api, monkeypatch):
    client, _, _ = api
    monkeypatch.setattr(routes, "_current_handover_duty_context", lambda: ("2026-09-03", "night"))
    assert client.get("/api/handover/review/environment").json()["batch_key"] == "2026-09-03|night"


@pytest.mark.parametrize("now, expected", [
    (datetime(2026, 9, 4, 8, 59), ("2026-09-03", "night")),
    (datetime(2026, 9, 4, 9), ("2026-09-04", "day")),
    (datetime(2026, 9, 4, 18), ("2026-09-04", "night")),
])
def test_default_shift_boundaries(now, expected):
    assert routes._current_handover_duty_context(now=now) == expected
