from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.modules.shared_bridge.service.shared_source_cache_service import (  # noqa: E402
    FAMILY_BRANCH_POWER,
    FAMILY_ALARM_EVENT,
    FAMILY_ELECTRICAL_INSPECTION,
    FAMILY_HANDOVER_LOG,
    FAMILY_HVAC_INSPECTION,
    SharedSourceCacheService,
)


def make_service(tmp_path, monkeypatch):
    service = SharedSourceCacheService(
        runtime_config={"deployment": {"role_mode": "internal"}, "shared_bridge": {"root_dir": str(tmp_path)}},
        store=None,
        emit_log=lambda _line: None,
    )
    monkeypatch.setattr(service, "get_enabled_buildings", lambda: ["A楼", "B楼"])
    monkeypatch.setattr(service, "_family_buildings", lambda _family: ["A楼", "B楼"])
    return service


def test_each_family_and_building_starts_before_slow_unit_finishes(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    started = set()
    lock = threading.Lock()
    all_started = threading.Event()
    release = threading.Event()

    def run_unit(*, building, steps, **_kwargs):
        family = steps[0][0]
        with lock:
            started.add((building, family))
            if len(started) == 10:
                all_started.set()
        assert release.wait(3)
        return {"results": {family: {}}, "completed_units": [f"{building}/{family}"]}

    monkeypatch.setattr(service, "_run_building_latest_source_steps", run_unit)
    output = []
    runner = threading.Thread(target=lambda: output.append(service._run_latest_source_steps_by_building(
        steps=[(FAMILY_HANDOVER_LOG, "2026-01-01 00", lambda **_: None),
               (FAMILY_BRANCH_POWER, "2026-01-01", lambda **_: None),
               (FAMILY_ELECTRICAL_INSPECTION, "2026-01-01 00", lambda **_: None),
               (FAMILY_HVAC_INSPECTION, "2026-01-01 00", lambda **_: None),
               (FAMILY_ALARM_EVENT, "2026-01-01 00", lambda **_: None)],
        force_retry_failed=False,
    )))
    runner.start()
    try:
        assert all_started.wait(2)
    finally:
        release.set()
        runner.join(4)
    assert not runner.is_alive()
    assert len(output[0]["completed_units"]) == 10
    assert len(output[0]["building_results"]["A楼"]["results"]) == 5


def test_browser_sources_remain_grouped_in_building_order(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    calls = []

    def run_unit(*, building, steps, **_kwargs):
        calls.append((building, [step[0] for step in steps]))
        return {"results": {step[0]: {} for step in steps}}

    monkeypatch.setattr(service, "_run_building_latest_source_steps", run_unit)
    service._run_latest_source_steps_by_building(
        steps=[("excel_a", "2026-01-01", lambda **_: None),
               ("excel_b", "2026-01-01", lambda **_: None)], force_retry_failed=False,
    )
    assert sorted(calls) == [("A楼", ["excel_a", "excel_b"]), ("B楼", ["excel_a", "excel_b"])]


def test_same_key_deduplicated_and_completed_not_reported_running(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    monkeypatch.setattr(service, "_entry_exists_for_bucket", lambda **_: False)
    monkeypatch.setattr(service, "_get_ready_entry", lambda **_: None)
    monkeypatch.setattr(service, "_get_source_cache_entry", lambda **_: None)
    started = threading.Event()
    release = threading.Event()
    calls = []

    def fill(**kwargs):
        calls.append(kwargs["building"])
        started.set()
        assert release.wait(3)
        return {"relative_path": "test.json", "file_path": "test.json"}

    def refresh():
        return service._refresh_family_bucket(source_family=FAMILY_BRANCH_POWER, bucket_key="2026-01-01",
                                              fill_func=fill, buildings=["A楼"])

    result = []
    runner = threading.Thread(target=lambda: result.append(refresh()))
    runner.start()
    try:
        assert started.wait(2)
        duplicate = refresh()
        assert duplicate["running_buildings"] == ["A楼"]
        assert calls == ["A楼"]
    finally:
        release.set()
        runner.join(4)
    assert result[0]["completed_buildings"] == ["A楼"]
    assert result[0]["running_buildings"] == []
    assert not service._active_latest_downloads


def test_failure_recording_error_does_not_leave_other_units_active(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    monkeypatch.setattr(service, "_entry_exists_for_bucket", lambda **_: False)
    monkeypatch.setattr(service, "_get_ready_entry", lambda **_: None)
    monkeypatch.setattr(service, "_get_source_cache_entry", lambda **_: None)
    monkeypatch.setattr(service, "_record_failed_entry", lambda **_: (_ for _ in ()).throw(RuntimeError("store")))

    def fail(**_kwargs):
        raise ValueError("collection")

    result = service._refresh_family_bucket(source_family=FAMILY_BRANCH_POWER, bucket_key="2026-01-01",
                                            fill_func=fail, buildings=["A楼", "B楼"])
    assert set(result["failed_buildings"]) == {"A楼", "B楼"}
    assert not service._active_latest_downloads


def test_scheduler_runs_independent_jobs_and_keeps_inflight_on_restart(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    first_started = threading.Event()
    second_started = threading.Event()
    release = threading.Event()
    calls = []

    def slow():
        calls.append("slow")
        first_started.set()
        assert release.wait(3)

    service._start_scheduled_refresh("slow", slow)
    try:
        assert first_started.wait(2)
        service._start_scheduled_refresh("other", second_started.set)
        assert second_started.wait(2)
        service._stop_event.set()
        service._stop_event.clear()
        service._start_scheduled_refresh("slow", slow)
        assert calls == ["slow"]
    finally:
        release.set()
        deadline = time.monotonic() + 3
        while service._scheduled_refresh_threads and time.monotonic() < deadline:
            time.sleep(0.01)
    assert not service._scheduled_refresh_threads


def test_late_hourly_completion_does_not_replace_newer_status(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    family = FAMILY_HANDOVER_LOG
    with service._lock:
        service._ensure_light_family_cache_unlocked(source_family=family, bucket_key="2999-01-01 11")
        service._set_light_building_status_unlocked(source_family=family, building="A楼",
                                                   bucket_key="2999-01-01 11", payload={"status": "downloading"})
        service._ensure_light_family_cache_unlocked(source_family=family, bucket_key="2999-01-01 10")
        service._set_light_building_status_unlocked(source_family=family, building="A楼",
                                                   bucket_key="2999-01-01 10", payload={"status": "ready"})
        service._recompute_family_status_from_light_unlocked(source_family=family, bucket_key="2999-01-01 10")
    assert service._family_status[family]["current_bucket"] == "2999-01-01 11"
    assert service._light_building_status[family]["A楼"]["status"] == "downloading"


def test_api_families_ignore_browser_pause(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    service.download_browser_pool = type("Pool", (), {
        "get_building_pause_info": lambda self, _building: {"suspended": True},
    })()
    monkeypatch.setattr(service, "_entry_exists_for_bucket", lambda **_: False)
    monkeypatch.setattr(service, "_get_ready_entry", lambda **_: None)
    monkeypatch.setattr(service, "_get_source_cache_entry", lambda **_: None)
    for family in (FAMILY_BRANCH_POWER, FAMILY_ELECTRICAL_INSPECTION, FAMILY_HVAC_INSPECTION, FAMILY_ALARM_EVENT):
        result = service._refresh_family_bucket(
            source_family=family, bucket_key="2999-01-01 10", buildings=["A楼"],
            fill_func=lambda **_: {"relative_path": "test.json", "file_path": "test.json"},
        )
        assert result["completed_buildings"] == ["A楼"]
        assert result["blocked_buildings"] == []


def test_inspection_fill_does_not_require_browser_pool(tmp_path, monkeypatch):
    import app.modules.shared_bridge.service.shared_source_cache_service as module

    service = make_service(tmp_path, monkeypatch)
    service.download_browser_pool = None
    service._tmp_root = tmp_path
    monkeypatch.setattr(service, "_ensure_internal_source_writer", lambda _message: None)
    monkeypatch.setattr(service, "_existing_latest_refresh_result", lambda **_: None)
    monkeypatch.setattr(service, "_inspection_point_rules", lambda: {})
    monkeypatch.setattr(service, "_store_entry", lambda **_: {"stored": True})
    monkeypatch.setattr(module, "load_handover_config", lambda _config: {})
    gateway = Mock()
    gateway.run_inspection_data_only.return_value = {
        "query_start": "2026-01-01 09:00:00", "query_end": "2026-01-01 10:00:00",
    }
    factory = Mock(return_value=gateway)
    monkeypatch.setattr(module, "HandoverDownloadService", factory)
    result = service._fill_inspection_latest(source_family=FAMILY_ELECTRICAL_INSPECTION, building="A楼",
                                             bucket_key="2026-01-01 10", emit_log=lambda _line: None)
    assert result == {"stored": True}
    assert factory.call_args.kwargs["download_browser_pool"] is None
    gateway.run_inspection_data_only.assert_called_once()


def test_running_loop_does_not_wait_for_slow_source_and_restart_deduplicates(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    service.enabled = True
    service.role_mode = "internal"
    service.store = object()
    service.shared_root = tmp_path
    service.run_on_startup = True
    service.check_interval_sec = 0.02
    started = threading.Event()
    other_started = threading.Event()
    release = threading.Event()
    calls = []

    def slow(_bucket):
        calls.append("current")
        started.set()
        assert release.wait(5)

    monkeypatch.setattr(service, "_run_current_bucket_once", slow)
    monkeypatch.setattr(service, "_run_monthly_report_file_if_due", other_started.set)
    for name in ("_run_top5_monthly_report_file_if_due", "_run_alarm_bucket_if_due",
                 "_run_temperature_humidity_file_if_due", "_run_daily_source_files_if_due",
                 "_retry_inspection_data_if_needed", "_run_energy_report_files_if_due"):
        monkeypatch.setattr(service, name, lambda: None)
    monkeypatch.setattr(service, "_run_chiller_mode_switch_if_due", lambda **_: None)
    try:
        assert service.start()["started"]
        assert started.wait(2)
        assert other_started.wait(2)
        assert service.stop()["stopped"]
        assert service.start()["started"]
        time.sleep(0.1)
        assert calls == ["current"]
    finally:
        service.stop()
        release.set()
        deadline = time.monotonic() + 3
        while service._scheduled_refresh_threads and time.monotonic() < deadline:
            time.sleep(0.01)


def test_scheduled_exception_is_recorded_and_key_can_run_again(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    completed = threading.Event()

    def fail():
        raise RuntimeError("local failure")

    service._start_scheduled_refresh("failing", fail)
    deadline = time.monotonic() + 2
    while "failing" in service._scheduled_refresh_threads and time.monotonic() < deadline:
        time.sleep(0.01)
    assert service._last_error == "local failure"
    service._start_scheduled_refresh("failing", completed.set)
    assert completed.wait(2)


def test_new_hour_starts_while_previous_hour_is_still_running(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    service.enabled = True
    service.role_mode = "internal"
    service.store = object()
    service.shared_root = tmp_path
    service.check_interval_sec = 0.02
    bucket = ["2999-01-01 10"]
    started = {key: threading.Event() for key in ("2999-01-01 10", "2999-01-01 11")}
    release = threading.Event()
    monkeypatch.setattr(service, "current_hour_bucket", lambda: bucket[0])

    def slow_hour(value):
        with service._lock:
            service._current_hour_bucket = value
        started[value].set()
        assert release.wait(4)

    monkeypatch.setattr(service, "_run_current_bucket_once", slow_hour)
    for name in ("_run_monthly_report_file_if_due", "_run_top5_monthly_report_file_if_due",
                 "_run_alarm_bucket_if_due", "_run_temperature_humidity_file_if_due",
                 "_run_daily_source_files_if_due", "_retry_inspection_data_if_needed", "_run_energy_report_files_if_due"):
        monkeypatch.setattr(service, name, lambda: None)
    monkeypatch.setattr(service, "_run_chiller_mode_switch_if_due", lambda **_: None)
    try:
        assert service.start()["started"]
        assert started["2999-01-01 10"].wait(2)
        bucket[0] = "2999-01-01 11"
        assert started["2999-01-01 11"].wait(2)
    finally:
        service.stop()
        release.set()
        deadline = time.monotonic() + 3
        while service._scheduled_refresh_threads and time.monotonic() < deadline:
            time.sleep(0.01)


def test_inspection_status_ignores_browser_pause(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    service.download_browser_pool = type("Pool", (), {
        "get_building_pause_info": lambda self, _building: {"suspended": True, "suspend_reason": "paused"},
    })()
    monkeypatch.setattr(service, "_get_source_cache_entry", lambda **_: None)
    row = service._build_building_cache_status(source_family=FAMILY_ELECTRICAL_INSPECTION,
                                               building="A楼", bucket_key="2026-01-01 10")
    assert not row["blocked"]


def test_alarm_manual_and_automatic_same_bucket_share_active_key(tmp_path, monkeypatch):
    service = make_service(tmp_path, monkeypatch)
    monkeypatch.setattr(service, "get_enabled_buildings", lambda: ["A楼"])
    monkeypatch.setattr(service, "_ensure_dirs", lambda: None)
    monkeypatch.setattr(service, "_alarm_manual_bucket", lambda: "2999-01-01 10")
    monkeypatch.setattr(service, "_entry_exists_for_bucket", lambda **_: False)
    monkeypatch.setattr(service, "_get_ready_entry", lambda **_: None)
    monkeypatch.setattr(service, "_get_source_cache_entry", lambda **_: None)
    service.download_browser_pool = type("Pool", (), {
        "get_building_pause_info": lambda self, _building: {"suspended": True},
    })()
    started = threading.Event()
    release = threading.Event()
    calls = []

    def manual_fill(**_kwargs):
        calls.append("manual")
        started.set()
        assert release.wait(3)
        return {"metadata": {}}

    monkeypatch.setattr(service, "fill_alarm_event_manual", manual_fill)
    manual_result = []
    runner = threading.Thread(target=lambda: manual_result.append(service._run_manual_alarm_refresh_impl()))
    runner.start()
    try:
        assert started.wait(2)
        automatic = service._refresh_family_bucket(
            source_family=FAMILY_ALARM_EVENT, bucket_key="2999-01-01 10", buildings=["A楼"],
            fill_func=lambda **_: calls.append("automatic"),
        )
        assert automatic["running_buildings"] == ["A楼"]
        assert calls == ["manual"]
    finally:
        release.set()
        runner.join(4)
    assert manual_result[0]["completed_buildings"] == ["A楼"]
    assert not service._active_latest_downloads

    with service._lock:
        service._active_latest_downloads[(FAMILY_ALARM_EVENT, "A楼", "latest", "2999-01-01 10")] = "started"
    try:
        duplicate_manual = service._run_manual_alarm_refresh_impl()
        assert duplicate_manual["running_buildings"] == ["A楼"]
        assert duplicate_manual["completed_buildings"] == []
        assert calls == ["manual"]
    finally:
        service._active_latest_downloads.clear()
