from __future__ import annotations

import threading
import time
from pathlib import Path

from app.modules.report_pipeline.service import job_service as job_service_module
from app.modules.report_pipeline.service.job_service import JobService


def _wait_until(predicate, timeout_sec: float = 3.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition not met before timeout")


def test_network_window_drains_and_switches_to_opposite_side(tmp_path: Path) -> None:
    service = JobService()
    service.configure_task_engine(
        runtime_config={
            "paths": {},
            "execution": {
                "network": {
                    "max_window_duration_sec": 600,
                    "max_dispatches_per_window": 1,
                    "max_opposite_wait_sec": 120,
                }
            },
        },
        app_dir=tmp_path,
        config_snapshot_getter=lambda: {"paths": {}},
    )
    release_external = threading.Event()
    release_internal = threading.Event()
    external_started = threading.Event()
    internal_started = threading.Event()

    def _external_first(_emit_log):  # noqa: ANN001
        external_started.set()
        release_external.wait(timeout=3)
        return {"status": "external-1"}

    def _internal_waiter(_emit_log):  # noqa: ANN001
        internal_started.set()
        release_internal.wait(timeout=3)
        return {"status": "internal"}

    def _external_second(_emit_log):  # noqa: ANN001
        return {"status": "external-2"}

    first = service.start_job("external-1", _external_first, resource_keys=["network:external"])
    external_started.wait(timeout=1)
    second = service.start_job("internal", _internal_waiter, resource_keys=["network:internal"])
    third = service.start_job("external-2", _external_second, resource_keys=["network:external"])

    _wait_until(lambda: service.get_job_state(second.job_id).status == "waiting_resource")
    _wait_until(lambda: service.get_job_state(third.job_id).status == "success")

    waiting_snapshot = service.get_resource_snapshot()
    assert waiting_snapshot["network"]["current_side"] == "external"
    assert waiting_snapshot["network"]["window_draining"] is False
    assert waiting_snapshot["network"]["pending_side"] == ""
    assert waiting_snapshot["network"]["running_external"] == 1

    release_external.set()
    _wait_until(lambda: service.get_job_state(second.job_id).status == "running")
    assert service.get_job_state(third.job_id).status == "success"

    release_internal.set()
    service.wait_job(first.job_id, timeout_sec=3)
    service.wait_job(second.job_id, timeout_sec=3)
    service.wait_job(third.job_id, timeout_sec=3)

    assert service.get_job(third.job_id)["status"] == "success"


def test_network_window_snapshot_exposes_wait_ages(tmp_path: Path) -> None:
    service = JobService()
    service.configure_task_engine(
        runtime_config={"paths": {}},
        app_dir=tmp_path,
        config_snapshot_getter=lambda: {"paths": {}},
    )
    release = threading.Event()
    started = threading.Event()

    def _external(_emit_log):  # noqa: ANN001
        started.set()
        release.wait(timeout=3)
        return {"status": "ok"}

    first = service.start_job("external", _external, resource_keys=["network:external"])
    started.wait(timeout=1)
    second = service.start_job("internal", lambda _emit_log: {"status": "ok"}, resource_keys=["network:internal"])
    _wait_until(lambda: service.get_job_state(second.job_id).status == "waiting_resource")
    time.sleep(1.1)
    snapshot = service.get_resource_snapshot()
    assert snapshot["network"]["oldest_internal_wait_sec"] >= 1
    assert snapshot["network"]["running_external"] == 1
    release.set()
    service.wait_job(first.job_id, timeout_sec=3)
    service.wait_job(second.job_id, timeout_sec=3)


def test_internal_unreachable_waits_until_manual_switch(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    current = {"ssid": "outer"}
    state = {
        "current_ssid": "outer",
        "ssid_side": "external",
        "internal_reachable": False,
        "external_reachable": True,
        "reachable_sides": ["external"],
        "mode": "external_only",
        "last_checked_at": "2026-03-27 10:00:00",
    }
    monkeypatch.setattr(job_service_module, "get_network_reachability_state", lambda **_kwargs: dict(state))
    service = JobService()
    service.configure_task_engine(
        runtime_config={
            "paths": {},
            "network": {
                "enable_auto_switch_wifi": False,
                "internal_ssid": "inner",
                "external_ssid": "outer",
            },
        },
        app_dir=tmp_path,
        config_snapshot_getter=lambda: {"paths": {}},
        current_ssid_getter=lambda: current["ssid"],
    )

    job = service.start_job(
        "internal-only",
        lambda _emit_log: {"status": "ok"},
        resource_keys=["network:internal"],
    )

    _wait_until(lambda: service.get_job_state(job.job_id).status == "waiting_resource")
    waiting_state = service.get_job(job.job_id)
    assert waiting_state["wait_reason"] == "waiting:network_internal_unreachable"

    waiting_snapshot = service.get_resource_snapshot()
    assert waiting_snapshot["network"]["auto_switch_enabled"] is False
    assert waiting_snapshot["network"]["current_ssid"] == "outer"
    assert waiting_snapshot["network"]["current_detected_side"] == "external"
    assert waiting_snapshot["network"]["current_side"] == "external"
    assert waiting_snapshot["network"]["internal_reachable"] is False
    assert waiting_snapshot["network"]["external_reachable"] is True

    current["ssid"] = "inner"
    state.update(
        {
            "current_ssid": "inner",
            "ssid_side": "internal",
            "internal_reachable": True,
            "external_reachable": True,
            "reachable_sides": ["internal", "external"],
            "mode": "internal_only",
        }
    )
    service._network_status_checked_monotonic = 0.0  # noqa: SLF001
    _wait_until(lambda: service.get_job_state(job.job_id).status == "success")


def test_switching_ready_prefers_current_ssid_side_without_parallel_run(tmp_path: Path) -> None:
    current = {"ssid": "outer"}
    service = JobService()
    service.configure_task_engine(
        runtime_config={
            "paths": {},
            "network": {
                "enable_auto_switch_wifi": False,
                "internal_ssid": "inner",
                "external_ssid": "outer",
                "post_switch_probe_external_host": "",
            },
            "download": {
                "sites": [
                    {"enabled": True, "host": "10.0.0.10"},
                ]
            },
        },
        app_dir=tmp_path,
        config_snapshot_getter=lambda: {"paths": {}},
        current_ssid_getter=lambda: current["ssid"],
    )

    service._network_status_checked_monotonic = 0.0  # noqa: SLF001
    service._network_status_snapshot = {  # noqa: SLF001
        "current_ssid": "outer",
        "ssid_side": "external",
        "internal_reachable": True,
        "external_reachable": True,
        "reachable_sides": ["internal", "external"],
        "mode": "switching_ready",
        "last_checked_at": "2026-03-27 10:00:00",
    }
    service._network_status_cache_ttl_sec = 9999  # noqa: SLF001
    service._network_status_checked_monotonic = time.monotonic()  # noqa: SLF001

    release_external = threading.Event()
    external_started = threading.Event()

    def _internal(_emit_log):  # noqa: ANN001
        return {"status": "internal"}

    def _external(_emit_log):  # noqa: ANN001
        external_started.set()
        release_external.wait(timeout=3)
        return {"status": "external"}

    internal_job = service.start_job("internal", _internal, resource_keys=["network:internal"])
    external_job = service.start_job("external", _external, resource_keys=["network:external"])

    _wait_until(lambda: service.get_job_state(external_job.job_id).status == "running")
    _wait_until(lambda: service.get_job_state(internal_job.job_id).status == "waiting_resource")

    snapshot = service.get_resource_snapshot()
    assert snapshot["network"]["mode"] == "switching_ready"
    assert snapshot["network"]["current_side"] == "external"
    assert snapshot["network"]["running_internal"] == 0
    assert snapshot["network"]["running_external"] == 1

    release_external.set()
    service.wait_job(external_job.job_id, timeout_sec=3)
    assert service.get_job_state(internal_job.job_id).status == "waiting_resource"
