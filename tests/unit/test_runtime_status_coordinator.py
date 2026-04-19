from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

from app.modules.shared_bridge.service.runtime_status_coordinator import RuntimeStatusCoordinator
from app.modules.shared_bridge.service.runtime_status_store import RuntimeStatusStore


class _FakeJobService:
    def active_job_id(self):
        return "job-active"

    def active_job_ids(self, *, include_waiting: bool = True):  # noqa: ANN001
        return ["job-active", "job-waiting"] if include_waiting else ["job-active"]

    def job_counts(self):
        return {"running": 1, "waiting_resource": 1, "success": 0}

    def list_jobs(self, *, limit: int = 60, statuses=None):  # noqa: ANN001
        return [
            {"job_id": "job-active", "status": "running"},
            {"job_id": "job-waiting", "status": "waiting_resource"},
        ][:limit]

    def get_resource_snapshot(self):
        return {
            "network": {"current_side": "internal"},
            "controlled_browser": {"holder_job_id": "job-active", "queue_length": 1},
            "batch_locks": [],
            "resources": [],
        }


class _FakeSharedBridgeService:
    def list_tasks(self, *, limit: int = 60):  # noqa: ANN001
        return [
            {"task_id": "bridge-1", "feature": "monthly_report_pipeline", "status": "queued_for_internal"},
            {"task_id": "bridge-2", "feature": "alarm_export", "status": "queued_for_internal"},
        ][:limit]


def _fake_container(*, role_mode: str = "internal") -> SimpleNamespace:
    snapshot = {
        "enabled": True,
        "role_mode": role_mode,
        "db_status": "ok",
        "agent_status": "running",
        "last_error": "",
        "last_poll_at": "2026-04-15 09:00:00",
        "pending_internal": 1,
        "pending_external": 0,
        "problematic": 0,
        "task_count": 1,
        "internal_download_pool": {
            "enabled": True,
            "browser_ready": True,
            "active_buildings": ["A楼"],
            "last_error": "",
            "page_slots": [
                {"building": "A楼", "page_ready": True, "in_use": True},
                {"building": "B楼", "page_ready": True, "in_use": False},
                {"building": "C楼", "page_ready": True, "in_use": False},
                {"building": "D楼", "page_ready": True, "in_use": False},
                {"building": "E楼", "page_ready": True, "in_use": False},
            ],
        },
        "internal_source_cache": {
            "enabled": True,
            "scheduler_running": True,
            "current_hour_bucket": "2026-04-15 09",
            "last_run_at": "2026-04-15 09:00:00",
            "last_success_at": "2026-04-15 09:00:02",
            "last_error": "",
            "cache_root": "D:/QJPT_Shared/cache",
            "current_hour_refresh": {},
            "handover_log_family": {
                "ready_count": 1,
                "failed_buildings": [],
                "blocked_buildings": [],
                "last_success_at": "2026-04-15 09:00:02",
                "current_bucket": "2026-04-15 09",
                "buildings": [{"building": "A楼", "status": "ready", "ready": True}],
            },
            "handover_capacity_report_family": {
                "ready_count": 1,
                "failed_buildings": [],
                "blocked_buildings": [],
                "last_success_at": "2026-04-15 09:00:02",
                "current_bucket": "2026-04-15 09",
                "buildings": [{"building": "A楼", "status": "ready", "ready": True}],
            },
            "monthly_report_family": {
                "ready_count": 1,
                "failed_buildings": [],
                "blocked_buildings": [],
                "last_success_at": "2026-04-15 09:00:02",
                "current_bucket": "2026-04-15 09",
                "buildings": [{"building": "A楼", "status": "ready", "ready": True}],
            },
            "alarm_event_family": {
                "ready_count": 1,
                "failed_buildings": [],
                "blocked_buildings": [],
                "last_success_at": "2026-04-15 09:00:02",
                "current_bucket": "2026-04-15 09",
                "buildings": [{"building": "A楼", "status": "ready", "ready": True}],
            },
        },
    }
    return SimpleNamespace(
        version="web-3.0.0",
        config={"version": 3},
        job_service=_FakeJobService(),
        shared_bridge_service=_FakeSharedBridgeService(),
        shared_bridge_snapshot=lambda mode="internal_light": snapshot,
        deployment_snapshot=lambda: {
            "role_mode": role_mode,
            "node_id": "node-1",
            "node_label": "外网端" if role_mode == "external" else "内网端",
        },
    )


def _wait_until(predicate, timeout_sec: float = 3.0) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def test_runtime_status_coordinator_refreshes_sqlite_snapshots(tmp_path: Path) -> None:
    coordinator = RuntimeStatusCoordinator(
        container=_fake_container(),
        runtime_state_root=tmp_path,
        app_state_getter=lambda: {
            "runtime_activated": True,
            "activation_phase": "activated",
            "activation_error": "",
            "startup_role_confirmed": True,
            "started_at": "2026-04-15 09:00:00",
        },
        emit_log=None,
        refresh_interval_sec=60.0,
    )
    coordinator.start()
    try:
        coordinator.request_refresh(reason="test")
        assert _wait_until(lambda: coordinator.read_scope_snapshot("internal_runtime_summary") is not None)
        summary = coordinator.read_scope_snapshot("internal_runtime_summary")
        building = coordinator.read_building_snapshot("A楼")
        jobs = coordinator.read_scope_snapshot("job_panel_dashboard_summary")
        resources = coordinator.read_scope_snapshot("runtime_resources_summary")
        bridge_tasks = coordinator.read_scope_snapshot("bridge_tasks_dashboard_summary")
        health_lite = coordinator.read_scope_snapshot("runtime_health_lite")

        assert summary is not None
        assert summary["payload"]["pool"]["browser_ready"] is True
        assert building is not None
        assert building["payload"]["building"] == "A楼"
        assert jobs is not None
        assert jobs["payload"]["jobs"][0]["job_id"] == "job-active"
        assert resources is not None
        assert resources["payload"]["network"]["current_side"] == "internal"
        assert bridge_tasks is not None
        assert bridge_tasks["payload"]["tasks"] == [
            {"task_id": "bridge-1", "feature": "monthly_report_pipeline", "status": "queued_for_internal"}
        ]
        assert health_lite is not None
        assert health_lite["payload"]["runtime_activated"] is True
    finally:
        coordinator.stop()


def test_runtime_status_coordinator_writes_external_shared_bridge_full_scope(tmp_path: Path) -> None:
    coordinator = RuntimeStatusCoordinator(
        container=_fake_container(role_mode="external"),
        runtime_state_root=tmp_path,
        app_state_getter=lambda: {
            "runtime_activated": True,
            "activation_phase": "activated",
            "activation_error": "",
            "startup_role_confirmed": True,
            "started_at": "2026-04-15 09:00:00",
        },
        emit_log=None,
        refresh_interval_sec=60.0,
    )
    coordinator.start()
    try:
        coordinator.request_refresh(reason="test-external")
        assert _wait_until(lambda: coordinator.read_scope_snapshot("external_shared_bridge_full") is not None)
        snapshot = coordinator.read_scope_snapshot("external_shared_bridge_full")
        assert snapshot is not None
        assert snapshot["payload"]["role_mode"] == "external"
        assert snapshot["payload"]["internal_source_cache"]["handover_log_family"]["ready_count"] == 1
    finally:
        coordinator.stop()


def test_runtime_status_store_read_self_heals_when_db_file_exists_without_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_status.sqlite"
    sqlite3.connect(str(db_path)).close()
    store = RuntimeStatusStore(db_path)

    assert store.read_scope_snapshot("internal_runtime_summary") is None

    written = store.write_scope_snapshot("internal_runtime_summary", {"ok": True})
    assert written["scope"] == "internal_runtime_summary"
    assert store.read_scope_snapshot("internal_runtime_summary")["payload"]["ok"] is True
