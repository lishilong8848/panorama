from __future__ import annotations

import sys
import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx

PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from app.core.app_state import AppStateRepository  # noqa: E402
from app.bootstrap.app_factory import create_app  # noqa: E402
from app.modules.scheduler.service.apscheduler_orchestrator import ApschedulerOrchestrator  # noqa: E402
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService  # noqa: E402
from app.modules.tasks.api.routes import cancel_task  # noqa: E402
from handover_log_module.service.review_session_service import ReviewSessionService  # noqa: E402


LEGACY_SCHEDULER_MODULES = (
    "daily_scheduler_service.py",
    "interval_scheduler_service.py",
    "monthly_scheduler_service.py",
    "handover_scheduler_manager.py",
)


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def check_app_state_repository() -> None:
    runtime_root = PROJECT_DIR / ".runtime" / "architecture_baseline_check"
    repository = AppStateRepository(
        runtime_config={"paths": {"runtime_state_root": str(runtime_root)}},
        app_dir=PROJECT_DIR,
    )
    snapshot = repository.snapshot()
    _assert(snapshot.get("ready") is True, "app_state repository is not ready")
    _assert(Path(str(snapshot.get("db_path", ""))).name == "app_state.sqlite3", "unexpected db path")
    table_counts = snapshot.get("table_counts", {})
    _assert(isinstance(table_counts, dict) and "generated_files" in table_counts, "missing generated_files table")
    _assert("runtime_kv" in table_counts, "missing runtime_kv table")
    _assert("config_write_queue" in table_counts, "missing config_write_queue table")
    repository.put_runtime_kv("baseline", "health", {"ok": True})
    _assert(repository.get_runtime_kv("baseline", "health") == {"ok": True}, "runtime_kv roundtrip failed")
    print("[OK] app_state_repository")


def check_generated_file_history_index() -> None:
    runtime_root = PROJECT_DIR / ".runtime" / "architecture_baseline_check"
    cfg = {
        "review_ui": {"enabled_buildings": ["A楼", "B楼", "C楼", "D楼", "E楼"]},
        "_global_paths": {"runtime_state_root": str(runtime_root)},
    }
    repository = AppStateRepository(
        runtime_config={"paths": cfg["_global_paths"]},
        app_dir=PROJECT_DIR,
    )
    repository.ensure_ready()
    service = ReviewSessionService(cfg)
    service.configure_generated_file_index(repository)

    service.register_generated_output(
        building="A楼",
        duty_date="2026-05-21",
        duty_shift="day",
        data_file=str(runtime_root / "A_source.xlsx"),
        output_file=str(runtime_root / "A_log.xlsx"),
        capacity_output_file=str(runtime_root / "A_capacity.xlsx"),
        source_mode="baseline_check",
    )
    rows = service.list_building_generated_file_history_sessions_fast(
        "A楼",
        limit=10,
        duty_date="2026-05-21",
    )
    _assert(rows, "history index returned no rows")
    first = rows[0]
    _assert(str(first.get("session_id", "")).strip() == "A楼|2026-05-21|day", "unexpected session id")
    _assert(str(first.get("output_file", "")).endswith("A_log.xlsx"), "handover output file not indexed")
    _assert(str(first.get("capacity_output_file", "")).endswith("A_capacity.xlsx"), "capacity output file not indexed")
    print("[OK] generated_file_history_index")


class _FakeJobService:
    def __init__(self) -> None:
        self.cancelled = False
        self._job = {
            "job_id": "job-1",
            "status": "waiting_resource",
            "feature": "baseline",
            "name": "baseline",
            "bridge_task_id": "bridge-1",
            "wait_reason": "waiting:shared_bridge",
        }

    def get_job(self, job_id: str) -> dict:
        _assert(job_id == "job-1", "unexpected job id")
        return dict(self._job)

    def cancel_job(self, job_id: str) -> dict:
        _assert(job_id == "job-1", "unexpected cancel job id")
        self.cancelled = True
        payload = dict(self._job)
        payload["status"] = "cancelled"
        return payload

    def list_jobs(self, *, limit: int, statuses=None) -> list[dict]:  # noqa: ANN001
        return []

    def active_job_ids(self, *, include_waiting: bool = False) -> list[str]:
        return []

    def job_counts(self) -> dict:
        return {}


class _FakeBridgeService:
    def __init__(self) -> None:
        self.cancelled_task_ids: list[str] = []

    def cancel_task(self, task_id: str) -> bool:
        self.cancelled_task_ids.append(task_id)
        return True

    def list_active_tasks(self, *, limit: int) -> list[dict]:
        return []


class _FakeRuntimeStatusCoordinator:
    def __init__(self) -> None:
        self.refresh_count = 0

    def refresh_now(self) -> None:
        self.refresh_count += 1


def check_task_cancel_bridge_binding() -> None:
    job_service = _FakeJobService()
    bridge_service = _FakeBridgeService()
    coordinator = _FakeRuntimeStatusCoordinator()
    logs: list[str] = []
    container = SimpleNamespace(
        job_service=job_service,
        shared_bridge_service=bridge_service,
        runtime_status_coordinator=coordinator,
        add_system_log=lambda text, **_: logs.append(str(text)),
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))
    result = cancel_task("job-1", request)  # type: ignore[arg-type]
    _assert(result.get("ok") is True, "cancel endpoint did not return ok")
    _assert(job_service.cancelled, "job was not cancelled")
    _assert(bridge_service.cancelled_task_ids == ["bridge-1"], "bridge task was not cancelled")
    _assert(coordinator.refresh_count == 1, "runtime status was not refreshed")
    print("[OK] task_cancel_bridge_binding")


def check_apscheduler_facade() -> None:
    app = create_app(enable_lifespan=False)
    container = app.state.container
    container._ensure_runtime_dependencies_initialized()
    _assert(
        isinstance(container.ensure_scheduler_orchestrator(), ApschedulerOrchestrator),
        "container did not create APScheduler orchestrator",
    )
    result = container.start_branch_power_upload_scheduler(source="baseline_check")
    _assert(result.get("running") is True, "branch scheduler did not start")
    snapshot = container.branch_power_upload_scheduler_status()
    _assert(snapshot.get("running") is True, "branch scheduler status is not running")
    _assert(str(snapshot.get("next_run_time", "")).endswith("00:30:00"), "branch scheduler next_run_time is not 00:30")
    engine_snapshot = container.scheduler_engine_snapshot()
    _assert(engine_snapshot.get("engine") == "APScheduler", "scheduler engine is not APScheduler")
    _assert(engine_snapshot.get("ready") is True, "scheduler engine snapshot is not ready")
    _assert(int(engine_snapshot.get("job_count", 0) or 0) >= 1, "scheduler engine has no registered jobs")
    container.stop_branch_power_upload_scheduler(source="baseline_check")
    container.shutdown_scheduler_orchestrator(source="baseline_check")
    print("[OK] apscheduler_facade")


def check_legacy_scheduler_modules_removed() -> None:
    scheduler_service_dir = PROJECT_DIR / "app" / "modules" / "scheduler" / "service"
    remaining = [name for name in LEGACY_SCHEDULER_MODULES if (scheduler_service_dir / name).exists()]
    _assert(not remaining, f"legacy scheduler modules still exist: {', '.join(remaining)}")
    print("[OK] legacy_scheduler_modules_removed")


def check_http_only_shared_bridge() -> None:
    runtime_config = {
        "deployment": {"role_mode": "external", "node_id": "baseline-external"},
        "shared_bridge": {
            "enabled": True,
            "root_dir": r"\\172.16.1.2\share",
            "bridge_mode": "shared_db",
        },
        "internal_bridge_http": {
            "enabled": True,
            "port": 18765,
            "connect_timeout_sec": 1,
            "read_timeout_sec": 1,
        },
    }
    service = SharedBridgeRuntimeService(runtime_config=runtime_config, app_version="baseline")
    _assert(service._bridge_mode == "http", "external bridge mode was not forced to http")
    _assert(service._store is None, "external HTTP bridge initialized shared bridge store")
    _assert(service._internal_bridge_http_client is not None, "external HTTP bridge client was not initialized")
    service._http_source_index_entries = lambda **_: []  # type: ignore[method-assign]
    diagnostic = service.diagnose_shared_root(initialize=True)
    _assert(str(diagnostic.get("db_path", "") or "") == "", "HTTP diagnostic still reports bridge_db path")
    directory_keys = {str(row.get("key", "") or "") for row in diagnostic.get("directories", []) if isinstance(row, dict)}
    _assert("bridge_db" not in directory_keys, "HTTP diagnostic still exposes bridge_db")
    _assert("artifacts" not in directory_keys, "HTTP diagnostic still exposes shared task artifacts")

    class _ExplodingSourceCache:
        def get_latest_ready_entries(self, **_: object) -> list[dict]:
            raise AssertionError("external HTTP bridge fell back to local source cache")

        def get_external_source_cache_overview_fast(self) -> dict:
            raise AssertionError("external HTTP bridge fell back to local source cache overview")

        def get_latest_ready_selection(self, **_: object) -> dict:
            raise AssertionError("external HTTP bridge fell back to local source cache selection")

        def get_handover_by_date_entries(self, **_: object) -> list[dict]:
            raise AssertionError("external HTTP bridge fell back to handover source cache")

        def get_handover_capacity_by_date_entries(self, **_: object) -> list[dict]:
            raise AssertionError("external HTTP bridge fell back to capacity source cache")

        def get_day_metric_by_date_entries(self, **_: object) -> list[dict]:
            raise AssertionError("external HTTP bridge fell back to day metric source cache")

        def get_monthly_by_date_entries(self, **_: object) -> list[dict]:
            raise AssertionError("external HTTP bridge fell back to monthly source cache")

        def get_alarm_event_upload_selection(self, **_: object) -> dict:
            raise AssertionError("external HTTP bridge fell back to alarm source cache")

    no_client_config = {
        "deployment": {"role_mode": "external", "node_id": "baseline-external"},
        "shared_bridge": {"enabled": True, "root_dir": r"D:\share", "bridge_mode": "shared_db"},
        "internal_bridge_http": {"enabled": True, "base_url": "", "port": 18765},
    }
    no_client_service = SharedBridgeRuntimeService(runtime_config=no_client_config, app_version="baseline")
    _assert(no_client_service._bridge_mode == "http", "external no-client bridge mode was not forced to http")
    no_client_service._source_cache_service = _ExplodingSourceCache()  # type: ignore[assignment]
    _assert(no_client_service.get_latest_source_cache_entries(source_family="handover_log_family") == [], "HTTP-only latest entries did not fail closed")
    _assert(no_client_service.get_handover_by_date_cache_entries(duty_date="2026-05-21", duty_shift="day") == [], "HTTP-only handover date entries did not fail closed")
    _assert(no_client_service.get_handover_capacity_by_date_cache_entries(duty_date="2026-05-21", duty_shift="day") == [], "HTTP-only capacity date entries did not fail closed")
    _assert(no_client_service.get_day_metric_by_date_cache_entries(selected_dates=["2026-05-21"], buildings=["A楼"]) == [], "HTTP-only day metric entries did not fail closed")
    _assert(no_client_service.get_monthly_by_date_cache_entries(selected_dates=["2026-05-21"], buildings=["A楼"]) == [], "HTTP-only monthly entries did not fail closed")
    _assert(no_client_service.get_alarm_event_upload_selection().get("transport") == "http", "HTTP-only alarm selection did not fail closed")
    print("[OK] http_only_shared_bridge")


async def _check_lightweight_routes_async() -> None:
    app = create_app(enable_lifespan=False)
    app.state.runtime_services_activated = True
    app.state.startup_role_confirmed = True
    app.state.container._ensure_runtime_dependencies_initialized()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as client:
        task_state = await client.get("/api/tasks/state")
        _assert(task_state.status_code == 200, f"unexpected task state status: {task_state.status_code}")
        task_payload = task_state.json()
        _assert(task_payload.get("ready") is True, "task state did not expose ready app_state")
        _assert(isinstance(task_payload.get("table_counts"), dict), "task state did not expose table counts")

        capabilities = await client.get("/api/tasks/capabilities")
        _assert(capabilities.status_code == 200, f"unexpected task capabilities status: {capabilities.status_code}")
        capabilities_payload = capabilities.json()
        submit_actions = capabilities_payload.get("submit_actions", [])
        _assert("worker" in submit_actions, "task capabilities missing worker action")
        _assert("branch_power_daily" in submit_actions, "task capabilities missing branch_power_daily action")

        health = await client.get("/api/runtime/health-lite")
        if health.status_code == 200:
            health_payload = health.json()
            scheduler_engine = health_payload.get("scheduler_engine", {})
            _assert(isinstance(scheduler_engine, dict), "health-lite did not expose scheduler_engine")

        scheduler_status_routes = (
            "/api/scheduler/branch-power-upload/status",
            "/api/scheduler/chiller-mode-upload/status",
            "/api/scheduler/wet-bulb-collection/status",
            "/api/scheduler/day-metric-upload/status",
            "/api/scheduler/alarm-event-upload/status",
            "/api/scheduler/handover/status",
            "/api/scheduler/monthly-change-report/status",
            "/api/scheduler/monthly-event-report/status",
        )
        for path in scheduler_status_routes:
            response = await client.get(path)
            _assert(response.status_code == 200, f"unexpected scheduler status for {path}: {response.status_code}")
            payload = response.json()
            _assert(isinstance(payload, dict), f"scheduler status for {path} is not an object")

        snapshot = await client.get(
            "/api/handover/review/a/snapshot",
            params={"duty_date": "2026-05-21", "duty_shift": "day"},
        )
        _assert(snapshot.status_code == 200, f"unexpected review snapshot status: {snapshot.status_code}")
        snapshot_payload = snapshot.json()
        _assert(snapshot_payload.get("ok") is True, "review snapshot did not return ok")
        _assert(snapshot_payload.get("document_omitted") is True, "review snapshot loaded document content")
    print("[OK] lightweight_routes")


def check_lightweight_routes() -> None:
    asyncio.run(_check_lightweight_routes_async())


def main() -> int:
    check_app_state_repository()
    check_generated_file_history_index()
    check_task_cancel_bridge_binding()
    check_apscheduler_facade()
    check_legacy_scheduler_modules_removed()
    check_http_only_shared_bridge()
    check_lightweight_routes()
    print("[OK] architecture baseline checks completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
