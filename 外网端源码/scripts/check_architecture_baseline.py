from __future__ import annotations

import sys
import asyncio
import json
import time
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import httpx
import requests

PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from app.core.app_state import AppStateRepository  # noqa: E402
from app.bootstrap.app_factory import create_app  # noqa: E402
from app.config.config_schema_v3 import DEFAULT_CONFIG_V3  # noqa: E402
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient  # noqa: E402
from app.modules.feishu.service.feishu_token_manager import feishu_token_manager  # noqa: E402
from app.modules.feishu.service.im_file_message_client import FeishuImFileMessageClient  # noqa: E402
from app.modules.feishu.service.sheets_client_runtime import FeishuSheetsClientRuntime  # noqa: E402
from app.modules.report_pipeline.api.routes import _filter_accessible_cached_entries  # noqa: E402
from app.modules.report_pipeline.service.job_service import JobService, JobState, StageState  # noqa: E402
from app.shared.runtime_dependency_spec import normalized_runtime_dependency_specs  # noqa: E402
from app.modules.scheduler.service.apscheduler_orchestrator import (  # noqa: E402
    ApschedulerOrchestrator,
    ApschedulerSchedulerFacade,
)
from app.modules.shared_bridge.api.routes import _alarm_event_upload_waiting_response_if_missing  # noqa: E402
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


def check_alarm_upload_missing_source_returns_waiting_job() -> None:
    class _FakeWaitingJob:
        job_id = "alarm-wait-job-1"

        def to_dict(self) -> dict:
            return {"job_id": self.job_id, "status": "waiting_resource", "feature": "alarm_event_upload"}

    class _FakeAlarmJobService:
        def __init__(self) -> None:
            self.bound: list[tuple[str, str]] = []

        def create_waiting_worker_job(self, **kwargs):  # noqa: ANN001
            _assert(kwargs.get("feature") == "alarm_event_upload", "unexpected waiting job feature")
            _assert(kwargs.get("worker_handler") == "alarm_event_upload", "unexpected waiting job handler")
            return _FakeWaitingJob()

        def bind_bridge_task(self, job_id: str, task_id: str) -> None:
            self.bound.append((job_id, task_id))

        def fail_waiting_job(self, job_id: str, *, error_text: str, summary: str = "") -> None:
            raise AssertionError(f"waiting job should not fail: {job_id} {error_text} {summary}")

    class _FakeAlarmBridgeService:
        def __init__(self) -> None:
            self.created_kwargs: dict = {}

        def get_source_cache_buildings(self) -> list[str]:
            return ["A楼", "B楼"]

        def get_alarm_event_upload_selection(self, building: str = "") -> dict:
            return {
                "selected_entries": [{"building": "A楼", "file_path": r"\\172.16.1.2\share\a.json"}],
                "transport": "http",
                "target_bucket_key": "2026-05-28 11",
            }

        def create_http_bridge_task(self, *, get_or_create_name: str, create_name: str, bridge_kwargs: dict) -> dict:
            self.created_kwargs = {
                "get_or_create_name": get_or_create_name,
                "create_name": create_name,
                "bridge_kwargs": dict(bridge_kwargs or {}),
            }
            return {"task_id": "alarm-http-task-1", "status": "queued"}

    logs: list[str] = []
    bridge_service = _FakeAlarmBridgeService()
    job_service = _FakeAlarmJobService()
    container = SimpleNamespace(
        job_service=job_service,
        add_system_log=lambda text, **_: logs.append(str(text)),
    )
    payload = _alarm_event_upload_waiting_response_if_missing(
        container=container,
        service=bridge_service,
        mode="full",
        building="",
        priority="manual",
        submitted_by="manual",
    )
    _assert(isinstance(payload, dict), "missing alarm source should return waiting payload")
    _assert(payload.get("mode") == "waiting_shared_bridge", "waiting payload mode is wrong")
    _assert(payload.get("missing_buildings") == ["B楼"], "missing building not surfaced")
    _assert(payload.get("job", {}).get("job_id") == "alarm-wait-job-1", "response did not expose waiting job")
    _assert(job_service.bound == [("alarm-wait-job-1", "alarm-http-task-1")], "bridge task not bound to waiting job")
    _assert(
        bridge_service.created_kwargs.get("bridge_kwargs", {}).get("target_bucket_key") == "2026-05-28 11",
        "alarm bridge target bucket was not passed through",
    )
    print("[OK] alarm_upload_missing_source_waiting_job")


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
    _assert(engine_snapshot.get("jobstore") == "sqlalchemy_sqlite", "scheduler jobstore is not persistent SQLite")
    _assert(str(engine_snapshot.get("jobstore_path", "") or "").endswith("apscheduler_jobs.sqlite3"), "scheduler jobstore path is unexpected")
    _assert(int(engine_snapshot.get("job_count", 0) or 0) >= 1, "scheduler engine has no registered jobs")
    container.stop_branch_power_upload_scheduler(source="baseline_check")
    container.shutdown_scheduler_orchestrator(source="baseline_check")
    print("[OK] apscheduler_facade")


def check_runtime_dependencies_include_scheduler_jobstore() -> None:
    spec_pairs = {
        (str(item.get("package", "") or "").strip(), str(item.get("import_name", "") or "").strip())
        for item in normalized_runtime_dependency_specs()
        if isinstance(item, dict)
    }
    _assert(("APScheduler", "apscheduler") in spec_pairs, "APScheduler missing from runtime dependency specs")
    _assert(("SQLAlchemy", "sqlalchemy") in spec_pairs, "SQLAlchemy missing from runtime dependency specs")
    lock_path = PROJECT_DIR / "runtime_dependency_lock.json"
    payload = json.loads(lock_path.read_text(encoding="utf-8"))
    packages = payload.get("packages", []) if isinstance(payload, dict) else []
    lock_pairs = {
        (str(item.get("package", "") or "").strip(), str(item.get("import_name", "") or "").strip())
        for item in packages
        if isinstance(item, dict)
    }
    _assert(("APScheduler", "apscheduler") in lock_pairs, "APScheduler missing from runtime dependency lock")
    _assert(("SQLAlchemy", "sqlalchemy") in lock_pairs, "SQLAlchemy missing from runtime dependency lock")
    print("[OK] runtime_dependencies_scheduler_jobstore")


def check_daily_scheduler_failed_run_schedules_retry() -> None:
    class _FakeOrchestrator:
        timezone = "Asia/Shanghai"

        def __init__(self) -> None:
            self.date_jobs: list[dict] = []
            self.removed_prefixes: list[str] = []
            self.handlers: dict[str, Any] = {}

        def register_fire_handler(self, scheduler_key: str, handler) -> None:  # noqa: ANN001
            self.handlers[str(scheduler_key or "")] = handler

        def remove_jobs_by_prefix(self, prefix: str) -> None:
            self.removed_prefixes.append(str(prefix or ""))

        def add_date_job(self, **kwargs):  # noqa: ANN001
            self.date_jobs.append(dict(kwargs))

        def get_job(self, job_id: str):  # noqa: ANN001
            return SimpleNamespace(next_run_time=None)

    calls: list[str] = []
    fake_orchestrator = _FakeOrchestrator()
    facade = ApschedulerSchedulerFacade(
        scheduler_key="baseline_daily_retry",
        feature="baseline",
        scheduler_cfg={
            "enabled": True,
            "run_time": "00:10:00",
            "check_interval_sec": 30,
            "retry_failed_in_same_period": True,
            "state_file": f".runtime/architecture_baseline_check/baseline_daily_retry_state_{time.time_ns()}.json",
        },
        runtime_state_root=".runtime",
        emit_log=lambda _text: None,
        run_callback=lambda source: (calls.append(str(source)) is None, "submitted retry"),
        is_busy=lambda: False,
        orchestrator=fake_orchestrator,  # type: ignore[arg-type]
        schedule_kind="daily",
        source_name="每日失败重试基线",
    )
    facade._active = True  # noqa: SLF001
    facade.record_external_run(status="failed", source="baseline", detail="first failure")
    period = facade.state.get("last_attempt_period", "")
    _assert(period, "daily failed external run did not record attempt period")
    _assert(len(fake_orchestrator.date_jobs) == 1, "daily failed external run did not schedule retry")
    retry_job = fake_orchestrator.date_jobs[-1]
    _assert(
        retry_job.get("args") == ["baseline_daily_retry", "retry_failed"],
        f"daily retry job args unexpected: {retry_job.get('args')}",
    )
    facade._fire(trigger_source="baseline")  # noqa: SLF001
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline and facade._dispatch_in_progress():  # noqa: SLF001
        time.sleep(0.02)
    _assert(len(calls) == 1, "daily retry trigger did not enter callback")
    _assert(facade.state.get("retry_done_period") == period, "daily retry did not mark retry_done_period")
    retry_count_after_dispatch = len(fake_orchestrator.date_jobs)
    facade.record_external_run(status="failed", source="baseline", detail="second failure")
    _assert(
        len(fake_orchestrator.date_jobs) == retry_count_after_dispatch,
        "daily scheduler scheduled more than one retry in the same period",
    )
    print("[OK] daily_scheduler_failed_run_schedules_retry")


def check_apscheduler_dispatch_nonblocking() -> None:
    logs: list[str] = []
    calls: list[str] = []

    def _emit_log(text: str) -> None:
        logs.append(str(text))

    def _callback(source: str) -> tuple[bool, str]:
        calls.append(str(source))
        time.sleep(0.5)
        return True, "submitted baseline job"

    orchestrator = ApschedulerOrchestrator(emit_log=_emit_log)
    facade = ApschedulerSchedulerFacade(
        scheduler_key="baseline_dispatch_nonblocking",
        feature="baseline",
        scheduler_cfg={
            "enabled": True,
            "interval_minutes": 10,
            "state_file": ".runtime/baseline_dispatch_nonblocking_state.json",
        },
        runtime_state_root=".runtime",
        emit_log=_emit_log,
        run_callback=_callback,
        is_busy=lambda: False,
        orchestrator=orchestrator,
        schedule_kind="interval",
        source_name="调度非阻塞基线",
    )
    facade._active = True  # noqa: SLF001
    start = time.perf_counter()
    facade._fire(trigger_source="baseline")  # noqa: SLF001
    first_ms = int((time.perf_counter() - start) * 1000)
    second_start = time.perf_counter()
    facade._fire(trigger_source="baseline")  # noqa: SLF001
    second_ms = int((time.perf_counter() - second_start) * 1000)
    time.sleep(0.8)
    _assert(first_ms < 100, f"scheduler fire blocked too long: {first_ms}ms")
    _assert(second_ms < 100, f"scheduler busy skip blocked too long: {second_ms}ms")
    _assert(len(calls) == 1, "scheduler dispatch did not dedupe in-flight trigger")
    _assert(any("上一轮调度派发仍在运行" in line for line in logs), "missing dispatch_busy diagnostic log")
    _assert(facade.state.get("last_status") == "submitted", "scheduler dispatch did not record submitted status")

    stopped_calls: list[str] = []

    def _stopped_callback(source: str) -> tuple[bool, str]:
        stopped_calls.append(str(source))
        return True, "should not run"

    stopped_facade = ApschedulerSchedulerFacade(
        scheduler_key="baseline_dispatch_stopped",
        feature="baseline",
        scheduler_cfg={
            "enabled": True,
            "interval_minutes": 10,
            "state_file": ".runtime/baseline_dispatch_stopped_state.json",
        },
        runtime_state_root=".runtime",
        emit_log=_emit_log,
        run_callback=_stopped_callback,
        is_busy=lambda: False,
        orchestrator=orchestrator,
        schedule_kind="interval",
        source_name="停用调度基线",
    )
    stopped_facade._fire(trigger_source="baseline")  # noqa: SLF001
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline and stopped_facade._dispatch_in_progress():  # noqa: SLF001
        time.sleep(0.02)
    _assert(not stopped_calls, "stopped scheduler dispatch entered callback")
    _assert(
        stopped_facade.runtime.get("last_decision") == "skip:stopped",
        "stopped scheduler did not record skip:stopped",
    )
    print("[OK] apscheduler_dispatch_nonblocking")


def check_external_cache_filter_does_not_probe_files() -> None:
    unc_path = r"\\172.16.1.2\share\baseline_missing\missing.xlsx"
    entries = [{"building": "A楼", "file_path": unc_path}]
    filtered = _filter_accessible_cached_entries(entries)
    _assert(len(filtered) == 1, "default source-index filter probed file accessibility")
    filtered_with_probe = _filter_accessible_cached_entries(entries, verify_files=True)
    _assert(len(filtered_with_probe) == 1, "explicit source-index filter probed UNC file accessibility")
    local_missing = PROJECT_DIR / ".runtime" / "architecture_baseline_check" / "missing.xlsx"
    filtered_with_probe = _filter_accessible_cached_entries(
        [{"building": "A楼", "file_path": str(local_missing)}],
        verify_files=True,
    )
    _assert(filtered_with_probe == [], "explicit file accessibility probe did not filter missing path")
    print("[OK] external_cache_filter_no_file_probe")


def check_job_failure_notification_policy() -> None:
    calls: list[tuple[str, float]] = []

    class _FakeResponse:
        def __enter__(self):  # noqa: ANN001
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def read(self) -> bytes:
            return b"ok"

    def _fake_urlopen(request, timeout=0):  # noqa: ANN001
        calls.append((str(getattr(request, "full_url", "") or ""), float(timeout or 0)))
        return _FakeResponse()

    original_urlopen = urllib.request.urlopen
    urllib.request.urlopen = _fake_urlopen  # type: ignore[assignment]
    try:
        service = JobService()
        service.set_global_log_sink(lambda _text: None)
        service._runtime_config = {  # noqa: SLF001
            "notify": {
                "task_failure_webhook_enabled": True,
                "task_failure_webhook_url": "https://example.invalid/baseline-webhook",
                "task_failure_webhook_timeout": 1,
            }
        }
        cases = {
            "success": JobState(job_id="success", name="成功任务", status="success", result={"ok": True, "failed": 0}),
            "cancelled": JobState(job_id="cancelled", name="取消任务", status="cancelled"),
            "failed": JobState(job_id="failed", name="失败任务", status="failed", error="boom"),
            "partial": JobState(
                job_id="partial",
                name="部分失败任务",
                status="success",
                result={"status": "partial_failed", "error": "partial"},
            ),
            "count_failed": JobState(
                job_id="count_failed",
                name="失败计数任务",
                status="success",
                result={"failed_count": 1},
            ),
            "interrupted": JobState(job_id="interrupted", name="中断任务", status="interrupted", error="worker stopped"),
            "blocked": JobState(
                job_id="blocked_precondition",
                name="前置条件阻塞任务",
                status="blocked_precondition",
                error="missing source",
            ),
            "stage_failed": JobState(
                job_id="stage_failed",
                name="阶段失败任务",
                status="success",
                stages=[StageState(stage_id="main", name="main", status="failed", error="stage boom")],
            ),
        }
        for job in cases.values():
            service._notify_job_failure_to_feishu_async(job)  # noqa: SLF001
            service._notify_job_failure_to_feishu_async(job)  # noqa: SLF001
        time.sleep(0.5)
    finally:
        urllib.request.urlopen = original_urlopen  # type: ignore[assignment]
    _assert(len(calls) == 6, f"unexpected failure notification calls: {len(calls)}")
    _assert(not cases["success"].failure_notified_at, "success job should not notify")
    _assert(not cases["cancelled"].failure_notified_at, "cancelled job should not notify")
    for key in ("failed", "partial", "count_failed", "interrupted", "blocked", "stage_failed"):
        _assert(bool(cases[key].failure_notified_at), f"{key} job should notify once")

    failed_send_calls: list[str] = []

    def _fake_failed_urlopen(request, timeout=0):  # noqa: ANN001
        failed_send_calls.append(str(getattr(request, "full_url", "") or ""))
        raise OSError("temporary webhook outage")

    failing_job = JobState(job_id="webhook_failed", name="机器人失败任务", status="failed", error="boom")
    original_urlopen = urllib.request.urlopen
    urllib.request.urlopen = _fake_failed_urlopen  # type: ignore[assignment]
    try:
        service = JobService()
        service.set_global_log_sink(lambda _text: None)
        service._runtime_config = {  # noqa: SLF001
            "notify": {
                "task_failure_webhook_enabled": True,
                "task_failure_webhook_url": "https://example.invalid/baseline-webhook",
                "task_failure_webhook_timeout": 1,
            }
        }
        service._notify_job_failure_to_feishu_async(failing_job)  # noqa: SLF001
        time.sleep(1.8)
    finally:
        urllib.request.urlopen = original_urlopen  # type: ignore[assignment]
    _assert(len(failed_send_calls) == 3, "failed webhook send should retry three times")
    _assert(not failing_job.failure_notified_at, "failed webhook send must not mark job as notified")
    print("[OK] job_failure_notification_policy")


def check_no_task_success_group_notification_runtime() -> None:
    job_service_text = (PROJECT_DIR / "app" / "modules" / "report_pipeline" / "service" / "job_service.py").read_text(
        encoding="utf-8",
        errors="ignore",
    )
    forbidden = [
        "_notify_job_success",
        "_job_success_notify",
        "全景助手控制台任务完成",
    ]
    hits = [item for item in forbidden if item in job_service_text]
    _assert(not hits, "job service still contains task success group notification runtime: " + ", ".join(hits))
    _assert("_notify_job_failure_to_feishu_async" in job_service_text, "job failure notification runtime missing")
    print("[OK] no_task_success_group_notification_runtime")


def check_scheduler_callbacks_do_not_wait_for_jobs() -> None:
    paths = [
        PROJECT_DIR / "app" / "bootstrap" / "app_factory.py",
        PROJECT_DIR / "app" / "bootstrap" / "container.py",
        PROJECT_DIR / "app" / "modules" / "scheduler" / "service" / "apscheduler_orchestrator.py",
    ]
    wait_hits: list[str] = []
    blocking_patterns = ("wait_job(", ".result(", "thread.join(")
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern in blocking_patterns:
            if pattern in text:
                wait_hits.append(f"{path.relative_to(PROJECT_DIR)}:{pattern}")
    _assert(not wait_hits, "scheduler path contains blocking job wait patterns: " + ", ".join(wait_hits))
    app_factory_text = paths[0].read_text(encoding="utf-8", errors="ignore")
    for callback_name in (
        "scheduler_callback",
        "handover_scheduler_callback",
        "wet_bulb_collection_scheduler_callback",
        "chiller_mode_upload_scheduler_callback",
        "day_metric_upload_scheduler_callback",
        "branch_power_upload_scheduler_callback",
        "alarm_event_upload_scheduler_callback",
        "monthly_event_report_scheduler_callback",
        "monthly_change_report_scheduler_callback",
    ):
        _assert(f"def {callback_name}" in app_factory_text, f"scheduler callback missing: {callback_name}")
    print("[OK] scheduler_callbacks_do_not_wait_for_jobs")


def check_feishu_invalid_token_refresh_retry() -> None:
    class _FakeResponse:
        def __init__(self, body: dict, status_code: int = 200) -> None:
            self._body = body
            self.status_code = status_code
            self.url = "https://open.feishu.cn/open-apis/baseline"
            self.text = str(body)

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise requests.HTTPError(response=self)

        def json(self) -> dict:
            return dict(self._body)

    def _exercise(label: str, client_factory, request_call, expected_token_calls: list[bool]) -> None:  # noqa: ANN001
        token_calls: list[bool] = []
        invalidated: list[bool] = []
        request_tokens: list[str] = []

        def _fake_get_token(*, app_id: str, app_secret: str, timeout: int, force_refresh: bool = False) -> str:
            token_calls.append(bool(force_refresh))
            return "fresh-token" if force_refresh else "stale-token"

        def _fake_invalidate(*, app_id: str, app_secret: str) -> None:
            invalidated.append(True)

        def _fake_request(method: str, url: str, **kwargs):  # noqa: ANN001
            headers = kwargs.get("headers", {}) if isinstance(kwargs.get("headers", {}), dict) else {}
            request_tokens.append(str(headers.get("Authorization", "") or ""))
            if len(request_tokens) == 1:
                return _FakeResponse({"code": 99991663, "msg": "Invalid access token for authorization."})
            return _FakeResponse({"code": 0, "data": {"ok": True}})

        original_get_token = feishu_token_manager.get_token
        original_invalidate = feishu_token_manager.invalidate
        original_request = requests.request
        feishu_token_manager.get_token = _fake_get_token  # type: ignore[method-assign]
        feishu_token_manager.invalidate = _fake_invalidate  # type: ignore[method-assign]
        requests.request = _fake_request  # type: ignore[assignment]
        try:
            client = client_factory()
            result = request_call(client)
        finally:
            feishu_token_manager.get_token = original_get_token  # type: ignore[method-assign]
            feishu_token_manager.invalidate = original_invalidate  # type: ignore[method-assign]
            requests.request = original_request  # type: ignore[assignment]

        _assert(result.get("code") == 0, f"{label} client did not recover after invalid token")
        _assert(invalidated, f"{label} invalid token did not invalidate shared token cache")
        _assert(token_calls == expected_token_calls, f"{label} unexpected token refresh sequence: {token_calls}")
        _assert(
            request_tokens == ["Bearer stale-token", "Bearer fresh-token"],
            f"{label} unexpected request auth tokens: {request_tokens}",
        )

    _exercise(
        "bitable",
        lambda: FeishuBitableClient(
            app_id="cli_a",
            app_secret="secret",
            app_token="base",
            calc_table_id="tbl",
            attachment_table_id="tbl_attach",
            timeout=1,
            request_retry_count=0,
            request_retry_interval_sec=0,
            date_text_to_timestamp_ms_fn=lambda **_: 0,
            canonical_metric_name_fn=lambda value: str(value),
            dimension_mapping={},
        ),
        lambda client: client._request_json_with_auth_retry(  # noqa: SLF001
            "GET",
            "https://open.feishu.cn/open-apis/baseline",
        ),
        [False, True, True],
    )
    _exercise(
        "sheets",
        lambda: FeishuSheetsClientRuntime(
            app_id="cli_a",
            app_secret="secret",
            timeout=1,
            request_retry_count=0,
            request_retry_interval_sec=0,
        ),
        lambda client: client._request_json_with_auth_retry(  # noqa: SLF001
            "GET",
            "https://open.feishu.cn/open-apis/baseline",
        ),
        [False, True, True],
    )
    _exercise(
        "im",
        lambda: FeishuImFileMessageClient(
            app_id="cli_a",
            app_secret="secret",
            timeout=1,
            request_retry_count=0,
            request_retry_interval_sec=0,
        ),
        lambda client: client._request_json_with_auth_retry(  # noqa: SLF001
            "POST",
            "https://open.feishu.cn/open-apis/baseline",
            payload={"text": "baseline"},
            content_type_json=True,
        ),
        [False, True],
    )
    print("[OK] feishu_invalid_token_refresh_retry")


def check_no_legacy_lark_cli_runtime() -> None:
    roots = [PROJECT_DIR / "app", PROJECT_DIR / "handover_log_module"]
    legacy_hits: list[str] = []
    for root in roots:
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "lark-cli" in text or "lark_cli" in text or "LarkCli" in text:
                legacy_hits.append(str(path.relative_to(PROJECT_DIR)))
    _assert(not legacy_hits, "legacy lark-cli runtime references remain: " + ", ".join(legacy_hits[:8]))

    auth_endpoint_hits: list[str] = []
    for root in roots:
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "auth/v3/tenant_access_token/internal" in text and path.name != "feishu_token_manager.py":
                auth_endpoint_hits.append(str(path.relative_to(PROJECT_DIR)))
    _assert(not auth_endpoint_hits, "Feishu auth endpoint is used outside FeishuTokenManager: " + ", ".join(auth_endpoint_hits[:8]))
    print("[OK] no_legacy_lark_cli_runtime")


def check_legacy_shared_bridge_store_removed() -> None:
    legacy_paths = [
        PROJECT_DIR / "app" / "modules" / "shared_bridge" / "service" / "shared_bridge_store.py",
        PROJECT_DIR / "app" / "modules" / "shared_bridge" / "service" / "shared_source_cache_index_store.py",
        PROJECT_DIR / "app" / "modules" / "shared_bridge" / "service" / "alarm_external_selection.py",
    ]
    remaining = [str(path.relative_to(PROJECT_DIR)) for path in legacy_paths if path.exists()]
    _assert(not remaining, "legacy shared bridge store modules remain: " + ", ".join(remaining))

    roots = [PROJECT_DIR / "app", PROJECT_DIR / "handover_log_module"]
    import_hits: list[str] = []
    for root in roots:
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "shared_bridge_store" in text or "SharedBridgeStore" in text or "shared_source_cache_index_store" in text:
                import_hits.append(str(path.relative_to(PROJECT_DIR)))
    _assert(not import_hits, "legacy shared bridge store imports remain: " + ", ".join(import_hits[:8]))
    print("[OK] legacy_shared_bridge_store_removed")


def check_internal_bridge_http_config_preserved() -> None:
    default_cfg = DEFAULT_CONFIG_V3.get("common", {}).get("internal_bridge_http", {})
    _assert(isinstance(default_cfg, dict), "internal_bridge_http defaults missing")
    _assert(int(default_cfg.get("connect_timeout_sec", 0) or 0) <= 3, "internal bridge connect timeout default is too high")
    _assert(int(default_cfg.get("read_timeout_sec", 0) or 0) <= 5, "internal bridge read timeout default is too high")
    for path in (
        PROJECT_DIR / "config" / "表格计算配置.template.json",
        PROJECT_DIR / "表格计算配置.json",
    ):
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        common = payload.get("common", {}) if isinstance(payload, dict) else {}
        cfg = common.get("internal_bridge_http", {}) if isinstance(common, dict) else {}
        _assert(isinstance(cfg, dict) and cfg, f"internal_bridge_http missing from {path.name}")
        _assert(int(cfg.get("read_timeout_sec", 0) or 0) <= 5, f"internal bridge read timeout too high in {path.name}")
    for path in (
        PROJECT_DIR / "web" / "frontend" / "src" / "config_runtime_convert.js",
        PROJECT_DIR / "web" / "frontend" / "src" / "config_runtime_defaults.js",
        PROJECT_DIR / "web" / "frontend" / "src" / "config_save_validation.js",
    ):
        text = path.read_text(encoding="utf-8")
        _assert("internal_bridge_http" in text, f"frontend config does not preserve internal_bridge_http: {path.name}")
    print("[OK] internal_bridge_http_config_preserved")


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

    class _EmptyHttpTaskClient:
        base_url = "http://127.0.0.1:18765"

        def list_tasks(self, *, status: str = "", limit: int = 100) -> list[dict]:
            return []

        def cancel_task(self, task_id: str) -> bool:
            return bool(str(task_id or "").strip())

    cached_task_service = SharedBridgeRuntimeService(runtime_config=runtime_config, app_version="baseline")
    cached_task_service._internal_bridge_http_client = _EmptyHttpTaskClient()  # type: ignore[assignment]
    cached_task_service._cached_task_list = [{"task_id": "stale-active", "status": "internal_running", "transport": "http"}]  # noqa: SLF001
    _assert(cached_task_service.list_active_tasks() == [], "HTTP-only active task list fell back to stale cache")
    _assert(cached_task_service._cached_task_list == [], "HTTP-only empty task list did not clear stale cache")  # noqa: SLF001

    cancel_service = SharedBridgeRuntimeService(runtime_config=runtime_config, app_version="baseline")
    cancel_service._internal_bridge_http_client = _EmptyHttpTaskClient()  # type: ignore[assignment]
    cancel_service._cached_task_list = [{"task_id": "cancel-me", "status": "internal_running", "transport": "http"}]  # noqa: SLF001
    cancel_service._cached_task_details = {"cancel-me": {"task_id": "cancel-me", "status": "internal_running", "transport": "http"}}  # noqa: SLF001
    _assert(cancel_service.cancel_task("cancel-me") is True, "HTTP task cancel did not return success")
    _assert(
        cancel_service._cached_task_details["cancel-me"]["status"] == "cancelled",  # noqa: SLF001
        "HTTP task cancel did not mark cached task cancelled",
    )
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
    check_alarm_upload_missing_source_returns_waiting_job()
    check_apscheduler_facade()
    check_runtime_dependencies_include_scheduler_jobstore()
    check_daily_scheduler_failed_run_schedules_retry()
    check_apscheduler_dispatch_nonblocking()
    check_external_cache_filter_does_not_probe_files()
    check_job_failure_notification_policy()
    check_no_task_success_group_notification_runtime()
    check_scheduler_callbacks_do_not_wait_for_jobs()
    check_feishu_invalid_token_refresh_retry()
    check_no_legacy_lark_cli_runtime()
    check_legacy_shared_bridge_store_removed()
    check_internal_bridge_http_config_preserved()
    check_legacy_scheduler_modules_removed()
    check_http_only_shared_bridge()
    check_lightweight_routes()
    print("[OK] architecture baseline checks completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
