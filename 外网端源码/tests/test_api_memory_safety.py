from __future__ import annotations

import json
import threading
from types import SimpleNamespace

from app.shared.utils import cached_json_file
from app.bootstrap.app_factory import _exception_contains_memory_error
from app.modules.handover_review.api.routes import _prune_review_cache_locked
from app.modules.report_pipeline.service.job_panel_presenter import (
    build_bridge_tasks_summary,
    present_job_item,
)
from app.modules.report_pipeline.service.job_service import JobService, JobState
from app.modules.report_pipeline.service.task_engine_database import TaskEngineDatabase
from app.modules.shared_bridge.service.runtime_status_coordinator import RuntimeStatusCoordinator


def _job_payload(*, result):
    return {
        "job_id": "job-1",
        "name": "large job",
        "feature": "test",
        "status": "success",
        "created_at": "2026-08-05 10:00:00",
        "result": result,
        "stages": [
            {
                "stage_id": "main",
                "name": "main",
                "status": "success",
                "worker_handler": "test_handler",
                "result": result,
            }
        ],
    }


def test_job_list_presentation_omits_large_results_but_detail_keeps_them():
    large_result = {"rows": [{"value": "x" * 1000} for _ in range(1000)], "uploaded_count": 1000}

    compact = present_job_item(_job_payload(result=large_result), compact=True)
    detail = present_job_item(_job_payload(result=large_result))

    assert compact["result"] is None
    assert compact["result_available"] is True
    assert compact["stages"][0].get("result") is None
    assert len(json.dumps(compact, ensure_ascii=False)) < 20_000
    assert detail["result"] is large_result
    assert detail["stages"][0]["result"] is large_result


def test_bridge_task_summary_bounds_events_and_artifacts():
    task = {
        "task_id": "bridge-1",
        "feature": "monthly_report_pipeline",
        "status": "internal_running",
        "created_at": "2026-08-05 10:00:00",
        "events": [
            {
                "event_id": index,
                "event_type": "log",
                "level": "info",
                "payload": {"message": "x" * 10_000},
            }
            for index in range(100)
        ],
        "artifacts": [
            {"artifact_id": str(index), "status": "ready", "metadata": "x" * 10_000}
            for index in range(100)
        ],
    }

    summary = build_bridge_tasks_summary([task])
    row = summary["tasks"][0]

    assert row["payload_truncated"] is True
    assert len(row["events"]) == 20
    assert len(row["artifacts"]) == 20
    assert len(json.dumps(summary, ensure_ascii=False)) < 500_000


def test_runtime_status_bridge_snapshot_is_compact_and_honors_limit():
    tasks = [
        {
            "task_id": f"bridge-{index}",
            "feature": "monthly_report_pipeline",
            "status": "internal_running",
            "request": {"rows": ["x" * 10_000 for _ in range(20)]},
            "events": [
                {"event_id": event, "payload": {"message": "x" * 10_000}}
                for event in range(30)
            ],
        }
        for index in range(20)
    ]

    class _BridgeService:
        def list_active_tasks(self, *, limit):
            return tasks[:limit]

        def list_recent_tasks(self, *, limit):
            return tasks[:limit]

    coordinator = RuntimeStatusCoordinator.__new__(RuntimeStatusCoordinator)
    coordinator._container = SimpleNamespace(shared_bridge_service=_BridgeService())
    coordinator._emit_log = None

    summary = coordinator._build_bridge_tasks_summary_with_limit(limit=12)

    assert summary["count"] == 20
    assert len(summary["tasks"]) == 12
    assert summary["payload_compact"] is True
    assert all("request" not in item for item in summary["tasks"])
    assert len(json.dumps(summary, ensure_ascii=False)) < 500_000


def test_runtime_status_ignores_legacy_unbounded_task_snapshot():
    coordinator = RuntimeStatusCoordinator.__new__(RuntimeStatusCoordinator)
    coordinator._snapshot_cache_lock = threading.Lock()
    coordinator._scope_cache = {}
    coordinator._store = SimpleNamespace(
        read_scope_snapshot=lambda scope: {
            "scope": scope,
            "payload": {"tasks": [{"request": {"rows": ["x" * 1000]}}]},
        }
    )

    assert coordinator.read_scope_snapshot("bridge_tasks_dashboard_summary") is None


def test_task_database_list_can_skip_result_json(tmp_path):
    database = TaskEngineDatabase(
        runtime_config={"paths": {"runtime_state_root": str(tmp_path)}},
        app_dir=tmp_path,
    )
    large_result = {"rows": [{"value": "x" * 1000} for _ in range(100)]}
    try:
        database.upsert_job(_job_payload(result=large_result))
        database.upsert_stage("job-1", _job_payload(result=large_result)["stages"][0])

        compact = database.list_jobs(limit=10, include_results=False)[0]
        detail = database.get_job("job-1")

        assert compact["result"] is None
        assert compact["stages"][0]["result"] is None
        assert detail is not None
        assert len(detail["result"]["rows"]) == 100
    finally:
        database.close()


def test_task_database_cleanup_records_completion_time(tmp_path):
    database = TaskEngineDatabase(
        runtime_config={"paths": {"runtime_state_root": str(tmp_path)}},
        app_dir=tmp_path,
    )
    try:
        database.upsert_job(_job_payload(result={}))

        assert database.cleanup_terminal_jobs(retention_days=1) == 1
        assert database.runtime_snapshot()["last_cleanup_at"]
    finally:
        database.close()


def test_review_cache_pruning_removes_expired_and_oldest_entries():
    cache = {
        "expired": {"updated_at": 1.0},
        "old": {"updated_at": 99.0},
        "new": {"updated_at": 100.0},
    }

    _prune_review_cache_locked(cache, now=100.0, ttl_sec=10.0, max_entries=1)

    assert list(cache) == ["new"]


def test_memory_error_detection_handles_exception_groups():
    nested = ExceptionGroup(
        "outer",
        [RuntimeError("normal"), ExceptionGroup("inner", [MemoryError()])],
    )

    assert _exception_contains_memory_error(nested) is True
    assert _exception_contains_memory_error(ExceptionGroup("normal", [RuntimeError("x")])) is False


def test_job_service_prunes_only_old_terminal_jobs_from_memory():
    service = JobService()
    service._task_engine_db = object()  # type: ignore[assignment]
    for index in range(40):
        job = JobState(
            job_id=f"job-{index}",
            name=f"job {index}",
            status="success",
            sequence=index,
            result={"value": index},
        )
        job.done_event.set()
        job.thread = threading.current_thread()
        service._jobs[job.job_id] = job
    active = JobState(job_id="active", name="active", status="running", sequence=100)
    service._jobs[active.job_id] = active

    removed = service._prune_terminal_jobs_in_memory()

    assert removed == 8
    assert "active" in service._jobs
    assert "job-0" not in service._jobs
    assert "job-39" in service._jobs


def test_job_service_stops_workers_before_closing_task_database():
    events = []

    class _Process:
        pid = 123
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            events.append("worker_waited")
            return int(self.returncode or 0)

    class _Database:
        def close(self):
            events.append("database_closed")

    process = _Process()
    service = JobService()
    service._worker_processes[("job-1", "main")] = process  # type: ignore[assignment]
    service._task_engine_db = _Database()  # type: ignore[assignment]

    def _cancel(**_kwargs):
        events.append("cancel_sent")
        process.returncode = 0
        return True

    service._send_worker_command = _cancel  # type: ignore[method-assign]
    service.shutdown_task_engine()

    assert events == ["cancel_sent", "worker_waited", "database_closed"]


def test_cached_json_file_keeps_only_recent_entries(tmp_path):
    previous_cache = dict(cached_json_file._CACHE)
    previous_limit = cached_json_file._CACHE_MAX_ENTRIES
    try:
        cached_json_file._CACHE.clear()
        cached_json_file._CACHE_MAX_ENTRIES = 3
        for index in range(5):
            cached_json_file.save_cached_json(tmp_path / f"{index}.json", {"index": index})

        assert len(cached_json_file._CACHE) == 3
        assert not any(key.endswith("0.json") for key in cached_json_file._CACHE)
        assert any(key.endswith("4.json") for key in cached_json_file._CACHE)
    finally:
        cached_json_file._CACHE.clear()
        cached_json_file._CACHE.update(previous_cache)
        cached_json_file._CACHE_MAX_ENTRIES = previous_limit


def test_thread_job_preserves_failed_result_status():
    service = JobService()
    job = service.start_job(
        name="failed-result",
        feature="test",
        run_func=lambda _emit: {"ok": False, "status": "failed", "error": "expected failure"},
    )

    assert job.done_event.wait(5)
    assert job.status == "failed"
    assert job.error == "expected failure"
