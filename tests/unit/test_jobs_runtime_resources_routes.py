from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.modules.report_pipeline.api import routes
from app.modules.report_pipeline.service.job_service import TaskEngineUnavailableError


class _FakeJobService:
    def get_job(self, job_id: str):
        return {
            "job_id": job_id,
            "name": "交接班下载",
            "feature": "handover_from_download",
            "status": "running",
            "priority": "manual",
            "submitted_by": "manual",
            "resource_keys": ["network:internal"],
            "wait_reason": "",
            "stages": [
                {
                    "stage_id": "main",
                    "name": "handover_from_download",
                    "status": "running",
                    "resource_keys": ["network:internal"],
                    "resume_policy": "manual_resume",
                    "started_at": "2026-03-26 10:00:00",
                    "finished_at": "",
                    "summary": "",
                    "error": "",
                    "result": None,
                }
            ],
        }

    def active_job_ids(self, *, include_waiting: bool = True):  # noqa: ANN001
        return ["job-running", "job-waiting"] if include_waiting else ["job-running"]

    def job_counts(self):
        return {"running": 1, "waiting_resource": 1, "success": 2}

    def list_jobs(self, *, limit: int = 50, statuses=None):  # noqa: ANN001
        return [
            {
                "job_id": "job-running",
                "name": "告警多维上传",
                "status": "running",
                "priority": "manual",
                "resource_keys": ["network:external"],
                "wait_reason": "",
            },
            {
                "job_id": "job-waiting",
                "name": "今日航图截图",
                "status": "waiting_resource",
                "priority": "manual",
                "resource_keys": ["browser:controlled"],
                "wait_reason": "waiting:browser_controlled",
            },
        ][:limit]

    def get_resource_snapshot(self):
        return {
            "network": {
                "current_side": "external",
                "switching": False,
                "queued_internal": 0,
                "queued_external": 1,
                "queued_pipeline": 0,
                "running_internal": 0,
                "running_external": 1,
                "running_pipeline": 0,
            },
            "controlled_browser": {
                "holder_job_id": "job-running",
                "queue_length": 1,
            },
            "batch_locks": [],
            "resources": [],
        }


def _fake_request():
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=SimpleNamespace(job_service=_FakeJobService()))))


def test_list_jobs_route_returns_job_list_and_counts() -> None:
    payload = routes.list_jobs(_fake_request(), limit=10, statuses="")
    assert payload["count"] == 2
    assert payload["active_job_ids"] == ["job-running", "job-waiting"]
    assert payload["job_counts"]["running"] == 1
    assert payload["jobs"][0]["job_id"] == "job-running"


def test_runtime_resources_route_returns_resource_snapshot() -> None:
    payload = routes.get_runtime_resources(_fake_request())
    assert payload["network"]["current_side"] == "external"
    assert payload["controlled_browser"]["queue_length"] == 1


def test_list_jobs_route_prefers_runtime_status_snapshot_when_available() -> None:
    request = _fake_request()

    class _Coordinator:
        @staticmethod
        def is_running() -> bool:
            return True

        @staticmethod
        def read_scope_snapshot(scope: str):
            assert scope == "job_panel_summary"
            return {
                "payload": {
                    "jobs": [{"job_id": "job-from-sqlite", "status": "running"}],
                    "count": 1,
                    "active_job_ids": ["job-from-sqlite"],
                    "job_counts": {"running": 1},
                }
            }

        @staticmethod
        def request_refresh(*, reason: str = "") -> None:
            raise AssertionError(f"unexpected refresh request: {reason}")

    request.app.state.container.runtime_status_coordinator = _Coordinator()

    payload = routes.list_jobs(request, limit=10, statuses="")

    assert payload["count"] == 1
    assert payload["jobs"][0]["job_id"] == "job-from-sqlite"


def test_runtime_resources_route_prefers_runtime_status_snapshot_when_available() -> None:
    request = _fake_request()

    class _Coordinator:
        @staticmethod
        def is_running() -> bool:
            return True

        @staticmethod
        def read_scope_snapshot(scope: str):
            assert scope == "runtime_resources_summary"
            return {
                "payload": {
                    "network": {"current_side": "internal"},
                    "controlled_browser": {"holder_job_id": "job-from-sqlite", "queue_length": 0},
                    "batch_locks": [],
                    "resources": [],
                }
            }

        @staticmethod
        def request_refresh(*, reason: str = "") -> None:
            raise AssertionError(f"unexpected refresh request: {reason}")

    request.app.state.container.runtime_status_coordinator = _Coordinator()

    payload = routes.get_runtime_resources(request)

    assert payload["network"]["current_side"] == "internal"
    assert payload["controlled_browser"]["holder_job_id"] == "job-from-sqlite"


def test_get_job_route_returns_stage_details() -> None:
    payload = routes.get_job("job-running", _fake_request())
    assert payload["job_id"] == "job-running"
    assert payload["feature"] == "handover_from_download"
    assert payload["stages"][0]["stage_id"] == "main"
    assert payload["stages"][0]["resource_keys"] == ["network:internal"]


def test_get_job_route_returns_503_when_task_engine_temporarily_unavailable() -> None:
    class _UnavailableJobService(_FakeJobService):
        def get_job(self, _job_id: str):
            raise TaskEngineUnavailableError("任务状态存储暂时不可用，请稍后重试")

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=SimpleNamespace(job_service=_UnavailableJobService()))))

    with pytest.raises(HTTPException) as exc_info:
        routes.get_job("job-running", request)

    assert exc_info.value.status_code == 503
    assert "任务状态存储暂时不可用" in str(exc_info.value.detail)


def test_list_jobs_route_returns_503_when_task_engine_temporarily_unavailable() -> None:
    class _UnavailableJobService(_FakeJobService):
        def list_jobs(self, *, limit: int = 50, statuses=None):  # noqa: ANN001
            raise TaskEngineUnavailableError("任务状态存储暂时不可用，请稍后重试")

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=SimpleNamespace(job_service=_UnavailableJobService()))))

    with pytest.raises(HTTPException) as exc_info:
        routes.list_jobs(request, limit=10, statuses="")

    assert exc_info.value.status_code == 503
    assert "任务状态存储暂时不可用" in str(exc_info.value.detail)
