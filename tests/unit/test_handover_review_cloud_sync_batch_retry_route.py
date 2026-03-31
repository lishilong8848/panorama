from __future__ import annotations

from types import SimpleNamespace

from app.modules.handover_review.api import routes


class _FakeJob:
    def __init__(self, *, job_id: str, name: str, resource_keys: list[str], priority: str, feature: str, result):
        self.job_id = job_id
        self.name = name
        self.resource_keys = list(resource_keys)
        self.priority = priority
        self.feature = feature
        self.result = result

    def to_dict(self):
        return {
            "job_id": self.job_id,
            "name": self.name,
            "feature": self.feature,
            "submitted_by": self.priority,
            "status": "queued",
            "priority": self.priority,
            "resource_keys": list(self.resource_keys),
            "wait_reason": "",
            "summary": "",
            "result": self.result,
        }


class _FakeJobService:
    def __init__(self):
        self.calls = []

    def start_job(self, name, run_func, *, resource_keys=None, priority="manual", feature="", submitted_by=""):  # noqa: ANN001
        result = run_func(lambda *_args, **_kwargs: None)
        job = _FakeJob(
            job_id=f"job-{len(self.calls) + 1}",
            name=name,
            resource_keys=list(resource_keys or []),
            priority=submitted_by or priority,
            feature=feature or name,
            result=result,
        )
        self.calls.append(
            {
                "name": name,
                "resource_keys": list(resource_keys or []),
                "priority": priority,
                "feature": feature,
                "submitted_by": submitted_by,
                "result": result,
                "job": job,
            }
        )
        return job


class _FakeContainer:
    def __init__(self):
        self.logs = []
        self.job_service = _FakeJobService()

    def add_system_log(self, message: str) -> None:
        self.logs.append(message)


def _fake_request(container: _FakeContainer):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_handover_review_retry_cloud_sync_batch_submits_blocked_job(monkeypatch):
    container = _FakeContainer()

    class _FakeService:
        def get_batch_status(self, batch_key: str):  # noqa: ARG002
            return {"batch_key": "2026-03-22|night"}

        def list_batch_sessions(self, batch_key: str):  # noqa: ARG002
            return []

    class _FakeFollowup:
        def retry_failed_cloud_sheet_in_batch(self, batch_key: str, emit_log=print):  # noqa: ARG002
            return {
                "status": "blocked",
                "batch_status": {"batch_key": batch_key, "all_confirmed": False},
                "updated_sessions": [],
                "cloud_sheet_sync": {
                    "status": "blocked",
                    "blocked_reason": "当前批次尚未全部确认",
                },
            }

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_FakeService(), None, None, _FakeFollowup()))

    response = routes.handover_review_retry_cloud_sync_batch("2026-03-22|night", _fake_request(container))

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["job"]["feature"] == "handover_cloud_retry_batch"
    assert response["job"]["result"]["status"] == "blocked"
    assert "network:external" in container.job_service.calls[0]["resource_keys"]
    assert "handover_batch:2026-03-22|night" in container.job_service.calls[0]["resource_keys"]


def test_handover_review_retry_cloud_sync_batch_submits_updated_sessions_job(monkeypatch):
    container = _FakeContainer()

    updated_sessions = [
        {
            "building": "A楼",
            "cloud_sheet_sync": {"status": "success"},
        }
    ]
    batch_status = {"batch_key": "2026-03-22|night", "all_confirmed": True}

    class _FakeService:
        def get_batch_status(self, batch_key: str):  # noqa: ARG002
            return batch_status

        def list_batch_sessions(self, batch_key: str):  # noqa: ARG002
            return updated_sessions

    class _FakeFollowup:
        def retry_failed_cloud_sheet_in_batch(self, batch_key: str, emit_log=print):  # noqa: ARG002
            return {
                "status": "ok",
                "batch_status": batch_status,
                "updated_sessions": updated_sessions,
                "cloud_sheet_sync": {
                    "status": "ok",
                    "uploaded_buildings": ["A楼"],
                    "failed_buildings": [],
                    "skipped_buildings": [],
                    "details": {},
                },
            }

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_FakeService(), None, None, _FakeFollowup()))

    response = routes.handover_review_retry_cloud_sync_batch("2026-03-22|night", _fake_request(container))

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["job"]["result"]["status"] == "ok"
    assert response["job"]["result"]["batch_status"] == batch_status
    assert response["job"]["result"]["updated_sessions"] == updated_sessions
    assert response["job"]["result"]["cloud_sheet_sync"]["uploaded_buildings"] == ["A楼"]

