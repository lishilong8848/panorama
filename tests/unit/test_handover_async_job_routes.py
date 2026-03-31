from __future__ import annotations

from types import SimpleNamespace

from app.modules.handover_review.api import routes


class _FakeJob:
    def __init__(self, *, job_id: str, name: str, resource_keys: list[str], priority: str, feature: str) -> None:
        self.job_id = job_id
        self.name = name
        self.resource_keys = list(resource_keys)
        self.priority = priority
        self.feature = feature

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
            "result": None,
        }


class _FakeJobService:
    def __init__(self):
        self.calls = []

    def start_worker_job(self, name, *, worker_handler, worker_payload=None, resource_keys=None, priority="manual", feature="", submitted_by=""):  # noqa: ANN001
        job = _FakeJob(
            job_id=f"job-{len(self.calls) + 1}",
            name=name,
            resource_keys=list(resource_keys or []),
            priority=submitted_by or priority,
            feature=feature or name,
        )
        self.calls.append(
            {
                "name": name,
                "worker_handler": worker_handler,
                "worker_payload": worker_payload,
                "resource_keys": list(resource_keys or []),
                "priority": priority,
                "feature": feature,
                "submitted_by": submitted_by,
                "job": job,
            }
        )
        return job


def _fake_request():
    container = SimpleNamespace(
        add_system_log=lambda *_args, **_kwargs: None,
        config=object(),
        config_path="config.json",
        runtime_config={},
        reload_config=lambda _cfg: None,
        job_service=_FakeJobService(),
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_handover_review_confirm_all_prefers_worker_job() -> None:
    request = _fake_request()
    job_service = request.app.state.container.job_service

    payload = routes.handover_review_confirm_all("2026-03-26|day", request)

    assert payload["ok"] is True
    assert payload["accepted"] is True
    assert payload["job"]["feature"] == "handover_confirm_all"
    assert job_service.calls[0]["worker_handler"] == "handover_confirm_all"
    assert job_service.calls[0]["worker_payload"] == {"batch_key": "2026-03-26|day"}
    assert "handover_batch:2026-03-26|day" in job_service.calls[0]["resource_keys"]


def test_handover_daily_report_rewrite_prefers_worker_job() -> None:
    request = _fake_request()
    job_service = request.app.state.container.job_service

    payload = routes.handover_daily_report_rewrite_record(
        request,
        {"duty_date": "2026-03-26", "duty_shift": "night"},
    )

    assert payload["ok"] is True
    assert payload["accepted"] is True
    assert payload["job"]["feature"] == "daily_report_record_rewrite"
    assert job_service.calls[0]["worker_handler"] == "daily_report_record_rewrite"
    assert job_service.calls[0]["worker_payload"] == {"duty_date": "2026-03-26", "duty_shift": "night"}
    assert "network:external" in job_service.calls[0]["resource_keys"]
    assert "handover_batch:2026-03-26|night" in job_service.calls[0]["resource_keys"]


def test_handover_cloud_retry_batch_prefers_worker_job() -> None:
    request = _fake_request()
    job_service = request.app.state.container.job_service

    payload = routes.handover_review_retry_cloud_sync_batch("2026-03-26|day", request)

    assert payload["ok"] is True
    assert payload["accepted"] is True
    assert payload["job"]["feature"] == "handover_cloud_retry_batch"
    assert job_service.calls[0]["worker_handler"] == "handover_cloud_retry_batch"
    assert job_service.calls[0]["worker_payload"] == {"batch_key": "2026-03-26|day"}
    assert "handover_batch:2026-03-26|day" in job_service.calls[0]["resource_keys"]
