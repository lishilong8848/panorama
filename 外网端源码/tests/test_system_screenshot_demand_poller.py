from __future__ import annotations

from types import SimpleNamespace

from app.modules.system_screenshot_upload.service.system_screenshot_demand_poller import (
    SystemScreenshotDemandPoller,
    mark_demand_record_completed,
)


class FakeBitableClient:
    def __init__(self, records):
        self.records = records
        self.updated = []

    def list_records(self, **kwargs):  # noqa: ANN001
        self.list_kwargs = kwargs
        return self.records

    def update_record(self, table_id, record_id, fields):  # noqa: ANN001
        self.updated.append((table_id, record_id, fields))
        return {"code": 0}


class FakeJobService:
    def __init__(self, active=False):
        self.active = active
        self.started = []

    def has_active_jobs_for_feature_prefixes(self, prefixes):  # noqa: ANN001
        self.prefixes = prefixes
        return self.active

    def start_worker_job(self, **kwargs):  # noqa: ANN001
        self.started.append(kwargs)
        return SimpleNamespace(job_id="job-1")


def _config():
    return {
        "system_screenshot_upload": {
            "demand_poll": {
                "enabled": True,
                "interval_sec": 30,
                "app_token": "app-token",
                "table_id": "demand-table",
                "request_field": "同步需求",
                "completed_field": "上传完成",
            }
        }
    }


def test_poll_does_not_submit_when_request_unchecked():
    client = FakeBitableClient([{"record_id": "rec-1", "fields": {"同步需求": False}}])
    jobs = FakeJobService()
    poller = SystemScreenshotDemandPoller(
        runtime_config_getter=_config,
        job_service=jobs,
        role_mode_getter=lambda: "external",
        client_factory=lambda _app_token: client,
    )

    result = poller.poll_once()

    assert result["submitted"] is False
    assert jobs.started == []


def test_poll_submits_one_demand_upload_job_when_request_checked():
    client = FakeBitableClient(
        [
            {"record_id": "rec-1", "fields": {"同步需求": True}},
            {"record_id": "rec-2", "fields": {"同步需求": True}},
        ]
    )
    jobs = FakeJobService()
    poller = SystemScreenshotDemandPoller(
        runtime_config_getter=_config,
        job_service=jobs,
        role_mode_getter=lambda: "external",
        client_factory=lambda _app_token: client,
    )

    result = poller.poll_once()

    assert result["submitted"] is True
    assert len(jobs.started) == 1
    job = jobs.started[0]
    assert job["worker_handler"] == "system_screenshot_demand_upload"
    assert job["feature"] == "system_screenshot_upload"
    assert job["resource_keys"] == [f"system_screenshot_upload:{job['worker_payload']['capture_date']}"]
    assert job["worker_payload"]["demand_record_id"] == "rec-1"
    assert job["worker_payload"]["trigger_internal_capture"] is True
    assert job["worker_payload"]["internal_capture_force"] is True


def test_poll_skips_when_system_screenshot_job_is_active():
    client = FakeBitableClient([{"record_id": "rec-1", "fields": {"同步需求": True}}])
    jobs = FakeJobService(active=True)
    poller = SystemScreenshotDemandPoller(
        runtime_config_getter=_config,
        job_service=jobs,
        role_mode_getter=lambda: "external",
        client_factory=lambda _app_token: client,
    )

    result = poller.poll_once()

    assert result["reason"] == "job_active"
    assert jobs.started == []


def test_mark_demand_record_completed_unchecks_request_and_marks_completed():
    client = FakeBitableClient([])

    mark_demand_record_completed(
        _config(),
        {
            "demand_record_id": "rec-1",
            "demand_app_token": "app-token",
            "demand_table_id": "demand-table",
            "demand_request_field": "同步需求",
            "demand_completed_field": "上传完成",
        },
        client_factory=lambda _app_token: client,
    )

    assert client.updated == [("demand-table", "rec-1", {"同步需求": False, "上传完成": True})]
