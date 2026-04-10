from __future__ import annotations

import shutil
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.bootstrap import app_factory
from app.modules.report_pipeline.api import routes


TEMP_ROOT = PROJECT_ROOT / '.tmp_runtime_tests' / 'bridge_flow_smoke_matrix'


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _touch_file(path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")
    return str(path)


class _FakeJob:
    def __init__(self, job_id: str, *, status: str = "queued", summary: str = "ok", wait_reason: str = "", bridge_task_id: str = "") -> None:
        self.job_id = job_id
        self.status = status
        self.summary = summary
        self.wait_reason = wait_reason
        self.bridge_task_id = bridge_task_id

    def to_dict(self) -> dict:
        payload = {"job_id": self.job_id, "status": self.status, "summary": self.summary}
        if self.wait_reason:
            payload["wait_reason"] = self.wait_reason
        if self.bridge_task_id:
            payload["bridge_task_id"] = self.bridge_task_id
        return payload


class _FakeJobService:
    def __init__(self) -> None:
        self.start_job_calls: list[dict] = []
        self.worker_calls: list[dict] = []
        self.waiting_calls: list[dict] = []
        self.bind_calls: list[tuple[str, str]] = []
        self.last_waiting_job: _FakeJob | None = None

    def active_job_id(self):
        return ""

    def start_job(self, **kwargs):  # noqa: ANN003
        self.start_job_calls.append(dict(kwargs))
        return _FakeJob(f"job-{len(self.start_job_calls)}")

    def start_worker_job(self, **kwargs):  # noqa: ANN003
        self.worker_calls.append(dict(kwargs))
        raise AssertionError("smoke matrix external path should not start local worker job")

    def create_waiting_worker_job(self, **kwargs):  # noqa: ANN003
        self.waiting_calls.append(dict(kwargs))
        job = _FakeJob(
            f"job-waiting-{len(self.waiting_calls)}",
            status="waiting_resource",
            summary=str(kwargs.get("summary", "") or "").strip(),
            wait_reason=str(kwargs.get("wait_reason", "") or "").strip(),
        )
        self.last_waiting_job = job
        return job

    def bind_bridge_task(self, job_id: str, bridge_task_id: str):
        self.bind_calls.append((job_id, bridge_task_id))
        if self.last_waiting_job and self.last_waiting_job.job_id == job_id:
            self.last_waiting_job.bridge_task_id = bridge_task_id
        return self.last_waiting_job

    def wait_job(self, _job_id):  # noqa: ANN001
        raise AssertionError("smoke matrix should not synchronously wait jobs")


class _FakeBridgeService:
    def __init__(self, *, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.buildings = ["A楼", "B楼"]
        self.calls: list[tuple[str, dict]] = []
        self.monthly_latest_selection: dict | None = None
        self.handover_latest_selection: dict | None = None
        self.day_metric_entries: list[dict] = []

    def get_source_cache_buildings(self):
        self.calls.append(("get_source_cache_buildings", {}))
        return list(self.buildings)

    def get_latest_source_cache_selection(self, **kwargs):  # noqa: ANN003
        self.calls.append(("get_latest_source_cache_selection", dict(kwargs)))
        family = str(kwargs.get("source_family") or "")
        if family == "monthly_report_family":
            return dict(self.monthly_latest_selection or {})
        if family == "handover_log_family":
            return dict(self.handover_latest_selection or {})
        return {}

    def get_day_metric_by_date_cache_entries(self, *, selected_dates, buildings):
        self.calls.append((
            "get_day_metric_by_date_cache_entries",
            {"selected_dates": list(selected_dates), "buildings": list(buildings)},
        ))
        return list(self.day_metric_entries)

    def create_monthly_auto_once_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_monthly_auto_once_task", dict(kwargs)))
        return {"task_id": "bridge-monthly-auto-once-1", "feature": "monthly_report_pipeline", "status": "queued_for_internal"}

    def create_handover_from_download_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_handover_from_download_task", dict(kwargs)))
        return {"task_id": "bridge-handover-latest-1", "feature": "handover_from_download", "status": "queued_for_internal"}

    def create_handover_cache_fill_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_handover_cache_fill_task", dict(kwargs)))
        return {"task_id": "bridge-handover-cache-fill-1", "feature": "handover_cache_fill", "status": "queued_for_internal"}

    def create_day_metric_from_download_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_day_metric_from_download_task", dict(kwargs)))
        return {"task_id": "bridge-day-metric-from-download-1", "feature": "day_metric_from_download", "status": "queued_for_internal"}


class _FakeContainer(SimpleNamespace):
    pass


def _make_request(bridge_service: _FakeBridgeService):
    container = _FakeContainer(
        job_service=_FakeJobService(),
        shared_bridge_service=bridge_service,
        logs=[],
        runtime_config={
            "paths": {},
            "network": {"enable_auto_switch_wifi": False},
            "input": {"buildings": ["A楼", "B楼"]},
            "day_metric_upload": {"enabled": True},
        },
        deployment_snapshot=lambda: {"role_mode": "external", "node_id": "node-ext-01", "node_label": "外网端"},
        shared_bridge_snapshot=lambda: {"enabled": True, "root_dir": "D:/QJPT_Shared"},
        add_system_log=lambda *_args, **_kwargs: None,
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def _make_scheduler_app(monkeypatch: pytest.MonkeyPatch, work_dir: Path, bridge_service: _FakeBridgeService):
    (work_dir / "index.html").write_text("<!doctype html><html><body>ok</body></html>", encoding="utf-8")
    container = _FakeContainer(
        config={"common": {"console": {}}},
        runtime_config={},
        config_path=work_dir / "config.json",
        frontend_mode="dist",
        frontend_root=work_dir,
        frontend_assets_dir=work_dir,
        job_service=_FakeJobService(),
        scheduler=None,
        handover_scheduler_manager=None,
        wet_bulb_collection_scheduler=None,
        updater_service=None,
        alert_log_uploader=None,
        shared_bridge_service=bridge_service,
        version="web-3.0.0",
        logs=[],
        scheduler_callback=None,
        handover_scheduler_callback=None,
        wet_bulb_collection_scheduler_callback=None,
        day_metric_upload_scheduler_callback=None,
        alarm_event_upload_scheduler_callback=None,
        monthly_change_report_scheduler_callback=None,
        monthly_event_report_scheduler_callback=None,
        add_system_log=lambda *_args, **_kwargs: None,
        set_scheduler_callback=lambda callback: setattr(container, "scheduler_callback", callback),
        set_handover_scheduler_callback=lambda callback: setattr(container, "handover_scheduler_callback", callback),
        set_wet_bulb_collection_scheduler_callback=lambda callback: setattr(container, "wet_bulb_collection_scheduler_callback", callback),
        set_day_metric_upload_scheduler_callback=lambda callback: setattr(container, "day_metric_upload_scheduler_callback", callback),
        set_alarm_event_upload_scheduler_callback=lambda callback: setattr(container, "alarm_event_upload_scheduler_callback", callback),
        set_monthly_change_report_scheduler_callback=lambda callback: setattr(container, "monthly_change_report_scheduler_callback", callback),
        set_monthly_event_report_scheduler_callback=lambda callback: setattr(container, "monthly_event_report_scheduler_callback", callback),
        set_updater_restart_callback=lambda *_args, **_kwargs: None,
        scheduler_executor_name=lambda: "scheduler_callback",
        is_scheduler_executor_bound=lambda: True,
        handover_scheduler_executor_name=lambda: "handover_scheduler_callback",
        is_handover_scheduler_executor_bound=lambda: True,
        wet_bulb_collection_scheduler_executor_name=lambda: "wet_bulb_collection_scheduler_callback",
        is_wet_bulb_collection_scheduler_executor_bound=lambda: True,
        day_metric_upload_scheduler_executor_name=lambda: "day_metric_upload_scheduler_callback",
        is_day_metric_upload_scheduler_executor_bound=lambda: True,
        alarm_event_upload_scheduler_executor_name=lambda: "alarm_event_upload_scheduler_callback",
        is_alarm_event_upload_scheduler_executor_bound=lambda: True,
        monthly_change_report_scheduler_executor_name=lambda: "monthly_change_report_scheduler_callback",
        is_monthly_change_report_scheduler_executor_bound=lambda: True,
        monthly_event_report_scheduler_executor_name=lambda: "monthly_event_report_scheduler_callback",
        is_monthly_event_report_scheduler_executor_bound=lambda: True,
        deployment_snapshot=lambda: {"role_mode": "external", "node_id": "node-1", "node_label": "外网端"},
        shared_bridge_snapshot=lambda: {"enabled": True, "root_dir": "D:/QJPT_Shared"},
    )
    monkeypatch.setattr(app_factory, "build_container", lambda: container)
    app_factory.create_app(enable_lifespan=False)
    return container


def test_smoke_monthly_latest_ready_continues_locally(work_dir: Path) -> None:
    bridge = _FakeBridgeService(base_dir=work_dir)
    bridge.monthly_latest_selection = {
        "best_bucket_key": "2026-03-31 13",
        "best_bucket_age_hours": 1.0,
        "is_best_bucket_too_old": False,
        "selected_entries": [
            {"building": "A楼", "file_path": _touch_file(work_dir / "monthly" / "A楼.xlsx"), "bucket_key": "2026-03-31 13", "duty_date": "2026-03-30", "metadata": {"upload_date": "2026-03-30"}},
            {"building": "B楼", "file_path": _touch_file(work_dir / "monthly" / "B楼.xlsx"), "bucket_key": "2026-03-31 13", "duty_date": "2026-03-30", "metadata": {"upload_date": "2026-03-30"}},
        ],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": True,
    }
    request = _make_request(bridge)

    response = routes.job_auto_once(request)

    assert response["job_id"] == "job-1"
    assert request.app.state.container.job_service.start_job_calls[0]["feature"] == "monthly_cache_latest"


def test_smoke_monthly_latest_too_old_creates_bridge_task(work_dir: Path) -> None:
    bridge = _FakeBridgeService(base_dir=work_dir)
    bridge.monthly_latest_selection = {
        "best_bucket_key": "2026-03-30 08",
        "best_bucket_age_hours": 30.9,
        "is_best_bucket_too_old": True,
        "selected_entries": [
            {"building": "A楼", "file_path": _touch_file(work_dir / "monthly-too-old" / "A楼.xlsx"), "bucket_key": "2026-03-30 08", "duty_date": "2026-03-29", "metadata": {"upload_date": "2026-03-29"}},
            {"building": "B楼", "file_path": _touch_file(work_dir / "monthly-too-old" / "B楼.xlsx"), "bucket_key": "2026-03-30 08", "duty_date": "2026-03-29", "metadata": {"upload_date": "2026-03-29"}},
        ],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": False,
    }
    request = _make_request(bridge)

    response = routes.job_auto_once(request)

    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-monthly-auto-once-1"
    assert response["job"]["status"] == "waiting_resource"
    assert response["job"]["wait_reason"] == "waiting:shared_bridge"
    assert request.app.state.container.job_service.start_job_calls == []


def test_smoke_handover_latest_missing_creates_bridge_task(work_dir: Path) -> None:
    bridge = _FakeBridgeService(base_dir=work_dir)
    bridge.handover_latest_selection = {
        "best_bucket_key": "",
        "best_bucket_age_hours": None,
        "is_best_bucket_too_old": False,
        "selected_entries": [],
        "fallback_buildings": [],
        "missing_buildings": ["A楼", "B楼"],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": False,
    }
    request = _make_request(bridge)

    response = routes.job_handover_from_download({"buildings": ["A楼", "B楼"]}, request)

    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-handover-latest-1"
    assert response["job"]["status"] == "waiting_resource"


def test_smoke_day_metric_by_date_missing_creates_cache_fill_task(work_dir: Path) -> None:
    bridge = _FakeBridgeService(base_dir=work_dir)
    request = _make_request(bridge)

    response = routes.job_day_metric_from_download(
        {"dates": ["2026-03-30"], "building_scope": "single", "building": "A楼"},
        request,
    )

    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-day-metric-from-download-1"
    assert response["job"]["status"] == "waiting_resource"


def test_smoke_day_metric_by_date_ready_continues_locally(work_dir: Path) -> None:
    bridge = _FakeBridgeService(base_dir=work_dir)
    bridge.day_metric_entries = [
        {"building": "A楼", "duty_date": "2026-03-30", "file_path": _touch_file(work_dir / "day-metric" / "A楼.xlsx")}
    ]
    request = _make_request(bridge)

    response = routes.job_day_metric_from_download(
        {"dates": ["2026-03-30"], "building_scope": "single", "building": "A楼"},
        request,
    )

    assert response["job_id"] == "job-1"
    assert request.app.state.container.job_service.start_job_calls[0]["feature"] == "day_metric_cache_by_date"


def test_smoke_scheduler_latest_missing_creates_bridge_task(monkeypatch: pytest.MonkeyPatch, work_dir: Path) -> None:
    bridge = _FakeBridgeService(base_dir=work_dir)
    bridge.monthly_latest_selection = {
        "best_bucket_key": "",
        "best_bucket_age_hours": None,
        "is_best_bucket_too_old": False,
        "selected_entries": [],
        "fallback_buildings": [],
        "missing_buildings": ["A楼", "B楼"],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": False,
    }
    container = _make_scheduler_app(monkeypatch, work_dir, bridge)

    ok, message = container.scheduler_callback("自动流程调度")

    assert ok is True
    assert "已受理共享桥接任务" in message
    assert container.job_service.start_job_calls == []
