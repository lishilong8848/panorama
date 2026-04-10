from __future__ import annotations

import shutil
import sys
import uuid
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.bootstrap import app_factory


TEMP_ROOT = PROJECT_ROOT / '.tmp_runtime_tests' / 'app_factory_scheduler_role_bridge'


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


class _FakeBridgeService:
    def __init__(self, *, ready: bool = True, stale: bool = False) -> None:
        self.ready = ready
        self.stale = stale
        self.calls: list[tuple[str, dict]] = []

    def get_source_cache_buildings(self):
        self.calls.append(("get_source_cache_buildings", {}))
        return ["A楼", "B楼"]

    def get_latest_source_cache_selection(self, **kwargs):  # noqa: ANN003
        self.calls.append(("get_latest_source_cache_selection", dict(kwargs)))
        buildings = list(kwargs.get("buildings") or [])
        family = str(kwargs.get("source_family") or "")
        if not self.ready:
            return {
                "best_bucket_key": "",
                "best_bucket_age_hours": None,
                "is_best_bucket_too_old": False,
                "selected_entries": [],
                "fallback_buildings": [],
                "missing_buildings": buildings,
                "stale_buildings": [],
                "buildings": [],
                "can_proceed": False,
            }
        if self.stale:
            return {
                "best_bucket_key": "2026-03-29 10",
                "best_bucket_age_hours": 1.0,
                "is_best_bucket_too_old": False,
                "selected_entries": [
                    {
                        "building": "A楼",
                        "file_path": f"D:/QJPT_Shared/{family}/A楼.xlsx",
                        "bucket_key": "2026-03-29 10",
                        "duty_date": "2026-03-29",
                        "metadata": {"upload_date": "2026-03-29"},
                    }
                ],
                "fallback_buildings": [],
                "missing_buildings": [],
                "stale_buildings": ["B楼"],
                "buildings": [],
                "can_proceed": False,
            }
        return {
            "best_bucket_key": "2026-03-29 10",
            "best_bucket_age_hours": 1.0,
            "is_best_bucket_too_old": False,
            "selected_entries": [
                {
                    "building": building,
                    "file_path": f"D:/QJPT_Shared/{family}/{building}.xlsx",
                    "bucket_key": "2026-03-29 10",
                    "duty_date": "2026-03-29",
                    "metadata": {"upload_date": "2026-03-29"},
                }
                for building in buildings
            ],
            "fallback_buildings": [],
            "missing_buildings": [],
            "stale_buildings": [],
            "buildings": [],
            "can_proceed": True,
        }

    def create_monthly_auto_once_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_monthly_auto_once_task", dict(kwargs)))
        return {"task_id": "bridge-monthly-auto-once-1", "feature": "monthly_report_pipeline", "status": "queued_for_internal"}

    def create_handover_from_download_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_handover_from_download_task", dict(kwargs)))
        return {"task_id": "bridge-handover-latest-1", "feature": "handover_from_download", "status": "queued_for_internal"}

    def get_or_create_handover_from_download_task(self, **kwargs):  # noqa: ANN003
        return self.create_handover_from_download_task(**kwargs)

    def get_handover_capacity_by_date_cache_entries(self, *, duty_date, duty_shift, buildings):  # noqa: ANN001
        self.calls.append(
            (
                "get_handover_capacity_by_date_cache_entries",
                {"duty_date": duty_date, "duty_shift": duty_shift, "buildings": list(buildings)},
            )
        )
        if not self.ready:
            return []
        if self.stale:
            return [
                {
                    "building": "A楼",
                    "file_path": "D:/QJPT_Shared/handover_capacity_report_family/A楼.xlsx",
                    "duty_date": duty_date,
                    "duty_shift": duty_shift,
                }
            ]
        return [
            {
                "building": building,
                "file_path": f"D:/QJPT_Shared/handover_capacity_report_family/{building}.xlsx",
                "duty_date": duty_date,
                "duty_shift": duty_shift,
            }
            for building in buildings
        ]

    def create_wet_bulb_collection_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_wet_bulb_collection_task", dict(kwargs)))
        return {"task_id": "bridge-wet-bulb-1", "feature": "wet_bulb_collection", "status": "queued_for_internal"}


class _FakeJob:
    def __init__(self, job_id: str = "job-cache-1", *, status: str = "queued", summary: str = "ok", wait_reason: str = "", bridge_task_id: str = "") -> None:
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
        self.started_jobs: list[dict] = []
        self.waiting_jobs: list[dict] = []
        self.bind_calls: list[tuple[str, str]] = []
        self.last_waiting_job: _FakeJob | None = None

    def active_job_id(self):
        return ""

    def start_job(self, **kwargs):  # noqa: ANN003
        self.started_jobs.append(dict(kwargs))
        return _FakeJob(f"job-{len(self.started_jobs)}")

    def create_waiting_worker_job(self, **kwargs):  # noqa: ANN003
        self.waiting_jobs.append(dict(kwargs))
        job = _FakeJob(
            f"job-waiting-{len(self.waiting_jobs)}",
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
        raise AssertionError("external cache scheduler path should not wait synchronously")


class _FakeContainer:
    def __init__(
        self,
        *,
        frontend_root: Path,
        role_mode: str,
        bridge_enabled: bool,
        bridge_service: _FakeBridgeService | None,
    ) -> None:
        self.config = {"common": {"console": {}}}
        self.runtime_config = {}
        self.config_path = frontend_root / "config.json"
        self.frontend_mode = "dist"
        self.frontend_root = frontend_root
        self.frontend_assets_dir = frontend_root
        self.job_service = _FakeJobService()
        self.scheduler = None
        self.handover_scheduler_manager = None
        self.wet_bulb_collection_scheduler = None
        self.updater_service = None
        self.alert_log_uploader = None
        self.shared_bridge_service = bridge_service
        self.version = "web-3.0.0"
        self._role_mode = role_mode
        self._bridge_enabled = bridge_enabled
        self.logs: list[str] = []
        self.wet_bulb_records: list[dict] = []
        self.scheduler_callback = None
        self.handover_scheduler_callback = None
        self.wet_bulb_collection_scheduler_callback = None

    def add_system_log(self, text: str, *_args, **_kwargs):
        self.logs.append(str(text))

    def record_wet_bulb_collection_external_run(self, **kwargs):  # noqa: ANN003
        self.wet_bulb_records.append(dict(kwargs))

    def deployment_snapshot(self):
        return {
            "role_mode": self._role_mode,
            "node_id": "node-1",
            "node_label": "node-label",
        }

    def shared_bridge_snapshot(self):
        return {
            "enabled": self._bridge_enabled,
            "root_dir": "D:/QJPT_Shared" if self._bridge_enabled else "",
        }

    def set_scheduler_callback(self, callback):
        self.scheduler_callback = callback

    def set_handover_scheduler_callback(self, callback):
        self.handover_scheduler_callback = callback

    def set_wet_bulb_collection_scheduler_callback(self, callback):
        self.wet_bulb_collection_scheduler_callback = callback

    def set_updater_restart_callback(self, *_args, **_kwargs):
        return None

    def scheduler_executor_name(self):
        return "scheduler_callback"

    def is_scheduler_executor_bound(self):
        return True

    def handover_scheduler_executor_name(self):
        return "handover_scheduler_callback"

    def is_handover_scheduler_executor_bound(self):
        return True

    def wet_bulb_collection_scheduler_executor_name(self):
        return "wet_bulb_collection_scheduler_callback"

    def is_wet_bulb_collection_scheduler_executor_bound(self):
        return True


def _build_app(monkeypatch: pytest.MonkeyPatch, work_dir: Path, container: _FakeContainer):
    (work_dir / "index.html").write_text("<!doctype html><html><body>ok</body></html>", encoding="utf-8")
    monkeypatch.setattr(app_factory, "build_container", lambda: container)
    return app_factory.create_app(enable_lifespan=False)


def test_scheduler_callback_external_starts_cache_job_when_latest_selection_can_proceed(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
):
    bridge = _FakeBridgeService(ready=True)
    container = _FakeContainer(
        frontend_root=work_dir,
        role_mode="external",
        bridge_enabled=True,
        bridge_service=bridge,
    )
    _build_app(monkeypatch, work_dir, container)

    ok, message = container.scheduler_callback("自动流程调度")

    assert ok is True
    assert "共享文件继续处理任务" in message
    assert [item[0] for item in bridge.calls] == ["get_source_cache_buildings", "get_latest_source_cache_selection"]
    assert container.job_service.started_jobs[0]["feature"] == "monthly_cache_latest"


def test_scheduler_callback_external_waits_when_latest_selection_has_missing_building(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
):
    bridge = _FakeBridgeService(ready=False)
    container = _FakeContainer(
        frontend_root=work_dir,
        role_mode="external",
        bridge_enabled=True,
        bridge_service=bridge,
    )
    _build_app(monkeypatch, work_dir, container)

    ok, message = container.scheduler_callback("自动流程调度")

    assert ok is True
    assert "等待缺失楼栋共享文件补齐" in message
    assert "已受理共享桥接任务" in message
    assert container.job_service.started_jobs == []


def test_handover_scheduler_external_waits_when_latest_selection_is_stale(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
):
    bridge = _FakeBridgeService(ready=True, stale=True)
    container = _FakeContainer(
        frontend_root=work_dir,
        role_mode="external",
        bridge_enabled=True,
        bridge_service=bridge,
    )
    _build_app(monkeypatch, work_dir, container)

    ok, message = container.handover_scheduler_callback("morning", "交接班调度")

    assert ok is True
    assert "等待过旧楼栋共享文件更新" in message
    assert "已受理共享桥接任务" in message
    assert container.job_service.started_jobs == []


def test_scheduler_callback_external_waits_when_best_bucket_is_older_than_three_hours(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
):
    bridge = _FakeBridgeService(ready=True)
    container = _FakeContainer(
        frontend_root=work_dir,
        role_mode="external",
        bridge_enabled=True,
        bridge_service=bridge,
    )
    _build_app(monkeypatch, work_dir, container)
    bridge.get_latest_source_cache_selection = lambda **_kwargs: {
        "best_bucket_key": "2026-03-30 08",
        "best_bucket_age_hours": 30.9,
        "is_best_bucket_too_old": True,
        "selected_entries": [
            {
                "building": "A楼",
                "file_path": "D:/QJPT_Shared/monthly_report_family/A楼.xlsx",
                "bucket_key": "2026-03-30 08",
                "duty_date": "2026-03-29",
                "metadata": {"upload_date": "2026-03-29"},
            },
            {
                "building": "B楼",
                "file_path": "D:/QJPT_Shared/monthly_report_family/B楼.xlsx",
                "bucket_key": "2026-03-30 08",
                "duty_date": "2026-03-29",
                "metadata": {"upload_date": "2026-03-29"},
            },
        ],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": False,
    }

    ok, message = container.scheduler_callback("自动流程调度")

    assert ok is True
    assert "等待最新共享文件更新" in message
    assert "超过 3 小时" in message
    assert "已受理共享桥接任务" in message
    assert container.job_service.started_jobs == []


def test_wet_bulb_scheduler_external_requires_bridge(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
):
    container = _FakeContainer(
        frontend_root=work_dir,
        role_mode="external",
        bridge_enabled=False,
        bridge_service=None,
    )
    _build_app(monkeypatch, work_dir, container)

    ok, message = container.wet_bulb_collection_scheduler_callback("湿球调度")

    assert ok is False
    assert "共享桥接未启用或共享目录未配置" in message
    assert container.job_service.started_jobs == []
    assert container.wet_bulb_records and container.wet_bulb_records[-1]["status"] == "failed"
