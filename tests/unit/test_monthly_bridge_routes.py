from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.modules.report_pipeline.api import routes


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


def _touch_file(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")
    return path


class _FakeBridgeService:
    def __init__(self, *, base_dir: Path | None = None) -> None:
        self.calls = []
        self.pending_runs = [{"run_id": "run-1", "run_save_dir": "D:/QJPT_Shared/artifacts/monthly_report/t1/source_files"}]
        self.buildings = ["A楼", "B楼"]
        self.latest_ready = True
        self.by_date_ready = False
        self.base_dir = base_dir

    def get_source_cache_buildings(self):
        self.calls.append(("get_source_cache_buildings", {}))
        return list(self.buildings)

    def get_latest_source_cache_selection(self, **kwargs):  # noqa: ANN003
        self.calls.append(("get_latest_source_cache_selection", dict(kwargs)))
        requested_buildings = list(kwargs.get("buildings") or [])
        if not self.latest_ready:
            return {
                "best_bucket_key": "",
                "selected_entries": [],
                "fallback_buildings": [],
                "missing_buildings": requested_buildings,
                "stale_buildings": [],
                "buildings": [],
                "can_proceed": False,
            }
        base_dir = self.base_dir or (Path.cwd() / ".tmp_monthly_bridge_routes" / "shared")
        entries = [
            {
                "building": building,
                "file_path": str(_touch_file(base_dir / "source_cache" / "monthly" / "latest" / building / "latest.xlsx")),
                "bucket_key": "2026-03-29 10",
                "duty_date": "2026-03-29",
                "metadata": {"upload_date": "2026-03-29"},
            }
            for building in requested_buildings
        ]
        return {
            "best_bucket_key": "2026-03-29 10",
            "selected_entries": entries,
            "fallback_buildings": [],
            "missing_buildings": [],
            "stale_buildings": [],
            "buildings": [],
            "can_proceed": True,
        }

    def get_monthly_by_date_cache_entries(self, *, selected_dates, buildings=None):  # noqa: ANN001
        self.calls.append(("get_monthly_by_date_cache_entries", {"selected_dates": list(selected_dates), "buildings": list(buildings or [])}))
        if not self.by_date_ready:
            return []
        base_dir = self.base_dir or (Path.cwd() / ".tmp_monthly_bridge_routes" / "shared")
        output = []
        for duty_date in selected_dates:
            for building in buildings or []:
                output.append(
                    {
                        "building": building,
                        "file_path": str(_touch_file(base_dir / "source_cache" / "monthly" / "by_date" / duty_date / building / "data.xlsx")),
                        "duty_date": duty_date,
                        "metadata": {"upload_date": duty_date},
                    }
                )
        return output

    def create_monthly_cache_fill_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_monthly_cache_fill_task", dict(kwargs)))
        return {
            "task_id": "bridge-monthly-cache-fill-1",
            "feature": "monthly_cache_fill",
            "status": "queued_for_internal",
        }

    def create_monthly_auto_once_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_monthly_auto_once_task", dict(kwargs)))
        return {
            "task_id": "bridge-monthly-auto-once-1",
            "feature": "monthly_report_pipeline",
            "status": "queued_for_internal",
        }

    def create_monthly_resume_upload_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("resume_upload", dict(kwargs)))
        return {
            "task_id": "bridge-monthly-resume-1",
            "feature": "monthly_report_pipeline",
            "status": "ready_for_external",
            "request": {"run_id": kwargs.get("run_id") or "", "auto_trigger": bool(kwargs.get("auto_trigger", False))},
        }

    def list_monthly_pending_resume_runs(self):
        self.calls.append(("list_pending", {}))
        return list(self.pending_runs)

    def delete_monthly_resume_run(self, run_id: str):
        self.calls.append(("delete_run", {"run_id": run_id}))
        if run_id == "missing":
            return {"ok": False, "deleted": False, "run_id": run_id, "message": "not found"}
        return {"ok": True, "deleted": True, "run_id": run_id, "message": "deleted"}


class _FakeJobService:
    def __init__(self) -> None:
        self.start_job_calls = []
        self.worker_calls = []
        self.waiting_calls = []
        self.bind_calls = []
        self.last_waiting_job = None

    def start_worker_job(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.worker_calls.append((args, kwargs))
        raise AssertionError("external cache mode should not start local worker job")

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

    def start_job(self, **kwargs):  # noqa: ANN003
        self.start_job_calls.append(dict(kwargs))
        return _FakeJob(f"job-{len(self.start_job_calls)}")


class _FakeContainer(SimpleNamespace):
    pass


def _fake_request(*, role_mode: str = "external", bridge_enabled: bool = True, base_dir: Path | None = None):
    bridge_service = _FakeBridgeService(base_dir=base_dir)
    container = _FakeContainer(
        job_service=_FakeJobService(),
        shared_bridge_service=bridge_service,
        logs=[],
        runtime_config={"paths": {}, "network": {"enable_auto_switch_wifi": True}},
        deployment_snapshot=lambda: {"role_mode": role_mode, "node_id": "node-ext-01", "node_label": ""},
        shared_bridge_snapshot=lambda: {"enabled": bridge_enabled, "root_dir": "D:/QJPT_Shared" if bridge_enabled else ""},
        add_system_log=lambda *_args, **_kwargs: None,
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_auto_once_route_starts_from_latest_cache_on_external_role() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)

    response = routes.job_auto_once(request)

    assert response["job_id"] == "job-1"
    assert request.app.state.container.job_service.worker_calls == []
    assert request.app.state.container.job_service.start_job_calls[0]["feature"] == "monthly_cache_latest"
    assert request.app.state.container.job_service.start_job_calls[0]["dedupe_key"].startswith("monthly_cache_latest:")
    assert '"bucket_key":"2026-03-29 10"' in request.app.state.container.job_service.start_job_calls[0]["dedupe_key"]
    assert '"buildings":["A楼","B楼"]' in request.app.state.container.job_service.start_job_calls[0]["dedupe_key"]
    assert request.app.state.container.shared_bridge_service.calls[:2] == [
        ("get_source_cache_buildings", {}),
        ("get_latest_source_cache_selection", {"source_family": "monthly_report_family", "buildings": ["A楼", "B楼"]}),
    ]


def test_auto_once_route_waits_when_indexed_file_is_missing() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    request.app.state.container.shared_bridge_service.buildings = ["A楼"]
    request.app.state.container.shared_bridge_service.get_latest_source_cache_selection = lambda **_kwargs: {  # noqa: E731
        "best_bucket_key": "2026-03-29 10",
        "selected_entries": [
            {
                "building": "A楼",
                "file_path": str(Path.cwd() / ".tmp_monthly_bridge_routes" / "missing" / "A楼-actual.xlsx"),
                "bucket_key": "2026-03-29 10",
                "duty_date": "2026-03-29",
                "metadata": {"upload_date": "2026-03-29"},
            }
        ],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": True,
    }

    response = routes.job_auto_once(request)

    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-monthly-auto-once-1"
    assert response["job"]["status"] == "waiting_resource"
    assert response["job"]["wait_reason"] == "waiting:shared_bridge"
    assert response["job"]["bridge_task_id"] == "bridge-monthly-auto-once-1"


def test_auto_once_route_waits_when_indexed_file_is_inaccessible(monkeypatch) -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    request.app.state.container.shared_bridge_service.buildings = ["A楼"]
    actual_file = _touch_file(Path.cwd() / ".tmp_monthly_bridge_routes" / "indexed" / "A楼-actual.xlsx")
    request.app.state.container.shared_bridge_service.get_latest_source_cache_selection = lambda **_kwargs: {  # noqa: E731
        "best_bucket_key": "2026-03-29 10",
        "selected_entries": [
            {
                "building": "A楼",
                "file_path": str(actual_file),
                "bucket_key": "2026-03-29 10",
                "duty_date": "2026-03-29",
                "metadata": {"upload_date": "2026-03-29"},
            }
        ],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": True,
    }
    monkeypatch.setattr(routes, "is_accessible_cached_file_path", lambda _path: False)

    response = routes.job_auto_once(request)

    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-monthly-auto-once-1"


def test_auto_once_route_uses_cached_file_path_verbatim_on_external_role(monkeypatch) -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    request.app.state.container.shared_bridge_service.buildings = ["A楼"]
    actual_file = _touch_file(Path.cwd() / ".tmp_monthly_bridge_routes" / "monthly-report" / "A楼-actual.xlsx")
    request.app.state.container.shared_bridge_service.get_latest_source_cache_selection = lambda **_kwargs: {  # noqa: E731
        "best_bucket_key": "2026-03-29 10",
        "selected_entries": [
            {
                "building": "A楼",
                "file_path": str(actual_file),
                "bucket_key": "2026-03-29 10",
                "duty_date": "2026-03-29",
                "metadata": {"upload_date": "2026-03-29"},
            }
        ],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": True,
    }

    captured = {}

    def _fake_run_monthly_from_file_items(_config, *, file_items, emit_log, source_label):  # noqa: ANN001
        captured["file_items"] = file_items
        captured["source_label"] = source_label
        captured["emit_log"] = emit_log
        return {"ok": True}

    monkeypatch.setattr(routes, "run_monthly_from_file_items", _fake_run_monthly_from_file_items)

    response = routes.job_auto_once(request)
    run_func = request.app.state.container.job_service.start_job_calls[0]["run_func"]
    result = run_func(lambda *_args, **_kwargs: None)

    assert response["job_id"] == "job-1"
    assert result == {"ok": True}
    assert captured["source_label"] == "月报共享文件"
    assert captured["file_items"] == [
        {
            "building": "A楼",
            "file_path": str(actual_file),
            "upload_date": "2026-03-29",
        }
    ]


def test_auto_once_route_allows_fallback_within_three_buckets() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    actual_a = _touch_file(Path.cwd() / ".tmp_monthly_bridge_routes" / "fallback" / "A楼-10.xlsx")
    actual_b = _touch_file(Path.cwd() / ".tmp_monthly_bridge_routes" / "fallback" / "B楼-09.xlsx")
    request.app.state.container.shared_bridge_service.get_latest_source_cache_selection = lambda **_kwargs: {  # noqa: E731
        "best_bucket_key": "2026-03-29 10",
        "selected_entries": [
            {"building": "A楼", "file_path": str(actual_a), "bucket_key": "2026-03-29 10", "duty_date": "2026-03-29", "metadata": {"upload_date": "2026-03-29"}},
            {"building": "B楼", "file_path": str(actual_b), "bucket_key": "2026-03-29 09", "duty_date": "2026-03-29", "metadata": {"upload_date": "2026-03-29"}},
        ],
        "fallback_buildings": ["B楼"],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": True,
    }

    response = routes.job_auto_once(request)

    assert response["job_id"] == "job-1"


def test_auto_once_route_waits_when_fallback_is_stale() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    actual_a = _touch_file(Path.cwd() / ".tmp_monthly_bridge_routes" / "stale" / "A楼-10.xlsx")
    request.app.state.container.shared_bridge_service.get_latest_source_cache_selection = lambda **_kwargs: {  # noqa: E731
        "best_bucket_key": "2026-03-29 10",
        "selected_entries": [
            {"building": "A楼", "file_path": str(actual_a), "bucket_key": "2026-03-29 10", "duty_date": "2026-03-29", "metadata": {"upload_date": "2026-03-29"}},
        ],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": ["B楼"],
        "buildings": [],
        "can_proceed": False,
    }

    response = routes.job_auto_once(request)

    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-monthly-auto-once-1"


def test_auto_once_route_waits_when_best_bucket_is_older_than_three_hours() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    actual_a = _touch_file(Path.cwd() / ".tmp_monthly_bridge_routes" / "too-old" / "A楼-08.xlsx")
    actual_b = _touch_file(Path.cwd() / ".tmp_monthly_bridge_routes" / "too-old" / "B楼-08.xlsx")
    request.app.state.container.shared_bridge_service.get_latest_source_cache_selection = lambda **_kwargs: {  # noqa: E731
        "best_bucket_key": "2026-03-30 08",
        "best_bucket_age_hours": 30.9,
        "is_best_bucket_too_old": True,
        "selected_entries": [
            {"building": "A楼", "file_path": str(actual_a), "bucket_key": "2026-03-30 08", "duty_date": "2026-03-29", "metadata": {"upload_date": "2026-03-29"}},
            {"building": "B楼", "file_path": str(actual_b), "bucket_key": "2026-03-30 08", "duty_date": "2026-03-29", "metadata": {"upload_date": "2026-03-29"}},
        ],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": False,
    }

    response = routes.job_auto_once(request)

    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-monthly-auto-once-1"


def test_multi_date_route_creates_cache_fill_task_when_missing() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)

    response = routes.job_multi_date({"dates": ["2026-03-20", "2026-03-21"]}, request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-monthly-cache-fill-1"
    assert response["job"]["status"] == "waiting_resource"
    assert response["job"]["wait_reason"] == "waiting:shared_bridge"
    assert response["job"]["bridge_task_id"] == "bridge-monthly-cache-fill-1"
    assert (
        "create_monthly_cache_fill_task",
        {"selected_dates": ["2026-03-20", "2026-03-21"], "resume_job_id": "job-waiting-1", "requested_by": "manual"},
    ) in request.app.state.container.shared_bridge_service.calls


def test_multi_date_route_starts_from_shared_cache_when_ready() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    request.app.state.container.shared_bridge_service.by_date_ready = True

    response = routes.job_multi_date({"dates": ["2026-03-20", "2026-03-21"]}, request)

    assert response["job_id"] == "job-1"
    assert request.app.state.container.job_service.start_job_calls[0]["feature"] == "monthly_cache_by_date"
    assert request.app.state.container.job_service.start_job_calls[0]["dedupe_key"].startswith("monthly_cache_by_date:")
    assert '"selected_dates":["2026-03-20","2026-03-21"]' in request.app.state.container.job_service.start_job_calls[0]["dedupe_key"]
    assert '"buildings":["A楼","B楼"]' in request.app.state.container.job_service.start_job_calls[0]["dedupe_key"]


def test_resume_routes_use_bridge_on_external_role() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)

    pending = routes.list_resume_pending(request)
    submit = routes.job_resume_upload({"run_id": "run-1", "auto": False}, request)
    deleted = routes.delete_resume_run({"run_id": "run-1"}, request)

    assert pending["count"] == 1
    assert submit["accepted"] is True
    assert submit["bridge_task"]["task_id"] == "bridge-monthly-resume-1"
    assert submit["job"]["status"] == "waiting_resource"
    assert submit["job"]["wait_reason"] == "waiting:shared_bridge"
    assert submit["job"]["bridge_task_id"] == "bridge-monthly-resume-1"
    assert deleted["ok"] is True
    assert deleted["deleted"] is True
    calls = request.app.state.container.shared_bridge_service.calls
    assert ("list_pending", {}) in calls
    assert (
        "resume_upload",
        {"run_id": "run-1", "auto_trigger": False, "resume_job_id": "job-waiting-1", "requested_by": "manual"},
    ) in calls
    assert ("delete_run", {"run_id": "run-1"}) in calls


def test_resume_pending_returns_empty_list_on_internal_role() -> None:
    request = _fake_request(role_mode="internal", bridge_enabled=True)

    response = routes.list_resume_pending(request)

    assert response == {"runs": [], "count": 0}


def test_auto_once_route_rejects_internal_role() -> None:
    request = _fake_request(role_mode="internal", bridge_enabled=True)
    with pytest.raises(HTTPException) as excinfo:
        routes.job_auto_once(request)
    assert excinfo.value.status_code == 409
