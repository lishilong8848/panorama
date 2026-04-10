from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.modules.report_pipeline.api import routes


class _FakeJob:
    def __init__(self, job_id: str = "job-worker", *, status: str = "queued", summary: str = "ok", wait_reason: str = "", bridge_task_id: str = "") -> None:
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


class _FakeJobService:
    def __init__(self) -> None:
        self.worker_calls = []
        self.start_job_calls = []
        self.cancel_calls = []
        self.retry_calls = []
        self.waiting_calls = []
        self.bind_calls = []
        self.last_waiting_job = None

    def start_worker_job(self, name, *, worker_handler, worker_payload=None, **kwargs):  # noqa: ANN001
        self.worker_calls.append(
            {
                "name": name,
                "worker_handler": worker_handler,
                "worker_payload": worker_payload,
                "kwargs": kwargs,
            }
        )
        return _FakeJob("job-worker")

    def start_job(self, **kwargs):  # noqa: ANN003
        self.start_job_calls.append(dict(kwargs))
        return _FakeJob(f"job-cache-{len(self.start_job_calls)}")

    def get_job(self, job_id):  # noqa: ANN001
        return {"job_id": job_id, "status": "running", "wait_reason": "", "bridge_task_id": ""}

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

    def cancel_job(self, job_id):  # noqa: ANN001
        self.cancel_calls.append(job_id)
        return {"job_id": job_id, "status": "cancelled"}

    def retry_job(self, job_id):  # noqa: ANN001
        self.retry_calls.append(job_id)
        return {"job_id": f"retry-{job_id}", "status": "queued"}


class _FakeSharedBridgeService:
    def __init__(self, *, base_dir: Path | None = None) -> None:
        self.calls = []
        self.buildings = ["A楼", "B楼"]
        self.latest_ready = False
        self.by_date_ready = False
        self.day_metric_ready = False
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
        base_dir = self.base_dir or (Path.cwd() / ".tmp_subprocess_worker_job_routes" / "shared")
        entries = [
            {
                "building": building,
                "file_path": str(_touch_file(base_dir / "source_cache" / "handover" / "latest" / building / "latest.xlsx")),
                "bucket_key": "2026-03-29 10",
                "duty_date": "2026-03-29",
                "duty_shift": "day",
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

    def get_handover_by_date_cache_entries(self, *, duty_date, duty_shift, buildings):  # noqa: ANN001
        self.calls.append(("get_handover_by_date_cache_entries", {"duty_date": duty_date, "duty_shift": duty_shift, "buildings": list(buildings)}))
        if not self.by_date_ready:
            return []
        base_dir = self.base_dir or (Path.cwd() / ".tmp_subprocess_worker_job_routes" / "shared")
        return [
            {
                "building": building,
                "file_path": str(_touch_file(base_dir / "source_cache" / "handover" / "by_date" / duty_date / duty_shift / building / "data.xlsx")),
            }
            for building in buildings
        ]

    def get_handover_capacity_by_date_cache_entries(self, *, duty_date, duty_shift, buildings):  # noqa: ANN001
        self.calls.append(
            (
                "get_handover_capacity_by_date_cache_entries",
                {"duty_date": duty_date, "duty_shift": duty_shift, "buildings": list(buildings)},
            )
        )
        base_dir = self.base_dir or (Path.cwd() / ".tmp_subprocess_worker_job_routes" / "shared")
        if self.latest_ready or self.by_date_ready:
            return [
                {
                    "building": building,
                    "file_path": str(
                        _touch_file(
                            base_dir
                            / "source_cache"
                            / "handover_capacity"
                            / "by_date"
                            / duty_date
                            / duty_shift
                            / building
                            / "capacity.xlsx"
                        )
                    ),
                }
                for building in buildings
            ]
        return []

    def get_day_metric_by_date_cache_entries(self, *, selected_dates, buildings):  # noqa: ANN001
        self.calls.append(("get_day_metric_by_date_cache_entries", {"selected_dates": list(selected_dates), "buildings": list(buildings)}))
        if not self.day_metric_ready:
            return []
        base_dir = self.base_dir or (Path.cwd() / ".tmp_subprocess_worker_job_routes" / "shared")
        output = []
        for duty_date in selected_dates:
            for building in buildings:
                output.append(
                    {
                        "building": building,
                        "duty_date": duty_date,
                        "file_path": str(_touch_file(base_dir / "source_cache" / "handover" / "by_date" / duty_date / "all" / building / "data.xlsx")),
                    }
                )
        return output

    def create_handover_cache_fill_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_handover_cache_fill_task", dict(kwargs)))
        return {
            "task_id": "bridge-cache-fill-1",
            "feature": "handover_cache_fill",
            "status": "queued_for_internal",
            "request": dict(kwargs),
        }

    def create_handover_from_download_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_handover_from_download_task", dict(kwargs)))
        return {
            "task_id": "bridge-handover-latest-1",
            "feature": "handover_from_download",
            "status": "queued_for_internal",
            "request": dict(kwargs),
        }

    def get_or_create_handover_from_download_task(self, **kwargs):  # noqa: ANN003
        return self.create_handover_from_download_task(**kwargs)


def _fake_request(*, role_mode: str = "switching", bridge_enabled: bool = False, base_dir: Path | None = None):
    bridge_service = _FakeSharedBridgeService(base_dir=base_dir)
    container = SimpleNamespace(
        job_service=_FakeJobService(),
        shared_bridge_service=bridge_service,
        logs=[],
        runtime_config={
            "paths": {},
            "network": {"enable_auto_switch_wifi": True},
            "input": {"buildings": ["A楼", "B楼"]},
            "day_metric_upload": {"enabled": True},
        },
        deployment_snapshot=lambda: {"role_mode": role_mode, "node_id": "node-1", "node_label": ""},
        shared_bridge_snapshot=lambda: {
            "enabled": bridge_enabled,
            "root_dir": "D:/QJPT_Shared" if bridge_enabled else "",
        },
        add_system_log=lambda *_args, **_kwargs: None,
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_handover_from_download_route_prefers_worker_job() -> None:
    request = _fake_request()
    payload = {
        "buildings": ["A楼"],
        "duty_date": "2026-03-26",
        "duty_shift": "day",
    }

    response = routes.job_handover_from_download(payload, request)

    assert response["job_id"] == "job-worker"
    call = request.app.state.container.job_service.worker_calls[0]
    assert call["worker_handler"] == "handover_from_download"
    assert call["worker_payload"] == {
        "buildings": ["A楼"],
        "end_time": None,
        "duty_date": "2026-03-26",
        "duty_shift": "day",
    }


def test_handover_from_download_route_starts_from_latest_cache_on_external_role() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    request.app.state.container.shared_bridge_service.latest_ready = True
    payload = {"buildings": ["A楼", "B楼"]}

    response = routes.job_handover_from_download(payload, request)

    assert response["job_id"] == "job-cache-1"
    assert request.app.state.container.job_service.start_job_calls[0]["feature"] == "handover_cache_continue"
    assert request.app.state.container.job_service.start_job_calls[0]["dedupe_key"].startswith("handover_cache_continue:")
    assert '"mode":"latest"' in request.app.state.container.job_service.start_job_calls[0]["dedupe_key"]
    assert '"bucket_key":"2026-03-29 10"' in request.app.state.container.job_service.start_job_calls[0]["dedupe_key"]
    assert request.app.state.container.job_service.worker_calls == []


def test_handover_from_download_route_uses_cached_file_path_verbatim_on_external_role(monkeypatch) -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    request.app.state.container.shared_bridge_service.latest_ready = True
    actual_file = _touch_file(Path.cwd() / ".tmp_subprocess_worker_job_routes" / "indexed" / "handover" / "actual-A.xlsx")
    request.app.state.container.shared_bridge_service.get_latest_source_cache_selection = lambda **_kwargs: {  # noqa: E731
        "best_bucket_key": "2026-03-29 10",
        "selected_entries": [{"building": "A楼", "file_path": str(actual_file), "bucket_key": "2026-03-29 10", "duty_date": "2026-03-29", "duty_shift": "day"}],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": True,
    }
    payload = {"buildings": ["A楼"]}
    captured = {}

    class _FakeOrchestrator:
        def __init__(self, _config):  # noqa: D401, ANN001
            pass

        def run_handover_from_files(self, *, building_files, capacity_building_files, end_time, duty_date, duty_shift, emit_log):  # noqa: ANN001
            captured["building_files"] = building_files
            captured["capacity_building_files"] = capacity_building_files
            captured["end_time"] = end_time
            captured["duty_date"] = duty_date
            captured["duty_shift"] = duty_shift
            captured["emit_log"] = emit_log
            return {"ok": True}

    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestrator)

    response = routes.job_handover_from_download(payload, request)
    run_func = request.app.state.container.job_service.start_job_calls[0]["run_func"]
    result = run_func(lambda *_args, **_kwargs: None)

    assert response["job_id"] == "job-cache-1"
    assert result == {"ok": True}
    assert captured["building_files"] == [("A楼", str(actual_file))]
    assert captured["capacity_building_files"][0][0] == "A楼"


def test_handover_from_download_route_creates_cache_fill_on_external_role_for_history() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    payload = {
        "buildings": ["A楼"],
        "duty_date": "2026-03-26",
        "duty_shift": "day",
    }

    response = routes.job_handover_from_download(payload, request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-cache-fill-1"
    assert response["job"]["status"] == "waiting_resource"
    assert response["job"]["wait_reason"] == "waiting:shared_bridge"
    assert response["job"]["bridge_task_id"] == "bridge-cache-fill-1"
    assert (
        "create_handover_cache_fill_task",
        {
            "continuation_kind": "handover",
            "buildings": ["A楼"],
            "duty_date": "2026-03-26",
            "duty_shift": "day",
            "selected_dates": None,
            "building_scope": None,
            "building": None,
            "resume_job_id": "job-waiting-1",
            "requested_by": "manual",
        },
    ) in request.app.state.container.shared_bridge_service.calls


def test_handover_from_download_route_rejects_internal_role() -> None:
    request = _fake_request(role_mode="internal", bridge_enabled=True)
    with pytest.raises(HTTPException) as excinfo:
        routes.job_handover_from_download(
            {"buildings": ["A楼"], "duty_date": "2026-03-26", "duty_shift": "day"},
            request,
        )
    assert excinfo.value.status_code == 409


def test_day_metric_from_download_route_prefers_worker_job() -> None:
    request = _fake_request()
    payload = {
        "dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
    }

    response = routes.job_day_metric_from_download(payload, request)

    assert response["job_id"] == "job-worker"
    call = request.app.state.container.job_service.worker_calls[0]
    assert call["worker_handler"] == "day_metric_from_download"
    assert call["worker_payload"] == {
        "selected_dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
    }


def test_alarm_export_route_is_retired() -> None:
    request = _fake_request()

    with pytest.raises(HTTPException) as excinfo:
        routes.job_alarm_export_run(request)
    assert excinfo.value.status_code == 410
    assert excinfo.value.detail == "旧告警导出入口已退役，当前主链为“内网 API 拉取 -> 共享 JSON -> 外网上传”"


def test_multi_date_route_prefers_worker_job() -> None:
    request = _fake_request()
    payload = {"dates": ["2026-03-20", "2026-03-21"]}

    response = routes.job_multi_date(payload, request)

    assert response["job_id"] == "job-worker"
    call = request.app.state.container.job_service.worker_calls[0]
    assert call["worker_handler"] == "multi_date"
    assert call["worker_payload"] == {"selected_dates": ["2026-03-20", "2026-03-21"]}


def test_cancel_job_route_calls_job_service() -> None:
    request = _fake_request()

    response = routes.cancel_job("job-1", request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["job"]["status"] == "cancelled"
    assert request.app.state.container.job_service.cancel_calls == ["job-1"]


def test_cancel_job_route_cancels_bound_bridge_task_first() -> None:
    request = _fake_request(role_mode="external", bridge_enabled=True)
    cancelled_bridge_tasks = []

    request.app.state.container.job_service.get_job = lambda job_id: {  # noqa: E731
        "job_id": job_id,
        "status": "waiting_resource",
        "wait_reason": "waiting:shared_bridge",
        "bridge_task_id": "bridge-cache-fill-1",
    }
    request.app.state.container.shared_bridge_service.cancel_task = lambda task_id: cancelled_bridge_tasks.append(task_id) or {  # noqa: E731
        "ok": True,
        "task_id": task_id,
    }

    response = routes.cancel_job("job-1", request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert request.app.state.container.job_service.cancel_calls == ["job-1"]
    assert cancelled_bridge_tasks == ["bridge-cache-fill-1"]


def test_retry_job_route_calls_job_service() -> None:
    request = _fake_request()

    response = routes.retry_job("job-1", request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["job"]["job_id"] == "retry-job-1"
    assert request.app.state.container.job_service.retry_calls == ["job-1"]


def test_day_metric_retry_unit_route_prefers_worker_job() -> None:
    request = _fake_request()
    payload = {
        "mode": "from_download",
        "duty_date": "2026-03-20",
        "building": "A楼",
        "source_file": "D:/tmp/source.xlsx",
        "stage": "upload",
    }

    response = routes.job_day_metric_retry_unit(payload, request)

    assert response["job_id"] == "job-worker"
    call = request.app.state.container.job_service.worker_calls[0]
    assert call["worker_handler"] == "day_metric_retry_unit"
    assert call["worker_payload"]["building"] == "A楼"
    assert call["worker_payload"]["stage"] == "upload"


def test_day_metric_retry_failed_route_prefers_worker_job() -> None:
    request = _fake_request()

    response = routes.job_day_metric_retry_failed({"mode": "from_file"}, request)

    assert response["job_id"] == "job-worker"
    call = request.app.state.container.job_service.worker_calls[0]
    assert call["worker_handler"] == "day_metric_retry_failed"
    assert call["worker_payload"] == {"mode": "from_file"}
