from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

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
        self.buildings = ["A楼", "B楼"]
        self.cache_ready = False
        self.base_dir = base_dir
        self.fill_history_ready = False

    def get_source_cache_buildings(self):
        self.calls.append(("get_source_cache_buildings", {}))
        return list(self.buildings)

    def get_day_metric_by_date_cache_entries(self, *, selected_dates, buildings):
        self.calls.append(("get_day_metric_by_date_cache_entries", {"selected_dates": list(selected_dates), "buildings": list(buildings)}))
        if not self.cache_ready:
            return []
        base_dir = self.base_dir or (Path.cwd() / ".tmp_day_metric_bridge_routes" / "shared")
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

    def fill_day_metric_history(self, *, selected_dates, building_scope, building, emit_log):  # noqa: ANN001
        self.calls.append(
            (
                "fill_day_metric_history",
                {
                    "selected_dates": list(selected_dates),
                    "building_scope": building_scope,
                    "building": building,
                },
            )
        )
        if self.fill_history_ready:
            self.cache_ready = True
        return self.get_day_metric_by_date_cache_entries(
            selected_dates=selected_dates,
            buildings=[building] if building_scope == "single" and building else self.buildings,
        )

    def create_day_metric_from_download_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_day_metric_from_download_task", dict(kwargs)))
        return {
            "task_id": "bridge-day-metric-from-download-1",
            "feature": "day_metric_from_download",
            "status": "queued_for_internal",
        }

    def get_or_create_day_metric_from_download_task(self, **kwargs):  # noqa: ANN003
        return self.create_day_metric_from_download_task(**kwargs)


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


def _fake_request(*, ready: bool = False, base_dir: Path | None = None):
    bridge_service = _FakeBridgeService(base_dir=base_dir)
    bridge_service.cache_ready = ready
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
        deployment_snapshot=lambda: {"role_mode": "external", "node_id": "node-ext-01", "node_label": ""},
        shared_bridge_snapshot=lambda: {"enabled": True, "root_dir": "D:/QJPT_Shared"},
        add_system_log=lambda *_args, **_kwargs: None,
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_day_metric_from_download_route_creates_cache_fill_task_when_missing() -> None:
    request = _fake_request(ready=False)
    payload = {
        "dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
    }

    response = routes.job_day_metric_from_download(payload, request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-day-metric-from-download-1"
    assert response["job"]["status"] == "waiting_resource"
    assert response["job"]["wait_reason"] == "waiting:shared_bridge"
    assert response["job"]["bridge_task_id"] == "bridge-day-metric-from-download-1"
    assert ("create_day_metric_from_download_task", {
        "selected_dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
        "resume_job_id": "job-waiting-1",
        "requested_by": "manual",
    }) in request.app.state.container.shared_bridge_service.calls


def test_day_metric_from_download_route_starts_from_shared_cache_when_ready() -> None:
    request = _fake_request(ready=True)
    payload = {
        "dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
    }

    response = routes.job_day_metric_from_download(payload, request)

    assert response["job_id"] == "job-1"
    assert request.app.state.container.job_service.start_job_calls[0]["feature"] == "day_metric_cache_by_date"
    assert request.app.state.container.job_service.start_job_calls[0]["dedupe_key"].startswith("day_metric_cache_by_date:")
    assert '"selected_dates":["2026-03-20"]' in request.app.state.container.job_service.start_job_calls[0]["dedupe_key"]
    assert '"building":"A楼"' in request.app.state.container.job_service.start_job_calls[0]["dedupe_key"]
    assert request.app.state.container.job_service.worker_calls == []


def test_day_metric_from_download_route_fills_day_cache_from_existing_handover_files_before_waiting() -> None:
    request = _fake_request(ready=False)
    request.app.state.container.shared_bridge_service.fill_history_ready = True
    payload = {
        "dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
    }

    response = routes.job_day_metric_from_download(payload, request)

    assert response["job_id"] == "job-1"
    assert ("fill_day_metric_history", {
        "selected_dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
    }) in request.app.state.container.shared_bridge_service.calls
    assert request.app.state.container.job_service.start_job_calls[0]["feature"] == "day_metric_cache_by_date"
    assert request.app.state.container.job_service.waiting_calls == []


def test_day_metric_from_download_route_creates_cache_fill_when_indexed_file_is_missing() -> None:
    request = _fake_request(ready=True)
    payload = {
        "dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
    }
    request.app.state.container.shared_bridge_service.get_day_metric_by_date_cache_entries = lambda **_kwargs: [  # noqa: E731
        {
            "building": "A楼",
            "duty_date": "2026-03-20",
            "file_path": str(Path.cwd() / ".tmp_day_metric_bridge_routes" / "missing" / "A楼-20260320.xlsx"),
        }
    ]

    response = routes.job_day_metric_from_download(payload, request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-day-metric-from-download-1"


def test_day_metric_from_download_route_creates_cache_fill_when_indexed_file_is_inaccessible(monkeypatch) -> None:
    request = _fake_request(ready=True)
    payload = {
        "dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
    }
    actual_file = _touch_file(Path.cwd() / ".tmp_day_metric_bridge_routes" / "indexed" / "A楼-20260320.xlsx")
    request.app.state.container.shared_bridge_service.get_day_metric_by_date_cache_entries = lambda **_kwargs: [  # noqa: E731
        {
            "building": "A楼",
            "duty_date": "2026-03-20",
            "file_path": str(actual_file),
        }
    ]
    monkeypatch.setattr(routes, "is_accessible_cached_file_path", lambda _path: False)

    response = routes.job_day_metric_from_download(payload, request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-day-metric-from-download-1"


def test_day_metric_from_download_route_uses_cached_file_path_verbatim(monkeypatch) -> None:
    request = _fake_request(ready=True)
    payload = {
        "dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
    }

    captured = {}

    class _FakeService:
        def __init__(self, _config):  # noqa: D401, ANN001
            pass

        def continue_from_source_files(
            self,
            *,
            selected_dates,
            buildings,
            source_units,
            building_scope,
            building,
            emit_log,
        ):  # noqa: ANN001
            captured["selected_dates"] = selected_dates
            captured["buildings"] = buildings
            captured["source_units"] = source_units
            captured["building_scope"] = building_scope
            captured["building"] = building
            captured["emit_log"] = emit_log
            return {"ok": True}

    monkeypatch.setattr(routes, "DayMetricStandaloneUploadService", _FakeService)
    actual_file = _touch_file(Path.cwd() / ".tmp_day_metric_bridge_routes" / "indexed" / "A楼-20260320.xlsx")
    request.app.state.container.shared_bridge_service.get_day_metric_by_date_cache_entries = lambda **_kwargs: [  # noqa: E731
        {
            "building": "A楼",
            "duty_date": "2026-03-20",
            "file_path": str(actual_file),
        }
    ]

    response = routes.job_day_metric_from_download(payload, request)
    run_func = request.app.state.container.job_service.start_job_calls[0]["run_func"]
    result = run_func(lambda *_args, **_kwargs: None)

    assert response["job_id"] == "job-1"
    assert result == {"ok": True}
    assert captured["source_units"] == [
        {
            "duty_date": "2026-03-20",
            "building": "A楼",
            "source_file": str(actual_file),
        }
    ]


def test_day_metric_from_download_route_runs_ready_dates_and_waits_missing_dates(monkeypatch) -> None:
    request = _fake_request(ready=False)
    payload = {
        "dates": ["2026-03-19", "2026-03-20"],
        "building_scope": "all_enabled",
        "building": "",
    }

    captured = {}

    class _FakeService:
        def __init__(self, _config):  # noqa: D401, ANN001
            pass

        def continue_from_source_files(
            self,
            *,
            selected_dates,
            buildings,
            source_units,
            building_scope,
            building,
            emit_log,
        ):  # noqa: ANN001
            captured["selected_dates"] = list(selected_dates)
            captured["buildings"] = list(buildings)
            captured["source_units"] = list(source_units)
            captured["building_scope"] = building_scope
            captured["building"] = building
            captured["emit_log"] = emit_log
            return {"ok": True}

    monkeypatch.setattr(routes, "DayMetricStandaloneUploadService", _FakeService)
    ready_dir = Path.cwd() / ".tmp_day_metric_bridge_routes" / "partial-ready"
    request.app.state.container.shared_bridge_service.get_day_metric_by_date_cache_entries = lambda **_kwargs: [  # noqa: E731
        {
            "building": "A楼",
            "duty_date": "2026-03-19",
            "file_path": str(_touch_file(ready_dir / "2026-03-19" / "A楼.xlsx")),
        },
        {
            "building": "B楼",
            "duty_date": "2026-03-19",
            "file_path": str(_touch_file(ready_dir / "2026-03-19" / "B楼.xlsx")),
        },
    ]

    response = routes.job_day_metric_from_download(payload, request)
    run_func = request.app.state.container.job_service.start_job_calls[0]["run_func"]
    result = run_func(lambda *_args, **_kwargs: None)

    assert response["job_id"] == "job-1"
    assert response["partial_waiting"] is True
    assert response["partial_waiting_dates"] == ["2026-03-20"]
    assert response["partial_bridge_task"]["task_id"] == "bridge-day-metric-from-download-1"
    assert response["partial_waiting_job"]["status"] == "waiting_resource"
    assert result == {"ok": True}
    assert captured["selected_dates"] == ["2026-03-19"]
    assert captured["buildings"] == ["A楼", "B楼"]
    assert captured["building_scope"] == "all_enabled"
    assert captured["building"] is None
    assert captured["source_units"] == [
        {
            "duty_date": "2026-03-19",
            "building": "A楼",
            "source_file": str(ready_dir / "2026-03-19" / "A楼.xlsx"),
        },
        {
            "duty_date": "2026-03-19",
            "building": "B楼",
            "source_file": str(ready_dir / "2026-03-19" / "B楼.xlsx"),
        },
    ]
    assert request.app.state.container.job_service.waiting_calls[0]["worker_payload"]["selected_dates"] == ["2026-03-20"]
    assert ("create_day_metric_from_download_task", {
        "selected_dates": ["2026-03-20"],
        "building_scope": "all_enabled",
        "building": None,
        "resume_job_id": "job-waiting-1",
        "requested_by": "manual",
    }) in request.app.state.container.shared_bridge_service.calls
