from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.modules.report_pipeline.api import routes


class _FakeJob:
    def __init__(self, job_id: str) -> None:
        self.job_id = job_id

    def to_dict(self) -> dict:
        return {"job_id": self.job_id, "status": "queued", "summary": "ok"}


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

    def create_handover_cache_fill_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_handover_cache_fill_task", dict(kwargs)))
        return {
            "task_id": "bridge-handover-cache-fill-1",
            "feature": "handover_cache_fill",
            "status": "queued_for_internal",
        }


class _FakeJobService:
    def __init__(self) -> None:
        self.start_job_calls = []
        self.worker_calls = []

    def start_worker_job(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.worker_calls.append((args, kwargs))
        raise AssertionError("external cache mode should not start local worker job")

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
    assert response["bridge_task"]["task_id"] == "bridge-handover-cache-fill-1"
    assert response["job"]["kind"] == "bridge"
    assert ("create_handover_cache_fill_task", {
        "continuation_kind": "day_metric",
        "buildings": None,
        "duty_date": None,
        "duty_shift": None,
        "selected_dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
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
    assert response["bridge_task"]["task_id"] == "bridge-handover-cache-fill-1"


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
    assert response["bridge_task"]["task_id"] == "bridge-handover-cache-fill-1"


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
