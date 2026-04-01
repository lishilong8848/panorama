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
    def __init__(self, *, ready: bool, base_dir: Path | None = None) -> None:
        self.ready = ready
        self.calls = []
        self.buildings = ["A楼", "B楼"]
        self.base_dir = base_dir

    def get_source_cache_buildings(self):
        self.calls.append(("get_source_cache_buildings", {}))
        return list(self.buildings)

    def get_latest_source_cache_selection(self, **kwargs):  # noqa: ANN003
        self.calls.append(("get_latest_source_cache_selection", dict(kwargs)))
        requested_buildings = list(kwargs.get("buildings") or [])
        if not self.ready:
            return {
                "best_bucket_key": "",
                "selected_entries": [],
                "fallback_buildings": [],
                "missing_buildings": requested_buildings,
                "stale_buildings": [],
                "buildings": [],
                "can_proceed": False,
            }
        base_dir = self.base_dir or (Path.cwd() / ".tmp_wet_bulb_bridge_routes" / "shared")
        entries = [
            {
                "building": building,
                "file_path": str(_touch_file(base_dir / "source_cache" / "handover" / "latest" / building / "latest.xlsx")),
                "bucket_key": "2026-03-29 10",
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

    def create_wet_bulb_collection_task(self, **kwargs):  # noqa: ANN003
        self.calls.append(("create_wet_bulb_collection_task", dict(kwargs)))
        return {
            "task_id": "bridge-wet-bulb-1",
            "feature": "wet_bulb_collection",
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


def _fake_request(*, ready: bool, base_dir: Path | None = None):
    bridge_service = _FakeBridgeService(ready=ready, base_dir=base_dir)
    container = SimpleNamespace(
        job_service=_FakeJobService(),
        shared_bridge_service=bridge_service,
        logs=[],
        runtime_config={
            "paths": {},
            "network": {"enable_auto_switch_wifi": True},
            "wet_bulb_collection": {"enabled": True},
        },
        deployment_snapshot=lambda: {"role_mode": "external", "node_id": "node-ext-01", "node_label": ""},
        shared_bridge_snapshot=lambda: {"enabled": True, "root_dir": "D:/QJPT_Shared"},
        add_system_log=lambda *_args, **_kwargs: None,
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_wet_bulb_route_waits_for_latest_cache_when_missing() -> None:
    request = _fake_request(ready=False)

    response = routes.job_wet_bulb_collection_run(request)

    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-wet-bulb-1"


def test_wet_bulb_route_starts_from_latest_cache_on_external_role() -> None:
    request = _fake_request(ready=True)

    response = routes.job_wet_bulb_collection_run(request)

    assert response["job_id"] == "job-1"
    assert request.app.state.container.job_service.start_job_calls[0]["feature"] == "wet_bulb_cache_latest"
    assert request.app.state.container.job_service.start_job_calls[0]["dedupe_key"].startswith("wet_bulb_cache_latest:")
    assert '"bucket_key":"2026-03-29 10"' in request.app.state.container.job_service.start_job_calls[0]["dedupe_key"]
    assert '"buildings":["A楼","B楼"]' in request.app.state.container.job_service.start_job_calls[0]["dedupe_key"]
    assert request.app.state.container.job_service.worker_calls == []


def test_wet_bulb_route_uses_cached_file_path_verbatim_on_external_role(monkeypatch) -> None:
    request = _fake_request(ready=True)
    request.app.state.container.shared_bridge_service.buildings = ["A楼"]
    actual_file = _touch_file(Path.cwd() / ".tmp_wet_bulb_bridge_routes" / "monthly" / "actual-A.xlsx")

    captured = {}

    class _FakeService:
        def __init__(self, _config):  # noqa: D401, ANN001
            pass

        def continue_from_source_units(self, *, source_units, emit_log):  # noqa: ANN001
            captured["source_units"] = source_units
            captured["emit_log"] = emit_log
            return {"ok": True}

    monkeypatch.setattr(routes, "WetBulbCollectionService", _FakeService)
    request.app.state.container.shared_bridge_service.get_latest_source_cache_selection = lambda **_kwargs: {  # noqa: E731
        "best_bucket_key": "2026-03-29 10",
        "selected_entries": [{"building": "A楼", "file_path": str(actual_file), "bucket_key": "2026-03-29 10"}],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": True,
    }

    response = routes.job_wet_bulb_collection_run(request)

    run_func = request.app.state.container.job_service.start_job_calls[0]["run_func"]
    result = run_func(lambda *_args, **_kwargs: None)

    assert response["job_id"] == "job-1"
    assert result == {"ok": True}
    assert captured["source_units"] == [
        {"building": "A楼", "file_path": str(actual_file)}
    ]


def test_wet_bulb_route_waits_when_fallback_is_stale() -> None:
    request = _fake_request(ready=True)
    actual_file = _touch_file(Path.cwd() / ".tmp_wet_bulb_bridge_routes" / "stale" / "A楼.xlsx")
    request.app.state.container.shared_bridge_service.get_latest_source_cache_selection = lambda **_kwargs: {  # noqa: E731
        "best_bucket_key": "2026-03-29 10",
        "selected_entries": [{"building": "A楼", "file_path": str(actual_file), "bucket_key": "2026-03-29 10"}],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": ["B楼"],
        "buildings": [],
        "can_proceed": False,
    }

    response = routes.job_wet_bulb_collection_run(request)

    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-wet-bulb-1"


def test_wet_bulb_route_waits_when_best_bucket_is_older_than_three_hours() -> None:
    request = _fake_request(ready=True)
    actual_a = _touch_file(Path.cwd() / ".tmp_wet_bulb_bridge_routes" / "too-old" / "A楼.xlsx")
    actual_b = _touch_file(Path.cwd() / ".tmp_wet_bulb_bridge_routes" / "too-old" / "B楼.xlsx")
    request.app.state.container.shared_bridge_service.get_latest_source_cache_selection = lambda **_kwargs: {  # noqa: E731
        "best_bucket_key": "2026-03-30 08",
        "best_bucket_age_hours": 30.9,
        "is_best_bucket_too_old": True,
        "selected_entries": [
            {"building": "A楼", "file_path": str(actual_a), "bucket_key": "2026-03-30 08"},
            {"building": "B楼", "file_path": str(actual_b), "bucket_key": "2026-03-30 08"},
        ],
        "fallback_buildings": [],
        "missing_buildings": [],
        "stale_buildings": [],
        "buildings": [],
        "can_proceed": False,
    }

    response = routes.job_wet_bulb_collection_run(request)

    assert response["accepted"] is True
    assert response["bridge_task"]["task_id"] == "bridge-wet-bulb-1"
