from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.modules.report_pipeline.api import routes


class _FakeJob:
    def __init__(self, job_id: str = "job-day-metric") -> None:
        self.job_id = job_id

    def to_dict(self) -> dict:
        return {"job_id": self.job_id, "status": "running"}


class _FakeLocalJobService:
    def __init__(self) -> None:
        self.run_func = None
        self.start_kwargs = {}

    def start_job(self, name, run_func, **kwargs):  # noqa: ANN001
        self.run_func = run_func
        self.start_kwargs = {"name": name, **kwargs}
        return _FakeJob()


class _FakeWorkerJobService(_FakeLocalJobService):
    def __init__(self) -> None:
        super().__init__()
        self.worker_called = False
        self.worker_handler = ""
        self.worker_payload = None

    def start_worker_job(self, name, *, worker_handler, worker_payload=None, **kwargs):  # noqa: ANN001
        self.worker_called = True
        self.worker_handler = worker_handler
        self.worker_payload = worker_payload
        self.start_kwargs = {"name": name, **kwargs}
        return _FakeJob()


class _FakeContainer:
    def __init__(
        self,
        runtime_root: Path,
        *,
        role_mode: str = "switching",
        use_worker: bool = True,
    ) -> None:
        self.job_service = _FakeWorkerJobService() if use_worker else _FakeLocalJobService()
        self.logs: list[str] = []
        self.runtime_config = {
            "paths": {"runtime_state_root": str(runtime_root)},
            "network": {"enable_auto_switch_wifi": True},
            "input": {"buildings": ["A楼", "B楼"]},
            "handover_log": {"day_metric_export": {"enabled": True}},
            "day_metric_upload": {"enabled": True},
        }
        self._role_mode = role_mode

    def add_system_log(self, message: str) -> None:
        self.logs.append(str(message))

    def deployment_snapshot(self) -> dict:
        return {"role_mode": self._role_mode}


def _fake_request(container: _FakeContainer):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def _upload(filename: str, content: bytes = b"demo") -> UploadFile:
    return UploadFile(BytesIO(content), filename=filename)


def test_day_metric_from_download_route_runs_locally_without_worker(monkeypatch, tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", use_worker=False)
    captured = {}
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)

    class _FakeOrchestrator:
        def __init__(self, config):  # noqa: ANN001
            captured["config"] = config

        def run_day_metric_from_download(self, *, selected_dates, building_scope, building, emit_log):  # noqa: ANN001
            captured["selected_dates"] = selected_dates
            captured["building_scope"] = building_scope
            captured["building"] = building
            return {"status": "ok"}

    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestrator)

    response = routes.job_day_metric_from_download(
        {"dates": ["2026-03-20", "2026-03-21"], "building_scope": "single", "building": "A楼"},
        _fake_request(container),
    )

    assert response["job_id"] == "job-day-metric"
    result = container.job_service.run_func(lambda _msg: None)
    assert result["status"] == "ok"
    assert captured["selected_dates"] == ["2026-03-20", "2026-03-21"]
    assert captured["building_scope"] == "single"
    assert captured["building"] == "A楼"


def test_day_metric_from_download_route_rejects_future_date(tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime")

    try:
        routes.job_day_metric_from_download(
            {"dates": ["2099-03-20"], "building_scope": "single", "building": "A楼"},
            _fake_request(container),
        )
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "不允许未来日期" in str(exc.detail)
    else:
        raise AssertionError("expected HTTPException")


def test_day_metric_from_file_route_uses_runtime_temp_and_keeps_file_for_retry(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    container = _FakeContainer(runtime_root, use_worker=False)
    captured = {}
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)

    class _FakeOrchestrator:
        def __init__(self, config):  # noqa: ANN001
            captured["config"] = config

        def run_day_metric_from_file(self, *, building, duty_date, file_path, emit_log):  # noqa: ANN001
            captured["building"] = building
            captured["duty_date"] = duty_date
            captured["file_path"] = file_path
            return {"status": "ok"}

    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestrator)

    response = asyncio.run(
        routes.job_day_metric_from_file(
            _fake_request(container),
            building="A楼",
            duty_date="2026-03-24",
            file=_upload("day_metric.xlsx"),
        )
    )

    assert response["job_id"] == "job-day-metric"
    result = container.job_service.run_func(lambda _msg: None)
    assert result["status"] == "ok"

    temp_path = Path(str(captured["file_path"]))
    assert temp_path.is_relative_to(runtime_root / "temp" / "day_metric_from_file")
    assert temp_path.exists()


def test_day_metric_retry_unit_route_runs_locally_without_worker(monkeypatch, tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", use_worker=False)
    captured = {}
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)

    class _FakeOrchestrator:
        def __init__(self, config):  # noqa: ANN001
            captured["config"] = config

        def retry_day_metric_unit(self, *, mode, duty_date, building, source_file, stage, emit_log):  # noqa: ANN001
            captured["mode"] = mode
            captured["duty_date"] = duty_date
            captured["building"] = building
            captured["source_file"] = source_file
            captured["stage"] = stage
            return {"status": "ok"}

    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestrator)

    response = routes.job_day_metric_retry_unit(
        {
          "mode": "from_download",
          "duty_date": "2026-03-24",
          "building": "A楼",
          "source_file": "D:/runtime/source.xlsx",
          "stage": "attachment",
        },
        _fake_request(container),
    )

    assert response["job_id"] == "job-day-metric"
    result = container.job_service.run_func(lambda _msg: None)
    assert result["status"] == "ok"
    assert captured["mode"] == "from_download"
    assert captured["duty_date"] == "2026-03-24"
    assert captured["building"] == "A楼"
    assert captured["source_file"] == "D:/runtime/source.xlsx"
    assert captured["stage"] == "attachment"


def test_day_metric_retry_failed_route_runs_locally_without_worker(monkeypatch, tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", use_worker=False)
    captured = {}
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)

    class _FakeOrchestrator:
        def __init__(self, config):  # noqa: ANN001
            captured["config"] = config

        def retry_day_metric_failed(self, *, mode, emit_log):  # noqa: ANN001
            captured["mode"] = mode
            return {"status": "ok"}

    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestrator)

    response = routes.job_day_metric_retry_failed(
        {"mode": "from_download"},
        _fake_request(container),
    )

    assert response["job_id"] == "job-day-metric"
    result = container.job_service.run_func(lambda _msg: None)
    assert result["status"] == "ok"
    assert captured["mode"] == "from_download"


def test_day_metric_from_download_route_prefers_worker_job_when_available(tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", use_worker=True)

    response = routes.job_day_metric_from_download(
        {"dates": ["2026-03-20"], "building_scope": "single", "building": "A楼"},
        _fake_request(container),
    )

    assert response["job_id"] == "job-day-metric"
    assert container.job_service.worker_called is True
    assert container.job_service.worker_handler == "day_metric_from_download"
    assert container.job_service.worker_payload == {
        "selected_dates": ["2026-03-20"],
        "building_scope": "single",
        "building": "A楼",
    }


def test_day_metric_from_file_route_rejects_internal_role(tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", role_mode="internal")

    try:
        asyncio.run(
            routes.job_day_metric_from_file(
                _fake_request(container),
                building="A楼",
                duty_date="2026-03-24",
                file=_upload("day_metric.xlsx"),
            )
        )
    except HTTPException as exc:
        assert exc.status_code == 409
        assert exc.detail == "当前为内网端角色，请在外网端执行12项本地文件补录"
    else:
        raise AssertionError("expected HTTPException")


def test_day_metric_retry_unit_route_rejects_internal_role(tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", role_mode="internal")

    try:
        routes.job_day_metric_retry_unit(
            {
                "mode": "from_download",
                "duty_date": "2026-03-24",
                "building": "A楼",
                "source_file": "D:/runtime/source.xlsx",
                "stage": "attachment",
            },
            _fake_request(container),
        )
    except HTTPException as exc:
        assert exc.status_code == 409
        assert exc.detail == "当前为内网端角色，请在外网端执行12项单元重试"
    else:
        raise AssertionError("expected HTTPException")


def test_day_metric_retry_failed_route_rejects_internal_role(tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", role_mode="internal")

    try:
        routes.job_day_metric_retry_failed({"mode": "from_download"}, _fake_request(container))
    except HTTPException as exc:
        assert exc.status_code == 409
        assert exc.detail == "当前为内网端角色，请在外网端执行12项失败重试"
    else:
        raise AssertionError("expected HTTPException")
