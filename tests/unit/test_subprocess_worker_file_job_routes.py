from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.modules.report_pipeline.api import routes as pipeline_routes
from app.modules.sheet_import.api import routes as sheet_routes


class _FakeJob:
    def __init__(self, job_id: str = "job-worker-file") -> None:
        self.job_id = job_id

    def to_dict(self) -> dict:
        return {"job_id": self.job_id, "status": "running"}


class _FakeJobService:
    def __init__(self) -> None:
        self.worker_calls = []
        self.start_job_called = False

    def start_worker_job(self, name, *, worker_handler, worker_payload=None, **kwargs):  # noqa: ANN001
        self.worker_calls.append(
            {
                "name": name,
                "worker_handler": worker_handler,
                "worker_payload": worker_payload,
                "kwargs": kwargs,
            }
        )
        return _FakeJob()

    def start_job(self, *args, **kwargs):  # noqa: ANN001
        self.start_job_called = True
        raise AssertionError("start_job should not be used when start_worker_job is available")


class _FakeContainer:
    def __init__(self, runtime_root: Path, *, role_mode: str = "switching") -> None:
        self.job_service = _FakeJobService()
        self.logs: list[str] = []
        self.runtime_config = {
            "paths": {"runtime_state_root": str(runtime_root)},
            "network": {"enable_auto_switch_wifi": True},
            "input": {"buildings": ["A楼", "B楼"]},
            "day_metric_upload": {"enabled": True},
        }
        self._role_mode = role_mode

    def add_system_log(self, message: str) -> None:
        self.logs.append(message)

    def deployment_snapshot(self) -> dict:
        return {"role_mode": self._role_mode}


def _fake_request(container: _FakeContainer):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def _upload(filename: str, content: bytes = b"demo") -> UploadFile:
    return UploadFile(BytesIO(content), filename=filename)


def test_resume_upload_route_prefers_worker_job(tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime")

    response = pipeline_routes.job_resume_upload(
        {"run_id": "resume-1", "auto": True},
        _fake_request(container),
    )

    assert response["job_id"] == "job-worker-file"
    call = container.job_service.worker_calls[0]
    assert call["worker_handler"] == "resume_upload"
    assert call["worker_payload"] == {"run_id": "resume-1", "auto_trigger": True}


def test_manual_upload_route_prefers_worker_job(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    container = _FakeContainer(runtime_root)
    monkeypatch.setattr(pipeline_routes, "get_app_dir", lambda: tmp_path)

    response = asyncio.run(
        pipeline_routes.job_manual_upload(
            _fake_request(container),
            building="A楼",
            upload_date="2026-03-24",
            legacy_switch_external_before_upload=True,
            file=_upload("manual.xlsx"),
        )
    )

    assert response["job_id"] == "job-worker-file"
    call = container.job_service.worker_calls[0]
    assert call["worker_handler"] == "manual_upload"
    payload = call["worker_payload"]
    assert payload["building"] == "A楼"
    assert payload["upload_date"] == "2026-03-24"
    assert payload["switch_external_before_upload"] is True
    assert Path(payload["file_path"]).exists()
    assert Path(payload["cleanup_dir"]).is_relative_to(runtime_root / "temp" / "manual_upload")


def test_handover_from_file_route_prefers_worker_job(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    container = _FakeContainer(runtime_root)
    monkeypatch.setattr(pipeline_routes, "get_app_dir", lambda: tmp_path)

    response = asyncio.run(
        pipeline_routes.job_handover_from_file(
            _fake_request(container),
            building="A楼",
            duty_date="2026-03-24",
            duty_shift="night",
            file=_upload("handover.xlsx"),
        )
    )

    assert response["job_id"] == "job-worker-file"
    call = container.job_service.worker_calls[0]
    assert call["worker_handler"] == "handover_from_file"
    payload = call["worker_payload"]
    assert payload["building"] == "A楼"
    assert payload["duty_date"] == "2026-03-24"
    assert payload["duty_shift"] == "night"
    assert Path(payload["file_path"]).exists()
    assert Path(payload["cleanup_dir"]).is_relative_to(runtime_root / "temp" / "handover_from_file")


def test_handover_from_files_route_prefers_worker_job(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    container = _FakeContainer(runtime_root)
    monkeypatch.setattr(pipeline_routes, "get_app_dir", lambda: tmp_path)

    response = asyncio.run(
        pipeline_routes.job_handover_from_files(
            _fake_request(container),
            buildings=["A楼", "B楼"],
            duty_date="2026-03-24",
            duty_shift="day",
            files=[_upload("a.xlsx"), _upload("b.xlsx")],
        )
    )

    assert response["job_id"] == "job-worker-file"
    call = container.job_service.worker_calls[0]
    assert call["worker_handler"] == "handover_from_files"
    payload = call["worker_payload"]
    assert payload["duty_date"] == "2026-03-24"
    assert payload["duty_shift"] == "day"
    assert [item["building"] for item in payload["building_files"]] == ["A楼", "B楼"]
    assert all(Path(item["file_path"]).exists() for item in payload["building_files"])
    assert Path(payload["cleanup_dir"]).is_relative_to(runtime_root / "temp" / "handover_from_files")


def test_day_metric_from_file_route_prefers_worker_job(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    container = _FakeContainer(runtime_root)
    monkeypatch.setattr(pipeline_routes, "get_app_dir", lambda: tmp_path)

    response = asyncio.run(
        pipeline_routes.job_day_metric_from_file(
            _fake_request(container),
            building="A楼",
            duty_date="2026-03-24",
            file=_upload("day_metric.xlsx"),
        )
    )

    assert response["job_id"] == "job-worker-file"
    call = container.job_service.worker_calls[0]
    assert call["worker_handler"] == "day_metric_from_file"
    payload = call["worker_payload"]
    assert payload["building"] == "A楼"
    assert payload["duty_date"] == "2026-03-24"
    assert Path(payload["file_path"]).exists()
    assert "cleanup_dir" not in payload


def test_sheet_import_route_prefers_worker_job(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    container = _FakeContainer(runtime_root)
    monkeypatch.setattr(sheet_routes, "get_app_dir", lambda: tmp_path)

    response = asyncio.run(
        sheet_routes.run_sheet_import(
            _fake_request(container),
            legacy_switch_external_before_upload=True,
            file=_upload("sheet-import.xlsx"),
        )
    )

    assert response["job_id"] == "job-worker-file"
    call = container.job_service.worker_calls[0]
    assert call["worker_handler"] == "sheet_import"
    payload = call["worker_payload"]
    assert payload["switch_external_before_upload"] is True
    assert Path(payload["xlsx_path"]).exists()
    assert Path(payload["cleanup_dir"]).is_relative_to(runtime_root / "temp" / "sheet_import")


def test_handover_from_file_route_rejects_internal_role(tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", role_mode="internal")

    try:
        asyncio.run(
            pipeline_routes.job_handover_from_file(
                _fake_request(container),
                building="A楼",
                duty_date="2026-03-24",
                duty_shift="night",
                file=_upload("handover.xlsx"),
            )
        )
    except HTTPException as exc:
        assert exc.status_code == 409
        assert exc.detail == "当前为内网端角色，请在外网端执行交接班已有文件生成"
    else:
        raise AssertionError("expected HTTPException")


def test_handover_from_files_route_rejects_internal_role(tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", role_mode="internal")

    try:
        asyncio.run(
            pipeline_routes.job_handover_from_files(
                _fake_request(container),
                buildings=["A楼", "B楼"],
                duty_date="2026-03-24",
                duty_shift="day",
                files=[_upload("a.xlsx"), _upload("b.xlsx")],
            )
        )
    except HTTPException as exc:
        assert exc.status_code == 409
        assert exc.detail == "当前为内网端角色，请在外网端执行交接班已有文件批量生成"
    else:
        raise AssertionError("expected HTTPException")
