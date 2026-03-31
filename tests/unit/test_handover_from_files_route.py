from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.modules.report_pipeline.api import routes


class _FakeJob:
    def __init__(self, job_id: str = "job-1") -> None:
        self.job_id = job_id

    def to_dict(self):
        return {"job_id": self.job_id, "name": "交接班日志-已有文件批量生成", "status": "running"}


class _FakeJobService:
    def __init__(self) -> None:
        self.name = ""
        self.run_func = None
        self.start_kwargs = {}

    def start_job(self, name, run_func, **kwargs):
        self.name = name
        self.run_func = run_func
        self.start_kwargs = kwargs
        return _FakeJob()


class _FakeContainer:
    def __init__(self) -> None:
        self.job_service = _FakeJobService()
        self.logs: list[str] = []
        self.runtime_config = {
            "paths": {"runtime_state_root": ""},
            "input": {"buildings": ["A楼", "B楼", "C楼", "D楼", "E楼"]},
        }

    def add_system_log(self, message: str) -> None:
        self.logs.append(message)


def _fake_request(container: _FakeContainer):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def _upload(filename: str, content: bytes = b"demo") -> UploadFile:
    return UploadFile(BytesIO(content), filename=filename)


def test_handover_from_files_route_submits_batch_job_in_runtime_temp(monkeypatch, tmp_path: Path):
    container = _FakeContainer()
    runtime_root = tmp_path / ".runtime"
    captured = {}
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)

    monkeypatch.setattr(
        routes,
        "_runtime_config",
        lambda _container: {
            "paths": {"runtime_state_root": str(runtime_root)},
            "input": {"buildings": ["A楼", "B楼", "C楼", "D楼", "E楼"]},
        },
    )

    class _FakeOrchestratorService:
        def __init__(self, config):
            captured["config"] = config

        def run_handover_from_files(self, *, building_files, end_time, duty_date, duty_shift, emit_log):  # noqa: ARG002
            captured["building_files"] = building_files
            captured["end_time"] = end_time
            captured["duty_date"] = duty_date
            captured["duty_shift"] = duty_shift
            return {
                "mode": "from_existing_files",
                "success_count": 2,
                "failed_count": 0,
                "selected_buildings": ["A楼", "C楼"],
                "skipped_buildings": ["B楼", "D楼", "E楼"],
                "results": [],
                "errors": [],
            }

    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestratorService)

    response = asyncio.run(
        routes.job_handover_from_files(
            _fake_request(container),
            buildings=["A楼", "C楼"],
            duty_date="2026-03-23",
            duty_shift="day",
            files=[_upload("A.xlsx"), _upload("C.xlsx")],
        )
    )

    assert response["job_id"] == "job-1"
    assert container.job_service.name == "交接班日志-已有文件批量生成"

    result = container.job_service.run_func(lambda _message: None)
    assert result["selected_buildings"] == ["A楼", "C楼"]
    assert captured["duty_date"] == "2026-03-23"
    assert captured["duty_shift"] == "day"

    temp_paths = [Path(item[1]) for item in captured["building_files"]]
    assert [item[0] for item in captured["building_files"]] == ["A楼", "C楼"]
    assert all(path.is_relative_to(runtime_root / "temp" / "handover_from_files") for path in temp_paths)
    assert not any(path.exists() for path in temp_paths)


def test_handover_from_file_route_uses_runtime_temp_and_cleans_up(monkeypatch, tmp_path: Path):
    container = _FakeContainer()
    runtime_root = tmp_path / ".runtime"
    captured = {}
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)

    monkeypatch.setattr(
        routes,
        "_runtime_config",
        lambda _container: {"paths": {"runtime_state_root": str(runtime_root)}},
    )

    class _FakeOrchestratorService:
        def __init__(self, config):
            captured["config"] = config

        def run_handover_from_file(self, *, building, file_path, end_time, duty_date, duty_shift, emit_log):  # noqa: ARG002
            captured["building"] = building
            captured["file_path"] = file_path
            captured["duty_date"] = duty_date
            captured["duty_shift"] = duty_shift
            return {"mode": "from_existing_file", "results": [], "errors": []}

    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestratorService)

    response = asyncio.run(
        routes.job_handover_from_file(
            _fake_request(container),
            building="A楼",
            duty_date="2026-03-23",
            duty_shift="night",
            file=_upload("A.xlsx"),
        )
    )

    assert response["job_id"] == "job-1"
    container.job_service.run_func(lambda _message: None)

    file_path = Path(captured["file_path"])
    assert captured["building"] == "A楼"
    assert captured["duty_date"] == "2026-03-23"
    assert captured["duty_shift"] == "night"
    assert file_path.is_relative_to(runtime_root / "temp" / "handover_from_file")
    assert not file_path.exists()


def test_handover_from_files_route_rejects_mismatched_buildings_and_files():
    container = _FakeContainer()

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            routes.job_handover_from_files(
                _fake_request(container),
                buildings=["A楼"],
                duty_date="2026-03-23",
                duty_shift="day",
                files=[_upload("A.xlsx"), _upload("C.xlsx")],
            )
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "buildings 与 files 数量必须一致"


def test_handover_from_files_route_rejects_empty_selection():
    container = _FakeContainer()

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            routes.job_handover_from_files(
                _fake_request(container),
                buildings=[],
                duty_date="2026-03-23",
                duty_shift="day",
                files=[],
            )
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "请选择至少一个楼的已有数据表文件"


def test_handover_from_files_route_validates_duty_date():
    container = _FakeContainer()

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            routes.job_handover_from_files(
                _fake_request(container),
                buildings=["A楼"],
                duty_date="2026/03/23",
                duty_shift="day",
                files=[_upload("A.xlsx")],
            )
        )

    assert exc_info.value.status_code == 400
    assert "duty_date 格式错误" in exc_info.value.detail
