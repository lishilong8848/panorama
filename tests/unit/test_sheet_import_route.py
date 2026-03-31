from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.modules.sheet_import.api import routes


class _FakeJob:
    def __init__(self, job_id: str = "job-1") -> None:
        self.job_id = job_id

    def to_dict(self) -> dict:
        return {"job_id": self.job_id, "status": "running"}


class _FakeJobService:
    def __init__(self) -> None:
        self.run_func = None
        self.start_kwargs = {}

    def start_job(self, name, run_func, **kwargs):  # noqa: ANN001
        self.run_func = run_func
        self.start_kwargs = kwargs
        return _FakeJob()


class _FakeContainer:
    def __init__(self, runtime_root: Path, *, role_mode: str = "switching") -> None:
        self.job_service = _FakeJobService()
        self.logs: list[str] = []
        self.runtime_config = {"paths": {"runtime_state_root": str(runtime_root)}}
        self._role_mode = role_mode

    def add_system_log(self, message: str) -> None:
        self.logs.append(message)

    def deployment_snapshot(self) -> dict:
        return {"role_mode": self._role_mode}


def _fake_request(container: _FakeContainer):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def _upload(filename: str, content: bytes = b"demo") -> UploadFile:
    return UploadFile(BytesIO(content), filename=filename)


def test_sheet_import_route_uses_runtime_temp_and_cleans_up(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    container = _FakeContainer(runtime_root)
    captured: dict[str, object] = {}
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)

    class _FakeSheetImportService:
        def __init__(self, config):  # noqa: ANN001
            captured["config"] = config

        def run(self, file_path, switch_external_before_upload, emit_log):  # noqa: ANN001, ARG002
            captured["file_path"] = file_path
            captured["switch_external_before_upload"] = switch_external_before_upload
            return {"status": "ok", "failed_count": 0}

    monkeypatch.setattr(routes, "SheetImportService", _FakeSheetImportService)

    response = asyncio.run(
        routes.run_sheet_import(
            _fake_request(container),
            legacy_switch_external_before_upload=True,
            file=_upload("sheet-import.xlsx"),
        )
    )

    assert response["job_id"] == "job-1"
    result = container.job_service.run_func(lambda _message: None)
    assert result["status"] == "ok"

    temp_path = Path(str(captured["file_path"]))
    assert temp_path.is_relative_to(runtime_root / "temp" / "sheet_import")
    assert captured["switch_external_before_upload"] is True
    assert not temp_path.exists()


def test_sheet_import_route_rejects_internal_role(tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", role_mode="internal")

    try:
        asyncio.run(
            routes.run_sheet_import(
                _fake_request(container),
                legacy_switch_external_before_upload=False,
                file=_upload("sheet-import.xlsx"),
            )
        )
    except HTTPException as exc:
        assert exc.status_code == 409
        assert exc.detail == "当前为内网端角色，请在外网端执行 5Sheet 导入"
    else:
        raise AssertionError("expected HTTPException")
