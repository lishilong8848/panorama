from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.modules.report_pipeline.api import routes


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


def test_manual_upload_route_uses_runtime_temp_and_cleans_up(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    container = _FakeContainer(runtime_root)
    captured: dict[str, object] = {}
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)

    class _FakeCalculationService:
        def __init__(self, config):  # noqa: ANN001
            captured["config"] = config

        def run_manual_upload(
            self,
            *,
            building,
            file_path,
            upload_date,
            switch_external_before_upload,
            emit_log,  # noqa: ARG002
        ):
            captured["building"] = building
            captured["file_path"] = file_path
            captured["upload_date"] = upload_date
            captured["switch_external_before_upload"] = switch_external_before_upload
            return {"status": "ok"}

    monkeypatch.setattr(routes, "CalculationService", _FakeCalculationService)

    response = asyncio.run(
        routes.job_manual_upload(
            _fake_request(container),
            building="A楼",
            upload_date="2026-03-23",
            legacy_switch_external_before_upload=True,
            file=_upload("manual.xlsx"),
        )
    )

    assert response["job_id"] == "job-1"
    result = container.job_service.run_func(lambda _message: None)
    assert result["status"] == "ok"

    temp_path = Path(str(captured["file_path"]))
    assert temp_path.is_relative_to(runtime_root / "temp" / "manual_upload")
    assert captured["building"] == "A楼"
    assert captured["upload_date"] == "2026-03-23"
    assert captured["switch_external_before_upload"] is True
    assert not temp_path.exists()


def test_manual_upload_route_rejects_internal_role(tmp_path: Path) -> None:
    container = _FakeContainer(tmp_path / ".runtime", role_mode="internal")

    try:
        asyncio.run(
            routes.job_manual_upload(
                _fake_request(container),
                building="A楼",
                upload_date="2026-03-23",
                legacy_switch_external_before_upload=False,
                file=_upload("manual.xlsx"),
            )
        )
    except HTTPException as exc:
        assert exc.status_code == 409
        assert exc.detail == "当前为内网端角色，请在外网端执行手动补传"
    else:
        raise AssertionError("expected HTTPException")
