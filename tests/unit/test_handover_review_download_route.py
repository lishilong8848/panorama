from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote

import pytest
from fastapi import HTTPException

from app.modules.handover_review.api import routes


class _FakeContainer:
    def __init__(self):
        self.logs = []

    def add_system_log(self, message: str) -> None:
        self.logs.append(message)


def _fake_request(container: _FakeContainer):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_handover_review_download_returns_file_response(tmp_path, monkeypatch):
    output_file = tmp_path / "A楼交接班日志.xlsx"
    output_file.write_bytes(b"demo")
    container = _FakeContainer()

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **kwargs: {"session_id": "sess-1", "output_file": str(output_file), "building": "A楼"},
    )

    response = routes.handover_review_download("a", _fake_request(container), session_id="sess-1")

    assert Path(response.path) == output_file
    assert response.media_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert "A楼交接班日志.xlsx" in unquote(response.headers.get("content-disposition", ""))
    assert any("[交接班][下载成品]" in message for message in container.logs)


def test_handover_review_download_rejects_empty_session_id(monkeypatch):
    container = _FakeContainer()

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_download("a", _fake_request(container), session_id="")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "session_id 不能为空"


def test_handover_review_download_uses_requested_session_id(monkeypatch, tmp_path):
    output_file = tmp_path / "A楼交接班日志.xlsx"
    output_file.write_bytes(b"demo")
    container = _FakeContainer()

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")

    calls = []

    def _fake_loader(_service, **kwargs):
        calls.append(kwargs)
        return {"session_id": "sess-old", "output_file": str(output_file), "building": "A楼"}

    monkeypatch.setattr(routes, "_load_target_session_or_404", _fake_loader)

    response = routes.handover_review_download("a", _fake_request(container), session_id="sess-old")

    assert Path(response.path) == output_file
    assert calls == [{"building": "A楼", "session_id": "sess-old"}]


def test_handover_review_download_rejects_missing_output_file(monkeypatch, tmp_path):
    output_file = tmp_path / "missing.xlsx"
    container = _FakeContainer()

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **kwargs: {"session_id": "sess-1", "output_file": str(output_file), "building": "A楼"},
    )

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_download("a", _fake_request(container), session_id="sess-1")

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "最新交接班文件不存在，请重新生成"
