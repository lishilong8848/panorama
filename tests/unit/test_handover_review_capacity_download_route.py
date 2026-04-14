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


def test_handover_review_capacity_download_returns_file_response(tmp_path, monkeypatch):
    output_file = tmp_path / "A楼交接班容量报表.xlsx"
    output_file.write_bytes(b"demo")
    container = _FakeContainer()

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **kwargs: {
            "session_id": "sess-1",
            "capacity_output_file": str(output_file),
            "capacity_sync": {"status": "ready"},
            "building": "A楼",
        },
    )

    response = routes.handover_review_capacity_download("a", _fake_request(container), session_id="sess-1")

    assert Path(response.path) == output_file
    assert response.media_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert "A楼交接班容量报表.xlsx" in unquote(response.headers.get("content-disposition", ""))
    assert any("[交接班][下载容量报表]" in message for message in container.logs)


def test_handover_review_capacity_download_rejects_empty_session_id(monkeypatch):
    container = _FakeContainer()

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_capacity_download("a", _fake_request(container), session_id="")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "session_id 不能为空"


def test_handover_review_capacity_download_rejects_missing_capacity_path(monkeypatch):
    container = _FakeContainer()

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **kwargs: {
            "session_id": "sess-1",
            "capacity_output_file": "",
            "capacity_sync": {"status": "ready"},
            "building": "A楼",
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_capacity_download("a", _fake_request(container), session_id="sess-1")

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "当前交接班容量报表尚未生成"


def test_handover_review_capacity_download_rejects_missing_file(monkeypatch, tmp_path):
    output_file = tmp_path / "missing.xlsx"
    container = _FakeContainer()

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **kwargs: {
            "session_id": "sess-1",
            "capacity_output_file": str(output_file),
            "capacity_sync": {"status": "ready"},
            "building": "A楼",
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_capacity_download("a", _fake_request(container), session_id="sess-1")

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "交接班容量报表文件不存在，请重新生成"


def test_handover_review_capacity_download_rejects_when_sync_not_ready(monkeypatch, tmp_path):
    output_file = tmp_path / "A楼交接班容量报表.xlsx"
    output_file.write_bytes(b"demo")
    container = _FakeContainer()

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **kwargs: {
            "session_id": "sess-1",
            "capacity_output_file": str(output_file),
            "capacity_sync": {"status": "pending_input", "error": "容量报表待补写输入不完整"},
            "building": "A楼",
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_capacity_download("a", _fake_request(container), session_id="sess-1")

    assert exc_info.value.status_code == 409
    assert "待补写" in str(exc_info.value.detail)
