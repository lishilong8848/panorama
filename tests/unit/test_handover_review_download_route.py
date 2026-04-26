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
    document_state_calls = []
    queue_calls = []

    class _DocumentState:
        def ensure_document_for_session(self, session):
            document_state_calls.append(("ensure", session["session_id"]))

        def attach_excel_sync(self, session):
            document_state_calls.append(("attach", session["session_id"]))
            payload = dict(session)
            payload["excel_sync"] = {"status": "synced", "synced_revision": 1, "pending_revision": 0, "error": ""}
            return payload

    class _Queue:
        def enqueue_review_excel_sync(self, session, *, target_revision):
            queue_calls.append(("enqueue", session["session_id"], target_revision))
            return {"status": "pending"}

        def wait_for_barrier(self, *, building, timeout_sec):
            queue_calls.append(("barrier", building, timeout_sec))
            return {"status": "success"}

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: _DocumentState())
    monkeypatch.setattr(routes, "_build_xlsx_write_queue_service", lambda *_args, **_kwargs: _Queue())
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
    assert document_state_calls == [("ensure", "sess-1"), ("attach", "sess-1")]
    assert queue_calls == [("enqueue", "sess-1", 0), ("barrier", "A楼", 120.0)]
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

    class _DocumentState:
        def ensure_document_for_session(self, _session):
            return None

        def attach_excel_sync(self, session):
            payload = dict(session)
            payload["excel_sync"] = {"status": "synced"}
            return payload

    class _Queue:
        def enqueue_review_excel_sync(self, session, *, target_revision):
            return {"status": "pending"}

        def wait_for_barrier(self, *, building, timeout_sec):
            return {"status": "success"}

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: _DocumentState())
    monkeypatch.setattr(routes, "_build_xlsx_write_queue_service", lambda *_args, **_kwargs: _Queue())
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

    class _DocumentState:
        def ensure_document_for_session(self, _session):
            return None

        def force_sync_session_dict(self, _session, *, reason="manual"):
            raise AssertionError("should not sync when file missing")

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: _DocumentState())
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **kwargs: {"session_id": "sess-1", "output_file": str(output_file), "building": "A楼"},
    )

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_download("a", _fake_request(container), session_id="sess-1")

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "交接班文件不存在，无法同步最新审核内容"
