from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.modules.handover_review.api import routes


class _FakeContainer:
    def __init__(self):
        self.logs = []
        self.config_path = ""
        self.runtime_config = {"handover_log": {"review_ui": {}}, "paths": {}, "network": {}, "download": {}}
        self.job_service = SimpleNamespace(start_job=self._start_job)

    def add_system_log(self, message: str) -> None:
        self.logs.append(message)

    @staticmethod
    def _start_job(**kwargs):
        return SimpleNamespace(
            job_id="job-1",
            to_dict=lambda: {"job_id": "job-1", "name": kwargs.get("name", ""), "status": "queued"},
        )


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


def test_handover_review_capacity_download_waits_for_active_xlsx_queue(tmp_path, monkeypatch):
    output_file = tmp_path / "A楼交接班容量报表.xlsx"
    output_file.write_bytes(b"demo")
    container = _FakeContainer()
    queue_calls = []
    session = {
        "session_id": "sess-1",
        "capacity_output_file": str(output_file),
        "capacity_sync": {"status": "ready"},
        "building": "A楼",
    }

    class _Queue:
        def __init__(self):
            self.active_checks = 0

        def has_active_write_jobs(self, *, building):
            self.active_checks += 1
            queue_calls.append(("active", building, self.active_checks))
            return self.active_checks == 1

        def wait_for_barrier(self, *, building, timeout_sec):
            queue_calls.append(("barrier", building, timeout_sec))
            return {"status": "success"}

    queue = _Queue()
    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda _service, **kwargs: dict(session))
    monkeypatch.setattr(routes, "_ensure_capacity_overlay_queue_drained_for_session", lambda **_kwargs: dict(session))
    monkeypatch.setattr(routes, "_build_xlsx_write_queue_service", lambda *_args, **_kwargs: queue)

    response = routes.handover_review_capacity_download("a", _fake_request(container), session_id="sess-1")

    assert Path(response.path) == output_file
    assert queue_calls == [
        ("active", "A楼", 1),
        ("barrier", "A楼", 120.0),
        ("active", "A楼", 2),
    ]
    assert any("继续等待" in message for message in container.logs)


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


def test_capacity_overlay_clean_fence_rejects_active_110kv_dirty(monkeypatch):
    class _FakeReviewService:
        def __init__(self):
            self.cleared_calls = 0
            self.lock_checks = 0

        def clear_expired_substation_110kv_dirty(self, *, batch_key: str, client_id: str = "") -> dict:
            self.cleared_calls += 1
            return {"expired_dirty_cleared": False}

        def get_substation_110kv_lock(self, *, batch_key: str, client_id: str = "") -> dict:
            self.lock_checks += 1
            return {
                "dirty": True,
                "dirty_at": "2026-04-26 21:00:00",
                "dirty_by_building": "B楼",
                "active_editor": {"holder_building": "B楼"},
            }

    logs: list[str] = []
    service = _FakeReviewService()

    with pytest.raises(HTTPException) as exc_info:
        routes._ensure_substation_110kv_clean_or_409(
            service,
            batch_key="2026-04-26|day",
            client_id="client-a",
            emit_log=logs.append,
            wait_sec=0.01,
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "110KV变电站正在自动保存，请稍后重试"
    assert service.cleared_calls == 1
    assert service.lock_checks >= 1
    assert any("等待dirty状态清除超时" in line for line in logs)


def test_capacity_overlay_clean_fence_allows_clean_lock() -> None:
    class _FakeReviewService:
        def clear_expired_substation_110kv_dirty(self, *, batch_key: str, client_id: str = "") -> dict:
            return {"expired_dirty_cleared": False}

        def get_substation_110kv_lock(self, *, batch_key: str, client_id: str = "") -> dict:
            return {"dirty": False, "client_holds_lock": False}

    routes._ensure_substation_110kv_clean_or_409(
        _FakeReviewService(),
        batch_key="2026-04-26|day",
        client_id="client-a",
        emit_log=lambda _message: None,
        wait_sec=0.01,
    )


def test_handover_review_capacity_image_send_returns_sync_result(monkeypatch, tmp_path):
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

    class _FakeDeliveryService:
        def __init__(self, *_args, **_kwargs):
            pass

        def begin_delivery(self, _session, *, building: str, source: str = "manual"):
            assert building == "A楼"
            assert source == "manual"
            return {"status": "sending"}

        def mark_failed(self, *, session_id: str, error: str, source: str = "manual"):
            raise AssertionError("mark_failed should not be called")

        def send_for_session(self, session, *, building: str, source: str = "manual", emit_log):
            emit_log("[交接班][容量表图片发送] fake done")
            return {
                "ok": True,
                "status": "success",
                "building": building,
                "session_id": session["session_id"],
                "capacity_image_delivery": {"status": "success"},
            }

    monkeypatch.setattr(routes, "CapacityReportImageDeliveryService", _FakeDeliveryService)

    response = routes.handover_review_capacity_image_send(
        "a",
        _fake_request(container),
        payload={"session_id": "sess-1"},
    )

    assert response["ok"] is True
    assert response["status"] == "success"
    assert any("同步接口已命中" in message for message in container.logs)
    assert any("fake done" in message for message in container.logs)


def test_handover_review_capacity_image_send_http_route_does_not_return_job(monkeypatch, tmp_path):
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

    class _FakeDeliveryService:
        def __init__(self, *_args, **_kwargs):
            pass

        def begin_delivery(self, _session, *, building: str, source: str = "manual"):
            return {"status": "sending"}

        def mark_failed(self, *, session_id: str, error: str, source: str = "manual"):
            raise AssertionError("mark_failed should not be called")

        def send_for_session(self, session, *, building: str, source: str = "manual", emit_log):
            emit_log("[交接班][容量表图片发送] fake http done")
            return {
                "ok": True,
                "status": "success",
                "building": building,
                "session_id": session["session_id"],
                "successful_recipients": ["ou_1"],
                "failed_recipients": [],
                "capacity_image_delivery": {"status": "success"},
                "review_link_delivery": {"status": "success"},
            }

    monkeypatch.setattr(routes, "CapacityReportImageDeliveryService", _FakeDeliveryService)
    app = FastAPI()
    app.include_router(routes.router)
    app.state.container = container

    response = TestClient(app).post(
        "/api/handover/review/a/capacity-image/send",
        json={"session_id": "sess-1", "client_id": "client-1"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["status"] == "success"
    assert "accepted" not in payload
    assert "job" not in payload
    assert "job_id" not in payload
    assert any("同步接口已命中" in message for message in container.logs)
    assert any("fake http done" in message for message in container.logs)


def test_handover_review_capacity_image_send_rejects_when_already_sending(monkeypatch, tmp_path):
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
            "capacity_image_delivery": {"status": "sending"},
            "building": "A楼",
        },
    )

    class _FakeDeliveryService:
        def __init__(self, *_args, **_kwargs):
            pass

        def begin_delivery(self, _session, *, building: str, source: str = "manual"):
            raise ValueError("容量表图片正在发送中，请等待发送完成")

    monkeypatch.setattr(routes, "CapacityReportImageDeliveryService", _FakeDeliveryService)

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_capacity_image_send(
            "a",
            _fake_request(container),
            payload={"session_id": "sess-1"},
        )

    assert exc_info.value.status_code == 409
    assert "正在发送中" in str(exc_info.value.detail)
