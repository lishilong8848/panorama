from __future__ import annotations

import asyncio
import json
import socket
import threading
from datetime import datetime

import pytest

from app.modules.shared_bridge.service import alarm_api_session_adapter as adapter
from app.modules.shared_bridge.service.alarm_event_page_export_service import stream_alarm_event_json_document
from app.modules.shared_bridge.service.report_api_session import BmsReportApiSession, ReportApiError


SITE = {"host": "https://bms.example.invalid", "username": "fake-user", "password": "fake-password"}
NOW = datetime(2026, 1, 3, 12)
OPTIONS = {"source_family": "alarm_event_family", "building": "A楼", "bucket_kind": "manual", "bucket_key": "test", "now": NOW}


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status_code = status
        self.text = "fake response"

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class FakeCookies:
    def __init__(self):
        self.values = {}

    def set(self, name, value, **kwargs):
        self.values[name] = (value, kwargs)


class FakeHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.cookies = FakeCookies()
        self.trust_env = True
        self.closed = False

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        response = self.responses.pop(0)
        return response() if callable(response) else response

    def close(self):
        self.closed = True


def login_response():
    return FakeResponse({"error_code": "00", "data": {"session": "fake-session", "id": 3}})


def event_response(number):
    return FakeResponse({"error_code": "00", "data": {
        "page": {"total_page": 2},
        "event_list": [{"event_level": number, "content": f"事件{number}", "event_source": "机房/设备"}],
    }})


def count_response():
    return FakeResponse({"error_code": "00", "data": [{"event_level": 1, "count": 2}]})


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    original_connect = socket.socket.connect

    def local_only(sock, address):
        if isinstance(address, tuple) and address[0] in {"127.0.0.1", "::1"}:
            return original_connect(sock, address)
        raise AssertionError("network access forbidden in tests")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("network access forbidden in tests")

    monkeypatch.setattr(socket.socket, "connect", local_only)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)


def install_fake(monkeypatch, *http_sessions):
    sessions = iter(http_sessions)

    def make_session(site):
        return BmsReportApiSession(site, session=next(sessions), encryptor=lambda *_: "fake-encrypted")

    monkeypatch.setattr(adapter, "BmsReportApiSession", make_session)


def test_export_preserves_direct_stream_bytes_pagination_and_auth(monkeypatch, tmp_path):
    http = FakeHttp([login_response(), event_response(1), event_response(2), count_response()])
    install_fake(monkeypatch, http)
    output = tmp_path / "alarm.json"
    logs = []
    result = asyncio.run(adapter.export_alarm_event_json(SITE, output_path=output, emit_log=logs.append, **OPTIONS))

    class DirectResponse:
        ok = True

        def __init__(self, response):
            self.response = response

        async def json(self):
            return self.response.json()

    class DirectContext:
        def __init__(self):
            self.responses = iter([event_response(1), event_response(2), count_response()])
            self.calls = []

        async def post(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return DirectResponse(next(self.responses))

    direct = DirectContext()
    reference = tmp_path / "reference.json"
    direct_result = asyncio.run(stream_alarm_event_json_document(
        direct, base_url=SITE["host"], output_path=reference, **OPTIONS
    ))
    assert output.read_bytes() == reference.read_bytes()
    assert result == direct_result
    assert result["row_count"] == 2 and result["pages_fetched"] == 2
    assert [call[2]["json"] for call in http.calls[1:]] == [call[1]["data"] for call in direct.calls]
    assert [call[2]["json"]["page"] for call in http.calls[1:]] == [
        {"number": 1, "size": 50}, {"number": 2, "size": 50}, {"number": "1", "size": "20"}
    ]
    assert http.calls[0][1].endswith("/api/v3/auth/sso/login")
    assert http.calls[0][2]["json"] == {"text": "fake-encrypted"}
    assert all(call[2]["allow_redirects"] is False for call in http.calls)
    assert http.cookies.values["X_GU_SID"][0] == "fake-session"
    assert http.trust_env is False and http.closed
    assert str(output) in logs[-1] and ".stage" not in logs[-1]
    assert not list(tmp_path.glob(".*.stage*"))


@pytest.mark.parametrize("responses", [
    [FakeResponse({"error_code": "01"})],
    [login_response(), FakeResponse({}, status=401)],
    [login_response(), FakeResponse({"error_code": "01", "error_msg": "failed"})],
    [login_response(), FakeResponse(ValueError("invalid json"))],
    [login_response(), event_response(1), FakeResponse({}, status=500)],
])
def test_failure_preserves_existing_output_and_cleans_up(monkeypatch, tmp_path, responses):
    http = FakeHttp(responses)
    install_fake(monkeypatch, http)
    output = tmp_path / "alarm.json"
    output.write_text("previous", encoding="utf-8")
    with pytest.raises((RuntimeError, ValueError)) as exc:
        asyncio.run(adapter.export_alarm_event_json(SITE, output_path=output, **OPTIONS))
    assert "fake-password" not in str(exc.value)
    assert output.read_text(encoding="utf-8") == "previous"
    assert http.closed and not list(tmp_path.glob(".*.stage*"))


def test_publish_failure_does_not_log_completion(monkeypatch, tmp_path):
    http = FakeHttp([login_response(), event_response(1), event_response(2), count_response()])
    install_fake(monkeypatch, http)
    output = tmp_path / "existing-directory"
    output.mkdir()
    logs = []
    with pytest.raises(OSError):
        asyncio.run(adapter.export_alarm_event_json(SITE, output_path=output, emit_log=logs.append, **OPTIONS))
    assert output.is_dir() and http.closed
    assert not any("告警 API 拉取完成" in message for message in logs)
    assert not list(tmp_path.glob(".*.stage*"))


def test_count_failure_remains_best_effort(monkeypatch, tmp_path):
    http = FakeHttp([login_response(), event_response(1), event_response(2), FakeResponse({}, status=500)])
    install_fake(monkeypatch, http)
    output = tmp_path / "alarm.json"
    result = asyncio.run(adapter.export_alarm_event_json(SITE, output_path=output, **OPTIONS))
    assert result["count_summary"] == {}
    assert json.loads(output.read_text(encoding="utf-8"))["row_count"] == 2
    assert http.closed


@pytest.mark.parametrize("cancel_stage", ["login", "event", "count"])
def test_cancel_waits_for_active_request_then_closes_and_cleans(monkeypatch, tmp_path, cancel_stage):
    entered = threading.Event()
    release = threading.Event()

    def blocked():
        entered.set()
        assert release.wait(5)
        assert not http.closed
        return {"login": login_response, "event": lambda: event_response(1), "count": count_response}[cancel_stage]()

    responses = {
        "login": [blocked],
        "event": [login_response(), blocked],
        "count": [login_response(), event_response(1), event_response(2), blocked],
    }
    http = FakeHttp(responses[cancel_stage])
    install_fake(monkeypatch, http)
    output = tmp_path / "alarm.json"
    output.write_text("previous", encoding="utf-8")

    async def run():
        task = asyncio.create_task(adapter.export_alarm_event_json(SITE, output_path=output, **OPTIONS))
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            task.cancel()
            await asyncio.sleep(0.05)
            assert not task.done() and not http.closed
            task.cancel()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert http.closed and output.read_text(encoding="utf-8") == "previous"
    assert not list(tmp_path.glob(".*.stage*"))


def test_distinct_exports_use_distinct_sessions(monkeypatch, tmp_path):
    first = FakeHttp([login_response(), event_response(1), event_response(2), count_response()])
    second = FakeHttp([login_response(), event_response(1), event_response(2), count_response()])
    install_fake(monkeypatch, first, second)

    async def run():
        return await asyncio.gather(*(
            adapter.export_alarm_event_json(SITE, output_path=tmp_path / f"{index}.json", **OPTIONS)
            for index in range(2)
        ))

    results = asyncio.run(run())
    assert [result["row_count"] for result in results] == [2, 2]
    assert first.closed and second.closed and first.cookies is not second.cookies


def test_request_adapter_rejects_other_origins_and_query(monkeypatch):
    http = FakeHttp([])
    session = BmsReportApiSession(SITE, session=http, encryptor=lambda *_: "unused")
    context = adapter._AsyncRequestContext(session)
    for url in ("https://other.example.invalid/api", "https://bms.example.invalid/api?x=1"):
        with pytest.raises(ReportApiError):
            asyncio.run(context.post(url, data={}))
    assert not http.calls
    session.close()
