from __future__ import annotations

import base64
import json

import pytest

from app.modules.shared_bridge.service.report_api_session import (
    BmsReportApiSession,
    ReportApiError,
    encrypt_login_payload,
)


class FakeResponse:
    def __init__(self, *, status=200, payload=None, text=""):
        self.status_code = status
        self._payload = payload
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeCookies:
    def __init__(self):
        self.values = {}

    def set(self, name, value, **_kwargs):
        self.values[name] = value


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.cookies = FakeCookies()
        self.trust_env = True

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def close(self):
        pass


def site():
    return {"building": "A楼", "host": "192.168.1.2", "username": "user", "password": "secret"}


def test_encrypt_login_payload_uses_existing_playwright_node():
    encrypted = encrypt_login_payload("user", "secret")
    assert isinstance(encrypted, str)
    assert len(base64.b64decode(encrypted, validate=True)) % 16 == 0
    assert encrypted == encrypt_login_payload("user", "secret")


def test_login_sets_vendor_session_cookies_without_logging_credentials():
    fake = FakeSession([FakeResponse(payload={"error_code": "00", "data": {
        "session": "private-session", "id": 7, "account": "user", "name": "姓名"
    }})])
    client = BmsReportApiSession(site(), session=fake, encryptor=lambda _u, _p: "encrypted")
    client.login()
    assert fake.trust_env is False
    assert fake.cookies.values["X_GU_SID"] == "private-session"
    assert base64.b64decode(fake.cookies.values["USER_NAME"]).decode() == "姓名"
    assert fake.calls[0][0] == "POST"
    assert fake.calls[0][2]["json"] == {"text": "encrypted"}
    assert fake.calls[0][2]["allow_redirects"] is False


@pytest.mark.parametrize("payload", [{"error_code": "01"}, {"error_code": "00", "data": {}}])
def test_login_failure_is_explicit_and_does_not_expose_credentials(payload):
    client = BmsReportApiSession(site(), session=FakeSession([FakeResponse(payload=payload)]), encryptor=lambda *_: "x")
    with pytest.raises(ReportApiError) as exc:
        client.login()
    assert "secret" not in str(exc.value)


def test_api_request_forbids_external_urls_and_redirects():
    client = BmsReportApiSession(site(), session=FakeSession([]), encryptor=lambda *_: "x")
    with pytest.raises(ReportApiError):
        client.request("GET", "http://other.example/path")
    fake = FakeSession([FakeResponse(status=302)])
    client = BmsReportApiSession(site(), session=fake, encryptor=lambda *_: "x")
    with pytest.raises(ReportApiError):
        client.request("GET", "/api/path")
    assert fake.calls[0][2]["allow_redirects"] is False


def test_network_failure_does_not_include_sensitive_exception_text():
    import requests
    fake = FakeSession([requests.Timeout("secret-token")])
    client = BmsReportApiSession(site(), session=fake, encryptor=lambda *_: "x")
    with pytest.raises(ReportApiError) as exc:
        client.request("GET", "/api/path")
    assert "secret-token" not in str(exc.value)


def test_report_list_is_paginated_and_requires_exact_unique_name():
    fake = FakeSession([
        FakeResponse(payload={"list": [{"plan_name": "其他"}], "total": 2}),
        FakeResponse(payload={"list": [{"plan_name": "目标"}], "total": 2}),
    ])
    client = BmsReportApiSession(site(), session=fake, encryptor=lambda *_: "x")
    result = client.find_report("目标", page_size=1)
    assert result["plan_name"] == "目标"
    assert [call[2]["params"]["page_num"] for call in fake.calls] == [1, 2]


def test_report_list_rejects_duplicate_exact_names_and_truncated_pages():
    fake = FakeSession([FakeResponse(payload={"list": [{"plan_name": "目标"}, {"plan_name": "目标"}], "total": 2})])
    client = BmsReportApiSession(site(), session=fake, encryptor=lambda *_: "x")
    with pytest.raises(ReportApiError, match="重复"):
        client.find_report("目标")
    fake = FakeSession([FakeResponse(payload={"list": [], "total": 2})])
    client = BmsReportApiSession(site(), session=fake, encryptor=lambda *_: "x")
    with pytest.raises(ReportApiError, match="不完整"):
        client.find_report("目标")


def test_validates_site_configuration_without_echoing_secrets():
    with pytest.raises(ReportApiError):
        BmsReportApiSession({**site(), "host": "https://user:secret@example.com"})
    with pytest.raises(ReportApiError):
        BmsReportApiSession({**site(), "password": ""})
