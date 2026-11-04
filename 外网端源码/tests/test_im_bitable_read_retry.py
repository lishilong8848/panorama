import requests
import pytest

from app.modules.feishu.service import im_file_message_client as module


class Response:
    def __init__(self, status, body):
        self.status_code = status
        self.body = body
        self.text = ""

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("request failed")

    def json(self):
        return self.body


def client(monkeypatch, responses):
    instance = module.FeishuImFileMessageClient.__new__(module.FeishuImFileMessageClient)
    instance.request_retry_interval_sec = 1
    monkeypatch.setattr(instance, "refresh_token", lambda **_kwargs: "fake-token")
    sent = []

    def request(method, url, **_kwargs):
        sent.append((method, url))
        return responses.pop(0)

    monkeypatch.setattr(instance, "_request_with_retry", request)
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    return instance, sent


@pytest.mark.parametrize("status", [200, 400])
def test_bitable_directory_data_not_ready_retries_read_only(monkeypatch, status):
    instance, sent = client(monkeypatch, [Response(status, {"code": 1254607}), Response(200, {"code": 0})])
    result = instance._request_json_with_auth_retry("GET", "https://open.feishu.cn/open-apis/bitable/v1/apps/test/tables/test/records")
    assert result["code"] == 0
    assert len(sent) == 2


def test_data_not_ready_does_not_retry_message_post(monkeypatch):
    instance, sent = client(monkeypatch, [Response(400, {"code": 1254607})])
    with pytest.raises(RuntimeError, match="1254607"):
        instance._request_json_with_auth_retry("POST", instance.SEND_MESSAGE_URL, params={"receive_id_type": "chat_id"})
    assert len(sent) == 1


def test_other_bitable_errors_are_not_retried(monkeypatch):
    instance, sent = client(monkeypatch, [Response(400, {"code": 1254040})])
    with pytest.raises(RuntimeError, match="1254040"):
        instance._request_json_with_auth_retry("GET", "https://open.feishu.cn/open-apis/bitable/v1/apps/test/tables/test/records")
    assert len(sent) == 1


def test_bitable_read_exhaustion_reports_attempts_and_original_error(monkeypatch):
    instance, sent = client(monkeypatch, [Response(400, {"code": 1254607, "error": {"log_id": "test-id"}}) for _ in range(4)])
    with pytest.raises(RuntimeError, match="已重试4次.*1254607.*test-id"):
        instance._request_json_with_auth_retry("GET", "https://open.feishu.cn/open-apis/bitable/v1/apps/test/tables/test/records")
    assert len(sent) == 4


def test_recipient_directory_retry_sends_message_once(monkeypatch):
    from app.modules.feishu.service import feishu_recipient_directory as directory_module

    instance, sent = client(monkeypatch, [
        Response(400, {"code": 1254607}),
        Response(200, {"code": 0, "data": {}}),
        Response(200, {"code": 0, "data": {"message_id": "test"}}),
    ])
    instance.app_id = "test-app"
    instance.app_secret = "test-secret"
    monkeypatch.setattr(module, "current_feishu_config", lambda: {})

    def resolve(_open_id, *, request, **_kwargs):
        request("GET", "https://open.feishu.cn/open-apis/bitable/v1/apps/test/tables/test/records")
        return "ou-current"

    monkeypatch.setattr(directory_module.feishu_recipient_directory, "resolve", resolve)
    instance._request_json_with_auth_retry(
        "POST", instance.SEND_MESSAGE_URL,
        params={"receive_id_type": "open_id"}, payload={"receive_id": "ou-previous"},
    )
    assert [method for method, _url in sent] == ["GET", "GET", "POST"]
