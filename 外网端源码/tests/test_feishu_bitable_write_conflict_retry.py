import json
from unittest.mock import Mock

import pytest
import requests

from app.modules.feishu.service import bitable_client_runtime as module


def _response(code, *, status=200):
    response = requests.Response()
    response.status_code = status
    response.url = "https://open.feishu.cn/open-apis/bitable/v1/apps/app/tables/attachment/records"
    response._content = json.dumps({
        "code": code, "msg": "LockNotObtainedError" if str(code) == "1254291" else "ok",
        "error": {"log_id": "test-lock-log"},
    }).encode()
    return response


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(module, "resolve_feishu_auth_settings", lambda settings: settings)
    instance = module.FeishuBitableClient(
        "test-id", "test-secret", "app", "calc", "attachment", date_field_mode="text",
        request_retry_count=1, request_retry_interval_sec=2,
        date_text_to_timestamp_ms_fn=lambda **kwargs: 0,
        canonical_metric_name_fn=str, dimension_mapping={},
    )
    monkeypatch.setattr(instance, "refresh_token", lambda **kwargs: "test-token")
    monkeypatch.setattr(module.time, "sleep", Mock())
    return instance


@pytest.mark.parametrize("status", [200, 400])
@pytest.mark.parametrize("code", [1254291, "1254291"])
def test_attachment_record_lock_conflict_retries_with_one_create_key(client, monkeypatch, status, code):
    request = Mock(side_effect=[_response(code, status=status), _response(code, status=status), _response(0), _response(0)])
    monkeypatch.setattr(module.requests, "request", request)
    for _ in range(2):
        result = client.upload_attachment_record("全景平台月报", "B楼", "2026-10-05", ["file-token"])
        assert result["code"] == 0
    calls = [call.kwargs for call in request.call_args_list]
    assert len(calls) == 4
    tokens = [call["params"]["client_token"] for call in calls]
    assert tokens[0] == tokens[1] == tokens[2]
    assert tokens[3] != tokens[2]
    for call in calls:
        assert call["json"]["fields"] == {
            "类型": "全景平台月报", "楼栋": "B楼", "日期": "2026-10-05",
            "附件": [{"file_token": "file-token"}],
        }
    assert [call.args[0] for call in module.time.sleep.call_args_list] == [2, 4]


def test_permanent_lock_conflict_is_bounded_and_keeps_error_details(client, monkeypatch):
    request = Mock(return_value=_response(1254291))
    monkeypatch.setattr(module.requests, "request", request)
    with pytest.raises(RuntimeError, match="test-lock-log"):
        client.upload_attachment_record("全景平台月报", "B楼", "2026-10-05", ["file-token"])
    assert request.call_count == client._api_retry_attempts() == 10
    assert module.time.sleep.call_count == 9
    assert max(call.args[0] for call in module.time.sleep.call_args_list) == 12
    assert len({call.kwargs["params"]["client_token"] for call in request.call_args_list}) == 1


@pytest.mark.parametrize("operation", ["update", "delete"])
def test_other_record_writes_retry_lock_conflicts(client, monkeypatch, operation):
    request = Mock(side_effect=[_response(1254291), _response(0)])
    monkeypatch.setattr(module.requests, "request", request)
    if operation == "update":
        client.update_record("attachment", "record-id", {"楼栋": "B楼"})
    else:
        assert client.batch_delete_records("attachment", ["record-id"]) == 1
    assert request.call_count == 2
    module.time.sleep.assert_called_once_with(2)


def test_validation_errors_are_not_retried(client, monkeypatch):
    request = Mock(return_value=_response(1254060))
    monkeypatch.setattr(module.requests, "request", request)
    with pytest.raises(RuntimeError, match="1254060"):
        client.upload_attachment_record("全景平台月报", "B楼", "2026-10-05", ["file-token"])
    assert request.call_count == 1
    module.time.sleep.assert_not_called()
