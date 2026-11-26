import io
import json
from unittest.mock import Mock

import pytest

from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient
from app.modules.shared_bridge.service import shared_bridge_runtime_service as bridge


NOTICE_ID = "1" * 64
LEASE = "2" * 32
ROBOT = "https://robot.invalid/secret-test"


@pytest.fixture
def runtime(monkeypatch):
    service = object.__new__(bridge.SharedBridgeRuntimeService)
    service.role_mode = "external"
    service._bridge_mode = "http"
    service._internal_bridge_http_enabled = True
    service._http_bridge_unavailable_until_monotonic = 0
    service._report_missing_notice_receipt = None
    service._internal_bridge_http_client = InternalBridgeHttpClient(base_url="http://bridge.invalid")
    service.runtime_config = {
        "common": {"notify": {"enable_webhook": True, "feishu_webhook_url": ROBOT, "keyword": "事件"}},
    }
    calls = []

    def urlopen(request, timeout):
        calls.append((request.full_url, request.data, timeout))
        if request.full_url.endswith("/claim"):
            body = {"notice": {"notice_id": NOTICE_ID, "lease_token": LEASE, "message": "缺测" * 1000}}
        else:
            assert request.full_url.endswith("/ack")
            body = {"ok": True}
        return io.BytesIO(json.dumps(body).encode())

    monkeypatch.setattr("urllib.request.urlopen", urlopen)
    post = Mock(return_value=Mock(**{"json.return_value": {"StatusCode": 0}}))
    monkeypatch.setattr(bridge.requests, "post", post)
    return service, calls, post


def test_notice_uses_service_robot_and_acknowledges_only_after_success(runtime):
    service, calls, post = runtime

    def accept(*args, **kwargs):
        assert len(calls) == 1 and calls[0][0].endswith("/claim")
        return post.return_value

    post.side_effect = accept
    assert service._poll_report_missing_notice()["status"] == "success"
    assert [url.rsplit("/", 1)[-1] for url, _, _ in calls] == ["claim", "ack"]
    assert json.loads(calls[1][1]) == {"notice_id": NOTICE_ID, "lease_token": LEASE}
    assert post.call_args.args == (ROBOT,)
    text = post.call_args.kwargs["json"]["content"]["text"]
    assert text.startswith("事件：缺测")
    assert len(text) <= 1603


@pytest.mark.parametrize("body", [{"code": 123}, {"StatusCode": 123}, {}])
def test_robot_rejection_does_not_ack_or_expose_details(runtime, body):
    service, calls, post = runtime
    post.return_value.json.return_value = body
    result = service._poll_report_missing_notice()
    assert result["status"] == "failed"
    assert len(calls) == 1
    assert ROBOT not in str(result)


def test_robot_timeout_is_not_raised_and_does_not_ack(runtime):
    service, calls, post = runtime
    post.side_effect = TimeoutError(ROBOT)
    result = service._poll_report_missing_notice()
    assert result["status"] == "failed"
    assert len(calls) == 1
    assert ROBOT not in str(result)


def test_disabled_robot_leaves_notice_unclaimed(runtime):
    service, calls, post = runtime
    service.runtime_config["common"]["notify"]["enable_webhook"] = False
    assert service._poll_report_missing_notice()["status"] == "idle"
    assert not calls
    post.assert_not_called()


@pytest.mark.parametrize("general_disabled", [True, False])
def test_falls_back_to_enabled_task_failure_robot(runtime, general_disabled):
    service, _, post = runtime
    general = service.runtime_config["common"]["notify"]
    if general_disabled:
        general["enable_webhook"] = False
    else:
        general.pop("feishu_webhook_url")
    service.runtime_config["notify"] = {
        "task_failure_webhook_enabled": True,
        "task_failure_webhook_url": "https://fallback.invalid/secret-test",
        "task_failure_webhook_timeout": 7,
    }
    assert service._poll_report_missing_notice()["status"] == "success"
    assert post.call_args.args == ("https://fallback.invalid/secret-test",)
    assert post.call_args.kwargs["timeout"] == 7


def test_disabled_fallback_robot_leaves_notice_unclaimed(runtime):
    service, calls, post = runtime
    service.runtime_config = {
        "notify": {"task_failure_webhook_enabled": False, "task_failure_webhook_url": ROBOT},
    }
    assert service._poll_report_missing_notice()["status"] == "idle"
    assert not calls
    post.assert_not_called()


def test_lost_bridge_ack_retries_receipt_without_resending_robot_message(runtime, monkeypatch):
    service, calls, post = runtime
    client = service._internal_bridge_http_client
    real_ack = client.acknowledge_report_missing_notice
    monkeypatch.setattr(client, "acknowledge_report_missing_notice", Mock(side_effect=OSError(ROBOT)))
    result = service._poll_report_missing_notice()
    assert result["status"] == "failed"
    assert ROBOT not in str(result)
    assert service._report_missing_notice_receipt is not None
    monkeypatch.setattr(client, "acknowledge_report_missing_notice", real_ack)
    assert service._poll_report_missing_notice()["status"] == "success"
    assert [url.rsplit("/", 1)[-1] for url, _, _ in calls] == ["claim", "ack"]
    assert service._report_missing_notice_receipt is None
    post.assert_called_once()


@pytest.mark.parametrize("status", ["idle", "unavailable"])
def test_idle_poll_does_not_touch_background_ui_or_logs(runtime, status):
    service, _, _ = runtime
    service._lock = bridge.threading.Lock()
    service._background_task_threads = {}
    service._mark_background_task_started = Mock()
    service._mark_background_task_finished = Mock()
    service._emit_system_log = Mock()
    service._run_background_task_worker(task_key="report_missing_notice", target=lambda: {"status": status})
    service._mark_background_task_started.assert_not_called()
    service._mark_background_task_finished.assert_not_called()
    service._emit_system_log.assert_not_called()
