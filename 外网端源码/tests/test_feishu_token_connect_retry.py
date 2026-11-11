from datetime import datetime
from unittest.mock import Mock

import pytest
import requests

from app.modules.feishu.service import feishu_token_manager as module
from app.modules.feishu.service.bitable_target_resolver import BitableTargetResolver
from app.modules.report_pipeline.service import energy_report_upload_service as energy


def test_token_retries_connection_timeout_once_and_caches_success(monkeypatch):
    response = Mock()
    response.json.return_value = {"code": 0, "tenant_access_token": "fake-token"}
    post = Mock(side_effect=[requests.ConnectTimeout("test timeout"), response])
    sleep = Mock()
    monkeypatch.setattr(module.requests, "post", post)
    monkeypatch.setattr(module.time, "sleep", sleep)
    manager = module.FeishuTokenManager()
    auth = {"app_id": "test-app", "app_secret": "test-secret", "timeout": 30}
    assert manager.get_token(**auth) == manager.get_token(**auth) == "fake-token"
    assert [call.kwargs["timeout"] for call in post.call_args_list] == [30, 5]
    sleep.assert_called_once_with(1)


def test_auth_rejection_is_not_retried(monkeypatch):
    response = Mock()
    response.json.return_value = {"code": 10001, "msg": "test credential rejection"}
    post = Mock(return_value=response)
    monkeypatch.setattr(module.requests, "post", post)
    with pytest.raises(RuntimeError, match="test credential rejection"):
        module.FeishuTokenManager._fetch_token(app_id="test-app", app_secret="test-secret", timeout=30)
    post.assert_called_once()


@pytest.mark.parametrize("propagate", [False, True])
def test_only_opted_in_target_probe_propagates_token_timeout(propagate):
    resolver = BitableTargetResolver(app_id="test-app", app_secret="test-secret",
                                    propagate_token_connect_timeout=propagate)
    client = Mock()
    client.refresh_token.side_effect = module.FeishuTokenConnectTimeout("test timeout")
    if propagate:
        with pytest.raises(module.FeishuTokenConnectTimeout, match="test timeout"):
            resolver._probe_bitable_fields(client, app_token="app", table_id="table", context_label="test")
    else:
        result = resolver._probe_bitable_fields(client, app_token="app", table_id="table", context_label="test")
        assert result["kind"] == "probe_error"
        assert "test timeout" in result["message"]
    client._request_with_retry.assert_not_called()


def test_energy_report_auth_timeout_leaves_month_pending_without_writes(monkeypatch, tmp_path):
    post = Mock(side_effect=requests.ConnectTimeout("test timeout"))
    monkeypatch.setattr(module.requests, "post", post)
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    bitable = Mock()
    bitable.list_fields.side_effect = lambda _: module.FeishuTokenManager._fetch_token(
        app_id="test-app", app_secret="test-secret", timeout=30,
    )
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    service = energy.EnergyReportUploadService(config, bitable=bitable)
    monkeypatch.setattr(service, "source_paths", lambda _: {})
    now = datetime(2026, 11, 9, 7, 5)
    assert energy.pending_periods(config, now) == ("2026-10",)
    with pytest.raises(module.FeishuTokenConnectTimeout, match="test timeout"):
        service.run("2026-10", log=lambda _: None)
    assert post.call_count == 2
    assert energy.pending_periods(config, now) == ("2026-10",)
    bitable._post_json.assert_not_called()
    bitable.upload_attachment_bytes.assert_not_called()
    bitable.create_record.assert_not_called()
    bitable.update_record.assert_not_called()
