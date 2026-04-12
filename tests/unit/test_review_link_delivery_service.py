from __future__ import annotations

import pytest

from handover_log_module.service import review_link_delivery_service as review_module


class _FakeReviewSessionService:
    sessions = []
    updated_states = {}

    def __init__(self, _handover_cfg):
        pass

    def list_batch_sessions(self, _batch_key):
        return [dict(item) for item in self.sessions]

    def update_review_link_delivery(self, *, session_id: str, review_link_delivery):
        self.updated_states[session_id] = dict(review_link_delivery)
        return {"session_id": session_id, "review_link_delivery": dict(review_link_delivery)}


def _make_service(monkeypatch, *, recipients_by_building, review_links, review_base_url_effective=""):
    _FakeReviewSessionService.updated_states = {}
    _FakeReviewSessionService.sessions = [
        {
            "session_id": "session-a",
            "building": "A楼",
            "duty_date": "2026-04-10",
            "duty_shift": "night",
            "review_link_delivery": {},
        }
    ]
    monkeypatch.setattr(review_module, "ReviewSessionService", _FakeReviewSessionService)
    monkeypatch.setattr(
        review_module,
        "materialize_review_access_snapshot",
        lambda _cfg: {
            "review_links": list(review_links),
            "review_base_url_effective": review_base_url_effective,
        },
    )
    monkeypatch.setattr(
        review_module,
        "load_review_access_state",
        lambda _cfg: {},
    )
    return review_module.ReviewLinkDeliveryService(
        {
            "review_ui": {
                "review_link_recipients_by_building": recipients_by_building,
            },
            "_global_feishu": {
                "app_id": "app-id",
                "app_secret": "app-secret",
            },
        }
    )


def test_send_for_batch_manual_unconfigured_raises(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={},
        review_links=[{"building": "A楼", "url": "http://example.com/review/A"}],
    )
    logs = []

    with pytest.raises(RuntimeError, match="当前楼未配置审核链接接收人"):
        service.send_for_batch(
            batch_key="2026-04-10|night",
            building="A楼",
            source="manual",
            emit_log=logs.append,
        )

    assert _FakeReviewSessionService.updated_states["session-a"]["status"] == "unconfigured"
    assert any("跳过发送" in line and "未配置审核链接接收人" in line for line in logs)
    assert any("批次完成" in line for line in logs)


def test_send_for_batch_manual_pending_access_raises(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={"A楼": [{"open_id": "ou_abc", "note": "测试"}]},
        review_links=[],
    )
    logs = []

    with pytest.raises(RuntimeError, match="审核访问地址尚未就绪"):
        service.send_for_batch(
            batch_key="2026-04-10|night",
            building="A楼",
            source="manual",
            emit_log=logs.append,
        )

    assert _FakeReviewSessionService.updated_states["session-a"]["status"] == "pending_access"
    assert any("跳过发送" in line and "审核访问地址尚未就绪" in line for line in logs)
    assert any("批次完成" in line for line in logs)


def test_send_for_batch_manual_success_uses_open_id(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={"A楼": [{"open_id": "ou_abc", "note": "本人"}]},
        review_links=[{"building": "A楼", "url": "http://example.com/review/A"}],
    )
    logs = []
    calls = []

    class _FakeClient:
        def send_text_message(self, *, receive_id: str, receive_id_type: str, text: str):
            calls.append(
                {
                    "receive_id": receive_id,
                    "receive_id_type": receive_id_type,
                    "text": text,
                }
            )
            return {"message_id": "msg-1"}

    service._build_feishu_client = lambda: _FakeClient()

    result = service.send_for_batch(
        batch_key="2026-04-10|night",
        building="A楼",
        source="manual",
        emit_log=logs.append,
    )

    assert result["results"][0]["delivery"]["status"] == "success"
    assert calls == [
        {
            "receive_id": "ou_abc",
            "receive_id_type": "open_id",
            "text": (
                "这是一条交接班审核访问链接，请在办公电脑的浏览器中打开。\n"
                "楼栋：A楼\n"
                "日期：2026-04-10\n"
                "班次：夜班\n"
                "审核链接：http://example.com/review/A"
            ),
        }
    ]
    assert _FakeReviewSessionService.updated_states["session-a"]["status"] == "success"
    assert any("发送成功" in line and "receive_id_type=open_id" in line for line in logs)
    assert any("批次完成" in line for line in logs)


def test_send_for_batch_auto_uses_effective_base_url_fallback(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={"A楼": [{"open_id": "ou_abc", "note": "本人"}]},
        review_links=[],
        review_base_url_effective="http://192.168.224.157:18765",
    )
    logs = []
    calls = []

    class _FakeClient:
        def send_text_message(self, *, receive_id: str, receive_id_type: str, text: str):
            calls.append(
                {
                    "receive_id": receive_id,
                    "receive_id_type": receive_id_type,
                    "text": text,
                }
            )
            return {"message_id": "msg-1"}

    service._build_feishu_client = lambda: _FakeClient()

    session = dict(_FakeReviewSessionService.sessions[0])
    result = service.send_for_session(
        session,
        source="auto",
        force=False,
        emit_log=logs.append,
    )

    assert result["status"] == "success"
    assert result["url"] == "http://192.168.224.157:18765/handover/review/a"
    assert calls == [
        {
            "receive_id": "ou_abc",
            "receive_id_type": "open_id",
            "text": (
                "这是一条交接班审核访问链接，请在办公电脑的浏览器中打开。\n"
                "楼栋：A楼\n"
                "日期：2026-04-10\n"
                "班次：夜班\n"
                "审核链接：http://192.168.224.157:18765/handover/review/a"
            ),
        }
    ]


def test_validate_manual_send_preflight_building_unconfigured(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={},
        review_links=[{"building": "A楼", "url": "http://example.com/review/A"}],
    )

    with pytest.raises(ValueError, match="当前楼未配置审核链接接收人"):
        service.validate_manual_send_preflight(
            batch_key="2026-04-10|night",
            building="A楼",
        )


def test_validate_manual_send_preflight_building_pending_access(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={"A楼": [{"open_id": "ou_abc", "note": "测试"}]},
        review_links=[],
    )

    result = service.validate_manual_send_preflight(
        batch_key="2026-04-10|night",
        building="A楼",
    )
    assert result["building"] == "A楼"
    assert result["session_count"] == 1


def test_validate_manual_send_preflight_building_ok(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={"A楼": [{"open_id": "ou_abc", "note": "测试"}]},
        review_links=[{"building": "A楼", "url": "http://example.com/review/A"}],
    )

    result = service.validate_manual_send_preflight(
        batch_key="2026-04-10|night",
        building="A楼",
    )
    assert result["session_count"] == 1
    assert result["building"] == "A楼"


def test_send_manual_test_without_review_url_still_sends(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={"A楼": [{"open_id": "ou_abc", "note": "测试"}]},
        review_links=[],
    )
    calls = []

    class _FakeClient:
        def send_text_message(self, *, receive_id: str, receive_id_type: str, text: str):
            calls.append(
                {
                    "receive_id": receive_id,
                    "receive_id_type": receive_id_type,
                    "text": text,
                }
            )
            return {"message_id": "msg-1"}

    service._build_feishu_client = lambda: _FakeClient()

    result = service.send_manual_test(
        building="A楼",
        batch_key="2026-04-10|night",
        emit_log=lambda _msg: None,
    )

    assert result["status"] == "success"
    assert result["review_url"] == ""
    assert calls == [
        {
            "receive_id": "ou_abc",
            "receive_id_type": "open_id",
            "text": (
                "这是一条交接班审核链接测试消息，请在办公电脑的浏览器中打开。\n"
                "楼栋：A楼\n"
                "日期：2026-04-10\n"
                "班次：夜班\n"
                "审核链接：当前尚未生成，本次仅测试发送通道"
            ),
        }
    ]


def test_send_manual_test_uses_fallback_building_review_url(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={"A楼": [{"open_id": "ou_abc", "note": "测试"}]},
        review_links=[],
        review_base_url_effective="http://192.168.224.157:18765",
    )
    calls = []

    class _FakeClient:
        def send_text_message(self, *, receive_id: str, receive_id_type: str, text: str):
            calls.append(
                {
                    "receive_id": receive_id,
                    "receive_id_type": receive_id_type,
                    "text": text,
                }
            )
            return {"message_id": "msg-1"}

    service._build_feishu_client = lambda: _FakeClient()

    result = service.send_manual_test(
        building="A楼",
        batch_key="2026-04-12|day",
        emit_log=lambda _msg: None,
    )

    assert result["status"] == "success"
    assert result["review_url"] == "http://192.168.224.157:18765/handover/review/a"
    assert calls == [
        {
            "receive_id": "ou_abc",
            "receive_id_type": "open_id",
            "text": (
                "这是一条交接班审核链接测试消息，请在办公电脑的浏览器中打开。\n"
                "楼栋：A楼\n"
                "日期：2026-04-12\n"
                "班次：白班\n"
                "审核链接：http://192.168.224.157:18765/handover/review/a"
            ),
        }
    ]


def test_send_manual_test_uses_configured_public_base_url_when_snapshot_missing(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={"A楼": [{"open_id": "ou_abc", "note": "测试"}]},
        review_links=[],
    )
    service.handover_cfg.setdefault("review_ui", {})["public_base_url"] = "http://192.168.224.157:18765"
    calls = []

    class _FakeClient:
        def send_text_message(self, *, receive_id: str, receive_id_type: str, text: str):
            calls.append(
                {
                    "receive_id": receive_id,
                    "receive_id_type": receive_id_type,
                    "text": text,
                }
            )
            return {"message_id": "msg-1"}

    service._build_feishu_client = lambda: _FakeClient()

    result = service.send_manual_test(
        building="A楼",
        batch_key="2026-04-12|day",
        emit_log=lambda _msg: None,
    )

    assert result["status"] == "success"
    assert result["review_url"] == "http://192.168.224.157:18765/handover/review/a"
    assert calls == [
        {
            "receive_id": "ou_abc",
            "receive_id_type": "open_id",
            "text": (
                "这是一条交接班审核链接测试消息，请在办公电脑的浏览器中打开。\n"
                "楼栋：A楼\n"
                "日期：2026-04-12\n"
                "班次：白班\n"
                "审核链接：http://192.168.224.157:18765/handover/review/a"
            ),
        }
    ]


def test_build_feishu_client_uses_resolved_auth(monkeypatch):
    service = _make_service(
        monkeypatch,
        recipients_by_building={"A楼": [{"open_id": "ou_abc", "note": "测试"}]},
        review_links=[],
    )
    monkeypatch.setattr(
        review_module,
        "require_feishu_auth_settings",
        lambda _cfg, config_path=None: {
            "app_id": "resolved-app",
            "app_secret": "resolved-secret",
            "timeout": 31,
            "request_retry_count": 4,
            "request_retry_interval_sec": 5,
        },
    )

    client = service._build_feishu_client()

    assert client.app_id == "resolved-app"
    assert client.app_secret == "resolved-secret"
    assert client.timeout == 31
