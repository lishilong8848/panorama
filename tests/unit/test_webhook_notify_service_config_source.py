from __future__ import annotations

from app.modules.notify.service.webhook_notify_service import WebhookNotifyService


def test_webhook_notify_service_prefers_common_notify(monkeypatch) -> None:
    sent: dict[str, object] = {}

    def _fake_send(webhook_url: str, text: str, keyword: str, timeout: int):
        sent["webhook_url"] = webhook_url
        sent["text"] = text
        sent["keyword"] = keyword
        sent["timeout"] = timeout
        return True, "ok"

    cfg = {
        "notify": {
            "enable_webhook": True,
            "feishu_webhook_url": "https://legacy.example/hook",
            "keyword": "旧关键词",
            "timeout": 5,
        },
        "common": {
            "notify": {
                "enable_webhook": True,
                "feishu_webhook_url": "https://common.example/hook",
                "keyword": "告警通知",
                "timeout": 12,
                "on_upload_failure": True,
            }
        },
    }
    service = WebhookNotifyService(cfg)
    monkeypatch.setattr(service._repo, "send", _fake_send)

    service.send_failure(stage="12项独立上传", detail="测试失败", category="upload")

    assert sent["webhook_url"] == "https://common.example/hook"
    assert sent["keyword"] == "告警通知"
    assert sent["timeout"] == 12


def test_webhook_notify_service_falls_back_to_root_notify(monkeypatch) -> None:
    sent: dict[str, object] = {}

    def _fake_send(webhook_url: str, text: str, keyword: str, timeout: int):
        sent["webhook_url"] = webhook_url
        sent["keyword"] = keyword
        sent["timeout"] = timeout
        return True, "ok"

    cfg = {
        "notify": {
            "enable_webhook": True,
            "feishu_webhook_url": "https://legacy.example/hook",
            "keyword": "旧配置",
            "timeout": 9,
            "on_upload_failure": True,
        }
    }
    service = WebhookNotifyService(cfg)
    monkeypatch.setattr(service._repo, "send", _fake_send)

    service.send_failure(stage="12项独立上传", detail="测试失败", category="upload")

    assert sent["webhook_url"] == "https://legacy.example/hook"
    assert sent["keyword"] == "旧配置"
    assert sent["timeout"] == 9
