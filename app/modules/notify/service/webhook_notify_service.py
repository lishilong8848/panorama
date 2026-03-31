from __future__ import annotations

from typing import Any, Callable, Dict

from app.modules.network.service.wifi_switch_service import WifiSwitchService
from app.modules.notify.core.event_message_builder import build_event_text
from app.modules.notify.repository.webhook_http_repository import WebhookHttpRepository


class WebhookNotifyService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self._repo = WebhookHttpRepository()
        self._wifi = WifiSwitchService(config)

    def send_failure(
        self,
        stage: str,
        detail: str,
        building: str | None = None,
        emit_log: Callable[[str], None] | None = None,
        category: str = "upload",
    ) -> None:
        notify_cfg = self.config.get("notify", {})
        if not bool(notify_cfg.get("enable_webhook", False)):
            return
        normalized_category = str(category or "upload").strip().lower() or "upload"
        category_enabled_map = {
            "download": bool(notify_cfg.get("on_download_failure", True)),
            "wifi": bool(notify_cfg.get("on_wifi_failure", True)),
            "upload": bool(notify_cfg.get("on_upload_failure", True)),
        }
        if not category_enabled_map.get(normalized_category, True):
            if emit_log:
                emit_log(f"[Webhook] 当前类别已禁用，跳过发送: category={normalized_category}")
            return

        webhook_url = str(notify_cfg.get("feishu_webhook_url", "")).strip()
        keyword = str(notify_cfg.get("keyword", "事件")).strip()
        timeout = int(notify_cfg.get("timeout", 10))
        if not webhook_url:
            return

        external_ssid = str(self.config.get("network", {}).get("external_ssid", "")).strip()
        auto_switch_enabled = bool(self.config.get("network", {}).get("enable_auto_switch_wifi", True))
        if not auto_switch_enabled and emit_log:
            emit_log("[Webhook] 当前角色不使用单机切网，按当前网络直接发送")
        if auto_switch_enabled and external_ssid:
            current = self._wifi.current_ssid()
            if current != external_ssid:
                ok, msg = self._wifi.connect(external_ssid)
                if not ok:
                    if emit_log:
                        emit_log(f"[Webhook] 切换外网失败，本次不发送: {msg}")
                    return
                if emit_log:
                    emit_log(f"[Webhook] 为发送告警已切换外网: {msg}")

        text = build_event_text(stage=stage, detail=detail, building=building)
        ok, msg = self._repo.send(webhook_url, text, keyword=keyword, timeout=timeout)
        if emit_log:
            emit_log(f"[Webhook] {'发送成功' if ok else '发送失败'}: {msg}")
