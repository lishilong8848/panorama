from __future__ import annotations

from typing import Tuple

from pipeline_utils import send_feishu_webhook


class WebhookHttpRepository:
    def send(self, webhook_url: str, text: str, keyword: str, timeout: int) -> Tuple[bool, str]:
        return send_feishu_webhook(webhook_url, text, keyword=keyword, timeout=timeout)
