from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import patch

from app.modules.notify.service import top5_completion_notify_service as notify_module


class Top5CompletionNotifyServiceTest(unittest.TestCase):
    def test_sends_to_fixed_chat_with_table_and_attachment_links(self) -> None:
        captured: dict[str, Any] = {}

        class FakeClient:
            def __init__(self, **kwargs: Any) -> None:
                captured["client"] = dict(kwargs)

            def send_text_message(self, **kwargs: Any) -> dict[str, Any]:
                captured["message"] = dict(kwargs)
                return {"message_id": "om_test"}

        config = {
            "common": {
                "feishu_auth": {
                    "app_id": "cli_test",
                    "app_secret": "secret_test",
                    "timeout": 18,
                    "request_retry_count": 4,
                    "request_retry_interval_sec": 1.5,
                }
            }
        }
        with patch.object(notify_module, "FeishuImFileMessageClient", FakeClient):
            result = notify_module.Top5CompletionNotifyService(config).send_completion(
                year="2026",
                month=7,
                file_name="TOP5功率文件_202607.xlsx",
                upload_result={
                    "status": "ok",
                    "app_token": "MliKbC3fXa8PXrsndKscmxjdn1g",
                    "table_id": "tblkh6YCMYtS8nHa",
                    "shared_url": "https://vnet.feishu.cn/share/base/record_test",
                    "link": "https://open.feishu.cn/open-apis/drive/v1/medias/file/download",
                },
                emit_log=lambda _message: None,
            )

        self.assertTrue(result["sent"])
        self.assertEqual(result["chat_id"], "oc_3bc648b9b761f24a65366a9b04b32eb2")
        self.assertEqual(captured["message"]["receive_id"], "oc_3bc648b9b761f24a65366a9b04b32eb2")
        self.assertEqual(captured["message"]["receive_id_type"], "chat_id")
        message = captured["message"]["text"]
        self.assertIn("目标月份：2026-07", message)
        self.assertIn("TOP5功率文件_202607.xlsx", message)
        self.assertIn(
            "https://vnet.feishu.cn/base/MliKbC3fXa8PXrsndKscmxjdn1g?table=tblkh6YCMYtS8nHa",
            message,
        )
        self.assertIn("附件记录链接：https://vnet.feishu.cn/share/base/record_test", message)
        self.assertNotIn("open.feishu.cn/open-apis", message)

    def test_disabled_notification_does_not_require_feishu_credentials(self) -> None:
        service = notify_module.Top5CompletionNotifyService(
            {
                "handover_log": {
                    "top5_power_report": {
                        "notification": {"enabled": False},
                    }
                }
            }
        )
        result = service.send_completion(
            year="2026",
            month=7,
            file_name="top5.xlsx",
            upload_result={"status": "ok"},
            emit_log=lambda _message: None,
        )
        self.assertEqual(result["status"], "skipped")
        self.assertFalse(result["sent"])


if __name__ == "__main__":
    unittest.main()
