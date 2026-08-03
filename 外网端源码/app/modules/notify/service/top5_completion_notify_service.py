from __future__ import annotations

from typing import Any, Callable, Dict

from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.feishu.service.im_file_message_client import FeishuImFileMessageClient


_DEFAULT_CHAT_ID = "oc_3bc648b9b761f24a65366a9b04b32eb2"
_DEFAULT_TABLE_URL = (
    "https://vnet.feishu.cn/wiki/MliKbC3fXa8PXrsndKscmxjdn1g"
    "?table=tblkh6YCMYtS8nHa"
)


class Top5CompletionNotifyService:
    """Send TOP5 completion messages through the application's Feishu bot."""

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        self.runtime_config = runtime_config if isinstance(runtime_config, dict) else {}

    @staticmethod
    def defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "chat_id": _DEFAULT_CHAT_ID,
            "receive_id_type": "chat_id",
            "table_url": _DEFAULT_TABLE_URL,
        }

    def _config(self) -> Dict[str, Any]:
        handover_cfg = self.runtime_config.get("handover_log", {})
        if not isinstance(handover_cfg, dict):
            handover_cfg = {}
        top5_cfg = handover_cfg.get("top5_power_report", {})
        if not isinstance(top5_cfg, dict):
            top5_cfg = {}
        raw_cfg = top5_cfg.get("notification", {})
        if not isinstance(raw_cfg, dict):
            raw_cfg = {}
        cfg = {**self.defaults(), **raw_cfg}
        cfg["enabled"] = bool(cfg.get("enabled", True))
        for key in ("chat_id", "receive_id_type", "table_url"):
            cfg[key] = str(cfg.get(key, "") or "").strip()
        if not cfg["receive_id_type"]:
            cfg["receive_id_type"] = "chat_id"
        return cfg

    def send_completion(
        self,
        *,
        year: str,
        month: int,
        file_name: str,
        upload_result: Dict[str, Any],
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        cfg = self._config()
        if not cfg["enabled"]:
            emit_log("[TOP5功率文件生成][完成通知] 通知已禁用，跳过发送")
            return {"status": "skipped", "sent": False, "reason": "disabled"}
        if not cfg["chat_id"]:
            raise ValueError("TOP5完成通知 chat_id 不能为空")
        if not cfg["table_url"]:
            raise ValueError("TOP5完成通知多维表链接不能为空")

        upload_status = str(upload_result.get("status", "") or "").strip().lower()
        upload_status_text = "已上传多维表" if upload_status == "ok" else "多维表上传已跳过"
        attachment_link = str(upload_result.get("link", "") or "").strip()
        lines = [
            "TOP5功率文件生成完成",
            f"目标月份：{str(year).strip()}-{int(month):02d}",
            f"文件：{str(file_name or '-').strip() or '-'}",
            f"多维状态：{upload_status_text}",
            f"多维表链接：{cfg['table_url']}",
        ]
        if attachment_link:
            lines.append(f"附件链接：{attachment_link}")

        auth = require_feishu_auth_settings(self.runtime_config)
        client = FeishuImFileMessageClient(
            app_id=str(auth.get("app_id", "") or "").strip(),
            app_secret=str(auth.get("app_secret", "") or "").strip(),
            timeout=int(auth.get("timeout", 30) or 30),
            request_retry_count=int(auth.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(auth.get("request_retry_interval_sec", 2) or 2),
        )
        response = client.send_text_message(
            receive_id=cfg["chat_id"],
            receive_id_type=cfg["receive_id_type"],
            text="\n".join(lines),
        )
        message_id = str(response.get("message_id", "") or "").strip()
        emit_log(
            "[TOP5功率文件生成][完成通知] 已发送飞书群消息: "
            f"chat_id={cfg['chat_id']}, message_id={message_id or '-'}"
        )
        return {
            "status": "sent",
            "sent": True,
            "chat_id": cfg["chat_id"],
            "receive_id_type": cfg["receive_id_type"],
            "message_id": message_id,
            "table_url": cfg["table_url"],
        }
