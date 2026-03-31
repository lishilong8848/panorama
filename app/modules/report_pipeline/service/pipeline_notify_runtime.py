from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List


ROLE_FIXED_NETWORK_TEXT = "当前角色不使用单机切网，按当前网络直接发送告警"
ROLE_FIXED_PENDING_TEXT = "当前角色不使用单机切网，按当前网络直接发送缓存告警"


@dataclass
class PendingNotifyEvent:
    stage: str
    detail: str
    building: str | None = None
    toggle_key: str | None = None


def is_auto_switch_wifi_enabled(config: Dict[str, Any]) -> bool:
    network_cfg = config.get("network", {})
    if not isinstance(network_cfg, dict):
        return True
    return bool(network_cfg.get("enable_auto_switch_wifi", True))


def notify_event(
    config: Dict[str, Any],
    stage: str,
    detail: str,
    *,
    building: str | None = None,
    toggle_key: str | None = None,
    wifi: Any = None,
    external_ssid: str | None = None,
    pending_events: List[PendingNotifyEvent] | None = None,
    build_event_text: Callable[..., str],
    send_feishu_webhook: Callable[..., tuple[bool, str]],
    emit_log: Callable[[str], None] = print,
) -> None:
    notify_cfg = config["notify"]
    if not bool(notify_cfg["enable_webhook"]):
        return

    if toggle_key:
        if toggle_key not in notify_cfg:
            raise ValueError(f"配置错误: notify.{toggle_key} 缺失")
        if not bool(notify_cfg[toggle_key]):
            return

    webhook_url = str(notify_cfg.get("feishu_webhook_url", "")).strip()
    keyword = str(notify_cfg.get("keyword", "")).strip()
    timeout = int(notify_cfg.get("timeout", 10))

    enable_auto_switch_wifi = is_auto_switch_wifi_enabled(config)
    if wifi is not None and external_ssid and enable_auto_switch_wifi:
        try:
            current_ssid = wifi.get_current_ssid()
        except Exception:  # noqa: BLE001
            current_ssid = ""

        if current_ssid != external_ssid:
            if pending_events is not None:
                pending_events.append(
                    PendingNotifyEvent(
                        stage=stage,
                        detail=detail,
                        building=building,
                        toggle_key=toggle_key,
                    )
                )
                emit_log(
                    f"[Webhook] 当前SSID={current_ssid or '-'}，尚未切到外网，暂存告警等待外网发送: "
                    f"stage={stage}, building={building or '-'}"
                )
                return
    elif wifi is not None and external_ssid and not enable_auto_switch_wifi:
        emit_log(f"[Webhook] {ROLE_FIXED_NETWORK_TEXT}")

    text = build_event_text(stage=stage, detail=detail, building=building)
    ok, msg = send_feishu_webhook(webhook_url, text, keyword=keyword, timeout=timeout)
    if ok:
        emit_log(f"[Webhook] 已发送异常通知: stage={stage}, building={building or '-'}")
    else:
        emit_log(f"[Webhook] 发送失败: {msg}")


def flush_pending_notify_events(
    *,
    config: Dict[str, Any],
    wifi: Any,
    external_ssid: str,
    external_profile_name: str | None = None,
    require_saved_profile: bool,
    enable_auto_switch_wifi: bool,
    pending_events: List[PendingNotifyEvent],
    try_switch_wifi: Callable[..., tuple[bool, str, bool]],
    notify_event: Callable[..., None],
    emit_log: Callable[[str], None] = print,
) -> None:
    if not pending_events:
        return

    if enable_auto_switch_wifi:
        current_ssid = wifi.get_current_ssid()
        if external_ssid and current_ssid != external_ssid:
            ok, msg, skipped = try_switch_wifi(
                wifi=wifi,
                network_cfg=config.get("network", {}),
                enable_auto_switch_wifi=enable_auto_switch_wifi,
                target_ssid=external_ssid,
                require_saved_profile=require_saved_profile,
                profile_name=external_profile_name,
            )
            if skipped:
                emit_log(f"[Webhook] {ROLE_FIXED_PENDING_TEXT}")
            elif not ok:
                emit_log(f"[Webhook] 无法切换到外网发送告警: {msg}")
                return
            else:
                emit_log(f"[Webhook] 为发送告警已切换到外网: {msg}")
    else:
        emit_log(f"[Webhook] {ROLE_FIXED_PENDING_TEXT}")

    while pending_events:
        event = pending_events.pop(0)
        notify_event(
            config=config,
            stage=event.stage,
            detail=event.detail,
            building=event.building,
            toggle_key=event.toggle_key,
        )
