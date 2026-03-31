from __future__ import annotations

from typing import Any, Callable, Dict, Tuple

from app.modules.network.service.network_stability import wait_for_network_stability
from wifi_switcher import WifiSwitcher


def build_wifi_switcher(network_cfg: Dict[str, Any], log_cb: Callable[[str], None] = print) -> WifiSwitcher:
    return WifiSwitcher(
        timeout_sec=int(network_cfg["switch_timeout_sec"]),
        retry_count=int(network_cfg["retry_count"]),
        retry_interval_sec=int(network_cfg["retry_interval_sec"]),
        connect_poll_interval_sec=float(network_cfg.get("connect_poll_interval_sec", 1)),
        fail_fast_on_netsh_error=bool(network_cfg.get("fail_fast_on_netsh_error", True)),
        scan_before_connect=bool(network_cfg.get("scan_before_connect", True)),
        scan_attempts=int(network_cfg.get("scan_attempts", 3)),
        scan_wait_sec=int(network_cfg.get("scan_wait_sec", 2)),
        strict_target_visible_before_connect=bool(network_cfg.get("strict_target_visible_before_connect", True)),
        connect_with_ssid_param=bool(network_cfg.get("connect_with_ssid_param", True)),
        preferred_interface=str(network_cfg.get("preferred_interface", "") or "").strip(),
        auto_disconnect_before_connect=bool(network_cfg.get("auto_disconnect_before_connect", True)),
        hard_recovery_enabled=bool(network_cfg.get("hard_recovery_enabled", True)),
        hard_recovery_after_scan_failures=int(network_cfg.get("hard_recovery_after_scan_failures", 2)),
        hard_recovery_steps=network_cfg.get("hard_recovery_steps", ["toggle_adapter", "restart_wlansvc"]),
        hard_recovery_cooldown_sec=int(network_cfg.get("hard_recovery_cooldown_sec", 20)),
        require_admin_for_hard_recovery=bool(network_cfg.get("require_admin_for_hard_recovery", True)),
        log_cb=log_cb,
    )


def try_switch_wifi(
    *,
    wifi: WifiSwitcher,
    network_cfg: Dict[str, Any] | None = None,
    enable_auto_switch_wifi: bool,
    target_ssid: str,
    require_saved_profile: bool,
    profile_name: str | None = None,
) -> Tuple[bool, str, bool]:
    if not enable_auto_switch_wifi:
        return True, "当前角色不使用单机切网，按当前网络继续执行", True

    ok, msg = wifi.connect(
        target_ssid,
        require_saved_profile=require_saved_profile,
        profile_name=profile_name,
    )
    if not ok:
        return bool(ok), str(msg), False

    cfg = network_cfg or {}
    internal_ssid = str(cfg.get("internal_ssid", "") or "").strip()
    external_ssid = str(cfg.get("external_ssid", "") or "").strip()
    side = ""
    if target_ssid == internal_ssid:
        side = "internal"
    elif target_ssid == external_ssid:
        side = "external"

    emit_log = getattr(wifi, "_log_cb", None)
    stable_ok, stable_msg = wait_for_network_stability(
        network_cfg=cfg,
        target_side=side,
        emit_log=emit_log if callable(emit_log) else None,
    )
    if not stable_ok:
        return False, f"{msg}; {stable_msg}", False
    return True, f"{msg}; {stable_msg}", False
