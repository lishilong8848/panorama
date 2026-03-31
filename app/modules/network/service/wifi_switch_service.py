from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from app.modules.network.repository.netsh_repository import NetshRepository
from app.modules.network.service.network_stability import wait_for_network_stability


class WifiSwitchService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config
        network_cfg = config.get("network", {})
        self._repo = NetshRepository(
            timeout_sec=int(network_cfg.get("switch_timeout_sec", 30)),
            retry_count=int(network_cfg.get("retry_count", 3)),
            retry_interval_sec=int(network_cfg.get("retry_interval_sec", 2)),
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
        )
        self._require_saved_profile = bool(network_cfg.get("require_saved_profiles", True))
    
    def _is_auto_switch_enabled(self) -> bool:
        network_cfg = self._config.get("network", {})
        return bool(network_cfg.get("enable_auto_switch_wifi", True))

    def current_ssid(self) -> Optional[str]:
        return self._repo.current_ssid()

    def current_interface_name(self) -> str:
        return self._repo.current_interface_name()

    def visible_targets(self) -> Dict[str, bool]:
        network_cfg = self._config.get("network", {})
        visible_set = set(self._repo.list_visible_ssids())
        internal_ssid = str(network_cfg.get("internal_ssid", "") or "").strip()
        external_ssid = str(network_cfg.get("external_ssid", "") or "").strip()
        return {
            "internal": bool(internal_ssid and internal_ssid in visible_set),
            "external": bool(external_ssid and external_ssid in visible_set),
        }

    def is_admin(self) -> bool:
        return self._repo.is_admin()

    def get_last_switch_report(self) -> Dict[str, Any]:
        return self._repo.get_last_switch_report()

    def connect(self, ssid: str, profile_name: str | None = None) -> Tuple[bool, str]:
        if not self._is_auto_switch_enabled():
            return True, "当前角色不使用单机切网，按当前网络继续执行"

        target = str(ssid or "").strip()
        if not target:
            return False, "目标SSID为空"

        if not profile_name:
            network_cfg = self._config.get("network", {})
            internal_ssid = str(network_cfg.get("internal_ssid", "") or "").strip()
            external_ssid = str(network_cfg.get("external_ssid", "") or "").strip()
            if target == internal_ssid:
                profile_name = str(network_cfg.get("internal_profile_name", "") or "").strip() or None
            elif target == external_ssid:
                profile_name = str(network_cfg.get("external_profile_name", "") or "").strip() or None

        ok, msg = self._repo.connect(
            target,
            require_saved_profile=self._require_saved_profile,
            profile_name=profile_name,
        )
        if not ok:
            return ok, msg

        network_cfg = self._config.get("network", {})
        internal_ssid = str(network_cfg.get("internal_ssid", "") or "").strip()
        external_ssid = str(network_cfg.get("external_ssid", "") or "").strip()
        if target == internal_ssid:
            side = "internal"
        elif target == external_ssid:
            side = "external"
        else:
            side = ""

        stable_ok, stable_msg = wait_for_network_stability(
            network_cfg=network_cfg,
            target_side=side,
            emit_log=None,
        )
        if not stable_ok:
            return False, f"{msg}; {stable_msg}"
        return True, f"{msg}; {stable_msg}"
