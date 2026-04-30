from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from wifi_switcher import WifiSwitcher


class NetshRepository:
    def __init__(
        self,
        timeout_sec: int = 30,
        retry_count: int = 3,
        retry_interval_sec: int = 2,
        connect_poll_interval_sec: float = 1.0,
        fail_fast_on_netsh_error: bool = True,
        scan_before_connect: bool = True,
        scan_attempts: int = 3,
        scan_wait_sec: int = 2,
        strict_target_visible_before_connect: bool = True,
        connect_with_ssid_param: bool = True,
        preferred_interface: str = "",
        auto_disconnect_before_connect: bool = True,
        hard_recovery_enabled: bool = True,
        hard_recovery_after_scan_failures: int = 2,
        hard_recovery_steps: List[str] | None = None,
        hard_recovery_cooldown_sec: int = 20,
        require_admin_for_hard_recovery: bool = True,
    ) -> None:
        self._wifi = WifiSwitcher(
            timeout_sec=timeout_sec,
            retry_count=retry_count,
            retry_interval_sec=retry_interval_sec,
            connect_poll_interval_sec=connect_poll_interval_sec,
            fail_fast_on_netsh_error=fail_fast_on_netsh_error,
            scan_before_connect=scan_before_connect,
            scan_attempts=scan_attempts,
            scan_wait_sec=scan_wait_sec,
            strict_target_visible_before_connect=strict_target_visible_before_connect,
            connect_with_ssid_param=connect_with_ssid_param,
            preferred_interface=preferred_interface,
            auto_disconnect_before_connect=auto_disconnect_before_connect,
            hard_recovery_enabled=hard_recovery_enabled,
            hard_recovery_after_scan_failures=hard_recovery_after_scan_failures,
            hard_recovery_steps=hard_recovery_steps,
            hard_recovery_cooldown_sec=hard_recovery_cooldown_sec,
            require_admin_for_hard_recovery=require_admin_for_hard_recovery,
        )

    def current_ssid(self) -> Optional[str]:
        return self._wifi.get_current_ssid()

    def current_interface_name(self) -> str:
        return self._wifi.get_interface_name()

    def list_visible_ssids(self) -> List[str]:
        return self._wifi.list_visible_ssids(interface_name=self.current_interface_name())

    def is_admin(self) -> bool:
        return self._wifi.is_admin()

    def get_last_switch_report(self) -> Dict[str, Any]:
        return self._wifi.get_last_switch_report()

    def connect(
        self,
        target_ssid: str,
        require_saved_profile: bool = True,
        profile_name: str | None = None,
    ) -> Tuple[bool, str]:
        return self._wifi.connect(
            target_ssid,
            require_saved_profile=require_saved_profile,
            profile_name=profile_name,
        )
