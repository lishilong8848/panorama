from __future__ import annotations

import copy
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple


class WifiSwitchError(RuntimeError):
    pass


class WifiSwitcher:
    """Compatibility shim for legacy callers.

    The project no longer performs local Wi-Fi switching. This class preserves the
    historical API surface so old flows can continue running on the current role
    network without requiring SSID configuration.
    """

    def __init__(
        self,
        timeout_sec: int = 30,
        retry_count: int = 2,
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
        log_cb: Callable[[str], None] | None = None,
    ) -> None:
        self.timeout_sec = max(1, int(timeout_sec or 30))
        self.retry_count = max(1, int(retry_count or 2))
        self.retry_interval_sec = max(1, int(retry_interval_sec or 2))
        self.connect_poll_interval_sec = max(0.2, float(connect_poll_interval_sec or 1.0))
        self.fail_fast_on_netsh_error = bool(fail_fast_on_netsh_error)
        self.scan_before_connect = bool(scan_before_connect)
        self.scan_attempts = max(1, int(scan_attempts or 3))
        self.scan_wait_sec = max(1, int(scan_wait_sec or 2))
        self.strict_target_visible_before_connect = bool(strict_target_visible_before_connect)
        self.connect_with_ssid_param = bool(connect_with_ssid_param)
        self.preferred_interface = str(preferred_interface or "").strip()
        self.auto_disconnect_before_connect = bool(auto_disconnect_before_connect)
        self.hard_recovery_enabled = bool(hard_recovery_enabled)
        self.hard_recovery_after_scan_failures = max(1, int(hard_recovery_after_scan_failures or 2))
        self.hard_recovery_steps = list(hard_recovery_steps or ["toggle_adapter", "restart_wlansvc"])
        self.hard_recovery_cooldown_sec = max(0, int(hard_recovery_cooldown_sec or 20))
        self.require_admin_for_hard_recovery = bool(require_admin_for_hard_recovery)
        self._log_cb = log_cb
        self._last_switch_report: Dict[str, Any] = {}
        self._reset_report()

    def _log(self, message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        line = f"[WiFi] {text}"
        if self._log_cb is not None:
            try:
                self._log_cb(line)
                return
            except Exception:
                pass
        print(line)

    def _reset_report(self) -> None:
        self._last_switch_report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "target_ssid": "",
            "profile_name": "",
            "interface_name": "",
            "current_ssid": "",
            "result": "disabled",
            "stage": "idle",
            "error_type": "",
            "error": "",
            "visible_target": False,
            "visible_ssid_count": 0,
            "hard_recovery_attempted": False,
            "is_admin": self.is_admin(),
            "elapsed_ms": 0,
        }

    def _update_report(self, **kwargs: Any) -> None:
        self._last_switch_report.update(kwargs)
        self._last_switch_report["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def is_admin() -> bool:
        return False

    def get_last_switch_report(self) -> Dict[str, Any]:
        return copy.deepcopy(self._last_switch_report)

    def get_current_ssid(self) -> Optional[str]:
        return None

    def get_interface_name(self) -> str:
        return ""

    def list_visible_ssids(self, interface_name: str = "") -> List[str]:
        return []

    def connect(
        self,
        target_ssid: str,
        require_saved_profile: bool = True,
        profile_name: str | None = None,
    ) -> Tuple[bool, str]:
        target_text = str(target_ssid or "").strip()
        self._update_report(
            target_ssid=target_text,
            profile_name=str(profile_name or "").strip(),
            interface_name=self.get_interface_name(),
            current_ssid=self.get_current_ssid() or "",
            result="skipped",
            stage="disabled",
            error_type="",
            error="",
            visible_target=False,
            visible_ssid_count=0,
            hard_recovery_attempted=False,
            elapsed_ms=0,
        )
        message = "网络切换功能已移除，按当前网络继续执行"
        self._log(message if not target_text else f"跳过切换到 {target_text}: {message}")
        return True, message
