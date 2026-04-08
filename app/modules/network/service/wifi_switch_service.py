from __future__ import annotations

from typing import Any, Dict, Optional, Tuple


class WifiSwitchService:
    """Compatibility shim after network switching removal."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config if isinstance(config, dict) else {}

    def _message(self) -> str:
        return "网络切换功能已移除，按当前网络继续执行"

    def _is_auto_switch_enabled(self) -> bool:
        return False

    def current_ssid(self) -> Optional[str]:
        return None

    def current_interface_name(self) -> str:
        return ""

    def visible_targets(self) -> Dict[str, bool]:
        return {
            "internal": False,
            "external": False,
        }

    def is_admin(self) -> bool:
        return False

    def get_last_switch_report(self) -> Dict[str, Any]:
        return {
            "enabled": False,
            "message": self._message(),
        }

    def connect(self, ssid: str, profile_name: str | None = None) -> Tuple[bool, str]:
        return True, self._message()
