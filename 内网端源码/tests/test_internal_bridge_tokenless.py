from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.modules.internal_bridge_http.api.routes import _require_enabled_and_authorized  # noqa: E402


def test_internal_bridge_does_not_require_token_when_legacy_config_has_token():
    bridge_cfg = {
        "enabled": True,
        "auth_token": "legacy-token-must-be-ignored",
        "allowed_client_ips": ["172.16.1.1"],
    }
    container = SimpleNamespace(
        config={"common": {"internal_bridge_http": dict(bridge_cfg)}},
        runtime_config={"internal_bridge_http": dict(bridge_cfg)},
    )
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(container=container)),
        client=SimpleNamespace(host="172.16.1.1"),
    )

    result = _require_enabled_and_authorized(request)

    assert result["enabled"] is True
