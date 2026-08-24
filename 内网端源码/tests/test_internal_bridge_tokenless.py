from __future__ import annotations

import asyncio
import sys
import threading
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.modules.internal_bridge_http.api import routes  # noqa: E402
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


def test_source_index_route_runs_on_dedicated_executor(monkeypatch):
    class Runner:
        def list_source_index(self, **_kwargs):
            return [{"thread": threading.current_thread().name}]

        def source_index_recovery_active(self, **_kwargs):
            return False

    monkeypatch.setattr(routes, "_require_enabled_and_authorized", lambda _request: {})
    monkeypatch.setattr(routes, "_runner", lambda _request: Runner())

    result = asyncio.run(routes.query_internal_source_index(SimpleNamespace()))

    assert result["ok"] is True
    assert result["entries"][0]["thread"].startswith("internal-source-index-http")
