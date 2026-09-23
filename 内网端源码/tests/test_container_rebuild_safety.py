import pytest
from types import SimpleNamespace
from unittest.mock import Mock

from app.bootstrap.container import AppContainer


@pytest.mark.parametrize("entry", ["role_change", "reload_config"])
def test_rebuild_keeps_old_bridge_when_cleanup_has_not_finished(entry, monkeypatch):
    container = AppContainer.__new__(AppContainer)
    old = SimpleNamespace(
        stop=Mock(return_value={"running": True, "reason": "cleanup_timeout"}),
        is_running=lambda: True,
        get_deployment_snapshot=lambda: {"role_mode": "external"},
    )
    container.shared_bridge_service = old
    builder = Mock()
    monkeypatch.setattr(container, "_build_shared_bridge_service", builder)
    if entry == "role_change":
        monkeypatch.setattr(container, "_configured_deployment_snapshot", lambda: {"role_mode": "internal"})
        monkeypatch.setattr(container, "_shared_bridge_runtime_role_mode", lambda: "external")
        action = container._ensure_shared_bridge_service_matches_configured_role
    else:
        monkeypatch.setattr(container, "_apply_runtime_config_snapshot", lambda config: None)
        action = lambda: container.reload_config({})
    with pytest.raises(RuntimeError, match="仍在关闭"):
        action()
    assert container.shared_bridge_service is old
    builder.assert_not_called()

