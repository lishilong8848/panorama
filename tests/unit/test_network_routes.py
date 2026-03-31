from __future__ import annotations

from types import SimpleNamespace

from app.modules.network.api import routes


class _FakeContainer:
    def __init__(self, *, role_mode: str) -> None:
        self.config = {"common": {"network_switch": {"hard_recovery_enabled": True}}}
        self._role_mode = role_mode
        self.wifi_service = None
        self.logs: list[str] = []

    def deployment_snapshot(self):
        return {"role_mode": self._role_mode, "node_id": "", "node_label": ""}

    def add_system_log(self, text: str) -> None:
        self.logs.append(text)


def _fake_request(role_mode: str):
    container = _FakeContainer(role_mode=role_mode)
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_auto_switch_route_is_retired_for_switching_role() -> None:
    request = _fake_request("switching")

    payload = routes.set_auto_switch({"enabled": False}, request)

    assert payload["ok"] is True
    assert payload["retired"] is True
    assert payload["enabled"] is True
    assert payload["message"] == "单机切网端固定按切网流程执行，已不再提供自动切网开关。"
    assert request.app.state.container.logs[-1] == "[网络配置] 自动切网开关接口已退役: 角色=switching"


def test_auto_switch_route_is_retired_for_external_role() -> None:
    request = _fake_request("external")

    payload = routes.set_auto_switch({"enabled": True}, request)

    assert payload["ok"] is True
    assert payload["retired"] is True
    assert payload["enabled"] is False
    assert payload["message"] == "当前角色为内网端/外网端，不使用单机切网开关。"
    assert request.app.state.container.logs[-1] == "[网络配置] 自动切网开关接口已退役: 角色=external"


def test_network_status_switch_strategy_follows_role_mode() -> None:
    switching = routes.network_status(_fake_request("switching"))
    external = routes.network_status(_fake_request("external"))

    assert switching["switch_strategy"] == "single_machine_switching"
    assert switching["role_mode"] == "switching"
    assert external["switch_strategy"] == "role_fixed_network"
    assert external["role_mode"] == "external"
