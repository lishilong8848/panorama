from __future__ import annotations

from app.modules.network.service import network_stability


def test_probe_internal_reachability_succeeds_when_any_site_ping_ok(monkeypatch) -> None:  # noqa: ANN001
    attempts: list[str] = []

    def _fake_ping(host: str, timeout_ms: int) -> bool:
        attempts.append(f"{host}:{timeout_ms}")
        return host == "192.168.220.50"

    monkeypatch.setattr(network_stability, "probe_ping", _fake_ping)
    result = network_stability.probe_internal_reachability(
        network_cfg={"internal_probe_timeout_ms": 1200, "internal_probe_parallelism": 5},
        sites=[
            {"enabled": True, "host": "192.168.210.50"},
            {"enabled": True, "host": "192.168.220.50"},
            {"enabled": False, "host": "192.168.230.50"},
        ],
    )
    assert result["reachable"] is True
    assert result["successful_host"] == "192.168.220.50"
    assert "192.168.210.50" in result["attempted_hosts"]
    assert "192.168.220.50" in result["attempted_hosts"]


def test_get_network_reachability_state_prefers_current_ssid_side(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        network_stability,
        "probe_internal_reachability",
        lambda **_kwargs: {
            "reachable": True,
            "successful_host": "192.168.210.50",
            "attempted_hosts": ["192.168.210.50"],
            "error": "",
        },
    )
    monkeypatch.setattr(
        network_stability,
        "probe_external_reachability",
        lambda **_kwargs: {
            "reachable": True,
            "host": "open.feishu.cn",
            "port": 443,
            "error": "",
        },
    )
    state = network_stability.get_network_reachability_state(
        network_cfg={
            "internal_ssid": "h-dh",
            "external_ssid": "outer",
        },
        sites=[{"enabled": True, "host": "192.168.210.50"}],
        current_ssid="outer",
    )
    assert state["ssid_side"] == "external"
    assert state["internal_reachable"] is True
    assert state["external_reachable"] is True
    assert state["reachable_sides"] == ["internal", "external"]
    assert state["mode"] == "external_only"
