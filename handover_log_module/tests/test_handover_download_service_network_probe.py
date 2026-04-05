from __future__ import annotations

import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from handover_log_module.service import handover_download_service as module


def _base_config() -> dict:
    return {
        "download": {
            "sites": [
                {"enabled": True, "host": "192.168.210.50"},
                {"enabled": True, "host": "192.168.220.50"},
            ]
        },
        "network": {
            "enable_auto_switch_wifi": False,
            "internal_ssid": "h-dh",
            "external_ssid": "outer",
            "post_switch_probe_enabled": True,
            "post_switch_probe_external_host": "open.feishu.cn",
            "post_switch_probe_external_port": 443,
        },
        "paths": {},
    }


def test_ensure_internal_ready_skips_probe_when_auto_switch_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    messages: list[str] = []
    service = module.HandoverDownloadService(_base_config())

    def _probe_should_not_run(**_kwargs):
        raise AssertionError("internal fixed-network flow should skip reachability probe")

    monkeypatch.setattr(module, "probe_internal_reachability", _probe_should_not_run)

    service.ensure_internal_ready(messages.append)

    assert any("按当前网络直接执行内网阶段" in item for item in messages)
    assert not any("探活" in item for item in messages)


def test_ensure_external_ready_still_uses_probe_when_auto_switch_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    messages: list[str] = []
    service = module.HandoverDownloadService(_base_config())
    monkeypatch.setattr(
        module,
        "probe_external_reachability",
        lambda **_kwargs: {
            "reachable": True,
            "host": "open.feishu.cn",
            "port": 443,
            "error": "",
        },
    )

    service.ensure_external_ready(messages.append)

    assert any("直接探测external网络可达性" in item for item in messages)
    assert any("外网探活成功" in item for item in messages)


def test_ensure_external_ready_raises_probe_failure_when_probe_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    service = module.HandoverDownloadService(_base_config())
    monkeypatch.setattr(
        module,
        "probe_external_reachability",
        lambda **_kwargs: {
            "reachable": False,
            "host": "open.feishu.cn",
            "port": 443,
            "error": "外网探活失败",
        },
    )

    with pytest.raises(RuntimeError, match="external网络探活失败"):
        service.ensure_external_ready(lambda _text: None)
