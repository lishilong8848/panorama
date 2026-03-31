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


def test_ensure_internal_ready_uses_probe_when_auto_switch_disabled(monkeypatch) -> None:  # noqa: ANN001
    messages: list[str] = []
    service = module.HandoverDownloadService(_base_config())
    monkeypatch.setattr(
        module,
        "probe_internal_reachability",
        lambda **kwargs: {
            "reachable": True,
            "successful_host": "192.168.210.50",
            "attempted_hosts": [site["host"] for site in kwargs["sites"]],
            "error": "",
        },
    )

    service.ensure_internal_ready(messages.append)

    assert any("当前角色不使用单机切网" in item for item in messages)
    assert any("内网探活成功" in item for item in messages)


def test_ensure_internal_ready_raises_probe_failure_instead_of_ssid_error(monkeypatch) -> None:  # noqa: ANN001
    service = module.HandoverDownloadService(_base_config())
    monkeypatch.setattr(
        module,
        "probe_internal_reachability",
        lambda **_kwargs: {
            "reachable": False,
            "successful_host": "",
            "attempted_hosts": ["192.168.210.50", "192.168.220.50"],
            "error": "5 个楼栋站点 IP 均不可达",
        },
    )

    with pytest.raises(RuntimeError, match="internal网络探活失败"):
        service.ensure_internal_ready(lambda _text: None)
