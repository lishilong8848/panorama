from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.modules.report_pipeline.service.pipeline_notify_runtime import (
    PendingNotifyEvent,
    flush_pending_notify_events,
    is_auto_switch_wifi_enabled,
    notify_event,
)


class _FakeWifi:
    def __init__(self, ssid: str) -> None:
        self._ssid = ssid

    def get_current_ssid(self) -> str:
        return self._ssid


def _base_config() -> Dict[str, Any]:
    return {
        "notify": {
            "enable_webhook": True,
            "feishu_webhook_url": "https://example.com/hook",
            "keyword": "事件",
            "timeout": 10,
            "on_download_failure": True,
            "on_wifi_failure": True,
            "on_upload_failure": True,
        },
        "network": {
            "enable_auto_switch_wifi": True,
        },
    }


def test_is_auto_switch_wifi_enabled_defaults_true() -> None:
    assert is_auto_switch_wifi_enabled({}) is True
    assert is_auto_switch_wifi_enabled({"network": "bad"}) is True
    assert is_auto_switch_wifi_enabled({"network": {"enable_auto_switch_wifi": False}}) is False


def test_notify_event_queue_when_not_external_and_auto_switch_enabled() -> None:
    config = _base_config()
    pending: List[PendingNotifyEvent] = []
    sent: List[str] = []

    notify_event(
        config=config,
        stage="内网下载",
        detail="download failed",
        building="A楼",
        toggle_key="on_download_failure",
        wifi=_FakeWifi("e-donghuan"),
        external_ssid="EL-BG",
        pending_events=pending,
        build_event_text=lambda **kwargs: str(kwargs),
        send_feishu_webhook=lambda *_args, **_kwargs: sent.append("sent") or (True, "ok"),
        emit_log=lambda _msg: None,
    )

    assert len(pending) == 1
    assert sent == []


def test_notify_event_send_direct_when_auto_switch_disabled() -> None:
    config = _base_config()
    config["network"]["enable_auto_switch_wifi"] = False
    sent: List[str] = []

    notify_event(
        config=config,
        stage="计算上传",
        detail="upload failed",
        toggle_key="on_upload_failure",
        wifi=_FakeWifi("e-donghuan"),
        external_ssid="EL-BG",
        pending_events=[],
        build_event_text=lambda **kwargs: f"stage={kwargs['stage']}",
        send_feishu_webhook=lambda *_args, **_kwargs: sent.append("sent") or (True, "ok"),
        emit_log=lambda _msg: None,
    )

    assert sent == ["sent"]


def test_flush_pending_notify_events_switch_then_send() -> None:
    called: List[str] = []
    pending = [
        PendingNotifyEvent(stage="内网下载", detail="x", building="A楼", toggle_key="on_download_failure"),
        PendingNotifyEvent(stage="计算上传", detail="y", building="B楼", toggle_key="on_upload_failure"),
    ]

    def _try_switch_wifi(**_kwargs: Any) -> tuple[bool, str, bool]:
        called.append("switch")
        return True, "ok", False

    def _notify_event(**kwargs: Any) -> None:
        called.append(f"notify:{kwargs['stage']}")

    flush_pending_notify_events(
        config=_base_config(),
        wifi=_FakeWifi("e-donghuan"),
        external_ssid="EL-BG",
        external_profile_name="EL-BG",
        require_saved_profile=True,
        enable_auto_switch_wifi=True,
        pending_events=pending,
        try_switch_wifi=_try_switch_wifi,
        notify_event=_notify_event,
        emit_log=lambda _msg: None,
    )

    assert called == ["switch", "notify:内网下载", "notify:计算上传"]
    assert pending == []


def test_flush_pending_notify_events_keep_queue_when_switch_failed() -> None:
    pending = [PendingNotifyEvent(stage="内网下载", detail="x", building="A楼")]

    flush_pending_notify_events(
        config=_base_config(),
        wifi=_FakeWifi("e-donghuan"),
        external_ssid="EL-BG",
        external_profile_name=None,
        require_saved_profile=True,
        enable_auto_switch_wifi=True,
        pending_events=pending,
        try_switch_wifi=lambda **_kwargs: (False, "failed", False),
        notify_event=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("should not call")),
        emit_log=lambda _msg: None,
    )

    assert len(pending) == 1
