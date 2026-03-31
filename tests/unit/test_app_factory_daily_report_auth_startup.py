from __future__ import annotations

from app.bootstrap import app_factory


class _FakeContainer:
    def __init__(self) -> None:
        self.runtime_config = {}
        self.logs: list[str] = []

    def add_system_log(self, message: str) -> None:
        self.logs.append(str(message))


def test_initialize_handover_daily_report_auth_opens_system_browser(monkeypatch) -> None:
    events: list[str] = []

    class _ScreenshotService:
        def __init__(self, handover_cfg):
            assert handover_cfg["daily_report_bitable_export"]["enabled"] is True

        def open_login_browser(self, emit_log):
            events.append("open")
            emit_log("opened")
            return {"status": "opened", "message": "browser opened"}

    monkeypatch.setattr(
        app_factory,
        "load_handover_config",
        lambda _runtime_config: {"daily_report_bitable_export": {"enabled": True}},
    )
    monkeypatch.setattr(app_factory, "HandoverDailyReportScreenshotService", _ScreenshotService)

    container = _FakeContainer()
    app_factory._initialize_handover_daily_report_auth(container)

    assert events == ["open"]
    assert any("启动自动初始化: browser opened" in line for line in container.logs)


def test_initialize_handover_daily_report_auth_logs_failed_open(monkeypatch) -> None:
    class _ScreenshotService:
        def __init__(self, _handover_cfg):
            pass

        def open_login_browser(self, emit_log):
            emit_log("failed")
            return {"status": "failed", "message": "browser_debug_port_unavailable"}

    monkeypatch.setattr(
        app_factory,
        "load_handover_config",
        lambda _runtime_config: {"daily_report_bitable_export": {"enabled": True}},
    )
    monkeypatch.setattr(app_factory, "HandoverDailyReportScreenshotService", _ScreenshotService)

    container = _FakeContainer()
    app_factory._initialize_handover_daily_report_auth(container)

    assert any("启动自动初始化: browser_debug_port_unavailable" in line for line in container.logs)
