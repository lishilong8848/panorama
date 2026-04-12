from __future__ import annotations

import asyncio
import json
import subprocess

from handover_log_module.service.handover_daily_report_screenshot_service import (
    HandoverDailyReportScreenshotService,
)


class _FakePage:
    def __init__(self, url: str) -> None:
        self.url = url

    def is_closed(self) -> bool:
        return False


class _FakeContext:
    def __init__(self, pages) -> None:  # noqa: ANN001
        self.pages = list(pages)


class _FakeBrowser:
    def __init__(self, contexts) -> None:  # noqa: ANN001
        self.contexts = list(contexts)


def _service() -> HandoverDailyReportScreenshotService:
    return HandoverDailyReportScreenshotService({"_global_paths": {}, "daily_report_bitable_export": {}})


def test_auth_state_from_existing_pages_async_returns_ready_for_logged_in_feishu_page(monkeypatch) -> None:
    service = _service()
    browser = _FakeBrowser([_FakeContext([_FakePage("https://vnet.feishu.cn/app/demo?pageId=p1")])])

    async def _looks_like_login(_page) -> bool:  # noqa: ANN001
        return False

    monkeypatch.setattr(service, "_looks_like_login_page_async", _looks_like_login)

    result = asyncio.run(service._auth_state_from_existing_pages_async(browser))

    assert result == {"status": "ready", "error": ""}


def test_auth_state_from_existing_pages_async_reports_missing_login_for_login_page(monkeypatch) -> None:
    service = _service()
    browser = _FakeBrowser([_FakeContext([_FakePage("https://vnet.feishu.cn/app/demo?pageId=p1")])])

    async def _looks_like_login(_page) -> bool:  # noqa: ANN001
        return True

    monkeypatch.setattr(service, "_looks_like_login_page_async", _looks_like_login)

    result = asyncio.run(service._auth_state_from_existing_pages_async(browser))

    assert result == {"status": "missing_login", "error": "login_required"}


def test_authenticated_feishu_url_is_not_treated_as_login_even_if_query_contains_login() -> None:
    service = _service()

    assert service._looks_like_authenticated_feishu_url(
        "https://vnet.feishu.cn/app/demo?pageId=login_view&from=login"
    ) is True
    assert service._looks_like_login_url(
        "https://vnet.feishu.cn/app/demo?pageId=login_view&from=login"
    ) is False


def test_accounts_login_url_is_treated_as_login_page() -> None:
    service = _service()

    assert service._looks_like_login_url("https://accounts.feishu.cn/login/index") is True


def test_resolve_system_browser_prefers_edge_over_running_chrome(monkeypatch) -> None:
    service = _service()
    chrome_meta = {
        "browser_kind": "chrome",
        "browser_label": "Google Chrome",
        "profile_dir": r"C:\Chrome",
    }
    edge_meta = {
        "browser_kind": "edge",
        "browser_label": "Microsoft Edge",
        "profile_dir": r"C:\Edge",
    }

    monkeypatch.setattr(service, "_probe_debug_endpoint", lambda: {"Browser": "Google Chrome 136.0"})
    monkeypatch.setattr(service, "_browser_meta_from_debug_payload", lambda _payload: chrome_meta)
    monkeypatch.setattr(
        service,
        "_resolve_browser_meta_by_kind",
        lambda kind: edge_meta if kind == "edge" else chrome_meta,
    )

    resolved = service._resolve_system_browser(prefer_running_debug=True)

    assert resolved == edge_meta


def test_ensure_browser_debug_ready_rejects_running_chrome_when_edge_is_available(monkeypatch) -> None:
    service = _service()
    chrome_meta = {
        "browser_kind": "chrome",
        "browser_label": "Google Chrome",
        "profile_dir": r"C:\Chrome",
    }
    edge_meta = {"browser_kind": "edge", "browser_label": "Microsoft Edge", "profile_dir": r"C:\Edge"}

    monkeypatch.setattr(service, "_probe_debug_endpoint", lambda: {"Browser": "Google Chrome 136.0"})
    monkeypatch.setattr(service, "_browser_meta_from_debug_payload", lambda _payload: chrome_meta)
    monkeypatch.setattr(service, "_resolve_browser_meta_by_kind", lambda kind: edge_meta if kind == "edge" else chrome_meta)

    ok, error, matched = service.ensure_browser_debug_ready(startup_url="https://example.com", emit_log=lambda *_args: None)

    assert ok is False
    assert matched == ""
    assert "Microsoft Edge" in error


def test_resolve_profile_directory_name_prefers_configured_profile(tmp_path, monkeypatch) -> None:
    user_data_dir = tmp_path / "User Data"
    (user_data_dir / "Profile 2").mkdir(parents=True)
    (user_data_dir / "Default").mkdir(parents=True)
    (user_data_dir / "Local State").write_text(
        json.dumps({"profile": {"last_used": "Default"}}),
        encoding="utf-8",
    )
    service = HandoverDailyReportScreenshotService(
        {
            "_global_paths": {},
            "daily_report_bitable_export": {
                "browser_profile_directory": "Profile 2",
            },
        }
    )
    monkeypatch.setattr(service, "_profile_dir", lambda _browser_meta=None: user_data_dir)

    result = service._resolve_profile_directory_name({"browser_kind": "chrome"})

    assert result == "Profile 2"


def test_resolve_profile_directory_name_uses_last_used_when_config_missing(tmp_path, monkeypatch) -> None:
    user_data_dir = tmp_path / "User Data"
    (user_data_dir / "Profile 1").mkdir(parents=True)
    (user_data_dir / "Default").mkdir(parents=True)
    (user_data_dir / "Local State").write_text(
        json.dumps({"profile": {"last_used": "Profile 1"}}),
        encoding="utf-8",
    )
    service = _service()
    monkeypatch.setattr(service, "_profile_dir", lambda _browser_meta=None: user_data_dir)

    result = service._resolve_profile_directory_name({"browser_kind": "chrome"})

    assert result == "Profile 1"


def test_resolve_profile_directory_name_falls_back_to_default(tmp_path, monkeypatch) -> None:
    user_data_dir = tmp_path / "User Data"
    (user_data_dir / "Default").mkdir(parents=True)
    service = _service()
    monkeypatch.setattr(service, "_profile_dir", lambda _browser_meta=None: user_data_dir)

    result = service._resolve_profile_directory_name({"browser_kind": "chrome"})

    assert result == "Default"


def test_profile_dir_uses_runtime_managed_browser_directory(tmp_path) -> None:
    service = HandoverDailyReportScreenshotService(
        {
            "_global_paths": {
                "runtime_state_root": str(tmp_path),
            },
            "daily_report_bitable_export": {},
        }
    )

    result = service._profile_dir({"browser_kind": "edge"})

    assert result == tmp_path / "handover" / "daily_report_browser" / "edge"


def test_start_system_browser_uses_managed_runtime_user_data_dir(tmp_path, monkeypatch) -> None:
    service = HandoverDailyReportScreenshotService(
        {
            "_global_paths": {
                "runtime_state_root": str(tmp_path),
            },
            "daily_report_bitable_export": {},
        }
    )
    edge_meta = service._build_browser_meta("edge", executable_path="msedge.exe")
    captured: dict[str, object] = {}

    class _FakePopen:
        def __init__(self, command):  # noqa: ANN001
            captured["command"] = list(command)

    monkeypatch.setattr(service, "_resolve_system_browser", lambda prefer_running_debug=False: edge_meta)
    monkeypatch.setattr(service, "_wait_for_debug_endpoint", lambda timeout_sec=15.0: True)
    monkeypatch.setattr(subprocess, "Popen", _FakePopen)

    ok, error = service._start_system_browser(url="https://example.com", emit_log=lambda *_args: None)

    assert ok is True
    assert error == ""
    command = [str(item) for item in (captured.get("command") or [])]
    assert any(str(tmp_path / "handover" / "daily_report_browser" / "edge") in item for item in command)
    assert "--remote-debugging-port=29333" in command


def test_check_auth_status_preserves_debug_port_failure_state(tmp_path, monkeypatch) -> None:
    service = HandoverDailyReportScreenshotService(
        {
            "_global_paths": {
                "runtime_state_root": str(tmp_path),
            },
            "daily_report_bitable_export": {},
        }
    )
    browser_meta = service._build_browser_meta("chrome", executable_path="chrome.exe")
    service._state_service.update_screenshot_auth_state(
        service._auth_state_payload(
            status="browser_unavailable",
            error="browser_debug_port_unavailable: 请先关闭所有 Google Chrome 窗口后重试",
            browser_meta=browser_meta,
        )
    )
    monkeypatch.setattr(service, "_resolve_system_browser", lambda prefer_running_debug=True: browser_meta)
    monkeypatch.setattr(service, "_probe_debug_endpoint", lambda: None)

    result = asyncio.run(service.check_auth_status_async(ensure_browser_running=False))

    assert result["status"] == "browser_unavailable"
    assert "browser_debug_port_unavailable" in str(result["error"])


def test_open_login_browser_logs_failure_reason(monkeypatch) -> None:
    service = _service()
    browser_meta = service._build_browser_meta("edge", executable_path="msedge.exe")
    logs: list[str] = []

    monkeypatch.setattr(service, "_resolve_system_browser", lambda prefer_running_debug=True: browser_meta)

    async def _failing_to_thread(func, *args, **kwargs):  # noqa: ANN001
        return False, "browser_debug_port_unavailable: 调试端口当前被Google Chrome占用", ""

    monkeypatch.setattr(asyncio, "to_thread", _failing_to_thread)

    result = asyncio.run(service.open_login_browser_async(emit_log=logs.append))

    assert result["ok"] is False
    assert any("开始初始化" in line for line in logs)
    assert any("初始化失败" in line and "browser_debug_port_unavailable" in line for line in logs)
