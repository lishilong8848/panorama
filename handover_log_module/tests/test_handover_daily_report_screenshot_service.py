import handover_log_module.service.handover_daily_report_screenshot_service as screenshot_module
from handover_log_module.service.handover_daily_report_screenshot_service import (
    HandoverDailyReportScreenshotService,
)
from types import SimpleNamespace


def test_screenshot_service_uses_fixed_page_urls(monkeypatch):
    service = HandoverDailyReportScreenshotService(
        {
            "daily_report_bitable_export": {
                "summary_page_url": "https://example.com/app/old-summary",
                "external_page_url": "https://example.com/app/old-external",
            }
        }
    )
    calls = []

    def fake_capture(**kwargs):
        calls.append(kwargs)
        return {"status": "ok", "path": "ok.png", "error": ""}

    monkeypatch.setattr(service, "_capture_sheet_like_page", fake_capture)

    summary = service.capture_summary_sheet(duty_date="2026-03-25", duty_shift="night", emit_log=lambda *_args: None)
    external = service.capture_external_page(duty_date="2026-03-25", duty_shift="night", emit_log=lambda *_args: None)

    assert summary["status"] == "ok"
    assert external["status"] == "ok"
    assert calls[0]["url"] == service.DEFAULT_SUMMARY_PAGE_URL
    assert calls[1]["url"] == service.DEFAULT_EXTERNAL_PAGE_URL


class _FakePage:
    def __init__(self, url: str) -> None:
        self.url = url
        self.goto_calls = []
        self.brought_to_front = False

    def is_closed(self):
        return False

    def goto(self, url: str, **_kwargs):
        self.url = url
        self.goto_calls.append(url)

    def bring_to_front(self):
        self.brought_to_front = True


class _FakeContext:
    def __init__(self, pages):
        self.pages = pages
        self.created_pages = []

    def new_page(self):
        page = _FakePage("about:blank")
        self.pages.append(page)
        self.created_pages.append(page)
        return page


def _browser_meta(kind: str, executable: str = ""):
    service = HandoverDailyReportScreenshotService({})
    return service._build_browser_meta(kind, executable_path=executable or f"C:/{kind}.exe")


def test_find_matching_page_requires_same_page_id():
    service = HandoverDailyReportScreenshotService({})
    context = _FakeContext(
        [
            _FakePage(service.DEFAULT_EXTERNAL_PAGE_URL),
            _FakePage("https://example.com/other"),
        ]
    )
    browser = SimpleNamespace(contexts=[context])

    matched = service._find_matching_page(browser, target_url=service.DEFAULT_SUMMARY_PAGE_URL)

    assert matched is None


def test_resolve_system_browser_prefers_edge_over_chrome(monkeypatch):
    service = HandoverDailyReportScreenshotService({})

    def fake_resolve(kind):
        if kind == "edge":
            return _browser_meta("edge", "C:/Program Files/Microsoft/Edge/Application/msedge.exe")
        if kind == "chrome":
            return _browser_meta("chrome", "C:/Program Files/Google/Chrome/Application/chrome.exe")
        return None

    monkeypatch.setattr(service, "_resolve_browser_meta_by_kind", fake_resolve)
    monkeypatch.setattr(service, "_probe_debug_endpoint", lambda: None)

    browser = service._resolve_system_browser(prefer_running_debug=False)

    assert browser is not None
    assert browser["browser_kind"] == "edge"


def test_resolve_system_browser_falls_back_to_chrome(monkeypatch):
    service = HandoverDailyReportScreenshotService({})

    def fake_resolve(kind):
        if kind == "chrome":
            return _browser_meta("chrome", "C:/Program Files/Google/Chrome/Application/chrome.exe")
        return None

    monkeypatch.setattr(service, "_resolve_browser_meta_by_kind", fake_resolve)
    monkeypatch.setattr(service, "_probe_debug_endpoint", lambda: None)

    browser = service._resolve_system_browser(prefer_running_debug=False)

    assert browser is not None
    assert browser["browser_kind"] == "chrome"


def test_find_matching_page_matches_same_page_id_with_different_query_order():
    service = HandoverDailyReportScreenshotService({})
    context = _FakeContext(
        [
            _FakePage(
                "https://vnet.feishu.cn/app/LTjUbmZsTaTFIVsuQSLcUi4Onf4?foo=1&pageId=pgeZUMIpMDuIIfLA&bar=2"
            )
        ]
    )
    browser = SimpleNamespace(contexts=[context])

    matched = service._find_matching_page(browser, target_url=service.DEFAULT_SUMMARY_PAGE_URL)

    assert matched is not None
    assert matched.url.endswith("bar=2")


def test_ensure_target_page_reuses_existing_page():
    service = HandoverDailyReportScreenshotService({})
    page = _FakePage(service.DEFAULT_SUMMARY_PAGE_URL)
    context = _FakeContext([page])
    browser = SimpleNamespace(contexts=[context])

    resolved_page, matched_mode = service.ensure_target_page(
        browser,
        target_url=service.DEFAULT_SUMMARY_PAGE_URL,
        emit_log=lambda *_args: None,
        open_if_missing=True,
    )

    assert resolved_page is page
    assert matched_mode == "reused"
    assert context.created_pages == []


def test_ensure_target_page_opens_missing_target():
    service = HandoverDailyReportScreenshotService({})
    context = _FakeContext([_FakePage(service.DEFAULT_EXTERNAL_PAGE_URL)])
    browser = SimpleNamespace(contexts=[context])

    resolved_page, matched_mode = service.ensure_target_page(
        browser,
        target_url=service.DEFAULT_SUMMARY_PAGE_URL,
        emit_log=lambda *_args: None,
        open_if_missing=True,
    )

    assert matched_mode == "opened_missing_target"
    assert len(context.created_pages) == 1
    assert resolved_page is context.created_pages[0]
    assert resolved_page.goto_calls == [service.DEFAULT_SUMMARY_PAGE_URL]


def test_check_auth_status_uses_requested_startup_url(monkeypatch):
    service = HandoverDailyReportScreenshotService({})
    observed = {}
    browser = SimpleNamespace(contexts=[_FakeContext([_FakePage(service.DEFAULT_SUMMARY_PAGE_URL)])])

    class _FakePlaywrightContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(service, "_playwright_context", lambda: _FakePlaywrightContext())

    def fake_ensure_browser_debug_ready(*, startup_url, emit_log):
        observed["startup_url"] = startup_url
        return True, "", "opened_browser_startup"

    monkeypatch.setattr(service, "ensure_browser_debug_ready", fake_ensure_browser_debug_ready)
    monkeypatch.setattr(
        service,
        "_connect_browser",
        lambda playwright, *, ensure_started, open_url, emit_log: browser,
    )

    state = service.check_auth_status(
        emit_log=lambda *_args: None,
        ensure_browser_running=True,
        startup_url=service.DEFAULT_SUMMARY_PAGE_URL,
    )

    assert observed["startup_url"] == service.DEFAULT_SUMMARY_PAGE_URL
    assert state["status"] == "ready"


def test_check_auth_status_includes_browser_metadata(monkeypatch):
    service = HandoverDailyReportScreenshotService({})
    browser = SimpleNamespace(contexts=[_FakeContext([_FakePage(service.DEFAULT_SUMMARY_PAGE_URL)])])
    chrome_meta = _browser_meta("chrome", "C:/Program Files/Google/Chrome/Application/chrome.exe")

    class _FakePlaywrightContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(service, "_playwright_context", lambda: _FakePlaywrightContext())
    monkeypatch.setattr(service, "_resolve_system_browser", lambda prefer_running_debug=True: chrome_meta)
    monkeypatch.setattr(service, "_browser_meta_from_debug_payload", lambda payload: chrome_meta)
    monkeypatch.setattr(service, "_probe_debug_endpoint", lambda: {"Browser": "Chrome/123"})
    monkeypatch.setattr(
        service,
        "_connect_browser",
        lambda playwright, *, ensure_started, open_url, emit_log: browser,
    )

    state = service.check_auth_status(
        emit_log=lambda *_args: None,
        ensure_browser_running=False,
        startup_url=service.DEFAULT_SUMMARY_PAGE_URL,
    )

    assert state["status"] == "ready"
    assert state["browser_kind"] == "chrome"
    assert state["browser_label"] == "Google Chrome"
    assert state["browser_executable"].endswith("chrome.exe")


def test_check_auth_status_without_browser_returns_browser_unavailable(monkeypatch):
    service = HandoverDailyReportScreenshotService({})
    monkeypatch.setattr(service, "_resolve_system_browser", lambda prefer_running_debug=True: None)
    monkeypatch.setattr(service, "_probe_debug_endpoint", lambda: None)

    class _FakePlaywrightContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(service, "_playwright_context", lambda: _FakePlaywrightContext())

    state = service.check_auth_status(emit_log=lambda *_args: None, ensure_browser_running=False)

    assert state["status"] == "browser_unavailable"
    assert "未找到系统 Edge 或 Chrome" in state["error"]


def test_open_login_browser_reuses_existing_page(monkeypatch):
    service = HandoverDailyReportScreenshotService({})
    page = _FakePage(service.DEFAULT_EXTERNAL_PAGE_URL)
    browser = SimpleNamespace(contexts=[_FakeContext([page])])
    observed = {}
    chrome_meta = _browser_meta("chrome", "C:/Program Files/Google/Chrome/Application/chrome.exe")

    class _FakePlaywrightContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(service, "_playwright_context", lambda: _FakePlaywrightContext())
    monkeypatch.setattr(service, "_resolve_system_browser", lambda prefer_running_debug=True: chrome_meta)
    monkeypatch.setattr(service, "_browser_meta_from_debug_payload", lambda payload: chrome_meta)
    monkeypatch.setattr(service, "_probe_debug_endpoint", lambda: {"Browser": "Chrome/123"})
    monkeypatch.setattr(
        service,
        "ensure_browser_debug_ready",
        lambda *, startup_url, emit_log: (True, "", "reused"),
    )
    monkeypatch.setattr(
        service,
        "_connect_browser",
        lambda playwright, *, ensure_started, open_url, emit_log: browser,
    )

    def fake_ensure_target_page(_browser, *, target_url, emit_log, open_if_missing):  # noqa: ARG001
        observed["target_url"] = target_url
        observed["open_if_missing"] = open_if_missing
        return page, "reused"

    monkeypatch.setattr(service, "ensure_target_page", fake_ensure_target_page)
    monkeypatch.setattr(screenshot_module, "_LOGIN_BROWSER_THREAD", SimpleNamespace(is_alive=lambda: True))

    result = service.open_login_browser(emit_log=lambda *_args: None)

    assert result["ok"] is True
    assert result["browser_kind"] == "chrome"
    assert "Google Chrome" in result["message"]
    assert observed["target_url"] == service.DEFAULT_EXTERNAL_PAGE_URL
    assert observed["open_if_missing"] is True
    assert page.brought_to_front is True


def test_capture_failure_result_preserves_target_resolution_meta():
    result = HandoverDailyReportScreenshotService._capture_failure_result(
        stage="find_existing_page",
        error="target_page_mismatch",
        error_detail="mismatch",
        error_message="当前打开页面与目标页面不一致，请重新打开对应飞书页面后重试。",
        resolved_url="https://example.com/current",
        resolved_page_id="pgecZCUXaEtvP9Yl",
        matched_mode="reused",
    )

    assert result["error"] == "target_page_mismatch"
    assert result["resolved_url"] == "https://example.com/current"
    assert result["resolved_page_id"] == "pgecZCUXaEtvP9Yl"
    assert result["matched_mode"] == "reused"
