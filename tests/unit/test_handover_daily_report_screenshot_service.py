from __future__ import annotations

import asyncio

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
