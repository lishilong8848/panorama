import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.modules.report_pipeline.service import download_site_worker as worker
from handover_log_module.repository.download_gateway import DownloadGateway


def test_monthly_lookup_does_not_use_substring_report_names():
    selectors = DownloadGateway._report_template_selectors("全景平台月报")
    assert selectors
    assert all("has-text" not in selector and "contains(normalize-space(@title)" not in selector
               for selector in selectors)
    assert any("normalize-space(@title)='全景平台月报'" in selector for selector in selectors)


@pytest.mark.parametrize("target_page", [1, 2, 3, None])
def test_monthly_template_lookup_visits_later_pages(monkeypatch, target_page):
    gateway = DownloadGateway({})
    page_number = 1
    visited = []
    settled = []

    class Candidate:
        @property
        def first(self):
            return self

        async def wait_for(self, **_):
            visited.append(page_number)
            assert page_number in settled
            if page_number != target_page:
                raise TimeoutError("not on this page")

    candidate = Candidate()

    async def wait_settled(*_, **__):
        settled.append(page_number)

    async def next_page(*_, **__):
        nonlocal page_number
        if page_number >= 3:
            return False
        page_number += 1
        return True

    monkeypatch.setattr(gateway, "_wait_template_page_settled", wait_settled)
    monkeypatch.setattr(gateway, "_click_active_template_page_number", AsyncMock(return_value=True))
    monkeypatch.setattr(gateway, "_click_template_next_page", next_page)
    frame = SimpleNamespace(locator=lambda _: candidate)
    found = asyncio.run(gateway._find_report_template_locator(
        frame, template_name="全景平台月报", timeout_ms=10000, building="E楼",
    ))
    assert (found is candidate) == (target_page is not None)
    assert sorted(set(visited)) == list(range(1, (target_page or 3) + 1))


def test_monthly_download_reuses_gateway_and_keeps_query_contract(monkeypatch):
    inputs = {}

    class Locator:
        def __init__(self, selector):
            self.selector = selector
            self.first = self

        async def wait_for(self, **_):
            return None

        async def click(self, **_):
            return None

        async def fill(self, value):
            inputs[self.selector] = value

        async def press(self, *_):
            return None

        async def scroll_into_view_if_needed(self):
            return None

    class Page:
        def locator(self, selector):
            return Locator(selector)

        async def wait_for_selector(self, selector, **_):
            if selector == "#username":
                raise TimeoutError("already logged in")

    page = Page()
    frame = Page()
    resolver = AsyncMock(return_value=frame)
    monkeypatch.setattr(DownloadGateway, "_resolve_report_frames", resolver)
    monkeypatch.setattr(worker, "prepare_reusable_page", AsyncMock())
    monkeypatch.setattr(worker, "_wait_query_loading_started", AsyncMock())
    ready = AsyncMock(side_effect=RuntimeError("stop before export"))
    monkeypatch.setattr(worker, "_wait_query_result_ready", ready)
    success, _, error = asyncio.run(worker.download_single_url_once(
        page, "test-output", "2026-10-28 23:50:00", "2026-10-30 01:20:00",
        "http://report.test", "unused", "unused", "E楼", 180000, 1000, 1000, True,
    ))
    assert not success and error == "stop before export"
    resolver.assert_awaited_once_with(
        page, template_name="全景平台月报", force_iframe_reopen_each_task=True,
        iframe_timeout_ms=10000, debug_step_log=True, building="E楼",
    )
    assert list(inputs.values()) == ["2026-10-28 23:50:00", "2026-10-30 01:20:00"]
    assert ready.await_args.kwargs["required_text"] == "IT负载"
