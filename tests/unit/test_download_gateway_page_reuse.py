from __future__ import annotations

import asyncio
import concurrent.futures
from pathlib import Path

import handover_log_module.repository.download_gateway as gateway_module
from handover_log_module.repository.download_gateway import DownloadGateway


class _FakePage:
    def __init__(self) -> None:
        self.closed = False
        self.close_calls = 0

    async def close(self) -> None:
        self.closed = True
        self.close_calls += 1

    def is_closed(self) -> bool:
        return self.closed


class _FakeContext:
    def __init__(self) -> None:
        self.new_page_calls = 0
        self.pages: list[_FakePage] = []

    async def new_page(self) -> _FakePage:
        self.new_page_calls += 1
        page = _FakePage()
        self.pages.append(page)
        return page


class _FakeBrowser:
    def __init__(self, context: _FakeContext) -> None:
        self._context = context
        self.closed = False

    async def new_context(self, **_kwargs) -> _FakeContext:
        return self._context

    async def close(self) -> None:
        self.closed = True


class _FakeChromium:
    def __init__(self, browser: _FakeBrowser) -> None:
        self._browser = browser

    async def launch(self, **_kwargs) -> _FakeBrowser:
        return self._browser


class _FakeAsyncPlaywright:
    def __init__(self, browser: _FakeBrowser) -> None:
        self.chromium = _FakeChromium(browser)

    async def __aenter__(self) -> _FakeAsyncPlaywright:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakePool:
    def __init__(self) -> None:
        self.pages = {
            "A楼": _FakePage(),
            "B楼": _FakePage(),
        }
        self.calls: list[str] = []

    def submit_building_job(self, building, runner):
        self.calls.append(building)
        future: concurrent.futures.Future = concurrent.futures.Future()

        async def _bridge():
            try:
                result = await runner(self.pages[building])
                future.set_result(result)
            except Exception as exc:  # noqa: BLE001
                future.set_exception(exc)

        asyncio.get_running_loop().create_task(_bridge())
        return future


def test_handover_batch_serial_mode_reuses_one_page(monkeypatch, tmp_path: Path) -> None:
    fake_context = _FakeContext()
    fake_browser = _FakeBrowser(fake_context)
    seen_pages: list[_FakePage] = []

    gateway = DownloadGateway(
        {
            "download": {
                "browser_headless": True,
                "browser_channel": "",
            },
            "handover_log": {
                "sites": [
                    {"building": "A楼", "enabled": True, "host": "a.example.invalid"},
                    {"building": "B楼", "enabled": True, "host": "b.example.invalid"},
                ]
            },
        }
    )

    async def _fake_download_single_building_with_retry(self, page, **kwargs):
        seen_pages.append(page)
        return {
            "success": True,
            "building": kwargs["building"],
            "file_path": str(tmp_path / f'{kwargs["building"]}.xlsx'),
            "used_url": "http://example.invalid/page/main/main.html",
            "error": "",
            "failed_step": "",
        }

    monkeypatch.setattr(
        gateway_module,
        "configure_playwright_environment",
        lambda _config: None,
    )
    monkeypatch.setattr(
        gateway_module,
        "async_playwright",
        lambda: _FakeAsyncPlaywright(fake_browser),
    )
    monkeypatch.setattr(
        DownloadGateway,
        "_download_single_building_with_retry",
        _fake_download_single_building_with_retry,
    )

    rows = asyncio.run(
        gateway._download_handover_xlsx_batch_async(
            buildings=["A楼", "B楼"],
            start_time="2026-03-29 00:00:00",
            end_time="2026-03-29 01:00:00",
            scale_label="小时",
            template_name="交接班日志（李世龙）",
            save_dir=str(tmp_path),
            query_result_timeout_ms=1000,
            download_event_timeout_ms=1000,
            login_fill_timeout_ms=1000,
            menu_visible_timeout_ms=1000,
            iframe_timeout_ms=1000,
            start_end_visible_timeout_ms=1000,
            page_refresh_retry_count=0,
            max_retries=1,
            retry_wait_sec=0,
            force_iframe_reopen_each_task=False,
            export_button_text="导出",
            menu_path=["报表报告", "数据查询", "即时报表"],
            parallel_by_building=False,
            site_start_delay_sec=0,
            debug_step_log=False,
        )
    )

    assert [row["building"] for row in rows] == ["A楼", "B楼"]
    assert fake_context.new_page_calls == 1
    assert len(seen_pages) == 2
    assert seen_pages[0] is seen_pages[1]
    assert fake_context.pages[0].close_calls == 1


def test_handover_batch_uses_browser_pool_pages(monkeypatch, tmp_path: Path) -> None:
    fake_pool = _FakePool()
    seen_pages: list[_FakePage] = []

    gateway = DownloadGateway(
        {
            "download": {
                "browser_headless": True,
                "browser_channel": "",
            },
            "handover_log": {
                "sites": [
                    {"building": "A楼", "enabled": True, "host": "a.example.invalid"},
                    {"building": "B楼", "enabled": True, "host": "b.example.invalid"},
                ]
            },
        }
    )

    async def _fake_download_single_building_with_retry(self, page, **kwargs):
        seen_pages.append(page)
        return {
            "success": True,
            "building": kwargs["building"],
            "file_path": str(tmp_path / f'{kwargs["building"]}.xlsx'),
            "used_url": "http://example.invalid/page/main/main.html",
            "error": "",
            "failed_step": "",
        }

    monkeypatch.setattr(
        gateway_module,
        "async_playwright",
        lambda: (_ for _ in ()).throw(AssertionError("browser pool path should not launch local browser")),
    )
    monkeypatch.setattr(
        DownloadGateway,
        "_download_single_building_with_retry",
        _fake_download_single_building_with_retry,
    )

    rows = asyncio.run(
        gateway._download_handover_xlsx_batch_async(
            buildings=["A楼", "B楼"],
            start_time="2026-03-29 00:00:00",
            end_time="2026-03-29 01:00:00",
            scale_label="小时",
            template_name="交接班日志（李世龙）",
            save_dir=str(tmp_path),
            query_result_timeout_ms=1000,
            download_event_timeout_ms=1000,
            login_fill_timeout_ms=1000,
            menu_visible_timeout_ms=1000,
            iframe_timeout_ms=1000,
            start_end_visible_timeout_ms=1000,
            page_refresh_retry_count=0,
            max_retries=1,
            retry_wait_sec=0,
            force_iframe_reopen_each_task=False,
            export_button_text="导出",
            menu_path=["报表报告", "数据查询", "即时报表"],
            parallel_by_building=True,
            site_start_delay_sec=0,
            debug_step_log=False,
            browser_pool=fake_pool,
        )
    )

    assert [row["building"] for row in rows] == ["A楼", "B楼"]
    assert fake_pool.calls == ["A楼", "B楼"]
    assert seen_pages[0] is fake_pool.pages["A楼"]
    assert seen_pages[1] is fake_pool.pages["B楼"]


def test_handover_batch_waits_for_late_browser_pool(monkeypatch, tmp_path: Path) -> None:
    fake_pool = _FakePool()
    seen_pages: list[_FakePage] = []
    pool_reads = {"count": 0}

    gateway = DownloadGateway(
        {
            "download": {
                "browser_headless": True,
                "browser_channel": "",
                "browser_pool_wait_timeout_sec": 1,
            },
            "handover_log": {
                "sites": [
                    {"building": "A楼", "enabled": True, "host": "a.example.invalid"},
                ]
            },
        }
    )

    async def _fake_download_single_building_with_retry(self, page, **kwargs):
        seen_pages.append(page)
        return {
            "success": True,
            "building": kwargs["building"],
            "file_path": str(tmp_path / f'{kwargs["building"]}.xlsx'),
            "used_url": "http://example.invalid/page/main/main.html",
            "error": "",
            "failed_step": "",
        }

    def _fake_get_pool():
        pool_reads["count"] += 1
        if pool_reads["count"] < 3:
            return None
        return fake_pool

    monkeypatch.setattr(
        gateway_module,
        "_RUNTIME_CONFIG",
        {"deployment": {"role_mode": "internal"}, "download": {"browser_pool_wait_timeout_sec": 1}},
    )
    monkeypatch.setattr(
        gateway_module,
        "get_internal_download_browser_pool",
        _fake_get_pool,
    )
    monkeypatch.setattr(
        gateway_module,
        "async_playwright",
        lambda: (_ for _ in ()).throw(AssertionError("late browser pool path should not launch local browser")),
    )
    monkeypatch.setattr(
        DownloadGateway,
        "_download_single_building_with_retry",
        _fake_download_single_building_with_retry,
    )

    rows = asyncio.run(
        gateway._download_handover_xlsx_batch_async(
            buildings=["A楼"],
            start_time="2026-03-29 00:00:00",
            end_time="2026-03-29 01:00:00",
            scale_label="小时",
            template_name="交接班日志（李世龙）",
            save_dir=str(tmp_path),
            query_result_timeout_ms=1000,
            download_event_timeout_ms=1000,
            login_fill_timeout_ms=1000,
            menu_visible_timeout_ms=1000,
            iframe_timeout_ms=1000,
            start_end_visible_timeout_ms=1000,
            page_refresh_retry_count=0,
            max_retries=1,
            retry_wait_sec=0,
            force_iframe_reopen_each_task=False,
            export_button_text="导出",
            menu_path=["报表报告", "数据查询", "即时报表"],
            parallel_by_building=False,
            site_start_delay_sec=0,
            debug_step_log=False,
            browser_pool=None,
        )
    )

    assert [row["building"] for row in rows] == ["A楼"]
    assert fake_pool.calls == ["A楼"]
    assert seen_pages[0] is fake_pool.pages["A楼"]
