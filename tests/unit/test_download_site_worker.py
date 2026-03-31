import asyncio

from app.modules.report_pipeline.service.download_site_worker import (
    _build_query_end_time_for_hour_scale,
    download_site_with_retry,
)


def test_build_query_end_time_for_hour_scale_adds_one_second_on_hour_boundary():
    query_end, adjusted = _build_query_end_time_for_hour_scale(
        "2026-03-10 00:00:00",
        "2026-03-11 00:00:00",
    )
    assert adjusted is True
    assert query_end == "2026-03-11 00:00:01"


def test_build_query_end_time_for_hour_scale_keeps_non_boundary_time():
    query_end, adjusted = _build_query_end_time_for_hour_scale(
        "2026-03-10 00:00:00",
        "2026-03-10 23:59:59",
    )
    assert adjusted is False
    assert query_end == "2026-03-10 23:59:59"


def test_build_query_end_time_for_hour_scale_keeps_invalid_text():
    query_end, adjusted = _build_query_end_time_for_hour_scale(
        "invalid",
        "2026-03-11 00:00:00",
    )
    assert adjusted is False
    assert query_end == "2026-03-11 00:00:00"


def test_build_query_end_time_for_hour_scale_keeps_non_positive_window():
    query_end, adjusted = _build_query_end_time_for_hour_scale(
        "2026-03-11 00:00:00",
        "2026-03-11 00:00:00",
    )
    assert adjusted is False
    assert query_end == "2026-03-11 00:00:00"


class _FakePage:
    def __init__(self) -> None:
        self.closed = False
        self.close_calls = 0

    async def close(self) -> None:
        self.closed = True
        self.close_calls += 1


class _FakeContext:
    def __init__(self) -> None:
        self.new_page_calls = 0

    async def new_page(self):
        self.new_page_calls += 1
        return _FakePage()


def test_download_site_with_retry_reuses_injected_page(monkeypatch) -> None:
    injected_page = _FakePage()
    context = _FakeContext()
    seen_pages = []

    async def _fake_download_single_url(**kwargs):
        seen_pages.append(kwargs["page"])
        return True, "D:\\temp\\A楼.xlsx", ""

    monkeypatch.setattr(
        "app.modules.report_pipeline.service.download_site_worker.download_single_url",
        _fake_download_single_url,
    )

    result = asyncio.run(
        download_site_with_retry(
            context=context,
            download_cfg={
                "save_dir": "D:\\temp",
                "max_retries": 2,
                "retry_wait_sec": 0,
            },
            perf_cfg={
                "query_result_timeout_ms": 1000,
                "login_fill_timeout_ms": 1000,
                "start_end_visible_timeout_ms": 1000,
                "force_iframe_reopen_each_task": False,
                "page_refresh_retry_count": 0,
            },
            site={
                "building": "A楼",
                "username": "user",
                "password": "pass",
                "url": "http://example.invalid/page/main/main.html",
            },
            start_time="2026-03-29 00:00:00",
            end_time="2026-03-29 01:00:00",
            resolve_site_urls=lambda site: [str(site["url"])],
            is_retryable_download_timeout=lambda _text: False,
            emit_log=lambda _text: None,
            page=injected_page,
        )
    )

    assert result.success is True
    assert context.new_page_calls == 0
    assert injected_page.close_calls == 0
    assert seen_pages == [injected_page]
