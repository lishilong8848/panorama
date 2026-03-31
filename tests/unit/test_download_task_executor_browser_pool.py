from __future__ import annotations

import asyncio
import concurrent.futures
from types import SimpleNamespace

from app.modules.report_pipeline.service.download_task_executor import (
    run_download_tasks_by_building,
)


class _FakePool:
    def __init__(self) -> None:
        self.pages = {
            "A楼": object(),
            "B楼": object(),
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


class _Task:
    def __init__(self, building: str, date_text: str) -> None:
        self.site = {"building": building}
        self.start_time = "2026-03-29 00:00:00"
        self.end_time = "2026-03-29 01:00:00"
        self.save_dir = "D:\\temp"
        self.date_text = date_text


def test_download_task_executor_uses_browser_pool_pages(monkeypatch) -> None:
    fake_pool = _FakePool()
    seen_pages: dict[str, list[object]] = {"A楼": [], "B楼": []}

    async def _fake_download_site_with_retry(**kwargs):
        building = str(kwargs["site"]["building"])
        seen_pages[building].append(kwargs["page"])
        return SimpleNamespace(
            building=building,
            success=True,
            file_path=f"D:\\temp\\{building}.xlsx",
            used_url=f"http://{building}.example.invalid",
            error="",
        )

    monkeypatch.setattr(
        "app.modules.report_pipeline.service.download_task_executor.async_playwright",
        lambda: (_ for _ in ()).throw(AssertionError("browser pool path should not launch local browser")),
    )

    success_logs = []
    failure_logs = []

    pairs = asyncio.run(
        run_download_tasks_by_building(
            config={
                "download": {
                    "performance": {
                        "query_result_timeout_ms": 1000,
                        "login_fill_timeout_ms": 1000,
                        "start_end_visible_timeout_ms": 1000,
                        "force_iframe_reopen_each_task": False,
                        "page_refresh_retry_count": 0,
                    },
                    "browser_headless": True,
                    "browser_channel": "",
                    "site_start_delay_sec": 0,
                }
            },
            download_tasks=[
                _Task("A楼", "2026-03-29"),
                _Task("A楼", "2026-03-30"),
                _Task("B楼", "2026-03-29"),
            ],
            feature="月报下载",
            success_stage="内网下载",
            failure_stage="内网下载",
            success_detail_prefix="下载成功 URL=",
            group_download_tasks_by_building=lambda tasks: [
                ("A楼", tasks[:2]),
                ("B楼", tasks[2:]),
            ],
            download_site_with_retry=_fake_download_site_with_retry,
            log_file_success=lambda **kwargs: success_logs.append(kwargs),
            log_file_failure=lambda **kwargs: failure_logs.append(kwargs),
            browser_pool=fake_pool,
        )
    )

    assert len(pairs) == 3
    assert failure_logs == []
    assert fake_pool.calls == ["A楼", "B楼"]
    assert len(success_logs) == 3
    assert seen_pages["A楼"][0] is seen_pages["A楼"][1]
    assert seen_pages["A楼"][0] is fake_pool.pages["A楼"]
    assert seen_pages["B楼"][0] is fake_pool.pages["B楼"]

