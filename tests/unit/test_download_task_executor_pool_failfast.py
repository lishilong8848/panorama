from __future__ import annotations

import asyncio
import concurrent.futures

from app.modules.report_pipeline.service.download_task_executor import (
    run_download_tasks_by_building,
)


class _FailingPool:
    def submit_building_job(self, building, _runner):
        future: concurrent.futures.Future = concurrent.futures.Future()
        future.set_exception(RuntimeError(f"{building} 登录失败: 页面无响应，请检查楼栋页面服务或网络"))
        return future


class _Task:
    def __init__(self, building: str, date_text: str) -> None:
        self.site = {"building": building, "host": f"{building.lower()}.example.invalid"}
        self.start_time = "2026-03-29 00:00:00"
        self.end_time = "2026-03-29 01:00:00"
        self.save_dir = r"D:\temp"
        self.date_text = date_text


def test_browser_pool_failure_only_marks_current_building_failed() -> None:
    failure_logs = []
    success_logs = []

    async def _fake_download_site_with_retry(**_kwargs):
        raise AssertionError("浏览器池异常时不应继续执行站点下载")

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
                _Task("B楼", "2026-03-29"),
            ],
            feature="月报下载",
            success_stage="内网下载",
            failure_stage="内网下载",
            success_detail_prefix="下载成功 URL=",
            group_download_tasks_by_building=lambda tasks: [
                ("A楼", tasks[:1]),
                ("B楼", tasks[1:]),
            ],
            download_site_with_retry=_fake_download_site_with_retry,
            log_file_success=lambda **kwargs: success_logs.append(kwargs),
            log_file_failure=lambda **kwargs: failure_logs.append(kwargs),
            browser_pool=_FailingPool(),
        )
    )

    assert success_logs == []
    assert len(pairs) == 2
    assert all(not outcome.success for _, outcome in pairs)
    assert failure_logs[0]["building"] == "A楼"
    assert "A楼 登录失败" in failure_logs[0]["error"]
    assert failure_logs[1]["building"] == "B楼"
    assert "B楼 登录失败" in failure_logs[1]["error"]
