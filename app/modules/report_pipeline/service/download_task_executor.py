from __future__ import annotations

import asyncio
import copy
from typing import Any, Awaitable, Callable, Dict, List, Tuple

from playwright.async_api import async_playwright
from app.shared.runtime.internal_download_browser_pool_runtime import (
    get_internal_download_browser_pool,
)
from app.modules.report_pipeline.service.download_site_worker import DownloadOutcome


def _resolve_used_url(site: Any) -> str:
    if not isinstance(site, dict):
        return ""
    raw = str(site.get("url", "") or site.get("host", "") or "").strip()
    if not raw:
        return ""
    if raw.startswith(("http://", "https://")):
        return raw
    return f"http://{raw}"


async def run_download_tasks_by_building(
    *,
    config: Dict[str, Any],
    download_tasks: List[Any],
    feature: str,
    success_stage: str,
    failure_stage: str,
    success_detail_prefix: str,
    group_download_tasks_by_building: Callable[[List[Any]], List[Tuple[str, List[Any]]]],
    download_site_with_retry: Callable[..., Awaitable[Any]],
    log_file_success: Callable[..., None],
    log_file_failure: Callable[..., None],
    browser_pool: Any | None = None,
) -> List[Tuple[Any, Any]]:
    if not download_tasks:
        return []

    download_cfg = config["download"]
    perf_cfg = download_cfg["performance"]
    browser_headless = bool(download_cfg["browser_headless"])
    browser_channel = str(download_cfg["browser_channel"]).strip()
    site_start_delay_sec = int(download_cfg["site_start_delay_sec"])
    grouped_tasks = group_download_tasks_by_building(download_tasks)
    if not grouped_tasks:
        return []
    browser_pool = browser_pool or get_internal_download_browser_pool()

    if browser_pool is not None:
        pairs: List[Tuple[Any, Any]] = []

        async def _worker_with_pool(building: str, task_items: List[Any], worker_index: int) -> None:
            if site_start_delay_sec > 0 and worker_index > 0:
                await asyncio.sleep(worker_index * site_start_delay_sec)

            async def _runner(page) -> List[Tuple[Any, Any]]:
                building_pairs: List[Tuple[Any, Any]] = []
                for task in task_items:
                    task_download_cfg = dict(download_cfg)
                    task_download_cfg["save_dir"] = getattr(task, "save_dir", "")
                    outcome = await download_site_with_retry(
                        context=None,
                        download_cfg=task_download_cfg,
                        perf_cfg=perf_cfg,
                        site=copy.deepcopy(getattr(task, "site", {})),
                        start_time=getattr(task, "start_time", ""),
                        end_time=getattr(task, "end_time", ""),
                        page=page,
                    )
                    building_pairs.append((task, outcome))
                return building_pairs

            try:
                building_pairs = await asyncio.wrap_future(
                    browser_pool.submit_building_job(building, _runner)
                )
            except Exception as exc:
                error_text = str(exc)
                for task in task_items:
                    outcome = DownloadOutcome(
                        building=building,
                        success=False,
                        file_path="",
                        used_url=_resolve_used_url(getattr(task, "site", {})),
                        error=error_text,
                    )
                    pairs.append((task, outcome))
                    log_file_failure(
                        feature=feature,
                        stage=failure_stage,
                        building=building,
                        file_path="-",
                        upload_date=str(getattr(task, "date_text", "") or "-"),
                        error=error_text,
                    )
                return
            for task, outcome in building_pairs:
                pairs.append((task, outcome))
                if bool(getattr(outcome, "success", False)):
                    log_file_success(
                        feature=feature,
                        stage=success_stage,
                        building=building,
                        file_path=getattr(outcome, "file_path", ""),
                        upload_date=str(getattr(task, "date_text", "") or "-"),
                        detail=f"{success_detail_prefix}{getattr(outcome, 'used_url', '') or '-'}",
                    )
                else:
                    log_file_failure(
                        feature=feature,
                        stage=failure_stage,
                        building=building,
                        file_path="-",
                        upload_date=str(getattr(task, "date_text", "") or "-"),
                        error=str(getattr(outcome, "error", "")),
                    )

        await asyncio.gather(
            *(
                asyncio.create_task(_worker_with_pool(building, task_items, idx))
                for idx, (building, task_items) in enumerate(grouped_tasks)
            )
        )
        return pairs

    async with async_playwright() as p:
        launch_kwargs: Dict[str, Any] = dict(
            headless=browser_headless,
            args=[
                "--no-sandbox",
                "--disable-gpu",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
            ],
        )
        if browser_channel:
            launch_kwargs["channel"] = browser_channel

        try:
            browser = await p.chromium.launch(**launch_kwargs)
        except Exception as exc:  # noqa: BLE001
            if browser_channel:
                print(f"[下载] 浏览器channel={browser_channel}启动失败，改为内置Chromium: {exc}")
                launch_kwargs.pop("channel", None)
                browser = await p.chromium.launch(**launch_kwargs)
            else:
                raise

        context = await browser.new_context(accept_downloads=True)
        pairs: List[Tuple[Any, Any]] = []

        async def _worker(building: str, task_items: List[Any], worker_index: int) -> None:
            if site_start_delay_sec > 0 and worker_index > 0:
                await asyncio.sleep(worker_index * site_start_delay_sec)

            page = await context.new_page()
            try:
                for task in task_items:
                    task_download_cfg = dict(download_cfg)
                    task_download_cfg["save_dir"] = getattr(task, "save_dir", "")
                    outcome = await download_site_with_retry(
                        context=context,
                        download_cfg=task_download_cfg,
                        perf_cfg=perf_cfg,
                        site=copy.deepcopy(getattr(task, "site", {})),
                        start_time=getattr(task, "start_time", ""),
                        end_time=getattr(task, "end_time", ""),
                        page=page,
                    )
                    pairs.append((task, outcome))
                    if bool(getattr(outcome, "success", False)):
                        log_file_success(
                            feature=feature,
                            stage=success_stage,
                            building=building,
                            file_path=getattr(outcome, "file_path", ""),
                            upload_date=str(getattr(task, "date_text", "") or "-"),
                            detail=f"{success_detail_prefix}{getattr(outcome, 'used_url', '') or '-'}",
                        )
                    else:
                        log_file_failure(
                            feature=feature,
                            stage=failure_stage,
                            building=building,
                            file_path="-",
                            upload_date=str(getattr(task, "date_text", "") or "-"),
                            error=str(getattr(outcome, "error", "")),
                        )
            finally:
                try:
                    await page.close()
                except Exception:  # noqa: BLE001
                    pass

        await asyncio.gather(
            *(
                asyncio.create_task(_worker(building, task_items, idx))
                for idx, (building, task_items) in enumerate(grouped_tasks)
            )
        )
        await browser.close()
        return pairs
