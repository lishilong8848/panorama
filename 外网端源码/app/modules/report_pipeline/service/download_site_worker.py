from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List

from app.shared.utils.playwright_page_reuse import prepare_reusable_page


@dataclass
class DownloadOutcome:
    building: str
    success: bool
    file_path: str = ""
    used_url: str = ""
    error: str = ""


def _safe_parse_dt(text: str) -> datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except Exception:  # noqa: BLE001
        return None


def _build_query_end_time_for_hour_scale(start_time: str, end_time: str) -> tuple[str, bool]:
    """
    查询刻度为“小时”时，平台部分场景按右开区间处理。
    结束时间落在整点则补 +1 秒，以包含结束整点记录。
    """
    start_dt = _safe_parse_dt(start_time)
    end_dt = _safe_parse_dt(end_time)
    if start_dt is None or end_dt is None:
        return str(end_time), False
    if end_dt <= start_dt:
        return str(end_time), False
    if end_dt.minute == 0 and end_dt.second == 0:
        return (end_dt + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"), True
    return str(end_time), False


async def download_single_url_once(
    page,
    save_dir: str,
    start_time: str,
    end_time: str,
    url: str,
    username: str,
    password: str,
    building: str,
    query_result_timeout_ms: int,
    login_fill_timeout_ms: int,
    start_end_visible_timeout_ms: int,
    force_iframe_reopen_each_task: bool,
) -> tuple[bool, str, str]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_path = f"{save_dir}\\{building}_{timestamp}.xlsx"
    try:
        async def _login_if_needed() -> None:
            login_visible = False
            try:
                await page.wait_for_selector("#username", state="visible", timeout=login_fill_timeout_ms)
                login_visible = True
            except Exception:  # noqa: BLE001
                login_visible = False

            if login_visible:
                await page.fill("#username", username)
                await page.fill("#password", password)
                await page.click("text=登录")

            await page.wait_for_selector("a.p-main__header__menu-item", state="visible", timeout=15000)

        async def _open_report_query_page() -> None:
            report_menu = page.locator('a.p-main__header__menu-item:has-text("报表报告")')
            await report_menu.first.wait_for(state="visible", timeout=10000)
            await report_menu.first.click()
            await asyncio.sleep(0.3)

            data_query = page.locator('span.c-leftMenu__level-1__item-title:has-text("数据查询")')
            await data_query.first.wait_for(state="visible", timeout=10000)
            await data_query.first.click()
            await asyncio.sleep(0.2)

            instant_report = page.locator('li.c-leftMenu__level-2__item:has-text("即时报表")')
            await instant_report.first.wait_for(state="visible", timeout=10000)
            await instant_report.first.click()

        async def _resolve_report_frames(frame_timeout_ms: int = 10000):
            first_frame_sel = await page.wait_for_selector("iframe#right-content", state="attached", timeout=frame_timeout_ms)
            first_iframe = await first_frame_sel.content_frame()
            if first_iframe is None:
                raise RuntimeError("未获取到第一级iframe")

            await first_iframe.wait_for_selector("div.showTemplate >> text=全景平台月报", state="visible", timeout=frame_timeout_ms)
            target_element = first_iframe.locator('div.showTemplate:has-text("全景平台月报")')
            await target_element.scroll_into_view_if_needed()
            await target_element.hover()
            await asyncio.sleep(0.2)
            await target_element.click(force=True)

            first_frame_sel = await page.wait_for_selector("iframe#right-content", state="attached", timeout=frame_timeout_ms)
            first_iframe = await first_frame_sel.content_frame()
            if first_iframe is None:
                raise RuntimeError("未获取到第一级iframe(模板点击后)")
            second_frame_sel = await first_iframe.wait_for_selector(
                "iframe#laminationFrame",
                state="attached",
                timeout=frame_timeout_ms,
            )
            second_iframe = await second_frame_sel.content_frame()
            if second_iframe is None:
                raise RuntimeError("未获取到第二级iframe")
            return second_iframe

        await prepare_reusable_page(
            page,
            target_url=url,
            refresh_timeout_ms=max(query_result_timeout_ms, 20000),
        )
        await _login_if_needed()

        frame_chain_attempts = 2 if force_iframe_reopen_each_task else 1
        second_iframe = None
        for frame_chain_round in range(frame_chain_attempts):
            try:
                await _open_report_query_page()
                second_iframe = await _resolve_report_frames(frame_timeout_ms=10000)
                break
            except Exception:
                if frame_chain_round >= frame_chain_attempts - 1:
                    raise
                await page.reload(wait_until="domcontentloaded", timeout=20000)
                await _login_if_needed()

        if second_iframe is None:
            raise RuntimeError("未获取到可用报表iframe")

        time_input = second_iframe.locator(
            'div.fr-trigger-editor[widgetname="开始时间"] >> input.fr-trigger-texteditor'
        )
        await time_input.wait_for(state="visible", timeout=start_end_visible_timeout_ms)
        await time_input.click()
        await time_input.fill(start_time)
        await time_input.press("Tab")

        time_input = second_iframe.locator(
            'div.fr-trigger-editor[widgetname="结束时间"] >> input.fr-trigger-texteditor'
        )
        await time_input.wait_for(state="visible", timeout=start_end_visible_timeout_ms)
        await time_input.click()
        query_end_time, end_adjusted = _build_query_end_time_for_hour_scale(start_time, end_time)
        if end_adjusted:
            print(
                f"[{building}] 查询结束时间边界补偿: original={end_time}, query_end={query_end_time}, scale=小时"
            )
        await time_input.fill(query_end_time)
        await time_input.press("Tab")

        scale_trigger = second_iframe.locator(
            'div.fr-trigger-editor[widgetname="查询刻度"] >> div.fr-trigger-btn-up'
        )
        await scale_trigger.scroll_into_view_if_needed()
        await scale_trigger.click()

        await second_iframe.wait_for_selector("div.fr-combo-list", state="visible", timeout=5000)
        option = second_iframe.locator(
            'xpath=//div[contains(@class,"fr-combo-list-item") and @title="小时" and text()="小时"]'
        )
        await option.click()

        query_btn = second_iframe.locator('button.fr-btn-text:has-text("查询"):visible')
        await query_btn.wait_for(state="visible", timeout=5000)
        await query_btn.click(delay=100)

        await second_iframe.wait_for_function(
            """() => {
                const container = document.getElementById('content-container');
                return container && container.textContent.includes('IT负载');
            }""",
            timeout=query_result_timeout_ms,
        )

        export_button = second_iframe.locator('button.fr-btn-text.x-emb-excel:has-text("原样导出")')
        async with page.expect_download() as download_info:
            await export_button.wait_for(state="visible", timeout=5000)
            await export_button.click()
        download = await download_info.value
        await download.save_as(save_path)
        return True, save_path, ""
    except Exception as exc:  # noqa: BLE001
        return False, save_path, str(exc)


async def download_single_url(
    page,
    save_dir: str,
    start_time: str,
    end_time: str,
    url: str,
    username: str,
    password: str,
    building: str,
    query_result_timeout_ms: int,
    login_fill_timeout_ms: int,
    start_end_visible_timeout_ms: int,
    force_iframe_reopen_each_task: bool,
    page_refresh_retry_count: int,
    is_retryable_download_timeout: Callable[[str], bool],
) -> tuple[bool, str, str]:
    refresh_retries = max(0, int(page_refresh_retry_count))
    last_error = "未知错误"
    last_file_path = f"{save_dir}\\{building}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    for refresh_round in range(refresh_retries + 1):
        success, file_path, error = await download_single_url_once(
            page=page,
            save_dir=save_dir,
            start_time=start_time,
            end_time=end_time,
            url=url,
            username=username,
            password=password,
            building=building,
            query_result_timeout_ms=query_result_timeout_ms,
            login_fill_timeout_ms=login_fill_timeout_ms,
            start_end_visible_timeout_ms=start_end_visible_timeout_ms,
            force_iframe_reopen_each_task=force_iframe_reopen_each_task,
        )
        if success:
            return True, file_path, ""

        last_error = error
        last_file_path = file_path
        if refresh_round >= refresh_retries:
            break
        if not is_retryable_download_timeout(error):
            break

        try:
            await page.reload(wait_until="domcontentloaded", timeout=20000)
        except Exception as exc:  # noqa: BLE001
            last_error = f"{last_error}; 页面刷新失败: {exc}"
            break

    return False, last_file_path, last_error


async def download_site_with_retry(
    context,
    download_cfg: Dict[str, Any],
    perf_cfg: Dict[str, Any],
    site: Dict[str, Any],
    start_time: str,
    end_time: str,
    *,
    resolve_site_urls: Callable[[Dict[str, Any]], List[str]],
    is_retryable_download_timeout: Callable[[str], bool],
    emit_log: Callable[[str], None] = print,
    page=None,
) -> DownloadOutcome:
    building = str(site["building"]).strip()
    username = str(site["username"]).strip()
    password = str(site["password"]).strip()
    save_dir = str(download_cfg["save_dir"]).strip()
    max_retries = int(download_cfg["max_retries"])
    retry_wait_sec = int(download_cfg["retry_wait_sec"])
    query_result_timeout_ms = int(perf_cfg["query_result_timeout_ms"])
    login_fill_timeout_ms = int(perf_cfg["login_fill_timeout_ms"])
    start_end_visible_timeout_ms = int(perf_cfg["start_end_visible_timeout_ms"])
    force_iframe_reopen_each_task = bool(perf_cfg["force_iframe_reopen_each_task"])
    page_refresh_retry_count = int(perf_cfg["page_refresh_retry_count"])
    urls = resolve_site_urls(site)

    if not urls:
        return DownloadOutcome(building=building, success=False, error="未配置可用URL")

    last_error = "未知错误"
    working_page = page
    owns_page = working_page is None
    if working_page is None:
        working_page = await context.new_page()
    try:
        for attempt in range(1, max_retries + 1):
            try:
                for url in urls:
                    emit_log(f"[{building}] 尝试下载 (attempt={attempt}, url={url})")
                    success, file_path, error = await download_single_url(
                        page=working_page,
                        save_dir=save_dir,
                        start_time=start_time,
                        end_time=end_time,
                        url=url,
                        username=username,
                        password=password,
                        building=building,
                        query_result_timeout_ms=query_result_timeout_ms,
                        login_fill_timeout_ms=login_fill_timeout_ms,
                        start_end_visible_timeout_ms=start_end_visible_timeout_ms,
                        force_iframe_reopen_each_task=force_iframe_reopen_each_task,
                        page_refresh_retry_count=page_refresh_retry_count,
                        is_retryable_download_timeout=is_retryable_download_timeout,
                    )
                    if success:
                        emit_log(f"[{building}] 下载成功: {file_path}")
                        return DownloadOutcome(
                            building=building,
                            success=True,
                            file_path=file_path,
                            used_url=url,
                        )
                    last_error = error
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)

            if attempt < max_retries:
                await asyncio.sleep(retry_wait_sec)
    finally:
        if owns_page:
            try:
                await working_page.close()
            except Exception:  # noqa: BLE001
                pass

    return DownloadOutcome(building=building, success=False, error=last_error)
