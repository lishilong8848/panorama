from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from playwright.async_api import Page, async_playwright

from app.modules.shared_bridge.service.alarm_event_page_export_service import (
    _current_page_number,
    _extract_current_rows,
    _expand_time_filter,
    _open_custom_columns_panel,
    _select_all_columns,
    _select_page_size,
    _set_start_time,
    _table_signature,
    _wait_for_table_change,
    query_window_start_text,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="调试当前已打开的告警页面，不刷新、不关闭、不新开页。")
    parser.add_argument("--debug-url", default="http://127.0.0.1:29333", help="CDP 调试地址")
    parser.add_argument("--url-contains", default="/page/warn_event/warn_event.html", help="现有页面 URL 关键字")
    parser.add_argument(
        "--screenshot",
        default="output/playwright/live_alarm_page_after_actions.png",
        help="操作完成后的截图路径",
    )
    return parser.parse_args()


async def _find_target_page(browser: Any, url_contains: str) -> Page | None:
    candidates: list[Page] = []
    for context in list(getattr(browser, "contexts", []) or []):
        for page in list(getattr(context, "pages", []) or []):
            try:
                if page.is_closed():
                    continue
            except Exception:
                continue
            candidates.append(page)
    for page in reversed(candidates):
        if url_contains in str(page.url or ""):
            return page
    return None


async def _print_open_pages(browser: Any) -> None:
    index = 0
    for context in list(getattr(browser, "contexts", []) or []):
        for page in list(getattr(context, "pages", []) or []):
            try:
                if page.is_closed():
                    continue
            except Exception:
                continue
            print(f"[OPEN_PAGE] #{index} {page.url}")
            index += 1


async def _run() -> int:
    args = _parse_args()
    screenshot_path = Path(args.screenshot)
    screenshot_path.parent.mkdir(parents=True, exist_ok=True)

    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.connect_over_cdp(args.debug_url, timeout=10000)
        page = await _find_target_page(browser, args.url_contains)
        if page is None:
            print("[ERROR] 未找到当前已打开的告警页面，不会新开页。现有页面如下：")
            await _print_open_pages(browser)
            return 2

        print(f"[INFO] 命中现有页面: {page.url}")
        await page.bring_to_front()
        await page.wait_for_timeout(300)

        print("[STEP] 点击定制列")
        await _open_custom_columns_panel(page)

        print("[STEP] 点击全部并确认“内容”列出现")
        await _select_all_columns(page)

        print("[STEP] 展开时间筛选")
        await _expand_time_filter(page)

        query_start = query_window_start_text(datetime.now())
        print(f"[STEP] 设置开始时间: {query_start}")
        await _set_start_time(page, query_start)

        print("[STEP] 点击查询")
        previous_signature = await _table_signature(page)
        previous_page = await _current_page_number(page)
        search_button = page.locator("#search").first
        await search_button.scroll_into_view_if_needed(timeout=5000)
        await search_button.click(timeout=5000)
        await _wait_for_table_change(
            page,
            previous_signature=previous_signature,
            previous_page=previous_page,
            timeout_ms=15000,
        )

        print("[STEP] 切换分页大小到 50")
        await _select_page_size(page, "50")

        rows = await _extract_current_rows(page)
        current_page = await _current_page_number(page)
        await page.screenshot(path=str(screenshot_path))
        print(f"[OK] 操作完成: 当前页码={current_page}, 当前页行数={len(rows)}, 截图={screenshot_path}")
        return 0
    finally:
        await asyncio.sleep(0.05)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
