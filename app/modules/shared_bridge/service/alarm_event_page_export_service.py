from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Sequence

from openpyxl import Workbook
from playwright.async_api import Locator, Page


ALARM_EVENT_EXPORT_COLUMNS: list[tuple[str, str]] = [
    ("level", "级别"),
    ("content", "内容"),
    ("position", "位置"),
    ("object", "对象"),
    ("event_time", "告警时间"),
    ("accept_time", "接警时间"),
    ("is_accept", "处理状态"),
    ("accept_by", "处理人"),
    ("accept_content", "处理内容"),
    ("recover_time", "恢复时间"),
    ("is_recover", "恢复状态"),
    ("event_snapshot", "告警快照"),
    ("event_type", "事件类型"),
    ("confirm_type", "确认类型"),
    ("event_suggest", "建议"),
    ("confirm_time", "确认时间"),
    ("confirm_by", "确认人"),
    ("confirm_description", "确认说明"),
    ("real_value", "实时值"),
    ("alarm_threshold", "阈值"),
]


def query_window_start(when: datetime | None = None) -> datetime:
    now = when or datetime.now()
    month = now.month - 2
    year = now.year
    while month <= 0:
        month += 12
        year -= 1
    return datetime(year, month, 1, 0, 0, 0)


def query_window_start_text(when: datetime | None = None) -> str:
    return query_window_start(when).strftime("%Y-%m-%d %H:%M:%S")


def scheduled_bucket_for_time(when: datetime | None = None) -> str:
    now = when or datetime.now()
    if now.hour >= 16:
        return now.strftime("%Y-%m-%d 16")
    if now.hour >= 8:
        return now.strftime("%Y-%m-%d 08")
    previous_day = now - timedelta(days=1)
    return previous_day.strftime("%Y-%m-%d 16")


async def _safe_click_locator(locator: Locator, *, timeout: int = 5000) -> bool:
    try:
        await locator.wait_for(state="visible", timeout=timeout)
        await locator.scroll_into_view_if_needed(timeout=timeout)
        await locator.click(timeout=timeout)
        return True
    except Exception:
        return False


async def _safe_click(page: Page, selector: str, *, timeout: int = 5000) -> bool:
    return await _safe_click_locator(page.locator(selector).first, timeout=timeout)


async def _wait_for_content_column_visible(page: Page, *, timeout_ms: int = 1500) -> bool:
    locator = page.locator("th.content span").filter(has_text="内容").first
    try:
        await locator.wait_for(state="visible", timeout=timeout_ms)
        return True
    except Exception:
        return False


async def _wait_for_start_time_input_ready(page: Page, *, timeout_ms: int = 2500) -> bool:
    locator = page.locator("#startTime").first
    try:
        await locator.wait_for(state="visible", timeout=timeout_ms)
        await locator.scroll_into_view_if_needed(timeout=timeout_ms)
    except Exception:
        return False
    try:
        return not await locator.is_disabled()
    except Exception:
        return False


async def _open_custom_columns_panel(page: Page) -> None:
    candidates = [
        page.get_by_text("定制列", exact=True).first,
        page.locator("button:has-text('定制列')").first,
        page.locator("a:has-text('定制列')").first,
        page.locator("span:has-text('定制列')").first,
    ]
    for locator in candidates:
        if await _safe_click_locator(locator, timeout=3000):
            await page.wait_for_timeout(200)
            return
    raise RuntimeError("定制列入口不可用")


async def _select_all_columns(page: Page) -> None:
    candidates = [
        page.locator("li.all[data-value='all']").first,
        page.locator("label.c-checkbox-all.c-checkbox__label.sub_check[for='checkAll']").first,
    ]
    for attempt in range(3):
        locator = candidates[attempt % len(candidates)]
        if not await _safe_click_locator(locator, timeout=5000):
            continue
        await page.wait_for_timeout(200)
        if await _wait_for_content_column_visible(page, timeout_ms=1200):
            return
    raise RuntimeError("告警定制列全选后内容列未生效")


async def _expand_time_filter(page: Page) -> None:
    for _ in range(3):
        if not await _safe_click(page, ".toggleClass", timeout=5000):
            continue
        await page.wait_for_timeout(200)
        if await _wait_for_start_time_input_ready(page, timeout_ms=2500):
            return
    raise RuntimeError("告警时间筛选展开按钮不可用")


async def _set_start_time(page: Page, start_text: str) -> None:
    start_dt = datetime.strptime(start_text, "%Y-%m-%d %H:%M:%S")
    start_input = page.locator("#startTime").first
    for _ in range(2):
        if await _safe_click_locator(start_input, timeout=4000):
            break
        await _expand_time_filter(page)
    else:
        raise RuntimeError("告警开始时间输入框不可用")
    await page.wait_for_timeout(150)
    ym_inputs = page.locator("input.yminput")
    if await ym_inputs.count() < 2:
        raise RuntimeError("告警时间面板年月输入框不可用")
    year_input = ym_inputs.nth(0)
    month_input = ym_inputs.nth(1)
    await year_input.wait_for(state="visible", timeout=4000)
    await year_input.scroll_into_view_if_needed(timeout=4000)
    await year_input.click(timeout=4000)
    await year_input.fill(str(start_dt.year), timeout=4000)
    await month_input.wait_for(state="visible", timeout=4000)
    await month_input.scroll_into_view_if_needed(timeout=4000)
    await month_input.click(timeout=4000)
    await month_input.fill(str(start_dt.month), timeout=4000)
    if not await _safe_click(page, "#dpOkInput", timeout=3000):
        raise RuntimeError("告警时间面板确定按钮不可用")
    await page.wait_for_timeout(120)


async def _table_signature(page: Page) -> dict[str, Any]:
    return await page.evaluate(
        """
        () => {
          const rows = Array.from(document.querySelectorAll('#warn-table tbody tr'));
          const first = rows[0];
          return {
            row_count: rows.length,
            first_key: first ? `${first.id || ''}|${first.getAttribute('data-value') || ''}` : '',
          };
        }
        """
    )


async def _current_page_number(page: Page) -> int:
    try:
        payload = await page.evaluate(
            r"""
            () => {
              const current = document.querySelector('li.pageNum.current a[data-val], li.pageNum.current a, li.pageNum.current');
              if (!current) return 1;
              const raw = current.getAttribute('data-val') || current.textContent || '1';
              const match = String(raw || '').match(/\d+/);
              return match ? parseInt(match[0], 10) : 1;
            }
            """
        )
        parsed = int(payload or 1)
        return parsed if parsed > 0 else 1
    except Exception:
        return 1


async def _wait_for_table_change(
    page: Page,
    *,
    previous_signature: dict[str, Any] | None = None,
    previous_page: int | None = None,
    timeout_ms: int = 15000,
) -> None:
    deadline = datetime.now().timestamp() + max(1.0, timeout_ms / 1000.0)
    while datetime.now().timestamp() < deadline:
        signature = await _table_signature(page)
        current_page = await _current_page_number(page)
        if previous_signature is None:
            if int(signature.get("row_count", 0) or 0) >= 0:
                return
        elif current_page != previous_page or signature != previous_signature:
            return
        await page.wait_for_timeout(300)


async def _select_page_size(page: Page, value: str) -> None:
    locator = page.locator("select").filter(has=page.locator("option[value='50']")).first
    if await locator.count() <= 0:
        locator = page.locator("select").first
    if await locator.count() <= 0:
        return
    previous_signature = await _table_signature(page)
    previous_page = await _current_page_number(page)
    try:
        await locator.select_option(str(value))
    except Exception:
        return
    await _wait_for_table_change(
        page,
        previous_signature=previous_signature,
        previous_page=previous_page,
        timeout_ms=8000,
    )


async def _extract_current_rows(page: Page) -> List[Dict[str, str]]:
    rows = await page.evaluate(
        r"""
        () => Array.from(document.querySelectorAll('#warn-table tbody tr')).map((tr) => {
          const row = {
            _row_id: tr.id || '',
            _data_value: tr.getAttribute('data-value') || '',
          };
          for (const td of Array.from(tr.querySelectorAll('td[name]'))) {
            const key = String(td.getAttribute('name') || '').trim();
            if (!key) continue;
            row[key] = String(td.innerText || td.textContent || '').replace(/\s+/g, ' ').trim();
          }
          return row;
        })
        """
    )
    output: List[Dict[str, str]] = []
    for item in rows if isinstance(rows, list) else []:
        if not isinstance(item, dict):
            continue
        output.append({str(key): str(value or "").strip() for key, value in item.items()})
    return output


async def _visible_page_numbers(page: Page) -> List[int]:
    values = await page.evaluate(
        """
        () => Array.from(document.querySelectorAll('li.pageNum a[data-val], li.pageNum[data-val] a'))
          .map((node) => parseInt(String(node.getAttribute('data-val') || '').trim(), 10))
          .filter((value) => Number.isFinite(value) && value > 0)
        """
    )
    return sorted({int(item) for item in (values if isinstance(values, list) else []) if int(item or 0) > 0})


async def _goto_next_page(page: Page, current_page: int, previous_signature: dict[str, Any]) -> bool:
    next_candidates = [number for number in await _visible_page_numbers(page) if number > current_page]
    if next_candidates:
        next_page = min(next_candidates)
        locator = page.locator(f"li.pageNum a[data-val='{next_page}']").first
        if await locator.count() > 0:
            await locator.click()
            await _wait_for_table_change(
                page,
                previous_signature=previous_signature,
                previous_page=current_page,
                timeout_ms=8000,
            )
            return True
    next_locator = page.locator("li.next:not(.disabled) a, a.next, .next a").first
    if await next_locator.count() > 0:
        await next_locator.click()
        await _wait_for_table_change(
            page,
            previous_signature=previous_signature,
            previous_page=current_page,
            timeout_ms=8000,
        )
        return True
    return False


async def _collect_all_pages(page: Page) -> List[Dict[str, str]]:
    all_rows: List[Dict[str, str]] = []
    seen_pages: set[int] = set()
    seen_rows: set[str] = set()
    while True:
        current_page = await _current_page_number(page)
        if current_page in seen_pages:
            break
        seen_pages.add(current_page)
        rows = await _extract_current_rows(page)
        for row in rows:
            row_key = "|".join(
                [
                    str(row.get("_row_id", "") or "").strip(),
                    str(row.get("_data_value", "") or "").strip(),
                    str(row.get("event_time", "") or "").strip(),
                    str(row.get("object", "") or "").strip(),
                    str(row.get("content", "") or "").strip(),
                ]
            )
            if row_key in seen_rows:
                continue
            seen_rows.add(row_key)
            all_rows.append(row)
        signature = await _table_signature(page)
        if not await _goto_next_page(page, current_page, signature):
            break
    return all_rows


async def collect_alarm_event_rows(page: Page, *, now: datetime | None = None) -> Dict[str, Any]:
    target_now = now or datetime.now()
    query_start = query_window_start_text(target_now)
    await page.reload(wait_until="domcontentloaded")
    await page.wait_for_timeout(500)
    await _open_custom_columns_panel(page)
    await _select_all_columns(page)
    await _expand_time_filter(page)
    await _set_start_time(page, query_start)
    before_search_signature = await _table_signature(page)
    before_search_page = await _current_page_number(page)
    if not await _safe_click(page, "#search", timeout=5000):
        raise RuntimeError("告警查询按钮不可用")
    await _wait_for_table_change(
        page,
        previous_signature=before_search_signature,
        previous_page=before_search_page,
        timeout_ms=15000,
    )
    await _select_page_size(page, "50")
    rows = await _collect_all_pages(page)
    return {
        "query_start": query_start,
        "query_end": target_now.strftime("%Y-%m-%d %H:%M:%S"),
        "row_count": len(rows),
        "rows": rows,
    }


async def exercise_alarm_event_page_actions(page: Page, *, now: datetime | None = None) -> Dict[str, Any]:
    target_now = now or datetime.now()
    query_start = query_window_start_text(target_now)
    await _open_custom_columns_panel(page)
    await _select_all_columns(page)
    await _expand_time_filter(page)
    await _set_start_time(page, query_start)
    previous_signature = await _table_signature(page)
    previous_page = await _current_page_number(page)
    if not await _safe_click(page, "#search", timeout=5000):
        raise RuntimeError("告警查询按钮不可用")
    await _wait_for_table_change(
        page,
        previous_signature=previous_signature,
        previous_page=previous_page,
        timeout_ms=15000,
    )
    await _select_page_size(page, "50")
    rows = await _extract_current_rows(page)
    return {
        "query_start": query_start,
        "query_end": target_now.strftime("%Y-%m-%d %H:%M:%S"),
        "row_count": len(rows),
        "current_page": await _current_page_number(page),
        "content_column_visible": await _wait_for_content_column_visible(page, timeout_ms=500),
        "start_time_ready": await _wait_for_start_time_input_ready(page, timeout_ms=500),
        "page_url": str(page.url or "").strip(),
    }


def write_alarm_event_workbook(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "告警信息"
    worksheet.append([header for _key, header in ALARM_EVENT_EXPORT_COLUMNS])
    for row in rows:
        payload = row if isinstance(row, dict) else {}
        worksheet.append([str(payload.get(key, "") or "").strip() for key, _header in ALARM_EVENT_EXPORT_COLUMNS])
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
