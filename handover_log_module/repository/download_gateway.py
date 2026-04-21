from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from playwright.async_api import BrowserContext, Page, async_playwright

from pipeline_utils import configure_playwright_environment

from app.shared.runtime.internal_download_browser_pool_runtime import (
    get_internal_download_browser_pool,
)
from app.shared.utils.playwright_page_reuse import prepare_reusable_page


_RUNTIME_CONFIG: Optional[Dict[str, Any]] = None


def set_runtime_config(config: Dict[str, Any]) -> None:
    global _RUNTIME_CONFIG
    _RUNTIME_CONFIG = config


def _extract_site_host(raw_value: Any) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        return str(parsed.hostname or "").strip()
    parsed = urlparse(f"http://{raw}")
    return str(parsed.hostname or "").strip()


def _resolve_site_url(site: Dict[str, Any]) -> str:
    host = _extract_site_host(site.get("host", "")) or _extract_site_host(
        site.get("url", "")
    )
    if not host:
        return ""
    return f"http://{host}/page/main/main.html"


async def _await_ready_browser_pool(*, browser_pool: Any | None) -> Any | None:
    config = _RUNTIME_CONFIG if isinstance(_RUNTIME_CONFIG, dict) else {}
    download_cfg = config.get("download", {}) if isinstance(config.get("download", {}), dict) else {}
    deployment_cfg = config.get("deployment", {}) if isinstance(config.get("deployment", {}), dict) else {}
    role_mode = str(deployment_cfg.get("role_mode", "") or "").strip().lower()
    wait_timeout_sec = 0.0
    try:
        configured_timeout = float(download_cfg.get("browser_pool_wait_timeout_sec", 0) or 0)
    except Exception:  # noqa: BLE001
        configured_timeout = 0.0
    if configured_timeout > 0:
        wait_timeout_sec = configured_timeout
    elif role_mode == "internal":
        wait_timeout_sec = 30.0
    if wait_timeout_sec <= 0:
        return browser_pool or get_internal_download_browser_pool()

    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_timeout_sec
    candidate = browser_pool
    while loop.time() < deadline:
        candidate = candidate or get_internal_download_browser_pool()
        if candidate is None:
            await asyncio.sleep(0.25)
            continue
        wait_until_ready = getattr(candidate, "wait_until_ready", None)
        if callable(wait_until_ready):
            remaining = max(0.1, deadline - loop.time())
            try:
                ready_result = await asyncio.to_thread(wait_until_ready, timeout_sec=remaining)
            except TypeError:
                ready_result = await asyncio.to_thread(wait_until_ready, remaining)
            except Exception:  # noqa: BLE001
                ready_result = {}
            if bool(ready_result.get("ready", False)):
                return candidate
        is_running = getattr(candidate, "is_running", None)
        if callable(is_running):
            try:
                if bool(is_running()):
                    return candidate
            except Exception:  # noqa: BLE001
                pass
        await asyncio.sleep(0.25)
    return candidate


def _normalize_text(value: Any, default: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else default


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return int(default)


def _is_timeout_error(text: str) -> bool:
    return "timeout" in str(text).lower()


@dataclass
class StepError(Exception):
    step: str
    detail: str

    def __str__(self) -> str:
        return self.detail


def _template_name_candidates(template_name: str) -> List[str]:
    base = str(template_name or "").strip()
    candidates: List[str] = []
    if base:
        candidates.append(base)

    aliases = [
        "交接班日志（李世龙）",
        "交接班日志(李世龙）",
        "交接班日志（李世龙)",
        "交接班日志(李世龙)",
    ]
    for alias in aliases:
        if alias not in candidates:
            candidates.append(alias)
    return candidates


class DownloadGateway:
    def __init__(self, full_config: Dict[str, Any]) -> None:
        self.full_config = full_config
        self.download_cfg = (
            full_config.get("download", {}) if isinstance(full_config, dict) else {}
        )
        self.handover_cfg = (
            full_config.get("handover_log", {}) if isinstance(full_config, dict) else {}
        )

    def _find_site(self, building: str) -> Dict[str, Any]:
        sites = self.handover_cfg.get("sites")
        if not isinstance(sites, list) or not sites:
            sites = self.download_cfg.get("sites", [])
        if not isinstance(sites, list):
            sites = []
        building_text = str(building).strip()
        for site in sites:
            if not isinstance(site, dict):
                continue
            if str(site.get("building", "")).strip() != building_text:
                continue
            if not bool(site.get("enabled", False)):
                continue
            return site
        raise ValueError(f"未找到已启用站点配置: {building_text}")

    @staticmethod
    def _debug_log(enabled: bool, building: str, message: str) -> None:
        if not enabled:
            return
        print(f"[交接班调试][{_normalize_text(building)}] {_normalize_text(message)}")

    async def _login_if_needed(
        self, page: Page, username: str, password: str, login_fill_timeout_ms: int
    ) -> None:
        login_visible = False
        try:
            await page.wait_for_selector(
                "#username", state="visible", timeout=login_fill_timeout_ms
            )
            login_visible = True
        except Exception:  # noqa: BLE001
            login_visible = False

        if login_visible:
            try:
                await page.fill("#username", username)
                await page.fill("#password", password)
                await page.click("text=登录")
            except Exception as exc:  # noqa: BLE001
                raise StepError("登录", str(exc)) from exc

        try:
            await page.wait_for_selector(
                "a.p-main__header__menu-item", state="visible", timeout=20000
            )
        except Exception as exc:  # noqa: BLE001
            raise StepError("登录", f"登录后未进入主页: {exc}") from exc

    async def _open_report_query_page(
        self,
        page: Page,
        menu_path: List[str],
        *,
        menu_visible_timeout_ms: int,
    ) -> None:
        items = [str(x).strip() for x in menu_path if str(x).strip()]
        if len(items) < 3:
            items = ["报表报告", "数据查询", "即时报表"]
        top_menu, level1_menu, level2_menu = items[0], items[1], items[2]
        try:
            await page.locator(
                f'a.p-main__header__menu-item:has-text("{top_menu}")'
            ).first.wait_for(
                state="visible",
                timeout=menu_visible_timeout_ms,
            )
            await page.locator(
                f'a.p-main__header__menu-item:has-text("{top_menu}")'
            ).first.click()
            await asyncio.sleep(0.3)
            await page.locator(
                f'span.c-leftMenu__level-1__item-title:has-text("{level1_menu}")'
            ).first.wait_for(
                state="visible",
                timeout=menu_visible_timeout_ms,
            )
            await page.locator(
                f'span.c-leftMenu__level-1__item-title:has-text("{level1_menu}")'
            ).first.click()
            await asyncio.sleep(0.2)
            await page.locator(
                f'li.c-leftMenu__level-2__item:has-text("{level2_menu}")'
            ).first.wait_for(
                state="visible",
                timeout=menu_visible_timeout_ms,
            )
            await page.locator(
                f'li.c-leftMenu__level-2__item:has-text("{level2_menu}")'
            ).first.click()
        except Exception as exc:  # noqa: BLE001
            raise StepError("菜单", str(exc)) from exc

    async def _resolve_report_frames(
        self,
        page: Page,
        *,
        template_name: str,
        force_iframe_reopen_each_task: bool,
        iframe_timeout_ms: int,
    ):
        frame_timeout = max(1000, int(iframe_timeout_ms))
        _ = force_iframe_reopen_each_task
        names = _template_name_candidates(template_name)
        try:
            level1 = await page.wait_for_selector(
                "iframe#right-content", state="attached", timeout=frame_timeout
            )
            frame1 = await level1.content_frame()
            if frame1 is None:
                raise StepError("iframe", "未获取到right-content iframe")

            report_locator = None
            template_wait_timeout = frame_timeout
            for name in names:
                candidate = frame1.locator(
                    f'div.showTemplate:has-text("{name}")'
                ).first
                try:
                    await candidate.wait_for(
                        state="visible", timeout=template_wait_timeout
                    )
                    report_locator = candidate
                    break
                except Exception:  # noqa: BLE001
                    continue
            if report_locator is None:
                raise StepError("iframe", f"未找到报表模板: {template_name}")

            await report_locator.scroll_into_view_if_needed()
            await report_locator.hover()
            await asyncio.sleep(0.2)
            await report_locator.click(force=True)

            level1 = await page.wait_for_selector(
                "iframe#right-content", state="attached", timeout=frame_timeout
            )
            frame1 = await level1.content_frame()
            if frame1 is None:
                raise StepError("iframe", "模板点击后未获取到 right-content iframe")

            level2 = await frame1.wait_for_selector(
                "iframe#laminationFrame", state="attached", timeout=frame_timeout
            )
            frame2 = await level2.content_frame()
            if frame2 is None:
                raise StepError("iframe", "未获取到laminationFrame iframe")
            return frame2
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, StepError):
                raise exc
            raise StepError("iframe", str(exc)) from exc

    async def _fill_query_conditions(
        self,
        frame2,
        *,
        start_time: str,
        end_time: str,
        scale_label: str,
        start_end_visible_timeout_ms: int,
    ) -> None:
        try:
            start_input = frame2.locator(
                'div.fr-trigger-editor[widgetname="开始时间"] >> input.fr-trigger-texteditor'
            )
            await start_input.wait_for(
                state="visible", timeout=start_end_visible_timeout_ms
            )
            await start_input.click()
            await start_input.fill(start_time)
            await start_input.press("Tab")

            end_input = frame2.locator(
                'div.fr-trigger-editor[widgetname="结束时间"] >> input.fr-trigger-texteditor'
            )
            await end_input.wait_for(
                state="visible", timeout=start_end_visible_timeout_ms
            )
            await end_input.click()
            await end_input.fill(end_time)
            await end_input.press("Tab")

            scale_trigger = frame2.locator(
                'div.fr-trigger-editor[widgetname="查询刻度"] >> div.fr-trigger-btn-up'
            )
            await scale_trigger.scroll_into_view_if_needed()
            await scale_trigger.click()
            await frame2.wait_for_selector(
                "div.fr-combo-list", state="visible", timeout=5000
            )

            option = frame2.locator(
                f'xpath=//div[contains(@class,"fr-combo-list-item") and @title="{scale_label}" and text()="{scale_label}"]'
            )
            if await option.count() == 0:
                option = frame2.locator(
                    f'xpath=//div[contains(@class,"fr-combo-list-item") and contains(@title,"{scale_label}")]'
                )
            await option.first.click()

            query_btn = frame2.locator('button.fr-btn-text:has-text("查询"):visible')
            await query_btn.wait_for(state="visible", timeout=8000)
            await query_btn.click(delay=100)
        except Exception as exc:  # noqa: BLE001
            raise StepError("查询", str(exc)) from exc

    async def _wait_query_ready(self, frame2, timeout_ms: int) -> None:
        min_rows = max(1, _as_int(self.handover_cfg.get("query_ready_min_rows", 8), 8))
        min_cells = max(1, _as_int(self.handover_cfg.get("query_ready_min_cells", 40), 40))
        try:
            await frame2.wait_for_function(
                """(cfg) => {
                    const loading = document.querySelector('.x-mask-msg, .x-mask-loading, .fr-loading');
                    if (loading && loading.offsetParent !== null) {
                        return false;
                    }
                    const container = document.getElementById('content-container');
                    if (!container) return false;

                    const minRows = Number(cfg?.minRows || 8);
                    const minCells = Number(cfg?.minCells || 40);

                    const tables = Array.from(container.querySelectorAll('table'));
                    for (const table of tables) {
                        if (table.offsetParent === null) continue;
                        const rowCount = table.querySelectorAll('tr').length;
                        const cellCount = table.querySelectorAll('td').length;
                        if (rowCount >= minRows || cellCount >= minCells) {
                            return true;
                        }
                    }

                    const visibleCells = Array.from(container.querySelectorAll('td')).filter((el) => el.offsetParent !== null);
                    return visibleCells.length >= minCells;
                }""",
                arg={"minRows": min_rows, "minCells": min_cells},
                timeout=timeout_ms,
            )
        except Exception as exc:  # noqa: BLE001
            raise StepError("查询", f"查询结果等待超时: {exc}") from exc

    async def _export_with_download(
        self,
        page: Page,
        frame2,
        *,
        building: str,
        debug_step_log: bool,
        export_button_text: str,
        save_path: str,
        download_event_timeout_ms: int,
    ) -> None:
        export_btn = frame2.locator(
            f'button.fr-btn-text.x-emb-excel:has-text("{export_button_text}")'
        )
        await export_btn.wait_for(state="visible", timeout=download_event_timeout_ms)

        grace_timeout_ms = max(15000, min(120000, int(download_event_timeout_ms * 0.5)))
        click_strategies = [
            ("normal", int(download_event_timeout_ms)),
            ("force", int(grace_timeout_ms)),
            ("js", int(grace_timeout_ms)),
        ]
        errors: List[str] = []

        for strategy, timeout_ms in click_strategies:
            try:
                async with page.expect_download(timeout=timeout_ms) as download_info:
                    await export_btn.scroll_into_view_if_needed()
                    if strategy == "normal":
                        await export_btn.click(delay=120)
                    elif strategy == "force":
                        await export_btn.click(force=True, delay=120)
                    else:
                        await export_btn.evaluate("(el) => el.click()")
                download = await download_info.value
                await download.save_as(save_path)
                self._debug_log(
                    debug_step_log,
                    building,
                    f"导出下载成功 strategy={strategy}",
                )
                return
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
                errors.append(f"{strategy}:{err}")
                self._debug_log(
                    debug_step_log,
                    building,
                    f"导出下载未捕获 strategy={strategy} timeout={timeout_ms}ms error={err}",
                )
                continue

        raise StepError("导出", "; ".join(errors) if errors else "导出下载失败")

    async def _download_file_once(
        self,
        page: Page,
        *,
        building: str,
        site: Dict[str, Any],
        start_time: str,
        end_time: str,
        scale_label: str,
        template_name: str,
        save_dir: str,
        query_result_timeout_ms: int,
        download_event_timeout_ms: int,
        login_fill_timeout_ms: int,
        menu_visible_timeout_ms: int,
        iframe_timeout_ms: int,
        start_end_visible_timeout_ms: int,
        force_iframe_reopen_each_task: bool,
        export_button_text: str,
        menu_path: List[str],
        debug_step_log: bool,
        attempt_index: int,
        total_attempts: int,
    ) -> Dict[str, Any]:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_path = str(Path(save_dir) / f"{building}_{timestamp}.xlsx")
        url = _resolve_site_url(site)

        if not url:
            return {
                "success": False,
                "building": building,
                "file_path": "",
                "used_url": "",
                "error": "站点url为空",
                "failed_step": "初始化",
            }

        try:
            async def _run_step(step_name: str, coro_factory):
                started = time.perf_counter()
                try:
                    result = await coro_factory()
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    self._debug_log(
                        debug_step_log,
                        building,
                        f"refresh={attempt_index}/{total_attempts} step={step_name} 成功 elapsed={elapsed_ms:.0f}ms",
                    )
                    return result
                except Exception as exc:  # noqa: BLE001
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    self._debug_log(
                        debug_step_log,
                        building,
                        f"refresh={attempt_index}/{total_attempts} step={step_name} 失败 elapsed={elapsed_ms:.0f}ms error={exc}",
                    )
                    if isinstance(exc, StepError):
                        raise
                    raise StepError(step_name, str(exc)) from exc

            await _run_step(
                "刷新并进入目标页",
                lambda: prepare_reusable_page(
                    page,
                    target_url=url,
                    refresh_timeout_ms=max(int(query_result_timeout_ms), 20000),
                ),
            )
            await _run_step(
                "登录态识别",
                lambda: self._login_if_needed(
                    page,
                    username=str(site.get("username", "")).strip(),
                    password=str(site.get("password", "")).strip(),
                    login_fill_timeout_ms=int(login_fill_timeout_ms),
                ),
            )
            await _run_step(
                "菜单进入",
                lambda: self._open_report_query_page(
                    page,
                    menu_path,
                    menu_visible_timeout_ms=int(menu_visible_timeout_ms),
                ),
            )
            frame2 = await _run_step(
                "iframe定位",
                lambda: self._resolve_report_frames(
                    page,
                    template_name=template_name,
                    force_iframe_reopen_each_task=force_iframe_reopen_each_task,
                    iframe_timeout_ms=int(iframe_timeout_ms),
                ),
            )
            await _run_step(
                "填参查询",
                lambda: self._fill_query_conditions(
                    frame2,
                    start_time=start_time,
                    end_time=end_time,
                    scale_label=scale_label,
                    start_end_visible_timeout_ms=start_end_visible_timeout_ms,
                ),
            )
            await _run_step(
                "查询等待",
                lambda: self._wait_query_ready(
                    frame2, timeout_ms=int(query_result_timeout_ms)
                ),
            )
            await _run_step(
                "导出下载",
                lambda: self._export_with_download(
                    page,
                    frame2,
                    building=building,
                    debug_step_log=debug_step_log,
                    export_button_text=export_button_text,
                    save_path=save_path,
                    download_event_timeout_ms=int(download_event_timeout_ms),
                ),
            )
            return {
                "success": True,
                "building": building,
                "file_path": save_path,
                "used_url": url,
                "error": "",
                "failed_step": "",
            }
        except Exception as exc:  # noqa: BLE001
            step = exc.step if isinstance(exc, StepError) else "未分类"
            detail = str(exc)
            context_detail = (
                f"building={_normalize_text(building)}; "
                f"url={_normalize_text(url)}; "
                f"template={_normalize_text(template_name)}; "
                f"start={_normalize_text(start_time)}; "
                f"end={_normalize_text(end_time)}; "
                f"scale={_normalize_text(scale_label)}; "
                f"step={_normalize_text(step)}"
            )
            return {
                "success": False,
                "building": building,
                "file_path": "",
                "used_url": url,
                "error": f"{detail}; {context_detail}",
                "failed_step": step,
            }

    async def _download_single_building_with_retry(
        self,
        page: Page,
        *,
        building: str,
        site: Dict[str, Any],
        start_time: str,
        end_time: str,
        scale_label: str,
        template_name: str,
        save_dir: str,
        query_result_timeout_ms: int,
        download_event_timeout_ms: int,
        login_fill_timeout_ms: int,
        menu_visible_timeout_ms: int,
        iframe_timeout_ms: int,
        start_end_visible_timeout_ms: int,
        page_refresh_retry_count: int,
        max_retries: int,
        retry_wait_sec: int,
        force_iframe_reopen_each_task: bool,
        export_button_text: str,
        menu_path: List[str],
        debug_step_log: bool,
    ) -> Dict[str, Any]:
        configured_attempts = max(1, int(max_retries))
        fresh_page_attempts = max(1, int(page_refresh_retry_count) + 1)
        retries = max(configured_attempts, fresh_page_attempts)
        wait_sec = max(0, int(retry_wait_sec))
        last_result: Dict[str, Any] = {
            "success": False,
            "building": building,
            "file_path": "",
            "used_url": _resolve_site_url(site),
            "error": "未知错误",
            "failed_step": "初始化",
        }
        for attempt in range(1, retries + 1):
            self._debug_log(
                debug_step_log,
                building,
                f"楼栋下载尝试 attempt={attempt}/{retries}",
            )
            result = await self._download_file_once(
                page=page,
                building=building,
                site=site,
                start_time=start_time,
                end_time=end_time,
                scale_label=scale_label,
                template_name=template_name,
                save_dir=save_dir,
                query_result_timeout_ms=query_result_timeout_ms,
                download_event_timeout_ms=download_event_timeout_ms,
                login_fill_timeout_ms=login_fill_timeout_ms,
                menu_visible_timeout_ms=menu_visible_timeout_ms,
                iframe_timeout_ms=iframe_timeout_ms,
                start_end_visible_timeout_ms=start_end_visible_timeout_ms,
                force_iframe_reopen_each_task=force_iframe_reopen_each_task,
                export_button_text=export_button_text,
                menu_path=menu_path,
                debug_step_log=debug_step_log,
                attempt_index=attempt,
                total_attempts=retries,
            )
            if result.get("success"):
                return result
            result["error"] = f"attempt={attempt}/{retries}; {result.get('error', '')}"
            last_result = result
            if attempt < retries:
                self._debug_log(
                    debug_step_log,
                    building,
                    f"触发页面刷新重试 attempt={attempt}/{retries}, step={result.get('failed_step', '-')}, error={result.get('error', '-')}",
                )
                await asyncio.sleep(wait_sec)
        return last_result

    async def _download_handover_xlsx_batch_async(
        self,
        *,
        buildings: List[str],
        start_time: str,
        end_time: str,
        scale_label: str,
        template_name: str,
        save_dir: str,
        query_result_timeout_ms: int,
        download_event_timeout_ms: int,
        login_fill_timeout_ms: int,
        menu_visible_timeout_ms: int,
        iframe_timeout_ms: int,
        start_end_visible_timeout_ms: int,
        page_refresh_retry_count: int,
        max_retries: int,
        retry_wait_sec: int,
        force_iframe_reopen_each_task: bool,
        export_button_text: str,
        menu_path: List[str],
        parallel_by_building: bool,
        site_start_delay_sec: int,
        debug_step_log: bool,
        browser_pool: Any | None = None,
    ) -> List[Dict[str, Any]]:
        targets = [str(x).strip() for x in buildings if str(x).strip()]
        if not targets:
            return []

        Path(save_dir).mkdir(parents=True, exist_ok=True)
        configure_playwright_environment(self.full_config)

        browser_channel = str(self.download_cfg.get("browser_channel", "")).strip()
        browser_headless = bool(self.download_cfg.get("browser_headless", True))
        launch_kwargs: Dict[str, Any] = {
            "headless": browser_headless,
            "args": [
                "--no-sandbox",
                "--disable-gpu",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
            ],
        }
        if browser_channel:
            launch_kwargs["channel"] = browser_channel

        site_entries: List[tuple[int, str, Dict[str, Any]]] = []
        result_by_index: Dict[int, Dict[str, Any]] = {}
        for idx, building in enumerate(targets):
            try:
                site = self._find_site(building)
            except Exception as exc:  # noqa: BLE001
                result_by_index[idx] = {
                    "success": False,
                    "building": building,
                    "file_path": "",
                    "used_url": "",
                    "error": str(exc),
                    "failed_step": "初始化",
                }
                continue
            site_entries.append((idx, building, site))

        if not site_entries:
            return [result_by_index[idx] for idx in sorted(result_by_index.keys())]

        browser_pool = await _await_ready_browser_pool(browser_pool=browser_pool)

        if browser_pool is not None:
            async def _run_entry_with_pool(
                idx: int, building: str, site: Dict[str, Any], worker_index: int
            ) -> tuple[int, Dict[str, Any]]:
                if parallel_by_building and site_start_delay_sec > 0 and worker_index > 0:
                    await asyncio.sleep(worker_index * site_start_delay_sec)

                async def _runner(page: Page) -> Dict[str, Any]:
                    return await self._download_single_building_with_retry(
                        page=page,
                        building=building,
                        site=site,
                        start_time=start_time,
                        end_time=end_time,
                        scale_label=scale_label,
                        template_name=template_name,
                        save_dir=save_dir,
                        query_result_timeout_ms=query_result_timeout_ms,
                        download_event_timeout_ms=download_event_timeout_ms,
                        login_fill_timeout_ms=login_fill_timeout_ms,
                        menu_visible_timeout_ms=menu_visible_timeout_ms,
                        iframe_timeout_ms=iframe_timeout_ms,
                        start_end_visible_timeout_ms=start_end_visible_timeout_ms,
                        page_refresh_retry_count=page_refresh_retry_count,
                        max_retries=max_retries,
                        retry_wait_sec=retry_wait_sec,
                        force_iframe_reopen_each_task=force_iframe_reopen_each_task,
                        export_button_text=export_button_text,
                        menu_path=menu_path,
                        debug_step_log=debug_step_log,
                    )

                result = await asyncio.wrap_future(
                    browser_pool.submit_building_job(building, _runner)
                )
                return idx, result

            if parallel_by_building:
                tasks = [
                    asyncio.create_task(
                        _run_entry_with_pool(idx, building, site, worker_index)
                    )
                    for worker_index, (idx, building, site) in enumerate(site_entries)
                ]
                gathered = await asyncio.gather(*tasks, return_exceptions=True)
                for task_result, (idx, building, site) in zip(gathered, site_entries):
                    if isinstance(task_result, Exception):
                        result_by_index[idx] = {
                            "success": False,
                            "building": building,
                            "file_path": "",
                            "used_url": _resolve_site_url(site),
                            "error": f"并发下载异常: {task_result}",
                            "failed_step": "并发调度",
                        }
                        self._debug_log(
                            debug_step_log,
                            building,
                            f"并发调度异常导致该楼下载失败, error={task_result}",
                        )
                        continue
                    out_idx, result = task_result
                    result_by_index[out_idx] = result
            else:
                for worker_index, (idx, building, site) in enumerate(site_entries):
                    out_idx, result = await _run_entry_with_pool(
                        idx, building, site, worker_index
                    )
                    result_by_index[out_idx] = result
            return [result_by_index[idx] for idx in sorted(result_by_index.keys())]

        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(**launch_kwargs)
            except Exception:
                if "channel" in launch_kwargs:
                    launch_kwargs.pop("channel", None)
                    browser = await p.chromium.launch(**launch_kwargs)
                else:
                    raise

            try:
                context = await browser.new_context(accept_downloads=True)

                async def _run_entry(
                    idx: int, building: str, site: Dict[str, Any], worker_index: int
                ) -> tuple[int, Dict[str, Any]]:
                    if (
                        parallel_by_building
                        and site_start_delay_sec > 0
                        and worker_index > 0
                    ):
                        await asyncio.sleep(worker_index * site_start_delay_sec)
                    page = await context.new_page()
                    try:
                        result = await self._download_single_building_with_retry(
                            page=page,
                            building=building,
                            site=site,
                            start_time=start_time,
                            end_time=end_time,
                            scale_label=scale_label,
                            template_name=template_name,
                            save_dir=save_dir,
                            query_result_timeout_ms=query_result_timeout_ms,
                            download_event_timeout_ms=download_event_timeout_ms,
                            login_fill_timeout_ms=login_fill_timeout_ms,
                            menu_visible_timeout_ms=menu_visible_timeout_ms,
                            iframe_timeout_ms=iframe_timeout_ms,
                            start_end_visible_timeout_ms=start_end_visible_timeout_ms,
                            page_refresh_retry_count=page_refresh_retry_count,
                            max_retries=max_retries,
                            retry_wait_sec=retry_wait_sec,
                            force_iframe_reopen_each_task=force_iframe_reopen_each_task,
                            export_button_text=export_button_text,
                            menu_path=menu_path,
                            debug_step_log=debug_step_log,
                        )
                        return idx, result
                    finally:
                        try:
                            if not page.is_closed():
                                await page.close()
                        except Exception:  # noqa: BLE001
                            pass

                if parallel_by_building:
                    tasks = [
                        asyncio.create_task(
                            _run_entry(idx, building, site, worker_index)
                        )
                        for worker_index, (idx, building, site) in enumerate(
                            site_entries
                        )
                    ]
                    gathered = await asyncio.gather(*tasks, return_exceptions=True)
                    for task_result, (idx, building, site) in zip(gathered, site_entries):
                        if isinstance(task_result, Exception):
                            result_by_index[idx] = {
                                "success": False,
                                "building": building,
                                "file_path": "",
                                "used_url": _resolve_site_url(site),
                                "error": f"并发下载异常: {task_result}",
                                "failed_step": "并发调度",
                            }
                            self._debug_log(
                                debug_step_log,
                                building,
                                f"并发调度异常导致该楼下载失败, error={task_result}",
                            )
                            continue
                        out_idx, result = task_result
                        result_by_index[out_idx] = result
                else:
                    shared_page = await context.new_page()
                    try:
                        for worker_index, (idx, building, site) in enumerate(
                            site_entries
                        ):
                            if site_start_delay_sec > 0 and worker_index > 0:
                                await asyncio.sleep(worker_index * site_start_delay_sec)
                            result = await self._download_single_building_with_retry(
                                page=shared_page,
                                building=building,
                                site=site,
                                start_time=start_time,
                                end_time=end_time,
                                scale_label=scale_label,
                                template_name=template_name,
                                save_dir=save_dir,
                                query_result_timeout_ms=query_result_timeout_ms,
                                download_event_timeout_ms=download_event_timeout_ms,
                                login_fill_timeout_ms=login_fill_timeout_ms,
                                menu_visible_timeout_ms=menu_visible_timeout_ms,
                                iframe_timeout_ms=iframe_timeout_ms,
                                start_end_visible_timeout_ms=start_end_visible_timeout_ms,
                                page_refresh_retry_count=page_refresh_retry_count,
                                max_retries=max_retries,
                                retry_wait_sec=retry_wait_sec,
                                force_iframe_reopen_each_task=force_iframe_reopen_each_task,
                                export_button_text=export_button_text,
                                menu_path=menu_path,
                                debug_step_log=debug_step_log,
                            )
                            result_by_index[idx] = result
                    finally:
                        try:
                            if not shared_page.is_closed():
                                await shared_page.close()
                        except Exception:  # noqa: BLE001
                            pass
            finally:
                await browser.close()

        return [result_by_index[idx] for idx in sorted(result_by_index.keys())]

    async def _download_handover_xlsx_async(
        self,
        *,
        building: str,
        start_time: str,
        end_time: str,
        scale_label: str,
        template_name: str,
        save_dir: str,
        query_result_timeout_ms: int,
        download_event_timeout_ms: int,
        login_fill_timeout_ms: int,
        menu_visible_timeout_ms: int,
        iframe_timeout_ms: int,
        start_end_visible_timeout_ms: int,
        page_refresh_retry_count: int,
        max_retries: int,
        retry_wait_sec: int,
        force_iframe_reopen_each_task: bool,
        export_button_text: str,
        menu_path: List[str],
        debug_step_log: bool = True,
    ) -> Dict[str, Any]:
        rows = await self._download_handover_xlsx_batch_async(
            buildings=[building],
            start_time=start_time,
            end_time=end_time,
            scale_label=scale_label,
            template_name=template_name,
            save_dir=save_dir,
            query_result_timeout_ms=query_result_timeout_ms,
            download_event_timeout_ms=download_event_timeout_ms,
            login_fill_timeout_ms=login_fill_timeout_ms,
            menu_visible_timeout_ms=menu_visible_timeout_ms,
            iframe_timeout_ms=iframe_timeout_ms,
            start_end_visible_timeout_ms=start_end_visible_timeout_ms,
            page_refresh_retry_count=page_refresh_retry_count,
            max_retries=max_retries,
            retry_wait_sec=retry_wait_sec,
            force_iframe_reopen_each_task=force_iframe_reopen_each_task,
            export_button_text=export_button_text,
            menu_path=menu_path,
            parallel_by_building=False,
            site_start_delay_sec=0,
            debug_step_log=debug_step_log,
        )
        if not rows:
            return {
                "success": False,
                "building": building,
                "file_path": "",
                "used_url": "",
                "error": "未产生下载结果",
                "failed_step": "初始化",
            }
        return rows[0]

    def download_handover_xlsx(
        self,
        *,
        building: str,
        start_time: str,
        end_time: str,
        scale_label: str,
        template_name: str,
        save_dir: str,
        query_result_timeout_ms: int = 20000,
        download_event_timeout_ms: int = 120000,
        login_fill_timeout_ms: int = 5000,
        menu_visible_timeout_ms: int = 20000,
        iframe_timeout_ms: int = 15000,
        start_end_visible_timeout_ms: int = 5000,
        page_refresh_retry_count: int = 1,
        max_retries: int = 2,
        retry_wait_sec: int = 2,
        force_iframe_reopen_each_task: bool = True,
        export_button_text: str = "原样导出",
        menu_path: List[str] | None = None,
        debug_step_log: bool = True,
    ) -> Dict[str, Any]:
        return asyncio.run(
            self._download_handover_xlsx_async(
                building=building,
                start_time=start_time,
                end_time=end_time,
                scale_label=scale_label,
                template_name=template_name,
                save_dir=save_dir,
                query_result_timeout_ms=int(query_result_timeout_ms),
                download_event_timeout_ms=int(download_event_timeout_ms),
                login_fill_timeout_ms=int(login_fill_timeout_ms),
                menu_visible_timeout_ms=int(menu_visible_timeout_ms),
                iframe_timeout_ms=int(iframe_timeout_ms),
                start_end_visible_timeout_ms=int(start_end_visible_timeout_ms),
                page_refresh_retry_count=int(page_refresh_retry_count),
                max_retries=int(max_retries),
                retry_wait_sec=int(retry_wait_sec),
                force_iframe_reopen_each_task=bool(force_iframe_reopen_each_task),
                export_button_text=str(export_button_text or "原样导出").strip()
                or "原样导出",
                menu_path=menu_path
                if isinstance(menu_path, list)
                else ["报表报告", "数据查询", "即时报表"],
                debug_step_log=bool(debug_step_log),
            )
        )

    def download_handover_xlsx_batch(
        self,
        *,
        buildings: List[str],
        start_time: str,
        end_time: str,
        scale_label: str,
        template_name: str,
        save_dir: str,
        query_result_timeout_ms: int = 20000,
        download_event_timeout_ms: int = 120000,
        login_fill_timeout_ms: int = 5000,
        menu_visible_timeout_ms: int = 20000,
        iframe_timeout_ms: int = 15000,
        start_end_visible_timeout_ms: int = 5000,
        page_refresh_retry_count: int = 1,
        max_retries: int = 2,
        retry_wait_sec: int = 2,
        force_iframe_reopen_each_task: bool = True,
        export_button_text: str = "原样导出",
        menu_path: List[str] | None = None,
        parallel_by_building: bool = False,
        site_start_delay_sec: int = 1,
        debug_step_log: bool = True,
        browser_pool: Any | None = None,
    ) -> List[Dict[str, Any]]:
        return asyncio.run(
            self._download_handover_xlsx_batch_async(
                buildings=buildings,
                start_time=start_time,
                end_time=end_time,
                scale_label=scale_label,
                template_name=template_name,
                save_dir=save_dir,
                query_result_timeout_ms=int(query_result_timeout_ms),
                download_event_timeout_ms=int(download_event_timeout_ms),
                login_fill_timeout_ms=int(login_fill_timeout_ms),
                menu_visible_timeout_ms=int(menu_visible_timeout_ms),
                iframe_timeout_ms=int(iframe_timeout_ms),
                start_end_visible_timeout_ms=int(start_end_visible_timeout_ms),
                page_refresh_retry_count=int(page_refresh_retry_count),
                max_retries=int(max_retries),
                retry_wait_sec=int(retry_wait_sec),
                force_iframe_reopen_each_task=bool(force_iframe_reopen_each_task),
                export_button_text=str(export_button_text or "原样导出").strip()
                or "原样导出",
                menu_path=menu_path
                if isinstance(menu_path, list)
                else ["报表报告", "数据查询", "即时报表"],
                parallel_by_building=bool(parallel_by_building),
                site_start_delay_sec=max(0, int(site_start_delay_sec)),
                debug_step_log=bool(debug_step_log),
                browser_pool=browser_pool,
            )
        )


def download_handover_xlsx(
    building: str,
    start_time: str,
    end_time: str,
    scale_label: str,
    template_name: str,
    save_dir: str,
    query_result_timeout_ms: int = 20000,
    download_event_timeout_ms: int = 120000,
    login_fill_timeout_ms: int = 5000,
    menu_visible_timeout_ms: int = 20000,
    iframe_timeout_ms: int = 15000,
    start_end_visible_timeout_ms: int = 5000,
    page_refresh_retry_count: int = 1,
    max_retries: int = 2,
    retry_wait_sec: int = 2,
    force_iframe_reopen_each_task: bool = True,
    export_button_text: str = "原样导出",
    menu_path: List[str] | None = None,
    debug_step_log: bool = True,
) -> Dict[str, Any]:
    if not isinstance(_RUNTIME_CONFIG, dict):
        raise RuntimeError("请先调用 set_runtime_config(config)")
    gateway = DownloadGateway(_RUNTIME_CONFIG)
    return gateway.download_handover_xlsx(
        building=building,
        start_time=start_time,
        end_time=end_time,
        scale_label=scale_label,
        template_name=template_name,
        save_dir=save_dir,
        query_result_timeout_ms=query_result_timeout_ms,
        download_event_timeout_ms=download_event_timeout_ms,
        login_fill_timeout_ms=login_fill_timeout_ms,
        menu_visible_timeout_ms=menu_visible_timeout_ms,
        iframe_timeout_ms=iframe_timeout_ms,
        start_end_visible_timeout_ms=start_end_visible_timeout_ms,
        page_refresh_retry_count=page_refresh_retry_count,
        max_retries=max_retries,
        retry_wait_sec=retry_wait_sec,
        force_iframe_reopen_each_task=force_iframe_reopen_each_task,
        export_button_text=export_button_text,
        menu_path=menu_path,
        debug_step_log=debug_step_log,
    )


def download_handover_xlsx_batch(
    buildings: List[str],
    start_time: str,
    end_time: str,
    scale_label: str,
    template_name: str,
    save_dir: str,
    query_result_timeout_ms: int = 20000,
    download_event_timeout_ms: int = 120000,
    login_fill_timeout_ms: int = 5000,
    menu_visible_timeout_ms: int = 20000,
    iframe_timeout_ms: int = 15000,
    start_end_visible_timeout_ms: int = 5000,
    page_refresh_retry_count: int = 1,
    max_retries: int = 2,
    retry_wait_sec: int = 2,
    force_iframe_reopen_each_task: bool = True,
    export_button_text: str = "原样导出",
    menu_path: List[str] | None = None,
    parallel_by_building: bool = False,
    site_start_delay_sec: int = 1,
    debug_step_log: bool = True,
    browser_pool: Any | None = None,
) -> List[Dict[str, Any]]:
    if not isinstance(_RUNTIME_CONFIG, dict):
        raise RuntimeError("请先调用 set_runtime_config(config)")
    gateway = DownloadGateway(_RUNTIME_CONFIG)
    return gateway.download_handover_xlsx_batch(
        buildings=buildings,
        start_time=start_time,
        end_time=end_time,
        scale_label=scale_label,
        template_name=template_name,
        save_dir=save_dir,
        query_result_timeout_ms=query_result_timeout_ms,
        download_event_timeout_ms=download_event_timeout_ms,
        login_fill_timeout_ms=login_fill_timeout_ms,
        menu_visible_timeout_ms=menu_visible_timeout_ms,
        iframe_timeout_ms=iframe_timeout_ms,
        start_end_visible_timeout_ms=start_end_visible_timeout_ms,
        page_refresh_retry_count=page_refresh_retry_count,
        max_retries=max_retries,
        retry_wait_sec=retry_wait_sec,
        force_iframe_reopen_each_task=force_iframe_reopen_each_task,
        export_button_text=export_button_text,
        menu_path=menu_path,
        parallel_by_building=parallel_by_building,
        site_start_delay_sec=site_start_delay_sec,
        debug_step_log=debug_step_log,
        browser_pool=browser_pool,
    )
