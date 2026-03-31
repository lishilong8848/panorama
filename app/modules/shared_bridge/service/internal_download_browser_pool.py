from __future__ import annotations

import asyncio
import concurrent.futures
import copy
import threading
import time
from typing import Any, Awaitable, Callable, Dict

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

from pipeline_utils import configure_playwright_environment
from app.shared.utils.playwright_page_reuse import prepare_reusable_page


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


class InternalDownloadBrowserPool:
    BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")

    def __init__(
        self,
        runtime_config: Dict[str, Any],
        *,
        emit_log: Callable[[str], None] | None = None,
    ) -> None:
        self.runtime_config = copy.deepcopy(runtime_config if isinstance(runtime_config, dict) else {})
        self.emit_log = emit_log
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready_event = threading.Event()
        self._startup_error = ""
        self._playwright: Playwright | None = None
        self._browser_slots: Dict[str, Dict[str, Any]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._state_lock = threading.Lock()
        self._last_error = ""
        self._slot_state: Dict[str, Dict[str, Any]] = {
            building: {
                "building": building,
                "browser_ready": False,
                "page_ready": False,
                "login_state": "waiting",
                "last_login_at": "",
                "login_error": "",
                "in_use": False,
                "last_used_at": "",
                "last_result": "",
                "last_error": "",
            }
            for building in self.BUILDINGS
        }

    def _log(self, text: str) -> None:
        if not self.emit_log:
            return
        try:
            self.emit_log(text)
        except Exception:
            pass

    def _resolve_browser_options(self) -> Dict[str, Any]:
        handover_cfg = self.runtime_config.get("handover_log", {})
        handover_download = handover_cfg.get("download", {}) if isinstance(handover_cfg, dict) else {}
        monthly_download = self.runtime_config.get("download", {})
        if not isinstance(monthly_download, dict):
            monthly_download = {}
        browser_channel = str(
            handover_download.get("browser_channel", monthly_download.get("browser_channel", ""))
        ).strip()
        launch_kwargs: Dict[str, Any] = {
            "headless": False,
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
        return launch_kwargs

    def _update_slot(self, building: str, **changes: Any) -> None:
        with self._state_lock:
            slot = self._slot_state.setdefault(building, {"building": building})
            slot.update(changes)

    def _slot_login_state(self, building: str) -> str:
        with self._state_lock:
            slot = self._slot_state.get(building, {})
            return str(slot.get("login_state", "") or "").strip().lower()

    def _resolve_login_timeout_ms(self) -> int:
        handover_cfg = self.runtime_config.get("handover_log", {})
        handover_download = handover_cfg.get("download", {}) if isinstance(handover_cfg, dict) else {}
        monthly_perf = self.runtime_config.get("download", {}).get("performance", {})
        if not isinstance(monthly_perf, dict):
            monthly_perf = {}
        value = handover_download.get("login_fill_timeout_ms", monthly_perf.get("login_fill_timeout_ms", 5000))
        try:
            return max(1000, int(value))
        except Exception:
            return 5000

    def _resolve_refresh_timeout_ms(self) -> int:
        handover_cfg = self.runtime_config.get("handover_log", {})
        handover_download = handover_cfg.get("download", {}) if isinstance(handover_cfg, dict) else {}
        monthly_perf = self.runtime_config.get("download", {}).get("performance", {})
        if not isinstance(monthly_perf, dict):
            monthly_perf = {}
        value = handover_download.get("query_result_timeout_ms", monthly_perf.get("query_result_timeout_ms", 20000))
        try:
            return max(5000, int(value))
        except Exception:
            return 20000

    def _clean_sites(self, raw_sites: Any) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        if not isinstance(raw_sites, list):
            return output
        for item in raw_sites:
            if not isinstance(item, dict):
                continue
            output.append(
                {
                    "building": str(item.get("building", "") or "").strip(),
                    "enabled": bool(item.get("enabled", False)),
                    "host": str(item.get("host", "") or item.get("url", "") or "").strip(),
                    "username": str(item.get("username", "") or "").strip(),
                    "password": str(item.get("password", "") or ""),
                }
            )
        return output

    def _find_site(self, building: str) -> dict[str, Any] | None:
        candidates = self._clean_sites(self.runtime_config.get("internal_source_sites", []))
        if not candidates:
            download_cfg = self.runtime_config.get("download", {})
            if isinstance(download_cfg, dict):
                candidates = self._clean_sites(download_cfg.get("sites", []))
        if not candidates:
            handover_cfg = self.runtime_config.get("handover_log", {})
            if isinstance(handover_cfg, dict):
                candidates = self._clean_sites(handover_cfg.get("sites", []))
                if not candidates:
                    download_cfg = handover_cfg.get("download", {})
                    if isinstance(download_cfg, dict):
                        candidates = self._clean_sites(download_cfg.get("sites", []))
        building_text = str(building or "").strip()
        for site in candidates:
            if str(site.get("building", "")).strip() != building_text:
                continue
            if not bool(site.get("enabled", False)):
                continue
            host = str(site.get("host", "") or "").strip()
            username = str(site.get("username", "") or "").strip()
            password = str(site.get("password", "") or "")
            if not host or not username or not password:
                continue
            return site
        return None

    @staticmethod
    def _resolve_site_url(site: dict[str, Any]) -> str:
        host = str(site.get("host", "") or "").strip()
        if not host:
            return ""
        host = host.replace("http://", "").replace("https://", "").strip().strip("/")
        if not host:
            return ""
        return f"http://{host}/page/main/main.html"

    async def _login_if_needed(self, building: str, page: Page, site: dict[str, Any]) -> None:
        login_timeout_ms = self._resolve_login_timeout_ms()
        username = str(site.get("username", "") or "").strip()
        password = str(site.get("password", "") or "")
        refresh_timeout_ms = self._resolve_refresh_timeout_ms()
        target_url = self._resolve_site_url(site)
        if not target_url:
            return

        await prepare_reusable_page(page, target_url=target_url, refresh_timeout_ms=refresh_timeout_ms)
        login_visible = False
        try:
            await page.wait_for_selector("#username", state="visible", timeout=login_timeout_ms)
            login_visible = True
        except Exception:
            login_visible = False

        if login_visible:
            self._update_slot(building, login_state="logging_in", login_error="")
            self._log(f"[共享桥接] 楼栋预登录开始: {building}")
            try:
                await page.fill("#username", username)
                await page.fill("#password", password)
                await page.click("text=登录")
            except Exception as exc:
                error_text = str(exc)
                self._update_slot(building, login_state="failed", login_error=error_text, last_error=error_text)
                raise RuntimeError(f"{building} 预登录失败: {error_text}") from exc

        try:
            await page.wait_for_selector("a.p-main__header__menu-item", state="visible", timeout=20000)
        except Exception as exc:
            error_text = str(exc)
            if login_visible:
                self._update_slot(building, login_state="failed", login_error=error_text, last_error=error_text)
            else:
                self._update_slot(building, login_state="expired", login_error=error_text, last_error=error_text)
            raise RuntimeError(f"{building} 登录态未就绪: {error_text}") from exc

        self._update_slot(
            building,
            login_state="ready",
            login_error="",
            last_error="",
            last_login_at=_now_text(),
        )
        if login_visible:
            self._log(f"[共享桥接] 楼栋预登录成功: {building}")

    async def _ensure_logged_in(self, building: str, page: Page) -> None:
        site = self._find_site(building)
        if site is None:
            self._update_slot(building, login_state="waiting", login_error="", last_error="")
            return
        try:
            await self._login_if_needed(building, page, site)
        except Exception as exc:
            error_text = str(exc)
            self._last_error = error_text
            if self._slot_login_state(building) not in {"failed", "expired"}:
                self._update_slot(building, login_state="failed", login_error=error_text, last_error=error_text)
            raise

    async def _async_prelogin_building(self, building: str) -> None:
        lock = self._locks.get(building)
        if lock is None:
            return
        async with lock:
            page = await self._ensure_page(building)
            try:
                await self._ensure_logged_in(building, page)
            except Exception:
                return

    async def _close_slot(self, building: str) -> None:
        slot = self._browser_slots.pop(building, None)
        if not slot:
            self._update_slot(
                building,
                browser_ready=False,
                page_ready=False,
                login_state="waiting",
                last_login_at="",
                login_error="",
                in_use=False,
            )
            return
        page = slot.get("page")
        context = slot.get("context")
        browser = slot.get("browser")
        try:
            if page is not None and not page.is_closed():
                await page.close()
        except Exception:
            pass
        try:
            if context is not None:
                await context.close()
        except Exception:
            pass
        try:
            if browser is not None:
                await browser.close()
        except Exception:
            pass
        self._update_slot(
            building,
            browser_ready=False,
            page_ready=False,
            login_state="waiting",
            last_login_at="",
            login_error="",
            in_use=False,
        )

    async def _create_or_replace_slot(self, building: str) -> Page:
        if self._playwright is None:
            raise RuntimeError("内网下载浏览器池未初始化")
        await self._close_slot(building)
        launch_kwargs = self._resolve_browser_options()
        try:
            browser = await self._playwright.chromium.launch(**launch_kwargs)
        except Exception:
            if "channel" in launch_kwargs:
                launch_kwargs.pop("channel", None)
                browser = await self._playwright.chromium.launch(**launch_kwargs)
            else:
                raise
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        self._browser_slots[building] = {
            "browser": browser,
            "context": context,
            "page": page,
        }
        self._update_slot(
            building,
            browser_ready=True,
            page_ready=True,
            login_state="waiting",
            last_login_at="",
            login_error="",
            in_use=False,
            last_result="ready",
            last_error="",
        )
        return page

    async def _ensure_page(self, building: str) -> Page:
        slot = self._browser_slots.get(building)
        if not slot:
            return await self._create_or_replace_slot(building)
        page = slot.get("page")
        context = slot.get("context")
        browser = slot.get("browser")
        try:
            browser_connected = bool(browser and browser.is_connected())
        except Exception:
            browser_connected = False
        try:
            page_closed = True if page is None else page.is_closed()
        except Exception:
            page_closed = True
        if not browser_connected or context is None or page_closed:
            return await self._create_or_replace_slot(building)
        return page

    async def _async_start(self) -> None:
        configure_playwright_environment(self.runtime_config)
        self._playwright = await async_playwright().start()
        self._locks = {building: asyncio.Lock() for building in self.BUILDINGS}
        for building in self.BUILDINGS:
            await self._create_or_replace_slot(building)

    async def _async_stop(self) -> None:
        for building in list(self.BUILDINGS):
            await self._close_slot(building)
        self._browser_slots.clear()
        if self._playwright is not None:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._async_start())
            self._log("[共享桥接] 内网下载浏览器池已启动：5 个楼栋浏览器实例已就绪")
        except Exception as exc:
            self._startup_error = str(exc)
            self._last_error = self._startup_error
            self._ready_event.set()
            try:
                loop.run_until_complete(self._async_stop())
            except Exception:
                pass
            loop.close()
            self._loop = None
            return
        self._ready_event.set()
        for building in self.BUILDINGS:
            loop.create_task(self._async_prelogin_building(building))
        try:
            loop.run_forever()
        finally:
            try:
                loop.run_until_complete(self._async_stop())
            except Exception:
                pass
            loop.close()
            self._loop = None

    def start(self) -> Dict[str, Any]:
        if self.is_running():
            return {"started": False, "running": True, "reason": "already_running"}
        self._startup_error = ""
        self._ready_event.clear()
        self._thread = threading.Thread(
            target=self._thread_main,
            name="internal-download-browser-pool",
            daemon=True,
        )
        self._thread.start()
        self._ready_event.wait(timeout=30)
        if self._startup_error:
            return {
                "started": False,
                "running": False,
                "reason": "startup_failed",
                "error": self._startup_error,
            }
        return {"started": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        loop = self._loop
        thread = self._thread
        if loop is None or thread is None:
            return {"stopped": False, "running": False, "reason": "not_running"}
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=10)
        self._thread = None
        self._loop = None
        return {"stopped": True, "running": False, "reason": "stopped"}

    def is_running(self) -> bool:
        thread = self._thread
        return bool(thread and thread.is_alive() and self._loop is not None and not self._startup_error)

    async def _run_building_job(
        self,
        building: str,
        runner: Callable[[Page], Awaitable[Any]],
    ) -> Any:
        if building not in self.BUILDINGS:
            raise ValueError(f"不支持的内网下载楼栋: {building}")
        lock = self._locks.get(building)
        if lock is None:
            raise RuntimeError(f"楼栋浏览器锁未初始化: {building}")
        async with lock:
            page = await self._ensure_page(building)
            await self._ensure_logged_in(building, page)
            self._update_slot(
                building,
                in_use=True,
                last_used_at=_now_text(),
                last_result="running",
                last_error="",
            )
            try:
                result = await runner(page)
                refreshed_page = await self._ensure_page(building)
                if refreshed_page is not page:
                    self._log(f"[共享桥接] 楼栋浏览器异常，已重建: {building}")
                    await self._ensure_logged_in(building, refreshed_page)
                self._update_slot(
                    building,
                    in_use=False,
                    last_used_at=_now_text(),
                    last_result="success",
                    last_error="",
                )
                self._log(f"[共享桥接] 楼栋浏览器复用成功: {building}")
                return result
            except Exception as exc:
                error_text = str(exc)
                self._last_error = error_text
                try:
                    recovered_page = await self._ensure_page(building)
                    if recovered_page is not page:
                        self._log(f"[共享桥接] 楼栋浏览器异常，已重建: {building}")
                    await self._ensure_logged_in(building, recovered_page)
                except Exception:
                    pass
                self._update_slot(
                    building,
                    in_use=False,
                    last_used_at=_now_text(),
                    last_result="failed",
                    last_error=error_text,
                )
                raise

    def submit_building_job(
        self,
        building: str,
        runner: Callable[[Page], Awaitable[Any]],
    ) -> concurrent.futures.Future[Any]:
        loop = self._loop
        if not self.is_running() or loop is None:
            future: concurrent.futures.Future[Any] = concurrent.futures.Future()
            future.set_exception(RuntimeError("内网下载浏览器池未启动"))
            return future
        return asyncio.run_coroutine_threadsafe(
            self._run_building_job(building, runner),
            loop,
        )

    def get_health_snapshot(self) -> Dict[str, Any]:
        with self._state_lock:
            page_slots = [copy.deepcopy(self._slot_state[building]) for building in self.BUILDINGS]
        active_buildings = [slot["building"] for slot in page_slots if bool(slot.get("in_use", False))]
        return {
            "enabled": True,
            "browser_ready": self.is_running(),
            "page_slots": page_slots,
            "active_buildings": active_buildings,
            "last_error": self._last_error,
        }
