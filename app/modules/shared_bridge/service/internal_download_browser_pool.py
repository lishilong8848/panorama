from __future__ import annotations

import asyncio
import concurrent.futures
import copy
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

from pipeline_utils import configure_playwright_environment
from app.shared.utils.playwright_page_reuse import prepare_reusable_page


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _future_text(seconds: int) -> str:
    return (datetime.now() + timedelta(seconds=max(1, int(seconds or 1)))).strftime("%Y-%m-%d %H:%M:%S")


def _parse_text_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


class BuildingPausedError(RuntimeError):
    def __init__(self, building: str, reason: str, *, failure_kind: str = "browser_issue") -> None:
        self.building = str(building or "").strip()
        self.reason = str(reason or "").strip()
        self.failure_kind = str(failure_kind or "browser_issue").strip() or "browser_issue"
        super().__init__(self.reason)


class InternalDownloadBrowserPool:
    BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")
    MAX_RECOVERY_ATTEMPTS = 3
    RECOVERY_PROBE_INTERVAL_SEC = 60
    RECYCLE_AFTER_AGE_SEC = 12 * 60 * 60
    RECYCLE_AFTER_JOB_COUNT = 100
    ALARM_PAGE_PATH = "/page/warn_event/warn_event.html"

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
        self._recovery_probe_task: asyncio.Task[Any] | None = None
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
                "suspended": False,
                "suspend_reason": "",
                "failure_kind": "",
                "recovery_attempts": 0,
                "last_failure_at": "",
                "next_probe_at": "",
                "pending_issue_summary": "",
                "slot_created_at": "",
                "last_recycled_at": "",
                "jobs_since_recycle": 0,
                "pending_recycle": False,
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

    def _slot_snapshot(self, building: str) -> dict[str, Any]:
        with self._state_lock:
            slot = self._slot_state.get(building, {})
            return copy.deepcopy(slot)

    def get_building_pause_info(self, building: str) -> dict[str, Any]:
        slot = self._slot_snapshot(building)
        return {
            "building": str(slot.get("building", building) or building).strip(),
            "suspended": bool(slot.get("suspended", False)),
            "suspend_reason": str(slot.get("suspend_reason", "") or "").strip(),
            "failure_kind": str(slot.get("failure_kind", "") or "").strip(),
            "recovery_attempts": int(slot.get("recovery_attempts", 0) or 0),
            "last_failure_at": str(slot.get("last_failure_at", "") or "").strip(),
            "next_probe_at": str(slot.get("next_probe_at", "") or "").strip(),
            "pending_issue_summary": str(slot.get("pending_issue_summary", "") or "").strip(),
            "login_state": str(slot.get("login_state", "") or "").strip(),
        }

    def _slot_age_sec(self, building: str) -> int:
        slot = self._slot_snapshot(building)
        created_at = _parse_text_datetime(slot.get("slot_created_at"))
        if created_at is None:
            return 0
        return max(0, int((datetime.now() - created_at).total_seconds()))

    def _mark_slot_recycle_pending_if_needed(self, building: str) -> None:
        slot = self._slot_snapshot(building)
        jobs_since_recycle = int(slot.get("jobs_since_recycle", 0) or 0)
        slot_age_sec = self._slot_age_sec(building)
        should_recycle = (
            slot_age_sec > self.RECYCLE_AFTER_AGE_SEC
            or jobs_since_recycle >= self.RECYCLE_AFTER_JOB_COUNT
        )
        if should_recycle:
            self._update_slot(building, pending_recycle=True)

    async def _recycle_slot_if_needed(self, building: str) -> None:
        slot = self._slot_snapshot(building)
        if not bool(slot.get("pending_recycle", False)) or bool(slot.get("in_use", False)):
            return
        await self._create_or_replace_slot(building)
        self._log(f"[共享桥接] 楼栋浏览器已滚动回收: {building}")

    @staticmethod
    def _classify_failure_kind(error_text: Any, *, login_state: str = "") -> str:
        normalized = str(error_text or "").strip().lower()
        state = str(login_state or "").strip().lower()
        if state == "expired" or "login_required" in normalized or "登录态" in str(error_text or ""):
            return "login_expired"
        if "告警页面" in str(error_text or "") or "告警查询" in str(error_text or ""):
            return "alarm_query_failed"
        if "err_empty_response" in normalized or "page.goto:" in normalized:
            return "page_unreachable"
        if "err_connection_refused" in normalized:
            return "page_connection_refused"
        if "err_connection_timed_out" in normalized or "err_timed_out" in normalized:
            return "page_timeout"
        if "err_name_not_resolved" in normalized:
            return "page_address_invalid"
        if "err_internet_disconnected" in normalized:
            return "network_disconnected"
        if "target page, context or browser has been closed" in normalized or "browser has been closed" in normalized:
            return "browser_closed"
        if "page closed" in normalized or "context closed" in normalized or "execution context was destroyed" in normalized:
            return "page_closed"
        if "登录失败" in str(error_text or ""):
            return "login_failed"
        return "unknown"

    @classmethod
    def _failure_kind_label(cls, failure_kind: str) -> str:
        normalized = str(failure_kind or "").strip().lower()
        mapping = {
            "login_failed": "登录失败",
            "login_expired": "登录失效",
            "alarm_query_failed": "告警查询失败",
            "page_unreachable": "页面无响应",
            "page_connection_refused": "页面拒绝连接",
            "page_timeout": "页面访问超时",
            "page_address_invalid": "页面地址异常",
            "network_disconnected": "网络未连接",
            "browser_closed": "浏览器已关闭",
            "page_closed": "页面已关闭",
            "browser_issue": "浏览器异常",
        }
        return mapping.get(normalized, "页面异常")

    def _clear_issue_state(self, building: str) -> None:
        self._update_slot(
            building,
            suspended=False,
            suspend_reason="",
            failure_kind="",
            recovery_attempts=0,
            last_failure_at="",
            next_probe_at="",
            pending_issue_summary="",
        )

    def _suspend_building(self, building: str, *, failure_kind: str, reason: str, recovery_attempts: int) -> str:
        summary = f"{building} {self._failure_kind_label(failure_kind)}: {reason}"
        self._update_slot(
            building,
            suspended=True,
            suspend_reason=summary,
            failure_kind=failure_kind,
            recovery_attempts=max(self.MAX_RECOVERY_ATTEMPTS, int(recovery_attempts or 0)),
            last_failure_at=_now_text(),
            next_probe_at=_future_text(self.RECOVERY_PROBE_INTERVAL_SEC),
            pending_issue_summary=summary,
            last_error=summary,
        )
        self._last_error = summary
        return summary

    async def _raise_if_suspended(self, building: str, page: Page | None = None) -> None:
        slot = self._slot_snapshot(building)
        if not bool(slot.get("suspended", False)):
            return
        if page is not None:
            probe_state = await self._probe_existing_login_state(page)
            if probe_state == "ready":
                self._clear_issue_state(building)
                self._update_slot(
                    building,
                    login_state="ready",
                    login_error="",
                    last_error="",
                    last_login_at=_now_text(),
                    last_result="ready",
                )
                self._log(f"[共享桥接] 楼栋浏览器状态恢复: {building}")
                return
        reason = str(slot.get("suspend_reason", "") or slot.get("pending_issue_summary", "") or slot.get("login_error", "") or "").strip()
        raise BuildingPausedError(
            building,
            reason or f"{building} 已暂停等待恢复",
            failure_kind=str(slot.get("failure_kind", "") or "browser_issue").strip() or "browser_issue",
        )

    @staticmethod
    def _format_login_error(raw: Any) -> str:
        text = str(raw or "").strip()
        if not text:
            return "登录失败，请检查楼栋页面和账号状态"
        normalized = text.lower()
        if "net::err_empty_response" in normalized:
            return "页面无响应，请检查楼栋页面服务或网络"
        if "net::err_connection_refused" in normalized:
            return "页面拒绝连接，请检查楼栋页面服务是否启动"
        if "net::err_connection_timed_out" in normalized or "net::err_timed_out" in normalized:
            return "页面访问超时，请检查楼栋网络或站点状态"
        if "net::err_name_not_resolved" in normalized:
            return "页面地址无法解析，请检查楼栋地址配置"
        if "net::err_internet_disconnected" in normalized:
            return "网络未连接，请检查当前网络"
        if "login_required" in normalized:
            return "登录态已失效，请重新登录"
        if "page.goto:" in normalized:
            return "页面访问失败，请检查楼栋页面是否可达"
        return text

    @staticmethod
    def _build_login_failure_message(building: str, login_state: str, reason: str) -> str:
        state = str(login_state or "").strip().lower()
        if state == "expired":
            return f"{building} 登录态未就绪: {reason}"
        return f"{building} 登录失败: {reason}"

    async def _probe_existing_login_state(self, page: Page) -> str:
        try:
            await page.wait_for_selector("a.p-main__header__menu-item", state="visible", timeout=1500)
            return "ready"
        except Exception:
            pass
        try:
            await page.wait_for_selector("#username", state="visible", timeout=800)
            return "login_required"
        except Exception:
            pass
        return "unknown"

    async def _fail_if_login_not_ready(self, building: str, page: Page) -> None:
        await self._raise_if_suspended(building, page)
        slot = self._slot_snapshot(building)
        login_state = str(slot.get("login_state", "") or "").strip().lower()
        if login_state not in {"failed", "expired"}:
            return
        probe_state = await self._probe_existing_login_state(page)
        if probe_state == "ready":
            self._update_slot(
                building,
                login_state="ready",
                login_error="",
                last_error="",
                last_login_at=_now_text(),
            )
            return
        reason = self._format_login_error(slot.get("login_error") or slot.get("last_error"))
        message = self._build_login_failure_message(building, login_state, reason)
        self._last_error = message
        self._update_slot(
            building,
            login_state="expired" if probe_state == "login_required" else login_state,
            login_error=reason,
            in_use=False,
            last_used_at=_now_text(),
            last_result="failed",
            last_error=message,
        )
        raise BuildingPausedError(
            building,
            message,
            failure_kind=self._classify_failure_kind(reason, login_state=login_state) or "browser_issue",
        )

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

    @classmethod
    def _resolve_alarm_page_url(cls, site: dict[str, Any]) -> str:
        host = str(site.get("host", "") or "").strip()
        if not host:
            return ""
        host = host.replace("http://", "").replace("https://", "").strip().strip("/")
        if not host:
            return ""
        return f"http://{host}{cls.ALARM_PAGE_PATH}"

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
                reason = self._format_login_error(exc)
                self._update_slot(building, login_state="failed", login_error=reason, last_error=reason)
                raise RuntimeError(self._build_login_failure_message(building, "failed", reason)) from exc

        try:
            await page.wait_for_selector("a.p-main__header__menu-item", state="visible", timeout=20000)
        except Exception as exc:
            reason = self._format_login_error(exc)
            if login_visible:
                self._update_slot(building, login_state="failed", login_error=reason, last_error=reason)
            else:
                self._update_slot(building, login_state="expired", login_error=reason, last_error=reason)
            raise RuntimeError(self._build_login_failure_message(building, "failed" if login_visible else "expired", reason)) from exc

        self._update_slot(
            building,
            login_state="ready",
            login_error="",
            last_error="",
            last_login_at=_now_text(),
        )
        self._clear_issue_state(building)
        if login_visible:
            self._log(f"[共享桥接] 楼栋预登录成功: {building}")

    async def _ensure_logged_in(self, building: str, page: Page) -> None:
        site = self._find_site(building)
        if site is None:
            self._clear_issue_state(building)
            self._update_slot(building, login_state="waiting", login_error="", last_error="")
            return
        try:
            await self._login_if_needed(building, page, site)
        except Exception as exc:
            error_text = str(exc)
            reason = self._format_login_error(error_text)
            self._last_error = error_text
            if self._slot_login_state(building) not in {"failed", "expired"}:
                self._update_slot(building, login_state="failed", login_error=reason, last_error=error_text)
            raise

    async def _try_recovery_refresh(self, building: str, page: Page) -> tuple[bool, str]:
        try:
            await page.reload(wait_until="domcontentloaded", timeout=self._resolve_refresh_timeout_ms())
            await self._ensure_logged_in(building, page)
            return True, ""
        except Exception as exc:  # noqa: BLE001
            return False, self._format_login_error(exc)

    async def _try_recovery_reopen(self, building: str, page: Page) -> tuple[bool, str]:
        site = self._find_site(building)
        target_url = self._resolve_site_url(site or {}) if site else ""
        if not target_url:
            return False, "页面地址未配置，请检查楼栋配置"
        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=self._resolve_refresh_timeout_ms())
            await self._ensure_logged_in(building, page)
            return True, ""
        except Exception as exc:  # noqa: BLE001
            return False, self._format_login_error(exc)

    async def _try_recovery_rebuild(self, building: str) -> tuple[bool, str]:
        try:
            page = await self._create_or_replace_slot(building)
            await self._ensure_logged_in(building, page)
            return True, ""
        except Exception as exc:  # noqa: BLE001
            return False, self._format_login_error(exc)

    async def _attempt_building_recovery(
        self,
        building: str,
        *,
        base_reason: str,
        failure_kind: str,
        from_probe: bool = False,
    ) -> bool:
        latest_reason = self._format_login_error(base_reason)
        recovery_attempts = 0
        try:
            page = await self._ensure_page(building)
        except Exception as exc:  # noqa: BLE001
            page = None
            latest_reason = self._format_login_error(exc)
        attempts: list[tuple[str, Callable[[], Awaitable[tuple[bool, str]]]]] = []
        if page is not None:
            attempts.append(("刷新页面", lambda: self._try_recovery_refresh(building, page)))
            attempts.append(("重新进入页面", lambda: self._try_recovery_reopen(building, page)))
        attempts.append(("重建浏览器", lambda: self._try_recovery_rebuild(building)))
        for _label, step in attempts[: self.MAX_RECOVERY_ATTEMPTS]:
            recovery_attempts += 1
            self._update_slot(building, recovery_attempts=recovery_attempts)
            ok, reason = await step()
            if ok:
                self._clear_issue_state(building)
                self._update_slot(
                    building,
                    login_state="ready",
                    login_error="",
                    last_error="",
                    last_login_at=_now_text(),
                    last_result="ready",
                )
                self._log(f"[共享桥接] 楼栋浏览器状态恢复: {building}")
                return True
            latest_reason = self._format_login_error(reason or latest_reason)
        summary = self._suspend_building(
            building,
            failure_kind=failure_kind or "browser_issue",
            reason=latest_reason,
            recovery_attempts=recovery_attempts,
        )
        self._update_slot(
            building,
            login_state="expired" if failure_kind == "login_expired" else "failed",
            login_error=latest_reason,
            last_result="failed",
        )
        if not from_probe:
            self._log(f"[共享桥接] 楼栋浏览器已暂停等待恢复: {summary}")
        return False

    async def _async_recovery_probe_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(1)
                for building in self.BUILDINGS:
                    slot = self._slot_snapshot(building)
                    if not bool(slot.get("suspended", False)):
                        continue
                    next_probe_at = _parse_text_datetime(slot.get("next_probe_at"))
                    if next_probe_at and datetime.now() < next_probe_at:
                        continue
                    lock = self._locks.get(building)
                    if lock is None or lock.locked():
                        continue
                    async with lock:
                        current = self._slot_snapshot(building)
                        if not bool(current.get("suspended", False)):
                            continue
                        try:
                            page = await self._ensure_page(building)
                            await self._raise_if_suspended(building, page)
                            if not bool(self._slot_snapshot(building).get("suspended", False)):
                                continue
                        except BuildingPausedError:
                            pass
                        await self._attempt_building_recovery(
                            building,
                            base_reason=str(current.get("suspend_reason", "") or current.get("pending_issue_summary", "") or current.get("login_error", "") or ""),
                            failure_kind=str(current.get("failure_kind", "") or "browser_issue").strip() or "browser_issue",
                            from_probe=True,
                        )
        except asyncio.CancelledError:
            return

    async def _async_prelogin_building(self, building: str) -> None:
        lock = self._locks.get(building)
        if lock is None:
            return
        async with lock:
            page = await self._ensure_page(building)
            try:
                await self._ensure_logged_in(building, page)
            except Exception as exc:  # noqa: BLE001
                failure_kind = self._classify_failure_kind(exc, login_state=self._slot_login_state(building))
                if failure_kind != "unknown":
                    await self._attempt_building_recovery(
                        building,
                        base_reason=str(exc),
                        failure_kind=failure_kind or "browser_issue",
                        from_probe=False,
                    )
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
        alarm_page = slot.get("alarm_page")
        context = slot.get("context")
        browser = slot.get("browser")
        try:
            if alarm_page is not None and alarm_page is not page and not alarm_page.is_closed():
                await alarm_page.close()
        except Exception:
            pass
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
            suspended=False,
            suspend_reason="",
            failure_kind="",
            recovery_attempts=0,
            last_failure_at="",
            next_probe_at="",
            pending_issue_summary="",
            slot_created_at="",
            last_recycled_at="",
            jobs_since_recycle=0,
            pending_recycle=False,
        )

    async def _create_or_replace_slot(self, building: str) -> Page:
        if self._playwright is None:
            raise RuntimeError("内网下载浏览器池未初始化")
        await self._close_slot(building)
        recycled_at = _now_text()
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
            "alarm_page": None,
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
            suspended=False,
            suspend_reason="",
            failure_kind="",
            recovery_attempts=0,
            last_failure_at="",
            next_probe_at="",
            pending_issue_summary="",
            slot_created_at=recycled_at,
            last_recycled_at=recycled_at,
            jobs_since_recycle=0,
            pending_recycle=False,
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

    async def _ensure_alarm_page(self, building: str) -> Page:
        slot = self._browser_slots.get(building)
        if not slot:
            await self._create_or_replace_slot(building)
            slot = self._browser_slots.get(building)
        if not slot:
            raise RuntimeError(f"{building} 浏览器上下文未初始化")
        context = slot.get("context")
        if context is None:
            await self._create_or_replace_slot(building)
            slot = self._browser_slots.get(building)
            context = slot.get("context") if isinstance(slot, dict) else None
        if context is None:
            raise RuntimeError(f"{building} 浏览器上下文未初始化")
        alarm_page = slot.get("alarm_page")
        try:
            alarm_page_closed = True if alarm_page is None else alarm_page.is_closed()
        except Exception:
            alarm_page_closed = True
        if alarm_page_closed:
            alarm_page = await context.new_page()
            slot["alarm_page"] = alarm_page
        site = self._find_site(building)
        target_url = self._resolve_alarm_page_url(site or {})
        if not target_url:
            raise RuntimeError(f"{building} 告警页面地址未配置")
        await prepare_reusable_page(
            alarm_page,
            target_url=target_url,
            refresh_timeout_ms=self._resolve_refresh_timeout_ms(),
        )
        return alarm_page

    async def _async_start(self) -> None:
        configure_playwright_environment(self.runtime_config)
        self._playwright = await async_playwright().start()
        self._locks = {building: asyncio.Lock() for building in self.BUILDINGS}
        for building in self.BUILDINGS:
            await self._create_or_replace_slot(building)
        self._recovery_probe_task = asyncio.create_task(self._async_recovery_probe_loop())

    async def _async_stop(self) -> None:
        if self._recovery_probe_task is not None:
            self._recovery_probe_task.cancel()
            try:
                await self._recovery_probe_task
            except Exception:
                pass
            self._recovery_probe_task = None
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
            self._mark_slot_recycle_pending_if_needed(building)
            await self._recycle_slot_if_needed(building)
            last_exc: Exception | None = None
            for attempt in range(2):
                page = await self._ensure_page(building)
                await self._fail_if_login_not_ready(building, page)
                try:
                    await self._ensure_logged_in(building, page)
                except Exception as exc:  # noqa: BLE001
                    failure_kind = self._classify_failure_kind(exc, login_state=self._slot_login_state(building))
                    if failure_kind == "unknown":
                        raise
                    recovered = await self._attempt_building_recovery(
                        building,
                        base_reason=str(exc),
                        failure_kind=failure_kind or "browser_issue",
                        from_probe=False,
                    )
                    if not recovered:
                        pause_info = self.get_building_pause_info(building)
                        raise BuildingPausedError(
                            building,
                            str(pause_info.get("suspend_reason", "") or str(exc)).strip(),
                            failure_kind=str(pause_info.get("failure_kind", "") or failure_kind).strip() or "browser_issue",
                        ) from exc
                    last_exc = exc if isinstance(exc, Exception) else RuntimeError(str(exc))
                    continue
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
                        jobs_since_recycle=int(self._slot_snapshot(building).get("jobs_since_recycle", 0) or 0) + 1,
                    )
                    self._mark_slot_recycle_pending_if_needed(building)
                    await self._recycle_slot_if_needed(building)
                    self._log(f"[共享桥接] 楼栋浏览器复用成功: {building}")
                    return result
                except Exception as exc:  # noqa: BLE001
                    error_text = str(exc)
                    self._last_error = error_text
                    failure_kind = self._classify_failure_kind(error_text)
                    if failure_kind != "unknown":
                        recovered = await self._attempt_building_recovery(
                            building,
                            base_reason=error_text,
                            failure_kind=failure_kind or "browser_issue",
                            from_probe=False,
                        )
                        if recovered and attempt == 0:
                            last_exc = exc if isinstance(exc, Exception) else RuntimeError(error_text)
                            continue
                        if not recovered:
                            pause_info = self.get_building_pause_info(building)
                            self._update_slot(
                                building,
                                in_use=False,
                                last_used_at=_now_text(),
                                last_result="failed",
                                last_error=str(pause_info.get("suspend_reason", "") or error_text).strip(),
                            )
                            raise BuildingPausedError(
                                building,
                                str(pause_info.get("suspend_reason", "") or error_text).strip(),
                                failure_kind=str(pause_info.get("failure_kind", "") or failure_kind).strip() or "browser_issue",
                            ) from exc
                    self._update_slot(
                        building,
                        in_use=False,
                        last_used_at=_now_text(),
                        last_result="failed",
                        last_error=error_text,
                    )
                    raise
            if last_exc is not None:
                raise last_exc
            raise RuntimeError(f"{building} 下载未能开始")

    async def _run_building_alarm_job(
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
            self._mark_slot_recycle_pending_if_needed(building)
            await self._recycle_slot_if_needed(building)
            last_exc: Exception | None = None
            for attempt in range(2):
                page = await self._ensure_page(building)
                await self._fail_if_login_not_ready(building, page)
                try:
                    await self._ensure_logged_in(building, page)
                except Exception as exc:  # noqa: BLE001
                    failure_kind = self._classify_failure_kind(exc, login_state=self._slot_login_state(building))
                    if failure_kind == "unknown":
                        raise
                    recovered = await self._attempt_building_recovery(
                        building,
                        base_reason=str(exc),
                        failure_kind=failure_kind or "browser_issue",
                        from_probe=False,
                    )
                    if not recovered:
                        pause_info = self.get_building_pause_info(building)
                        raise BuildingPausedError(
                            building,
                            str(pause_info.get("suspend_reason", "") or str(exc)).strip(),
                            failure_kind=str(pause_info.get("failure_kind", "") or failure_kind).strip() or "browser_issue",
                        ) from exc
                    last_exc = exc if isinstance(exc, Exception) else RuntimeError(str(exc))
                    continue
                self._update_slot(
                    building,
                    in_use=True,
                    last_used_at=_now_text(),
                    last_result="running",
                    last_error="",
                )
                try:
                    alarm_page = await self._ensure_alarm_page(building)
                    result = await runner(alarm_page)
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
                        jobs_since_recycle=int(self._slot_snapshot(building).get("jobs_since_recycle", 0) or 0) + 1,
                    )
                    self._mark_slot_recycle_pending_if_needed(building)
                    await self._recycle_slot_if_needed(building)
                    return result
                except Exception as exc:  # noqa: BLE001
                    error_text = str(exc)
                    self._last_error = error_text
                    failure_kind = self._classify_failure_kind(error_text)
                    if failure_kind == "unknown":
                        failure_kind = "alarm_query_failed"
                    recovered = await self._attempt_building_recovery(
                        building,
                        base_reason=error_text,
                        failure_kind=failure_kind or "browser_issue",
                        from_probe=False,
                    )
                    if recovered and attempt == 0:
                        last_exc = exc if isinstance(exc, Exception) else RuntimeError(error_text)
                        continue
                    if not recovered:
                        pause_info = self.get_building_pause_info(building)
                        self._update_slot(
                            building,
                            in_use=False,
                            last_used_at=_now_text(),
                            last_result="failed",
                            last_error=str(pause_info.get("suspend_reason", "") or error_text).strip(),
                        )
                        raise BuildingPausedError(
                            building,
                            str(pause_info.get("suspend_reason", "") or error_text).strip(),
                            failure_kind=str(pause_info.get("failure_kind", "") or failure_kind).strip() or "browser_issue",
                        ) from exc
                    self._update_slot(
                        building,
                        in_use=False,
                        last_used_at=_now_text(),
                        last_result="failed",
                        last_error=error_text,
                    )
                    raise
            if last_exc is not None:
                raise last_exc
            raise RuntimeError(f"{building} 告警信息导出未能开始")

    async def _run_existing_building_alarm_page_job(
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
            slot = self._browser_slots.get(building)
            if not isinstance(slot, dict):
                raise RuntimeError(f"{building} 浏览器上下文未初始化")
            alarm_page = slot.get("alarm_page")
            try:
                alarm_page_closed = True if alarm_page is None else alarm_page.is_closed()
            except Exception:
                alarm_page_closed = True
            if alarm_page_closed:
                raise RuntimeError(f"{building} 当前告警页面未打开，调试不会新开页")
            await self._fail_if_login_not_ready(building, alarm_page)
            self._update_slot(
                building,
                in_use=True,
                last_used_at=_now_text(),
                last_result="running",
                last_error="",
            )
            try:
                result = await runner(alarm_page)
                self._update_slot(
                    building,
                    in_use=False,
                    last_used_at=_now_text(),
                    last_result="success",
                    last_error="",
                )
                return result
            except Exception as exc:  # noqa: BLE001
                error_text = str(exc)
                self._last_error = error_text
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

    def submit_building_alarm_job(
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
            self._run_building_alarm_job(building, runner),
            loop,
        )

    def submit_existing_building_alarm_page_job(
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
            self._run_existing_building_alarm_page_job(building, runner),
            loop,
        )

    def get_health_snapshot(self) -> Dict[str, Any]:
        with self._state_lock:
            page_slots = [copy.deepcopy(self._slot_state[building]) for building in self.BUILDINGS]
        for slot in page_slots:
            created_at = _parse_text_datetime(slot.get("slot_created_at"))
            slot["slot_age_sec"] = (
                max(0, int((datetime.now() - created_at).total_seconds()))
                if created_at is not None
                else 0
            )
            slot["jobs_since_recycle"] = int(slot.get("jobs_since_recycle", 0) or 0)
            slot["pending_recycle"] = bool(slot.get("pending_recycle", False))
        active_buildings = [slot["building"] for slot in page_slots if bool(slot.get("in_use", False))]
        return {
            "enabled": True,
            "browser_ready": self.is_running(),
            "page_slots": page_slots,
            "active_buildings": active_buildings,
            "last_error": self._last_error,
        }
