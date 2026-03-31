from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

from app.shared.utils.atomic_file import atomic_write_bytes, validate_image_file
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from handover_log_module.service.handover_daily_report_asset_service import HandoverDailyReportAssetService
from handover_log_module.service.handover_daily_report_state_service import HandoverDailyReportStateService


_LOGIN_BROWSER_GUARD = threading.Lock()
_LOGIN_BROWSER_THREAD: threading.Thread | None = None
_SYSTEM_BROWSER_PROCESS: subprocess.Popen | None = None


class HandoverDailyReportScreenshotService:
    DEFAULT_EXTERNAL_PAGE_URL = "https://vnet.feishu.cn/app/LTjUbmZsTaTFIVsuQSLcUi4Onf4?pageId=pgecZCUXaEtvP9Yl"
    DEFAULT_SUMMARY_PAGE_URL = "https://vnet.feishu.cn/app/LTjUbmZsTaTFIVsuQSLcUi4Onf4?pageId=pgeZUMIpMDuIIfLA"
    DEFAULT_DEBUG_PORT = 29333

    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}
        self._state_service = HandoverDailyReportStateService(self.handover_cfg)
        self._asset_service = HandoverDailyReportAssetService(self.handover_cfg)

    def _runtime_root(self) -> Path:
        return resolve_runtime_state_root(
            runtime_config={"paths": self.handover_cfg.get("_global_paths", {})},
            app_dir=Path(__file__).resolve().parents[2],
        )

    @staticmethod
    def _browser_catalog() -> List[Dict[str, str]]:
        return [
            {
                "browser_kind": "edge",
                "browser_label": "Microsoft Edge",
                "family": "chromium",
            },
            {
                "browser_kind": "chrome",
                "browser_label": "Google Chrome",
                "family": "chromium",
            },
        ]

    @staticmethod
    def _browser_kind(browser_meta: Dict[str, Any] | None) -> str:
        return str((browser_meta or {}).get("browser_kind", "") or "").strip().lower()

    @staticmethod
    def _browser_label(browser_meta: Dict[str, Any] | None) -> str:
        label = str((browser_meta or {}).get("browser_label", "") or "").strip()
        return label or "系统浏览器"

    def _profile_dir(self, browser_meta: Dict[str, Any] | None = None) -> Path:
        browser_kind = self._browser_kind(browser_meta)
        local_app_data = str(os.environ.get("LOCALAPPDATA", "") or "").strip()
        if browser_kind == "chrome":
            if local_app_data:
                return Path(local_app_data) / "Google" / "Chrome" / "User Data"
            return Path.home() / "AppData" / "Local" / "Google" / "Chrome" / "User Data"
        if local_app_data:
            return Path(local_app_data) / "Microsoft" / "Edge" / "User Data"
        return Path.home() / "AppData" / "Local" / "Microsoft" / "Edge" / "User Data"

    def _browser_executable_candidates(self, browser_kind: str) -> List[Path]:
        local_app_data = str(os.environ.get("LOCALAPPDATA", "") or "").strip()
        program_files_x86 = str(os.environ.get("ProgramFiles(x86)", "") or "").strip()
        program_files = str(os.environ.get("ProgramFiles", "") or "").strip()
        if str(browser_kind or "").strip().lower() == "chrome":
            return [
                Path(program_files) / "Google" / "Chrome" / "Application" / "chrome.exe",
                Path(program_files_x86) / "Google" / "Chrome" / "Application" / "chrome.exe",
                Path(local_app_data) / "Google" / "Chrome" / "Application" / "chrome.exe",
            ]
        return [
            Path(program_files_x86) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
            Path(program_files) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
            Path(local_app_data) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        ]

    def _build_browser_meta(self, browser_kind: str, *, executable_path: Path | str | None = None) -> Dict[str, str]:
        normalized_kind = str(browser_kind or "").strip().lower()
        label = "Google Chrome" if normalized_kind == "chrome" else "Microsoft Edge"
        executable = str(executable_path or "").strip()
        profile_dir = str(self._profile_dir({"browser_kind": normalized_kind}))
        return {
            "browser_kind": normalized_kind,
            "browser_label": label,
            "executable_path": executable,
            "profile_dir": profile_dir,
            "family": "chromium",
        }

    def _resolve_browser_meta_by_kind(self, browser_kind: str) -> Dict[str, str] | None:
        normalized_kind = str(browser_kind or "").strip().lower()
        for path in self._browser_executable_candidates(normalized_kind):
            if str(path).strip() and path.exists():
                return self._build_browser_meta(normalized_kind, executable_path=path)
        return None

    def _browser_meta_from_debug_payload(self, payload: Dict[str, Any] | None) -> Dict[str, str] | None:
        if not isinstance(payload, dict):
            return None
        browser_text = str(payload.get("Browser", "") or "").strip().lower()
        if "edge" in browser_text or "edg/" in browser_text:
            return self._resolve_browser_meta_by_kind("edge") or self._build_browser_meta("edge")
        if "chrome" in browser_text:
            return self._resolve_browser_meta_by_kind("chrome") or self._build_browser_meta("chrome")
        return None

    def _resolve_system_browser(self, *, prefer_running_debug: bool = True) -> Dict[str, str] | None:
        if prefer_running_debug:
            running_meta = self._browser_meta_from_debug_payload(self._probe_debug_endpoint())
            if running_meta is not None:
                return running_meta
        for item in self._browser_catalog():
            meta = self._resolve_browser_meta_by_kind(item["browser_kind"])
            if meta is not None:
                return meta
        return None

    def _browser_state_fields(self, browser_meta: Dict[str, Any] | None) -> Dict[str, str]:
        if not isinstance(browser_meta, dict):
            return {
                "browser_kind": "",
                "browser_label": "",
                "browser_executable": "",
            }
        return {
            "browser_kind": self._browser_kind(browser_meta),
            "browser_label": self._browser_label(browser_meta),
            "browser_executable": str(browser_meta.get("executable_path", "") or "").strip(),
        }

    def _auth_state_payload(
        self,
        *,
        status: str,
        error: str,
        browser_meta: Dict[str, Any] | None,
        last_checked_at: str | None = None,
    ) -> Dict[str, Any]:
        profile_dir = ""
        if isinstance(browser_meta, dict):
            profile_dir = str(browser_meta.get("profile_dir", "") or "").strip()
            if not profile_dir:
                profile_dir = str(self._profile_dir(browser_meta))
        return {
            "status": str(status or "").strip(),
            "profile_dir": profile_dir,
            "last_checked_at": str(last_checked_at or self._now_text()).strip(),
            "error": str(error or "").strip(),
            **self._browser_state_fields(browser_meta),
        }

    def _debug_port(self) -> int:
        raw = self.handover_cfg.get("daily_report_bitable_export", {})
        if not isinstance(raw, dict):
            raw = {}
        try:
            value = int(raw.get("browser_debug_port", self.DEFAULT_DEBUG_PORT) or self.DEFAULT_DEBUG_PORT)
        except Exception:
            value = self.DEFAULT_DEBUG_PORT
        return value if value > 0 else self.DEFAULT_DEBUG_PORT

    def _debug_base_url(self) -> str:
        return f"http://127.0.0.1:{self._debug_port()}"

    def _debug_version_url(self) -> str:
        return f"{self._debug_base_url()}/json/version"

    @staticmethod
    def _now_text() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def _cfg(self) -> Dict[str, Any]:
        return {
            "summary_page_url": self.DEFAULT_SUMMARY_PAGE_URL,
            "external_page_url": self.DEFAULT_EXTERNAL_PAGE_URL,
        }

    def _probe_debug_endpoint(self) -> Dict[str, Any] | None:
        try:
            with urlopen(self._debug_version_url(), timeout=1.5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def _wait_for_debug_endpoint(self, timeout_sec: float = 15.0) -> bool:
        deadline = time.time() + max(1.0, float(timeout_sec))
        while time.time() < deadline:
            if self._probe_debug_endpoint():
                return True
            time.sleep(0.5)
        return False

    @staticmethod
    def _async_playwright_context():
        from playwright.async_api import async_playwright

        return async_playwright()

    @staticmethod
    def _run_async_fn(async_fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        def _invoke() -> Any:
            return asyncio.run(async_fn(*args, **kwargs))

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return _invoke()

        result: Dict[str, Any] = {}
        error: Dict[str, BaseException] = {}

        def _worker() -> None:
            try:
                result["value"] = _invoke()
            except BaseException as exc:  # noqa: BLE001
                error["value"] = exc

        thread = threading.Thread(target=_worker, name="handover-daily-report-async-runner", daemon=True)
        thread.start()
        thread.join()
        if "value" in error:
            raise error["value"]
        return result.get("value")

    def _start_system_browser(self, *, url: str, emit_log: Callable[[str], None]) -> tuple[bool, str]:
        browser_meta = self._resolve_system_browser(prefer_running_debug=False)
        if browser_meta is None:
            return False, "browser_unavailable: 未找到系统 Edge 或 Chrome 浏览器"
        executable = str(browser_meta.get("executable_path", "") or "").strip()
        if not executable:
            return False, "browser_unavailable: 未找到系统 Edge 或 Chrome 浏览器"
        profile_dir_text = str(browser_meta.get("profile_dir", "") or "").strip() or str(self._profile_dir(browser_meta))
        command = [
            executable,
            f"--remote-debugging-port={self._debug_port()}",
            "--new-window",
            "--profile-directory=Default",
        ]
        if profile_dir_text:
            command.append(f"--user-data-dir={profile_dir_text}")
        command.append(str(url or self._probe_url()).strip() or self._probe_url())
        global _SYSTEM_BROWSER_PROCESS
        try:
            _SYSTEM_BROWSER_PROCESS = subprocess.Popen(command)
        except Exception as exc:  # noqa: BLE001
            return False, f"browser_unavailable: {exc}"
        if self._wait_for_debug_endpoint():
            emit_log(
                f"[交接班][日报截图登录] 已接管系统浏览器调试端口 "
                f"browser={self._browser_label(browser_meta)}, port={self._debug_port()}, profile={profile_dir_text}"
            )
            return True, ""
        return False, f"browser_debug_port_unavailable: 请先关闭所有 {self._browser_label(browser_meta)} 窗口后重试"

    def open_target_page_in_system_browser(self, target_url: str) -> tuple[bool, str]:
        browser_meta = self._resolve_system_browser()
        if browser_meta is None:
            return False, "browser_unavailable: 未找到系统 Edge 或 Chrome 浏览器"
        executable = str(browser_meta.get("executable_path", "") or "").strip()
        if not executable:
            return False, "browser_unavailable: 未找到系统 Edge 或 Chrome 浏览器"
        profile_dir_text = str(browser_meta.get("profile_dir", "") or "").strip() or str(self._profile_dir(browser_meta))
        command = [
            executable,
            "--profile-directory=Default",
        ]
        if profile_dir_text:
            command.append(f"--user-data-dir={profile_dir_text}")
        command.append(str(target_url or self._probe_url()).strip() or self._probe_url())
        try:
            subprocess.Popen(command)
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
        return True, ""

    def ensure_browser_debug_ready(
        self,
        *,
        startup_url: str,
        emit_log: Callable[[str], None],
    ) -> tuple[bool, str, str]:
        if self._probe_debug_endpoint() is not None:
            return True, "", "reused"
        ok, error = self._start_system_browser(url=startup_url, emit_log=emit_log)
        if not ok:
            return False, error, ""
        return True, "", "opened_browser_startup"

    def _connect_browser(self, playwright, *, ensure_started: bool, open_url: str, emit_log: Callable[[str], None]):
        if self._probe_debug_endpoint() is None:
            if not ensure_started:
                raise RuntimeError("browser_debug_port_unavailable")
            ok, error = self._start_system_browser(url=open_url, emit_log=emit_log)
            if not ok:
                raise RuntimeError(error)
        return playwright.chromium.connect_over_cdp(self._debug_base_url(), timeout=10000)

    async def _connect_browser_async(
        self,
        playwright,
        *,
        ensure_started: bool,
        open_url: str,
        emit_log: Callable[[str], None],
    ):
        if self._probe_debug_endpoint() is None:
            if not ensure_started:
                raise RuntimeError("browser_debug_port_unavailable")
            ok, error = await asyncio.to_thread(self._start_system_browser, url=open_url, emit_log=emit_log)
            if not ok:
                raise RuntimeError(error)
        return await playwright.chromium.connect_over_cdp(self._debug_base_url(), timeout=10000)

    @staticmethod
    def _iter_browser_contexts(browser) -> List[Any]:
        contexts = list(getattr(browser, "contexts", []) or [])
        result: List[Any] = []
        for context in contexts:
            if context is None:
                continue
            result.append(context)
        return result

    def _resolve_browser_context(self, browser):
        contexts = self._iter_browser_contexts(browser)
        if not contexts:
            try:
                return browser.new_context()
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError("browser_context_missing") from exc
        for context in contexts:
            if self._iter_open_pages(context):
                return context
        return contexts[0]

    @staticmethod
    def _iter_open_pages(context) -> List[Any]:
        pages = list(getattr(context, "pages", []) or [])
        result: List[Any] = []
        for page in pages:
            try:
                if page.is_closed():
                    continue
            except Exception:  # noqa: BLE001
                continue
            result.append(page)
        return result

    @staticmethod
    def _looks_like_login_page(page) -> bool:
        url = str(page.url or "").strip().lower()
        if any(token in url for token in ("passport", "login", "signin", "accounts")):
            return True
        try:
            text = str(page.locator("body").inner_text(timeout=1500) or "")
        except Exception:  # noqa: BLE001
            return False
        lowered = text.lower()
        return any(token in lowered for token in ("扫码", "登录", "sign in", "qr code"))


    @staticmethod
    async def _looks_like_login_page_async(page) -> bool:
        url = str(page.url or "").strip().lower()
        if any(token in url for token in ("passport", "login", "signin", "accounts")):
            return True
        try:
            text = str(await page.locator("body").inner_text(timeout=1500) or "")
        except Exception:  # noqa: BLE001
            return False
        lowered = text.lower()
        return any(token in lowered for token in ("扫码", "登录", "sign in", "qr code"))

    def _probe_url(self) -> str:
        return self._cfg()["external_page_url"]

    @staticmethod
    def _capture_failure_result(
        *,
        stage: str,
        error: str,
        error_detail: str = "",
        status: str = "failed",
        error_message: str = "",
        resolved_url: str = "",
        resolved_page_id: str = "",
        matched_mode: str = "",
    ) -> Dict[str, Any]:
        return {
            "status": str(status or "failed").strip().lower() or "failed",
            "stage": str(stage or "").strip().lower(),
            "error": str(error or "").strip(),
            "error_detail": str(error_detail or "").strip(),
            "error_message": str(error_message or "").strip(),
            "path": "",
            "resolved_url": str(resolved_url or "").strip(),
            "resolved_page_id": str(resolved_page_id or "").strip(),
            "matched_mode": str(matched_mode or "").strip().lower(),
        }

    @staticmethod
    def _capture_success_result(
        path: Path,
        *,
        resolved_url: str = "",
        resolved_page_id: str = "",
        matched_mode: str = "",
    ) -> Dict[str, Any]:
        return {
            "status": "ok",
            "stage": "capture",
            "error": "",
            "error_detail": "",
            "error_message": "",
            "path": str(path),
            "resolved_url": str(resolved_url or "").strip(),
            "resolved_page_id": str(resolved_page_id or "").strip(),
            "matched_mode": str(matched_mode or "").strip().lower(),
        }

    @staticmethod
    def _capture_error_message(error: str, *, fallback: str = "", browser_label: str = "") -> str:
        error_code = str(error or "").strip()
        fallback_text = str(fallback or "").strip()
        resolved_browser_label = str(browser_label or "").strip() or "系统浏览器"
        if error_code == "target_page_not_open":
            return f"目标网页当前没有在{resolved_browser_label}中打开，请先打开对应页面后再重试。"
        if error_code == "summary_sheet_not_found":
            return "未找到今日航图页面，请确认当前飞书页面已打开且内容可见。"
        if error_code == "target_page_mismatch":
            return "当前打开页面与目标页面不一致，请重新打开对应飞书页面后重试。"
        if error_code == "login_required":
            return f"当前{resolved_browser_label}中的飞书登录态未就绪，请先完成扫码登录。"
        if error_code == "browser_unavailable":
            if "未找到系统 Edge 或 Chrome 浏览器" in fallback_text:
                return "未找到可用系统浏览器（Edge/Chrome），请安装 Microsoft Edge 或 Google Chrome。"
            if "browser_debug_port_unavailable" in fallback_text:
                return f"请先关闭所有 {resolved_browser_label} 窗口后，再重试。"
            return f"当前无法接管{resolved_browser_label}，请检查浏览器与调试端口后重试。"
        if error_code == "capture_dom_unavailable":
            return "截图页面当前不可用，请稍后重试。"
        if error_code == "timeout":
            return "截图操作超时，请查看系统错误日志后重试。"
        return fallback_text or "截图失败，请查看系统错误日志。"

    @staticmethod
    def _classify_capture_exception(exc: Exception) -> str:
        text = str(exc or "").strip().lower()
        if "timeout" in text or "timed out" in text:
            return "timeout"
        return "capture_dom_unavailable"

    @staticmethod
    def _target_label(target: str) -> str:
        target_text = str(target or "").strip().lower()
        if target_text == "summary_sheet":
            return "今日航图截图"
        if target_text == "external_page":
            return "排班截图"
        return target_text or "-"

    @staticmethod
    def _emit_capture_stage_log(
        emit_log: Callable[[str], None],
        *,
        duty_date: str,
        duty_shift: str,
        target: str,
        stage: str,
    ) -> None:
        emit_log(
            f"[交接班][日报截图] 阶段 batch={duty_date}|{duty_shift}, "
            f"target={target}, label={HandoverDailyReportScreenshotService._target_label(target)}, stage={stage}"
        )

    @staticmethod
    def _emit_capture_result_log(
        emit_log: Callable[[str], None],
        *,
        duty_date: str,
        duty_shift: str,
        target: str,
        result: Dict[str, Any],
    ) -> None:
        status = str(result.get("status", "")).strip().lower()
        if status == "ok":
            emit_log(
                f"[交接班][日报截图] 完成 batch={duty_date}|{duty_shift}, "
                f"target={target}, label={HandoverDailyReportScreenshotService._target_label(target)}, "
                f"file={result.get('path', '')}"
            )
            return
        emit_log(
            f"[交接班][日报截图] 失败 batch={duty_date}|{duty_shift}, "
            f"target={target}, label={HandoverDailyReportScreenshotService._target_label(target)}, "
            f"stage={str(result.get('stage', '')).strip() or '-'}, "
            f"status={status or '-'}, error={str(result.get('error_detail', '') or result.get('error', '')).strip()}"
        )

    @staticmethod
    def _emit_capture_page_match_log(
        emit_log: Callable[[str], None],
        *,
        duty_date: str,
        duty_shift: str,
        target: str,
        matched_mode: str,
        resolved_url: str,
        resolved_page_id: str,
    ) -> None:
        matched_mode_text = str(matched_mode or "").strip().lower() or "-"
        prefix = "[交接班][日报截图] 页面命中"
        if matched_mode_text == "opened_missing_target":
            prefix = "[交接班][日报截图] 页面缺失，已自动补开目标页"
        emit_log(
            f"{prefix} batch={duty_date}|{duty_shift}, "
            f"target={target}, label={HandoverDailyReportScreenshotService._target_label(target)}, "
            f"matched={matched_mode_text}, "
            f"current_url={str(resolved_url or '').strip() or '-'}, "
            f"current_page_id={str(resolved_page_id or '').strip() or '-'}"
        )

    def _auth_state_from_existing_pages(self, browser) -> Dict[str, str]:
        saw_any_page = False
        saw_feishu_page = False
        for context in reversed(self._iter_browser_contexts(browser)):
            pages = self._iter_open_pages(context)
            if pages:
                saw_any_page = True
            for page in reversed(pages):
                url = str(getattr(page, "url", "") or "").strip().lower()
                if not url:
                    continue
                if "feishu.cn" not in url and "larksuite.com" not in url:
                    continue
                saw_feishu_page = True
                if self._looks_like_login_page(page):
                    continue
                return {"status": "ready", "error": ""}
        if saw_feishu_page:
            return {"status": "missing_login", "error": "login_required"}
        if saw_any_page:
            return {"status": "missing_login", "error": "feishu_page_not_open"}
        return {"status": "missing_login", "error": "browser_started_without_pages"}

    async def _auth_state_from_existing_pages_async(self, browser) -> Dict[str, str]:
        saw_any_page = False
        saw_feishu_page = False
        for context in reversed(self._iter_browser_contexts(browser)):
            pages = self._iter_open_pages(context)
            if pages:
                saw_any_page = True
            for page in reversed(pages):
                url = str(getattr(page, "url", "") or "").strip().lower()
                if not url:
                    continue
                if "feishu.cn" not in url and "larksuite.com" not in url:
                    continue
                saw_feishu_page = True
                if await self._looks_like_login_page_async(page):
                    continue
                return {"status": "ready", "error": ""}
        if saw_feishu_page:
            return {"status": "missing_login", "error": "login_required"}
        if saw_any_page:
            return {"status": "missing_login", "error": "feishu_page_not_open"}
        return {"status": "missing_login", "error": "browser_started_without_pages"}

    @staticmethod
    def _normalize_url_for_match(url: str) -> str:
        text = str(url or "").strip()
        if not text:
            return ""
        return text.split("#", 1)[0]

    @staticmethod
    def _extract_feishu_app_page_identity(url: str) -> Dict[str, str]:
        normalized = HandoverDailyReportScreenshotService._normalize_url_for_match(url)
        if not normalized:
            return {
                "normalized_url": "",
                "app_id": "",
                "page_id": "",
                "path": "",
            }
        parsed = urlparse(normalized)
        path = str(parsed.path or "").strip()
        query = parse_qs(parsed.query or "", keep_blank_values=False)
        app_match = re.search(r"/app/([^/?#]+)", path)
        app_id = str(app_match.group(1)).strip() if app_match else ""
        page_id = str((query.get("pageId") or [""])[0]).strip()
        return {
            "normalized_url": normalized,
            "app_id": app_id,
            "page_id": page_id,
            "path": path,
        }

    @classmethod
    def _is_target_url_match(cls, *, current_url: str, target_url: str) -> bool:
        current = cls._extract_feishu_app_page_identity(current_url)
        target = cls._extract_feishu_app_page_identity(target_url)
        if not current["normalized_url"] or not target["normalized_url"]:
            return False
        if current["normalized_url"] == target["normalized_url"]:
            return True
        if target["page_id"]:
            if current["page_id"] != target["page_id"]:
                return False
            if target["app_id"] and current["app_id"] and current["app_id"] != target["app_id"]:
                return False
            return True
        return False

    @classmethod
    def _resolve_page_capture_meta(cls, page, *, target_url: str, matched_mode: str) -> Dict[str, str]:
        current_url = str(getattr(page, "url", "") or "").strip()
        current = cls._extract_feishu_app_page_identity(current_url)
        target = cls._extract_feishu_app_page_identity(target_url)
        return {
            "resolved_url": current["normalized_url"] or current_url,
            "resolved_page_id": current["page_id"],
            "matched_mode": str(matched_mode or "").strip().lower() or "reused",
            "target_page_id": target["page_id"],
        }

    @classmethod
    def _build_target_page_mismatch_detail(cls, *, target_url: str, page, matched_mode: str) -> str:
        meta = cls._resolve_page_capture_meta(page, target_url=target_url, matched_mode=matched_mode)
        return (
            f"target_page_id={meta.get('target_page_id', '') or '-'}, "
            f"resolved_page_id={meta.get('resolved_page_id', '') or '-'}, "
            f"resolved_url={meta.get('resolved_url', '') or '-'}, "
            f"matched_mode={meta.get('matched_mode', '') or '-'}"
        )

    def _find_matching_page(self, browser, *, target_url: str):
        normalized_target = self._normalize_url_for_match(target_url)
        if not normalized_target:
            return None
        for context in reversed(self._iter_browser_contexts(browser)):
            for page in reversed(self._iter_open_pages(context)):
                current_url = self._normalize_url_for_match(str(getattr(page, "url", "") or ""))
                if not current_url:
                    continue
                if self._is_target_url_match(current_url=current_url, target_url=normalized_target):
                    return page
        return None

    def ensure_target_page(
        self,
        browser,
        *,
        target_url: str,
        emit_log: Callable[[str], None],
        open_if_missing: bool = True,
    ):
        page = self._find_matching_page(browser, target_url=target_url)
        if page is not None:
            return page, "reused"
        if not open_if_missing:
            return None, ""
        context = self._resolve_browser_context(browser)
        page = context.new_page()
        page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        emit_log(
            "[交接班][日报截图登录] 目标页缺失，已自动补开 "
            f"url={str(target_url or '').strip() or '-'}"
        )
        return page, "opened_missing_target"

    async def _resolve_browser_context_async(self, browser):
        contexts = self._iter_browser_contexts(browser)
        if not contexts:
            try:
                return await browser.new_context()
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError("browser_context_missing") from exc
        for context in contexts:
            if self._iter_open_pages(context):
                return context
        return contexts[0]

    async def ensure_target_page_async(
        self,
        browser,
        *,
        target_url: str,
        emit_log: Callable[[str], None],
        open_if_missing: bool = True,
    ):
        page = self._find_matching_page(browser, target_url=target_url)
        if page is not None:
            return page, "reused"
        if not open_if_missing:
            return None, ""
        context = await self._resolve_browser_context_async(browser)
        page = await context.new_page()
        await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        emit_log(
            "[交接班][日报截图登录] 目标页缺失，已自动补开 "
            f"url={str(target_url or '').strip() or '-'}"
        )
        return page, "opened_missing_target"

    @staticmethod
    def _wait_for_page_stable(page, *, timeout_ms: int = 8000, stable_rounds: int = 1, interval_ms: int = 500) -> None:
        deadline = time.time() + max(1.0, timeout_ms / 1000.0)
        dom_timeout = max(1500, min(timeout_ms, 5000))
        idle_timeout = max(1200, min(timeout_ms // 2, 3000))
        try:
            page.wait_for_load_state("domcontentloaded", timeout=dom_timeout)
        except Exception:  # noqa: BLE001
            pass
        try:
            page.wait_for_load_state("networkidle", timeout=idle_timeout)
        except Exception:  # noqa: BLE001
            pass
        last_signature = None
        stable_count = 0
        while time.time() < deadline:
            try:
                signature = page.evaluate(
                    """
() => ({
  readyState: document.readyState || "",
  scrollHeight: Math.max(
    document.documentElement ? document.documentElement.scrollHeight || 0 : 0,
    document.body ? document.body.scrollHeight || 0 : 0
  ),
  scrollWidth: Math.max(
    document.documentElement ? document.documentElement.scrollWidth || 0 : 0,
    document.body ? document.body.scrollWidth || 0 : 0
  ),
  title: document.title || "",
  bodyTextLength: (document.body && document.body.innerText ? document.body.innerText.trim().length : 0)
})
"""
                )
            except Exception:  # noqa: BLE001
                time.sleep(interval_ms / 1000.0)
                continue
            ready_state = str(signature.get("readyState", "") or "").strip().lower()
            body_text_length = int(signature.get("bodyTextLength", 0) or 0)
            content_visible = body_text_length >= 80
            if signature == last_signature and (ready_state in {"interactive", "complete"} or content_visible):
                stable_count += 1
                if stable_count >= stable_rounds:
                    break
            else:
                stable_count = 0
                last_signature = signature
            time.sleep(interval_ms / 1000.0)
        try:
            page.wait_for_timeout(500)
        except Exception:  # noqa: BLE001
            pass

    @staticmethod
    async def _wait_for_page_stable_async(page, *, timeout_ms: int = 8000, stable_rounds: int = 1, interval_ms: int = 500) -> None:
        deadline = time.time() + max(1.0, timeout_ms / 1000.0)
        dom_timeout = max(1500, min(timeout_ms, 5000))
        idle_timeout = max(1200, min(timeout_ms // 2, 3000))
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=dom_timeout)
        except Exception:  # noqa: BLE001
            pass
        try:
            await page.wait_for_load_state("networkidle", timeout=idle_timeout)
        except Exception:  # noqa: BLE001
            pass
        last_signature = None
        stable_count = 0
        while time.time() < deadline:
            try:
                signature = await page.evaluate(
                    """
() => ({
  readyState: document.readyState || "",
  scrollHeight: Math.max(
    document.documentElement ? document.documentElement.scrollHeight || 0 : 0,
    document.body ? document.body.scrollHeight || 0 : 0
  ),
  scrollWidth: Math.max(
    document.documentElement ? document.documentElement.scrollWidth || 0 : 0,
    document.body ? document.body.scrollWidth || 0 : 0
  ),
  title: document.title || "",
  bodyTextLength: (document.body && document.body.innerText ? document.body.innerText.trim().length : 0)
})
"""
                )
            except Exception:  # noqa: BLE001
                await asyncio.sleep(interval_ms / 1000.0)
                continue
            ready_state = str(signature.get("readyState", "") or "").strip().lower()
            body_text_length = int(signature.get("bodyTextLength", 0) or 0)
            content_visible = body_text_length >= 80
            if signature == last_signature and (ready_state in {"interactive", "complete"} or content_visible):
                stable_count += 1
                if stable_count >= stable_rounds:
                    break
            else:
                stable_count = 0
                last_signature = signature
            await asyncio.sleep(interval_ms / 1000.0)
        try:
            await page.wait_for_timeout(500)
        except Exception:  # noqa: BLE001
            pass

    def check_auth_status(
        self,
        emit_log: Callable[[str], None] = print,
        *,
        ensure_browser_running: bool = False,
        startup_url: str = "",
    ) -> Dict[str, Any]:
        return self._run_async_fn(
            self.check_auth_status_async,
            emit_log=emit_log,
            ensure_browser_running=ensure_browser_running,
            startup_url=startup_url,
        )


    async def check_auth_status_async(
        self,
        emit_log: Callable[[str], None] = print,
        *,
        ensure_browser_running: bool = False,
        startup_url: str = "",
    ) -> Dict[str, Any]:
        browser_meta = self._resolve_system_browser(prefer_running_debug=True)
        if self._probe_debug_endpoint() is None and browser_meta is None:
            return self._state_service.update_screenshot_auth_state(
                self._auth_state_payload(
                    status="browser_unavailable",
                    error="browser_unavailable: 未找到系统 Edge 或 Chrome 浏览器",
                    browser_meta=None,
                )
            )

        if self._probe_debug_endpoint() is None and not ensure_browser_running:
            return self._state_service.update_screenshot_auth_state(
                self._auth_state_payload(
                    status="missing_login",
                    error="browser_not_started",
                    browser_meta=browser_meta,
                )
            )

        browser = None
        try:
            if ensure_browser_running:
                ok, error, _matched_mode = await asyncio.to_thread(
                    self.ensure_browser_debug_ready,
                    startup_url=str(startup_url or self._probe_url()).strip() or self._probe_url(),
                    emit_log=emit_log,
                )
                if not ok:
                    raise RuntimeError(error)
            async with self._async_playwright_context() as playwright:
                browser = await self._connect_browser_async(
                    playwright,
                    ensure_started=False,
                    open_url=str(startup_url or self._probe_url()).strip() or self._probe_url(),
                    emit_log=emit_log,
                )
                browser_meta = self._browser_meta_from_debug_payload(self._probe_debug_endpoint()) or browser_meta
                existing_state = await self._auth_state_from_existing_pages_async(browser)
                return self._state_service.update_screenshot_auth_state(
                    self._auth_state_payload(
                        status=existing_state["status"],
                        error=existing_state["error"],
                        browser_meta=browser_meta,
                    )
                )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            browser_meta = self._browser_meta_from_debug_payload(self._probe_debug_endpoint()) or browser_meta
            status = "browser_unavailable" if "browser_unavailable" in error_text or "browser_debug_port_unavailable" in error_text else "expired"
            if status == "expired":
                status = "missing_login" if "login_required" in error_text.lower() else "expired"
            if status != "missing_login":
                emit_log(f"[交接班][日报截图登录] 检测失败: {exc}")
            return self._state_service.update_screenshot_auth_state(
                self._auth_state_payload(
                    status=status,
                    error=error_text,
                    browser_meta=browser_meta,
                )
            )
        finally:
            browser = None

    def open_login_browser(self, emit_log: Callable[[str], None] = print) -> Dict[str, Any]:
        return self._run_async_fn(self.open_login_browser_async, emit_log=emit_log)


    async def open_login_browser_async(self, emit_log: Callable[[str], None] = print) -> Dict[str, Any]:
        browser_meta = self._resolve_system_browser(prefer_running_debug=True)
        profile_dir = str((browser_meta or {}).get("profile_dir", "") or "")
        global _LOGIN_BROWSER_THREAD
        with _LOGIN_BROWSER_GUARD:
            try:
                ok, error, browser_mode = await asyncio.to_thread(
                    self.ensure_browser_debug_ready,
                    startup_url=self._probe_url(),
                    emit_log=emit_log,
                )
                if not ok:
                    raise RuntimeError(error)
                browser_meta = self._browser_meta_from_debug_payload(self._probe_debug_endpoint()) or browser_meta
                profile_dir = str((browser_meta or {}).get("profile_dir", "") or "") or str(self._profile_dir(browser_meta))
                async with self._async_playwright_context() as playwright:
                    browser = await self._connect_browser_async(
                        playwright,
                        ensure_started=False,
                        open_url=self._probe_url(),
                        emit_log=emit_log,
                    )
                    page = self._find_matching_page(browser, target_url=self._probe_url())
                    matched_mode = "reused"
                    if page is None:
                        context = await self._resolve_browser_context_async(browser)
                        page = await context.new_page()
                        await page.goto(self._probe_url(), wait_until="domcontentloaded", timeout=30000)
                        matched_mode = "opened_missing_target"
                    if page is not None:
                        try:
                            await page.bring_to_front()
                        except Exception:  # noqa: BLE001
                            pass
                    effective_mode = matched_mode or browser_mode or "reused"
                    if effective_mode == "opened_missing_target":
                        emit_log(
                            f"[交接班][日报截图登录] 登录页缺失，已自动补开 browser={self._browser_label(browser_meta)}, profile={profile_dir}"
                        )
                    else:
                        emit_log(
                            f"[交接班][日报截图登录] 已复用系统浏览器登录页 browser={self._browser_label(browser_meta)}, profile={profile_dir}, matched={effective_mode}"
                        )
            except Exception as exc:  # noqa: BLE001
                error = str(exc)
                self._state_service.update_screenshot_auth_state(
                    self._auth_state_payload(
                        status="browser_unavailable",
                        error=error,
                        browser_meta=browser_meta,
                    )
                )
                return {
                    "ok": False,
                    "status": "failed",
                    "message": error,
                    "profile_dir": profile_dir,
                    **self._browser_state_fields(browser_meta),
                }
            if _LOGIN_BROWSER_THREAD is None or not _LOGIN_BROWSER_THREAD.is_alive():

                def _worker() -> None:
                    last_status = ""
                    last_error = ""
                    for _ in range(120):
                        state = self.check_auth_status(emit_log=emit_log, ensure_browser_running=False)
                        status = str(state.get("status", "") or "").strip().lower()
                        error = str(state.get("error", "") or "").strip()
                        if status and (status != last_status or error != last_error):
                            emit_log(f"[交接班][日报截图登录] 当前登录态 status={status}")
                            if status == "missing_login" and error in {
                                "browser_not_started",
                                "browser_started_without_pages",
                                "feishu_page_not_open",
                            }:
                                browser_label = self._browser_label(state)
                                emit_log(
                                    f"[交接班][日报截图登录] {browser_label} 登录页已关闭或未打开，请重新点击“初始化飞书截图登录态”"
                                )
                            last_status = status
                            last_error = error
                        if status == "ready":
                            emit_log("[交接班][日报截图登录] 登录态已就绪")
                            return
                        time.sleep(3)

                _LOGIN_BROWSER_THREAD = threading.Thread(target=_worker, daemon=True, name="handover-daily-report-login-monitor")
                _LOGIN_BROWSER_THREAD.start()
        return {
            "ok": True,
            "status": "opened",
            "message": f"已打开{self._browser_label(browser_meta)}登录页，请在系统浏览器中扫码登录飞书。",
            "profile_dir": profile_dir,
            **self._browser_state_fields(browser_meta),
        }

    async def capture_summary_sheet_async(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
        prefer_existing_page: bool = True,
        allow_open_fallback: bool = True,
    ) -> Dict[str, Any]:
        url = self._cfg()["summary_page_url"]
        emit_log(
            f"[交接班][日报截图] 开始 batch={duty_date}|{duty_shift}, "
            f"target=summary_sheet, label=今日航图截图, url={url}"
        )
        result = await self._capture_sheet_like_page_async(
            url=url,
            sheet_name="",
            duty_date=duty_date,
            duty_shift=duty_shift,
            target="summary_sheet",
            output_path=self._asset_service.get_summary_sheet_path(duty_date=duty_date, duty_shift=duty_shift),
            prefer_existing_page=prefer_existing_page,
            allow_open_fallback=allow_open_fallback,
            emit_log=emit_log,
        )
        self._emit_capture_result_log(
            emit_log,
            duty_date=duty_date,
            duty_shift=duty_shift,
            target="summary_sheet",
            result=result,
        )
        return result

    async def capture_external_page_async(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
        prefer_existing_page: bool = True,
        allow_open_fallback: bool = True,
    ) -> Dict[str, Any]:
        url = self._cfg()["external_page_url"]
        emit_log(
            f"[交接班][日报截图] 开始 batch={duty_date}|{duty_shift}, "
            f"target=external_page, label=排班截图, url={url}"
        )
        result = await self._capture_sheet_like_page_async(
            url=url,
            sheet_name="",
            duty_date=duty_date,
            duty_shift=duty_shift,
            target="external_page",
            output_path=self._asset_service.get_external_page_path(duty_date=duty_date, duty_shift=duty_shift),
            prefer_existing_page=prefer_existing_page,
            allow_open_fallback=allow_open_fallback,
            emit_log=emit_log,
        )
        self._emit_capture_result_log(
            emit_log,
            duty_date=duty_date,
            duty_shift=duty_shift,
            target="external_page",
            result=result,
        )
        return result

    def capture_summary_sheet(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
        prefer_existing_page: bool = True,
        allow_open_fallback: bool = True,
    ) -> Dict[str, Any]:
        return self._run_async_fn(
            self.capture_summary_sheet_async,
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
            prefer_existing_page=prefer_existing_page,
            allow_open_fallback=allow_open_fallback,
        )

    def capture_external_page(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
        prefer_existing_page: bool = True,
        allow_open_fallback: bool = True,
    ) -> Dict[str, Any]:
        return self._run_async_fn(
            self.capture_external_page_async,
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
            prefer_existing_page=prefer_existing_page,
            allow_open_fallback=allow_open_fallback,
        )

    def _capture_sheet_like_page(
        self,
        *,
        url: str,
        sheet_name: str,
        duty_date: str,
        duty_shift: str,
        target: str,
        output_path: Path,
        prefer_existing_page: bool = True,
        allow_open_fallback: bool = True,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        return self._run_async_fn(
            self._capture_sheet_like_page_async,
            url=url,
            sheet_name=sheet_name,
            duty_date=duty_date,
            duty_shift=duty_shift,
            target=target,
            output_path=output_path,
            prefer_existing_page=prefer_existing_page,
            allow_open_fallback=allow_open_fallback,
            emit_log=emit_log,
        )

    async def _capture_sheet_like_page_async(
        self,
        *,
        url: str,
        sheet_name: str,
        duty_date: str,
        duty_shift: str,
        target: str,
        output_path: Path,
        prefer_existing_page: bool = True,
        allow_open_fallback: bool = True,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        stage = "check_auth"
        self._emit_capture_stage_log(
            emit_log,
            duty_date=duty_date,
            duty_shift=duty_shift,
            target=target,
            stage=stage,
        )
        profile_state = await self.check_auth_status_async(
            emit_log=lambda *_args, **_kwargs: None,
            ensure_browser_running=True,
            startup_url=url,
        )
        browser_label = self._browser_label(profile_state)
        profile_status = str(profile_state.get("status", "")).strip().lower()
        if profile_status != "ready":
            return self._capture_failure_result(
                stage=stage,
                error="login_required",
                error_detail=str(profile_state.get("error", "") or "login_required"),
                error_message=self._capture_error_message("login_required", browser_label=browser_label),
            )

        page = None
        matched_mode = ""
        try:
            async with self._async_playwright_context() as playwright:
                stage = "connect_browser"
                self._emit_capture_stage_log(
                    emit_log,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    target=target,
                    stage=stage,
                )
                browser = await self._connect_browser_async(
                    playwright,
                    ensure_started=True,
                    open_url=url,
                    emit_log=lambda *_args, **_kwargs: None,
                )
                stage = "find_existing_page"
                self._emit_capture_stage_log(
                    emit_log,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    target=target,
                    stage=stage,
                )
                open_if_missing = allow_open_fallback
                if not prefer_existing_page and not allow_open_fallback:
                    open_if_missing = False
                page, matched_mode = await self.ensure_target_page_async(
                    browser,
                    target_url=url,
                    emit_log=lambda *_args, **_kwargs: None,
                    open_if_missing=open_if_missing,
                )
                if page is None:
                    return self._capture_failure_result(
                        stage=stage,
                        error="target_page_not_open",
                        error_detail="target_page_not_open",
                        error_message=self._capture_error_message("target_page_not_open", browser_label=browser_label),
                    )
                page_meta = self._resolve_page_capture_meta(page, target_url=url, matched_mode=matched_mode)
                self._emit_capture_page_match_log(
                    emit_log,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    target=target,
                    matched_mode=page_meta["matched_mode"],
                    resolved_url=page_meta["resolved_url"],
                    resolved_page_id=page_meta["resolved_page_id"],
                )
                if not self._is_target_url_match(current_url=page_meta["resolved_url"], target_url=url):
                    return self._capture_failure_result(
                        stage=stage,
                        error="target_page_mismatch",
                        error_detail=self._build_target_page_mismatch_detail(
                            target_url=url,
                            page=page,
                            matched_mode=page_meta["matched_mode"],
                        ),
                        error_message=self._capture_error_message("target_page_mismatch", browser_label=browser_label),
                        resolved_url=page_meta["resolved_url"],
                        resolved_page_id=page_meta["resolved_page_id"],
                        matched_mode=page_meta["matched_mode"],
                    )
                try:
                    await page.bring_to_front()
                except Exception:  # noqa: BLE001
                    pass
                stage = "wait_page_stable"
                self._emit_capture_stage_log(
                    emit_log,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    target=target,
                    stage=stage,
                )
                await self._wait_for_page_stable_async(page)
                if await self._looks_like_login_page_async(page):
                    running_browser_meta = self._browser_meta_from_debug_payload(self._probe_debug_endpoint()) or profile_state
                    self._state_service.update_screenshot_auth_state(
                        self._auth_state_payload(
                            status="missing_login",
                            error="login_required",
                            browser_meta=running_browser_meta,
                        )
                    )
                    return self._capture_failure_result(
                        stage=stage,
                        error="login_required",
                        error_detail="login_required",
                        error_message=self._capture_error_message("login_required", browser_label=self._browser_label(running_browser_meta)),
                        resolved_url=page_meta["resolved_url"],
                        resolved_page_id=page_meta["resolved_page_id"],
                        matched_mode=page_meta["matched_mode"],
                    )
                page_meta = self._resolve_page_capture_meta(page, target_url=url, matched_mode=page_meta["matched_mode"])
                if not self._is_target_url_match(current_url=page_meta["resolved_url"], target_url=url):
                    return self._capture_failure_result(
                        stage=stage,
                        error="target_page_mismatch",
                        error_detail=self._build_target_page_mismatch_detail(
                            target_url=url,
                            page=page,
                            matched_mode=page_meta["matched_mode"],
                        ),
                        error_message=self._capture_error_message("target_page_mismatch", browser_label=browser_label),
                        resolved_url=page_meta["resolved_url"],
                        resolved_page_id=page_meta["resolved_page_id"],
                        matched_mode=page_meta["matched_mode"],
                    )
                if sheet_name:
                    stage = "open_sheet"
                    self._emit_capture_stage_log(
                        emit_log,
                        duty_date=duty_date,
                        duty_shift=duty_shift,
                        target=target,
                        stage=stage,
                    )
                    try:
                        await page.get_by_text(sheet_name, exact=True).first.click(timeout=8000)
                        await self._wait_for_page_stable_async(page, timeout_ms=8000, stable_rounds=1, interval_ms=500)
                    except Exception as exc:  # noqa: BLE001
                        return self._capture_failure_result(
                            stage=stage,
                            error="summary_sheet_not_found",
                            error_detail=str(exc),
                            error_message=self._capture_error_message("summary_sheet_not_found", browser_label=browser_label),
                            resolved_url=page_meta["resolved_url"],
                            resolved_page_id=page_meta["resolved_page_id"],
                            matched_mode=page_meta["matched_mode"],
                        )
                stage = "capture"
                self._emit_capture_stage_log(
                    emit_log,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    target=target,
                    stage=stage,
                )
                content = await self._capture_page_with_fallback_async(
                    page,
                    prefer_document_full_page=(str(target or "").strip().lower() in {"summary_sheet", "external_page"}),
                )
                output_path.parent.mkdir(parents=True, exist_ok=True)
                atomic_write_bytes(output_path, content, validator=validate_image_file, temp_suffix=".tmp")
                running_browser_meta = self._browser_meta_from_debug_payload(self._probe_debug_endpoint()) or profile_state
                self._state_service.update_screenshot_auth_state(
                    self._auth_state_payload(
                        status="ready",
                        error="",
                        browser_meta=running_browser_meta,
                    )
                )
                return self._capture_success_result(
                    output_path,
                    resolved_url=page_meta["resolved_url"],
                    resolved_page_id=page_meta["resolved_page_id"],
                    matched_mode=page_meta["matched_mode"],
                )
        except Exception as exc:  # noqa: BLE001
            error_code = self._classify_capture_exception(exc)
            return self._capture_failure_result(
                stage=stage,
                error=error_code,
                error_detail=str(exc),
                error_message=self._capture_error_message(error_code, fallback=str(exc), browser_label=browser_label),
                matched_mode=matched_mode,
            )

    @staticmethod
    def _read_document_capture_metrics(page) -> Dict[str, int]:
        return page.evaluate(
            """
() => {
  const doc = document.documentElement || {};
  const body = document.body || {};
  const viewportHeight = window.innerHeight || doc.clientHeight || body.clientHeight || 0;
  const viewportWidth = window.innerWidth || doc.clientWidth || body.clientWidth || 0;
  return {
    scrollHeight: Math.max(doc.scrollHeight || 0, body.scrollHeight || 0, doc.clientHeight || 0, body.clientHeight || 0),
    scrollWidth: Math.max(doc.scrollWidth || 0, body.scrollWidth || 0, doc.clientWidth || 0, body.clientWidth || 0),
    viewportHeight,
    viewportWidth,
    bodyTextLength: body && body.innerText ? body.innerText.trim().length : 0,
  };
}
"""
        )

    @staticmethod
    async def _read_document_capture_metrics_async(page) -> Dict[str, int]:
        return await page.evaluate(
            """
() => {
  const doc = document.documentElement || {};
  const body = document.body || {};
  const viewportHeight = window.innerHeight || doc.clientHeight || body.clientHeight || 0;
  const viewportWidth = window.innerWidth || doc.clientWidth || body.clientWidth || 0;
  return {
    scrollHeight: Math.max(doc.scrollHeight || 0, body.scrollHeight || 0, doc.clientHeight || 0, body.clientHeight || 0),
    scrollWidth: Math.max(doc.scrollWidth || 0, body.scrollWidth || 0, doc.clientWidth || 0, body.clientWidth || 0),
    viewportHeight,
    viewportWidth,
    bodyTextLength: body && body.innerText ? body.innerText.trim().length : 0,
  };
}
"""
        )

    def _capture_page_with_fallback(self, page, *, prefer_document_full_page: bool = False) -> bytes:
        if prefer_document_full_page:
            try:
                metrics = self._read_document_capture_metrics(page)
                scroll_height = int(metrics.get("scrollHeight", 0) or 0)
                viewport_height = int(metrics.get("viewportHeight", 0) or 0)
                body_text_length = int(metrics.get("bodyTextLength", 0) or 0)
                if scroll_height > max(1200, viewport_height + 300) and body_text_length >= 80:
                    page.evaluate("() => { window.scrollTo(0, 0); }")
                    page.wait_for_timeout(200)
                    image = page.screenshot(full_page=True, type="png")
                    if image:
                        return image
            except Exception:  # noqa: BLE001
                pass
        locator = self._mark_primary_scrollable_locator(page)
        if locator is not None:
            try:
                # Prefer the page's primary scroll container so SPA pages are captured as a full long image,
                # not just the currently visible viewport.
                locator.evaluate("(el) => { el.scrollTop = 0; el.scrollLeft = 0; }")
                try:
                    page.evaluate("() => { window.scrollTo(0, 0); }")
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.2)
                return self._capture_scrollable_locator(locator)
            except Exception:  # noqa: BLE001
                pass
        try:
            try:
                page.evaluate("() => { window.scrollTo(0, 0); }")
                page.wait_for_timeout(200)
            except Exception:  # noqa: BLE001
                pass
            image = page.screenshot(full_page=True, type="png")
            if image:
                return image
        except Exception:  # noqa: BLE001
            pass
        return page.screenshot(full_page=False, type="png")

    async def _capture_page_with_fallback_async(self, page, *, prefer_document_full_page: bool = False) -> bytes:
        if prefer_document_full_page:
            try:
                metrics = await self._read_document_capture_metrics_async(page)
                scroll_height = int(metrics.get("scrollHeight", 0) or 0)
                viewport_height = int(metrics.get("viewportHeight", 0) or 0)
                body_text_length = int(metrics.get("bodyTextLength", 0) or 0)
                if scroll_height > max(1200, viewport_height + 300) and body_text_length >= 80:
                    await page.evaluate("() => { window.scrollTo(0, 0); }")
                    await page.wait_for_timeout(200)
                    image = await page.screenshot(full_page=True, type="png")
                    if image:
                        return image
            except Exception:  # noqa: BLE001
                pass
        locator = await self._mark_primary_scrollable_locator_async(page)
        if locator is not None:
            try:
                await locator.evaluate("(el) => { el.scrollTop = 0; el.scrollLeft = 0; }")
                try:
                    await page.evaluate("() => { window.scrollTo(0, 0); }")
                except Exception:  # noqa: BLE001
                    pass
                await asyncio.sleep(0.2)
                return await self._capture_scrollable_locator_async(locator)
            except Exception:  # noqa: BLE001
                pass
        try:
            try:
                await page.evaluate("() => { window.scrollTo(0, 0); }")
                await page.wait_for_timeout(200)
            except Exception:  # noqa: BLE001
                pass
            image = await page.screenshot(full_page=True, type="png")
            if image:
                return image
        except Exception:  # noqa: BLE001
            pass
        return await page.screenshot(full_page=False, type="png")

    @staticmethod
    def _mark_primary_scrollable_locator(page):
        script = """
() => {
  const marker = 'data-qjpt-scroll-capture';
  document.querySelectorAll('[' + marker + ']').forEach((el) => el.removeAttribute(marker));
  const isVisible = (el) => {
    const style = window.getComputedStyle(el);
    if (!style || style.visibility === 'hidden' || style.display === 'none') return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 400 && rect.height >= 300;
  };
  let best = null;
  let bestScore = 0;
  for (const el of Array.from(document.querySelectorAll('*'))) {
    if (!isVisible(el)) continue;
    const scrollHeight = el.scrollHeight || 0;
    const scrollWidth = el.scrollWidth || 0;
    const clientHeight = el.clientHeight || 0;
    const clientWidth = el.clientWidth || 0;
    if (scrollHeight <= clientHeight + 80 && scrollWidth <= clientWidth + 80) continue;
    const score = Math.max(scrollHeight, clientHeight) * Math.max(scrollWidth, clientWidth);
    if (score > bestScore) {
      best = el;
      bestScore = score;
    }
  }
  if (!best) return false;
  best.setAttribute(marker, '1');
  return true;
}
"""
        marked = page.evaluate(script)
        if not marked:
            return None
        return page.locator('[data-qjpt-scroll-capture="1"]').first

    @staticmethod
    async def _mark_primary_scrollable_locator_async(page):
        script = """
() => {
  const marker = 'data-qjpt-scroll-capture';
  document.querySelectorAll('[' + marker + ']').forEach((el) => el.removeAttribute(marker));
  const isVisible = (el) => {
    const style = window.getComputedStyle(el);
    if (!style || style.visibility === 'hidden' || style.display === 'none') return false;
    const rect = el.getBoundingClientRect();
    return rect.width >= 400 && rect.height >= 300;
  };
  let best = null;
  let bestScore = 0;
  for (const el of Array.from(document.querySelectorAll('*'))) {
    if (!isVisible(el)) continue;
    const scrollHeight = el.scrollHeight || 0;
    const scrollWidth = el.scrollWidth || 0;
    const clientHeight = el.clientHeight || 0;
    const clientWidth = el.clientWidth || 0;
    if (scrollHeight <= clientHeight + 80 && scrollWidth <= clientWidth + 80) continue;
    const score = Math.max(scrollHeight, clientHeight) * Math.max(scrollWidth, clientWidth);
    if (score > bestScore) {
      best = el;
      bestScore = score;
    }
  }
  if (!best) return false;
  best.setAttribute(marker, '1');
  return true;
}
"""
        marked = await page.evaluate(script)
        if not marked:
            return None
        return page.locator('[data-qjpt-scroll-capture="1"]').first

    @staticmethod
    def _capture_scrollable_locator(locator) -> bytes:
        from PIL import Image
        from io import BytesIO

        metrics = locator.evaluate(
            """
(el) => ({
  scrollWidth: Math.max(el.scrollWidth || 0, el.clientWidth || 0),
  scrollHeight: Math.max(el.scrollHeight || 0, el.clientHeight || 0),
  clientWidth: Math.max(el.clientWidth || 0, 1),
  clientHeight: Math.max(el.clientHeight || 0, 1)
})
"""
        )
        scroll_height = int(metrics.get("scrollHeight", 0) or 0)
        client_width = int(metrics.get("clientWidth", 0) or 1)
        client_height = int(metrics.get("clientHeight", 0) or 1)
        y_positions = list(range(0, max(scroll_height, 1), client_height)) or [0]

        tiles: List[tuple[int, Image.Image]] = []
        scale_y = 1.0
        for y in y_positions:
            locator.evaluate("(el, pos) => { el.scrollTop = pos.y; }", {"y": y})
            time.sleep(0.2)
            shot = locator.screenshot(type="png")
            image = Image.open(BytesIO(shot)).convert("RGBA")
            if not tiles:
                scale_y = image.height / float(client_height)
            tiles.append((y, image))

        canvas = Image.new(
            "RGBA",
            (max(1, image.width if tiles else client_width), max(1, int(round(scroll_height * scale_y)))),
            (255, 255, 255, 255),
        )
        for y, image in tiles:
            crop_height = int(round(min(client_height, scroll_height - y) * scale_y))
            tile = image.crop((0, 0, image.width, max(1, crop_height)))
            canvas.paste(tile, (0, int(round(y * scale_y))))
        buffer = BytesIO()
        canvas.save(buffer, format="PNG")
        return buffer.getvalue()

    @staticmethod
    async def _capture_scrollable_locator_async(locator) -> bytes:
        from PIL import Image
        from io import BytesIO

        metrics = await locator.evaluate(
            """
(el) => ({
  scrollWidth: Math.max(el.scrollWidth || 0, el.clientWidth || 0),
  scrollHeight: Math.max(el.scrollHeight || 0, el.clientHeight || 0),
  clientWidth: Math.max(el.clientWidth || 0, 1),
  clientHeight: Math.max(el.clientHeight || 0, 1)
})
"""
        )
        scroll_height = int(metrics.get("scrollHeight", 0) or 0)
        client_width = int(metrics.get("clientWidth", 0) or 1)
        client_height = int(metrics.get("clientHeight", 0) or 1)
        y_positions = list(range(0, max(scroll_height, 1), client_height)) or [0]

        tiles: List[tuple[int, Image.Image]] = []
        scale_y = 1.0
        for y in y_positions:
            await locator.evaluate("(el, pos) => { el.scrollTop = pos.y; }", {"y": y})
            await asyncio.sleep(0.2)
            shot = await locator.screenshot(type="png")
            image = Image.open(BytesIO(shot)).convert("RGBA")
            if not tiles:
                scale_y = image.height / float(client_height)
            tiles.append((y, image))

        canvas = Image.new(
            "RGBA",
            (max(1, image.width if tiles else client_width), max(1, int(round(scroll_height * scale_y)))),
            (255, 255, 255, 255),
        )
        for y, image in tiles:
            crop_height = int(round(min(client_height, scroll_height - y) * scale_y))
            tile = image.crop((0, 0, image.width, max(1, crop_height)))
            canvas.paste(tile, (0, int(round(y * scale_y))))
        buffer = BytesIO()
        canvas.save(buffer, format="PNG")
        return buffer.getvalue()
