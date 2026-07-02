from __future__ import annotations

import argparse
import asyncio
import json
import mimetypes
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from app.modules.alarm_rule_export.service.alarm_rule_export_service import (
    SiteConfig,
    _ensure_main_page_loaded,
    _extract_sites,
    _goto_with_retries,
    _resolve_browser_executable,
)
from app.shared.runtime.building_browser_locks import (
    acquire_building_browser_lock,
    release_building_browser_lock,
)
from app.shared.utils.atomic_file import atomic_write_text
from pipeline_utils import get_app_dir


DEFAULT_RUNTIME_ROOT = Path(".runtime")
DEFAULT_STATE_FILE = DEFAULT_RUNTIME_ROOT / "system_screenshot_capture" / "capture_records.json"
DEFAULT_SCREENSHOT_DIR_NAME = "系统截图源文件"
DEFAULT_SITE_BUILDING = "A楼"
DEFAULT_TARGETS: tuple[dict[str, str], ...] = (
    {
        "key": "power_distribution",
        "label": "供配电系统图",
        "span_id": "CBB7E9721D900001F67E8580D274179E",
        "text": "供电配电",
    },
    {
        "key": "hvac",
        "label": "暖通系统图",
        "span_id": "CBB7E9721D900001F2AF182011D110A1",
        "text": "暖通系统",
    },
    {
        "key": "fuel",
        "label": "燃油系统图",
        "span_id": "CBB7E9721D9000015C121B3067A0105D",
        "text": "燃油系统",
    },
    {
        "key": "generator",
        "label": "柴发系统图",
        "span_id": "CBB7E9721D8000019D742851B0D016C4",
        "text": "柴发系统",
    },
    {
        "key": "weak_current",
        "label": "弱电系统图",
        "span_id": "CBB7E9721D900001DE5A26BA6862EDC0",
        "text": "动环自监控系统",
    },
)


@dataclass(frozen=True)
class ScreenshotTarget:
    key: str
    label: str
    span_id: str
    text: str


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _feature_cfg(config: Dict[str, Any]) -> Dict[str, Any]:
    features = _dict(config.get("features"))
    if "system_screenshot_capture" in features:
        return _dict(features.get("system_screenshot_capture"))
    return _dict(config.get("system_screenshot_capture"))


def _paths_cfg(config: Dict[str, Any]) -> Dict[str, Any]:
    common = _dict(config.get("common"))
    paths = _dict(common.get("paths"))
    if paths:
        return paths
    return _dict(config.get("paths"))


def _shared_bridge_cfg(config: Dict[str, Any]) -> Dict[str, Any]:
    common = _dict(config.get("common"))
    shared = _dict(common.get("shared_bridge"))
    if shared:
        return shared
    return _dict(config.get("shared_bridge"))


def _runtime_root(config: Dict[str, Any]) -> Path:
    root_text = str(_paths_cfg(config).get("runtime_state_root", "") or "").strip()
    root = Path(root_text) if root_text else DEFAULT_RUNTIME_ROOT
    if not root.is_absolute():
        root = get_app_dir() / root
    return root


def _download_root(config: Dict[str, Any], configured: str | None = None) -> Path:
    if configured:
        return Path(configured)
    feature = _feature_cfg(config)
    configured_root = str(feature.get("download_root", "") or "").strip()
    if configured_root:
        return Path(configured_root)
    shared = _shared_bridge_cfg(config)
    shared_root = str(shared.get("root_dir") or shared.get("internal_root_dir") or "").strip()
    if shared_root:
        return Path(shared_root)
    paths = _paths_cfg(config)
    fallback = str(paths.get("business_root_dir") or paths.get("download_save_dir") or "").strip()
    return Path(fallback) if fallback else Path(r"D:\QLDownload")


def _resolve_path(config: Dict[str, Any], value: str | None, default_path: Path) -> Path:
    text = str(value or "").strip()
    path = Path(text) if text else default_path
    if path.is_absolute():
        return path
    return _runtime_root(config) / path


def _capture_date(value: str | None = None) -> str:
    text = str(value or "").strip()
    if text:
        if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", text):
            raise ValueError("capture_date 必须是 YYYY-MM-DD")
        return text
    return datetime.now().strftime("%Y-%m-%d")


def _safe_file_part(value: str) -> str:
    text = re.sub(r"[\\/:*?\"<>|\s]+", "_", str(value or "").strip())
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "系统截图"


def _normalize_targets(raw_targets: Any = None) -> list[ScreenshotTarget]:
    raw = raw_targets if isinstance(raw_targets, list) and raw_targets else list(DEFAULT_TARGETS)
    targets: list[ScreenshotTarget] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key", "") or "").strip()
        label = str(item.get("label", "") or "").strip()
        span_id = str(item.get("span_id", "") or "").strip()
        text = str(item.get("text", "") or "").strip()
        if not key or not label or (not span_id and not text) or key in seen:
            continue
        seen.add(key)
        targets.append(ScreenshotTarget(key=key, label=label, span_id=span_id, text=text))
    return targets


def _state_path(config: Dict[str, Any], state_file: str | None = None) -> Path:
    feature = _feature_cfg(config)
    configured = state_file or str(feature.get("state_file", "") or "").strip()
    return _resolve_path(config, configured, DEFAULT_STATE_FILE)


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"records": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"records": []}
    if not isinstance(payload, dict):
        return {"records": []}
    records = payload.get("records")
    if not isinstance(records, list):
        payload["records"] = []
    return payload


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(
        path,
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _upsert_record(path: Path, record: Dict[str, Any]) -> None:
    state = _load_state(path)
    records = state.get("records", [])
    if not isinstance(records, list):
        records = []
    key = (
        str(record.get("capture_date", "") or ""),
        str(record.get("target_key", "") or ""),
    )
    replaced = False
    output: list[Dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        item_key = (
            str(item.get("capture_date", "") or ""),
            str(item.get("target_key", "") or ""),
        )
        if item_key == key:
            output.append(record)
            replaced = True
        else:
            output.append(item)
    if not replaced:
        output.append(record)
    state["records"] = output
    _save_state(path, state)


def _record_file_exists(item: Dict[str, Any]) -> bool:
    file_path = Path(str(item.get("file_path", "") or ""))
    if not file_path.exists() or not file_path.is_file():
        return False
    try:
        return file_path.stat().st_size > 0
    except OSError:
        return False


def list_system_screenshot_files(
    *,
    config: Dict[str, Any],
    capture_date: str | None = None,
    state_file: str | None = None,
    target_key: str = "",
) -> Dict[str, Any]:
    date_text = _capture_date(capture_date)
    state_path = _state_path(config, state_file)
    state = _load_state(state_path)
    files: list[Dict[str, Any]] = []
    for item in state.get("records", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("capture_date", "") or "") != date_text:
            continue
        if target_key and str(item.get("target_key", "") or "") != str(target_key or "").strip():
            continue
        entry = dict(item)
        entry["file_exists"] = _record_file_exists(entry)
        if entry["file_exists"]:
            try:
                entry["size_bytes"] = Path(str(entry.get("file_path", "") or "")).stat().st_size
            except OSError:
                entry["size_bytes"] = 0
        files.append(entry)
    files.sort(key=lambda row: str(row.get("target_key", "") or ""))
    return {
        "capture_date": date_text,
        "state_file": str(state_path),
        "files": files,
    }


def resolve_system_screenshot_file(
    *,
    config: Dict[str, Any],
    capture_date: str,
    target_key: str,
    file_name: str = "",
    state_file: str | None = None,
) -> Tuple[Path, Dict[str, Any]]:
    listing = list_system_screenshot_files(
        config=config,
        capture_date=capture_date,
        state_file=state_file,
        target_key=target_key,
    )
    wanted_name = str(file_name or "").strip()
    for item in listing.get("files", []):
        if not isinstance(item, dict):
            continue
        if wanted_name and str(item.get("file_name", "") or "") != wanted_name:
            continue
        if item.get("file_exists") is not True:
            continue
        path = Path(str(item.get("file_path", "") or ""))
        return path, item
    detail = f"{capture_date}/{target_key}"
    if wanted_name:
        detail += f"/{wanted_name}"
    raise FileNotFoundError(f"系统截图文件不存在: {detail}")


def _select_site(config: Dict[str, Any], site_building: str = "") -> SiteConfig:
    sites = _extract_sites(config)
    if not sites:
        raise RuntimeError("没有可用楼栋配置，请检查 common.internal_source_sites")
    preferred = str(site_building or _feature_cfg(config).get("site_building", "") or DEFAULT_SITE_BUILDING).strip()
    if preferred:
        for site in sites:
            if site.building == preferred:
                return site
    return sites[0]


def _output_dir(config: Dict[str, Any], date_text: str, download_root: str | None = None) -> Path:
    compact = date_text.replace("-", "")
    root = _download_root(config, download_root)
    return root / DEFAULT_SCREENSHOT_DIR_NAME / compact[:6] / f"{compact}--系统截图"


def _relative_path_or_name(file_path: Path, root: Path) -> str:
    try:
        return str(file_path.relative_to(root))
    except Exception:
        return file_path.name


async def _click_target_and_capture(page: Any, target: ScreenshotTarget, path: Path, args: argparse.Namespace) -> None:
    locators = []
    if target.span_id:
        locators.append(page.locator(f"#{target.span_id}").first)
    if target.text:
        locators.append(page.locator(f"text={target.text}").first)
    last_error: Exception | None = None
    for locator in locators:
        try:
            await locator.wait_for(state="visible", timeout=args.menu_timeout_ms)
            await locator.evaluate(
                """element => {
                    element.scrollIntoView({ block: "center", inline: "center" });
                    element.click();
                }""",
                timeout=args.action_timeout_ms,
            )
            last_error = None
            break
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise RuntimeError(f"未找到系统入口: {target.label}({target.text or target.span_id})，{last_error}") from last_error
    try:
        await page.wait_for_load_state("networkidle", timeout=args.network_idle_timeout_ms)
    except Exception:
        await asyncio.sleep(max(0.5, float(args.settle_sec)))
    await asyncio.sleep(max(0.0, float(args.settle_sec)))
    path.parent.mkdir(parents=True, exist_ok=True)
    await page.screenshot(path=str(path), full_page=bool(args.full_page))
    if not path.exists() or path.stat().st_size <= 0:
        raise RuntimeError(f"截图文件为空: {path}")


async def _capture_once(config: Dict[str, Any], args: argparse.Namespace, emit_log: Callable[[str], None]) -> Dict[str, Any]:
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("缺少 playwright，无法执行系统截图采集") from exc

    date_text = _capture_date(args.capture_date)
    targets = _normalize_targets(args.targets)
    if not targets:
        raise RuntimeError("系统截图目标为空")
    state_path = _state_path(config, args.state_file)
    listing = list_system_screenshot_files(config=config, capture_date=date_text, state_file=str(state_path))
    ready_keys = {
        str(item.get("target_key", "") or "")
        for item in listing.get("files", [])
        if isinstance(item, dict) and item.get("file_exists") is True and str(item.get("status", "") or "") == "captured"
    }
    if not bool(args.force) and all(target.key in ready_keys for target in targets):
        emit_log(f"[系统截图采集] {date_text} 已有 {len(targets)} 张截图，跳过重复采集")
        return {
            "status": "skipped",
            "capture_date": date_text,
            "skipped_reason": "already_captured",
            "files": listing.get("files", []),
        }

    site = _select_site(config, str(args.site_building or ""))
    download_root_path = _download_root(config, args.download_root)
    output_dir = _output_dir(config, date_text, args.download_root)
    compact = date_text.replace("-", "")
    emit_log(f"[系统截图采集] 开始: date={date_text}, site={site.building}, targets={len(targets)}")

    resource_acquired = await asyncio.to_thread(
        acquire_building_browser_lock,
        site.building,
        owner="system_screenshot_capture",
        timeout_sec=float(args.browser_resource_wait_sec),
    )
    if not resource_acquired:
        raise RuntimeError(f"{site.building} 浏览器资源正在被其他任务占用，等待超时")

    captured: list[Dict[str, Any]] = []
    try:
        async with async_playwright() as p:
            launch_options: dict[str, Any] = {
                "headless": bool(args.headless),
                "slow_mo": int(args.slow_mo_ms),
                "args": ["--start-maximized"],
            }
            browser_executable = None if bool(args.use_playwright_chromium) else _resolve_browser_executable(args.browser_executable)
            if browser_executable:
                launch_options["executable_path"] = str(browser_executable)
            browser = await p.chromium.launch(**launch_options)
            try:
                context = await browser.new_context(
                    ignore_https_errors=True,
                    viewport={"width": int(args.viewport_width), "height": int(args.viewport_height)},
                    accept_downloads=False,
                )
                page = await context.new_page()
                await _goto_with_retries(page, site.target_url, args, site.building)
                await _ensure_main_page_loaded(page, site, args)

                for target in targets:
                    file_name = f"{compact}--系统截图--{_safe_file_part(target.label)}.png"
                    file_path = output_dir / file_name
                    await _click_target_and_capture(page, target, file_path, args)
                    record = {
                        "capture_date": date_text,
                        "target_key": target.key,
                        "target_label": target.label,
                        "entry_text": target.text,
                        "span_id": target.span_id,
                        "status": "captured",
                        "site_building": site.building,
                        "file_name": file_name,
                        "file_path": str(file_path),
                        "relative_path": _relative_path_or_name(file_path, download_root_path),
                        "size_bytes": file_path.stat().st_size,
                        "content_type": mimetypes.guess_type(file_name)[0] or "image/png",
                        "captured_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    _upsert_record(state_path, record)
                    captured.append(record)
                    emit_log(f"[系统截图采集] 已截图: {target.label} -> {file_path}")

                if int(args.keep_open_sec) > 0:
                    await asyncio.sleep(int(args.keep_open_sec))
            finally:
                await browser.close()
    finally:
        await asyncio.to_thread(release_building_browser_lock, site.building)

    return {
        "status": "success",
        "capture_date": date_text,
        "site_building": site.building,
        "state_file": str(state_path),
        "output_dir": str(output_dir),
        "files": captured,
    }


def _build_args(
    *,
    config: Dict[str, Any],
    capture_date: str | None = None,
    state_file: str | None = None,
    download_root: str | None = None,
    site_building: str | None = None,
    headless: bool | None = None,
    force: bool = False,
) -> argparse.Namespace:
    cfg = _feature_cfg(config)
    return argparse.Namespace(
        capture_date=_capture_date(capture_date),
        state_file=state_file or str(cfg.get("state_file", "") or ""),
        download_root=download_root or str(cfg.get("download_root", "") or ""),
        site_building=site_building or str(cfg.get("site_building", "") or ""),
        targets=cfg.get("targets") if isinstance(cfg.get("targets"), list) else list(DEFAULT_TARGETS),
        browser_executable=str(cfg.get("browser_executable", "") or ""),
        use_playwright_chromium=bool(cfg.get("use_playwright_chromium", False)),
        headless=bool(cfg.get("headless", False)) if headless is None else bool(headless),
        slow_mo_ms=int(cfg.get("slow_mo_ms", 0) or 0),
        keep_open_sec=int(cfg.get("keep_open_sec", 1) or 1),
        navigation_timeout_ms=int(cfg.get("navigation_timeout_ms", 30000) or 30000),
        navigation_retries=int(cfg.get("navigation_retries", 2) or 2),
        login_detect_timeout_ms=int(cfg.get("login_detect_timeout_ms", 5000) or 5000),
        action_timeout_ms=int(cfg.get("action_timeout_ms", 10000) or 10000),
        menu_timeout_ms=int(cfg.get("menu_timeout_ms", 20000) or 20000),
        network_idle_timeout_ms=int(cfg.get("network_idle_timeout_ms", 12000) or 12000),
        settle_sec=float(cfg.get("settle_sec", 3) or 3),
        viewport_width=int(cfg.get("viewport_width", 1920) or 1920),
        viewport_height=int(cfg.get("viewport_height", 1080) or 1080),
        full_page=bool(cfg.get("full_page", True)),
        browser_resource_wait_sec=float(cfg.get("browser_resource_wait_sec", 300) or 300),
        force=bool(force),
    )


async def run_system_screenshot_capture_async(
    *,
    config: Dict[str, Any],
    capture_date: str | None = None,
    state_file: str | None = None,
    download_root: str | None = None,
    site_building: str | None = None,
    headless: bool | None = None,
    force: bool = False,
    emit_log: Callable[[str], None] | None = None,
) -> Dict[str, Any]:
    log = emit_log if callable(emit_log) else (lambda text: print(text, flush=True))
    args = _build_args(
        config=config,
        capture_date=capture_date,
        state_file=state_file,
        download_root=download_root,
        site_building=site_building,
        headless=headless,
        force=force,
    )
    return await _capture_once(config, args, log)


def run_system_screenshot_capture(
    *,
    config: Dict[str, Any],
    capture_date: str | None = None,
    state_file: str | None = None,
    download_root: str | None = None,
    site_building: str | None = None,
    headless: bool | None = None,
    force: bool = False,
    emit_log: Callable[[str], None] | None = None,
) -> Dict[str, Any]:
    return asyncio.run(
        run_system_screenshot_capture_async(
            config=config,
            capture_date=capture_date,
            state_file=state_file,
            download_root=download_root,
            site_building=site_building,
            headless=headless,
            force=force,
            emit_log=emit_log,
        )
    )
