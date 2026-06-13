from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse


DEFAULT_CONFIG_CANDIDATES = [
    Path(os.environ.get("INTERNAL_CONFIG_PATH", "")) if os.environ.get("INTERNAL_CONFIG_PATH") else None,
    Path(r"D:\桌面\ShiJian_Code\pythonProject\全景平台月报自动定时上传\内网端源码\表格计算配置.json"),
    Path(__file__).with_name("表格计算配置.json"),
]
DEFAULT_RUNTIME_ROOT = Path(".runtime")
DEFAULT_STATE_FILE = DEFAULT_RUNTIME_ROOT / "alarm_rule_export" / "export_records.json"
DEFAULT_SCREENSHOT_DIR = DEFAULT_RUNTIME_ROOT / "alarm_rule_export" / "screenshots"
DEFAULT_DOWNLOAD_ROOT = Path(r"D:\QLDownload")
ALARM_RULE_EXPORT_DIR_NAME = "告警规则导出"


@dataclass(frozen=True)
class SiteConfig:
    building: str
    host: str
    username: str
    password: str
    url: str | None = None

    @property
    def target_url(self) -> str:
        if self.url:
            return self.url
        host = _normalize_host(self.host)
        return f"http://{host}/page/main/main.html"


def _normalize_host(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("楼栋 host 为空")
    if text.startswith("http://") or text.startswith("https://"):
        parsed = urlparse(text)
        return parsed.netloc or parsed.path.strip("/")
    return text.strip("/")


def _resolve_default_config() -> Path:
    for candidate in DEFAULT_CONFIG_CANDIDATES:
        if candidate and candidate.exists():
            return candidate
    paths = [str(p) for p in DEFAULT_CONFIG_CANDIDATES if p]
    raise FileNotFoundError("未找到内网端配置文件，请使用 --config 指定路径。候选路径: " + "; ".join(paths))


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _extract_sites(config: dict[str, Any]) -> list[SiteConfig]:
    raw_sites = (
        config.get("common", {}).get("internal_source_sites")
        or config.get("internal_source_sites")
        or []
    )
    sites: list[SiteConfig] = []
    for item in raw_sites:
        if not isinstance(item, dict):
            continue
        if item.get("enabled") is False:
            continue
        building = str(item.get("building") or "").strip()
        host = str(item.get("host") or "").strip()
        username = str(item.get("username") or "").strip()
        password = str(item.get("password") or "")
        url = str(item.get("url") or "").strip() or None
        if not building or not host or not username:
            continue
        sites.append(SiteConfig(building=building, host=host, username=username, password=password, url=url))
    return sites


def _paths_cfg(config: dict[str, Any]) -> dict[str, Any]:
    common = config.get("common", {}) if isinstance(config, dict) else {}
    paths = common.get("paths", {}) if isinstance(common, dict) else {}
    return paths if isinstance(paths, dict) else {}


def _alarm_rule_export_cfg(config: dict[str, Any]) -> dict[str, Any]:
    features = config.get("features", {}) if isinstance(config, dict) else {}
    cfg = features.get("alarm_rule_export", {}) if isinstance(features, dict) else {}
    return cfg if isinstance(cfg, dict) else {}


def _resolve_period(value: str | None = None) -> str:
    text = str(value or "").strip()
    if text:
        if not re.fullmatch(r"\d{4}-\d{2}", text):
            raise ValueError("period 必须是 YYYY-MM")
        return text
    return datetime.now().strftime("%Y-%m")


def _default_runtime_root(config: dict[str, Any] | None = None) -> Path:
    paths = _paths_cfg(config or {})
    configured = str(paths.get("runtime_state_root") or "").strip()
    root = Path(configured) if configured else DEFAULT_RUNTIME_ROOT
    if not root.is_absolute():
        root = Path.cwd() / root
    return root


def _default_download_root(config: dict[str, Any] | None = None) -> Path:
    root_config = config or {}
    common = root_config.get("common", {}) if isinstance(root_config, dict) else {}
    shared_bridge = root_config.get("shared_bridge", {}) if isinstance(root_config, dict) else {}
    if not isinstance(shared_bridge, dict) or not shared_bridge:
        shared_bridge = common.get("shared_bridge", {}) if isinstance(common, dict) else {}
    if isinstance(shared_bridge, dict):
        shared_root = str(
            shared_bridge.get("root_dir")
            or shared_bridge.get("internal_root_dir")
            or ""
        ).strip()
        if shared_root:
            return Path(shared_root)
    paths = _paths_cfg(config or {})
    configured = str(
        paths.get("download_save_dir")
        or paths.get("business_root_dir")
        or paths.get("excel_dir")
        or ""
    ).strip()
    return Path(configured) if configured else DEFAULT_DOWNLOAD_ROOT


def _is_transient_frame_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        "Frame was detached" in message
        or "Execution context was destroyed" in message
        or "Cannot find context with specified id" in message
    )


def _select_sites(sites: list[SiteConfig], buildings: str | None) -> list[SiteConfig]:
    if not buildings:
        return sites
    wanted = {part.strip() for part in buildings.split(",") if part.strip()}
    selected = [site for site in sites if site.building in wanted]
    missing = sorted(wanted - {site.building for site in selected})
    if missing:
        raise ValueError("配置中未找到这些楼栋: " + ", ".join(missing))
    return selected


def _system_browser_candidates() -> list[Path]:
    paths = [
        Path(os.environ.get("PROGRAMFILES", "")) / "Google/Chrome/Application/chrome.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google/Chrome/Application/chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Google/Chrome/Application/chrome.exe",
        Path(os.environ.get("PROGRAMFILES", "")) / "Microsoft/Edge/Application/msedge.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Microsoft/Edge/Application/msedge.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft/Edge/Application/msedge.exe",
    ]
    for command in ("chrome", "chrome.exe", "msedge", "msedge.exe"):
        found = shutil.which(command)
        if found:
            paths.append(Path(found))
    return paths


def _resolve_browser_executable(explicit_path: str | None) -> Path | None:
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"指定的浏览器不存在: {path}")
        return path

    seen: set[str] = set()
    for path in _system_browser_candidates():
        if not str(path) or str(path) in seen:
            continue
        seen.add(str(path))
        if path.exists():
            return path
    return None


def _safe_name(value: str, max_len: int = 18) -> str:
    text = re.sub(r"[\\/:*?\"<>|\s]+", "_", str(value or "").strip())
    text = re.sub(r"_+", "_", text).strip("_")
    return (text or "未命名")[:max_len]


def _period_from_args(args: argparse.Namespace) -> str:
    return _resolve_period(getattr(args, "period", None))


def _export_file_prefix(site: SiteConfig, task: dict[str, Any], args: argparse.Namespace) -> str:
    label = _safe_name(str(task.get("text") or ""), 18)
    period = _period_from_args(args).replace("-", "")
    return f"{site.building}_{period}_{int(task['order']):03d}_{label}_{datetime.now():%m%d%H%M%S}"[:60]


def _download_path(site: SiteConfig, suggested_filename: str, args: argparse.Namespace) -> Path:
    period = _period_from_args(args)
    root = Path(getattr(args, "download_root", "") or DEFAULT_DOWNLOAD_ROOT)
    download_dir = root / ALARM_RULE_EXPORT_DIR_NAME / period / site.building
    download_dir.mkdir(parents=True, exist_ok=True)
    path = download_dir / suggested_filename
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    for index in range(1, 1000):
        candidate = download_dir / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
    return download_dir / f"{stem}_{datetime.now():%Y%m%d%H%M%S}{suffix}"


def _state_file(args: argparse.Namespace) -> Path:
    return Path(args.state_file) if args.state_file else DEFAULT_STATE_FILE


def _load_export_state(args: argparse.Namespace) -> dict[str, Any]:
    path = _state_file(args)
    if not path.exists():
        return {"version": 2, "records": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {"version": 2, "records": []}
    if not isinstance(data, dict):
        return {"version": 2, "records": []}
    records = data.get("records")
    if not isinstance(records, list):
        data["records"] = []
    period = _period_from_args(args)
    migrated = False
    normalized_records: list[dict[str, Any]] = []
    for item in data.get("records", []):
        if not isinstance(item, dict):
            continue
        if not str(item.get("period") or "").strip():
            item = dict(item)
            item["period"] = period
            migrated = True
        normalized_records.append(item)
    data["records"] = normalized_records
    data["version"] = 2
    if migrated:
        _save_export_state(args, data)
    return data


def _save_export_state(args: argparse.Namespace, state: dict[str, Any]) -> None:
    path = _state_file(args)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _upsert_export_state_record(args: argparse.Namespace, site: SiteConfig, record: dict[str, Any], status: str) -> None:
    file_name = str(record.get("file_name") or "").strip()
    if not file_name:
        return
    state = _load_export_state(args)
    records = [item for item in state.get("records", []) if isinstance(item, dict)]
    now = datetime.now().isoformat(timespec="seconds")
    updated = False
    period = _period_from_args(args)
    for item in records:
        if (
            item.get("building") == site.building
            and item.get("period") == period
            and item.get("file_name") == file_name
        ):
            item.update(
                {
                    "file_prefix": record.get("file_prefix") or file_name,
                    "status": status,
                    "updated_at": now,
                }
            )
            updated = True
            break
    if not updated:
        records.append(
            {
                "building": site.building,
                "period": period,
                "url": site.target_url,
                "file_prefix": record.get("file_prefix") or file_name,
                "file_name": file_name,
                "status": status,
                "created_at": now,
                "updated_at": now,
            }
        )
    state["records"] = records
    _save_export_state(args, state)


def _update_export_state_record(
    args: argparse.Namespace,
    site: SiteConfig,
    file_name: str,
    status: str,
    operation_text: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    state = _load_export_state(args)
    period = _period_from_args(args)
    now = datetime.now().isoformat(timespec="seconds")
    changed = False
    for item in state.get("records", []):
        if (
            isinstance(item, dict)
            and item.get("building") == site.building
            and item.get("period") == period
            and item.get("file_name") == file_name
        ):
            item["status"] = status
            item["operation_text"] = operation_text
            item["updated_at"] = now
            if extra:
                item.update(extra)
            changed = True
            break
    if changed:
        _save_export_state(args, state)


def _pending_state_records(args: argparse.Namespace, site: SiteConfig) -> list[dict[str, Any]]:
    state = _load_export_state(args)
    pending_statuses = {"created", "generating", "ready", "missing", "skipped"}
    period = _period_from_args(args)
    records: list[dict[str, Any]] = []
    for item in state.get("records", []):
        if not isinstance(item, dict):
            continue
        if item.get("building") != site.building:
            continue
        if item.get("period") != period:
            continue
        if item.get("status") not in pending_statuses:
            continue
        file_name = str(item.get("file_name") or "").strip()
        if file_name:
            records.append(item)
    return records


def _completed_state_records(args: argparse.Namespace, site: SiteConfig) -> list[dict[str, Any]]:
    state = _load_export_state(args)
    period = _period_from_args(args)
    records: list[dict[str, Any]] = []
    for item in state.get("records", []):
        if not isinstance(item, dict):
            continue
        if item.get("building") != site.building:
            continue
        if item.get("period") != period:
            continue
        if item.get("status") != "downloaded":
            continue
        file_name = str(item.get("file_name") or "").strip()
        if file_name:
            records.append(item)
    return records


async def _is_visible(locator: Any, timeout_ms: int) -> bool:
    try:
        await locator.wait_for(state="visible", timeout=timeout_ms)
        return True
    except Exception:
        return False


async def _dom_click(locator: Any, timeout_ms: int) -> None:
    await locator.wait_for(state="attached", timeout=timeout_ms)
    await locator.evaluate(
        """element => {
            element.scrollIntoView({ block: "center", inline: "nearest" });
            element.click();
        }""",
        timeout=timeout_ms,
    )


async def _goto_with_retries(page: Any, url: str, args: argparse.Namespace, building: str) -> None:
    last_error: Exception | None = None
    attempts = max(1, args.navigation_retries + 1)
    for attempt in range(1, attempts + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=args.navigation_timeout_ms)
            return
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            print(f"[{building}] 页面打开失败，重试 {attempt}/{args.navigation_retries}: {exc}", flush=True)
            await asyncio.sleep(min(3, attempt))
    if last_error:
        raise last_error


async def _right_content_frame(page: Any, timeout_ms: int) -> Any:
    iframe = page.locator("#right-content").first
    await iframe.wait_for(state="attached", timeout=timeout_ms)
    frame = page.frame(name="rightContent")
    if frame:
        return frame
    handle = await iframe.element_handle(timeout=timeout_ms)
    if handle:
        frame = await handle.content_frame()
    if not frame:
        raise RuntimeError("未找到 right-content iframe")
    return frame


SYSTEM_TOOLS_EXPANDED_SCRIPT = """() => {
    const title = Array.from(document.querySelectorAll(".c-leftMenu__level-1__item-title"))
        .find(element => element.textContent.trim() === "系统工具");
    const item = title && title.closest(".c-leftMenu__level-1__item");
    const submenu = item && item.querySelector(".c-leftMenu__level-2");
    if (!submenu) return false;
    const style = window.getComputedStyle(submenu);
    const rect = submenu.getBoundingClientRect();
    return style.display !== "none" && style.visibility !== "hidden" && rect.height > 0;
}"""


ROW_FILE_NAME_SCRIPT = """prefix => {
    const rows = Array.from(document.querySelectorAll(".c-table__tbody tbody tr"));
    const row = rows.find(item => {
        const cell = item.querySelector("td:nth-child(2)");
        return cell && cell.textContent.trim().startsWith(prefix);
    });
    if (!row) return null;
    const cell = row.querySelector("td:nth-child(2)");
    return cell ? cell.textContent.trim() : null;
}"""


ROW_STATUS_SCRIPT = """prefix => {
    const rows = Array.from(document.querySelectorAll(".c-table__tbody tbody tr"));
    const row = rows.find(item => {
        const cell = item.querySelector("td:nth-child(2)");
        return cell && cell.textContent.trim() === prefix;
    });
    if (!row) return null;
    const fileCell = row.querySelector("td:nth-child(2)");
    const operationCell = row.querySelector("td:nth-child(4)");
    const downloadLink = row.querySelector('a[data-type="xz"]');
    const operationText = operationCell ? operationCell.textContent.trim() : "";
    return {
        fileName: fileCell ? fileCell.textContent.trim() : "",
        operationText,
        isGenerating: operationText.includes("正在生成"),
        hasDownload: !!downloadLink,
    };
}"""


EXPORT_LIST_ROWS_SCRIPT = """() => {
    const rows = Array.from(document.querySelectorAll(".c-table__tbody tbody tr"));
    return rows.map(row => {
        const fileCell = row.querySelector("td:nth-child(2)");
        const operationCell = row.querySelector("td:nth-child(4)");
        const downloadLink = row.querySelector('a[data-type="xz"]');
        const operationText = operationCell ? operationCell.textContent.trim() : "";
        return {
            fileName: fileCell ? fileCell.textContent.trim() : "",
            operationText,
            isGenerating: operationText.includes("正在生成"),
            hasDownload: !!downloadLink,
        };
    }).filter(item => item.fileName);
}"""


def _filename_has_period_date(file_name: str, period: str) -> bool:
    text = str(file_name or "")
    if not text:
        return False
    year_text, month_text = str(period or "").split("-", 1)
    year = int(year_text)
    month = int(month_text)

    compact_pattern = re.compile(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(?:[0-3]\d)?(?:[0-2]\d[0-5]\d[0-5]\d)?(?!\d)")
    for match in compact_pattern.finditer(text):
        if int(match.group(1)) == year and int(match.group(2)) == month:
            return True

    separated_pattern = re.compile(
        r"(?<!\d)(20\d{2})\s*(?:年|[-_/\.])\s*(1[0-2]|0?[1-9])"
        r"(?:\s*(?:月|[-_/\.])\s*([0-3]?\d)\s*(?:日)?)?(?!\d)"
    )
    for match in separated_pattern.finditer(text):
        if int(match.group(1)) == year and int(match.group(2)) == month:
            return True
    return False


def _xpath_literal(value: str) -> str:
    text = str(value or "")
    if "'" not in text:
        return f"'{text}'"
    if '"' not in text:
        return f'"{text}"'
    parts = text.split("'")
    return "concat(" + ", \"'\", ".join(f"'{part}'" for part in parts) + ")"


def _exact_file_row(frame: Any, file_name: str) -> Any:
    literal = _xpath_literal(file_name)
    return frame.locator(
        "xpath=.//div[contains(concat(' ', normalize-space(@class), ' '), ' c-table__tbody ')]"
        f"//tbody//tr[td[2][normalize-space(.)={literal}]]"
    ).first


TREE_READ_TASKS_SCRIPT = """() => {
    const isVisible = element => !!element && !!(element.offsetWidth || element.offsetHeight || element.getClientRects().length);
    const nodeText = node => {
        const span = document.getElementById(`${node.id}_span`);
        const link = document.getElementById(`${node.id}_a`) || node.querySelector("a[treenode_a]");
        return ((span && span.textContent) || (link && (link.getAttribute("title") || link.textContent)) || "").trim();
    };
    const nodeCheckId = node => {
        const check = document.getElementById(`${node.id}_check`) || node.querySelector("span[treenode_check]");
        return check ? check.id : "";
    };
    const rootNode = Array.from(document.querySelectorAll("li.level0")).find(isVisible);
    if (!rootNode) return { ok: false, message: "未找到整栋根节点" };
    const tasks = [{
        kind: "root",
        order: 1,
        checkId: nodeCheckId(rootNode),
        text: nodeText(rootNode),
    }];
    return {
        ok: true,
        tasks,
    };
}"""


SELECT_TREE_TASK_SCRIPT = """({ task }) => {
    const isVisible = element => !!element && !!(element.offsetWidth || element.offsetHeight || element.getClientRects().length);
    const nodeText = node => {
        const span = document.getElementById(`${node.id}_span`);
        const link = document.getElementById(`${node.id}_a`) || node.querySelector("a[treenode_a]");
        return ((span && span.textContent) || (link && (link.getAttribute("title") || link.textContent)) || "").trim();
    };
    const treeApi = () => {
        const jq = window.jQuery || window.$;
        if (!jq || !jq.fn || !jq.fn.zTree || !jq.fn.zTree.getZTreeObj) return null;
        return jq.fn.zTree.getZTreeObj("js-ztree");
    };
    const treeNode = (api, nodeId) => {
        if (!api || typeof api.getNodeByTId !== "function") return null;
        return api.getNodeByTId(nodeId);
    };
    const checkState = (check, nodeId, api) => {
        const apiNode = treeNode(api, nodeId);
        const className = check.className || "";
        return {
            className,
            checkedByClass: className.includes("checkbox_true"),
            treeChecked: apiNode ? !!apiNode.checked : null,
        };
    };
    const isChecked = state => state.checkedByClass || state.treeChecked === true;
    const fireMouseClick = target => {
        ["mouseover", "mousedown", "mouseup", "click"].forEach(type => {
            target.dispatchEvent(new MouseEvent(type, {
                bubbles: true,
                cancelable: true,
                view: window,
            }));
        });
    };
    const clickCheck = node => {
        const check = document.getElementById(`${node.id}_check`)
            || node.querySelector("span[treenode_check]");
        if (!check) return { ok: false, message: `节点没有勾选框: ${node.id}` };
        const beforeClass = check.className || "";
        check.scrollIntoView({ block: "center", inline: "nearest" });
        const api = treeApi();
        const jq = window.jQuery || window.$;
        const nodeId = (check.id || "").replace(/_check$/, "");
        const attempts = [];
        let treeChecked = null;
        let apiNodeId = "";
        let method = "";

        fireMouseClick(check);
        let state = checkState(check, nodeId, api);
        attempts.push(`dom-event:${state.className}:${state.treeChecked}`);
        if (isChecked(state)) {
            method = "dom-event";
            treeChecked = state.treeChecked;
        }

        if (!method && jq) {
            jq(check).trigger("click");
            state = checkState(check, nodeId, api);
            attempts.push(`jquery-trigger:${state.className}:${state.treeChecked}`);
            if (isChecked(state)) {
                method = "jquery-trigger";
                treeChecked = state.treeChecked;
            }
        }

        if (!method && api && typeof api.checkNode === "function") {
            const apiNode = treeNode(api, nodeId);
            if (apiNode) {
                apiNodeId = apiNode.tId || nodeId;
                if (typeof api.checkAllNodes === "function") api.checkAllNodes(false);
                api.checkNode(apiNode, true, true, true);
                if (typeof api.updateNode === "function") api.updateNode(apiNode, true);
                state = checkState(check, nodeId, api);
                attempts.push(`ztree-api:${state.className}:${state.treeChecked}`);
                if (isChecked(state)) {
                    method = "ztree-api";
                    treeChecked = state.treeChecked;
                }
            } else {
                attempts.push("ztree-api-node-missing");
            }
        }
        const afterClass = check.className || "";
        const checkedByClass = afterClass.includes("checkbox_true");
        if (!method && !checkedByClass && treeChecked !== true) {
            return {
                ok: false,
                message: `点击后节点仍未选中: ${check.id || node.id}`,
                id: check.id || "",
                beforeClass,
                afterClass,
                method,
                treeChecked,
                apiNodeId,
                attempts,
                text: nodeText(node),
            };
        }
        return {
            ok: true,
            id: check.id || "",
            beforeClass,
            afterClass,
            method,
            treeChecked,
            apiNodeId,
            attempts,
            text: nodeText(node),
        };
    };

    const rootNode = Array.from(document.querySelectorAll("li.level0")).find(isVisible);
    if (!rootNode) return { ok: false, message: "未找到整栋根节点" };
    return clickCheck(rootNode);
}"""


async def _open_export_dialog(frame: Any, args: argparse.Namespace) -> None:
    existing_iframe = frame.locator('iframe[src*="config_export"]').first
    try:
        await existing_iframe.wait_for(state="attached", timeout=1000)
        return
    except Exception:
        pass

    export_button = frame.locator('.d-config__body-operation button:has-text("导出")').first
    await export_button.wait_for(state="visible", timeout=args.export_timeout_ms)
    await export_button.click(timeout=args.action_timeout_ms)
    if args.export_wait_sec > 0:
        await asyncio.sleep(args.export_wait_sec)


async def _export_dialog_frame(frame: Any, args: argparse.Namespace) -> Any:
    iframe = frame.locator('iframe[src*="config_export"]').first
    last_error: Exception | None = None
    for _ in range(10):
        try:
            await iframe.wait_for(state="attached", timeout=args.export_timeout_ms)
            await asyncio.sleep(args.iframe_stabilize_sec)
            handle = await iframe.element_handle(timeout=args.export_timeout_ms)
            dialog_frame = await handle.content_frame() if handle else None
            if not dialog_frame:
                raise RuntimeError("未找到导出弹窗 iframe: config_export.html")
            try:
                await dialog_frame.wait_for_load_state("domcontentloaded", timeout=args.export_timeout_ms)
            except Exception:
                pass
            await dialog_frame.locator("#fileName").first.wait_for(state="visible", timeout=args.export_timeout_ms)
            await dialog_frame.locator("li.level0").first.wait_for(state="attached", timeout=args.export_timeout_ms)
            return dialog_frame
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(args.iframe_stabilize_sec)
    raise RuntimeError(f"未找到稳定的导出弹窗 iframe: {last_error}")


async def _submit_export_dialog(frame: Any, file_prefix: str, args: argparse.Namespace) -> None:
    file_input = frame.locator("#fileName").first
    await file_input.wait_for(state="visible", timeout=args.export_timeout_ms)
    await file_input.fill(file_prefix, timeout=args.action_timeout_ms)
    confirm = frame.locator(".operButton .confirmButton, button.confirmButton").first
    await confirm.wait_for(state="visible", timeout=args.export_timeout_ms)
    await confirm.click(timeout=args.action_timeout_ms)


async def _click_tree_task_checkbox(
    dialog_frame: Any,
    task: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    result = await dialog_frame.evaluate(SELECT_TREE_TASK_SCRIPT, {"task": task})
    if result.get("ok"):
        await asyncio.sleep(0.3)
    return result


async def _read_tree_tasks_from_dialog(frame: Any, site: SiteConfig, args: argparse.Namespace) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, args.frame_operation_retries + 2):
        dialog_frame = await _export_dialog_frame(frame, args)
        try:
            return await dialog_frame.evaluate(TREE_READ_TASKS_SCRIPT)
        except Exception as exc:
            last_error = exc
            if not _is_transient_frame_error(exc) or attempt > args.frame_operation_retries:
                raise
            print(f"[{site.building}] 读取树节点时 iframe 抖动，重新获取 iframe {attempt}/{args.frame_operation_retries}", flush=True)
            await asyncio.sleep(args.iframe_stabilize_sec)
    raise RuntimeError(f"未能读取导出树节点: {last_error}")


async def _collect_export_tasks(frame: Any, site: SiteConfig, args: argparse.Namespace) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, args.frame_operation_retries + 3):
        try:
            await _open_export_dialog(frame, args)
            result = await _read_tree_tasks_from_dialog(frame, site, args)
            if not result.get("ok"):
                raise RuntimeError(str(result.get("message") or "未能读取导出树节点"))
            tasks = list(result.get("tasks") or [])
            print(f"[{site.building}] 导出计划：整栋楼 {len(tasks)} 个", flush=True)
            return tasks
        except Exception as exc:
            last_error = exc
            print(
                f"[{site.building}] 读取导出树任务时 iframe 抖动，重新执行 {attempt}/{args.frame_operation_retries + 2}: {exc}",
                flush=True,
            )
            await asyncio.sleep(args.iframe_stabilize_sec)
    raise RuntimeError(f"未能读取导出树任务: {last_error}")


async def _wait_export_row(frame: Any, file_prefix: str, args: argparse.Namespace) -> str:
    timeout_ms = args.export_generate_timeout_ms
    if bool(getattr(args, "single_check_only", True)):
        timeout_ms = max(int(getattr(args, "export_timeout_ms", 30000) or 30000), 30000)
    await frame.wait_for_function(ROW_FILE_NAME_SCRIPT, arg=file_prefix, timeout=timeout_ms)
    file_name = await frame.evaluate(ROW_FILE_NAME_SCRIPT, file_prefix)
    if not file_name:
        raise RuntimeError(f"导出后未在列表中找到文件: {file_prefix}")
    return str(file_name)


async def _download_export_file(page: Any, frame: Any, site: SiteConfig, file_name: str, args: argparse.Namespace) -> Path | None:
    row = _exact_file_row(frame, file_name)
    download_link = row.locator('a[data-type="xz"]').first
    try:
        await download_link.wait_for(state="visible", timeout=args.download_button_timeout_ms)
    except Exception:
        print(f"[{site.building}] 跳过下载：{file_name} 这一行没有可见的下载按钮", flush=True)
        return None
    async with page.expect_download(timeout=args.download_timeout_ms) as download_info:
        await download_link.click(timeout=args.action_timeout_ms)
    download = await download_info.value
    suggested = download.suggested_filename or f"{file_name}.xlsx"
    path = _download_path(site, suggested, args)
    await download.save_as(str(path))
    return path


async def _delete_export_file(frame: Any, site: SiteConfig, file_name: str, args: argparse.Namespace) -> bool:
    row = _exact_file_row(frame, file_name)
    delete_link = row.locator('a[data-type="sc"]').first
    try:
        await delete_link.wait_for(state="visible", timeout=args.delete_timeout_ms)
    except Exception:
        print(f"[{site.building}] 跳过删除：{file_name} 这一行没有可见的删除按钮", flush=True)
        return False

    await delete_link.click(timeout=args.action_timeout_ms)
    await asyncio.sleep(0.3)
    confirm_candidates = [
        frame.locator('button:has-text("确定")').last,
        frame.locator(".confirmButton").last,
    ]
    for confirm in confirm_candidates:
        if await _is_visible(confirm, 800):
            await confirm.click(timeout=args.action_timeout_ms)
            break

    try:
        await frame.wait_for_function(
            """fileName => !Array.from(document.querySelectorAll(".c-table__tbody tbody tr"))
                .some(row => {
                    const cell = row.querySelector("td:nth-child(2)");
                    return cell && cell.textContent.trim() === fileName;
                })""",
            arg=file_name,
            timeout=args.delete_timeout_ms,
        )
    except Exception:
        pass
    print(f"[{site.building}] 已删除导出记录: {file_name}", flush=True)
    return True


async def _create_export_for_task(frame: Any, site: SiteConfig, task: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    last_error: Exception | None = None
    click_result: dict[str, Any] | None = None
    dialog_frame = None
    await _open_export_dialog(frame, args)
    for attempt in range(1, args.frame_operation_retries + 2):
        try:
            dialog_frame = await _export_dialog_frame(frame, args)
            print(
                f"[{site.building}] 选择导出节点 {task['order']}: "
                f"整栋楼 {task.get('text')} "
                f"({task.get('checkId')})",
                flush=True,
            )
            click_result = await _click_tree_task_checkbox(dialog_frame, task, args)
            break
        except Exception as exc:
            last_error = exc
            if not _is_transient_frame_error(exc) or attempt > args.frame_operation_retries:
                raise
            print(f"[{site.building}] 选择节点时 iframe 抖动，重新获取 iframe {attempt}/{args.frame_operation_retries}", flush=True)
            await asyncio.sleep(args.iframe_stabilize_sec)
    if click_result is None or dialog_frame is None:
        raise RuntimeError(f"未能勾选导出节点: {last_error}")
    if not click_result.get("ok"):
        raise RuntimeError(str(click_result.get("message") or "未能勾选导出节点"))
    print(
        f"[{site.building}] 已点击勾选节点: {click_result.get('id') or click_result.get('source')} "
        f"({click_result.get('beforeClass')} -> {click_result.get('afterClass')}), "
        f"方式={click_result.get('method')}, zTree checked={click_result.get('treeChecked')}, "
        f"当前节点={click_result.get('text')}, attempts={click_result.get('attempts')}",
        flush=True,
    )
    file_prefix = _export_file_prefix(site, task, args)
    print(f"[{site.building}] 填写文件名: {file_prefix}", flush=True)
    await _submit_export_dialog(dialog_frame, file_prefix, args)
    file_name = await _wait_export_row(frame, file_prefix, args)
    record = {
        "task": task,
        "file_prefix": file_prefix,
        "file_name": file_name,
        "downloaded": False,
        "deleted": False,
    }
    _upsert_export_state_record(args, site, record, "created")
    print(f"[{site.building}] 已持久化等待文件名: {file_name}", flush=True)
    return record


async def _download_and_delete_when_ready(
    page: Any,
    frame: Any,
    site: SiteConfig,
    records: list[dict[str, Any]],
    args: argparse.Namespace,
) -> tuple[list[Path], int, int, int]:
    pending = list(records)
    downloaded_paths: list[Path] = []
    skipped_downloads = 0
    deleted_count = 0
    deadline = asyncio.get_running_loop().time() + (args.export_generate_timeout_ms / 1000)

    while pending:
        next_pending: list[dict[str, Any]] = []
        for record in pending:
            file_name = str(record["file_name"])
            status = await frame.evaluate(ROW_STATUS_SCRIPT, file_name)
            if not status:
                print(f"[{site.building}] 未找到导出记录，暂缓: {file_name}", flush=True)
                _update_export_state_record(args, site, file_name, "missing", "列表中未找到该文件名")
                next_pending.append(record)
                continue

            operation_text = str(status.get("operationText") or "")
            if status.get("isGenerating"):
                print(f"[{site.building}] 文件仍在生成: {file_name} / {operation_text}", flush=True)
                _update_export_state_record(args, site, file_name, "generating", operation_text)
                next_pending.append(record)
                continue

            if not status.get("hasDownload"):
                print(f"[{site.building}] 文件暂不可下载，继续按精确文件名等待: {file_name} / {operation_text}", flush=True)
                _update_export_state_record(args, site, file_name, "missing", operation_text)
                next_pending.append(record)
                continue

            _update_export_state_record(args, site, file_name, "ready", operation_text)
            path = await _download_export_file(page, frame, site, file_name, args)
            if path:
                downloaded_paths.append(path)
                print(f"[{site.building}] 已下载 downloaded_path={path}", flush=True)
                delete_ok = await _delete_export_file(frame, site, file_name, args)
                if delete_ok:
                    deleted_count += 1
                _update_export_state_record(
                    args,
                    site,
                    file_name,
                    "downloaded",
                    operation_text,
                    {
                        "downloaded_path": str(path),
                        "downloaded_at": datetime.now().isoformat(timespec="seconds"),
                        "deleted_from_page": delete_ok,
                    },
                )
            else:
                _update_export_state_record(args, site, file_name, "missing", "下载按钮消失，等待下次检查")
                next_pending.append(record)

        pending = next_pending
        if pending:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(f"等待导出文件生成超时，剩余 {len(pending)} 个")
            if bool(getattr(args, "single_check_only", True)):
                return downloaded_paths, skipped_downloads, deleted_count, len(pending)
            print(f"[{site.building}] 仍有 {len(pending)} 个文件未完成，{args.generate_poll_interval_sec} 秒后再次检查", flush=True)
            await asyncio.sleep(args.generate_poll_interval_sec)

    return downloaded_paths, skipped_downloads, deleted_count, 0


async def _existing_export_records(frame: Any, site: SiteConfig, args: argparse.Namespace) -> list[dict[str, Any]]:
    persisted = _pending_state_records(args, site)
    records: list[dict[str, Any]] = []
    for index, item in enumerate(persisted, start=1):
        file_name = str(item.get("file_name") or "").strip()
        if not file_name:
            continue
        row = await frame.evaluate(ROW_STATUS_SCRIPT, file_name)
        operation_text = ""
        if row:
            operation_text = str(row.get("operationText") or "")
            if row.get("isGenerating"):
                _update_export_state_record(args, site, file_name, "generating", operation_text)
            elif row.get("hasDownload"):
                _update_export_state_record(args, site, file_name, "ready", operation_text)
            else:
                _update_export_state_record(args, site, file_name, "missing", operation_text)
        else:
            operation_text = "列表中未找到该持久化文件名"
            _update_export_state_record(args, site, file_name, "missing", operation_text)
            continue
        records.append(
            {
                "task": {"kind": "existing", "order": index, "text": "历史导出"},
                "file_prefix": file_name,
                "file_name": file_name,
                "downloaded": False,
                "deleted": False,
                "operation_text": operation_text,
            }
        )
    return records


async def _current_month_export_records(frame: Any, site: SiteConfig, args: argparse.Namespace) -> list[dict[str, Any]]:
    period = _period_from_args(args)
    rows = await frame.evaluate(EXPORT_LIST_ROWS_SCRIPT)
    candidates: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        file_name = str(row.get("fileName") or "").strip()
        if not _filename_has_period_date(file_name, period):
            continue
        operation_text = str(row.get("operationText") or "")
        if row.get("isGenerating"):
            status = "generating"
        elif row.get("hasDownload"):
            status = "ready"
        else:
            status = "missing"
        _upsert_export_state_record(
            args,
            site,
            {"file_name": file_name, "file_prefix": file_name},
            status,
        )
        _update_export_state_record(args, site, file_name, status, operation_text)
        candidates.append(
            {
                "task": {"kind": "current_month_existing", "order": len(candidates) + 1, "text": "本月已有导出"},
                "file_prefix": file_name,
                "file_name": file_name,
                "status": status,
                "downloaded": False,
                "deleted": False,
                "operation_text": operation_text,
            }
        )
    if not candidates:
        return []
    candidates.sort(
        key=lambda item: (
            0 if item.get("status") == "ready" else 1 if item.get("status") == "generating" else 2,
            str(item.get("file_name") or ""),
        )
    )
    return candidates[:1]


async def _ensure_main_page_loaded(page: Any, site: SiteConfig, args: argparse.Namespace) -> None:
    menu = page.locator("a.p-main__header__menu-item").first
    attempts = max(2, args.navigation_retries + 2)
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        username_input = page.locator("#username").first
        if await _is_visible(username_input, args.login_detect_timeout_ms):
            print(f"[{site.building}] 检测到登录页，开始登录 {attempt}/{attempts}", flush=True)
            await username_input.fill(site.username, timeout=args.action_timeout_ms)
            await page.locator("#password").first.fill(site.password, timeout=args.action_timeout_ms)
            await page.locator("text=登录").first.click(timeout=args.action_timeout_ms)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=args.navigation_timeout_ms)
            except Exception:
                pass
        elif attempt == 1:
            print(f"[{site.building}] 未检测到登录页，按已有登录态继续", flush=True)

        try:
            await menu.wait_for(state="visible", timeout=args.menu_timeout_ms)
            return
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            print(f"[{site.building}] 登录后未看到主菜单，重试 {attempt}/{attempts}: {exc}", flush=True)
            try:
                await page.reload(wait_until="domcontentloaded", timeout=args.navigation_timeout_ms)
            except Exception:
                pass

    raise RuntimeError(f"登录后未进入主页面: {last_error}")


async def export_alarm_rules(browser: Any, site: SiteConfig, args: argparse.Namespace) -> dict[str, Any]:
    context = await browser.new_context(
        ignore_https_errors=True,
        viewport={"width": 1440, "height": 900},
        accept_downloads=True,
    )
    page = await context.new_page()
    result: dict[str, Any] = {
        "building": site.building,
        "url": site.target_url,
        "status": "failed",
        "message": "",
    }
    try:
        print(f"[{site.building}] 打开页面: {site.target_url}", flush=True)
        await _goto_with_retries(page, site.target_url, args, site.building)

        await _ensure_main_page_loaded(page, site, args)
        project_menu = page.locator('a.p-main__header__menu-item:has-text("工程配置")').first
        await project_menu.wait_for(state="visible", timeout=args.menu_timeout_ms)
        print(f"[{site.building}] 点击顶部菜单：工程配置", flush=True)
        await project_menu.click(timeout=args.action_timeout_ms)

        alarm_rules = page.locator('.c-leftMenu__level-2__item:has-text("告警规则管理")').first
        if not await page.evaluate(SYSTEM_TOOLS_EXPANDED_SCRIPT):
            system_tools = page.locator('.c-leftMenu__level-1__item-title:has-text("系统工具")').first
            await system_tools.wait_for(state="visible", timeout=args.menu_timeout_ms)
            print(f"[{site.building}] 点击左侧菜单：系统工具", flush=True)
            await _dom_click(system_tools, args.action_timeout_ms)
            await page.wait_for_function(SYSTEM_TOOLS_EXPANDED_SCRIPT, timeout=args.menu_timeout_ms)
        await alarm_rules.wait_for(state="attached", timeout=args.menu_timeout_ms)
        print(f"[{site.building}] 点击左侧菜单：告警规则管理", flush=True)
        await _dom_click(alarm_rules, args.action_timeout_ms)

        frame = await _right_content_frame(page, args.menu_timeout_ms)
        await frame.locator('.d-config__body-operation button:has-text("导出")').first.wait_for(
            state="visible",
            timeout=args.export_timeout_ms,
        )
        completed_records = _completed_state_records(args, site)
        if completed_records:
            print(
                f"[{site.building}] 本月已下载，跳过重复导出和下载: count={len(completed_records)}",
                flush=True,
            )
            for record in completed_records:
                print(
                    f"[{site.building}] 已下载记录: {record.get('file_name')} / {record.get('downloaded_path')}",
                    flush=True,
                )
            result["status"] = "exported"
            result["message"] = f"本月已有 {len(completed_records)} 个导出已下载，已跳过重复导出"
            return result
        records = await _existing_export_records(frame, site, args)
        used_existing_records = bool(records)
        if records:
            print(
                f"[{site.building}] 发现持久化记录，跳过重复导出，直接等待下载: count={len(records)}",
                flush=True,
            )
            for record in records:
                print(
                    f"[{site.building}] 复用持久化记录: {record['file_name']} / {record.get('operation_text')}",
                    flush=True,
                )
        else:
            records = await _current_month_export_records(frame, site, args)
            used_existing_records = bool(records)
            if records:
                print(
                    f"[{site.building}] 页面已存在本月导出文件，跳过重复创建并直接等待下载: {records[0]['file_name']}",
                    flush=True,
                )

        if not records:
            print(f"[{site.building}] 未发现本月导出文件，开始创建整栋楼导出", flush=True)
            tasks = await _collect_export_tasks(frame, site, args)
            if not tasks:
                raise RuntimeError("导出树中没有需要导出的节点")

            records = []
            for task in tasks:
                records.append(await _create_export_for_task(frame, site, task, args))

            print(f"[{site.building}] 已生成 {len(records)} 个导出记录，开始检查下载状态", flush=True)

        downloaded_paths, skipped_downloads, deleted_count, pending_count = await _download_and_delete_when_ready(
            page,
            frame,
            site,
            records,
            args,
        )

        if args.export_wait_sec > 0:
            await asyncio.sleep(args.export_wait_sec)

        result["status"] = "exported"
        if pending_count:
            result["status"] = "pending"
        action_text = "复用历史" if used_existing_records else "已生成"
        result["message"] = (
            f"{action_text} {len(records)} 个导出记录，下载 {len(downloaded_paths)} 个，"
            f"删除 {deleted_count} 个，跳过 {skipped_downloads} 个，待完成 {pending_count} 个"
        )
        print(
            f"[{site.building}] 完成：{action_text} {len(records)} 个导出记录，"
            f"下载 {len(downloaded_paths)} 个，删除 {deleted_count} 个，跳过 {skipped_downloads} 个，待完成 {pending_count} 个",
            flush=True,
        )
        return result
    except Exception as exc:
        result["message"] = str(exc)
        print(f"[{site.building}] 失败: {exc}", flush=True)
        try:
            screenshot_dir = Path(getattr(args, "screenshots_dir", "") or DEFAULT_SCREENSHOT_DIR)
            screenshot_dir.mkdir(parents=True, exist_ok=True)
            await page.screenshot(path=str(screenshot_dir / f"{site.building}.png"), full_page=True)
        except Exception:
            pass
        return result


async def _run(args: argparse.Namespace) -> int:
    config_payload = getattr(args, "config_payload", None)
    if isinstance(config_payload, dict):
        config_path = Path(getattr(args, "config", "") or "<runtime-config>")
        config = config_payload
    else:
        config_path = Path(args.config) if args.config else _resolve_default_config()
        config = _load_json(config_path)
    sites = _select_sites(_extract_sites(config), args.buildings)
    if not sites:
        raise RuntimeError("没有可用楼栋配置，请检查 common.internal_source_sites")

    args.period = _resolve_period(getattr(args, "period", None))
    if not hasattr(args, "single_check_only"):
        args.single_check_only = not bool(getattr(args, "wait_until_ready", False))
    if not str(getattr(args, "download_root", "") or "").strip():
        args.download_root = str(_default_download_root(config))
    print(f"读取配置: {config_path}", flush=True)
    print(f"导出月份: {args.period}", flush=True)
    print("楼栋: " + ", ".join(site.building for site in sites), flush=True)
    if args.list_sites:
        for site in sites:
            print(f"{site.building}: {site.target_url}", flush=True)
        return 0

    try:
        from playwright.async_api import async_playwright
    except Exception:
        print("缺少 playwright，请先执行: python -m pip install playwright && python -m playwright install chromium")
        return 2

    Path(getattr(args, "screenshots_dir", "") or DEFAULT_SCREENSHOT_DIR).mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        launch_options: dict[str, Any] = {
            "headless": args.headless,
            "slow_mo": args.slow_mo_ms,
            "args": ["--start-maximized"],
        }
        browser_executable = None if args.use_playwright_chromium else _resolve_browser_executable(args.browser_executable)
        if browser_executable:
            launch_options["executable_path"] = str(browser_executable)
            print(f"使用本机浏览器: {browser_executable}", flush=True)
        else:
            print("未找到本机 Chrome/Edge，尝试使用 Playwright 自带 Chromium。", flush=True)

        opened_browsers: list[Any] = []
        try:
            semaphore = asyncio.Semaphore(max(1, args.parallel))

            async def one(site: SiteConfig) -> dict[str, Any]:
                async with semaphore:
                    site_browser = None
                    try:
                        site_browser = await p.chromium.launch(**dict(launch_options))
                        opened_browsers.append(site_browser)
                        return await export_alarm_rules(site_browser, site, args)
                    except Exception as exc:
                        return {
                            "building": site.building,
                            "url": site.target_url,
                            "status": "failed",
                            "message": str(exc),
                        }

            results = await asyncio.gather(*(one(site) for site in sites))
            ok = [item for item in results if item["status"] == "exported"]
            pending = [item for item in results if item["status"] == "pending"]
            failed = [item for item in results if item["status"] not in {"exported", "pending"}]
            print(f"完成: exported={len(ok)}/{len(results)}, pending={len(pending)}, failed={len(failed)}", flush=True)
            for item in pending:
                print(f"  - {item['building']}: {item['message']}", flush=True)
            for item in failed:
                print(f"  - {item['building']}: {item['message']}", flush=True)

            if args.keep_open_sec > 0:
                print(f"浏览器保留 {args.keep_open_sec} 秒后退出。", flush=True)
                await asyncio.sleep(args.keep_open_sec)
            else:
                print("浏览器将保持打开。按 Ctrl+C 退出。", flush=True)
                await asyncio.Event().wait()
            return 0 if not failed and not pending else 1
        finally:
            for browser in opened_browsers:
                try:
                    await browser.close()
                except Exception:
                    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="打开各楼内网页面并导出工程配置中的告警规则。")
    parser.add_argument("--config", help="内网端 表格计算配置.json 路径；默认自动查找现有项目配置")
    parser.add_argument("--buildings", help="只运行指定楼栋，例如: A楼,B楼；默认运行全部启用楼栋")
    parser.add_argument("--period", help="导出月份 YYYY-MM；默认当前月")
    parser.add_argument("--list-sites", action="store_true", help="只列出楼栋和目标地址，不打开浏览器")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE), help="导出等待状态持久化文件，默认 .runtime/alarm_rule_export/export_records.json")
    parser.add_argument("--download-root", default="", help="下载根目录；默认使用内网端共享目录，未配置共享目录时回退 D:\\QLDownload")
    parser.add_argument("--screenshots-dir", default=str(DEFAULT_SCREENSHOT_DIR), help="失败截图目录")
    parser.add_argument("--parallel", type=int, default=5, help="并发楼栋数，默认 5；每个楼栋启动一个独立浏览器")
    parser.add_argument("--browser-executable", help="指定本机 Chrome/Edge 可执行文件路径")
    parser.add_argument("--use-playwright-chromium", action="store_true", help="强制使用 Playwright 自带 Chromium")
    parser.add_argument("--headless", action="store_true", help="无界面模式运行")
    parser.add_argument("--slow-mo-ms", type=int, default=0, help="Playwright 操作慢放毫秒数")
    parser.add_argument("--keep-open-sec", type=int, default=0, help="浏览器保留秒数；默认一直保留到 Ctrl+C")
    parser.add_argument("--navigation-timeout-ms", type=int, default=30000)
    parser.add_argument("--navigation-retries", type=int, default=2, help="页面打开失败后的重试次数，默认 2")
    parser.add_argument("--login-detect-timeout-ms", type=int, default=5000)
    parser.add_argument("--menu-timeout-ms", type=int, default=30000)
    parser.add_argument("--export-timeout-ms", type=int, default=30000)
    parser.add_argument("--iframe-stabilize-sec", type=float, default=1.0, help="导出弹窗 iframe 出现后等待稳定的秒数，默认 1")
    parser.add_argument("--frame-operation-retries", type=int, default=2, help="导出弹窗 iframe 刷新导致操作失败时的重试次数")
    parser.add_argument("--export-generate-timeout-ms", type=int, default=172800000, help="确认导出后等待列表生成文件的毫秒数，默认 48 小时")
    parser.add_argument("--generate-poll-interval-sec", type=float, default=86400.0, help="导出生成状态检查间隔秒数，默认 86400 秒")
    parser.add_argument("--wait-until-ready", action="store_true", help="持续等待导出生成完成；默认只检查一次，未完成交给下次日常检查")
    parser.add_argument("--download-button-timeout-ms", type=int, default=3000, help="单行下载按钮可见性等待毫秒数；超时则跳过该文件")
    parser.add_argument("--download-timeout-ms", type=int, default=60000, help="点击下载后等待文件下载的毫秒数")
    parser.add_argument("--delete-timeout-ms", type=int, default=10000, help="下载后点击删除并等待列表变化的毫秒数")
    parser.add_argument("--export-wait-sec", type=float, default=2.0, help="点击导出后等待秒数，默认 2")
    parser.add_argument("--action-timeout-ms", type=int, default=10000)
    return parser


def build_default_args(**overrides: Any) -> argparse.Namespace:
    args = build_parser().parse_args([])
    for key, value in overrides.items():
        if value is not None:
            setattr(args, key, value)
    return args


async def run_alarm_rule_export(
    config_path: str | Path | None = None,
    config: dict[str, Any] | None = None,
    buildings: str | None = None,
    period: str | None = None,
    parallel: int = 5,
    state_file: str | Path | None = None,
    download_root: str | Path | None = None,
    screenshots_dir: str | Path | None = None,
    browser_executable: str | Path | None = None,
    headless: bool = False,
    keep_open_sec: int = 1,
    **overrides: Any,
) -> int:
    """Programmatic entry point for integration into another Python project."""
    config_payload = config if isinstance(config, dict) else None
    default_state_file = state_file
    default_download_root = download_root
    default_screenshots_dir = screenshots_dir
    if config_payload is not None:
        runtime_root = _default_runtime_root(config_payload)
        if default_state_file is None:
            default_state_file = runtime_root / "alarm_rule_export" / "export_records.json"
        if default_screenshots_dir is None:
            default_screenshots_dir = runtime_root / "alarm_rule_export" / "screenshots"
        if default_download_root is None:
            default_download_root = _default_download_root(config_payload)
    args = build_default_args(
        config=str(config_path) if config_path else None,
        config_payload=config_payload,
        buildings=buildings,
        period=_resolve_period(period),
        parallel=parallel,
        state_file=str(default_state_file) if default_state_file else None,
        download_root=str(default_download_root) if default_download_root else None,
        screenshots_dir=str(default_screenshots_dir) if default_screenshots_dir else None,
        browser_executable=str(browser_executable) if browser_executable else None,
        headless=headless,
        keep_open_sec=keep_open_sec,
        **overrides,
    )
    return await _run(args)


def list_alarm_rule_export_sites(
    *,
    config_path: str | Path | None = None,
    config: dict[str, Any] | None = None,
    buildings: str | None = None,
) -> list[dict[str, str]]:
    payload = config if isinstance(config, dict) else _load_json(Path(config_path) if config_path else _resolve_default_config())
    sites = _select_sites(_extract_sites(payload), buildings)
    return [
        {
            "building": site.building,
            "host": site.host,
            "url": site.target_url,
            "username": site.username,
            "enabled": True,
        }
        for site in sites
    ]


def alarm_rule_export_status(
    *,
    config: dict[str, Any] | None = None,
    period: str | None = None,
    state_file: str | Path | None = None,
) -> dict[str, Any]:
    payload = config if isinstance(config, dict) else {}
    runtime_root = _default_runtime_root(payload)
    path = Path(state_file) if state_file else runtime_root / "alarm_rule_export" / "export_records.json"
    args = build_default_args(period=_resolve_period(period), state_file=str(path))
    state = _load_export_state(args)
    selected_period = _period_from_args(args)
    records = [
        item
        for item in state.get("records", [])
        if isinstance(item, dict) and item.get("period") == selected_period
    ]
    by_building: dict[str, list[dict[str, Any]]] = {}
    for item in records:
        by_building.setdefault(str(item.get("building") or ""), []).append(item)
    return {
        "period": selected_period,
        "state_file": str(path),
        "state_exists": path.exists(),
        "records": records,
        "by_building": by_building,
        "downloaded_count": sum(1 for item in records if item.get("status") == "downloaded"),
        "pending_count": sum(1 for item in records if item.get("status") in {"created", "generating", "ready", "missing"}),
    }


def _shared_root_from_config(config: dict[str, Any] | None = None) -> Path | None:
    root_config = config or {}
    common = root_config.get("common", {}) if isinstance(root_config, dict) else {}
    shared_bridge = root_config.get("shared_bridge", {}) if isinstance(root_config, dict) else {}
    if not isinstance(shared_bridge, dict) or not shared_bridge:
        shared_bridge = common.get("shared_bridge", {}) if isinstance(common, dict) else {}
    if not isinstance(shared_bridge, dict):
        return None
    root_text = str(shared_bridge.get("root_dir") or shared_bridge.get("internal_root_dir") or "").strip()
    return Path(root_text) if root_text else None


def _relative_to_shared_root(config: dict[str, Any] | None, file_path: Path) -> str:
    shared_root = _shared_root_from_config(config)
    if shared_root is None:
        return ""
    try:
        return file_path.resolve().relative_to(shared_root.resolve()).as_posix()
    except Exception:
        return ""


def list_alarm_rule_export_files(
    *,
    config: dict[str, Any] | None = None,
    period: str | None = None,
    building: str = "",
    state_file: str | Path | None = None,
) -> dict[str, Any]:
    status = alarm_rule_export_status(config=config, period=period, state_file=state_file)
    target_building = str(building or "").strip()
    files: list[dict[str, Any]] = []
    for item in status.get("records", []):
        if not isinstance(item, dict):
            continue
        if item.get("status") != "downloaded":
            continue
        if target_building and str(item.get("building") or "").strip() != target_building:
            continue
        file_name = str(item.get("file_name") or "").strip()
        path_text = str(item.get("downloaded_path") or "").strip()
        file_path = Path(path_text) if path_text else None
        exists = bool(file_path and file_path.exists() and file_path.is_file())
        size_bytes = file_path.stat().st_size if exists and file_path is not None else 0
        files.append(
            {
                "building": str(item.get("building") or "").strip(),
                "period": str(item.get("period") or "").strip(),
                "file_name": file_name,
                "downloaded_at": str(item.get("downloaded_at") or "").strip(),
                "downloaded_path": path_text,
                "relative_path": _relative_to_shared_root(config, file_path) if file_path is not None else "",
                "file_exists": exists,
                "size_bytes": size_bytes,
                "deleted_from_page": bool(item.get("deleted_from_page")),
            }
        )
    return {
        "period": status.get("period"),
        "state_file": status.get("state_file"),
        "files": files,
        "count": len(files),
    }


def resolve_alarm_rule_export_file(
    *,
    config: dict[str, Any] | None = None,
    period: str | None = None,
    building: str,
    file_name: str,
    state_file: str | Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    target_building = str(building or "").strip()
    target_file_name = str(file_name or "").strip()
    if not target_building:
        raise FileNotFoundError("building 不能为空")
    if not target_file_name:
        raise FileNotFoundError("file_name 不能为空")
    listing = list_alarm_rule_export_files(
        config=config,
        period=period,
        building=target_building,
        state_file=state_file,
    )
    for item in listing.get("files", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("file_name") or "").strip() != target_file_name:
            continue
        path = Path(str(item.get("downloaded_path") or "").strip())
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"告警规则导出文件不存在: {target_file_name}")
        return path, item
    raise FileNotFoundError(f"未找到已下载的告警规则导出文件: {target_building}/{target_file_name}")


def reset_alarm_rule_export_building(
    *,
    config: dict[str, Any] | None = None,
    building: str,
    period: str | None = None,
    state_file: str | Path | None = None,
) -> dict[str, Any]:
    payload = config if isinstance(config, dict) else {}
    runtime_root = _default_runtime_root(payload)
    path = Path(state_file) if state_file else runtime_root / "alarm_rule_export" / "export_records.json"
    args = build_default_args(period=_resolve_period(period), state_file=str(path))
    state = _load_export_state(args)
    selected_period = _period_from_args(args)
    target_building = str(building or "").strip()
    kept: list[dict[str, Any]] = []
    removed = 0
    for item in state.get("records", []):
        if (
            isinstance(item, dict)
            and item.get("building") == target_building
            and item.get("period") == selected_period
            and item.get("status") == "downloaded"
        ):
            removed += 1
            continue
        if isinstance(item, dict):
            kept.append(item)
    state["records"] = kept
    _save_export_state(args, state)
    return {
        "period": selected_period,
        "building": target_building,
        "removed_downloaded_records": removed,
        "state_file": str(path),
    }


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\n已退出。")
        return 130
    except Exception as exc:
        print(f"运行失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
