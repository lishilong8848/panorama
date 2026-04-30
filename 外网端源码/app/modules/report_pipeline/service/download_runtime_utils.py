from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse


def extract_site_host(raw_value: Any) -> str:
    raw = str(raw_value).strip()
    if not raw:
        return ""
    candidate = raw if raw.startswith("http://") or raw.startswith("https://") else f"http://{raw}"
    parsed = urlparse(candidate)
    host = (parsed.hostname or parsed.netloc or "").strip()
    return host


def resolve_site_urls(site: Dict[str, Any]) -> List[str]:
    host = extract_site_host(site.get("host", "")) or extract_site_host(site.get("url", ""))
    if not host:
        return []
    return [f"http://{host}/page/main/main.html"]


def resolve_run_save_dir(download_cfg: Dict[str, Any]) -> str:
    base_dir_text = str(download_cfg.get("save_dir", "")).strip()
    if not base_dir_text:
        raise ValueError("配置错误: download.save_dir 不能为空")

    base_dir = Path(base_dir_text)
    mode = str(download_cfg.get("run_subdir_mode", "")).strip().lower()
    if mode in {"none", "off", "false", "0"}:
        target_dir = base_dir
    else:
        prefix = str(download_cfg.get("run_subdir_prefix", "")).strip()
        if not prefix:
            raise ValueError("配置错误: download.run_subdir_prefix 不能为空")
        run_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
        target_dir = base_dir / f"{prefix}{run_tag}"

    target_dir.mkdir(parents=True, exist_ok=True)
    return str(target_dir)


def get_multi_date_max(download_cfg: Dict[str, Any], default_value: int = 31) -> int:
    raw = download_cfg.get("multi_date", {})
    if isinstance(raw, dict) and "max_dates_per_run" in raw:
        return int(raw["max_dates_per_run"])
    return int(default_value)


def is_retryable_download_timeout(error_text: str) -> bool:
    text = str(error_text or "").strip().lower()
    if "timeout" not in text:
        return False
    if "page.fill" in text:
        return True
    if "wait_for_function" in text:
        return True
    if "wait_for_selector" in text:
        return True
    if "locator.wait_for" in text:
        return True
    return True


def group_download_tasks_by_building(download_tasks: List[Any]) -> List[Tuple[str, List[Any]]]:
    grouped: Dict[str, List[Any]] = {}
    order: List[str] = []
    for task in download_tasks:
        site = getattr(task, "site", {})
        building = str(site.get("building", "")).strip() if isinstance(site, dict) else ""
        if not building:
            continue
        if building not in grouped:
            grouped[building] = []
            order.append(building)
        grouped[building].append(task)
    for building in order:
        grouped[building].sort(
            key=lambda item: (
                str(getattr(item, "date_text", "")),
                str(getattr(item, "start_time", "")),
                str(getattr(item, "end_time", "")),
            )
        )
    return [(building, grouped[building]) for building in order]
