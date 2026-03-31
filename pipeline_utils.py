from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


DEFAULT_CONFIG_FILENAME = "表格计算配置.json"
DEFAULT_CALC_FILENAME = "表格计算部分代码.py"
DEFAULT_DOWNLOAD_FILENAME = "下载动环表格.py"
DEFAULT_WEBHOOK_KEYWORD = "事件"


def get_app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def get_bundle_dir() -> Path:
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(str(meipass)).resolve()
    return Path(__file__).resolve().parent


def _expand_candidate_path(raw_path: str | Path) -> List[Path]:
    path = Path(raw_path)
    if path.is_absolute():
        return [path]
    return [
        Path.cwd() / path,
        get_app_dir() / path,
        get_bundle_dir() / path,
    ]


def _pick_first_existing(candidates: Iterable[Path]) -> Path | None:
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate
    return None


def _resolve_single_config_target() -> Path:
    return get_app_dir() / DEFAULT_CONFIG_FILENAME


def _migrate_legacy_config_if_needed(target: Path) -> Path | None:
    if target.exists():
        return target

    candidates: List[Path] = [
        get_app_dir() / "config" / DEFAULT_CONFIG_FILENAME,
    ]
    if getattr(sys, "frozen", False):
        candidates.append(get_bundle_dir() / "config" / DEFAULT_CONFIG_FILENAME)

    legacy = _pick_first_existing(candidates)
    if legacy is None:
        return None

    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(legacy, target)
    print(f"[配置迁移] 已迁移旧配置: {legacy} -> {target}")
    return target


def resolve_config_path(config_path: str | Path | None = None) -> Path:
    if config_path is not None:
        candidates = _expand_candidate_path(config_path)
        resolved = _pick_first_existing(candidates)
        if resolved is None:
            probed = "\n".join(str(p) for p in candidates)
            raise FileNotFoundError(f"Config file not found. Tried:\n{probed}")
        return resolved

    env_path = os.getenv("MONTHLY_REPORT_CONFIG", "").strip()
    if env_path:
        candidates = _expand_candidate_path(env_path)
        resolved = _pick_first_existing(candidates)
        if resolved is None:
            probed = "\n".join(str(p) for p in candidates)
            raise FileNotFoundError(f"Config file not found. Tried:\n{probed}")
        return resolved

    target = _resolve_single_config_target()
    if target.exists():
        return target

    migrated = _migrate_legacy_config_if_needed(target)
    if migrated is not None and migrated.exists():
        return migrated

    raise FileNotFoundError(
        "Config file not found. Expected single config at:\n"
        f"{target}\n"
        "Legacy checked for one-time migration:\n"
        f"{get_app_dir() / 'config' / DEFAULT_CONFIG_FILENAME}\n"
        f"{get_bundle_dir() / 'config' / DEFAULT_CONFIG_FILENAME}"
    )


def resolve_calc_path(calc_path: str | Path | None = None) -> Path:
    candidates: List[Path] = []
    if calc_path is not None:
        candidates.extend(_expand_candidate_path(calc_path))

    env_path = os.getenv("MONTHLY_REPORT_CALC", "").strip()
    if env_path:
        candidates.extend(_expand_candidate_path(env_path))

    candidates.extend(
        [
            get_app_dir() / DEFAULT_CALC_FILENAME,
            Path.cwd() / DEFAULT_CALC_FILENAME,
            get_bundle_dir() / DEFAULT_CALC_FILENAME,
        ]
    )

    resolved = _pick_first_existing(candidates)
    if resolved is None:
        probed = "\n".join(str(p) for p in candidates)
        raise FileNotFoundError(f"Calculation script not found. Tried:\n{probed}")
    return resolved


def resolve_download_path(download_path: str | Path | None = None) -> Path:
    candidates: List[Path] = []
    if download_path is not None:
        candidates.extend(_expand_candidate_path(download_path))

    env_path = os.getenv("MONTHLY_REPORT_PIPELINE", "").strip()
    if env_path:
        candidates.extend(_expand_candidate_path(env_path))

    candidates.extend(
        [
            get_app_dir() / DEFAULT_DOWNLOAD_FILENAME,
            Path.cwd() / DEFAULT_DOWNLOAD_FILENAME,
            get_bundle_dir() / DEFAULT_DOWNLOAD_FILENAME,
        ]
    )

    resolved = _pick_first_existing(candidates)
    if resolved is None:
        probed = "\n".join(str(p) for p in candidates)
        raise FileNotFoundError(f"Pipeline script not found. Tried:\n{probed}")
    return resolved


def load_pipeline_config(config_path: str | Path | None = None) -> Dict[str, Any]:
    resolved = resolve_config_path(config_path)
    try:
        # Use utf-8-sig so config files saved with BOM can still be parsed.
        with resolved.open("r", encoding="utf-8-sig") as f:
            return json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"配置文件解析失败: {resolved} ({exc})") from exc


def _load_module(module_name: str, source_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(source_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module: {source_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_calc_module(calc_path: str | Path | None = None):
    return _load_module("monthly_calc_runtime_module", resolve_calc_path(calc_path))


def load_download_module(download_path: str | Path | None = None):
    return _load_module("monthly_pipeline_runtime_module", resolve_download_path(download_path))


def get_last_month_window(now: datetime | None = None) -> Tuple[str, str]:
    now = now or datetime.now()
    this_month_start = datetime(now.year, now.month, 1, 0, 0, 0)
    if this_month_start.month == 1:
        prev_month_start = datetime(this_month_start.year - 1, 12, 1, 0, 0, 0)
    else:
        prev_month_start = datetime(this_month_start.year, this_month_start.month - 1, 1, 0, 0, 0)
    return (
        prev_month_start.strftime("%Y-%m-%d %H:%M:%S"),
        this_month_start.strftime("%Y-%m-%d %H:%M:%S"),
    )


def ensure_keyword(text: str, keyword: str) -> str:
    if not keyword:
        return text
    if keyword in text:
        return text
    return f"{keyword}：{text}"


def send_feishu_webhook(
    webhook_url: str,
    text: str,
    keyword: str = DEFAULT_WEBHOOK_KEYWORD,
    timeout: int = 10,
) -> Tuple[bool, str]:
    if not webhook_url:
        return False, "Webhook is not configured."
    payload = {"msg_type": "text", "content": {"text": ensure_keyword(text, keyword)}}
    try:
        import requests

        response = requests.post(webhook_url, json=payload, timeout=timeout)
        response.raise_for_status()
        body = response.json()
        code = body.get("code")
        if code not in (0, "0", None):
            return False, f"Webhook returned error: {body}"
        return True, "ok"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def build_event_text(stage: str, detail: str, building: str | None = None) -> str:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    building_text = f"楼栋: {building}\n" if building else ""
    return (
        "事件：全景月报任务异常\n"
        f"时间: {now_text}\n"
        f"阶段: {stage}\n"
        f"{building_text}"
        f"详情: {detail}\n"
        "处理建议: 可在《全景月报Web控制台》中选择对应功能进行补传。"
    )


def configure_playwright_environment(config: Dict[str, Any] | None = None) -> Path | None:
    env_existing = os.getenv("PLAYWRIGHT_BROWSERS_PATH", "").strip()
    if env_existing:
        path = Path(env_existing)
        if path.exists():
            return path

    candidates: List[Path] = []
    if config is not None:
        raw = str(config.get("download", {}).get("playwright_browsers_path", "")).strip()
        if raw:
            candidates.extend(_expand_candidate_path(raw))

    app_dir = get_app_dir()
    bundle_dir = get_bundle_dir()
    candidates.extend(
        [
            app_dir / "ms-playwright",
            app_dir.parent / "ms-playwright",
            Path.cwd() / "ms-playwright",
            bundle_dir / "ms-playwright",
        ]
    )

    path = _pick_first_existing(candidates)
    if path is None:
        return None
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(path)
    return path
