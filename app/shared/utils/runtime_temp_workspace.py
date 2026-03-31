from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import re
import shutil
import uuid
from typing import Any

from pipeline_utils import get_app_dir


def _extract_runtime_state_root_text(runtime_config: dict[str, Any] | None) -> str:
    runtime = runtime_config if isinstance(runtime_config, dict) else {}
    paths_cfg = runtime.get("paths", {})
    if isinstance(paths_cfg, dict):
        return str(paths_cfg.get("runtime_state_root", "") or "").strip()
    return ""


def resolve_runtime_state_root(
    *,
    runtime_config: dict[str, Any] | None = None,
    app_dir: Path | None = None,
) -> Path:
    base_dir = app_dir or get_app_dir()
    runtime_root_text = _extract_runtime_state_root_text(runtime_config)
    runtime_root = Path(runtime_root_text) if runtime_root_text else base_dir / ".runtime"
    if not runtime_root.is_absolute():
        runtime_root = base_dir / runtime_root
    runtime_root.mkdir(parents=True, exist_ok=True)
    return runtime_root


def resolve_runtime_temp_root(
    *,
    runtime_config: dict[str, Any] | None = None,
    app_dir: Path | None = None,
) -> Path:
    temp_root = resolve_runtime_state_root(runtime_config=runtime_config, app_dir=app_dir) / "temp"
    temp_root.mkdir(parents=True, exist_ok=True)
    return temp_root


def _sanitize_kind(kind: str) -> str:
    text = str(kind or "").strip().lower()
    if not text:
        return "task"
    sanitized = re.sub(r"[^a-z0-9_.-]+", "_", text)
    sanitized = sanitized.strip("._-")
    return sanitized or "task"


def create_runtime_temp_dir(
    *,
    kind: str,
    runtime_config: dict[str, Any] | None = None,
    app_dir: Path | None = None,
) -> Path:
    prune_stale_runtime_temp_dirs(runtime_config=runtime_config, app_dir=app_dir)
    temp_root = resolve_runtime_temp_root(runtime_config=runtime_config, app_dir=app_dir)
    safe_kind = _sanitize_kind(kind)
    folder_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    workspace = temp_root / safe_kind / folder_name
    workspace.mkdir(parents=True, exist_ok=True)
    return workspace


def _is_within(parent: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(parent.resolve())
        return True
    except Exception:  # noqa: BLE001
        return False


def _cleanup_empty_parent_dirs(path: Path, stop_at: Path) -> None:
    current = path
    stop_resolved = stop_at.resolve()
    while current.exists():
        try:
            current_resolved = current.resolve()
        except Exception:  # noqa: BLE001
            return
        if current_resolved == stop_resolved:
            return
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def cleanup_runtime_temp_dir(
    path: Path,
    *,
    runtime_config: dict[str, Any] | None = None,
    app_dir: Path | None = None,
) -> None:
    candidate = Path(path)
    if not candidate.exists():
        return
    temp_root = resolve_runtime_temp_root(runtime_config=runtime_config, app_dir=app_dir)
    if not _is_within(temp_root, candidate):
        return
    shutil.rmtree(candidate, ignore_errors=True)
    _cleanup_empty_parent_dirs(candidate.parent, temp_root)


def prune_stale_runtime_temp_dirs(
    *,
    runtime_config: dict[str, Any] | None = None,
    app_dir: Path | None = None,
    older_than_hours: int = 72,
) -> int:
    temp_root = resolve_runtime_temp_root(runtime_config=runtime_config, app_dir=app_dir)
    cutoff = datetime.now() - timedelta(hours=max(1, int(older_than_hours or 72)))
    removed = 0
    for kind_dir in temp_root.iterdir():
        if not kind_dir.is_dir():
            continue
        for workspace in kind_dir.iterdir():
            if not workspace.is_dir():
                continue
            try:
                modified_at = datetime.fromtimestamp(workspace.stat().st_mtime)
            except OSError:
                continue
            if modified_at >= cutoff:
                continue
            shutil.rmtree(workspace, ignore_errors=True)
            removed += 1
        _cleanup_empty_parent_dirs(kind_dir, temp_root)
    return removed
