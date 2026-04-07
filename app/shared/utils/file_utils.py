from __future__ import annotations

import re
from pathlib import Path
from typing import Callable


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _windows_drive_available(drive: str) -> bool:
    drive_text = str(drive or "").strip()
    if not re.fullmatch(r"[A-Za-z]:", drive_text):
        return False
    try:
        import ctypes

        drive_type = ctypes.windll.kernel32.GetDriveTypeW(f"{drive_text}\\")  # type: ignore[attr-defined]
        return int(drive_type or 0) > 1
    except Exception:
        drive_root = Path(f"{drive_text}\\")
        try:
            return drive_root.exists()
        except OSError:
            return False


def fallback_missing_windows_drive_path(
    path: str | Path,
    *,
    app_dir: str | Path,
    emit_log: Callable[[str], None] | None = None,
    label: str = "输出目录",
) -> Path:
    resolved = Path(path)
    app_root = Path(app_dir)
    if not resolved.is_absolute():
        return resolved
    raw_text = str(resolved)
    if raw_text.startswith("\\\\"):
        return resolved
    drive = str(resolved.drive or "").strip()
    if not drive:
        return resolved
    if _windows_drive_available(drive):
        return resolved
    tail_parts = [part for idx, part in enumerate(resolved.parts) if idx > 0 and str(part).strip()]
    fallback = app_root.joinpath(*tail_parts) if tail_parts else app_root
    if emit_log is not None:
        emit_log(
            f"[路径回退] {label} 配置路径不可用: {resolved}，"
            f"已自动回退到程序目录: {fallback}"
        )
    return fallback
