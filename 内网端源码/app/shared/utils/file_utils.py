from __future__ import annotations

import ntpath
import os
import re
from pathlib import Path
from typing import Callable


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def normalize_windows_path_text(path: str | Path | None) -> str:
    text = str(path or "").strip()
    if not text:
        return ""
    expanded = os.path.expandvars(os.path.expanduser(text))
    try:
        return ntpath.normpath(expanded)
    except Exception:
        return expanded.replace("/", "\\")


def _mapped_drive_unc_root(drive: str) -> str:
    drive_text = str(drive or "").strip().upper()
    if not re.fullmatch(r"[A-Z]:", drive_text):
        return ""
    try:
        import ctypes

        buffer_length = 2048
        remote_name = ctypes.create_unicode_buffer(buffer_length)
        length = ctypes.c_uint(buffer_length)
        result = ctypes.windll.mpr.WNetGetConnectionW(  # type: ignore[attr-defined]
            drive_text,
            remote_name,
            ctypes.byref(length),
        )
        if int(result or 0) == 0:
            return normalize_windows_path_text(remote_name.value)
    except Exception:
        return ""
    return ""


def resolve_windows_network_path(path: str | Path | None) -> str:
    normalized = normalize_windows_path_text(path)
    if not normalized:
        return ""
    drive_match = re.match(r"^([A-Za-z]:)(.*)$", normalized)
    if drive_match:
        unc_root = _mapped_drive_unc_root(drive_match.group(1))
        if unc_root:
            suffix = str(drive_match.group(2) or "").lstrip("\\/")
            joined = ntpath.join(unc_root, suffix) if suffix else unc_root
            return normalize_windows_path_text(joined)
    return normalized


def canonicalize_windows_path_for_compare(path: str | Path | None) -> str:
    normalized = resolve_windows_network_path(path)
    if not normalized:
        return ""
    if normalized.startswith("\\\\"):
        return normalized.rstrip("\\/").casefold()
    drive_match = re.match(r"^([A-Za-z]:)(.*)$", normalized)
    if drive_match:
        drive = drive_match.group(1).upper()
        suffix = str(drive_match.group(2) or "").rstrip("\\/")
        return f"{drive}{suffix}".casefold()
    return normalized.rstrip("\\/").casefold()


def windows_paths_point_to_same_location(left: str | Path | None, right: str | Path | None) -> bool:
    left_key = canonicalize_windows_path_for_compare(left)
    right_key = canonicalize_windows_path_for_compare(right)
    return bool(left_key) and left_key == right_key


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
