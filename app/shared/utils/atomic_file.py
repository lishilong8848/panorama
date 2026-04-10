from __future__ import annotations

import json
import os
import shutil
import threading
import time
import uuid
from pathlib import Path
from typing import Callable

import openpyxl
from PIL import Image


PathValidator = Callable[[Path], None]
PathWriter = Callable[[Path], None]

_WINDOWS_REPLACE_RETRY_DELAYS_SEC = (0.05, 0.1, 0.2, 0.35, 0.5, 0.8, 1.2)
_ATOMIC_WRITE_LOCKS_GUARD = threading.Lock()
_ATOMIC_WRITE_LOCKS: dict[str, threading.RLock] = {}


def _target_lock_for_path(path: Path) -> threading.RLock:
    normalized = str(path.resolve(strict=False)).casefold()
    with _ATOMIC_WRITE_LOCKS_GUARD:
        lock = _ATOMIC_WRITE_LOCKS.get(normalized)
        if lock is None:
            lock = threading.RLock()
            _ATOMIC_WRITE_LOCKS[normalized] = lock
        return lock


def _build_temp_path(target: Path, *, temp_suffix: str) -> Path:
    suffix = str(temp_suffix or ".tmp").strip() or ".tmp"
    if not suffix.startswith("."):
        suffix = f".{suffix}"
    final_suffix = "".join(target.suffixes)
    base_name = target.name[: -len(final_suffix)] if final_suffix else target.name
    temp_name = f".{base_name}.{uuid.uuid4().hex}{suffix}{final_suffix}"
    return target.parent / temp_name


def _is_retryable_replace_error(exc: BaseException) -> bool:
    if isinstance(exc, PermissionError):
        return True
    if isinstance(exc, OSError):
        return getattr(exc, "winerror", None) in {5, 32}
    return False


def _replace_with_retry(
    temp_path: Path,
    target: Path,
    *,
    allow_overwrite_fallback: bool = True,
) -> None:
    attempts = len(_WINDOWS_REPLACE_RETRY_DELAYS_SEC) + 1
    for index in range(attempts):
        try:
            os.replace(temp_path, target)
            return
        except Exception as exc:
            if index >= attempts - 1 or not _is_retryable_replace_error(exc):
                if (
                    index >= attempts - 1
                    and _is_retryable_replace_error(exc)
                    and allow_overwrite_fallback
                ):
                    _overwrite_from_temp_with_retry(temp_path, target)
                    return
                raise
            time.sleep(_WINDOWS_REPLACE_RETRY_DELAYS_SEC[index])


def _overwrite_from_temp_with_retry(temp_path: Path, target: Path) -> None:
    attempts = len(_WINDOWS_REPLACE_RETRY_DELAYS_SEC) + 1
    for index in range(attempts):
        try:
            data = temp_path.read_bytes()
            with target.open("wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            return
        except Exception as exc:
            if index >= attempts - 1 or not _is_retryable_replace_error(exc):
                raise
            time.sleep(_WINDOWS_REPLACE_RETRY_DELAYS_SEC[index])


def atomic_write_file(
    path: str | Path,
    writer: PathWriter,
    *,
    validator: PathValidator | None = None,
    temp_suffix: str = ".tmp",
    allow_overwrite_fallback: bool = True,
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = _build_temp_path(target, temp_suffix=temp_suffix)
    target_lock = _target_lock_for_path(target)
    with target_lock:
        try:
            writer(temp_path)
            if not temp_path.exists():
                raise FileNotFoundError(f"atomic writer did not create temp file: {temp_path}")
            if validator is not None:
                validator(temp_path)
            _replace_with_retry(
                temp_path,
                target,
                allow_overwrite_fallback=allow_overwrite_fallback,
            )
            return target
        except Exception:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception:
                pass
            raise


def atomic_write_bytes(
    path: str | Path,
    content: bytes,
    *,
    validator: PathValidator | None = None,
    temp_suffix: str = ".tmp",
    allow_overwrite_fallback: bool = True,
) -> Path:
    return atomic_write_file(
        path,
        lambda temp_path: temp_path.write_bytes(content),
        validator=validator,
        temp_suffix=temp_suffix,
        allow_overwrite_fallback=allow_overwrite_fallback,
    )


def atomic_write_text(
    path: str | Path,
    content: str,
    *,
    encoding: str = "utf-8",
    validator: PathValidator | None = None,
    temp_suffix: str = ".tmp",
    allow_overwrite_fallback: bool = True,
) -> Path:
    return atomic_write_file(
        path,
        lambda temp_path: temp_path.write_text(content, encoding=encoding),
        validator=validator,
        temp_suffix=temp_suffix,
        allow_overwrite_fallback=allow_overwrite_fallback,
    )


def atomic_copy_file(
    source_path: str | Path,
    target_path: str | Path,
    *,
    validator: PathValidator | None = None,
    temp_suffix: str = ".tmp",
    allow_overwrite_fallback: bool = True,
) -> Path:
    source = Path(source_path)
    if not source.exists():
        raise FileNotFoundError(f"source file missing: {source}")
    return atomic_write_file(
        target_path,
        lambda temp_path: shutil.copy2(source, temp_path),
        validator=validator,
        temp_suffix=temp_suffix,
        allow_overwrite_fallback=allow_overwrite_fallback,
    )


def atomic_save_workbook(
    workbook,
    output_path: str | Path,
    *,
    validator: PathValidator | None = None,
    temp_suffix: str = ".tmp",
    allow_overwrite_fallback: bool = True,
) -> Path:
    return atomic_write_file(
        output_path,
        lambda temp_path: workbook.save(temp_path),
        validator=validator or validate_excel_workbook_file,
        temp_suffix=temp_suffix,
        allow_overwrite_fallback=allow_overwrite_fallback,
    )


def validate_non_empty_file(path: str | Path) -> None:
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"file missing: {target}")
    if target.stat().st_size <= 0:
        raise ValueError(f"file is empty: {target}")


def validate_json_file(path: str | Path) -> None:
    target = Path(path)
    validate_non_empty_file(target)
    json.loads(target.read_text(encoding="utf-8"))


def validate_excel_workbook_file(path: str | Path) -> None:
    target = Path(path)
    validate_non_empty_file(target)
    workbook = openpyxl.load_workbook(target, read_only=True, data_only=False)
    workbook.close()


def validate_image_file(path: str | Path) -> None:
    target = Path(path)
    validate_non_empty_file(target)
    with Image.open(target) as image:
        image.verify()
