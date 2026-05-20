from __future__ import annotations

import copy
import json
import os
import threading
import time
from pathlib import Path
from typing import Any


_CACHE_LOCK = threading.RLock()
_CACHE: dict[str, tuple[int | None, Any]] = {}
_PATH_LOCKS_GUARD = threading.Lock()
_PATH_LOCKS: dict[str, threading.RLock] = {}


def _cache_key(path: str | Path) -> str:
    return str(Path(path).resolve(strict=False)).casefold()


def _lock_for_path(path: str | Path) -> threading.RLock:
    key = _cache_key(path)
    with _PATH_LOCKS_GUARD:
        lock = _PATH_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _PATH_LOCKS[key] = lock
        return lock


def _mtime_ns(path: Path) -> int | None:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return None


def _set_cache(path: Path, payload: Any, mtime_ns: int | None = None) -> tuple[int | None, Any] | None:
    entry = (mtime_ns if mtime_ns is not None else _mtime_ns(path), copy.deepcopy(payload))
    with _CACHE_LOCK:
        previous = _CACHE.get(_cache_key(path))
        _CACHE[_cache_key(path)] = entry
        return previous


def _restore_cache(path: Path, previous: tuple[int | None, Any] | None) -> None:
    with _CACHE_LOCK:
        key = _cache_key(path)
        if previous is None:
            _CACHE.pop(key, None)
        else:
            _CACHE[key] = previous


def load_cached_json(path: str | Path, default: Any = None, *, encoding: str = "utf-8-sig") -> Any:
    target = Path(path)
    with _lock_for_path(target):
        mtime_ns = _mtime_ns(target)
        key = _cache_key(target)
        with _CACHE_LOCK:
            cached = _CACHE.get(key)
            if cached is not None and (cached[0] is None or cached[0] == mtime_ns):
                return copy.deepcopy(cached[1])
        if not target.exists():
            return copy.deepcopy(default)
        try:
            payload = json.loads(target.read_text(encoding=encoding))
        except Exception:  # noqa: BLE001
            return copy.deepcopy(default)
        _set_cache(target, payload, mtime_ns)
        return copy.deepcopy(payload)


def save_cached_json(
    path: str | Path,
    payload: Any,
    *,
    indent: int | None = 2,
    encoding: str = "utf-8",
    ensure_ascii: bool = False,
) -> Path:
    target = Path(path)
    with _lock_for_path(target):
        target.parent.mkdir(parents=True, exist_ok=True)
        previous = _set_cache(target, payload, None)
        tmp_path = target.with_name(
            f".{target.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp"
        )
        try:
            text = json.dumps(payload, ensure_ascii=ensure_ascii, indent=indent)
            with tmp_path.open("w", encoding=encoding, newline="\n") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, target)
        except Exception:
            _restore_cache(target, previous)
            try:
                tmp_path.unlink()
            except OSError:
                pass
            raise
        _set_cache(target, payload, _mtime_ns(target))
        return target
