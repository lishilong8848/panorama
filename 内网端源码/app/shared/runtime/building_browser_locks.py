from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional


_GLOBAL_LOCK = threading.RLock()
_LOCKS: Dict[str, threading.Lock] = {}
_OWNERS: Dict[str, Dict[str, Any]] = {}


def _normalize_building(building: str) -> str:
    return str(building or "").strip() or "-"


def _get_lock(building: str) -> threading.Lock:
    key = _normalize_building(building)
    with _GLOBAL_LOCK:
        lock = _LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _LOCKS[key] = lock
        return lock


def acquire_building_browser_lock(
    building: str,
    *,
    owner: str,
    timeout_sec: Optional[float] = None,
) -> bool:
    key = _normalize_building(building)
    lock = _get_lock(key)
    started = time.time()
    if timeout_sec is None:
        acquired = lock.acquire()
    else:
        acquired = lock.acquire(timeout=max(0.0, float(timeout_sec)))
    if acquired:
        with _GLOBAL_LOCK:
            _OWNERS[key] = {
                "building": key,
                "owner": str(owner or "-"),
                "acquired_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "wait_ms": int((time.time() - started) * 1000),
            }
    return bool(acquired)


def release_building_browser_lock(building: str) -> None:
    key = _normalize_building(building)
    with _GLOBAL_LOCK:
        lock = _LOCKS.get(key)
        _OWNERS.pop(key, None)
    if lock is not None and lock.locked():
        lock.release()


def building_browser_lock_snapshot() -> Dict[str, Any]:
    with _GLOBAL_LOCK:
        owners = {key: dict(value) for key, value in _OWNERS.items()}
        locked = sorted(owners)
    return {
        "locked_buildings": locked,
        "owners": owners,
        "locked_count": len(locked),
    }
