from __future__ import annotations

import threading
from typing import Any


_LOCK = threading.Lock()
_POOL: Any = None


def set_internal_download_browser_pool(pool: Any) -> None:
    global _POOL
    with _LOCK:
        _POOL = pool


def get_internal_download_browser_pool() -> Any:
    with _LOCK:
        return _POOL


def clear_internal_download_browser_pool(pool: Any | None = None) -> None:
    global _POOL
    with _LOCK:
        if pool is None or _POOL is pool:
            _POOL = None
