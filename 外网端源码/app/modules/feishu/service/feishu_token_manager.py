from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict

import requests


AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
TOKEN_TTL_SEC = 7200
TOKEN_REFRESH_MARGIN_SEC = 600


@dataclass
class _TokenEntry:
    token: str = ""
    expires_at_monotonic: float = 0.0


class FeishuTokenManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: Dict[str, _TokenEntry] = {}
        self._refresh_locks: Dict[str, threading.Lock] = {}

    @staticmethod
    def _cache_key(app_id: str, app_secret: str) -> str:
        return f"{str(app_id or '').strip()}::{str(app_secret or '').strip()}"

    @staticmethod
    def _is_entry_valid(entry: _TokenEntry) -> bool:
        return bool(entry.token) and time.monotonic() < entry.expires_at_monotonic - TOKEN_REFRESH_MARGIN_SEC

    def invalidate(self, *, app_id: str, app_secret: str) -> None:
        key = self._cache_key(app_id, app_secret)
        with self._lock:
            self._entries.pop(key, None)

    def get_token(
        self,
        *,
        app_id: str,
        app_secret: str,
        timeout: int,
        force_refresh: bool = False,
    ) -> str:
        app_id_text = str(app_id or "").strip()
        app_secret_text = str(app_secret or "").strip()
        if not app_id_text or not app_secret_text:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")
        key = self._cache_key(app_id_text, app_secret_text)
        if not force_refresh:
            with self._lock:
                entry = self._entries.get(key)
                if entry is not None and self._is_entry_valid(entry):
                    return entry.token
        with self._lock:
            refresh_lock = self._refresh_locks.get(key)
            if refresh_lock is None:
                refresh_lock = threading.Lock()
                self._refresh_locks[key] = refresh_lock
        with refresh_lock:
            if not force_refresh:
                with self._lock:
                    entry = self._entries.get(key)
                    if entry is not None and self._is_entry_valid(entry):
                        return entry.token
            token = self._fetch_token(app_id=app_id_text, app_secret=app_secret_text, timeout=timeout)
            with self._lock:
                self._entries[key] = _TokenEntry(
                    token=token,
                    expires_at_monotonic=time.monotonic() + TOKEN_TTL_SEC,
                )
            return token

    @staticmethod
    def _fetch_token(*, app_id: str, app_secret: str, timeout: int) -> str:
        try:
            response = requests.post(
                AUTH_URL,
                json={"app_id": app_id, "app_secret": app_secret},
                headers={"Content-Type": "application/json; charset=utf-8", "Connection": "close"},
                timeout=max(1, int(timeout or 30)),
            )
            response.raise_for_status()
            payload: Dict[str, Any] = response.json()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"飞书获取 token 失败: {exc}") from exc
        if payload.get("code") != 0:
            raise RuntimeError(f"飞书获取 token 失败: {payload}")
        token = str(payload.get("tenant_access_token", "") or "").strip()
        if not token:
            raise RuntimeError("飞书获取 token 失败: tenant_access_token 为空")
        return token


feishu_token_manager = FeishuTokenManager()
