from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, List, Tuple

from app.config.settings_loader import get_settings_path, load_settings


SENSITIVE_KEYS = {"password", "app_secret", "feishu_webhook_url", "seniverse_private_key"}


def _mask_string(text: str) -> str:
    if not text:
        return text
    if len(text) <= 6:
        return "*" * len(text)
    return f"{'*' * (len(text) - 4)}{text[-4:]}"


def is_sensitive_key(key: str) -> bool:
    return key.casefold() in SENSITIVE_KEYS


def mask_settings(cfg: Dict[str, Any]) -> Dict[str, Any]:
    def _mask(value: Any, key: str | None = None) -> Any:
        if isinstance(value, dict):
            return {k: _mask(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [_mask(item, key) for item in value]
        if isinstance(value, str) and key and is_sensitive_key(key):
            return _mask_string(value)
        return value

    return _mask(copy.deepcopy(cfg))  # type: ignore[return-value]


def _looks_masked(new_value: str, old_value: str) -> bool:
    if not new_value or "*" not in new_value:
        return False
    return len(new_value) == len(old_value)


def merge_masked_values(new_cfg: Dict[str, Any], old_cfg: Dict[str, Any]) -> Dict[str, Any]:
    def _merge(new_value: Any, old_value: Any, key: str | None = None) -> Any:
        if isinstance(new_value, dict) and isinstance(old_value, dict):
            merged: Dict[str, Any] = {}
            all_keys = set(old_value.keys()) | set(new_value.keys())
            for child_key in all_keys:
                merged[child_key] = _merge(new_value.get(child_key), old_value.get(child_key), child_key)
            return merged
        if isinstance(new_value, list):
            if not isinstance(old_value, list):
                old_value = []
            merged_list: List[Any] = []
            for idx, item in enumerate(new_value):
                old_item = old_value[idx] if idx < len(old_value) else None
                merged_list.append(_merge(item, old_item, key))
            return merged_list
        if (
            isinstance(new_value, str)
            and isinstance(old_value, str)
            and key
            and is_sensitive_key(key)
            and _looks_masked(new_value, old_value)
        ):
            return old_value
        return new_value

    return _merge(copy.deepcopy(new_cfg), copy.deepcopy(old_cfg))  # type: ignore[return-value]


def load_masked_settings(config_path: str | Path | None = None) -> Tuple[Dict[str, Any], Path]:
    cfg = load_settings(config_path)
    path = get_settings_path(config_path)
    return mask_settings(cfg), path
