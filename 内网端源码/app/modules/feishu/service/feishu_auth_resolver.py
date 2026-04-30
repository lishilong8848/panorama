from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from pipeline_utils import resolve_config_path


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_auth(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    current = _dict(payload)
    return {
        "app_id": _text(current.get("app_id")),
        "app_secret": _text(current.get("app_secret")),
        "request_retry_count": int(current.get("request_retry_count", 3) or 3),
        "request_retry_interval_sec": float(current.get("request_retry_interval_sec", 2) or 2),
        "timeout": int(current.get("timeout", 30) or 30),
    }


def _has_meaningful_auth(payload: Dict[str, Any] | None) -> bool:
    auth = _normalize_auth(payload)
    return bool(auth["app_id"] and auth["app_secret"])


def _merge_auth(base: Dict[str, Any], candidate: Dict[str, Any] | None) -> Dict[str, Any]:
    merged = dict(base)
    raw = _dict(candidate)
    current = _normalize_auth(raw)
    for key in ("app_id", "app_secret"):
        if current[key]:
            merged[key] = current[key]
    for key in ("request_retry_count", "request_retry_interval_sec", "timeout"):
        if key not in raw:
            continue
        value = current.get(key)
        if value not in (None, ""):
            merged[key] = value
    return merged


def _extract_from_source(source: Any) -> Dict[str, Any]:
    current = _dict(source)
    resolved: Dict[str, Any] = {}
    candidates = [
        _dict(_dict(current.get("common")).get("feishu_auth")),
        _dict(current.get("feishu")),
        _dict(current.get("_global_feishu")),
        _dict(_dict(current.get("handover_log")).get("_global_feishu")),
        _dict(_dict(_dict(current.get("features")).get("handover_log")).get("_global_feishu")),
        current,
    ]
    for candidate in candidates:
        resolved = _merge_auth(resolved, candidate)
    return resolved


def _load_from_config(config_path: str | Path | None = None) -> Dict[str, Any]:
    try:
        resolved_path = resolve_config_path(config_path)
    except Exception:
        return _normalize_auth({})
    try:
        from app.config.settings_loader import load_settings

        cfg = load_settings(resolved_path)
    except Exception:
        return _normalize_auth({})
    return _normalize_auth(_extract_from_source(cfg))


def resolve_feishu_auth_settings(source: Any = None, *, config_path: str | Path | None = None) -> Dict[str, Any]:
    primary = _extract_from_source(source)
    if _has_meaningful_auth(primary):
        return _normalize_auth(primary)
    fallback = _load_from_config(config_path)
    return _normalize_auth(_merge_auth(fallback, primary))


def require_feishu_auth_settings(source: Any = None, *, config_path: str | Path | None = None) -> Dict[str, Any]:
    resolved = resolve_feishu_auth_settings(source, config_path=config_path)
    if not _has_meaningful_auth(resolved):
        raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")
    return resolved
