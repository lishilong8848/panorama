from __future__ import annotations

from pathlib import Path
from typing import Any, Dict


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return default


def load_local_build_meta(app_dir: Path) -> Dict[str, Any]:
    candidates = [
        app_dir / "build_meta.json",
        app_dir / "bin" / "build_meta.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            import json

            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload["_meta_path"] = str(path)
                return payload
        except Exception:  # noqa: BLE001
            continue
    return {
        "build_id": "",
        "major_version": 0,
        "patch_version": 0,
        "release_revision": 0,
        "display_version": "",
        "created_at": "",
    }


def normalize_local_version(meta: Dict[str, Any]) -> Dict[str, Any]:
    payload = meta if isinstance(meta, dict) else {}
    return {
        "build_id": str(payload.get("build_id", "") or "").strip(),
        "major_version": _safe_int(payload.get("major_version"), 0),
        "patch_version": _safe_int(payload.get("patch_version"), 0),
        "release_revision": _safe_int(payload.get("release_revision"), 0),
        "display_version": str(payload.get("display_version", "") or "").strip(),
        "created_at": str(payload.get("created_at", "") or "").strip(),
    }


def normalize_remote_version(manifest: Dict[str, Any]) -> Dict[str, Any]:
    payload = manifest if isinstance(manifest, dict) else {}
    return {
        "build_id": str(payload.get("target_version", "") or "").strip(),
        "major_version": _safe_int(payload.get("major_version"), 0),
        "patch_version": _safe_int(payload.get("target_patch_version"), 0),
        "release_revision": _safe_int(
            payload.get("target_release_revision", payload.get("release_revision")),
            0,
        ),
        "display_version": str(payload.get("target_display_version", "") or "").strip(),
        "created_at": str(payload.get("created_at", "") or "").strip(),
    }


def compare_versions(local_version: Dict[str, Any], remote_version: Dict[str, Any]) -> int:
    """
    return:
      -1 -> local older
       0 -> equal
       1 -> local newer
    """
    local_major = _safe_int(local_version.get("major_version"), 0)
    remote_major = _safe_int(remote_version.get("major_version"), 0)
    if local_major != remote_major:
        return -1 if local_major < remote_major else 1

    local_patch = _safe_int(local_version.get("patch_version"), 0)
    remote_patch = _safe_int(remote_version.get("patch_version"), 0)
    if local_patch != remote_patch:
        return -1 if local_patch < remote_patch else 1

    local_revision = _safe_int(local_version.get("release_revision"), 0)
    remote_revision = _safe_int(remote_version.get("release_revision"), 0)
    if local_revision != remote_revision:
        return -1 if local_revision < remote_revision else 1

    return 0
