from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List

from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from pipeline_utils import get_app_dir


REVIEW_ACCESS_STATE_FILE_NAME = "handover_review_access_state.json"
DEFAULT_REVIEW_BUILDINGS = [
    {"code": "a", "name": "A楼"},
    {"code": "b", "name": "B楼"},
    {"code": "c", "name": "C楼"},
    {"code": "d", "name": "D楼"},
    {"code": "e", "name": "E楼"},
]


def _handover_review_cfg(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = runtime_config if isinstance(runtime_config, dict) else {}
    if isinstance(cfg.get("review_ui", {}), dict):
        return cfg.get("review_ui", {})
    features = cfg.get("features", {})
    if isinstance(features, dict):
        handover = features.get("handover_log", {})
        if isinstance(handover, dict) and isinstance(handover.get("review_ui", {}), dict):
            return handover.get("review_ui", {})
    return {}


def _runtime_state_root(runtime_config: Dict[str, Any]) -> Path:
    cfg = runtime_config if isinstance(runtime_config, dict) else {}
    if isinstance(cfg.get("_global_paths", {}), dict):
        global_paths = dict(cfg.get("_global_paths", {}))
        app_dir = get_app_dir()
        root_text = str(global_paths.get("runtime_state_root", "") or "").strip()
        root = Path(root_text) if root_text else app_dir / ".runtime"
        if not root.is_absolute():
            root = app_dir / root
        root.mkdir(parents=True, exist_ok=True)
        return root
    return resolve_runtime_state_root(runtime_config=cfg, app_dir=get_app_dir())


def resolve_review_access_state_path(runtime_config: Dict[str, Any]) -> Path:
    return _runtime_state_root(runtime_config) / REVIEW_ACCESS_STATE_FILE_NAME


def normalize_review_base_url(text: Any) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    normalized = raw.rstrip("/")
    if "://" not in normalized:
        normalized = f"http://{normalized}"
    return normalized.rstrip("/")


def review_access_state_template() -> Dict[str, Any]:
    return {
        "configured": False,
        "effective_base_url": "",
        "effective_source": "",
        "candidates": [],
        "validated_candidates": [],
        "candidate_results": [],
        "status": "",
        "error": "",
        "configured_at": "",
        "last_probe_at": "",
    }


def normalize_review_access_state(raw: Any) -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    base = review_access_state_template()
    base["configured"] = bool(payload.get("configured", False))
    base["effective_base_url"] = normalize_review_base_url(payload.get("effective_base_url", ""))
    source = str(payload.get("effective_source", "") or "").strip().lower()
    base["effective_source"] = source if source in {"manual", "auto"} else ""
    base["candidates"] = [str(item or "").strip() for item in payload.get("candidates", []) if str(item or "").strip()]
    base["validated_candidates"] = (
        copy.deepcopy(payload.get("validated_candidates", []))
        if isinstance(payload.get("validated_candidates", []), list)
        else []
    )
    base["candidate_results"] = (
        copy.deepcopy(payload.get("candidate_results", []))
        if isinstance(payload.get("candidate_results", []), list)
        else []
    )
    base["status"] = str(payload.get("status", "") or "").strip()
    base["error"] = str(payload.get("error", "") or "").strip()
    base["configured_at"] = str(payload.get("configured_at", "") or "").strip()
    base["last_probe_at"] = str(payload.get("last_probe_at", "") or "").strip()
    return base


def load_review_access_state(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    path = resolve_review_access_state_path(runtime_config)
    if not path.exists():
        return review_access_state_template()
    try:
        return normalize_review_access_state(json.loads(path.read_text(encoding="utf-8")))
    except Exception:  # noqa: BLE001
        return review_access_state_template()


def build_review_links_for_base_url(review_cfg: Dict[str, Any], effective_base_url: str) -> List[Dict[str, str]]:
    links: List[Dict[str, str]] = []
    if not effective_base_url:
        return links
    building_items = (
        review_cfg.get("buildings", [])
        if isinstance(review_cfg.get("buildings", []), list) and review_cfg.get("buildings", [])
        else DEFAULT_REVIEW_BUILDINGS
    )
    for item in building_items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code", "") or "").strip().lower()
        name = str(item.get("name", "") or "").strip()
        if not code:
            continue
        links.append(
            {
                "building": name or code.upper(),
                "code": code,
                "url": f"{effective_base_url}/handover/review/{code}",
            }
        )
    if not any(str(item.get("code", "")).strip().lower() == "110" for item in links):
        links.append(
            {
                "building": "110站",
                "code": "110",
                "url": f"{effective_base_url}/handover/review/110",
            }
        )
    return links


def materialize_review_access_snapshot(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    review_cfg = _handover_review_cfg(runtime_config)
    configured_base_url = normalize_review_base_url(
        review_cfg.get("public_base_url", "") if isinstance(review_cfg, dict) else ""
    )
    persisted = load_review_access_state(runtime_config)
    effective_base_url = ""
    effective_source = ""
    status = "manual_only"
    error = ""

    if configured_base_url:
        effective_base_url = configured_base_url
        effective_source = "manual"
        status = "manual_ok"
    else:
        status = "manual_only"
        error = "请先手工填写审核页访问基地址"

    return {
        "configured": bool(configured_base_url),
        "review_base_url": configured_base_url,
        "review_base_url_effective": effective_base_url,
        "review_base_url_effective_source": effective_source,
        "review_base_url_candidates": [],
        "review_base_url_status": status,
        "review_base_url_error": "" if effective_base_url else error,
        "review_base_url_validated_candidates": [],
        "review_base_url_candidate_results": [],
        "review_base_url_manual_available": True,
        "review_base_url_configured_at": str(persisted.get("configured_at", "") or "").strip(),
        "review_base_url_last_probe_at": str(persisted.get("last_probe_at", "") or "").strip(),
        "review_links": build_review_links_for_base_url(review_cfg if isinstance(review_cfg, dict) else {}, effective_base_url),
    }
