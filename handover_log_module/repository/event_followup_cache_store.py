from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.shared.utils.atomic_file import atomic_write_text
from pipeline_utils import get_app_dir


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_load_json(path: Path, default_obj: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return dict(default_obj)
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    except Exception:  # noqa: BLE001
        pass
    return dict(default_obj)


def _safe_save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _resolve_runtime_state_root(*, global_paths: Dict[str, Any] | None) -> Path:
    app_dir = get_app_dir()
    paths_cfg = global_paths if isinstance(global_paths, dict) else {}
    root_text = str(paths_cfg.get("runtime_state_root", "") or "").strip()
    root = Path(root_text) if root_text else app_dir / ".runtime"
    if not root.is_absolute():
        root = app_dir / root
    root.mkdir(parents=True, exist_ok=True)
    return root


class EventFollowupCacheStore:
    SHARED_STATE_FILE = "handover_shared_cache.json"
    DEFAULT_STATE: Dict[str, Any] = {
        "pending_by_id": {},
        "last_query_record_ids": [],
        "feishu_read_cache": {},
        "review_sessions": {},
        "review_latest_by_building": {},
        "review_batch_status": {},
        "updated_at": "",
    }

    def __init__(
        self,
        *,
        cache_state_file: str,
        global_paths: Dict[str, Any] | None = None,
    ) -> None:
        state_name = str(cache_state_file or "").strip()
        # 统一到单缓存文件：排班、长白岗、工程师目录、事件跟进、审核协作共用。
        if not state_name or state_name.casefold() in {
            "handover_event_followup_state.json",
            "handover_shift_cache_state.json",
            "handover_roster_cache_state.json",
        }:
            state_name = self.SHARED_STATE_FILE
        state_path = Path(state_name)
        if not state_path.is_absolute():
            runtime_root = _resolve_runtime_state_root(global_paths=global_paths)
            state_path = runtime_root / state_path
        self.state_path = state_path

    @staticmethod
    def _cache_key(building: str, record_id: str) -> str:
        return f"{building}::{record_id}"

    def _normalize_state(self, state: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = deepcopy(self.DEFAULT_STATE)
        if isinstance(state, dict):
            payload.update(state)
        if not isinstance(payload.get("pending_by_id"), dict):
            payload["pending_by_id"] = {}
        if not isinstance(payload.get("last_query_record_ids"), list):
            payload["last_query_record_ids"] = []
        if not isinstance(payload.get("feishu_read_cache"), dict):
            payload["feishu_read_cache"] = {}
        if not isinstance(payload.get("review_sessions"), dict):
            payload["review_sessions"] = {}
        if not isinstance(payload.get("review_latest_by_building"), dict):
            payload["review_latest_by_building"] = {}
        if not isinstance(payload.get("review_batch_status"), dict):
            payload["review_batch_status"] = {}
        payload["updated_at"] = str(payload.get("updated_at", "")).strip()
        return payload

    def load_state(self) -> Dict[str, Any]:
        state = _safe_load_json(self.state_path, self.DEFAULT_STATE)
        return self._normalize_state(state)

    def save_state(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        state = self._normalize_state(payload)
        state["updated_at"] = _now_text()
        _safe_save_json(self.state_path, state)
        return state

    def list_pending_for_building(self, building: str) -> List[Dict[str, Any]]:
        state = self.load_state()
        pending_by_id = state.get("pending_by_id", {})
        if not isinstance(pending_by_id, dict):
            return []
        prefix = f"{building}::"
        rows: List[Dict[str, Any]] = []
        for cache_key, payload in pending_by_id.items():
            if not str(cache_key).startswith(prefix):
                continue
            if not isinstance(payload, dict):
                continue
            rows.append(dict(payload))
        return rows

    def update_building_pending(
        self,
        *,
        building: str,
        pending_rows: List[Dict[str, Any]],
        max_pending: int,
        last_query_record_ids: List[str],
        max_last_query_ids: int,
    ) -> Dict[str, Any]:
        state = self.load_state()
        pending_by_id = state.get("pending_by_id", {})
        if not isinstance(pending_by_id, dict):
            pending_by_id = {}

        prefix = f"{building}::"
        remove_keys = [key for key in pending_by_id.keys() if str(key).startswith(prefix)]
        for key in remove_keys:
            pending_by_id.pop(key, None)

        normalized_rows: List[Dict[str, Any]] = []
        for row in pending_rows:
            if not isinstance(row, dict):
                continue
            record_id = str(row.get("record_id", "")).strip()
            if not record_id:
                continue
            row_copy = dict(row)
            row_copy["building"] = building
            row_copy["record_id"] = record_id
            row_copy["cached_at"] = _now_text()
            normalized_rows.append(row_copy)

        if max_pending <= 0:
            max_pending = 20000
        for row in normalized_rows[:max_pending]:
            cache_key = self._cache_key(building, str(row.get("record_id", "")))
            pending_by_id[cache_key] = row

        normalized_ids = [str(x).strip() for x in last_query_record_ids if str(x).strip()]
        if max_last_query_ids <= 0:
            max_last_query_ids = 5000
        state["last_query_record_ids"] = normalized_ids[:max_last_query_ids]
        state["pending_by_id"] = pending_by_id
        return self.save_state(state)

    def get_feishu_cached_records(
        self,
        *,
        namespace: str,
        cache_key: str,
    ) -> Optional[List[Dict[str, Any]]]:
        ns = str(namespace or "").strip()
        key = str(cache_key or "").strip()
        if not ns or not key:
            return None
        state = self.load_state()
        cache_root = state.get("feishu_read_cache", {})
        if not isinstance(cache_root, dict):
            return None
        ns_map = cache_root.get(ns, {})
        if not isinstance(ns_map, dict):
            return None
        payload = ns_map.get(key, {})
        if not isinstance(payload, dict):
            return None
        rows = payload.get("records", [])
        if not isinstance(rows, list):
            return None
        out: List[Dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                out.append(deepcopy(row))
        return out

    def get_feishu_cache_meta(
        self,
        *,
        namespace: str,
        cache_key: str,
    ) -> Dict[str, Any] | None:
        ns = str(namespace or "").strip()
        key = str(cache_key or "").strip()
        if not ns or not key:
            return None
        state = self.load_state()
        cache_root = state.get("feishu_read_cache", {})
        if not isinstance(cache_root, dict):
            return None
        ns_map = cache_root.get(ns, {})
        if not isinstance(ns_map, dict):
            return None
        payload = ns_map.get(key, {})
        if not isinstance(payload, dict):
            return None
        meta = payload.get("meta", {})
        return dict(meta) if isinstance(meta, dict) else {}

    def purge_feishu_cache_namespace(
        self,
        *,
        namespace: str,
        keep_scope: str | None = None,
    ) -> Dict[str, Any]:
        ns = str(namespace or "").strip()
        if not ns:
            return self.load_state()

        state = self.load_state()
        cache_root = state.get("feishu_read_cache", {})
        if not isinstance(cache_root, dict):
            return state
        ns_map = cache_root.get(ns, {})
        if not isinstance(ns_map, dict):
            return state

        keep_scope_text = str(keep_scope or "").strip()
        if not keep_scope_text:
            cache_root[ns] = {}
            state["feishu_read_cache"] = cache_root
            return self.save_state(state)

        new_ns_map: Dict[str, Any] = {}
        changed = False
        for key, payload in ns_map.items():
            if not isinstance(payload, dict):
                changed = True
                continue
            meta = payload.get("meta", {})
            meta_scope = str(meta.get("scope", "")).strip() if isinstance(meta, dict) else ""
            if meta_scope == keep_scope_text:
                new_ns_map[key] = payload
            else:
                changed = True
        if changed:
            cache_root[ns] = new_ns_map
            state["feishu_read_cache"] = cache_root
            return self.save_state(state)
        return self._normalize_state(state)

    def set_feishu_cached_records(
        self,
        *,
        namespace: str,
        cache_key: str,
        records: List[Dict[str, Any]],
        meta: Dict[str, Any] | None = None,
        max_entries_per_namespace: int = 12,
    ) -> Dict[str, Any]:
        ns = str(namespace or "").strip()
        key = str(cache_key or "").strip()
        if not ns or not key:
            return self.load_state()

        state = self.load_state()
        cache_root = state.get("feishu_read_cache", {})
        if not isinstance(cache_root, dict):
            cache_root = {}
        ns_map = cache_root.get(ns, {})
        if not isinstance(ns_map, dict):
            ns_map = {}

        normalized_rows: List[Dict[str, Any]] = []
        for row in records:
            if isinstance(row, dict):
                normalized_rows.append(deepcopy(row))

        payload: Dict[str, Any] = {
            "records": normalized_rows,
            "updated_at": _now_text(),
            "meta": dict(meta or {}),
        }
        ns_map[key] = payload

        if max_entries_per_namespace > 0 and len(ns_map) > max_entries_per_namespace:
            items = sorted(
                ns_map.items(),
                key=lambda kv: str(kv[1].get("updated_at", "")) if isinstance(kv[1], dict) else "",
            )
            overflow = len(ns_map) - max_entries_per_namespace
            for old_key, _ in items[:overflow]:
                ns_map.pop(old_key, None)

        cache_root[ns] = ns_map
        state["feishu_read_cache"] = cache_root
        return self.save_state(state)
