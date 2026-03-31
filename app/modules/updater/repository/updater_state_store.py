from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


DEFAULT_STATE = {
    "last_check_at": "",
    "last_result": "",
    "last_error": "",
    "local_version": "",
    "remote_version": "",
    "source_kind": "remote",
    "source_label": "远端正式更新源",
    "local_release_revision": 0,
    "remote_release_revision": 0,
    "update_available": False,
    "force_apply_available": False,
    "restart_required": False,
    "dependency_sync_status": "idle",
    "dependency_sync_error": "",
    "dependency_sync_at": "",
    "queued_apply": {
        "queued": False,
        "mode": "",
        "queued_at": "",
        "reason": "",
    },
    "last_updated_at": "",
    "last_applied_release_revision": 0,
    "mirror_ready": False,
    "mirror_version": "",
    "mirror_manifest_path": "",
    "last_publish_at": "",
    "last_publish_error": "",
}


class UpdaterStateStore:
    def __init__(self, state_path: Path) -> None:
        self.state_path = state_path

    def load(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return dict(DEFAULT_STATE)
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                state = dict(DEFAULT_STATE)
                state.update(payload)
                queued_apply = dict(DEFAULT_STATE.get("queued_apply", {}))
                raw_queued_apply = payload.get("queued_apply", {})
                if isinstance(raw_queued_apply, dict):
                    queued_apply.update(raw_queued_apply)
                state["queued_apply"] = queued_apply
                return state
        except Exception:  # noqa: BLE001
            pass
        return dict(DEFAULT_STATE)

    def save(self, state: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(DEFAULT_STATE)
        if isinstance(state, dict):
            payload.update(state)
        queued_apply = dict(DEFAULT_STATE.get("queued_apply", {}))
        raw_queued_apply = payload.get("queued_apply", {})
        if isinstance(raw_queued_apply, dict):
            queued_apply.update(raw_queued_apply)
        payload["queued_apply"] = queued_apply
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload
