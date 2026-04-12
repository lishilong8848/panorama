from __future__ import annotations

import copy
import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from app.shared.utils.atomic_file import atomic_write_text, validate_json_file


ACTIVE_COMMAND_STATUSES = frozenset({"pending", "accepted", "running"})
TERMINAL_COMMAND_STATUSES = frozenset({"completed", "failed"})


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def empty_internal_peer_snapshot(*, available: bool = False) -> Dict[str, Any]:
    return {
        "available": bool(available),
        "online": False,
        "heartbeat_at": "",
        "node_id": "",
        "node_label": "",
        "local_version": "",
        "local_release_revision": 0,
        "last_check_at": "",
        "last_result": "",
        "last_error": "",
        "update_available": False,
        "restart_required": False,
        "queued_apply": {
            "queued": False,
            "mode": "",
            "queued_at": "",
            "reason": "",
        },
        "last_command_id": "",
        "last_command_action": "",
        "last_command_status": "",
        "last_command_message": "",
        "command": {
            "exists": False,
            "command_id": "",
            "action": "",
            "status": "",
            "requested_at": "",
            "requested_by_node_id": "",
            "requested_by_role": "",
            "consumed_at": "",
            "finished_at": "",
            "message": "",
            "active": False,
        },
    }


class UpdaterRemoteControlStore:
    def __init__(self, shared_root_dir: str | Path) -> None:
        self.shared_root_dir = Path(shared_root_dir)
        self.root = self.shared_root_dir / "updater" / "remote_control"
        self._lock = threading.RLock()

    @property
    def command_path(self) -> Path:
        return self.root / "internal_command.json"

    @property
    def status_path(self) -> Path:
        return self.root / "internal_status.json"

    def ensure_ready(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def load_command(self) -> Dict[str, Any]:
        payload = _read_json(self.command_path)
        return {
            "command_id": str(payload.get("command_id", "") or "").strip(),
            "action": str(payload.get("action", "") or "").strip().lower(),
            "status": str(payload.get("status", "") or "").strip().lower(),
            "requested_at": str(payload.get("requested_at", "") or "").strip(),
            "requested_by_node_id": str(payload.get("requested_by_node_id", "") or "").strip(),
            "requested_by_role": str(payload.get("requested_by_role", "") or "").strip(),
            "consumed_at": str(payload.get("consumed_at", "") or "").strip(),
            "finished_at": str(payload.get("finished_at", "") or "").strip(),
            "message": str(payload.get("message", "") or "").strip(),
        }

    def is_active_command(self, command: Dict[str, Any] | None) -> bool:
        if not isinstance(command, dict):
            return False
        return str(command.get("status", "") or "").strip().lower() in ACTIVE_COMMAND_STATUSES

    def write_command(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = {
            "command_id": str(payload.get("command_id", "") or "").strip(),
            "action": str(payload.get("action", "") or "").strip().lower(),
            "status": str(payload.get("status", "") or "").strip().lower(),
            "requested_at": str(payload.get("requested_at", "") or "").strip(),
            "requested_by_node_id": str(payload.get("requested_by_node_id", "") or "").strip(),
            "requested_by_role": str(payload.get("requested_by_role", "") or "").strip(),
            "consumed_at": str(payload.get("consumed_at", "") or "").strip(),
            "finished_at": str(payload.get("finished_at", "") or "").strip(),
            "message": str(payload.get("message", "") or "").strip(),
        }
        self.ensure_ready()
        atomic_write_text(
            self.command_path,
            json.dumps(normalized, ensure_ascii=False, indent=2),
            encoding="utf-8",
            validator=validate_json_file,
            allow_overwrite_fallback=False,
        )
        return normalized

    def submit_command(
        self,
        *,
        command_id: str,
        action: str,
        requested_by_node_id: str,
        requested_by_role: str,
    ) -> Dict[str, Any]:
        with self._lock:
            current = self.load_command()
            if self.is_active_command(current):
                return {
                    "accepted": False,
                    "already_pending": True,
                    "command": current,
                }
            command = self.write_command(
                {
                    "command_id": command_id,
                    "action": action,
                    "status": "pending",
                    "requested_at": _now_text(),
                    "requested_by_node_id": requested_by_node_id,
                    "requested_by_role": requested_by_role,
                    "consumed_at": "",
                    "finished_at": "",
                    "message": (
                        "等待内网端执行开始更新"
                        if str(action or "").strip().lower() == "apply"
                        else "等待内网端执行检查更新"
                    ),
                }
            )
            return {
                "accepted": True,
                "already_pending": False,
                "command": command,
            }

    def update_command(
        self,
        *,
        command_id: str,
        status: str,
        message: str = "",
    ) -> Dict[str, Any] | None:
        with self._lock:
            current = self.load_command()
            if str(current.get("command_id", "") or "").strip() != str(command_id or "").strip():
                return None
            next_payload = copy.deepcopy(current)
            next_payload["status"] = str(status or "").strip().lower()
            next_payload["message"] = str(message or "").strip()
            if next_payload["status"] in {"accepted", "running"} and not str(next_payload.get("consumed_at", "") or "").strip():
                next_payload["consumed_at"] = _now_text()
            if next_payload["status"] in TERMINAL_COMMAND_STATUSES:
                next_payload["finished_at"] = _now_text()
            return self.write_command(next_payload)

    def load_status(self) -> Dict[str, Any]:
        payload = _read_json(self.status_path)
        snapshot = empty_internal_peer_snapshot(available=True)
        snapshot.update(
            {
                "available": True,
                "online": bool(payload.get("online", False)),
                "heartbeat_at": str(payload.get("heartbeat_at", "") or "").strip(),
                "node_id": str(payload.get("node_id", "") or "").strip(),
                "node_label": str(payload.get("node_label", "") or "").strip(),
                "local_version": str(payload.get("local_version", "") or "").strip(),
                "local_release_revision": int(payload.get("local_release_revision", 0) or 0),
                "last_check_at": str(payload.get("last_check_at", "") or "").strip(),
                "last_result": str(payload.get("last_result", "") or "").strip(),
                "last_error": str(payload.get("last_error", "") or "").strip(),
                "update_available": bool(payload.get("update_available", False)),
                "restart_required": bool(payload.get("restart_required", False)),
                "queued_apply": dict(payload.get("queued_apply", {}) or snapshot["queued_apply"]),
                "last_command_id": str(payload.get("last_command_id", "") or "").strip(),
                "last_command_action": str(payload.get("last_command_action", "") or "").strip().lower(),
                "last_command_status": str(payload.get("last_command_status", "") or "").strip().lower(),
                "last_command_message": str(payload.get("last_command_message", "") or "").strip(),
            }
        )
        return snapshot

    def write_status(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        snapshot = empty_internal_peer_snapshot(available=True)
        snapshot.update(copy.deepcopy(payload if isinstance(payload, dict) else {}))
        snapshot["available"] = True
        self.ensure_ready()
        atomic_write_text(
            self.status_path,
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
            validator=validate_json_file,
            allow_overwrite_fallback=False,
        )
        return snapshot

    def build_internal_peer_snapshot(self, *, heartbeat_timeout_sec: int = 15) -> Dict[str, Any]:
        with self._lock:
            snapshot = self.load_status()
            command = self.load_command()
        heartbeat_at = _parse_time(snapshot.get("heartbeat_at"))
        online = False
        if heartbeat_at is not None:
            age = (datetime.now() - heartbeat_at).total_seconds()
            online = age <= max(1, int(heartbeat_timeout_sec or 15))
        if not bool(snapshot.get("online", False)):
            online = False
        snapshot["online"] = online
        snapshot["command"] = {
            "exists": bool(str(command.get("command_id", "") or "").strip()),
            "command_id": str(command.get("command_id", "") or "").strip(),
            "action": str(command.get("action", "") or "").strip().lower(),
            "status": str(command.get("status", "") or "").strip().lower(),
            "requested_at": str(command.get("requested_at", "") or "").strip(),
            "requested_by_node_id": str(command.get("requested_by_node_id", "") or "").strip(),
            "requested_by_role": str(command.get("requested_by_role", "") or "").strip(),
            "consumed_at": str(command.get("consumed_at", "") or "").strip(),
            "finished_at": str(command.get("finished_at", "") or "").strip(),
            "message": str(command.get("message", "") or "").strip(),
            "active": self.is_active_command(command),
        }
        return snapshot
