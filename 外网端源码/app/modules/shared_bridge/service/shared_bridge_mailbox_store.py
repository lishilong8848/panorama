from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from app.shared.utils.atomic_file import atomic_write_text, validate_json_file


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _task_updated_at(payload: Dict[str, Any]) -> str:
    task = payload.get("task", {}) if isinstance(payload.get("task", {}), dict) else {}
    return (
        str(task.get("updated_at", "") or "").strip()
        or str(payload.get("updated_at", "") or "").strip()
        or str(task.get("created_at", "") or "").strip()
    )


class SharedBridgeMailboxStore:
    def __init__(self, shared_root_dir: str | Path) -> None:
        self.shared_root_dir = Path(shared_root_dir)
        self.root = self.shared_root_dir / "bridge_mailbox"

    def ensure_ready(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def task_dir(self, task_id: str) -> Path:
        self.ensure_ready()
        return self.root / str(task_id or "").strip()

    def request_path(self, task_id: str) -> Path:
        return self.task_dir(task_id) / "request.json"

    def internal_path(self, task_id: str) -> Path:
        return self.task_dir(task_id) / "internal.json"

    def external_path(self, task_id: str) -> Path:
        return self.task_dir(task_id) / "external.json"

    def write_request(self, task: Dict[str, Any]) -> None:
        task_id = str(task.get("task_id", "") or "").strip()
        if not task_id:
            return
        payload = {
            "task_id": task_id,
            "feature": str(task.get("feature", "") or "").strip(),
            "mode": str(task.get("mode", "") or "").strip(),
            "dedupe_key": str(task.get("dedupe_key", "") or "").strip(),
            "resume_job_id": str(
                (task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}).get("resume_job_id", "") or ""
            ).strip(),
            "created_by_role": str(task.get("created_by_role", "") or "").strip(),
            "created_by_node_id": str(task.get("created_by_node_id", "") or "").strip(),
            "requested_by": str(task.get("requested_by", "") or "").strip(),
            "request": copy.deepcopy(task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}),
            "created_at": str(task.get("created_at", "") or "").strip() or _now_text(),
            "updated_at": str(task.get("updated_at", "") or "").strip() or _now_text(),
            "task": copy.deepcopy(task),
        }
        path = self.request_path(task_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(
            path,
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
            validator=validate_json_file,
            allow_overwrite_fallback=False,
        )

    def write_side_snapshot(self, *, task: Dict[str, Any], side: str) -> None:
        task_id = str(task.get("task_id", "") or "").strip()
        side_text = str(side or "").strip().lower()
        if not task_id or side_text not in {"internal", "external"}:
            return
        payload = {
            "task_id": task_id,
            "status": str(task.get("status", "") or "").strip(),
            "error": str(task.get("error", "") or "").strip(),
            "updated_at": str(task.get("updated_at", "") or "").strip() or _now_text(),
            "result": copy.deepcopy(task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}),
            "task": copy.deepcopy(task),
        }
        path = self.internal_path(task_id) if side_text == "internal" else self.external_path(task_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(
            path,
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
            validator=validate_json_file,
            allow_overwrite_fallback=False,
        )

    def load_task(self, task_id: str) -> Dict[str, Any] | None:
        task_text = str(task_id or "").strip()
        if not task_text:
            return None
        request_payload = _read_json(self.request_path(task_text))
        internal_payload = _read_json(self.internal_path(task_text))
        external_payload = _read_json(self.external_path(task_text))
        candidates = [payload for payload in [request_payload, internal_payload, external_payload] if payload]
        if not candidates:
            return None
        candidates.sort(key=_task_updated_at, reverse=True)
        for payload in candidates:
            task = payload.get("task", {})
            if isinstance(task, dict) and str(task.get("task_id", "") or "").strip():
                return copy.deepcopy(task)
        return None

    def list_tasks(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        self.ensure_ready()
        tasks: List[Dict[str, Any]] = []
        for task_dir in self.root.iterdir():
            if not task_dir.is_dir():
                continue
            task = self.load_task(task_dir.name)
            if isinstance(task, dict):
                tasks.append(task)
        tasks.sort(
            key=lambda item: (
                str(item.get("updated_at", "") or "").strip(),
                str(item.get("created_at", "") or "").strip(),
                str(item.get("task_id", "") or "").strip(),
            ),
            reverse=True,
        )
        return tasks[: max(1, int(limit or 100))]
