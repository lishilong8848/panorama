from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from app.shared.utils.cached_json_file import load_cached_json, save_cached_json


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_load_json(path: Path, default_obj: Dict[str, Any]) -> Dict[str, Any]:
    obj = load_cached_json(path, dict(default_obj), encoding="utf-8")
    if isinstance(obj, dict):
        return obj
    return dict(default_obj)


def _safe_save_json(path: Path, payload: Dict[str, Any]) -> None:
    save_cached_json(path, payload, indent=2, encoding="utf-8")


def _safe_save_rows(path: Path, rows: List[Dict[str, Any]]) -> None:
    save_cached_json(path, rows, indent=None, encoding="utf-8")


def _safe_load_rows(path: Path) -> List[Dict[str, Any]]:
    payload = load_cached_json(path, [], encoding="utf-8")
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _resolve_runtime_state_root(*, app_dir: Path, runtime_state_root: str | None = None) -> Path:
    root_text = str(runtime_state_root or "").strip()
    if root_text:
        root = Path(root_text)
        if not root.is_absolute():
            root = app_dir / root
    else:
        root = app_dir / ".runtime"
    root.mkdir(parents=True, exist_ok=True)
    return root


def resolve_alarm_export_resume_root(
    *,
    app_dir: Path,
    root_dir: str | None = None,
    runtime_state_root: str | None = None,
) -> Path:
    root_text = str(root_dir or "").strip() or "alarm_export_resume"
    root = Path(root_text)
    if not root.is_absolute():
        runtime_root = _resolve_runtime_state_root(app_dir=app_dir, runtime_state_root=runtime_state_root)
        root = runtime_root / root
    root.mkdir(parents=True, exist_ok=True)
    return root


class AlarmExportResumeStore:
    def __init__(
        self,
        *,
        app_dir: Path,
        state_file: str,
        root_dir: str | None = None,
        runtime_state_root: str | None = None,
    ) -> None:
        self.app_dir = app_dir
        self.root = resolve_alarm_export_resume_root(
            app_dir=app_dir,
            root_dir=root_dir,
            runtime_state_root=runtime_state_root,
        )
        state_name = str(state_file or "").strip() or "alarm_export_resume_state.json"
        state_path = Path(state_name)
        if not state_path.is_absolute():
            state_path = self.root / state_name
        self.state_path = state_path

    def load_state(self) -> Dict[str, Any] | None:
        state = _safe_load_json(self.state_path, {})
        if not state:
            return None
        if not str(state.get("run_id", "")).strip():
            return None
        return state

    def save_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(state)
        payload.setdefault("created_at", _now_text())
        payload["updated_at"] = _now_text()
        _safe_save_json(self.state_path, payload)
        return payload

    def load_rows(self, rows_file: str | Path) -> List[Dict[str, Any]]:
        rows_path = Path(rows_file)
        if not rows_path.is_absolute():
            rows_path = self.root / rows_path
        return _safe_load_rows(rows_path)

    def save_rows(self, *, run_id: str, rows: List[Dict[str, Any]]) -> Path:
        rows_path = self.root / f"rows_{run_id}.json"
        _safe_save_rows(rows_path, rows)
        return rows_path

    def clear(self, state: Dict[str, Any] | None = None) -> None:
        rows_file = ""
        if isinstance(state, dict):
            rows_file = str(state.get("rows_file", "")).strip()
        if rows_file:
            rows_path = Path(rows_file)
            if not rows_path.is_absolute():
                rows_path = self.root / rows_path
            try:
                if rows_path.exists():
                    rows_path.unlink()
            except Exception:  # noqa: BLE001
                pass
        try:
            if self.state_path.exists():
                self.state_path.unlink()
        except Exception:  # noqa: BLE001
            pass
