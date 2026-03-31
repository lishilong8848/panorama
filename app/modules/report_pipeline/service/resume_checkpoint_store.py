from __future__ import annotations

import copy
import hashlib
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def new_run_id() -> str:
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"


def parse_time_or_none(value: str) -> datetime | None:
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def safe_load_json(path: Path, default_obj: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return copy.deepcopy(default_obj)
    try:
        with path.open("r", encoding="utf-8-sig") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return obj
    except Exception:  # noqa: BLE001
        pass
    return copy.deepcopy(default_obj)


def safe_save_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _resolve_runtime_state_root(
    *,
    app_dir: Path | None = None,
    runtime_state_root: str | None = None,
) -> Path:
    base = app_dir or Path.cwd()
    root_text = str(runtime_state_root or "").strip()
    if root_text:
        root = Path(root_text)
        if not root.is_absolute():
            root = base / root
    else:
        root = base / ".runtime"
    root.mkdir(parents=True, exist_ok=True)
    return root


def resolve_resume_root_dir(
    app_dir: Path | None = None,
    *,
    root_dir: str | None = None,
    runtime_state_root: str | None = None,
) -> Path:
    root_text = str(root_dir or "").strip() or "pipeline_resume"
    root = Path(root_text)
    if not root.is_absolute():
        runtime_root = _resolve_runtime_state_root(app_dir=app_dir, runtime_state_root=runtime_state_root)
        root = runtime_root / root
    root.mkdir(parents=True, exist_ok=True)
    return root


def resolve_resume_index_path(
    app_dir: Path | None = None,
    *,
    root_dir: str | None = None,
    index_file: str | None = None,
    runtime_state_root: str | None = None,
) -> Path:
    file_name = str(index_file or "").strip() or "index.json"
    path = Path(file_name)
    if path.is_absolute():
        return path
    return resolve_resume_root_dir(
        app_dir=app_dir,
        root_dir=root_dir,
        runtime_state_root=runtime_state_root,
    ) / path


def checkpoint_path(run_save_dir: str) -> Path:
    return Path(run_save_dir) / "_pipeline_checkpoint.json"


def refresh_checkpoint_summary(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    file_items = checkpoint.get("file_items")
    if not isinstance(file_items, list):
        file_items = []
        checkpoint["file_items"] = file_items

    uploaded = 0
    pending = 0
    upload_failed = 0
    file_missing = 0
    for item in file_items:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status", "")).strip()
        if status == "uploaded":
            uploaded += 1
        elif status == "pending":
            pending += 1
        elif status == "upload_failed":
            upload_failed += 1
        elif status == "file_missing":
            file_missing += 1
        else:
            item["status"] = "pending"
            pending += 1

    summary = checkpoint.get("summary")
    if not isinstance(summary, dict):
        summary = {}
        checkpoint["summary"] = summary
    summary["total_files"] = len(file_items)
    summary["uploaded_count"] = uploaded
    summary["upload_failed_count"] = upload_failed
    summary["file_missing_count"] = file_missing
    summary["pending_count"] = pending
    summary["pending_upload_count"] = pending + upload_failed
    summary["completed_count"] = uploaded + file_missing
    return summary


def normalize_checkpoint(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    checkpoint.setdefault("run_save_dir", "")
    run_id_raw = str(checkpoint.get("run_id", "")).strip()
    run_id_lower = run_id_raw.lower()
    if (not run_id_raw) or run_id_lower in {"none", "null", "-"}:
        run_save_dir = str(checkpoint.get("run_save_dir", "")).strip()
        if run_save_dir:
            digest = hashlib.md5(run_save_dir.encode("utf-8")).hexdigest()[:12]
            checkpoint["run_id"] = f"legacy_{digest}"
        else:
            checkpoint["run_id"] = new_run_id()
    checkpoint.setdefault("source", "")
    checkpoint.setdefault("run_save_dir", "")
    checkpoint.setdefault("selected_dates", [])
    checkpoint.setdefault("stage", "downloading")
    checkpoint.setdefault("file_items", [])
    checkpoint.setdefault("date_results", [])
    checkpoint.setdefault("summary", {})
    checkpoint.setdefault("last_error", "")
    checkpoint.setdefault("created_at", now_text())
    checkpoint["updated_at"] = now_text()
    refresh_checkpoint_summary(checkpoint)
    return checkpoint


def load_resume_index(
    app_dir: Path | None = None,
    *,
    root_dir: str | None = None,
    index_file: str | None = None,
    runtime_state_root: str | None = None,
) -> Dict[str, Any]:
    return safe_load_json(
        resolve_resume_index_path(
            app_dir=app_dir,
            root_dir=root_dir,
            index_file=index_file,
            runtime_state_root=runtime_state_root,
        ),
        {"items": []},
    )


def save_resume_index(
    index_obj: Dict[str, Any],
    app_dir: Path | None = None,
    *,
    root_dir: str | None = None,
    index_file: str | None = None,
    runtime_state_root: str | None = None,
) -> None:
    if "items" not in index_obj or not isinstance(index_obj["items"], list):
        index_obj["items"] = []
    safe_save_json(
        resolve_resume_index_path(
            app_dir=app_dir,
            root_dir=root_dir,
            index_file=index_file,
            runtime_state_root=runtime_state_root,
        ),
        index_obj,
    )


def cleanup_resume_index(
    retention_days: int,
    app_dir: Path | None = None,
    *,
    root_dir: str | None = None,
    index_file: str | None = None,
    runtime_state_root: str | None = None,
) -> None:
    index_obj = load_resume_index(
        app_dir=app_dir,
        root_dir=root_dir,
        index_file=index_file,
        runtime_state_root=runtime_state_root,
    )
    items = index_obj.get("items")
    if not isinstance(items, list):
        index_obj["items"] = []
        save_resume_index(
            index_obj,
            app_dir=app_dir,
            root_dir=root_dir,
            index_file=index_file,
            runtime_state_root=runtime_state_root,
        )
        return

    if retention_days <= 0:
        retention_days = 7
    now = datetime.now()
    kept: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        checkpoint_text = str(item.get("checkpoint_path", "")).strip()
        if not checkpoint_text:
            continue
        checkpoint_file = Path(checkpoint_text)
        if not checkpoint_file.exists():
            continue

        updated_at = parse_time_or_none(str(item.get("updated_at", "")))
        if updated_at is None:
            updated_at = datetime.fromtimestamp(checkpoint_file.stat().st_mtime)
        if now - updated_at > timedelta(days=retention_days):
            continue
        kept.append(item)

    index_obj["items"] = kept
    save_resume_index(
        index_obj,
        app_dir=app_dir,
        root_dir=root_dir,
        index_file=index_file,
        runtime_state_root=runtime_state_root,
    )


def save_checkpoint_and_index(
    checkpoint: Dict[str, Any],
    *,
    retention_days: int,
    app_dir: Path | None = None,
    root_dir: str | None = None,
    index_file: str | None = None,
    runtime_state_root: str | None = None,
) -> Dict[str, Any]:
    normalized = normalize_checkpoint(checkpoint)
    run_save_dir = str(normalized.get("run_save_dir", "")).strip()
    if not run_save_dir:
        raise ValueError("checkpoint.run_save_dir 不能为空")
    path = checkpoint_path(run_save_dir)
    safe_save_json(path, normalized)

    cleanup_resume_index(
        retention_days,
        app_dir=app_dir,
        root_dir=root_dir,
        index_file=index_file,
        runtime_state_root=runtime_state_root,
    )

    index_obj = load_resume_index(
        app_dir=app_dir,
        root_dir=root_dir,
        index_file=index_file,
        runtime_state_root=runtime_state_root,
    )
    items = index_obj.get("items")
    if not isinstance(items, list):
        items = []
    run_id = str(normalized.get("run_id", "")).strip() or new_run_id()
    normalized["run_id"] = run_id
    summary = normalized.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    index_item = {
        "run_id": run_id,
        "source": str(normalized.get("source", "")).strip(),
        "run_save_dir": run_save_dir,
        "checkpoint_path": str(path),
        "stage": str(normalized.get("stage", "")).strip(),
        "selected_dates": list(normalized.get("selected_dates", [])),
        "pending_upload_count": int(summary.get("pending_upload_count", 0)),
        "upload_failed_count": int(summary.get("upload_failed_count", 0)),
        "last_error": str(normalized.get("last_error", "")).strip(),
        "created_at": str(normalized.get("created_at", "")).strip(),
        "updated_at": str(normalized.get("updated_at", "")).strip(),
    }

    replaced = False
    for idx, item in enumerate(items):
        if isinstance(item, dict) and str(item.get("run_id", "")).strip() == run_id:
            items[idx] = index_item
            replaced = True
            break
    if not replaced:
        items.append(index_item)
    items.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
    index_obj["items"] = items
    save_resume_index(
        index_obj,
        app_dir=app_dir,
        root_dir=root_dir,
        index_file=index_file,
        runtime_state_root=runtime_state_root,
    )
    return normalized


def load_checkpoint_by_path(checkpoint_file: Path) -> Dict[str, Any]:
    if not checkpoint_file.exists():
        raise FileNotFoundError(f"checkpoint不存在: {checkpoint_file}")
    obj = safe_load_json(checkpoint_file, {})
    if not obj:
        raise ValueError(f"checkpoint内容为空: {checkpoint_file}")
    return normalize_checkpoint(obj)


def list_pending_upload_runs_internal(
    *,
    retention_days: int,
    app_dir: Path | None = None,
    root_dir: str | None = None,
    index_file: str | None = None,
    runtime_state_root: str | None = None,
) -> List[Dict[str, Any]]:
    cleanup_resume_index(
        retention_days,
        app_dir=app_dir,
        root_dir=root_dir,
        index_file=index_file,
        runtime_state_root=runtime_state_root,
    )
    index_obj = load_resume_index(
        app_dir=app_dir,
        root_dir=root_dir,
        index_file=index_file,
        runtime_state_root=runtime_state_root,
    )
    items = index_obj.get("items")
    if not isinstance(items, list):
        return []

    pending_runs: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        checkpoint_file = Path(str(item.get("checkpoint_path", "")).strip())
        if not checkpoint_file.exists():
            continue
        try:
            checkpoint = load_checkpoint_by_path(checkpoint_file)
        except Exception:  # noqa: BLE001
            continue
        summary = refresh_checkpoint_summary(checkpoint)
        pending_upload_count = int(summary.get("pending_upload_count", 0))
        if pending_upload_count <= 0:
            continue
        pending_runs.append(
            {
                "run_id": str(checkpoint.get("run_id", "")).strip(),
                "source": str(checkpoint.get("source", "")).strip(),
                "stage": str(checkpoint.get("stage", "")).strip(),
                "run_save_dir": str(checkpoint.get("run_save_dir", "")).strip(),
                "selected_dates": list(checkpoint.get("selected_dates", [])),
                "pending_upload_count": pending_upload_count,
                "upload_failed_count": int(summary.get("upload_failed_count", 0)),
                "created_at": str(checkpoint.get("created_at", "")).strip(),
                "updated_at": str(checkpoint.get("updated_at", "")).strip(),
                "last_error": str(checkpoint.get("last_error", "")).strip(),
            }
        )

    pending_runs.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
    return pending_runs


def load_pending_checkpoint(
    run_id: str | None = None,
    *,
    retention_days: int,
    app_dir: Path | None = None,
    root_dir: str | None = None,
    index_file: str | None = None,
    runtime_state_root: str | None = None,
) -> Dict[str, Any] | None:
    pending_runs = list_pending_upload_runs_internal(
        retention_days=retention_days,
        app_dir=app_dir,
        root_dir=root_dir,
        index_file=index_file,
        runtime_state_root=runtime_state_root,
    )
    if not pending_runs:
        return None
    target: Dict[str, Any] | None = None
    if run_id:
        run_text = str(run_id).strip()
        for item in pending_runs:
            if str(item.get("run_id", "")).strip() == run_text:
                target = item
                break
        if target is None:
            raise ValueError(f"未找到待续传任务: {run_id}")
    else:
        target = pending_runs[0]
    checkpoint_file = checkpoint_path(str(target.get("run_save_dir", "")).strip())
    return load_checkpoint_by_path(checkpoint_file)


def collect_retryable_file_items(checkpoint: Dict[str, Any]) -> List[Dict[str, Any]]:
    file_items = checkpoint.get("file_items")
    if not isinstance(file_items, list):
        return []
    rows: List[Dict[str, Any]] = []
    for item in file_items:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status", "")).strip()
        if status not in {"pending", "upload_failed"}:
            continue
        rows.append(item)
    return rows


def build_checkpoint(
    *,
    source_name: str,
    run_save_dir: str,
    selected_dates: List[str],
    run_id: str | None = None,
) -> Dict[str, Any]:
    text_now = now_text()
    return normalize_checkpoint(
        {
            "run_id": str(run_id).strip() or new_run_id(),
            "source": source_name,
            "run_save_dir": run_save_dir,
            "selected_dates": selected_dates,
            "stage": "downloading",
            "file_items": [],
            "date_results": [],
            "summary": {},
            "last_error": "",
            "created_at": text_now,
            "updated_at": text_now,
        }
    )


def sync_summary_from_checkpoint(summary: Dict[str, Any], checkpoint: Dict[str, Any]) -> None:
    summary_obj = refresh_checkpoint_summary(checkpoint)
    summary["resume_run_id"] = str(checkpoint.get("run_id", "")).strip()
    summary["pending_upload_count"] = int(summary_obj.get("pending_upload_count", 0))
    summary["upload_failed_count"] = int(summary_obj.get("upload_failed_count", 0))
    summary["uploaded_count"] = int(summary_obj.get("uploaded_count", 0))
    summary["file_missing_count"] = int(summary_obj.get("file_missing_count", 0))
    summary["pending_resume"] = summary["pending_upload_count"] > 0
