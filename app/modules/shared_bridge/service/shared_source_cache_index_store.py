from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from app.shared.utils.atomic_file import atomic_write_text, validate_json_file


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _normalize_entry(entry: Dict[str, Any] | None) -> Dict[str, Any]:
    data = dict(entry or {})
    metadata = data.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
    normalized = {
        "entry_id": str(data.get("entry_id", "") or "").strip(),
        "source_family": str(data.get("source_family", "") or "").strip().lower(),
        "building": str(data.get("building", "") or "").strip(),
        "bucket_kind": str(data.get("bucket_kind", "") or "").strip().lower(),
        "bucket_key": str(data.get("bucket_key", "") or "").strip(),
        "duty_date": str(data.get("duty_date", "") or "").strip(),
        "duty_shift": str(data.get("duty_shift", "") or "").strip().lower(),
        "downloaded_at": str(data.get("downloaded_at", "") or "").strip(),
        "relative_path": str(data.get("relative_path", "") or "").replace("\\", "/").strip(),
        "status": str(data.get("status", "") or "").strip().lower(),
        "file_hash": str(data.get("file_hash", "") or "").strip(),
        "size_bytes": int(data.get("size_bytes", 0) or 0),
        "metadata": metadata,
        "created_at": str(data.get("created_at", "") or "").strip(),
        "updated_at": str(data.get("updated_at", "") or "").strip(),
    }
    return normalized


def _safe_path_segment(value: Any, *, default: str) -> str:
    text = str(value or "").strip()
    if not text:
        return default
    sanitized = re.sub(r'[<>:"/\\\\|?*]+', "_", text)
    sanitized = re.sub(r"\s+", "_", sanitized).strip(" ._")
    return sanitized or default


def _sort_key(entry: Dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(entry.get("downloaded_at", "") or "").strip(),
        str(entry.get("updated_at", "") or "").strip(),
        str(entry.get("entry_id", "") or "").strip(),
    )


class SharedSourceCacheIndexStore:
    def __init__(self, shared_root_dir: str | Path) -> None:
        self.shared_root_dir = Path(shared_root_dir)
        self.root = self.shared_root_dir / "source_cache_index"

    def ensure_ready(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def _entry_path(
        self,
        *,
        source_family: str,
        building: str,
        bucket_key: str,
        duty_shift: str = "",
    ) -> Path:
        family_dir = self.root / _safe_path_segment(source_family, default="family")
        building_dir = family_dir / _safe_path_segment(building, default="building")
        filename = _safe_path_segment(bucket_key, default="bucket")
        shift_text = str(duty_shift or "").strip().lower()
        if shift_text and shift_text not in {"-", "all"}:
            filename = f"{filename}--{_safe_path_segment(shift_text, default='shift')}"
        return building_dir / f"{filename}.json"

    def _iter_candidate_paths(self, *, source_family: str = "", building: str = "") -> List[Path]:
        self.ensure_ready()
        families = (
            [self.root / _safe_path_segment(source_family, default="family")]
            if str(source_family or "").strip()
            else [path for path in self.root.iterdir() if path.is_dir()]
        )
        paths: List[Path] = []
        for family_dir in families:
            if not family_dir.exists() or not family_dir.is_dir():
                continue
            building_dirs = (
                [family_dir / _safe_path_segment(building, default="building")]
                if str(building or "").strip()
                else [path for path in family_dir.iterdir() if path.is_dir()]
            )
            for building_dir in building_dirs:
                if not building_dir.exists() or not building_dir.is_dir():
                    continue
                paths.extend(sorted(building_dir.glob("*.json")))
        return paths

    def upsert_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        self.ensure_ready()
        normalized = _normalize_entry(entry)
        path = self._entry_path(
            source_family=normalized["source_family"],
            building=normalized["building"],
            bucket_key=normalized["bucket_key"],
            duty_shift=normalized["duty_shift"],
        )
        entry_id = normalized["entry_id"]
        if entry_id:
            for candidate in self._iter_candidate_paths(
                source_family=normalized["source_family"],
                building=normalized["building"],
            ):
                if candidate == path:
                    continue
                try:
                    payload = json.loads(candidate.read_text(encoding="utf-8"))
                except Exception:
                    continue
                if not isinstance(payload, dict):
                    continue
                if _normalize_entry(payload)["entry_id"] == entry_id:
                    try:
                        candidate.unlink(missing_ok=True)
                    except Exception:
                        pass
        now_text = _now_text()
        if not normalized["created_at"]:
            normalized["created_at"] = now_text
        normalized["updated_at"] = normalized["updated_at"] or now_text
        atomic_write_text(
            path,
            json.dumps(normalized, ensure_ascii=False, indent=2),
            encoding="utf-8",
            validator=validate_json_file,
            allow_overwrite_fallback=False,
        )
        return normalized

    def delete_entry(self, entry: Dict[str, Any] | None) -> bool:
        normalized = _normalize_entry(entry)
        if not normalized["source_family"] or not normalized["building"] or not normalized["bucket_key"]:
            return False
        path = self._entry_path(
            source_family=normalized["source_family"],
            building=normalized["building"],
            bucket_key=normalized["bucket_key"],
            duty_shift=normalized["duty_shift"],
        )
        try:
            path.unlink(missing_ok=True)
        except Exception:
            return False
        return True

    def list_entries(
        self,
        *,
        source_family: str = "",
        building: str = "",
        bucket_kind: str = "",
        bucket_key: str = "",
        duty_date: str = "",
        duty_shift: str = "",
        status: str = "",
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        output: List[Dict[str, Any]] = []
        status_text = str(status or "").strip().lower()
        bucket_kind_text = str(bucket_kind or "").strip().lower()
        duty_shift_text = str(duty_shift or "").strip().lower()
        for path in self._iter_candidate_paths(source_family=source_family, building=building):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            entry = _normalize_entry(payload)
            if bucket_kind_text and entry["bucket_kind"] != bucket_kind_text:
                continue
            if str(bucket_key or "").strip() and entry["bucket_key"] != str(bucket_key or "").strip():
                continue
            if str(duty_date or "").strip() and entry["duty_date"] != str(duty_date or "").strip():
                continue
            if duty_shift_text and entry["duty_shift"] != duty_shift_text:
                continue
            if status_text and entry["status"] != status_text:
                continue
            output.append(entry)
        output.sort(key=_sort_key, reverse=True)
        return output[: max(1, int(limit or 200))]
