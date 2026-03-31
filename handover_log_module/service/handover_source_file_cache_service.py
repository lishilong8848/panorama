from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
import re
from typing import Any, Callable, Dict, Iterable

from pipeline_utils import get_app_dir
from app.shared.utils.atomic_file import (
    atomic_copy_file,
    atomic_write_text,
    validate_excel_workbook_file,
    validate_non_empty_file,
)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class HandoverSourceFileCacheService:
    CACHE_RETENTION_DAYS = 30
    SHARED_ROOT_NAME = "交接班共享源文件"
    DOWNLOAD_CACHE_DIR = "download_cache"
    DOWNLOAD_INDEX_FILE = "_download_index.json"

    def __init__(self, handover_cfg: Dict[str, Any], *, business_root_override: str | Path | None = None) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}
        self._business_root_override = Path(business_root_override) if business_root_override else None

    def _business_root(self) -> Path:
        if self._business_root_override is not None:
            root = self._business_root_override
            if not root.is_absolute():
                root = get_app_dir() / root
            root.mkdir(parents=True, exist_ok=True)
            return root
        global_paths = self.handover_cfg.get("_global_paths", {})
        root_text = ""
        if isinstance(global_paths, dict):
            root_text = str(global_paths.get("download_save_dir", "")).strip()
            if not root_text:
                root_text = str(global_paths.get("excel_dir", "")).strip()
        if not root_text:
            global_download = self.handover_cfg.get("_global_download", {})
            if isinstance(global_download, dict):
                root_text = str(global_download.get("save_dir", "")).strip()
        if not root_text:
            template_cfg = self.handover_cfg.get("template", {})
            if isinstance(template_cfg, dict):
                output_dir = str(template_cfg.get("output_dir", "")).strip()
                if output_dir:
                    root_text = str(Path(output_dir).parent)
        global_paths = self.handover_cfg.get("_global_paths", {})
        if not root_text and isinstance(global_paths, dict):
            root_text = str(global_paths.get("runtime_state_root", "")).strip()
        root = Path(root_text) if root_text else get_app_dir() / ".runtime"
        if not root.is_absolute():
            root = get_app_dir() / root
        root.mkdir(parents=True, exist_ok=True)
        return root

    def cache_root(self) -> Path:
        root = self._business_root() / self.SHARED_ROOT_NAME
        root.mkdir(parents=True, exist_ok=True)
        return root

    def download_cache_root(self) -> Path:
        root = self.cache_root() / self.DOWNLOAD_CACHE_DIR
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _download_index_path(self) -> Path:
        return self.cache_root() / self.DOWNLOAD_INDEX_FILE

    def _load_download_index(self) -> Dict[str, Any]:
        path = self._download_index_path()
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}
        if not isinstance(payload, dict):
            return {}
        output: Dict[str, Any] = {}
        for key, value in payload.items():
            if not isinstance(value, dict):
                continue
            output[str(key)] = dict(value)
        return output

    def _save_download_index(self, payload: Dict[str, Any]) -> None:
        path = self._download_index_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(
            path,
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _validate_cached_source(path: Path) -> None:
        suffix = path.suffix.lower()
        if suffix in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
            validate_excel_workbook_file(path)
            return
        validate_non_empty_file(path)

    @staticmethod
    def build_download_identity(
        *,
        building: str,
        template_name: str,
        duty_date: str = "",
        duty_shift: str = "",
        start_time: str = "",
        end_time: str = "",
        scale_label: str = "",
    ) -> str:
        return "|".join(
            [
                str(building or "").strip(),
                str(template_name or "").strip(),
                str(duty_date or "").strip(),
                str(duty_shift or "").strip().lower(),
                str(start_time or "").strip(),
                str(end_time or "").strip(),
                str(scale_label or "").strip(),
            ]
        )

    def lookup_downloaded_source(self, *, identity: str) -> str:
        identity_text = str(identity or "").strip()
        if not identity_text:
            return ""
        payload = self._load_download_index()
        record = payload.get(identity_text, {})
        if not isinstance(record, dict):
            return ""
        file_path = str(record.get("file_path", "") or "").strip()
        if not file_path:
            return ""
        path = Path(file_path)
        if not path.exists():
            return ""
        try:
            self._validate_cached_source(path)
        except Exception:
            payload.pop(identity_text, None)
            self._save_download_index(payload)
            return ""
        return str(path)

    def register_downloaded_source(
        self,
        *,
        identity: str,
        file_path: str,
        emit_log: Callable[[str], None] = print,
    ) -> None:
        identity_text = str(identity or "").strip()
        file_text = str(file_path or "").strip()
        if not identity_text or not file_text:
            return
        file_path_obj = Path(file_text)
        if not file_path_obj.exists():
            return
        self._validate_cached_source(file_path_obj)
        payload = self._load_download_index()
        payload[identity_text] = {
            "file_path": file_text,
            "updated_at": _now_text(),
        }
        self._save_download_index(payload)
        emit_log(f"[交接班][源文件缓存] 已登记共享源文件 identity={identity_text}, file={file_text}")

    @staticmethod
    def _sanitize_path_part(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return "_"
        sanitized = re.sub(r'[<>:"/\\\\|?*]+', "_", text)
        sanitized = sanitized.strip(" .")
        return sanitized or "_"

    def _cleanup_empty_parents(self, path: Path) -> None:
        cache_root = self.cache_root().resolve()
        current = path.parent
        while True:
            try:
                current_resolved = current.resolve()
            except Exception:  # noqa: BLE001
                return
            if current_resolved == cache_root:
                return
            try:
                current.rmdir()
            except OSError:
                return
            current = current.parent

    def is_managed_path(self, path: str | Path) -> bool:
        raw_path = Path(path)
        try:
            raw_path.resolve().relative_to(self.cache_root().resolve())
            return True
        except Exception:  # noqa: BLE001
            return False

    def build_stored_path(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        session_id: str,
        original_name: str,
    ) -> Path:
        suffix = Path(str(original_name or "").strip() or "source.xlsx").suffix or ".xlsx"
        duty_folder = f"{str(duty_date or '').strip()}_{str(duty_shift or '').strip().lower()}"
        return (
            self.cache_root()
            / self._sanitize_path_part(duty_folder)
            / self._sanitize_path_part(building)
            / self._sanitize_path_part(session_id)
            / f"source{suffix}"
        )

    def persist_uploaded_source(
        self,
        *,
        source_path: str,
        building: str,
        duty_date: str,
        duty_shift: str,
        session_id: str,
        original_name: str,
        previous_stored_path: str = "",
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        source = Path(str(source_path or "").strip())
        if not source.exists():
            raise FileNotFoundError(f"source file missing before cache persist: {source}")

        target = self.build_stored_path(
            building=building,
            duty_date=duty_date,
            duty_shift=duty_shift,
            session_id=session_id,
            original_name=original_name,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        source_resolved = source.resolve()
        target_resolved = target.resolve()
        if source_resolved != target_resolved:
            self._validate_cached_source(source)
            atomic_copy_file(
                source,
                target,
                validator=self._validate_cached_source,
                temp_suffix=".downloading",
            )
        else:
            self._validate_cached_source(target)
        stored_at = _now_text()

        previous_text = str(previous_stored_path or "").strip()
        previous_path = Path(previous_text) if previous_text else None
        if previous_path is not None:
            try:
                previous_resolved = previous_path.resolve()
            except Exception:  # noqa: BLE001
                previous_resolved = previous_path
            if previous_text and self.is_managed_path(previous_path) and previous_resolved != target_resolved:
                try:
                    if previous_path.exists():
                        previous_path.unlink()
                    self._cleanup_empty_parents(previous_path)
                    emit_log(
                        f"[交接班][源文件缓存] 已替换旧缓存 building={building}, "
                        f"old={previous_text}, new={str(target)}"
                    )
                except Exception as exc:  # noqa: BLE001
                    emit_log(f"[交接班][源文件缓存] 旧缓存清理失败 building={building}: {exc}")

        emit_log(
            f"[交接班][源文件缓存] 已持久化 building={building}, session={session_id}, path={str(target)}"
        )
        return {
            "managed": True,
            "stored_path": str(target),
            "original_name": Path(str(original_name or "").strip() or source.name).name,
            "stored_at": stored_at,
            "cleanup_status": "active",
            "cleanup_at": "",
        }

    def remove_managed_source(
        self,
        stored_path: str,
        *,
        emit_log: Callable[[str], None] = print,
    ) -> bool:
        raw = str(stored_path or "").strip()
        if not raw:
            return False
        path = Path(raw)
        if not self.is_managed_path(path):
            return False
        if not path.exists():
            return False
        try:
            path.unlink()
            self._cleanup_empty_parents(path)
            emit_log(f"[交接班][源文件缓存] 已移除缓存 path={raw}")
            return True
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][源文件缓存] 缓存移除失败 path={raw}, error={exc}")
            return False

    def cleanup_orphan_sources(
        self,
        *,
        referenced_paths: Iterable[str],
        emit_log: Callable[[str], None] = print,
    ) -> int:
        cache_root = self.cache_root()
        referenced: set[str] = set()
        for item in referenced_paths:
            text = str(item or "").strip()
            if not text:
                continue
            try:
                path = Path(text).resolve()
            except Exception:  # noqa: BLE001
                continue
            if self.is_managed_path(path):
                referenced.add(str(path))

        cutoff = datetime.now() - timedelta(days=self.CACHE_RETENTION_DAYS)
        removed = 0
        for candidate in cache_root.rglob("source*"):
            if not candidate.is_file():
                continue
            try:
                resolved = candidate.resolve()
            except Exception:  # noqa: BLE001
                continue
            if str(resolved) in referenced:
                continue
            try:
                modified_at = datetime.fromtimestamp(candidate.stat().st_mtime)
            except Exception:  # noqa: BLE001
                continue
            if modified_at >= cutoff:
                continue
            try:
                candidate.unlink()
                self._cleanup_empty_parents(candidate)
                removed += 1
            except Exception:  # noqa: BLE001
                continue
        if removed:
            emit_log(f"[交接班][源文件缓存] 清理孤儿缓存 count={removed}")
        return removed
