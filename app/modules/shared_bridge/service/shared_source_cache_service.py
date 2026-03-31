from __future__ import annotations

import copy
import hashlib
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.config.config_adapter import normalize_role_mode, resolve_shared_bridge_paths
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.shared.utils.atomic_file import atomic_copy_file, validate_excel_workbook_file
from handover_log_module.api.facade import load_handover_config
from handover_log_module.service.day_metric_standalone_upload_service import DayMetricStandaloneUploadService
from handover_log_module.service.handover_download_service import HandoverDownloadService
from pipeline_utils import load_download_module


_DEFAULT_BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"]
FAMILY_HANDOVER_LOG = "handover_log_family"
FAMILY_MONTHLY_REPORT = "monthly_report_family"
LEGACY_FAMILY_ALIASES = {
    FAMILY_HANDOVER_LOG: ("handover_family",),
    FAMILY_MONTHLY_REPORT: ("monthly_family",),
}
FAMILY_DIR_NAMES = {
    FAMILY_HANDOVER_LOG: "handover_log",
    FAMILY_MONTHLY_REPORT: "monthly_report",
}
FAMILY_LABELS = {
    FAMILY_HANDOVER_LOG: "交接班日志源文件",
    FAMILY_MONTHLY_REPORT: "全景平台月报源文件",
}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_dt() -> datetime:
    return datetime.now()


def is_accessible_cached_file_path(path: Path | str | None) -> bool:
    if path is None:
        return False
    candidate = path if isinstance(path, Path) else Path(str(path or "").strip())
    try:
        if not candidate.exists():
            return False
        if not candidate.is_file():
            return False
        candidate.stat()
    except OSError:
        return False
    return True


def _is_accessible_cached_file(path: Path | None) -> bool:
    return is_accessible_cached_file_path(path)


def _parse_hour_bucket(bucket_key: str) -> datetime | None:
    text = str(bucket_key or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H", "%Y%m%d%H"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 10:
        try:
            return datetime.strptime(digits[:10], "%Y%m%d%H")
        except ValueError:
            return None
    return None


class SharedSourceCacheService:
    def __init__(
        self,
        *,
        runtime_config: Dict[str, Any],
        store: SharedBridgeStore | None,
        download_browser_pool: Any | None = None,
        emit_log: Callable[[str], None] | None = None,
    ) -> None:
        self.runtime_config = copy.deepcopy(runtime_config if isinstance(runtime_config, dict) else {})
        self.store = store
        self.download_browser_pool = download_browser_pool
        self.emit_log = emit_log
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._current_hour_refresh_thread: threading.Thread | None = None
        self._last_error = ""
        self._last_run_at = ""
        self._last_success_at = ""
        self._current_hour_bucket = ""
        self._active_latest_downloads: Dict[tuple[str, str, str], str] = {}
        self._family_status: Dict[str, Dict[str, Any]] = {
            FAMILY_HANDOVER_LOG: {"ready_count": 0, "failed_buildings": [], "last_success_at": ""},
            FAMILY_MONTHLY_REPORT: {"ready_count": 0, "failed_buildings": [], "last_success_at": ""},
        }
        self._current_hour_refresh: Dict[str, Any] = {
            "running": False,
            "last_run_at": "",
            "last_success_at": "",
            "last_error": "",
            "failed_buildings": [],
            "scope_text": "当前小时",
        }
        self._refresh_config()

    def _refresh_config(self) -> None:
        deployment = self.runtime_config.get("deployment", {}) if isinstance(self.runtime_config.get("deployment", {}), dict) else {}
        shared_bridge = self.runtime_config.get("shared_bridge", {}) if isinstance(self.runtime_config.get("shared_bridge", {}), dict) else {}
        source_cache = self.runtime_config.get("internal_source_cache", {}) if isinstance(self.runtime_config.get("internal_source_cache", {}), dict) else {}
        resolved_bridge = resolve_shared_bridge_paths(shared_bridge, deployment.get("role_mode"))
        if isinstance(self.runtime_config, dict):
            self.runtime_config["shared_bridge"] = copy.deepcopy(resolved_bridge)
        self.role_mode = normalize_role_mode(deployment.get("role_mode"))
        self.shared_root = Path(str(resolved_bridge.get("root_dir", "") or "").strip()) if str(resolved_bridge.get("root_dir", "") or "").strip() else None
        self.enabled = bool(source_cache.get("enabled", True)) and bool(resolved_bridge.get("enabled", False)) and self.shared_root is not None
        self.run_on_startup = bool(source_cache.get("run_on_startup", True))
        self.check_interval_sec = max(5, int(source_cache.get("check_interval_sec", 30) or 30))
        self.latest_required = bool(source_cache.get("latest_required", True))
        self.history_fill_timeout_sec = max(60, int(source_cache.get("history_fill_timeout_sec", 1800) or 1800))
        self._handover_cache_root = self.shared_root / FAMILY_LABELS[FAMILY_HANDOVER_LOG] if self.shared_root else None
        self._monthly_cache_root = self.shared_root / FAMILY_LABELS[FAMILY_MONTHLY_REPORT] if self.shared_root else None
        self._tmp_root = self.shared_root / "tmp" / "source_cache" if self.shared_root else None

    def update_runtime_config(self, runtime_config: Dict[str, Any]) -> None:
        self.runtime_config = copy.deepcopy(runtime_config if isinstance(runtime_config, dict) else {})
        self._refresh_config()

    def update_download_browser_pool(self, download_browser_pool: Any | None) -> None:
        self.download_browser_pool = download_browser_pool

    def _emit(self, text: str) -> None:
        line = str(text or "").strip()
        if line and callable(self.emit_log):
            self.emit_log(line)

    def _normalize_source_family(self, source_family: str) -> str:
        text = str(source_family or "").strip().lower()
        if text in {FAMILY_HANDOVER_LOG, "handover_family"}:
            return FAMILY_HANDOVER_LOG
        if text in {FAMILY_MONTHLY_REPORT, "monthly_family"}:
            return FAMILY_MONTHLY_REPORT
        return text

    def _source_family_candidates(self, source_family: str) -> List[str]:
        normalized = self._normalize_source_family(source_family)
        aliases = LEGACY_FAMILY_ALIASES.get(normalized, ())
        return [normalized, *aliases]

    def _family_dir_name(self, source_family: str) -> str:
        normalized = self._normalize_source_family(source_family)
        return FAMILY_DIR_NAMES.get(normalized, normalized or "unknown")

    def current_hour_bucket(self, when: datetime | None = None) -> str:
        now = when or datetime.now()
        return now.strftime("%Y-%m-%d %H")

    def get_enabled_buildings(self) -> List[str]:
        configured_sites = self.runtime_config.get("internal_source_sites", [])
        if isinstance(configured_sites, list):
            output = []
            for site in configured_sites:
                if not isinstance(site, dict):
                    continue
                if not bool(site.get("enabled", True)):
                    continue
                building = str(site.get("building", "") or "").strip()
                if building and building not in output:
                    output.append(building)
            if output:
                return output
        try:
            cfg = load_handover_config(self.runtime_config)
        except Exception:
            cfg = {}
        output: List[str] = []
        for site in cfg.get("sites", []) if isinstance(cfg.get("sites", []), list) else []:
            if not isinstance(site, dict):
                continue
            if not bool(site.get("enabled", False)):
                continue
            building = str(site.get("building", "") or "").strip()
            if building and building not in output:
                output.append(building)
        return output or list(_DEFAULT_BUILDINGS)

    def start(self) -> Dict[str, Any]:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return {"started": False, "running": True, "reason": "already_running"}
            if not self.enabled or self.role_mode != "internal" or self.store is None or self.shared_root is None:
                return {"started": False, "running": False, "reason": "disabled"}
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._loop, name="shared-source-cache", daemon=True)
            self._thread.start()
            return {"started": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        with self._lock:
            thread = self._thread
            current_hour_refresh_thread = self._current_hour_refresh_thread
            if not thread:
                self._stop_event.set()
                self._current_hour_refresh_thread = None
                if current_hour_refresh_thread:
                    current_hour_refresh_thread.join(timeout=5)
                return {"stopped": False, "running": False, "reason": "not_running"}
            self._stop_event.set()
            self._thread = None
            self._current_hour_refresh_thread = None
        thread.join(timeout=5)
        if current_hour_refresh_thread:
            current_hour_refresh_thread.join(timeout=5)
        return {"stopped": True, "running": False, "reason": "stopped"}

    def is_running(self) -> bool:
        thread = self._thread
        return bool(thread and thread.is_alive())

    def get_health_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            current_bucket = self._current_hour_bucket or self.current_hour_bucket()
            families = copy.deepcopy(self._family_status)
            current_hour_refresh = copy.deepcopy(self._current_hour_refresh)
            last_run_at = self._last_run_at
            last_success_at = self._last_success_at
            last_error = self._last_error
        snapshot_error = ""
        try:
            handover_family = self._build_family_health_snapshot(source_family=FAMILY_HANDOVER_LOG, current_bucket=current_bucket)
        except Exception as exc:  # noqa: BLE001
            handover_family = {"current_bucket": current_bucket, "buildings": []}
            snapshot_error = str(exc)
        try:
            monthly_family = self._build_family_health_snapshot(source_family=FAMILY_MONTHLY_REPORT, current_bucket=current_bucket)
        except Exception as exc:  # noqa: BLE001
            monthly_family = {"current_bucket": current_bucket, "buildings": []}
            snapshot_error = snapshot_error or str(exc)
        families[FAMILY_HANDOVER_LOG] = {**families.get(FAMILY_HANDOVER_LOG, {}), **handover_family}
        families[FAMILY_MONTHLY_REPORT] = {**families.get(FAMILY_MONTHLY_REPORT, {}), **monthly_family}
        return {
            "enabled": bool(self.enabled and self.role_mode in {"internal", "external"}),
            "scheduler_running": bool(self.role_mode == "internal" and self.is_running()),
            "current_hour_bucket": current_bucket,
            "last_run_at": last_run_at,
            "last_success_at": last_success_at,
            "last_error": last_error or snapshot_error,
            "cache_root": str(self.shared_root) if self.shared_root else "",
            "current_hour_refresh": current_hour_refresh,
            FAMILY_HANDOVER_LOG: families.get(FAMILY_HANDOVER_LOG, {}),
            FAMILY_MONTHLY_REPORT: families.get(FAMILY_MONTHLY_REPORT, {}),
        }

    def _get_source_cache_entry(
        self,
        *,
        source_family: str,
        building: str,
        bucket_kind: str,
        bucket_key: str,
        status: str,
    ) -> Dict[str, Any] | None:
        if self.store is None:
            return None
        for family_name in self._source_family_candidates(source_family):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                building=building,
                bucket_kind=bucket_kind,
                bucket_key=bucket_key,
                status=status,
                limit=1,
            )
            if rows:
                return rows[0]
        return None

    def _build_building_cache_status(self, *, source_family: str, building: str, bucket_key: str) -> Dict[str, Any]:
        active_key = (self._normalize_source_family(source_family), building, bucket_key)
        with self._lock:
            active_started_at = self._active_latest_downloads.get(active_key, "")
        if active_started_at:
            return {
                "building": building,
                "bucket_key": bucket_key,
                "status": "downloading",
                "ready": False,
                "downloaded_at": "",
                "last_error": "",
                "relative_path": "",
                "resolved_file_path": "",
                "started_at": active_started_at,
            }
        ready_entry = self._get_source_cache_entry(
            source_family=source_family,
            building=building,
            bucket_kind="latest",
            bucket_key=bucket_key,
            status="ready",
        )
        if ready_entry:
            file_path = self._resolve_entry_file_path(ready_entry)
            if file_path is not None:
                return {
                    "building": building,
                    "bucket_key": bucket_key,
                    "status": "ready",
                    "ready": True,
                    "downloaded_at": str(ready_entry.get("downloaded_at", "") or "").strip(),
                    "last_error": "",
                    "relative_path": str(ready_entry.get("relative_path", "") or "").strip(),
                    "resolved_file_path": str(file_path),
                }
        failed_entry = self._get_source_cache_entry(
            source_family=source_family,
            building=building,
            bucket_kind="latest",
            bucket_key=bucket_key,
            status="failed",
        )
        if failed_entry:
            metadata = failed_entry.get("metadata", {}) if isinstance(failed_entry.get("metadata", {}), dict) else {}
            failed_file_path = self._resolve_relative_path_under_shared_root(
                str(failed_entry.get("relative_path", "") or "").strip()
            )
            return {
                "building": building,
                "bucket_key": bucket_key,
                "status": "failed",
                "ready": False,
                "downloaded_at": str(failed_entry.get("downloaded_at", "") or "").strip(),
                "last_error": str(metadata.get("error", "") or "").strip(),
                "relative_path": str(failed_entry.get("relative_path", "") or "").strip(),
                "resolved_file_path": str(failed_file_path) if failed_file_path is not None else "",
            }
        return {
            "building": building,
            "bucket_key": bucket_key,
            "status": "waiting",
            "ready": False,
            "downloaded_at": "",
            "last_error": "",
            "relative_path": "",
            "resolved_file_path": "",
        }

    def _build_family_health_snapshot(self, *, source_family: str, current_bucket: str) -> Dict[str, Any]:
        buildings = self.get_enabled_buildings()
        building_rows = [
            self._build_building_cache_status(
                source_family=source_family,
                building=building,
                bucket_key=current_bucket,
            )
            for building in buildings
        ]
        ready_count = sum(1 for item in building_rows if bool(item.get("ready")))
        failed_buildings = [
            str(item.get("building", "") or "").strip()
            for item in building_rows
            if str(item.get("status", "") or "").strip().lower() == "failed"
        ]
        last_success_candidates = [
            str(item.get("downloaded_at", "") or "").strip()
            for item in building_rows
            if str(item.get("downloaded_at", "") or "").strip()
        ]
        last_success_at = max(last_success_candidates) if last_success_candidates else ""
        return {
            "ready_count": ready_count,
            "failed_buildings": failed_buildings,
            "last_success_at": last_success_at,
            "current_bucket": current_bucket,
            "buildings": building_rows,
            "latest_selection": self.get_latest_ready_selection(
                source_family=source_family,
                buildings=buildings,
            ),
        }

    def _ensure_dirs(self) -> None:
        if self.shared_root is None or self._tmp_root is None:
            raise RuntimeError("共享缓存根目录未配置")
        if self._handover_cache_root is not None:
            self._handover_cache_root.mkdir(parents=True, exist_ok=True)
        if self._monthly_cache_root is not None:
            self._monthly_cache_root.mkdir(parents=True, exist_ok=True)
        self._tmp_root.mkdir(parents=True, exist_ok=True)

    def _hash_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _cache_file(self, *, source_path: Path, target_path: Path) -> Dict[str, Any]:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_copy_file(source_path, target_path, validator=validate_excel_workbook_file, temp_suffix=".downloading")
        return {
            "file_hash": self._hash_file(target_path),
            "size_bytes": int(target_path.stat().st_size),
            "target_path": target_path,
            "relative_path": target_path.relative_to(self.shared_root).as_posix() if self.shared_root else target_path.name,
        }

    def _family_root(self, source_family: str) -> Path:
        normalized_family = self._normalize_source_family(source_family)
        if normalized_family == FAMILY_HANDOVER_LOG and self._handover_cache_root is not None:
            return self._handover_cache_root
        if normalized_family == FAMILY_MONTHLY_REPORT and self._monthly_cache_root is not None:
            return self._monthly_cache_root
        raise RuntimeError("共享缓存根目录未配置")

    def _month_segment(self, value: str) -> str:
        digits = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
        if len(digits) >= 6:
            return digits[:6]
        now = datetime.now()
        return now.strftime("%Y%m")

    def _file_suffix(self, source_path: Path) -> str:
        suffix = str(source_path.suffix or "").strip()
        return suffix if suffix else ".xlsx"

    def _failed_marker_relative_path(
        self,
        *,
        source_family: str,
        bucket_kind: str,
        bucket_key: str,
        building: str,
    ) -> str:
        family_segment = self._family_dir_name(source_family) or "unknown"
        bucket_kind_segment = "".join(ch for ch in str(bucket_kind or "").strip().lower() if ch.isalnum() or ch in {"_", "-"}) or "bucket"
        bucket_key_segment = "".join(ch for ch in str(bucket_key or "").strip() if ch.isalnum() or ch in {"_", "-"}) or "current"
        building_segment = "".join(ch for ch in str(building or "").strip() if ch.isalnum() or ch in {"_", "-"}) or "building"
        return f"source_cache/_failed/{family_segment}/{bucket_kind_segment}/{bucket_key_segment}/{building_segment}.failed"

    def _latest_folder_name(self, bucket_key: str) -> str:
        digits = "".join(ch for ch in str(bucket_key or "").strip() if ch.isdigit())
        if len(digits) >= 10:
            return f"{digits[:8]}--{digits[8:10]}"
        return datetime.now().strftime("%Y%m%d--%H")

    def _handover_shift_text(self, duty_shift: str) -> str:
        shift = str(duty_shift or "").strip().lower()
        if shift == "day":
            return "白班"
        if shift == "night":
            return "夜班"
        return "交接班"

    def _latest_target_path(self, *, source_family: str, building: str, bucket_key: str, source_path: Path) -> Path:
        folder_name = self._latest_folder_name(bucket_key)
        month_segment = self._month_segment(folder_name)
        label = FAMILY_LABELS[self._normalize_source_family(source_family)]
        file_name = f"{folder_name}--{label}--{str(building or '').strip()}{self._file_suffix(source_path)}"
        return self._family_root(source_family) / month_segment / folder_name / file_name

    def _date_target_path(self, *, source_family: str, duty_date: str, duty_shift: str, building: str, source_path: Path) -> Path:
        duty_digits = "".join(ch for ch in str(duty_date or "").strip() if ch.isdigit())[:8]
        if len(duty_digits) != 8:
            duty_digits = datetime.now().strftime("%Y%m%d")
        month_segment = duty_digits[:6]
        normalized_family = self._normalize_source_family(source_family)
        if normalized_family == FAMILY_HANDOVER_LOG:
            period_text = self._handover_shift_text(duty_shift)
            folder_name = f"{duty_digits}--{period_text}"
            file_name = f"{duty_digits}--{period_text}--{FAMILY_LABELS[normalized_family]}--{str(building or '').strip()}{self._file_suffix(source_path)}"
        else:
            folder_name = f"{duty_digits}--月报"
            file_name = f"{duty_digits}--月报--{str(building or '').strip()}{self._file_suffix(source_path)}"
        return self._family_root(source_family) / month_segment / folder_name / file_name

    def _store_entry(self, *, source_family: str, building: str, bucket_kind: str, bucket_key: str, duty_date: str, duty_shift: str, source_path: Path, status: str, metadata: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if self.store is None:
            raise RuntimeError("共享缓存存储未初始化")
        normalized_family = self._normalize_source_family(source_family)
        target_path = self._latest_target_path(
            source_family=normalized_family,
            building=building,
            bucket_key=bucket_key,
            source_path=source_path,
        ) if bucket_kind == "latest" else self._date_target_path(
            source_family=normalized_family,
            duty_date=duty_date,
            duty_shift=duty_shift,
            building=building,
            source_path=source_path,
        )
        cached = self._cache_file(source_path=source_path, target_path=target_path)
        downloaded_at = _now_text()
        self.store.upsert_source_cache_entry(
            source_family=normalized_family,
            building=building,
            bucket_kind=bucket_kind,
            bucket_key=bucket_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
            downloaded_at=downloaded_at,
            relative_path=str(cached["relative_path"]),
            status=status,
            file_hash=str(cached["file_hash"]),
            size_bytes=int(cached["size_bytes"]),
            metadata=metadata or {},
        )
        return {
            "building": building,
            "bucket_kind": bucket_kind,
            "bucket_key": bucket_key,
            "duty_date": duty_date,
            "duty_shift": duty_shift,
            "source_family": normalized_family,
            "relative_path": str(cached["relative_path"]),
            "file_path": str(target_path),
            "downloaded_at": downloaded_at,
            "file_hash": str(cached["file_hash"]),
            "size_bytes": int(cached["size_bytes"]),
            "metadata": metadata or {},
        }

    def _record_failed_entry(
        self,
        *,
        source_family: str,
        building: str,
        bucket_kind: str,
        bucket_key: str,
        error_text: str,
        duty_date: str = "",
        duty_shift: str = "",
        metadata: Dict[str, Any] | None = None,
    ) -> None:
        if self.store is None:
            return
        payload = dict(metadata or {})
        if error_text:
            payload["error"] = error_text
        self.store.upsert_source_cache_entry(
            source_family=self._normalize_source_family(source_family),
            building=building,
            bucket_kind=bucket_kind,
            bucket_key=bucket_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
            downloaded_at=_now_text(),
            relative_path=self._failed_marker_relative_path(
                source_family=source_family,
                bucket_kind=bucket_kind,
                bucket_key=bucket_key,
                building=building,
            ),
            status="failed",
            file_hash="",
            size_bytes=0,
            metadata=payload,
        )

    def _get_ready_entry(self, *, source_family: str, building: str, bucket_kind: str, bucket_key: str = "", duty_date: str = "", duty_shift: str = "") -> Dict[str, Any] | None:
        if self.store is None:
            return None
        for family_name in self._source_family_candidates(source_family):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                building=building,
                bucket_kind=bucket_kind,
                bucket_key=bucket_key,
                duty_date=duty_date,
                duty_shift=duty_shift,
                status="ready",
                limit=1,
            )
            if rows:
                return rows[0]
        return None

    def _resolve_relative_path_under_shared_root(self, relative_path: str) -> Path | None:
        if self.shared_root is None:
            return None
        relative_text = str(relative_path or "").strip()
        if not relative_text:
            return None
        return self.shared_root / relative_text

    def _resolve_entry_file_path(self, entry: Dict[str, Any] | None) -> Path | None:
        if not isinstance(entry, dict):
            return None
        file_path = self._resolve_relative_path_under_shared_root(str(entry.get("relative_path", "") or "").strip())
        if file_path is None:
            return None
        if not _is_accessible_cached_file(file_path):
            return None
        return file_path

    def _get_latest_ready_entry_any_bucket(self, *, source_family: str, building: str) -> Dict[str, Any] | None:
        if self.store is None:
            return None
        candidates: List[Dict[str, Any]] = []
        for family_name in self._source_family_candidates(source_family):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                building=building,
                bucket_kind="latest",
                status="ready",
                limit=200,
            )
            candidates.extend(row for row in rows if isinstance(row, dict))
        for entry in candidates:
            if self._resolve_entry_file_path(entry) is not None:
                return entry
        return None

    def get_latest_ready_selection(
        self,
        *,
        source_family: str,
        buildings: List[str] | None = None,
        max_version_gap: int = 3,
        max_selection_age_hours: float = 3.0,
    ) -> Dict[str, Any]:
        requested = [
            str(item or "").strip()
            for item in (buildings or self.get_enabled_buildings())
            if str(item or "").strip()
        ]
        target_buildings = list(dict.fromkeys(requested))
        building_candidates: Dict[str, Dict[str, Any]] = {}
        latest_bucket_dt: datetime | None = None
        latest_bucket_key = ""

        for building in target_buildings:
            entry = self._get_latest_ready_entry_any_bucket(
                source_family=source_family,
                building=building,
            )
            if not entry:
                continue
            file_path = self._resolve_entry_file_path(entry)
            bucket_key = str(entry.get("bucket_key", "") or "").strip()
            bucket_dt = _parse_hour_bucket(bucket_key)
            if file_path is None or bucket_dt is None:
                continue
            candidate = {
                **entry,
                "file_path": str(file_path),
                "bucket_key": bucket_key,
                "_bucket_dt": bucket_dt,
            }
            building_candidates[building] = candidate
            if latest_bucket_dt is None or bucket_dt > latest_bucket_dt:
                latest_bucket_dt = bucket_dt
                latest_bucket_key = bucket_key

        selected_entries: List[Dict[str, Any]] = []
        fallback_buildings: List[str] = []
        missing_buildings: List[str] = []
        stale_buildings: List[str] = []
        building_rows: List[Dict[str, Any]] = []
        best_bucket_age_hours: float | None = None
        is_best_bucket_too_old = False

        if latest_bucket_dt is not None:
            age_hours = max(0.0, (_now_dt() - latest_bucket_dt).total_seconds() / 3600.0)
            best_bucket_age_hours = round(age_hours, 3)
            is_best_bucket_too_old = age_hours > float(max_selection_age_hours)

        for building in target_buildings:
            candidate = building_candidates.get(building)
            if candidate is None or latest_bucket_dt is None:
                missing_buildings.append(building)
                building_rows.append(
                    {
                        "building": building,
                        "bucket_key": latest_bucket_key,
                        "status": "waiting",
                        "using_fallback": False,
                        "version_gap": None,
                        "downloaded_at": "",
                        "last_error": "",
                        "relative_path": "",
                        "resolved_file_path": "",
                    }
                )
                continue
            version_gap = max(
                0,
                int((latest_bucket_dt - candidate["_bucket_dt"]).total_seconds() // 3600),
            )
            using_fallback = version_gap > 0
            row = {
                "building": building,
                "bucket_key": str(candidate.get("bucket_key", "") or "").strip(),
                "status": "ready",
                "using_fallback": using_fallback,
                "version_gap": version_gap,
                "downloaded_at": str(candidate.get("downloaded_at", "") or "").strip(),
                "last_error": "",
                "relative_path": str(candidate.get("relative_path", "") or "").strip(),
                "resolved_file_path": str(candidate.get("file_path", "") or "").strip(),
            }
            if version_gap > max_version_gap:
                row["status"] = "stale"
                stale_buildings.append(building)
            else:
                if using_fallback:
                    fallback_buildings.append(building)
                selected_entries.append(
                    {
                        key: value
                        for key, value in candidate.items()
                        if key != "_bucket_dt"
                    }
                )
            building_rows.append(row)

        return {
            "best_bucket_key": latest_bucket_key,
            "best_bucket_age_hours": best_bucket_age_hours,
            "is_best_bucket_too_old": is_best_bucket_too_old,
            "selected_entries": selected_entries,
            "fallback_buildings": fallback_buildings,
            "missing_buildings": missing_buildings,
            "stale_buildings": stale_buildings,
            "buildings": building_rows,
            "can_proceed": bool(target_buildings)
            and not missing_buildings
            and not stale_buildings
            and not is_best_bucket_too_old
            and len(selected_entries) == len(target_buildings),
        }

    def get_latest_ready_entries(self, *, source_family: str, buildings: List[str] | None = None, bucket_key: str | None = None) -> List[Dict[str, Any]]:
        if self.store is None:
            return []
        requested = [str(item or "").strip() for item in (buildings or self.get_enabled_buildings()) if str(item or "").strip()]
        target_bucket = str(bucket_key or "").strip()
        output: List[Dict[str, Any]] = []
        for building in requested:
            if target_bucket:
                entry = self._get_ready_entry(
                    source_family=source_family,
                    building=building,
                    bucket_kind="latest",
                    bucket_key=target_bucket,
                )
            else:
                entry = self._get_latest_ready_entry_any_bucket(source_family=source_family, building=building)
            if not entry:
                continue
            file_path = self._resolve_entry_file_path(entry)
            if file_path is None:
                continue
            output.append({**entry, "file_path": str(file_path)})
        return output

    def get_handover_by_date_entries(self, *, duty_date: str, duty_shift: str, buildings: List[str] | None = None) -> List[Dict[str, Any]]:
        if self.store is None:
            return []
        requested = [str(item or "").strip() for item in (buildings or self.get_enabled_buildings()) if str(item or "").strip()]
        output: List[Dict[str, Any]] = []
        for building in requested:
            entry = self._get_ready_entry(
                source_family=FAMILY_HANDOVER_LOG,
                building=building,
                bucket_kind="date",
                bucket_key=duty_date,
                duty_date=duty_date,
                duty_shift=duty_shift,
            )
            if not entry:
                continue
            file_path = self._resolve_entry_file_path(entry)
            if file_path is None:
                continue
            output.append({**entry, "file_path": str(file_path)})
        return output

    def get_day_metric_by_date_entries(self, *, selected_dates: List[str], buildings: List[str]) -> List[Dict[str, Any]]:
        if self.store is None:
            return []
        output: List[Dict[str, Any]] = []
        for duty_date in [str(item or "").strip() for item in (selected_dates or []) if str(item or "").strip()]:
            for building in [str(item or "").strip() for item in (buildings or []) if str(item or "").strip()]:
                entry = self._get_ready_entry(
                    source_family=FAMILY_HANDOVER_LOG,
                    building=building,
                    bucket_kind="date",
                    bucket_key=duty_date,
                    duty_date=duty_date,
                    duty_shift="all",
                )
                if not entry:
                    continue
                file_path = self._resolve_entry_file_path(entry)
                if file_path is None:
                    continue
                output.append({**entry, "file_path": str(file_path)})
        return output

    def get_monthly_by_date_entries(self, *, selected_dates: List[str], buildings: List[str] | None = None) -> List[Dict[str, Any]]:
        if self.store is None:
            return []
        requested = [str(item or "").strip() for item in (buildings or self.get_enabled_buildings()) if str(item or "").strip()]
        output: List[Dict[str, Any]] = []
        for duty_date in [str(item or "").strip() for item in (selected_dates or []) if str(item or "").strip()]:
            for building in requested:
                entry = self._get_ready_entry(
                    source_family=FAMILY_MONTHLY_REPORT,
                    building=building,
                    bucket_kind="date",
                    bucket_key=duty_date,
                    duty_date=duty_date,
                )
                if not entry:
                    continue
                file_path = self._resolve_entry_file_path(entry)
                if file_path is None:
                    continue
                output.append({**entry, "file_path": str(file_path)})
        return output

    def _prepare_monthly_runtime_config(self, *, buildings: List[str], save_dir: Path) -> Dict[str, Any]:
        cfg = copy.deepcopy(self.runtime_config if isinstance(self.runtime_config, dict) else {})
        download_cfg = cfg.setdefault("download", {})
        if not isinstance(download_cfg, dict):
            download_cfg = {}
            cfg["download"] = download_cfg
        feishu_cfg = cfg.setdefault("feishu", {})
        if not isinstance(feishu_cfg, dict):
            feishu_cfg = {}
            cfg["feishu"] = feishu_cfg
        feishu_cfg["enable_upload"] = False
        download_cfg["save_dir"] = str(save_dir)
        download_cfg["run_subdir_mode"] = "none"
        site_rows = [site for site in download_cfg.get("sites", []) if isinstance(site, dict)] if isinstance(download_cfg.get("sites", []), list) else []
        if site_rows:
            filtered = [site for site in site_rows if str(site.get("building", "") or "").strip() in buildings]
            if filtered:
                download_cfg["sites"] = filtered
        input_cfg = cfg.get("input", {}) if isinstance(cfg.get("input", {}), dict) else {}
        input_cfg["buildings"] = list(buildings)
        cfg["input"] = input_cfg
        return cfg

    def _handover_temp_root(
        self,
        *,
        bucket_kind: str,
        bucket_key: str,
        duty_date: str = "",
        duty_shift: str = "",
        building: str = "",
    ) -> Path:
        if self._tmp_root is None:
            raise RuntimeError("共享缓存临时目录未配置")
        if bucket_kind == "latest":
            return self._tmp_root / "handover_latest" / bucket_key / building
        shift_segment = str(duty_shift or "all").strip().lower() or "all"
        return self._tmp_root / "handover_by_date" / str(duty_date or "manual").strip() / shift_segment

    def fill_handover_latest(self, *, building: str, bucket_key: str, emit_log: Callable[[str], None]) -> Dict[str, Any]:
        cfg = load_handover_config(self.runtime_config)
        temp_root = self._handover_temp_root(
            bucket_kind="latest",
            bucket_key=bucket_key,
            building=building,
        )
        service = HandoverDownloadService(
            cfg,
            download_browser_pool=self.download_browser_pool,
            business_root_override=temp_root,
        )
        result = service.run(buildings=[building], switch_network=False, reuse_cached=False, emit_log=emit_log)
        success_files = result.get("success_files", []) if isinstance(result.get("success_files", []), list) else []
        if not success_files:
            raise RuntimeError(f"本小时缓存下载失败: {building}")
        item = success_files[0]
        source_path = Path(str(item.get("file_path", "") or "").strip())
        if not source_path.exists():
            raise FileNotFoundError(f"下载完成后的源文件不存在: {source_path}")
        return self._store_entry(
            source_family=FAMILY_HANDOVER_LOG,
            building=building,
            bucket_kind="latest",
            bucket_key=bucket_key,
            duty_date=str(result.get("duty_date", "") or "").strip(),
            duty_shift=str(result.get("duty_shift", "") or "").strip().lower(),
            source_path=source_path,
            status="ready",
            metadata={"family": FAMILY_HANDOVER_LOG, "building": building},
        )

    def fill_handover_history(self, *, buildings: List[str], duty_date: str, duty_shift: str, emit_log: Callable[[str], None]) -> List[Dict[str, Any]]:
        cfg = load_handover_config(self.runtime_config)
        temp_root = self._handover_temp_root(
            bucket_kind="date",
            bucket_key=duty_date,
            duty_date=duty_date,
            duty_shift=duty_shift,
        )
        service = HandoverDownloadService(
            cfg,
            download_browser_pool=self.download_browser_pool,
            business_root_override=temp_root,
        )
        result = service.run(buildings=buildings, duty_date=duty_date, duty_shift=duty_shift, switch_network=False, reuse_cached=False, emit_log=emit_log)
        success_files = result.get("success_files", []) if isinstance(result.get("success_files", []), list) else []
        output: List[Dict[str, Any]] = []
        for item in success_files:
            building = str(item.get("building", "") or "").strip()
            source_path = Path(str(item.get("file_path", "") or "").strip())
            if not building or not source_path.exists():
                continue
            output.append(
                self._store_entry(
                    source_family=FAMILY_HANDOVER_LOG,
                    building=building,
                    bucket_kind="date",
                    bucket_key=duty_date,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    source_path=source_path,
                    status="ready",
                    metadata={"family": FAMILY_HANDOVER_LOG, "building": building, "duty_date": duty_date, "duty_shift": duty_shift},
                )
            )
        return output

    def fill_day_metric_history(self, *, selected_dates: List[str], building_scope: str, building: str | None, emit_log: Callable[[str], None]) -> List[Dict[str, Any]]:
        service = DayMetricStandaloneUploadService(self.runtime_config, download_browser_pool=self.download_browser_pool)
        result = service.run_download_only(selected_dates=selected_dates, building_scope=building_scope, building=building, emit_log=emit_log)
        rows = result.get("downloaded_files", []) if isinstance(result.get("downloaded_files", []), list) else []
        output: List[Dict[str, Any]] = []
        for item in rows:
            duty_date = str(item.get("duty_date", "") or "").strip()
            building_name = str(item.get("building", "") or "").strip()
            source_path = Path(str(item.get("source_file", "") or "").strip())
            if not duty_date or not building_name or not source_path.exists():
                continue
            output.append(
                self._store_entry(
                    source_family=FAMILY_HANDOVER_LOG,
                    building=building_name,
                    bucket_kind="date",
                    bucket_key=duty_date,
                    duty_date=duty_date,
                    duty_shift="all",
                    source_path=source_path,
                    status="ready",
                    metadata={"family": FAMILY_HANDOVER_LOG, "building": building_name, "duty_date": duty_date, "duty_shift": "all"},
                )
            )
        return output

    def fill_monthly_latest(self, *, building: str, bucket_key: str, emit_log: Callable[[str], None]) -> List[Dict[str, Any]]:
        if self._tmp_root is None:
            raise RuntimeError("共享缓存临时目录未配置")
        save_dir = self._tmp_root / "monthly_latest" / bucket_key / building
        save_dir.mkdir(parents=True, exist_ok=True)
        cfg = self._prepare_monthly_runtime_config(buildings=[building], save_dir=save_dir)
        module = load_download_module()
        result = module.run_download_only_auto_once(cfg, source_name=f"共享缓存-月报-{building}")
        file_items = result.get("file_items", []) if isinstance(result.get("file_items", []), list) else []
        output: List[Dict[str, Any]] = []
        for item in file_items:
            building_name = str(item.get("building", "") or "").strip()
            source_path = Path(str(item.get("file_path", "") or "").strip())
            if building_name != building or not source_path.exists():
                continue
            output.append(
                self._store_entry(
                    source_family=FAMILY_MONTHLY_REPORT,
                    building=building_name,
                    bucket_kind="latest",
                    bucket_key=bucket_key,
                    duty_date=str(item.get("upload_date", "") or "").strip(),
                    duty_shift="",
                    source_path=source_path,
                    status="ready",
                    metadata={"family": FAMILY_MONTHLY_REPORT, "building": building_name, "upload_date": str(item.get("upload_date", "") or "").strip()},
                )
            )
        return output

    def fill_monthly_history(self, *, selected_dates: List[str], buildings: List[str] | None = None, emit_log: Callable[[str], None]) -> List[Dict[str, Any]]:
        if self._tmp_root is None:
            raise RuntimeError("共享缓存临时目录未配置")
        target_buildings = [str(item or "").strip() for item in (buildings or self.get_enabled_buildings()) if str(item or "").strip()]
        save_dir = self._tmp_root / "monthly_by_date" / ("_".join(selected_dates) or "manual")
        save_dir.mkdir(parents=True, exist_ok=True)
        cfg = self._prepare_monthly_runtime_config(buildings=target_buildings, save_dir=save_dir)
        module = load_download_module()
        result = module.run_download_only_with_selected_dates(cfg, selected_dates=selected_dates, source_name="共享缓存-月报历史日期")
        file_items = result.get("file_items", []) if isinstance(result.get("file_items", []), list) else []
        output: List[Dict[str, Any]] = []
        for item in file_items:
            building = str(item.get("building", "") or "").strip()
            duty_date = str(item.get("upload_date", "") or "").strip()
            source_path = Path(str(item.get("file_path", "") or "").strip())
            if not building or not duty_date or not source_path.exists():
                continue
            output.append(
                self._store_entry(
                    source_family=FAMILY_MONTHLY_REPORT,
                    building=building,
                    bucket_kind="date",
                    bucket_key=duty_date,
                    duty_date=duty_date,
                    duty_shift="",
                    source_path=source_path,
                    status="ready",
                    metadata={"family": FAMILY_MONTHLY_REPORT, "building": building, "upload_date": duty_date},
                )
            )
        return output

    def _entry_exists_for_bucket(self, *, source_family: str, building: str, bucket_kind: str, bucket_key: str) -> bool:
        if self.store is None:
            return False
        for family_name in self._source_family_candidates(source_family):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                building=building,
                bucket_kind=bucket_kind,
                bucket_key=bucket_key,
                limit=1,
            )
            if rows:
                return True
        return False

    def _refresh_family_bucket(
        self,
        *,
        source_family: str,
        bucket_key: str,
        fill_func: Callable[..., Any],
        force_retry_failed: bool = False,
    ) -> None:
        buildings = self.get_enabled_buildings()
        ready_count = 0
        failed_buildings: List[str] = []
        for building in buildings:
            entry_exists = self._entry_exists_for_bucket(
                source_family=source_family,
                building=building,
                bucket_kind="latest",
                bucket_key=bucket_key,
            )
            ready_entry = self._get_ready_entry(
                source_family=source_family,
                building=building,
                bucket_kind="latest",
                bucket_key=bucket_key,
            )
            if ready_entry:
                ready_count += 1
                continue
            if entry_exists and not force_retry_failed:
                failed_buildings.append(building)
                continue
            active_key = (self._normalize_source_family(source_family), building, bucket_key)
            try:
                with self._lock:
                    self._active_latest_downloads[active_key] = _now_text()
                fill_func(building=building, bucket_key=bucket_key, emit_log=self._emit)
                ready_count += 1
                with self._lock:
                    self._last_success_at = _now_text()
            except Exception as exc:  # noqa: BLE001
                failed_buildings.append(building)
                error_text = str(exc)
                with self._lock:
                    self._last_error = error_text
                self._record_failed_entry(
                    source_family=source_family,
                    building=building,
                    bucket_kind="latest",
                    bucket_key=bucket_key,
                    error_text=error_text,
                    metadata={"family": self._normalize_source_family(source_family), "building": building},
                )
                self._emit(f"[共享缓存] 本小时预下载失败 family={source_family} building={building}: {exc}")
            finally:
                with self._lock:
                    self._active_latest_downloads.pop(active_key, None)
        with self._lock:
            family_status = self._family_status.setdefault(source_family, {})
            family_status["ready_count"] = ready_count
            family_status["failed_buildings"] = failed_buildings
            family_status["last_success_at"] = self._last_success_at if ready_count > 0 else family_status.get("last_success_at", "")
            family_status["current_bucket"] = bucket_key

    def _run_current_bucket_once(self) -> None:
        self._ensure_dirs()
        current_bucket = self.current_hour_bucket()
        with self._lock:
            self._current_hour_bucket = current_bucket
        self._refresh_family_bucket(source_family=FAMILY_HANDOVER_LOG, bucket_key=current_bucket, fill_func=self.fill_handover_latest)
        self._refresh_family_bucket(source_family=FAMILY_MONTHLY_REPORT, bucket_key=current_bucket, fill_func=self.fill_monthly_latest)
        with self._lock:
            self._last_run_at = _now_text()
            handover_failed = list(self._family_status.get(FAMILY_HANDOVER_LOG, {}).get("failed_buildings", []) or [])
            monthly_failed = list(self._family_status.get(FAMILY_MONTHLY_REPORT, {}).get("failed_buildings", []) or [])
            if not handover_failed and not monthly_failed:
                self._last_error = ""

    def _mark_current_hour_refresh(self, **fields: Any) -> None:
        with self._lock:
            self._current_hour_refresh.update(fields)

    def _run_current_hour_refresh_impl(self) -> None:
        self._ensure_dirs()
        bucket_key = self.current_hour_bucket()
        failed_units: List[str] = []
        self._mark_current_hour_refresh(
            running=True,
            last_run_at=_now_text(),
            last_success_at="",
            last_error="",
            failed_buildings=[],
        )
        self._emit(f"[共享缓存] 开始立即补下当前小时全部文件 bucket={bucket_key}")
        self._refresh_family_bucket(
            source_family=FAMILY_HANDOVER_LOG,
            bucket_key=bucket_key,
            fill_func=self.fill_handover_latest,
            force_retry_failed=True,
        )
        self._refresh_family_bucket(
            source_family=FAMILY_MONTHLY_REPORT,
            bucket_key=bucket_key,
            fill_func=self.fill_monthly_latest,
            force_retry_failed=True,
        )
        for family_key in (FAMILY_HANDOVER_LOG, FAMILY_MONTHLY_REPORT):
            family_status = self._family_status.get(family_key, {})
            for building in family_status.get("failed_buildings", []) or []:
                failed_units.append(f"{building}/{family_key}")
        success_at = _now_text() if not failed_units else ""
        last_error = self._last_error if failed_units else ""
        with self._lock:
            self._last_run_at = _now_text()
            if not failed_units:
                self._last_error = ""
        self._mark_current_hour_refresh(
            running=False,
            last_success_at=success_at,
            last_error=last_error,
            failed_buildings=failed_units,
        )
        if failed_units:
            self._emit(f"[共享缓存] 当前小时立即补下结束：存在失败项 {', '.join(failed_units)}")
        else:
            self._emit("[共享缓存] 当前小时立即补下完成")

    def _run_current_hour_refresh_background(self) -> None:
        try:
            self._run_current_hour_refresh_impl()
        finally:
            with self._lock:
                self._current_hour_refresh_thread = None

    def start_current_hour_refresh(self) -> Dict[str, Any]:
        if not self.enabled or self.role_mode != "internal" or self.store is None:
            return {"accepted": False, "running": False, "reason": "disabled"}
        with self._lock:
            if bool(self._current_hour_refresh.get("running")):
                return {"accepted": False, "running": True, "reason": "already_running"}
            thread = self._current_hour_refresh_thread
            if thread and thread.is_alive():
                return {"accepted": False, "running": True, "reason": "already_running"}
            self._current_hour_refresh_thread = threading.Thread(
                target=self._run_current_hour_refresh_background,
                name="shared-source-cache-current-hour",
                daemon=True,
            )
            self._current_hour_refresh_thread.start()
        return {
            "accepted": True,
            "running": True,
            "reason": "started",
            "scope": "current_hour",
            "bucket_key": self.current_hour_bucket(),
        }

    def start_today_full_refresh(self) -> Dict[str, Any]:
        return self.start_current_hour_refresh()

    def _loop(self) -> None:
        startup_done = False
        while not self._stop_event.is_set():
            try:
                if not self.enabled or self.role_mode != "internal" or self.store is None:
                    self._stop_event.wait(self.check_interval_sec)
                    continue
                if self.run_on_startup and not startup_done:
                    self._run_current_bucket_once()
                    startup_done = True
                else:
                    bucket = self.current_hour_bucket()
                    if bucket != self._current_hour_bucket:
                        self._run_current_bucket_once()
                if startup_done and not self._last_error:
                    self._emit(f"[共享缓存] 小时预下载调度运行中: bucket={self._current_hour_bucket}")
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                self._last_run_at = _now_text()
            self._stop_event.wait(self.check_interval_sec)
