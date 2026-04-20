from __future__ import annotations

import ast
import concurrent.futures
import copy
import hashlib
import json
import threading
import time
from datetime import datetime, time as dt_time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Dict, List

import openpyxl

from app.modules.shared_bridge.service.alarm_event_page_export_service import (
    build_alarm_event_json_document,
    collect_alarm_event_rows,
    load_alarm_event_json,
    stream_alarm_event_json_document,
    scheduled_bucket_for_time,
    write_alarm_event_json,
)
from app.modules.shared_bridge.service.alarm_external_selection import build_alarm_external_selection
from app.modules.shared_bridge.service.bridge_status_presenter import (
    present_alarm_event_family,
    present_current_hour_refresh_overview,
    present_external_source_cache_overview,
    present_external_source_cache_family,
    present_latest_selection_overview,
    present_internal_source_cache_overview,
    present_source_cache_family,
)
from app.modules.feishu.service.bitable_target_resolver import BitableTargetResolver, build_bitable_url
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.config.config_adapter import normalize_role_mode, resolve_shared_bridge_paths
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from app.modules.sheet_import.core.field_value_converter import parse_timestamp_ms
from app.shared.utils.atomic_file import atomic_copy_file, validate_excel_workbook_file
from app.shared.utils.artifact_naming import build_source_artifact_path
from handover_log_module.api.facade import load_handover_config
from handover_log_module.repository.excel_reader import load_rows
from handover_log_module.service.handover_download_service import HandoverDownloadService
from pipeline_utils import load_download_module


_DEFAULT_BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"]
FAMILY_HANDOVER_LOG = "handover_log_family"
FAMILY_HANDOVER_CAPACITY_REPORT = "handover_capacity_report_family"
FAMILY_MONTHLY_REPORT = "monthly_report_family"
FAMILY_ALARM_EVENT = "alarm_event_family"
LEGACY_FAMILY_ALIASES = {
    FAMILY_HANDOVER_LOG: ("handover_family",),
    FAMILY_MONTHLY_REPORT: ("monthly_family",),
}
FAMILY_DIR_NAMES = {
    FAMILY_HANDOVER_LOG: "handover_log",
    FAMILY_HANDOVER_CAPACITY_REPORT: "handover_capacity_report",
    FAMILY_MONTHLY_REPORT: "monthly_report",
    FAMILY_ALARM_EVENT: "alarm_event",
}
FAMILY_LABELS = {
    FAMILY_HANDOVER_LOG: "交接班日志源文件",
    FAMILY_HANDOVER_CAPACITY_REPORT: "交接班容量报表源文件",
    FAMILY_MONTHLY_REPORT: "全景平台月报源文件",
    FAMILY_ALARM_EVENT: "告警信息源文件",
}

ALARM_EVENT_BITABLE_TARGET_FIELDS = {
    "level": "等级",
    "building": "楼栋",
    "content": "告警内容",
    "position": "具体位置",
    "object": "监控对象",
    "event_time": "产生时间",
    "accept_time": "受理时间",
    "is_accept": "处理状态",
    "accept_by": "受理人",
    "accept_content": "受理描述",
    "recover_time": "恢复时间",
    "is_recover": "恢复状态",
    "event_suggest": "处理建议",
    "event_type": "告警类型",
    "trigger_value": "触发值",
    "confirm_type": "告警分类",
    "confirm_time": "确认时间",
    "confirm_by": "确认人",
    "confirm_description": "确认描述",
}
ALARM_EVENT_ALLOWED_UPLOAD_KEYS = tuple(ALARM_EVENT_BITABLE_TARGET_FIELDS.keys())

ALARM_EVENT_HEADER_ALIASES = {
    "level": {"级别", "等级", "level"},
    "content": {"内容", "告警内容", "content"},
    "position": {"位置", "具体位置", "position"},
    "object": {"对象", "监控对象", "object"},
    "event_time": {"告警时间", "产生时间", "event_time"},
    "accept_time": {"接警时间", "受理时间", "accept_time"},
    "is_accept": {"处理状态", "is_accept"},
    "accept_by": {"处理人", "受理人", "accept_by"},
    "accept_content": {"处理内容", "受理描述", "accept_content"},
    "recover_time": {"恢复时间", "recover_time"},
    "is_recover": {"恢复状态", "is_recover"},
    "event_suggest": {"建议", "处理建议", "event_suggest"},
    "event_type": {"事件类型", "告警类型", "event_type"},
    "confirm_type": {"确认类型", "告警分类", "confirm_type"},
    "confirm_time": {"确认时间", "confirm_time"},
    "confirm_by": {"确认人", "confirm_by"},
    "confirm_description": {"确认说明", "确认描述", "confirm_description"},
    "trigger_value": {"触发值", "实时值", "当前值", "real_value", "trigger_value"},
}

ALARM_EVENT_BITABLE_DATETIME_FIELDS = {
    "event_time",
    "accept_time",
    "recover_time",
    "confirm_time",
}
ALARM_EVENT_BITABLE_NUMBER_FIELDS = {
    "trigger_value",
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


def _parse_datetime_text(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("/", "-")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _normalize_nullable_text(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() in {"none", "null", "undefined"}:
        return ""
    return text


class SharedSourceCacheService:
    EXTERNAL_FULL_SNAPSHOT_MAX_AGE_SEC = 15.0
    REFRESHING_ENTRY_TIMEOUT_SEC = 600.0
    REPAIR_SWEEP_COOLDOWN_SEC = 30.0
    ORPHAN_FILE_RETENTION_DAYS = 60

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
        self._active_latest_downloads: Dict[tuple[str, str, str, str], str] = {}
        self._building_latest_refresh_threads: Dict[tuple[str, str, str, str], threading.Thread] = {}
        self._manual_alarm_refresh_thread: threading.Thread | None = None
        self._manual_alarm_refresh: Dict[str, Any] = {
            "running": False,
            "last_run_at": "",
            "last_success_at": "",
            "last_error": "",
            "bucket_key": "",
            "successful_buildings": [],
            "failed_buildings": [],
            "blocked_buildings": [],
            "total_row_count": 0,
            "building_row_counts": {},
            "query_start": "",
            "query_end": "",
        }
        self._monthly_download_module: Any | None = None
        self._monthly_download_module_lock = threading.Lock()
        self._family_status: Dict[str, Dict[str, Any]] = {
            FAMILY_HANDOVER_LOG: {"ready_count": 0, "failed_buildings": [], "blocked_buildings": [], "last_success_at": ""},
            FAMILY_HANDOVER_CAPACITY_REPORT: {"ready_count": 0, "failed_buildings": [], "blocked_buildings": [], "last_success_at": ""},
            FAMILY_MONTHLY_REPORT: {"ready_count": 0, "failed_buildings": [], "blocked_buildings": [], "last_success_at": ""},
            FAMILY_ALARM_EVENT: {"ready_count": 0, "failed_buildings": [], "blocked_buildings": [], "last_success_at": ""},
        }
        self._light_building_status: Dict[str, Dict[str, Dict[str, Any]]] = {
            FAMILY_HANDOVER_LOG: {},
            FAMILY_HANDOVER_CAPACITY_REPORT: {},
            FAMILY_MONTHLY_REPORT: {},
            FAMILY_ALARM_EVENT: {},
        }
        self._current_hour_refresh: Dict[str, Any] = {
            "running": False,
            "last_run_at": "",
            "last_success_at": "",
            "last_error": "",
            "failed_buildings": [],
            "blocked_buildings": [],
            "running_buildings": [],
            "completed_buildings": [],
            "scope_text": "当前小时 / 最近定时",
        }
        self._last_scheduler_log_signature = ""
        self._alarm_external_upload_state: Dict[str, Any] = {
            "running": False,
            "started_at": "",
            "current_mode": "",
            "current_scope": "",
            "last_run_at": "",
            "last_success_at": "",
            "last_error": "",
            "last_mode": "",
            "last_scope": "",
            "uploaded_record_count": 0,
            "uploaded_file_count": 0,
            "consumed_count": 0,
            "failed_entries": [],
            "deleted_before_upload_count": 0,
        }
        self._external_full_snapshot_cache: Dict[str, Any] = {}
        self._external_full_snapshot_dirty = True
        self._external_full_snapshot_built_monotonic = 0.0
        self._last_refreshing_repair_monotonic = 0.0
        self._refresh_config()
        with self._lock:
            self._reset_light_building_state_unlocked()

    def _mark_external_full_snapshot_dirty(self) -> None:
        with self._lock:
            self._external_full_snapshot_dirty = True

    def _log_source_cache_event(self, message: str) -> None:
        line = str(message or "").strip()
        if line:
            self._emit(f"[共享缓存] {line}")

    def _repair_entry_to_failed(
        self,
        entry: Dict[str, Any] | None,
        *,
        error_text: str,
        log_reason: str,
    ) -> Dict[str, Any] | None:
        if self.store is None or not isinstance(entry, dict):
            return None
        entry_id = str(entry.get("entry_id", "") or "").strip()
        if not entry_id:
            return None
        metadata_update = {
            "error": str(error_text or "").strip() or "共享文件缺失或不可访问",
            "failed_at": _now_text(),
        }
        updated = self.store.update_source_cache_entry_status(
            entry_id,
            status="failed",
            metadata_update=metadata_update,
        )
        if updated is not None:
            self._mark_external_full_snapshot_dirty()
            if str(updated.get("bucket_kind", "") or "").strip().lower() == "latest":
                with self._lock:
                    self._ensure_light_family_cache_unlocked(
                        source_family=str(updated.get("source_family", "") or "").strip(),
                        bucket_key=str(updated.get("bucket_key", "") or "").strip(),
                        buildings=[str(updated.get("building", "") or "").strip()],
                    )
                    self._set_light_building_status_unlocked(
                        source_family=str(updated.get("source_family", "") or "").strip(),
                        building=str(updated.get("building", "") or "").strip(),
                        bucket_key=str(updated.get("bucket_key", "") or "").strip(),
                        payload={
                            "status": "failed",
                            "ready": False,
                            "downloaded_at": str(updated.get("downloaded_at", "") or "").strip(),
                            "last_error": metadata_update["error"],
                            "relative_path": str(updated.get("relative_path", "") or "").strip(),
                            "resolved_file_path": "",
                            "started_at": "",
                            "blocked": False,
                            "blocked_reason": "",
                            "next_probe_at": "",
                        },
                    )
            self._log_source_cache_event(
                f"{log_reason}: family={updated.get('source_family', '')}, "
                f"building={updated.get('building', '')}, bucket={updated.get('bucket_kind', '')}/"
                f"{updated.get('bucket_key', '')}, relative_path={updated.get('relative_path', '')}, "
                f"error={metadata_update['error']}"
            )
        return updated

    def _delete_entry_for_missing_file(
        self,
        entry: Dict[str, Any] | None,
        *,
        log_reason: str,
    ) -> bool:
        if self.store is None or not isinstance(entry, dict):
            return False
        entry_id = str(entry.get("entry_id", "") or "").strip()
        if not entry_id:
            return False
        deleted = self.store.delete_source_cache_entry(entry_id)
        if deleted:
            self._mark_external_full_snapshot_dirty()
            self._log_source_cache_event(
                f"{log_reason}: family={entry.get('source_family', '')}, "
                f"building={entry.get('building', '')}, bucket={entry.get('bucket_kind', '')}/"
                f"{entry.get('bucket_key', '')}, relative_path={entry.get('relative_path', '')}, "
                "error=共享文件缺失或不可访问"
            )
        return deleted

    def _entry_reference_datetime(self, entry: Dict[str, Any] | None) -> datetime | None:
        if not isinstance(entry, dict):
            return None
        metadata = entry.get("metadata", {}) if isinstance(entry.get("metadata", {}), dict) else {}
        for candidate in (
            metadata.get("refreshing_at"),
            entry.get("updated_at"),
            entry.get("downloaded_at"),
            entry.get("created_at"),
        ):
            parsed = _parse_datetime_text(candidate)
            if parsed is not None:
                return parsed
        return None

    def _repair_stale_refreshing_entries(
        self,
        *,
        force: bool = False,
        limit: int = 2000,
    ) -> int:
        if self.store is None:
            return 0
        now_monotonic = time.monotonic()
        with self._lock:
            if (
                not force
                and self._last_refreshing_repair_monotonic > 0
                and (now_monotonic - self._last_refreshing_repair_monotonic) < self.REPAIR_SWEEP_COOLDOWN_SEC
            ):
                return 0
            self._last_refreshing_repair_monotonic = now_monotonic
        rows = self.store.list_source_cache_entries(status="refreshing", limit=max(1, int(limit or 1)))
        if not rows:
            return 0
        now_dt = _now_dt()
        repaired = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            reference_dt = self._entry_reference_datetime(row)
            if reference_dt is None:
                continue
            age_sec = max(0.0, (now_dt - reference_dt).total_seconds())
            if age_sec < self.REFRESHING_ENTRY_TIMEOUT_SEC:
                continue
            if self._repair_entry_to_failed(
                row,
                error_text="共享文件刷新超时",
                log_reason="refreshing 条目超时已转失败",
            ):
                repaired += 1
        return repaired

    def _repair_inaccessible_ready_entries(
        self,
        *,
        source_family: str,
        limit: int = 2000,
    ) -> int:
        if self.store is None or self.shared_root is None:
            return 0
        repaired = 0
        for family_name in self._source_family_candidates(source_family):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                status="ready",
                limit=max(1, int(limit or 1)),
            )
            for row in rows:
                if not isinstance(row, dict):
                    continue
                relative_path = str(row.get("relative_path", "") or "").strip()
                file_path = self._resolve_relative_path_under_shared_root(relative_path)
                if file_path is not None and _is_accessible_cached_file(file_path):
                    continue
                if self._repair_entry_to_failed(
                    row,
                    error_text="共享文件缺失或不可访问",
                    log_reason="ready 条目已修复为 failed",
                ):
                    repaired += 1
        return repaired

    def _default_light_building_status(self, *, building: str, bucket_key: str) -> Dict[str, Any]:
        return {
            "building": str(building or "").strip(),
            "bucket_key": str(bucket_key or "").strip(),
            "status": "waiting",
            "ready": False,
            "downloaded_at": "",
            "last_error": "",
            "relative_path": "",
            "resolved_file_path": "",
            "started_at": "",
            "blocked": False,
            "blocked_reason": "",
            "next_probe_at": "",
        }

    def _reset_light_building_state_unlocked(self) -> None:
        buildings = self.get_enabled_buildings()
        current_bucket = self._current_hour_bucket or self.current_hour_bucket()
        alarm_bucket = (
            str(self._family_status.get(FAMILY_ALARM_EVENT, {}).get("current_bucket", "") or "").strip()
            or self.current_alarm_bucket()
        )
        self._family_status.setdefault(FAMILY_HANDOVER_LOG, {})["current_bucket"] = current_bucket
        self._family_status.setdefault(FAMILY_HANDOVER_CAPACITY_REPORT, {})["current_bucket"] = current_bucket
        self._family_status.setdefault(FAMILY_MONTHLY_REPORT, {})["current_bucket"] = current_bucket
        self._family_status.setdefault(FAMILY_ALARM_EVENT, {})["current_bucket"] = alarm_bucket
        for family_name, bucket_key in (
            (FAMILY_HANDOVER_LOG, current_bucket),
            (FAMILY_HANDOVER_CAPACITY_REPORT, current_bucket),
            (FAMILY_MONTHLY_REPORT, current_bucket),
            (FAMILY_ALARM_EVENT, alarm_bucket),
        ):
            family_cache = self._light_building_status.setdefault(family_name, {})
            for building in buildings:
                family_cache.setdefault(
                    building,
                    self._default_light_building_status(building=building, bucket_key=bucket_key),
                )

    def _ensure_light_family_cache_unlocked(
        self,
        *,
        source_family: str,
        bucket_key: str,
        buildings: List[str] | None = None,
    ) -> None:
        normalized_family = self._normalize_source_family(source_family)
        family_cache = self._light_building_status.setdefault(normalized_family, {})
        for building in (buildings or self.get_enabled_buildings()):
            building_name = str(building or "").strip()
            if not building_name:
                continue
            current = family_cache.get(building_name)
            current_bucket = str(current.get("bucket_key", "") or "").strip() if isinstance(current, dict) else ""
            if current_bucket != str(bucket_key or "").strip():
                family_cache[building_name] = self._default_light_building_status(
                    building=building_name,
                    bucket_key=bucket_key,
                )
        self._family_status.setdefault(normalized_family, {})["current_bucket"] = str(bucket_key or "").strip()

    def _set_light_building_status_unlocked(
        self,
        *,
        source_family: str,
        building: str,
        bucket_key: str,
        payload: Dict[str, Any],
    ) -> None:
        normalized_family = self._normalize_source_family(source_family)
        building_name = str(building or "").strip()
        effective_bucket = str(bucket_key or "").strip()
        base = self._default_light_building_status(building=building_name, bucket_key=effective_bucket)
        base.update(payload if isinstance(payload, dict) else {})
        base["building"] = building_name
        base["bucket_key"] = effective_bucket
        status_text = str(base.get("status", "") or "").strip().lower() or "waiting"
        base["status"] = status_text
        base["ready"] = bool(base.get("ready", False) or status_text == "ready")
        self._light_building_status.setdefault(normalized_family, {})[building_name] = base
        self._family_status.setdefault(normalized_family, {})["current_bucket"] = effective_bucket

    def _recompute_family_status_from_light_unlocked(
        self,
        *,
        source_family: str,
        bucket_key: str = "",
    ) -> Dict[str, Any]:
        normalized_family = self._normalize_source_family(source_family)
        effective_bucket = str(
            bucket_key
            or self._family_status.get(normalized_family, {}).get("current_bucket", "")
            or ""
        ).strip()
        family_cache = self._light_building_status.setdefault(normalized_family, {})
        rows: List[Dict[str, Any]] = []
        for building in self.get_enabled_buildings():
            row = family_cache.get(building)
            if not isinstance(row, dict):
                row = self._default_light_building_status(building=building, bucket_key=effective_bucket)
                family_cache[building] = row
            rows.append(row)
        ready_count = sum(
            1
            for item in rows
            if str(item.get("status", "") or "").strip().lower() == "ready" and bool(item.get("ready", False))
        )
        failed_buildings = [
            str(item.get("building", "") or "").strip()
            for item in rows
            if str(item.get("status", "") or "").strip().lower() == "failed"
        ]
        blocked_buildings = [
            str(item.get("building", "") or "").strip()
            for item in rows
            if bool(item.get("blocked", False))
        ]
        last_success_candidates = [
            str(item.get("downloaded_at", "") or "").strip()
            for item in rows
            if str(item.get("downloaded_at", "") or "").strip()
        ]
        family_status = self._family_status.setdefault(normalized_family, {})
        family_status["ready_count"] = ready_count
        family_status["failed_buildings"] = failed_buildings
        family_status["blocked_buildings"] = blocked_buildings
        family_status["last_success_at"] = (
            max(last_success_candidates)
            if last_success_candidates
            else str(family_status.get("last_success_at", "") or "").strip()
        )
        family_status["current_bucket"] = effective_bucket
        return family_status

    def _set_light_building_status_from_entry_unlocked(
        self,
        *,
        source_family: str,
        building: str,
        bucket_key: str,
        entry: Dict[str, Any],
        file_path: Path | None,
    ) -> None:
        metadata = entry.get("metadata", {}) if isinstance(entry.get("metadata", {}), dict) else {}
        status_text = str(entry.get("status", "") or "").strip().lower() or "waiting"
        resolved_path = str(file_path) if file_path is not None and status_text == "ready" else ""
        downloaded_at = ""
        if status_text == "consumed":
            downloaded_at = str(metadata.get("consumed_at", "") or entry.get("downloaded_at", "") or "").strip()
        else:
            downloaded_at = str(entry.get("downloaded_at", "") or "").strip()
        self._set_light_building_status_unlocked(
            source_family=source_family,
            building=building,
            bucket_key=bucket_key,
            payload={
                "status": status_text,
                "ready": status_text == "ready",
                "downloaded_at": downloaded_at,
                "last_error": str(metadata.get("error", "") or "").strip() if status_text == "failed" else "",
                "relative_path": str(entry.get("relative_path", "") or "").strip(),
                "resolved_file_path": resolved_path,
                "started_at": "",
                "blocked": False,
                "blocked_reason": "",
                "next_probe_at": "",
            },
        )

    def _build_internal_light_family_snapshot(
        self,
        *,
        source_family: str,
        current_bucket: str,
        cached_rows: Dict[str, Dict[str, Any]],
        active_downloads: Dict[tuple[str, str, str, str], str],
    ) -> Dict[str, Any]:
        normalized_family = self._normalize_source_family(source_family)
        buildings = self.get_enabled_buildings()
        building_rows: List[Dict[str, Any]] = []
        for building in buildings:
            cached = copy.deepcopy(cached_rows.get(building) or self._default_light_building_status(building=building, bucket_key=current_bucket))
            if str(cached.get("bucket_key", "") or "").strip() != str(current_bucket or "").strip():
                cached = self._default_light_building_status(building=building, bucket_key=current_bucket)
            active_key = (normalized_family, building, "latest", current_bucket)
            active_started_at = str(active_downloads.get(active_key, "") or "").strip()
            if active_started_at:
                cached = {
                    **self._default_light_building_status(building=building, bucket_key=current_bucket),
                    "status": "downloading",
                    "started_at": active_started_at,
                }
            pause_info = (
                self.download_browser_pool.get_building_pause_info(building)
                if self.download_browser_pool is not None and hasattr(self.download_browser_pool, "get_building_pause_info")
                else {}
            )
            if bool(pause_info.get("suspended", False)) and str(cached.get("status", "") or "").strip().lower() not in {"ready", "consumed", "failed"}:
                blocked_reason = str(
                    pause_info.get("suspend_reason", "") or pause_info.get("pending_issue_summary", "") or ""
                ).strip()
                cached = {
                    **self._default_light_building_status(building=building, bucket_key=current_bucket),
                    "status": "waiting",
                    "last_error": blocked_reason,
                    "blocked": True,
                    "blocked_reason": blocked_reason,
                    "next_probe_at": str(pause_info.get("next_probe_at", "") or "").strip(),
                }
            building_rows.append(cached)
        ready_count = sum(1 for item in building_rows if bool(item.get("ready")))
        failed_buildings = [str(item.get("building", "") or "").strip() for item in building_rows if str(item.get("status", "") or "").strip().lower() == "failed"]
        blocked_buildings = [str(item.get("building", "") or "").strip() for item in building_rows if bool(item.get("blocked", False))]
        last_success_candidates = [str(item.get("downloaded_at", "") or "").strip() for item in building_rows if str(item.get("downloaded_at", "") or "").strip()]
        raw_family = {
            "ready_count": ready_count,
            "failed_buildings": failed_buildings,
            "blocked_buildings": blocked_buildings,
            "last_success_at": max(last_success_candidates) if last_success_candidates else "",
            "current_bucket": current_bucket,
            "buildings": building_rows,
            "latest_selection": {},
        }
        bucket_scope_text = "本次定时" if self._normalize_source_family(source_family) == FAMILY_ALARM_EVENT else "本小时"
        return present_source_cache_family(
            raw_family,
            title=FAMILY_LABELS.get(self._normalize_source_family(source_family), ""),
            fallback_bucket=current_bucket,
            bucket_scope_text=bucket_scope_text,
        )

    def _build_external_full_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            current_bucket = self._current_hour_bucket or self.current_hour_bucket()
            families = copy.deepcopy(self._family_status)
            current_hour_refresh = copy.deepcopy(self._current_hour_refresh)
            last_run_at = self._last_run_at
            last_success_at = self._last_success_at
            last_error = self._last_error
            alarm_external_upload = copy.deepcopy(self._alarm_external_upload_state)
        include_latest_selection = True
        alarm_bucket = str(families.get(FAMILY_ALARM_EVENT, {}).get("current_bucket", "") or "").strip() or self.current_alarm_bucket()
        snapshot_error = ""
        try:
            handover_family = self._build_family_health_snapshot(
                source_family=FAMILY_HANDOVER_LOG,
                current_bucket=current_bucket,
                include_latest_selection=include_latest_selection,
            )
        except Exception as exc:  # noqa: BLE001
            handover_family = {"current_bucket": current_bucket, "buildings": []}
            snapshot_error = str(exc)
        try:
            handover_capacity_family = self._build_family_health_snapshot(
                source_family=FAMILY_HANDOVER_CAPACITY_REPORT,
                current_bucket=current_bucket,
                include_latest_selection=include_latest_selection,
            )
        except Exception as exc:  # noqa: BLE001
            handover_capacity_family = {"current_bucket": current_bucket, "buildings": []}
            snapshot_error = snapshot_error or str(exc)
        try:
            monthly_family = self._build_family_health_snapshot(
                source_family=FAMILY_MONTHLY_REPORT,
                current_bucket=current_bucket,
                include_latest_selection=include_latest_selection,
            )
        except Exception as exc:  # noqa: BLE001
            monthly_family = {"current_bucket": current_bucket, "buildings": []}
            snapshot_error = snapshot_error or str(exc)
        try:
            alarm_family = self._build_alarm_external_health_snapshot()
        except Exception as exc:  # noqa: BLE001
            alarm_family = {"current_bucket": alarm_bucket, "buildings": []}
            snapshot_error = snapshot_error or str(exc)
        families[FAMILY_HANDOVER_LOG] = {**families.get(FAMILY_HANDOVER_LOG, {}), **handover_family}
        families[FAMILY_HANDOVER_CAPACITY_REPORT] = {
            **families.get(FAMILY_HANDOVER_CAPACITY_REPORT, {}),
            **handover_capacity_family,
        }
        families[FAMILY_MONTHLY_REPORT] = {**families.get(FAMILY_MONTHLY_REPORT, {}), **monthly_family}
        families[FAMILY_ALARM_EVENT] = {
            **families.get(FAMILY_ALARM_EVENT, {}),
            **alarm_family,
            "external_upload": alarm_external_upload,
        }
        snapshot = {
            "enabled": bool(self.enabled and self.role_mode in {"internal", "external"}),
            "scheduler_running": bool(self.role_mode == "internal" and self.is_running()),
            "current_hour_bucket": current_bucket,
            "last_run_at": last_run_at,
            "last_success_at": last_success_at,
            "last_error": last_error or snapshot_error,
            "cache_root": str(self.shared_root) if self.shared_root else "",
            "current_hour_refresh": current_hour_refresh,
            FAMILY_HANDOVER_LOG: families.get(FAMILY_HANDOVER_LOG, {}),
            FAMILY_HANDOVER_CAPACITY_REPORT: families.get(FAMILY_HANDOVER_CAPACITY_REPORT, {}),
            FAMILY_MONTHLY_REPORT: families.get(FAMILY_MONTHLY_REPORT, {}),
            FAMILY_ALARM_EVENT: families.get(FAMILY_ALARM_EVENT, {}),
        }
        snapshot["display_overview"] = present_external_source_cache_overview(snapshot)
        return snapshot

    def _build_alarm_external_selection(self, *, building: str = "") -> Dict[str, Any]:
        if self.store is None or self.shared_root is None:
            return {
                "selection_policy": "today_latest_else_yesterday_fallback",
                "selection_reference_date": _now_dt().date().isoformat(),
                "used_previous_day_fallback": [],
                "missing_today_buildings": [],
                "missing_both_days_buildings": [],
                "ready_count": 0,
                "failed_buildings": [],
                "blocked_buildings": [],
                "last_success_at": "",
                "current_bucket": _now_dt().date().isoformat(),
                "buildings": [],
                "latest_selection": {},
                "selected_entries": [],
                "selected_by_building": {},
            }
        self._repair_inaccessible_ready_entries(source_family=FAMILY_ALARM_EVENT)
        return build_alarm_external_selection(
            store=self.store,
            shared_root=self.shared_root,
            reference_date=_now_dt().date(),
            enabled_buildings=self.get_enabled_buildings(),
            building=building,
        )

    def _build_alarm_external_health_snapshot(self) -> Dict[str, Any]:
        selection = self._build_alarm_external_selection()
        raw_family = {
            "ready_count": int(selection.get("ready_count", 0) or 0),
            "failed_buildings": list(selection.get("failed_buildings", []) or []),
            "blocked_buildings": list(selection.get("blocked_buildings", []) or []),
            "last_success_at": str(selection.get("last_success_at", "") or "").strip(),
            "current_bucket": str(selection.get("selection_reference_date", "") or "").strip(),
            "buildings": list(selection.get("buildings", []) or []),
            "latest_selection": {},
            "selection_policy": str(selection.get("selection_policy", "") or "").strip(),
            "selection_reference_date": str(selection.get("selection_reference_date", "") or "").strip(),
            "used_previous_day_fallback": list(selection.get("used_previous_day_fallback", []) or []),
            "missing_today_buildings": list(selection.get("missing_today_buildings", []) or []),
            "missing_both_days_buildings": list(selection.get("missing_both_days_buildings", []) or []),
        }
        presented = present_alarm_event_family(
            raw_family,
            key=FAMILY_ALARM_EVENT,
            title=FAMILY_LABELS.get(FAMILY_ALARM_EVENT, ""),
        )
        presented["display_overview"] = dict(presented)
        return presented

    def _get_external_full_snapshot_cached(self) -> Dict[str, Any]:
        with self._lock:
            should_rebuild = self._external_full_snapshot_dirty or not self._external_full_snapshot_cache
            if not should_rebuild:
                age_sec = time.monotonic() - float(self._external_full_snapshot_built_monotonic or 0.0)
                should_rebuild = age_sec >= self.EXTERNAL_FULL_SNAPSHOT_MAX_AGE_SEC
            cached_snapshot = copy.deepcopy(self._external_full_snapshot_cache) if not should_rebuild else {}
        if not should_rebuild:
            return cached_snapshot
        snapshot = self._build_external_full_snapshot()
        with self._lock:
            self._external_full_snapshot_cache = copy.deepcopy(snapshot)
            self._external_full_snapshot_dirty = False
            self._external_full_snapshot_built_monotonic = time.monotonic()
        return snapshot

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
        self._handover_capacity_cache_root = self.shared_root / FAMILY_LABELS[FAMILY_HANDOVER_CAPACITY_REPORT] if self.shared_root else None
        self._monthly_cache_root = self.shared_root / FAMILY_LABELS[FAMILY_MONTHLY_REPORT] if self.shared_root else None
        self._alarm_cache_root = self.shared_root / FAMILY_LABELS[FAMILY_ALARM_EVENT] if self.shared_root else None
        self._tmp_root = self.shared_root / "tmp" / "source_cache" if self.shared_root else None

    def update_runtime_config(self, runtime_config: Dict[str, Any]) -> None:
        self.runtime_config = copy.deepcopy(runtime_config if isinstance(runtime_config, dict) else {})
        self._refresh_config()
        self._mark_external_full_snapshot_dirty()
        with self._lock:
            self._reset_light_building_state_unlocked()

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
        if text == FAMILY_HANDOVER_CAPACITY_REPORT:
            return FAMILY_HANDOVER_CAPACITY_REPORT
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
            manual_alarm_refresh_thread = self._manual_alarm_refresh_thread
            if not thread:
                self._stop_event.set()
                self._current_hour_refresh_thread = None
                self._manual_alarm_refresh_thread = None
                if current_hour_refresh_thread:
                    current_hour_refresh_thread.join(timeout=5)
                if manual_alarm_refresh_thread:
                    manual_alarm_refresh_thread.join(timeout=5)
                return {"stopped": False, "running": False, "reason": "not_running"}
            self._stop_event.set()
            self._thread = None
            self._current_hour_refresh_thread = None
            self._manual_alarm_refresh_thread = None
        thread.join(timeout=5)
        if current_hour_refresh_thread:
            current_hour_refresh_thread.join(timeout=5)
        if manual_alarm_refresh_thread:
            manual_alarm_refresh_thread.join(timeout=5)
        return {"stopped": True, "running": False, "reason": "stopped"}

    def is_running(self) -> bool:
        thread = self._thread
        return bool(thread and thread.is_alive())

    def get_health_snapshot(self, *, mode: str = "external_full") -> Dict[str, Any]:
        normalized_mode = str(mode or "external_full").strip().lower() or "external_full"
        if normalized_mode != "internal_light":
            self._repair_stale_refreshing_entries()
        with self._lock:
            current_bucket = self._current_hour_bucket or self.current_hour_bucket()
            families = copy.deepcopy(self._family_status)
            current_hour_refresh = copy.deepcopy(self._current_hour_refresh)
            last_run_at = self._last_run_at
            last_success_at = self._last_success_at
            last_error = self._last_error
            light_building_status = copy.deepcopy(self._light_building_status)
            active_downloads = dict(self._active_latest_downloads)
            alarm_external_upload = copy.deepcopy(self._alarm_external_upload_state)
            manual_alarm_refresh = copy.deepcopy(self._manual_alarm_refresh)
        if normalized_mode == "internal_light":
            alarm_bucket = str(families.get(FAMILY_ALARM_EVENT, {}).get("current_bucket", "") or "").strip() or self.current_alarm_bucket()
            handover_family = self._build_internal_light_family_snapshot(
                source_family=FAMILY_HANDOVER_LOG,
                current_bucket=current_bucket,
                cached_rows=light_building_status.get(FAMILY_HANDOVER_LOG, {}),
                active_downloads=active_downloads,
            )
            handover_capacity_family = self._build_internal_light_family_snapshot(
                source_family=FAMILY_HANDOVER_CAPACITY_REPORT,
                current_bucket=current_bucket,
                cached_rows=light_building_status.get(FAMILY_HANDOVER_CAPACITY_REPORT, {}),
                active_downloads=active_downloads,
            )
            monthly_family = self._build_internal_light_family_snapshot(
                source_family=FAMILY_MONTHLY_REPORT,
                current_bucket=current_bucket,
                cached_rows=light_building_status.get(FAMILY_MONTHLY_REPORT, {}),
                active_downloads=active_downloads,
            )
            alarm_family = self._build_internal_light_family_snapshot(
                source_family=FAMILY_ALARM_EVENT,
                current_bucket=alarm_bucket,
                cached_rows=light_building_status.get(FAMILY_ALARM_EVENT, {}),
                active_downloads=active_downloads,
            )
            families[FAMILY_HANDOVER_LOG] = {**families.get(FAMILY_HANDOVER_LOG, {}), **handover_family}
            families[FAMILY_HANDOVER_CAPACITY_REPORT] = {
                **families.get(FAMILY_HANDOVER_CAPACITY_REPORT, {}),
                **handover_capacity_family,
            }
            families[FAMILY_MONTHLY_REPORT] = {**families.get(FAMILY_MONTHLY_REPORT, {}), **monthly_family}
            families[FAMILY_ALARM_EVENT] = {
                **families.get(FAMILY_ALARM_EVENT, {}),
                **alarm_family,
                "external_upload": alarm_external_upload,
                "manual_refresh": manual_alarm_refresh,
            }
            snapshot = {
                "enabled": bool(self.enabled and self.role_mode in {"internal", "external"}),
                "scheduler_running": bool(self.role_mode == "internal" and self.is_running()),
                "current_hour_bucket": current_bucket,
                "last_run_at": last_run_at,
                "last_success_at": last_success_at,
                "last_error": last_error,
                "cache_root": str(self.shared_root) if self.shared_root else "",
                "current_hour_refresh": current_hour_refresh,
                FAMILY_HANDOVER_LOG: families.get(FAMILY_HANDOVER_LOG, {}),
                FAMILY_HANDOVER_CAPACITY_REPORT: families.get(FAMILY_HANDOVER_CAPACITY_REPORT, {}),
                FAMILY_MONTHLY_REPORT: families.get(FAMILY_MONTHLY_REPORT, {}),
                FAMILY_ALARM_EVENT: families.get(FAMILY_ALARM_EVENT, {}),
            }
            snapshot["overview"] = present_internal_source_cache_overview(snapshot)
            snapshot["current_hour_refresh_overview"] = present_current_hour_refresh_overview(current_hour_refresh)
            return snapshot
        return self._get_external_full_snapshot_cached()

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
        pause_info = (
            self.download_browser_pool.get_building_pause_info(building)
            if self.download_browser_pool is not None and hasattr(self.download_browser_pool, "get_building_pause_info")
            else {}
        )
        active_key = (self._normalize_source_family(source_family), building, "latest", bucket_key)
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
                "blocked": False,
                "blocked_reason": "",
                "next_probe_at": "",
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
                    "blocked": False,
                    "blocked_reason": "",
                    "next_probe_at": "",
                }
        consumed_entry = self._get_source_cache_entry(
            source_family=source_family,
            building=building,
            bucket_kind="latest",
            bucket_key=bucket_key,
            status="consumed",
        )
        if consumed_entry:
            metadata = consumed_entry.get("metadata", {}) if isinstance(consumed_entry.get("metadata", {}), dict) else {}
            return {
                "building": building,
                "bucket_key": bucket_key,
                "status": "consumed",
                "ready": False,
                "downloaded_at": str(metadata.get("consumed_at", "") or consumed_entry.get("downloaded_at", "") or "").strip(),
                "last_error": "",
                "relative_path": str(consumed_entry.get("relative_path", "") or "").strip(),
                "resolved_file_path": "",
                "blocked": False,
                "blocked_reason": "",
                "next_probe_at": "",
            }
        if bool(pause_info.get("suspended", False)):
            blocked_reason = str(pause_info.get("suspend_reason", "") or pause_info.get("pending_issue_summary", "") or "").strip()
            return {
                "building": building,
                "bucket_key": bucket_key,
                "status": "waiting",
                "ready": False,
                "downloaded_at": "",
                "last_error": blocked_reason,
                "relative_path": "",
                "resolved_file_path": "",
                "blocked": True,
                "blocked_reason": blocked_reason,
                "next_probe_at": str(pause_info.get("next_probe_at", "") or "").strip(),
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
                "blocked": False,
                "blocked_reason": "",
                "next_probe_at": "",
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
            "blocked": False,
            "blocked_reason": "",
            "next_probe_at": "",
        }

    def _build_family_health_snapshot(
        self,
        *,
        source_family: str,
        current_bucket: str,
        include_latest_selection: bool,
    ) -> Dict[str, Any]:
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
        blocked_buildings = [
            str(item.get("building", "") or "").strip()
            for item in building_rows
            if bool(item.get("blocked", False))
        ]
        last_success_candidates = [
            str(item.get("downloaded_at", "") or "").strip()
            for item in building_rows
            if str(item.get("downloaded_at", "") or "").strip()
        ]
        last_success_at = max(last_success_candidates) if last_success_candidates else ""
        latest_selection = (
            self.get_latest_ready_selection(
                source_family=source_family,
                buildings=buildings,
            )
            if include_latest_selection and self._normalize_source_family(source_family) != FAMILY_ALARM_EVENT
            else {}
        )
        raw_family = {
            "ready_count": ready_count,
            "failed_buildings": failed_buildings,
            "blocked_buildings": blocked_buildings,
            "last_success_at": last_success_at,
            "current_bucket": current_bucket,
            "buildings": building_rows,
            "latest_selection": latest_selection,
        }
        presented_live = present_source_cache_family(
            raw_family,
            title=FAMILY_LABELS.get(self._normalize_source_family(source_family), ""),
            fallback_bucket=current_bucket,
            bucket_scope_text="当前桶",
        )
        if latest_selection:
            presented_live["latest_selection"] = present_latest_selection_overview(
                latest_selection,
                key=self._normalize_source_family(source_family),
                title=FAMILY_LABELS.get(self._normalize_source_family(source_family), ""),
            )
            presented_live["display_overview"] = present_external_source_cache_family(
                key=self._normalize_source_family(source_family),
                title=FAMILY_LABELS.get(self._normalize_source_family(source_family), ""),
                live_payload=presented_live,
                latest_payload=presented_live["latest_selection"],
            )
        else:
            presented_live["display_overview"] = present_source_cache_family(
                raw_family,
                title=FAMILY_LABELS.get(self._normalize_source_family(source_family), ""),
                fallback_bucket=current_bucket,
                bucket_scope_text="当前桶",
            )
        return presented_live

    def _ensure_dirs(self) -> None:
        if self.shared_root is None or self._tmp_root is None:
            raise RuntimeError("共享缓存根目录未配置")
        if self._handover_cache_root is not None:
            self._handover_cache_root.mkdir(parents=True, exist_ok=True)
        if self._handover_capacity_cache_root is not None:
            self._handover_capacity_cache_root.mkdir(parents=True, exist_ok=True)
        if self._monthly_cache_root is not None:
            self._monthly_cache_root.mkdir(parents=True, exist_ok=True)
        if self._alarm_cache_root is not None:
            self._alarm_cache_root.mkdir(parents=True, exist_ok=True)
        self._tmp_root.mkdir(parents=True, exist_ok=True)

    def ensure_required_directories(self) -> Dict[str, str]:
        self._ensure_dirs()
        return self.get_required_directory_paths()

    def get_required_directory_paths(self) -> Dict[str, str]:
        return {
            FAMILY_HANDOVER_LOG: str(self._handover_cache_root) if self._handover_cache_root is not None else "",
            FAMILY_HANDOVER_CAPACITY_REPORT: str(self._handover_capacity_cache_root) if self._handover_capacity_cache_root is not None else "",
            FAMILY_MONTHLY_REPORT: str(self._monthly_cache_root) if self._monthly_cache_root is not None else "",
            FAMILY_ALARM_EVENT: str(self._alarm_cache_root) if self._alarm_cache_root is not None else "",
            "tmp_source_cache": str(self._tmp_root) if self._tmp_root is not None else "",
        }

    def _hash_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _validate_handover_source_file(self, path: Path) -> None:
        validate_excel_workbook_file(path)
        cfg = load_handover_config(self.runtime_config)
        parsing_cfg = cfg.get("parsing", {}) if isinstance(cfg.get("parsing", {}), dict) else {}
        normalize_cfg = cfg.get("normalize", {}) if isinstance(cfg.get("normalize", {}), dict) else {}
        rows = load_rows(
            str(path),
            parsing_cfg=copy.deepcopy(parsing_cfg),
            normalize_cfg=copy.deepcopy(normalize_cfg),
        )
        if not rows:
            raise ValueError(f"交接班源文件无有效数据行: {path}")
        has_e_value = any(
            row.e_raw is not None and (not isinstance(row.e_raw, str) or str(row.e_raw).strip() != "")
            for row in rows
        )
        if not has_e_value:
            raise ValueError(f"交接班源文件E列无有效数据: {path}")

    def _validate_cached_source_file(self, *, source_family: str, path: Path) -> None:
        normalized_family = self._normalize_source_family(source_family)
        suffix = str(path.suffix or "").strip().lower()
        if suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("告警 JSON 文件顶层必须是对象")
            if not isinstance(payload.get("rows"), list):
                raise ValueError("告警 JSON 文件缺少 rows 数组")
            return
        if normalized_family == FAMILY_HANDOVER_LOG:
            self._validate_handover_source_file(path)
            return
        validate_excel_workbook_file(path)

    def _cache_file(self, *, source_family: str, source_path: Path, target_path: Path) -> Dict[str, Any]:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_copy_file(
            source_path,
            target_path,
            validator=lambda candidate: self._validate_cached_source_file(
                source_family=source_family,
                path=candidate,
            ),
            temp_suffix=".downloading",
            allow_overwrite_fallback=False,
        )
        self._log_source_cache_event(f"共享文件校验成功: target={target_path}")
        return {
            "file_hash": self._hash_file(target_path),
            "size_bytes": int(target_path.stat().st_size),
            "target_path": target_path,
            "relative_path": target_path.relative_to(self.shared_root).as_posix() if self.shared_root else target_path.name,
        }

    def _cache_file_compat(self, *, source_family: str, source_path: Path, target_path: Path) -> Dict[str, Any]:
        try:
            return self._cache_file(
                source_family=source_family,
                source_path=source_path,
                target_path=target_path,
            )
        except TypeError as exc:
            if "source_family" not in str(exc):
                raise
            return self._cache_file(
                source_path=source_path,
                target_path=target_path,
            )

    def _family_root(self, source_family: str) -> Path:
        normalized_family = self._normalize_source_family(source_family)
        if normalized_family == FAMILY_HANDOVER_LOG and self._handover_cache_root is not None:
            return self._handover_cache_root
        if normalized_family == FAMILY_HANDOVER_CAPACITY_REPORT and self._handover_capacity_cache_root is not None:
            return self._handover_capacity_cache_root
        if normalized_family == FAMILY_MONTHLY_REPORT and self._monthly_cache_root is not None:
            return self._monthly_cache_root
        if normalized_family == FAMILY_ALARM_EVENT and self._alarm_cache_root is not None:
            return self._alarm_cache_root
        raise RuntimeError("共享缓存根目录未配置")

    def _month_segment(self, value: str) -> str:
        digits = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
        if len(digits) >= 6:
            return digits[:6]
        now = datetime.now()
        return now.strftime("%Y%m")

    def _bucket_path_segment(self, bucket_key: str, *, bucket_kind: str = "latest") -> str:
        text = str(bucket_key or "").strip()
        if bucket_kind == "manual":
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H%M%S", "%Y%m%d%H%M%S"):
                try:
                    parsed = datetime.strptime(text, fmt)
                    return parsed.strftime("%Y%m%d--%H%M%S--manual")
                except ValueError:
                    continue
        bucket_dt = _parse_hour_bucket(text)
        if bucket_dt is not None:
            return bucket_dt.strftime("%Y%m%d--%H")
        digits = "".join(ch for ch in text if ch.isdigit())
        if bucket_kind == "manual" and len(digits) >= 14:
            try:
                parsed = datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
                return parsed.strftime("%Y%m%d--%H%M%S--manual")
            except ValueError:
                pass
        if bucket_kind == "latest" and len(digits) >= 10:
            return f"{digits[:8]}--{digits[8:10]}"
        safe = "".join(ch for ch in text if ch.isalnum() or ch in {"-", "_"})
        if safe:
            return safe
        return "manual" if bucket_kind == "manual" else "current"

    def _file_suffix(self, source_path: Path) -> str:
        suffix = str(source_path.suffix or "").strip()
        return suffix if suffix else ".xlsx"

    def _get_monthly_download_module(self):
        with self._monthly_download_module_lock:
            if self._monthly_download_module is None:
                self._monthly_download_module = load_download_module()
            return self._monthly_download_module

    @staticmethod
    def _normalize_single_fill_result(result: Any) -> Dict[str, Any] | None:
        if isinstance(result, dict):
            return result
        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict):
                    return item
        return None

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

    def _source_target_path(
        self,
        *,
        source_family: str,
        building: str,
        bucket_kind: str,
        bucket_key: str,
        duty_date: str,
        duty_shift: str,
        source_path: Path,
    ) -> Path:
        info = build_source_artifact_path(
            source_family=self._normalize_source_family(source_family),
            building=building,
            suffix=self._file_suffix(source_path),
            bucket_kind=bucket_kind,
            bucket_key=bucket_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
        )
        base_root = self.shared_root
        if base_root is None:
            base_root = self._family_root(source_family).parent
        return base_root / info.relative_path

    def current_alarm_bucket(self, when: datetime | None = None) -> str:
        return scheduled_bucket_for_time(when)

    def _auto_alarm_bucket(self, when: datetime | None = None) -> str:
        return self.current_alarm_bucket(when)

    def _alarm_temp_root(self, *, bucket_key: str, building: str, bucket_kind: str = "latest") -> Path:
        if self._tmp_root is None:
            raise RuntimeError("共享缓存临时目录未配置")
        normalized_kind = str(bucket_kind or "").strip().lower() or "latest"
        bucket_segment = self._bucket_path_segment(bucket_key, bucket_kind=normalized_kind)
        return self._tmp_root / "alarm_event_latest" / bucket_segment / building

    def _is_alarm_scheduled_bucket_key(self, bucket_key: str) -> bool:
        bucket_dt = _parse_hour_bucket(bucket_key)
        return bool(bucket_dt)

    def _alarm_manual_bucket(self, when: datetime | None = None) -> str:
        return (when or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

    def _store_entry(self, *, source_family: str, building: str, bucket_kind: str, bucket_key: str, duty_date: str, duty_shift: str, source_path: Path, status: str, metadata: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if self.store is None:
            raise RuntimeError("共享缓存存储未初始化")
        normalized_family = self._normalize_source_family(source_family)
        target_path = self._source_target_path(
            source_family=normalized_family,
            building=building,
            bucket_kind=bucket_kind,
            bucket_key=bucket_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
            source_path=source_path,
        )
        entry_metadata = dict(metadata or {})
        entry_metadata.setdefault("naming_version", 2)
        target_relative_path = (
            target_path.relative_to(self.shared_root).as_posix()
            if self.shared_root is not None
            else target_path.name
        )
        self._log_source_cache_event(
            f"开始刷新共享文件: family={normalized_family}, building={building}, "
            f"bucket={bucket_kind}/{bucket_key}, target={target_relative_path}"
        )
        refreshing_metadata = dict(entry_metadata)
        refreshing_metadata["refreshing_at"] = _now_text()
        self.store.upsert_source_cache_entry(
            source_family=normalized_family,
            building=building,
            bucket_kind=bucket_kind,
            bucket_key=bucket_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
            downloaded_at=_now_text(),
            relative_path=target_relative_path,
            status="refreshing",
            file_hash="",
            size_bytes=0,
            metadata=refreshing_metadata,
        )
        self._log_source_cache_event(
            f"条目已进入 refreshing: family={normalized_family}, building={building}, "
            f"bucket={bucket_kind}/{bucket_key}, target={target_relative_path}"
        )
        cached = self._cache_file_compat(
            source_family=normalized_family,
            source_path=source_path,
            target_path=target_path,
        )
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
            metadata=entry_metadata,
        )
        if normalized_family == FAMILY_ALARM_EVENT:
            try:
                if source_path.exists() and source_path.resolve() != target_path.resolve():
                    source_path.unlink(missing_ok=True)
            except Exception:  # noqa: BLE001
                pass
        self._mark_external_full_snapshot_dirty()
        self._log_source_cache_event(
            f"条目已切回 {str(status or '').strip().lower() or 'ready'}: "
            f"family={normalized_family}, building={building}, bucket={bucket_kind}/{bucket_key}, "
            f"target={target_relative_path}"
        )
        if bucket_kind == "latest":
            with self._lock:
                self._ensure_light_family_cache_unlocked(
                    source_family=normalized_family,
                    bucket_key=bucket_key,
                    buildings=[building],
                )
                self._set_light_building_status_unlocked(
                    source_family=normalized_family,
                    building=building,
                    bucket_key=bucket_key,
                    payload={
                        "status": str(status or "").strip().lower() or "ready",
                        "ready": str(status or "").strip().lower() == "ready",
                        "downloaded_at": downloaded_at,
                        "last_error": "",
                        "relative_path": str(cached["relative_path"]),
                        "resolved_file_path": str(target_path) if str(status or "").strip().lower() == "ready" else "",
                        "started_at": "",
                        "blocked": False,
                        "blocked_reason": "",
                        "next_probe_at": "",
                    },
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
            "metadata": entry_metadata,
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
        self._mark_external_full_snapshot_dirty()
        if bucket_kind == "latest":
            with self._lock:
                self._ensure_light_family_cache_unlocked(
                    source_family=source_family,
                    bucket_key=bucket_key,
                    buildings=[building],
                )
                self._set_light_building_status_unlocked(
                    source_family=source_family,
                    building=building,
                    bucket_key=bucket_key,
                    payload={
                        "status": "failed",
                        "ready": False,
                        "downloaded_at": _now_text(),
                        "last_error": str(payload.get("error", "") or "").strip(),
                        "relative_path": self._failed_marker_relative_path(
                            source_family=source_family,
                            bucket_kind=bucket_kind,
                            bucket_key=bucket_key,
                            building=building,
                        ),
                        "resolved_file_path": "",
                        "started_at": "",
                        "blocked": False,
                        "blocked_reason": "",
                        "next_probe_at": "",
                    },
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
        normalized_family = self._normalize_source_family(entry.get("source_family", ""))
        if not _is_accessible_cached_file(file_path):
            status_text = str(entry.get("status", "") or "").strip().lower()
            if status_text == "ready":
                self._repair_entry_to_failed(
                    entry,
                    error_text="共享文件缺失或不可访问",
                    log_reason="ready 条目已修复为 failed",
                )
            return None
        if normalized_family == FAMILY_HANDOVER_LOG:
            try:
                self._validate_handover_source_file(file_path)
            except Exception as exc:  # noqa: BLE001
                status_text = str(entry.get("status", "") or "").strip().lower()
                if status_text == "ready":
                    self._repair_entry_to_failed(
                        entry,
                        error_text=f"交接班共享源文件为空或无有效数据: {exc}",
                        log_reason="ready 交接班条目已修复为 failed",
                    )
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

    def _get_latest_ready_handover_entry_for_date_shift(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
    ) -> Dict[str, Any] | None:
        if self.store is None:
            return None
        candidates: List[Dict[str, Any]] = []
        for family_name in self._source_family_candidates(FAMILY_HANDOVER_LOG):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                building=building,
                duty_date=duty_date,
                duty_shift=duty_shift,
                status="ready",
                limit=20,
            )
            candidates.extend(row for row in rows if isinstance(row, dict))
        for entry in candidates:
            if self._resolve_entry_file_path(entry) is not None:
                return entry
        for family_name in self._source_family_candidates(FAMILY_HANDOVER_LOG):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                building=building,
                bucket_kind="latest",
                status="ready",
                limit=50,
            )
            candidates.extend(row for row in rows if isinstance(row, dict))
        for entry in candidates:
            file_path = self._resolve_entry_file_path(entry)
            if file_path is None:
                continue
            effective_context = self._resolve_handover_entry_duty_context(entry)
            if (
                effective_context["duty_date"] == duty_date
                and effective_context["duty_shift"] == duty_shift
            ):
                normalized_entry = dict(entry)
                normalized_entry["duty_date"] = effective_context["duty_date"]
                normalized_entry["duty_shift"] = effective_context["duty_shift"]
                return normalized_entry
        return None

    def _get_latest_ready_entry_for_date_shift(
        self,
        *,
        source_family: str,
        building: str,
        duty_date: str,
        duty_shift: str,
    ) -> Dict[str, Any] | None:
        normalized_family = self._normalize_source_family(source_family)
        if normalized_family == FAMILY_HANDOVER_LOG:
            return self._get_latest_ready_handover_entry_for_date_shift(
                building=building,
                duty_date=duty_date,
                duty_shift=duty_shift,
            )
        if self.store is None:
            return None
        candidates: List[Dict[str, Any]] = []
        for family_name in self._source_family_candidates(normalized_family):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                building=building,
                duty_date=duty_date,
                duty_shift=duty_shift,
                status="ready",
                limit=20,
            )
            candidates.extend(row for row in rows if isinstance(row, dict))
        for entry in candidates:
            file_path = self._resolve_entry_file_path(entry)
            if file_path is None:
                continue
            normalized_entry = dict(entry)
            normalized_entry["file_path"] = str(file_path)
            return normalized_entry
        for family_name in self._source_family_candidates(normalized_family):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                building=building,
                bucket_kind="latest",
                status="ready",
                limit=50,
            )
            candidates.extend(row for row in rows if isinstance(row, dict))
        for entry in candidates:
            file_path = self._resolve_entry_file_path(entry)
            if file_path is None:
                continue
            if (
                _normalize_nullable_text(entry.get("duty_date")) == duty_date
                and _normalize_nullable_text(entry.get("duty_shift")).lower() == duty_shift
            ):
                normalized_entry = dict(entry)
                normalized_entry["file_path"] = str(file_path)
                return normalized_entry
        return None

    def _get_cached_ready_handover_entry_for_date(
        self,
        *,
        building: str,
        duty_date: str,
    ) -> Dict[str, Any] | None:
        if self.store is None:
            return None
        candidates: List[Dict[str, Any]] = []
        for family_name in self._source_family_candidates(FAMILY_HANDOVER_LOG):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                building=building,
                duty_date=duty_date,
                status="ready",
                limit=50,
            )
            candidates.extend(row for row in rows if isinstance(row, dict))
        for entry in candidates:
            file_path = self._resolve_entry_file_path(entry)
            if file_path is None:
                continue
            effective_context = self._resolve_handover_entry_duty_context(entry)
            if effective_context["duty_date"] != duty_date:
                continue
            normalized_entry = dict(entry)
            normalized_entry["file_path"] = str(file_path)
            normalized_entry["duty_date"] = effective_context["duty_date"]
            normalized_entry["duty_shift"] = effective_context["duty_shift"]
            return normalized_entry
        for family_name in self._source_family_candidates(FAMILY_HANDOVER_LOG):
            rows = self.store.list_source_cache_entries(
                source_family=family_name,
                building=building,
                bucket_kind="latest",
                status="ready",
                limit=50,
            )
            candidates.extend(row for row in rows if isinstance(row, dict))
        for entry in candidates:
            file_path = self._resolve_entry_file_path(entry)
            if file_path is None:
                continue
            effective_context = self._resolve_handover_entry_duty_context(entry)
            if effective_context["duty_date"] != duty_date:
                continue
            normalized_entry = dict(entry)
            normalized_entry["file_path"] = str(file_path)
            normalized_entry["duty_date"] = effective_context["duty_date"]
            normalized_entry["duty_shift"] = effective_context["duty_shift"]
            return normalized_entry
        return None

    def _scan_handover_history_candidates(
        self,
        *,
        building: str,
        duty_date: str,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "selected": None,
            "candidates": [],
        }
        if self._handover_cache_root is None:
            return result
        duty_digits = "".join(ch for ch in str(duty_date or "").strip() if ch.isdigit())[:8]
        building_text = str(building or "").strip()
        if len(duty_digits) != 8 or not building_text:
            return result
        month_root = self._handover_cache_root / duty_digits[:6]
        if not month_root.exists():
            return result

        bucket_dirs = [item for item in month_root.glob(f"{duty_digits}--*") if item.is_dir()]
        bucket_dirs.sort(
            key=lambda item: (
                _parse_hour_bucket(item.name) or datetime.min,
                item.name,
            ),
            reverse=True,
        )
        for bucket_dir in bucket_dirs:
            candidates = [item for item in bucket_dir.glob(f"*--{FAMILY_LABELS[FAMILY_HANDOVER_LOG]}--{building_text}.*") if item.is_file()]
            candidates.sort(
                key=lambda item: (
                    float(item.stat().st_mtime) if item.exists() else 0.0,
                    item.name,
                ),
                reverse=True,
            )
            for candidate in candidates:
                try:
                    stat_result = candidate.stat()
                    downloaded_at = datetime.fromtimestamp(stat_result.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                except OSError:
                    downloaded_at = ""
                record = {
                    "bucket_key": bucket_dir.name,
                    "file_path": str(candidate),
                    "relative_path": (
                        candidate.relative_to(self.shared_root).as_posix()
                        if self.shared_root is not None
                        else str(candidate)
                    ),
                    "downloaded_at": downloaded_at,
                    "valid": False,
                    "error": "",
                }
                try:
                    self._validate_handover_source_file(candidate)
                except Exception as exc:  # noqa: BLE001
                    record["error"] = str(exc)
                    result["candidates"].append(record)
                    continue
                record["valid"] = True
                result["candidates"].append(record)
                result["selected"] = record
                return result
        return result

    def _build_handover_history_selection_entry(
        self,
        *,
        building: str,
        duty_date: str,
        selected: Dict[str, Any],
        resolution_source: str,
        scanned_candidates: List[Dict[str, Any]],
        redownload_attempted: bool,
    ) -> Dict[str, Any]:
        return {
            "entry_id": "",
            "source_family": FAMILY_HANDOVER_LOG,
            "building": building,
            "bucket_kind": "history_scan",
            "bucket_key": str(selected.get("bucket_key", "") or "").strip(),
            "duty_date": duty_date,
            "duty_shift": "",
            "downloaded_at": str(selected.get("downloaded_at", "") or "").strip(),
            "relative_path": str(selected.get("relative_path", "") or "").strip(),
            "status": "ready",
            "file_path": str(selected.get("file_path", "") or "").strip(),
            "resolution_source": str(resolution_source or "").strip(),
            "scanned_candidates": copy.deepcopy(scanned_candidates),
            "redownload_attempted": bool(redownload_attempted),
        }

    def _get_latest_ready_handover_entry_for_date(
        self,
        *,
        building: str,
        duty_date: str,
        emit_log: Callable[[str], None] | None = None,
    ) -> Dict[str, Any] | None:
        log_fn = emit_log if callable(emit_log) else None
        today_text = _now_dt().date().isoformat()
        is_today = duty_date == today_text
        redownload_attempted = False
        current_bucket_key = self.current_hour_bucket()
        current_bucket_segment = self._bucket_path_segment(current_bucket_key, bucket_kind="latest") if is_today else ""

        scan_info = self._scan_handover_history_candidates(building=building, duty_date=duty_date)
        selected = scan_info.get("selected") if isinstance(scan_info, dict) else None
        selected_bucket_key = str(selected.get("bucket_key", "") or "").strip() if isinstance(selected, dict) else ""
        latest_candidate_ready = bool(selected_bucket_key and selected_bucket_key == current_bucket_segment)

        can_redownload_today = bool(
            is_today
            and self.role_mode == "internal"
            and self.download_browser_pool is not None
        )
        if is_today and current_bucket_segment and not latest_candidate_ready:
            if can_redownload_today:
                redownload_attempted = True
                if log_fn:
                    log_fn(
                        f"[12项历史共享文件补采] 当天最新小时文件未就绪，触发补下载: "
                        f"duty_date={duty_date}, building={building}, bucket={current_bucket_key}"
                    )
                try:
                    self.fill_handover_latest(
                        building=building,
                        bucket_key=current_bucket_key,
                        emit_log=emit_log or self._emit,
                    )
                except Exception as exc:  # noqa: BLE001
                    if log_fn:
                        log_fn(
                            f"[12项历史共享文件补采] 当天最新小时补下载失败，继续回退扫描: "
                            f"duty_date={duty_date}, building={building}, error={exc}"
                        )
                scan_info = self._scan_handover_history_candidates(building=building, duty_date=duty_date)
                selected = scan_info.get("selected") if isinstance(scan_info, dict) else None
            elif log_fn:
                log_fn(
                    f"[12项历史共享文件补采] 当天任务无法触发最新小时补下载，直接按目录回退扫描: "
                    f"duty_date={duty_date}, building={building}"
                )

        selected = scan_info.get("selected") if isinstance(scan_info, dict) else None
        if isinstance(selected, dict):
            selected_bucket_key = str(selected.get("bucket_key", "") or "").strip()
            resolution_source = "history_scan"
            if redownload_attempted and current_bucket_segment and selected_bucket_key == current_bucket_segment:
                resolution_source = "latest_redownload"
            return self._build_handover_history_selection_entry(
                building=building,
                duty_date=duty_date,
                selected=selected,
                resolution_source=resolution_source,
                scanned_candidates=scan_info.get("candidates", []) if isinstance(scan_info.get("candidates", []), list) else [],
                redownload_attempted=redownload_attempted,
            )

        cached_entry = self._get_cached_ready_handover_entry_for_date(
            building=building,
            duty_date=duty_date,
        )
        if not isinstance(cached_entry, dict):
            return None
        normalized_entry = dict(cached_entry)
        normalized_entry["resolution_source"] = (
            "cache_date"
            if str(normalized_entry.get("bucket_kind", "") or "").strip().lower() == "date"
            else "cache_latest"
        )
        normalized_entry["scanned_candidates"] = copy.deepcopy(
            scan_info.get("candidates", []) if isinstance(scan_info, dict) else []
        )
        normalized_entry["redownload_attempted"] = bool(redownload_attempted)
        return normalized_entry

    def _find_latest_valid_handover_history_file(
        self,
        *,
        building: str,
        duty_date: str,
    ) -> Path | None:
        scan_info = self._scan_handover_history_candidates(building=building, duty_date=duty_date)
        selected = scan_info.get("selected") if isinstance(scan_info, dict) else None
        file_path = str(selected.get("file_path", "") or "").strip() if isinstance(selected, dict) else ""
        return Path(file_path) if file_path else None

    def _handover_shift_boundaries(self) -> tuple[dt_time, dt_time]:
        try:
            cfg = load_handover_config(self.runtime_config)
        except Exception:  # noqa: BLE001
            cfg = {}
        download_cfg = cfg.get("download", {}) if isinstance(cfg.get("download", {}), dict) else {}
        shift_windows = download_cfg.get("shift_windows", {}) if isinstance(download_cfg.get("shift_windows", {}), dict) else {}
        day_cfg = shift_windows.get("day", {}) if isinstance(shift_windows.get("day", {}), dict) else {}
        night_cfg = shift_windows.get("night", {}) if isinstance(shift_windows.get("night", {}), dict) else {}

        def _parse_hms(raw: Any, default_text: str) -> dt_time:
            text = str(raw or default_text).strip() or default_text
            try:
                parsed = datetime.strptime(text, "%H:%M:%S")
            except ValueError:
                parsed = datetime.strptime(default_text, "%H:%M:%S")
            return parsed.time()

        day_start = _parse_hms(day_cfg.get("start"), "08:00:00")
        night_start = _parse_hms(night_cfg.get("start"), "17:00:00")
        return day_start, night_start

    def _infer_handover_duty_context_from_bucket_key(self, bucket_key: str) -> Dict[str, str]:
        bucket_dt = _parse_hour_bucket(bucket_key)
        if bucket_dt is None:
            return {"duty_date": "", "duty_shift": ""}
        day_start, night_start = self._handover_shift_boundaries()
        bucket_time = bucket_dt.time()
        if day_start <= bucket_time < night_start:
            return {
                "duty_date": bucket_dt.strftime("%Y-%m-%d"),
                "duty_shift": "day",
            }
        if bucket_time >= night_start:
            duty_day = bucket_dt
        else:
            duty_day = bucket_dt - timedelta(days=1)
        return {
            "duty_date": duty_day.strftime("%Y-%m-%d"),
            "duty_shift": "night",
        }

    def _resolve_handover_entry_duty_context(self, entry: Dict[str, Any]) -> Dict[str, str]:
        duty_date = _normalize_nullable_text(entry.get("duty_date"))
        duty_shift = _normalize_nullable_text(entry.get("duty_shift")).lower()
        if duty_date and duty_shift in {"day", "night"}:
            return {"duty_date": duty_date, "duty_shift": duty_shift}
        inferred = self._infer_handover_duty_context_from_bucket_key(str(entry.get("bucket_key", "") or "").strip())
        if duty_date and not inferred.get("duty_date"):
            inferred["duty_date"] = duty_date
        if duty_date and duty_shift == "all":
            inferred["duty_date"] = duty_date
            if not inferred.get("duty_shift"):
                inferred["duty_shift"] = "all"
        return inferred

    def get_latest_ready_selection(
        self,
        *,
        source_family: str,
        buildings: List[str] | None = None,
        max_version_gap: int = 3,
        max_selection_age_hours: float = 3.0,
    ) -> Dict[str, Any]:
        self._repair_stale_refreshing_entries()
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
        blocked_buildings: List[Dict[str, Any]] = []
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
                pause_info = (
                    self.download_browser_pool.get_building_pause_info(building)
                    if self.download_browser_pool is not None and hasattr(self.download_browser_pool, "get_building_pause_info")
                    else {}
                )
                blocked_reason = (
                    str(pause_info.get("suspend_reason", "") or pause_info.get("pending_issue_summary", "") or "").strip()
                    if bool(pause_info.get("suspended", False))
                    else ""
                )
                missing_buildings.append(building)
                if blocked_reason:
                    blocked_buildings.append(
                        {
                            "building": building,
                            "reason": blocked_reason,
                            "failure_kind": str(pause_info.get("failure_kind", "") or "").strip(),
                            "next_probe_at": str(pause_info.get("next_probe_at", "") or "").strip(),
                        }
                    )
                building_rows.append(
                    {
                        "building": building,
                        "bucket_key": latest_bucket_key,
                        "status": "waiting",
                        "using_fallback": False,
                        "version_gap": None,
                        "downloaded_at": "",
                        "last_error": blocked_reason,
                        "relative_path": "",
                        "resolved_file_path": "",
                        "blocked": bool(blocked_reason),
                        "blocked_reason": blocked_reason,
                        "next_probe_at": str(pause_info.get("next_probe_at", "") or "").strip(),
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
                "blocked": False,
                "blocked_reason": "",
                "next_probe_at": "",
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
            "blocked_buildings": blocked_buildings,
            "buildings": building_rows,
            "can_proceed": bool(target_buildings)
            and not missing_buildings
            and not stale_buildings
            and not is_best_bucket_too_old
            and len(selected_entries) == len(target_buildings),
        }

    @staticmethod
    def _should_retry_failed_latest_entry(entry: Dict[str, Any] | None) -> bool:
        if not isinstance(entry, dict):
            return False
        metadata = entry.get("metadata", {}) if isinstance(entry.get("metadata", {}), dict) else {}
        error_text = str(metadata.get("error", "") or "").strip()
        if not error_text:
            return False
        return any(
            marker in error_text
            for marker in (
                "共享缓存根目录未配置",
                "共享缓存临时目录未配置",
                "共享缓存存储未初始化",
            )
        )

    def get_latest_ready_entries(self, *, source_family: str, buildings: List[str] | None = None, bucket_key: str | None = None) -> List[Dict[str, Any]]:
        if self.store is None:
            return []
        self._repair_stale_refreshing_entries()
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
        self._repair_stale_refreshing_entries()
        requested = [str(item or "").strip() for item in (buildings or self.get_enabled_buildings()) if str(item or "").strip()]
        output: List[Dict[str, Any]] = []
        for building in requested:
            entry = self._get_latest_ready_handover_entry_for_date_shift(
                building=building,
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

    def get_handover_capacity_by_date_entries(self, *, duty_date: str, duty_shift: str, buildings: List[str] | None = None) -> List[Dict[str, Any]]:
        if self.store is None:
            return []
        self._repair_stale_refreshing_entries()
        requested = [str(item or "").strip() for item in (buildings or self.get_enabled_buildings()) if str(item or "").strip()]
        output: List[Dict[str, Any]] = []
        for building in requested:
            entry = self._get_latest_ready_entry_for_date_shift(
                source_family=FAMILY_HANDOVER_CAPACITY_REPORT,
                building=building,
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
        self._repair_stale_refreshing_entries()
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

    def repair_day_metric_ready_entries(
        self,
        *,
        selected_dates: List[str],
        buildings: List[str],
        emit_log: Callable[[str], None],
    ) -> Dict[str, int]:
        if self.store is None:
            return {"scanned": 0, "downgraded": 0, "kept": 0}
        self._repair_stale_refreshing_entries()
        dates = [str(item or "").strip() for item in (selected_dates or []) if str(item or "").strip()]
        target_buildings = [str(item or "").strip() for item in (buildings or []) if str(item or "").strip()]
        seen_entry_ids: set[str] = set()
        scanned = 0
        downgraded = 0
        kept = 0

        for duty_date in dates:
            for building in target_buildings:
                for family_name in self._source_family_candidates(FAMILY_HANDOVER_LOG):
                    rows = self.store.list_source_cache_entries(
                        source_family=family_name,
                        building=building,
                        duty_date=duty_date,
                        status="ready",
                        limit=200,
                    )
                    for entry in rows:
                        if not isinstance(entry, dict):
                            continue
                        entry_id = str(entry.get("entry_id", "") or "").strip()
                        if entry_id and entry_id in seen_entry_ids:
                            continue
                        if entry_id:
                            seen_entry_ids.add(entry_id)
                        scanned += 1
                        file_path = self._resolve_entry_file_path(entry)
                        if file_path is None:
                            downgraded += 1
                            continue
                        kept += 1

        emit_log(
            f"[12项历史共享文件补采] 运行前扫描完成: dates={','.join(dates) or '-'}, "
            f"buildings={','.join(target_buildings) or '-'}, scanned={scanned}, "
            f"downgraded={downgraded}, kept={kept}"
        )
        return {
            "scanned": scanned,
            "downgraded": downgraded,
            "kept": kept,
        }

    @staticmethod
    def _parse_date_text(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d", "%Y%m%d%H%M%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    def _should_background_sweep_entry(self, entry: Dict[str, Any] | None, *, lookback_days: int) -> bool:
        if not isinstance(entry, dict):
            return False
        bucket_kind = str(entry.get("bucket_kind", "") or "").strip().lower()
        if bucket_kind == "latest":
            return True
        cutoff = datetime.now() - timedelta(days=max(1, int(lookback_days or 1)))
        for candidate in (
            entry.get("duty_date"),
            entry.get("downloaded_at"),
        ):
            parsed = self._parse_date_text(candidate)
            if parsed is not None and parsed >= cutoff:
                return True
        return False

    def sweep_invalid_ready_entries(
        self,
        *,
        lookback_days: int = 7,
        limit: int = 5000,
        emit_log: Callable[[str], None],
    ) -> Dict[str, int]:
        if self.store is None:
            return {"scanned": 0, "downgraded": 0, "kept": 0, "skipped": 0}
        self._repair_stale_refreshing_entries()
        rows = self.store.list_source_cache_entries(status="ready", limit=max(1, int(limit or 5000)))
        scanned = 0
        downgraded = 0
        kept = 0
        skipped = 0
        for entry in rows:
            if not isinstance(entry, dict):
                continue
            if not self._should_background_sweep_entry(entry, lookback_days=lookback_days):
                skipped += 1
                continue
            scanned += 1
            file_path = self._resolve_relative_path_under_shared_root(str(entry.get("relative_path", "") or "").strip())
            if file_path is None or not _is_accessible_cached_file(file_path):
                entry_id = str(entry.get("entry_id", "") or "").strip()
                if entry_id and self._repair_entry_to_failed(
                    entry,
                    error_text="共享文件缺失或不可访问",
                    log_reason="后台扫描已修复缺失共享文件为 failed",
                ):
                    downgraded += 1
                else:
                    downgraded += 1
                continue
            try:
                self._validate_cached_source_file(
                    source_family=str(entry.get("source_family", "")).strip(),
                    path=file_path,
                )
            except Exception as exc:  # noqa: BLE001
                entry_id = str(entry.get("entry_id", "") or "").strip()
                if entry_id:
                    self.store.update_source_cache_entry_status(
                        entry_id,
                        status="failed",
                        metadata_update={
                            "error": str(exc).strip() or "共享文件校验失败",
                            "failed_at": _now_text(),
                            "validated_by": "background_sweep",
                        },
                    )
                    self._mark_external_full_snapshot_dirty()
                downgraded += 1
                self._log_source_cache_event(
                    "后台扫描已修复 ready 条目为 failed: "
                    f"family={entry.get('source_family', '')}, building={entry.get('building', '')}, "
                    f"bucket={entry.get('bucket_kind', '')}/{entry.get('bucket_key', '')}, "
                    f"relative_path={entry.get('relative_path', '')}, error={exc}"
                )
                continue
            kept += 1

        emit_log(
            "[共享缓存] 后台扫描完成: "
            f"观察窗口={max(1, int(lookback_days or 1))}天, 已扫描={scanned}, "
            f"已降级={downgraded}, 保持有效={kept}, 已跳过={skipped}"
        )
        return {
            "scanned": scanned,
            "downgraded": downgraded,
            "kept": kept,
            "skipped": skipped,
        }

    def get_monthly_by_date_entries(self, *, selected_dates: List[str], buildings: List[str] | None = None) -> List[Dict[str, Any]]:
        if self.store is None:
            return []
        self._repair_stale_refreshing_entries()
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
                    entry = self._get_ready_entry(
                        source_family=FAMILY_MONTHLY_REPORT,
                        building=building,
                        bucket_kind="latest",
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

    def _handover_capacity_temp_root(
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
            return self._tmp_root / "handover_capacity_latest" / bucket_key / building
        shift_segment = str(duty_shift or "all").strip().lower() or "all"
        return self._tmp_root / "handover_capacity_by_date" / str(duty_date or "manual").strip() / shift_segment

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
        normalized_result_context = self._resolve_handover_entry_duty_context(
            {
                "bucket_key": bucket_key,
                "duty_date": result.get("duty_date"),
                "duty_shift": result.get("duty_shift"),
            }
        )
        return self._store_entry(
            source_family=FAMILY_HANDOVER_LOG,
            building=building,
            bucket_kind="latest",
            bucket_key=bucket_key,
            duty_date=normalized_result_context["duty_date"],
            duty_shift=normalized_result_context["duty_shift"],
            source_path=source_path,
            status="ready",
            metadata={
                "family": FAMILY_HANDOVER_LOG,
                "building": building,
                "duty_date": normalized_result_context["duty_date"],
                "duty_shift": normalized_result_context["duty_shift"],
            },
        )

    def fill_handover_capacity_latest(self, *, building: str, bucket_key: str, emit_log: Callable[[str], None]) -> Dict[str, Any]:
        cfg = load_handover_config(self.runtime_config)
        temp_root = self._handover_capacity_temp_root(
            bucket_kind="latest",
            bucket_key=bucket_key,
            building=building,
        )
        service = HandoverDownloadService(
            cfg,
            download_browser_pool=self.download_browser_pool,
            business_root_override=temp_root,
        )
        result = service.run_capacity_only(buildings=[building], switch_network=False, reuse_cached=False, emit_log=emit_log)
        success_files = result.get("success_files", []) if isinstance(result.get("success_files", []), list) else []
        if not success_files:
            raise RuntimeError(f"本小时容量缓存下载失败: {building}")
        item = success_files[0]
        source_path = Path(str(item.get("file_path", "") or "").strip())
        if not source_path.exists():
            raise FileNotFoundError(f"下载完成后的容量源文件不存在: {source_path}")
        normalized_result_context = self._resolve_handover_entry_duty_context(
            {
                "bucket_key": bucket_key,
                "duty_date": result.get("duty_date"),
                "duty_shift": result.get("duty_shift"),
            }
        )
        return self._store_entry(
            source_family=FAMILY_HANDOVER_CAPACITY_REPORT,
            building=building,
            bucket_kind="latest",
            bucket_key=bucket_key,
            duty_date=normalized_result_context["duty_date"],
            duty_shift=normalized_result_context["duty_shift"],
            source_path=source_path,
            status="ready",
            metadata={
                "family": FAMILY_HANDOVER_CAPACITY_REPORT,
                "building": building,
                "duty_date": normalized_result_context["duty_date"],
                "duty_shift": normalized_result_context["duty_shift"],
            },
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

    def fill_handover_capacity_history(self, *, buildings: List[str], duty_date: str, duty_shift: str, emit_log: Callable[[str], None]) -> List[Dict[str, Any]]:
        cfg = load_handover_config(self.runtime_config)
        temp_root = self._handover_capacity_temp_root(
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
        result = service.run_capacity_only(
            buildings=buildings,
            duty_date=duty_date,
            duty_shift=duty_shift,
            switch_network=False,
            reuse_cached=False,
            emit_log=emit_log,
        )
        success_files = result.get("success_files", []) if isinstance(result.get("success_files", []), list) else []
        output: List[Dict[str, Any]] = []
        for item in success_files:
            building = str(item.get("building", "") or "").strip()
            source_path = Path(str(item.get("file_path", "") or "").strip())
            if not building or not source_path.exists():
                continue
            output.append(
                self._store_entry(
                    source_family=FAMILY_HANDOVER_CAPACITY_REPORT,
                    building=building,
                    bucket_kind="date",
                    bucket_key=duty_date,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    source_path=source_path,
                    status="ready",
                    metadata={
                        "family": FAMILY_HANDOVER_CAPACITY_REPORT,
                        "building": building,
                        "duty_date": duty_date,
                        "duty_shift": duty_shift,
                    },
                )
            )
        return output

    def fill_day_metric_history(self, *, selected_dates: List[str], building_scope: str, building: str | None, emit_log: Callable[[str], None]) -> List[Dict[str, Any]]:
        target_buildings = (
            [str(building or "").strip()]
            if str(building_scope or "").strip() == "single"
            else [str(item or "").strip() for item in self.get_enabled_buildings() if str(item or "").strip()]
        )
        target_buildings = [item for item in target_buildings if item]
        self.repair_day_metric_ready_entries(
            selected_dates=selected_dates,
            buildings=target_buildings,
            emit_log=emit_log,
        )
        rows: List[Dict[str, str]] = []
        missing_units: List[str] = []
        for duty_date in [str(item or "").strip() for item in (selected_dates or []) if str(item or "").strip()]:
            for building_name in target_buildings:
                entry = self._get_latest_ready_handover_entry_for_date(
                    building=building_name,
                    duty_date=duty_date,
                    emit_log=emit_log,
                )
                source_file = str(entry.get("file_path", "") or "").strip() if isinstance(entry, dict) else ""
                if not source_file:
                    missing_units.append(f"{building_name}({duty_date})")
                    continue
                cache_bucket_kind = str(entry.get("bucket_kind", "") or "").strip()
                cache_bucket_key = str(entry.get("bucket_key", "") or "").strip()
                cache_downloaded_at = str(entry.get("downloaded_at", "") or "").strip()
                cache_entry_id = str(entry.get("entry_id", "") or "").strip()
                resolution_source = str(entry.get("resolution_source", "") or "").strip()
                scanned_candidates = entry.get("scanned_candidates", []) if isinstance(entry, dict) else []
                scanned_count = len(scanned_candidates) if isinstance(scanned_candidates, list) else 0
                redownload_attempted = bool(entry.get("redownload_attempted", False)) if isinstance(entry, dict) else False
                emit_log(
                    f"[12项历史共享文件补采] 复用交接班源文件: duty_date={duty_date}, "
                    f"building={building_name}, source={source_file}, "
                    f"resolution={resolution_source or '-'}, cache={cache_bucket_kind}/{cache_bucket_key}, "
                    f"downloaded_at={cache_downloaded_at}, entry_id={cache_entry_id}, "
                    f"scanned_candidates={scanned_count}, redownload_attempted={str(redownload_attempted).lower()}"
                )
                rows.append(
                    {
                        "duty_date": duty_date,
                        "building": building_name,
                        "source_file": source_file,
                        "resolution_source": resolution_source,
                        "cache_bucket_kind": cache_bucket_kind,
                        "cache_bucket_key": cache_bucket_key,
                        "cache_entry_id": cache_entry_id,
                        "redownload_attempted": redownload_attempted,
                        "scanned_candidates": scanned_count,
                    }
                )
        if missing_units:
            raise RuntimeError(f"缺少可复用的交接班源文件: {', '.join(missing_units)}")
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
                    metadata={
                        "family": FAMILY_HANDOVER_LOG,
                        "building": building_name,
                        "duty_date": duty_date,
                        "duty_shift": "all",
                        "resolution_source": str(item.get("resolution_source", "") or "").strip(),
                        "source_bucket_kind": str(item.get("cache_bucket_kind", "") or "").strip(),
                        "source_bucket_key": str(item.get("cache_bucket_key", "") or "").strip(),
                        "source_entry_id": str(item.get("cache_entry_id", "") or "").strip(),
                        "selected_source_file": str(item.get("source_file", "") or "").strip(),
                        "redownload_attempted": bool(item.get("redownload_attempted", False)),
                        "scanned_candidates": int(item.get("scanned_candidates", 0) or 0),
                    },
                )
            )
        return output

    def fill_monthly_latest(self, *, building: str, bucket_key: str, emit_log: Callable[[str], None]) -> List[Dict[str, Any]]:
        if self._tmp_root is None:
            raise RuntimeError("共享缓存临时目录未配置")
        save_dir = self._tmp_root / "monthly_latest" / bucket_key / building
        save_dir.mkdir(parents=True, exist_ok=True)
        cfg = self._prepare_monthly_runtime_config(buildings=[building], save_dir=save_dir)
        module = self._get_monthly_download_module()
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
        module = self._get_monthly_download_module()
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

    def fill_alarm_event_latest(self, *, building: str, bucket_key: str, emit_log: Callable[[str], None]) -> Dict[str, Any]:
        if self.download_browser_pool is None or not hasattr(self.download_browser_pool, "submit_building_alarm_job"):
            raise RuntimeError("内网下载浏览器池未启动")
        temp_root = self._alarm_temp_root(bucket_key=bucket_key, building=building, bucket_kind="latest")
        temp_root.mkdir(parents=True, exist_ok=True)
        json_path = temp_root / f"{building}.json"

        async def _runner(api_context, base_url):  # noqa: ANN001
            return await stream_alarm_event_json_document(
                api_context,
                base_url=base_url,
                output_path=json_path,
                source_family=FAMILY_ALARM_EVENT,
                building=building,
                bucket_kind="latest",
                bucket_key=bucket_key,
                emit_log=emit_log,
                log_prefix=f"[共享缓存][告警API][{building}] ",
            )

        future = self.download_browser_pool.submit_building_alarm_job(building, _runner)
        result = future.result(timeout=self.history_fill_timeout_sec)
        payload = result if isinstance(result, dict) else {}
        rows = payload.get("rows", []) if isinstance(payload.get("rows", []), list) else []
        if not json_path.exists():
            document = build_alarm_event_json_document(
                source_family=FAMILY_ALARM_EVENT,
                building=building,
                bucket_kind="latest",
                bucket_key=bucket_key,
                payload=payload,
            )
            write_alarm_event_json(json_path, document)
        return self._store_entry(
            source_family=FAMILY_ALARM_EVENT,
            building=building,
            bucket_kind="latest",
            bucket_key=bucket_key,
            duty_date="",
            duty_shift="",
            source_path=json_path,
            status="ready",
            metadata={
                "family": FAMILY_ALARM_EVENT,
                "building": building,
                "row_count": int(payload.get("row_count", len(rows)) or 0),
                "query_start": str(payload.get("query_start", "") or "").strip(),
                "query_end": str(payload.get("query_end", "") or "").strip(),
                "count_summary": payload.get("count_summary", {}) if isinstance(payload.get("count_summary", {}), dict) else {},
            },
        )

    def fill_alarm_event_manual(self, *, building: str, bucket_key: str, emit_log: Callable[[str], None]) -> Dict[str, Any]:
        if self.download_browser_pool is None or not hasattr(self.download_browser_pool, "submit_building_alarm_job"):
            raise RuntimeError("内网下载浏览器池未启动")
        temp_root = self._alarm_temp_root(bucket_key=bucket_key, building=building, bucket_kind="manual")
        temp_root.mkdir(parents=True, exist_ok=True)
        json_path = temp_root / f"{building}.json"

        async def _runner(api_context, base_url):  # noqa: ANN001
            return await stream_alarm_event_json_document(
                api_context,
                base_url=base_url,
                output_path=json_path,
                source_family=FAMILY_ALARM_EVENT,
                building=building,
                bucket_kind="manual",
                bucket_key=bucket_key,
                emit_log=emit_log,
                log_prefix=f"[共享缓存][告警API][{building}] ",
            )

        future = self.download_browser_pool.submit_building_alarm_job(building, _runner)
        result = future.result(timeout=self.history_fill_timeout_sec)
        payload = result if isinstance(result, dict) else {}
        rows = payload.get("rows", []) if isinstance(payload.get("rows", []), list) else []
        if not json_path.exists():
            document = build_alarm_event_json_document(
                source_family=FAMILY_ALARM_EVENT,
                building=building,
                bucket_kind="manual",
                bucket_key=bucket_key,
                payload=payload,
            )
            write_alarm_event_json(json_path, document)
        return self._store_entry(
            source_family=FAMILY_ALARM_EVENT,
            building=building,
            bucket_kind="manual",
            bucket_key=bucket_key,
            duty_date="",
            duty_shift="",
            source_path=json_path,
            status="ready",
            metadata={
                "family": FAMILY_ALARM_EVENT,
                "building": building,
                "row_count": int(payload.get("row_count", len(rows)) or 0),
                "query_start": str(payload.get("query_start", "") or "").strip(),
                "query_end": str(payload.get("query_end", "") or "").strip(),
                "count_summary": payload.get("count_summary", {}) if isinstance(payload.get("count_summary", {}), dict) else {},
                "manual": True,
            },
        )

    def _run_manual_alarm_refresh_impl(self) -> Dict[str, Any]:
        self._ensure_dirs()
        bucket_key = self._alarm_manual_bucket()
        completed_units: List[str] = []
        failed_units: List[str] = []
        blocked_units: List[str] = []
        manual_error_text = ""
        building_row_counts: Dict[str, int] = {}
        query_start = ""
        query_end = ""
        with self._lock:
            self._manual_alarm_refresh = {
                "running": True,
                "last_run_at": _now_text(),
                "last_success_at": str(self._manual_alarm_refresh.get("last_success_at", "") or "").strip(),
                "last_error": "",
                "bucket_key": bucket_key,
                "successful_buildings": [],
                "failed_buildings": [],
                "blocked_buildings": [],
                "total_row_count": 0,
                "building_row_counts": {},
                "query_start": "",
                "query_end": "",
            }
        self._emit(f"[共享缓存] 开始手动拉取告警信息文件 bucket={bucket_key}")
        pending_buildings: List[str] = []
        for building in self.get_enabled_buildings():
            pause_info = (
                self.download_browser_pool.get_building_pause_info(building)
                if self.download_browser_pool is not None and hasattr(self.download_browser_pool, "get_building_pause_info")
                else {}
            )
            if bool(pause_info.get("suspended", False)):
                blocked_units.append(building)
                continue
            pending_buildings.append(building)
        future_map: Dict[concurrent.futures.Future[Any], str] = {}
        if pending_buildings:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(pending_buildings), len(_DEFAULT_BUILDINGS))) as executor:
                for building in pending_buildings:
                    future = executor.submit(
                        self.fill_alarm_event_manual,
                        building=building,
                        bucket_key=bucket_key,
                        emit_log=self._emit,
                    )
                    future_map[future] = building
                for future in concurrent.futures.as_completed(future_map):
                    building = future_map[future]
                    try:
                        result = future.result()
                        completed_units.append(building)
                        if isinstance(result, dict):
                            metadata = result.get("metadata", {}) if isinstance(result.get("metadata", {}), dict) else {}
                            row_count = int(metadata.get("row_count", 0) or 0)
                            building_row_counts[building] = row_count
                            if not query_start:
                                query_start = str(metadata.get("query_start", "") or "").strip()
                            if not query_end:
                                query_end = str(metadata.get("query_end", "") or "").strip()
                        with self._lock:
                            self._last_success_at = _now_text()
                    except Exception as exc:  # noqa: BLE001
                        error_text = str(exc)
                        if not manual_error_text:
                            manual_error_text = error_text
                        failed_units.append(building)
                        with self._lock:
                            self._last_error = error_text
                        self._record_failed_entry(
                            source_family=FAMILY_ALARM_EVENT,
                            building=building,
                            bucket_kind="manual",
                            bucket_key=bucket_key,
                            error_text=error_text,
                            metadata={
                                "family": FAMILY_ALARM_EVENT,
                                "building": building,
                                "manual": True,
                            },
                        )
                        self._emit(f"[共享缓存] 手动拉取告警信息失败 building={building}: {exc}")
        total_row_count = sum(building_row_counts.values())
        with self._lock:
            self._manual_alarm_refresh = {
                "running": False,
                "last_run_at": str(self._manual_alarm_refresh.get("last_run_at", "") or "").strip() or _now_text(),
                "last_success_at": _now_text() if completed_units else str(self._manual_alarm_refresh.get("last_success_at", "") or "").strip(),
                "last_error": str(manual_error_text or "").strip(),
                "bucket_key": bucket_key,
                "successful_buildings": list(completed_units),
                "failed_buildings": list(failed_units),
                "blocked_buildings": list(blocked_units),
                "total_row_count": total_row_count,
                "building_row_counts": dict(building_row_counts),
                "query_start": query_start,
                "query_end": query_end,
            }
        if failed_units:
            self._emit(
                f"[共享缓存] 手动拉取告警信息结束：成功楼栋 {', '.join(completed_units) or '-'}；"
                f"失败楼栋 {', '.join(failed_units)}；等待恢复楼栋 {', '.join(blocked_units) or '-'}"
            )
        elif blocked_units:
            self._emit(
                f"[共享缓存] 手动拉取告警信息结束：成功楼栋 {', '.join(completed_units) or '-'}；"
                f"等待恢复楼栋 {', '.join(blocked_units)}"
            )
        else:
            self._emit(f"[共享缓存] 手动拉取告警信息完成：成功楼栋 {', '.join(completed_units) or '-'}")
        return {
            "accepted": True,
            "running": False,
            "reason": "completed",
            "scope": "alarm_manual",
            "bucket_key": bucket_key,
            "running_buildings": pending_buildings,
            "completed_buildings": completed_units,
            "failed_buildings": failed_units,
            "blocked_buildings": blocked_units,
            "total_row_count": total_row_count,
            "building_row_counts": building_row_counts,
            "query_start": query_start,
            "query_end": query_end,
        }

    def _run_manual_alarm_refresh_background(self) -> None:
        try:
            self._run_manual_alarm_refresh_impl()
        finally:
            with self._lock:
                self._manual_alarm_refresh_thread = None

    def start_manual_alarm_refresh(self) -> Dict[str, Any]:
        if not self.enabled or self.role_mode != "internal" or self.store is None:
            return {"accepted": False, "running": False, "reason": "disabled"}
        with self._lock:
            thread = self._manual_alarm_refresh_thread
            if thread and thread.is_alive():
                return {"accepted": False, "running": True, "reason": "already_running"}
            self._manual_alarm_refresh_thread = threading.Thread(
                target=self._run_manual_alarm_refresh_background,
                name="shared-source-cache-alarm-manual",
                daemon=True,
            )
            self._manual_alarm_refresh_thread.start()
        return {
            "accepted": True,
            "running": True,
            "reason": "started",
            "scope": "alarm_manual",
            "bucket_key": self._alarm_manual_bucket(),
        }

    @staticmethod
    def _cleanup_retention_days_for_entry(entry: Dict[str, Any]) -> int:
        bucket_kind = str(entry.get("bucket_kind", "") or "").strip().lower()
        status = str(entry.get("status", "") or "").strip().lower()
        if status == "failed":
            return 7
        if bucket_kind == "manual":
            return 3
        if status == "consumed":
            return 7
        if bucket_kind == "date":
            return 60
        return 7

    @staticmethod
    def _entry_cleanup_reference_time(entry: Dict[str, Any]) -> datetime | None:
        metadata = entry.get("metadata", {}) if isinstance(entry.get("metadata", {}), dict) else {}
        candidates = [
            metadata.get("consumed_at"),
            metadata.get("failed_at"),
            metadata.get("refreshing_at"),
            entry.get("downloaded_at"),
            entry.get("updated_at"),
            entry.get("created_at"),
        ]
        for candidate in candidates:
            text = str(candidate or "").strip()
            if not text:
                continue
            try:
                return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        return None

    def _managed_family_roots(self) -> List[Path]:
        roots: List[Path] = []
        for root in (
            self._handover_cache_root,
            self._handover_capacity_cache_root,
            self._monthly_cache_root,
            self._alarm_cache_root,
        ):
            if root is not None:
                roots.append(root)
        return roots

    def _safe_delete_cached_file(self, *, file_path: Path | None, log_prefix: str) -> bool:
        if file_path is None or not file_path.exists():
            return False
        try:
            file_path.unlink(missing_ok=True)
            return True
        except Exception as exc:  # noqa: BLE001
            self._log_source_cache_event(f"{log_prefix}: path={file_path}, error={exc}")
            return False

    def _collect_referenced_relative_paths(self, *, limit: int = 50000) -> set[str]:
        if self.store is None:
            return set()
        rows = self.store.list_cleanup_candidate_source_cache_entries(limit=max(1, int(limit or 1)))
        return {
            str(row.get("relative_path", "") or "").replace("\\", "/").strip()
            for row in rows
            if isinstance(row, dict) and str(row.get("relative_path", "") or "").strip()
        }

    def _cleanup_orphan_cached_files(self, *, limit: int = 1000) -> Dict[str, int]:
        if self.shared_root is None:
            return {"deleted_files": 0, "scanned_files": 0}
        referenced_paths = self._collect_referenced_relative_paths(limit=max(50000, int(limit or 1) * 10))
        cutoff_dt = _now_dt() - timedelta(days=self.ORPHAN_FILE_RETENTION_DAYS)
        deleted_files = 0
        scanned_files = 0
        for family_root in self._managed_family_roots():
            if not family_root.exists():
                continue
            for file_path in family_root.rglob("*"):
                if scanned_files >= max(1, int(limit or 1)):
                    return {"deleted_files": deleted_files, "scanned_files": scanned_files}
                if not file_path.is_file() or file_path.name.startswith("."):
                    continue
                scanned_files += 1
                try:
                    relative_path = file_path.relative_to(self.shared_root).as_posix()
                    stat = file_path.stat()
                except Exception:  # noqa: BLE001
                    continue
                if relative_path in referenced_paths:
                    continue
                modified_dt = datetime.fromtimestamp(stat.st_mtime)
                if modified_dt > cutoff_dt:
                    continue
                if self._safe_delete_cached_file(
                    file_path=file_path,
                    log_prefix="孤儿共享文件删除失败",
                ):
                    deleted_files += 1
                    self._log_source_cache_event(f"已清理孤儿共享文件: path={file_path}")
        return {"deleted_files": deleted_files, "scanned_files": scanned_files}

    def cleanup_expired_entries(self, *, limit: int = 20000) -> Dict[str, Any]:
        if self.store is None or self.shared_root is None or not hasattr(self.store, "list_cleanup_candidate_source_cache_entries"):
            return {"deleted_entries": 0, "deleted_files": 0}
        now_dt = datetime.now()
        deleted_entries = 0
        deleted_files = 0
        rows = self.store.list_cleanup_candidate_source_cache_entries(limit=limit)
        for row in rows:
            if not isinstance(row, dict):
                continue
            reference_dt = self._entry_cleanup_reference_time(row)
            if reference_dt is None:
                continue
            retention_days = self._cleanup_retention_days_for_entry(row)
            if reference_dt > now_dt - timedelta(days=retention_days):
                continue
            file_path = self._resolve_relative_path_under_shared_root(str(row.get("relative_path", "") or "").strip())
            if self.store.delete_source_cache_entry(str(row.get("entry_id", "") or "").strip()):
                deleted_entries += 1
                self._log_source_cache_event(
                    f"清理索引成功: entry_id={row.get('entry_id', '')}, relative_path={row.get('relative_path', '')}"
                )
                if self._safe_delete_cached_file(
                    file_path=file_path,
                    log_prefix="清理共享文件删除失败",
                ):
                    deleted_files += 1
        if deleted_entries > 0:
            self._mark_external_full_snapshot_dirty()
        orphan_cleanup = self._cleanup_orphan_cached_files(limit=min(max(limit, 1), 2000))
        deleted_files += int(orphan_cleanup.get("deleted_files", 0) or 0)
        return {
            "deleted_entries": deleted_entries,
            "deleted_files": deleted_files,
            "deleted_orphan_files": int(orphan_cleanup.get("deleted_files", 0) or 0),
        }

    def delete_manual_alarm_files(self) -> Dict[str, Any]:
        if not self.enabled or self.role_mode != "internal" or self.store is None:
            return {"accepted": False, "reason": "disabled", "deleted_count": 0}
        deleted_count = 0
        deleted_buildings: List[str] = []
        rows = self.store.list_source_cache_entries(
            source_family=FAMILY_ALARM_EVENT,
            bucket_kind="manual",
            limit=1000,
        )
        for row in rows:
            if not isinstance(row, dict):
                continue
            relative_path = str(row.get("relative_path", "") or "").strip()
            file_path = self._resolve_relative_path_under_shared_root(relative_path)
            if self.store.delete_source_cache_entry(str(row.get("entry_id", "") or "").strip()):
                deleted_count += 1
                self._log_source_cache_event(
                    f"清理索引成功: entry_id={row.get('entry_id', '')}, relative_path={relative_path}"
                )
                self._safe_delete_cached_file(
                    file_path=file_path,
                    log_prefix="手动删除共享文件失败",
                )
                building = str(row.get("building", "") or "").strip()
                if building:
                    deleted_buildings.append(building)
        if deleted_count > 0:
            self._mark_external_full_snapshot_dirty()
        with self._lock:
            self._manual_alarm_refresh = {
                "running": False,
                "last_run_at": "",
                "last_success_at": "",
                "last_error": "",
                "bucket_key": "",
                "successful_buildings": [],
                "failed_buildings": [],
                "blocked_buildings": [],
                "total_row_count": 0,
                "building_row_counts": {},
                "query_start": "",
                "query_end": "",
            }
        return {
            "accepted": True,
            "reason": "deleted",
            "deleted_count": deleted_count,
            "deleted_buildings": deleted_buildings,
        }

    def debug_alarm_page_actions(self, *, building: str) -> Dict[str, Any]:
        raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")

    @staticmethod
    def _normalize_alarm_header_text(value: Any) -> str:
        text = str(value or "").strip().replace("（", "(").replace("）", ")")
        return "".join(ch for ch in text.lower() if not ch.isspace())

    @staticmethod
    def _normalize_alarm_cell_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, dt_time):
            return value.strftime("%H:%M:%S")
        text = str(value).strip()
        if text in {"--", "-"}:
            return ""
        return text

    @staticmethod
    def _parse_alarm_datetime_text(text: Any) -> datetime | None:
        raw = str(text or "").strip()
        if not raw:
            return None
        normalized = raw.replace("/", "-")
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(normalized, fmt)
            except ValueError:
                continue
        return None

    def _resolve_alarm_event_upload_target_fields(self) -> Dict[str, str]:
        defaults = {key: str(value or "").strip() for key, value in ALARM_EVENT_BITABLE_TARGET_FIELDS.items()}
        alarm_export_cfg = self.runtime_config.get("alarm_export", {})
        if not isinstance(alarm_export_cfg, dict):
            return defaults
        shared_upload_cfg = alarm_export_cfg.get("shared_source_upload", {})
        if not isinstance(shared_upload_cfg, dict):
            return defaults
        custom_fields = shared_upload_cfg.get("target_fields", {})
        if not isinstance(custom_fields, dict):
            return defaults
        for key, value in custom_fields.items():
            key_text = str(key or "").strip()
            value_text = str(value or "").strip()
            if key_text in defaults and value_text:
                defaults[key_text] = value_text
        return {
            key: value
            for key, value in defaults.items()
            if key in ALARM_EVENT_ALLOWED_UPLOAD_KEYS and str(value or "").strip()
        }

    @staticmethod
    def _read_positive_int(value: Any, default: int, minimum: int = 1) -> int:
        try:
            parsed = int(value or 0)
        except Exception:  # noqa: BLE001
            parsed = default
        return max(minimum, parsed or default)

    def _resolve_alarm_event_upload_target(self) -> Dict[str, Any]:
        auth_cfg = self.runtime_config.get("feishu", {})
        if not isinstance(auth_cfg, dict):
            auth_cfg = {}
        app_id = str(auth_cfg.get("app_id", "") or "").strip()
        app_secret = str(auth_cfg.get("app_secret", "") or "").strip()
        if not app_id or not app_secret:
            raise RuntimeError("飞书鉴权配置缺失：feishu.app_id/app_secret 不能为空")

        alarm_export_cfg = self.runtime_config.get("alarm_export", {})
        if not isinstance(alarm_export_cfg, dict):
            alarm_export_cfg = {}
        legacy_target = alarm_export_cfg.get("feishu", {})
        if not isinstance(legacy_target, dict):
            legacy_target = {}
        shared_upload_cfg = alarm_export_cfg.get("shared_source_upload", {})
        if not isinstance(shared_upload_cfg, dict):
            shared_upload_cfg = {}
        target_cfg = shared_upload_cfg.get("target", {})
        if not isinstance(target_cfg, dict):
            target_cfg = {}

        merged_target = {**legacy_target, **target_cfg}
        configured_app_token = str(merged_target.get("app_token", "") or "").strip()
        table_id = str(merged_target.get("table_id", "") or "").strip()
        if not configured_app_token or not table_id:
            raise RuntimeError("告警多维目标未配置：alarm_export.feishu.app_token/table_id 不能为空")

        target_preview = self.get_alarm_event_upload_target_preview(force_refresh=False)
        operation_app_token = str(target_preview.get("operation_app_token", "") or "").strip() or configured_app_token
        display_url = str(target_preview.get("display_url", "") or target_preview.get("bitable_url", "") or "").strip()
        if not display_url:
            display_url = build_bitable_url(configured_app_token, table_id)

        replace_existing_on_full = shared_upload_cfg.get("replace_existing_on_full")
        if not isinstance(replace_existing_on_full, bool):
            replace_existing_on_full = True
        return {
            "app_id": app_id,
            "app_secret": app_secret,
            "configured_app_token": configured_app_token,
            "operation_app_token": operation_app_token,
            "app_token": operation_app_token,
            "table_id": table_id,
            "target_kind": str(target_preview.get("target_kind", "") or "").strip(),
            "resolved_from": str(target_preview.get("resolved_from", "") or "").strip(),
            "wiki_node_token": str(target_preview.get("wiki_node_token", "") or "").strip(),
            "display_url": display_url,
            "bitable_url": display_url,
            "timeout": self._read_positive_int(auth_cfg.get("timeout"), 30),
            "request_retry_count": max(0, int(auth_cfg.get("request_retry_count", 3) or 3)),
            "request_retry_interval_sec": max(0.0, float(auth_cfg.get("request_retry_interval_sec", 2) or 2)),
            "list_page_size": self._read_positive_int(merged_target.get("page_size"), 500),
            "delete_batch_size": self._read_positive_int(merged_target.get("delete_batch_size"), 500),
            "create_batch_size": self._read_positive_int(merged_target.get("create_batch_size"), 200),
            "replace_existing_on_full": bool(replace_existing_on_full),
        }

    def _new_alarm_event_target_resolver(self) -> BitableTargetResolver:
        global_feishu = self.runtime_config.get("feishu", {}) if isinstance(self.runtime_config.get("feishu", {}), dict) else {}
        return BitableTargetResolver(
            app_id=str(global_feishu.get("app_id", "")).strip(),
            app_secret=str(global_feishu.get("app_secret", "")).strip(),
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
        )

    def get_alarm_event_upload_target_preview(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        alarm_export_cfg = self.runtime_config.get("alarm_export", {})
        if not isinstance(alarm_export_cfg, dict):
            alarm_export_cfg = {}
        legacy_target = alarm_export_cfg.get("feishu", {})
        if not isinstance(legacy_target, dict):
            legacy_target = {}
        shared_upload_cfg = alarm_export_cfg.get("shared_source_upload", {})
        if not isinstance(shared_upload_cfg, dict):
            shared_upload_cfg = {}
        target_cfg = shared_upload_cfg.get("target", {})
        if not isinstance(target_cfg, dict):
            target_cfg = {}
        merged_target = {**legacy_target, **target_cfg}
        configured_app_token = str(merged_target.get("app_token", "") or "").strip()
        table_id = str(merged_target.get("table_id", "") or "").strip()
        if not configured_app_token or not table_id:
            return {
                "configured_app_token": configured_app_token,
                "operation_app_token": "",
                "app_token": "",
                "table_id": table_id,
                "target_kind": "invalid",
                "display_url": "",
                "bitable_url": "",
                "wiki_node_token": "",
                "message": "请先在配置中心补齐告警多维 App Token 和 Table ID",
                "resolved_at": "",
            }
        try:
            preview = self._new_alarm_event_target_resolver().resolve_token_pair_preview(
                configured_app_token=configured_app_token,
                table_id=table_id,
                force_refresh=force_refresh,
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "configured_app_token": configured_app_token,
                "operation_app_token": configured_app_token,
                "app_token": configured_app_token,
                "table_id": table_id,
                "target_kind": "probe_error",
                "display_url": build_bitable_url(configured_app_token, table_id),
                "bitable_url": build_bitable_url(configured_app_token, table_id),
                "wiki_node_token": "",
                "message": str(exc),
                "resolved_at": _now_text(),
            }
        preview_payload = dict(preview if isinstance(preview, dict) else {})
        preview_payload["configured_app_token"] = str(preview_payload.get("configured_app_token", "") or configured_app_token).strip()
        preview_payload["operation_app_token"] = str(preview_payload.get("operation_app_token", "") or preview_payload.get("app_token", "") or configured_app_token).strip()
        preview_payload["app_token"] = preview_payload["operation_app_token"]
        preview_payload["table_id"] = str(preview_payload.get("table_id", "") or table_id).strip()
        display_url = str(preview_payload.get("display_url", "") or preview_payload.get("bitable_url", "") or "").strip()
        if not display_url:
            display_url = build_bitable_url(configured_app_token, table_id)
        preview_payload["display_url"] = display_url
        preview_payload["bitable_url"] = display_url
        return preview_payload

    def _build_alarm_event_bitable_client(self, target: Dict[str, Any]) -> FeishuBitableClient:
        return FeishuBitableClient(
            app_id=str(target.get("app_id", "") or "").strip(),
            app_secret=str(target.get("app_secret", "") or "").strip(),
            app_token=str(target.get("app_token", "") or "").strip(),
            calc_table_id=str(target.get("table_id", "") or "").strip(),
            attachment_table_id=str(target.get("table_id", "") or "").strip(),
            timeout=self._read_positive_int(target.get("timeout"), 30),
            request_retry_count=max(0, int(target.get("request_retry_count", 3) or 3)),
            request_retry_interval_sec=max(0.0, float(target.get("request_retry_interval_sec", 2) or 2)),
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
        )

    def _select_alarm_ready_entries_for_external_upload(self, *, building: str = "") -> List[Dict[str, Any]]:
        selection = self._build_alarm_external_selection(building=building)
        return [
            item
            for item in selection.get("selected_entries", []) or []
            if isinstance(item, dict)
        ]

    def _extract_alarm_entry_records_from_json_for_upload(
        self,
        *,
        file_path: Path,
        building: str,
        target_fields: Dict[str, str],
        max_age_days: int,
    ) -> Dict[str, Any]:
        payload = load_alarm_event_json(file_path)
        bucket_kind = str(payload.get("bucket_kind", "") or "").strip().lower()
        payload_building = str(payload.get("building", "") or "").strip()
        if bucket_kind not in {"latest", "manual"}:
            raise RuntimeError(f"告警 JSON bucket_kind 不可外网消费: {bucket_kind or '<empty>'}")
        if payload_building and str(building or "").strip() and payload_building != str(building or "").strip():
            raise RuntimeError(f"告警 JSON building 与索引楼栋不一致: payload={payload_building}, entry={building}")
        rows = payload.get("rows", []) if isinstance(payload.get("rows", []), list) else []
        cutoff_at = datetime.now() - timedelta(days=max(1, int(max_age_days or 60)))
        records: List[Dict[str, Any]] = []
        total_rows = 0
        kept_rows = 0
        for item in rows:
            if not isinstance(item, dict):
                continue
            total_rows += 1
            event_time_text = self._normalize_alarm_cell_text(item.get("event_time"))
            event_time = self._parse_alarm_datetime_text(event_time_text)
            if event_time is None or event_time < cutoff_at:
                continue
            kept_rows += 1
            source_payload = {
                "level": self._normalize_alarm_cell_text(item.get("level")),
                "building": str(item.get("building", "") or "").strip() or str(building or "").strip(),
                "content": self._normalize_alarm_cell_text(item.get("content")),
                "position": self._normalize_alarm_cell_text(item.get("position")),
                "object": self._normalize_alarm_cell_text(item.get("object")),
                "event_time": event_time_text,
                "accept_time": self._normalize_alarm_cell_text(item.get("accept_time")),
                "is_accept": self._normalize_alarm_cell_text(item.get("is_accept")),
                "accept_by": self._normalize_alarm_cell_text(item.get("accept_by")),
                "accept_content": self._normalize_alarm_cell_text(item.get("accept_content")),
                "recover_time": self._normalize_alarm_cell_text(item.get("recover_time")),
                "is_recover": self._normalize_alarm_cell_text(item.get("is_recover")),
                "event_suggest": self._normalize_alarm_cell_text(item.get("event_suggest")),
                "event_type": self._normalize_alarm_cell_text(item.get("event_type")),
                "trigger_value": self._normalize_alarm_cell_text(item.get("event_snapshot")),
                "confirm_type": self._normalize_alarm_cell_text(item.get("confirm_type")),
                "confirm_time": self._normalize_alarm_cell_text(item.get("confirm_time")),
                "confirm_by": self._normalize_alarm_cell_text(item.get("confirm_by")),
                "confirm_description": self._normalize_alarm_cell_text(item.get("confirm_description")),
            }
            fields: Dict[str, Any] = {}
            for source_key, field_name in target_fields.items():
                text = str(field_name or "").strip()
                if not text:
                    continue
                mapped_value = self._map_alarm_upload_field_value(source_key, source_payload.get(source_key, ""))
                if source_key in (ALARM_EVENT_BITABLE_DATETIME_FIELDS | ALARM_EVENT_BITABLE_NUMBER_FIELDS) and mapped_value in {"", None}:
                    continue
                fields[text] = mapped_value
            if fields:
                records.append(fields)
        return {"records": records, "total_rows": total_rows, "kept_rows": kept_rows}

    @staticmethod
    def _bitable_field_text(value: Any) -> str:
        if isinstance(value, list):
            return " ".join(str(item or "").strip() for item in value if str(item or "").strip()).strip()
        if isinstance(value, dict):
            return str(value.get("text") or value.get("name") or value.get("value") or "").strip()
        return str(value or "").strip()

    def _emit_alarm_upload_log(self, text: str, emit_log: Callable[[str], None] | None = None) -> None:
        line = str(text or "").strip()
        if not line:
            return
        if callable(emit_log):
            emit_log(line)
            return
        self._emit(line)

    @staticmethod
    def _summarize_alarm_upload_error(error: Any) -> str:
        text = str(error or "").strip()
        if not text:
            return "告警信息文件上传失败"
        detail = ""
        candidate = text
        if "{" in candidate and "}" in candidate:
            brace_start = candidate.find("{")
            brace_end = candidate.rfind("}")
            if brace_start >= 0 and brace_end > brace_start:
                candidate = candidate[brace_start : brace_end + 1]
        try:
            payload = ast.literal_eval(candidate)
        except Exception:  # noqa: BLE001
            payload = None
        if isinstance(payload, dict):
            detail = str(payload.get("msg", "") or "").strip()
            inner = payload.get("error", {}) if isinstance(payload.get("error", {}), dict) else {}
            message = str(inner.get("message", "") or "").strip()
            lowered = f"{detail} {message}".lower()
            if "datetimefieldconvfail" in lowered or "unix timestamp" in lowered:
                summary = "飞书日期字段格式错误，请查看运行日志"
            elif "numberfieldconvfail" in lowered or "must be a number" in lowered:
                summary = "飞书数值字段格式错误，请查看运行日志"
            else:
                summary = detail or message or text
        else:
            summary = text
        return summary

    @staticmethod
    def _coerce_alarm_number_field(value: Any) -> Any:
        text = str(value or "").strip()
        if not text:
            return ""
        normalized = text.replace(",", "")
        try:
            number = Decimal(normalized)
        except (InvalidOperation, ValueError):
            return ""
        if number == number.to_integral():
            return int(number)
        return float(number)

    @staticmethod
    def _map_alarm_upload_field_value(source_key: str, value: Any) -> Any:
        if source_key in ALARM_EVENT_BITABLE_DATETIME_FIELDS:
            timestamp_ms = parse_timestamp_ms(value)
            return int(timestamp_ms) if timestamp_ms is not None else ""
        if source_key in ALARM_EVENT_BITABLE_NUMBER_FIELDS:
            return SharedSourceCacheService._coerce_alarm_number_field(value)
        return value

    def _extract_alarm_entry_records_from_workbook_for_upload(
        self,
        *,
        file_path: Path,
        building: str,
        target_fields: Dict[str, str],
        max_age_days: int,
    ) -> Dict[str, Any]:
        validate_excel_workbook_file(file_path)
        workbook = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        try:
            sheet = workbook.active
            rows_iter = sheet.iter_rows(values_only=True)
            header_row = next(rows_iter, None)
            if header_row is None:
                return {"records": [], "total_rows": 0, "kept_rows": 0}
            header_map: Dict[str, int] = {}
            for index, cell_value in enumerate(header_row):
                normalized_header = self._normalize_alarm_header_text(cell_value)
                if not normalized_header:
                    continue
                for canonical_key, aliases in ALARM_EVENT_HEADER_ALIASES.items():
                    if canonical_key in header_map:
                        continue
                    normalized_aliases = {
                        "".join(ch for ch in str(alias or "").strip().lower() if not ch.isspace())
                        for alias in aliases
                    }
                    if normalized_header in normalized_aliases:
                        header_map[canonical_key] = index
            if "event_time" not in header_map:
                raise RuntimeError("告警文件缺少“告警时间/产生时间”列")

            cutoff_at = datetime.now() - timedelta(days=max(1, int(max_age_days or 60)))
            records: List[Dict[str, Any]] = []
            total_rows = 0
            kept_rows = 0

            def _pick(row_values: tuple[Any, ...], key: str) -> str:
                idx = header_map.get(key)
                if idx is None or idx >= len(row_values):
                    return ""
                return self._normalize_alarm_cell_text(row_values[idx])

            for row_values in rows_iter:
                if row_values is None:
                    continue
                values = tuple(row_values)
                if not any(str(value or "").strip() for value in values):
                    continue
                total_rows += 1
                event_time_text = _pick(values, "event_time")
                event_time = self._parse_alarm_datetime_text(event_time_text)
                if event_time is None or event_time < cutoff_at:
                    continue
                kept_rows += 1
                source_payload = {
                    "level": _pick(values, "level"),
                    "building": str(building or "").strip(),
                    "content": _pick(values, "content"),
                    "position": _pick(values, "position"),
                    "object": _pick(values, "object"),
                    "event_time": event_time_text,
                    "accept_time": _pick(values, "accept_time"),
                    "is_accept": _pick(values, "is_accept"),
                    "accept_by": _pick(values, "accept_by"),
                    "accept_content": _pick(values, "accept_content"),
                    "recover_time": _pick(values, "recover_time"),
                    "is_recover": _pick(values, "is_recover"),
                    "event_suggest": _pick(values, "event_suggest"),
                    "event_type": _pick(values, "event_type"),
                    "trigger_value": _pick(values, "trigger_value"),
                    "confirm_type": _pick(values, "confirm_type"),
                    "confirm_time": _pick(values, "confirm_time"),
                    "confirm_by": _pick(values, "confirm_by"),
                    "confirm_description": _pick(values, "confirm_description"),
                }
                fields: Dict[str, Any] = {}
                for source_key, field_name in target_fields.items():
                    text = str(field_name or "").strip()
                    if not text:
                        continue
                    mapped_value = self._map_alarm_upload_field_value(source_key, source_payload.get(source_key, ""))
                    if source_key in (ALARM_EVENT_BITABLE_DATETIME_FIELDS | ALARM_EVENT_BITABLE_NUMBER_FIELDS) and mapped_value in {"", None}:
                        continue
                    fields[text] = mapped_value
                if fields:
                    records.append(fields)
            return {"records": records, "total_rows": total_rows, "kept_rows": kept_rows}
        finally:
            workbook.close()

    def _extract_alarm_entry_records_for_upload(
        self,
        *,
        entry: Dict[str, Any],
        building: str,
        target_fields: Dict[str, str],
        max_age_days: int,
    ) -> Dict[str, Any]:
        file_path = self._resolve_entry_file_path(entry)
        if file_path is None:
            raise RuntimeError("共享文件不存在或不可访问")
        if str(file_path.suffix or "").strip().lower() == ".json":
            return self._extract_alarm_entry_records_from_json_for_upload(
                file_path=file_path,
                building=building,
                target_fields=target_fields,
                max_age_days=max_age_days,
            )
        return self._extract_alarm_entry_records_from_workbook_for_upload(
            file_path=file_path,
            building=building,
            target_fields=target_fields,
            max_age_days=max_age_days,
        )

    def _delete_alarm_records_for_building_from_bitable(
        self,
        *,
        client: FeishuBitableClient,
        table_id: str,
        building: str,
        target_fields: Dict[str, str],
        list_page_size: int,
        delete_batch_size: int,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> int:
        building_field_name = str(target_fields.get("building", "") or "").strip()
        building_text = str(building or "").strip()
        if not table_id or not building_field_name or not building_text:
            return 0
        rows = client.list_records(table_id=table_id, page_size=list_page_size)
        record_ids: List[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            fields = row.get("fields", {}) if isinstance(row.get("fields", {}), dict) else {}
            if self._bitable_field_text(fields.get(building_field_name)) != building_text:
                continue
            record_id = str(row.get("record_id", "") or "").strip()
            if record_id:
                record_ids.append(record_id)
        if not record_ids:
            return 0
        try:
            return client.batch_delete_records(
                table_id=table_id,
                record_ids=record_ids,
                batch_size=delete_batch_size,
                progress_callback=progress_callback,
            )
        except TypeError:
            return client.batch_delete_records(table_id=table_id, record_ids=record_ids, batch_size=delete_batch_size)

    def _mark_alarm_entry_uploaded(
        self,
        *,
        entry: Dict[str, Any],
        uploaded_by_mode: str,
        uploaded_scope: str,
    ) -> bool:
        if self.store is None:
            return False
        entry_id = str(entry.get("entry_id", "") or "").strip()
        if not entry_id:
            return False
        uploaded_at = _now_text()
        current_status = str(entry.get("status", "") or "").strip().lower() or "ready"
        self.store.update_source_cache_entry_status(
            entry_id,
            status=current_status,
            metadata_update={
                "last_uploaded_at": uploaded_at,
                "last_uploaded_by_role": self.role_mode,
                "last_uploaded_by_family": FAMILY_ALARM_EVENT,
                "last_uploaded_mode": str(uploaded_by_mode or "").strip(),
                "last_uploaded_scope": str(uploaded_scope or "").strip(),
            },
        )
        self._mark_external_full_snapshot_dirty()
        return True

    def _set_alarm_external_upload_state(
        self,
        *,
        running: bool,
        started_at: str,
        current_mode: str,
        current_scope: str,
        mode: str,
        scope: str,
        uploaded_record_count: int,
        uploaded_file_count: int,
        consumed_count: int,
        failed_entries: List[str],
        deleted_before_upload_count: int,
        last_error: str,
        success: bool,
    ) -> None:
        now_text = _now_text()
        with self._lock:
            self._alarm_external_upload_state = {
                "running": bool(running),
                "started_at": str(started_at or "").strip(),
                "current_mode": str(current_mode or "").strip(),
                "current_scope": str(current_scope or "").strip(),
                "last_run_at": now_text,
                "last_success_at": now_text if success else str(self._alarm_external_upload_state.get("last_success_at", "") or "").strip(),
                "last_error": str(last_error or "").strip(),
                "last_mode": str(mode or "").strip(),
                "last_scope": str(scope or "").strip(),
                "uploaded_record_count": max(0, int(uploaded_record_count or 0)),
                "uploaded_file_count": max(0, int(uploaded_file_count or 0)),
                "consumed_count": max(0, int(consumed_count or 0)),
                "failed_entries": [str(item or "").strip() for item in failed_entries if str(item or "").strip()],
                "deleted_before_upload_count": max(0, int(deleted_before_upload_count or 0)),
            }
            self._external_full_snapshot_dirty = True

    def _begin_alarm_external_upload(self, *, mode: str, scope: str) -> Dict[str, Any]:
        started_at = _now_text()
        normalized_mode = str(mode or "").strip()
        normalized_scope = str(scope or "").strip() or "all"
        with self._lock:
            if bool(self._alarm_external_upload_state.get("running")):
                return {
                    "accepted": False,
                    "running": True,
                    "reason": "already_running",
                    "mode": str(self._alarm_external_upload_state.get("current_mode", "") or "").strip()
                    or str(self._alarm_external_upload_state.get("last_mode", "") or "").strip(),
                    "scope": str(self._alarm_external_upload_state.get("current_scope", "") or "").strip()
                    or str(self._alarm_external_upload_state.get("last_scope", "") or "").strip()
                    or "all",
                    "started_at": str(self._alarm_external_upload_state.get("started_at", "") or "").strip(),
                }
            self._alarm_external_upload_state = {
                **self._alarm_external_upload_state,
                "running": True,
                "started_at": started_at,
                "current_mode": normalized_mode,
                "current_scope": normalized_scope,
                "last_error": "",
            }
        return {"accepted": True, "running": True, "started_at": started_at}

    def upload_alarm_event_entries_to_bitable(
        self,
        *,
        mode: str,
        building: str = "",
        replace_existing: bool | None,
        max_age_days: int = 60,
        emit_log: Callable[[str], None] | None = None,
    ) -> Dict[str, Any]:
        if not self.enabled or self.role_mode != "external" or self.store is None:
            return {"accepted": False, "reason": "disabled"}

        normalized_mode = str(mode or "").strip().lower() or "full"
        target_building = str(building or "").strip()
        scope_text = target_building or "all"
        begin_state = self._begin_alarm_external_upload(mode=normalized_mode, scope=target_building or "all")
        if not bool(begin_state.get("accepted")):
            return begin_state
        started_at = str(begin_state.get("started_at", "") or "").strip()
        self._emit_alarm_upload_log(
            f"[共享缓存] 外网告警文件上传开始: mode={normalized_mode}, scope={scope_text}, kept_days={max(1, int(max_age_days or 60))}",
            emit_log=emit_log,
        )
        resolved_target: Dict[str, Any] | None = None
        if replace_existing is None:
            try:
                resolved_target = self._resolve_alarm_event_upload_target()
                replace_existing = bool(resolved_target.get("replace_existing_on_full", True))
            except Exception as exc:  # noqa: BLE001
                raw_error_text = str(exc)
                error_text = self._summarize_alarm_upload_error(raw_error_text)
                self._set_alarm_external_upload_state(
                    running=False,
                    started_at="",
                    current_mode="",
                    current_scope="",
                    mode=normalized_mode,
                    scope=target_building or "all",
                    uploaded_record_count=0,
                    uploaded_file_count=0,
                    consumed_count=0,
                    failed_entries=[],
                    deleted_before_upload_count=0,
                    last_error=error_text,
                    success=False,
                )
                self._emit_alarm_upload_log(
                    f"[共享缓存] 外网告警文件上传失败: mode={normalized_mode}, scope={scope_text}, error={error_text}",
                    emit_log=emit_log,
                )
                return {
                    "accepted": False,
                    "reason": "misconfigured",
                    "mode": normalized_mode,
                    "scope": scope_text,
                    "running": False,
                    "error": error_text,
                }
        selection = self._build_alarm_external_selection(building=target_building)
        selected_entries = [
            item
            for item in selection.get("selected_entries", []) or []
            if isinstance(item, dict)
        ]
        used_previous_day_fallback = [
            str(item or "").strip()
            for item in selection.get("used_previous_day_fallback", []) or []
            if str(item or "").strip()
        ]
        missing_today_buildings = [
            str(item or "").strip()
            for item in selection.get("missing_today_buildings", []) or []
            if str(item or "").strip()
        ]
        missing_both_days_buildings = [
            str(item or "").strip()
            for item in selection.get("missing_both_days_buildings", []) or []
            if str(item or "").strip()
        ]
        selection_reference_date = str(selection.get("selection_reference_date", "") or "").strip()
        if not selected_entries:
            self._set_alarm_external_upload_state(
                running=False,
                started_at="",
                current_mode="",
                current_scope="",
                mode=normalized_mode,
                scope=target_building or "all",
                uploaded_record_count=0,
                uploaded_file_count=0,
                consumed_count=0,
                failed_entries=[],
                deleted_before_upload_count=0,
                last_error="",
                success=True,
            )
            self._emit_alarm_upload_log(
                f"[共享缓存] 外网告警文件上传完成: mode={normalized_mode}, scope={scope_text}, no_ready_entries=true",
                emit_log=emit_log,
            )
            return {
                "accepted": True,
                "reason": "no_ready_entries",
                "mode": normalized_mode,
                "scope": scope_text,
                "uploaded_record_count": 0,
                "uploaded_file_count": 0,
                "consumed_count": 0,
                "failed_entries": [],
                "used_previous_day_fallback": used_previous_day_fallback,
                "missing_today_buildings": missing_today_buildings,
                "missing_both_days_buildings": missing_both_days_buildings,
                "selection_reference_date": selection_reference_date,
            }

        target_fields = self._resolve_alarm_event_upload_target_fields()
        parse_failed_entries: List[str] = []
        parsed_entries: List[tuple[Dict[str, Any], Dict[str, Any]]] = []
        aggregated_records: List[Dict[str, Any]] = []
        for entry in selected_entries:
            row_building = str(entry.get("building", "") or "").strip()
            try:
                parsed = self._extract_alarm_entry_records_for_upload(
                    entry=entry,
                    building=row_building,
                    target_fields=target_fields,
                    max_age_days=max_age_days,
                )
                parsed_entries.append((entry, parsed))
                aggregated_records.extend(parsed.get("records", []) if isinstance(parsed.get("records", []), list) else [])
            except Exception as exc:  # noqa: BLE001
                parse_failed_entries.append(row_building or "未知楼栋")
                entry_id = str(entry.get("entry_id", "") or "").strip()
                if entry_id:
                    self.store.update_source_cache_entry_status(
                        entry_id,
                        status="failed",
                        metadata_update={
                            "error": f"外网告警上传前解析失败：{exc}",
                            "upload_failed_at": _now_text(),
                        },
                    )

        if normalized_mode == "full" and parse_failed_entries:
            error_text = "存在告警文件预检失败楼栋，未执行清表或上传"
            self._set_alarm_external_upload_state(
                running=False,
                started_at="",
                current_mode="",
                current_scope="",
                mode=normalized_mode,
                scope=target_building or "all",
                uploaded_record_count=0,
                uploaded_file_count=0,
                consumed_count=0,
                failed_entries=parse_failed_entries,
                deleted_before_upload_count=0,
                last_error=error_text,
                success=False,
            )
            self._emit_alarm_upload_log(
                f"[共享缓存] 外网告警文件预检失败: mode={normalized_mode}, scope={scope_text}, failed_entries={', '.join(parse_failed_entries)}",
                emit_log=emit_log,
            )
            return {
                "accepted": False,
                "reason": "precheck_failed",
                "mode": normalized_mode,
                "scope": scope_text,
                "uploaded_record_count": 0,
                "uploaded_file_count": 0,
                "consumed_count": 0,
                "failed_entries": parse_failed_entries,
                "error": error_text,
                "started_at": started_at,
                "running": False,
                "used_previous_day_fallback": used_previous_day_fallback,
                "missing_today_buildings": missing_today_buildings,
                "missing_both_days_buildings": missing_both_days_buildings,
                "selection_reference_date": selection_reference_date,
            }

        if not parsed_entries:
            error_text = "所有告警文件解析失败，未执行上传"
            self._set_alarm_external_upload_state(
                running=False,
                started_at="",
                current_mode="",
                current_scope="",
                mode=normalized_mode,
                scope=target_building or "all",
                uploaded_record_count=0,
                uploaded_file_count=0,
                consumed_count=0,
                failed_entries=parse_failed_entries,
                deleted_before_upload_count=0,
                last_error=error_text,
                success=False,
            )
            self._emit_alarm_upload_log(
                f"[共享缓存] 外网告警文件上传失败: mode={normalized_mode}, scope={scope_text}, error={error_text}",
                emit_log=emit_log,
            )
            return {
                "accepted": False,
                "reason": "all_entries_parse_failed",
                "mode": normalized_mode,
                "scope": scope_text,
                "uploaded_record_count": 0,
                "uploaded_file_count": 0,
                "consumed_count": 0,
                "failed_entries": parse_failed_entries,
                "error": error_text,
                "used_previous_day_fallback": used_previous_day_fallback,
                "missing_today_buildings": missing_today_buildings,
                "missing_both_days_buildings": missing_both_days_buildings,
                "selection_reference_date": selection_reference_date,
            }

        try:
            target = resolved_target or self._resolve_alarm_event_upload_target()
            client = self._build_alarm_event_bitable_client(target)
            table_id = str(target.get("table_id", "") or "").strip()
            last_progress_checkpoint = 0
            last_delete_logged = -1

            def _progress_callback(uploaded: int, total: int) -> None:
                nonlocal last_progress_checkpoint
                current_checkpoint = max(0, int(uploaded or 0)) // 100
                while current_checkpoint > last_progress_checkpoint:
                    last_progress_checkpoint += 1
                    checkpoint_value = last_progress_checkpoint * 100
                    self._emit_alarm_upload_log(
                        f"[共享缓存] 外网告警上传进度: mode={normalized_mode}, scope={scope_text}, uploaded={min(checkpoint_value, max(0, int(total or 0)))}/{max(0, int(total or 0))}",
                        emit_log=emit_log,
                    )

            def _delete_progress_callback(deleted: int, total: int) -> None:
                nonlocal last_delete_logged
                normalized_deleted = max(0, int(deleted or 0))
                normalized_total = max(0, int(total or 0))
                if normalized_total <= 0:
                    return
                if normalized_deleted == last_delete_logged:
                    return
                last_delete_logged = normalized_deleted
                self._emit_alarm_upload_log(
                    f"[共享缓存] 外网告警删除旧记录进度: mode={normalized_mode}, scope={scope_text}, deleted={normalized_deleted}/{normalized_total}",
                    emit_log=emit_log,
                )

            deleted_count = 0
            if normalized_mode == "single_building":
                self._emit_alarm_upload_log(
                    f"[共享缓存] 外网告警开始删除旧记录: mode={normalized_mode}, scope={scope_text}, strategy=single_building",
                    emit_log=emit_log,
                )
                deleted_count = self._delete_alarm_records_for_building_from_bitable(
                    client=client,
                    table_id=table_id,
                    building=target_building,
                    target_fields=target_fields,
                    list_page_size=self._read_positive_int(target.get("list_page_size"), 500),
                    delete_batch_size=self._read_positive_int(target.get("delete_batch_size"), 500),
                    progress_callback=_delete_progress_callback,
                )
            elif bool(replace_existing):
                self._emit_alarm_upload_log(
                    f"[共享缓存] 外网告警开始删除旧记录: mode={normalized_mode}, scope={scope_text}, strategy=clear_table",
                    emit_log=emit_log,
                )
                try:
                    deleted_count = client.clear_table(
                        table_id=table_id,
                        list_page_size=self._read_positive_int(target.get("list_page_size"), 500),
                        delete_batch_size=self._read_positive_int(target.get("delete_batch_size"), 500),
                        progress_callback=_delete_progress_callback,
                    )
                except TypeError:
                    deleted_count = client.clear_table(
                        table_id=table_id,
                        list_page_size=self._read_positive_int(target.get("list_page_size"), 500),
                        delete_batch_size=self._read_positive_int(target.get("delete_batch_size"), 500),
                    )
            if normalized_mode == "single_building" or bool(replace_existing):
                self._emit_alarm_upload_log(
                    f"[共享缓存] 外网告警删除旧记录完成: mode={normalized_mode}, scope={scope_text}, deleted={deleted_count}",
                    emit_log=emit_log,
                )
            if aggregated_records:
                client.batch_create_records(
                    table_id=table_id,
                    fields_list=aggregated_records,
                    batch_size=self._read_positive_int(target.get("create_batch_size"), 200),
                    progress_callback=_progress_callback,
                )
        except Exception as exc:  # noqa: BLE001
            raw_error_text = str(exc)
            error_text = self._summarize_alarm_upload_error(raw_error_text)
            self._set_alarm_external_upload_state(
                running=False,
                started_at="",
                current_mode="",
                current_scope="",
                mode=normalized_mode,
                scope=target_building or "all",
                uploaded_record_count=0,
                uploaded_file_count=0,
                consumed_count=0,
                failed_entries=parse_failed_entries,
                deleted_before_upload_count=0,
                last_error=error_text,
                success=False,
            )
            self._emit_alarm_upload_log(
                f"[共享缓存] 外网告警文件上传失败: mode={normalized_mode}, scope={scope_text}, error={error_text}",
                emit_log=emit_log,
            )
            if raw_error_text and raw_error_text != error_text:
                self._emit_alarm_upload_log(
                    f"[共享缓存] 外网告警文件上传错误详情: mode={normalized_mode}, scope={scope_text}, detail={raw_error_text}",
                    emit_log=emit_log,
                )
            return {
                "accepted": False,
                "reason": "upload_failed",
                "mode": normalized_mode,
                "scope": scope_text,
                "uploaded_record_count": 0,
                "uploaded_file_count": 0,
                "consumed_count": 0,
                "failed_entries": parse_failed_entries,
                "error": error_text,
                "used_previous_day_fallback": used_previous_day_fallback,
                "missing_today_buildings": missing_today_buildings,
                "missing_both_days_buildings": missing_both_days_buildings,
                "selection_reference_date": selection_reference_date,
            }

        consumed_count = 0
        consume_failed_entries: List[str] = []
        consumed_buildings: List[str] = []
        for entry, _parsed in parsed_entries:
            row_building = str(entry.get("building", "") or "").strip()
            if self._mark_alarm_entry_uploaded(
                entry=entry,
                uploaded_by_mode=normalized_mode,
                uploaded_scope=scope_text,
            ):
                continue
            elif row_building:
                consume_failed_entries.append(row_building)

        failed_entries = [*parse_failed_entries, *consume_failed_entries]
        success = len(failed_entries) == 0
        self._set_alarm_external_upload_state(
            running=False,
            started_at="",
            current_mode="",
            current_scope="",
            mode=normalized_mode,
            scope=target_building or "all",
            uploaded_record_count=len(aggregated_records),
            uploaded_file_count=len(parsed_entries),
            consumed_count=consumed_count,
            failed_entries=failed_entries,
            deleted_before_upload_count=deleted_count,
            last_error="" if success else "存在失败楼栋，请查看 failed_entries",
            success=success,
        )
        self._emit_alarm_upload_log(
            f"[共享缓存] 外网告警文件上传完成: mode={normalized_mode}, scope={scope_text}, records={len(aggregated_records)}, files={len(parsed_entries)}, retained_files={len(parsed_entries)}"
            + (f", failed_entries={', '.join(failed_entries)}" if failed_entries else ""),
            emit_log=emit_log,
        )
        return {
            "accepted": True,
            "reason": "completed" if success else "partial_completed",
            "mode": normalized_mode,
            "scope": scope_text,
            "uploaded_record_count": len(aggregated_records),
            "uploaded_file_count": len(parsed_entries),
            "consumed_count": consumed_count,
            "consumed_buildings": consumed_buildings,
            "failed_entries": failed_entries,
            "deleted_before_upload_count": deleted_count,
            "kept_days": max(1, int(max_age_days or 60)),
            "started_at": started_at,
            "running": False,
            "used_previous_day_fallback": used_previous_day_fallback,
            "missing_today_buildings": missing_today_buildings,
            "missing_both_days_buildings": missing_both_days_buildings,
            "selection_reference_date": selection_reference_date,
        }

    def upload_alarm_event_entries_full_to_bitable(
        self,
        *,
        emit_log: Callable[[str], None] | None = None,
    ) -> Dict[str, Any]:
        return self.upload_alarm_event_entries_to_bitable(
            mode="full",
            building="",
            replace_existing=None,
            max_age_days=60,
            emit_log=emit_log,
        )

    def upload_alarm_event_entries_single_building_to_bitable(
        self,
        *,
        building: str,
        emit_log: Callable[[str], None] | None = None,
    ) -> Dict[str, Any]:
        building_name = str(building or "").strip()
        if not building_name:
            return {"accepted": False, "reason": "invalid_building", "error": "缺少楼栋参数"}
        return self.upload_alarm_event_entries_to_bitable(
            mode="single_building",
            building=building_name,
            replace_existing=False,
            max_age_days=60,
            emit_log=emit_log,
        )

    def consume_ready_alarm_event_entries(self) -> Dict[str, Any]:
        return {
            "accepted": False,
            "reason": "retired",
            "consumed_count": 0,
            "failed_count": 0,
            "consumed_buildings": [],
        }

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
        buildings: List[str] | None = None,
        force_refresh_existing: bool = False,
    ) -> Dict[str, Any]:
        enabled_buildings = set(self.get_enabled_buildings())
        requested_buildings = buildings if isinstance(buildings, list) and buildings else self.get_enabled_buildings()
        buildings = []
        for item in requested_buildings:
            building_name = str(item or "").strip()
            if building_name and building_name in enabled_buildings and building_name not in buildings:
                buildings.append(building_name)
        normalized_family = self._normalize_source_family(source_family)
        ready_count = 0
        failed_buildings: List[str] = []
        blocked_buildings: List[str] = []
        running_buildings: List[str] = []
        completed_buildings: List[str] = []
        pending_buildings: List[str] = []
        with self._lock:
            self._ensure_light_family_cache_unlocked(
                source_family=normalized_family,
                bucket_key=bucket_key,
                buildings=buildings,
            )
        for building in buildings:
            entry_exists = self._entry_exists_for_bucket(
                source_family=normalized_family,
                building=building,
                bucket_kind="latest",
                bucket_key=bucket_key,
            )
            ready_entry = self._get_ready_entry(
                source_family=normalized_family,
                building=building,
                bucket_kind="latest",
                bucket_key=bucket_key,
            )
            ready_file_path = self._resolve_entry_file_path(ready_entry) if ready_entry else None
            if ready_entry and ready_file_path is not None and not force_refresh_existing:
                with self._lock:
                    self._set_light_building_status_from_entry_unlocked(
                        source_family=normalized_family,
                        building=building,
                        bucket_key=bucket_key,
                        entry=ready_entry,
                        file_path=ready_file_path,
                    )
                ready_count += 1
                continue
            pause_info = (
                self.download_browser_pool.get_building_pause_info(building)
                if self.download_browser_pool is not None and hasattr(self.download_browser_pool, "get_building_pause_info")
                else {}
            )
            failed_entry = self._get_source_cache_entry(
                source_family=normalized_family,
                building=building,
                bucket_kind="latest",
                bucket_key=bucket_key,
                status="failed",
            )
            if bool(pause_info.get("suspended", False)):
                blocked_buildings.append(building)
                blocked_reason = str(
                    pause_info.get("suspend_reason", "") or pause_info.get("pending_issue_summary", "") or ""
                ).strip()
                with self._lock:
                    self._set_light_building_status_unlocked(
                        source_family=normalized_family,
                        building=building,
                        bucket_key=bucket_key,
                        payload={
                            "status": "waiting",
                            "ready": False,
                            "downloaded_at": "",
                            "last_error": blocked_reason,
                            "relative_path": "",
                            "resolved_file_path": "",
                            "started_at": "",
                            "blocked": True,
                            "blocked_reason": blocked_reason,
                            "next_probe_at": str(pause_info.get("next_probe_at", "") or "").strip(),
                        },
                    )
                continue
            if entry_exists and not force_retry_failed and not self._should_retry_failed_latest_entry(failed_entry):
                failed_buildings.append(building)
                with self._lock:
                    if failed_entry:
                        self._set_light_building_status_from_entry_unlocked(
                            source_family=normalized_family,
                            building=building,
                            bucket_key=bucket_key,
                            entry=failed_entry,
                            file_path=None,
                        )
                    else:
                        self._set_light_building_status_unlocked(
                            source_family=normalized_family,
                            building=building,
                            bucket_key=bucket_key,
                            payload={
                                "status": "failed",
                                "ready": False,
                                "downloaded_at": "",
                                "last_error": "",
                                "relative_path": "",
                                "resolved_file_path": "",
                                "started_at": "",
                                "blocked": False,
                                "blocked_reason": "",
                                "next_probe_at": "",
                            },
                        )
                continue
            pending_buildings.append(building)
        future_map: Dict[concurrent.futures.Future[Any], tuple[str, tuple[str, str, str, str]]] = {}
        if pending_buildings:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(pending_buildings), len(_DEFAULT_BUILDINGS))) as executor:
                for building in pending_buildings:
                    active_key = (normalized_family, building, "latest", bucket_key)
                    started_at = _now_text()
                    with self._lock:
                        self._active_latest_downloads[active_key] = started_at
                        self._set_light_building_status_unlocked(
                            source_family=normalized_family,
                            building=building,
                            bucket_key=bucket_key,
                            payload={
                                "status": "downloading",
                                "ready": False,
                                "downloaded_at": "",
                                "last_error": "",
                                "relative_path": "",
                                "resolved_file_path": "",
                                "started_at": started_at,
                                "blocked": False,
                                "blocked_reason": "",
                                "next_probe_at": "",
                            },
                        )
                    running_buildings.append(building)
                    future = executor.submit(
                        fill_func,
                        building=building,
                        bucket_key=bucket_key,
                        emit_log=self._emit,
                    )
                    future_map[future] = (building, active_key)
                for future in concurrent.futures.as_completed(future_map):
                    building, active_key = future_map[future]
                    try:
                        result = self._normalize_single_fill_result(future.result())
                        if isinstance(result, dict):
                            with self._lock:
                                self._set_light_building_status_unlocked(
                                    source_family=normalized_family,
                                    building=building,
                                    bucket_key=bucket_key,
                                    payload={
                                        "status": "ready",
                                        "ready": True,
                                        "downloaded_at": str(result.get("downloaded_at", "") or "").strip(),
                                        "last_error": "",
                                        "relative_path": str(result.get("relative_path", "") or "").strip(),
                                        "resolved_file_path": str(result.get("file_path", "") or "").strip(),
                                        "started_at": "",
                                        "blocked": False,
                                        "blocked_reason": "",
                                        "next_probe_at": "",
                                    },
                                )
                        ready_count += 1
                        completed_buildings.append(building)
                        with self._lock:
                            self._last_success_at = _now_text()
                    except Exception as exc:  # noqa: BLE001
                        failed_buildings.append(building)
                        error_text = str(exc)
                        with self._lock:
                            self._last_error = error_text
                            self._set_light_building_status_unlocked(
                                source_family=normalized_family,
                                building=building,
                                bucket_key=bucket_key,
                                payload={
                                    "status": "failed",
                                    "ready": False,
                                    "downloaded_at": "",
                                    "last_error": error_text,
                                    "relative_path": self._failed_marker_relative_path(
                                        source_family=normalized_family,
                                        bucket_kind="latest",
                                        bucket_key=bucket_key,
                                        building=building,
                                    ),
                                    "resolved_file_path": "",
                                    "started_at": "",
                                    "blocked": False,
                                    "blocked_reason": "",
                                    "next_probe_at": "",
                                },
                            )
                        self._record_failed_entry(
                            source_family=normalized_family,
                            building=building,
                            bucket_kind="latest",
                            bucket_key=bucket_key,
                            error_text=error_text,
                            metadata={"family": normalized_family, "building": building},
                        )
                        self._emit(f"[共享缓存] 本小时预下载失败 family={normalized_family} building={building}: {exc}")
                    finally:
                        with self._lock:
                            self._active_latest_downloads.pop(active_key, None)
        with self._lock:
            family_status = self._recompute_family_status_from_light_unlocked(
                source_family=normalized_family,
                bucket_key=bucket_key,
            )
        return {
            "ready_count": int(family_status.get("ready_count", ready_count) or 0),
            "failed_buildings": failed_buildings,
            "blocked_buildings": blocked_buildings,
            "running_buildings": running_buildings,
            "completed_buildings": completed_buildings,
            "current_bucket": bucket_key,
        }

    def _run_current_bucket_once(self) -> None:
        self._ensure_dirs()
        current_bucket = self.current_hour_bucket()
        with self._lock:
            self._current_hour_bucket = current_bucket
        self._refresh_family_bucket(source_family=FAMILY_HANDOVER_LOG, bucket_key=current_bucket, fill_func=self.fill_handover_latest)
        self._refresh_family_bucket(
            source_family=FAMILY_HANDOVER_CAPACITY_REPORT,
            bucket_key=current_bucket,
            fill_func=self.fill_handover_capacity_latest,
        )
        self._refresh_family_bucket(source_family=FAMILY_MONTHLY_REPORT, bucket_key=current_bucket, fill_func=self.fill_monthly_latest)
        with self._lock:
            self._last_run_at = _now_text()
            handover_failed = list(self._family_status.get(FAMILY_HANDOVER_LOG, {}).get("failed_buildings", []) or [])
            handover_capacity_failed = list(
                self._family_status.get(FAMILY_HANDOVER_CAPACITY_REPORT, {}).get("failed_buildings", []) or []
            )
            monthly_failed = list(self._family_status.get(FAMILY_MONTHLY_REPORT, {}).get("failed_buildings", []) or [])
            if not handover_failed and not handover_capacity_failed and not monthly_failed:
                self._last_error = ""

    def _run_alarm_bucket_if_due(self, when: datetime | None = None) -> None:
        bucket_key = self._auto_alarm_bucket(when)
        if not bucket_key:
            return
        with self._lock:
            current_bucket = str(self._family_status.get(FAMILY_ALARM_EVENT, {}).get("current_bucket", "") or "").strip()
        if current_bucket == bucket_key:
            return
        self._ensure_dirs()
        self._refresh_family_bucket(
            source_family=FAMILY_ALARM_EVENT,
            bucket_key=bucket_key,
            fill_func=self.fill_alarm_event_latest,
        )
        with self._lock:
            self._last_run_at = _now_text()

    def _mark_current_hour_refresh(self, **fields: Any) -> None:
        with self._lock:
            self._current_hour_refresh.update(fields)

    def _run_current_hour_refresh_impl(self) -> None:
        self._ensure_dirs()
        bucket_key = self.current_hour_bucket()
        failed_units: List[str] = []
        blocked_units: List[str] = []
        running_units: List[str] = []
        completed_units: List[str] = []
        self._mark_current_hour_refresh(
            running=True,
            last_run_at=_now_text(),
            last_success_at="",
            last_error="",
            failed_buildings=[],
            blocked_buildings=[],
            running_buildings=[],
            completed_buildings=[],
        )
        self._emit(f"[共享缓存] 开始立即补下当前小时全部文件 bucket={bucket_key}")
        handover_result = self._refresh_family_bucket(
            source_family=FAMILY_HANDOVER_LOG,
            bucket_key=bucket_key,
            fill_func=self.fill_handover_latest,
            force_retry_failed=True,
        ) or {}
        handover_capacity_result = self._refresh_family_bucket(
            source_family=FAMILY_HANDOVER_CAPACITY_REPORT,
            bucket_key=bucket_key,
            fill_func=self.fill_handover_capacity_latest,
            force_retry_failed=True,
        ) or {}
        monthly_result = self._refresh_family_bucket(
            source_family=FAMILY_MONTHLY_REPORT,
            bucket_key=bucket_key,
            fill_func=self.fill_monthly_latest,
            force_retry_failed=True,
        ) or {}
        alarm_result: Dict[str, Any] = {
            "failed_buildings": [],
            "blocked_buildings": [],
            "running_buildings": [],
            "completed_buildings": [],
        }
        alarm_bucket_key = self.current_alarm_bucket()
        if alarm_bucket_key:
            alarm_result = self._refresh_family_bucket(
                source_family=FAMILY_ALARM_EVENT,
                bucket_key=alarm_bucket_key,
                fill_func=self.fill_alarm_event_latest,
                force_retry_failed=True,
            ) or {}
        for family_key, result in (
            (FAMILY_HANDOVER_LOG, handover_result),
            (FAMILY_HANDOVER_CAPACITY_REPORT, handover_capacity_result),
            (FAMILY_MONTHLY_REPORT, monthly_result),
            (FAMILY_ALARM_EVENT, alarm_result),
        ):
            for building in result.get("running_buildings", []) or []:
                running_units.append(f"{building}/{family_key}")
            for building in result.get("completed_buildings", []) or []:
                completed_units.append(f"{building}/{family_key}")
            for building in result.get("failed_buildings", []) or []:
                failed_units.append(f"{building}/{family_key}")
            for building in result.get("blocked_buildings", []) or []:
                blocked_units.append(f"{building}/{family_key}")
        success_at = _now_text() if not failed_units else ""
        last_error = self._last_error if (failed_units or blocked_units) else ""
        with self._lock:
            self._last_run_at = _now_text()
            if not failed_units and not blocked_units:
                self._last_error = ""
        self._mark_current_hour_refresh(
            running=False,
            last_success_at=success_at,
            last_error=last_error,
            failed_buildings=failed_units,
            blocked_buildings=blocked_units,
            running_buildings=running_units,
            completed_buildings=completed_units,
        )
        if failed_units:
            self._emit(f"[共享缓存] 当前小时立即补下结束：存在失败项 {', '.join(failed_units)}")
        elif blocked_units:
            self._emit(f"[共享缓存] 当前小时立即补下结束：存在等待恢复楼栋 {', '.join(blocked_units)}")
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

    def _resolve_latest_refresh_target(self, *, source_family: str) -> Dict[str, Any]:
        normalized_family = self._normalize_source_family(source_family)
        if normalized_family == FAMILY_HANDOVER_LOG:
            return {
                "source_family": normalized_family,
                "bucket_key": self.current_hour_bucket(),
                "fill_func": self.fill_handover_latest,
            }
        if normalized_family == FAMILY_HANDOVER_CAPACITY_REPORT:
            return {
                "source_family": normalized_family,
                "bucket_key": self.current_hour_bucket(),
                "fill_func": self.fill_handover_capacity_latest,
            }
        if normalized_family == FAMILY_MONTHLY_REPORT:
            return {
                "source_family": normalized_family,
                "bucket_key": self.current_hour_bucket(),
                "fill_func": self.fill_monthly_latest,
            }
        if normalized_family == FAMILY_ALARM_EVENT:
            bucket_key = self._auto_alarm_bucket()
            if not bucket_key:
                return {
                    "accepted": False,
                    "source_family": normalized_family,
                    "bucket_key": "",
                    "fill_func": None,
                    "reason": "bucket_unavailable",
                }
            return {
                "source_family": normalized_family,
                "bucket_key": bucket_key,
                "fill_func": self.fill_alarm_event_latest,
            }
        return {
            "accepted": False,
            "source_family": normalized_family,
            "bucket_key": "",
            "fill_func": None,
            "reason": "unsupported_family",
        }

    def _run_building_latest_refresh_background(
        self,
        *,
        source_family: str,
        building: str,
        bucket_key: str,
        fill_func: Callable[..., Any],
        thread_key: tuple[str, str, str, str],
    ) -> None:
        try:
            self._refresh_family_bucket(
                source_family=source_family,
                bucket_key=bucket_key,
                fill_func=fill_func,
                force_retry_failed=True,
                buildings=[building],
                force_refresh_existing=True,
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            with self._lock:
                self._last_error = error_text
                self._set_light_building_status_unlocked(
                    source_family=source_family,
                    building=building,
                    bucket_key=bucket_key,
                    payload={
                        "status": "failed",
                        "ready": False,
                        "downloaded_at": "",
                        "last_error": error_text,
                        "relative_path": self._failed_marker_relative_path(
                            source_family=source_family,
                            bucket_kind="latest",
                            bucket_key=bucket_key,
                            building=building,
                        ),
                        "resolved_file_path": "",
                        "started_at": "",
                        "blocked": False,
                        "blocked_reason": "",
                        "next_probe_at": "",
                    },
                )
                self._recompute_family_status_from_light_unlocked(
                    source_family=source_family,
                    bucket_key=bucket_key,
                )
            self._record_failed_entry(
                source_family=source_family,
                building=building,
                bucket_kind="latest",
                bucket_key=bucket_key,
                error_text=error_text,
                metadata={"family": source_family, "building": building},
            )
            self._emit(f"[共享缓存] 单楼 latest 拉取失败 family={source_family} building={building}: {exc}")
        finally:
            with self._lock:
                self._building_latest_refresh_threads.pop(thread_key, None)

    def start_building_latest_refresh(self, *, source_family: str, building: str) -> Dict[str, Any]:
        if not self.enabled or self.role_mode != "internal" or self.store is None:
            return {"accepted": False, "running": False, "reason": "disabled"}
        building_name = str(building or "").strip()
        if not building_name or building_name not in self.get_enabled_buildings():
            return {"accepted": False, "running": False, "reason": "invalid_building"}
        target = self._resolve_latest_refresh_target(source_family=source_family)
        normalized_family = str(target.get("source_family", "") or "").strip()
        bucket_key = str(target.get("bucket_key", "") or "").strip()
        fill_func = target.get("fill_func")
        if not fill_func:
            return {
                "accepted": False,
                "running": False,
                "reason": str(target.get("reason", "") or "unsupported_family").strip() or "unsupported_family",
                "scope": "single_building_family",
                "source_family": normalized_family,
                "building": building_name,
                "bucket_key": bucket_key,
            }
        thread_key = (normalized_family, building_name, "latest", bucket_key)
        with self._lock:
            active_started_at = str(self._active_latest_downloads.get(thread_key, "") or "").strip()
            active_thread = self._building_latest_refresh_threads.get(thread_key)
            if active_started_at or (active_thread and active_thread.is_alive()):
                return {
                    "accepted": False,
                    "running": True,
                    "reason": "already_running",
                    "scope": "single_building_family",
                    "source_family": normalized_family,
                    "building": building_name,
                    "bucket_key": bucket_key,
                }
            started_at = _now_text()
            self._ensure_light_family_cache_unlocked(
                source_family=normalized_family,
                bucket_key=bucket_key,
                buildings=[building_name],
            )
            self._set_light_building_status_unlocked(
                source_family=normalized_family,
                building=building_name,
                bucket_key=bucket_key,
                payload={
                    "status": "downloading",
                    "ready": False,
                    "downloaded_at": "",
                    "last_error": "",
                    "relative_path": "",
                    "resolved_file_path": "",
                    "started_at": started_at,
                    "blocked": False,
                    "blocked_reason": "",
                    "next_probe_at": "",
                },
            )
            self._recompute_family_status_from_light_unlocked(
                source_family=normalized_family,
                bucket_key=bucket_key,
            )
            thread = threading.Thread(
                target=self._run_building_latest_refresh_background,
                kwargs={
                    "source_family": normalized_family,
                    "building": building_name,
                    "bucket_key": bucket_key,
                    "fill_func": fill_func,
                    "thread_key": thread_key,
                },
                name=f"shared-source-cache-{normalized_family}-{building_name}",
                daemon=True,
            )
            self._building_latest_refresh_threads[thread_key] = thread
            thread.start()
        return {
            "accepted": True,
            "running": True,
            "reason": "started",
            "scope": "single_building_family",
            "source_family": normalized_family,
            "building": building_name,
            "bucket_key": bucket_key,
        }

    def _loop(self) -> None:
        startup_done = False
        while not self._stop_event.is_set():
            try:
                if not self.enabled or self.role_mode != "internal" or self.store is None:
                    self._stop_event.wait(self.check_interval_sec)
                    continue
                if self.run_on_startup and not startup_done:
                    self._run_current_bucket_once()
                    self._run_alarm_bucket_if_due()
                    startup_done = True
                else:
                    bucket = self.current_hour_bucket()
                    if bucket != self._current_hour_bucket:
                        self._run_current_bucket_once()
                    self._run_alarm_bucket_if_due()
                if startup_done:
                    handover_status = self._family_status.get(FAMILY_HANDOVER_LOG, {})
                    monthly_status = self._family_status.get(FAMILY_MONTHLY_REPORT, {})
                    alarm_status = self._family_status.get(FAMILY_ALARM_EVENT, {})
                    signature = "|".join(
                        [
                            self._current_hour_bucket,
                            ",".join(str(item) for item in handover_status.get("blocked_buildings", []) or []),
                            ",".join(str(item) for item in monthly_status.get("blocked_buildings", []) or []),
                            ",".join(str(item) for item in alarm_status.get("blocked_buildings", []) or []),
                            str(bool(self._last_error)),
                        ]
                    )
                    if signature != self._last_scheduler_log_signature:
                        self._last_scheduler_log_signature = signature
                        if handover_status.get("blocked_buildings") or monthly_status.get("blocked_buildings") or alarm_status.get("blocked_buildings"):
                            blocked = (
                                list(handover_status.get("blocked_buildings", []) or [])
                                + list(monthly_status.get("blocked_buildings", []) or [])
                                + list(alarm_status.get("blocked_buildings", []) or [])
                            )
                            self._emit(
                                f"[共享缓存] 调度等待楼栋恢复: 小时桶={self._current_hour_bucket}, "
                                f"告警桶={str(alarm_status.get('current_bucket', '') or '').strip() or '-'}, 楼栋={' / '.join(blocked)}"
                            )
                        elif not self._last_error:
                            self._emit(
                                f"[共享缓存] 小时预下载调度运行中: bucket={self._current_hour_bucket}, "
                                f"告警定时桶={str(alarm_status.get('current_bucket', '') or '').strip() or '-'}"
                            )
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                self._last_run_at = _now_text()
            self._stop_event.wait(self.check_interval_sec)
