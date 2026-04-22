from __future__ import annotations

import copy
import json
import re
import sqlite3
import socket
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.config.config_adapter import normalize_role_mode as normalize_deployment_role_mode, resolve_shared_bridge_paths
from app.modules.report_pipeline.service.monthly_bridge_service import (
    delete_bridge_resume_run,
    list_bridge_pending_resume_runs,
    resolve_monthly_bridge_resume_root,
    resolve_monthly_bridge_source_root,
    run_bridge_download_only_auto_once,
    run_bridge_download_only_multi_date,
    run_bridge_resume_upload,
)
from app.modules.report_pipeline.service.monthly_cache_continue_service import run_monthly_from_file_items
from app.modules.notify.service.webhook_notify_service import WebhookNotifyService
from app.modules.report_pipeline.service.orchestrator_service import OrchestratorService
from app.modules.shared_bridge.service.internal_download_browser_pool import (
    InternalDownloadBrowserPool,
)
from app.modules.shared_bridge.service.shared_bridge_mailbox_store import SharedBridgeMailboxStore
from app.modules.shared_bridge.service.shared_bridge_runtime_mirror_store import SharedBridgeRuntimeMirrorStore
from app.modules.shared_bridge.service.shared_source_cache_service import (
    FAMILY_ALARM_EVENT,
    FAMILY_HANDOVER_CAPACITY_REPORT,
    FAMILY_HANDOVER_LOG,
    FAMILY_LABELS,
    FAMILY_MONTHLY_REPORT,
    SharedSourceCacheService,
    is_accessible_cached_file_path,
)
from app.modules.report_pipeline.service.job_service import JobService
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.shared.runtime.internal_download_browser_pool_runtime import (
    clear_internal_download_browser_pool,
    set_internal_download_browser_pool,
)
from app.shared.utils.atomic_file import (
    atomic_copy_file,
    atomic_write_text,
    validate_excel_workbook_file,
    validate_json_file,
)
from app.shared.utils.file_utils import normalize_windows_path_text
from handover_log_module.service.day_metric_standalone_upload_service import DayMetricStandaloneUploadService
from handover_log_module.api.facade import load_handover_config
from handover_log_module.repository.excel_reader import load_rows
from handover_log_module.service.handover_download_service import HandoverDownloadService
from handover_log_module.service.wet_bulb_collection_service import WetBulbCollectionService

RETIRED_SHARED_BRIDGE_FEATURES: Dict[str, str] = {
    "alarm_export": "旧告警导出入口已退役，当前版本仅保留“内网 API 拉取 -> 共享 JSON -> 外网上传”主链",
}


def normalize_role_mode(value: Any) -> str:
    return normalize_deployment_role_mode(value)


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _role_label(value: Any) -> str:
    text = normalize_role_mode(value)
    if text == "internal":
        return "内网端"
    if text == "external":
        return "外网端"
    return str(value or "").strip() or "-"


def _default_node_id(role_mode: Any) -> str:
    role = normalize_role_mode(role_mode)
    machine_id = f"{uuid.getnode():012x}"
    if role not in {"internal", "external"}:
        role = "unselected"
    return f"{role}-{machine_id}"


_INTERNAL_ALERT_BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")


class SharedBridgeRuntimeService:
    INTERNAL_BROWSER_ALERT_FEATURE = "internal_browser_alert"
    INTERNAL_BROWSER_ALERT_QUIET_SEC = 600
    INTERNAL_BROWSER_ALERT_DEDUPE_SEC = 3600
    INTERNAL_ALERT_STATUS_REFRESH_INTERVAL_SEC = 30
    CLEANUP_INTERVAL_SEC = 600
    BACKGROUND_SELF_HEAL_INTERVAL_SEC = 7200
    BACKGROUND_SELF_HEAL_RECENT_HOURS = 3
    BACKGROUND_SELF_HEAL_IDLE_QUIET_SEC = 600
    BACKGROUND_SELF_HEAL_RESUME_SKIP_BEFORE_NEXT_SEC = 1200
    SOURCE_CACHE_PREWARM_INTERVAL_SEC = 3600
    BACKGROUND_SELF_HEAL_LOOKBACK_DAYS = 7
    BACKGROUND_SELF_HEAL_SCAN_LIMIT = 5000
    WAITING_JOB_RECONCILE_INTERVAL_SEC = 30
    MAILBOX_SUMMARY_REFRESH_INTERVAL_SEC = 15
    TASK_RETENTION_DAYS = 14
    NODE_RETENTION_DAYS = 2
    STORE_ERROR_LOG_INTERVAL_SEC = 60
    BACKGROUND_TASK_BUSY_RETRY_SEC = 30

    def __init__(
        self,
        *,
        runtime_config: Dict[str, Any],
        app_version: str,
        job_service: JobService | None = None,
        emit_log: Callable[[str], None] | None = None,
        request_runtime_status_refresh: Callable[[str], None] | None = None,
    ) -> None:
        self.runtime_config = copy.deepcopy(runtime_config if isinstance(runtime_config, dict) else {})
        self.app_version = str(app_version or "").strip()
        self.emit_log = emit_log
        self._job_service = job_service
        self._request_runtime_status_refresh_callback = request_runtime_status_refresh
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_poll_at = ""
        self._db_status = "disabled"
        self._last_error = ""
        self._counts = {"pending_internal": 0, "pending_external": 0, "problematic": 0, "total_count": 0, "node_count": 0}
        self._cached_task_list: List[Dict[str, Any]] = []
        self._cached_task_details: Dict[str, Dict[str, Any]] = {}
        self._cached_internal_alert_status: Dict[str, Any] = self._empty_internal_alert_status()
        self._cached_health_snapshot: Dict[str, Any] | None = None
        self._store_issue_log_markers: Dict[str, float] = {}
        self._store: SharedBridgeStore | None = None
        self._mailbox_store: SharedBridgeMailboxStore | None = None
        self._mirror_store: SharedBridgeRuntimeMirrorStore | None = None
        self._internal_download_pool: InternalDownloadBrowserPool | None = None
        self._source_cache_service: SharedSourceCacheService | None = None
        self._startup_logged = False
        self._last_cleanup_at = ""
        self._cleanup_deleted_tasks = 0
        self._cleanup_deleted_entries = 0
        self._cleanup_deleted_files = 0
        self._last_waiting_job_reconcile_monotonic = 0.0
        self._active_bridge_task_count = 0
        self._background_task_threads: Dict[str, threading.Thread] = {}
        self._background_task_state = self._build_initial_background_task_state()
        self._background_scan_sessions: Dict[str, Dict[str, Any]] = {}
        self._background_task_next_due_monotonic: Dict[str, float] = {}
        self._last_foreground_busy_monotonic = 0.0
        self._source_cache_start_thread: threading.Thread | None = None
        self._refresh_config()

    @classmethod
    def _background_task_title(cls, task_key: str) -> str:
        key = str(task_key or "").strip().lower()
        if key == "source_cache_sweep":
            return "共享缓存库后台扫描"
        if key == "source_cache_prewarm":
            return "共享缓存按钮预热"
        if key == "artifact_self_heal":
            return "共享桥接产物后台自愈"
        return key or "后台任务"

    @classmethod
    def _build_background_task_snapshot(cls, task_key: str) -> Dict[str, Any]:
        key = str(task_key or "").strip().lower() or "background_task"
        return {
            "task_key": key,
            "title": cls._background_task_title(key),
            "kind": "background",
            "running": False,
            "status": "idle",
            "last_started_at": "",
            "last_finished_at": "",
            "last_success_at": "",
            "last_error": "",
            "last_summary": "",
            "last_duration_ms": 0,
            "last_result": {},
            "next_run_at": "",
        }

    @classmethod
    def _build_initial_background_task_state(cls) -> Dict[str, Dict[str, Any]]:
        return {
            "source_cache_sweep": cls._build_background_task_snapshot("source_cache_sweep"),
            "source_cache_prewarm": cls._build_background_task_snapshot("source_cache_prewarm"),
            "artifact_self_heal": cls._build_background_task_snapshot("artifact_self_heal"),
        }

    @staticmethod
    def _resume_job_id_from_task(task: Dict[str, Any]) -> str:
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        return str(request.get("resume_job_id", "") or "").strip()

    def _resume_bound_job(
        self,
        task: Dict[str, Any],
        *,
        worker_payload: Dict[str, Any],
        summary: str,
    ) -> None:
        job_id = self._resume_job_id_from_task(task)
        if not job_id:
            return
        if self._job_service is None:
            raise RuntimeError("共享桥接缺少任务服务，无法恢复原任务")
        task_id = str(task.get("task_id", "") or "").strip()
        started = time.perf_counter()
        self._emit_system_log(f"[共享桥接] 任务={task_id or '-'} 开始唤醒 waiting job: job={job_id}")
        resumed = self._job_service.resume_waiting_worker_job(
            job_id,
            worker_payload=worker_payload,
            summary=summary,
        )
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        self._emit_system_log(
            f"[共享桥接] 任务={task_id or '-'} 唤醒 waiting job 调用已返回: "
            f"job={job_id}, status={getattr(resumed, 'status', '-')}, elapsed_ms={elapsed_ms}"
        )

    def _fail_bound_job(self, task: Dict[str, Any], *, error_text: str, summary: str = "") -> None:
        job_id = self._resume_job_id_from_task(task)
        if not job_id or self._job_service is None:
            return
        self._job_service.fail_waiting_job(job_id, error_text=error_text, summary=summary)

    @staticmethod
    def _normalize_text_list(values: Any) -> List[str]:
        if not isinstance(values, list):
            return []
        return [str(item or "").strip() for item in values if str(item or "").strip()]

    @staticmethod
    def _parse_missing_day_metric_handover_units(
        error_text: str,
        *,
        selected_dates: List[str],
        target_buildings: List[str],
    ) -> Dict[str, List[str]]:
        text = str(error_text or "").strip()
        if ":" in text:
            text = text.split(":", 1)[1].strip()
        output: Dict[str, List[str]] = {}
        for building, duty_date in re.findall(r"(?:^|,\s*)([^,()]+)\((\d{4}-\d{2}-\d{2})\)", text):
            building_text = str(building or "").strip()
            duty_date_text = str(duty_date or "").strip()
            if not building_text or not duty_date_text:
                continue
            output.setdefault(duty_date_text, [])
            if building_text not in output[duty_date_text]:
                output[duty_date_text].append(building_text)
        if output:
            return output
        normalized_dates = [str(item or "").strip() for item in (selected_dates or []) if str(item or "").strip()]
        normalized_buildings = [str(item or "").strip() for item in (target_buildings or []) if str(item or "").strip()]
        return {
            duty_date: list(normalized_buildings)
            for duty_date in normalized_dates
            if duty_date
        }

    def _require_accessible_cached_file(self, file_path: Any, *, description: str) -> str:
        file_text = str(file_path or "").strip()
        if not file_text or not is_accessible_cached_file_path(file_text):
            raise FileNotFoundError(f"{description}不存在或不可访问: {file_text or '-'}")
        return file_text

    def _build_handover_resume_binding_from_artifacts(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接存储未初始化")
        task_id = str(task.get("task_id", "") or "").strip()
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        building_files: List[Dict[str, str]] = []
        for item in internal_result.get("handover_files", []) if isinstance(internal_result.get("handover_files", []), list) else []:
            if not isinstance(item, dict):
                continue
            building = str(item.get("building", "") or "").strip()
            file_path = self._require_accessible_cached_file(
                item.get("file_path", ""),
                description="交接班共享源文件",
            ) if building else ""
            if not building or not file_path:
                continue
            building_files.append({"building": building, "file_path": file_path})
        if not building_files:
            artifacts = self._store.get_artifacts(task_id, artifact_kind="source_file", status="ready")
            for item in artifacts:
                relative_path = str(item.get("relative_path", "") or "").strip()
                building = str(item.get("building", "") or "").strip()
                if not relative_path or not building:
                    continue
                file_path = self._resolve_ready_artifact_file_path(item)
                if file_path is None:
                    raise RuntimeError(f"共享目录中没有可继续处理的交接班源文件: {relative_path}")
                building_files.append({"building": building, "file_path": str(file_path)})
        capacity_items: List[Dict[str, str]] = []
        for item in internal_result.get("capacity_files", []) if isinstance(internal_result.get("capacity_files", []), list) else []:
            if not isinstance(item, dict):
                continue
            building = str(item.get("building", "") or "").strip()
            file_path = self._require_accessible_cached_file(
                item.get("file_path", ""),
                description="交接班容量共享源文件",
            ) if building else ""
            if not building or not file_path:
                continue
            capacity_items.append({"building": building, "file_path": file_path})
        if not capacity_items:
            capacity_artifacts = self._store.get_artifacts(task_id, artifact_kind="capacity_source_file", status="ready")
            for item in capacity_artifacts:
                relative_path = str(item.get("relative_path", "") or "").strip()
                building = str(item.get("building", "") or "").strip()
                if not relative_path or not building:
                    continue
                file_path = self._resolve_ready_artifact_file_path(item)
                if file_path is None:
                    raise RuntimeError(f"共享目录中没有完整可继续处理的交接班容量源文件: {relative_path}")
                capacity_items.append({"building": building, "file_path": str(file_path)})
        if not building_files:
            raise RuntimeError("共享目录中没有可继续处理的交接班源文件")
        handover_buildings = {item["building"] for item in building_files if item.get("building")}
        capacity_map = {item["building"]: item["file_path"] for item in capacity_items if item.get("building") and item.get("file_path")}
        if not handover_buildings or not handover_buildings.issubset(set(capacity_map)):
            raise RuntimeError("共享目录中没有完整可继续处理的交接班容量源文件")
        resolved_capacity_items = [
            {"building": item["building"], "file_path": capacity_map[item["building"]]}
            for item in building_files
            if item["building"] in capacity_map
        ]
        return {
            "worker_payload": {
                "resume_kind": "shared_bridge_handover",
                "building_files": building_files,
                "capacity_building_files": resolved_capacity_items,
                "end_time": str(request.get("end_time", "") or "").strip() or None,
                "duty_date": str(request.get("duty_date", "") or "").strip() or None,
                "duty_shift": str(request.get("duty_shift", "") or "").strip() or None,
                "bridge_task_id": task_id,
            },
            "summary": "共享文件已到位，正在继续生成交接班日志",
            "log_text": f"[共享桥接] 任务={task_id} 已完成文件准备，正在自动恢复交接班原任务",
        }

    def _build_day_metric_resume_binding_from_artifacts(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接存储未初始化")
        task_id = str(task.get("task_id", "") or "").strip()
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        artifacts = self._store.get_artifacts(task_id, artifact_kind="source_file", status="ready")
        source_units: List[Dict[str, str]] = []
        for item in artifacts:
            relative_path = str(item.get("relative_path", "") or "").strip()
            building = str(item.get("building", "") or "").strip()
            metadata = item.get("metadata", {}) if isinstance(item.get("metadata", {}), dict) else {}
            duty_date = str(metadata.get("duty_date", "") or "").strip()
            if not relative_path or not building or not duty_date:
                continue
            file_path = self._resolve_ready_artifact_file_path(item)
            if file_path is None:
                raise FileNotFoundError(f"共享目录中的12项源文件不存在或不可访问: {relative_path}")
            source_units.append({"duty_date": duty_date, "building": building, "source_file": str(file_path)})
        if not source_units:
            raise RuntimeError("共享目录中没有可继续上传的12项源文件")
        selected_dates = self._normalize_text_list(internal_result.get("selected_dates", []))
        buildings = self._normalize_text_list(internal_result.get("selected_buildings", []))
        if not selected_dates:
            selected_dates = self._normalize_text_list(request.get("selected_dates", []))
        if not buildings:
            buildings = sorted({str(item.get("building", "") or "").strip() for item in source_units if str(item.get("building", "") or "").strip()})
        return {
            "worker_payload": {
                "resume_kind": "shared_bridge_day_metric",
                "selected_dates": selected_dates,
                "buildings": buildings,
                "source_units": source_units,
                "building_scope": str(request.get("building_scope", "") or "").strip() or "all_enabled",
                "building": str(request.get("building", "") or "").strip() or None,
                "bridge_task_id": task_id,
            },
            "summary": "共享文件已到位，正在继续上传12项数据",
            "log_text": f"[共享桥接] 任务={task_id} 已完成文件准备，正在自动恢复12项原任务",
        }

    def _build_wet_bulb_resume_binding_from_artifacts(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接存储未初始化")
        task_id = str(task.get("task_id", "") or "").strip()
        artifacts = self._store.get_artifacts(task_id, artifact_kind="source_file", status="ready")
        source_units: List[Dict[str, str]] = []
        for item in artifacts:
            relative_path = str(item.get("relative_path", "") or "").strip()
            building = str(item.get("building", "") or "").strip()
            if not relative_path or not building:
                continue
            file_path = self._resolve_ready_artifact_file_path(item)
            if file_path is None:
                raise FileNotFoundError(f"共享目录中的湿球温度源文件不存在或不可访问: {relative_path}")
            source_units.append({"building": building, "file_path": str(file_path)})
        if not source_units:
            raise RuntimeError("共享目录中没有可继续处理的湿球温度源文件")
        return {
            "worker_payload": {
                "resume_kind": "shared_bridge_wet_bulb",
                "source_units": source_units,
                "bridge_task_id": task_id,
            },
            "summary": "共享文件已到位，正在继续处理湿球温度",
            "log_text": f"[共享桥接] 任务={task_id} 已完成文件准备，正在自动恢复湿球温度原任务",
        }

    def _build_handover_history_resume_binding(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if self._source_cache_service is None:
            raise RuntimeError("共享源缓存服务未初始化")
        task_id = str(task.get("task_id", "") or "").strip()
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        buildings = self._normalize_text_list(request.get("buildings", []))
        if not buildings:
            buildings = self._source_cache_service.get_enabled_buildings()
        duty_date = str(request.get("duty_date", "") or "").strip()
        duty_shift = str(request.get("duty_shift", "") or "").strip().lower()
        cached_entries = self._source_cache_service.get_handover_by_date_entries(
            duty_date=duty_date,
            duty_shift=duty_shift,
            buildings=buildings,
        )
        capacity_entries = self._source_cache_service.get_handover_capacity_by_date_entries(
            duty_date=duty_date,
            duty_shift=duty_shift,
            buildings=buildings,
        )
        if len(cached_entries) < len(buildings) or len(capacity_entries) < len(buildings):
            raise RuntimeError(f"交接班历史缓存未齐全: duty_date={duty_date or '-'}, duty_shift={duty_shift or '-'}")
        building_files = [
            {
                "building": str(item.get("building", "") or "").strip(),
                "file_path": self._require_accessible_cached_file(
                    item.get("file_path", ""),
                    description="交接班历史共享源文件",
                ),
            }
            for item in cached_entries
            if str(item.get("building", "") or "").strip()
        ]
        capacity_items = [
            {
                "building": str(item.get("building", "") or "").strip(),
                "file_path": self._require_accessible_cached_file(
                    item.get("file_path", ""),
                    description="交接班历史容量共享源文件",
                ),
            }
            for item in capacity_entries
            if str(item.get("building", "") or "").strip()
        ]
        return {
            "worker_payload": {
                "resume_kind": "shared_bridge_handover",
                "building_files": building_files,
                "capacity_building_files": capacity_items,
                "end_time": None,
                "duty_date": duty_date or None,
                "duty_shift": duty_shift or None,
                "bridge_task_id": task_id,
            },
            "summary": "共享文件已到位，正在继续生成交接班日志",
            "log_text": f"[共享桥接] 任务={task_id} 历史交接班共享文件已齐全，正在自动恢复原任务",
        }

    def _build_day_metric_history_resume_binding(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if self._source_cache_service is None:
            raise RuntimeError("共享源缓存服务未初始化")
        task_id = str(task.get("task_id", "") or "").strip()
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        selected_dates = self._normalize_text_list(request.get("selected_dates", []))
        building_scope = str(request.get("building_scope", "") or "").strip() or "all_enabled"
        target_buildings = [str(request.get("building", "") or "").strip()] if building_scope == "single" else self._source_cache_service.get_enabled_buildings()
        target_buildings = [item for item in target_buildings if item]
        cached_entries = self._source_cache_service.get_day_metric_by_date_entries(
            selected_dates=selected_dates,
            buildings=target_buildings,
        )
        if len(cached_entries) < len(selected_dates) * max(1, len(target_buildings)):
            raise RuntimeError(f"12项历史缓存未齐全: dates={','.join(selected_dates) or '-'}")
        source_units = [
            {
                "duty_date": str(item.get("duty_date", "") or "").strip(),
                "building": str(item.get("building", "") or "").strip(),
                "source_file": self._require_accessible_cached_file(
                    item.get("file_path", ""),
                    description="12项历史共享源文件",
                ),
            }
            for item in cached_entries
            if str(item.get("building", "") or "").strip() and str(item.get("duty_date", "") or "").strip()
        ]
        return {
            "worker_payload": {
                "resume_kind": "shared_bridge_day_metric",
                "selected_dates": selected_dates,
                "buildings": target_buildings,
                "source_units": source_units,
                "building_scope": building_scope,
                "building": str(request.get("building", "") or "").strip() or None,
                "bridge_task_id": task_id,
            },
            "summary": "共享文件已到位，正在继续上传12项数据",
            "log_text": f"[共享桥接] 任务={task_id} 历史12项共享文件已齐全，正在自动恢复原任务",
        }

    def _build_monthly_cache_fill_resume_binding(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if self._source_cache_service is None:
            raise RuntimeError("共享源缓存服务未初始化")
        task_id = str(task.get("task_id", "") or "").strip()
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        selected_dates = self._normalize_text_list(request.get("selected_dates", []))
        cached_entries = self._source_cache_service.get_monthly_by_date_entries(selected_dates=selected_dates)
        expected = len(selected_dates) * len(self._source_cache_service.get_enabled_buildings())
        if len(cached_entries) < expected:
            raise RuntimeError(f"月报历史缓存未齐全: dates={','.join(selected_dates) or '-'}")
        file_items = [
            {
                "building": str(item.get("building", "") or "").strip(),
                "file_path": self._require_accessible_cached_file(
                    item.get("file_path", ""),
                    description="月报历史共享源文件",
                ),
                "upload_date": str(item.get("metadata", {}).get("upload_date", "") or item.get("duty_date", "") or "").strip(),
            }
            for item in cached_entries
            if str(item.get("building", "") or "").strip()
        ]
        return {
            "worker_payload": {
                "resume_kind": "shared_bridge_monthly_multi_date",
                "selected_dates": selected_dates,
                "file_items": file_items,
                "source_label": "月报历史共享文件",
                "bridge_task_id": task_id,
            },
            "summary": "共享文件已到位，正在继续处理多日期月报",
            "log_text": f"[共享桥接] 任务={task_id} 月报历史共享文件已齐全，正在自动恢复原任务",
        }

    def _build_monthly_pipeline_resume_binding(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_id = str(task.get("task_id", "") or "").strip()
        mode = str(task.get("mode", "") or "").strip().lower()
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        if mode == "resume_upload":
            return {
                "worker_payload": {
                    "run_id": str(request.get("run_id", "") or "").strip() or None,
                    "auto_trigger": bool(request.get("auto_trigger", False)),
                    "bridge_task_id": task_id,
                },
                "summary": "共享续传状态已就绪，正在继续断点续传",
                "log_text": f"[共享桥接] 任务={task_id} 续传状态已就绪，正在自动恢复原任务",
            }
        source_root = str(internal_result.get("source_root", "") or "").strip()
        if mode == "auto_once" and self._source_cache_service is not None:
            target_bucket_key = (
                str(request.get("target_bucket_key", "") or "").strip()
                or str(internal_result.get("target_bucket_key", "") or "").strip()
                or self.current_source_cache_bucket()
            )
            target_buildings = [
                str(item or "").strip()
                for item in self._source_cache_service.get_enabled_buildings()
                if str(item or "").strip()
            ]
            cached_entries = self._source_cache_service.get_latest_ready_entries(
                source_family=FAMILY_MONTHLY_REPORT,
                buildings=target_buildings,
                bucket_key=target_bucket_key,
            )
            if target_buildings and len(cached_entries) >= len(target_buildings):
                file_items = [
                    {
                        "building": str(item.get("building", "") or "").strip(),
                        "file_path": self._require_accessible_cached_file(
                            item.get("file_path", ""),
                            description="月报共享源文件",
                        ),
                        "upload_date": str(
                            item.get("metadata", {}).get("upload_date", "")
                            or item.get("duty_date", "")
                            or ""
                        ).strip(),
                    }
                    for item in cached_entries
                    if str(item.get("building", "") or "").strip()
                ]
                if file_items:
                    return {
                        "worker_payload": {
                            "resume_kind": "shared_bridge_monthly_auto_once",
                            "file_items": file_items,
                            "source": str(request.get("source", "") or "共享桥接月报自动流程").strip() or "共享桥接月报自动流程",
                            "bridge_task_id": task_id,
                        },
                        "summary": "共享文件已到位，正在继续处理月报自动流程",
                        "log_text": f"[共享桥接] 任务={task_id} 月报 canonical 共享文件已齐全，正在自动恢复原任务",
                    }
            if not source_root:
                raise RuntimeError("月报共享桥接缺少可继续处理的 canonical 源文件")
        if not source_root:
            raise RuntimeError("月报共享桥接缺少 source_root")
        file_items = []
        source_root_path = Path(source_root)
        for file_path in sorted(source_root_path.rglob("*.xlsx")):
            if not file_path.is_file():
                continue
            if not is_accessible_cached_file_path(file_path):
                raise FileNotFoundError(f"月报共享源文件不存在或不可访问: {file_path}")
            building = str(file_path.stem.split("--")[-1] if "--" in file_path.stem else "").strip()
            upload_date = str(file_path.parent.name.split("--")[0] if "--" in file_path.parent.name else "").strip()
            file_items.append({"building": building, "file_path": str(file_path), "upload_date": upload_date})
        if not file_items:
            raise RuntimeError("月报共享桥接未找到可继续处理的共享源文件")
        return {
            "worker_payload": {
                "resume_kind": "shared_bridge_monthly_auto_once",
                "file_items": file_items,
                "source": str(request.get("source", "") or "共享桥接月报自动流程").strip() or "共享桥接月报自动流程",
                "bridge_task_id": task_id,
            },
            "summary": "共享文件已到位，正在继续处理月报自动流程",
            "log_text": f"[共享桥接] 任务={task_id} 月报共享文件已齐全，正在自动恢复原任务",
        }

    def _build_alarm_event_upload_resume_binding(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if self._source_cache_service is None:
            raise RuntimeError("共享源缓存服务未初始化")
        task_id = str(task.get("task_id", "") or "").strip()
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        mode = str(task.get("mode", "") or request.get("mode", "") or "").strip().lower() or "full"
        building = str(request.get("building", "") or "").strip()
        selection = self._source_cache_service.get_alarm_event_upload_selection(
            building=building if mode == "single_building" else "",
        )
        target_buildings = [building] if mode == "single_building" and building else self._source_cache_service.get_enabled_buildings()
        if not target_buildings:
            target_buildings = [
                str(item.get("building", "") or "").strip()
                for item in (selection.get("selected_entries", []) if isinstance(selection.get("selected_entries", []), list) else [])
                if isinstance(item, dict) and str(item.get("building", "") or "").strip()
            ]
        ready_buildings = {
            str(item.get("building", "") or "").strip()
            for item in (selection.get("selected_entries", []) if isinstance(selection.get("selected_entries", []), list) else [])
            if isinstance(item, dict) and str(item.get("building", "") or "").strip()
        }
        missing_buildings = [item for item in target_buildings if item and item not in ready_buildings]
        if missing_buildings:
            raise RuntimeError(f"告警共享源文件未齐全: {','.join(missing_buildings)}")
        return {
            "worker_payload": {
                "resume_kind": "shared_bridge_alarm_event_upload",
                "mode": mode,
                "building": building or None,
                "bridge_task_id": task_id,
            },
            "summary": "共享文件已到位，正在继续上传告警信息",
            "log_text": f"[共享桥接] 任务={task_id} 告警共享文件已齐全，正在自动恢复原任务",
        }

    def _build_waiting_job_resume_binding(self, task: Dict[str, Any]) -> Dict[str, Any]:
        feature = str(task.get("feature", "") or "").strip().lower()
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        if feature == "handover_from_download":
            return self._build_handover_resume_binding_from_artifacts(task)
        if feature == "day_metric_from_download":
            return self._build_day_metric_resume_binding_from_artifacts(task)
        if feature == "wet_bulb_collection":
            return self._build_wet_bulb_resume_binding_from_artifacts(task)
        if feature == "alarm_event_upload":
            return self._build_alarm_event_upload_resume_binding(task)
        if feature == "handover_cache_fill":
            continuation_kind = str(request.get("continuation_kind", "") or "").strip().lower()
            if continuation_kind == "handover":
                return self._build_handover_history_resume_binding(task)
            if continuation_kind == "day_metric":
                return self._build_day_metric_history_resume_binding(task)
            raise RuntimeError(f"不支持的共享缓存补采类型: {continuation_kind or '-'}")
        if feature == "monthly_cache_fill":
            return self._build_monthly_cache_fill_resume_binding(task)
        if feature == "monthly_report_pipeline":
            return self._build_monthly_pipeline_resume_binding(task)
        raise RuntimeError(f"不支持的等待任务恢复类型: {feature or '-'}")

    def _refresh_config(self) -> None:
        deployment = self.runtime_config.get("deployment", {})
        bridge_cfg = self.runtime_config.get("shared_bridge", {})
        if not isinstance(deployment, dict):
            deployment = {}
        if not isinstance(bridge_cfg, dict):
            bridge_cfg = {}
        resolved_bridge_cfg = resolve_shared_bridge_paths(bridge_cfg, deployment.get("role_mode"))
        resolved_bridge_cfg["root_dir"] = normalize_windows_path_text(
            str(resolved_bridge_cfg.get("root_dir", "") or "").strip()
        )
        if isinstance(self.runtime_config, dict):
            self.runtime_config["shared_bridge"] = copy.deepcopy(resolved_bridge_cfg)
        self.role_mode = normalize_role_mode(deployment.get("role_mode"))
        self.node_id = str(deployment.get("node_id", "") or "").strip() or _default_node_id(self.role_mode)
        self.node_label = _role_label(self.role_mode)
        self.shared_bridge_enabled = bool(resolved_bridge_cfg.get("enabled", False))
        self.shared_bridge_root = str(resolved_bridge_cfg.get("root_dir", "") or "").strip()
        self.poll_interval_sec = max(1, int(resolved_bridge_cfg.get("poll_interval_sec", 2) or 2))
        self.heartbeat_interval_sec = max(1, int(resolved_bridge_cfg.get("heartbeat_interval_sec", 5) or 5))
        self.claim_lease_sec = max(5, int(resolved_bridge_cfg.get("claim_lease_sec", 30) or 30))
        self.stale_task_timeout_sec = max(60, int(resolved_bridge_cfg.get("stale_task_timeout_sec", 1800) or 1800))
        self.artifact_retention_days = max(1, int(resolved_bridge_cfg.get("artifact_retention_days", 7) or 7))
        self.sqlite_busy_timeout_ms = max(1000, int(resolved_bridge_cfg.get("sqlite_busy_timeout_ms", 15000) or 15000))
        self._store = (
            SharedBridgeStore(self.shared_bridge_root, busy_timeout_ms=self.sqlite_busy_timeout_ms)
            if self.shared_bridge_root
            else None
        )
        self._mailbox_store = SharedBridgeMailboxStore(self.shared_bridge_root) if self.shared_bridge_root else None
        self._mirror_store = SharedBridgeRuntimeMirrorStore(
            runtime_config=self.runtime_config,
            role_mode=self.role_mode or "external",
        )
        if self._mirror_store is not None:
            cached_alert_status = self._mirror_store.get_snapshot(key="internal_alert_status")
            if isinstance(cached_alert_status, dict):
                payload = cached_alert_status.get("payload", cached_alert_status)
                if isinstance(payload, dict) and payload:
                    self._cached_internal_alert_status = copy.deepcopy(payload)
        if self._source_cache_service is None:
            self._source_cache_service = SharedSourceCacheService(
                runtime_config=self.runtime_config,
                store=self._store,
                download_browser_pool=self._internal_download_pool,
                emit_log=self._emit_system_log,
            )
        else:
            self._source_cache_service.store = self._store
            self._source_cache_service.update_runtime_config(self.runtime_config)

    def update_runtime_config(self, runtime_config: Dict[str, Any]) -> None:
        self.runtime_config = copy.deepcopy(runtime_config if isinstance(runtime_config, dict) else {})
        self._refresh_config()

    def get_deployment_snapshot(self) -> Dict[str, Any]:
        return {
            "role_mode": self.role_mode,
            "node_id": self.node_id,
            "node_label": self.node_label,
        }

    def _shared_bridge_db_path_text(self) -> str:
        if self._store is not None:
            store_db_path = getattr(self._store, "db_path", None)
            if store_db_path:
                return str(store_db_path)
        root_text = str(self.shared_bridge_root or "").strip()
        if not root_text:
            return ""
        return str(Path(root_text) / "bridge.db")

    @staticmethod
    def _store_error_state(exc: Exception) -> str:
        text = str(exc or "").strip().lower()
        if any(token in text for token in ("database is locked", "database table is locked", "database is busy", "busy")):
            return "busy"
        return "unavailable"

    def _emit_store_issue_log(self, scope: str, exc: Exception) -> None:
        error_text = str(exc or "").strip()
        status = self._store_error_state(exc)
        marker = f"{scope}|{status}|{type(exc).__name__}|{error_text.lower()}"
        now_monotonic = time.monotonic()
        previous = float(self._store_issue_log_markers.get(marker, 0.0) or 0.0)
        if previous and now_monotonic - previous < self.STORE_ERROR_LOG_INTERVAL_SEC:
            return
        self._store_issue_log_markers[marker] = now_monotonic
        self._emit_system_log(
            "[共享桥接] 数据库降级: "
            f"scope={scope}, status={status}, role={_role_label(self.role_mode)}, "
            f"root={self.shared_bridge_root or '-'}, db={self._shared_bridge_db_path_text() or '-'}, "
            f"error={type(exc).__name__}: {error_text or '-'}"
        )

    def _mark_store_read_degraded(
        self,
        *,
        scope: str,
        exc: Exception,
        busy_message: str,
        unavailable_message: str,
    ) -> str:
        status = self._store_error_state(exc)
        self._db_status = status
        self._last_error = busy_message if status == "busy" else unavailable_message
        self._last_poll_at = _now_text()
        self._emit_store_issue_log(scope, exc)
        return status

    def _run_internal_source_cache_index_startup_cleanup(self) -> None:
        if self.role_mode != "internal" or self._store is None:
            return
        try:
            result = self._store.cleanup_source_cache_indexes_once()
        except Exception as exc:  # noqa: BLE001
            self._emit_system_log(f"[共享缓存] 启动索引清理失败，已跳过本次清理: {exc}")
            return
        if bool(result.get("skipped", False)):
            return
        self._emit_system_log(
            "[共享缓存] 启动索引清理完成: "
            f"扫描DB={int(result.get('db_scanned', 0) or 0)}, "
            f"删除无效DB索引={int(result.get('db_deleted_invalid', 0) or 0)}, "
            f"扫描索引文件={int(result.get('index_scanned', 0) or 0)}, "
            f"删除损坏索引={int(result.get('index_deleted_invalid_json', 0) or 0)}, "
            f"删除孤儿索引={int(result.get('index_deleted_orphan', 0) or 0)}, "
            f"同步索引={int(result.get('index_synced_from_db', 0) or 0)}"
        )

    def _cache_task_list(self, tasks: List[Dict[str, Any]]) -> None:
        self._cached_task_list = copy.deepcopy(tasks if isinstance(tasks, list) else [])
        if self._mirror_store is not None:
            for task in self._cached_task_list:
                if isinstance(task, dict):
                    self._mirror_store.upsert_task(task)

    def _cache_task_detail(self, task: Dict[str, Any] | None) -> None:
        task_payload = task if isinstance(task, dict) else None
        task_id = str(task_payload.get("task_id", "") if task_payload else "").strip()
        if not task_id:
            return
        self._cached_task_details[task_id] = copy.deepcopy(task_payload)
        if self._mirror_store is not None:
            self._mirror_store.upsert_task(task_payload)

    def _list_mailbox_tasks(self, *, limit: int) -> List[Dict[str, Any]]:
        if self._mailbox_store is None:
            return []
        try:
            return self._mailbox_store.list_tasks(limit=limit)
        except Exception:
            return []

    def _get_mailbox_task(self, task_id: str) -> Dict[str, Any] | None:
        if self._mailbox_store is None:
            return None
        try:
            return self._mailbox_store.load_task(task_id)
        except Exception:
            return None

    @staticmethod
    def _task_counts_from_tasks(tasks: List[Dict[str, Any]]) -> Dict[str, int]:
        pending_internal = 0
        pending_external = 0
        problematic = 0
        for task in tasks if isinstance(tasks, list) else []:
            if not isinstance(task, dict):
                continue
            status = str(task.get("status", "") or "").strip().lower()
            if status in {"queued_for_internal", "internal_claimed", "internal_running"}:
                pending_internal += 1
            elif status in {"ready_for_external", "external_claimed", "external_running"}:
                pending_external += 1
            elif status in {"failed", "partial_failed", "stale"}:
                problematic += 1
        return {
            "pending_internal": pending_internal,
            "pending_external": pending_external,
            "problematic": problematic,
            "total_count": len(tasks if isinstance(tasks, list) else []),
        }

    def _snapshot_background_tasks(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                copy.deepcopy(self._background_task_state.get("source_cache_sweep", self._build_background_task_snapshot("source_cache_sweep"))),
                copy.deepcopy(self._background_task_state.get("source_cache_prewarm", self._build_background_task_snapshot("source_cache_prewarm"))),
                copy.deepcopy(self._background_task_state.get("artifact_self_heal", self._build_background_task_snapshot("artifact_self_heal"))),
            ]

    def _set_background_task_next_run(self, task_key: str, *, when_monotonic: float) -> None:
        next_run_at = ""
        delay_sec = max(0.0, float(when_monotonic or 0.0) - time.monotonic())
        if delay_sec > 0:
            next_run_at = (datetime.now() + timedelta(seconds=delay_sec)).strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            state = self._background_task_state.setdefault(
                str(task_key or "").strip().lower() or "background_task",
                self._build_background_task_snapshot(task_key),
            )
            state["next_run_at"] = next_run_at

    def _mark_background_task_started(self, task_key: str) -> float:
        started_at = time.monotonic()
        started_text = _now_text()
        with self._lock:
            state = self._background_task_state.setdefault(
                str(task_key or "").strip().lower() or "background_task",
                self._build_background_task_snapshot(task_key),
            )
            state.update(
                {
                    "running": True,
                    "status": "running",
                    "last_started_at": started_text,
                    "last_error": "",
                    "last_summary": "",
                    "last_duration_ms": 0,
                    "last_result": {},
                    "next_run_at": "",
                }
            )
        return started_at

    def _mark_background_task_finished(
        self,
        task_key: str,
        *,
        started_monotonic: float,
        status: str,
        summary: str,
        result: Dict[str, Any] | None = None,
        error_text: str = "",
    ) -> None:
        finished_text = _now_text()
        duration_ms = max(0, int((time.monotonic() - float(started_monotonic or 0.0)) * 1000))
        normalized_status = str(status or "").strip().lower() or "success"
        with self._lock:
            state = self._background_task_state.setdefault(
                str(task_key or "").strip().lower() or "background_task",
                self._build_background_task_snapshot(task_key),
            )
            state.update(
                {
                    "running": False,
                    "status": normalized_status,
                    "last_finished_at": finished_text,
                    "last_duration_ms": duration_ms,
                    "last_summary": str(summary or "").strip(),
                    "last_error": str(error_text or "").strip(),
                    "last_result": copy.deepcopy(result if isinstance(result, dict) else {}),
                }
            )
            if normalized_status == "success":
                state["last_success_at"] = finished_text

    def _mark_background_task_deferred(self, task_key: str, *, reason: str, next_monotonic: float) -> None:
        self._set_background_task_next_run(task_key, when_monotonic=next_monotonic)
        with self._lock:
            state = self._background_task_state.setdefault(
                str(task_key or "").strip().lower() or "background_task",
                self._build_background_task_snapshot(task_key),
            )
            if not bool(state.get("running", False)):
                state["status"] = "deferred"
                state["last_summary"] = str(reason or "").strip() or "后台任务已延后"
                state["last_error"] = ""

    def _business_task_started(self) -> None:
        with self._lock:
            self._active_bridge_task_count += 1

    def _business_task_finished(self) -> None:
        with self._lock:
            self._active_bridge_task_count = max(0, int(self._active_bridge_task_count or 0) - 1)

    def _has_active_business_task(self) -> bool:
        with self._lock:
            return bool(int(self._active_bridge_task_count or 0) > 0)

    def _has_active_foreground_work(self) -> bool:
        busy = self._has_active_business_task()
        if self._job_service is not None and hasattr(self._job_service, "has_incomplete_jobs"):
            try:
                busy = bool(busy or self._job_service.has_incomplete_jobs())
            except Exception:
                busy = bool(busy)
        if busy:
            with self._lock:
                self._last_foreground_busy_monotonic = time.monotonic()
        return bool(busy)

    def _background_idle_quiet_due(self, now_monotonic: float) -> float:
        with self._lock:
            last_busy = float(self._last_foreground_busy_monotonic or 0.0)
        if last_busy <= 0:
            return 0.0
        quiet_due = last_busy + max(0, int(self.BACKGROUND_SELF_HEAL_IDLE_QUIET_SEC))
        return quiet_due if now_monotonic < quiet_due else 0.0

    @staticmethod
    def _normalize_background_task_key(task_key: str) -> str:
        return str(task_key or "").strip().lower() or "background_task"

    def _get_background_scan_session(self, task_key: str) -> Dict[str, Any] | None:
        normalized_key = self._normalize_background_task_key(task_key)
        with self._lock:
            session = self._background_scan_sessions.get(normalized_key)
            return copy.deepcopy(session) if isinstance(session, dict) else None

    def _put_background_scan_session(self, task_key: str, session: Dict[str, Any]) -> None:
        normalized_key = self._normalize_background_task_key(task_key)
        with self._lock:
            self._background_scan_sessions[normalized_key] = copy.deepcopy(session)

    def _clear_background_scan_session(self, task_key: str) -> None:
        normalized_key = self._normalize_background_task_key(task_key)
        with self._lock:
            self._background_scan_sessions.pop(normalized_key, None)

    def _background_scan_counts_from_session(self, session: Dict[str, Any] | None) -> Dict[str, int]:
        counts = session.get("counts", {}) if isinstance(session, dict) and isinstance(session.get("counts"), dict) else {}
        return {
            "scanned": int(counts.get("scanned", 0) or 0),
            "downgraded": int(counts.get("downgraded", 0) or 0),
            "kept": int(counts.get("kept", 0) or 0),
            "skipped": int(counts.get("skipped", 0) or 0),
        }

    def _make_background_scan_session(
        self,
        task_key: str,
        *,
        candidates: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        now_monotonic = time.monotonic()
        return {
            "task_key": self._normalize_background_task_key(task_key),
            "candidates": copy.deepcopy(candidates),
            "next_index": 0,
            "counts": {"scanned": 0, "downgraded": 0, "kept": 0, "skipped": 0},
            "paused": False,
            "paused_at_monotonic": 0.0,
            "created_at_monotonic": now_monotonic,
            "formal_next_due_monotonic": now_monotonic + max(5, int(self.BACKGROUND_SELF_HEAL_INTERVAL_SEC)),
        }

    def _update_background_scan_session_from_result(self, task_key: str, result: Dict[str, Any]) -> None:
        if not isinstance(result, dict):
            return
        if not bool(result.get("paused", False)):
            self._clear_background_scan_session(task_key)
            return
        session = self._get_background_scan_session(task_key) or {
            "task_key": self._normalize_background_task_key(task_key),
            "candidates": [],
            "formal_next_due_monotonic": time.monotonic() + max(5, int(self.BACKGROUND_SELF_HEAL_INTERVAL_SEC)),
        }
        session["paused"] = True
        session["paused_at_monotonic"] = time.monotonic()
        session["next_index"] = max(0, int(result.get("next_index", 0) or 0))
        session["counts"] = {
            "scanned": int(result.get("scanned", 0) or 0),
            "downgraded": int(result.get("downgraded", 0) or 0),
            "kept": int(result.get("kept", 0) or 0),
            "skipped": int(result.get("skipped", 0) or 0),
        }
        self._put_background_scan_session(task_key, session)

    def _set_background_task_due_monotonic(self, task_key: str, when_monotonic: float) -> None:
        normalized_key = self._normalize_background_task_key(task_key)
        with self._lock:
            self._background_task_next_due_monotonic[normalized_key] = float(when_monotonic or 0.0)
        self._set_background_task_next_run(normalized_key, when_monotonic=when_monotonic)

    def _background_scan_should_pause(self) -> bool:
        return self._has_active_foreground_work()

    def _background_task_is_running(self, task_key: str) -> bool:
        thread = self._background_task_threads.get(str(task_key or "").strip().lower() or "background_task")
        return bool(thread and thread.is_alive())

    def _start_background_task_worker(self, task_key: str, *, target: Callable[[], Dict[str, Any]]) -> bool:
        normalized_key = str(task_key or "").strip().lower() or "background_task"
        with self._lock:
            thread = self._background_task_threads.get(normalized_key)
            if thread and thread.is_alive():
                return False
            worker = threading.Thread(
                target=self._run_background_task_worker,
                kwargs={"task_key": normalized_key, "target": target},
                name=f"shared-bridge-{normalized_key}",
                daemon=True,
            )
            self._background_task_threads[normalized_key] = worker
        worker.start()
        return True

    def _run_background_task_worker(self, *, task_key: str, target: Callable[[], Dict[str, Any]]) -> None:
        started_monotonic = self._mark_background_task_started(task_key)
        try:
            result = target() if callable(target) else {}
            summary = str(result.get("summary", "") or "").strip() if isinstance(result, dict) else ""
            status = str(result.get("status", "") or "").strip().lower() if isinstance(result, dict) else ""
            if not status:
                status = "success"
            self._mark_background_task_finished(
                task_key,
                started_monotonic=started_monotonic,
                status=status,
                summary=summary or self._background_task_title(task_key),
                result=result if isinstance(result, dict) else {},
            )
            self._finalize_background_task_schedule(task_key, result if isinstance(result, dict) else {})
        except Exception as exc:  # noqa: BLE001
            self._mark_background_task_finished(
                task_key,
                started_monotonic=started_monotonic,
                status="failed",
                summary=f"{self._background_task_title(task_key)}失败",
                result={},
                error_text=str(exc).strip(),
            )
            self._emit_system_log(
                f"[共享桥接][后台任务] {self._background_task_title(task_key)}失败: "
                f"error={type(exc).__name__}: {str(exc).strip() or '-'}"
            )
        finally:
            with self._lock:
                thread = self._background_task_threads.get(task_key)
                if thread is not None and not thread.is_alive():
                    self._background_task_threads.pop(task_key, None)

    def _finalize_background_task_schedule(self, task_key: str, result: Dict[str, Any]) -> None:
        normalized_key = self._normalize_background_task_key(task_key)
        status = str(result.get("status", "") or "").strip().lower() if isinstance(result, dict) else ""
        if status == "success":
            interval_sec = (
                self.SOURCE_CACHE_PREWARM_INTERVAL_SEC
                if normalized_key == "source_cache_prewarm"
                else self.BACKGROUND_SELF_HEAL_INTERVAL_SEC
            )
            self._set_background_task_due_monotonic(
                normalized_key,
                time.monotonic() + max(5, int(interval_sec)),
            )
            return
        if isinstance(result, dict) and bool(result.get("paused", False)):
            session = self._get_background_scan_session(normalized_key)
            paused_at = float(session.get("paused_at_monotonic", 0.0) or 0.0) if isinstance(session, dict) else 0.0
            next_check = max(
                time.monotonic() + max(5, int(self.BACKGROUND_TASK_BUSY_RETRY_SEC)),
                paused_at + max(0, int(self.BACKGROUND_SELF_HEAL_IDLE_QUIET_SEC)) if paused_at > 0 else 0.0,
            )
            self._set_background_task_next_run(normalized_key, when_monotonic=next_check)

    def _refresh_internal_alert_status_cache(self) -> None:
        if self._store is None or not hasattr(self._store, "list_external_alert_projections"):
            return
        try:
            projections = self._store.list_external_alert_projections()
            payload = self._build_external_internal_alert_status(projections)
            self._cached_internal_alert_status = copy.deepcopy(payload)
            if self._mirror_store is not None:
                self._mirror_store.set_snapshot(
                    key="internal_alert_status",
                    payload={
                        "updated_at": _now_text(),
                        "payload": copy.deepcopy(payload),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            if self._is_recoverable_store_error(exc):
                self._mark_store_read_degraded(
                    scope="internal_alert_status",
                    exc=exc,
                    busy_message="共享桥接数据库暂时忙碌，内网告警状态已降级为缓存结果",
                    unavailable_message="共享桥接数据库暂时不可用，内网告警状态已降级为缓存结果",
                )
                return
            raise

    def get_cached_tasks(self, *, limit: int | None = None) -> List[Dict[str, Any]]:
        tasks = copy.deepcopy(self._cached_task_list)
        if not tasks and self._mirror_store is not None:
            tasks = self._mirror_store.list_tasks(limit=max(100, int(limit or 100) if limit is not None else 500))
        if limit is None:
            return tasks
        return tasks[: max(1, int(limit or 1))]

    def get_cached_task(self, task_id: str) -> Dict[str, Any] | None:
        task_text = str(task_id or "").strip()
        if not task_text:
            return None
        payload = self._cached_task_details.get(task_text)
        if payload is None and self._mirror_store is not None:
            payload = self._mirror_store.get_task(task_text)
        return copy.deepcopy(payload) if isinstance(payload, dict) else None

    def _should_run(self) -> bool:
        return self.role_mode in {"internal", "external"} and self.shared_bridge_enabled and bool(self.shared_bridge_root)

    def _start_source_cache_after_internal_pool_ready(self) -> None:
        pool = self._internal_download_pool
        source_cache = self._source_cache_service
        if pool is None or source_cache is None:
            return
        existing = self._source_cache_start_thread
        if existing and existing.is_alive():
            return

        def _worker() -> None:
            try:
                wait_ready = getattr(pool, "wait_until_ready", None)
                if callable(wait_ready):
                    ready_result = wait_ready(timeout_sec=120)
                else:
                    try:
                        health = pool.get_health_snapshot() if hasattr(pool, "get_health_snapshot") else {}
                    except Exception:  # noqa: BLE001
                        health = {}
                    ready_result = {
                        "ready": bool(health.get("browser_ready", False)) if isinstance(health, dict) else False,
                        "reason": "ready",
                    }
                if self._stop_event.is_set():
                    return
                if not bool(ready_result.get("ready", False)):
                    reason = str(ready_result.get("error") or ready_result.get("reason") or "内网下载浏览器池未就绪").strip()
                    self._last_error = reason
                    self._emit_system_log(f"[共享缓存] 内网下载浏览器池尚未就绪，已延后启动源文件调度: {reason}")
                    return
                source_cache.update_download_browser_pool(pool)
                source_cache.start()
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                self._emit_system_log(f"[共享缓存] 源文件调度后台启动失败: {exc}")
            finally:
                if self._source_cache_start_thread is threading.current_thread():
                    self._source_cache_start_thread = None

        self._source_cache_start_thread = threading.Thread(
            target=_worker,
            name="shared-source-cache-start-after-browser-ready",
            daemon=True,
        )
        self._source_cache_start_thread.start()

    def start(self) -> Dict[str, Any]:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return {"started": False, "running": True, "reason": "already_running"}
            self._background_task_threads = {}
            self._background_task_state = self._build_initial_background_task_state()
            self._active_bridge_task_count = 0
            self._startup_logged = False
            self._last_error = ""
            self._last_poll_at = ""
            self._last_cleanup_at = ""
            self._cleanup_deleted_tasks = 0
            self._cleanup_deleted_entries = 0
            self._cleanup_deleted_files = 0
            if not self._should_run():
                self._db_status = "disabled"
                self._counts = {"pending_internal": 0, "pending_external": 0, "problematic": 0, "total_count": 0, "node_count": 0}
                self._source_cache_start_thread = None
                if self._source_cache_service is not None:
                    self._source_cache_service.stop()
                if self._internal_download_pool is not None:
                    clear_internal_download_browser_pool(self._internal_download_pool)
                    self._internal_download_pool.stop()
                    self._internal_download_pool = None
                return {"started": False, "running": False, "reason": "disabled_or_unselected"}
            self._db_status = "starting"
            if self.role_mode == "internal":
                self._run_internal_source_cache_index_startup_cleanup()
                if self._internal_download_pool is None:
                    self._internal_download_pool = InternalDownloadBrowserPool(
                        self.runtime_config,
                        emit_log=self._emit_system_log,
                        request_runtime_status_refresh=lambda reason: self._request_runtime_status_refresh(reason=reason),
                    )
                try:
                    pool_result = self._internal_download_pool.start(wait_ready=False)
                except TypeError:
                    pool_result = self._internal_download_pool.start()
                if not bool(pool_result.get("running", False)):
                    error_text = str(pool_result.get("error", "") or "内网下载浏览器池启动失败").strip()
                    self._last_error = error_text
                    return {
                        "started": False,
                        "running": False,
                        "reason": "internal_download_pool_failed",
                        "error": error_text,
                    }
                set_internal_download_browser_pool(self._internal_download_pool)
                if self._source_cache_service is not None:
                    self._source_cache_service.update_download_browser_pool(self._internal_download_pool)
                    self._start_source_cache_after_internal_pool_ready()
            else:
                self._source_cache_start_thread = None
                if self._internal_download_pool is not None:
                    clear_internal_download_browser_pool(self._internal_download_pool)
                    self._internal_download_pool.stop()
                    self._internal_download_pool = None
                if self._source_cache_service is not None:
                    self._source_cache_service.update_download_browser_pool(None)
                    self._source_cache_service.stop()
            self._stop_event.clear()
            self._wake_event.clear()
            self._thread = threading.Thread(target=self._loop, name="shared-bridge-runtime", daemon=True)
            self._thread.start()
            return {"started": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        with self._lock:
            thread = self._thread
            background_threads = [item for item in self._background_task_threads.values() if item]
            self._background_task_threads = {}
            if not thread:
                self._source_cache_start_thread = None
                if self._source_cache_service is not None:
                    self._source_cache_service.stop()
                if self._internal_download_pool is not None:
                    clear_internal_download_browser_pool(self._internal_download_pool)
                    self._internal_download_pool.stop()
                    self._internal_download_pool = None
                self._startup_logged = False
                self._background_task_state = self._build_initial_background_task_state()
                self._active_bridge_task_count = 0
                self._db_status = "disabled" if not self._should_run() else "stopped"
                self._counts = {"pending_internal": 0, "pending_external": 0, "problematic": 0, "total_count": 0, "node_count": 0}
                for background_thread in background_threads:
                    try:
                        background_thread.join(timeout=1)
                    except Exception:
                        pass
                return {"stopped": False, "running": False, "reason": "not_running"}
            self._stop_event.set()
            self._wake_event.set()
            self._thread = None
            self._source_cache_start_thread = None
        thread.join(timeout=5)
        for background_thread in background_threads:
            try:
                background_thread.join(timeout=1)
            except Exception:
                pass
        if self._source_cache_service is not None:
            self._source_cache_service.stop()
        if self._internal_download_pool is not None:
            clear_internal_download_browser_pool(self._internal_download_pool)
            self._internal_download_pool.stop()
            self._internal_download_pool = None
        self._startup_logged = False
        self._background_task_state = self._build_initial_background_task_state()
        self._active_bridge_task_count = 0
        self._db_status = "disabled" if not self._should_run() else "stopped"
        self._counts = {"pending_internal": 0, "pending_external": 0, "problematic": 0, "total_count": 0, "node_count": 0}
        return {"stopped": True, "running": False, "reason": "stopped"}

    def is_running(self) -> bool:
        thread = self._thread
        return bool(thread and thread.is_alive())

    def get_health_snapshot(self, *, mode: str = "external_full") -> Dict[str, Any]:
        normalized_mode = str(mode or "external_full").strip().lower() or "external_full"
        try:
            internal_download_pool = (
                self._internal_download_pool.get_health_snapshot()
                if self._internal_download_pool is not None
                else {
                    "enabled": False,
                    "browser_ready": False,
                    "page_slots": [],
                    "active_buildings": [],
                    "last_error": "",
                }
            )
            internal_source_cache = (
                self._source_cache_service.get_health_snapshot(mode=normalized_mode)
                if self._source_cache_service is not None
                else self._empty_internal_source_cache_snapshot()
            )
            internal_alert_status = copy.deepcopy(self._cached_internal_alert_status)
            if not isinstance(internal_alert_status, dict) or not internal_alert_status:
                if self._mirror_store is not None:
                    cached_alert_status = self._mirror_store.get_snapshot(key="internal_alert_status")
                    if isinstance(cached_alert_status, dict):
                        payload = cached_alert_status.get("payload", cached_alert_status)
                        if isinstance(payload, dict) and payload:
                            internal_alert_status = copy.deepcopy(payload)
            if not isinstance(internal_alert_status, dict) or not internal_alert_status:
                internal_alert_status = self._empty_internal_alert_status()
            self._cached_internal_alert_status = copy.deepcopy(internal_alert_status)
            background_tasks = self._snapshot_background_tasks()
            snapshot = {
                "enabled": self.shared_bridge_enabled,
                "role_mode": self.role_mode,
                "root_dir": self.shared_bridge_root,
                "db_status": self._db_status,
                "last_error": self._last_error,
                "last_poll_at": self._last_poll_at,
                "last_cleanup_at": self._last_cleanup_at,
                "cleanup_deleted_tasks": int(self._cleanup_deleted_tasks or 0),
                "cleanup_deleted_entries": int(self._cleanup_deleted_entries or 0),
                "cleanup_deleted_files": int(self._cleanup_deleted_files or 0),
                "pending_internal": int(self._counts.get("pending_internal", 0)),
                "pending_external": int(self._counts.get("pending_external", 0)),
                "problematic": int(self._counts.get("problematic", 0)),
                "task_count": int(self._counts.get("total_count", 0)),
                "node_count": int(self._counts.get("node_count", 0)),
                "node_heartbeat_ok": bool(self.is_running() and self._db_status == "ok"),
                "agent_status": "running" if self.is_running() else ("disabled" if not self._should_run() else "stopped"),
                "heartbeat_interval_sec": self.heartbeat_interval_sec,
                "poll_interval_sec": self.poll_interval_sec,
                "background_task_count": len(background_tasks),
                "background_running_count": sum(1 for item in background_tasks if bool(item.get("running", False))),
                "background_tasks": background_tasks,
                "internal_download_pool": internal_download_pool,
                "internal_source_cache": internal_source_cache,
                "internal_alert_status": internal_alert_status,
            }
            self._cached_health_snapshot = copy.deepcopy(snapshot)
            if self._mirror_store is not None:
                mirror_payload = copy.deepcopy(snapshot)
                mirror_payload["updated_at"] = _now_text()
                self._mirror_store.set_snapshot(key="health_snapshot", payload=mirror_payload)
            return snapshot
        except Exception as exc:  # noqa: BLE001
            if self._is_recoverable_store_error(exc):
                return self._build_degraded_health_snapshot(exc)
            raise

    def list_tasks(self, *, limit: int = 100) -> list[Dict[str, Any]]:
        mailbox_tasks = self._list_mailbox_tasks(limit=limit)
        if mailbox_tasks:
            self._cache_task_list(mailbox_tasks)
            return mailbox_tasks
        if not self._store:
            return self.get_cached_tasks(limit=limit)
        try:
            self._store.ensure_ready()
            tasks = self._store.list_tasks(limit=limit)
            self._cache_task_list(tasks)
            return tasks
        except Exception as exc:  # noqa: BLE001
            if self._is_recoverable_store_error(exc):
                self._mark_store_read_degraded(
                    scope="list_tasks",
                    exc=exc,
                    busy_message="共享桥接数据库暂时忙碌，共享任务列表已降级为缓存结果",
                    unavailable_message="共享桥接数据库暂时不可用，共享任务列表已降级为缓存结果",
                )
                mailbox_tasks = self._list_mailbox_tasks(limit=limit)
                if mailbox_tasks:
                    self._cache_task_list(mailbox_tasks)
                    return mailbox_tasks
                return self.get_cached_tasks(limit=limit)
            raise

    def get_task(self, task_id: str) -> Dict[str, Any] | None:
        task_text = str(task_id or "").strip()
        mailbox_task = self._get_mailbox_task(task_text)
        if isinstance(mailbox_task, dict):
            self._cache_task_detail(mailbox_task)
            return mailbox_task
        if not self._store:
            return self.get_cached_task(task_text)
        try:
            self._store.ensure_ready()
            task = self._store.get_task(task_text)
            if isinstance(task, dict):
                if self._repair_task_artifacts(task):
                    task = self._store.get_task(task_text)
                self._cache_task_detail(task)
            elif task_text:
                self._cached_task_details.pop(task_text, None)
            return task
        except Exception as exc:  # noqa: BLE001
            if self._is_recoverable_store_error(exc):
                self._mark_store_read_degraded(
                    scope="get_task",
                    exc=exc,
                    busy_message="共享桥接数据库暂时忙碌，共享任务详情已降级为缓存结果",
                    unavailable_message="共享桥接数据库暂时不可用，共享任务详情已降级为缓存结果",
                )
                mailbox_task = self._get_mailbox_task(task_text)
                if isinstance(mailbox_task, dict):
                    self._cache_task_detail(mailbox_task)
                    return mailbox_task
                return self.get_cached_task(task_text)
            raise

    def cancel_task(self, task_id: str) -> bool:
        if not self._store:
            return False
        self._store.ensure_ready()
        return self._store.cancel_task(task_id)

    def retry_task(self, task_id: str) -> bool:
        if not self._store:
            return False
        self._store.ensure_ready()
        return self._store.retry_task(task_id)

    def diagnose_shared_root(self, *, initialize: bool = True, ready_limit_per_family: int = 400) -> Dict[str, Any]:
        root_text = str(self.shared_bridge_root or "").strip()
        role_label = _role_label(self.role_mode)
        families = (
            (FAMILY_HANDOVER_LOG, FAMILY_LABELS[FAMILY_HANDOVER_LOG]),
            (FAMILY_HANDOVER_CAPACITY_REPORT, FAMILY_LABELS[FAMILY_HANDOVER_CAPACITY_REPORT]),
            (FAMILY_MONTHLY_REPORT, FAMILY_LABELS[FAMILY_MONTHLY_REPORT]),
            (FAMILY_ALARM_EVENT, FAMILY_LABELS[FAMILY_ALARM_EVENT]),
        )
        if not root_text:
            return {
                "status": "misconfigured",
                "status_text": "共享目录未配置",
                "tone": "danger",
                "message": "当前角色尚未配置共享目录，无法执行自检。",
                "role_mode": self.role_mode,
                "role_label": role_label,
                "root_dir": "",
                "db_path": "",
                "checked_at": _now_text(),
                "enabled_buildings": self.get_source_cache_buildings(),
                "directories": [],
                "families": [],
                "summary": {
                    "ready_entry_count": 0,
                    "accessible_ready_count": 0,
                    "missing_ready_count": 0,
                    "initialized_count": 0,
                },
                "error": "",
            }

        root_path = Path(root_text)
        cache_paths = (
            self._source_cache_service.get_required_directory_paths()
            if self._source_cache_service is not None and hasattr(self._source_cache_service, "get_required_directory_paths")
            else {}
        )
        path_specs = [
            ("root_dir", "共享根目录", root_path, "directory"),
            ("bridge_db", "共享桥接数据库", root_path / "bridge.db", "file"),
            ("artifacts", "任务产物目录", root_path / "artifacts", "directory"),
            ("logs", "桥接日志目录", root_path / "logs", "directory"),
            ("tmp", "桥接临时目录", root_path / "tmp", "directory"),
            (
                FAMILY_HANDOVER_LOG,
                FAMILY_LABELS[FAMILY_HANDOVER_LOG],
                Path(str(cache_paths.get(FAMILY_HANDOVER_LOG, "") or (root_path / FAMILY_LABELS[FAMILY_HANDOVER_LOG]))),
                "directory",
            ),
            (
                FAMILY_HANDOVER_CAPACITY_REPORT,
                FAMILY_LABELS[FAMILY_HANDOVER_CAPACITY_REPORT],
                Path(str(cache_paths.get(FAMILY_HANDOVER_CAPACITY_REPORT, "") or (root_path / FAMILY_LABELS[FAMILY_HANDOVER_CAPACITY_REPORT]))),
                "directory",
            ),
            (
                FAMILY_MONTHLY_REPORT,
                FAMILY_LABELS[FAMILY_MONTHLY_REPORT],
                Path(str(cache_paths.get(FAMILY_MONTHLY_REPORT, "") or (root_path / FAMILY_LABELS[FAMILY_MONTHLY_REPORT]))),
                "directory",
            ),
            (
                FAMILY_ALARM_EVENT,
                FAMILY_LABELS[FAMILY_ALARM_EVENT],
                Path(str(cache_paths.get(FAMILY_ALARM_EVENT, "") or (root_path / FAMILY_LABELS[FAMILY_ALARM_EVENT]))),
                "directory",
            ),
            (
                "tmp_source_cache",
                "共享缓存临时目录",
                Path(str(cache_paths.get("tmp_source_cache", "") or (root_path / "tmp" / "source_cache"))),
                "directory",
            ),
        ]

        def _path_exists(path: Path, kind: str) -> bool:
            try:
                if kind == "file":
                    return path.exists() and path.is_file()
                return path.exists() and path.is_dir()
            except OSError:
                return False

        before_exists = {key: _path_exists(path, kind) for key, _label, path, kind in path_specs}
        init_error = ""
        if initialize:
            try:
                if self._store is not None:
                    self._store.ensure_ready()
                else:
                    root_path.mkdir(parents=True, exist_ok=True)
                    for name in ("artifacts", "logs", "tmp"):
                        (root_path / name).mkdir(parents=True, exist_ok=True)
                if self._source_cache_service is not None and hasattr(self._source_cache_service, "ensure_required_directories"):
                    self._source_cache_service.ensure_required_directories()
            except Exception as exc:  # noqa: BLE001
                init_error = str(exc)

        directory_rows: List[Dict[str, Any]] = []
        initialized_count = 0
        for key, label, path, kind in path_specs:
            exists = _path_exists(path, kind)
            created_now = exists and not bool(before_exists.get(key, False))
            if created_now:
                initialized_count += 1
            directory_rows.append(
                {
                    "key": key,
                    "label": label,
                    "path": str(path),
                    "kind": kind,
                    "exists": exists,
                    "created_now": created_now,
                }
            )

        db_error = ""
        if self._store is not None:
            try:
                self._store.ensure_ready()
                mailbox_tasks = self._list_mailbox_tasks(limit=200)
                if not mailbox_tasks and self._mirror_store is not None:
                    self._mirror_store.list_tasks(limit=200)
            except Exception as exc:  # noqa: BLE001
                db_error = str(exc)

        family_rows: List[Dict[str, Any]] = []
        ready_entry_count = 0
        accessible_ready_count = 0
        missing_ready_count = 0
        for family_key, family_label in families:
            query_error = ""
            entries: List[Dict[str, Any]] = []
            if self._store is not None:
                try:
                    entries = self._store.list_source_cache_entries(
                        source_family=family_key,
                        status="ready",
                        limit=max(1, int(ready_limit_per_family or 400)),
                    )
                except Exception as exc:  # noqa: BLE001
                    query_error = str(exc)
            family_ready = len(entries)
            family_accessible = 0
            family_missing = 0
            sample_ready_path = ""
            sample_missing_path = ""
            latest_downloaded_at = ""
            family_root = next((item["path"] for item in directory_rows if item["key"] == family_key), "")
            for entry in entries:
                downloaded_at = str(entry.get("downloaded_at", "") or "").strip()
                if downloaded_at and downloaded_at > latest_downloaded_at:
                    latest_downloaded_at = downloaded_at
                relative_path = str(entry.get("relative_path", "") or "").strip()
                resolved_path = root_path / relative_path if relative_path else None
                if resolved_path is not None and is_accessible_cached_file_path(resolved_path):
                    family_accessible += 1
                    if not sample_ready_path:
                        sample_ready_path = str(resolved_path)
                else:
                    family_missing += 1
                    if not sample_missing_path:
                        sample_missing_path = str(resolved_path) if resolved_path is not None else relative_path
            ready_entry_count += family_ready
            accessible_ready_count += family_accessible
            missing_ready_count += family_missing
            if query_error:
                tone = "danger"
                status_text = "查询失败"
                summary_text = f"{family_label}状态读取失败：{query_error}"
            elif family_ready <= 0:
                tone = "neutral"
                status_text = "无就绪记录"
                summary_text = "当前还没有 ready 记录。"
            elif family_missing <= 0:
                tone = "success"
                status_text = "记录与文件一致"
                summary_text = "ready 记录对应文件当前角色均可访问。"
            elif family_accessible <= 0:
                tone = "danger"
                status_text = "记录存在但文件不可见"
                summary_text = "数据库有 ready 记录，但当前角色看不到对应文件。"
            else:
                tone = "warning"
                status_text = "部分文件不可见"
                summary_text = "部分 ready 记录对应文件当前角色不可访问。"
            family_rows.append(
                {
                    "key": family_key,
                    "title": family_label,
                    "tone": tone,
                    "status_text": status_text,
                    "summary_text": summary_text,
                    "path": family_root,
                    "ready_entry_count": family_ready,
                    "accessible_ready_count": family_accessible,
                    "missing_ready_count": family_missing,
                    "latest_downloaded_at": latest_downloaded_at,
                    "sample_ready_path": sample_ready_path,
                    "sample_missing_path": sample_missing_path,
                    "query_error": query_error,
                }
            )

        status = "success"
        status_text = "共享目录自检完成"
        tone = "success"
        message = "必要目录已检查完成。"
        if init_error or db_error:
            status = "error"
            status_text = "共享目录自检异常"
            tone = "danger"
            message = init_error or db_error
        elif missing_ready_count > 0 and ready_entry_count > 0:
            status = "warning"
            status_text = "发现记录与文件不一致"
            tone = "warning"
            message = "存在 ready 记录，但当前角色无法访问其中部分文件。"
        elif ready_entry_count <= 0:
            status = "empty"
            status_text = "已补齐必要目录"
            tone = "neutral"
            message = "必要目录已存在或已补齐，但当前还没有 ready 记录。"

        return {
            "status": status,
            "status_text": status_text,
            "tone": tone,
            "message": message,
            "role_mode": self.role_mode,
            "role_label": role_label,
            "root_dir": root_text,
            "db_path": str(root_path / "bridge.db"),
            "checked_at": _now_text(),
            "enabled_buildings": self.get_source_cache_buildings(),
            "directories": directory_rows,
            "families": family_rows,
            "summary": {
                "ready_entry_count": ready_entry_count,
                "accessible_ready_count": accessible_ready_count,
                "missing_ready_count": missing_ready_count,
                "initialized_count": initialized_count,
            },
            "error": init_error or db_error,
        }

    @staticmethod
    def _normalize_issue_summary(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return "页面异常，等待内网恢复"
        if ":" in text:
            _prefix, _sep, suffix = text.partition(":")
            suffix = suffix.strip()
            if suffix:
                return suffix
        return text

    @staticmethod
    def _internal_alert_status_key(*, failure_kind: Any, suspended: bool = False) -> str:
        normalized = str(failure_kind or "").strip().lower()
        if normalized in {"login_failed", "login_expired"}:
            return normalized
        if normalized in {
            "page_unreachable",
            "page_connection_refused",
            "page_timeout",
            "page_address_invalid",
            "browser_closed",
            "page_closed",
        }:
            return "page_error"
        if normalized == "network_disconnected":
            return "network_error"
        if suspended:
            return "suspended"
        return "browser_issue"

    @classmethod
    def _empty_internal_alert_status(cls) -> Dict[str, Any]:
        return {
            "buildings": [
                {
                    "building": building,
                    "status": "normal",
                    "status_text": "正常",
                    "summary": "未收到异常告警",
                    "detail": "",
                    "last_problem_at": "",
                    "last_recovered_at": "",
                    "active_count": 0,
                }
                for building in _INTERNAL_ALERT_BUILDINGS
            ],
            "active_count": 0,
            "last_notified_at": "",
        }

    @staticmethod
    def _empty_internal_source_cache_snapshot() -> Dict[str, Any]:
        return {
            "enabled": False,
            "scheduler_running": False,
            "current_hour_bucket": "",
            "last_run_at": "",
            "last_success_at": "",
            "last_error": "",
            "cache_root": "",
            "current_hour_refresh": {
                "running": False,
                "last_run_at": "",
                "last_success_at": "",
                "last_error": "",
                "failed_buildings": [],
                "blocked_buildings": [],
                "running_buildings": [],
                "completed_buildings": [],
                "scope_text": "当前小时",
            },
            "handover_log_family": {},
            "handover_capacity_report_family": {},
            "monthly_report_family": {},
            "alarm_event_family": {},
        }

    @classmethod
    def _empty_background_tasks_snapshot(cls) -> List[Dict[str, Any]]:
        return [
            cls._build_background_task_snapshot("source_cache_sweep"),
            cls._build_background_task_snapshot("source_cache_prewarm"),
            cls._build_background_task_snapshot("artifact_self_heal"),
        ]

    def _build_degraded_health_snapshot(self, exc: Exception) -> Dict[str, Any]:
        self._mark_store_read_degraded(
            scope="health_snapshot",
            exc=exc,
            busy_message="共享桥接数据库暂时忙碌，健康状态已降级为缓存结果",
            unavailable_message="共享桥接数据库暂时不可用，健康状态已降级为缓存结果",
        )
        base = copy.deepcopy(self._cached_health_snapshot) if isinstance(self._cached_health_snapshot, dict) else {}
        if not base and self._mirror_store is not None:
            base = copy.deepcopy(self._mirror_store.get_snapshot(key="health_snapshot") or {})
        if (not isinstance(self._cached_internal_alert_status, dict) or not self._cached_internal_alert_status) and self._mirror_store is not None:
            cached_alert_status = self._mirror_store.get_snapshot(key="internal_alert_status")
            if isinstance(cached_alert_status, dict):
                payload = cached_alert_status.get("payload", cached_alert_status)
                if isinstance(payload, dict) and payload:
                    self._cached_internal_alert_status = copy.deepcopy(payload)
        if self._mirror_store is not None:
            mirrored_tasks = self._mirror_store.list_tasks(limit=500)
            if mirrored_tasks:
                self._counts.update(self._task_counts_from_tasks(mirrored_tasks))
        if not base:
            background_tasks = self._snapshot_background_tasks()
            base = {
                "enabled": self.shared_bridge_enabled,
                "role_mode": self.role_mode,
                "root_dir": self.shared_bridge_root,
                "db_status": self._db_status,
                "last_error": self._last_error,
                "last_poll_at": self._last_poll_at,
                "last_cleanup_at": self._last_cleanup_at,
                "cleanup_deleted_tasks": int(self._cleanup_deleted_tasks or 0),
                "cleanup_deleted_entries": int(self._cleanup_deleted_entries or 0),
                "cleanup_deleted_files": int(self._cleanup_deleted_files or 0),
                "pending_internal": int(self._counts.get("pending_internal", 0)),
                "pending_external": int(self._counts.get("pending_external", 0)),
                "problematic": int(self._counts.get("problematic", 0)),
                "task_count": int(self._counts.get("total_count", 0)),
                "node_count": int(self._counts.get("node_count", 0)),
                "node_heartbeat_ok": False,
                "agent_status": "running" if self.is_running() else ("disabled" if not self._should_run() else "stopped"),
                "heartbeat_interval_sec": self.heartbeat_interval_sec,
                "poll_interval_sec": self.poll_interval_sec,
                "background_task_count": len(background_tasks),
                "background_running_count": sum(1 for item in background_tasks if bool(item.get("running", False))),
                "background_tasks": background_tasks,
                "internal_download_pool": {
                    "enabled": False,
                    "browser_ready": False,
                    "page_slots": [],
                    "active_buildings": [],
                    "last_error": "",
                },
                "internal_source_cache": self._empty_internal_source_cache_snapshot(),
                "internal_alert_status": copy.deepcopy(self._cached_internal_alert_status),
            }
        base.update(
            {
                "enabled": self.shared_bridge_enabled,
                "role_mode": self.role_mode,
                "root_dir": self.shared_bridge_root,
                "db_status": self._db_status,
                "last_error": self._last_error,
                "last_poll_at": self._last_poll_at,
                "last_cleanup_at": self._last_cleanup_at,
                "cleanup_deleted_tasks": int(self._cleanup_deleted_tasks or 0),
                "cleanup_deleted_entries": int(self._cleanup_deleted_entries or 0),
                "cleanup_deleted_files": int(self._cleanup_deleted_files or 0),
                "pending_internal": int(self._counts.get("pending_internal", 0)),
                "pending_external": int(self._counts.get("pending_external", 0)),
                "problematic": int(self._counts.get("problematic", 0)),
                "task_count": int(self._counts.get("total_count", 0)),
                "node_count": int(self._counts.get("node_count", 0)),
                "node_heartbeat_ok": bool(self.is_running() and self._db_status == "ok"),
                "agent_status": "running" if self.is_running() else ("disabled" if not self._should_run() else "stopped"),
                "heartbeat_interval_sec": self.heartbeat_interval_sec,
                "poll_interval_sec": self.poll_interval_sec,
                "background_task_count": len(self._snapshot_background_tasks()),
                "background_running_count": sum(
                    1 for item in self._snapshot_background_tasks() if bool(item.get("running", False))
                ),
                "background_tasks": self._snapshot_background_tasks(),
                "internal_alert_status": copy.deepcopy(self._cached_internal_alert_status),
            }
        )
        return base

    @classmethod
    def _build_external_internal_alert_status(cls, projections: List[Dict[str, Any]]) -> Dict[str, Any]:
        latest_notified_at = ""
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for item in projections if isinstance(projections, list) else []:
            if not isinstance(item, dict):
                continue
            building = str(item.get("building", "") or "").strip()
            if not building:
                continue
            grouped.setdefault(building, []).append(item)
            last_notified_at = str(item.get("last_notified_at", "") or "").strip()
            if last_notified_at and last_notified_at > latest_notified_at:
                latest_notified_at = last_notified_at
        buildings: List[Dict[str, Any]] = []
        active_count = 0
        for building in _INTERNAL_ALERT_BUILDINGS:
            items = grouped.get(building, [])
            active_items = [item for item in items if bool(item.get("still_unresolved", False))]
            if active_items:
                active_count += 1
                buildings.append(
                    {
                        "building": building,
                        "status": "problem",
                        "status_text": "异常",
                        "summary": "；".join(
                            str(item.get("summary", "") or "").strip()
                            for item in active_items
                            if str(item.get("summary", "") or "").strip()
                        ) or "存在内网异常告警",
                        "detail": "；".join(
                            str(item.get("latest_detail", "") or "").strip()
                            for item in active_items
                            if str(item.get("latest_detail", "") or "").strip()
                        ),
                        "last_problem_at": max(
                            (str(item.get("last_seen_at", "") or "").strip() for item in active_items),
                            default="",
                        ),
                        "last_recovered_at": "",
                        "active_count": len(active_items),
                    }
                )
                continue
            recovered_at = max((str(item.get("resolved_at", "") or "").strip() for item in items), default="")
            buildings.append(
                {
                    "building": building,
                    "status": "normal",
                    "status_text": "正常",
                    "summary": "正常" if recovered_at else "未收到异常告警",
                    "detail": "",
                    "last_problem_at": max((str(item.get("last_seen_at", "") or "").strip() for item in items), default=""),
                    "last_recovered_at": recovered_at,
                    "active_count": 0,
                }
            )
        return {
            "buildings": buildings,
            "active_count": active_count,
            "last_notified_at": latest_notified_at,
        }

    def _build_internal_browser_alert_request(self, slot: Dict[str, Any]) -> Dict[str, Any]:
        building = str(slot.get("building", "") or "").strip() or "-"
        failure_kind = str(slot.get("failure_kind", "") or "browser_issue").strip().lower() or "browser_issue"
        status_key = self._internal_alert_status_key(
            failure_kind=failure_kind,
            suspended=bool(slot.get("suspended", False)),
        )
        summary = str(
            slot.get("pending_issue_summary", "")
            or slot.get("suspend_reason", "")
            or slot.get("login_error", "")
            or slot.get("last_error", "")
            or ""
        ).strip()
        latest_detail = str(slot.get("last_error", "") or slot.get("login_error", "") or summary).strip() or summary
        return {
            "building": building,
            "failure_kind": failure_kind,
            "alert_state": "problem",
            "status_key": status_key,
            "summary": summary or f"{building} 页面异常，等待内网恢复",
            "latest_detail": latest_detail or summary or f"{building} 页面异常，等待内网恢复",
            "first_seen_at": str(slot.get("last_failure_at", "") or _now_text()).strip() or _now_text(),
            "last_seen_at": _now_text(),
            "resolved_at": "",
            "occurrence_count": 1,
            "still_unresolved": True,
        }

    def _process_internal_browser_alerts(self) -> None:
        if not self._store or self.role_mode != "internal" or self._internal_download_pool is None:
            return
        self._store.ensure_ready()
        snapshot = self._internal_download_pool.get_health_snapshot()
        page_slots = snapshot.get("page_slots", []) if isinstance(snapshot, dict) else []
        active_keys: set[str] = set()
        for raw_slot in page_slots if isinstance(page_slots, list) else []:
            if not isinstance(raw_slot, dict):
                continue
            if not bool(raw_slot.get("suspended", False)):
                continue
            request_payload = self._build_internal_browser_alert_request(raw_slot)
            alert_key = f"{request_payload['building']}|{request_payload['failure_kind']}"
            active_keys.add(alert_key)
            self._store.upsert_internal_issue_alert(
                building=request_payload["building"],
                failure_kind=request_payload["failure_kind"],
                status_key=request_payload["status_key"],
                summary=request_payload["summary"],
                latest_detail=request_payload["latest_detail"],
                observed_at=request_payload["last_seen_at"],
                active=True,
            )
        for alert in self._store.list_active_internal_issue_alerts():
            alert_key = str(alert.get("alert_key", "") or "").strip()
            if not alert_key or alert_key in active_keys:
                continue
            self._store.clear_internal_issue_alert(
                str(alert.get("building", "") or "").strip(),
                str(alert.get("failure_kind", "") or "").strip(),
            )
        for alert in self._store.list_due_internal_issue_alerts(
            quiet_window_sec=self.INTERNAL_BROWSER_ALERT_QUIET_SEC,
            dedupe_window_sec=self.INTERNAL_BROWSER_ALERT_DEDUPE_SEC,
        ):
            building = str(alert.get("building", "") or "").strip()
            failure_kind = str(alert.get("failure_kind", "") or "").strip().lower() or "browser_issue"
            dedupe_key = "|".join([self.INTERNAL_BROWSER_ALERT_FEATURE, "problem", building or "-", failure_kind or "-"])
            task = self._store.find_active_task_by_dedupe_key(dedupe_key)
            if task is None:
                task = self._store.create_internal_browser_alert_task(
                    building=building,
                    failure_kind=failure_kind,
                    alert_state="problem",
                    status_key=str(alert.get("status_key", "") or "").strip() or "suspended",
                    summary=str(alert.get("summary", "") or "").strip(),
                    latest_detail=str(alert.get("latest_detail", "") or "").strip(),
                    first_seen_at=str(alert.get("first_seen_at", "") or "").strip(),
                    last_seen_at=str(alert.get("last_seen_at", "") or "").strip(),
                    resolved_at="",
                    occurrence_count=int(alert.get("occurrence_count", 0) or 0),
                    still_unresolved=bool(alert.get("active", False)),
                    created_by_role=self.role_mode,
                    created_by_node_id=self.node_id,
                    requested_by="internal_monitor",
                )
            task_id = str(task.get("task_id", "") or "").strip() if isinstance(task, dict) else ""
            self._store.mark_internal_issue_alert_pushed(
                str(alert.get("alert_key", "") or "").strip(),
                task_id=task_id,
            )
            if task_id:
                self._emit_system_log(
                    f"[共享桥接] 已受理内网环境告警 task_id={task_id}, 楼栋={building}, 类型={failure_kind}"
                )
        for alert in self._store.list_due_internal_issue_recoveries():
            building = str(alert.get("building", "") or "").strip()
            failure_kind = str(alert.get("failure_kind", "") or "").strip().lower() or "browser_issue"
            task = self._store.create_internal_browser_alert_task(
                building=building,
                failure_kind=failure_kind,
                alert_state="recovered",
                status_key="healthy",
                summary=f"{building} 已恢复正常" if building else "楼栋已恢复正常",
                latest_detail=str(alert.get("latest_detail", "") or "").strip(),
                first_seen_at=str(alert.get("first_seen_at", "") or "").strip(),
                last_seen_at=str(alert.get("last_seen_at", "") or "").strip(),
                resolved_at=str(alert.get("resolved_at", "") or "").strip(),
                occurrence_count=int(alert.get("occurrence_count", 0) or 0),
                still_unresolved=False,
                created_by_role=self.role_mode,
                created_by_node_id=self.node_id,
                requested_by="internal_monitor",
            )
            task_id = str(task.get("task_id", "") or "").strip() if isinstance(task, dict) else ""
            self._store.mark_internal_issue_alert_recovery_pushed(
                str(alert.get("alert_key", "") or "").strip(),
                task_id=task_id,
            )
            if task_id:
                self._emit_system_log(
                    f"[共享桥接] 已受理内网环境恢复告警 task_id={task_id}, 楼栋={building}, 类型={failure_kind}"
                )

    def _run_internal_browser_alert_external(self, task: Dict[str, Any]) -> None:
        if not self._store:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "external_notify"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="external", claim_token=claim_token)
        building = str(request.get("building", "") or "").strip()
        alert_state = str(request.get("alert_state", "") or "problem").strip().lower() or "problem"
        status_key = str(request.get("status_key", "") or "").strip().lower() or ("healthy" if alert_state == "recovered" else "suspended")
        summary = self._normalize_issue_summary(request.get("summary"))
        latest_detail = self._normalize_issue_summary(request.get("latest_detail"))
        failure_kind = str(request.get("failure_kind", "") or "").strip() or "browser_issue"
        first_seen_at = str(request.get("first_seen_at", "") or "").strip()
        last_seen_at = str(request.get("last_seen_at", "") or "").strip()
        resolved_at = str(request.get("resolved_at", "") or "").strip()
        occurrence_count = int(request.get("occurrence_count", 0) or 0)
        still_unresolved = bool(request.get("still_unresolved", True))
        detail_lines = [
            f"楼栋：{building or '-'}",
            f"问题类型：{failure_kind}",
            f"最近摘要：{summary or latest_detail or '-'}",
        ]
        if first_seen_at:
            detail_lines.append(f"首次发现：{first_seen_at}")
        if last_seen_at:
            detail_lines.append(f"最近发现：{last_seen_at}")
        if resolved_at:
            detail_lines.append(f"恢复时间：{resolved_at}")
        if occurrence_count > 0:
            detail_lines.append(f"聚合次数：{occurrence_count}")
        detail_lines.append(f"当前状态：{'仍未恢复' if still_unresolved else '已恢复'}")
        if latest_detail and latest_detail != summary:
            detail_lines.append(f"最近原因：{latest_detail}")
        detail_text = "\n".join(detail_lines)
        try:
            notify = WebhookNotifyService(self.runtime_config)
            notify.send_failure(
                stage="内网环境告警",
                detail=detail_text,
                building=building or None,
                emit_log=emit_log,
                category="download",
            )
            stage_result = {
                "status": "success",
                "building": building,
                "failure_kind": failure_kind,
                "alert_state": alert_state,
                "status_key": status_key,
                "notified_at": _now_text(),
                "still_unresolved": still_unresolved,
            }
            self._store.upsert_external_alert_projection(
                building=building,
                failure_kind=failure_kind,
                alert_state=alert_state,
                status_key=status_key,
                summary=summary,
                latest_detail=latest_detail,
                first_seen_at=first_seen_at,
                last_seen_at=last_seen_at,
                resolved_at=resolved_at,
                occurrence_count=occurrence_count,
                still_unresolved=still_unresolved,
                last_notified_at=str(stage_result["notified_at"]),
            )
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result=stage_result,
                next_task_status="success",
                task_result={"status": "success", "external": stage_result},
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text},
            )

    def create_handover_from_download_task(
        self,
        *,
        buildings: List[str] | None,
        end_time: str | None,
        duty_date: str | None,
        duty_shift: str | None,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        task = self._store.create_handover_from_download_task(
            buildings=buildings,
            end_time=end_time,
            duty_date=duty_date,
            duty_shift=duty_shift,
            resume_job_id=resume_job_id,
            target_bucket_key=target_bucket_key,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )
        self._wake_loop(reason=f"bridge_task_created:{task.get('task_id', '')}")
        return task

    def get_or_create_handover_from_download_task(
        self,
        *,
        buildings: List[str] | None,
        end_time: str | None,
        duty_date: str | None,
        duty_shift: str | None,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        normalized_buildings = [
            str(item or "").strip()
            for item in (buildings or [])
            if str(item or "").strip()
        ]
        resolved_bucket_key = str(target_bucket_key or "").strip() or self.current_source_cache_bucket()
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        end_time_text = str(end_time or "").strip()
        if duty_date_text and duty_shift_text:
            dedupe_key = "|".join(
                [
                    "handover_from_download",
                    "date",
                    duty_date_text,
                    duty_shift_text,
                    ",".join(normalized_buildings) or "all_enabled",
                    end_time_text or "-",
                ]
            )
        else:
            dedupe_key = "|".join(
                [
                    "handover_from_download",
                    "latest",
                    resolved_bucket_key or "-",
                    ",".join(normalized_buildings) or "all_enabled",
                    end_time_text or "-",
                ]
            )
        existing = self._store.find_active_task_by_dedupe_key(dedupe_key)
        if existing:
            self._wake_loop(reason=f"bridge_task_existing:{existing.get('task_id', '')}")
            return existing
        return self.create_handover_from_download_task(
            buildings=normalized_buildings,
            end_time=end_time,
            duty_date=duty_date,
            duty_shift=duty_shift,
            resume_job_id=resume_job_id,
            target_bucket_key=resolved_bucket_key if not (duty_date_text and duty_shift_text) else "",
            requested_by=requested_by,
        )

    def create_day_metric_from_download_task(
        self,
        *,
        selected_dates: List[str],
        building_scope: str,
        building: str | None,
        resume_job_id: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        task = self._store.create_day_metric_from_download_task(
            selected_dates=selected_dates,
            building_scope=building_scope,
            building=building,
            resume_job_id=resume_job_id,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )
        self._wake_loop(reason=f"bridge_task_created:{task.get('task_id', '')}")
        return task

    def create_wet_bulb_collection_task(
        self,
        *,
        buildings: List[str] | None,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        task = self._store.create_wet_bulb_collection_task(
            buildings=buildings,
            resume_job_id=resume_job_id,
            target_bucket_key=target_bucket_key,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )
        self._wake_loop(reason=f"bridge_task_created:{task.get('task_id', '')}")
        return task

    def create_alarm_event_upload_task(
        self,
        *,
        mode: str,
        building: str | None = None,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        task = self._store.create_alarm_event_upload_task(
            mode=mode,
            building=building,
            resume_job_id=resume_job_id,
            target_bucket_key=target_bucket_key,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )
        self._wake_loop(reason=f"bridge_task_created:{task.get('task_id', '')}")
        return task

    def get_or_create_day_metric_from_download_task(
        self,
        *,
        selected_dates: List[str],
        building_scope: str,
        building: str | None,
        resume_job_id: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        normalized_dates = [
            str(item or "").strip()
            for item in (selected_dates or [])
            if str(item or "").strip()
        ]
        dedupe_key = "|".join(
            [
                "day_metric_from_download",
                ",".join(normalized_dates) or "-",
                str(building_scope or "").strip() or "-",
                str(building or "").strip() or "all_enabled",
            ]
        )
        existing = self._store.find_active_task_by_dedupe_key(dedupe_key)
        if existing:
            self._wake_loop(reason=f"bridge_task_existing:{existing.get('task_id', '')}")
            return existing
        return self.create_day_metric_from_download_task(
            selected_dates=normalized_dates,
            building_scope=building_scope,
            building=building,
            resume_job_id=resume_job_id,
            requested_by=requested_by,
        )

    def get_or_create_alarm_event_upload_task(
        self,
        *,
        mode: str,
        building: str | None = None,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        normalized_mode = str(mode or "").strip().lower() or "full"
        normalized_building = str(building or "").strip()
        resolved_bucket_key = str(target_bucket_key or "").strip() or self.current_alarm_event_bucket()
        dedupe_key = "|".join(
            [
                "alarm_event_upload",
                normalized_mode,
                normalized_building or "all",
                resolved_bucket_key or _now_text()[:13],
            ]
        )
        existing = self._store.find_active_task_by_dedupe_key(dedupe_key)
        if existing:
            self._wake_loop(reason=f"bridge_task_existing:{existing.get('task_id', '')}")
            return existing
        return self.create_alarm_event_upload_task(
            mode=normalized_mode,
            building=normalized_building or None,
            resume_job_id=resume_job_id,
            target_bucket_key=resolved_bucket_key,
            requested_by=requested_by,
        )

    def get_or_create_wet_bulb_collection_task(
        self,
        *,
        buildings: List[str] | None,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        normalized_buildings = [
            str(item or "").strip()
            for item in (buildings or [])
            if str(item or "").strip()
        ]
        resolved_bucket_key = str(target_bucket_key or "").strip() or self.current_source_cache_bucket()
        dedupe_key = "|".join(
            [
                "wet_bulb_collection",
                resolved_bucket_key or _now_text()[:10],
                ",".join(normalized_buildings) or "all_enabled",
            ]
        )
        existing = self._store.find_active_task_by_dedupe_key(dedupe_key)
        if existing:
            self._wake_loop(reason=f"bridge_task_existing:{existing.get('task_id', '')}")
            return existing
        return self.create_wet_bulb_collection_task(
            buildings=normalized_buildings,
            resume_job_id=resume_job_id,
            target_bucket_key=resolved_bucket_key,
            requested_by=requested_by,
        )

    def create_monthly_auto_once_task(
        self,
        *,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
        source: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        task = self._store.create_monthly_auto_once_task(
            target_bucket_key=target_bucket_key,
            resume_job_id=resume_job_id,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
            source=source,
        )
        self._wake_loop(reason=f"bridge_task_created:{task.get('task_id', '')}")
        return task

    def get_or_create_monthly_auto_once_task(
        self,
        *,
        resume_job_id: str | None = None,
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
        source: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        resolved_bucket_key = str(target_bucket_key or "").strip() or self.current_source_cache_bucket()
        dedupe_key = "|".join(["monthly_report_pipeline", "auto_once", resolved_bucket_key or _now_text()[:10] or "-"])
        existing = self._store.find_active_task_by_dedupe_key(dedupe_key)
        if existing:
            self._wake_loop(reason=f"bridge_task_existing:{existing.get('task_id', '')}")
            return existing
        return self.create_monthly_auto_once_task(
            target_bucket_key=resolved_bucket_key,
            resume_job_id=resume_job_id,
            requested_by=requested_by,
            source=source,
        )

    def create_monthly_multi_date_task(
        self,
        *,
        selected_dates: List[str],
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        task = self._store.create_monthly_multi_date_task(
            selected_dates=selected_dates,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )
        self._wake_loop(reason=f"bridge_task_created:{task.get('task_id', '')}")
        return task

    def create_monthly_resume_upload_task(
        self,
        *,
        run_id: str | None,
        auto_trigger: bool,
        resume_job_id: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        task = self._store.create_monthly_resume_upload_task(
            run_id=run_id,
            auto_trigger=auto_trigger,
            resume_job_id=resume_job_id,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )
        self._wake_loop(reason=f"bridge_task_created:{task.get('task_id', '')}")
        return task

    def get_or_create_monthly_resume_upload_task(
        self,
        *,
        run_id: str | None,
        auto_trigger: bool,
        resume_job_id: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        run_id_text = str(run_id or "").strip()
        dedupe_key = "|".join(["monthly_report_pipeline", "resume_upload", run_id_text or "latest"])
        existing = self._store.find_active_task_by_dedupe_key(dedupe_key)
        if existing:
            self._wake_loop(reason=f"bridge_task_existing:{existing.get('task_id', '')}")
            return existing
        return self.create_monthly_resume_upload_task(
            run_id=run_id,
            auto_trigger=auto_trigger,
            resume_job_id=resume_job_id,
            requested_by=requested_by,
        )

    def create_handover_cache_fill_task(
        self,
        *,
        continuation_kind: str,
        buildings: List[str] | None,
        duty_date: str | None,
        duty_shift: str | None,
        selected_dates: List[str] | None,
        building_scope: str | None,
        building: str | None,
        resume_job_id: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        task = self._store.create_handover_cache_fill_task(
            continuation_kind=continuation_kind,
            buildings=buildings,
            duty_date=duty_date,
            duty_shift=duty_shift,
            selected_dates=selected_dates,
            building_scope=building_scope,
            building=building,
            resume_job_id=resume_job_id,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )
        self._wake_loop(reason=f"bridge_task_created:{task.get('task_id', '')}")
        return task

    def get_or_create_handover_cache_fill_task(
        self,
        *,
        continuation_kind: str,
        buildings: List[str] | None,
        duty_date: str | None,
        duty_shift: str | None,
        selected_dates: List[str] | None,
        building_scope: str | None,
        building: str | None,
        resume_job_id: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        normalized_buildings = [str(item or "").strip() for item in (buildings or []) if str(item or "").strip()]
        normalized_dates = [str(item or "").strip() for item in (selected_dates or []) if str(item or "").strip()]
        dedupe_key = "|".join(
            [
                "handover_cache_fill",
                str(continuation_kind or "").strip().lower() or "-",
                str(duty_date or "").strip() or ",".join(normalized_dates) or "-",
                str(duty_shift or "").strip().lower() or "-",
                str(building or "").strip() or ",".join(normalized_buildings) or str(building_scope or "").strip() or "all_enabled",
            ]
        )
        existing = self._store.find_active_task_by_dedupe_key(dedupe_key)
        if existing:
            self._wake_loop(reason=f"bridge_task_existing:{existing.get('task_id', '')}")
            return existing
        return self.create_handover_cache_fill_task(
            continuation_kind=continuation_kind,
            buildings=normalized_buildings,
            duty_date=duty_date,
            duty_shift=duty_shift,
            selected_dates=normalized_dates,
            building_scope=building_scope,
            building=building,
            resume_job_id=resume_job_id,
            requested_by=requested_by,
        )

    def create_monthly_cache_fill_task(
        self,
        *,
        selected_dates: List[str] | None,
        resume_job_id: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        task = self._store.create_monthly_cache_fill_task(
            selected_dates=selected_dates,
            resume_job_id=resume_job_id,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )
        self._wake_loop(reason=f"bridge_task_created:{task.get('task_id', '')}")
        return task

    def get_or_create_monthly_cache_fill_task(
        self,
        *,
        selected_dates: List[str] | None,
        resume_job_id: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        normalized_dates = [str(item or "").strip() for item in (selected_dates or []) if str(item or "").strip()]
        dedupe_key = "|".join(["monthly_cache_fill", ",".join(normalized_dates) or "-"])
        existing = self._store.find_active_task_by_dedupe_key(dedupe_key)
        if existing:
            self._wake_loop(reason=f"bridge_task_existing:{existing.get('task_id', '')}")
            return existing
        return self.create_monthly_cache_fill_task(
            selected_dates=normalized_dates,
            resume_job_id=resume_job_id,
            requested_by=requested_by,
        )

    def current_source_cache_bucket(self) -> str:
        if self._source_cache_service is not None:
            try:
                return str(self._source_cache_service.current_hour_bucket() or "").strip()
            except Exception:
                pass
        return _now_text()[:13]

    def current_alarm_event_bucket(self) -> str:
        if self._source_cache_service is not None:
            try:
                return str(self._source_cache_service.current_alarm_bucket() or "").strip()
            except Exception:
                pass
        return self.current_source_cache_bucket()

    def get_latest_source_cache_entries(self, *, source_family: str, buildings: List[str] | None = None) -> List[Dict[str, Any]]:
        if self._source_cache_service is None:
            return []
        return self._source_cache_service.get_latest_ready_entries(source_family=source_family, buildings=buildings)

    def get_external_source_cache_overview_fast(self) -> Dict[str, Any]:
        if self._source_cache_service is None:
            return {}
        return self._source_cache_service.get_external_source_cache_overview_fast()

    def get_latest_source_cache_selection(
        self,
        *,
        source_family: str,
        buildings: List[str] | None = None,
        max_version_gap: int = 3,
        max_selection_age_hours: float = 3.0,
    ) -> Dict[str, Any]:
        if self._source_cache_service is None:
            return {
                "best_bucket_key": "",
                "best_bucket_age_hours": None,
                "is_best_bucket_too_old": False,
                "selected_entries": [],
                "fallback_buildings": [],
                "missing_buildings": list(buildings or []),
                "stale_buildings": [],
                "buildings": [],
                "can_proceed": False,
            }
        return self._source_cache_service.get_latest_ready_selection(
            source_family=source_family,
            buildings=buildings,
            max_version_gap=max_version_gap,
            max_selection_age_hours=max_selection_age_hours,
        )

    def get_handover_by_date_cache_entries(self, *, duty_date: str, duty_shift: str, buildings: List[str] | None = None) -> List[Dict[str, Any]]:
        if self._source_cache_service is None:
            return []
        return self._source_cache_service.get_handover_by_date_entries(
            duty_date=duty_date,
            duty_shift=duty_shift,
            buildings=buildings,
        )

    def get_handover_capacity_by_date_cache_entries(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        buildings: List[str] | None = None,
    ) -> List[Dict[str, Any]]:
        if self._source_cache_service is None:
            return []
        return self._source_cache_service.get_handover_capacity_by_date_entries(
            duty_date=duty_date,
            duty_shift=duty_shift,
            buildings=buildings,
        )

    def get_day_metric_by_date_cache_entries(self, *, selected_dates: List[str], buildings: List[str]) -> List[Dict[str, Any]]:
        if self._source_cache_service is None:
            return []
        return self._source_cache_service.get_day_metric_by_date_entries(
            selected_dates=selected_dates,
            buildings=buildings,
        )

    def fill_day_metric_history(
        self,
        *,
        selected_dates: List[str],
        building_scope: str,
        building: str | None,
        emit_log: Callable[[str], None],
    ) -> List[Dict[str, Any]]:
        if self._source_cache_service is None:
            return []
        return self._source_cache_service.fill_day_metric_history(
            selected_dates=selected_dates,
            building_scope=building_scope,
            building=building,
            emit_log=emit_log,
        )

    def get_monthly_by_date_cache_entries(self, *, selected_dates: List[str], buildings: List[str] | None = None) -> List[Dict[str, Any]]:
        if self._source_cache_service is None:
            return []
        return self._source_cache_service.get_monthly_by_date_entries(
            selected_dates=selected_dates,
            buildings=buildings,
        )

    def get_source_cache_buildings(self) -> List[str]:
        if self._source_cache_service is None:
            return []
        return self._source_cache_service.get_enabled_buildings()

    def get_alarm_event_upload_selection(self, *, building: str = "") -> Dict[str, Any]:
        if self._source_cache_service is None:
            return {
                "selected_entries": [],
                "selection_reference_date": _now_text()[:10],
                "missing_both_days_buildings": [],
            }
        return self._source_cache_service.get_alarm_event_upload_selection(building=building)

    def start_current_hour_source_cache_refresh(self) -> Dict[str, Any]:
        if self._source_cache_service is None:
            return {"accepted": False, "running": False, "reason": "disabled"}
        return self._source_cache_service.start_current_hour_refresh()

    def start_today_source_cache_refresh(self) -> Dict[str, Any]:
        return self.start_current_hour_source_cache_refresh()

    def start_manual_alarm_source_cache_refresh(self) -> Dict[str, Any]:
        if self._source_cache_service is None:
            return {"accepted": False, "running": False, "reason": "disabled"}
        return self._source_cache_service.start_manual_alarm_refresh()

    def start_building_latest_source_cache_refresh(self, *, source_family: str, building: str) -> Dict[str, Any]:
        if self._source_cache_service is None:
            return {"accepted": False, "running": False, "reason": "disabled"}
        return self._source_cache_service.start_building_latest_refresh(
            source_family=source_family,
            building=building,
        )

    def delete_manual_alarm_source_cache_files(self) -> Dict[str, Any]:
        if self._source_cache_service is None:
            return {"accepted": False, "reason": "disabled", "deleted_count": 0}
        return self._source_cache_service.delete_manual_alarm_files()

    def upload_alarm_event_source_cache_full_to_bitable(
        self,
        *,
        emit_log=None,
    ) -> Dict[str, Any]:
        if self._source_cache_service is None:
            return {"accepted": False, "reason": "disabled"}
        return self._source_cache_service.upload_alarm_event_entries_full_to_bitable(emit_log=emit_log)

    def upload_alarm_event_source_cache_single_building_to_bitable(
        self,
        *,
        building: str,
        emit_log=None,
    ) -> Dict[str, Any]:
        if self._source_cache_service is None:
            return {"accepted": False, "reason": "disabled"}
        return self._source_cache_service.upload_alarm_event_entries_single_building_to_bitable(
            building=building,
            emit_log=emit_log,
        )

    def debug_alarm_page_actions(self, *, building: str) -> Dict[str, Any]:
        if self._source_cache_service is None:
            return {"ok": False, "reason": "disabled"}
        raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")

    def list_monthly_pending_resume_runs(self) -> List[Dict[str, Any]]:
        if not self.shared_bridge_root:
            return []
        return list_bridge_pending_resume_runs(self.runtime_config, shared_root_dir=self.shared_bridge_root)

    def delete_monthly_resume_run(self, run_id: str) -> Dict[str, Any]:
        if not self.shared_bridge_root:
            raise RuntimeError("共享桥接未配置")
        return delete_bridge_resume_run(
            self.runtime_config,
            shared_root_dir=self.shared_bridge_root,
            run_id=run_id,
        )

    def _emit_system_log(self, text: str) -> None:
        line = str(text or "").strip()
        if line and callable(self.emit_log):
            self.emit_log(line)

    def _request_runtime_status_refresh(self, *, reason: str) -> None:
        callback = self._request_runtime_status_refresh_callback
        if not callable(callback):
            return
        try:
            callback(str(reason or "").strip() or "shared_bridge_runtime")
        except Exception:
            pass

    def _wake_loop(self, *, reason: str = "") -> None:
        self._wake_event.set()
        if reason:
            self._request_runtime_status_refresh(reason=reason)

    def _touch_node(self) -> None:
        if not self._store:
            return
        self._store.upsert_node(
            node_id=self.node_id,
            role_mode=self.role_mode,
            node_label=self.node_label,
            host_name=socket.gethostname(),
            version=self.app_version,
        )

    def _delete_relative_files_under_shared_root(self, relative_paths: List[str]) -> int:
        if not self.shared_bridge_root:
            return 0
        deleted_count = 0
        root = Path(self.shared_bridge_root)
        for item in relative_paths:
            relative_text = str(item or "").strip().replace("\\", "/")
            if not relative_text:
                continue
            try:
                candidate = root / relative_text
                if candidate.exists():
                    candidate.unlink(missing_ok=True)
                    deleted_count += 1
            except Exception:  # noqa: BLE001
                continue
        return deleted_count

    def _run_housekeeping(self) -> None:
        if not self._store:
            return
        if self._source_cache_service is not None and getattr(self._source_cache_service, "store", None) is not self._store:
            self._source_cache_service.store = self._store
        stale_count = 0
        if hasattr(self._store, "sweep_expired_running_tasks"):
            stale_count = int(
                self._store.sweep_expired_running_tasks(stale_task_timeout_sec=self.stale_task_timeout_sec) or 0
            )
        history_result: Dict[str, Any] = {"deleted_tasks": 0, "artifact_relative_paths": []}
        if hasattr(self._store, "cleanup_terminal_history"):
            raw_history_result = self._store.cleanup_terminal_history(retention_days=self.TASK_RETENTION_DAYS)
            if isinstance(raw_history_result, dict):
                history_result = raw_history_result
        deleted_task_files = self._delete_relative_files_under_shared_root(history_result.get("artifact_relative_paths", []))
        deleted_nodes = 0
        if hasattr(self._store, "cleanup_stale_nodes"):
            deleted_nodes = int(self._store.cleanup_stale_nodes(retention_days=self.NODE_RETENTION_DAYS) or 0)
        entry_cleanup = (
            self._source_cache_service.cleanup_expired_entries()
            if self._source_cache_service is not None
            else {"deleted_entries": 0, "deleted_files": 0}
        )
        self._last_cleanup_at = _now_text()
        self._cleanup_deleted_tasks = int(history_result.get("deleted_tasks", 0) if isinstance(history_result, dict) else 0)
        self._cleanup_deleted_entries = int(entry_cleanup.get("deleted_entries", 0) if isinstance(entry_cleanup, dict) else 0)
        self._cleanup_deleted_files = deleted_task_files + int(
            entry_cleanup.get("deleted_files", 0) if isinstance(entry_cleanup, dict) else 0
        )
        if stale_count or self._cleanup_deleted_tasks or self._cleanup_deleted_entries or deleted_nodes or self._cleanup_deleted_files:
            self._emit_system_log(
                "[共享桥接] housekeeping 完成: "
                f"stale={stale_count}, deleted_tasks={self._cleanup_deleted_tasks}, "
                f"deleted_entries={self._cleanup_deleted_entries}, deleted_files={self._cleanup_deleted_files}, "
                f"deleted_nodes={deleted_nodes}"
            )

    @staticmethod
    def _is_recoverable_store_error(exc: Exception) -> bool:
        text = str(exc or "").strip().lower()
        if isinstance(exc, PermissionError):
            return True
        recoverable_tokens = (
            "database is locked",
            "database table is locked",
            "database is busy",
            "busy",
            "unable to open database file",
            "disk i/o error",
            "readonly database",
            "cannot operate on a closed database",
            "invalid uri authority",
            "permissionerror",
            "winerror 5",
            "access is denied",
            "拒绝访问",
        )
        if isinstance(exc, (sqlite3.OperationalError, OSError)):
            return any(token in text for token in recoverable_tokens)
        return any(token in text for token in recoverable_tokens)

    def _mark_loop_error(self, exc: Exception) -> None:
        if self._is_recoverable_store_error(exc):
            if self._store_error_state(exc) == "busy":
                self._db_status = "busy"
                self._last_error = "共享桥接数据库暂时忙碌，下一轮自动重试"
            else:
                self._db_status = "unavailable"
                self._last_error = "共享桥接数据库暂时不可用，下一轮自动重试"
            self._emit_store_issue_log("runtime_loop", exc)
        else:
            self._db_status = "error"
            self._last_error = str(exc)
        self._last_poll_at = _now_text()

    def _stage_claim_token(self, task: Dict[str, Any], stage_id: str) -> str:
        for item in task.get("stages", []):
            if str(item.get("stage_id", "")).strip() == str(stage_id or "").strip():
                return str(item.get("claim_token", "") or "").strip()
        return ""

    def _resolve_running_claim(self, task: Dict[str, Any]) -> tuple[str, str]:
        current_stage = task.get("current_stage", {}) if isinstance(task.get("current_stage", {}), dict) else {}
        stage_id = str(current_stage.get("stage_id", "") or "").strip()
        claim_token = self._stage_claim_token(task, stage_id) if stage_id else ""
        if stage_id and claim_token:
            return stage_id, claim_token
        for stage in task.get("stages", []) if isinstance(task.get("stages", []), list) else []:
            if not isinstance(stage, dict):
                continue
            if str(stage.get("status", "") or "").strip().lower() != "running":
                continue
            candidate_stage_id = str(stage.get("stage_id", "") or "").strip()
            candidate_claim_token = str(stage.get("claim_token", "") or "").strip()
            if candidate_stage_id and candidate_claim_token:
                return candidate_stage_id, candidate_claim_token
        return "", ""

    def _fail_claimed_task(
        self,
        task: Dict[str, Any],
        *,
        error_text: str,
        event_type: str = "runtime_error",
        level: str = "error",
    ) -> None:
        if not self._store:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        if not task_id:
            return
        task_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        stage_id, claim_token = self._resolve_running_claim(task)
        if stage_id and claim_token:
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side=self.role_mode,
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={**task_result, "status": "failed", "error": error_text},
            )
        else:
            now_text = _now_text()
            with self._store.connect() as conn:
                conn.execute(
                    """
                    UPDATE bridge_tasks
                    SET status='failed', result_json=?, error=?, updated_at=?, revision=revision+1
                    WHERE task_id=?
                    """,
                    (
                        json.dumps({**task_result, "status": "failed", "error": error_text}, ensure_ascii=False),
                        error_text,
                        now_text,
                        task_id,
                    ),
                )
        self._store.append_event(
            task_id=task_id,
            stage_id=stage_id,
            side=self.role_mode,
            level=level,
            event_type=event_type,
            payload={"message": error_text},
        )

    def _bridge_emit(self, *, task_id: str, stage_id: str, side: str, claim_token: str) -> Callable[[str], None]:
        last_heartbeat = 0.0

        def _emit(text: str) -> None:
            nonlocal last_heartbeat
            line = str(text or "").strip()
            if not line:
                return
            self._emit_system_log(f"[共享桥接][{_role_label(side)}] 任务={task_id} {line}")
            if self._store:
                self._store.append_event(
                    task_id=task_id,
                    stage_id=stage_id,
                    side=side,
                    level="info",
                    event_type="log",
                    payload={"message": line},
                    sync_mailbox=False,
                )
                now_monotonic = time.monotonic()
                if now_monotonic - last_heartbeat >= max(1.0, min(float(self.heartbeat_interval_sec), float(self.claim_lease_sec) / 2.0)):
                    self._store.heartbeat_claim(
                        task_id=task_id,
                        stage_id=stage_id,
                        claim_token=claim_token,
                        lease_sec=self.claim_lease_sec,
                    )
                    self._touch_node()
                    last_heartbeat = now_monotonic

        return _emit

    def _resolve_shared_artifact_file_path(self, relative_path: str) -> Path | None:
        shared_root = str(self.shared_bridge_root or "").strip()
        relative_text = str(relative_path or "").strip()
        if not shared_root or not relative_text:
            return None
        candidate = Path(shared_root) / relative_text
        if not is_accessible_cached_file_path(candidate):
            return None
        return candidate

    def _repair_artifact_to_failed(
        self,
        artifact: Dict[str, Any] | None,
        *,
        error_text: str = "共享任务产物缺失或不可访问",
        metadata_update: Dict[str, Any] | None = None,
    ) -> Dict[str, Any] | None:
        if not self._store or not isinstance(artifact, dict):
            return None
        artifact_id = str(artifact.get("artifact_id", "") or "").strip()
        if not artifact_id:
            return None
        normalized_metadata_update = dict(metadata_update or {})
        normalized_metadata_update.update(
            {
                "error": str(error_text or "").strip() or "共享任务产物缺失或不可访问",
                "failed_at": _now_text(),
            }
        )
        updated = self._store.update_artifact_status(
            artifact_id,
            status="failed",
            metadata_update=normalized_metadata_update,
        )
        if updated:
            self._emit_system_log(
                "[共享桥接] 任务产物已修复为 failed: "
                f"task_id={updated.get('task_id', '')}, kind={updated.get('artifact_kind', '')}, "
                f"building={updated.get('building', '')}, relative_path={updated.get('relative_path', '')}, "
                f"error={updated.get('metadata', {}).get('error', '') if isinstance(updated.get('metadata'), dict) else ''}"
            )
        return updated

    def _delete_artifact_for_missing_file(
        self,
        artifact: Dict[str, Any] | None,
        *,
        error_text: str = "共享任务产物缺失或不可访问",
    ) -> bool:
        if not self._store or not isinstance(artifact, dict):
            return False
        artifact_id = str(artifact.get("artifact_id", "") or "").strip()
        if not artifact_id:
            return False
        deleted = self._store.delete_artifact(artifact_id)
        if deleted:
            self._emit_system_log(
                "[共享桥接] 缺失任务产物索引已移除: "
                f"task_id={artifact.get('task_id', '')}, kind={artifact.get('artifact_kind', '')}, "
                f"building={artifact.get('building', '')}, relative_path={artifact.get('relative_path', '')}, "
                f"error={str(error_text or '').strip() or '共享任务产物缺失或不可访问'}"
            )
        return deleted

    def _resolve_ready_artifact_file_path(self, artifact: Dict[str, Any] | None) -> Path | None:
        if not isinstance(artifact, dict):
            return None
        relative_path = str(artifact.get("relative_path", "") or "").strip()
        file_path = self._resolve_shared_artifact_file_path(relative_path)
        if file_path is None and str(artifact.get("status", "") or "").strip().lower() == "ready":
            self._delete_artifact_for_missing_file(artifact)
        return file_path

    @staticmethod
    def _handover_rows_have_effective_e_value(rows: List[Any]) -> bool:
        for row in rows:
            raw = getattr(row, "e_raw", None)
            if raw is None:
                continue
            if isinstance(raw, str):
                if raw.strip():
                    return True
                continue
            return True
        return False

    def _validate_day_metric_source_file(self, *, path: Path, duty_date: str = "", building: str = "") -> None:
        if self._source_cache_service is not None:
            validator = getattr(self._source_cache_service, "_validate_handover_source_file", None)
            if callable(validator):
                validator(path)
                return
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
            raise ValueError(
                f"交接班源文件无有效数据行: {path}"
                + (f"; duty_date={duty_date}, building={building}" if duty_date or building else "")
            )
        if not self._handover_rows_have_effective_e_value(rows):
            raise ValueError(
                f"交接班源文件E列无有效数据: {path}"
                + (f"; duty_date={duty_date}, building={building}" if duty_date or building else "")
            )

    @staticmethod
    def _parse_datetime_text(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        normalized = text.replace("/", "-")
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H",
            "%Y-%m-%d",
            "%Y%m%d--%H",
            "%Y%m%d-%H",
            "%Y%m%d%H%M%S",
            "%Y%m%d%H",
            "%Y%m%d",
        ):
            try:
                return datetime.strptime(normalized, fmt)
            except ValueError:
                continue
        return None

    def _should_background_sweep_artifact(
        self,
        artifact: Dict[str, Any] | None,
        *,
        lookback_days: int = 7,
        recent_hours: int | None = None,
    ) -> bool:
        if not isinstance(artifact, dict):
            return False
        if str(artifact.get("artifact_kind", "") or "").strip().lower() != "source_file":
            return False
        if recent_hours is not None:
            cutoff = datetime.now() - timedelta(hours=max(1, int(recent_hours or 1)))
        else:
            cutoff = datetime.now() - timedelta(days=max(1, int(lookback_days or 1)))
        metadata = artifact.get("metadata", {}) if isinstance(artifact.get("metadata", {}), dict) else {}
        for candidate in (
            metadata.get("duty_date"),
            metadata.get("bucket_key"),
            metadata.get("downloaded_at"),
            metadata.get("updated_at"),
            metadata.get("created_at"),
            artifact.get("updated_at"),
            artifact.get("created_at"),
        ):
            parsed = self._parse_datetime_text(candidate)
            if parsed is not None and parsed >= cutoff:
                return True
        return False

    def _build_artifact_background_candidates(self) -> List[Dict[str, Any]]:
        if self._store is None:
            return []
        recent_hours = max(1, int(self.BACKGROUND_SELF_HEAL_RECENT_HOURS))
        cutoff = datetime.now() - timedelta(hours=recent_hours)
        artifacts = self._store.list_artifacts(
            artifact_kind="source_file",
            status="ready",
            updated_after=cutoff.strftime("%Y-%m-%d %H:%M:%S"),
            limit=self.BACKGROUND_SELF_HEAL_SCAN_LIMIT,
        )
        return [
            artifact
            for artifact in artifacts
            if self._should_background_sweep_artifact(artifact, recent_hours=recent_hours)
        ]

    def _run_source_cache_background_sweep(self) -> Dict[str, Any]:
        task_key = "source_cache_sweep"
        source_cache_summary: Dict[str, Any] = {
            "status": "success",
            "scanned": 0,
            "downgraded": 0,
            "kept": 0,
            "skipped": 0,
            "next_index": 0,
            "total_candidates": 0,
            "paused": False,
        }
        if self._source_cache_service is not None:
            session = self._get_background_scan_session(task_key)
            if not isinstance(session, dict):
                session = self._make_background_scan_session(
                    task_key,
                    candidates=self._source_cache_service.list_background_sweep_candidates(
                        recent_hours=self.BACKGROUND_SELF_HEAL_RECENT_HOURS,
                        limit=self.BACKGROUND_SELF_HEAL_SCAN_LIMIT,
                    ),
                )
                self._put_background_scan_session(task_key, session)
            source_cache_summary = self._source_cache_service.sweep_invalid_ready_entries(
                recent_hours=self.BACKGROUND_SELF_HEAL_RECENT_HOURS,
                limit=self.BACKGROUND_SELF_HEAL_SCAN_LIMIT,
                emit_log=self._emit_system_log,
                candidates=list(session.get("candidates", []) if isinstance(session.get("candidates", []), list) else []),
                start_index=int(session.get("next_index", 0) or 0),
                initial_counts=self._background_scan_counts_from_session(session),
                should_pause=self._background_scan_should_pause,
            )
            self._update_background_scan_session_from_result(task_key, source_cache_summary)
        paused = bool(source_cache_summary.get("paused", False))
        summary_prefix = "共享缓存库后台扫描已暂停: " if paused else "共享缓存库后台扫描完成: "
        summary_text = (
            summary_prefix
            + f"候选={int(source_cache_summary.get('total_candidates', 0) or 0)}, "
            f"进度={int(source_cache_summary.get('next_index', 0) or 0)}/{int(source_cache_summary.get('total_candidates', 0) or 0)}, "
            f"已扫描={int(source_cache_summary.get('scanned', 0) or 0)}, "
            f"已降级={int(source_cache_summary.get('downgraded', 0) or 0)}, "
            f"保持有效={int(source_cache_summary.get('kept', 0) or 0)}, "
            f"已跳过={int(source_cache_summary.get('skipped', 0) or 0)}"
        )
        self._emit_system_log(f"[共享桥接][后台任务] {summary_text}")
        return {
            "status": "deferred" if paused else "success",
            "summary": summary_text,
            **source_cache_summary,
        }

    def _run_source_cache_prewarm(self) -> Dict[str, Any]:
        source_cache = self._source_cache_service
        if source_cache is None:
            return {"status": "skipped", "summary": "共享缓存服务未初始化", "checked": 0}
        checked = 0
        errors: List[str] = []
        buildings: List[str] = []
        try:
            buildings = [str(item or "").strip() for item in source_cache.get_enabled_buildings() if str(item or "").strip()]
            checked += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(f"buildings={exc}")
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        def _guard(label: str, func: Callable[[], Any]) -> None:
            nonlocal checked
            if self._background_scan_should_pause():
                raise RuntimeError("foreground_busy")
            try:
                func()
                checked += 1
            except RuntimeError:
                raise
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{label}={exc}")

        paused = False
        try:
            for family in (FAMILY_HANDOVER_LOG, FAMILY_HANDOVER_CAPACITY_REPORT, FAMILY_MONTHLY_REPORT):
                _guard(
                    f"latest:{family}",
                    lambda family_name=family: source_cache.get_latest_ready_selection(
                        source_family=family_name,
                        buildings=buildings,
                    ),
                )
            for shift in ("day", "night"):
                _guard(
                    f"handover:{shift}",
                    lambda shift_name=shift: source_cache.get_handover_by_date_entries(
                        duty_date=today,
                        duty_shift=shift_name,
                        buildings=buildings,
                    ),
                )
                _guard(
                    f"capacity:{shift}",
                    lambda shift_name=shift: source_cache.get_handover_capacity_by_date_entries(
                        duty_date=today,
                        duty_shift=shift_name,
                        buildings=buildings,
                    ),
                )
            _guard(
                "day_metric:today",
                lambda: source_cache.get_day_metric_by_date_entries(
                    selected_dates=[today],
                    buildings=buildings,
                ),
            )
            _guard(
                "monthly:recent_dates",
                lambda: source_cache.get_monthly_by_date_entries(
                    selected_dates=[today, yesterday],
                    buildings=buildings,
                ),
            )
            _guard("external_overview", source_cache.get_external_source_cache_overview_fast)
        except RuntimeError as exc:
            if str(exc) == "foreground_busy":
                paused = True
            else:
                errors.append(str(exc))

        summary_text = (
            f"共享缓存按钮预热已让出前台任务: checked={checked}"
            if paused
            else f"共享缓存按钮预热完成: buildings={len(buildings)}, checked={checked}, errors={len(errors)}"
        )
        self._emit_system_log(f"[共享桥接][后台任务] {summary_text}")
        return {
            "status": "deferred" if paused else ("success" if not errors else "partial_failed"),
            "paused": paused,
            "summary": summary_text,
            "checked": checked,
            "error_count": len(errors),
            "errors": errors[:5],
        }

    def _run_artifact_background_self_heal(self) -> Dict[str, Any]:
        task_key = "artifact_self_heal"
        session = self._get_background_scan_session(task_key)
        if not isinstance(session, dict):
            session = self._make_background_scan_session(
                task_key,
                candidates=self._build_artifact_background_candidates(),
            )
            self._put_background_scan_session(task_key, session)
        artifacts = list(session.get("candidates", []) if isinstance(session.get("candidates", []), list) else [])
        counts = self._background_scan_counts_from_session(session)
        artifact_scanned = counts["scanned"]
        artifact_downgraded = counts["downgraded"]
        artifact_kept = counts["kept"]
        artifact_skipped = counts["skipped"]
        next_index = max(0, min(int(session.get("next_index", 0) or 0), len(artifacts)))
        paused = False
        if self._store is not None:
            for index, artifact in enumerate(artifacts[next_index:], start=next_index):
                if self._background_scan_should_pause():
                    paused = True
                    next_index = index
                    break
                if not isinstance(artifact, dict):
                    next_index = index + 1
                    continue
                artifact_scanned += 1
                file_path = self._resolve_ready_artifact_file_path(artifact)
                if file_path is None:
                    artifact_downgraded += 1
                    next_index = index + 1
                    continue
                metadata = artifact.get("metadata", {}) if isinstance(artifact.get("metadata", {}), dict) else {}
                duty_date = str(metadata.get("duty_date", "") or "").strip()
                building = str(artifact.get("building", "") or "").strip()
                try:
                    self._validate_day_metric_source_file(
                        path=file_path,
                        duty_date=duty_date,
                        building=building,
                    )
                except Exception as exc:  # noqa: BLE001
                    self._repair_artifact_to_failed(
                        artifact,
                        error_text=str(exc).strip() or "共享任务产物校验失败",
                        metadata_update={"validated_by": "background_sweep"},
                    )
                    artifact_downgraded += 1
                    next_index = index + 1
                    continue
                artifact_kept += 1
                next_index = index + 1
        result = {
            "status": "deferred" if paused else "success",
            "scanned": artifact_scanned,
            "downgraded": artifact_downgraded,
            "kept": artifact_kept,
            "skipped": artifact_skipped,
            "next_index": next_index,
            "total_candidates": len(artifacts),
            "paused": paused,
        }
        self._update_background_scan_session_from_result(task_key, result)
        summary_prefix = "共享桥接产物后台自愈已暂停: " if paused else "共享桥接产物后台自愈完成: "
        summary_text = (
            summary_prefix
            + f"候选={len(artifacts)}, 进度={next_index}/{len(artifacts)}, "
            f"已扫描={artifact_scanned}, 已降级={artifact_downgraded}, "
            f"保持有效={artifact_kept}, 已跳过={artifact_skipped}"
        )
        self._emit_system_log(f"[共享桥接][后台任务] {summary_text}")
        return {
            **result,
            "summary": summary_text,
        }

    def _run_background_self_heal_scan(self) -> None:
        self._run_source_cache_background_sweep()
        self._run_artifact_background_self_heal()

    def _schedule_background_task_if_due(self, *, task_key: str, target: Callable[[], Dict[str, Any]], interval_sec: int) -> float:
        normalized_key = self._normalize_background_task_key(task_key)
        now_monotonic = time.monotonic()
        interval = max(5, int(interval_sec or self.BACKGROUND_SELF_HEAL_INTERVAL_SEC))
        busy_retry = max(5, int(self.BACKGROUND_TASK_BUSY_RETRY_SEC))
        if self._background_task_is_running(normalized_key):
            return now_monotonic + min(busy_retry, interval)

        session = self._get_background_scan_session(normalized_key)
        if isinstance(session, dict) and bool(session.get("paused", False)):
            formal_next_due = float(session.get("formal_next_due_monotonic", 0.0) or 0.0)
            if formal_next_due > 0 and formal_next_due - now_monotonic <= max(0, int(self.BACKGROUND_SELF_HEAL_RESUME_SKIP_BEFORE_NEXT_SEC)):
                self._clear_background_scan_session(normalized_key)
                next_due = max(now_monotonic + 5, formal_next_due)
                self._set_background_task_due_monotonic(normalized_key, next_due)
                self._mark_background_task_deferred(
                    normalized_key,
                    reason="距离下次正式扫描不足20分钟，本轮后台续扫已放弃",
                    next_monotonic=next_due,
                )
                return next_due
            if self._has_active_foreground_work():
                next_due = now_monotonic + min(busy_retry, interval)
                self._mark_background_task_deferred(
                    normalized_key,
                    reason="前台任务执行中，后台扫描已让路",
                    next_monotonic=next_due,
                )
                return next_due
            quiet_due = self._background_idle_quiet_due(now_monotonic)
            if quiet_due > 0:
                self._mark_background_task_deferred(
                    normalized_key,
                    reason="等待前台任务结束后空闲10分钟，再继续后台扫描",
                    next_monotonic=quiet_due,
                )
                return quiet_due
            self._start_background_task_worker(normalized_key, target=target)
            return now_monotonic + min(busy_retry, interval)

        with self._lock:
            next_due = float(self._background_task_next_due_monotonic.get(normalized_key, 0.0) or 0.0)
        if self._has_active_foreground_work():
            next_check = now_monotonic + min(busy_retry, interval)
            self._mark_background_task_deferred(
                normalized_key,
                reason="前台任务执行中，后台扫描已让路",
                next_monotonic=next_check,
            )
            return next_check
        quiet_due = self._background_idle_quiet_due(now_monotonic)
        if quiet_due > 0:
            self._mark_background_task_deferred(
                normalized_key,
                reason="等待前台任务结束后空闲10分钟，再启动后台扫描",
                next_monotonic=quiet_due,
            )
            return quiet_due
        if next_due <= 0:
            next_due = now_monotonic + interval
            self._set_background_task_due_monotonic(normalized_key, next_due)
            return next_due
        if now_monotonic < next_due:
            self._set_background_task_next_run(normalized_key, when_monotonic=next_due)
            return next_due

        self._start_background_task_worker(normalized_key, target=target)
        return now_monotonic + min(busy_retry, interval)

    def _repair_task_artifacts(self, task: Dict[str, Any] | None) -> bool:
        if not isinstance(task, dict):
            return False
        repaired = False
        for artifact in task.get("artifacts", []) if isinstance(task.get("artifacts", []), list) else []:
            if not isinstance(artifact, dict):
                continue
            if str(artifact.get("status", "") or "").strip().lower() != "ready":
                continue
            if self._resolve_ready_artifact_file_path(artifact) is None:
                repaired = True
        return repaired

    def _handover_artifact_target(self, task_id: str, building: str, source_path: Path) -> Path:
        return (
            Path(self.shared_bridge_root)
            / "artifacts"
            / "handover"
            / task_id
            / "source_files"
            / str(building or "").strip()
            / source_path.name
        )

    def _handover_capacity_artifact_target(self, task_id: str, building: str, source_path: Path) -> Path:
        return (
            Path(self.shared_bridge_root)
            / "artifacts"
            / "handover"
            / task_id
            / "capacity_source_files"
            / str(building or "").strip()
            / source_path.name
        )

    def _day_metric_artifact_target(self, task_id: str, duty_date: str, building: str, source_path: Path) -> Path:
        return (
            Path(self.shared_bridge_root)
            / "artifacts"
            / "day_metric"
            / task_id
            / "source_files"
            / str(duty_date or "").strip()
            / str(building or "").strip()
            / source_path.name
        )

    def _wet_bulb_artifact_target(self, task_id: str, building: str, source_path: Path) -> Path:
        return (
            Path(self.shared_bridge_root)
            / "artifacts"
            / "wet_bulb"
            / task_id
            / "source_files"
            / str(building or "").strip()
            / source_path.name
        )

    def _monthly_resume_state_artifact_target(self, task_id: str) -> Path:
        return (
            Path(self.shared_bridge_root)
            / "artifacts"
            / "monthly_report"
            / task_id
            / "resume"
            / "manifest.json"
        )

    def _copy_handover_source_artifact(self, *, task_id: str, building: str, source_file: str, emit_log: Callable[[str], None]) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接存储尚未初始化")
        source_path = Path(str(source_file or "").strip())
        if not source_path.exists():
            raise FileNotFoundError(f"写入共享目录前找不到交接班源文件: {source_path}")
        target_path = self._handover_artifact_target(task_id, building, source_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_copy_file(
            source_path,
            target_path,
            validator=validate_excel_workbook_file,
            temp_suffix=".downloading",
            allow_overwrite_fallback=False,
        )
        relative_path = target_path.relative_to(Path(self.shared_bridge_root)).as_posix()
        metadata = {"original_path": str(source_path), "file_name": source_path.name}
        self._store.upsert_artifact(
            task_id=task_id,
            stage_id="internal_download",
            artifact_kind="source_file",
            building=str(building or "").strip(),
            relative_path=relative_path,
            status="ready",
            size_bytes=target_path.stat().st_size,
            metadata=metadata,
            sync_mailbox=False,
        )
        emit_log(f"[共享桥接][交接班][内网] 已写入共享源文件 楼栋={building}, 路径={target_path}")
        return {"building": str(building or "").strip(), "relative_path": relative_path, "file_path": str(target_path)}

    def _copy_handover_capacity_source_artifact(
        self,
        *,
        task_id: str,
        building: str,
        source_file: str,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接存储尚未初始化")
        source_path = Path(str(source_file or "").strip())
        if not source_path.exists():
            raise FileNotFoundError(f"写入共享目录前找不到交接班容量源文件: {source_path}")
        target_path = self._handover_capacity_artifact_target(task_id, building, source_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_copy_file(
            source_path,
            target_path,
            validator=validate_excel_workbook_file,
            temp_suffix=".downloading",
            allow_overwrite_fallback=False,
        )
        relative_path = target_path.relative_to(Path(self.shared_bridge_root)).as_posix()
        metadata = {"original_path": str(source_path), "file_name": source_path.name}
        self._store.upsert_artifact(
            task_id=task_id,
            stage_id="internal_download",
            artifact_kind="capacity_source_file",
            building=str(building or "").strip(),
            relative_path=relative_path,
            status="ready",
            size_bytes=target_path.stat().st_size,
            metadata=metadata,
            sync_mailbox=False,
        )
        emit_log(f"[共享桥接][交接班容量][内网] 已写入共享源文件 楼栋={building}, 路径={target_path}")
        return {"building": str(building or "").strip(), "relative_path": relative_path, "file_path": str(target_path)}

    def _copy_day_metric_source_artifact(
        self,
        *,
        task_id: str,
        duty_date: str,
        building: str,
        source_file: str,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接存储尚未初始化")
        source_path = Path(str(source_file or "").strip())
        if not source_path.exists():
            raise FileNotFoundError(f"写入共享目录前找不到12项源文件: {source_path}")
        self._validate_day_metric_source_file(
            path=source_path,
            duty_date=str(duty_date or "").strip(),
            building=str(building or "").strip(),
        )
        target_path = self._day_metric_artifact_target(task_id, duty_date, building, source_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_copy_file(
            source_path,
            target_path,
            validator=lambda candidate: self._validate_day_metric_source_file(
                path=candidate,
                duty_date=str(duty_date or "").strip(),
                building=str(building or "").strip(),
            ),
            temp_suffix=".downloading",
            allow_overwrite_fallback=False,
        )
        relative_path = target_path.relative_to(Path(self.shared_bridge_root)).as_posix()
        metadata = {
            "original_path": str(source_path),
            "file_name": source_path.name,
            "duty_date": str(duty_date or "").strip(),
        }
        self._store.upsert_artifact(
            task_id=task_id,
            stage_id="internal_download",
            artifact_kind="source_file",
            building=str(building or "").strip(),
            relative_path=relative_path,
            status="ready",
            size_bytes=target_path.stat().st_size,
            metadata=metadata,
            sync_mailbox=False,
        )
        emit_log(
            f"[共享桥接][12项][内网] 已写入共享源文件 日期={duty_date}, 楼栋={building}, "
            f"路径={target_path}, original_path={source_path}"
        )
        return {
            "duty_date": str(duty_date or "").strip(),
            "building": str(building or "").strip(),
            "relative_path": relative_path,
            "source_file": str(target_path),
        }

    def _copy_wet_bulb_source_artifact(
        self,
        *,
        task_id: str,
        building: str,
        source_file: str,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接存储尚未初始化")
        source_path = Path(str(source_file or "").strip())
        if not source_path.exists():
            raise FileNotFoundError(f"写入共享目录前找不到湿球温度源文件: {source_path}")
        target_path = self._wet_bulb_artifact_target(task_id, building, source_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_copy_file(
            source_path,
            target_path,
            validator=validate_excel_workbook_file,
            temp_suffix=".downloading",
            allow_overwrite_fallback=False,
        )
        relative_path = target_path.relative_to(Path(self.shared_bridge_root)).as_posix()
        metadata = {"original_path": str(source_path), "file_name": source_path.name}
        self._store.upsert_artifact(
            task_id=task_id,
            stage_id="internal_download",
            artifact_kind="source_file",
            building=str(building or "").strip(),
            relative_path=relative_path,
            status="ready",
            size_bytes=target_path.stat().st_size,
            metadata=metadata,
            sync_mailbox=False,
        )
        emit_log(f"[共享桥接][湿球温度][内网] 已写入共享源文件 楼栋={building}, 路径={target_path}")
        return {
            "building": str(building or "").strip(),
            "relative_path": relative_path,
            "file_path": str(target_path),
        }

    def _save_monthly_resume_state_artifact(
        self,
        *,
        task_id: str,
        payload: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接存储尚未初始化")
        target_path = self._monthly_resume_state_artifact_target(task_id)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(
            target_path,
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8",
            validator=validate_json_file,
            temp_suffix=".downloading",
            allow_overwrite_fallback=False,
        )
        relative_path = target_path.relative_to(Path(self.shared_bridge_root)).as_posix()
        self._store.upsert_artifact(
            task_id=task_id,
            stage_id="internal_download",
            artifact_kind="resume_state",
            building="",
            relative_path=relative_path,
            status="ready",
            size_bytes=target_path.stat().st_size,
            metadata={
                "run_id": str(payload.get("run_id", "") or "").strip(),
                "run_save_dir": str(payload.get("run_save_dir", "") or "").strip(),
                "pending_upload_count": int(payload.get("pending_upload_count", 0) or 0),
            },
            sync_mailbox=False,
        )
        emit_log(f"[共享桥接][月报][内网] 已写入共享续传状态 路径={target_path}")
        return {
            "relative_path": relative_path,
            "file_path": str(target_path),
        }

    @staticmethod
    def _merge_handover_bridge_result(task_id: str, internal_result: Dict[str, Any], external_result: Dict[str, Any]) -> Dict[str, Any]:
        internal_failed_rows = []
        for item in internal_result.get("failed", []) if isinstance(internal_result.get("failed", []), list) else []:
            if not isinstance(item, dict):
                continue
            error_text = str(item.get("error", "") or "").strip() or "共享文件准备失败"
            internal_failed_rows.append(
                {
                    "building": str(item.get("building", "") or "").strip(),
                    "data_file": "",
                    "output_file": "",
                    "success": False,
                    "errors": [error_text],
                }
            )
        external_rows = external_result.get("results", []) if isinstance(external_result.get("results", []), list) else []
        success_count = int(external_result.get("success_count", 0) or 0)
        failed_count = len(internal_failed_rows) + int(external_result.get("failed_count", 0) or 0)
        if success_count > 0 and failed_count > 0:
            bridge_status = "partial_failed"
        elif success_count > 0:
            bridge_status = "success"
        else:
            bridge_status = "failed"

        errors: List[str] = []
        for row in internal_failed_rows:
            for error_item in row.get("errors", []) if isinstance(row.get("errors", []), list) else []:
                error_text = str(error_item or "").strip()
                if error_text and error_text not in errors:
                    errors.append(error_text)
        for error_item in external_result.get("errors", []) if isinstance(external_result.get("errors", []), list) else []:
            error_text = str(error_item or "").strip()
            if error_text and error_text not in errors:
                errors.append(error_text)

        return {
            "bridge_task_id": task_id,
            "mode": "from_download",
            "status": bridge_status,
            "success_count": success_count,
            "failed_count": failed_count,
            "results": internal_failed_rows + [item for item in external_rows if isinstance(item, dict)],
            "errors": errors,
            "selected_buildings": list(external_result.get("selected_buildings", [])) if isinstance(external_result.get("selected_buildings", []), list) else [],
            "skipped_buildings": list(external_result.get("skipped_buildings", [])) if isinstance(external_result.get("skipped_buildings", []), list) else [],
            "duty_date": str(external_result.get("duty_date", "") or internal_result.get("duty_date", "") or "").strip(),
            "duty_shift": str(external_result.get("duty_shift", "") or internal_result.get("duty_shift", "") or "").strip(),
            "internal": internal_result,
            "external": external_result,
        }

    @staticmethod
    def _first_failed_error_from_rows(rows: List[Dict[str, Any]] | None) -> str:
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            if str(row.get("status", "")).strip().lower() != "failed":
                continue
            error_text = str(row.get("error", "") or "").strip()
            if error_text:
                return error_text
        return ""

    def _run_handover_internal_download(self, task: Dict[str, Any]) -> None:
        if not self._store:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "internal_download"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="internal", claim_token=claim_token)
        cfg = load_handover_config(self.runtime_config)
        service = HandoverDownloadService(
            cfg,
            download_browser_pool=self._internal_download_pool,
        )
        try:
            emit_log("[共享桥接][交接班][内网] 开始下载阶段")
            service.ensure_internal_ready(emit_log)
            result = service.run_with_capacity_report(
                buildings=request.get("buildings") if isinstance(request.get("buildings"), list) else None,
                end_time=str(request.get("end_time", "") or "").strip() or None,
                duty_date=str(request.get("duty_date", "") or "").strip() or None,
                duty_shift=str(request.get("duty_shift", "") or "").strip() or None,
                switch_network=False,
                emit_log=emit_log,
            )
            handover_result = result.get("handover", {}) if isinstance(result.get("handover", {}), dict) else {}
            capacity_result = result.get("capacity", {}) if isinstance(result.get("capacity", {}), dict) else {}
            handover_success_files = handover_result.get("success_files", []) if isinstance(handover_result.get("success_files", []), list) else []
            capacity_success_files = capacity_result.get("success_files", []) if isinstance(capacity_result.get("success_files", []), list) else []
            handover_files = [
                {
                    "building": str(item.get("building", "") or "").strip(),
                    "file_path": str(item.get("file_path", "") or "").strip(),
                }
                for item in handover_success_files
                if isinstance(item, dict)
                and str(item.get("building", "") or "").strip()
                and str(item.get("file_path", "") or "").strip()
            ]
            capacity_files = [
                {
                    "building": str(item.get("building", "") or "").strip(),
                    "file_path": str(item.get("file_path", "") or "").strip(),
                }
                for item in capacity_success_files
                if isinstance(item, dict)
                and str(item.get("building", "") or "").strip()
                and str(item.get("file_path", "") or "").strip()
            ]
            stage_result = dict(result)
            stage_result["handover_files"] = list(handover_files)
            stage_result["capacity_files"] = list(capacity_files)
            stage_result["artifacts"] = []
            stage_result["capacity_artifacts"] = []
            stage_result["artifact_count"] = 0
            stage_result["capacity_artifact_count"] = 0
            handover_artifact_buildings = {
                str(item.get("building", "") or "").strip()
                for item in handover_files
                if str(item.get("building", "") or "").strip()
            }
            capacity_artifact_buildings = {
                str(item.get("building", "") or "").strip()
                for item in capacity_files
                if str(item.get("building", "") or "").strip()
            }
            if handover_artifact_buildings and handover_artifact_buildings.issubset(capacity_artifact_buildings):
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="internal",
                    stage_result=stage_result,
                    next_task_status="ready_for_external",
                    task_result={"internal": stage_result, "status": "ready_for_external"},
                    record_event=False,
                    sync_mailbox=False,
                )
                self._store.append_event(task_id=task_id, stage_id=stage_id, side="internal", level="info", event_type="await_external", payload={"message": "内网下载完成，等待外网继续处理"})
                self._request_runtime_status_refresh(reason=f"handover_internal_download_completed:{task_id}")
            else:
                error_text = (
                    str(handover_result.get("error", "") or "").strip()
                    or str(capacity_result.get("error", "") or "").strip()
                    or "内网下载未产生完整且可匹配的交接班共享源文件"
                )
                self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="internal",
                    stage_result=stage_result,
                    stage_error=error_text,
                    next_task_status="failed",
                    task_error=error_text,
                    stage_status="failed",
                    task_result={"internal": stage_result, "status": "failed"},
                )
                self._request_runtime_status_refresh(reason=f"handover_internal_download_failed:{task_id}")
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text},
            )
            self._emit_system_log(f"[共享桥接][内网端] 任务={task_id} 交接班共享文件准备失败: {error_text}")
            self._request_runtime_status_refresh(reason=f"handover_internal_download_exception:{task_id}")

    def _run_handover_external_continue(self, task: Dict[str, Any]) -> None:
        if not self._store:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "external_generate_review_output"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="external", claim_token=claim_token)
        try:
            handover_file_items = internal_result.get("handover_files", []) if isinstance(internal_result.get("handover_files", []), list) else []
            building_files: List[tuple[str, str]] = []
            for item in handover_file_items:
                if not isinstance(item, dict):
                    continue
                building = str(item.get("building", "") or "").strip()
                file_path = str(item.get("file_path", "") or "").strip()
                if not building or not file_path:
                    continue
                if not is_accessible_cached_file_path(file_path):
                    raise FileNotFoundError(f"共享目录中的交接班源文件不存在或不可访问: {file_path}")
                building_files.append((building, file_path))
            if not building_files:
                artifacts = self._store.get_artifacts(task_id, artifact_kind="source_file", status="ready")
                for item in artifacts:
                    relative_path = str(item.get("relative_path", "") or "").strip()
                    building = str(item.get("building", "") or "").strip()
                    if not relative_path or not building:
                        continue
                    file_path = self._resolve_ready_artifact_file_path(item)
                    if file_path is None:
                        raise FileNotFoundError(f"共享目录中的交接班源文件不存在或不可访问: {relative_path}")
                    building_files.append((building, str(file_path)))
            capacity_file_items = internal_result.get("capacity_files", []) if isinstance(internal_result.get("capacity_files", []), list) else []
            capacity_building_files: List[tuple[str, str]] = []
            for item in capacity_file_items:
                if not isinstance(item, dict):
                    continue
                building = str(item.get("building", "") or "").strip()
                file_path = str(item.get("file_path", "") or "").strip()
                if not building or not file_path:
                    continue
                if not is_accessible_cached_file_path(file_path):
                    raise FileNotFoundError(f"共享目录中的交接班容量源文件不存在或不可访问: {file_path}")
                capacity_building_files.append((building, file_path))
            if not capacity_building_files:
                capacity_artifacts = self._store.get_artifacts(task_id, artifact_kind="capacity_source_file", status="ready")
                for item in capacity_artifacts:
                    relative_path = str(item.get("relative_path", "") or "").strip()
                    building = str(item.get("building", "") or "").strip()
                    if not relative_path or not building:
                        continue
                    file_path = self._resolve_ready_artifact_file_path(item)
                    if file_path is None:
                        raise FileNotFoundError(f"共享目录中的交接班容量源文件不存在或不可访问: {relative_path}")
                    capacity_building_files.append((building, str(file_path)))
            if not building_files:
                raise RuntimeError("共享目录中没有可继续处理的交接班源文件")
            handover_buildings = {building for building, _ in building_files if building}
            capacity_building_map = {building: file_path for building, file_path in capacity_building_files if building and file_path}
            if not handover_buildings or not handover_buildings.issubset(set(capacity_building_map)):
                raise RuntimeError("共享目录中没有完整可继续处理的交接班容量源文件")
            resolved_capacity_files = [
                (building, capacity_building_map[building])
                for building, _ in building_files
                if building in capacity_building_map
            ]
            resume_job_id = self._resume_job_id_from_task(task)
            if resume_job_id:
                emit_log("[共享桥接][交接班][外网] 共享文件已齐全，准备唤醒原任务")
                self._resume_bound_job(
                    task,
                    worker_payload={
                        "resume_kind": "shared_bridge_handover",
                        "building_files": [{"building": building, "file_path": file_path} for building, file_path in building_files],
                        "capacity_building_files": [
                            {"building": building, "file_path": file_path}
                            for building, file_path in resolved_capacity_files
                        ],
                        "end_time": str(request.get("end_time", "") or "").strip() or None,
                        "duty_date": str(request.get("duty_date", "") or "").strip() or None,
                        "duty_shift": str(request.get("duty_shift", "") or "").strip() or None,
                        "bridge_task_id": task_id,
                    },
                    summary="共享文件已到位，正在继续生成交接班日志",
                )
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="external",
                    stage_result={"status": "resumed", "resume_job_id": resume_job_id},
                    next_task_status="success",
                    task_result={"status": "success", "bridge_task_id": task_id, "resume_job_id": resume_job_id, "internal": internal_result},
                )
                return
            emit_log("[共享桥接][交接班][外网] 共享文件已齐全，开始继续处理")
            external_result = OrchestratorService(self.runtime_config).run_handover_from_files(
                building_files=building_files,
                capacity_building_files=resolved_capacity_files,
                end_time=str(request.get("end_time", "") or "").strip() or None,
                duty_date=str(request.get("duty_date", "") or "").strip() or None,
                duty_shift=str(request.get("duty_shift", "") or "").strip() or None,
                emit_log=emit_log,
            )
            merged_result = self._merge_handover_bridge_result(task_id, internal_result, external_result)
            final_status = str(merged_result.get("status", "") or "failed").strip().lower() or "failed"
            task_error = ""
            if final_status in {"partial_failed", "failed"}:
                errors = merged_result.get("errors", []) if isinstance(merged_result.get("errors", []), list) else []
                task_error = str(errors[0] if errors else "").strip()
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result=external_result,
                next_task_status=final_status,
                task_error=task_error,
                task_result=merged_result,
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text, "internal": internal_result},
            )
            self._emit_system_log(f"[共享桥接][外网端] 任务={task_id} 交接班外网继续失败: {error_text}")

    def _run_day_metric_internal_download(self, task: Dict[str, Any]) -> None:
        if not self._store:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "internal_download"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="internal", claim_token=claim_token)
        try:
            selected_dates = [
                str(item or "").strip()
                for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates"), list) else [])
                if str(item or "").strip()
            ]
            building_scope = str(request.get("building_scope", "") or "").strip() or "all_enabled"
            building = str(request.get("building", "") or "").strip() or None
            target_buildings = (
                [building]
                if building_scope == "single" and building
                else (
                    self._source_cache_service.get_enabled_buildings()
                    if self._source_cache_service is not None
                    else []
                )
            )
            target_buildings = [str(item or "").strip() for item in target_buildings if str(item or "").strip()]

            if self._source_cache_service is not None:
                emit_log("[共享桥接][12项][内网] 优先复用交接班按日共享缓存")
                try:
                    cached_entries = self._source_cache_service.fill_day_metric_history(
                        selected_dates=selected_dates,
                        building_scope=building_scope,
                        building=building,
                        emit_log=emit_log,
                    )
                except RuntimeError as exc:
                    error_text = str(exc or "").strip()
                    if "缺少可复用的交接班源文件" not in error_text:
                        raise
                    missing_by_date = self._parse_missing_day_metric_handover_units(
                        error_text,
                        selected_dates=selected_dates,
                        target_buildings=target_buildings,
                    )
                    if not missing_by_date:
                        raise
                    emit_log(
                        "[共享桥接][12项][内网] 缺少目标日期交接班日志，开始触发真正历史补采: "
                        + ", ".join(
                            f"{duty_date} -> {','.join(buildings) or '-'}"
                            for duty_date, buildings in missing_by_date.items()
                        )
                    )
                    for duty_date, missing_buildings in missing_by_date.items():
                        self._source_cache_service.fill_handover_history(
                            buildings=list(missing_buildings),
                            duty_date=duty_date,
                            duty_shift="day",
                            emit_log=emit_log,
                        )
                    emit_log("[共享桥接][12项][内网] 历史交接班源文件补采完成，重新登记12项按日共享索引")
                    cached_entries = self._source_cache_service.fill_day_metric_history(
                        selected_dates=selected_dates,
                        building_scope=building_scope,
                        building=building,
                        emit_log=emit_log,
                    )
                rows_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
                downloaded_files: List[Dict[str, str]] = []
                for item in cached_entries:
                    if not isinstance(item, dict):
                        continue
                    duty_date = str(item.get("duty_date", "") or "").strip()
                    building_name = str(item.get("building", "") or "").strip()
                    source_file = str(item.get("file_path", "") or item.get("source_file", "") or "").strip()
                    if not duty_date or not building_name or not source_file:
                        continue
                    rows_by_key[(duty_date, building_name)] = {
                        "mode": "from_download",
                        "duty_date": duty_date,
                        "building": building_name,
                        "status": "ok",
                        "stage": "download",
                        "network_mode": "current_network",
                        "network_side": "internal",
                        "deleted_records": 0,
                        "created_records": 0,
                        "source_file": source_file,
                        "output_file": "",
                        "error": "",
                        "attempts": 1,
                        "retryable": False,
                        "retry_source": "persisted_state",
                        "failed_at": "",
                    }
                    downloaded_files.append(
                        {
                            "duty_date": duty_date,
                            "building": building_name,
                            "source_file": source_file,
                        }
                    )

                grouped_results: List[Dict[str, Any]] = []
                for duty_date in selected_dates:
                    grouped_rows: List[Dict[str, Any]] = []
                    for building_name in target_buildings:
                        row = rows_by_key.get((duty_date, building_name))
                        if row is not None:
                            grouped_rows.append(copy.deepcopy(row))
                    grouped_results.append({"duty_date": duty_date, "buildings": grouped_rows})

                total_units = len(selected_dates) * len(target_buildings)
                success_units = len(downloaded_files)
                failed_units = max(0, total_units - success_units)
                internal_result = {
                    "status": "ok" if failed_units <= 0 else "partial_failed",
                    "mode": "from_download",
                    "duty_shift": "day",
                    "selected_dates": list(selected_dates),
                    "selected_buildings": list(target_buildings),
                    "building_scope": building_scope,
                    "building": building or "",
                    "network_switch_followed_global_setting": False,
                    "network_auto_switch_enabled": False,
                    "results": grouped_results,
                    "total_units": total_units,
                    "success_units": success_units,
                    "failed_units": failed_units,
                    "skipped_units": 0,
                    "total_deleted_records": 0,
                    "total_created_records": 0,
                    "downloaded_files": downloaded_files,
                    "downloaded_file_count": len(downloaded_files),
                }
            else:
                service = DayMetricStandaloneUploadService(
                    self.runtime_config,
                    download_browser_pool=self._internal_download_pool,
                )
                emit_log("[共享桥接][12项][内网] 开始下载阶段")
                internal_result = service.run_download_only(
                    selected_dates=selected_dates,
                    building_scope=building_scope,
                    building=building,
                    emit_log=emit_log,
                )
            artifacts: List[Dict[str, Any]] = []
            if self._source_cache_service is None:
                for item in internal_result.get("downloaded_files", []) if isinstance(internal_result.get("downloaded_files", []), list) else []:
                    if not isinstance(item, dict):
                        continue
                    duty_date = str(item.get("duty_date", "") or "").strip()
                    building = str(item.get("building", "") or "").strip()
                    source_file = str(item.get("source_file", "") or "").strip()
                    if not duty_date or not building or not source_file:
                        continue
                    artifacts.append(self._copy_day_metric_source_artifact(task_id=task_id, duty_date=duty_date, building=building, source_file=source_file, emit_log=emit_log))
            else:
                emit_log("[共享桥接][12项][内网] 已使用 source cache canonical 文件，不再复制第二份12项源文件")
            stage_result = dict(internal_result)
            stage_result["artifacts"] = list(artifacts)
            stage_result["artifact_count"] = len(artifacts)
            downloaded_files = internal_result.get("downloaded_files", []) if isinstance(internal_result.get("downloaded_files", []), list) else []
            if artifacts or downloaded_files:
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="internal",
                    stage_result=stage_result,
                    next_task_status="ready_for_external",
                    task_result={"internal": stage_result, "status": "ready_for_external"},
                    record_event=False,
                    sync_mailbox=False,
                )
                self._store.append_event(task_id=task_id, stage_id=stage_id, side="internal", level="info", event_type="await_external", payload={"message": "内网下载完成，等待外网继续上传"})
                self._request_runtime_status_refresh(reason=f"day_metric_internal_download_completed:{task_id}")
            else:
                result_rows: List[Dict[str, Any]] = []
                for date_row in internal_result.get("results", []) if isinstance(internal_result.get("results", []), list) else []:
                    grouped = date_row.get("buildings", []) if isinstance(date_row, dict) else []
                    result_rows.extend([row for row in grouped if isinstance(row, dict)])
                error_text = self._first_failed_error_from_rows(result_rows) or "内网下载未产生任何共享源文件"
                self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="internal",
                    stage_result=stage_result,
                    stage_error=error_text,
                    next_task_status="failed",
                    task_error=error_text,
                    stage_status="failed",
                    task_result={"internal": stage_result, "status": "failed"},
                )
                self._request_runtime_status_refresh(reason=f"day_metric_internal_download_failed:{task_id}")
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text},
            )
            self._emit_system_log(f"[共享桥接][内网端] 任务={task_id} 12项共享文件准备失败: {error_text}")
            self._request_runtime_status_refresh(reason=f"day_metric_internal_download_exception:{task_id}")

    def _run_day_metric_external_continue(self, task: Dict[str, Any]) -> None:
        if not self._store:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "external_upload"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="external", claim_token=claim_token)
        service = DayMetricStandaloneUploadService(self.runtime_config)
        try:
            selected_dates = [str(item or "").strip() for item in (internal_result.get("selected_dates", []) if isinstance(internal_result.get("selected_dates", []), list) else []) if str(item or "").strip()]
            buildings = [str(item or "").strip() for item in (internal_result.get("selected_buildings", []) if isinstance(internal_result.get("selected_buildings", []), list) else []) if str(item or "").strip()]
            if not selected_dates:
                selected_dates = [str(item or "").strip() for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates"), list) else []) if str(item or "").strip()]
            if not buildings:
                requested_building = str(request.get("building", "") or "").strip()
                requested_scope = str(request.get("building_scope", "") or "").strip() or "all_enabled"
                if requested_scope == "single" and requested_building:
                    buildings = [requested_building]
                elif self._source_cache_service is not None:
                    buildings = [str(item or "").strip() for item in self._source_cache_service.get_enabled_buildings() if str(item or "").strip()]

            source_units_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
            expected_count = len(selected_dates) * len(buildings)
            downloaded_files = internal_result.get("downloaded_files", []) if isinstance(internal_result.get("downloaded_files", []), list) else []
            for item in downloaded_files:
                if not isinstance(item, dict):
                    continue
                duty_date = str(item.get("duty_date", "") or "").strip()
                building_name = str(item.get("building", "") or "").strip()
                source_file = str(item.get("source_file", "") or "").strip()
                if not duty_date or not building_name or not source_file:
                    continue
                source_units_by_key[(duty_date, building_name)] = {
                    "duty_date": duty_date,
                    "building": building_name,
                    "source_file": source_file,
                }
            if self._source_cache_service is not None and selected_dates and buildings:
                try:
                    emit_log("[共享桥接][12项][外网] 只读取内网端已登记的共享源文件索引")
                    cached_entries = self._source_cache_service.get_day_metric_by_date_entries(
                        selected_dates=selected_dates,
                        buildings=buildings,
                    )
                    for item in cached_entries:
                        if not isinstance(item, dict):
                            continue
                        duty_date = str(item.get("duty_date", "") or "").strip()
                        building = str(item.get("building", "") or "").strip()
                        source_file = str(item.get("file_path", "") or item.get("source_file", "") or "").strip()
                        if not duty_date or not building or not source_file:
                            continue
                        metadata = item.get("metadata", {}) if isinstance(item.get("metadata", {}), dict) else {}
                        emit_log(
                            f"[共享桥接][12项][外网] 已读取共享源文件索引: duty_date={duty_date}, "
                            f"building={building}, source={source_file}, "
                            f"resolution={str(metadata.get('resolution_source', '') or '').strip() or '-'}, "
                            f"source_bucket={str(metadata.get('source_bucket_kind', '') or '').strip()}/"
                            f"{str(metadata.get('source_bucket_key', '') or '').strip()}"
                        )
                        source_units_by_key[(duty_date, building)] = {
                            "duty_date": duty_date,
                            "building": building,
                            "source_file": source_file,
                        }
                except Exception as exc:  # noqa: BLE001
                    emit_log(f"[共享桥接][12项][外网] 读取共享源文件索引失败，回退artifact: {exc}")

            if len(source_units_by_key) < expected_count:
                artifacts = self._store.get_artifacts(task_id, artifact_kind="source_file", status="ready")
                for item in artifacts:
                    relative_path = str(item.get("relative_path", "") or "").strip()
                    building = str(item.get("building", "") or "").strip()
                    metadata = item.get("metadata", {}) if isinstance(item.get("metadata", {}), dict) else {}
                    duty_date = str(metadata.get("duty_date", "") or "").strip()
                    if not relative_path or not building or not duty_date:
                        continue
                    if (duty_date, building) in source_units_by_key:
                        continue
                    file_path = self._resolve_ready_artifact_file_path(item)
                    if file_path is None:
                        continue
                    emit_log(
                        f"[共享桥接][12项][外网] 回退使用artifact源文件: duty_date={duty_date}, "
                        f"building={building}, artifact={file_path}, "
                        f"original_path={str(metadata.get('original_path', '') or '').strip()}"
                    )
                    source_units_by_key[(duty_date, building)] = {
                        "duty_date": duty_date,
                        "building": building,
                        "source_file": str(file_path),
                    }

            source_units = [source_units_by_key[key] for key in sorted(source_units_by_key.keys())]
            if not source_units:
                raise RuntimeError("外网端未能解析到任何可继续上传的12项源文件")
            if not buildings:
                buildings = sorted({str(item.get("building", "") or "").strip() for item in source_units if str(item.get("building", "") or "").strip()})
            resume_job_id = self._resume_job_id_from_task(task)
            if resume_job_id:
                emit_log("[共享桥接][12项][外网] 共享文件已齐全，准备唤醒原任务")
                self._resume_bound_job(
                    task,
                    worker_payload={
                        "resume_kind": "shared_bridge_day_metric",
                        "selected_dates": selected_dates,
                        "buildings": buildings,
                        "source_units": source_units,
                        "building_scope": str(request.get("building_scope", "") or "").strip() or "all_enabled",
                        "building": str(request.get("building", "") or "").strip() or None,
                        "bridge_task_id": task_id,
                    },
                    summary="共享文件已到位，正在继续上传12项数据",
                )
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="external",
                    stage_result={"status": "resumed", "resume_job_id": resume_job_id},
                    next_task_status="success",
                    task_result={"status": "success", "bridge_task_id": task_id, "resume_job_id": resume_job_id, "internal": internal_result},
                )
                return
            emit_log("[共享桥接][12项][外网] 共享文件已齐全，开始继续上传")
            external_result = service.continue_from_source_files(
                selected_dates=selected_dates,
                buildings=buildings,
                source_units=source_units,
                building_scope=str(request.get("building_scope", "") or "").strip() or "all_enabled",
                building=str(request.get("building", "") or "").strip() or None,
                emit_log=emit_log,
            )
            raw_status = str(external_result.get("status", "") or "failed").strip().lower()
            final_status = "success" if raw_status in {"ok", "success"} else ("partial_failed" if raw_status == "partial_failed" else "failed")
            task_error = str(external_result.get("error", "") or external_result.get("last_error", "") or "").strip() if final_status in {"partial_failed", "failed"} else ""
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result=external_result,
                next_task_status=final_status,
                task_error=task_error,
                task_result={"status": final_status, "bridge_task_id": task_id, "internal": internal_result, "external": external_result},
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text, "internal": internal_result},
            )
            self._emit_system_log(f"[共享桥接][外网端] 任务={task_id} 12项外网继续失败: {error_text}")

    def _run_wet_bulb_internal_download(self, task: Dict[str, Any]) -> None:
        if not self._store:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "internal_download"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="internal", claim_token=claim_token)
        service = WetBulbCollectionService(
            self.runtime_config,
            download_browser_pool=self._internal_download_pool,
        )
        try:
            emit_log("[共享桥接][湿球温度][内网] 开始下载阶段")
            internal_result = service.download_source_units(
                buildings=request.get("buildings") if isinstance(request.get("buildings"), list) else None,
                emit_log=emit_log,
            )
            stage_result = dict(internal_result)
            internal_source_units = internal_result.get("source_units", []) if isinstance(internal_result.get("source_units", []), list) else []
            source_units = [
                {
                    "building": str(item.get("building", "") or "").strip(),
                    "file_path": str(item.get("file_path", "") or item.get("source_file", "") or "").strip(),
                }
                for item in internal_source_units
                if isinstance(item, dict)
                and str(item.get("building", "") or "").strip()
                and str(item.get("file_path", "") or item.get("source_file", "") or "").strip()
            ]
            stage_result["artifacts"] = []
            stage_result["artifact_count"] = 0
            stage_result["source_units"] = list(source_units)
            if source_units:
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="internal",
                    stage_result=stage_result,
                    next_task_status="ready_for_external",
                    task_result={"internal": stage_result, "status": "ready_for_external"},
                    record_event=False,
                    sync_mailbox=False,
                )
                self._store.append_event(
                    task_id=task_id,
                    stage_id=stage_id,
                    side="internal",
                    level="info",
                    event_type="await_external",
                    payload={"message": "内网下载完成，等待外网继续处理"},
                )
                self._request_runtime_status_refresh(reason=f"wet_bulb_internal_download_completed:{task_id}")
            else:
                failed_buildings = internal_result.get("failed_buildings", []) if isinstance(internal_result.get("failed_buildings", []), list) else []
                error_text = "内网下载未产生任何共享源文件"
                for item in failed_buildings:
                    if not isinstance(item, dict):
                        continue
                    error_text = str(item.get("error", "") or "").strip() or error_text
                    if error_text:
                        break
                self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="internal",
                    stage_result=stage_result,
                    stage_error=error_text,
                    next_task_status="failed",
                    task_error=error_text,
                    stage_status="failed",
                    task_result={"internal": stage_result, "status": "failed"},
                )
                self._request_runtime_status_refresh(reason=f"wet_bulb_internal_download_failed:{task_id}")
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text},
            )
            self._emit_system_log(f"[共享桥接][内网端] 任务={task_id} 湿球温度共享文件准备失败: {error_text}")
            self._request_runtime_status_refresh(reason=f"wet_bulb_internal_download_exception:{task_id}")

    def _run_wet_bulb_external_continue(self, task: Dict[str, Any]) -> None:
        if not self._store:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "external_extract_and_upload"
        claim_token = self._stage_claim_token(task, stage_id)
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="external", claim_token=claim_token)
        service = WetBulbCollectionService(self.runtime_config)
        try:
            internal_source_units = internal_result.get("source_units", []) if isinstance(internal_result.get("source_units", []), list) else []
            source_units: List[Dict[str, Any]] = []
            for item in internal_source_units:
                if not isinstance(item, dict):
                    continue
                building = str(item.get("building", "") or "").strip()
                file_path = str(item.get("file_path", "") or item.get("source_file", "") or "").strip()
                if not building or not file_path:
                    continue
                if not is_accessible_cached_file_path(file_path):
                    raise FileNotFoundError(f"共享目录中的湿球温度源文件不存在或不可访问: {file_path}")
                source_units.append({"building": building, "file_path": file_path})
            if not source_units:
                artifacts = self._store.get_artifacts(task_id, artifact_kind="source_file", status="ready")
                for item in artifacts:
                    relative_path = str(item.get("relative_path", "") or "").strip()
                    building = str(item.get("building", "") or "").strip()
                    if not relative_path or not building:
                        continue
                    file_path = self._resolve_ready_artifact_file_path(item)
                    if file_path is None:
                        raise FileNotFoundError(f"共享目录中的湿球温度源文件不存在或不可访问: {relative_path}")
                    source_units.append({"building": building, "file_path": str(file_path)})
            if not source_units:
                raise RuntimeError("共享目录中没有可继续处理的湿球温度源文件")
            resume_job_id = self._resume_job_id_from_task(task)
            if resume_job_id:
                emit_log("[共享桥接][湿球温度][外网] 共享文件已齐全，准备唤醒原任务")
                self._resume_bound_job(
                    task,
                    worker_payload={
                        "resume_kind": "shared_bridge_wet_bulb",
                        "source_units": source_units,
                        "bridge_task_id": task_id,
                    },
                    summary="共享文件已到位，正在继续处理湿球温度",
                )
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="external",
                    stage_result={"status": "resumed", "resume_job_id": resume_job_id},
                    next_task_status="success",
                    task_result={"status": "success", "bridge_task_id": task_id, "resume_job_id": resume_job_id, "internal": internal_result},
                )
                return
            emit_log("[共享桥接][湿球温度][外网] 共享文件已齐全，开始继续处理")
            external_result = service.continue_from_source_units(
                source_units=source_units,
                emit_log=emit_log,
            )
            raw_status = str(external_result.get("status", "") or "failed").strip().lower()
            final_status = "success" if raw_status in {"ok", "success"} else ("partial_failed" if raw_status == "partial_failed" else "failed")
            task_error = str(external_result.get("error", "") or external_result.get("last_error", "") or "").strip() if final_status in {"partial_failed", "failed"} else ""
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result=external_result,
                next_task_status=final_status,
                task_error=task_error,
                task_result={"status": final_status, "bridge_task_id": task_id, "internal": internal_result, "external": external_result},
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text, "internal": internal_result},
            )
            self._emit_system_log(f"[共享桥接][外网端] 任务={task_id} 湿球温度外网继续失败: {error_text}")


    def _run_alarm_event_upload_internal_fill(self, task: Dict[str, Any]) -> None:
        if not self._store or self._source_cache_service is None:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "internal_fill"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        mode = str(task.get("mode", "") or request.get("mode", "") or "").strip().lower() or "full"
        building = str(request.get("building", "") or "").strip()
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="internal", claim_token=claim_token)
        try:
            target_buildings = [building] if mode == "single_building" and building else self._source_cache_service.get_enabled_buildings()
            target_buildings = [item for item in target_buildings if item]
            selection = self._source_cache_service.get_alarm_event_upload_selection(
                building=building if mode == "single_building" else "",
            )
            selected_entries = [
                item
                for item in (selection.get("selected_entries", []) if isinstance(selection.get("selected_entries", []), list) else [])
                if isinstance(item, dict)
            ]
            ready_buildings = {
                str(item.get("building", "") or "").strip()
                for item in selected_entries
                if str(item.get("building", "") or "").strip()
            }
            missing_buildings = [item for item in target_buildings if item and item not in ready_buildings]
            target_bucket_key = str(request.get("target_bucket_key", "") or "").strip() or self.current_alarm_event_bucket()
            filled_entries: List[Dict[str, Any]] = []
            if missing_buildings:
                emit_log(
                    "[共享桥接][告警上传][内网] 开始补采缺失楼栋: "
                    f"mode={mode}, buildings={','.join(missing_buildings)}, bucket={target_bucket_key or '-'}"
                )
                for item in missing_buildings:
                    filled = self._source_cache_service.fill_alarm_event_latest(
                        building=item,
                        bucket_key=target_bucket_key,
                        emit_log=emit_log,
                    )
                    if isinstance(filled, dict):
                        filled_entries.append(dict(filled))
                selection = self._source_cache_service.get_alarm_event_upload_selection(
                    building=building if mode == "single_building" else "",
                )
                selected_entries = [
                    item
                    for item in (selection.get("selected_entries", []) if isinstance(selection.get("selected_entries", []), list) else [])
                    if isinstance(item, dict)
                ]
                ready_buildings = {
                    str(item.get("building", "") or "").strip()
                    for item in selected_entries
                    if str(item.get("building", "") or "").strip()
                }
                missing_buildings = [item for item in target_buildings if item and item not in ready_buildings]
            if missing_buildings:
                raise RuntimeError(f"告警共享文件补采后仍缺失楼栋: {','.join(missing_buildings)}")
            stage_result = {
                "status": "ready_for_external",
                "mode": mode,
                "building": building,
                "target_bucket_key": target_bucket_key,
                "requested_buildings": list(target_buildings),
                "selected_buildings": sorted(ready_buildings),
                "selection_reference_date": str(selection.get("selection_reference_date", "") or "").strip(),
                "selected_entry_count": len(selected_entries),
                "filled_buildings": [str(item.get("building", "") or "").strip() for item in filled_entries if str(item.get("building", "") or "").strip()],
            }
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result=stage_result,
                next_task_status="ready_for_external",
                task_result={"internal": stage_result, "status": "ready_for_external"},
                record_event=False,
                sync_mailbox=False,
            )
            self._store.append_event(
                task_id=task_id,
                stage_id=stage_id,
                side="internal",
                level="info",
                event_type="await_external",
                payload={"message": "告警共享文件已齐全，等待外网继续上传"},
            )
            self._request_runtime_status_refresh(reason=f"alarm_event_upload_internal_fill_completed:{task_id}")
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text},
            )
            self._emit_system_log(f"[共享桥接][内网端] 任务={task_id} 告警共享文件补采失败: {error_text}")
            self._request_runtime_status_refresh(reason=f"alarm_event_upload_internal_fill_exception:{task_id}")

    def _run_alarm_event_upload_external(self, task: Dict[str, Any]) -> None:
        if not self._store or self._source_cache_service is None:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "external_upload"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        mode = str(task.get("mode", "") or request.get("mode", "") or "").strip().lower() or "full"
        building = str(request.get("building", "") or "").strip()
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="external", claim_token=claim_token)
        try:
            selection = self._source_cache_service.get_alarm_event_upload_selection(
                building=building if mode == "single_building" else "",
            )
            target_buildings = [building] if mode == "single_building" and building else self._source_cache_service.get_enabled_buildings()
            target_buildings = [item for item in target_buildings if item]
            ready_buildings = {
                str(item.get("building", "") or "").strip()
                for item in (selection.get("selected_entries", []) if isinstance(selection.get("selected_entries", []), list) else [])
                if isinstance(item, dict) and str(item.get("building", "") or "").strip()
            }
            missing_buildings = [item for item in target_buildings if item and item not in ready_buildings]
            if missing_buildings:
                self._requeue_external_waiting_task(
                    task_id=task_id,
                    stage_id=stage_id,
                    side="external",
                    wait_message="等待内网补采同步",
                    detail=f"告警共享文件仍未齐全，等待内网补采同步后自动继续。楼栋={','.join(missing_buildings)}",
                )
                return
            resume_job_id = self._resume_job_id_from_task(task)
            if resume_job_id:
                emit_log("[共享桥接][告警上传][外网] 共享文件已齐全，准备唤醒原任务")
                self._resume_bound_job(
                    task,
                    worker_payload={
                        "resume_kind": "shared_bridge_alarm_event_upload",
                        "mode": mode,
                        "building": building or None,
                        "bridge_task_id": task_id,
                    },
                    summary="共享文件已到位，正在继续上传告警信息",
                )
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="external",
                    stage_result={"status": "resumed", "resume_job_id": resume_job_id},
                    next_task_status="success",
                    task_result={"status": "success", "bridge_task_id": task_id, "resume_job_id": resume_job_id, "internal": internal_result},
                )
                return
            emit_log("[共享桥接][告警上传][外网] 共享文件已齐全，开始继续上传")
            if mode == "single_building":
                external_result = self.upload_alarm_event_source_cache_single_building_to_bitable(
                    building=building,
                    emit_log=emit_log,
                )
            else:
                external_result = self.upload_alarm_event_source_cache_full_to_bitable(
                    emit_log=emit_log,
                )
            accepted = bool(external_result.get("accepted"))
            reason = str(external_result.get("reason", "") or "").strip().lower()
            if not accepted and reason == "already_running":
                self._requeue_external_waiting_task(
                    task_id=task_id,
                    stage_id=stage_id,
                    side="external",
                    wait_message="等待当前告警上传完成",
                    detail="告警上传资源当前正忙，稍后自动继续。",
                )
                return
            if not accepted:
                raise RuntimeError(str(external_result.get("error", "") or "").strip() or "告警信息上传失败")
            final_status = "success"
            if reason == "partial_completed":
                final_status = "partial_failed"
            task_error = (
                str(external_result.get("error", "") or external_result.get("last_error", "") or "").strip()
                if final_status in {"partial_failed", "failed"}
                else ""
            )
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result=external_result,
                next_task_status=final_status,
                task_error=task_error,
                task_result={"status": final_status, "bridge_task_id": task_id, "internal": internal_result, "external": external_result},
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text, "internal": internal_result},
            )
            self._emit_system_log(f"[共享桥接][外网端] 任务={task_id} 告警信息上传失败: {error_text}")


    def _run_handover_cache_fill_internal(self, task: Dict[str, Any]) -> None:
        if not self._store or self._source_cache_service is None:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "internal_fill"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        continuation_kind = str(request.get("continuation_kind", "") or "").strip().lower()
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="internal", claim_token=claim_token)
        try:
            if continuation_kind == "handover":
                buildings = [
                    str(item or "").strip()
                    for item in (request.get("buildings", []) if isinstance(request.get("buildings", []), list) else [])
                    if str(item or "").strip()
                ]
                if not buildings:
                    buildings = self._source_cache_service.get_enabled_buildings()
                duty_date = str(request.get("duty_date", "") or "").strip()
                duty_shift = str(request.get("duty_shift", "") or "").strip().lower()
                if not duty_date or not duty_shift:
                    raise RuntimeError("交接班历史缓存补采缺少日期或班次")
                handover_entries = self._source_cache_service.fill_handover_history(
                    buildings=buildings,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    emit_log=emit_log,
                )
                capacity_entries = self._source_cache_service.fill_handover_capacity_history(
                    buildings=buildings,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    emit_log=emit_log,
                )
                cached_entries = list(handover_entries) + list(capacity_entries)
            elif continuation_kind == "day_metric":
                selected_dates = [
                    str(item or "").strip()
                    for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates", []), list) else [])
                    if str(item or "").strip()
                ]
                if not selected_dates:
                    raise RuntimeError("12项历史缓存补采缺少日期列表")
                cached_entries = self._source_cache_service.fill_day_metric_history(
                    selected_dates=selected_dates,
                    building_scope=str(request.get("building_scope", "") or "").strip() or "all_enabled",
                    building=str(request.get("building", "") or "").strip() or None,
                    emit_log=emit_log,
                )
            else:
                raise RuntimeError(f"不支持的共享缓存补采类型: {continuation_kind or '-'}")
            if not cached_entries:
                if continuation_kind == "day_metric":
                    raise RuntimeError("缺少可复用的交接班源文件")
                raise RuntimeError("共享缓存补采未生成任何源文件")
            stage_result = {
                "status": "ready_for_external",
                "continuation_kind": continuation_kind,
                "cached_entries": cached_entries,
                "cached_count": len(cached_entries),
            }
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result=stage_result,
                next_task_status="ready_for_external",
                task_result={"status": "ready_for_external", "internal": stage_result},
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text},
            )

    def _run_handover_cache_fill_external(self, task: Dict[str, Any]) -> None:
        if not self._store or self._source_cache_service is None:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "external_continue"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        continuation_kind = str(request.get("continuation_kind", "") or "").strip().lower()
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="external", claim_token=claim_token)
        try:
            resume_job_id = self._resume_job_id_from_task(task)
            if continuation_kind == "handover":
                buildings = [
                    str(item or "").strip()
                    for item in (request.get("buildings", []) if isinstance(request.get("buildings", []), list) else [])
                    if str(item or "").strip()
                ]
                if not buildings:
                    buildings = self._source_cache_service.get_enabled_buildings()
                duty_date = str(request.get("duty_date", "") or "").strip()
                duty_shift = str(request.get("duty_shift", "") or "").strip().lower()
                cached_entries = self._source_cache_service.get_handover_by_date_entries(
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    buildings=buildings,
                )
                capacity_entries = self._source_cache_service.get_handover_capacity_by_date_entries(
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    buildings=buildings,
                )
                if len(cached_entries) < len(buildings) or len(capacity_entries) < len(buildings):
                    self._requeue_external_waiting_task(
                        task_id=task_id,
                        stage_id=stage_id,
                        side="external",
                        wait_message="等待内网补采同步",
                        detail=f"交接班历史缓存未齐全，等待内网补采同步后自动继续。日期={duty_date} 班次={duty_shift}",
                    )
                    return
                building_files = [(str(item.get("building", "")).strip(), str(item.get("file_path", "")).strip()) for item in cached_entries]
                capacity_building_files = [
                    (str(item.get("building", "")).strip(), str(item.get("file_path", "")).strip())
                    for item in capacity_entries
                ]
                if resume_job_id:
                    emit_log("[共享桥接][交接班历史补采][外网] 共享文件已齐全，准备唤醒原任务")
                    self._resume_bound_job(
                        task,
                        worker_payload={
                            "resume_kind": "shared_bridge_handover",
                            "building_files": [{"building": building, "file_path": file_path} for building, file_path in building_files],
                            "capacity_building_files": [
                                {"building": building, "file_path": file_path}
                                for building, file_path in capacity_building_files
                            ],
                            "end_time": None,
                            "duty_date": duty_date or None,
                            "duty_shift": duty_shift or None,
                            "bridge_task_id": task_id,
                        },
                        summary="共享文件已到位，正在继续生成交接班日志",
                    )
                    self._store.complete_stage(
                        task_id=task_id,
                        stage_id=stage_id,
                        claim_token=claim_token,
                        side="external",
                        stage_result={"status": "resumed", "resume_job_id": resume_job_id},
                        next_task_status="success",
                        task_result={"status": "success", "bridge_task_id": task_id, "resume_job_id": resume_job_id, "internal": internal_result},
                    )
                    return
                external_result = OrchestratorService(self.runtime_config).run_handover_from_files(
                    building_files=building_files,
                    capacity_building_files=capacity_building_files,
                    end_time=None,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    emit_log=emit_log,
                )
                success_count = int(external_result.get("success_count", 0) or 0)
                failed_count = int(external_result.get("failed_count", 0) or 0)
                final_status = "partial_failed" if success_count > 0 and failed_count > 0 else ("success" if failed_count <= 0 else "failed")
            elif continuation_kind == "day_metric":
                selected_dates = [
                    str(item or "").strip()
                    for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates", []), list) else [])
                    if str(item or "").strip()
                ]
                building_scope = str(request.get("building_scope", "") or "").strip() or "all_enabled"
                target_buildings = [str(request.get("building", "") or "").strip()] if building_scope == "single" else self._source_cache_service.get_enabled_buildings()
                target_buildings = [item for item in target_buildings if item]
                cached_entries = self._source_cache_service.get_day_metric_by_date_entries(
                    selected_dates=selected_dates,
                    buildings=target_buildings,
                )
                if len(cached_entries) < len(selected_dates) * max(1, len(target_buildings)):
                    self._requeue_external_waiting_task(
                        task_id=task_id,
                        stage_id=stage_id,
                        side="external",
                        wait_message="等待内网补采同步",
                        detail=f"12项历史缓存未齐全，等待内网补采同步后自动继续。日期={','.join(selected_dates)}",
                    )
                    return
                source_units = [
                    {
                        "duty_date": str(item.get("duty_date", "") or "").strip(),
                        "building": str(item.get("building", "") or "").strip(),
                        "source_file": str(item.get("file_path", "") or "").strip(),
                    }
                    for item in cached_entries
                ]
                if resume_job_id:
                    emit_log("[共享桥接][12项历史补采][外网] 共享文件已齐全，准备唤醒原任务")
                    self._resume_bound_job(
                        task,
                        worker_payload={
                            "resume_kind": "shared_bridge_day_metric",
                            "selected_dates": selected_dates,
                            "buildings": target_buildings,
                            "source_units": source_units,
                            "building_scope": building_scope,
                            "building": str(request.get("building", "") or "").strip() or None,
                            "bridge_task_id": task_id,
                        },
                        summary="共享文件已到位，正在继续上传12项数据",
                    )
                    self._store.complete_stage(
                        task_id=task_id,
                        stage_id=stage_id,
                        claim_token=claim_token,
                        side="external",
                        stage_result={"status": "resumed", "resume_job_id": resume_job_id},
                        next_task_status="success",
                        task_result={"status": "success", "bridge_task_id": task_id, "resume_job_id": resume_job_id, "internal": internal_result},
                    )
                    return
                service = DayMetricStandaloneUploadService(self.runtime_config)
                external_result = service.continue_from_source_files(
                    selected_dates=selected_dates,
                    buildings=target_buildings,
                    source_units=source_units,
                    building_scope=building_scope,
                    building=str(request.get("building", "") or "").strip() or None,
                    emit_log=emit_log,
                )
                raw_status = str(external_result.get("status", "") or "failed").strip().lower()
                final_status = "success" if raw_status in {"ok", "success"} else ("partial_failed" if raw_status == "partial_failed" else "failed")
            else:
                raise RuntimeError(f"不支持的共享缓存补采类型: {continuation_kind or '-'}")
            merged_result = {
                "bridge_task_id": task_id,
                "status": final_status,
                "internal": internal_result,
                "external": external_result,
            }
            task_error = ""
            if final_status in {"partial_failed", "failed"}:
                task_error = str(external_result.get("error", "") or external_result.get("last_error", "") or "").strip()
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result=external_result,
                next_task_status=final_status,
                task_error=task_error,
                task_result=merged_result,
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text, "internal": internal_result},
            )

    def _run_monthly_cache_fill_internal(self, task: Dict[str, Any]) -> None:
        if not self._store or self._source_cache_service is None:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "internal_fill"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="internal", claim_token=claim_token)
        try:
            selected_dates = [
                str(item or "").strip()
                for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates", []), list) else [])
                if str(item or "").strip()
            ]
            if not selected_dates:
                raise RuntimeError("月报历史缓存补采缺少日期列表")
            cached_entries = self._source_cache_service.fill_monthly_history(
                selected_dates=selected_dates,
                emit_log=emit_log,
            )
            if not cached_entries:
                raise RuntimeError("月报历史缓存补采未生成任何源文件")
            stage_result = {
                "status": "ready_for_external",
                "selected_dates": selected_dates,
                "cached_entries": cached_entries,
                "cached_count": len(cached_entries),
            }
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result=stage_result,
                next_task_status="ready_for_external",
                task_result={"status": "ready_for_external", "internal": stage_result},
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text},
            )

    def _run_monthly_cache_fill_external(self, task: Dict[str, Any]) -> None:
        if not self._store or self._source_cache_service is None:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "external_continue"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="external", claim_token=claim_token)
        try:
            resume_job_id = self._resume_job_id_from_task(task)
            selected_dates = [
                str(item or "").strip()
                for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates", []), list) else [])
                if str(item or "").strip()
            ]
            cached_entries = self._source_cache_service.get_monthly_by_date_entries(selected_dates=selected_dates)
            expected = len(selected_dates) * len(self._source_cache_service.get_enabled_buildings())
            if len(cached_entries) < expected:
                self._requeue_external_waiting_task(
                    task_id=task_id,
                    stage_id=stage_id,
                    side="external",
                    wait_message="等待内网补采同步",
                    detail=f"月报历史缓存未齐全，等待内网补采同步后自动继续。日期={','.join(selected_dates)}",
                )
                return
            file_items = [
                {
                    "building": str(item.get("building", "") or "").strip(),
                    "file_path": str(item.get("file_path", "") or "").strip(),
                    "upload_date": str(item.get("metadata", {}).get("upload_date", "") or item.get("duty_date", "") or "").strip(),
                }
                for item in cached_entries
            ]
            if resume_job_id:
                emit_log("[共享桥接][月报历史补采][外网] 共享文件已齐全，准备唤醒原任务")
                self._resume_bound_job(
                    task,
                    worker_payload={
                        "resume_kind": "shared_bridge_monthly_multi_date",
                        "selected_dates": selected_dates,
                        "file_items": file_items,
                        "source_label": "月报历史共享文件",
                        "bridge_task_id": task_id,
                    },
                    summary="共享文件已到位，正在继续处理多日期月报",
                )
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="external",
                    stage_result={"status": "resumed", "resume_job_id": resume_job_id},
                    next_task_status="success",
                    task_result={"status": "success", "bridge_task_id": task_id, "resume_job_id": resume_job_id, "internal": internal_result},
                )
                return
            external_result = run_monthly_from_file_items(
                self.runtime_config,
                file_items=file_items,
                emit_log=emit_log,
                source_label="月报共享缓存",
            )
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result=external_result,
                next_task_status="success",
                task_result={"status": "success", "bridge_task_id": task_id, "internal": internal_result, "external": external_result},
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text, "internal": internal_result},
            )

    def _retire_disabled_feature_task(self, task: Dict[str, Any], *, feature: str, error_text: str) -> None:
        task_id = str(task.get("task_id", "") or "").strip()
        self._fail_claimed_task(task, error_text=error_text, event_type="retired_feature", level="warning")
        self._emit_system_log(f"[共享桥接] 已拦截停用功能任务: task_id={task_id}, feature={feature}")

    def _run_monthly_internal_download(self, task: Dict[str, Any]) -> None:
        if not self._store:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "internal_download"
        claim_token = self._stage_claim_token(task, stage_id)
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        mode = str(task.get("mode", "") or "").strip().lower()
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="internal", claim_token=claim_token)
        try:
            if mode == "auto_once" and self._source_cache_service is not None:
                target_bucket_key = str(request.get("target_bucket_key", "") or "").strip() or self.current_source_cache_bucket()
                emit_log(
                    "[共享桥接][月报][内网] 开始 canonical 源文件登记阶段, "
                    f"bucket={target_bucket_key or '-'}"
                )
                target_buildings = [
                    str(item or "").strip()
                    for item in self._source_cache_service.get_enabled_buildings()
                    if str(item or "").strip()
                ]
                cached_entries: List[Dict[str, Any]] = []
                for building in target_buildings:
                    cached_entries.extend(
                        self._source_cache_service.fill_monthly_latest(
                            building=building,
                            bucket_key=target_bucket_key,
                            emit_log=emit_log,
                        )
                    )
                if not cached_entries:
                    raise RuntimeError("月报 canonical 源文件登记未生成任何共享源文件")
                resume_artifact = self._save_monthly_resume_state_artifact(
                    task_id=task_id,
                    payload={
                        "run_id": "",
                        "run_save_dir": "",
                        "pending_upload_count": 0,
                        "source_root": "",
                        "resume_root": str(resolve_monthly_bridge_resume_root(self.shared_bridge_root)),
                        "mode": mode,
                        "target_bucket_key": target_bucket_key,
                    },
                    emit_log=emit_log,
                )
                stage_result = {
                    "status": "ready_for_external",
                    "mode": mode,
                    "target_bucket_key": target_bucket_key,
                    "cached_entries": cached_entries,
                    "cached_count": len(cached_entries),
                    "resume_root": str(resolve_monthly_bridge_resume_root(self.shared_bridge_root)),
                    "resume_artifact": resume_artifact,
                    "artifact_count": 1,
                }
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="internal",
                    stage_result=stage_result,
                    next_task_status="ready_for_external",
                    task_result={"internal": stage_result, "status": "ready_for_external"},
                    record_event=False,
                    sync_mailbox=False,
                )
                self._store.append_event(
                    task_id=task_id,
                    stage_id=stage_id,
                    side="internal",
                    level="info",
                    event_type="await_external",
                    payload={"message": "月报内网阶段完成，等待外网继续断点续传"},
                )
                return
            if mode == "auto_once":
                emit_log("[共享桥接][月报][内网] 开始自动流程下载阶段")
                internal_result = run_bridge_download_only_auto_once(
                    self.runtime_config,
                    shared_root_dir=self.shared_bridge_root,
                    task_id=task_id,
                    source_name="shared_bridge_monthly_auto_once",
                )
            elif mode == "multi_date":
                selected_dates = [
                    str(item or "").strip()
                    for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates", []), list) else [])
                    if str(item or "").strip()
                ]
                if not selected_dates:
                    raise RuntimeError("月报共享桥接多日期任务缺少日期列表")
                emit_log(
                    "[共享桥接][月报][内网] 开始多日期下载阶段, "
                    f"日期={','.join(selected_dates)}"
                )
                internal_result = run_bridge_download_only_multi_date(
                    self.runtime_config,
                    shared_root_dir=self.shared_bridge_root,
                    task_id=task_id,
                    selected_dates=selected_dates,
                    source_name="shared_bridge_monthly_multi_date",
                )
            else:
                raise RuntimeError(f"未支持的月报共享桥接模式: {mode or '-'}")

            run_id = str(internal_result.get("run_id", "") or "").strip()
            if not run_id:
                raise RuntimeError("月报共享桥接内网阶段缺少 run_id")
            run_save_dir = str(internal_result.get("run_save_dir", "") or "").strip()
            pending_upload_count = int(internal_result.get("pending_upload_count", 0) or 0)
            source_root = str(resolve_monthly_bridge_source_root(self.shared_bridge_root, task_id))
            resume_root = str(resolve_monthly_bridge_resume_root(self.shared_bridge_root))
            resume_artifact = self._save_monthly_resume_state_artifact(
                task_id=task_id,
                payload={
                    "run_id": run_id,
                    "run_save_dir": run_save_dir,
                    "pending_upload_count": pending_upload_count,
                    "source_root": source_root,
                    "resume_root": resume_root,
                    "mode": mode,
                    "selected_dates": list(request.get("selected_dates", []))
                    if isinstance(request.get("selected_dates", []), list)
                    else [],
                },
                emit_log=emit_log,
            )

            stage_result = dict(internal_result)
            stage_result["mode"] = mode
            stage_result["source_root"] = source_root
            stage_result["resume_root"] = resume_root
            stage_result["resume_artifact"] = resume_artifact
            stage_result["artifact_count"] = 1
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result=stage_result,
                next_task_status="ready_for_external",
                task_result={"internal": stage_result, "status": "ready_for_external"},
                record_event=False,
                sync_mailbox=False,
            )
            self._store.append_event(
                task_id=task_id,
                stage_id=stage_id,
                side="internal",
                level="info",
                event_type="await_external",
                payload={"message": "月报内网阶段完成，等待外网继续断点续传"},
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="internal",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text},
            )
            self._emit_system_log(f"[共享桥接][内网端] 任务={task_id} 月报内网阶段失败: {error_text}")

    def _run_monthly_external_resume(self, task: Dict[str, Any]) -> None:
        if not self._store:
            return
        task_id = str(task.get("task_id", "") or "").strip()
        stage_id = "external_resume"
        claim_token = self._stage_claim_token(task, stage_id)
        mode = str(task.get("mode", "") or "").strip().lower()
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="external", claim_token=claim_token)
        try:
            resume_job_id = self._resume_job_id_from_task(task)
            if resume_job_id and mode == "resume_upload":
                emit_log("[共享桥接][月报续传][外网] 准备唤醒原任务继续续传")
                self._resume_bound_job(
                    task,
                    worker_payload={
                        "run_id": str(request.get("run_id", "") or "").strip() or None,
                        "auto_trigger": bool(request.get("auto_trigger", False)),
                        "bridge_task_id": task_id,
                    },
                    summary="共享续传状态已就绪，正在继续断点续传",
                )
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="external",
                    stage_result={"status": "resumed", "resume_job_id": resume_job_id},
                    next_task_status="success",
                    task_result={"status": "success", "bridge_task_id": task_id, "resume_job_id": resume_job_id, "internal": internal_result},
                )
                return
            if not resume_job_id and mode == "resume_upload":
                emit_log("[共享桥接][月报][外网] 开始继续执行续传上传")
                external_result = run_bridge_resume_upload(
                    self.runtime_config,
                    shared_root_dir=self.shared_bridge_root,
                    run_id=str(request.get("run_id", "") or "").strip() or None,
                    auto_trigger=bool(request.get("auto_trigger", False)),
                )
                raw_status = str(external_result.get("status", "") or "failed").strip().lower()
                final_status = "success" if raw_status in {"ok", "success"} else ("partial_failed" if raw_status == "partial_failed" else "failed")
                task_error = str(external_result.get("error", "") or external_result.get("last_error", "") or "").strip() if final_status in {"partial_failed", "failed"} else ""
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="external",
                    stage_result=external_result,
                    next_task_status=final_status,
                    task_error=task_error,
                    task_result={"status": final_status, "bridge_task_id": task_id, "internal": internal_result, "external": external_result},
                )
                return
            file_items = []
            if mode == "auto_once" and self._source_cache_service is not None:
                target_bucket_key = (
                    str(request.get("target_bucket_key", "") or "").strip()
                    or str(internal_result.get("target_bucket_key", "") or "").strip()
                    or self.current_source_cache_bucket()
                )
                target_buildings = [
                    str(item or "").strip()
                    for item in self._source_cache_service.get_enabled_buildings()
                    if str(item or "").strip()
                ]
                cached_entries = self._source_cache_service.get_latest_ready_entries(
                    source_family=FAMILY_MONTHLY_REPORT,
                    buildings=target_buildings,
                    bucket_key=target_bucket_key,
                )
                for item in cached_entries:
                    if not isinstance(item, dict):
                        continue
                    building = str(item.get("building", "") or "").strip()
                    file_path = str(item.get("file_path", "") or "").strip()
                    upload_date = str(item.get("metadata", {}).get("upload_date", "") or item.get("duty_date", "") or "").strip()
                    if not building or not file_path:
                        continue
                    file_items.append({"building": building, "file_path": file_path, "upload_date": upload_date})
                if target_buildings and len(file_items) < len(target_buildings):
                    source_root = str(internal_result.get("source_root", "") or "").strip()
                    if not source_root:
                        self._requeue_external_waiting_task(
                            task_id=task_id,
                            stage_id=stage_id,
                            side="external",
                            wait_message="等待内网补采同步",
                            detail=f"月报 canonical 共享文件暂未齐全，等待共享目录同步后自动继续。bucket={target_bucket_key or '-'}",
                        )
                        return
            if not file_items:
                source_root = str(internal_result.get("source_root", "") or "").strip()
                if not source_root:
                    raise RuntimeError("月报共享桥接缺少 source_root")
                source_root_path = Path(source_root)
                for file_path in sorted(source_root_path.rglob("*.xlsx")):
                    if not file_path.is_file():
                        continue
                    building = str(file_path.stem.split("--")[-1] if "--" in file_path.stem else "").strip()
                    upload_date = str(file_path.parent.name.split("--")[0] if "--" in file_path.parent.name else "").strip()
                    file_items.append({"building": building, "file_path": str(file_path), "upload_date": upload_date})
            if not file_items:
                raise RuntimeError("月报共享桥接未找到可继续处理的共享源文件")
            if resume_job_id:
                emit_log("[共享桥接][月报][外网] 共享文件已齐全，准备唤醒原任务")
                self._resume_bound_job(
                    task,
                    worker_payload={
                        "resume_kind": "shared_bridge_monthly_auto_once",
                        "file_items": file_items,
                        "source": str(request.get("source", "") or "共享桥接月报自动流程").strip() or "共享桥接月报自动流程",
                        "bridge_task_id": task_id,
                    },
                    summary="共享文件已到位，正在继续处理月报自动流程",
                )
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="external",
                    stage_result={"status": "resumed", "resume_job_id": resume_job_id},
                    next_task_status="success",
                    task_result={"status": "success", "bridge_task_id": task_id, "resume_job_id": resume_job_id, "internal": internal_result},
                )
                return
            emit_log("[共享桥接][月报][外网] 共享文件已齐全，开始继续处理")
            external_result = run_monthly_from_file_items(
                self.runtime_config,
                file_items=file_items,
                emit_log=emit_log,
                source_label=str(request.get("source", "") or "共享桥接月报自动流程").strip() or "共享桥接月报自动流程",
            )
            raw_status = str(external_result.get("status", "") or "failed").strip().lower()
            final_status = "success" if raw_status in {"ok", "success"} else ("partial_failed" if raw_status == "partial_failed" else "failed")
            task_error = str(external_result.get("error", "") or external_result.get("last_error", "") or "").strip() if final_status in {"partial_failed", "failed"} else ""
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result=external_result,
                next_task_status=final_status,
                task_error=task_error,
                task_result={"status": final_status, "bridge_task_id": task_id, "internal": internal_result, "external": external_result},
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            self._fail_bound_job(task, error_text=error_text, summary="等待内网补采同步失败")
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result={"status": "failed", "error": error_text},
                stage_error=error_text,
                next_task_status="failed",
                task_error=error_text,
                stage_status="failed",
                task_result={"status": "failed", "error": error_text, "internal": internal_result},
            )
            self._emit_system_log(f"[共享桥接][外网端] 任务={task_id} 月报外网阶段失败: {error_text}")

    def _requeue_external_waiting_task(
        self,
        *,
        task_id: str,
        stage_id: str,
        side: str,
        wait_message: str,
        detail: str,
    ) -> None:
        if not self._store:
            return
        task_text = str(task_id or "").strip()
        stage_text = str(stage_id or "").strip()
        if not task_text or not stage_text:
            return
        retried = self._store.retry_task(task_text, record_event=False, sync_mailbox=False)
        payload = {
            "message": wait_message,
            "detail": detail,
            "next_status": "ready_for_external",
        }
        self._store.append_event(
            task_id=task_text,
            stage_id=stage_text,
            side=str(side or "").strip() or "external",
            level="info",
            event_type="waiting_source_sync",
            payload=payload,
        )
        if retried:
            self._emit_system_log(f"[共享桥接][外网端] 任务={task_text} {detail}")

    def _should_keep_waiting_job_for_retry(self, task: Dict[str, Any], exc: Exception) -> bool:
        feature = str(task.get("feature", "") or "").strip().lower()
        mode = str(task.get("mode", "") or "").strip().lower()
        if feature != "monthly_report_pipeline" or mode != "auto_once":
            return False
        error_text = str(exc or "").strip()
        if not error_text:
            return False
        return any(
            marker in error_text
            for marker in (
                "月报共享桥接缺少可继续处理的 canonical 源文件",
                "月报共享桥接缺少 source_root",
                "月报共享桥接未找到可继续处理的共享源文件",
                "月报共享源文件不存在或不可访问",
            )
        )

    def _reconcile_waiting_jobs(self) -> None:
        if self._job_service is None or not self._store:
            return
        try:
            waiting_jobs = self._job_service.list_jobs(limit=300, statuses=["waiting_resource"])
        except Exception:
            return
        for snapshot in waiting_jobs if isinstance(waiting_jobs, list) else []:
            if not isinstance(snapshot, dict):
                continue
            if str(snapshot.get("wait_reason", "") or "").strip().lower() != "waiting:shared_bridge":
                continue
            job_id = str(snapshot.get("job_id", "") or "").strip()
            bridge_task_id = str(snapshot.get("bridge_task_id", "") or "").strip()
            if not job_id or not bridge_task_id:
                continue
            task = self.get_task(bridge_task_id)
            if not task:
                self._job_service.fail_waiting_job(
                    job_id,
                    error_text="绑定补采任务丢失",
                    summary="等待内网补采同步失败",
                )
                continue
            status = str(task.get("status", "") or "").strip().lower()
            if status == "success":
                try:
                    resume_delay_ms = 0
                    updated_at = str(task.get("updated_at", "") or task.get("created_at", "")).strip()
                    if updated_at:
                        try:
                            resume_delay_ms = int((datetime.now() - datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")).total_seconds() * 1000)
                        except ValueError:
                            resume_delay_ms = 0
                    self._emit_system_log(
                        f"[共享桥接] 任务={bridge_task_id} 开始恢复 waiting job: job={job_id}, resume_delay_ms={max(0, resume_delay_ms)}"
                    )
                    binding = self._build_waiting_job_resume_binding(task)
                    if binding.get("log_text"):
                        self._emit_system_log(str(binding.get("log_text", "")).strip())
                    self._job_service.resume_waiting_worker_job(
                        job_id,
                        worker_payload=binding.get("worker_payload") if isinstance(binding.get("worker_payload"), dict) else {},
                        summary=str(binding.get("summary", "") or "共享文件已到位，正在继续处理").strip() or "共享文件已到位，正在继续处理",
                    )
                except Exception as exc:  # noqa: BLE001
                    if self._should_keep_waiting_job_for_retry(task, exc):
                        self._emit_system_log(
                            f"[共享桥接] 任务={bridge_task_id} 月报 canonical 文件暂未可见，保留 waiting job 稍后重试: {exc}"
                        )
                        continue
                    self._job_service.fail_waiting_job(
                        job_id,
                        error_text=f"共享桥接已完成，但自动恢复原任务失败: {exc}",
                        summary="等待内网补采同步失败",
                    )
                continue
            if status in {"failed", "partial_failed", "cancelled"}:
                error_text = str(task.get("error", "") or "").strip() or f"绑定补采任务已{status}"
                self._job_service.fail_waiting_job(
                    job_id,
                    error_text=error_text,
                    summary="等待内网补采同步失败",
                )

    def _refresh_runtime_task_cache(self, *, limit: int = 500) -> None:
        mailbox_tasks = self._list_mailbox_tasks(limit=limit)
        if mailbox_tasks:
            self._counts.update(self._task_counts_from_tasks(mailbox_tasks))
            self._cache_task_list(mailbox_tasks)
            return
        mirrored_tasks = self._mirror_store.list_tasks(limit=limit) if self._mirror_store is not None else []
        if mirrored_tasks:
            self._counts.update(self._task_counts_from_tasks(mirrored_tasks))
        else:
            self._counts.update({"pending_internal": 0, "pending_external": 0, "problematic": 0, "total_count": 0})

    def _process_claimed_task(self, task: Dict[str, Any]) -> None:
        feature = str(task.get("feature", "") or "").strip()
        if feature == "handover_from_download":
            if self.role_mode == "internal":
                self._run_handover_internal_download(task)
                return
            if self.role_mode == "external":
                self._run_handover_external_continue(task)
                return
        if feature == "day_metric_from_download":
            if self.role_mode == "internal":
                self._run_day_metric_internal_download(task)
                return
            if self.role_mode == "external":
                self._run_day_metric_external_continue(task)
                return
        if feature == "wet_bulb_collection":
            if self.role_mode == "internal":
                self._run_wet_bulb_internal_download(task)
                return
            if self.role_mode == "external":
                self._run_wet_bulb_external_continue(task)
                return
        if feature == "alarm_event_upload":
            if self.role_mode == "internal":
                self._run_alarm_event_upload_internal_fill(task)
                return
            if self.role_mode == "external":
                self._run_alarm_event_upload_external(task)
                return
        if feature == "handover_cache_fill":
            if self.role_mode == "internal":
                self._run_handover_cache_fill_internal(task)
                return
            if self.role_mode == "external":
                self._run_handover_cache_fill_external(task)
                return
        if feature == "monthly_cache_fill":
            if self.role_mode == "internal":
                self._run_monthly_cache_fill_internal(task)
                return
            if self.role_mode == "external":
                self._run_monthly_cache_fill_external(task)
                return
        if feature in RETIRED_SHARED_BRIDGE_FEATURES:
            self._retire_disabled_feature_task(
                task,
                feature=feature,
                error_text=RETIRED_SHARED_BRIDGE_FEATURES[feature],
            )
            return
        if feature == "monthly_report_pipeline":
            if self.role_mode == "internal":
                self._run_monthly_internal_download(task)
                return
            if self.role_mode == "external":
                self._run_monthly_external_resume(task)
                return
        if feature == self.INTERNAL_BROWSER_ALERT_FEATURE:
            if self.role_mode == "external":
                self._run_internal_browser_alert_external(task)
                return
        error_text = f"共享桥接未识别或不支持的任务类型: 功能={feature}, 角色={_role_label(self.role_mode)}"
        self._fail_claimed_task(task, error_text=error_text, event_type="unsupported_feature", level="error")
        self._emit_system_log(f"[共享桥接] {error_text}")

    def _process_one_task_if_needed(self) -> bool:
        if not self._store:
            return False
        task = self._store.claim_next_task(
            role_target=self.role_mode,
            node_id=self.node_id,
            lease_sec=self.claim_lease_sec,
        )
        if not task:
            return False
        task_id = str(task.get("task_id", "") or "").strip()
        created_at = str(task.get("created_at", "") or "").strip()
        claim_delay_ms = 0
        if created_at:
            try:
                claim_delay_ms = int((datetime.now() - datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")).total_seconds() * 1000)
            except ValueError:
                claim_delay_ms = 0
        self._emit_system_log(
            f"[共享桥接] 任务开始处理: task={task_id or '-'}, feature={str(task.get('feature', '') or '').strip() or '-'}, claim_delay_ms={max(0, claim_delay_ms)}"
        )
        self._business_task_started()
        try:
            self._process_claimed_task(task)
        finally:
            self._business_task_finished()
        return True

    def _loop(self) -> None:
        next_heartbeat = 0.0
        next_cleanup = 0.0
        next_source_cache_sweep = 0.0
        next_source_cache_prewarm = 0.0
        next_artifact_self_heal = 0.0
        next_internal_alert_refresh = 0.0
        next_waiting_job_reconcile = 0.0
        next_mailbox_summary_refresh = 0.0
        while not self._stop_event.is_set():
            now_monotonic = time.monotonic()
            woke = self._wake_event.is_set()
            if woke:
                self._wake_event.clear()
            try:
                if not self._store:
                    self._db_status = "misconfigured"
                    self._last_error = "共享桥接目录未配置"
                else:
                    self._store.ensure_ready()
                    if now_monotonic >= next_heartbeat:
                        self._touch_node()
                        next_heartbeat = now_monotonic + self.heartbeat_interval_sec
                    processed_any = False
                    if woke:
                        for _ in range(20):
                            if not self._process_one_task_if_needed():
                                break
                            processed_any = True
                            self._reconcile_waiting_jobs()
                        if processed_any:
                            next_waiting_job_reconcile = now_monotonic + self.WAITING_JOB_RECONCILE_INTERVAL_SEC
                    if now_monotonic >= next_cleanup:
                        self._run_housekeeping()
                        next_cleanup = now_monotonic + self.CLEANUP_INTERVAL_SEC
                    if now_monotonic >= next_source_cache_sweep:
                        next_source_cache_sweep = self._schedule_background_task_if_due(
                            task_key="source_cache_sweep",
                            target=self._run_source_cache_background_sweep,
                            interval_sec=self.BACKGROUND_SELF_HEAL_INTERVAL_SEC,
                        )
                    if now_monotonic >= next_source_cache_prewarm:
                        next_source_cache_prewarm = self._schedule_background_task_if_due(
                            task_key="source_cache_prewarm",
                            target=self._run_source_cache_prewarm,
                            interval_sec=self.SOURCE_CACHE_PREWARM_INTERVAL_SEC,
                        )
                    if now_monotonic >= next_artifact_self_heal:
                        next_artifact_self_heal = self._schedule_background_task_if_due(
                            task_key="artifact_self_heal",
                            target=self._run_artifact_background_self_heal,
                            interval_sec=self.BACKGROUND_SELF_HEAL_INTERVAL_SEC,
                        )
                    if now_monotonic >= next_internal_alert_refresh:
                        self._refresh_internal_alert_status_cache()
                        next_internal_alert_refresh = now_monotonic + self.INTERNAL_ALERT_STATUS_REFRESH_INTERVAL_SEC
                    if now_monotonic >= next_waiting_job_reconcile:
                        self._reconcile_waiting_jobs()
                        next_waiting_job_reconcile = now_monotonic + self.WAITING_JOB_RECONCILE_INTERVAL_SEC
                    if self.role_mode == "internal":
                        self._process_internal_browser_alerts()
                    for _ in range(20):
                        if not self._process_one_task_if_needed():
                            break
                        processed_any = True
                        self._reconcile_waiting_jobs()
                    if processed_any or woke or now_monotonic >= next_mailbox_summary_refresh:
                        self._refresh_runtime_task_cache(limit=500)
                        next_mailbox_summary_refresh = now_monotonic + self.MAILBOX_SUMMARY_REFRESH_INTERVAL_SEC
                    self._db_status = "ok"
                    self._last_error = ""
                    self._last_poll_at = _now_text()
                    if not self._startup_logged:
                        self._emit_system_log(
                            f"[共享桥接] 运行时已启动: 角色={_role_label(self.role_mode)}, 共享目录={self.shared_bridge_root}, 节点ID={self.node_id}"
                        )
                        self._startup_logged = True
            except Exception as exc:  # noqa: BLE001
                self._mark_loop_error(exc)
            if self._stop_event.is_set():
                break
            self._wake_event.wait(self.poll_interval_sec)
            try:
                self._stop_event.wait(0)
            except Exception:
                pass

