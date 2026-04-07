from __future__ import annotations

import copy
import json
import sqlite3
import socket
import threading
import time
import uuid
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
from app.modules.shared_bridge.service.shared_source_cache_service import (
    SharedSourceCacheService,
    is_accessible_cached_file_path,
)
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
from handover_log_module.service.day_metric_standalone_upload_service import DayMetricStandaloneUploadService
from handover_log_module.api.facade import load_handover_config
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
    CLEANUP_INTERVAL_SEC = 600
    TASK_RETENTION_DAYS = 14
    NODE_RETENTION_DAYS = 2

    def __init__(
        self,
        *,
        runtime_config: Dict[str, Any],
        app_version: str,
        emit_log: Callable[[str], None] | None = None,
    ) -> None:
        self.runtime_config = copy.deepcopy(runtime_config if isinstance(runtime_config, dict) else {})
        self.app_version = str(app_version or "").strip()
        self.emit_log = emit_log
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_poll_at = ""
        self._db_status = "disabled"
        self._last_error = ""
        self._counts = {"pending_internal": 0, "pending_external": 0, "problematic": 0, "total_count": 0, "node_count": 0}
        self._store: SharedBridgeStore | None = None
        self._internal_download_pool: InternalDownloadBrowserPool | None = None
        self._source_cache_service: SharedSourceCacheService | None = None
        self._startup_logged = False
        self._last_cleanup_at = ""
        self._cleanup_deleted_tasks = 0
        self._cleanup_deleted_entries = 0
        self._cleanup_deleted_files = 0
        self._refresh_config()

    def _refresh_config(self) -> None:
        deployment = self.runtime_config.get("deployment", {})
        bridge_cfg = self.runtime_config.get("shared_bridge", {})
        if not isinstance(deployment, dict):
            deployment = {}
        if not isinstance(bridge_cfg, dict):
            bridge_cfg = {}
        resolved_bridge_cfg = resolve_shared_bridge_paths(bridge_cfg, deployment.get("role_mode"))
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
        self.sqlite_busy_timeout_ms = max(1000, int(resolved_bridge_cfg.get("sqlite_busy_timeout_ms", 5000) or 5000))
        self._store = (
            SharedBridgeStore(self.shared_bridge_root, busy_timeout_ms=self.sqlite_busy_timeout_ms)
            if self.shared_bridge_root
            else None
        )
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

    def _should_run(self) -> bool:
        return self.role_mode in {"internal", "external"} and self.shared_bridge_enabled and bool(self.shared_bridge_root)

    def start(self) -> Dict[str, Any]:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return {"started": False, "running": True, "reason": "already_running"}
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
                if self._source_cache_service is not None:
                    self._source_cache_service.stop()
                if self._internal_download_pool is not None:
                    clear_internal_download_browser_pool(self._internal_download_pool)
                    self._internal_download_pool.stop()
                    self._internal_download_pool = None
                return {"started": False, "running": False, "reason": "disabled_or_unselected"}
            self._db_status = "starting"
            if self.role_mode == "internal":
                if self._internal_download_pool is None:
                    self._internal_download_pool = InternalDownloadBrowserPool(
                        self.runtime_config,
                        emit_log=self._emit_system_log,
                    )
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
                    self._source_cache_service.start()
            else:
                if self._internal_download_pool is not None:
                    clear_internal_download_browser_pool(self._internal_download_pool)
                    self._internal_download_pool.stop()
                    self._internal_download_pool = None
                if self._source_cache_service is not None:
                    self._source_cache_service.update_download_browser_pool(None)
                    self._source_cache_service.stop()
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._loop, name="shared-bridge-runtime", daemon=True)
            self._thread.start()
            return {"started": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        with self._lock:
            thread = self._thread
            if not thread:
                if self._source_cache_service is not None:
                    self._source_cache_service.stop()
                if self._internal_download_pool is not None:
                    clear_internal_download_browser_pool(self._internal_download_pool)
                    self._internal_download_pool.stop()
                    self._internal_download_pool = None
                self._startup_logged = False
                self._db_status = "disabled" if not self._should_run() else "stopped"
                self._counts = {"pending_internal": 0, "pending_external": 0, "problematic": 0, "total_count": 0, "node_count": 0}
                return {"stopped": False, "running": False, "reason": "not_running"}
            self._stop_event.set()
            self._thread = None
        thread.join(timeout=5)
        if self._source_cache_service is not None:
            self._source_cache_service.stop()
        if self._internal_download_pool is not None:
            clear_internal_download_browser_pool(self._internal_download_pool)
            self._internal_download_pool.stop()
            self._internal_download_pool = None
        self._startup_logged = False
        self._db_status = "disabled" if not self._should_run() else "stopped"
        self._counts = {"pending_internal": 0, "pending_external": 0, "problematic": 0, "total_count": 0, "node_count": 0}
        return {"stopped": True, "running": False, "reason": "stopped"}

    def is_running(self) -> bool:
        thread = self._thread
        return bool(thread and thread.is_alive())

    def get_health_snapshot(self, *, mode: str = "external_full") -> Dict[str, Any]:
        normalized_mode = str(mode or "external_full").strip().lower() or "external_full"
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
            else {
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
                    "scope_text": "当前小时",
                },
                "handover_log_family": {},
                "monthly_report_family": {},
                "alarm_event_family": {},
            }
        )
        if self._store is not None and normalized_mode != "internal_light":
            try:
                internal_alert_status = self._build_external_internal_alert_status(
                    self._store.list_external_alert_projections()
                )
            except Exception as exc:  # noqa: BLE001
                if self._is_recoverable_store_error(exc):
                    self._db_status = "busy"
                    self._last_error = "共享桥接数据库暂时忙碌，告警摘要已降级为空结果"
                    internal_alert_status = self._empty_internal_alert_status()
                else:
                    raise
        else:
            internal_alert_status = self._empty_internal_alert_status()
        return {
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
            "internal_download_pool": internal_download_pool,
            "internal_source_cache": internal_source_cache,
            "internal_alert_status": internal_alert_status,
        }

    def list_tasks(self, *, limit: int = 100) -> list[Dict[str, Any]]:
        if not self._store:
            return []
        try:
            self._store.ensure_ready()
            return self._store.list_tasks(limit=limit)
        except Exception as exc:  # noqa: BLE001
            if self._is_recoverable_store_error(exc):
                self._db_status = "busy"
                self._last_error = "共享桥接数据库暂时忙碌，共享任务列表已降级为空结果"
                return []
            raise

    def get_task(self, task_id: str) -> Dict[str, Any] | None:
        if not self._store:
            return None
        try:
            self._store.ensure_ready()
            return self._store.get_task(task_id)
        except Exception as exc:  # noqa: BLE001
            if self._is_recoverable_store_error(exc):
                self._db_status = "busy"
                self._last_error = "共享桥接数据库暂时忙碌，共享任务详情暂时不可用"
                return None
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
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        return self._store.create_handover_from_download_task(
            buildings=buildings,
            end_time=end_time,
            duty_date=duty_date,
            duty_shift=duty_shift,
            target_bucket_key=target_bucket_key,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )

    def get_or_create_handover_from_download_task(
        self,
        *,
        buildings: List[str] | None,
        end_time: str | None,
        duty_date: str | None,
        duty_shift: str | None,
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
            return existing
        return self.create_handover_from_download_task(
            buildings=normalized_buildings,
            end_time=end_time,
            duty_date=duty_date,
            duty_shift=duty_shift,
            target_bucket_key=resolved_bucket_key if not (duty_date_text and duty_shift_text) else "",
            requested_by=requested_by,
        )

    def create_day_metric_from_download_task(
        self,
        *,
        selected_dates: List[str],
        building_scope: str,
        building: str | None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        return self._store.create_day_metric_from_download_task(
            selected_dates=selected_dates,
            building_scope=building_scope,
            building=building,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )

    def create_wet_bulb_collection_task(
        self,
        *,
        buildings: List[str] | None,
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        return self._store.create_wet_bulb_collection_task(
            buildings=buildings,
            target_bucket_key=target_bucket_key,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )

    def get_or_create_wet_bulb_collection_task(
        self,
        *,
        buildings: List[str] | None,
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
            return existing
        return self.create_wet_bulb_collection_task(
            buildings=normalized_buildings,
            target_bucket_key=resolved_bucket_key,
            requested_by=requested_by,
        )

    def create_monthly_auto_once_task(
        self,
        *,
        target_bucket_key: str | None = None,
        requested_by: str = "manual",
        source: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        return self._store.create_monthly_auto_once_task(
            target_bucket_key=target_bucket_key,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
            source=source,
        )

    def get_or_create_monthly_auto_once_task(
        self,
        *,
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
            return existing
        return self.create_monthly_auto_once_task(
            target_bucket_key=resolved_bucket_key,
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
        return self._store.create_monthly_multi_date_task(
            selected_dates=selected_dates,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )

    def create_monthly_resume_upload_task(
        self,
        *,
        run_id: str | None,
        auto_trigger: bool,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        return self._store.create_monthly_resume_upload_task(
            run_id=run_id,
            auto_trigger=auto_trigger,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
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
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        return self._store.create_handover_cache_fill_task(
            continuation_kind=continuation_kind,
            buildings=buildings,
            duty_date=duty_date,
            duty_shift=duty_shift,
            selected_dates=selected_dates,
            building_scope=building_scope,
            building=building,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )

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
            return existing
        return self.create_handover_cache_fill_task(
            continuation_kind=continuation_kind,
            buildings=normalized_buildings,
            duty_date=duty_date,
            duty_shift=duty_shift,
            selected_dates=normalized_dates,
            building_scope=building_scope,
            building=building,
            requested_by=requested_by,
        )

    def create_monthly_cache_fill_task(
        self,
        *,
        selected_dates: List[str] | None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        return self._store.create_monthly_cache_fill_task(
            selected_dates=selected_dates,
            created_by_role=self.role_mode,
            created_by_node_id=self.node_id,
            requested_by=requested_by,
        )

    def get_or_create_monthly_cache_fill_task(
        self,
        *,
        selected_dates: List[str] | None,
        requested_by: str = "manual",
    ) -> Dict[str, Any]:
        if not self._store:
            raise RuntimeError("共享桥接未配置")
        self._store.ensure_ready()
        normalized_dates = [str(item or "").strip() for item in (selected_dates or []) if str(item or "").strip()]
        dedupe_key = "|".join(["monthly_cache_fill", ",".join(normalized_dates) or "-"])
        existing = self._store.find_active_task_by_dedupe_key(dedupe_key)
        if existing:
            return existing
        return self.create_monthly_cache_fill_task(
            selected_dates=normalized_dates,
            requested_by=requested_by,
        )

    def current_source_cache_bucket(self) -> str:
        if self._source_cache_service is not None:
            try:
                return str(self._source_cache_service.current_hour_bucket() or "").strip()
            except Exception:
                pass
        return _now_text()[:13]

    def get_latest_source_cache_entries(self, *, source_family: str, buildings: List[str] | None = None) -> List[Dict[str, Any]]:
        if self._source_cache_service is None:
            return []
        return self._source_cache_service.get_latest_ready_entries(source_family=source_family, buildings=buildings)

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
        if isinstance(exc, sqlite3.OperationalError):
            return any(token in text for token in ("database is locked", "database table is locked", "database is busy", "busy"))
        return any(token in text for token in ("database is locked", "database table is locked"))

    def _mark_loop_error(self, exc: Exception) -> None:
        if self._is_recoverable_store_error(exc):
            self._db_status = "busy"
            self._last_error = "共享桥接数据库暂时忙碌，下一轮自动重试"
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
        atomic_copy_file(source_path, target_path, validator=validate_excel_workbook_file, temp_suffix=".downloading")
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
        atomic_copy_file(source_path, target_path, validator=validate_excel_workbook_file, temp_suffix=".downloading")
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
        target_path = self._day_metric_artifact_target(task_id, duty_date, building, source_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_copy_file(source_path, target_path, validator=validate_excel_workbook_file, temp_suffix=".downloading")
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
        )
        emit_log(f"[共享桥接][12项][内网] 已写入共享源文件 日期={duty_date}, 楼栋={building}, 路径={target_path}")
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
        atomic_copy_file(source_path, target_path, validator=validate_excel_workbook_file, temp_suffix=".downloading")
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
            artifacts: List[Dict[str, Any]] = []
            handover_result = result.get("handover", {}) if isinstance(result.get("handover", {}), dict) else {}
            capacity_result = result.get("capacity", {}) if isinstance(result.get("capacity", {}), dict) else {}
            for item in handover_result.get("success_files", []) if isinstance(handover_result.get("success_files", []), list) else []:
                if not isinstance(item, dict):
                    continue
                building = str(item.get("building", "") or "").strip()
                file_path = str(item.get("file_path", "") or "").strip()
                if not building or not file_path:
                    continue
                artifacts.append(self._copy_handover_source_artifact(task_id=task_id, building=building, source_file=file_path, emit_log=emit_log))
            capacity_artifacts: List[Dict[str, Any]] = []
            for item in capacity_result.get("success_files", []) if isinstance(capacity_result.get("success_files", []), list) else []:
                if not isinstance(item, dict):
                    continue
                building = str(item.get("building", "") or "").strip()
                file_path = str(item.get("file_path", "") or "").strip()
                if not building or not file_path:
                    continue
                capacity_artifacts.append(
                    self._copy_handover_capacity_source_artifact(
                        task_id=task_id,
                        building=building,
                        source_file=file_path,
                        emit_log=emit_log,
                    )
                )
            stage_result = dict(result)
            stage_result["artifacts"] = list(artifacts)
            stage_result["capacity_artifacts"] = list(capacity_artifacts)
            stage_result["artifact_count"] = len(artifacts)
            stage_result["capacity_artifact_count"] = len(capacity_artifacts)
            handover_artifact_buildings = {
                str(item.get("building", "") or "").strip()
                for item in artifacts
                if str(item.get("building", "") or "").strip()
            }
            capacity_artifact_buildings = {
                str(item.get("building", "") or "").strip()
                for item in capacity_artifacts
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
                )
                self._store.append_event(task_id=task_id, stage_id=stage_id, side="internal", level="info", event_type="await_external", payload={"message": "内网下载完成，等待外网继续处理"})
            else:
                error_text = (
                    str(handover_result.get("error", "") or "").strip()
                    or str(capacity_result.get("error", "") or "").strip()
                    or "内网下载未产生完整且可匹配的交接班共享源文件"
                )
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
            self._emit_system_log(f"[共享桥接][内网端] 任务={task_id} 交接班共享文件准备失败: {error_text}")

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
            artifacts = self._store.get_artifacts(task_id, artifact_kind="source_file", status="ready")
            building_files: List[tuple[str, str]] = []
            for item in artifacts:
                relative_path = str(item.get("relative_path", "") or "").strip()
                building = str(item.get("building", "") or "").strip()
                if not relative_path or not building:
                    continue
                file_path = self._resolve_shared_artifact_file_path(relative_path)
                if file_path is None:
                    raise FileNotFoundError(f"共享目录中的交接班源文件不存在或不可访问: {relative_path}")
                building_files.append((building, str(file_path)))
            capacity_artifacts = self._store.get_artifacts(task_id, artifact_kind="capacity_source_file", status="ready")
            capacity_building_files: List[tuple[str, str]] = []
            for item in capacity_artifacts:
                relative_path = str(item.get("relative_path", "") or "").strip()
                building = str(item.get("building", "") or "").strip()
                if not relative_path or not building:
                    continue
                file_path = self._resolve_shared_artifact_file_path(relative_path)
                if file_path is None:
                    raise FileNotFoundError(f"共享目录中的交接班容量源文件不存在或不可访问: {relative_path}")
                capacity_building_files.append((building, str(file_path)))
            if not building_files:
                raise RuntimeError("共享目录中没有可继续处理的交接班源文件")
            handover_buildings = {building for building, _ in building_files if building}
            capacity_building_map = {building: file_path for building, file_path in capacity_building_files if building and file_path}
            if not handover_buildings or not handover_buildings.issubset(set(capacity_building_map)):
                raise RuntimeError("共享目录中没有完整可继续处理的交接班容量源文件")
            emit_log("[共享桥接][交接班][外网] 开始继续生成")
            result = OrchestratorService(self.runtime_config).run_handover_from_files(
                building_files=building_files,
                capacity_building_files=[(building, capacity_building_map[building]) for building, _ in building_files if building in capacity_building_map],
                end_time=str(request.get("end_time", "") or "").strip() or None,
                duty_date=str(request.get("duty_date", "") or "").strip() or None,
                duty_shift=str(request.get("duty_shift", "") or "").strip() or None,
                emit_log=emit_log,
            )
            merged_result = {**result, "bridge_task_id": task_id, "internal": internal_result}
            raw_status = str(result.get("status", "") or "failed").strip().lower()
            final_status = "success" if raw_status in {"ok", "success"} else ("partial_failed" if raw_status == "partial_failed" else "failed")
            task_error = ""
            if final_status in {"partial_failed", "failed"}:
                errors = result.get("errors", []) if isinstance(result.get("errors", []), list) else []
                task_error = str(errors[0] if errors else "").strip()
            self._store.complete_stage(
                task_id=task_id,
                stage_id=stage_id,
                claim_token=claim_token,
                side="external",
                stage_result=result,
                next_task_status=final_status,
                task_error=task_error,
                task_result=merged_result,
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
        service = DayMetricStandaloneUploadService(
            self.runtime_config,
            download_browser_pool=self._internal_download_pool,
        )
        try:
            emit_log("[共享桥接][12项][内网] 开始下载阶段")
            internal_result = service.run_download_only(
                selected_dates=request.get("selected_dates") if isinstance(request.get("selected_dates"), list) else [],
                building_scope=str(request.get("building_scope", "") or "").strip() or "all_enabled",
                building=str(request.get("building", "") or "").strip() or None,
                emit_log=emit_log,
            )
            artifacts: List[Dict[str, Any]] = []
            for item in internal_result.get("downloaded_files", []) if isinstance(internal_result.get("downloaded_files", []), list) else []:
                if not isinstance(item, dict):
                    continue
                duty_date = str(item.get("duty_date", "") or "").strip()
                building = str(item.get("building", "") or "").strip()
                source_file = str(item.get("source_file", "") or "").strip()
                if not duty_date or not building or not source_file:
                    continue
                artifacts.append(self._copy_day_metric_source_artifact(task_id=task_id, duty_date=duty_date, building=building, source_file=source_file, emit_log=emit_log))
            stage_result = dict(internal_result)
            stage_result["artifacts"] = list(artifacts)
            stage_result["artifact_count"] = len(artifacts)
            if artifacts:
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="internal",
                    stage_result=stage_result,
                    next_task_status="ready_for_external",
                    task_result={"internal": stage_result, "status": "ready_for_external"},
                )
                self._store.append_event(task_id=task_id, stage_id=stage_id, side="internal", level="info", event_type="await_external", payload={"message": "内网下载完成，等待外网继续上传"})
            else:
                result_rows: List[Dict[str, Any]] = []
                for date_row in internal_result.get("results", []) if isinstance(internal_result.get("results", []), list) else []:
                    grouped = date_row.get("buildings", []) if isinstance(date_row, dict) else []
                    result_rows.extend([row for row in grouped if isinstance(row, dict)])
                error_text = self._first_failed_error_from_rows(result_rows) or "内网下载未产生任何共享源文件"
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
            self._emit_system_log(f"[共享桥接][内网端] 任务={task_id} 12项共享文件准备失败: {error_text}")

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
            artifacts = self._store.get_artifacts(task_id, artifact_kind="source_file", status="ready")
            source_units: List[Dict[str, Any]] = []
            for item in artifacts:
                relative_path = str(item.get("relative_path", "") or "").strip()
                building = str(item.get("building", "") or "").strip()
                metadata = item.get("metadata", {}) if isinstance(item.get("metadata", {}), dict) else {}
                duty_date = str(metadata.get("duty_date", "") or "").strip()
                if not relative_path or not building or not duty_date:
                    continue
                file_path = self._resolve_shared_artifact_file_path(relative_path)
                if file_path is None:
                    raise FileNotFoundError(f"共享目录中的12项源文件不存在或不可访问: {relative_path}")
                source_units.append({"duty_date": duty_date, "building": building, "source_file": str(file_path)})
            if not source_units:
                raise RuntimeError("共享目录中没有可继续上传的12项源文件")
            selected_dates = [str(item or "").strip() for item in (internal_result.get("selected_dates", []) if isinstance(internal_result.get("selected_dates", []), list) else []) if str(item or "").strip()]
            buildings = [str(item or "").strip() for item in (internal_result.get("selected_buildings", []) if isinstance(internal_result.get("selected_buildings", []), list) else []) if str(item or "").strip()]
            if not selected_dates:
                selected_dates = [str(item or "").strip() for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates"), list) else []) if str(item or "").strip()]
            if not buildings:
                buildings = sorted({str(item.get("building", "") or "").strip() for item in source_units if str(item.get("building", "") or "").strip()})
            emit_log("[共享桥接][12项][外网] 开始继续上传")
            external_result = service.continue_from_source_files(
                selected_dates=selected_dates,
                buildings=buildings,
                source_units=source_units,
                building_scope=str(request.get("building_scope", "") or "").strip() or "all_enabled",
                building=str(request.get("building", "") or "").strip() or None,
                emit_log=emit_log,
            )
            merged_result = service.merge_bridge_results(
                internal_result=internal_result,
                external_result=external_result,
                selected_dates=selected_dates,
                buildings=buildings,
                building_scope=str(request.get("building_scope", "") or "").strip() or "all_enabled",
                building=str(request.get("building", "") or "").strip() or None,
                emit_log=emit_log,
            )
            merged_result["bridge_task_id"] = task_id
            merged_result["internal"] = internal_result
            merged_result["external"] = external_result
            raw_status = str(merged_result.get("status", "") or "failed").strip().lower()
            final_status = "success" if raw_status in {"ok", "success"} else ("partial_failed" if raw_status == "partial_failed" else "failed")
            task_error = ""
            if final_status in {"partial_failed", "failed"}:
                result_rows: List[Dict[str, Any]] = []
                for date_row in merged_result.get("results", []) if isinstance(merged_result.get("results", []), list) else []:
                    grouped = date_row.get("buildings", []) if isinstance(date_row, dict) else []
                    result_rows.extend([row for row in grouped if isinstance(row, dict)])
                task_error = self._first_failed_error_from_rows(result_rows)
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
            artifacts: List[Dict[str, Any]] = []
            for item in internal_result.get("source_units", []) if isinstance(internal_result.get("source_units", []), list) else []:
                if not isinstance(item, dict):
                    continue
                building = str(item.get("building", "") or "").strip()
                source_file = str(item.get("file_path", "") or item.get("source_file", "") or "").strip()
                if not building or not source_file:
                    continue
                artifacts.append(
                    self._copy_wet_bulb_source_artifact(
                        task_id=task_id,
                        building=building,
                        source_file=source_file,
                        emit_log=emit_log,
                    )
                )
            stage_result = dict(internal_result)
            stage_result["artifacts"] = list(artifacts)
            stage_result["artifact_count"] = len(artifacts)
            if artifacts:
                self._store.complete_stage(
                    task_id=task_id,
                    stage_id=stage_id,
                    claim_token=claim_token,
                    side="internal",
                    stage_result=stage_result,
                    next_task_status="ready_for_external",
                    task_result={"internal": stage_result, "status": "ready_for_external"},
                )
                self._store.append_event(
                    task_id=task_id,
                    stage_id=stage_id,
                    side="internal",
                    level="info",
                    event_type="await_external",
                    payload={"message": "内网下载完成，等待外网继续处理"},
                )
            else:
                failed_buildings = internal_result.get("failed_buildings", []) if isinstance(internal_result.get("failed_buildings", []), list) else []
                error_text = "内网下载未产生任何共享源文件"
                for item in failed_buildings:
                    if not isinstance(item, dict):
                        continue
                    error_text = str(item.get("error", "") or "").strip() or error_text
                    if error_text:
                        break
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
            self._emit_system_log(f"[共享桥接][内网端] 任务={task_id} 湿球温度共享文件准备失败: {error_text}")

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
            artifacts = self._store.get_artifacts(task_id, artifact_kind="source_file", status="ready")
            source_units: List[Dict[str, Any]] = []
            for item in artifacts:
                relative_path = str(item.get("relative_path", "") or "").strip()
                building = str(item.get("building", "") or "").strip()
                if not relative_path or not building:
                    continue
                file_path = self._resolve_shared_artifact_file_path(relative_path)
                if file_path is None:
                    raise FileNotFoundError(f"共享目录中的湿球温度源文件不存在或不可访问: {relative_path}")
                source_units.append({"building": building, "file_path": str(file_path)})
            if not source_units:
                raise RuntimeError("共享目录中没有可继续处理的湿球温度源文件")
            emit_log("[共享桥接][湿球温度][外网] 开始继续提取并上传")
            external_result = service.continue_from_source_units(
                source_units=source_units,
                emit_log=emit_log,
            )
            merged_result = dict(external_result)
            merged_result["bridge_task_id"] = task_id
            merged_result["internal"] = internal_result
            merged_result["external"] = external_result
            raw_status = str(external_result.get("status", "") or "failed").strip().lower()
            if raw_status in {"ok", "success", "skipped"}:
                final_status = "success"
            elif raw_status == "partial_failed":
                final_status = "partial_failed"
            else:
                final_status = "failed"
            task_error = ""
            if final_status in {"partial_failed", "failed"}:
                for item in external_result.get("failed_buildings", []) if isinstance(external_result.get("failed_buildings", []), list) else []:
                    if not isinstance(item, dict):
                        continue
                    task_error = str(item.get("error", "") or "").strip()
                    if task_error:
                        break
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
                    raise RuntimeError("等待历史日期缓存补采完成")
                building_files = [(str(item.get("building", "")).strip(), str(item.get("file_path", "")).strip()) for item in cached_entries]
                capacity_building_files = [
                    (str(item.get("building", "")).strip(), str(item.get("file_path", "")).strip())
                    for item in capacity_entries
                ]
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
                    raise RuntimeError("等待历史日期缓存补采完成")
                source_units = [
                    {
                        "duty_date": str(item.get("duty_date", "") or "").strip(),
                        "building": str(item.get("building", "") or "").strip(),
                        "source_file": str(item.get("file_path", "") or "").strip(),
                    }
                    for item in cached_entries
                ]
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
            selected_dates = [
                str(item or "").strip()
                for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates", []), list) else [])
                if str(item or "").strip()
            ]
            cached_entries = self._source_cache_service.get_monthly_by_date_entries(selected_dates=selected_dates)
            expected = len(selected_dates) * len(self._source_cache_service.get_enabled_buildings())
            if len(cached_entries) < expected:
                raise RuntimeError("等待历史日期缓存补采完成")
            file_items = [
                {
                    "building": str(item.get("building", "") or "").strip(),
                    "file_path": str(item.get("file_path", "") or "").strip(),
                    "upload_date": str(item.get("metadata", {}).get("upload_date", "") or item.get("duty_date", "") or "").strip(),
                }
                for item in cached_entries
            ]
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
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        prior_result = task.get("result", {}) if isinstance(task.get("result", {}), dict) else {}
        internal_result = prior_result.get("internal", {}) if isinstance(prior_result.get("internal", {}), dict) else {}
        emit_log = self._bridge_emit(task_id=task_id, stage_id=stage_id, side="external", claim_token=claim_token)
        try:
            run_id = str(request.get("run_id", "") or internal_result.get("run_id", "") or "").strip() or None
            auto_trigger = bool(request.get("auto_trigger", False))
            emit_log(
                "[共享桥接][月报][外网] 开始断点续传阶段 "
                f"运行ID={run_id or '-'}, 自动触发={str(auto_trigger).lower()}"
            )
            external_result = run_bridge_resume_upload(
                self.runtime_config,
                shared_root_dir=self.shared_bridge_root,
                run_id=run_id,
                auto_trigger=auto_trigger,
            )
            merged_result = dict(external_result)
            merged_result["bridge_task_id"] = task_id
            merged_result["internal"] = internal_result
            merged_result["external"] = external_result

            raw_status = str(external_result.get("status", "") or "failed").strip().lower()
            if raw_status in {"ok", "success", "skipped"}:
                final_status = "success"
            elif raw_status == "partial_failed":
                final_status = "partial_failed"
            else:
                final_status = "failed"

            task_error = ""
            if final_status in {"partial_failed", "failed"}:
                task_error = str(external_result.get("last_error", "") or "").strip()
                if not task_error:
                    task_error = str(external_result.get("error", "") or "").strip()
                if not task_error:
                    task_error = "月报外网断点续传阶段失败"

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

    def _process_one_task_if_needed(self) -> None:
        if not self._store:
            return
        task = self._store.claim_next_task(
            role_target=self.role_mode,
            node_id=self.node_id,
            lease_sec=self.claim_lease_sec,
        )
        if not task:
            return
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

    def _loop(self) -> None:
        next_heartbeat = 0.0
        next_cleanup = 0.0
        while not self._stop_event.is_set():
            now_monotonic = time.monotonic()
            try:
                if not self._store:
                    self._db_status = "misconfigured"
                    self._last_error = "共享桥接目录未配置"
                else:
                    self._store.ensure_ready()
                    if now_monotonic >= next_heartbeat:
                        self._touch_node()
                        next_heartbeat = now_monotonic + self.heartbeat_interval_sec
                    if now_monotonic >= next_cleanup:
                        self._run_housekeeping()
                        next_cleanup = now_monotonic + self.CLEANUP_INTERVAL_SEC
                    if self.role_mode == "internal":
                        self._process_internal_browser_alerts()
                    self._process_one_task_if_needed()
                    self._counts = self._store.get_task_counts()
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
            self._stop_event.wait(self.poll_interval_sec)

