from __future__ import annotations

import copy
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_source_cache_service import SharedSourceCacheService
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from pipeline_utils import get_app_dir


_TERMINAL_STATUSES = {"success", "failed", "partial_failed", "cancelled", "stale"}
_READY_FOR_EXTERNAL = "ready_for_external"


class InternalBridgeHttpTaskRunner:
    """HTTP bridge facade backed by an internal-only local bridge store.

    The existing shared-bridge runtime already owns all browser download flows.
    This runner reuses that code with a local SQLite store under `.runtime`
    instead of the UNC shared directory database.
    """

    _ALLOWED_CREATE_METHODS = {
        "create_handover_from_download_task",
        "create_day_metric_from_download_task",
        "create_wet_bulb_collection_task",
        "create_branch_power_upload_task",
        "create_alarm_event_upload_task",
        "create_monthly_auto_once_task",
        "create_monthly_multi_date_task",
        "create_monthly_resume_upload_task",
        "create_handover_cache_fill_task",
        "create_monthly_cache_fill_task",
    }
    _ALLOWED_GET_OR_CREATE_METHODS = {
        "get_or_create_handover_from_download_task",
        "get_or_create_day_metric_from_download_task",
        "get_or_create_wet_bulb_collection_task",
        "get_or_create_branch_power_upload_task",
        "get_or_create_alarm_event_upload_task",
        "get_or_create_monthly_auto_once_task",
        "get_or_create_monthly_resume_upload_task",
        "get_or_create_handover_cache_fill_task",
        "get_or_create_monthly_cache_fill_task",
    }

    def __init__(
        self,
        *,
        runtime_service: SharedBridgeRuntimeService,
        emit_log: Callable[[str], None] | None = None,
    ) -> None:
        self._main_service = runtime_service
        self._emit_log = emit_log or getattr(runtime_service, "emit_log", None)
        self._lock = threading.RLock()
        self._store: SharedBridgeStore | None = None
        self._runtime: SharedBridgeRuntimeService | None = None
        self._workers: Dict[str, threading.Thread] = {}
        self._started_at = time.time()

    def _emit(self, text: str) -> None:
        line = str(text or "").strip()
        if line and callable(self._emit_log):
            self._emit_log(line)

    def _local_root(self) -> Path:
        runtime_config = getattr(self._main_service, "runtime_config", {}) or {}
        root = resolve_runtime_state_root(runtime_config=runtime_config, app_dir=get_app_dir())
        return Path(root) / "internal_bridge_http"

    def _ensure_runtime(self) -> SharedBridgeRuntimeService:
        with self._lock:
            if self._runtime is not None and self._store is not None:
                return self._runtime

            main = self._main_service
            runtime_config = copy.deepcopy(getattr(main, "runtime_config", {}) or {})
            local_root = self._local_root()
            busy_timeout = max(1000, int(getattr(main, "sqlite_busy_timeout_ms", 15000) or 15000))
            store = SharedBridgeStore(local_root, busy_timeout_ms=busy_timeout)
            store.ensure_ready()
            with store.connect() as conn:
                conn.executescript(
                    """
                    CREATE VIEW IF NOT EXISTS internal_bridge_tasks AS
                    SELECT task_id, feature AS task_type, status, request_json, result_json, error, created_at, updated_at,
                           '' AS cancelled_at
                    FROM bridge_tasks;
                    CREATE VIEW IF NOT EXISTS internal_bridge_events AS
                    SELECT event_id, task_id, level, event_type, payload_json, created_at
                    FROM bridge_events;
                    CREATE VIEW IF NOT EXISTS internal_source_index AS
                    SELECT entry_id, source_family, bucket_key AS bucket_or_date, building, relative_path, metadata_json, updated_at
                    FROM source_cache_entries;
                    """
                )

            runtime = SharedBridgeRuntimeService(
                runtime_config=runtime_config,
                app_version=str(getattr(main, "app_version", "") or ""),
                job_service=None,
                emit_log=self._emit,
                request_runtime_status_refresh=getattr(main, "_request_runtime_status_refresh", None),
            )
            runtime._store = store
            runtime._mailbox_store = None
            runtime._mirror_store = None
            runtime.role_mode = "internal"
            runtime.node_id = f"{str(getattr(main, 'node_id', '') or 'internal').strip()}-http"
            runtime.node_label = "内网端HTTP桥接"
            runtime.shared_bridge_root = str(getattr(main, "shared_bridge_root", "") or "").strip()
            runtime._internal_download_pool = getattr(main, "_internal_download_pool", None)
            runtime._source_cache_service = SharedSourceCacheService(
                runtime_config=runtime_config,
                store=store,
                download_browser_pool=runtime._internal_download_pool,
                emit_log=self._emit,
            )
            self._store = store
            self._runtime = runtime
            return runtime

    @staticmethod
    def _normalize_status(task: Dict[str, Any]) -> Dict[str, Any]:
        output = copy.deepcopy(task if isinstance(task, dict) else {})
        raw_status = str(output.get("status", "") or "").strip().lower()
        output["raw_bridge_status"] = raw_status
        if raw_status == _READY_FOR_EXTERNAL:
            output["status"] = "success"
        elif raw_status in {"queued_for_internal", "internal_claimed"}:
            output["status"] = "queued"
        elif raw_status == "internal_running":
            output["status"] = "running"
        elif raw_status in _TERMINAL_STATUSES:
            output["status"] = raw_status
        return output

    def _get_store(self) -> SharedBridgeStore:
        self._ensure_runtime()
        if self._store is None:
            raise RuntimeError("内网端HTTP桥接状态库未初始化")
        return self._store

    def health(self) -> Dict[str, Any]:
        store = self._get_store()
        counts = store.get_task_counts()
        pool = getattr(self._main_service, "_internal_download_pool", None)
        browser_pool_ready = False
        if pool is not None:
            try:
                snapshot = pool.get_runtime_snapshot() if hasattr(pool, "get_runtime_snapshot") else {}
                browser_pool_ready = bool(snapshot.get("ready", False)) if isinstance(snapshot, dict) else True
            except Exception:
                browser_pool_ready = False
        return {
            "ok": True,
            "role": "internal",
            "version": str(getattr(self._main_service, "app_version", "") or ""),
            "browser_pool_ready": browser_pool_ready,
            "active_tasks": int(counts.get("pending_internal", 0) or 0),
            "task_counts": counts,
            "db_path": str(getattr(store, "db_path", "") or ""),
            "uptime_sec": int(time.time() - self._started_at),
        }

    def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        runtime = self._ensure_runtime()
        body = payload if isinstance(payload, dict) else {}
        get_or_create_name = str(body.get("get_or_create_name", "") or "").strip()
        create_name = str(body.get("create_name", "") or body.get("task_type", "") or "").strip()
        task_payload = body.get("payload", {}) if isinstance(body.get("payload", {}), dict) else {}
        task_payload = copy.deepcopy(task_payload)
        requested_by = str(body.get("requested_by", "") or task_payload.get("requested_by", "") or "external_http").strip()
        task_payload["requested_by"] = requested_by

        method_name = ""
        if get_or_create_name:
            if get_or_create_name not in self._ALLOWED_GET_OR_CREATE_METHODS:
                raise ValueError(f"不支持的HTTP桥接任务方法: {get_or_create_name}")
            method_name = get_or_create_name
        else:
            if create_name not in self._ALLOWED_CREATE_METHODS:
                raise ValueError(f"不支持的HTTP桥接任务方法: {create_name or '-'}")
            method_name = create_name
        method = getattr(runtime, method_name, None)
        if not callable(method):
            raise RuntimeError(f"内网端缺少桥接任务方法: {method_name}")

        task = method(**task_payload)
        task_id = str(task.get("task_id", "") or "").strip()
        if not task_id:
            raise RuntimeError("内网端HTTP桥接任务创建失败，缺少 task_id")
        self._start_worker_if_needed(task_id)
        normalized = self._normalize_status(task)
        normalized["transport"] = "http"
        return normalized

    def _start_worker_if_needed(self, task_id: str) -> None:
        task_text = str(task_id or "").strip()
        if not task_text:
            return
        with self._lock:
            existing = self._workers.get(task_text)
            if existing is not None and existing.is_alive():
                return
            worker = threading.Thread(
                target=self._run_worker,
                args=(task_text,),
                name=f"internal-bridge-http-task-{task_text[:8] or uuid.uuid4().hex[:8]}",
                daemon=True,
            )
            self._workers[task_text] = worker
            worker.start()

    def _run_worker(self, task_id: str) -> None:
        runtime = self._ensure_runtime()
        try:
            runtime._process_one_task_if_needed()
        except Exception as exc:  # noqa: BLE001
            self._emit(f"[内网HTTP桥接] 任务执行线程失败 task_id={task_id}, error={exc}")

    def get_task(self, task_id: str) -> Dict[str, Any] | None:
        task_text = str(task_id or "").strip()
        if not task_text:
            return None
        store = self._get_store()
        task = store.get_task(task_text)
        if not isinstance(task, dict):
            return None
        normalized = self._normalize_status(task)
        normalized["transport"] = "http"
        return normalized

    def list_tasks(self, *, status: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        store = self._get_store()
        status_text = str(status or "").strip().lower()
        safe_limit = max(1, int(limit or 100))
        raw_tasks = store.list_tasks(limit=max(safe_limit, 1000 if status_text == "active" else safe_limit))
        output: List[Dict[str, Any]] = []
        for task in raw_tasks if isinstance(raw_tasks, list) else []:
            if not isinstance(task, dict):
                continue
            normalized = self._normalize_status(task)
            normalized["transport"] = "http"
            normalized_status = str(normalized.get("status", "") or "").strip().lower()
            if status_text in {"active", "running"} and normalized_status in _TERMINAL_STATUSES:
                continue
            if status_text and status_text not in {"active", "running"} and normalized_status != status_text:
                continue
            output.append(normalized)
            if len(output) >= safe_limit:
                break
        return output

    def cancel_task(self, task_id: str) -> bool:
        task_text = str(task_id or "").strip()
        if not task_text:
            return False
        store = self._get_store()
        return bool(store.cancel_task(task_text))

    def list_source_index(
        self,
        *,
        source_family: str = "",
        bucket_or_date: str = "",
        building: str = "",
        bucket_kind: str = "",
        duty_shift: str = "",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        store = self._get_store()
        bucket_text = str(bucket_or_date or "").strip()
        kind_text = str(bucket_kind or "").strip().lower()
        duty_date = ""
        bucket_key = bucket_text
        if kind_text == "date" or (len(bucket_text) == 10 and bucket_text.count("-") == 2):
            duty_date = bucket_text
            bucket_key = ""
        entries = store.list_source_cache_entries(
            source_family=source_family,
            building=building,
            bucket_kind=kind_text,
            bucket_key=bucket_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
            status="ready",
            limit=limit,
        )
        shared_root_text = str(getattr(self._main_service, "shared_bridge_root", "") or "").strip()
        shared_root = Path(shared_root_text) if shared_root_text else None
        output: List[Dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            item = copy.deepcopy(entry)
            relative = str(item.get("relative_path", "") or "").replace("\\", "/").strip()
            if relative and shared_root is not None:
                item.setdefault("file_path", str(shared_root / relative.replace("/", "\\")))
            output.append(item)
        return output

    def list_source_index_batch(self, queries: List[Dict[str, Any]], *, default_limit: int = 50) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for index, query in enumerate(queries if isinstance(queries, list) else []):
            if not isinstance(query, dict):
                results.append({"index": index, "ok": False, "entries": [], "error": "query must be an object"})
                continue
            try:
                entries = self.list_source_index(
                    source_family=str(query.get("source_family", "") or ""),
                    bucket_or_date=str(query.get("bucket_or_date", "") or ""),
                    building=str(query.get("building", "") or ""),
                    bucket_kind=str(query.get("bucket_kind", "") or ""),
                    duty_shift=str(query.get("duty_shift", "") or ""),
                    limit=int(query.get("limit", default_limit) or default_limit),
                )
                results.append({"index": index, "ok": True, "entries": entries})
            except Exception as exc:  # noqa: BLE001
                results.append({"index": index, "ok": False, "entries": [], "error": str(exc)})
        return results
