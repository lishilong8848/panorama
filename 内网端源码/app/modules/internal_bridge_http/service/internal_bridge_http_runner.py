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
        "create_alarm_event_window_query_task",
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
        "get_or_create_alarm_event_window_query_task",
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
        entries = self._list_source_cache_entries_fast(
            store,
            source_family=source_family,
            building=building,
            bucket_kind=kind_text,
            bucket_key=bucket_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
            status="ready",
            limit=limit,
        )
        entries = self._merge_main_source_cache_entries(
            entries,
            source_family=source_family,
            building=building,
            bucket_kind=kind_text,
            bucket_key=bucket_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
            limit=limit,
        )
        entries = self._recover_source_index_from_existing_files_if_needed(
            entries,
            source_family=source_family,
            building=building,
            bucket_kind=kind_text,
            bucket_key=bucket_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
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
            file_path_text = str(item.get("file_path", "") or "").strip()
            if not self._source_index_file_accessible(file_path_text):
                self._emit(
                    "[内网HTTP桥接] source-index 已过滤不可访问源文件: "
                    f"family={source_family or '-'}, building={item.get('building') or building or '-'}, "
                    f"bucket={item.get('bucket_key') or bucket_key or duty_date or '-'}, path={file_path_text or '-'}"
                )
                continue
            output.append(item)
        return output

    @staticmethod
    def _source_index_file_accessible(file_path: str) -> bool:
        text = str(file_path or "").strip()
        if not text:
            return False
        try:
            candidate = Path(text)
            return candidate.is_file()
        except OSError:
            return False

    def _list_source_cache_entries_fast(
        self,
        store: Any,
        *,
        source_family: str = "",
        building: str = "",
        bucket_kind: str = "",
        bucket_key: str = "",
        duty_date: str = "",
        duty_shift: str = "",
        status: str = "",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Read source-index from the lightweight JSON index only.

        HTTP source-index is a request path used by the external UI and
        schedulers. It must not wait behind internal SQLite writers because
        downloads can hold the local store busy long enough for the external
        side to time out. The JSON index is written together with ready source
        entries and is safe to read without the SQLite lock.
        """
        index_store = getattr(store, "_source_cache_index_store", None)
        if index_store is None:
            return []
        try:
            return index_store.list_entries(
                source_family=source_family,
                building=building,
                bucket_kind=bucket_kind,
                bucket_key=bucket_key,
                duty_date=duty_date,
                duty_shift=duty_shift,
                status=status,
                limit=max(1, int(limit or 50)),
            )
        except Exception as exc:  # noqa: BLE001
            self._emit(f"[内网HTTP桥接] 读取轻量源文件索引失败: {exc}")
            return []

    def _merge_main_source_cache_entries(
        self,
        entries: List[Dict[str, Any]],
        *,
        source_family: str = "",
        building: str = "",
        bucket_kind: str = "",
        bucket_key: str = "",
        duty_date: str = "",
        duty_shift: str = "",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Merge HTTP-task-local index with the main internal source cache index.

        The source-cache scheduler may have downloaded files before the HTTP
        bridge runner was created. Reading both local SQLite indexes avoids
        reporting a building as missing while still avoiding shared-folder scans.
        """
        merged: Dict[str, Dict[str, Any]] = {}

        def _add(row: Dict[str, Any]) -> None:
            if not isinstance(row, dict):
                return
            key = "|".join(
                [
                    str(row.get("entry_id", "") or "").strip(),
                    str(row.get("source_family", "") or "").strip(),
                    str(row.get("building", "") or "").strip(),
                    str(row.get("bucket_kind", "") or "").strip(),
                    str(row.get("bucket_key", "") or "").strip(),
                    str(row.get("duty_date", "") or "").strip(),
                    str(row.get("duty_shift", "") or "").strip(),
                    str(row.get("relative_path", "") or "").strip(),
                ]
            )
            if key.strip("|"):
                existing = merged.get(key)
                if existing is None or str(row.get("updated_at", "") or "") >= str(existing.get("updated_at", "") or ""):
                    merged[key] = dict(row)

        for row in entries if isinstance(entries, list) else []:
            _add(row)

        main_cache = getattr(self._main_service, "_source_cache_service", None)
        main_store = getattr(main_cache, "store", None)
        if main_store is not None:
            try:
                rows = self._list_source_cache_entries_fast(
                    main_store,
                    source_family=source_family,
                    building=building,
                    bucket_kind=bucket_kind,
                    bucket_key=bucket_key,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    status="ready",
                    limit=max(int(limit or 50), 200),
                )
                for row in rows if isinstance(rows, list) else []:
                    _add(row)
            except Exception as exc:  # noqa: BLE001
                self._emit(f"[内网HTTP桥接] 合并主源文件索引失败: {exc}")

        output = list(merged.values())
        output.sort(
            key=lambda row: (
                str(row.get("downloaded_at", "") or "").strip(),
                str(row.get("updated_at", "") or "").strip(),
                str(row.get("entry_id", "") or "").strip(),
            ),
            reverse=True,
        )
        return output[: max(1, int(limit or 50))]

    def _recover_source_index_from_existing_files_if_needed(
        self,
        entries: List[Dict[str, Any]],
        *,
        source_family: str = "",
        building: str = "",
        bucket_kind: str = "",
        bucket_key: str = "",
        duty_date: str = "",
        duty_shift: str = "",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Recover missing ready source-index rows from files already on disk.

        External pages only read HTTP source-index and must not scan the UNC
        share. The internal side owns the shared directory locally, so when a
        specific building/family has no ready index row we can do a bounded
        local scan and register the existing file before returning the index.
        """
        if entries:
            return entries
        family = str(source_family or "").strip()
        building_name = str(building or "").strip()
        if not family or not building_name:
            return entries
        main_cache = getattr(self._main_service, "_source_cache_service", None)
        if main_cache is None:
            return entries
        target_bucket = str(bucket_key or duty_date or "").strip()
        if not target_bucket:
            try:
                resolver = getattr(main_cache, "_resolve_latest_refresh_target", None)
                target = resolver(source_family=family) if callable(resolver) else {}
                target_bucket = str(target.get("bucket_key", "") or "").strip() if isinstance(target, dict) else ""
            except Exception:
                target_bucket = ""
        if not target_bucket:
            return entries
        try:
            recovered = self._recover_exact_existing_source_file_to_index(
                main_cache=main_cache,
                source_family=family,
                building=building_name,
                bucket_key=target_bucket,
            )
            if recovered:
                self._emit(
                    "[内网HTTP桥接] source-index 查询时补登记现有源文件: "
                    f"family={family}, building={building_name}, bucket={target_bucket}, "
                    f"path={recovered.get('relative_path') or recovered.get('file_path') or '-'}"
                )
        except Exception as exc:  # noqa: BLE001
            self._emit(
                "[内网HTTP桥接] source-index 现有文件恢复失败: "
                f"family={family}, building={building_name}, bucket={target_bucket}, error={exc}"
            )
            return entries
        main_store = getattr(main_cache, "store", None)
        if main_store is None:
            return entries
        try:
            rows = self._list_source_cache_entries_fast(
                main_store,
                source_family=family,
                building=building_name,
                bucket_kind=bucket_kind,
                bucket_key=bucket_key,
                duty_date=duty_date,
                duty_shift=duty_shift,
                status="ready",
                limit=max(int(limit or 50), 200),
            )
            if rows:
                return rows
            if bucket_key or duty_date:
                return entries
            return self._list_source_cache_entries_fast(
                main_store,
                source_family=family,
                building=building_name,
                status="ready",
                limit=max(int(limit or 50), 200),
            )
        except Exception as exc:  # noqa: BLE001
            self._emit(
                "[内网HTTP桥接] source-index 恢复后重新读取索引失败: "
                f"family={family}, building={building_name}, error={exc}"
            )
            return entries

    def _recover_exact_existing_source_file_to_index(
        self,
        *,
        main_cache: Any,
        source_family: str,
        building: str,
        bucket_key: str,
    ) -> Dict[str, Any] | None:
        candidate_builder = getattr(main_cache, "_candidate_existing_latest_paths", None)
        resolver = getattr(main_cache, "_resolve_relative_path_under_shared_root", None)
        store = getattr(main_cache, "store", None)
        index_store = getattr(store, "_source_cache_index_store", None)
        if not callable(candidate_builder) or not callable(resolver) or index_store is None:
            return None
        now_text = time.strftime("%Y-%m-%d %H:%M:%S")
        for context in candidate_builder(source_family=source_family, building=building, bucket_key=bucket_key) or []:
            if not isinstance(context, dict):
                continue
            relative_path = str(context.get("relative_path", "") or "").replace("\\", "/").strip()
            if not relative_path:
                continue
            candidate = resolver(relative_path)
            if candidate is None:
                continue
            try:
                candidate_path = Path(candidate)
                if not candidate_path.is_file():
                    continue
                size_bytes = int(candidate_path.stat().st_size)
            except OSError:
                continue
            bucket_kind = str(context.get("bucket_kind", "") or "").strip().lower()
            context_bucket = str(context.get("bucket_key", "") or bucket_key).strip()
            duty_date = str(context.get("duty_date", "") or "").strip()
            duty_shift = str(context.get("duty_shift", "") or "").strip().lower()
            entry = {
                "entry_id": "|".join(
                    [
                        str(source_family or "").strip().lower(),
                        bucket_kind,
                        context_bucket,
                        duty_date or "-",
                        duty_shift or "-",
                        str(building or "").strip(),
                    ]
                ),
                "source_family": str(source_family or "").strip().lower(),
                "building": str(building or "").strip(),
                "bucket_kind": bucket_kind,
                "bucket_key": context_bucket,
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "downloaded_at": now_text,
                "relative_path": relative_path,
                "status": "ready",
                "file_hash": "",
                "size_bytes": size_bytes,
                "metadata": {
                    "family": str(source_family or "").strip().lower(),
                    "building": str(building or "").strip(),
                    "recovered_from_existing_file": True,
                    "recovered_by_http_source_index": True,
                    "recovered_at": now_text,
                },
                "created_at": now_text,
                "updated_at": now_text,
            }
            try:
                return index_store.upsert_entry(entry)
            except Exception as exc:  # noqa: BLE001
                self._emit(
                    "[内网HTTP桥接] 现有源文件轻量索引补登记失败: "
                    f"family={source_family}, building={building}, path={relative_path}, error={exc}"
                )
                return None
        return None

    def refresh_latest_source_cache(
        self,
        *,
        source_family: str,
        buildings: List[str],
    ) -> Dict[str, Any]:
        family = str(source_family or "").strip()
        target_buildings = [str(item or "").strip() for item in (buildings or []) if str(item or "").strip()]
        if not family:
            raise ValueError("source_family 不能为空")
        if not target_buildings:
            raise ValueError("buildings 不能为空")
        accepted = len(target_buildings)
        results: List[Dict[str, Any]] = [
            {
                "building": building,
                "source_family": family,
                "accepted": True,
                "running": True,
                "reason": "queued",
            }
            for building in target_buildings
        ]

        def _start_one(building: str) -> None:
            try:
                result = self._main_service.start_building_latest_source_cache_refresh(
                    source_family=family,
                    building=building,
                )
                reason = str(result.get("reason", "") or "").strip() if isinstance(result, dict) else ""
                self._emit(
                    "[内网HTTP桥接] 源文件补采启动结果: "
                    f"family={family}, building={building}, reason={reason or '-'}"
                )
            except Exception as exc:  # noqa: BLE001
                self._emit(
                    "[内网HTTP桥接] 源文件补采后台启动失败: "
                    f"family={family}, building={building}, error={exc}"
                )

        for building in target_buildings:
            thread = threading.Thread(
                target=_start_one,
                args=(building,),
                name=f"internal-http-source-refresh-{family}-{building}",
                daemon=True,
            )
            thread.start()
        return {
            "ok": accepted > 0,
            "source_family": family,
            "requested_buildings": target_buildings,
            "accepted_count": accepted,
            "already_ready_count": 0,
            "results": results,
        }

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
