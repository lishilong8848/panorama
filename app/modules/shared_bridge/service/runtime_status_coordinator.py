from __future__ import annotations

import copy
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict

from app.config.config_adapter import normalize_role_mode
from app.modules.report_pipeline.service.job_panel_presenter import build_job_panel_summary
from app.modules.report_pipeline.service.job_service import TaskEngineUnavailableError
from app.modules.shared_bridge.service.internal_runtime_status_presenter import (
    INTERNAL_RUNTIME_BUILDINGS,
    build_internal_runtime_building_status,
    build_internal_runtime_summary,
)
from app.modules.shared_bridge.service.runtime_status_store import RuntimeStatusStore


_SCOPE_RUNTIME_HEALTH_LITE = "runtime_health_lite"
_SCOPE_INTERNAL_RUNTIME_SUMMARY = "internal_runtime_summary"
_SCOPE_EXTERNAL_SHARED_BRIDGE_FULL = "external_shared_bridge_full"
_SCOPE_JOB_PANEL_SUMMARY = "job_panel_summary"
_SCOPE_DASHBOARD_JOB_PANEL_SUMMARY = "job_panel_dashboard_summary"
_SCOPE_BRIDGE_TASKS_SUMMARY = "bridge_tasks_summary"
_SCOPE_DASHBOARD_BRIDGE_TASKS_SUMMARY = "bridge_tasks_dashboard_summary"
_SCOPE_RUNTIME_RESOURCES_SUMMARY = "runtime_resources_summary"

_EVENT_TRIGGER_KEYWORDS = (
    "[共享桥接]",
    "[共享缓存]",
    "[任务]",
    "浏览器池",
    "楼栋浏览器",
    "页池",
    "补采",
    "下载",
)


class RuntimeStatusWriter:
    def __init__(
        self,
        store: RuntimeStatusStore,
        *,
        emit_log: Callable[[str], None] | None = None,
    ) -> None:
        self._store = store
        self._emit_log = emit_log
        self._lock = threading.Lock()
        self._pending_scopes: Dict[str, Dict[str, Any]] = {}
        self._pending_buildings: Dict[str, Dict[str, Any]] = {}
        self._event = threading.Event()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self.is_running():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="runtime-status-writer",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._event.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=2.0)
        self._thread = None

    def is_running(self) -> bool:
        thread = self._thread
        return bool(thread and thread.is_alive())

    def write_scope_snapshot(self, scope: str, payload: Dict[str, Any]) -> None:
        scope_text = str(scope or "").strip()
        if not scope_text:
            return
        if not self.is_running():
            self.start()
        with self._lock:
            self._pending_scopes[scope_text] = copy.deepcopy(payload if isinstance(payload, dict) else {})
        self._event.set()

    def write_building_snapshot(self, building: str, payload: Dict[str, Any]) -> None:
        building_text = str(building or "").strip()
        if not building_text:
            return
        if not self.is_running():
            self.start()
        with self._lock:
            self._pending_buildings[building_text] = copy.deepcopy(payload if isinstance(payload, dict) else {})
        self._event.set()

    def _drain(self) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        with self._lock:
            scopes = dict(self._pending_scopes)
            buildings = dict(self._pending_buildings)
            self._pending_scopes.clear()
            self._pending_buildings.clear()
        return scopes, buildings

    def _run(self) -> None:
        try:
            self._store.ensure_ready()
        except Exception as exc:  # noqa: BLE001
            if callable(self._emit_log):
                self._emit_log(f"[运行状态] SQLite 状态库初始化失败: {exc}")
            return
        while not self._stop_event.is_set():
            self._event.wait(timeout=0.5)
            self._event.clear()
            scopes, buildings = self._drain()
            if not scopes and not buildings:
                continue
            for scope, payload in scopes.items():
                try:
                    self._store.write_scope_snapshot(scope, payload)
                except Exception as exc:  # noqa: BLE001
                    if callable(self._emit_log):
                        self._emit_log(f"[运行状态] 写入 scope 快照失败: scope={scope}, error={exc}")
            for building, payload in buildings.items():
                try:
                    self._store.write_building_snapshot(building, payload)
                except Exception as exc:  # noqa: BLE001
                    if callable(self._emit_log):
                        self._emit_log(f"[运行状态] 写入楼栋快照失败: building={building}, error={exc}")


class RuntimeStatusCoordinator:
    def __init__(
        self,
        *,
        container: Any,
        runtime_state_root: Path,
        app_state_getter: Callable[[], Dict[str, Any]] | None = None,
        emit_log: Callable[[str], None] | None = None,
        refresh_interval_sec: float = 10.0,
    ) -> None:
        self._container = container
        self._emit_log = emit_log
        self._app_state_getter = app_state_getter or (lambda: {})
        self._refresh_interval_sec = max(1.0, float(refresh_interval_sec or 10.0))
        self._store = RuntimeStatusStore(Path(runtime_state_root) / "runtime_status.sqlite")
        self._writer = RuntimeStatusWriter(self._store, emit_log=emit_log)
        self._snapshot_cache_lock = threading.Lock()
        self._scope_cache: Dict[str, Dict[str, Any]] = {}
        self._building_cache: Dict[str, Dict[str, Any]] = {}
        self._scope_seq = 0
        self._building_seq = 0
        self._refresh_event = threading.Event()
        self._internal_refresh_event = threading.Event()
        self._stop_event = threading.Event()
        self._refresh_requested = False
        self._internal_refresh_requested = False
        self._refresh_thread: threading.Thread | None = None
        self._internal_refresh_thread: threading.Thread | None = None

    @property
    def store(self) -> RuntimeStatusStore:
        return self._store

    def start(self) -> None:
        try:
            self._store.ensure_ready()
        except Exception as exc:  # noqa: BLE001
            if callable(self._emit_log):
                self._emit_log(f"[运行状态] SQLite 状态库初始化失败: {exc}")
        self._writer.start()
        if self.is_running():
            self.request_refresh(reason="startup")
            return
        self._stop_event.clear()
        self._refresh_thread = threading.Thread(
            target=self._run_refresh_loop,
            name="runtime-status-refresh",
            daemon=True,
        )
        self._refresh_thread.start()
        self._internal_refresh_thread = threading.Thread(
            target=self._run_internal_refresh_loop,
            name="runtime-status-internal-refresh",
            daemon=True,
        )
        self._internal_refresh_thread.start()
        self.request_refresh(reason="startup")

    def stop(self) -> None:
        self._stop_event.set()
        self._refresh_event.set()
        self._internal_refresh_event.set()
        thread = self._refresh_thread
        if thread and thread.is_alive():
            thread.join(timeout=2.0)
        self._refresh_thread = None
        internal_thread = self._internal_refresh_thread
        if internal_thread and internal_thread.is_alive():
            internal_thread.join(timeout=2.0)
        self._internal_refresh_thread = None
        self._writer.stop()

    def is_running(self) -> bool:
        thread = self._refresh_thread
        return bool(thread and thread.is_alive())

    def request_refresh(self, *, reason: str = "") -> None:
        _ = reason
        self._refresh_requested = True
        self._refresh_event.set()
        self._internal_refresh_requested = True
        self._internal_refresh_event.set()

    def request_internal_runtime_refresh(self, *, reason: str = "") -> None:
        _ = reason
        self._internal_refresh_requested = True
        self._internal_refresh_event.set()

    def observe_log_line(self, text: str) -> None:
        raw = str(text or "").strip()
        if not raw:
            return
        if any(keyword in raw for keyword in _EVENT_TRIGGER_KEYWORDS):
            self.request_refresh(reason="log")

    def read_scope_snapshot(self, scope: str) -> Dict[str, Any] | None:
        scope_text = str(scope or "").strip()
        if not scope_text:
            return None
        with self._snapshot_cache_lock:
            entry = self._scope_cache.get(scope_text)
            if isinstance(entry, dict):
                return copy.deepcopy(entry)
        snapshot = self._store.read_scope_snapshot(scope_text)
        if isinstance(snapshot, dict) and snapshot:
            with self._snapshot_cache_lock:
                self._scope_cache[scope_text] = copy.deepcopy(snapshot)
        return snapshot

    def read_building_snapshot(self, building: str) -> Dict[str, Any] | None:
        building_text = str(building or "").strip()
        if not building_text:
            return None
        with self._snapshot_cache_lock:
            entry = self._building_cache.get(building_text)
            if isinstance(entry, dict):
                return copy.deepcopy(entry)
        snapshot = self._store.read_building_snapshot(building_text)
        if isinstance(snapshot, dict) and snapshot:
            with self._snapshot_cache_lock:
                self._building_cache[building_text] = copy.deepcopy(snapshot)
        return snapshot

    def refresh_now(self) -> None:
        self._refresh_all_snapshots()
        self._refresh_internal_runtime_snapshots()

    def _cache_scope_snapshot(self, scope: str, payload: Dict[str, Any]) -> None:
        scope_text = str(scope or "").strip()
        if not scope_text:
            return
        payload_value = copy.deepcopy(payload if isinstance(payload, dict) else {})
        with self._snapshot_cache_lock:
            self._scope_seq += 1
            self._scope_cache[scope_text] = {
                "scope": scope_text,
                "payload": payload_value,
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "seq": self._scope_seq,
            }

    def _cache_building_snapshot(self, building: str, payload: Dict[str, Any]) -> None:
        building_text = str(building or "").strip()
        if not building_text:
            return
        payload_value = copy.deepcopy(payload if isinstance(payload, dict) else {})
        with self._snapshot_cache_lock:
            self._building_seq += 1
            self._building_cache[building_text] = {
                "building": building_text,
                "payload": payload_value,
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "seq": self._building_seq,
            }

    def _write_scope_snapshot(self, scope: str, payload: Dict[str, Any]) -> None:
        self._cache_scope_snapshot(scope, payload)
        self._writer.write_scope_snapshot(scope, payload)

    def _write_building_snapshot(self, building: str, payload: Dict[str, Any]) -> None:
        self._cache_building_snapshot(building, payload)
        self._writer.write_building_snapshot(building, payload)

    def _run_refresh_loop(self) -> None:
        next_run = time.monotonic()
        while not self._stop_event.is_set():
            timeout = max(0.0, next_run - time.monotonic())
            self._refresh_event.wait(timeout=timeout)
            self._refresh_event.clear()
            if self._stop_event.is_set():
                break
            now = time.monotonic()
            if now < next_run:
                continue
            self._refresh_requested = False
            self._refresh_all_snapshots()
            next_run = time.monotonic() + self._refresh_interval_sec

    def _run_internal_refresh_loop(self) -> None:
        next_run = time.monotonic()
        interval_sec = min(2.0, self._refresh_interval_sec)
        while not self._stop_event.is_set():
            timeout = max(0.0, next_run - time.monotonic())
            self._internal_refresh_event.wait(timeout=timeout)
            self._internal_refresh_event.clear()
            if self._stop_event.is_set():
                break
            now = time.monotonic()
            if now < next_run:
                continue
            self._internal_refresh_requested = False
            self._refresh_internal_runtime_snapshots()
            next_run = time.monotonic() + interval_sec

    def _refresh_all_snapshots(self) -> None:
        try:
            deployment = (
                self._container.deployment_snapshot()
                if hasattr(self._container, "deployment_snapshot")
                else {}
            )
            role_mode = normalize_role_mode(
                deployment.get("role_mode") if isinstance(deployment, dict) else ""
            )
            prebuilt_shared_bridge_snapshot: Dict[str, Any] | None = None
            if role_mode == "external":
                prebuilt_shared_bridge_snapshot = self._safe_shared_bridge_snapshot(mode="external_full")
                self._write_scope_snapshot(
                    _SCOPE_EXTERNAL_SHARED_BRIDGE_FULL,
                    prebuilt_shared_bridge_snapshot,
                )
            self._write_scope_snapshot(
                _SCOPE_RUNTIME_HEALTH_LITE,
                self._build_runtime_health_lite_snapshot(
                    shared_bridge_snapshot=prebuilt_shared_bridge_snapshot,
                ),
            )
            self._write_scope_snapshot(
                _SCOPE_DASHBOARD_JOB_PANEL_SUMMARY,
                self._build_job_panel_dashboard_summary(),
            )
            self._write_scope_snapshot(_SCOPE_RUNTIME_RESOURCES_SUMMARY, self._build_runtime_resources_summary())
            self._write_scope_snapshot(
                _SCOPE_DASHBOARD_BRIDGE_TASKS_SUMMARY,
                self._build_bridge_tasks_dashboard_summary(),
            )
        except Exception as exc:  # noqa: BLE001
            if callable(self._emit_log):
                self._emit_log(f"[运行状态] 刷新状态快照失败: {exc}")

    def _refresh_internal_runtime_snapshots(self) -> None:
        try:
            deployment = (
                self._container.deployment_snapshot()
                if hasattr(self._container, "deployment_snapshot")
                else {}
            )
            role_mode = normalize_role_mode(
                deployment.get("role_mode") if isinstance(deployment, dict) else ""
            )
            if role_mode != "internal":
                return
            snapshot = self._safe_shared_bridge_snapshot(mode="internal_light")
        except Exception as exc:  # noqa: BLE001
            if callable(self._emit_log):
                self._emit_log(f"[运行状态] 读取共享桥接轻量快照失败: {exc}")
            return
        normalized_snapshot = snapshot if isinstance(snapshot, dict) else {}
        for building_code, building in INTERNAL_RUNTIME_BUILDINGS.items():
            self._write_building_snapshot(
                building,
                build_internal_runtime_building_status(
                    normalized_snapshot,
                    building=building,
                    building_code=building_code,
                ),
            )
        self._write_scope_snapshot(
            _SCOPE_INTERNAL_RUNTIME_SUMMARY,
            build_internal_runtime_summary(normalized_snapshot),
        )

    def _build_runtime_health_lite_snapshot(
        self,
        *,
        shared_bridge_snapshot: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        app_state = self._app_state_getter() if callable(self._app_state_getter) else {}
        deployment = (
            self._container.deployment_snapshot()
            if hasattr(self._container, "deployment_snapshot")
            else {}
        )
        role_mode = normalize_role_mode(
            deployment.get("role_mode") if isinstance(deployment, dict) else ""
        )
        shared_bridge_mode = "internal_light" if role_mode == "internal" else "external_full"
        shared_bridge = (
            dict(shared_bridge_snapshot)
            if isinstance(shared_bridge_snapshot, dict)
            else self._safe_shared_bridge_snapshot(mode=shared_bridge_mode)
        )
        return {
            "ok": True,
            "health_mode": "lite",
            "version": str(getattr(self._container, "version", "") or ""),
            "config_version": int(getattr(self._container, "config", {}).get("version", 0) or 0)
            if isinstance(getattr(self._container, "config", None), dict)
            else 0,
            "active_job_id": self._safe_active_job_id(),
            "active_job_ids": self._safe_active_job_ids(),
            "job_counts": self._safe_job_counts(),
            "deployment": deployment if isinstance(deployment, dict) else {},
            "shared_bridge": shared_bridge if isinstance(shared_bridge, dict) else {},
            "runtime_activated": bool(app_state.get("runtime_activated", False)),
            "activation_phase": str(app_state.get("activation_phase", "") or "").strip(),
            "activation_error": str(app_state.get("activation_error", "") or "").strip(),
            "startup_role_confirmed": bool(app_state.get("startup_role_confirmed", False)),
            "started_at": str(app_state.get("started_at", "") or "").strip(),
        }

    def _safe_shared_bridge_snapshot(self, *, mode: str) -> Dict[str, Any]:
        getter = getattr(self._container, "shared_bridge_snapshot", None)
        if not callable(getter):
            return {}
        try:
            payload = getter(mode=mode)
        except TypeError:
            payload = getter()
        if isinstance(payload, dict):
            return dict(payload)
        return {}

    def _build_job_panel_summary(self) -> Dict[str, Any]:
        return build_job_panel_summary(self._container, limit=60, emit_log=self._emit_log)

    def _build_job_panel_dashboard_summary(self) -> Dict[str, Any]:
        return build_job_panel_summary(self._container, limit=12, emit_log=self._emit_log)

    def _build_runtime_resources_summary(self) -> Dict[str, Any]:
        try:
            snapshot = self._container.job_service.get_resource_snapshot()
        except TaskEngineUnavailableError:
            snapshot = None
        except Exception as exc:  # noqa: BLE001
            if callable(self._emit_log):
                self._emit_log(f"[运行状态] 刷新资源摘要失败: {exc}")
            snapshot = None
        if isinstance(snapshot, dict):
            return snapshot
        return {
            "network": {},
            "controlled_browser": {"holder_job_id": "", "queue_length": 0},
            "batch_locks": [],
            "resources": [],
        }

    def _build_bridge_tasks_summary(self) -> Dict[str, Any]:
        return self._build_bridge_tasks_summary_with_limit(limit=60)

    def _build_bridge_tasks_dashboard_summary(self) -> Dict[str, Any]:
        return self._build_bridge_tasks_summary_with_limit(limit=12)

    def _build_bridge_tasks_summary_with_limit(self, *, limit: int) -> Dict[str, Any]:
        service = getattr(self._container, "shared_bridge_service", None)
        tasks = []
        if service is None:
            return {"tasks": [], "count": 0}
        try:
            tasks = service.list_tasks(limit=limit)
        except Exception as exc:  # noqa: BLE001
            checker = getattr(service, "_is_recoverable_store_error", None)
            if callable(checker) and checker(exc):
                cache_reader = getattr(service, "get_cached_tasks", None)
                tasks = cache_reader(limit=limit) if callable(cache_reader) else []
            else:
                if callable(self._emit_log):
                    self._emit_log(f"[运行状态] 刷新共享桥接任务摘要失败: {exc}")
                tasks = []
        normalized_tasks = [
            task
            for task in (tasks if isinstance(tasks, list) else [])
            if str(task.get("feature", "") or "").strip().lower() != "alarm_export"
        ]
        return {"tasks": normalized_tasks, "count": len(normalized_tasks)}

    def _safe_active_job_id(self) -> str:
        getter = getattr(self._container.job_service, "active_job_id", None)
        if callable(getter):
            try:
                return str(getter() or "").strip()
            except Exception:
                return ""
        return ""

    def _safe_active_job_ids(self) -> list[str]:
        getter = getattr(self._container.job_service, "active_job_ids", None)
        if callable(getter):
            try:
                rows = getter(include_waiting=True)
            except Exception:
                rows = []
            if isinstance(rows, list):
                return [str(item or "").strip() for item in rows if str(item or "").strip()]
        return []

    def _safe_job_counts(self) -> Dict[str, Any]:
        getter = getattr(self._container.job_service, "job_counts", None)
        if callable(getter):
            try:
                payload = getter()
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                return dict(payload)
        return {"queued": 0, "running": 0, "finished": 0, "failed": 0}
