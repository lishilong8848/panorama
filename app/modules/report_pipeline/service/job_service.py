from __future__ import annotations

import contextlib
import io
import json
import os
import queue
import signal
import sqlite3
import socket
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from app.modules.report_pipeline.service.task_engine_database import TaskEngineDatabase
from app.modules.report_pipeline.service.task_engine_store import TaskEngineStore
from app.modules.updater.service.runtime_dependency_sync_service import RuntimeDependencySyncService


class JobBusyError(RuntimeError):
    pass


class TaskEngineUnavailableError(RuntimeError):
    pass


def _is_recoverable_task_engine_error(exc: Exception) -> bool:
    if isinstance(exc, TaskEngineUnavailableError):
        return True
    if isinstance(exc, PermissionError):
        return True
    text = f"{type(exc).__name__}: {exc}".lower()
    tokens = (
        "database is locked",
        "database table is locked",
        "database is busy",
        "busy",
        "unable to open database file",
        "disk i/o error",
        "readonly database",
        "cannot operate on a closed database",
        "permission denied",
        "winerror 5",
    )
    if isinstance(exc, sqlite3.Error):
        return any(token in text for token in tokens)
    return any(token in text for token in tokens)


_INCOMPLETE_JOB_STATUSES = {"queued", "waiting_resource", "running"}
_RUNNING_JOB_STATUSES = {"running"}
_RESOURCE_CAPACITY_OVERRIDES = {
    "network:external": 3,
    "network:internal": 2,
    "network:pipeline": 1,
    "browser:controlled": 1,
    "updater:global": 1,
}
_WORKER_PYTHON_PROBE_CODE = "import encodings, json, sys; print(sys.executable)"
_WORKER_IMPORT_PROBE_CODE = "import encodings, json, sys; import app.worker.entry; print('ok')"
_WORKER_RUNTIME_REPAIR_TOKENS = (
    "no module named 'encodings'",
    "failed to import encodings module",
    "could not find platform independent libraries",
    "modulenotfounderror",
    "no module named",
)


@dataclass
class JobState:
    job_id: str
    name: str
    feature: str = ""
    dedupe_key: str = ""
    submitted_by: str = "manual"
    status: str = "queued"
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    started_at: str = ""
    finished_at: str = ""
    summary: str = ""
    error: str = ""
    result: Any = None
    logs: List[str] = field(default_factory=list)
    done_event: threading.Event = field(default_factory=threading.Event)
    thread: Optional[threading.Thread] = None
    priority: str = "manual"
    resource_keys: List[str] = field(default_factory=list)
    wait_reason: str = ""
    bridge_task_id: str = ""
    sequence: int = 0
    acquired_resources: List[str] = field(default_factory=list)
    stages: List["StageState"] = field(default_factory=list)
    cancel_requested: bool = False
    revision: int = 0
    last_event_id: int = 0
    wait_started_monotonic: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "name": self.name,
            "feature": self.feature,
            "dedupe_key": self.dedupe_key,
            "submitted_by": self.submitted_by,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "summary": self.summary,
            "error": self.error,
            "result": self.result,
            "log_count": len(self.logs),
            "priority": self.priority,
            "resource_keys": list(self.resource_keys),
            "wait_reason": self.wait_reason,
            "bridge_task_id": self.bridge_task_id,
            "stages": [stage.to_dict() for stage in self.stages],
            "cancel_requested": bool(self.cancel_requested),
            "revision": int(self.revision or 0),
            "last_event_id": int(self.last_event_id or 0),
        }


@dataclass
class StageState:
    stage_id: str
    name: str
    status: str = "pending"
    resource_keys: List[str] = field(default_factory=list)
    resume_policy: str = "manual_resume"
    worker_handler: str = ""
    worker_pid: int = 0
    started_at: str = ""
    finished_at: str = ""
    summary: str = ""
    error: str = ""
    result: Any = None
    cancel_requested: bool = False
    revision: int = 0
    worker_status: str = ""
    last_heartbeat_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "name": self.name,
            "status": self.status,
            "resource_keys": list(self.resource_keys),
            "resume_policy": self.resume_policy,
            "worker_handler": self.worker_handler,
            "worker_pid": int(self.worker_pid or 0),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "summary": self.summary,
            "error": self.error,
            "result": self.result,
            "cancel_requested": bool(self.cancel_requested),
            "revision": int(self.revision or 0),
            "worker_status": self.worker_status,
            "last_heartbeat_at": self.last_heartbeat_at,
        }


class _LineEmitter:
    def __init__(self, emit_line: Callable[[str], None]) -> None:
        self.emit_line = emit_line
        self._buffer = ""

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._buffer += text.replace("\r\n", "\n").replace("\r", "\n")
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.strip()
            if line:
                self.emit_line(line)
        return len(text)

    def flush(self) -> None:
        line = self._buffer.strip()
        if line:
            self.emit_line(line)
        self._buffer = ""


class _StreamProxy(io.TextIOBase):
    def __init__(self, emitter: _LineEmitter) -> None:
        super().__init__()
        self._emitter = emitter

    def write(self, s: str) -> int:  # type: ignore[override]
        return self._emitter.write(s)

    def flush(self) -> None:  # type: ignore[override]
        self._emitter.flush()


class JobService:
    TASK_ENGINE_CLEANUP_INTERVAL_SEC = 600.0

    def __init__(self, log_buffer_size: int = 5000) -> None:
        self.log_buffer_size = max(200, int(log_buffer_size))
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._network_status_lock = threading.Lock()
        self._jobs: Dict[str, JobState] = {}
        self._job_sequence = 0
        self._global_log_sink: Callable[[str], None] | None = None
        self._global_log_sink_queue: queue.Queue[str] = queue.Queue(maxsize=10000)
        self._global_log_sink_worker = threading.Thread(
            target=self._global_log_sink_loop,
            daemon=True,
            name="job-global-log-sink",
        )
        self._global_log_sink_worker.start()
        self._resource_holders: Dict[str, List[str]] = {}
        self._task_engine_store: TaskEngineStore | None = None
        self._task_engine_db: TaskEngineDatabase | None = None
        self._runtime_config: Dict[str, Any] = {}
        self._config_snapshot_getter: Callable[[], Dict[str, Any]] | None = None
        self._current_ssid_getter: Callable[[], str | None] | None = None
        self._worker_app_dir: Path = Path.cwd()
        self._network_auto_switch_enabled = False
        self._network_internal_ssid = ""
        self._network_external_ssid = ""
        self._network_probe_internal_host = ""
        self._network_probe_internal_port = 80
        self._network_probe_external_host = "open.feishu.cn"
        self._network_probe_external_port = 443
        self._network_probe_timeout_sec = 2.0
        self._network_probe_retries = 3
        self._network_probe_interval_sec = 1.0
        self._network_internal_probe_timeout_ms = 1200
        self._network_internal_probe_parallelism = 5
        self._network_probe_sites: List[Dict[str, Any]] = []
        self._network_status_cache_ttl_sec = 2.0
        self._network_status_checked_monotonic = 0.0
        self._network_status_snapshot: Dict[str, Any] = {}
        self._network_window_current_side = "none"
        self._network_window_started_at = ""
        self._network_window_started_monotonic = 0.0
        self._network_window_dispatch_count = 0
        self._network_window_draining = False
        self._network_window_pending_side = ""
        self._network_window_max_duration_sec = 600
        self._network_window_max_dispatches = 20
        self._network_window_max_opposite_wait_sec = 120
        self._worker_processes: Dict[tuple[str, str], subprocess.Popen[str]] = {}
        self._worker_control_ports: Dict[tuple[str, str], int] = {}
        self._worker_force_killed: set[tuple[str, str]] = set()
        self._worker_cancel_timeout_sec = 10.0
        self._worker_heartbeat_interval_sec = 5.0
        self._worker_dependency_sync_lock = threading.Lock()
        self._worker_python_probe_cache: Dict[str, tuple[bool, str]] = {}
        self._worker_python_fallback_logged: set[str] = set()
        self._worker_runtime_ready_cache: Dict[str, str] = {}
        self._resource_snapshot_dirty = False
        self._resource_snapshot_last_flushed_monotonic = 0.0
        self._resource_snapshot_flush_interval_sec = 1.0
        self._resource_snapshot_flush_timer: threading.Timer | None = None
        self._task_engine_recovery_completed = False
        self._task_engine_last_cleanup_monotonic = 0.0

    def update_log_buffer_size(self, value: int) -> None:
        self.log_buffer_size = max(200, int(value))

    def set_global_log_sink(self, sink: Callable[[str], None] | None) -> None:
        self._global_log_sink = sink

    def _global_log_sink_loop(self) -> None:
        while True:
            text = self._global_log_sink_queue.get()
            sink = self._global_log_sink
            if not callable(sink):
                continue
            try:
                sink(text)
            except Exception:  # noqa: BLE001
                pass

    def configure_task_engine(
        self,
        *,
        runtime_config: dict[str, Any] | None = None,
        app_dir: Path | None = None,
        config_snapshot_getter: Callable[[], Dict[str, Any]] | None = None,
        worker_app_dir: Path | None = None,
        current_ssid_getter: Callable[[], str | None] | None = None,
    ) -> None:
        with self._lock:
            self._cancel_resource_snapshot_flush_timer_locked()
        previous_db = self._task_engine_db
        if previous_db is not None:
            previous_db.close()
        self._runtime_config = runtime_config if isinstance(runtime_config, dict) else {}
        self._task_engine_store = TaskEngineStore(runtime_config=runtime_config, app_dir=app_dir)
        self._task_engine_db = TaskEngineDatabase(runtime_config=runtime_config, app_dir=app_dir)
        self._config_snapshot_getter = config_snapshot_getter
        self._current_ssid_getter = current_ssid_getter
        self._worker_runtime_ready_cache.clear()
        self._worker_app_dir = Path(worker_app_dir or app_dir or Path.cwd()).resolve()
        network_runtime_cfg = runtime_config.get("network", {}) if isinstance(runtime_config, dict) else {}
        if not isinstance(network_runtime_cfg, dict):
            network_runtime_cfg = {}
        self._network_auto_switch_enabled = False
        self._network_internal_ssid = str(network_runtime_cfg.get("internal_ssid", "") or "").strip()
        self._network_external_ssid = str(network_runtime_cfg.get("external_ssid", "") or "").strip()
        self._network_probe_internal_host = str(network_runtime_cfg.get("post_switch_probe_internal_host", "") or "").strip()
        self._network_probe_internal_port = int(network_runtime_cfg.get("post_switch_probe_internal_port", 80) or 80)
        self._network_probe_external_host = str(network_runtime_cfg.get("post_switch_probe_external_host", "open.feishu.cn") or "open.feishu.cn").strip()
        self._network_probe_external_port = int(network_runtime_cfg.get("post_switch_probe_external_port", 443) or 443)
        self._network_probe_timeout_sec = float(network_runtime_cfg.get("post_switch_probe_timeout_sec", 2) or 2)
        self._network_probe_retries = int(network_runtime_cfg.get("post_switch_probe_retries", 3) or 3)
        self._network_probe_interval_sec = float(network_runtime_cfg.get("post_switch_probe_interval_sec", 1) or 1)
        self._network_internal_probe_timeout_ms = int(network_runtime_cfg.get("internal_probe_timeout_ms", 1200) or 1200)
        self._network_internal_probe_parallelism = int(network_runtime_cfg.get("internal_probe_parallelism", 5) or 5)
        download_runtime_cfg = runtime_config.get("download", {}) if isinstance(runtime_config, dict) else {}
        sites_cfg = download_runtime_cfg.get("sites", []) if isinstance(download_runtime_cfg, dict) else []
        self._network_probe_sites = [site for site in sites_cfg if isinstance(site, dict)]
        internal_probe_ttl_sec = max(
            0.5,
            float(network_runtime_cfg.get("internal_probe_cache_ttl_sec", 2) or 2),
        )
        external_probe_ttl_sec = max(
            0.5,
            float(network_runtime_cfg.get("external_probe_cache_ttl_sec", 2) or 2),
        )
        self._network_status_cache_ttl_sec = min(internal_probe_ttl_sec, external_probe_ttl_sec)
        with self._network_status_lock:
            self._network_status_checked_monotonic = 0.0
            self._network_status_snapshot = {}
        with self._lock:
            self._resource_snapshot_dirty = False
            self._resource_snapshot_last_flushed_monotonic = 0.0
            self._cancel_resource_snapshot_flush_timer_locked()
        execution_cfg = runtime_config.get("execution", {}) if isinstance(runtime_config, dict) else {}
        network_cfg = execution_cfg.get("network", {}) if isinstance(execution_cfg, dict) else {}
        self._network_window_max_duration_sec = max(30, int(network_cfg.get("max_window_duration_sec", 600) or 600))
        self._network_window_max_dispatches = max(1, int(network_cfg.get("max_dispatches_per_window", 20) or 20))
        self._network_window_max_opposite_wait_sec = max(5, int(network_cfg.get("max_opposite_wait_sec", 120) or 120))
        self._worker_cancel_timeout_sec = max(1.0, float(execution_cfg.get("graceful_cancel_timeout_sec", 10) or 10))
        self._worker_heartbeat_interval_sec = max(1.0, float(execution_cfg.get("worker_heartbeat_interval_sec", 5) or 5))
        self._persist_resource_snapshot(force=True)
        self._restore_incomplete_jobs()

    def shutdown_task_engine(self) -> None:
        self._persist_resource_snapshot(force=True)
        with self._lock:
            self._cancel_resource_snapshot_flush_timer_locked()
        db = self._task_engine_db
        self._task_engine_db = None
        if db is not None:
            db.close()

    def cleanup_terminal_jobs(self, *, retention_days: int = 14) -> int:
        if not self._task_engine_db:
            return 0
        return int(self._task_engine_db.cleanup_terminal_jobs(retention_days=retention_days) or 0)

    def _maybe_cleanup_task_engine(self) -> None:
        if not self._task_engine_db:
            return
        now_monotonic = time.monotonic()
        if now_monotonic - float(self._task_engine_last_cleanup_monotonic or 0.0) < self.TASK_ENGINE_CLEANUP_INTERVAL_SEC:
            return
        self._task_engine_last_cleanup_monotonic = now_monotonic
        try:
            self._task_engine_db.cleanup_terminal_jobs(retention_days=14)
        except Exception:  # noqa: BLE001
            pass

    def task_engine_runtime_snapshot(self) -> Dict[str, Any]:
        self._maybe_cleanup_task_engine()
        if not self._task_engine_db:
            return {"write_queue_length": 0, "last_cleanup_at": "", "closed": True}
        try:
            return self._task_engine_db.runtime_snapshot()
        except Exception as exc:  # noqa: BLE001
            if _is_recoverable_task_engine_error(exc):
                return {
                    "write_queue_length": 0,
                    "last_cleanup_at": "",
                    "closed": False,
                    "degraded": True,
                    "last_error": "任务状态存储暂时不可用，已降级为内存结果",
                }
            raise

    def _next_sequence(self) -> int:
        self._job_sequence += 1
        return self._job_sequence

    def _ordered_jobs(self) -> List[JobState]:
        return sorted(self._jobs.values(), key=lambda item: item.sequence)

    def active_job_id(self) -> str:
        with self._lock:
            running = [job for job in self._ordered_jobs() if job.status in _RUNNING_JOB_STATUSES]
            if running:
                return running[0].job_id
            pending = [job for job in self._ordered_jobs() if job.status in _INCOMPLETE_JOB_STATUSES]
            return pending[0].job_id if pending else ""

    def active_job_ids(self, *, include_waiting: bool = True) -> List[str]:
        with self._lock:
            target_statuses = _INCOMPLETE_JOB_STATUSES if include_waiting else _RUNNING_JOB_STATUSES
            return [job.job_id for job in self._ordered_jobs() if job.status in target_statuses]

    def has_incomplete_jobs(self) -> bool:
        return bool(self.active_job_ids(include_waiting=True))

    def has_running_jobs(self) -> bool:
        return bool(self.active_job_ids(include_waiting=False))

    def _append_log(self, job: JobState, text: str) -> None:
        raw = str(text or "").strip()
        if not raw:
            return
        timestamp = self._now_text()
        line = f"[{timestamp}] {raw}"
        job.logs.append(line)
        overflow = len(job.logs) - self.log_buffer_size
        if overflow > 0:
            del job.logs[:overflow]
        self._record_job_event_async(
            job,
            stage_id=self._get_primary_stage(job).stage_id,
            stream="job",
            event_type="log",
            level="info",
            payload={"line": line, "message": raw, "timestamp": timestamp},
        )

    @staticmethod
    def _write_console_line(text: str) -> None:
        line = str(text or "").strip()
        if not line:
            return
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = f"[{ts}] {line}\n"
        stream = getattr(sys, "__stdout__", None) or getattr(sys, "__stderr__", None)
        try:
            if stream is not None:
                stream.write(payload)
                stream.flush()
        except Exception:  # noqa: BLE001
            pass

    @staticmethod
    def _normalize_resource_keys(resource_keys: List[str] | tuple[str, ...] | None) -> List[str]:
        normalized: List[str] = []
        for item in resource_keys or []:
            text = str(item or "").strip()
            if text and text not in normalized:
                normalized.append(text)
        return normalized

    @staticmethod
    def _normalize_dedupe_key(dedupe_key: str | None) -> str:
        return str(dedupe_key or "").strip()

    def _find_active_job_by_dedupe_key_locked(self, dedupe_key: str) -> JobState | None:
        normalized = self._normalize_dedupe_key(dedupe_key)
        if not normalized:
            return None
        candidates = [
            job for job in self._ordered_jobs()
            if str(job.dedupe_key or "").strip() == normalized
            and str(job.status or "").strip().lower() in _INCOMPLETE_JOB_STATUSES
        ]
        return candidates[-1] if candidates else None

    def _restore_job_from_db_locked(self, job_id: str) -> JobState | None:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id or not self._task_engine_db:
            return None
        snapshot = self._task_engine_db.get_job(normalized_job_id)
        if not isinstance(snapshot, dict):
            return None
        job = self._job_from_snapshot(snapshot)
        self._jobs[normalized_job_id] = job
        return job

    def find_active_job_by_dedupe_key(self, dedupe_key: str) -> Dict[str, Any] | None:
        normalized = self._normalize_dedupe_key(dedupe_key)
        if not normalized:
            return None
        with self._lock:
            existing = self._find_active_job_by_dedupe_key_locked(normalized)
            if existing is not None:
                return existing.to_dict()
        if self._task_engine_db:
            return self._task_engine_db.find_active_job_by_dedupe_key(normalized, statuses=sorted(_INCOMPLETE_JOB_STATUSES))
        return None

    @staticmethod
    def _resource_capacity(resource_key: str) -> int:
        text = str(resource_key or "").strip()
        if not text:
            return 1
        if text.startswith("handover_batch:"):
            return 1
        if text.startswith("output_path:"):
            return 1
        return max(1, int(_RESOURCE_CAPACITY_OVERRIDES.get(text, 1) or 1))

    @staticmethod
    def _network_side_from_resource_key(resource_key: str) -> str:
        text = str(resource_key or "").strip().lower()
        if text == "network:internal":
            return "internal"
        if text == "network:external":
            return "external"
        if text == "network:pipeline":
            return "pipeline"
        return ""

    @staticmethod
    def _normalize_ssid(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _ssid_matches(current_ssid: str, target_ssid: str) -> bool:
        current_text = str(current_ssid or "").strip()
        target_text = str(target_ssid or "").strip()
        if not current_text or not target_text:
            return False
        return current_text.casefold() == target_text.casefold()

    @staticmethod
    def _priority_weight(priority: str) -> int:
        text = str(priority or "").strip().lower()
        if text == "manual":
            return 0
        if text == "resume":
            return 1
        if text == "scheduler":
            return 2
        return 3

    @staticmethod
    def _format_wait_reason(resource_keys: List[str]) -> str:
        tokens: List[str] = []
        for item in resource_keys:
            text = str(item or "").strip().lower()
            if not text:
                continue
            if text == "browser:controlled":
                token = "waiting:browser_controlled"
            elif text == "network:pipeline":
                token = "waiting:network_pipeline"
            elif text == "network:internal":
                token = "waiting:network_internal"
            elif text == "network:external":
                token = "waiting:network_external"
            elif text.startswith("handover_batch:"):
                token = "waiting:handover_batch"
            elif text.startswith("output_path:"):
                token = "waiting:output_path"
            elif text.startswith("source_identity:"):
                token = "waiting:source_identity"
            elif text.startswith("updater:"):
                token = "waiting:app_update"
            else:
                token = f"waiting:{text.replace(':', '_')}"
            if token not in tokens:
                tokens.append(token)
        return ", ".join(tokens)

    @staticmethod
    def _blocks_resource_queue(candidate: JobState) -> bool:
        status = str(candidate.status or "").strip().lower()
        if status not in {"queued", "waiting_resource"}:
            return False
        wait_reason = str(candidate.wait_reason or "").strip().lower()
        if status == "waiting_resource" and wait_reason == "waiting:shared_bridge":
            return False
        return True

    @staticmethod
    def _json_ready(value: Any) -> Any:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))

    @staticmethod
    def _default_stage_name(job: JobState) -> str:
        return str(job.feature or job.name or "stage").strip() or "stage"

    def _get_primary_stage(self, job: JobState) -> StageState:
        if job.stages:
            return job.stages[0]
        stage = StageState(
            stage_id="main",
            name=self._default_stage_name(job),
            resource_keys=list(job.resource_keys),
            resume_policy="manual_resume",
        )
        job.stages = [stage]
        return stage

    def _capture_config_snapshot(self) -> Dict[str, Any] | None:
        if not callable(self._config_snapshot_getter):
            return None
        try:
            snapshot = self._config_snapshot_getter()
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(snapshot, dict):
            return None
        return self._json_ready(snapshot)

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _record_job_event(
        self,
        job: JobState,
        *,
        stage_id: str = "",
        stream: str = "job",
        event_type: str = "log",
        level: str = "info",
        payload: Dict[str, Any] | None = None,
    ) -> int:
        if not self._task_engine_db:
            return 0
        event_id = int(
            self._task_engine_db.append_job_event(
                job_id=job.job_id,
                stage_id=stage_id,
                stream=stream,
                event_type=event_type,
                level=level,
                payload=payload or {},
                created_at=self._now_text(),
            )
            or 0
        )
        job.last_event_id = max(job.last_event_id, event_id)
        return event_id

    def _record_job_event_async(
        self,
        job: JobState,
        *,
        stage_id: str = "",
        stream: str = "job",
        event_type: str = "log",
        level: str = "info",
        payload: Dict[str, Any] | None = None,
    ) -> None:
        if not self._task_engine_db:
            return
        append_async = getattr(self._task_engine_db, "append_job_event_async", None)
        if not callable(append_async):
            self._record_job_event(
                job,
                stage_id=stage_id,
                stream=stream,
                event_type=event_type,
                level=level,
                payload=payload,
            )
            return
        ok = bool(
            append_async(
                job_id=job.job_id,
                stage_id=stage_id,
                stream=stream,
                event_type=event_type,
                level=level,
                payload=payload or {},
                created_at=self._now_text(),
            )
        )
        if not ok:
            self._write_console_line(f"[任务日志] SQLite 日志队列繁忙，已跳过一条后台日志: job={job.job_id}")

    def _persist_stage_snapshot(self, job: JobState, stage: StageState) -> None:
        if not self._task_engine_db:
            return
        self._task_engine_db.upsert_stage(job.job_id, stage.to_dict())

    def _cancel_resource_snapshot_flush_timer_locked(self) -> None:
        timer = self._resource_snapshot_flush_timer
        self._resource_snapshot_flush_timer = None
        if timer is not None:
            timer.cancel()

    def _schedule_resource_snapshot_flush_locked(self) -> None:
        if self._resource_snapshot_flush_timer is not None:
            return
        timer = threading.Timer(self._resource_snapshot_flush_interval_sec, self._flush_resource_snapshot_from_timer)
        timer.daemon = True
        self._resource_snapshot_flush_timer = timer
        timer.start()

    def _flush_resource_snapshot_from_timer(self) -> None:
        with self._lock:
            self._resource_snapshot_flush_timer = None
        try:
            self._persist_resource_snapshot(force=True)
        except Exception:  # noqa: BLE001
            pass

    def _persist_job_snapshot(self, job: JobState, *, config_snapshot: dict[str, Any] | None = None) -> None:
        if not self._task_engine_db:
            return
        job.revision = int(job.revision or 0) + 1
        for stage in job.stages:
            stage.revision = int(stage.revision or 0) + 1
        payload = job.to_dict()
        self._task_engine_db.upsert_job(payload, config_snapshot=config_snapshot)
        for stage in job.stages:
            self._persist_stage_snapshot(job, stage)

    def _persist_resource_snapshot(self, *, force: bool = False) -> None:
        if not self._task_engine_db:
            return
        with self._lock:
            self._resource_snapshot_dirty = True
            if not force:
                self._schedule_resource_snapshot_flush_locked()
                return
            self._cancel_resource_snapshot_flush_timer_locked()
            self._resource_snapshot_dirty = False
            self._resource_snapshot_last_flushed_monotonic = time.monotonic()
        snapshot = self._build_resource_snapshot_from_memory()
        snapshot["updated_at"] = self._now_text()
        try:
            self._task_engine_db.persist_resource_snapshot(snapshot)
        except Exception:
            with self._lock:
                self._resource_snapshot_dirty = True
                if self._task_engine_db is not None:
                    self._schedule_resource_snapshot_flush_locked()
            raise

    @staticmethod
    def _build_clean_python_env(base_env: Dict[str, str] | None = None) -> Dict[str, str]:
        env = dict(base_env or os.environ)
        env.pop("PYTHONHOME", None)
        env.pop("PYTHONPATH", None)
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        return env

    def _emit_service_log(self, text: str) -> None:
        line = str(text or "").strip()
        if not line:
            return
        sink = self._global_log_sink
        if callable(sink):
            try:
                sink(line)
                return
            except Exception:  # noqa: BLE001
                pass
        self._write_console_line(line)

    def _probe_worker_python(self, executable: Path | str) -> tuple[bool, str]:
        exe_text = str(executable or "").strip()
        if not exe_text:
            return False, "empty_python_executable"
        cached = self._worker_python_probe_cache.get(exe_text)
        if cached is not None:
            return cached
        try:
            result = subprocess.run(
                [exe_text, "-c", _WORKER_PYTHON_PROBE_CODE],
                cwd=str(self._worker_app_dir),
                env=self._build_clean_python_env(),
                text=True,
                capture_output=True,
                timeout=10,
                check=False,
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode == 0:
                payload = (True, "")
            else:
                payload = (
                    False,
                    " ".join(str(result.stderr or result.stdout or f"exit={result.returncode}").split()),
                )
        except Exception as exc:  # noqa: BLE001
            payload = (False, str(exc))
        self._worker_python_probe_cache[exe_text] = payload
        return payload

    def _resolve_worker_python_executable(self) -> str:
        runtime_root = self._worker_app_dir / "runtime" / "python"
        candidates = [
            runtime_root / "python.exe",
            runtime_root / "python",
            runtime_root / "bin" / "python",
            runtime_root / "bin" / "python3",
            Path(sys.executable),
        ]
        runtime_failures: List[str] = []
        for candidate in candidates:
            if not candidate.exists():
                continue
            ok, detail = self._probe_worker_python(candidate)
            if ok:
                candidate_text = str(candidate)
                if candidate_text == str(sys.executable) and runtime_failures:
                    signature = "|".join(runtime_failures)
                    if signature not in self._worker_python_fallback_logged:
                        self._worker_python_fallback_logged.add(signature)
                        self._emit_service_log("[依赖同步] 检测到 runtime/python 不完整，已切换到当前 Python 运行 worker")
                return candidate_text
            if runtime_root in candidate.parents or candidate == runtime_root:
                runtime_failures.append(f"{candidate}: {detail}")
        raise RuntimeError("未找到可用 Python 运行时")

    @staticmethod
    def _file_signature(path: Path) -> str:
        try:
            stat = path.stat()
        except OSError:
            return "missing"
        return f"{int(stat.st_mtime_ns)}:{int(stat.st_size)}"

    def _worker_runtime_ready_cache_key(self, python_executable: str) -> str:
        executable_text = str(python_executable or "").strip()
        worker_entry = self._worker_app_dir / "app" / "worker" / "entry.py"
        dependency_lock = self._worker_app_dir / "runtime_dependency_lock.json"
        return "|".join(
            [
                executable_text,
                self._file_signature(worker_entry),
                self._file_signature(dependency_lock),
            ]
        )

    def _invalidate_worker_runtime_ready_cache(self) -> None:
        self._worker_runtime_ready_cache.clear()

    def _build_worker_env(self) -> Dict[str, str]:
        env = self._build_clean_python_env()
        env["QJPT_WORKER_MODE"] = "1"
        env["QJPT_DISABLE_BROWSER_AUTO_OPEN"] = "1"
        env["QJPT_DISABLE_UPDATER_AUTOSTART"] = "1"
        env["QJPT_DISABLE_AUTO_OPEN_BROWSER"] = "1"
        return env

    def _build_worker_command(
        self,
        *,
        job_dir: Path,
        stage: StageState,
        worker_handler: str,
        payload_path: Path,
        control_port: int,
        python_executable: str | None = None,
    ) -> List[str]:
        bootstrap_script = self._worker_app_dir / "worker_bootstrap.py"
        return [
            str(python_executable or self._resolve_worker_python_executable()),
            str(bootstrap_script),
            "--job-dir",
            str(job_dir),
            "--stage-id",
            stage.stage_id,
            "--handler",
            str(worker_handler or "").strip(),
            "--payload-file",
            str(payload_path),
            "--control-port",
            str(control_port),
            "--heartbeat-interval",
            str(self._worker_heartbeat_interval_sec),
        ]

    @staticmethod
    def _is_worker_runtime_repairable_detail(detail: str) -> bool:
        lowered = str(detail or "").strip().lower()
        return bool(lowered and any(token in lowered for token in _WORKER_RUNTIME_REPAIR_TOKENS))

    def _mark_worker_dependency_state(self, job: JobState, stage: StageState, *, worker_status: str, summary: str) -> None:
        now_text = self._now_text()
        with self._lock:
            if job.status in {"success", "failed", "cancelled", "partial_failed"} or job.cancel_requested or stage.cancel_requested:
                return
            job.status = "waiting_resource"
            job.wait_reason = "waiting:dependency_sync"
            job.summary = summary
            job.wait_started_monotonic = time.monotonic()
            stage.status = "waiting_resource"
            stage.summary = summary
            stage.worker_status = worker_status
            self._persist_job_snapshot(job)
            self._record_job_event(
                job,
                stage_id=stage.stage_id,
                stream="job",
                event_type="dependency_sync",
                level="info",
                payload={"status": worker_status, "summary": summary, "timestamp": now_text},
            )
        self._persist_worker_snapshot(
            job,
            stage,
            {
                "pid": 0,
                "status": worker_status,
                "exit_code": 0,
                "last_heartbeat_at": stage.last_heartbeat_at,
                "updated_at": now_text,
            },
        )
        self._persist_resource_snapshot()

    def _sync_worker_dependencies(self, job: JobState, stage: StageState, *, python_executable: str, reason: str) -> Dict[str, Any]:
        summary = "正在自动补齐运行依赖"
        self._mark_worker_dependency_state(job, stage, worker_status="dependency_syncing", summary=summary)
        self._append_log(job, f"[依赖同步] {summary}: reason={reason or '-'}")
        runtime_paths = self._runtime_config.get("paths", {}) if isinstance(self._runtime_config, dict) else {}
        service = RuntimeDependencySyncService(
            app_dir=self._worker_app_dir,
            runtime_state_root=str(runtime_paths.get("runtime_state_root", "") or "") if isinstance(runtime_paths, dict) else "",
            emit_log=lambda text: self._append_log(job, text),
            python_executable=python_executable,
        )
        with self._worker_dependency_sync_lock:
            result = service.ensure_startup_dependencies(self._worker_app_dir / "runtime_dependency_lock.json")
        self._append_log(
            job,
            f"[依赖同步] 运行依赖自动补齐完成: checked={result.get('checked', 0)}, installed={result.get('installed', 0)}",
        )
        self._invalidate_worker_runtime_ready_cache()
        return result

    def _probe_worker_imports(self, python_executable: str) -> tuple[bool, str]:
        try:
            result = subprocess.run(
                [python_executable, "-c", _WORKER_IMPORT_PROBE_CODE],
                cwd=str(self._worker_app_dir),
                env=self._build_worker_env(),
                text=True,
                capture_output=True,
                timeout=20,
                check=False,
                encoding="utf-8",
                errors="replace",
            )
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
        if result.returncode == 0:
            return True, ""
        return False, " ".join(str(result.stderr or result.stdout or f"exit={result.returncode}").split())

    def _ensure_worker_runtime_ready(self, job: JobState, stage: StageState) -> str:
        python_executable = self._resolve_worker_python_executable()
        cache_key = self._worker_runtime_ready_cache_key(python_executable)
        if self._worker_runtime_ready_cache.get(cache_key) == python_executable:
            self._append_log(job, "[worker] 启动预检命中缓存: preflight_ms=0")
            return python_executable
        probe_started = time.perf_counter()
        ok, detail = self._probe_worker_imports(python_executable)
        probe_elapsed_ms = int((time.perf_counter() - probe_started) * 1000)
        if ok:
            self._worker_runtime_ready_cache[cache_key] = python_executable
            self._append_log(job, f"[worker] 启动预检完成: preflight_ms={probe_elapsed_ms}, cache=miss")
            return python_executable
        if not self._is_worker_runtime_repairable_detail(detail):
            raise RuntimeError(f"worker 启动预检失败: {detail}")
        self._mark_worker_dependency_state(
            job,
            stage,
            worker_status="dependency_repairing",
            summary="正在自动修复 worker 运行依赖",
        )
        self._sync_worker_dependencies(job, stage, python_executable=python_executable, reason=detail)
        self._invalidate_worker_runtime_ready_cache()
        retry_started = time.perf_counter()
        ok, detail_after = self._probe_worker_imports(python_executable)
        retry_elapsed_ms = int((time.perf_counter() - retry_started) * 1000)
        if ok:
            self._worker_runtime_ready_cache[self._worker_runtime_ready_cache_key(python_executable)] = python_executable
            self._append_log(job, f"[worker] 自动修复后预检完成: preflight_ms={retry_elapsed_ms}, cache=repair")
            return python_executable
        raise RuntimeError(f"worker 运行依赖自动修复后仍无法启动: {detail_after or detail}")

    def _persist_worker_snapshot(self, job: JobState, stage: StageState, payload: Dict[str, Any]) -> None:
        if not self._task_engine_db:
            return
        self._task_engine_db.upsert_worker(job_id=job.job_id, stage_id=stage.stage_id, snapshot=payload)

    @staticmethod
    def _allocate_worker_control_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            return int(probe.getsockname()[1] or 0)

    def _send_worker_command(self, *, job_id: str, stage_id: str, payload: Dict[str, Any]) -> bool:
        key = (str(job_id or "").strip(), str(stage_id or "").strip())
        with self._lock:
            port = int(self._worker_control_ports.get(key) or 0)
        if port <= 0:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.5) as client:
                client.sendall((json.dumps(self._json_ready(payload), ensure_ascii=False) + "\n").encode("utf-8"))
            return True
        except OSError:
            return False

    @staticmethod
    def _pid_exists(pid: int) -> bool:
        target_pid = int(pid or 0)
        if target_pid <= 0:
            return False
        try:
            if os.name == "nt":
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {target_pid}"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    check=False,
                )
                return str(target_pid) in str(result.stdout or "")
            os.kill(target_pid, 0)
            return True
        except Exception:  # noqa: BLE001
            return False

    @staticmethod
    def _terminate_orphan_worker(pid: int) -> bool:
        target_pid = int(pid or 0)
        if target_pid <= 0:
            return False
        try:
            if os.name == "nt":
                result = subprocess.run(
                    ["taskkill", "/PID", str(target_pid), "/T", "/F"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    check=False,
                )
                return int(result.returncode or 1) == 0
            os.kill(target_pid, signal.SIGKILL)
            return True
        except Exception:  # noqa: BLE001
            return False

    def _job_from_snapshot(self, snapshot: Dict[str, Any]) -> JobState:
        job = JobState(
            job_id=str(snapshot.get("job_id", "") or "").strip(),
            name=str(snapshot.get("name", "") or "").strip(),
            feature=str(snapshot.get("feature", "") or "").strip(),
            dedupe_key=str(snapshot.get("dedupe_key", "") or "").strip(),
            submitted_by=str(snapshot.get("submitted_by", "manual") or "manual").strip() or "manual",
            status=str(snapshot.get("status", "queued") or "queued").strip() or "queued",
            created_at=str(snapshot.get("created_at", "") or "").strip(),
            started_at=str(snapshot.get("started_at", "") or "").strip(),
            finished_at=str(snapshot.get("finished_at", "") or "").strip(),
            summary=str(snapshot.get("summary", "") or "").strip(),
            error=str(snapshot.get("error", "") or "").strip(),
            result=self._json_ready(snapshot.get("result")),
            priority=str(snapshot.get("priority", "manual") or "manual").strip() or "manual",
            resource_keys=self._normalize_resource_keys(snapshot.get("resource_keys") or []),
            wait_reason=str(snapshot.get("wait_reason", "") or "").strip(),
            bridge_task_id=str(snapshot.get("bridge_task_id", "") or "").strip(),
            cancel_requested=bool(snapshot.get("cancel_requested", False)),
            revision=int(snapshot.get("revision") or 0),
            last_event_id=int(snapshot.get("last_event_id") or 0),
        )
        stages: List[StageState] = []
        for item in list(snapshot.get("stages") or []):
            if not isinstance(item, dict):
                continue
            stages.append(
                StageState(
                    stage_id=str(item.get("stage_id", "") or "").strip() or "main",
                    name=str(item.get("name", "") or "").strip() or self._default_stage_name(job),
                    status=str(item.get("status", "pending") or "pending").strip() or "pending",
                    resource_keys=self._normalize_resource_keys(item.get("resource_keys") or job.resource_keys),
                    resume_policy=str(item.get("resume_policy", "manual_resume") or "manual_resume").strip() or "manual_resume",
                    worker_handler=str(item.get("worker_handler", "") or "").strip(),
                    worker_pid=int(item.get("worker_pid") or 0),
                    started_at=str(item.get("started_at", "") or "").strip(),
                    finished_at=str(item.get("finished_at", "") or "").strip(),
                    summary=str(item.get("summary", "") or "").strip(),
                    error=str(item.get("error", "") or "").strip(),
                    result=self._json_ready(item.get("result")),
                    cancel_requested=bool(item.get("cancel_requested", False)),
                    revision=int(item.get("revision") or 0),
                    worker_status=str(item.get("worker_status", "") or "").strip(),
                    last_heartbeat_at=str(item.get("last_heartbeat_at", "") or "").strip(),
                )
            )
        job.stages = stages or [self._get_primary_stage(job)]
        return job

    def _mark_restored_job_terminal(
        self,
        job: JobState,
        stage: StageState,
        *,
        status: str,
        summary: str,
        error: str = "",
        worker_status: str = "",
    ) -> None:
        now_text = self._now_text()
        job.status = status
        job.summary = summary
        job.error = error
        job.finished_at = now_text
        job.wait_reason = ""
        job.wait_started_monotonic = 0.0
        stage.status = status
        stage.summary = summary
        stage.error = error
        stage.finished_at = now_text
        stage.worker_pid = 0
        stage.worker_status = worker_status
        self._persist_worker_snapshot(
            job,
            stage,
            {
                "pid": 0,
                "status": worker_status or status,
                "exit_code": 0,
                "last_heartbeat_at": stage.last_heartbeat_at,
                "updated_at": now_text,
            },
        )
        self._persist_job_snapshot(job)
        self._record_job_event(
            job,
            stage_id=stage.stage_id,
            stream="job",
            event_type="recovery",
            level="warning" if status in {"interrupted", "failed"} else "info",
            payload={"action": status, "summary": summary, "timestamp": now_text},
        )
        job.done_event.set()

    def _prepare_job_for_restart(self, job: JobState, stage: StageState, *, summary: str) -> None:
        job.status = "queued"
        job.started_at = ""
        job.finished_at = ""
        job.summary = summary
        job.error = ""
        job.result = None
        job.wait_reason = ""
        job.cancel_requested = False
        job.wait_started_monotonic = 0.0
        stage.status = "pending"
        stage.started_at = ""
        stage.finished_at = ""
        stage.summary = summary
        stage.error = ""
        stage.result = None
        stage.worker_pid = 0
        stage.worker_status = ""
        stage.cancel_requested = False
        self._persist_worker_snapshot(
            job,
            stage,
            {
                "pid": 0,
                "status": "queued",
                "exit_code": 0,
                "last_heartbeat_at": "",
                "updated_at": self._now_text(),
            },
        )
        self._persist_job_snapshot(job)
        self._record_job_event(
            job,
            stage_id=stage.stage_id,
            stream="job",
            event_type="recovery",
            level="info",
            payload={"action": "requeue", "summary": summary, "timestamp": self._now_text()},
        )

    @staticmethod
    def _should_preserve_waiting_job_on_restore(job: JobState, stage: StageState) -> bool:
        previous_status = str(stage.status or job.status or "").strip().lower()
        wait_reason = str(job.wait_reason or "").strip().lower()
        if previous_status != "waiting_resource":
            return False
        return wait_reason == "waiting:shared_bridge"

    def _launch_existing_worker_job(self, job: JobState, stage: StageState, *, payload_path: Path, worker_handler: str) -> None:
        normalized_handler = str(worker_handler or "").strip()
        if not normalized_handler:
            raise ValueError("worker_handler is required")

        def _run() -> None:
            stage = self._get_primary_stage(job)
            repair_retry_used = False
            while True:
                process: subprocess.Popen[str] | None = None
                unhandled_error_detail = ""
                worker_result: Dict[str, Any] = {}
                worker_stderr_lines: List[str] = []
                control_port = 0
                retry_after_repair = False
                try:
                    python_executable = self._ensure_worker_runtime_ready(job, stage)
                    with self._lock:
                        if job.cancel_requested or stage.cancel_requested or job.status == "cancelled":
                            return
                    self._acquire_job_resources(job)
                    control_port = self._allocate_worker_control_port()
                    command = self._build_worker_command(
                        job_dir=self._task_engine_store.resolve_job_dir(job.job_id),
                        stage=stage,
                        worker_handler=normalized_handler,
                        payload_path=payload_path,
                        control_port=control_port,
                        python_executable=python_executable,
                    )
                    popen_kwargs: Dict[str, Any] = {
                        "cwd": str(self._worker_app_dir),
                        "env": self._build_worker_env(),
                        "stdout": subprocess.PIPE,
                        "stderr": subprocess.PIPE,
                        "text": True,
                        "encoding": "utf-8",
                        "errors": "replace",
                        "bufsize": 1,
                    }
                    if os.name == "nt":
                        popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
                    process = subprocess.Popen(command, **popen_kwargs)
                    with self._lock:
                        stage.worker_pid = int(process.pid or 0)
                        stage.worker_status = "running"
                        self._worker_processes[(job.job_id, stage.stage_id)] = process
                        self._worker_control_ports[(job.job_id, stage.stage_id)] = control_port
                        self._persist_job_snapshot(job)
                    self._persist_worker_snapshot(
                        job,
                        stage,
                        {
                            "pid": int(process.pid or 0),
                            "status": "running",
                            "command": command,
                            "started_at": self._now_text(),
                            "updated_at": self._now_text(),
                        },
                    )

                    def _consume_stdout() -> None:
                        if process is None or process.stdout is None:
                            return
                        for raw_line in process.stdout:
                            parsed = self._parse_worker_event(raw_line)
                            if parsed:
                                self._handle_worker_event(job, stage, parsed, worker_result)
                            else:
                                self._record_worker_text_line(job, stage, raw_line, stream="stdout")

                    def _consume_stderr() -> None:
                        if process is None or process.stderr is None:
                            return
                        for raw_line in process.stderr:
                            worker_stderr_lines.append(str(raw_line or "").strip())
                            self._record_worker_text_line(job, stage, raw_line, stream="stderr")

                    stdout_thread = threading.Thread(target=_consume_stdout, daemon=True, name=f"worker-stdout-{job.job_id[:8]}")
                    stderr_thread = threading.Thread(target=_consume_stderr, daemon=True, name=f"worker-stderr-{job.job_id[:8]}")
                    stdout_thread.start()
                    stderr_thread.start()
                    return_code = int(process.wait())
                    stdout_thread.join(timeout=2)
                    stderr_thread.join(timeout=2)
                    stderr_detail = " ".join(line for line in worker_stderr_lines if line).strip()
                    repairable_crash = (
                        (return_code != 0 or not worker_result)
                        and not repair_retry_used
                        and self._is_worker_runtime_repairable_detail(stderr_detail)
                    )
                    if repairable_crash:
                        repair_retry_used = True
                        retry_after_repair = True
                        unhandled_error_detail = stderr_detail
                        self._mark_worker_dependency_state(
                            job,
                            stage,
                            worker_status="dependency_repairing",
                            summary="worker 运行依赖异常，正在自动修复后重试",
                        )
                        self._sync_worker_dependencies(
                            job,
                            stage,
                            python_executable=python_executable,
                            reason=stderr_detail,
                        )
                    else:
                        with self._lock:
                            stage.worker_pid = 0
                            self._worker_processes.pop((job.job_id, stage.stage_id), None)
                            self._worker_control_ports.pop((job.job_id, stage.stage_id), None)
                            force_killed = (job.job_id, stage.stage_id) in self._worker_force_killed
                            if force_killed:
                                self._worker_force_killed.discard((job.job_id, stage.stage_id))
                            stage.worker_status = (
                                "cancelled"
                                if (bool(worker_result.get("cancelled", False)) or force_killed)
                                else ("success" if return_code == 0 else "failed")
                            )
                            if bool(worker_result.get("cancelled", False)) or force_killed:
                                summary = "interrupted_force_killed" if force_killed else "cancelled"
                                job.status = "cancelled"
                                job.summary = summary
                                job.finished_at = self._now_text()
                                stage.status = "cancelled"
                                stage.summary = summary
                                stage.finished_at = job.finished_at
                            elif return_code != 0 or not worker_result:
                                detail = (
                                    str(worker_result.get("error", "") or "").strip()
                                    or str(worker_result.get("message", "") or "").strip()
                                    or stderr_detail
                                    or f"worker exited with code {return_code}"
                                )
                                unhandled_error_detail = detail
                                job.status = "failed"
                                job.error = detail
                                job.summary = detail
                                job.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                stage.status = "failed"
                                stage.error = detail
                                stage.summary = detail
                                stage.finished_at = job.finished_at
                            else:
                                result_payload = worker_result.get("payload", worker_result.get("result"))
                                job.status = "success"
                                job.summary = "ok"
                                job.result = result_payload
                                job.finished_at = self._now_text()
                                stage.status = "success"
                                stage.summary = "ok"
                                stage.result = result_payload
                                stage.finished_at = job.finished_at
                            self._persist_job_snapshot(job)
                        self._persist_worker_snapshot(
                            job,
                            stage,
                            {
                                "pid": 0,
                                "status": stage.worker_status or ("success" if (return_code == 0 and worker_result) else "failed"),
                                "exit_code": return_code,
                                "last_heartbeat_at": stage.last_heartbeat_at,
                                "updated_at": self._now_text(),
                            },
                        )
                except Exception as exc:  # noqa: BLE001
                    detail = str(exc)
                    unhandled_error_detail = detail
                    if self._is_worker_runtime_repairable_detail(detail) and not repair_retry_used:
                        repair_retry_used = True
                        try:
                            python_executable = self._resolve_worker_python_executable()
                            self._mark_worker_dependency_state(
                                job,
                                stage,
                                worker_status="dependency_repairing",
                                summary="worker 启动异常，正在自动修复后重试",
                            )
                            self._sync_worker_dependencies(
                                job,
                                stage,
                                python_executable=python_executable,
                                reason=detail,
                            )
                            retry_after_repair = True
                        except Exception as repair_exc:  # noqa: BLE001
                            detail = f"{detail}; 自动修复失败: {repair_exc}"
                            unhandled_error_detail = detail
                    if not retry_after_repair:
                        with self._lock:
                            stage.worker_pid = 0
                            stage.worker_status = "failed"
                            job.status = "failed"
                            job.error = detail
                            job.summary = detail
                            job.finished_at = self._now_text()
                            stage.status = "failed"
                            stage.error = detail
                            stage.summary = detail
                            stage.finished_at = job.finished_at
                            self._persist_job_snapshot(job)
                        self._persist_worker_snapshot(
                            job,
                            stage,
                            {
                                "pid": 0,
                                "status": "failed",
                                "exit_code": int(getattr(process, "returncode", 0) or 0),
                                "last_heartbeat_at": stage.last_heartbeat_at,
                                "updated_at": self._now_text(),
                            },
                        )
                finally:
                    with self._lock:
                        self._worker_processes.pop((job.job_id, stage.stage_id), None)
                        self._worker_control_ports.pop((job.job_id, stage.stage_id), None)
                    with self._lock:
                        if job.status == "failed":
                            has_failure_line = any("[文件流程失败]" in line for line in job.logs)
                            if not has_failure_line:
                                detail = unhandled_error_detail or job.error or "未提供错误详情"
                                detail = " ".join(str(detail).split())
                                self._append_log(
                                    job,
                                    f"[文件流程失败] 功能=任务执行 阶段=worker 楼栋=- 文件=- 日期=- 错误={detail}",
                                )
                        self._persist_job_snapshot(job)
                    self._release_resources(job.job_id, list(job.acquired_resources))
                    job.acquired_resources = []
                    if not retry_after_repair:
                        job.done_event.set()
                if retry_after_repair:
                    continue
                break

        thread = threading.Thread(target=_run, daemon=True, name=f"worker-job-{job.job_id[:8]}")
        job.thread = thread
        thread.start()

    def _job_network_side(self, job: JobState) -> str:
        for resource_key in job.resource_keys:
            side = self._network_side_from_resource_key(resource_key)
            if side:
                return side
        return ""

    def _get_current_network_state(self) -> Dict[str, Any]:
        now_monotonic = time.monotonic()
        with self._network_status_lock:
            if (
                self._network_status_checked_monotonic > 0
                and (now_monotonic - self._network_status_checked_monotonic) < self._network_status_cache_ttl_sec
            ):
                return dict(self._network_status_snapshot)
        current_ssid = ""
        getter = self._current_ssid_getter
        if callable(getter):
            try:
                current_ssid = self._normalize_ssid(getter())
            except Exception:  # noqa: BLE001
                current_ssid = ""
        ssid_side = "none"
        if self._ssid_matches(current_ssid, self._network_internal_ssid):
            ssid_side = "internal"
        elif self._ssid_matches(current_ssid, self._network_external_ssid):
            ssid_side = "external"
        snapshot = {
            "current_ssid": current_ssid,
            "ssid_side": ssid_side,
            "internal_reachable": True,
            "external_reachable": True,
            "reachable_sides": ["internal", "external"],
            "mode": "switching_ready",
            "last_checked_at": self._now_text(),
        }
        with self._network_status_lock:
            self._network_status_checked_monotonic = now_monotonic
            self._network_status_snapshot = dict(snapshot)
        return snapshot

    def _network_target_ssid(self, side: str) -> str:
        if side == "internal":
            return self._network_internal_ssid
        if side == "external":
            return self._network_external_ssid
        return ""

    def _network_side_unreachable(self, side: str) -> bool:
        requested_side = str(side or "").strip().lower()
        if self._network_auto_switch_enabled or requested_side not in {"internal", "external"}:
            return False
        current_state = self._get_current_network_state()
        if requested_side == "internal":
            return not bool(current_state.get("internal_reachable", False))
        if requested_side == "external":
            return not bool(current_state.get("external_reachable", False))
        return False

    def _wait_reason_for_job(self, job: JobState, unavailable: List[str]) -> str:
        tokens: List[str] = []
        requested_side = self._job_network_side(job)
        if self._network_side_unreachable(requested_side):
            if requested_side == "internal":
                tokens.append("waiting:network_internal_unreachable")
            elif requested_side == "external":
                tokens.append("waiting:network_external_unreachable")
        raw = self._format_wait_reason(unavailable)
        for item in raw.split(","):
            token = str(item or "").strip()
            if not token:
                continue
            if tokens and token in {"waiting:network_internal", "waiting:network_external"}:
                continue
            if token not in tokens:
                tokens.append(token)
        return ", ".join(tokens)

    def _oldest_wait_sec_locked(self, side: str) -> int:
        now_monotonic = time.monotonic()
        waits = [
            max(0, int(now_monotonic - float(job.wait_started_monotonic or 0)))
            for job in self._ordered_jobs()
            if job.status in {"queued", "waiting_resource"}
            and self._job_network_side(job) == side
            and float(job.wait_started_monotonic or 0) > 0
        ]
        if not waits:
            return 0
        return max(waits)

    def _choose_next_network_side_locked(self) -> str:
        waiting_jobs = [
            job
            for job in self._ordered_jobs()
            if job.status in {"queued", "waiting_resource"}
            and self._job_network_side(job) in {"internal", "external"}
        ]
        if not waiting_jobs:
            return "none"
        internal_jobs = [job for job in waiting_jobs if self._job_network_side(job) == "internal"]
        external_jobs = [job for job in waiting_jobs if self._job_network_side(job) == "external"]
        if internal_jobs and not external_jobs:
            return "internal"
        if external_jobs and not internal_jobs:
            return "external"
        internal_key = min((self._priority_weight(job.priority), job.sequence) for job in internal_jobs)
        external_key = min((self._priority_weight(job.priority), job.sequence) for job in external_jobs)
        if internal_key < external_key:
            return "internal"
        if external_key < internal_key:
            return "external"
        return "internal"

    def _update_network_window_locked(self) -> None:
        now_monotonic = time.monotonic()
        running_internal = len(self._resource_holders.get("network:internal", []))
        running_external = len(self._resource_holders.get("network:external", []))
        running_pipeline = len(self._resource_holders.get("network:pipeline", []))
        queued_internal = sum(
            1
            for job in self._ordered_jobs()
            if job.status in {"queued", "waiting_resource"} and self._job_network_side(job) == "internal"
        )
        queued_external = sum(
            1
            for job in self._ordered_jobs()
            if job.status in {"queued", "waiting_resource"} and self._job_network_side(job) == "external"
        )
        if running_pipeline:
            self._network_window_current_side = "pipeline"
            if not self._network_window_started_at:
                self._network_window_started_at = self._now_text()
                self._network_window_started_monotonic = now_monotonic
            return
        if self._network_window_current_side == "pipeline" and not running_pipeline:
            self._network_window_current_side = "none"
            self._network_window_started_at = ""
            self._network_window_started_monotonic = 0.0
            self._network_window_dispatch_count = 0
            self._network_window_draining = False
            self._network_window_pending_side = ""
        if not self._network_auto_switch_enabled:
            if running_internal and not running_external:
                current_side = "internal"
            elif running_external and not running_internal:
                current_side = "external"
            elif queued_internal or queued_external:
                chosen_side = self._choose_next_network_side_locked()
                current_side = chosen_side if chosen_side in {"internal", "external"} else "none"
            else:
                current_side = "none"
            if current_side == "none":
                self._network_window_current_side = "none"
                self._network_window_started_at = ""
                self._network_window_started_monotonic = 0.0
                self._network_window_dispatch_count = 0
                self._network_window_draining = False
                self._network_window_pending_side = ""
                return
            if self._network_window_current_side != current_side or not self._network_window_started_at:
                self._network_window_current_side = current_side
                self._network_window_started_at = self._now_text()
                self._network_window_started_monotonic = now_monotonic
                self._network_window_dispatch_count = 0
            self._network_window_draining = False
            self._network_window_pending_side = ""
            return
        if self._network_window_current_side == "none":
            next_side = self._choose_next_network_side_locked()
            if next_side in {"internal", "external"}:
                self._network_window_current_side = next_side
                self._network_window_started_at = self._now_text()
                self._network_window_started_monotonic = now_monotonic
                self._network_window_dispatch_count = 0
                self._network_window_draining = False
                self._network_window_pending_side = ""
            return
        if self._network_window_current_side not in {"internal", "external"}:
            return
        current_side = self._network_window_current_side
        opposite_side = "external" if current_side == "internal" else "internal"
        current_running = running_internal if current_side == "internal" else running_external
        current_queued = queued_internal if current_side == "internal" else queued_external
        opposite_queued = queued_external if current_side == "internal" else queued_internal
        elapsed_sec = 0
        if self._network_window_started_monotonic > 0:
            elapsed_sec = int(max(0, now_monotonic - self._network_window_started_monotonic))
        opposite_wait_sec = self._oldest_wait_sec_locked(opposite_side)
        if (
            not self._network_window_draining
            and opposite_queued > 0
            and (
                elapsed_sec >= self._network_window_max_duration_sec
                or self._network_window_dispatch_count >= self._network_window_max_dispatches
                or opposite_wait_sec >= self._network_window_max_opposite_wait_sec
            )
        ):
            self._network_window_draining = True
            self._network_window_pending_side = opposite_side
        if self._network_window_draining and current_running == 0:
            if opposite_queued > 0:
                self._network_window_current_side = opposite_side
                self._network_window_started_at = self._now_text()
                self._network_window_started_monotonic = now_monotonic
                self._network_window_dispatch_count = 0
                self._network_window_draining = False
                self._network_window_pending_side = ""
                return
            self._network_window_draining = False
            self._network_window_pending_side = ""
            if current_queued == 0:
                self._network_window_current_side = "none"
                self._network_window_started_at = ""
                self._network_window_started_monotonic = 0.0
                self._network_window_dispatch_count = 0
                return
            self._network_window_started_at = self._now_text()
            self._network_window_started_monotonic = now_monotonic
            self._network_window_dispatch_count = 0
        if current_running == 0 and current_queued == 0 and opposite_queued == 0:
            self._network_window_current_side = "none"
            self._network_window_started_at = ""
            self._network_window_started_monotonic = 0.0
            self._network_window_dispatch_count = 0
            self._network_window_draining = False
            self._network_window_pending_side = ""

    def _network_resource_unavailable(self, job: JobState, resource_key: str, ordered_jobs: List[JobState]) -> bool:
        requested_side = self._network_side_from_resource_key(resource_key)
        if not requested_side:
            return False
        if requested_side == "pipeline":
            return any(
                owner != job.job_id
                for key, owners in self._resource_holders.items()
                if self._network_side_from_resource_key(key)
                for owner in owners
            )
        if self._network_side_unreachable(requested_side):
            return True
        self._update_network_window_locked()
        if not self._network_auto_switch_enabled:
            current_state = self._get_current_network_state()
            mode = str(current_state.get("mode", "") or "").strip().lower()
            if mode == "internal_only" and requested_side != "internal":
                return True
            if mode == "external_only" and requested_side != "external":
                return True
            if mode == "switching_ready":
                current_side = self._network_window_current_side
                if current_side in {"internal", "external"} and requested_side != current_side:
                    return True
            if mode == "none_reachable":
                return True
            holders = [owner for owner in self._resource_holders.get(resource_key, []) if owner != job.job_id]
            capacity = self._resource_capacity(resource_key)
            available_slots = max(0, capacity - len(holders))
            earlier_waiters = 0
            for candidate in ordered_jobs:
                if candidate.job_id == job.job_id:
                    break
                if resource_key not in candidate.resource_keys:
                    continue
                if not self._blocks_resource_queue(candidate):
                    continue
                if self._priority_weight(candidate.priority) > self._priority_weight(job.priority):
                    continue
                earlier_waiters += 1
            return len(holders) >= capacity or earlier_waiters >= available_slots
        current_side = self._network_window_current_side
        if current_side == "pipeline":
            return True
        if current_side == "none":
            chosen = self._choose_next_network_side_locked()
            if chosen not in {"none", requested_side}:
                return True
        elif current_side != requested_side:
            return True
        if self._network_window_draining:
            return True
        holders = [owner for owner in self._resource_holders.get(resource_key, []) if owner != job.job_id]
        capacity = self._resource_capacity(resource_key)
        available_slots = max(0, capacity - len(holders))
        earlier_waiters = 0
        for candidate in ordered_jobs:
            if candidate.job_id == job.job_id:
                break
            if resource_key not in candidate.resource_keys:
                continue
            if not self._blocks_resource_queue(candidate):
                continue
            if self._priority_weight(candidate.priority) > self._priority_weight(job.priority):
                continue
            earlier_waiters += 1
        return len(holders) >= capacity or earlier_waiters >= available_slots

    def _resource_unavailable_keys(self, job: JobState) -> List[str]:
        unavailable: List[str] = []
        ordered_jobs = self._ordered_jobs()
        updater_holders = [owner for owner in self._resource_holders.get("updater:global", []) if owner != job.job_id]
        if "updater:global" in job.resource_keys:
            other_busy = any(
                owner != job.job_id
                for key, owners in self._resource_holders.items()
                if key != "updater:global"
                for owner in owners
            )
            if updater_holders or other_busy:
                unavailable.append("updater:global")
        elif updater_holders:
            unavailable.append("updater:global")
        for resource_key in job.resource_keys:
            if self._network_side_from_resource_key(resource_key):
                if self._network_resource_unavailable(job, resource_key, ordered_jobs):
                    unavailable.append(resource_key)
                continue
            holders = [owner for owner in self._resource_holders.get(resource_key, []) if owner != job.job_id]
            capacity = self._resource_capacity(resource_key)
            available_slots = max(0, capacity - len(holders))
            earlier_waiters = 0
            for candidate in ordered_jobs:
                if candidate.job_id == job.job_id:
                    break
                if resource_key not in candidate.resource_keys:
                    continue
                if not self._blocks_resource_queue(candidate):
                    continue
                candidate_priority = self._priority_weight(candidate.priority)
                current_priority = self._priority_weight(job.priority)
                if candidate_priority > current_priority:
                    continue
                earlier_waiters += 1
            if len(holders) >= capacity or earlier_waiters >= available_slots:
                unavailable.append(resource_key)
        return unavailable

    def _acquire_job_resources(self, job: JobState) -> None:
        stage = self._get_primary_stage(job)
        if not job.resource_keys:
            with self._lock:
                job.status = "running"
                job.started_at = self._now_text()
                stage.status = "running"
                stage.started_at = job.started_at
                job.wait_started_monotonic = 0.0
                self._persist_job_snapshot(job)
                self._persist_resource_snapshot()
            return

        waiting_logged = False
        wait_started_at = datetime.now()
        while True:
            with self._condition:
                unavailable = self._resource_unavailable_keys(job)
                if not unavailable:
                    for resource_key in job.resource_keys:
                        holders = self._resource_holders.setdefault(resource_key, [])
                        if job.job_id not in holders:
                            holders.append(job.job_id)
                    self._update_network_window_locked()
                    if self._network_auto_switch_enabled and self._job_network_side(job) == "pipeline":
                        self._network_window_current_side = "pipeline"
                        self._network_window_started_at = self._now_text()
                        self._network_window_started_monotonic = time.monotonic()
                        self._network_window_dispatch_count = 1
                        self._network_window_draining = False
                        self._network_window_pending_side = ""
                    elif self._job_network_side(job) in {"internal", "external"}:
                        if self._network_window_current_side == "none":
                            self._network_window_current_side = self._job_network_side(job)
                            self._network_window_started_at = self._now_text()
                            self._network_window_started_monotonic = time.monotonic()
                            self._network_window_dispatch_count = 0
                            self._network_window_draining = False
                            self._network_window_pending_side = ""
                        self._network_window_dispatch_count += 1
                    job.acquired_resources = list(job.resource_keys)
                    job.wait_reason = ""
                    job.status = "running"
                    job.started_at = self._now_text()
                    job.wait_started_monotonic = 0.0
                    stage.status = "running"
                    stage.started_at = job.started_at
                    if waiting_logged:
                        waited_ms = int((datetime.now() - wait_started_at).total_seconds() * 1000)
                        self._append_log(
                            job,
                            f"[任务资源] 已获取资源: {','.join(job.resource_keys)} (waited_ms={waited_ms})",
                        )
                    self._persist_job_snapshot(job)
                    self._persist_resource_snapshot()
                    return
                job.status = "waiting_resource"
                if job.wait_started_monotonic <= 0:
                    job.wait_started_monotonic = time.monotonic()
                job.wait_reason = self._wait_reason_for_job(job, unavailable)
                stage.status = "waiting_resource"
                stage.summary = job.wait_reason
                self._persist_job_snapshot(job)
                if not waiting_logged:
                    self._append_log(job, f"[任务资源] 等待资源: {job.wait_reason}")
                    waiting_logged = True
                self._condition.wait(timeout=0.5)

    def _release_resources(self, owner_id: str, resource_keys: List[str]) -> None:
        if not resource_keys:
            return
        with self._condition:
            for resource_key in resource_keys:
                holders = list(self._resource_holders.get(resource_key, []))
                if owner_id in holders:
                    holders = [item for item in holders if item != owner_id]
                    if holders:
                        self._resource_holders[resource_key] = holders
                    else:
                        self._resource_holders.pop(resource_key, None)
            self._update_network_window_locked()
            self._condition.notify_all()
        self._persist_resource_snapshot()

    @contextlib.contextmanager
    def resource_guard(
        self,
        *,
        name: str,
        resource_keys: List[str] | tuple[str, ...] | None = None,
        timeout_sec: float | None = None,
    ):
        owner_id = f"sync:{uuid.uuid4().hex}"
        normalized_resources = self._normalize_resource_keys(resource_keys)
        acquired_resources: List[str] = []
        wait_started = time.monotonic()
        try:
            while normalized_resources:
                with self._condition:
                    unavailable = []
                    updater_holders = [item for item in self._resource_holders.get("updater:global", []) if item != owner_id]
                    if "updater:global" in normalized_resources:
                        other_busy = any(
                            owner != owner_id
                            for key, owners in self._resource_holders.items()
                            if key != "updater:global"
                            for owner in owners
                        )
                        if updater_holders or other_busy:
                            unavailable.append("updater:global")
                    elif updater_holders:
                        unavailable.append("updater:global")
                    for resource_key in normalized_resources:
                        side = self._network_side_from_resource_key(resource_key)
                        if side in {"internal", "external"}:
                            if self._network_side_unreachable(side):
                                unavailable.append(resource_key)
                                continue
                            if not self._network_auto_switch_enabled:
                                current_state = self._get_current_network_state()
                                mode = str(current_state.get("mode", "") or "").strip().lower()
                                if mode == "internal_only" and side != "internal":
                                    unavailable.append(resource_key)
                                    continue
                                if mode == "external_only" and side != "external":
                                    unavailable.append(resource_key)
                                    continue
                                if mode == "none_reachable":
                                    unavailable.append(resource_key)
                                    continue
                            self._update_network_window_locked()
                            if self._network_auto_switch_enabled and (
                                self._network_window_current_side in {"internal", "external", "pipeline"}
                                and self._network_window_current_side != side
                            ) or self._network_window_draining:
                                unavailable.append(resource_key)
                                continue
                        holders = [owner for owner in self._resource_holders.get(resource_key, []) if owner != owner_id]
                        if len(holders) >= self._resource_capacity(resource_key):
                            unavailable.append(resource_key)
                    if not unavailable:
                        for resource_key in normalized_resources:
                            holders = self._resource_holders.setdefault(resource_key, [])
                            if owner_id not in holders:
                                holders.append(owner_id)
                        self._update_network_window_locked()
                        if self._network_auto_switch_enabled and any(self._network_side_from_resource_key(resource_key) == "pipeline" for resource_key in normalized_resources):
                            self._network_window_current_side = "pipeline"
                            self._network_window_started_at = self._now_text()
                            self._network_window_started_monotonic = time.monotonic()
                            self._network_window_dispatch_count = 1
                            self._network_window_draining = False
                            self._network_window_pending_side = ""
                        elif self._network_auto_switch_enabled:
                            for resource_key in normalized_resources:
                                side = self._network_side_from_resource_key(resource_key)
                                if side in {"internal", "external"}:
                                    if self._network_window_current_side == "none":
                                        self._network_window_current_side = side
                                        self._network_window_started_at = self._now_text()
                                        self._network_window_started_monotonic = time.monotonic()
                                        self._network_window_dispatch_count = 0
                                        self._network_window_draining = False
                                        self._network_window_pending_side = ""
                                    self._network_window_dispatch_count += 1
                        acquired_resources = list(normalized_resources)
                        break
                    if timeout_sec is not None and (time.monotonic() - wait_started) >= float(timeout_sec):
                        detail = ", ".join(unavailable)
                        raise TimeoutError(f"{name} 等待资源超时: {detail}")
                    self._condition.wait(timeout=0.5)
            yield
        finally:
            self._release_resources(owner_id, acquired_resources)

    @staticmethod
    def _parse_worker_event(raw_line: str) -> Dict[str, Any] | None:
        text = str(raw_line or "").strip()
        if not text:
            return None
        try:
            payload = json.loads(text)
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(payload, dict):
            return None
        if not str(payload.get("type", "") or "").strip():
            return None
        return payload

    def _record_worker_text_line(self, job: JobState, stage: StageState, raw_line: str, *, stream: str) -> None:
        text = str(raw_line or "").strip()
        if not text:
            return
        timestamp = self._now_text()
        level = "error" if stream == "stderr" else "info"
        event_type = "raw_stderr_log" if stream == "stderr" else "raw_stdout_log"
        line = f"[{timestamp}] {text}"
        with self._lock:
            job.logs.append(line)
            overflow = len(job.logs) - self.log_buffer_size
            if overflow > 0:
                del job.logs[:overflow]
            self._record_job_event_async(
                job,
                stage_id=stage.stage_id,
                stream=stream,
                event_type=event_type,
                level=level,
                payload={"line": line, "message": text, "timestamp": timestamp},
            )
        self._write_console_line(text)

    def _handle_worker_event(
        self,
        job: JobState,
        stage: StageState,
        event: Dict[str, Any],
        result_box: Dict[str, Any],
    ) -> None:
        event_type = str(event.get("type", "") or "").strip().lower()
        stage_id = str(event.get("stage_id", "") or stage.stage_id).strip() or stage.stage_id
        timestamp = str(event.get("ts", "") or "").strip() or self._now_text()
        if event_type == "log":
            message = str(event.get("message", "") or "").strip()
            if not message:
                return
            line = f"[{timestamp}] {message}"
            level = str(event.get("level", "info") or "info").strip() or "info"
            with self._lock:
                job.logs.append(line)
                overflow = len(job.logs) - self.log_buffer_size
                if overflow > 0:
                    del job.logs[:overflow]
                self._record_job_event_async(
                    job,
                    stage_id=stage_id,
                    stream="stdout",
                    event_type="log",
                    level=level,
                    payload={"line": line, "message": message, "timestamp": timestamp},
                )
            self._write_console_line(message)
            return
        if event_type == "stage_status":
            status = str(event.get("status", "") or "").strip() or stage.status
            summary = str(event.get("summary", "") or "").strip()
            metadata = self._json_ready(event.get("metadata") or {})
            with self._lock:
                stage.status = status
                if summary:
                    stage.summary = summary
                if status == "running" and not stage.started_at:
                    stage.started_at = timestamp
                if status in {"success", "failed", "cancelled", "blocked"}:
                    stage.finished_at = timestamp
                self._record_job_event(
                    job,
                    stage_id=stage_id,
                    stream="stdout",
                    event_type="stage_status",
                    level="info",
                    payload={"status": status, "summary": summary, "metadata": metadata, "timestamp": timestamp},
                )
                self._persist_job_snapshot(job)
            return
        if event_type == "progress":
            payload = {
                "progress": int(event.get("progress") or 0),
                "message": str(event.get("message", "") or "").strip(),
                "timestamp": timestamp,
            }
            with self._lock:
                self._record_job_event_async(
                    job,
                    stage_id=stage_id,
                    stream="stdout",
                    event_type="progress",
                    level="info",
                    payload=payload,
                )
            return
        if event_type == "heartbeat":
            heartbeat_at = timestamp
            with self._lock:
                stage.last_heartbeat_at = heartbeat_at
                stage.worker_status = "running"
            return
        if event_type == "result":
            result_box.clear()
            result_box.update(self._json_ready(event))
            with self._lock:
                self._record_job_event(
                    job,
                    stage_id=stage_id,
                    stream="stdout",
                    event_type="result",
                    level="info" if bool(event.get("ok", False)) else "error",
                    payload=self._json_ready(event),
                )
                self._persist_job_snapshot(job)
            return
        with self._lock:
            self._record_job_event(
                job,
                stage_id=stage_id,
                stream="stdout",
                event_type=event_type or "worker_event",
                level="info",
                payload=self._json_ready(event),
            )
            self._persist_job_snapshot(job)

    def start_job(
        self,
        name: str,
        run_func: Callable[[Callable[[str], None]], Any],
        *,
        resource_keys: List[str] | tuple[str, ...] | None = None,
        priority: str = "manual",
        feature: str = "",
        dedupe_key: str = "",
        submitted_by: str = "",
    ) -> JobState:
        submit_started = time.perf_counter()
        normalized_resources = self._normalize_resource_keys(resource_keys)
        normalized_priority = str(priority or "manual").strip().lower() or "manual"
        normalized_submitted_by = str(submitted_by or normalized_priority).strip().lower() or "manual"
        normalized_dedupe_key = self._normalize_dedupe_key(dedupe_key)
        if normalized_dedupe_key and self._task_engine_db:
            dedupe_started = time.perf_counter()
            snapshot = self._task_engine_db.find_active_job_by_dedupe_key(
                normalized_dedupe_key,
                statuses=sorted(_INCOMPLETE_JOB_STATUSES),
            )
            dedupe_elapsed_ms = int((time.perf_counter() - dedupe_started) * 1000)
            if dedupe_elapsed_ms >= 1000:
                self._write_console_line(
                    f"[任务热路径] dedupe DB 查询耗时: feature={feature or name}, elapsed_ms={dedupe_elapsed_ms}"
                )
            if isinstance(snapshot, dict):
                return self._job_from_snapshot(snapshot)
        with self._lock:
            existing = self._find_active_job_by_dedupe_key_locked(normalized_dedupe_key)
            if existing is not None:
                return existing
            job_id = uuid.uuid4().hex
            job = JobState(
                job_id=job_id,
                name=name,
                feature=str(feature or name or "").strip(),
                dedupe_key=normalized_dedupe_key,
                submitted_by=normalized_submitted_by,
                priority=normalized_priority,
                resource_keys=normalized_resources,
                sequence=self._next_sequence(),
            )
            job.stages = [
                StageState(
                    stage_id="main",
                    name=str(feature or name or "").strip() or str(name or "stage").strip() or "stage",
                    resource_keys=list(normalized_resources),
                    resume_policy="manual_resume",
                )
            ]
            self._jobs[job_id] = job
            persist_started = time.perf_counter()
            self._persist_job_snapshot(job, config_snapshot=None)
            persist_elapsed_ms = int((time.perf_counter() - persist_started) * 1000)
            if persist_elapsed_ms >= 1000:
                self._write_console_line(
                    f"[任务热路径] job 主记录写入耗时: job={job_id}, feature={feature or name}, elapsed_ms={persist_elapsed_ms}"
                )
        self._persist_resource_snapshot()

        def _persist_config_snapshot_async() -> None:
            try:
                snapshot_started = time.perf_counter()
                config_snapshot = self._capture_config_snapshot()
                if isinstance(config_snapshot, dict):
                    self._persist_job_snapshot(job, config_snapshot=config_snapshot)
                snapshot_elapsed_ms = int((time.perf_counter() - snapshot_started) * 1000)
                if snapshot_elapsed_ms >= 1000:
                    self._write_console_line(
                        f"[任务热路径] config snapshot 异步写入耗时: job={job_id}, feature={feature or name}, elapsed_ms={snapshot_elapsed_ms}"
                    )
            except Exception as exc:  # noqa: BLE001
                self._write_console_line(
                    f"[任务热路径] config snapshot 异步写入失败: job={job_id}, feature={feature or name}, error={exc}"
                )

        def _run() -> None:
            unhandled_error_detail = ""
            emitter = _LineEmitter(lambda line: self.log(job.job_id, line))
            stream = _StreamProxy(emitter)
            stage = self._get_primary_stage(job)
            try:
                self._acquire_job_resources(job)
                with contextlib.redirect_stdout(stream), contextlib.redirect_stderr(stream):
                    result = run_func(lambda line: self.log(job.job_id, line))
                with self._lock:
                    job.status = "success"
                    job.summary = "ok"
                    job.result = result
                    job.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    stage.status = "success"
                    stage.summary = "ok"
                    stage.result = result
                    stage.finished_at = job.finished_at
                    self._persist_job_snapshot(job)
            except Exception as exc:  # noqa: BLE001
                detail = str(exc)
                unhandled_error_detail = detail
                with self._lock:
                    job.status = "failed"
                    job.error = detail
                    job.summary = detail
                    job.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    stage.status = "failed"
                    stage.error = detail
                    stage.summary = detail
                    stage.finished_at = job.finished_at
                    self._persist_job_snapshot(job)
            finally:
                emitter.flush()
                with self._lock:
                    if job.status == "failed":
                        has_failure_line = any("[文件流程失败]" in line for line in job.logs)
                        if not has_failure_line:
                            detail = unhandled_error_detail or job.error or "未提供错误详情"
                            detail = " ".join(str(detail).split())
                            self._append_log(
                                job,
                                f"[文件流程失败] 功能=任务执行 阶段=未分类 楼栋=- 文件=- 日期=- 错误={detail}",
                            )
                    self._persist_job_snapshot(job)
                self._release_resources(job.job_id, list(job.acquired_resources))
                job.acquired_resources = []
                job.done_event.set()

        thread = threading.Thread(target=_run, daemon=True, name=f"job-{job_id[:8]}")
        job.thread = thread
        thread.start()
        threading.Thread(
            target=_persist_config_snapshot_async,
            daemon=True,
            name=f"job-config-snapshot-{job_id[:8]}",
        ).start()
        submit_elapsed_ms = int((time.perf_counter() - submit_started) * 1000)
        if submit_elapsed_ms >= 1000:
            self._write_console_line(
                f"[任务热路径] start_job 提交耗时: job={job_id}, feature={feature or name}, elapsed_ms={submit_elapsed_ms}"
            )
        return job

    def _restore_incomplete_jobs(self) -> None:
        if not self._task_engine_db or not self._task_engine_store:
            return
        with self._lock:
            if self._task_engine_recovery_completed or self._jobs:
                self._task_engine_recovery_completed = True
                return
        snapshots = self._task_engine_db.list_jobs(limit=1000, statuses=sorted(_INCOMPLETE_JOB_STATUSES))
        ordered_snapshots = sorted(
            [item for item in snapshots if isinstance(item, dict)],
            key=lambda item: (str(item.get("created_at", "") or ""), str(item.get("job_id", "") or "")),
        )
        for snapshot in ordered_snapshots:
            job_id = str(snapshot.get("job_id", "") or "").strip()
            if not job_id:
                continue
            job = self._job_from_snapshot(snapshot)
            stage = self._get_primary_stage(job)
            payload_path = self._task_engine_store.resolve_stage_payload_path(job.job_id, stage.stage_id)
            with self._lock:
                if job.job_id in self._jobs:
                    continue
                job.sequence = self._next_sequence()
                self._jobs[job.job_id] = job
            if job.cancel_requested or stage.cancel_requested:
                self._mark_restored_job_terminal(
                    job,
                    stage,
                    status="cancelled",
                    summary="cancelled_on_restart",
                    worker_status="cancelled",
                )
                continue
            if not stage.worker_handler:
                self._mark_restored_job_terminal(
                    job,
                    stage,
                    status="interrupted",
                    summary="restart_recovery_not_supported",
                    error="restart_recovery_not_supported",
                    worker_status="interrupted",
                )
                continue
            if not payload_path.exists():
                self._mark_restored_job_terminal(
                    job,
                    stage,
                    status="failed",
                    summary="restart_payload_missing",
                    error="restart_payload_missing",
                    worker_status="failed",
                )
                continue
            if self._should_preserve_waiting_job_on_restore(job, stage):
                job.wait_started_monotonic = time.monotonic()
                self._persist_job_snapshot(job)
                self._record_job_event(
                    job,
                    stage_id=stage.stage_id,
                    stream="job",
                    event_type="recovery",
                    level="info",
                    payload={
                        "action": "preserve_waiting_shared_bridge",
                        "summary": str(job.summary or "等待内网补采同步").strip() or "等待内网补采同步",
                        "timestamp": self._now_text(),
                    },
                )
                continue
            previous_status = str(stage.status or job.status or "").strip().lower()
            if previous_status == "running":
                orphan_pid = int(stage.worker_pid or 0)
                orphan_terminated = False
                if self._pid_exists(orphan_pid):
                    orphan_terminated = self._terminate_orphan_worker(orphan_pid)
                if str(stage.resume_policy or "manual_resume").strip().lower() == "requeue":
                    self._prepare_job_for_restart(job, stage, summary="restart_requeued")
                    self._record_job_event(
                        job,
                        stage_id=stage.stage_id,
                        stream="job",
                        event_type="recovery",
                        level="warning",
                        payload={
                            "action": "restart_requeue_running",
                            "orphan_pid": orphan_pid,
                            "orphan_terminated": orphan_terminated,
                            "timestamp": self._now_text(),
                        },
                    )
                    self._launch_existing_worker_job(job, stage, payload_path=payload_path, worker_handler=stage.worker_handler)
                    continue
                self._mark_restored_job_terminal(
                    job,
                    stage,
                    status="interrupted",
                    summary="worker_interrupted_by_main_restart",
                    error="worker_interrupted_by_main_restart",
                    worker_status="interrupted",
                )
                self._record_job_event(
                    job,
                    stage_id=stage.stage_id,
                    stream="job",
                    event_type="recovery",
                    level="warning",
                    payload={
                        "action": "interrupt_running_worker",
                        "orphan_pid": orphan_pid,
                        "orphan_terminated": orphan_terminated,
                        "timestamp": self._now_text(),
                    },
                )
                continue
            self._prepare_job_for_restart(job, stage, summary="restart_requeued")
            self._launch_existing_worker_job(job, stage, payload_path=payload_path, worker_handler=stage.worker_handler)
        with self._lock:
            self._task_engine_recovery_completed = True
        self._persist_resource_snapshot()

    def start_worker_job(
        self,
        name: str,
        *,
        worker_handler: str,
        worker_payload: Dict[str, Any] | None = None,
        resource_keys: List[str] | tuple[str, ...] | None = None,
        priority: str = "manual",
        feature: str = "",
        dedupe_key: str = "",
        submitted_by: str = "",
        resume_policy: str = "manual_resume",
    ) -> JobState:
        return self._create_worker_job(
            name=name,
            worker_handler=worker_handler,
            worker_payload=worker_payload,
            resource_keys=resource_keys,
            priority=priority,
            feature=feature,
            dedupe_key=dedupe_key,
            submitted_by=submitted_by,
            resume_policy=resume_policy,
            launch_immediately=True,
        )

    def create_waiting_worker_job(
        self,
        name: str,
        *,
        worker_handler: str,
        worker_payload: Dict[str, Any] | None = None,
        resource_keys: List[str] | tuple[str, ...] | None = None,
        priority: str = "manual",
        feature: str = "",
        dedupe_key: str = "",
        submitted_by: str = "",
        resume_policy: str = "manual_resume",
        wait_reason: str = "waiting:shared_bridge",
        summary: str = "等待内网补采同步",
        bridge_task_id: str = "",
    ) -> JobState:
        return self._create_worker_job(
            name=name,
            worker_handler=worker_handler,
            worker_payload=worker_payload,
            resource_keys=resource_keys,
            priority=priority,
            feature=feature,
            dedupe_key=dedupe_key,
            submitted_by=submitted_by,
            resume_policy=resume_policy,
            launch_immediately=False,
            initial_status="waiting_resource",
            initial_wait_reason=wait_reason,
            initial_summary=summary,
            bridge_task_id=bridge_task_id,
        )

    def _create_worker_job(
        self,
        name: str,
        *,
        worker_handler: str,
        worker_payload: Dict[str, Any] | None = None,
        resource_keys: List[str] | tuple[str, ...] | None = None,
        priority: str = "manual",
        feature: str = "",
        dedupe_key: str = "",
        submitted_by: str = "",
        resume_policy: str = "manual_resume",
        launch_immediately: bool = True,
        initial_status: str = "queued",
        initial_wait_reason: str = "",
        initial_summary: str = "",
        bridge_task_id: str = "",
    ) -> JobState:
        if not self._task_engine_store:
            raise RuntimeError("task engine not configured")
        normalized_resources = self._normalize_resource_keys(resource_keys)
        normalized_priority = str(priority or "manual").strip().lower() or "manual"
        normalized_submitted_by = str(submitted_by or normalized_priority).strip().lower() or "manual"
        normalized_dedupe_key = self._normalize_dedupe_key(dedupe_key)
        normalized_handler = str(worker_handler or "").strip()
        if not normalized_handler:
            raise ValueError("worker_handler is required")
        if normalized_dedupe_key and self._task_engine_db:
            snapshot = self._task_engine_db.find_active_job_by_dedupe_key(
                normalized_dedupe_key,
                statuses=sorted(_INCOMPLETE_JOB_STATUSES),
            )
            if isinstance(snapshot, dict):
                return self._job_from_snapshot(snapshot)
        with self._lock:
            existing = self._find_active_job_by_dedupe_key_locked(normalized_dedupe_key)
            if existing is not None:
                return existing
            job_id = uuid.uuid4().hex
            job = JobState(
                job_id=job_id,
                name=name,
                feature=str(feature or name or "").strip(),
                dedupe_key=normalized_dedupe_key,
                submitted_by=normalized_submitted_by,
                priority=normalized_priority,
                resource_keys=normalized_resources,
                status=str(initial_status or "queued").strip() or "queued",
                wait_reason=str(initial_wait_reason or "").strip(),
                summary=str(initial_summary or "").strip(),
                bridge_task_id=str(bridge_task_id or "").strip(),
                sequence=self._next_sequence(),
            )
            stage = StageState(
                stage_id="main",
                name=str(feature or name or "").strip() or str(name or "stage").strip() or "stage",
                resource_keys=list(normalized_resources),
                resume_policy=str(resume_policy or "manual_resume").strip() or "manual_resume",
                worker_handler=normalized_handler,
                status="waiting_resource" if str(initial_status or "").strip() == "waiting_resource" else "pending",
                summary=str(initial_summary or "").strip(),
            )
            job.stages = [stage]
            self._jobs[job_id] = job
            payload_path = self._task_engine_store.persist_stage_payload(
                job_id,
                stage.stage_id,
                self._json_ready(worker_payload or {}),
            )
            config_snapshot = self._capture_config_snapshot()
            self._persist_job_snapshot(job, config_snapshot=config_snapshot)
            if job.status == "waiting_resource":
                job.wait_started_monotonic = time.monotonic()
                self._record_job_event(
                    job,
                    stage_id=stage.stage_id,
                    stream="job",
                    event_type="waiting_resource",
                    level="info",
                    payload={
                        "reason": job.wait_reason,
                        "summary": job.summary,
                        "bridge_task_id": job.bridge_task_id,
                        "timestamp": self._now_text(),
                    },
                )
        self._persist_resource_snapshot()
        if launch_immediately:
            self._launch_existing_worker_job(job, stage, payload_path=payload_path, worker_handler=normalized_handler)
        return job

    def bind_bridge_task(self, job_id: str, bridge_task_id: str) -> JobState:
        with self._lock:
            normalized_job_id = str(job_id or "").strip()
            job = self._jobs.get(normalized_job_id)
            if job is None:
                job = self._restore_job_from_db_locked(normalized_job_id)
            if job is None:
                raise KeyError(f"任务不存在: {job_id}")
            job.bridge_task_id = str(bridge_task_id or "").strip()
            if isinstance(job.result, dict):
                job.result["bridge_task_id"] = job.bridge_task_id
            elif job.bridge_task_id:
                job.result = {"bridge_task_id": job.bridge_task_id}
            self._persist_job_snapshot(job)
            return job

    def resume_waiting_worker_job(
        self,
        job_id: str,
        *,
        worker_payload: Dict[str, Any] | None = None,
        summary: str = "共享文件已到位，正在继续处理",
    ) -> JobState:
        resume_started = time.perf_counter()
        if not self._task_engine_store:
            raise RuntimeError("task engine not configured")
        job: JobState
        stage: StageState
        payload_path: Path
        with self._lock:
            normalized_job_id = str(job_id or "").strip()
            job = self._jobs.get(normalized_job_id)
            if job is None:
                job = self._restore_job_from_db_locked(normalized_job_id)
            if job is None:
                raise KeyError(f"任务不存在: {job_id}")
            if str(job.status or "").strip().lower() in {"success", "failed", "cancelled", "partial_failed"}:
                return job
            stage = self._get_primary_stage(job)
            payload_path = self._task_engine_store.resolve_stage_payload_path(job.job_id, stage.stage_id)
            if isinstance(worker_payload, dict):
                payload_started = time.perf_counter()
                self._task_engine_store.persist_stage_payload(job.job_id, stage.stage_id, self._json_ready(worker_payload))
                payload_elapsed_ms = int((time.perf_counter() - payload_started) * 1000)
                if payload_elapsed_ms >= 1000:
                    self._write_console_line(
                        f"[共享桥接] waiting job payload 写入耗时: job={job.job_id}, elapsed_ms={payload_elapsed_ms}"
                    )
            job.status = "queued"
            job.wait_reason = ""
            job.summary = str(summary or "").strip()
            job.error = ""
            job.finished_at = ""
            job.started_at = ""
            job.wait_started_monotonic = 0.0
            stage.status = "pending"
            stage.summary = job.summary
            stage.error = ""
            stage.started_at = ""
            stage.finished_at = ""
            stage.result = None
            stage.cancel_requested = False
        self._launch_existing_worker_job(job, stage, payload_path=payload_path, worker_handler=stage.worker_handler)

        def _persist_resume_state() -> None:
            try:
                persist_started = time.perf_counter()
                self._append_log(job, "[共享桥接] 共享文件已到位，正在继续处理")
                self._persist_job_snapshot(job)
                self._record_job_event(
                    job,
                    stage_id=stage.stage_id,
                    stream="job",
                    event_type="shared_bridge_resume",
                    level="info",
                    payload={"summary": job.summary, "bridge_task_id": job.bridge_task_id, "timestamp": self._now_text()},
                )
                self._persist_resource_snapshot()
                persist_elapsed_ms = int((time.perf_counter() - persist_started) * 1000)
                if persist_elapsed_ms >= 1000:
                    self._write_console_line(
                        f"[共享桥接] waiting job 恢复状态持久化耗时: job={job.job_id}, elapsed_ms={persist_elapsed_ms}"
                    )
            except Exception as exc:  # noqa: BLE001
                self._write_console_line(f"[共享桥接] waiting job 恢复状态持久化失败: job={job.job_id}, error={exc}")

        threading.Thread(
            target=_persist_resume_state,
            daemon=True,
            name=f"bridge-resume-persist-{job.job_id[:8]}",
        ).start()
        resume_elapsed_ms = int((time.perf_counter() - resume_started) * 1000)
        if resume_elapsed_ms >= 1000:
            self._write_console_line(
                f"[共享桥接] waiting job 唤醒耗时: job={job.job_id}, elapsed_ms={resume_elapsed_ms}"
            )
        return job

    def fail_waiting_job(self, job_id: str, *, error_text: str, summary: str = "") -> JobState:
        with self._lock:
            normalized_job_id = str(job_id or "").strip()
            job = self._jobs.get(normalized_job_id)
            if job is None:
                job = self._restore_job_from_db_locked(normalized_job_id)
            if job is None:
                raise KeyError(f"任务不存在: {job_id}")
            if str(job.status or "").strip().lower() in {"success", "failed", "cancelled", "partial_failed"}:
                return job
            now_text = self._now_text()
            detail = str(error_text or "").strip() or "共享桥接阶段失败"
            job.status = "failed"
            job.error = detail
            job.summary = str(summary or "").strip() or detail
            job.finished_at = now_text
            job.wait_reason = ""
            job.wait_started_monotonic = 0.0
            stage = self._get_primary_stage(job)
            stage.status = "failed"
            stage.error = detail
            stage.summary = job.summary
            stage.finished_at = now_text
            self._append_log(job, f"[共享桥接] {detail}")
            self._persist_job_snapshot(job)
            self._record_job_event(
                job,
                stage_id=stage.stage_id,
                stream="job",
                event_type="shared_bridge_failed",
                level="error",
                payload={"error": detail, "bridge_task_id": job.bridge_task_id, "timestamp": now_text},
            )
            job.done_event.set()
            self._persist_resource_snapshot()
            return job

    def wait_job(self, job_id: str, timeout_sec: float | None = None) -> JobState:
        job = self.get_job_state(job_id)
        if job is None:
            raise KeyError(f"任务不存在: {job_id}")
        job.done_event.wait(timeout=timeout_sec)
        return job

    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        with self._lock:
            normalized_job_id = str(job_id or "").strip()
            job = self._jobs.get(normalized_job_id)
            if job is None:
                job = self._restore_job_from_db_locked(normalized_job_id)
            if job is None:
                raise KeyError(f"任务不存在: {job_id}")
            if job.status in {"success", "failed", "cancelled", "partial_failed"}:
                return job.to_dict()
            job.cancel_requested = True
            for stage in job.stages:
                stage.cancel_requested = True
            if job.status in {"queued", "waiting_resource"}:
                now_text = self._now_text()
                job.status = "cancelled"
                job.summary = "cancelled"
                job.finished_at = now_text
                for stage in job.stages:
                    stage.status = "cancelled"
                    stage.summary = "cancelled"
                    stage.finished_at = now_text
                self._persist_job_snapshot(job)
                self._record_job_event(
                    job,
                    stage_id="main",
                    stream="job",
                    event_type="cancel",
                    level="info",
                    payload={"reason": "user_requested", "timestamp": now_text},
                )
                job.done_event.set()
                result = job.to_dict()
                self._persist_resource_snapshot()
                return result
            stage = self._get_primary_stage(job)
            command_sent = self._send_worker_command(
                job_id=job.job_id,
                stage_id=stage.stage_id,
                payload={"type": "cancel", "reason": "user_requested"},
            )
            self._persist_job_snapshot(job)
            self._record_job_event(
                job,
                stage_id=stage.stage_id,
                stream="job",
                event_type="cancel",
                level="info",
                payload={"reason": "user_requested", "command_sent": bool(command_sent), "timestamp": self._now_text()},
            )
            process = self._worker_processes.get((job.job_id, stage.stage_id))
        if process is not None:
            def _force_kill_later() -> None:
                if process.poll() is not None:
                    return
                time.sleep(self._worker_cancel_timeout_sec)
                if process.poll() is None:
                    with self._lock:
                        self._worker_force_killed.add((job.job_id, stage.stage_id))
                    try:
                        process.kill()
                    except Exception:  # noqa: BLE001
                        pass
            threading.Thread(
                target=_force_kill_later,
                daemon=True,
                name=f"job-cancel-{job.job_id[:8]}",
            ).start()
        self._persist_resource_snapshot()
        return self.get_job(job_id)

    def retry_job(self, job_id: str) -> Dict[str, Any]:
        snapshot = self.get_job(job_id)
        stages = list(snapshot.get("stages") or [])
        if not stages:
            raise RuntimeError("任务没有可重试阶段")
        stage = dict(stages[0] or {})
        worker_handler = str(stage.get("worker_handler", "") or "").strip()
        if not worker_handler:
            raise RuntimeError("当前仅支持 worker 任务重试")
        if not self._task_engine_store:
            raise RuntimeError("task engine not configured")
        payload_path = self._task_engine_store.resolve_stage_payload_path(job_id, str(stage.get("stage_id", "") or "main"))
        if not payload_path.exists():
            raise RuntimeError("任务输入上下文不存在，无法重试")
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        new_job = self.start_worker_job(
            name=str(snapshot.get("name", "") or "").strip() or f"retry-{job_id}",
            worker_handler=worker_handler,
            worker_payload=payload,
            resource_keys=list(snapshot.get("resource_keys") or []),
            priority=str(snapshot.get("priority", "manual") or "manual").strip() or "manual",
            feature=str(snapshot.get("feature", "") or "").strip(),
            submitted_by="manual",
            resume_policy=str(stage.get("resume_policy", "manual_resume") or "manual_resume").strip() or "manual_resume",
        )
        return new_job.to_dict()

    def get_job_state(self, job_id: str) -> Optional[JobState]:
        with self._lock:
            return self._jobs.get(job_id)

    def get_job(self, job_id: str) -> Dict[str, Any]:
        if self._task_engine_db:
            try:
                payload = self._task_engine_db.get_job(job_id)
                if payload is not None:
                    return payload
            except Exception as exc:  # noqa: BLE001
                if not _is_recoverable_task_engine_error(exc):
                    raise
                job = self.get_job_state(job_id)
                if job is None:
                    raise TaskEngineUnavailableError("任务状态存储暂时不可用，请稍后重试") from exc
                with self._lock:
                    return job.to_dict()
        job = self.get_job_state(job_id)
        if job is None:
            raise KeyError(f"任务不存在: {job_id}")
        with self._lock:
            return job.to_dict()

    def list_jobs(self, *, limit: int = 50, statuses: List[str] | tuple[str, ...] | None = None) -> List[Dict[str, Any]]:
        if self._task_engine_db:
            try:
                return self._task_engine_db.list_jobs(limit=limit, statuses=statuses)
            except Exception as exc:  # noqa: BLE001
                if not _is_recoverable_task_engine_error(exc):
                    raise
        normalized_statuses = {str(item or "").strip().lower() for item in (statuses or []) if str(item or "").strip()}
        with self._lock:
            items = list(self._ordered_jobs())
            if normalized_statuses:
                items = [item for item in items if str(item.status or "").strip().lower() in normalized_statuses]
            items = list(reversed(items[-max(1, int(limit or 1)) :]))
            return [item.to_dict() for item in items]

    def job_counts(self) -> Dict[str, int]:
        if self._task_engine_db:
            try:
                return self._task_engine_db.job_counts()
            except Exception as exc:  # noqa: BLE001
                if not _is_recoverable_task_engine_error(exc):
                    raise
        with self._lock:
            counts: Dict[str, int] = {}
            for job in self._jobs.values():
                counts[job.status] = counts.get(job.status, 0) + 1
            return counts

    def _build_resource_snapshot_from_memory(self) -> Dict[str, Any]:
        with self._lock:
            self._update_network_window_locked()
            ordered_jobs = self._ordered_jobs()
            waiting_jobs = [job for job in ordered_jobs if job.status in {"queued", "waiting_resource"}]
            holders = {key: list(value) for key, value in self._resource_holders.items() if value}

        browser_holders = holders.get("browser:controlled", [])
        batch_locks: List[Dict[str, Any]] = []
        batch_seen: List[str] = []
        for key in list(holders.keys()) + [rk for job in waiting_jobs for rk in job.resource_keys]:
            if not str(key).startswith("handover_batch:"):
                continue
            batch_key = str(key).split(":", 1)[1]
            if batch_key and batch_key not in batch_seen:
                batch_seen.append(batch_key)
        for batch_key in batch_seen:
            resource_key = f"handover_batch:{batch_key}"
            batch_locks.append(
                {
                    "batch_key": batch_key,
                    "holder_job_id": (holders.get(resource_key, [""]) or [""])[0],
                    "queue_length": sum(1 for job in waiting_jobs if resource_key in job.resource_keys),
                }
            )

        network_keys = [
            ("network:pipeline", "pipeline"),
            ("network:internal", "internal"),
            ("network:external", "external"),
        ]
        network_snapshot: Dict[str, Any] = {}
        for resource_key, label in network_keys:
            network_snapshot[label] = {
                "resource_key": resource_key,
                "capacity": self._resource_capacity(resource_key),
                "holder_job_ids": list(holders.get(resource_key, [])),
                "queue_length": sum(1 for job in waiting_jobs if resource_key in job.resource_keys),
            }

        pipeline_holders = list(holders.get("network:pipeline", []))
        internal_holders = list(holders.get("network:internal", []))
        external_holders = list(holders.get("network:external", []))
        current_network_state = self._get_current_network_state()
        network_snapshot.update(
            {
                "current_side": self._network_window_current_side,
                "switching": False,
                "auto_switch_enabled": bool(self._network_auto_switch_enabled),
                "current_ssid": str(current_network_state.get("current_ssid", "") or "").strip(),
                "current_detected_side": str(current_network_state.get("ssid_side", "") or "").strip(),
                "ssid_side": str(current_network_state.get("ssid_side", "") or "").strip(),
                "internal_reachable": bool(current_network_state.get("internal_reachable", False)),
                "external_reachable": bool(current_network_state.get("external_reachable", False)),
                "reachable_sides": list(current_network_state.get("reachable_sides") or []),
                "mode": str(current_network_state.get("mode", "") or "").strip(),
                "last_checked_at": str(current_network_state.get("last_checked_at", "") or "").strip(),
                "target_internal_ssid": self._network_internal_ssid,
                "target_external_ssid": self._network_external_ssid,
                "window_started_at": self._network_window_started_at,
                "window_dispatch_count": int(self._network_window_dispatch_count or 0),
                "window_draining": bool(self._network_window_draining),
                "pending_side": self._network_window_pending_side,
                "queued_internal": network_snapshot["internal"]["queue_length"],
                "queued_external": network_snapshot["external"]["queue_length"],
                "queued_pipeline": network_snapshot["pipeline"]["queue_length"],
                "running_internal": len(internal_holders),
                "running_external": len(external_holders),
                "running_pipeline": len(pipeline_holders),
                "oldest_internal_wait_sec": self._oldest_wait_sec_locked("internal"),
                "oldest_external_wait_sec": self._oldest_wait_sec_locked("external"),
            }
        )

        return {
            "network": network_snapshot,
            "controlled_browser": {
                "holder_job_id": browser_holders[0] if browser_holders else "",
                "queue_length": sum(1 for job in waiting_jobs if "browser:controlled" in job.resource_keys),
            },
            "batch_locks": batch_locks,
            "resources": [
                {
                    "resource_key": key,
                    "capacity": self._resource_capacity(key),
                    "holder_job_ids": list(value),
                    "queue_length": sum(1 for job in waiting_jobs if key in job.resource_keys),
                }
                for key, value in sorted(holders.items(), key=lambda item: item[0])
            ],
        }

    def get_resource_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            has_live_jobs = any(job.status in _INCOMPLETE_JOB_STATUSES for job in self._jobs.values())
            resource_snapshot_dirty = bool(self._resource_snapshot_dirty)
        if has_live_jobs or resource_snapshot_dirty:
            return self._build_resource_snapshot_from_memory()
        if self._task_engine_db:
            try:
                snapshot = self._task_engine_db.get_resource_snapshot()
                if snapshot and (snapshot.get("resources") or snapshot.get("network")):
                    return snapshot
            except Exception as exc:  # noqa: BLE001
                if not _is_recoverable_task_engine_error(exc):
                    raise
        return self._build_resource_snapshot_from_memory()

    def log(self, job_id: str, text: str) -> None:
        sink = self._global_log_sink
        has_job = False
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            has_job = True
            self._write_console_line(text)
            self._append_log(job, text)
        if has_job and callable(sink):
            self._emit_global_log_sink_async(str(text or ""))

    def _emit_global_log_sink_async(self, text: str) -> None:
        raw = str(text or "").strip()
        if not raw:
            return
        if not callable(self._global_log_sink):
            return
        try:
            self._global_log_sink_queue.put_nowait(raw)
        except queue.Full:
            self._write_console_line("[任务日志] 全局日志队列繁忙，已跳过一条系统日志")

    def get_logs(self, job_id: str, offset: int = 0, *, after_event_id: int | None = None, limit: int = 1000) -> Dict[str, Any]:
        if self._task_engine_db:
            try:
                payload = self._task_engine_db.get_job(job_id)
                if payload is None:
                    raise KeyError(f"任务不存在: {job_id}")
                event_cursor = max(0, int(after_event_id or 0))
                events = self._task_engine_db.list_job_events(job_id, after_event_id=event_cursor, limit=limit)
                lines: List[str] = []
                for item in events:
                    payload_item = dict(item.get("payload") or {})
                    line = str(payload_item.get("line", "") or payload_item.get("message", "") or "").strip()
                    if line:
                        lines.append(line)
                return {
                    "job_id": job_id,
                    "offset": max(0, int(offset or 0)),
                    "next_offset": max(0, int(offset or 0)) + len(lines),
                    "status": str(payload.get("status", "") or ""),
                    "lines": lines,
                    "events": events,
                    "last_event_id": int(payload.get("last_event_id") or 0),
                }
            except KeyError:
                raise
            except Exception as exc:  # noqa: BLE001
                if not _is_recoverable_task_engine_error(exc):
                    raise
                with self._lock:
                    job = self._jobs.get(job_id)
                    if job is None:
                        raise TaskEngineUnavailableError("任务状态存储暂时不可用，请稍后重试") from exc
                    start = max(0, int(offset))
                    lines = job.logs[start:]
                    return {
                        "job_id": job_id,
                        "offset": start,
                        "next_offset": start + len(lines),
                        "status": job.status,
                        "lines": list(lines),
                        "events": [],
                        "last_event_id": int(job.last_event_id or 0),
                    }
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"任务不存在: {job_id}")
            start = max(0, int(offset))
            lines = job.logs[start:]
            return {
                "job_id": job_id,
                "offset": start,
                "next_offset": start + len(lines),
                "status": job.status,
                "lines": list(lines),
            }
