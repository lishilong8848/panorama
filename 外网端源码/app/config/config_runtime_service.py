from __future__ import annotations

import copy
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict

from app.config.config_adapter import adapt_runtime_config
from app.core.app_state import AppStateRepository


class ConfigRuntimeService:
    """In-memory config cache with SQLite snapshots and a single JSON writer."""

    def __init__(
        self,
        *,
        config_path: str | Path,
        repository: AppStateRepository | None,
        save_settings_func: Callable[..., Dict[str, Any]],
        emit_log: Callable[[str], None] | None = None,
        debounce_sec: float = 0.35,
    ) -> None:
        self.config_path = Path(config_path)
        self.repository = repository
        self._save_settings = save_settings_func
        self._emit_log = emit_log
        self._debounce_sec = max(0.05, float(debounce_sec or 0.35))
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._config: Dict[str, Any] = {}
        self._runtime_config: Dict[str, Any] = {}
        self._config_version = 0
        self._pending: Dict[str, Any] | None = None
        self._pending_version = 0
        self._pending_queue_ids: list[str] = []
        self._pending_source = ""
        self._pending_save_options: Dict[str, Any] = {}
        self._writing = False
        self._shutdown = False
        self._writer = threading.Thread(
            target=self._writer_loop,
            name="config-runtime-json-writer",
            daemon=True,
        )
        self._writer.start()

    def configure(self, config: Dict[str, Any], runtime_config: Dict[str, Any] | None = None) -> None:
        cfg = copy.deepcopy(config if isinstance(config, dict) else {})
        runtime = copy.deepcopy(runtime_config) if isinstance(runtime_config, dict) else adapt_runtime_config(cfg)
        with self._lock:
            self._config = cfg
            self._runtime_config = runtime
            self._config_version += 1

    def get_config(self) -> Dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._config)

    def get_runtime_config(self) -> Dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._runtime_config)

    def apply_config_snapshot(
        self,
        settings: Dict[str, Any],
        *,
        source: str,
        persist_json: bool = False,
        save_options: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        cfg = copy.deepcopy(settings if isinstance(settings, dict) else {})
        runtime = adapt_runtime_config(cfg)
        with self._lock:
            self._config = copy.deepcopy(cfg)
            self._runtime_config = copy.deepcopy(runtime)
            self._config_version += 1
            version = self._config_version
        self._record_snapshot(cfg, source=source)
        if persist_json:
            self._enqueue_json_write(cfg, source=source, save_options=save_options or {}, version=version)
        return cfg

    def update_config_patch(
        self,
        patch: Dict[str, Any],
        *,
        source: str,
        persist_json: bool = True,
        save_options: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        with self._lock:
            merged = copy.deepcopy(self._config)
        self._deep_merge(merged, patch if isinstance(patch, dict) else {})
        return self.apply_config_snapshot(
            merged,
            source=source,
            persist_json=persist_json,
            save_options=save_options,
        )

    def flush(self, timeout_sec: float = 5.0) -> bool:
        deadline = time.time() + max(0.1, float(timeout_sec or 5.0))
        while time.time() < deadline:
            with self._lock:
                if self._pending is None and not self._writing:
                    return True
            time.sleep(0.05)
        return False

    def shutdown(self, timeout_sec: float = 3.0) -> None:
        with self._condition:
            self._shutdown = True
            self._condition.notify_all()
        self._writer.join(timeout=max(0.1, float(timeout_sec or 3.0)))

    def _record_snapshot(self, cfg: Dict[str, Any], *, source: str) -> None:
        repository = self.repository
        if repository is None:
            return
        try:
            repository.record_config_snapshot(
                snapshot_id=uuid.uuid4().hex,
                source=str(source or "").strip(),
                config_path=str(self.config_path),
                payload=cfg,
            )
        except Exception as exc:  # noqa: BLE001
            self._log(f"[配置] SQLite 配置快照记录失败，已降级: {exc}")

    def _enqueue_json_write(
        self,
        cfg: Dict[str, Any],
        *,
        source: str,
        save_options: Dict[str, Any],
        version: int,
    ) -> None:
        queue_id = uuid.uuid4().hex
        repository = self.repository
        if repository is not None:
            try:
                repository.enqueue_config_write(
                    queue_id=queue_id,
                    patch={"config_path": str(self.config_path), "full_snapshot": cfg},
                    source=source,
                )
            except Exception as exc:  # noqa: BLE001
                self._log(f"[配置] JSON 落盘队列记录失败，仍继续异步落盘: {exc}")
        with self._condition:
            if self._pending_queue_ids:
                for old_queue_id in self._pending_queue_ids:
                    if repository is None:
                        continue
                    try:
                        repository.finish_config_write(queue_id=old_queue_id, status="coalesced")
                    except Exception:
                        pass
            self._pending = copy.deepcopy(cfg)
            self._pending_version = int(version or 0)
            self._pending_queue_ids = [queue_id]
            self._pending_source = str(source or "").strip()
            self._pending_save_options = copy.deepcopy(save_options if isinstance(save_options, dict) else {})
            self._condition.notify_all()

    def _writer_loop(self) -> None:
        while True:
            with self._condition:
                while self._pending is None and not self._shutdown:
                    self._condition.wait(timeout=1.0)
                if self._shutdown and self._pending is None:
                    return
                if not self._shutdown:
                    self._condition.wait(timeout=self._debounce_sec)
                snapshot = copy.deepcopy(self._pending)
                snapshot_version = int(self._pending_version or 0)
                queue_ids = list(self._pending_queue_ids)
                source = self._pending_source
                save_options = copy.deepcopy(self._pending_save_options)
                self._pending = None
                self._pending_version = 0
                self._pending_queue_ids = []
                self._pending_source = ""
                self._pending_save_options = {}
                self._writing = True
            if snapshot is None:
                with self._condition:
                    self._writing = False
                    self._condition.notify_all()
                continue
            status = "success"
            error = ""
            try:
                saved = self._save_settings(snapshot, self.config_path, **save_options)
                if isinstance(saved, dict):
                    with self._lock:
                        if snapshot_version == self._config_version:
                            self._config = copy.deepcopy(saved)
                            self._runtime_config = adapt_runtime_config(saved)
                self._log(f"[配置] JSON 异步落盘完成 source={source or '-'}")
            except Exception as exc:  # noqa: BLE001
                status = "failed"
                error = str(exc)
                self._log(f"[配置] JSON 异步落盘失败 source={source or '-'}, error={error}")
            repository = self.repository
            if repository is not None:
                for queue_id in queue_ids:
                    try:
                        repository.finish_config_write(queue_id=queue_id, status=status, error=error)
                    except Exception:
                        pass
            with self._condition:
                self._writing = False
                self._condition.notify_all()

    @staticmethod
    def _deep_merge(target: Dict[str, Any], patch: Dict[str, Any]) -> None:
        for key, value in patch.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                ConfigRuntimeService._deep_merge(target[key], value)
            else:
                target[key] = copy.deepcopy(value)

    def _log(self, message: str) -> None:
        if callable(self._emit_log):
            try:
                self._emit_log(message)
            except Exception:
                pass
