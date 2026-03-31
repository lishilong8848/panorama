from __future__ import annotations

import copy
import re
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.config.config_adapter import adapt_runtime_config, resolve_shared_bridge_paths
from app.config.secret_masking import load_masked_settings
from app.config.settings_loader import load_bootstrap_settings
from app.modules.network.service.wifi_switch_service import WifiSwitchService
from app.modules.report_pipeline.service.job_service import JobService
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService
from app.modules.report_pipeline.service.system_alert_log_upload_service import (
    SystemAlertLogUploadService,
)
from app.modules.scheduler.service.daily_scheduler_service import DailyAutoSchedulerService
from app.modules.scheduler.service.handover_scheduler_manager import HandoverSchedulerManager
from app.modules.scheduler.service.interval_scheduler_service import IntervalSchedulerService
from app.modules.updater.service.updater_service import UpdaterService
from pipeline_utils import get_app_dir, get_bundle_dir


APP_VERSION = "web-3.0.0"
_WARNING_RE = re.compile(r"(^|:)\s*(?:\w+)?Warning:", re.IGNORECASE)
_ERROR_PATTERNS = (
    "traceback (most recent call last):",
    "exception",
    "[文件流程失败]",
    " error",
    "错误",
    "失败",
)


@dataclass
class AppContainer:
    config: Dict[str, Any]
    runtime_config: Dict[str, Any]
    config_path: Path
    frontend_mode: str
    frontend_root: Path
    frontend_assets_dir: Path
    job_service: JobService
    wifi_service: WifiSwitchService | None = None
    scheduler: DailyAutoSchedulerService | None = None
    scheduler_callback: Callable[[str], tuple[bool, str]] | None = None
    handover_scheduler_manager: HandoverSchedulerManager | None = None
    handover_scheduler_callback: Callable[[str, str], tuple[bool, str]] | None = None
    wet_bulb_collection_scheduler: IntervalSchedulerService | None = None
    wet_bulb_collection_scheduler_callback: Callable[[str], tuple[bool, str]] | None = None
    updater_service: UpdaterService | None = None
    updater_restart_callback: Callable[[Dict[str, Any]], tuple[bool, str]] | None = None
    alert_log_uploader: SystemAlertLogUploadService | None = None
    shared_bridge_service: SharedBridgeRuntimeService | None = None
    system_logs: List[str] = field(default_factory=list)
    system_log_entries: List[Dict[str, Any]] = field(default_factory=list)
    version: str = APP_VERSION
    runtime_services_armed: bool = False
    _system_log_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _system_log_next_id: int = 0

    def _configured_deployment_snapshot(self) -> Dict[str, Any]:
        common = self.config.get("common", {}) if isinstance(self.config, dict) else {}
        deployment = common.get("deployment", {}) if isinstance(common, dict) else {}
        if not isinstance(deployment, dict):
            deployment = {}
        role_mode = str(deployment.get("role_mode", "") or "").strip().lower()
        if role_mode not in {"internal", "external"}:
            role_mode = ""
        node_id = str(deployment.get("node_id", "") or "").strip()
        node_label = str(deployment.get("node_label", "") or "").strip()
        if not node_label and role_mode == "internal":
            node_label = "内网端"
        if not node_label and role_mode == "external":
            node_label = "外网端"
        return {
            "role_mode": role_mode,
            "node_id": node_id,
            "node_label": node_label,
        }

    def _ensure_runtime_dependencies_initialized(self) -> None:
        if not self.wifi_service:
            self.wifi_service = WifiSwitchService(self.runtime_config)
        self.job_service.configure_task_engine(
            runtime_config=self.runtime_config,
            app_dir=get_app_dir(),
            config_snapshot_getter=lambda: self.runtime_config,
            current_ssid_getter=lambda: self.wifi_service.current_ssid() if self.wifi_service else "",
        )
        self.job_service.set_global_log_sink(
            lambda line: self.add_system_log(
                line,
                source="job",
                write_console=False,
            )
        )
        if not self.scheduler:
            self.scheduler = self._build_scheduler()
        if not self.handover_scheduler_manager:
            self.handover_scheduler_manager = self._build_handover_scheduler_manager()
        if not self.wet_bulb_collection_scheduler:
            self.wet_bulb_collection_scheduler = self._build_wet_bulb_collection_scheduler()
        if not self.updater_service:
            self.updater_service = self._build_updater_service()
        if not self.shared_bridge_service:
            self.shared_bridge_service = self._build_shared_bridge_service()
        if not self.alert_log_uploader:
            paths_cfg = self.runtime_config.get("paths", {}) if isinstance(self.runtime_config, dict) else {}
            runtime_state_root = (
                str(paths_cfg.get("runtime_state_root", "") or "").strip()
                if isinstance(paths_cfg, dict)
                else ""
            )
            self.alert_log_uploader = SystemAlertLogUploadService(
                config_getter=lambda: self.config,
                active_job_id_getter=self.job_service.active_job_id,
                emit_log=lambda text: self.add_system_log(
                    text,
                    source="uploader",
                    suppress_alert_upload=True,
                ),
                runtime_state_root=runtime_state_root,
                mark_uploaded=self.mark_system_log_entries_uploaded,
            )
        if self.scheduler_callback and self.scheduler:
            self.scheduler.run_callback = self.scheduler_callback
        if self.handover_scheduler_callback and self.handover_scheduler_manager:
            self.handover_scheduler_manager.set_run_callback(self.handover_scheduler_callback)
        if self.wet_bulb_collection_scheduler_callback and self.wet_bulb_collection_scheduler:
            self.wet_bulb_collection_scheduler.run_callback = self.wet_bulb_collection_scheduler_callback
        if self.updater_restart_callback and self.updater_service:
            self.updater_service.restart_callback = self.updater_restart_callback

    def _console_cfg(self) -> Dict[str, Any]:
        return self.config.get("common", {}).get("console", {}) if isinstance(self.config, dict) else {}

    @staticmethod
    def _classify_log_level(text: str) -> str:
        raw = str(text or "").strip()
        lowered = raw.lower()
        if any(pattern in lowered for pattern in _ERROR_PATTERNS):
            return "error"
        if _WARNING_RE.search(raw) or "deprecationwarning:" in lowered or "userwarning:" in lowered:
            return "warning"
        return "info"

    @staticmethod
    def _normalize_log_source(*, source: str, level: str, text: str) -> str:
        raw_source = str(source or "").strip().lower() or "system"
        lowered = str(text or "").lower()
        if raw_source == "uploader":
            return "uploader"
        if level == "warning" and ("warning:" in lowered or _WARNING_RE.search(str(text or ""))):
            return "python_warning"
        if raw_source in {"system", "job", "python_warning", "uploader"}:
            return raw_source
        return "system"

    def _trim_system_log_buffers(self) -> None:
        max_size = int(self._console_cfg().get("log_buffer_size", 5000))
        max_items = max(100, max_size)
        raw_overflow = len(self.system_logs) - max_items
        if raw_overflow > 0:
            del self.system_logs[:raw_overflow]
        entry_overflow = len(self.system_log_entries) - max_items
        if entry_overflow > 0:
            del self.system_log_entries[:entry_overflow]

    def add_system_log(
        self,
        text: str,
        *,
        source: str = "system",
        write_console: bool = True,
        suppress_alert_upload: bool = False,
    ) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raw_text = str(text or "").strip()
        line = f"[{timestamp}] {raw_text}"
        level = self._classify_log_level(raw_text)
        normalized_source = self._normalize_log_source(source=source, level=level, text=raw_text)
        if write_console:
            print(line)
        entry: Dict[str, Any] = {}
        with self._system_log_lock:
            self._system_log_next_id += 1
            entry = {
                "id": self._system_log_next_id,
                "timestamp": timestamp,
                "level": level,
                "source": normalized_source,
                "line": line,
                "uploaded": False,
            }
            self.system_logs.append(line)
            self.system_log_entries.append(entry)
            self._trim_system_log_buffers()
        if level in {"warning", "error"} and self.alert_log_uploader and not suppress_alert_upload:
            try:
                self.alert_log_uploader.enqueue_entry(entry)
            except Exception:  # noqa: BLE001
                pass

    def get_system_log_entries(
        self,
        *,
        levels: set[str] | None = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        normalized_levels = {str(item or "").strip().lower() for item in (levels or set()) if str(item or "").strip()}
        with self._system_log_lock:
            items = list(self.system_log_entries)
        if normalized_levels:
            items = [item for item in items if str(item.get("level", "")).strip().lower() in normalized_levels]
        return [copy.deepcopy(item) for item in items[-max(1, int(limit or 1)) :]]

    def system_log_next_offset(self) -> int:
        with self._system_log_lock:
            return int(self._system_log_next_id)

    def mark_system_log_entries_uploaded(self, entry_ids: List[int]) -> None:
        target_ids = {int(item) for item in entry_ids if int(item or 0) > 0}
        if not target_ids:
            return
        with self._system_log_lock:
            for item in self.system_log_entries:
                if int(item.get("id", 0) or 0) in target_ids:
                    item["uploaded"] = True

    def _build_scheduler(self) -> DailyAutoSchedulerService:
        return DailyAutoSchedulerService(
            config=self.runtime_config,
            emit_log=self.add_system_log,
            run_callback=self.scheduler_callback or self._scheduler_run_callback,
            is_busy=lambda: False,
        )

    def _build_handover_scheduler_manager(self) -> HandoverSchedulerManager:
        return HandoverSchedulerManager(
            config=self.runtime_config,
            emit_log=self.add_system_log,
            run_callback=self.handover_scheduler_callback or self._handover_scheduler_run_callback,
            is_busy=lambda: False,
        )

    def _build_wet_bulb_collection_scheduler(self) -> IntervalSchedulerService:
        wet_cfg = self.runtime_config.get("wet_bulb_collection", {})
        if not isinstance(wet_cfg, dict):
            wet_cfg = {}
        scheduler_cfg = wet_cfg.get("scheduler", {})
        if not isinstance(scheduler_cfg, dict):
            scheduler_cfg = {}
        return IntervalSchedulerService(
            scheduler_cfg=scheduler_cfg,
            runtime_state_root="runtime_state",
            emit_log=self.add_system_log,
            run_callback=self.wet_bulb_collection_scheduler_callback or self._wet_bulb_collection_scheduler_run_callback,
            is_busy=lambda: False,
            thread_name="wet-bulb-collection-scheduler",
            source_name="湿球温度定时采集",
        )

    def _build_updater_service(self) -> UpdaterService:
        return UpdaterService(
            config=self.runtime_config,
            emit_log=self.add_system_log,
            restart_callback=self.updater_restart_callback,
            is_busy=self.job_service.has_incomplete_jobs,
        )

    def _build_shared_bridge_service(self) -> SharedBridgeRuntimeService:
        return SharedBridgeRuntimeService(
            runtime_config=self.runtime_config,
            app_version=self.version,
            emit_log=self.add_system_log,
        )

    def _scheduler_run_callback(self, source: str) -> tuple[bool, str]:
        return False, "调度回调尚未绑定执行器"

    def _handover_scheduler_run_callback(self, slot: str, source: str) -> tuple[bool, str]:
        return False, f"交接班调度回调尚未绑定执行器(slot={slot}, source={source})"

    def _wet_bulb_collection_scheduler_run_callback(self, source: str) -> tuple[bool, str]:
        return False, f"湿球温度定时采集调度回调尚未绑定执行器(source={source})"

    def _is_placeholder_callback(self, callback: Any) -> bool:
        if callback is None:
            return True
        placeholder = self._scheduler_run_callback
        cb_func = getattr(callback, "__func__", None)
        cb_self = getattr(callback, "__self__", None)
        ph_func = getattr(placeholder, "__func__", None)
        ph_self = getattr(placeholder, "__self__", None)
        if cb_func is not None and ph_func is not None:
            return cb_func is ph_func and cb_self is ph_self
        return callback is placeholder

    def is_scheduler_executor_bound(self) -> bool:
        callback = None
        if self.scheduler:
            callback = getattr(self.scheduler, "run_callback", None)
        if callback is None:
            callback = self.scheduler_callback
        return not self._is_placeholder_callback(callback)

    def scheduler_executor_name(self) -> str:
        callback = None
        if self.scheduler:
            callback = getattr(self.scheduler, "run_callback", None)
        if callback is None:
            callback = self.scheduler_callback
        if callback is None:
            callback = self._scheduler_run_callback
        name = getattr(callback, "__name__", "")
        if not name:
            name = getattr(getattr(callback, "__func__", None), "__name__", "")
        return str(name or "-")

    def is_handover_scheduler_executor_bound(self) -> bool:
        callback = self.handover_scheduler_callback
        return callable(callback)

    def handover_scheduler_executor_name(self) -> str:
        callback = self.handover_scheduler_callback
        if callback is None:
            return "-"
        name = getattr(callback, "__name__", "")
        if not name:
            name = getattr(getattr(callback, "__func__", None), "__name__", "")
        return str(name or "-")

    def is_wet_bulb_collection_scheduler_executor_bound(self) -> bool:
        callback = self.wet_bulb_collection_scheduler_callback
        return callable(callback)

    def wet_bulb_collection_scheduler_executor_name(self) -> str:
        callback = self.wet_bulb_collection_scheduler_callback
        if callback is None:
            return "-"
        name = getattr(callback, "__name__", "")
        if not name:
            name = getattr(getattr(callback, "__func__", None), "__name__", "")
        return str(name or "-")

    @staticmethod
    def _runtime_action_reason_text(reason: Any) -> str:
        text = str(reason or "").strip().lower()
        if text == "already_running":
            return "已在运行"
        if text == "disabled":
            return "未启用"
        if text == "disabled_or_switching":
            return "当前未启用共享桥接或处于单机切网端"
        if text == "started":
            return "已启动"
        if text == "partial_started":
            return "部分已启动"
        if text == "not_running":
            return "当前未运行"
        if text == "stopped":
            return "已停止"
        if text == "not_initialized":
            return "尚未初始化"
        return str(reason or "").strip() or "-"

    def set_scheduler_callback(self, callback: Callable[[str], tuple[bool, str]]) -> None:
        self.scheduler_callback = callback
        if self.scheduler:
            self.scheduler.run_callback = callback

    def set_handover_scheduler_callback(self, callback: Callable[[str, str], tuple[bool, str]]) -> None:
        self.handover_scheduler_callback = callback
        if self.handover_scheduler_manager:
            self.handover_scheduler_manager.set_run_callback(callback)

    def set_wet_bulb_collection_scheduler_callback(self, callback: Callable[[str], tuple[bool, str]]) -> None:
        self.wet_bulb_collection_scheduler_callback = callback
        if self.wet_bulb_collection_scheduler:
            self.wet_bulb_collection_scheduler.run_callback = callback

    def set_updater_restart_callback(self, callback: Callable[[Dict[str, Any]], tuple[bool, str]]) -> None:
        self.updater_restart_callback = callback
        if self.updater_service:
            self.updater_service.restart_callback = callback

    def request_app_restart(self, context: Dict[str, Any] | None = None) -> tuple[bool, str]:
        callback = self.updater_restart_callback
        if callback is None:
            return False, "当前未绑定程序重启回调"
        return callback(dict(context or {}))

    def start_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if not self.scheduler:
            self.scheduler = self._build_scheduler()
        result = self.scheduler.start()
        self.add_system_log(
            f"[调度] {source}启动请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}, "
            f"executor_bound={self.is_scheduler_executor_bound()}, "
            f"callback={self.scheduler_executor_name()}"
        )
        return result

    def start_role_runtime_services(self, source: str = "启动确认") -> Dict[str, Any]:
        self._ensure_runtime_dependencies_initialized()
        role_mode = str(self.deployment_snapshot().get("role_mode", "") or "").strip().lower()
        self.runtime_services_armed = True

        self.add_system_log(
            f"[调度] 启动阶段执行器状态: executor_bound={self.is_scheduler_executor_bound()}, "
            f"callback={self.scheduler_executor_name()}"
        )
        if role_mode == "internal":
            self.add_system_log("[调度] 当前为内网端，启动时不自动开启月报调度")
        elif self.scheduler and self.scheduler.enabled and self.scheduler.auto_start_in_gui:
            self.start_scheduler(source=source)
        else:
            self.add_system_log("[调度] 启动时未自动开启")

        self.add_system_log(
            f"[交接班调度] 启动阶段执行器状态: executor_bound={self.is_handover_scheduler_executor_bound()}, "
            f"callback={self.handover_scheduler_executor_name()}"
        )
        handover_status = self.handover_scheduler_status()
        if role_mode == "internal":
            self.add_system_log("[交接班调度] 当前为内网端，启动时不自动开启")
        elif bool(handover_status.get("enabled", False)):
            handover_cfg = self.runtime_config.get("handover_log", {})
            if not isinstance(handover_cfg, dict):
                handover_cfg = {}
            handover_scheduler_cfg = handover_cfg.get("scheduler", {})
            if not isinstance(handover_scheduler_cfg, dict):
                handover_scheduler_cfg = {}
            if bool(handover_scheduler_cfg.get("auto_start_in_gui", False)):
                self.start_handover_scheduler(source=source)
            else:
                self.add_system_log("[交接班调度] 启动时未自动开启")
        else:
            self.add_system_log("[交接班调度] 已禁用")

        self.add_system_log(
            f"[湿球温度定时采集调度] 启动阶段执行器状态: executor_bound={self.is_wet_bulb_collection_scheduler_executor_bound()}, "
            f"callback={self.wet_bulb_collection_scheduler_executor_name()}"
        )
        wet_bulb_status = self.wet_bulb_collection_scheduler_status()
        if role_mode == "internal":
            self.add_system_log("[湿球温度定时采集调度] 当前为内网端，启动时不自动开启")
        elif bool(wet_bulb_status.get("enabled", False)):
            wet_bulb_cfg = self.runtime_config.get("wet_bulb_collection", {})
            if not isinstance(wet_bulb_cfg, dict):
                wet_bulb_cfg = {}
            wet_bulb_scheduler_cfg = wet_bulb_cfg.get("scheduler", {})
            if not isinstance(wet_bulb_scheduler_cfg, dict):
                wet_bulb_scheduler_cfg = {}
            if bool(wet_bulb_scheduler_cfg.get("auto_start_in_gui", False)):
                self.start_wet_bulb_collection_scheduler(source=source)
            else:
                self.add_system_log("[湿球温度定时采集调度] 启动时未自动开启")
        else:
            self.add_system_log("[湿球温度定时采集调度] 已禁用")

        if self.updater_service and self.updater_service.enabled:
            self.start_updater(source=source)
        else:
            self.add_system_log("[更新] 启动时未自动开启")

        if role_mode != "internal" and self.alert_log_uploader:
            self.start_alert_log_uploader(source=source)
        self.start_shared_bridge(source=source)
        return {"ok": True, "armed": True, "role_mode": role_mode}

    def stop_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if self.scheduler:
            result = self.scheduler.stop()
        else:
            result = {"stopped": False, "running": False, "reason": "not_initialized"}
        self.add_system_log(
            f"[调度] {source}停止请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}"
        )
        return result

    def start_handover_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if not self.handover_scheduler_manager:
            self.handover_scheduler_manager = self._build_handover_scheduler_manager()
        result = self.handover_scheduler_manager.start()
        self.add_system_log(
            f"[交接班调度] {source}启动请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}, "
            f"executor_bound={self.is_handover_scheduler_executor_bound()}, "
            f"callback={self.handover_scheduler_executor_name()}"
        )
        return result

    def stop_handover_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if self.handover_scheduler_manager:
            result = self.handover_scheduler_manager.stop()
        else:
            result = {"stopped": False, "running": False, "reason": "not_initialized"}
        self.add_system_log(
            f"[交接班调度] {source}停止请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}"
        )
        return result

    def start_wet_bulb_collection_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if not self.wet_bulb_collection_scheduler:
            self.wet_bulb_collection_scheduler = self._build_wet_bulb_collection_scheduler()
        result = self.wet_bulb_collection_scheduler.start()
        self.add_system_log(
            f"[湿球温度定时采集调度] {source}启动请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}, "
            f"executor_bound={self.is_wet_bulb_collection_scheduler_executor_bound()}, "
            f"callback={self.wet_bulb_collection_scheduler_executor_name()}"
        )
        return result

    def stop_wet_bulb_collection_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if self.wet_bulb_collection_scheduler:
            result = self.wet_bulb_collection_scheduler.stop()
        else:
            result = {"stopped": False, "running": False, "reason": "not_initialized"}
        self.add_system_log(
            f"[湿球温度定时采集调度] {source}停止请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}"
        )
        return result

    def start_updater(self, source: str = "自动") -> Dict[str, Any]:
        self.ensure_updater_service()
        result = self.updater_service.start()
        self.add_system_log(
            f"[更新] {source}启动请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}"
        )
        return result

    def ensure_updater_service(self) -> UpdaterService:
        if not self.updater_service:
            self.updater_service = self._build_updater_service()
            if self.updater_restart_callback:
                self.updater_service.restart_callback = self.updater_restart_callback
        return self.updater_service

    def stop_updater(self, source: str = "自动") -> Dict[str, Any]:
        if self.updater_service:
            result = self.updater_service.stop()
        else:
            result = {"stopped": False, "running": False, "reason": "not_initialized"}
        self.add_system_log(
            f"[更新] {source}停止请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}"
        )
        return result

    @staticmethod
    def _shared_bridge_reason_text(reason: Any) -> str:
        return AppContainer._runtime_action_reason_text(reason)

    def start_shared_bridge(self, source: str = "自动") -> Dict[str, Any]:
        if not self.shared_bridge_service:
            self.shared_bridge_service = self._build_shared_bridge_service()
        result = self.shared_bridge_service.start()
        self.add_system_log(
            f"[共享桥接] {source}启动请求: 原因={self._shared_bridge_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}"
        )
        return result

    def stop_shared_bridge(self, source: str = "自动") -> Dict[str, Any]:
        if self.shared_bridge_service:
            result = self.shared_bridge_service.stop()
        else:
            result = {"stopped": False, "running": False, "reason": "not_initialized"}
        self.add_system_log(
            f"[共享桥接] {source}停止请求: 原因={self._shared_bridge_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}"
        )
        return result

    def deployment_snapshot(self) -> Dict[str, Any]:
        if not self.shared_bridge_service:
            return self._configured_deployment_snapshot()
        return self.shared_bridge_service.get_deployment_snapshot()

    def shared_bridge_snapshot(self) -> Dict[str, Any]:
        if not self.shared_bridge_service:
            deployment = self.deployment_snapshot()
            runtime_shared_bridge = {}
            if isinstance(self.runtime_config, dict):
                runtime_shared_bridge = self.runtime_config.get("shared_bridge", {})
            if not isinstance(runtime_shared_bridge, dict) or not runtime_shared_bridge:
                common_cfg = self.config.get("common", {}) if isinstance(self.config, dict) else {}
                runtime_shared_bridge = common_cfg.get("shared_bridge", {}) if isinstance(common_cfg, dict) else {}
            resolved_bridge = resolve_shared_bridge_paths(runtime_shared_bridge, deployment.get("role_mode"))
            bridge_enabled = bool(resolved_bridge.get("enabled", False))
            bridge_root = str(resolved_bridge.get("root_dir", "") or "").strip()
            bridge_active = bool(bridge_enabled and bridge_root)
            snapshot = {
                "enabled": bridge_enabled,
                "role_mode": str(deployment.get("role_mode", "") or "").strip(),
                "root_dir": bridge_root,
                "db_status": "stopped" if bridge_active else "disabled",
                "last_error": "",
                "last_poll_at": "",
                "pending_internal": 0,
                "pending_external": 0,
                "problematic": 0,
                "task_count": 0,
                "node_count": 0,
                "node_heartbeat_ok": False,
                "agent_status": "stopped" if bridge_active else "disabled",
                "heartbeat_interval_sec": 5,
                "poll_interval_sec": 2,
            }
        else:
            snapshot = self.shared_bridge_service.get_health_snapshot()
        role_mode = str(snapshot.get("role_mode", "") or self.deployment_snapshot().get("role_mode", "")).strip().lower()
        if role_mode != "internal":
            snapshot = dict(snapshot)
            snapshot["internal_download_pool"] = {
                "enabled": False,
                "browser_ready": False,
                "page_slots": [],
                "active_buildings": [],
                "last_error": "",
            }
        return snapshot

    def updater_snapshot(self) -> Dict[str, Any]:
        if not self.updater_service:
            return {
                "running": False,
                "last_check_at": "",
                "last_result": "",
                "last_error": "",
                "local_version": "",
                "remote_version": "",
                "source_kind": "remote",
                "source_label": "远端正式更新源",
                "local_release_revision": 0,
                "remote_release_revision": 0,
                "update_available": False,
                "force_apply_available": False,
                "restart_required": False,
                "dependency_sync_status": "idle",
                "dependency_sync_error": "",
                "dependency_sync_at": "",
                "queued_apply": {
                    "queued": False,
                    "mode": "",
                    "queued_at": "",
                    "reason": "",
                },
                "state_path": "",
                "mirror_ready": False,
                "mirror_version": "",
                "mirror_manifest_path": "",
                "last_publish_at": "",
                "last_publish_error": "",
            }
        return self.updater_service.get_runtime_snapshot()

    def start_alert_log_uploader(self, source: str = "自动") -> Dict[str, Any]:
        if not self.alert_log_uploader:
            return {"started": False, "running": False, "reason": "not_initialized"}
        result = self.alert_log_uploader.start()
        self.add_system_log(
            f"[系统告警上报] {source}启动请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}",
            source="uploader",
            suppress_alert_upload=True,
        )
        return result

    def stop_alert_log_uploader(self, source: str = "自动") -> Dict[str, Any]:
        if not self.alert_log_uploader:
            return {"stopped": False, "running": False, "reason": "not_initialized"}
        result = self.alert_log_uploader.stop()
        self.add_system_log(
            f"[系统告警上报] {source}停止请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}",
            source="uploader",
            suppress_alert_upload=True,
        )
        return result

    def handover_scheduler_status(self) -> Dict[str, Any]:
        if not self.handover_scheduler_manager:
            return {"enabled": False, "running": False, "status": "未初始化", "slots": {}, "state_paths": {}}
        return self.handover_scheduler_manager.get_runtime_snapshot()

    def handover_scheduler_diagnostics(self, limit: int = 50) -> Dict[str, Any]:
        if not self.handover_scheduler_manager:
            return {
                "ok": True,
                "config": {},
                "running": False,
                "status": "未初始化",
                "slots": {},
                "executor_bound": self.is_handover_scheduler_executor_bound(),
                "callback_name": self.handover_scheduler_executor_name(),
            }
        data = self.handover_scheduler_manager.get_diagnostics(limit=limit)
        data["ok"] = True
        data["executor_bound"] = self.is_handover_scheduler_executor_bound()
        data["callback_name"] = self.handover_scheduler_executor_name()
        return data

    def wet_bulb_collection_scheduler_status(self) -> Dict[str, Any]:
        if not self.wet_bulb_collection_scheduler:
            return {
                "enabled": False,
                "running": False,
                "status": "未初始化",
                "next_run_time": "",
                "last_check_at": "",
                "last_decision": "",
                "last_trigger_at": "",
                "last_trigger_result": "",
                "state_path": "",
                "state_exists": False,
            }
        runtime = self.wet_bulb_collection_scheduler.get_runtime_snapshot()
        return {
            "enabled": bool(self.wet_bulb_collection_scheduler.enabled),
            **runtime,
        }

    def record_wet_bulb_collection_external_run(
        self,
        *,
        status: str,
        source: str,
        detail: str = "",
        duration_ms: int = 0,
    ) -> None:
        if not self.wet_bulb_collection_scheduler:
            return
        self.wet_bulb_collection_scheduler.record_external_run(
            status=status,
            source=source,
            detail=detail,
            duration_ms=duration_ms,
        )

    def reload_config(self, settings: Dict[str, Any]) -> None:
        self.config = copy.deepcopy(settings)
        self.runtime_config = adapt_runtime_config(self.config)
        self.job_service.update_log_buffer_size(int(self._console_cfg().get("log_buffer_size", 5000)))
        self.wifi_service = WifiSwitchService(self.runtime_config)
        self.job_service.configure_task_engine(
            runtime_config=self.runtime_config,
            app_dir=get_app_dir(),
            config_snapshot_getter=lambda: self.runtime_config,
            current_ssid_getter=lambda: self.wifi_service.current_ssid() if self.wifi_service else "",
        )

        was_running = self.scheduler.is_running() if self.scheduler else False
        was_handover_running = (
            self.handover_scheduler_manager.is_running() if self.handover_scheduler_manager else False
        )
        was_wet_bulb_running = (
            self.wet_bulb_collection_scheduler.is_running() if self.wet_bulb_collection_scheduler else False
        )
        was_alert_log_uploader_running = self.alert_log_uploader.is_running() if self.alert_log_uploader else False
        was_shared_bridge_running = self.shared_bridge_service.is_running() if self.shared_bridge_service else False
        previous_role_mode = (
            str(self.shared_bridge_service.get_deployment_snapshot().get("role_mode", "") or "").strip().lower()
            if self.shared_bridge_service
            else ""
        )
        if self.scheduler:
            self.scheduler.stop()
        if self.handover_scheduler_manager:
            self.handover_scheduler_manager.stop()
        if self.wet_bulb_collection_scheduler:
            self.wet_bulb_collection_scheduler.stop()
        if self.updater_service:
            self.updater_service.stop()
        if self.alert_log_uploader:
            self.alert_log_uploader.stop()
        if self.shared_bridge_service:
            self.shared_bridge_service.stop()
        self.scheduler = self._build_scheduler()
        self.handover_scheduler_manager = self._build_handover_scheduler_manager()
        self.wet_bulb_collection_scheduler = self._build_wet_bulb_collection_scheduler()
        self.updater_service = self._build_updater_service()
        self.shared_bridge_service = self._build_shared_bridge_service()
        paths_cfg = self.runtime_config.get("paths", {}) if isinstance(self.runtime_config, dict) else {}
        runtime_state_root = str(paths_cfg.get("runtime_state_root", "") or "").strip() if isinstance(paths_cfg, dict) else ""
        self.alert_log_uploader = SystemAlertLogUploadService(
            config_getter=lambda: self.config,
            active_job_id_getter=self.job_service.active_job_id,
            emit_log=lambda text: self.add_system_log(
                text,
                source="uploader",
                suppress_alert_upload=True,
            ),
            runtime_state_root=runtime_state_root,
            mark_uploaded=self.mark_system_log_entries_uploaded,
        )
        if self.scheduler_callback:
            self.scheduler.run_callback = self.scheduler_callback
        if self.handover_scheduler_callback:
            self.handover_scheduler_manager.set_run_callback(self.handover_scheduler_callback)
        if self.wet_bulb_collection_scheduler_callback:
            self.wet_bulb_collection_scheduler.run_callback = self.wet_bulb_collection_scheduler_callback
        if self.updater_restart_callback:
            self.updater_service.restart_callback = self.updater_restart_callback
        self.job_service.set_global_log_sink(
            lambda line: self.add_system_log(
                line,
                source="job",
                write_console=False,
            )
        )

        auto_start = bool(self.runtime_config.get("scheduler", {}).get("auto_start_in_gui", False))
        handover_cfg = self.runtime_config.get("handover_log", {})
        if not isinstance(handover_cfg, dict):
            handover_cfg = {}
        handover_scheduler_cfg = handover_cfg.get("scheduler", {})
        if not isinstance(handover_scheduler_cfg, dict):
            handover_scheduler_cfg = {}
        handover_auto_start = bool(handover_scheduler_cfg.get("auto_start_in_gui", False))
        wet_bulb_cfg = self.runtime_config.get("wet_bulb_collection", {})
        if not isinstance(wet_bulb_cfg, dict):
            wet_bulb_cfg = {}
        wet_bulb_scheduler_cfg = wet_bulb_cfg.get("scheduler", {})
        if not isinstance(wet_bulb_scheduler_cfg, dict):
            wet_bulb_scheduler_cfg = {}
        wet_bulb_auto_start = bool(wet_bulb_scheduler_cfg.get("auto_start_in_gui", False))
        if was_running or auto_start:
            self.scheduler.start()
        if was_handover_running or handover_auto_start:
            self.handover_scheduler_manager.start()
        if was_wet_bulb_running or wet_bulb_auto_start:
            self.wet_bulb_collection_scheduler.start()
        updater_cfg = self.runtime_config.get("updater", {})
        if not isinstance(updater_cfg, dict):
            updater_cfg = {}
        if self.runtime_services_armed and bool(updater_cfg.get("enabled", True)):
            self.updater_service.start()
        if self.runtime_services_armed and was_alert_log_uploader_running and self.alert_log_uploader:
            self.alert_log_uploader.start()
        new_role_mode = str(self.shared_bridge_service.get_deployment_snapshot().get("role_mode", "") or "").strip().lower()
        if self.runtime_services_armed and was_shared_bridge_running and previous_role_mode == new_role_mode:
            self.shared_bridge_service.start()
        elif previous_role_mode != new_role_mode:
            self.add_system_log(f"[共享桥接] 角色模式已变更: {previous_role_mode} -> {new_role_mode}，需重启后完全生效")


def _resolve_frontend_dist() -> Path:
    candidates = [
        get_app_dir() / "web" / "frontend" / "dist",
        get_app_dir() / "web_frontend" / "dist",
        get_bundle_dir() / "web" / "frontend" / "dist",
        get_bundle_dir() / "web_frontend" / "dist",
        Path(__file__).resolve().parent.parent.parent / "web" / "frontend" / "dist",
        Path(__file__).resolve().parent.parent.parent / "web_frontend" / "dist",
    ]
    for path in candidates:
        if (path / "index.html").exists():
            return path
    raise FileNotFoundError(f"找不到前端dist目录: {candidates}")


def _resolve_frontend_source() -> Path:
    candidates = [
        get_app_dir() / "web" / "frontend" / "src",
        Path(__file__).resolve().parent.parent.parent / "web" / "frontend" / "src",
    ]
    for path in candidates:
        if (path / "index.html").exists():
            return path
    raise FileNotFoundError(f"找不到前端src目录: {candidates}")


def _assert_runtime_assets(mode: str, root: Path, assets_dir: Path) -> None:
    missing: List[str] = []
    if not (root / "index.html").exists():
        missing.append(str(root / "index.html"))
    for name in ("app.js", "style.css", "vue.global.prod.js"):
        if not (assets_dir / name).exists():
            missing.append(str(assets_dir / name))
    if missing:
        raise FileNotFoundError(
            f"前端资源不完整(mode={mode})，缺少: {missing}. "
            f"当前 root={root}, assets={assets_dir}"
        )


def _resolve_frontend_runtime() -> tuple[str, Path, Path]:
    if getattr(sys, "frozen", False):
        root = _resolve_frontend_dist()
        assets_dir = root / "assets"
        _assert_runtime_assets("dist", root, assets_dir)
        return "dist", root, assets_dir

    root = _resolve_frontend_source()
    assets_dir = root
    _assert_runtime_assets("source", root, assets_dir)
    return "source", root, assets_dir


def build_container() -> AppContainer:
    _, cfg_path = load_masked_settings()
    raw_cfg = load_bootstrap_settings(cfg_path)
    runtime_cfg = adapt_runtime_config(raw_cfg)
    frontend_mode, frontend_root, frontend_assets_dir = _resolve_frontend_runtime()
    console_cfg = raw_cfg.get("common", {}).get("console", {}) if isinstance(raw_cfg, dict) else {}
    job_service = JobService(log_buffer_size=int(console_cfg.get("log_buffer_size", 5000)))

    container = AppContainer(
        config=raw_cfg,
        runtime_config=runtime_cfg,
        config_path=cfg_path,
        frontend_mode=frontend_mode,
        frontend_root=frontend_root,
        frontend_assets_dir=frontend_assets_dir,
        job_service=job_service,
    )
    container.add_system_log(
        f"Web控制台已就绪, 前端模式={container.frontend_mode}, "
        f"root={container.frontend_root}, assets={container.frontend_assets_dir}"
    )
    return container
