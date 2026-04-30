from __future__ import annotations

import copy
import json
import re
import sys
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.config.config_adapter import adapt_runtime_config, normalize_role_mode, resolve_shared_bridge_paths
from app.config.secret_masking import load_masked_settings
from app.config.settings_loader import load_bootstrap_settings, save_settings
from app.modules.network.service.wifi_switch_service import WifiSwitchService
from app.modules.report_pipeline.service.job_service import JobService
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService
from app.modules.shared_bridge.service.bridge_status_presenter import (
    apply_external_source_cache_backfill_overlays,
)
from app.modules.report_pipeline.service.system_alert_log_upload_service import (
    SystemAlertLogUploadService,
)
from app.modules.scheduler.service.daily_scheduler_service import DailyAutoSchedulerService
from app.modules.scheduler.service.handover_scheduler_manager import HandoverSchedulerManager
from app.modules.scheduler.service.interval_scheduler_service import IntervalSchedulerService
from app.modules.scheduler.service.monthly_scheduler_service import MonthlySchedulerService
from app.modules.updater.service.updater_service import UpdaterService
from app.shared.utils.atomic_file import atomic_write_text, validate_json_file
from app.shared.utils.file_utils import (
    canonicalize_windows_path_for_compare,
    normalize_windows_path_text,
    resolve_windows_network_path,
    windows_paths_point_to_same_location,
)
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from pipeline_utils import get_app_dir, get_bundle_dir


APP_VERSION = "web-3.0.0"
_STARTUP_ROLE_HANDOFF_FILE_NAME = "startup_role_handoff.json"
_STARTUP_ROLE_HANDOFF_TTL = timedelta(minutes=5)
_EXTERNAL_SCHEDULER_AUTOSTART_FILE_NAME = "external_scheduler_autostart_state.json"
_EXTERNAL_SCHEDULER_AUTOSTART_ITEMS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("auto_flow", "每日用电明细自动流程", ("common", "scheduler")),
    ("handover", "交接班调度", ("features", "handover_log", "scheduler")),
    ("wet_bulb_collection", "湿球温度定时采集", ("features", "wet_bulb_collection", "scheduler")),
    ("day_metric_upload", "12项独立上传", ("features", "day_metric_upload", "scheduler")),
    ("alarm_event_upload", "告警信息上传", ("features", "alarm_export", "scheduler")),
    ("monthly_change_report", "月度变更统计表", ("features", "handover_log", "monthly_change_report", "scheduler")),
    ("monthly_event_report", "月度事件统计表", ("features", "handover_log", "monthly_event_report", "scheduler")),
)
_EXTERNAL_SCHEDULER_AUTOSTART_PATHS = {
    key: path for key, _label, path in _EXTERNAL_SCHEDULER_AUTOSTART_ITEMS
}
_EXTERNAL_SCHEDULER_OBJECT_ATTRS = {
    "auto_flow": "scheduler",
    "handover": "handover_scheduler_manager",
    "wet_bulb_collection": "wet_bulb_collection_scheduler",
    "day_metric_upload": "day_metric_upload_scheduler",
    "alarm_event_upload": "alarm_event_upload_scheduler",
    "monthly_change_report": "monthly_change_report_scheduler",
    "monthly_event_report": "monthly_event_report_scheduler",
}
_EXTERNAL_SCHEDULER_LEGACY_EXIT_SOURCE_HINTS = (
    "退出快照",
    "退出当前系统",
    "用户退出当前系统",
)
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
    scheduler: IntervalSchedulerService | None = None
    scheduler_callback: Callable[[str], tuple[bool, str]] | None = None
    handover_scheduler_manager: HandoverSchedulerManager | None = None
    handover_scheduler_callback: Callable[[str, str], tuple[bool, str]] | None = None
    wet_bulb_collection_scheduler: IntervalSchedulerService | None = None
    wet_bulb_collection_scheduler_callback: Callable[[str], tuple[bool, str]] | None = None
    day_metric_upload_scheduler: IntervalSchedulerService | None = None
    day_metric_upload_scheduler_callback: Callable[[str], tuple[bool, str]] | None = None
    alarm_event_upload_scheduler: DailyAutoSchedulerService | None = None
    alarm_event_upload_scheduler_callback: Callable[[str], tuple[bool, str]] | None = None
    monthly_change_report_scheduler: MonthlySchedulerService | None = None
    monthly_change_report_scheduler_callback: Callable[[str], tuple[bool, str]] | None = None
    monthly_event_report_scheduler: MonthlySchedulerService | None = None
    monthly_event_report_scheduler_callback: Callable[[str], tuple[bool, str]] | None = None
    updater_service: UpdaterService | None = None
    updater_restart_callback: Callable[[Dict[str, Any]], tuple[bool, str]] | None = None
    alert_log_uploader: SystemAlertLogUploadService | None = None
    shared_bridge_service: SharedBridgeRuntimeService | None = None
    runtime_status_coordinator: Any | None = None
    system_logs: List[str] = field(default_factory=list)
    system_log_entries: List[Dict[str, Any]] = field(default_factory=list)
    version: str = APP_VERSION
    runtime_services_armed: bool = False
    external_scheduler_autostart_runtime_state: Dict[str, Any] = field(default_factory=dict)
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
        last_started_role_mode = str(deployment.get("last_started_role_mode", "") or "").strip().lower()
        if last_started_role_mode not in {"internal", "external"}:
            last_started_role_mode = ""
        node_id = str(deployment.get("node_id", "") or "").strip()
        node_label = str(deployment.get("node_label", "") or "").strip()
        if not node_label and role_mode == "internal":
            node_label = "内网端"
        if not node_label and role_mode == "external":
            node_label = "外网端"
        return {
            "role_mode": role_mode,
            "last_started_role_mode": last_started_role_mode,
            "node_id": node_id,
            "node_label": node_label,
        }

    def _shared_bridge_runtime_deployment_snapshot(self) -> Dict[str, Any]:
        service = self.shared_bridge_service
        if service is None:
            return {}
        try:
            snapshot = service.get_deployment_snapshot()
        except Exception:  # noqa: BLE001
            return {}
        return snapshot if isinstance(snapshot, dict) else {}

    def _shared_bridge_runtime_role_mode(self) -> str:
        snapshot = self._shared_bridge_runtime_deployment_snapshot()
        return normalize_role_mode(snapshot.get("role_mode", "") if isinstance(snapshot, dict) else "")

    def _ensure_shared_bridge_service_matches_configured_role(self) -> None:
        configured_role_mode = normalize_role_mode(self._configured_deployment_snapshot().get("role_mode", ""))
        if configured_role_mode not in {"internal", "external"}:
            return
        if self.shared_bridge_service is None:
            self.shared_bridge_service = self._build_shared_bridge_service()
            return
        runtime_role_mode = self._shared_bridge_runtime_role_mode()
        if runtime_role_mode == configured_role_mode:
            return
        try:
            self.shared_bridge_service.stop()
        except Exception:  # noqa: BLE001
            pass
        self.shared_bridge_service = self._build_shared_bridge_service()

    def _ensure_runtime_dependencies_initialized(self) -> None:
        progress_callback = getattr(self, "runtime_activation_progress_callback", None)
        role_mode = normalize_role_mode(self._configured_deployment_snapshot().get("role_mode", ""))

        def _report_progress(step: str) -> None:
            if callable(progress_callback):
                try:
                    progress_callback(str(step or "").strip())
                except Exception:  # noqa: BLE001
                    pass

        _report_progress("initializing_wifi_service")
        if not self.wifi_service:
            self.wifi_service = WifiSwitchService(self.runtime_config)
        _report_progress("configuring_job_service")
        self.job_service.configure_task_engine(
            runtime_config=self.runtime_config,
            app_dir=get_app_dir(),
            config_snapshot_getter=lambda: self.runtime_config,
            current_ssid_getter=lambda: self.wifi_service.current_ssid() if self.wifi_service else "",
        )
        _report_progress("binding_job_log_sink")
        self.job_service.set_global_log_sink(
            lambda line: self.add_system_log(
                line,
                source="job",
                write_console=False,
            )
        )
        if role_mode == "internal":
            if not self.shared_bridge_service:
                _report_progress("building_shared_bridge_service")
                self.shared_bridge_service = self._build_shared_bridge_service()
            _report_progress("runtime_dependencies_initialized")
            return
        if not self.scheduler:
            _report_progress("building_monthly_scheduler")
            self.scheduler = self._build_scheduler()
        if not self.handover_scheduler_manager:
            _report_progress("building_handover_scheduler_manager")
            self.handover_scheduler_manager = self._build_handover_scheduler_manager()
        if not self.wet_bulb_collection_scheduler:
            _report_progress("building_wet_bulb_scheduler")
            self.wet_bulb_collection_scheduler = self._build_wet_bulb_collection_scheduler()
        if not self.day_metric_upload_scheduler:
            _report_progress("building_day_metric_scheduler")
            self.day_metric_upload_scheduler = self._build_day_metric_upload_scheduler()
        if not self.alarm_event_upload_scheduler:
            _report_progress("building_alarm_scheduler")
            self.alarm_event_upload_scheduler = self._build_alarm_event_upload_scheduler()
        if not self.monthly_change_report_scheduler:
            _report_progress("building_monthly_change_scheduler")
            self.monthly_change_report_scheduler = self._build_monthly_change_report_scheduler()
        if not self.monthly_event_report_scheduler:
            _report_progress("building_monthly_event_scheduler")
            self.monthly_event_report_scheduler = self._build_monthly_event_report_scheduler()
        if not self.updater_service:
            _report_progress("building_updater_service")
            self.updater_service = self._build_updater_service()
        if not self.shared_bridge_service:
            _report_progress("building_shared_bridge_service")
            self.shared_bridge_service = self._build_shared_bridge_service()
        if not self.alert_log_uploader:
            _report_progress("building_alert_log_uploader")
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
            _report_progress("binding_monthly_scheduler_callback")
            self.scheduler.run_callback = self.scheduler_callback
        if self.handover_scheduler_callback and self.handover_scheduler_manager:
            _report_progress("binding_handover_scheduler_callback")
            self.handover_scheduler_manager.set_run_callback(self.handover_scheduler_callback)
        if self.wet_bulb_collection_scheduler_callback and self.wet_bulb_collection_scheduler:
            _report_progress("binding_wet_bulb_scheduler_callback")
            self.wet_bulb_collection_scheduler.run_callback = self.wet_bulb_collection_scheduler_callback
        if self.day_metric_upload_scheduler_callback and self.day_metric_upload_scheduler:
            _report_progress("binding_day_metric_scheduler_callback")
            self.day_metric_upload_scheduler.run_callback = self.day_metric_upload_scheduler_callback
        if self.alarm_event_upload_scheduler_callback and self.alarm_event_upload_scheduler:
            _report_progress("binding_alarm_scheduler_callback")
            self.alarm_event_upload_scheduler.run_callback = self.alarm_event_upload_scheduler_callback
        if self.monthly_change_report_scheduler_callback and self.monthly_change_report_scheduler:
            _report_progress("binding_monthly_change_scheduler_callback")
            self.monthly_change_report_scheduler.run_callback = self.monthly_change_report_scheduler_callback
        if self.monthly_event_report_scheduler_callback and self.monthly_event_report_scheduler:
            _report_progress("binding_monthly_event_scheduler_callback")
            self.monthly_event_report_scheduler.run_callback = self.monthly_event_report_scheduler_callback
        if self.updater_restart_callback and self.updater_service:
            _report_progress("binding_updater_restart_callback")
            self.updater_service.restart_callback = self.updater_restart_callback
        _report_progress("runtime_dependencies_initialized")

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
        coordinator = getattr(self, "runtime_status_coordinator", None)
        if coordinator is not None:
            try:
                coordinator.observe_log_line(raw_text)
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

    def _build_scheduler(self) -> IntervalSchedulerService:
        scheduler_cfg = self.runtime_config.get("scheduler", {})
        if not isinstance(scheduler_cfg, dict):
            scheduler_cfg = {}
        paths_cfg = self.runtime_config.get("paths", {})
        if not isinstance(paths_cfg, dict):
            paths_cfg = {}
        runtime_state_root = str(paths_cfg.get("runtime_state_root", "") or "").strip()
        return IntervalSchedulerService(
            scheduler_cfg=scheduler_cfg,
            runtime_state_root=runtime_state_root or "runtime_state",
            emit_log=self.add_system_log,
            run_callback=self.scheduler_callback or self._scheduler_run_callback,
            is_busy=lambda: False,
            thread_name="daily-auto-flow-interval-scheduler",
            source_name="每日用电明细自动流程",
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

    def _build_day_metric_upload_scheduler(self) -> IntervalSchedulerService:
        day_metric_cfg = self.runtime_config.get("day_metric_upload", {})
        if not isinstance(day_metric_cfg, dict):
            day_metric_cfg = {}
        paths_cfg = self.runtime_config.get("paths", {})
        if not isinstance(paths_cfg, dict):
            paths_cfg = {}
        runtime_state_root = str(paths_cfg.get("runtime_state_root", "") or "").strip()
        scheduler_cfg = day_metric_cfg.get("scheduler", {})
        if not isinstance(scheduler_cfg, dict):
            scheduler_cfg = {}
        return IntervalSchedulerService(
            scheduler_cfg=scheduler_cfg,
            runtime_state_root=runtime_state_root or "runtime_state",
            emit_log=self.add_system_log,
            run_callback=self.day_metric_upload_scheduler_callback or self._day_metric_upload_scheduler_run_callback,
            is_busy=self.job_service.has_incomplete_jobs,
            thread_name="day-metric-upload-interval-scheduler",
            source_name="12项独立上传",
        )

    def _build_alarm_event_upload_scheduler(self) -> DailyAutoSchedulerService:
        alarm_cfg = self.runtime_config.get("alarm_export", {})
        if not isinstance(alarm_cfg, dict):
            alarm_cfg = {}
        paths_cfg = self.runtime_config.get("paths", {})
        if not isinstance(paths_cfg, dict):
            paths_cfg = {}
        return DailyAutoSchedulerService(
            config={
                "scheduler": alarm_cfg.get("scheduler", {}),
                "paths": paths_cfg,
            },
            emit_log=self.add_system_log,
            run_callback=self.alarm_event_upload_scheduler_callback or self._alarm_event_upload_scheduler_run_callback,
            is_busy=self.job_service.has_incomplete_jobs,
        )

    def _build_monthly_change_report_scheduler(self) -> MonthlySchedulerService:
        handover_cfg = self.runtime_config.get("handover_log", {})
        if not isinstance(handover_cfg, dict):
            handover_cfg = {}
        monthly_cfg = handover_cfg.get("monthly_change_report", {})
        if not isinstance(monthly_cfg, dict):
            monthly_cfg = {}
        scheduler_cfg = monthly_cfg.get("scheduler", {})
        if not isinstance(scheduler_cfg, dict):
            scheduler_cfg = {}
        paths_cfg = self.runtime_config.get("paths", {}) if isinstance(self.runtime_config, dict) else {}
        runtime_state_root = str(paths_cfg.get("runtime_state_root", "") or "").strip() if isinstance(paths_cfg, dict) else ""
        return MonthlySchedulerService(
            scheduler_cfg=scheduler_cfg,
            runtime_state_root=runtime_state_root,
            emit_log=self.add_system_log,
            run_callback=self.monthly_change_report_scheduler_callback or self._monthly_change_report_scheduler_run_callback,
            is_busy=self.job_service.has_incomplete_jobs,
            thread_name="monthly-change-report-scheduler",
            source_name="月度变更统计表处理",
        )

    def _build_monthly_event_report_scheduler(self) -> MonthlySchedulerService:
        handover_cfg = self.runtime_config.get("handover_log", {})
        if not isinstance(handover_cfg, dict):
            handover_cfg = {}
        monthly_cfg = handover_cfg.get("monthly_event_report", {})
        if not isinstance(monthly_cfg, dict):
            monthly_cfg = {}
        scheduler_cfg = monthly_cfg.get("scheduler", {})
        if not isinstance(scheduler_cfg, dict):
            scheduler_cfg = {}
        paths_cfg = self.runtime_config.get("paths", {}) if isinstance(self.runtime_config, dict) else {}
        runtime_state_root = str(paths_cfg.get("runtime_state_root", "") or "").strip() if isinstance(paths_cfg, dict) else ""
        return MonthlySchedulerService(
            scheduler_cfg=scheduler_cfg,
            runtime_state_root=runtime_state_root,
            emit_log=self.add_system_log,
            run_callback=self.monthly_event_report_scheduler_callback or self._monthly_event_report_scheduler_run_callback,
            is_busy=self.job_service.has_incomplete_jobs,
            thread_name="monthly-event-report-scheduler",
            source_name="月度事件统计表处理",
        )

    def _build_updater_service(self) -> UpdaterService:
        return UpdaterService(
            config=self.runtime_config,
            emit_log=self.add_system_log,
            restart_callback=self.updater_restart_callback,
            is_busy=self.job_service.has_running_jobs,
        )

    def _build_shared_bridge_service(self) -> SharedBridgeRuntimeService:
        return SharedBridgeRuntimeService(
            runtime_config=self.runtime_config,
            app_version=self.version,
            job_service=self.job_service,
            emit_log=self.add_system_log,
            request_runtime_status_refresh=lambda reason: (
                getattr(self, "runtime_status_coordinator", None).request_refresh(reason=str(reason or "").strip() or "shared_bridge_runtime")
                if getattr(self, "runtime_status_coordinator", None) is not None
                and callable(getattr(getattr(self, "runtime_status_coordinator", None), "request_refresh", None))
                else None
            ),
        )

    def _scheduler_run_callback(self, source: str) -> tuple[bool, str]:
        return False, "调度回调尚未绑定执行器"

    def _handover_scheduler_run_callback(self, slot: str, source: str) -> tuple[bool, str]:
        return False, f"交接班调度回调尚未绑定执行器(slot={slot}, source={source})"

    def _wet_bulb_collection_scheduler_run_callback(self, source: str) -> tuple[bool, str]:
        return False, f"湿球温度定时采集调度回调尚未绑定执行器(source={source})"

    def _day_metric_upload_scheduler_run_callback(self, source: str) -> tuple[bool, str]:
        return False, f"12项独立上传调度回调尚未绑定执行器(source={source})"

    def _alarm_event_upload_scheduler_run_callback(self, source: str) -> tuple[bool, str]:
        return False, f"告警信息上传调度回调尚未绑定执行器(source={source})"

    def _monthly_change_report_scheduler_run_callback(self, source: str) -> tuple[bool, str]:
        return False, f"月度变更统计表调度回调尚未绑定执行器(source={source})"

    def _monthly_event_report_scheduler_run_callback(self, source: str) -> tuple[bool, str]:
        return False, f"月度事件统计表调度回调尚未绑定执行器(source={source})"

    def _is_placeholder_callback(self, callback: Any, placeholder: Any | None = None) -> bool:
        if callback is None:
            return True
        placeholder = placeholder or self._scheduler_run_callback
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

    def is_day_metric_upload_scheduler_executor_bound(self) -> bool:
        callback = None
        if self.day_metric_upload_scheduler:
            callback = getattr(self.day_metric_upload_scheduler, "run_callback", None)
        if callback is None:
            callback = self.day_metric_upload_scheduler_callback
        return not self._is_placeholder_callback(callback, self._day_metric_upload_scheduler_run_callback)

    def day_metric_upload_scheduler_executor_name(self) -> str:
        callback = None
        if self.day_metric_upload_scheduler:
            callback = getattr(self.day_metric_upload_scheduler, "run_callback", None)
        if callback is None:
            callback = self.day_metric_upload_scheduler_callback
        if callback is None:
            callback = self._day_metric_upload_scheduler_run_callback
        name = getattr(callback, "__name__", "")
        if not name:
            name = getattr(getattr(callback, "__func__", None), "__name__", "")
        return str(name or "-")

    def is_alarm_event_upload_scheduler_executor_bound(self) -> bool:
        callback = None
        if self.alarm_event_upload_scheduler:
            callback = getattr(self.alarm_event_upload_scheduler, "run_callback", None)
        if callback is None:
            callback = self.alarm_event_upload_scheduler_callback
        return not self._is_placeholder_callback(callback, self._alarm_event_upload_scheduler_run_callback)

    def alarm_event_upload_scheduler_executor_name(self) -> str:
        callback = None
        if self.alarm_event_upload_scheduler:
            callback = getattr(self.alarm_event_upload_scheduler, "run_callback", None)
        if callback is None:
            callback = self.alarm_event_upload_scheduler_callback
        if callback is None:
            callback = self._alarm_event_upload_scheduler_run_callback
        name = getattr(callback, "__name__", "")
        if not name:
            name = getattr(getattr(callback, "__func__", None), "__name__", "")
        return str(name or "-")

    def is_monthly_change_report_scheduler_executor_bound(self) -> bool:
        callback = self.monthly_change_report_scheduler_callback
        return callable(callback)

    def monthly_change_report_scheduler_executor_name(self) -> str:
        callback = self.monthly_change_report_scheduler_callback
        if callback is None:
            return "-"
        name = getattr(callback, "__name__", "")
        if not name:
            name = getattr(getattr(callback, "__func__", None), "__name__", "")
        return str(name or "-")

    def is_monthly_event_report_scheduler_executor_bound(self) -> bool:
        callback = self.monthly_event_report_scheduler_callback
        return callable(callback)

    def monthly_event_report_scheduler_executor_name(self) -> str:
        callback = self.monthly_event_report_scheduler_callback
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
        if text in {"disabled_or_switching", "disabled_or_unselected"}:
            return "当前未启用共享桥接"
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

    def set_day_metric_upload_scheduler_callback(self, callback: Callable[[str], tuple[bool, str]]) -> None:
        self.day_metric_upload_scheduler_callback = callback
        if self.day_metric_upload_scheduler:
            self.day_metric_upload_scheduler.run_callback = callback

    def set_alarm_event_upload_scheduler_callback(self, callback: Callable[[str], tuple[bool, str]]) -> None:
        self.alarm_event_upload_scheduler_callback = callback
        if self.alarm_event_upload_scheduler:
            self.alarm_event_upload_scheduler.run_callback = callback

    def set_monthly_change_report_scheduler_callback(self, callback: Callable[[str], tuple[bool, str]]) -> None:
        self.monthly_change_report_scheduler_callback = callback
        if self.monthly_change_report_scheduler:
            self.monthly_change_report_scheduler.run_callback = callback

    def set_monthly_event_report_scheduler_callback(self, callback: Callable[[str], tuple[bool, str]]) -> None:
        self.monthly_event_report_scheduler_callback = callback
        if self.monthly_event_report_scheduler:
            self.monthly_event_report_scheduler.run_callback = callback

    def set_updater_restart_callback(self, callback: Callable[[Dict[str, Any]], tuple[bool, str]]) -> None:
        self.updater_restart_callback = callback
        if self.updater_service:
            self.updater_service.restart_callback = callback

    def request_app_restart(self, context: Dict[str, Any] | None = None) -> tuple[bool, str]:
        callback = self.updater_restart_callback
        if callback is None:
            return False, "当前未绑定程序重启回调"
        return callback(dict(context or {}))

    def _startup_role_handoff_path(self) -> Path:
        runtime_state_root = resolve_runtime_state_root(
            runtime_config=self.runtime_config,
            app_dir=get_app_dir(),
        )
        return runtime_state_root / _STARTUP_ROLE_HANDOFF_FILE_NAME

    def _external_scheduler_autostart_path(self) -> Path:
        runtime_state_root = resolve_runtime_state_root(
            runtime_config=self.runtime_config,
            app_dir=get_app_dir(),
        )
        return runtime_state_root / _EXTERNAL_SCHEDULER_AUTOSTART_FILE_NAME

    @staticmethod
    def _dict_path(root: Dict[str, Any], path: tuple[str, ...]) -> Dict[str, Any]:
        current: Any = root
        for key in path:
            if not isinstance(current, dict):
                return {}
            current = current.get(key)
        return current if isinstance(current, dict) else {}

    def _external_scheduler_autostart_snapshot(self, config: Dict[str, Any] | None = None) -> Dict[str, bool]:
        source = config if isinstance(config, dict) else self.config
        states: Dict[str, bool] = {}
        for key, _label, path in _EXTERNAL_SCHEDULER_AUTOSTART_ITEMS:
            cfg = self._dict_path(source, path)
            states[key] = bool(cfg.get("auto_start_in_gui", False)) if isinstance(cfg, dict) else False
        return states

    def _normalize_external_scheduler_states(self, states: Dict[str, Any] | None = None) -> Dict[str, bool]:
        raw = states if isinstance(states, dict) else {}
        normalized: Dict[str, bool] = {}
        for key, _label, _path in _EXTERNAL_SCHEDULER_AUTOSTART_ITEMS:
            normalized[key] = bool(raw.get(key, False))
        return normalized

    @staticmethod
    def _is_external_scheduler_legacy_exit_source(source: str) -> bool:
        text = str(source or "").strip().lower()
        if not text:
            return False
        return any(hint.lower() in text for hint in _EXTERNAL_SCHEDULER_LEGACY_EXIT_SOURCE_HINTS)

    def _write_external_scheduler_autostart_state(self, states: Dict[str, bool], *, source: str = "") -> Dict[str, Any]:
        normalized_states = self._normalize_external_scheduler_states(states)
        payload = {
            "version": 1,
            "role_mode": "external",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": str(source or "").strip(),
            "states": normalized_states,
        }
        atomic_write_text(
            self._external_scheduler_autostart_path(),
            json.dumps(payload, ensure_ascii=False, indent=2),
            validator=validate_json_file,
        )
        return payload

    def _load_external_scheduler_autostart_payload(self) -> Dict[str, Any]:
        path = self._external_scheduler_autostart_path()
        if not path.exists():
            return {}
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            self.add_system_log(f"[调度] 读取外网端调度记忆失败，已忽略: {exc}")
            return {}
        if not isinstance(raw, dict):
            return {}
        return {
            "source": str(raw.get("source", "") or "").strip(),
            "updated_at": str(raw.get("updated_at", "") or "").strip(),
            "states": self._normalize_external_scheduler_states(raw.get("states")),
        }

    def resolve_external_scheduler_autostart_state(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        cached = self.external_scheduler_autostart_runtime_state
        if not force_refresh and isinstance(cached, dict) and isinstance(cached.get("states"), dict):
            return copy.deepcopy(cached)
        config_snapshot = self._external_scheduler_autostart_snapshot()
        payload = self._load_external_scheduler_autostart_payload()
        loaded_states = self._normalize_external_scheduler_states(payload.get("states"))
        memory_source = str(payload.get("source", "") or "").strip()
        changed = False
        if not payload:
            loaded_states = dict(config_snapshot)
            memory_source = "config_fallback"
            changed = True
        elif self._is_external_scheduler_legacy_exit_source(memory_source):
            all_disabled = all(not bool(loaded_states.get(key, False)) for key in loaded_states)
            any_config_enabled = any(bool(config_snapshot.get(key, False)) for key in config_snapshot)
            if all_disabled and any_config_enabled:
                loaded_states = dict(config_snapshot)
                memory_source = "legacy_repair"
                changed = True
        if changed:
            try:
                self._write_external_scheduler_autostart_state(loaded_states, source=memory_source)
            except Exception as exc:  # noqa: BLE001
                self.add_system_log(f"[调度] 更新外网端调度记忆失败，不阻断主流程: {exc}")
        result = {
            "ok": True,
            "states": self._normalize_external_scheduler_states(loaded_states),
            "memory_source": memory_source or "config_fallback",
            "changed": changed,
        }
        self.external_scheduler_autostart_runtime_state = copy.deepcopy(result)
        return result

    def persist_external_scheduler_autostart_state(
        self,
        source: str = "调度状态保存",
        states: Dict[str, bool] | None = None,
    ) -> Dict[str, Any]:
        try:
            snapshot = self._normalize_external_scheduler_states(
                states if isinstance(states, dict) else self._external_scheduler_autostart_snapshot()
            )
            payload = self._write_external_scheduler_autostart_state(snapshot, source=source)
            state = {
                "ok": True,
                "states": dict(payload.get("states", {})),
                "memory_source": str(payload.get("source", "") or "").strip() or "config_fallback",
                "changed": True,
            }
            self.external_scheduler_autostart_runtime_state = copy.deepcopy(state)
            return {"ok": True, "state": payload, **state}
        except Exception as exc:  # noqa: BLE001
            self.add_system_log(f"[调度] 保存外网端调度记忆失败，不阻断主流程: {exc}")
            return {"ok": False, "error": str(exc)}

    def persist_external_scheduler_autostart_state_on_exit(self, source: str = "退出当前系统") -> Dict[str, Any]:
        resolved = self.resolve_external_scheduler_autostart_state(force_refresh=True)
        return {
            "ok": True,
            "skipped": True,
            "reason": "exit_does_not_override_memory",
            "source": source,
            "states": resolved.get("states", {}),
            "memory_source": resolved.get("memory_source", ""),
        }

    def record_external_scheduler_toggle(
        self,
        *,
        path: tuple[str, ...],
        auto_start_in_gui: bool,
        source: str = "调度开关",
    ) -> Dict[str, Any]:
        try:
            normalized_path = tuple(str(item) for item in path)
            target_key = ""
            for key, _label, item_path in _EXTERNAL_SCHEDULER_AUTOSTART_ITEMS:
                if item_path == normalized_path:
                    target_key = key
                    break
            if not target_key:
                return {"ok": True, "ignored": True, "reason": "path_not_mapped"}
            resolved = self.resolve_external_scheduler_autostart_state(force_refresh=True)
            states = self._normalize_external_scheduler_states(resolved.get("states"))
            states[target_key] = bool(auto_start_in_gui)
            return self.persist_external_scheduler_autostart_state(source=source, states=states)
        except Exception as exc:  # noqa: BLE001
            self.add_system_log(f"[调度] 记录外网端调度开关失败，不阻断主流程: {exc}")
            return {"ok": False, "error": str(exc)}

    def _effective_external_scheduler_auto_start(self, key: str, fallback: bool = False) -> bool:
        path = _EXTERNAL_SCHEDULER_AUTOSTART_PATHS.get(str(key or "").strip())
        if not path:
            return bool(fallback)
        cfg = self._dict_path(self.runtime_config, path)
        if isinstance(cfg, dict) and "auto_start_in_gui" in cfg:
            return bool(cfg.get("auto_start_in_gui", False))
        return bool(fallback)

    def external_scheduler_runtime_memory_fields(self, key: str) -> Dict[str, Any]:
        resolved = self.resolve_external_scheduler_autostart_state()
        states = self._normalize_external_scheduler_states(resolved.get("states"))
        remembered = bool(states.get(str(key or "").strip(), False))
        return {
            "remembered_enabled": remembered,
            "effective_auto_start_in_gui": self._effective_external_scheduler_auto_start(
                str(key or "").strip(),
                remembered,
            ),
            "memory_source": str(resolved.get("memory_source", "") or "config_fallback"),
        }

    def _apply_scheduler_memory_to_runtime_object(self, key: str, desired: bool) -> bool:
        attr_name = _EXTERNAL_SCHEDULER_OBJECT_ATTRS.get(str(key or "").strip())
        if not attr_name:
            return False
        scheduler_obj = getattr(self, attr_name, None)
        if scheduler_obj is None:
            return False
        changed = False
        if getattr(scheduler_obj, "auto_start_in_gui", None) is not desired:
            setattr(scheduler_obj, "auto_start_in_gui", desired)
            changed = True
        if isinstance(getattr(scheduler_obj, "cfg", None), dict) and scheduler_obj.cfg.get("auto_start_in_gui") is not desired:
            scheduler_obj.cfg["auto_start_in_gui"] = desired
            changed = True
        if isinstance(getattr(scheduler_obj, "_cfg", None), dict) and scheduler_obj._cfg.get("auto_start_in_gui") is not desired:
            scheduler_obj._cfg["auto_start_in_gui"] = desired
            changed = True
        if desired and getattr(scheduler_obj, "enabled", None) is not True:
            try:
                setattr(scheduler_obj, "enabled", True)
                changed = True
            except Exception:  # noqa: BLE001
                pass
        if desired and isinstance(getattr(scheduler_obj, "cfg", None), dict) and scheduler_obj.cfg.get("enabled") is not True:
            scheduler_obj.cfg["enabled"] = True
            changed = True
        if desired and isinstance(getattr(scheduler_obj, "_cfg", None), dict) and scheduler_obj._cfg.get("enabled") is not True:
            scheduler_obj._cfg["enabled"] = True
            changed = True
        child_schedulers = getattr(scheduler_obj, "schedulers", None)
        if isinstance(child_schedulers, dict):
            for child in child_schedulers.values():
                if child is None:
                    continue
                if getattr(child, "auto_start_in_gui", None) is not desired:
                    setattr(child, "auto_start_in_gui", desired)
                    changed = True
                if isinstance(getattr(child, "cfg", None), dict) and child.cfg.get("auto_start_in_gui") is not desired:
                    child.cfg["auto_start_in_gui"] = desired
                    changed = True
                if desired and getattr(child, "enabled", None) is not True:
                    try:
                        setattr(child, "enabled", True)
                        changed = True
                    except Exception:  # noqa: BLE001
                        pass
                if desired and isinstance(getattr(child, "cfg", None), dict) and child.cfg.get("enabled") is not True:
                    child.cfg["enabled"] = True
                    changed = True
        return changed

    def _apply_single_external_scheduler_memory(self, key: str, desired: bool) -> bool:
        path = _EXTERNAL_SCHEDULER_AUTOSTART_PATHS.get(str(key or "").strip())
        changed = False
        if path:
            scheduler_cfg = self._ensure_config_dict_path(self.runtime_config, path)
            if scheduler_cfg.get("auto_start_in_gui") is not desired:
                scheduler_cfg["auto_start_in_gui"] = desired
                changed = True
            if desired and scheduler_cfg.get("enabled") is not True:
                scheduler_cfg["enabled"] = True
                changed = True
        if self._apply_scheduler_memory_to_runtime_object(str(key or "").strip(), desired):
            changed = True
        return changed

    def apply_external_scheduler_autostart_state(self, source: str = "进入外网端") -> Dict[str, Any]:
        try:
            resolved = self.resolve_external_scheduler_autostart_state(force_refresh=True)
            states = self._normalize_external_scheduler_states(resolved.get("states"))
            changed_labels: list[str] = []
            for key, label, _path in _EXTERNAL_SCHEDULER_AUTOSTART_ITEMS:
                if self._apply_single_external_scheduler_memory(key, bool(states.get(key, False))):
                    changed_labels.append(label)
            result = {
                "ok": True,
                "changed": bool(changed_labels),
                "states": states,
                "changed_labels": changed_labels,
                "memory_source": str(resolved.get("memory_source", "") or "config_fallback"),
                "source": source,
            }
            self.external_scheduler_autostart_runtime_state = copy.deepcopy(result)
            if changed_labels:
                self.add_system_log(f"[调度] {source}: 已应用外网端调度记忆: {','.join(changed_labels)}")
            return result
        except Exception as exc:  # noqa: BLE001
            self.add_system_log(f"[调度] {source}: 应用外网端调度记忆失败，继续按当前运行态: {exc}")
            return {"ok": False, "error": str(exc)}

    @staticmethod
    def _inactive_startup_role_handoff() -> Dict[str, Any]:
        return {
            "active": False,
            "mode": "",
            "target_role_mode": "",
            "requested_at": "",
            "source": "",
            "reason": "",
            "source_startup_time": "",
            "nonce": "",
        }

    def clear_startup_role_handoff(self) -> None:
        path = self._startup_role_handoff_path()
        try:
            if path.exists():
                path.unlink()
        except Exception:  # noqa: BLE001
            pass

    def get_startup_role_handoff(self) -> Dict[str, Any]:
        inactive = self._inactive_startup_role_handoff()
        path = self._startup_role_handoff_path()
        if not path.exists():
            return inactive
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            self.clear_startup_role_handoff()
            return inactive
        if not isinstance(raw, dict):
            self.clear_startup_role_handoff()
            return inactive
        mode = str(raw.get("mode", "") or "").strip()
        target_role_mode = normalize_role_mode(raw.get("target_role_mode"))
        requested_at_text = str(raw.get("requested_at", "") or "").strip()
        source = str(raw.get("source", "") or "").strip()
        reason = str(raw.get("reason", "") or "").strip()
        source_startup_time = str(raw.get("source_startup_time", "") or "").strip()
        nonce = str(raw.get("nonce", "") or "").strip()
        if mode != "startup_role_resume" or target_role_mode not in {"internal", "external"} or not requested_at_text or not nonce:
            self.clear_startup_role_handoff()
            return inactive
        try:
            requested_at = datetime.strptime(requested_at_text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            self.clear_startup_role_handoff()
            return inactive
        if datetime.now() - requested_at > _STARTUP_ROLE_HANDOFF_TTL:
            self.clear_startup_role_handoff()
            return inactive
        return {
            "active": True,
            "mode": mode,
            "target_role_mode": target_role_mode,
            "requested_at": requested_at_text,
            "source": source,
            "reason": reason,
            "source_startup_time": source_startup_time,
            "nonce": nonce,
        }

    def write_startup_role_handoff(
        self,
        *,
        target_role_mode: str,
        source: str,
        reason: str = "",
        source_startup_time: str = "",
    ) -> Dict[str, Any]:
        normalized_role = normalize_role_mode(target_role_mode)
        if normalized_role not in {"internal", "external"}:
            raise ValueError("启动角色交接只支持 internal/external")
        payload = {
            "mode": "startup_role_resume",
            "target_role_mode": normalized_role,
            "requested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": str(source or "").strip(),
            "reason": str(reason or "").strip(),
            "source_startup_time": str(source_startup_time or "").strip(),
            "nonce": uuid.uuid4().hex,
        }
        atomic_write_text(
            self._startup_role_handoff_path(),
            json.dumps(payload, ensure_ascii=False, indent=2),
            validator=validate_json_file,
        )
        return self.get_startup_role_handoff()

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
        role_mode = normalize_role_mode(self._configured_deployment_snapshot().get("role_mode", ""))
        progress_callback = getattr(self, "runtime_activation_progress_callback", None)

        def _report_progress(step: str) -> None:
            if callable(progress_callback):
                try:
                    progress_callback(str(step or "").strip())
                except Exception:  # noqa: BLE001
                    pass

        _report_progress("ensuring_runtime_dependencies")
        self._ensure_runtime_dependencies_initialized()
        _report_progress("ensuring_shared_bridge_role")
        self._ensure_shared_bridge_service_matches_configured_role()
        external_scheduler_autostart_state: Dict[str, Any] = {}
        if role_mode == "internal":
            self.external_scheduler_autostart_runtime_state = {
                "ok": True,
                "states": self._normalize_external_scheduler_states({}),
                "memory_source": "internal_role_ignored",
                "changed": False,
            }
            self.runtime_services_armed = True
            _report_progress("starting_shared_bridge")
            self.start_shared_bridge(source=source)
            _report_progress("runtime_services_started")
            return {
                "ok": True,
                "armed": True,
                "role_mode": role_mode,
                "external_scheduler_autostart_state": self.external_scheduler_autostart_runtime_state,
            }
        if role_mode == "external":
            _report_progress("applying_external_scheduler_autostart_state")
            external_scheduler_autostart_state = self.apply_external_scheduler_autostart_state(source=source)
        else:
            self.external_scheduler_autostart_runtime_state = {
                "ok": True,
                "states": self._normalize_external_scheduler_states({}),
                "memory_source": "internal_role_ignored",
                "changed": False,
            }
        self.runtime_services_armed = True

        self.add_system_log(
            f"[调度] 启动阶段执行器状态: executor_bound={self.is_scheduler_executor_bound()}, "
            f"callback={self.scheduler_executor_name()}"
        )
        if role_mode == "internal":
            self.add_system_log("[调度] 当前为内网端，启动时不自动开启月报调度")
        elif self.scheduler and self.scheduler.enabled and self.scheduler.auto_start_in_gui:
            _report_progress("starting_monthly_scheduler")
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
                _report_progress("starting_handover_scheduler")
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
                _report_progress("starting_wet_bulb_scheduler")
                self.start_wet_bulb_collection_scheduler(source=source)
            else:
                self.add_system_log("[湿球温度定时采集调度] 启动时未自动开启")
        else:
            self.add_system_log("[湿球温度定时采集调度] 已禁用")

        self.add_system_log(
            f"[12项独立上传调度] 启动阶段执行器状态: executor_bound={self.is_day_metric_upload_scheduler_executor_bound()}, "
            f"callback={self.day_metric_upload_scheduler_executor_name()}"
        )
        day_metric_scheduler_status = self.day_metric_upload_scheduler_status()
        if role_mode == "internal":
            self.add_system_log("[12项独立上传调度] 当前为内网端，启动时不自动开启")
        elif bool(day_metric_scheduler_status.get("enabled", False)):
            day_metric_cfg = self.runtime_config.get("day_metric_upload", {})
            if not isinstance(day_metric_cfg, dict):
                day_metric_cfg = {}
            day_metric_scheduler_cfg = day_metric_cfg.get("scheduler", {})
            if not isinstance(day_metric_scheduler_cfg, dict):
                day_metric_scheduler_cfg = {}
            if bool(day_metric_scheduler_cfg.get("auto_start_in_gui", False)):
                _report_progress("starting_day_metric_scheduler")
                self.start_day_metric_upload_scheduler(source=source)
            else:
                self.add_system_log("[12项独立上传调度] 启动时未自动开启")
        else:
            self.add_system_log("[12项独立上传调度] 已禁用")

        self.add_system_log(
            f"[告警信息上传调度] 启动阶段执行器状态: executor_bound={self.is_alarm_event_upload_scheduler_executor_bound()}, "
            f"callback={self.alarm_event_upload_scheduler_executor_name()}"
        )
        alarm_scheduler_status = self.alarm_event_upload_scheduler_status()
        if role_mode == "internal":
            self.add_system_log("[告警信息上传调度] 当前为内网端，启动时不自动开启")
        elif bool(alarm_scheduler_status.get("enabled", False)):
            alarm_cfg = self.runtime_config.get("alarm_export", {})
            if not isinstance(alarm_cfg, dict):
                alarm_cfg = {}
            alarm_scheduler_cfg = alarm_cfg.get("scheduler", {})
            if not isinstance(alarm_scheduler_cfg, dict):
                alarm_scheduler_cfg = {}
            if bool(alarm_scheduler_cfg.get("auto_start_in_gui", False)):
                _report_progress("starting_alarm_scheduler")
                self.start_alarm_event_upload_scheduler(source=source)
            else:
                self.add_system_log("[告警信息上传调度] 启动时未自动开启")
        else:
            self.add_system_log("[告警信息上传调度] 已禁用")

        self.add_system_log(
            f"[月度变更统计表调度] 启动阶段执行器状态: executor_bound={self.is_monthly_change_report_scheduler_executor_bound()}, "
            f"callback={self.monthly_change_report_scheduler_executor_name()}"
        )
        monthly_change_status = self.monthly_change_report_scheduler_status()
        if role_mode == "internal":
            self.add_system_log("[月度变更统计表调度] 当前为内网端，启动时不自动开启")
        elif bool(monthly_change_status.get("enabled", False)):
            handover_cfg = self.runtime_config.get("handover_log", {})
            if not isinstance(handover_cfg, dict):
                handover_cfg = {}
            monthly_change_cfg = handover_cfg.get("monthly_change_report", {})
            if not isinstance(monthly_change_cfg, dict):
                monthly_change_cfg = {}
            monthly_change_scheduler_cfg = monthly_change_cfg.get("scheduler", {})
            if not isinstance(monthly_change_scheduler_cfg, dict):
                monthly_change_scheduler_cfg = {}
            if bool(monthly_change_scheduler_cfg.get("auto_start_in_gui", False)):
                _report_progress("starting_monthly_change_scheduler")
                self.start_monthly_change_report_scheduler(source=source)
            else:
                self.add_system_log("[月度变更统计表调度] 启动时未自动开启")
        else:
            self.add_system_log("[月度变更统计表调度] 已禁用")

        self.add_system_log(
            f"[月度事件统计表调度] 启动阶段执行器状态: executor_bound={self.is_monthly_event_report_scheduler_executor_bound()}, "
            f"callback={self.monthly_event_report_scheduler_executor_name()}"
        )
        monthly_event_status = self.monthly_event_report_scheduler_status()
        if role_mode == "internal":
            self.add_system_log("[月度事件统计表调度] 当前为内网端，启动时不自动开启")
        elif bool(monthly_event_status.get("enabled", False)):
            handover_cfg = self.runtime_config.get("handover_log", {})
            if not isinstance(handover_cfg, dict):
                handover_cfg = {}
            monthly_event_cfg = handover_cfg.get("monthly_event_report", {})
            if not isinstance(monthly_event_cfg, dict):
                monthly_event_cfg = {}
            monthly_event_scheduler_cfg = monthly_event_cfg.get("scheduler", {})
            if not isinstance(monthly_event_scheduler_cfg, dict):
                monthly_event_scheduler_cfg = {}
            if bool(monthly_event_scheduler_cfg.get("auto_start_in_gui", False)):
                _report_progress("starting_monthly_event_scheduler")
                self.start_monthly_event_report_scheduler(source=source)
            else:
                self.add_system_log("[月度事件统计表调度] 启动时未自动开启")
        else:
            self.add_system_log("[月度事件统计表调度] 已禁用")

        if self.updater_service and self.updater_service.enabled:
            _report_progress("starting_updater")
            self.start_updater(source=source)
        else:
            self.add_system_log("[更新] 启动时未自动开启")

        if role_mode != "internal" and self.alert_log_uploader:
            _report_progress("starting_alert_log_uploader")
            self.start_alert_log_uploader(source=source)
        _report_progress("starting_shared_bridge")
        self.start_shared_bridge(source=source)
        _report_progress("runtime_services_started")
        return {
            "ok": True,
            "armed": True,
            "role_mode": role_mode,
            "external_scheduler_autostart_state": external_scheduler_autostart_state,
        }

    @staticmethod
    def _ensure_config_dict_path(root: Dict[str, Any], path: tuple[str, ...]) -> Dict[str, Any]:
        current = root
        for key in path:
            next_value = current.get(key)
            if not isinstance(next_value, dict):
                next_value = {}
                current[key] = next_value
            current = next_value
        return current

    def _persist_running_scheduler_autostart_flags(self, source: str = "退出当前系统") -> Dict[str, Any]:
        """Persist currently running schedulers as auto-start for the next role entry."""
        entries: list[tuple[str, tuple[str, ...], bool, Any]] = [
            (
                "自动流程调度",
                ("common", "scheduler"),
                bool(self.scheduler.is_running()) if self.scheduler else False,
                self.scheduler,
            ),
            (
                "交接班调度",
                ("features", "handover_log", "scheduler"),
                bool(self.handover_scheduler_manager.is_running()) if self.handover_scheduler_manager else False,
                self.handover_scheduler_manager,
            ),
            (
                "湿球温度定时采集调度",
                ("features", "wet_bulb_collection", "scheduler"),
                bool(self.wet_bulb_collection_scheduler.is_running()) if self.wet_bulb_collection_scheduler else False,
                self.wet_bulb_collection_scheduler,
            ),
            (
                "12项独立上传调度",
                ("features", "day_metric_upload", "scheduler"),
                bool(self.day_metric_upload_scheduler.is_running()) if self.day_metric_upload_scheduler else False,
                self.day_metric_upload_scheduler,
            ),
            (
                "告警信息上传调度",
                ("features", "alarm_export", "scheduler"),
                bool(self.alarm_event_upload_scheduler.is_running()) if self.alarm_event_upload_scheduler else False,
                self.alarm_event_upload_scheduler,
            ),
            (
                "月度变更统计表调度",
                ("features", "handover_log", "monthly_change_report", "scheduler"),
                bool(self.monthly_change_report_scheduler.is_running()) if self.monthly_change_report_scheduler else False,
                self.monthly_change_report_scheduler,
            ),
            (
                "月度事件统计表调度",
                ("features", "handover_log", "monthly_event_report", "scheduler"),
                bool(self.monthly_event_report_scheduler.is_running()) if self.monthly_event_report_scheduler else False,
                self.monthly_event_report_scheduler,
            ),
        ]
        running_entries = [entry for entry in entries if entry[2]]
        if not running_entries:
            return {"ok": True, "changed": False, "saved": []}

        try:
            merged = copy.deepcopy(self.config if isinstance(self.config, dict) else {})
            saved_labels: list[str] = []
            for label, path, _running, scheduler_obj in running_entries:
                scheduler_cfg = self._ensure_config_dict_path(merged, path)
                changed = False
                if scheduler_cfg.get("auto_start_in_gui") is not True:
                    scheduler_cfg["auto_start_in_gui"] = True
                    changed = True
                if scheduler_cfg.get("enabled") is not True:
                    scheduler_cfg["enabled"] = True
                    changed = True
                if changed:
                    saved_labels.append(label)
                if scheduler_obj is not None:
                    try:
                        setattr(scheduler_obj, "auto_start_in_gui", True)
                        setattr(scheduler_obj, "enabled", True)
                        if isinstance(getattr(scheduler_obj, "cfg", None), dict):
                            scheduler_obj.cfg["auto_start_in_gui"] = True
                            scheduler_obj.cfg["enabled"] = True
                        if isinstance(getattr(scheduler_obj, "_cfg", None), dict):
                            scheduler_obj._cfg["auto_start_in_gui"] = True
                            scheduler_obj._cfg["enabled"] = True
                        child_schedulers = getattr(scheduler_obj, "schedulers", None)
                        if isinstance(child_schedulers, dict):
                            for child in child_schedulers.values():
                                try:
                                    setattr(child, "auto_start_in_gui", True)
                                    setattr(child, "enabled", True)
                                    if isinstance(getattr(child, "cfg", None), dict):
                                        child.cfg["auto_start_in_gui"] = True
                                        child.cfg["enabled"] = True
                                except Exception:  # noqa: BLE001
                                    pass
                    except Exception:  # noqa: BLE001
                        pass
            if not saved_labels:
                return {"ok": True, "changed": False, "saved": []}
            saved_config = save_settings(merged, self.config_path)
            self.config = copy.deepcopy(saved_config)
            self.runtime_config = adapt_runtime_config(self.config)
            self.add_system_log(
                f"[调度] {source}: 已保存当前运行调度为下次自动启动: {','.join(saved_labels)}"
            )
            return {"ok": True, "changed": True, "saved": saved_labels}
        except Exception as exc:  # noqa: BLE001
            self.add_system_log(f"[调度] {source}: 保存当前运行调度状态失败，不阻断退出: {exc}")
            return {"ok": False, "changed": False, "saved": [], "error": str(exc)}

    def stop_role_runtime_services(self, source: str = "退出当前系统") -> Dict[str, Any]:
        role_mode = str(self.deployment_snapshot().get("role_mode", "") or "").strip().lower()
        results: Dict[str, Any] = {}
        cancelled_jobs: list[str] = []
        failed_cancellations: list[Dict[str, str]] = []
        scheduler_autostart_persist = {
            "ok": True,
            "skipped": True,
            "reason": "exit_does_not_override_memory",
        }
        external_scheduler_autostart_state = self.persist_external_scheduler_autostart_state_on_exit(
            source=f"{source}:退出不覆盖记忆"
        )

        active_job_ids: list[str] = []
        try:
            active_job_ids = list(self.job_service.active_job_ids(include_waiting=True))
        except Exception as exc:  # noqa: BLE001
            failed_cancellations.append({"job_id": "*", "error": str(exc)})
        cancel_job = getattr(self.job_service, "cancel_job", None)
        if callable(cancel_job):
            for job_id in active_job_ids:
                job_text = str(job_id or "").strip()
                if not job_text:
                    continue
                try:
                    cancel_job(job_text)
                    cancelled_jobs.append(job_text)
                except Exception as exc:  # noqa: BLE001
                    failed_cancellations.append({"job_id": job_text, "error": str(exc)})

        stop_actions = (
            ("scheduler", self.stop_scheduler),
            ("handover_scheduler", self.stop_handover_scheduler),
            ("wet_bulb_collection_scheduler", self.stop_wet_bulb_collection_scheduler),
            ("day_metric_upload_scheduler", self.stop_day_metric_upload_scheduler),
            ("alarm_event_upload_scheduler", self.stop_alarm_event_upload_scheduler),
            ("monthly_change_report_scheduler", self.stop_monthly_change_report_scheduler),
            ("monthly_event_report_scheduler", self.stop_monthly_event_report_scheduler),
            ("updater", self.stop_updater),
            ("alert_log_uploader", self.stop_alert_log_uploader),
            ("shared_bridge", self.stop_shared_bridge),
        )
        for key, action in stop_actions:
            try:
                results[key] = action(source=source)
            except Exception as exc:  # noqa: BLE001
                results[key] = {"ok": False, "error": str(exc)}

        try:
            self.job_service.shutdown_task_engine()
            results["task_engine"] = {"ok": True, "stopped": True}
        except Exception as exc:  # noqa: BLE001
            results["task_engine"] = {"ok": False, "error": str(exc)}

        self.runtime_services_armed = False
        role_label = "内网端" if role_mode == "internal" else "外网端" if role_mode == "external" else "当前角色"
        self.add_system_log(
            f"[启动] {source}: 已退出{role_label}运行时, "
            f"cancelled_jobs={len(cancelled_jobs)}, failed_cancellations={len(failed_cancellations)}"
        )
        return {
            "ok": True,
            "armed": False,
            "role_mode": role_mode,
            "cancelled_jobs": cancelled_jobs,
            "failed_cancellations": failed_cancellations,
            "scheduler_autostart_persist": scheduler_autostart_persist,
            "external_scheduler_autostart_state": external_scheduler_autostart_state,
            "results": results,
        }

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

    def start_day_metric_upload_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if not self.day_metric_upload_scheduler:
            self.day_metric_upload_scheduler = self._build_day_metric_upload_scheduler()
        result = self.day_metric_upload_scheduler.start()
        self.add_system_log(
            f"[12项独立上传调度] {source}启动请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}, "
            f"executor_bound={self.is_day_metric_upload_scheduler_executor_bound()}, "
            f"callback={self.day_metric_upload_scheduler_executor_name()}"
        )
        return result

    def stop_day_metric_upload_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if self.day_metric_upload_scheduler:
            result = self.day_metric_upload_scheduler.stop()
        else:
            result = {"stopped": False, "running": False, "reason": "not_initialized"}
        self.add_system_log(
            f"[12项独立上传调度] {source}停止请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}"
        )
        return result

    def start_alarm_event_upload_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if not self.alarm_event_upload_scheduler:
            self.alarm_event_upload_scheduler = self._build_alarm_event_upload_scheduler()
        result = self.alarm_event_upload_scheduler.start()
        self.add_system_log(
            f"[告警信息上传调度] {source}启动请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}, "
            f"executor_bound={self.is_alarm_event_upload_scheduler_executor_bound()}, "
            f"callback={self.alarm_event_upload_scheduler_executor_name()}"
        )
        return result

    def stop_alarm_event_upload_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if self.alarm_event_upload_scheduler:
            result = self.alarm_event_upload_scheduler.stop()
        else:
            result = {"stopped": False, "running": False, "reason": "not_initialized"}
        self.add_system_log(
            f"[告警信息上传调度] {source}停止请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}"
        )
        return result

    def start_monthly_change_report_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if not self.monthly_change_report_scheduler:
            self.monthly_change_report_scheduler = self._build_monthly_change_report_scheduler()
        result = self.monthly_change_report_scheduler.start()
        self.add_system_log(
            f"[月度变更统计表调度] {source}启动请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}, "
            f"executor_bound={self.is_monthly_change_report_scheduler_executor_bound()}, "
            f"callback={self.monthly_change_report_scheduler_executor_name()}"
        )
        return result

    def stop_monthly_change_report_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if self.monthly_change_report_scheduler:
            result = self.monthly_change_report_scheduler.stop()
        else:
            result = {"stopped": False, "running": False, "reason": "not_initialized"}
        self.add_system_log(
            f"[月度变更统计表调度] {source}停止请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}"
        )
        return result

    def start_monthly_event_report_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if not self.monthly_event_report_scheduler:
            self.monthly_event_report_scheduler = self._build_monthly_event_report_scheduler()
        result = self.monthly_event_report_scheduler.start()
        self.add_system_log(
            f"[月度事件统计表调度] {source}启动请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
            f"running={bool(result.get('running', False))}, "
            f"executor_bound={self.is_monthly_event_report_scheduler_executor_bound()}, "
            f"callback={self.monthly_event_report_scheduler_executor_name()}"
        )
        return result

    def stop_monthly_event_report_scheduler(self, source: str = "手动") -> Dict[str, Any]:
        if self.monthly_event_report_scheduler:
            result = self.monthly_event_report_scheduler.stop()
        else:
            result = {"stopped": False, "running": False, "reason": "not_initialized"}
        self.add_system_log(
            f"[月度事件统计表调度] {source}停止请求: 原因={self._runtime_action_reason_text(result.get('reason', '-'))}, "
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
        self._ensure_shared_bridge_service_matches_configured_role()
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
        configured = self._configured_deployment_snapshot()
        if not self.shared_bridge_service:
            return configured
        if not self.runtime_services_armed:
            return configured
        runtime_snapshot = self._shared_bridge_runtime_deployment_snapshot()
        if not isinstance(runtime_snapshot, dict):
            return configured
        configured_role_mode = normalize_role_mode(configured.get("role_mode", ""))
        runtime_role_mode = normalize_role_mode(runtime_snapshot.get("role_mode", ""))
        if (
            configured_role_mode in {"internal", "external"}
            and runtime_role_mode in {"internal", "external"}
            and configured_role_mode != runtime_role_mode
        ):
            return configured
        return {
            **configured,
            **{
                "role_mode": str(runtime_role_mode or configured.get("role_mode", "")).strip().lower(),
                "node_id": str(runtime_snapshot.get("node_id", "") or configured.get("node_id", "")).strip(),
                "node_label": str(runtime_snapshot.get("node_label", "") or configured.get("node_label", "")).strip(),
            },
        }

    def shared_bridge_snapshot(self, *, mode: str = "external_full") -> Dict[str, Any]:
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
                "last_cleanup_at": "",
                "cleanup_deleted_tasks": 0,
                "cleanup_deleted_entries": 0,
                "cleanup_deleted_files": 0,
                "pending_internal": 0,
                "pending_external": 0,
                "problematic": 0,
                "task_count": 0,
                "node_count": 0,
                "node_heartbeat_ok": False,
                "agent_status": "stopped" if bridge_active else "disabled",
                "background_task_count": 0,
                "background_running_count": 0,
                "background_tasks": [],
                "heartbeat_interval_sec": 5,
                "poll_interval_sec": 2,
                "internal_alert_status": {
                    "buildings": [],
                    "active_count": 0,
                    "last_notified_at": "",
                },
            }
        else:
            snapshot = self.shared_bridge_service.get_health_snapshot(mode=mode)
            if str(mode or "").strip().lower() == "external_full":
                try:
                    internal_source_cache = (
                        snapshot.get("internal_source_cache", {})
                        if isinstance(snapshot.get("internal_source_cache", {}), dict)
                        else {}
                    )
                    display_overview = (
                        internal_source_cache.get("display_overview", {})
                        if isinstance(internal_source_cache.get("display_overview", {}), dict)
                        else {}
                    )
                    if display_overview:
                        tasks = []
                        get_cached_tasks = getattr(self.shared_bridge_service, "get_cached_tasks", None)
                        if callable(get_cached_tasks):
                            tasks = get_cached_tasks(limit=60)
                        if not tasks:
                            list_tasks = getattr(self.shared_bridge_service, "list_tasks", None)
                            if callable(list_tasks):
                                tasks = list_tasks(limit=60)
                        if tasks:
                            internal_source_cache = dict(internal_source_cache)
                            internal_source_cache["display_overview"] = apply_external_source_cache_backfill_overlays(
                                display_overview,
                                tasks,
                            )
                            snapshot = dict(snapshot)
                            snapshot["internal_source_cache"] = internal_source_cache
                except Exception:
                    pass
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

    def alert_log_uploader_snapshot(self) -> Dict[str, Any]:
        if not self.alert_log_uploader:
            return {
                "running": False,
                "pending_lines": 0,
                "queue_file_size_bytes": 0,
                "oldest_pending_at": "",
                "last_flush_at": "",
                "last_error": "",
            }
        return self.alert_log_uploader.runtime_snapshot()

    def task_engine_snapshot(self) -> Dict[str, Any]:
        return self.job_service.task_engine_runtime_snapshot()

    def updater_snapshot(self) -> Dict[str, Any]:
        if not self.updater_service:
            return {
                "running": False,
                "last_check_at": "",
                "last_result": "",
                "last_error": "",
                "local_version": "",
                "remote_version": "",
                "update_mode": "patch_zip",
                "app_root_dir": "",
                "persistent_user_data_dir": "",
                "git_available": False,
                "git_repo_detected": False,
                "branch": "",
                "local_commit": "",
                "remote_commit": "",
                "worktree_dirty": False,
                "dirty_files": [],
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
                "approved_commit": "",
                "approved_manifest": {},
                "internal_peer": {},
            }
        return self.updater_service.get_runtime_snapshot()

    def shared_root_diagnostic_snapshot(
        self,
        *,
        shared_bridge_snapshot: Dict[str, Any] | None = None,
        updater_snapshot: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        deployment = self.deployment_snapshot()
        role_mode = str(deployment.get("role_mode", "") or "").strip().lower()
        role_label = str(deployment.get("node_label", "") or "").strip() or (
            "内网端" if role_mode == "internal" else "外网端" if role_mode == "external" else "当前角色"
        )
        runtime_shared_bridge = {}
        if isinstance(self.runtime_config, dict):
            runtime_shared_bridge = self.runtime_config.get("shared_bridge", {})
        if not isinstance(runtime_shared_bridge, dict) or not runtime_shared_bridge:
            common_cfg = self.config.get("common", {}) if isinstance(self.config, dict) else {}
            runtime_shared_bridge = common_cfg.get("shared_bridge", {}) if isinstance(common_cfg, dict) else {}
        resolved_bridge = resolve_shared_bridge_paths(runtime_shared_bridge, role_mode)
        internal_root = normalize_windows_path_text(resolved_bridge.get("internal_root_dir", ""))
        external_root = normalize_windows_path_text(resolved_bridge.get("external_root_dir", ""))
        active_root = normalize_windows_path_text(resolved_bridge.get("root_dir", ""))

        bridge_snapshot = dict(shared_bridge_snapshot) if isinstance(shared_bridge_snapshot, dict) else {}
        bridge_runtime_root = normalize_windows_path_text(bridge_snapshot.get("root_dir", "")) or active_root
        updater_runtime = dict(updater_snapshot) if isinstance(updater_snapshot, dict) else {}
        updater_root = ""
        if self.updater_service is not None:
            updater_root = normalize_windows_path_text(getattr(self.updater_service, "shared_bridge_root", ""))
        if not updater_root:
            updater_root = active_root

        internal_canonical = canonicalize_windows_path_for_compare(internal_root)
        external_canonical = canonicalize_windows_path_for_compare(external_root)
        active_canonical = canonicalize_windows_path_for_compare(bridge_runtime_root)
        updater_canonical = canonicalize_windows_path_for_compare(updater_root)

        configured_complete = bool(internal_root and external_root and active_root)
        internal_external_consistent = bool(
            internal_canonical and external_canonical and internal_canonical == external_canonical
        )
        active_matches_role = bool(
            active_canonical
            and (
                windows_paths_point_to_same_location(active_root, internal_root if role_mode == "internal" else external_root)
                if role_mode in {"internal", "external"}
                else False
            )
        )
        updater_matches_bridge = bool(
            active_canonical and updater_canonical and active_canonical == updater_canonical
        )
        alias_same_root = internal_external_consistent and internal_root != external_root

        tone = "success"
        status = "consistent"
        status_text = "共享目录配置一致"
        summary_text = "内外网与更新镜像当前都指向同一共享目录。"
        if not configured_complete:
            tone = "danger"
            status = "misconfigured"
            status_text = "共享目录配置不完整"
            summary_text = "内网、外网或当前角色的共享目录配置仍有缺失。"
        elif not internal_external_consistent:
            tone = "danger"
            status = "mismatch"
            status_text = "内外网共享目录不一致"
            summary_text = "内网和外网当前配置指向了不同共享目录，补采与跟随更新会失效。"
        elif not active_matches_role:
            tone = "danger"
            status = "role_mismatch"
            status_text = "当前角色共享目录异常"
            summary_text = "当前角色运行时使用的共享目录与角色配置不一致，请检查角色切换后的配置。"
        elif not updater_matches_bridge:
            tone = "warning"
            status = "updater_mismatch"
            status_text = "更新镜像目录与共享桥接目录不一致"
            summary_text = "更新链和共享桥接当前没有落到同一共享目录，自动跟随更新可能失效。"
        elif alias_same_root:
            tone = "info"
            status = "alias_match"
            status_text = "路径写法不同但目录一致"
            summary_text = "内外网路径文本不同，例如映射盘与 UNC，但归一后仍是同一共享目录。"

        return {
            "role_mode": role_mode,
            "role_label": role_label,
            "status": status,
            "status_text": status_text,
            "tone": tone,
            "summary_text": summary_text,
            "source_kind": str(updater_runtime.get("source_kind", "") or ""),
            "paths": [
                {
                    "key": "internal_root",
                    "label": "内网共享目录",
                    "path": internal_root,
                    "canonical_path": resolve_windows_network_path(internal_root),
                },
                {
                    "key": "external_root",
                    "label": "外网共享目录",
                    "path": external_root,
                    "canonical_path": resolve_windows_network_path(external_root),
                },
                {
                    "key": "active_root",
                    "label": "当前角色共享目录",
                    "path": bridge_runtime_root,
                    "canonical_path": resolve_windows_network_path(bridge_runtime_root),
                },
                {
                    "key": "updater_root",
                    "label": "更新镜像共享目录",
                    "path": updater_root,
                    "canonical_path": resolve_windows_network_path(updater_root),
                },
            ],
            "items": [
                {
                    "label": "当前角色",
                    "value": role_label,
                    "tone": "info",
                },
                {
                    "label": "路径一致性",
                    "value": status_text,
                    "tone": tone,
                },
                {
                    "label": "共享桥接目录",
                    "value": bridge_runtime_root or "未配置",
                    "tone": "success" if bridge_runtime_root else "danger",
                },
                {
                    "label": "更新镜像目录",
                    "value": updater_root or "未配置",
                    "tone": "success" if updater_root else "danger",
                },
            ],
            "notes": [
                "当前角色运行值和 updater 实际共享目录都来自后端运行时，不是前端推测值。",
                "若内外网目录文本不同但状态为“路径写法不同但目录一致”，通常是映射盘和 UNC 的正常差异。",
            ],
        }

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

    def scheduler_status(self) -> Dict[str, Any]:
        memory_fields = self.external_scheduler_runtime_memory_fields("auto_flow")
        if not self.scheduler:
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
                **memory_fields,
            }
        runtime = self.scheduler.get_runtime_snapshot()
        return {
            "enabled": bool(self.scheduler.enabled),
            "status": self.scheduler.status_text(),
            "next_run_time": self.scheduler.next_run_text(),
            **runtime,
            **memory_fields,
        }

    def handover_scheduler_status(self) -> Dict[str, Any]:
        memory_fields = self.external_scheduler_runtime_memory_fields("handover")
        if not self.handover_scheduler_manager:
            return {
                "enabled": False,
                "running": False,
                "status": "未初始化",
                "slots": {},
                "state_paths": {},
                **memory_fields,
            }
        snapshot = self.handover_scheduler_manager.get_runtime_snapshot()
        return {
            **snapshot,
            **memory_fields,
        }

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
        memory_fields = self.external_scheduler_runtime_memory_fields("wet_bulb_collection")
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
                **memory_fields,
            }
        runtime = self.wet_bulb_collection_scheduler.get_runtime_snapshot()
        return {
            "enabled": bool(self.wet_bulb_collection_scheduler.enabled),
            **runtime,
            **memory_fields,
        }

    def day_metric_upload_scheduler_status(self) -> Dict[str, Any]:
        memory_fields = self.external_scheduler_runtime_memory_fields("day_metric_upload")
        if not self.day_metric_upload_scheduler:
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
                **memory_fields,
            }
        runtime = self.day_metric_upload_scheduler.get_runtime_snapshot()
        return {
            "enabled": bool(self.day_metric_upload_scheduler.enabled),
            "status": self.day_metric_upload_scheduler.status_text(),
            "next_run_time": self.day_metric_upload_scheduler.next_run_text(),
            **runtime,
            **memory_fields,
        }

    def alarm_event_upload_scheduler_status(self) -> Dict[str, Any]:
        memory_fields = self.external_scheduler_runtime_memory_fields("alarm_event_upload")
        if not self.alarm_event_upload_scheduler:
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
                **memory_fields,
            }
        runtime = self.alarm_event_upload_scheduler.get_runtime_snapshot()
        return {
            "enabled": bool(self.alarm_event_upload_scheduler.enabled),
            "status": self.alarm_event_upload_scheduler.status_text(),
            "next_run_time": self.alarm_event_upload_scheduler.next_run_text(),
            **runtime,
            **memory_fields,
        }

    def monthly_event_report_scheduler_status(self) -> Dict[str, Any]:
        memory_fields = self.external_scheduler_runtime_memory_fields("monthly_event_report")
        if not self.monthly_event_report_scheduler:
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
                **memory_fields,
            }
        runtime = self.monthly_event_report_scheduler.get_runtime_snapshot()
        return {
            "enabled": bool(self.monthly_event_report_scheduler.enabled),
            **runtime,
            **memory_fields,
        }

    def monthly_change_report_scheduler_status(self) -> Dict[str, Any]:
        memory_fields = self.external_scheduler_runtime_memory_fields("monthly_change_report")
        if not self.monthly_change_report_scheduler:
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
                **memory_fields,
            }
        runtime = self.monthly_change_report_scheduler.get_runtime_snapshot()
        return {
            "enabled": bool(self.monthly_change_report_scheduler.enabled),
            **runtime,
            **memory_fields,
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

    def _apply_runtime_config_snapshot(self, settings: Dict[str, Any]) -> None:
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

    def apply_config_snapshot(self, settings: Dict[str, Any], *, mode: str = "light") -> None:
        normalized_mode = str(mode or "light").strip().lower() or "light"
        if normalized_mode == "full":
            self.reload_config(settings)
            return
        if normalized_mode != "light":
            raise ValueError(f"unsupported config apply mode: {mode}")
        self._apply_runtime_config_snapshot(settings)

    def reload_config(self, settings: Dict[str, Any]) -> None:
        self._apply_runtime_config_snapshot(settings)

        was_running = self.scheduler.is_running() if self.scheduler else False
        was_handover_running = (
            self.handover_scheduler_manager.is_running() if self.handover_scheduler_manager else False
        )
        was_wet_bulb_running = (
            self.wet_bulb_collection_scheduler.is_running() if self.wet_bulb_collection_scheduler else False
        )
        was_day_metric_upload_running = (
            self.day_metric_upload_scheduler.is_running() if self.day_metric_upload_scheduler else False
        )
        was_alarm_event_upload_running = (
            self.alarm_event_upload_scheduler.is_running() if self.alarm_event_upload_scheduler else False
        )
        was_monthly_change_report_running = (
            self.monthly_change_report_scheduler.is_running() if self.monthly_change_report_scheduler else False
        )
        was_monthly_event_report_running = (
            self.monthly_event_report_scheduler.is_running() if self.monthly_event_report_scheduler else False
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
        if self.day_metric_upload_scheduler:
            self.day_metric_upload_scheduler.stop()
        if self.alarm_event_upload_scheduler:
            self.alarm_event_upload_scheduler.stop()
        if self.monthly_change_report_scheduler:
            self.monthly_change_report_scheduler.stop()
        if self.monthly_event_report_scheduler:
            self.monthly_event_report_scheduler.stop()
        if self.updater_service:
            self.updater_service.stop()
        if self.alert_log_uploader:
            self.alert_log_uploader.stop()
        if self.shared_bridge_service:
            self.shared_bridge_service.stop()
        self.scheduler = self._build_scheduler()
        self.handover_scheduler_manager = self._build_handover_scheduler_manager()
        self.wet_bulb_collection_scheduler = self._build_wet_bulb_collection_scheduler()
        self.day_metric_upload_scheduler = self._build_day_metric_upload_scheduler()
        self.alarm_event_upload_scheduler = self._build_alarm_event_upload_scheduler()
        self.monthly_change_report_scheduler = self._build_monthly_change_report_scheduler()
        self.monthly_event_report_scheduler = self._build_monthly_event_report_scheduler()
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
        if self.day_metric_upload_scheduler_callback:
            self.day_metric_upload_scheduler.run_callback = self.day_metric_upload_scheduler_callback
        if self.alarm_event_upload_scheduler_callback:
            self.alarm_event_upload_scheduler.run_callback = self.alarm_event_upload_scheduler_callback
        if self.monthly_change_report_scheduler_callback:
            self.monthly_change_report_scheduler.run_callback = self.monthly_change_report_scheduler_callback
        if self.monthly_event_report_scheduler_callback:
            self.monthly_event_report_scheduler.run_callback = self.monthly_event_report_scheduler_callback
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
        day_metric_cfg = self.runtime_config.get("day_metric_upload", {})
        if not isinstance(day_metric_cfg, dict):
            day_metric_cfg = {}
        day_metric_scheduler_cfg = day_metric_cfg.get("scheduler", {})
        if not isinstance(day_metric_scheduler_cfg, dict):
            day_metric_scheduler_cfg = {}
        day_metric_auto_start = bool(day_metric_scheduler_cfg.get("auto_start_in_gui", False))
        alarm_event_cfg = self.runtime_config.get("alarm_export", {})
        if not isinstance(alarm_event_cfg, dict):
            alarm_event_cfg = {}
        alarm_event_scheduler_cfg = alarm_event_cfg.get("scheduler", {})
        if not isinstance(alarm_event_scheduler_cfg, dict):
            alarm_event_scheduler_cfg = {}
        alarm_event_auto_start = bool(alarm_event_scheduler_cfg.get("auto_start_in_gui", False))
        monthly_change_cfg = self.runtime_config.get("handover_log", {})
        if not isinstance(monthly_change_cfg, dict):
            monthly_change_cfg = {}
        monthly_change_report_cfg = monthly_change_cfg.get("monthly_change_report", {})
        if not isinstance(monthly_change_report_cfg, dict):
            monthly_change_report_cfg = {}
        monthly_change_scheduler_cfg = monthly_change_report_cfg.get("scheduler", {})
        if not isinstance(monthly_change_scheduler_cfg, dict):
            monthly_change_scheduler_cfg = {}
        monthly_change_auto_start = bool(monthly_change_scheduler_cfg.get("auto_start_in_gui", False))
        monthly_event_cfg = self.runtime_config.get("handover_log", {})
        if not isinstance(monthly_event_cfg, dict):
            monthly_event_cfg = {}
        monthly_event_report_cfg = monthly_event_cfg.get("monthly_event_report", {})
        if not isinstance(monthly_event_report_cfg, dict):
            monthly_event_report_cfg = {}
        monthly_event_scheduler_cfg = monthly_event_report_cfg.get("scheduler", {})
        if not isinstance(monthly_event_scheduler_cfg, dict):
            monthly_event_scheduler_cfg = {}
        monthly_event_auto_start = bool(monthly_event_scheduler_cfg.get("auto_start_in_gui", False))
        if was_running or (self.runtime_services_armed and auto_start):
            self.scheduler.start()
        if was_handover_running or (self.runtime_services_armed and handover_auto_start):
            self.handover_scheduler_manager.start()
        if was_wet_bulb_running or (self.runtime_services_armed and wet_bulb_auto_start):
            self.wet_bulb_collection_scheduler.start()
        if was_day_metric_upload_running or (self.runtime_services_armed and day_metric_auto_start):
            self.day_metric_upload_scheduler.start()
        if was_alarm_event_upload_running or (self.runtime_services_armed and alarm_event_auto_start):
            self.alarm_event_upload_scheduler.start()
        if was_monthly_change_report_running or (self.runtime_services_armed and monthly_change_auto_start):
            self.monthly_change_report_scheduler.start()
        if was_monthly_event_report_running or (self.runtime_services_armed and monthly_event_auto_start):
            self.monthly_event_report_scheduler.start()
        updater_cfg = self.runtime_config.get("updater", {})
        if not isinstance(updater_cfg, dict):
            updater_cfg = {}
        configured_role_mode = normalize_role_mode(self._configured_deployment_snapshot().get("role_mode", ""))
        if (
            self.runtime_services_armed
            and configured_role_mode != "internal"
            and bool(updater_cfg.get("enabled", True))
        ):
            self.updater_service.start()
        if self.runtime_services_armed and was_alert_log_uploader_running and self.alert_log_uploader:
            self.alert_log_uploader.start()
        new_role_mode = str(self.shared_bridge_service.get_deployment_snapshot().get("role_mode", "") or "").strip().lower()
        bridge_cfg = self.runtime_config.get("shared_bridge", {}) if isinstance(self.runtime_config, dict) else {}
        if not isinstance(bridge_cfg, dict):
            bridge_cfg = {}
        resolved_bridge = resolve_shared_bridge_paths(bridge_cfg, new_role_mode)
        shared_bridge_configured = bool(
            resolved_bridge.get("enabled", False)
            and str(resolved_bridge.get("root_dir", "") or "").strip()
        )
        if (
            self.runtime_services_armed
            and previous_role_mode == new_role_mode
            and (was_shared_bridge_running or shared_bridge_configured)
        ):
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
