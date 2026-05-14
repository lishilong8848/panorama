from __future__ import annotations

from typing import Any, Callable, Dict

from app.modules.scheduler.service.daily_scheduler_service import DailyAutoSchedulerService


class HandoverSchedulerManager:
    def __init__(
        self,
        config: Dict[str, Any],
        emit_log: Callable[[str], None],
        run_callback: Callable[[str, str], tuple[bool, str]] | None,
        is_busy: Callable[[], bool],
    ) -> None:
        self._full_config = config if isinstance(config, dict) else {}
        self._emit_log = emit_log
        self._run_callback = run_callback
        self._is_busy = is_busy
        self._cfg = self._normalize_cfg(self._full_config)
        self.schedulers: Dict[str, DailyAutoSchedulerService] = {}
        self._build_schedulers()

    def _normalize_cfg(self, config: Dict[str, Any]) -> Dict[str, Any]:
        handover_cfg = config.get("handover_log", {}) if isinstance(config.get("handover_log", {}), dict) else {}
        raw = handover_cfg.get("scheduler", {}) if isinstance(handover_cfg.get("scheduler", {}), dict) else {}
        return {
            "enabled": bool(raw.get("enabled", True)),
            "auto_start_in_gui": bool(raw.get("auto_start_in_gui", False)),
            "morning_time": str(raw.get("morning_time", "07:00:00")).strip() or "07:00:00",
            "afternoon_time": str(raw.get("afternoon_time", "16:00:00")).strip() or "16:00:00",
            "cloud_catchup_enabled": bool(raw.get("cloud_catchup_enabled", True)),
            "cloud_catchup_morning_time": str(raw.get("cloud_catchup_morning_time", "08:00:00")).strip()
            or "08:00:00",
            "cloud_catchup_afternoon_time": str(raw.get("cloud_catchup_afternoon_time", "17:30:00")).strip()
            or "17:30:00",
            "station_110_review_link_enabled": bool(raw.get("station_110_review_link_enabled", True)),
            "station_110_midnight_time": str(raw.get("station_110_midnight_time", "00:00:00")).strip()
            or "00:00:00",
            "station_110_noon_time": str(raw.get("station_110_noon_time", "12:00:00")).strip() or "12:00:00",
            "check_interval_sec": int(raw.get("check_interval_sec", 30)),
            "catch_up_if_missed": bool(raw.get("catch_up_if_missed", False)),
            "retry_failed_in_same_period": bool(raw.get("retry_failed_in_same_period", False)),
            "morning_state_file": str(raw.get("morning_state_file", "handover_scheduler_morning_state.json")).strip()
            or "handover_scheduler_morning_state.json",
            "afternoon_state_file": str(
                raw.get("afternoon_state_file", "handover_scheduler_afternoon_state.json")
            ).strip()
            or "handover_scheduler_afternoon_state.json",
            "cloud_catchup_morning_state_file": str(
                raw.get("cloud_catchup_morning_state_file", "handover_cloud_catchup_morning_state.json")
            ).strip()
            or "handover_cloud_catchup_morning_state.json",
            "cloud_catchup_afternoon_state_file": str(
                raw.get("cloud_catchup_afternoon_state_file", "handover_cloud_catchup_afternoon_state.json")
            ).strip()
            or "handover_cloud_catchup_afternoon_state.json",
            "station_110_midnight_state_file": str(
                raw.get("station_110_midnight_state_file", "handover_scheduler_110_midnight_state.json")
            ).strip()
            or "handover_scheduler_110_midnight_state.json",
            "station_110_noon_state_file": str(
                raw.get("station_110_noon_state_file", "handover_scheduler_110_noon_state.json")
            ).strip()
            or "handover_scheduler_110_noon_state.json",
        }

    def _slot_callback(self, slot: str) -> Callable[[str], tuple[bool, str]]:
        def _callback(source: str) -> tuple[bool, str]:
            if not callable(self._run_callback):
                return False, "交接班调度回调尚未绑定执行器"
            return self._run_callback(slot, source)

        return _callback

    def _slot_service_config(self, *, run_time: str, state_file: str) -> Dict[str, Any]:
        paths_cfg = self._full_config.get("paths", {}) if isinstance(self._full_config.get("paths", {}), dict) else {}
        return {
            "paths": dict(paths_cfg),
            "scheduler": {
                "enabled": bool(self._cfg["enabled"]),
                "auto_start_in_gui": bool(self._cfg["auto_start_in_gui"]),
                "run_time": run_time,
                "check_interval_sec": int(self._cfg["check_interval_sec"]),
                "catch_up_if_missed": bool(self._cfg["catch_up_if_missed"]),
                "retry_failed_in_same_period": bool(self._cfg["retry_failed_in_same_period"]),
                "state_file": state_file,
            },
        }

    def _daily_scheduler(
        self,
        *,
        slot: str,
        run_time: str,
        state_file: str,
        thread_name: str,
        source_name: str,
    ) -> DailyAutoSchedulerService:
        return DailyAutoSchedulerService(
            config=self._slot_service_config(
                run_time=run_time,
                state_file=state_file,
            ),
            emit_log=self._emit_log,
            run_callback=self._slot_callback(slot),
            is_busy=self._is_busy,
            thread_name=thread_name,
            source_name=source_name,
        )

    def _build_schedulers(self) -> None:
        schedulers = {
            "morning": DailyAutoSchedulerService(
                config=self._slot_service_config(
                    run_time=str(self._cfg["morning_time"]),
                    state_file=str(self._cfg["morning_state_file"]),
                ),
                emit_log=self._emit_log,
                run_callback=self._slot_callback("morning"),
                is_busy=self._is_busy,
            ),
            "afternoon": DailyAutoSchedulerService(
                config=self._slot_service_config(
                    run_time=str(self._cfg["afternoon_time"]),
                    state_file=str(self._cfg["afternoon_state_file"]),
                ),
                emit_log=self._emit_log,
                run_callback=self._slot_callback("afternoon"),
                is_busy=self._is_busy,
            ),
        }
        if bool(self._cfg.get("cloud_catchup_enabled", True)):
            schedulers["cloud_catchup_morning"] = self._daily_scheduler(
                slot="cloud_catchup_morning",
                run_time=str(self._cfg["cloud_catchup_morning_time"]),
                state_file=str(self._cfg["cloud_catchup_morning_state_file"]),
                thread_name="handover-cloud-catchup-morning-scheduler",
                source_name="交接班定时确认并上传云文档（8点）",
            )
            schedulers["cloud_catchup_afternoon"] = self._daily_scheduler(
                slot="cloud_catchup_afternoon",
                run_time=str(self._cfg["cloud_catchup_afternoon_time"]),
                state_file=str(self._cfg["cloud_catchup_afternoon_state_file"]),
                thread_name="handover-cloud-catchup-afternoon-scheduler",
                source_name="交接班定时确认并上传云文档（17点30）",
            )
        if bool(self._cfg.get("station_110_review_link_enabled", True)):
            schedulers["station_110_midnight"] = self._daily_scheduler(
                slot="station_110_midnight",
                run_time=str(self._cfg["station_110_midnight_time"]),
                state_file=str(self._cfg["station_110_midnight_state_file"]),
                thread_name="handover-110-review-link-midnight-scheduler",
                source_name="110站审核链接定时发送（0点）",
            )
            schedulers["station_110_noon"] = self._daily_scheduler(
                slot="station_110_noon",
                run_time=str(self._cfg["station_110_noon_time"]),
                state_file=str(self._cfg["station_110_noon_state_file"]),
                thread_name="handover-110-review-link-noon-scheduler",
                source_name="110站审核链接定时发送（12点）",
            )
        self.schedulers = schedulers

    def set_run_callback(self, callback: Callable[[str, str], tuple[bool, str]]) -> None:
        self._run_callback = callback
        for slot, scheduler in self.schedulers.items():
            scheduler.run_callback = self._slot_callback(slot)

    def is_running(self) -> bool:
        return any(s.is_running() for s in self.schedulers.values())

    def status_text(self) -> str:
        return "运行中" if self.is_running() else "未启动"

    def start(self) -> Dict[str, Any]:
        if not bool(self._cfg.get("enabled", True)):
            self._cfg["enabled"] = True
            self._emit_log("[交接班调度] 启动请求已接管: enabled=false，按手动启动自动启用调度")
            for scheduler in self.schedulers.values():
                scheduler.enabled = True
                scheduler.cfg["enabled"] = True

        action: Dict[str, Any] = {"running": False, "reason": "disabled", "slots": {}}

        any_running = False
        all_started = True
        for slot, scheduler in self.schedulers.items():
            result = scheduler.start()
            action["slots"][slot] = result
            any_running = any_running or bool(result.get("running", False))
            all_started = all_started and bool(result.get("started", False))

        action["running"] = any_running
        if all_started:
            action["reason"] = "started"
        elif any_running:
            action["reason"] = "partial_started"
        else:
            action["reason"] = "already_running"
        return action

    def stop(self) -> Dict[str, Any]:
        action: Dict[str, Any] = {"running": False, "reason": "stopped", "slots": {}}
        for slot, scheduler in self.schedulers.items():
            action["slots"][slot] = scheduler.stop()
        action["running"] = self.is_running()
        return action

    def reset_today_state_for_time_change(self, slot: str) -> Dict[str, Any]:
        scheduler = self.schedulers.get(slot)
        if not scheduler:
            return {"changed": False, "period": "", "reset_keys": [], "state_path": "", "slot": slot}
        result = scheduler.reset_today_state_for_run_time_change()
        result["slot"] = slot
        return result

    def get_runtime_snapshot(self) -> Dict[str, Any]:
        slots: Dict[str, Any] = {}
        state_paths: Dict[str, str] = {}
        for slot, scheduler in self.schedulers.items():
            snap = scheduler.get_runtime_snapshot()
            snap["next_run_time"] = scheduler.next_run_text()
            snap["status"] = scheduler.status_text()
            slots[slot] = snap
            state_paths[slot] = str(snap.get("state_path", ""))
        return {
            "enabled": bool(self._cfg.get("enabled", True)),
            "running": self.is_running(),
            "status": self.status_text(),
            "slots": slots,
            "state_paths": state_paths,
        }

    def get_diagnostics(self, limit: int = 50) -> Dict[str, Any]:
        return {
            "config": dict(self._cfg),
            "running": self.is_running(),
            "status": self.status_text(),
            "slots": {
                slot: scheduler.get_diagnostics(limit=limit) for slot, scheduler in self.schedulers.items()
            },
        }
