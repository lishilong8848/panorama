from __future__ import annotations

import json
import re
import threading
from calendar import monthrange
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict

from pipeline_utils import get_app_dir


def _valid_time(value: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", str(value or "").strip()))


class MonthlySchedulerService:
    def __init__(
        self,
        *,
        scheduler_cfg: Dict[str, Any],
        runtime_state_root: str,
        emit_log: Callable[[str], None],
        run_callback: Callable[[str], tuple[bool, str]],
        is_busy: Callable[[], bool],
        thread_name: str = "monthly-scheduler",
        source_name: str = "月度事件统计表处理",
    ) -> None:
        self.cfg = self._normalize_cfg(scheduler_cfg)
        self.enabled = bool(self.cfg["enabled"])
        self.auto_start_in_gui = bool(self.cfg["auto_start_in_gui"])
        self.runtime_state_root = str(runtime_state_root or "").strip()
        self.emit_log = emit_log
        self.run_callback = run_callback
        self.is_busy = is_busy
        self.thread_name = str(thread_name or "monthly-scheduler")
        self.source_name = str(source_name or "月度事件统计表处理")
        self.started_at = datetime.now()
        self.state_path = self._resolve_state_path(str(self.cfg["state_file"]))
        self.state = self._load_state()
        self.runtime: Dict[str, Any] = {
            "started_at": "",
            "last_check_at": "",
            "last_decision": "skip:not_started",
            "last_trigger_at": "",
            "last_trigger_result": "",
        }
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @staticmethod
    def _normalize_cfg(scheduler_cfg: Dict[str, Any]) -> Dict[str, Any]:
        raw = scheduler_cfg if isinstance(scheduler_cfg, dict) else {}
        day_of_month = int(raw.get("day_of_month", 1) or 1)
        check_interval_sec = int(raw.get("check_interval_sec", 30) or 30)
        run_time = str(raw.get("run_time", "") or "").strip() or "01:00:00"
        state_file = str(raw.get("state_file", "") or "").strip() or "monthly_event_report_scheduler_state.json"
        if day_of_month < 1 or day_of_month > 31:
            raise ValueError("配置错误: day_of_month 必须在 1 到 31 之间")
        if check_interval_sec <= 0:
            raise ValueError("配置错误: check_interval_sec 必须大于 0")
        if not _valid_time(run_time):
            raise ValueError("配置错误: run_time 必须是 HH:MM:SS")
        return {
            "enabled": bool(raw.get("enabled", False)),
            "auto_start_in_gui": bool(raw.get("auto_start_in_gui", False)),
            "day_of_month": day_of_month,
            "run_time": run_time,
            "check_interval_sec": check_interval_sec,
            "state_file": state_file,
        }

    def _resolve_state_path(self, state_file: str) -> Path:
        path = Path(state_file)
        if path.is_absolute():
            return path
        if self.runtime_state_root:
            root = Path(self.runtime_state_root)
            if not root.is_absolute():
                root = get_app_dir() / root
        else:
            root = get_app_dir()
        root.mkdir(parents=True, exist_ok=True)
        return root / path

    def _log(self, text: str) -> None:
        self.emit_log(f"[{self.source_name}调度] {text}")

    def _load_state(self) -> Dict[str, Any]:
        default = {
            "last_success_period": "",
            "last_attempt_period": "",
            "last_run_at": "",
            "last_status": "",
            "last_error": "",
        }
        if not self.state_path.exists():
            return default
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return default
        if not isinstance(payload, dict):
            return default
        state = dict(default)
        for key in state:
            state[key] = str(payload.get(key, "") or "")
        return state

    def _save_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            self._log(f"保存状态失败: {exc}")

    def reset_current_month_state_for_schedule_change(self, now: datetime | None = None) -> Dict[str, Any]:
        current = now or datetime.now()
        period = self._period(current)
        reset_keys: list[str] = []
        for key in ("last_success_period", "last_attempt_period"):
            if self.state.get(key, "") == period:
                self.state[key] = ""
                reset_keys.append(key)
        for key in ("last_status", "last_error", "last_run_at"):
            if self.state.get(key, ""):
                self.state[key] = ""
                reset_keys.append(key)
        changed = bool(reset_keys)
        if changed:
            self._save_state()
            self._log(f"调度时间变更，已重置当月调度状态: period={period}, keys={reset_keys}")
        else:
            self._log(f"调度时间变更，当月状态无需重置: period={period}")
        return {
            "changed": changed,
            "period": period,
            "reset_keys": reset_keys,
            "state_path": str(self.state_path),
        }

    @staticmethod
    def _period(dt: datetime) -> str:
        return dt.strftime("%Y-%m")

    def _scheduled_datetime_for_month(self, year: int, month: int) -> datetime:
        last_day = monthrange(year, month)[1]
        day = min(int(self.cfg["day_of_month"]), last_day)
        hour, minute, second = [int(part) for part in str(self.cfg["run_time"]).split(":")]
        return datetime(year, month, day, hour, minute, second)

    def _should_trigger(self, now: datetime) -> tuple[bool, str, str]:
        if not self.enabled:
            return False, "", "disabled"
        period = self._period(now)
        if self.state.get("last_success_period", "") == period:
            return False, period, "already_success_this_month"
        if now < self._scheduled_datetime_for_month(now.year, now.month):
            return False, period, "before_schedule_time"
        if self.state.get("last_attempt_period", "") == period:
            return False, period, "already_attempted_this_month"
        return True, period, "due"

    def next_run_time(self, now: datetime | None = None) -> datetime:
        current = now or datetime.now()
        current_period = self._period(current)
        scheduled_current = self._scheduled_datetime_for_month(current.year, current.month)
        should_run, _, _ = self._should_trigger(current)
        if should_run:
            return current
        if current < scheduled_current and self.state.get("last_success_period", "") != current_period:
            return scheduled_current
        if current.month == 12:
            next_year = current.year + 1
            next_month = 1
        else:
            next_year = current.year
            next_month = current.month + 1
        return self._scheduled_datetime_for_month(next_year, next_month)

    def next_run_text(self) -> str:
        return self.next_run_time().strftime("%Y-%m-%d %H:%M:%S")

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status_text(self) -> str:
        return "运行中" if self.is_running() else "未启动"

    def start(self) -> Dict[str, Any]:
        if not self.enabled:
            self.enabled = True
            self.cfg["enabled"] = True
            self._log("启动请求已接管: enabled=false，按手动启动自动启用调度")
        if self.is_running():
            return {"started": False, "running": True, "reason": "already_running"}
        self.started_at = datetime.now()
        self.runtime["started_at"] = self.started_at.strftime("%Y-%m-%d %H:%M:%S")
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name=self.thread_name, daemon=True)
        self._thread.start()
        self._log(
            f"调度已启动: day_of_month={self.cfg['day_of_month']}, run_time={self.cfg['run_time']}"
        )
        return {"started": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None
        self.runtime["last_decision"] = "skip:stopped"
        self._log("调度已停止")
        return {"stopped": True, "running": False, "reason": "stopped"}

    def get_runtime_snapshot(self) -> Dict[str, Any]:
        return {
            "running": self.is_running(),
            "status": self.status_text(),
            "started_at": str(self.runtime.get("started_at", "")),
            "last_check_at": str(self.runtime.get("last_check_at", "")),
            "last_decision": str(self.runtime.get("last_decision", "")),
            "last_trigger_at": str(self.runtime.get("last_trigger_at", "")),
            "last_trigger_result": str(self.runtime.get("last_trigger_result", "")),
            "next_run_time": self.next_run_text(),
            "state_path": str(self.state_path),
            "state_exists": self.state_path.exists(),
            "last_success_period": str(self.state.get("last_success_period", "")),
            "last_attempt_period": str(self.state.get("last_attempt_period", "")),
            "last_run_at": str(self.state.get("last_run_at", "")),
            "last_status": str(self.state.get("last_status", "")),
            "last_error": str(self.state.get("last_error", "")),
        }

    def _loop(self) -> None:
        interval_sec = int(self.cfg["check_interval_sec"])
        while not self._stop.is_set():
            now = datetime.now()
            self.runtime["last_check_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            should_run, period, reason = self._should_trigger(now)
            if not should_run:
                self.runtime["last_decision"] = f"skip:{reason}"
                try:
                    next_run = self.next_run_time(now)
                    wait_seconds = min(interval_sec, max(0.2, (next_run - now).total_seconds()))
                except Exception:
                    wait_seconds = interval_sec
                self._stop.wait(wait_seconds)
                continue
            if self.is_busy():
                self.runtime["last_decision"] = "skip:busy"
                self.runtime["last_trigger_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
                self.runtime["last_trigger_result"] = "skip_busy"
                self._stop.wait(interval_sec)
                continue

            self.runtime["last_decision"] = "run:due"
            self.runtime["last_trigger_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            try:
                ok, detail = self.run_callback(self.source_name)
            except Exception as exc:
                ok, detail = False, str(exc)

            finished_at = datetime.now()
            self.state["last_attempt_period"] = period
            self.state["last_run_at"] = finished_at.strftime("%Y-%m-%d %H:%M:%S")
            self.state["last_status"] = "success" if ok else "failed"
            self.state["last_error"] = "" if ok else str(detail or "").strip()
            if ok:
                self.state["last_success_period"] = period
            self._save_state()
            self.runtime["last_trigger_result"] = self.state["last_status"]
            self._stop.wait(interval_sec)
