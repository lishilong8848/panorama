from __future__ import annotations

import json
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

from pipeline_utils import get_app_dir


def _valid_time(value: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", str(value).strip()))


class DailyAutoSchedulerService:
    def __init__(
        self,
        config: Dict[str, Any],
        emit_log: Callable[[str], None],
        run_callback: Callable[[str], Tuple[bool, str]],
        is_busy: Callable[[], bool],
    ) -> None:
        scheduler_cfg = config.get("scheduler")
        paths_cfg = config.get("paths", {})
        if not isinstance(scheduler_cfg, dict):
            raise ValueError("配置错误: scheduler 缺失或格式错误")
        if not isinstance(paths_cfg, dict):
            paths_cfg = {}

        required = [
            "enabled",
            "auto_start_in_gui",
            "run_time",
            "check_interval_sec",
            "catch_up_if_missed",
            "retry_failed_in_same_period",
            "state_file",
        ]
        missing = [k for k in required if k not in scheduler_cfg]
        if missing:
            raise ValueError(f"配置错误: scheduler 缺少字段 {missing}")

        run_time = str(scheduler_cfg["run_time"]).strip()
        if not _valid_time(run_time):
            raise ValueError("配置错误: scheduler.run_time 必须是 HH:MM:SS")
        check_interval_sec = int(scheduler_cfg["check_interval_sec"])
        if check_interval_sec <= 0:
            raise ValueError("配置错误: scheduler.check_interval_sec 必须大于0")

        state_file = str(scheduler_cfg["state_file"]).strip()
        if not state_file:
            raise ValueError("配置错误: scheduler.state_file 不能为空")

        self.cfg = {
            "enabled": bool(scheduler_cfg["enabled"]),
            "auto_start_in_gui": bool(scheduler_cfg["auto_start_in_gui"]),
            "run_time": run_time,
            "check_interval_sec": check_interval_sec,
            "catch_up_if_missed": bool(scheduler_cfg["catch_up_if_missed"]),
            "retry_failed_in_same_period": bool(scheduler_cfg["retry_failed_in_same_period"]),
            "state_file": state_file,
        }
        self.runtime_state_root = str(paths_cfg.get("runtime_state_root", "") or "").strip()
        self.enabled = bool(self.cfg["enabled"])
        self.auto_start_in_gui = bool(self.cfg["auto_start_in_gui"])

        self.emit_log = emit_log
        self.run_callback = run_callback
        self.is_busy = is_busy

        self.started_at = datetime.now()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._diag_lock = threading.Lock()
        self._diag_logs: list[str] = []
        self._max_diag_logs = 200

        self.runtime: Dict[str, str] = {
            "started_at": "",
            "last_check_at": "",
            "last_decision": "skip:not_started",
            "last_trigger_at": "",
            "last_trigger_result": "",
        }

        self.state_path = self._resolve_state_path(state_file)
        self.state = self._load_state()

    def _resolve_state_path(self, state_file: str) -> Path:
        path = Path(state_file)
        if path.is_absolute():
            return path
        root_text = self.runtime_state_root
        if root_text:
            root = Path(root_text)
            if not root.is_absolute():
                root = get_app_dir() / root
        else:
            root = get_app_dir()
        root.mkdir(parents=True, exist_ok=True)
        return root / path

    def _diag(self, text: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {text}"
        with self._diag_lock:
            self._diag_logs.append(line)
            overflow = len(self._diag_logs) - self._max_diag_logs
            if overflow > 0:
                del self._diag_logs[:overflow]
        self.emit_log(f"[调度诊断] {text}")

    def _load_state(self) -> Dict[str, str]:
        default = {
            "last_success_period": "",
            "last_attempt_period": "",
            "last_run_at": "",
            "last_status": "",
            "last_error": "",
            "retry_done_period": "",
        }
        if not self.state_path.exists():
            return default
        try:
            obj = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(obj, dict):
                return default
            out = dict(default)
            for key in out:
                out[key] = str(obj.get(key, "") or "")
            return out
        except Exception:  # noqa: BLE001
            return default

    def _save_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            self._diag(f"保存状态失败: {exc}")

    def reset_today_state_for_run_time_change(self, now: datetime | None = None) -> Dict[str, Any]:
        now = now or datetime.now()
        today_period = self._period(now)
        reset_keys: list[str] = []

        for key in ("last_success_period", "last_attempt_period", "retry_done_period"):
            if self.state.get(key, "") == today_period:
                self.state[key] = ""
                reset_keys.append(key)

        for key in ("last_status", "last_error", "last_run_at"):
            if self.state.get(key, ""):
                self.state[key] = ""
                reset_keys.append(key)

        changed = bool(reset_keys)
        if changed:
            self._save_state()
            self._diag(f"run_time变更，已重置当日调度状态: period={today_period}, keys={reset_keys}")
        else:
            self._diag(f"run_time变更，今日状态无需重置: period={today_period}")
        return {
            "changed": changed,
            "period": today_period,
            "reset_keys": reset_keys,
            "state_path": str(self.state_path),
        }

    def _schedule_for_day(self, day: datetime) -> datetime:
        h, m, s = [int(x) for x in str(self.cfg["run_time"]).split(":")]
        h, m, s = max(0, min(h, 23)), max(0, min(m, 59)), max(0, min(s, 59))
        return datetime(day.year, day.month, day.day, h, m, s)

    @staticmethod
    def _period(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")

    def _should_trigger(self, now: datetime) -> tuple[bool, str, str]:
        if not self.enabled:
            return False, "", "disabled"

        period = self._period(now)
        if self.state.get("last_success_period", "") == period:
            return False, period, "already_success_today"

        scheduled = self._schedule_for_day(now)
        if now < scheduled:
            return False, period, "before_schedule_time"

        attempted_today = self.state.get("last_attempt_period", "") == period
        failed_today = self.state.get("last_status", "") == "failed"

        if not attempted_today:
            started_after_schedule = self.started_at > scheduled
            if started_after_schedule and not bool(self.cfg["catch_up_if_missed"]):
                return False, period, "missed_and_no_catchup"
            return True, period, "due"

        if failed_today and bool(self.cfg["retry_failed_in_same_period"]):
            if self.state.get("retry_done_period", "") == period:
                return False, period, "retry_already_done"
            return True, period, "due_retry"

        return False, period, "already_attempted_no_retry"

    def next_run_time(self, now: datetime | None = None) -> datetime:
        now = now or datetime.now()
        scheduled = self._schedule_for_day(now)
        if now < scheduled:
            return scheduled
        should_run, _, _ = self._should_trigger(now)
        if should_run:
            return now
        return self._schedule_for_day(now + timedelta(days=1))

    def next_run_text(self) -> str:
        return self.next_run_time().strftime("%Y-%m-%d %H:%M:%S")

    def status_text(self) -> str:
        return "运行中" if self.is_running() else "未启动"

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> Dict[str, Any]:
        if not self.enabled:
            self.enabled = True
            self.cfg["enabled"] = True
            self._diag("启动请求已接管: enabled=false，按手动启动自动启用调度")
        if self.is_running():
            self._diag("启动请求被忽略: already_running")
            return {"started": False, "running": True, "reason": "already_running"}

        self.started_at = datetime.now()
        self.runtime["started_at"] = self.started_at.strftime("%Y-%m-%d %H:%M:%S")
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="web-daily-auto-scheduler")
        self._thread.start()
        return {"started": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None
        self.runtime["last_decision"] = "skip:stopped"
        self._diag("调度线程已停止")
        return {"stopped": True, "running": False, "reason": "stopped"}

    def get_runtime_snapshot(self) -> Dict[str, Any]:
        return {
            "running": self.is_running(),
            "started_at": self.runtime.get("started_at", ""),
            "last_check_at": self.runtime.get("last_check_at", ""),
            "last_decision": self.runtime.get("last_decision", ""),
            "last_trigger_at": self.runtime.get("last_trigger_at", ""),
            "last_trigger_result": self.runtime.get("last_trigger_result", ""),
            "state_path": str(self.state_path),
            "state_exists": self.state_path.exists(),
        }

    def get_diagnostics(self, limit: int = 50) -> Dict[str, Any]:
        with self._diag_lock:
            logs = list(self._diag_logs[-max(1, int(limit)):])
        return {
            "config": dict(self.cfg),
            "runtime": self.get_runtime_snapshot(),
            "state": dict(self.state),
            "logs": logs,
        }

    def _loop(self) -> None:
        self._diag(f"调度线程已启动，每天 {self.cfg['run_time']} 自动执行")
        interval = int(self.cfg["check_interval_sec"])
        last_next = ""
        last_decision = ""
        while not self._stop.is_set():
            now = datetime.now()
            self.runtime["last_check_at"] = now.strftime("%Y-%m-%d %H:%M:%S")

            next_text = self.next_run_text()
            if next_text != last_next:
                self._diag(f"下次执行: {next_text}")
                last_next = next_text

            should_run, period, reason = self._should_trigger(now)
            decision_text = f"run:{reason}" if should_run else f"skip:{reason}"
            self.runtime["last_decision"] = decision_text
            if decision_text != last_decision:
                self._diag(f"触发决策: {decision_text}, period={period or '-'}")
                last_decision = decision_text

            if should_run:
                self.runtime["last_trigger_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
                if self.is_busy():
                    self.runtime["last_trigger_result"] = "skip_busy"
                    self._diag("触发跳过: 当前有任务运行")
                else:
                    is_retry = (
                        self.state.get("last_attempt_period", "") == period
                        and self.state.get("last_status", "") == "failed"
                    )
                    source = "内置每日调度补跑" if is_retry else "内置每日调度"
                    self._diag(f"触发执行: source={source}, period={period}")
                    ok, detail = self.run_callback(source)

                    self.state["last_attempt_period"] = period
                    self.state["last_run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.state["last_status"] = "success" if ok else "failed"
                    self.state["last_error"] = "" if ok else detail
                    if ok:
                        self.state["last_success_period"] = period
                    if is_retry:
                        self.state["retry_done_period"] = period
                    self._save_state()

                    self.runtime["last_trigger_result"] = "success" if ok else "failed"
                    self._diag(f"触发结果: {'success' if ok else 'failed'} {'' if ok else detail}")

            self._stop.wait(interval)


