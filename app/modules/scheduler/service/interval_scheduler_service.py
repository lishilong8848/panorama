from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict

from pipeline_utils import get_app_dir


class IntervalSchedulerService:
    def __init__(
        self,
        *,
        scheduler_cfg: Dict[str, Any],
        runtime_state_root: str,
        emit_log: Callable[[str], None],
        run_callback: Callable[[str], tuple[bool, str]],
        is_busy: Callable[[], bool],
        thread_name: str = "interval-scheduler",
        source_name: str = "间隔调度",
    ) -> None:
        self.cfg = self._normalize_cfg(scheduler_cfg)
        self.enabled = bool(self.cfg["enabled"])
        self.auto_start_in_gui = bool(self.cfg["auto_start_in_gui"])
        self.emit_log = emit_log
        self.run_callback = run_callback
        self.is_busy = is_busy
        self.thread_name = str(thread_name or "interval-scheduler")
        self.source_name = str(source_name or "间隔调度")
        self.runtime_state_root = str(runtime_state_root or "").strip()

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
        interval_minutes = int(raw.get("interval_minutes", 60) or 60)
        check_interval_sec = int(raw.get("check_interval_sec", 30) or 30)
        state_file = str(raw.get("state_file", "")).strip() or "interval_scheduler_state.json"
        if interval_minutes < 1:
            raise ValueError("配置错误: interval_minutes 必须大于等于1")
        if check_interval_sec < 1:
            raise ValueError("配置错误: check_interval_sec 必须大于0")
        return {
            "enabled": bool(raw.get("enabled", True)),
            "auto_start_in_gui": bool(raw.get("auto_start_in_gui", False)),
            "interval_minutes": interval_minutes,
            "check_interval_sec": check_interval_sec,
            "retry_failed_on_next_tick": bool(raw.get("retry_failed_on_next_tick", True)),
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

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _log(self, text: str) -> None:
        self.emit_log(f"[{self.source_name}] {text}")

    def _load_state(self) -> Dict[str, Any]:
        default = {
            "last_attempt_at": "",
            "last_success_at": "",
            "last_status": "",
            "last_error": "",
            "last_source": "",
            "last_duration_ms": 0,
        }
        if not self.state_path.exists():
            return default
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return default
        if not isinstance(data, dict):
            return default
        state = dict(default)
        for key in state:
            if key == "last_duration_ms":
                try:
                    state[key] = max(0, int(data.get(key, 0) or 0))
                except Exception:
                    state[key] = 0
            else:
                state[key] = str(data.get(key, "") or "")
        return state

    def _save_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            self._log(f"保存状态失败: {exc}")

    @staticmethod
    def _parse_time(text: str) -> datetime | None:
        raw = str(text or "").strip()
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status_text(self) -> str:
        return "运行中" if self.is_running() else "未启动"

    def next_run_time(self, now: datetime | None = None) -> datetime:
        interval = timedelta(minutes=int(self.cfg["interval_minutes"]))
        last_attempt = self._parse_time(str(self.state.get("last_attempt_at", "")))
        if last_attempt is not None:
            return last_attempt + interval
        return (now or self.started_at) + interval if not self.runtime.get("started_at") else self.started_at + interval

    def next_run_text(self) -> str:
        return self.next_run_time().strftime("%Y-%m-%d %H:%M:%S")

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
        self._log(f"调度已启动，间隔={self.cfg['interval_minutes']}分钟")
        return {"started": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None
        self.runtime["last_decision"] = "skip:stopped"
        self._log("调度已停止")
        return {"stopped": True, "running": False, "reason": "stopped"}

    def record_external_run(
        self,
        *,
        status: str,
        source: str,
        detail: str = "",
        duration_ms: int = 0,
        attempt_at: datetime | None = None,
    ) -> None:
        attempt = attempt_at or datetime.now()
        normalized_status = str(status or "").strip().lower() or "unknown"
        success_like = {"ok", "success", "skipped"}
        self.state["last_attempt_at"] = attempt.strftime("%Y-%m-%d %H:%M:%S")
        self.state["last_status"] = normalized_status
        self.state["last_error"] = "" if normalized_status in success_like else str(detail or "").strip()
        self.state["last_source"] = str(source or "").strip()
        self.state["last_duration_ms"] = max(0, int(duration_ms or 0))
        if normalized_status in success_like:
            self.state["last_success_at"] = self.state["last_attempt_at"]
        self._save_state()
        self.runtime["last_trigger_at"] = self.state["last_attempt_at"]
        self.runtime["last_trigger_result"] = normalized_status

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
            "last_success_at": str(self.state.get("last_success_at", "")),
            "last_status": str(self.state.get("last_status", "")),
            "last_error": str(self.state.get("last_error", "")),
            "last_source": str(self.state.get("last_source", "")),
            "last_duration_ms": int(self.state.get("last_duration_ms", 0) or 0),
        }

    def _loop(self) -> None:
        interval_sec = int(self.cfg["check_interval_sec"])
        while not self._stop.is_set():
            now = datetime.now()
            self.runtime["last_check_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            next_run = self.next_run_time(now)
            if now < next_run:
                self.runtime["last_decision"] = "skip:before_next_run"
                self._stop.wait(interval_sec)
                continue
            if self.is_busy():
                self.runtime["last_decision"] = "skip:busy"
                self.runtime["last_trigger_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
                self.runtime["last_trigger_result"] = "skip_busy"
                self._stop.wait(interval_sec)
                continue

            self.runtime["last_decision"] = "run:due"
            self.runtime["last_trigger_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            started_at = datetime.now()
            try:
                ok, detail = self.run_callback(self.source_name)
            except Exception as exc:
                ok, detail = False, str(exc)
            finished_at = datetime.now()
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)
            self.state["last_attempt_at"] = finished_at.strftime("%Y-%m-%d %H:%M:%S")
            self.state["last_status"] = "success" if ok else "failed"
            self.state["last_error"] = "" if ok else str(detail or "").strip()
            self.state["last_source"] = self.source_name
            self.state["last_duration_ms"] = duration_ms
            if ok:
                self.state["last_success_at"] = self.state["last_attempt_at"]
            self._save_state()
            self.runtime["last_trigger_result"] = self.state["last_status"]
            self._stop.wait(interval_sec)
