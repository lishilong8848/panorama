from __future__ import annotations

import threading
from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict

from app.modules.scheduler.repository.scheduler_state_repository import SchedulerStateRepository
from pipeline_utils import get_app_dir


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_time_parts(value: str, fallback: str = "00:00:00") -> tuple[int, int, int]:
    text = str(value or "").strip() or fallback
    try:
        hour, minute, second = [int(part) for part in text.split(":")]
    except Exception:
        hour, minute, second = [int(part) for part in fallback.split(":")]
    return max(0, min(hour, 23)), max(0, min(minute, 59)), max(0, min(second, 59))


def _parse_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


_SCHEDULER_FIRE_REGISTRY: Dict[str, Callable[[str], None]] = {}
_SCHEDULER_FIRE_REGISTRY_LOCK = threading.RLock()


def _run_registered_scheduler_job(scheduler_key: str, trigger_source: str = "scheduler") -> None:
    key = str(scheduler_key or "").strip()
    source = str(trigger_source or "scheduler").strip() or "scheduler"
    if not key:
        return
    with _SCHEDULER_FIRE_REGISTRY_LOCK:
        handler = _SCHEDULER_FIRE_REGISTRY.get(key)
    if callable(handler):
        handler(source)


class ApschedulerOrchestrator:
    """Single APScheduler owner for all external-side scheduler facades.

    APScheduler is imported lazily so the existing runtime dependency installer can
    install it before the scheduler is first built on a fresh machine.
    """

    def __init__(
        self,
        *,
        emit_log: Callable[[str], None],
        timezone: str = "Asia/Shanghai",
    ) -> None:
        self.emit_log = emit_log
        self.timezone = str(timezone or "Asia/Shanghai")
        self._lock = threading.RLock()
        self._scheduler: Any | None = None
        self._jobstore_path = get_app_dir() / ".runtime" / "apscheduler_jobs.sqlite3"

    def register_fire_handler(self, scheduler_key: str, handler: Callable[[str], None]) -> None:
        key = str(scheduler_key or "").strip()
        if not key or not callable(handler):
            return
        with _SCHEDULER_FIRE_REGISTRY_LOCK:
            _SCHEDULER_FIRE_REGISTRY[key] = handler

    def _ensure_scheduler(self):
        with self._lock:
            if self._scheduler is not None:
                return self._scheduler
            from apscheduler.executors.pool import ThreadPoolExecutor
            from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
            from apscheduler.schedulers.background import BackgroundScheduler

            self._jobstore_path.parent.mkdir(parents=True, exist_ok=True)
            self._scheduler = BackgroundScheduler(
                timezone=self.timezone,
                executors={"default": ThreadPoolExecutor(max_workers=12)},
                jobstores={"default": SQLAlchemyJobStore(url=f"sqlite:///{self._jobstore_path.as_posix()}")},
                job_defaults={
                    "coalesce": True,
                    "max_instances": 1,
                    "misfire_grace_time": 300,
                },
            )
            return self._scheduler

    def start(self) -> None:
        scheduler = self._ensure_scheduler()
        if not scheduler.running:
            scheduler.start(paused=False)
            self.emit_log("[调度] APScheduler 编排器已启动")

    def shutdown(self) -> None:
        with self._lock:
            scheduler = self._scheduler
            if scheduler is None:
                return
            try:
                if scheduler.running:
                    scheduler.shutdown(wait=False)
            finally:
                self._scheduler = None
            self.emit_log("[调度] APScheduler 编排器已停止")

    def add_job(
        self,
        *,
        job_id: str,
        name: str,
        trigger: Any,
        func: Callable[[], None],
        args: list[Any] | tuple[Any, ...] | None = None,
        misfire_grace_time: int = 300,
    ) -> None:
        scheduler = self._ensure_scheduler()
        scheduler.add_job(
            func=func,
            trigger=trigger,
            args=list(args or []),
            id=job_id,
            name=name,
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=max(1, int(misfire_grace_time or 300)),
        )
        self.start()

    def add_date_job(
        self,
        *,
        job_id: str,
        name: str,
        run_date: datetime,
        func: Callable[[], None],
        args: list[Any] | tuple[Any, ...] | None = None,
        misfire_grace_time: int = 300,
    ) -> None:
        from apscheduler.triggers.date import DateTrigger

        self.add_job(
            job_id=job_id,
            name=name,
            trigger=DateTrigger(run_date=run_date),
            func=func,
            args=list(args or []),
            misfire_grace_time=misfire_grace_time,
        )

    def remove_job(self, job_id: str) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass

    def remove_jobs_by_prefix(self, prefix: str) -> None:
        scheduler = self._scheduler
        prefix_text = str(prefix or "").strip()
        if scheduler is None or not prefix_text:
            return
        try:
            jobs = list(scheduler.get_jobs())
        except Exception:
            jobs = []
        for job in jobs:
            job_id = str(getattr(job, "id", "") or "")
            if not job_id.startswith(prefix_text):
                continue
            try:
                scheduler.remove_job(job_id)
            except Exception:
                pass

    def get_job(self, job_id: str):
        scheduler = self._scheduler
        if scheduler is None:
            return None
        try:
            return scheduler.get_job(job_id)
        except Exception:
            return None

    @staticmethod
    def _datetime_text(value: Any) -> str:
        if not isinstance(value, datetime):
            return ""
        try:
            value = value.replace(tzinfo=None)
        except Exception:
            pass
        return value.strftime("%Y-%m-%d %H:%M:%S")

    def snapshot(self) -> Dict[str, Any]:
        """Return a lightweight runtime snapshot for status pages.

        This deliberately does not create the scheduler when it has not been
        initialized yet; status endpoints must stay cheap and side-effect free.
        """
        with self._lock:
            scheduler = self._scheduler
            if scheduler is None:
                return {
                    "engine": "APScheduler",
                    "ready": False,
                    "running": False,
                    "timezone": self.timezone,
                    "jobstore": "sqlalchemy_sqlite",
                    "jobstore_path": str(self._jobstore_path),
                    "job_count": 0,
                    "jobs": [],
                    "next_run_time": "",
                    "last_result": "not_initialized",
                    "checked_at": _now_text(),
                }
            try:
                jobs = list(scheduler.get_jobs())
            except Exception:
                jobs = []
            rows: list[Dict[str, Any]] = []
            next_run_candidates: list[datetime] = []
            for job in jobs:
                next_run = getattr(job, "next_run_time", None)
                if isinstance(next_run, datetime):
                    next_run_candidates.append(next_run)
                rows.append(
                    {
                        "id": str(getattr(job, "id", "") or ""),
                        "name": str(getattr(job, "name", "") or ""),
                        "next_run_time": self._datetime_text(next_run),
                    }
                )
            rows.sort(key=lambda item: (str(item.get("next_run_time", "") or "9999"), str(item.get("id", ""))))
            next_run_text = ""
            if next_run_candidates:
                next_run_text = self._datetime_text(min(next_run_candidates))
            return {
                "engine": "APScheduler",
                "ready": True,
                "running": bool(getattr(scheduler, "running", False)),
                "timezone": self.timezone,
                "jobstore": "sqlalchemy_sqlite",
                "jobstore_path": str(self._jobstore_path),
                "job_count": len(rows),
                "jobs": rows,
                "next_run_time": next_run_text,
                "last_result": "running" if bool(getattr(scheduler, "running", False)) else "initialized",
                "checked_at": _now_text(),
            }


class ApschedulerSchedulerFacade:
    def __init__(
        self,
        *,
        scheduler_key: str,
        feature: str,
        scheduler_cfg: Dict[str, Any],
        runtime_state_root: str,
        emit_log: Callable[[str], None],
        run_callback: Callable[[str], tuple[bool, str]],
        is_busy: Callable[[], bool],
        orchestrator: ApschedulerOrchestrator,
        schedule_kind: str,
        source_name: str,
    ) -> None:
        self.scheduler_key = str(scheduler_key or feature or source_name).strip()
        self.feature = str(feature or self.scheduler_key).strip()
        self.schedule_kind = str(schedule_kind or "interval").strip().lower()
        self.source_name = str(source_name or self.scheduler_key).strip()
        self.emit_log = emit_log
        self.run_callback = run_callback
        self.is_busy = is_busy
        self.orchestrator = orchestrator
        self.runtime_state_root = str(runtime_state_root or "").strip()
        self.cfg = self._normalize_cfg(scheduler_cfg)
        self.enabled = bool(self.cfg.get("enabled", False))
        self.auto_start_in_gui = bool(self.cfg.get("auto_start_in_gui", False))
        self.started_at = datetime.now()
        self.state_repository = SchedulerStateRepository()
        self.state_path = self._resolve_state_path(str(self.cfg.get("state_file", "") or ""))
        self.state = self._load_state()
        self.runtime: Dict[str, Any] = {
            "started_at": "",
            "last_check_at": "",
            "last_decision": "skip:not_started",
            "last_trigger_at": "",
            "last_trigger_result": "",
        }
        self._active = False
        self._diag_lock = threading.Lock()
        self._diag_logs: list[str] = []
        self._max_diag_logs = 200
        self.orchestrator.register_fire_handler(
            self.scheduler_key,
            lambda trigger_source: self._fire(trigger_source=trigger_source),
        )

    def _normalize_cfg(self, scheduler_cfg: Dict[str, Any]) -> Dict[str, Any]:
        raw = scheduler_cfg if isinstance(scheduler_cfg, dict) else {}
        if self.schedule_kind == "daily":
            return {
                "enabled": bool(raw.get("enabled", True)),
                "auto_start_in_gui": bool(raw.get("auto_start_in_gui", False)),
                "run_time": str(raw.get("run_time", "00:00:00") or "00:00:00").strip(),
                "check_interval_sec": max(1, int(raw.get("check_interval_sec", 30) or 30)),
                "catch_up_if_missed": bool(raw.get("catch_up_if_missed", False)),
                "retry_failed_in_same_period": bool(raw.get("retry_failed_in_same_period", False)),
                "state_file": str(raw.get("state_file", f"{self.scheduler_key}_state.json") or "").strip()
                or f"{self.scheduler_key}_state.json",
            }
        if self.schedule_kind == "monthly":
            day_of_month = max(1, min(31, int(raw.get("day_of_month", 1) or 1)))
            return {
                "enabled": bool(raw.get("enabled", False)),
                "auto_start_in_gui": bool(raw.get("auto_start_in_gui", False)),
                "day_of_month": day_of_month,
                "run_time": str(raw.get("run_time", "01:00:00") or "01:00:00").strip(),
                "check_interval_sec": max(1, int(raw.get("check_interval_sec", 30) or 30)),
                "catch_up_if_missed": bool(raw.get("catch_up_if_missed", False)),
                "state_file": str(raw.get("state_file", f"{self.scheduler_key}_state.json") or "").strip()
                or f"{self.scheduler_key}_state.json",
            }
        interval_minutes = max(1, int(raw.get("interval_minutes", 60) or 60))
        minute_offset = int(raw.get("minute_offset", raw.get("start_minute", raw.get("run_minute", 0))) or 0)
        return {
            "enabled": bool(raw.get("enabled", True)),
            "auto_start_in_gui": bool(raw.get("auto_start_in_gui", False)),
            "interval_minutes": interval_minutes,
            "minute_offset": max(0, minute_offset) % max(1, min(interval_minutes, 1440)),
            "check_interval_sec": max(1, int(raw.get("check_interval_sec", 30) or 30)),
            "retry_failed_on_next_tick": bool(raw.get("retry_failed_on_next_tick", True)),
            "align_to_wall_clock": bool(raw.get("align_to_wall_clock", True)),
            "state_file": str(raw.get("state_file", f"{self.scheduler_key}_state.json") or "").strip()
            or f"{self.scheduler_key}_state.json",
        }

    def _resolve_state_path(self, state_file: str) -> Path:
        path = Path(str(state_file or "").strip() or f"{self.scheduler_key}_state.json")
        if path.is_absolute():
            return path
        root = Path(self.runtime_state_root) if self.runtime_state_root else get_app_dir()
        if not root.is_absolute():
            root = get_app_dir() / root
        root.mkdir(parents=True, exist_ok=True)
        return root / path

    def _default_state(self) -> Dict[str, Any]:
        if self.schedule_kind == "interval":
            return {
                "last_attempt_at": "",
                "last_success_at": "",
                "last_status": "",
                "last_error": "",
                "last_source": "",
                "last_duration_ms": 0,
            }
        if self.schedule_kind == "monthly":
            return {
                "last_success_period": "",
                "last_attempt_period": "",
                "last_run_at": "",
                "last_status": "",
                "last_error": "",
            }
        return {
            "last_success_period": "",
            "last_attempt_period": "",
            "last_run_at": "",
            "last_status": "",
            "last_error": "",
            "retry_done_period": "",
        }

    def _load_state(self) -> Dict[str, Any]:
        default = self._default_state()
        payload = self.state_repository.load(self.state_path, default)
        state = dict(default)
        if isinstance(payload, dict):
            for key, default_value in default.items():
                value = payload.get(key, default_value)
                if key == "last_duration_ms":
                    try:
                        state[key] = max(0, int(value or 0))
                    except Exception:
                        state[key] = 0
                else:
                    state[key] = str(value or "")
        return state

    def _save_state(self) -> None:
        try:
            self.state_repository.save(self.state_path, self.state)
        except Exception as exc:
            self._log(f"保存状态失败: {exc}")

    def _log(self, text: str) -> None:
        line = f"[{self.source_name}调度] {text}"
        self.emit_log(line)
        with self._diag_lock:
            self._diag_logs.append(line)
            overflow = len(self._diag_logs) - self._max_diag_logs
            if overflow > 0:
                del self._diag_logs[:overflow]

    def _period(self, value: datetime) -> str:
        return value.strftime("%Y-%m" if self.schedule_kind == "monthly" else "%Y-%m-%d")

    def _daily_scheduled_at(self, value: datetime) -> datetime:
        hour, minute, second = _parse_time_parts(str(self.cfg.get("run_time", "")), "00:00:00")
        return datetime(value.year, value.month, value.day, hour, minute, second)

    def _monthly_scheduled_at(self, value: datetime) -> datetime:
        day = min(int(self.cfg.get("day_of_month", 1) or 1), monthrange(value.year, value.month)[1])
        hour, minute, second = _parse_time_parts(str(self.cfg.get("run_time", "")), "01:00:00")
        return datetime(value.year, value.month, day, hour, minute, second)

    def _next_monthly_run_after(self, value: datetime) -> datetime:
        current = value
        for _ in range(14):
            scheduled = self._monthly_scheduled_at(current)
            period = self._period(scheduled)
            if scheduled > value and self.state.get("last_success_period", "") != period:
                return scheduled
            year = current.year + 1 if current.month == 12 else current.year
            month = 1 if current.month == 12 else current.month + 1
            current = datetime(year, month, 1)
        return self._monthly_scheduled_at(value + timedelta(days=32))

    def _next_interval_run_after(self, value: datetime) -> datetime:
        current = value.replace(microsecond=0)
        interval = max(1, int(self.cfg.get("interval_minutes", 60) or 60))
        offset = max(0, int(self.cfg.get("minute_offset", 0) or 0))
        if not bool(self.cfg.get("align_to_wall_clock", True)):
            last_attempt = _parse_datetime(str(self.state.get("last_attempt_at", "") or ""))
            if last_attempt is not None:
                return last_attempt + timedelta(minutes=interval)
            return current + timedelta(minutes=interval)
        day_start = current.replace(hour=0, minute=0, second=0)
        if interval >= 1440:
            anchor = day_start + timedelta(minutes=offset % 1440)
            return anchor if current < anchor else anchor + timedelta(days=1)
        anchor = day_start + timedelta(minutes=offset % interval)
        if current < anchor:
            return anchor
        elapsed_minutes = int((current - anchor).total_seconds() // 60)
        next_slot = anchor + timedelta(minutes=((elapsed_minutes // interval) + 1) * interval)
        return next_slot

    def _should_daily_catch_up(self, now: datetime) -> bool:
        if self.schedule_kind != "daily" or not bool(self.cfg.get("catch_up_if_missed", False)):
            return False
        period = self._period(now)
        if self.state.get("last_success_period", "") == period:
            return False
        if now < self._daily_scheduled_at(now):
            return False
        attempted_today = self.state.get("last_attempt_period", "") == period
        failed_today = self.state.get("last_status", "") == "failed"
        if not attempted_today:
            return True
        return bool(failed_today and self.cfg.get("retry_failed_in_same_period", False) and self.state.get("retry_done_period", "") != period)

    def _should_monthly_catch_up(self, now: datetime) -> bool:
        if self.schedule_kind != "monthly" or not bool(self.cfg.get("catch_up_if_missed", False)):
            return False
        period = self._period(now)
        if self.state.get("last_success_period", "") == period:
            return False
        if now < self._monthly_scheduled_at(now):
            return False
        return self.state.get("last_attempt_period", "") != period

    def _build_trigger(self):
        if self.schedule_kind == "daily":
            from apscheduler.triggers.cron import CronTrigger

            hour, minute, second = _parse_time_parts(str(self.cfg.get("run_time", "")), "00:00:00")
            return CronTrigger(hour=hour, minute=minute, second=second, timezone=self.orchestrator.timezone)
        if self.schedule_kind == "monthly":
            from apscheduler.triggers.date import DateTrigger

            return DateTrigger(run_date=self._next_monthly_run_after(datetime.now()))
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger

        interval = max(1, int(self.cfg.get("interval_minutes", 60) or 60))
        offset = max(0, int(self.cfg.get("minute_offset", 0) or 0))
        if bool(self.cfg.get("align_to_wall_clock", True)):
            if interval >= 1440:
                return CronTrigger(hour=0, minute=offset % 60, second=0, timezone=self.orchestrator.timezone)
            if interval == 60:
                return CronTrigger(minute=offset % 60, second=0, timezone=self.orchestrator.timezone)
            if interval < 60 and 60 % interval == 0:
                minute_values = ",".join(str(item) for item in range(offset % interval, 60, interval))
                return CronTrigger(minute=minute_values, second=0, timezone=self.orchestrator.timezone)
        return IntervalTrigger(minutes=interval, start_date=self._next_interval_run_after(datetime.now()))

    def _schedule_main_job(self) -> None:
        self.orchestrator.add_job(
            job_id=self.scheduler_key,
            name=self.source_name,
            trigger=self._build_trigger(),
            func=_run_registered_scheduler_job,
            args=[self.scheduler_key, "scheduler"],
            misfire_grace_time=max(60, int(self.cfg.get("check_interval_sec", 30) or 30) * 3),
        )

    def _schedule_catch_up_if_needed(self) -> None:
        now = datetime.now()
        if not (self._should_daily_catch_up(now) or self._should_monthly_catch_up(now)):
            return
        self.orchestrator.remove_jobs_by_prefix(f"{self.scheduler_key}:catchup:")
        job_id = f"{self.scheduler_key}:catchup:{now.strftime('%Y%m%d%H%M%S')}"
        self.orchestrator.add_date_job(
            job_id=job_id,
            name=f"{self.source_name}-启动补偿",
            run_date=now + timedelta(seconds=1),
            func=_run_registered_scheduler_job,
            args=[self.scheduler_key, "catch_up"],
            misfire_grace_time=max(60, int(self.cfg.get("check_interval_sec", 30) or 30) * 3),
        )
        self._log("已登记启动补偿检查任务")

    def is_running(self) -> bool:
        return self._active and self.orchestrator.get_job(self.scheduler_key) is not None

    def status_text(self) -> str:
        return "运行中" if self.is_running() else "未启动"

    def next_run_time(self, now: datetime | None = None) -> datetime:
        job = self.orchestrator.get_job(self.scheduler_key)
        next_run = getattr(job, "next_run_time", None) if job is not None else None
        if isinstance(next_run, datetime):
            return next_run.replace(tzinfo=None)
        current = now or datetime.now()
        if self.schedule_kind == "monthly":
            return self._next_monthly_run_after(current)
        if self.schedule_kind == "daily":
            scheduled = self._daily_scheduled_at(current)
            return scheduled if current < scheduled else self._daily_scheduled_at(current + timedelta(days=1))
        return self._next_interval_run_after(current)

    def next_run_text(self) -> str:
        if not self._active:
            return ""
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
        self.runtime["last_decision"] = "scheduled"
        self._active = True
        self._schedule_main_job()
        self._schedule_catch_up_if_needed()
        self._log(f"调度已启动，next_run={self.next_run_text() or '-'}")
        return {"started": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        self._active = False
        self.orchestrator.remove_job(self.scheduler_key)
        self.orchestrator.remove_jobs_by_prefix(f"{self.scheduler_key}:catchup:")
        self.runtime["last_decision"] = "skip:stopped"
        self._log("调度已停止")
        return {"stopped": True, "running": False, "reason": "stopped"}

    def _period_allows_fire(self, now: datetime) -> tuple[bool, str]:
        if self.schedule_kind == "interval":
            return True, ""
        period = self._period(now)
        if self.state.get("last_success_period", "") == period:
            return False, "already_success_period"
        if self.state.get("last_attempt_period", "") == period and self.schedule_kind == "monthly":
            return False, "already_attempted_this_month"
        if self.schedule_kind == "daily" and self.state.get("last_attempt_period", "") == period:
            failed_today = self.state.get("last_status", "") == "failed"
            retry_allowed = bool(self.cfg.get("retry_failed_in_same_period", False))
            if not (failed_today and retry_allowed and self.state.get("retry_done_period", "") != period):
                return False, "already_attempted_no_retry"
        return True, period

    def _record_fire_state(self, *, now: datetime, status: str, detail: str, duration_ms: int) -> None:
        if self.schedule_kind == "interval":
            self.state["last_attempt_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            self.state["last_status"] = status
            self.state["last_error"] = "" if status in {"submitted", "skipped", "accepted", "success"} else detail
            self.state["last_source"] = self.source_name
            self.state["last_duration_ms"] = max(0, int(duration_ms or 0))
            if status in {"submitted", "accepted", "success"}:
                self.state["last_success_at"] = self.state["last_attempt_at"]
        else:
            period = self._period(now)
            was_retry = (
                self.schedule_kind == "daily"
                and self.state.get("last_attempt_period", "") == period
                and self.state.get("last_status", "") == "failed"
            )
            self.state["last_attempt_period"] = period
            self.state["last_run_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            self.state["last_status"] = status
            self.state["last_error"] = "" if status in {"submitted", "skipped", "accepted", "success"} else detail
            if status in {"accepted", "success", "ok"}:
                self.state["last_success_period"] = period
            elif was_retry:
                self.state["retry_done_period"] = period
        self._save_state()

    def _fire(self, *, trigger_source: str) -> None:
        now = datetime.now()
        self.runtime["last_check_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        self.runtime["last_trigger_at"] = self.runtime["last_check_at"]
        started = datetime.now()
        status = "submitted"
        detail = ""
        try:
            if not self.enabled:
                status = "skipped"
                detail = "skip:disabled"
                self.runtime["last_decision"] = "skip:disabled"
                return
            allowed, reason_or_period = self._period_allows_fire(now)
            if not allowed:
                status = "skipped"
                detail = f"skip:{reason_or_period}"
                self.runtime["last_decision"] = detail
                return
            if callable(self.is_busy) and self.is_busy():
                status = "skipped"
                detail = "skip:busy"
                self.runtime["last_decision"] = "skip:busy"
                self._log("本次触发跳过: 当前同功能任务运行中")
                return
            self.runtime["last_decision"] = f"run:{trigger_source}"
            ok, callback_detail = self.run_callback(self.source_name)
            detail = str(callback_detail or "").strip()
            if ok:
                status = "submitted"
            elif detail.lower().startswith("skip:"):
                status = "skipped"
            else:
                status = "failed"
        except Exception as exc:
            status = "failed"
            detail = str(exc)
            self._log(f"触发失败: {detail}")
        finally:
            finished = datetime.now()
            duration_ms = int((finished - started).total_seconds() * 1000)
            self.runtime["last_trigger_result"] = status
            if detail and status != "submitted":
                self.runtime["last_trigger_result"] = f"{status}:{detail[:120]}"
            skip_detail = str(detail or "").strip().lower()
            should_record_state = not (
                status == "skipped"
                and (
                    skip_detail in {"skip:busy", "skip:disabled"}
                    or skip_detail.startswith("skip:already_")
                )
            )
            if should_record_state:
                self._record_fire_state(now=finished, status=status, detail=detail, duration_ms=duration_ms)
            if self.schedule_kind == "monthly" and self._active:
                self._schedule_main_job()

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
        attempt_text = attempt.strftime("%Y-%m-%d %H:%M:%S")
        success_statuses = {"ok", "success", "accepted"}
        if self.schedule_kind == "interval":
            self.state["last_attempt_at"] = attempt_text
            self.state["last_status"] = normalized_status
            self.state["last_error"] = "" if normalized_status in (success_statuses | {"skipped"}) else str(detail or "").strip()
            self.state["last_source"] = str(source or "").strip()
            self.state["last_duration_ms"] = max(0, int(duration_ms or 0))
            if normalized_status in success_statuses:
                self.state["last_success_at"] = self.state["last_attempt_at"]
        else:
            period = self._period(attempt)
            self.state["last_attempt_period"] = period
            self.state["last_run_at"] = attempt_text
            self.state["last_status"] = normalized_status
            self.state["last_error"] = "" if normalized_status in (success_statuses | {"skipped"}) else str(detail or "").strip()
            if normalized_status in success_statuses:
                self.state["last_success_period"] = period
            elif self.state.get("last_success_period", "") == period:
                self.state["last_success_period"] = ""
        self._save_state()
        self.runtime["last_trigger_at"] = attempt_text
        self.runtime["last_trigger_result"] = normalized_status

    def reset_today_state_for_run_time_change(self, now: datetime | None = None) -> Dict[str, Any]:
        current = now or datetime.now()
        period = current.strftime("%Y-%m-%d")
        reset_keys: list[str] = []
        for key in ("last_success_period", "last_attempt_period", "retry_done_period"):
            if self.state.get(key, "") == period:
                self.state[key] = ""
                reset_keys.append(key)
        for key in ("last_status", "last_error", "last_run_at"):
            if self.state.get(key, ""):
                self.state[key] = ""
                reset_keys.append(key)
        if reset_keys:
            self._save_state()
        return {"changed": bool(reset_keys), "period": period, "reset_keys": reset_keys, "state_path": str(self.state_path)}

    def reset_current_month_state_for_schedule_change(self, now: datetime | None = None) -> Dict[str, Any]:
        current = now or datetime.now()
        period = current.strftime("%Y-%m")
        reset_keys: list[str] = []
        for key in ("last_success_period", "last_attempt_period"):
            if self.state.get(key, "") == period:
                self.state[key] = ""
                reset_keys.append(key)
        for key in ("last_status", "last_error", "last_run_at"):
            if self.state.get(key, ""):
                self.state[key] = ""
                reset_keys.append(key)
        if reset_keys:
            self._save_state()
        return {"changed": bool(reset_keys), "period": period, "reset_keys": reset_keys, "state_path": str(self.state_path)}

    def get_runtime_snapshot(self) -> Dict[str, Any]:
        payload = {
            "running": self.is_running(),
            "status": self.status_text(),
            "started_at": str(self.runtime.get("started_at", "")),
            "last_check_at": str(self.runtime.get("last_check_at", "")),
            "last_decision": str(self.runtime.get("last_decision", "")),
            "last_trigger_at": str(self.runtime.get("last_trigger_at", "")),
            "last_trigger_result": str(self.runtime.get("last_trigger_result", "")),
            "next_run_time": self.next_run_text(),
            "state_path": str(self.state_path),
            "state_exists": self.state_repository.exists(self.state_path),
            **dict(self.state),
        }
        if self.schedule_kind == "interval":
            payload["interval_minutes"] = int(self.cfg.get("interval_minutes", 0) or 0)
            payload["minute_offset"] = int(self.cfg.get("minute_offset", 0) or 0)
            payload["check_interval_sec"] = int(self.cfg.get("check_interval_sec", 0) or 0)
        return payload

    def get_diagnostics(self, limit: int = 50) -> Dict[str, Any]:
        with self._diag_lock:
            logs = list(self._diag_logs[-max(1, int(limit)):])
        return {
            "config": dict(self.cfg),
            "runtime": self.get_runtime_snapshot(),
            "state": dict(self.state),
            "logs": logs,
        }


class ApschedulerHandoverSchedulerManager:
    def __init__(
        self,
        *,
        config: Dict[str, Any],
        emit_log: Callable[[str], None],
        run_callback: Callable[[str, str], tuple[bool, str]] | None,
        is_busy: Callable[[], bool],
        orchestrator: ApschedulerOrchestrator,
    ) -> None:
        self._full_config = config if isinstance(config, dict) else {}
        self._emit_log = emit_log
        self._run_callback = run_callback
        self._is_busy = is_busy
        self._orchestrator = orchestrator
        self._cfg = self._normalize_cfg(self._full_config)
        self.enabled = bool(self._cfg.get("enabled", True))
        self.auto_start_in_gui = bool(self._cfg.get("auto_start_in_gui", False))
        self.schedulers: Dict[str, ApschedulerSchedulerFacade] = {}
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
            "cloud_catchup_morning_time": str(raw.get("cloud_catchup_morning_time", "08:00:00")).strip() or "08:00:00",
            "cloud_catchup_afternoon_time": str(raw.get("cloud_catchup_afternoon_time", "17:30:00")).strip() or "17:30:00",
            "station_110_review_link_enabled": bool(raw.get("station_110_review_link_enabled", True)),
            "station_110_midnight_time": str(raw.get("station_110_midnight_time", "00:00:00")).strip() or "00:00:00",
            "station_110_noon_time": str(raw.get("station_110_noon_time", "12:00:00")).strip() or "12:00:00",
            "check_interval_sec": int(raw.get("check_interval_sec", 30) or 30),
            "catch_up_if_missed": bool(raw.get("catch_up_if_missed", False)),
            "retry_failed_in_same_period": bool(raw.get("retry_failed_in_same_period", False)),
            "morning_state_file": str(raw.get("morning_state_file", "handover_scheduler_morning_state.json")).strip()
            or "handover_scheduler_morning_state.json",
            "afternoon_state_file": str(raw.get("afternoon_state_file", "handover_scheduler_afternoon_state.json")).strip()
            or "handover_scheduler_afternoon_state.json",
            "cloud_catchup_morning_state_file": str(raw.get("cloud_catchup_morning_state_file", "handover_cloud_catchup_morning_state.json")).strip()
            or "handover_cloud_catchup_morning_state.json",
            "cloud_catchup_afternoon_state_file": str(raw.get("cloud_catchup_afternoon_state_file", "handover_cloud_catchup_afternoon_state.json")).strip()
            or "handover_cloud_catchup_afternoon_state.json",
            "station_110_midnight_state_file": str(raw.get("station_110_midnight_state_file", "handover_scheduler_110_midnight_state.json")).strip()
            or "handover_scheduler_110_midnight_state.json",
            "station_110_noon_state_file": str(raw.get("station_110_noon_state_file", "handover_scheduler_110_noon_state.json")).strip()
            or "handover_scheduler_110_noon_state.json",
        }

    def _slot_callback(self, slot: str) -> Callable[[str], tuple[bool, str]]:
        def _callback(source: str) -> tuple[bool, str]:
            if not callable(self._run_callback):
                return False, "交接班调度回调尚未绑定执行器"
            return self._run_callback(slot, source)

        return _callback

    def _slot_cfg(self, *, run_time: str, state_file: str) -> Dict[str, Any]:
        return {
            "enabled": bool(self._cfg["enabled"]),
            "auto_start_in_gui": bool(self._cfg["auto_start_in_gui"]),
            "run_time": run_time,
            "check_interval_sec": int(self._cfg["check_interval_sec"]),
            "catch_up_if_missed": bool(self._cfg["catch_up_if_missed"]),
            "retry_failed_in_same_period": bool(self._cfg["retry_failed_in_same_period"]),
            "state_file": state_file,
        }

    def _runtime_state_root(self) -> str:
        paths_cfg = self._full_config.get("paths", {}) if isinstance(self._full_config.get("paths", {}), dict) else {}
        return str(paths_cfg.get("runtime_state_root", "") or "").strip() or "runtime_state"

    def _build_schedulers(self) -> None:
        specs = {
            "morning": ("交接班日志定时生成（上午）", self._cfg["morning_time"], self._cfg["morning_state_file"]),
            "afternoon": ("交接班日志定时生成（下午）", self._cfg["afternoon_time"], self._cfg["afternoon_state_file"]),
        }
        if bool(self._cfg.get("cloud_catchup_enabled", True)):
            specs["cloud_catchup_morning"] = (
                "交接班定时确认并上传云文档（8点）",
                self._cfg["cloud_catchup_morning_time"],
                self._cfg["cloud_catchup_morning_state_file"],
            )
            specs["cloud_catchup_afternoon"] = (
                "交接班定时确认并上传云文档（17点30）",
                self._cfg["cloud_catchup_afternoon_time"],
                self._cfg["cloud_catchup_afternoon_state_file"],
            )
        if bool(self._cfg.get("station_110_review_link_enabled", True)):
            specs["station_110_midnight"] = (
                "110站审核链接定时发送（0点）",
                self._cfg["station_110_midnight_time"],
                self._cfg["station_110_midnight_state_file"],
            )
            specs["station_110_noon"] = (
                "110站审核链接定时发送（12点）",
                self._cfg["station_110_noon_time"],
                self._cfg["station_110_noon_state_file"],
            )
        root = self._runtime_state_root()
        self.schedulers = {
            slot: ApschedulerSchedulerFacade(
                scheduler_key=f"handover_{slot}",
                feature="handover",
                scheduler_cfg=self._slot_cfg(run_time=str(run_time), state_file=str(state_file)),
                runtime_state_root=root,
                emit_log=self._emit_log,
                run_callback=self._slot_callback(slot),
                is_busy=self._is_busy,
                orchestrator=self._orchestrator,
                schedule_kind="daily",
                source_name=str(source_name),
            )
            for slot, (source_name, run_time, state_file) in specs.items()
        }

    def set_run_callback(self, callback: Callable[[str, str], tuple[bool, str]]) -> None:
        self._run_callback = callback
        for slot, scheduler in self.schedulers.items():
            scheduler.run_callback = self._slot_callback(slot)

    def is_running(self) -> bool:
        return any(scheduler.is_running() for scheduler in self.schedulers.values())

    def status_text(self) -> str:
        return "运行中" if self.is_running() else "未启动"

    def start(self) -> Dict[str, Any]:
        if not bool(self._cfg.get("enabled", True)):
            self._cfg["enabled"] = True
            self.enabled = True
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
        action["reason"] = "started" if all_started else "partial_started" if any_running else "already_running"
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
            "slots": {slot: scheduler.get_diagnostics(limit=limit) for slot, scheduler in self.schedulers.items()},
        }
