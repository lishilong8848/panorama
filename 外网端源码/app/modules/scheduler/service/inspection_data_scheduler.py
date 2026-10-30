from __future__ import annotations

import copy
import threading
from datetime import datetime, timedelta

from app.config.inspection_data_config import normalize_inspection_scheduler
from app.modules.scheduler.service.apscheduler_orchestrator import (
    ApschedulerSchedulerFacade,
    _run_registered_scheduler_job,
)


class InspectionDataScheduler(ApschedulerSchedulerFacade):
    """Daily slots whose outcome follows the task completion event, not submission."""

    def __init__(self, *, lookup_job, find_slot_job=None, **kwargs):
        self.lookup_job = lookup_job
        self.find_slot_job = find_slot_job or (lambda slot, attempt_at: None)
        self._slot_lock = threading.RLock()
        self._watchers = {}
        super().__init__(**kwargs)
        for key in ("last_trigger_at", "last_trigger_result"):
            self.runtime[key] = self.state.get(key, "")

    def _normalize_cfg(self, scheduler_cfg):
        return normalize_inspection_scheduler(scheduler_cfg)

    def _default_state(self):
        return {"slots": {}, "last_success_slot": "", "last_attempt_slot": "",
                "last_trigger_at": "", "last_trigger_result": ""}

    def _load_state(self):
        state = self.state_repository.load(self.state_path, self._default_state())
        if not isinstance(state.get("slots"), dict):
            state["slots"] = {}
        return state

    def reconfigure(self, scheduler_cfg):
        cfg = self._normalize_cfg(scheduler_cfg)
        with self._slot_lock:
            self.stop()
            self.cfg = cfg
            self.enabled = cfg["enabled"]
            self.auto_start_in_gui = cfg["auto_start_in_gui"]
            # Keep slot history and task ids; start reconnects only retained slots.
            self.state_path = self._resolve_state_path(cfg["state_file"])
            self._expire_obsolete_slots(datetime.now())
            self._save_state()

    def _save_state(self):
        # A failed checkpoint must stop dispatch, not permit duplicate uploads.
        self.state_repository.save(self.state_path, self.state)

    def is_running(self):
        return self._active

    def _expire_obsolete_slots(self, now):
        today = f"{now:%Y-%m-%d}"
        cutoff = f"{now - timedelta(days=1):%Y-%m-%d}"
        changed = False
        for slot, row in list(self.state["slots"].items()):
            obsolete = slot[:10] != today or slot[11:] not in self.cfg["run_times"]
            if obsolete:
                for kind in ("retry", "catchup"):
                    self.orchestrator.remove_job(f"{self.scheduler_key}:{kind}:{slot}")
                watcher = self._watchers.pop(slot, None)
                if watcher:
                    watcher[0].set()
                if row.get("status") in {"submitted", "dispatching", "deferred", "failed"}:
                    row.update(status="expired", detail="past_day" if slot[:10] != today else "slot_removed")
                    changed = True
            if slot[:10] < cutoff:
                del self.state["slots"][slot]
                changed = True
        if changed:
            self._save_state()

    def next_run_text(self):
        if not self._active:
            return ""
        now = datetime.now()
        candidates = []
        for run_time in self.cfg["run_times"]:
            scheduled = datetime.fromisoformat(f"{now:%Y-%m-%d} {run_time}")
            candidates.append(scheduled if scheduled > now else scheduled + timedelta(days=1))
        for slot, row in self.state["slots"].items():
            job = self.orchestrator.get_job(f"{self.scheduler_key}:retry:{slot}")
            next_run = getattr(job, "next_run_time", None)
            if isinstance(next_run, datetime):
                candidates.append(next_run.replace(tzinfo=None))
        return min(candidates).strftime("%Y-%m-%d %H:%M:%S") if candidates else ""

    def start(self):
        from apscheduler.triggers.cron import CronTrigger

        with self._slot_lock:
            if not self.cfg["run_times"]:
                raise ValueError("请先配置至少一个巡检调度时间")
            if self._active:
                return {"started": False, "running": True, "reason": "already_running"}
            self.enabled = self.cfg["enabled"] = True
            self._active = True
            self.started_at = datetime.now()
            self.runtime["started_at"] = f"{self.started_at:%Y-%m-%d %H:%M:%S}"
            try:
                self._expire_obsolete_slots(self.started_at)
                for run_time in self.cfg["run_times"]:
                    hour, minute, second = map(int, run_time.split(":"))
                    self.orchestrator.add_job(
                        job_id=f"{self.scheduler_key}:slot:{run_time}", name=self.source_name,
                        trigger=CronTrigger(hour=hour, minute=minute, second=second,
                                            timezone=self.orchestrator.timezone),
                        func=_run_registered_scheduler_job,
                        args=[self.scheduler_key, f"slot:{run_time}"],
                    )
                for slot, row in list(self.state["slots"].items()):
                    if row.get("status") in {"submitted", "dispatching"}:
                        job = (self.lookup_job(row["job_id"]) if row.get("job_id") else
                               self.find_slot_job(slot, row.get("attempt_started_at", slot)))
                        if job is not None:
                            self._observe_job(slot, job)
                        elif row.get("status") == "dispatching":
                            row.update(status="deferred", attempts=max(0, row.get("attempts", 1) - 1))
                            self._save_state()
                            self._schedule_retry(slot)
                        else:
                            self._finish(slot, "failed", "任务状态不可恢复")
                    elif row.get("status") in {"failed", "deferred"}:
                        self._schedule_retry(slot)
                if self.cfg["catch_up_if_missed"]:
                    for run_time in self.cfg["run_times"]:
                        slot = f"{self.started_at:%Y-%m-%d} {run_time}"
                        if datetime.fromisoformat(slot) <= self.started_at and slot not in self.state["slots"]:
                            self._schedule_date(slot, "catchup", self.started_at + timedelta(seconds=1))
            except Exception:
                self.stop()
                raise
            self.runtime["last_decision"] = "scheduled"
            return {"started": True, "running": True, "reason": "started"}

    def stop(self):
        with self._slot_lock:
            self._active = False
            self.orchestrator.remove_jobs_by_prefix(f"{self.scheduler_key}:")
            for stopped, _thread in self._watchers.values():
                stopped.set()
            self._watchers.clear()
            self.runtime["last_decision"] = "skip:stopped"
            return {"stopped": True, "running": False, "reason": "stopped"}

    def _schedule_date(self, slot, kind, when):
        self.orchestrator.add_date_job(
            job_id=f"{self.scheduler_key}:{kind}:{slot}", name=f"{self.source_name}-{kind}",
            run_date=when, func=_run_registered_scheduler_job,
            args=[self.scheduler_key, f"{kind}:{slot}"],
        )

    def _schedule_retry(self, slot):
        row = self.state["slots"][slot]
        now = datetime.now()
        when = now + timedelta(seconds=max(60, self.cfg["check_interval_sec"]))
        eligible = row.get("status") == "deferred" or (
            self.cfg["retry_failed_in_same_period"] and row.get("status") == "failed"
            and row.get("attempts", 0) < 2
        )
        if (self._active and eligible
                and slot[:10] == f"{when:%Y-%m-%d}"
                and slot[11:] in self.cfg["run_times"]):
            self._schedule_date(slot, "retry", when)
        elif eligible and slot[:10] != f"{when:%Y-%m-%d}":
            row.update(status="expired", detail="past_day")
            self._save_state()

    def _finish(self, slot, status, detail=""):
        with self._slot_lock:
            row = self.state["slots"][slot]
            row.update(status=status, detail=detail, finished_at=f"{datetime.now():%Y-%m-%d %H:%M:%S}")
            if status == "success":
                self.state["last_success_slot"] = slot
            self.state["last_trigger_result"] = status
            self.runtime["last_trigger_result"] = status
            self._save_state()
            self._schedule_retry(slot)

    @staticmethod
    def _job_result_status(job):
        status = str(job.get("status", "") if isinstance(job, dict) else job.status)
        result = job.get("result") if isinstance(job, dict) else getattr(job, "result", None)
        if isinstance(result, dict) and result.get("dry_run") is True:
            return "skipped"
        if isinstance(result, dict) and result.get("status") not in (None, "success"):
            status = str(result.get("status"))
        return status

    def _observe_job(self, slot, job):
        row = self.state["slots"][slot]
        row.update(status="submitted", job_id=job["job_id"] if isinstance(job, dict) else job.job_id)
        self._save_state()
        if isinstance(job, dict):
            status = self._job_result_status(job)
            if job.get("status") in {"queued", "running", "waiting_resource", "waiting"}:
                raise RuntimeError("巡检任务尚未由任务引擎恢复，暂不重复提交")
            self._finish(slot, "success" if status == "success" else "failed", status)
        else:
            self._watch_job(slot, job)

    def _watch_job(self, slot, job):
        if slot in self._watchers:
            return
        stopped = threading.Event()

        def complete():
            try:
                while not stopped.is_set():
                    if job.done_event.wait(0.2):
                        break
                with self._slot_lock:
                    if stopped.is_set():
                        return
                    status = self._job_result_status(job)
                    if job.status not in {"queued", "running", "waiting_resource", "waiting"}:
                        self._finish(slot, "success" if status == "success" else "failed", status)
            except Exception as exc:
                self._log(f"记录任务结果失败: {exc}")
            finally:
                with self._slot_lock:
                    if self._watchers.get(slot, (None,))[0] is stopped:
                        self._watchers.pop(slot, None)

        thread = threading.Thread(target=complete, name="inspection-slot-completion", daemon=True)
        self._watchers[slot] = (stopped, thread)
        thread.start()

    def _fire(self, *, trigger_source):
        with self._slot_lock:
            now = datetime.now()
            self.runtime["last_check_at"] = f"{now:%Y-%m-%d %H:%M:%S}"
            if not self._active or not self.enabled:
                return
            self._expire_obsolete_slots(now)
            kind, value = trigger_source.split(":", 1)
            slot = f"{now:%Y-%m-%d} {value}" if kind == "slot" else value
            rows = self.state["slots"]
            if slot[:10] != f"{now:%Y-%m-%d}" or slot[11:] not in self.cfg["run_times"]:
                return
            row = rows.get(slot, {})
            if row.get("status") in {"submitted", "dispatching", "success", "expired"} or row.get("attempts", 0) >= 2:
                return
            if row.get("status") == "failed" and not self.cfg["retry_failed_in_same_period"]:
                return
            cutoff = f"{now - timedelta(days=1):%Y-%m-%d}"
            self.state["slots"] = rows = {key: item for key, item in rows.items()
                                           if key[:10] >= cutoff or item.get("status") in {"submitted", "deferred"}}
            busy = any(item.get("status") == "submitted" for item in rows.values()) or self.is_busy()
            if busy:
                rows[slot] = {**row, "attempts": row.get("attempts", 0), "status": "deferred"}
                self.runtime["last_decision"] = "skip:busy"
                self._save_state()
                self._schedule_retry(slot)
                return
            rows[slot] = {"attempts": row.get("attempts", 0) + 1, "status": "dispatching",
                          "attempt_started_at": f"{now:%Y-%m-%d %H:%M:%S}"}
            self.state["last_attempt_slot"] = slot
            self.state["last_trigger_at"] = self.runtime["last_trigger_at"] = f"{now:%Y-%m-%d %H:%M:%S}"
            self.runtime["last_decision"] = f"dispatch:{slot}"
            self._save_state()
            try:
                job = self.run_callback(slot)
            except Exception as exc:
                self._finish(slot, "failed", str(exc))
                return
            rows[slot].update(status="submitted", job_id=job.job_id)
            self.state["last_trigger_result"] = self.runtime["last_trigger_result"] = "submitted"
            try:
                self._save_state()
            finally:
                self._watch_job(slot, job)

    def get_runtime_snapshot(self):
        with self._slot_lock:
            return {
                **copy.deepcopy(self.state), **self.runtime,
                "enabled": self.enabled, "running": self.is_running(), "status": self.status_text(),
                "run_times": list(self.cfg["run_times"]), "next_run_time": self.next_run_text(),
                "state_path": str(self.state_path), "state_exists": self.state_repository.exists(self.state_path),
            }
