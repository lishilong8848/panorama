from __future__ import annotations

import contextlib
import threading
import time
import uuid
from typing import Any, Callable, Dict

from handover_log_module.core.building_title_rules import HANDOVER_BUILDINGS
from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore


class HandoverXlsxWriteQueueTimeoutError(TimeoutError):
    pass


class HandoverXlsxWriteQueueService:
    """Per-building serialized queue for handover review related xlsx writes."""

    _worker_guard = threading.Lock()
    _workers: Dict[str, threading.Thread] = {}

    def __init__(
        self,
        config: Dict[str, Any],
        *,
        emit_log: Callable[[str], None] | None = None,
        job_service: Any = None,
        parser: Any = None,
        writer: Any = None,
    ) -> None:
        self.config = config if isinstance(config, dict) else {}
        self.emit_log = emit_log if callable(emit_log) else print
        self.job_service = job_service
        self.parser = parser
        self.writer = writer

    def _store(self, building: str) -> ReviewBuildingDocumentStore:
        return ReviewBuildingDocumentStore(config=self.config, building=str(building or "").strip())

    def _start_worker(self, *, building: str) -> None:
        building_text = str(building or "").strip()
        if not building_text:
            return
        store = self._store(building_text)
        key = str(store.db_path.resolve(strict=False)).casefold()
        with self._worker_guard:
            worker = self._workers.get(key)
            if worker is not None and worker.is_alive():
                return
            worker = threading.Thread(
                target=self._run_worker,
                args=(building_text, key),
                name=f"handover-xlsx-write-queue-{building_text}",
                daemon=True,
            )
            self._workers[key] = worker
            worker.start()

    def enqueue_review_excel_sync(self, session: Dict[str, Any], *, target_revision: int) -> Dict[str, Any]:
        building = str(session.get("building", "") or "").strip()
        session_id = str(session.get("session_id", "") or "").strip()
        if not building or not session_id:
            return {}
        store = self._store(building)
        current_sync = store.get_sync_state(session_id)
        store.update_sync_state(
            session_id=session_id,
            status="pending",
            synced_revision=int(current_sync.get("synced_revision", 0) or 0),
            pending_revision=int(target_revision or 0),
            error="",
        )
        store.enqueue_xlsx_write_job(
            task_type="review_excel_sync",
            dedupe_key=session_id,
            payload={
                "building": building,
                "session_id": session_id,
                "target_revision": int(target_revision or 0),
            },
            dedupe_pending=True,
        )
        self._start_worker(building=building)
        return store.get_sync_state(session_id)

    def enqueue_capacity_overlay_sync(
        self,
        *,
        building: str,
        session_id: str,
        tracked_cells: Dict[str, Any],
        shared_110kv: Dict[str, Any] | None = None,
        cooling_pump_pressures: Dict[str, Any] | None = None,
        capacity_output_file: str = "",
        overlay_scope: str = "",
    ) -> Dict[str, Any]:
        building_text = str(building or "").strip()
        session_id_text = str(session_id or "").strip()
        if not building_text or not session_id_text:
            return {}
        dedupe_file = str(capacity_output_file or "").strip()
        scope_text = str(overlay_scope or "").strip() or "full"
        dedupe_key = f"{session_id_text}|{dedupe_file}|{scope_text}"
        job = self._store(building_text).enqueue_xlsx_write_job(
            task_type="capacity_overlay_sync",
            dedupe_key=dedupe_key,
            payload={
                "building": building_text,
                "session_id": session_id_text,
                "tracked_cells": tracked_cells if isinstance(tracked_cells, dict) else {},
                "shared_110kv": shared_110kv if isinstance(shared_110kv, dict) else {},
                "cooling_pump_pressures": cooling_pump_pressures if isinstance(cooling_pump_pressures, dict) else {},
                "capacity_output_file": dedupe_file,
                "overlay_scope": scope_text,
            },
            dedupe_pending=True,
        )
        self._start_worker(building=building_text)
        return job

    def enqueue_barrier(self, *, building: str) -> Dict[str, Any]:
        building_text = str(building or "").strip()
        if not building_text:
            return {}
        job = self._store(building_text).enqueue_xlsx_write_job(
            task_type="barrier",
            dedupe_key=f"barrier:{uuid.uuid4().hex}",
            payload={"building": building_text},
            dedupe_pending=False,
        )
        self._start_worker(building=building_text)
        return job

    def wait_for_job(self, *, building: str, job_id: str, timeout_sec: float = 120.0) -> Dict[str, Any]:
        building_text = str(building or "").strip()
        job_id_text = str(job_id or "").strip()
        if not building_text or not job_id_text:
            raise HandoverXlsxWriteQueueTimeoutError("xlsx写入队列任务不存在")
        deadline = time.monotonic() + max(0.1, float(timeout_sec or 120.0))
        store = self._store(building_text)
        self._start_worker(building=building_text)
        last_job: Dict[str, Any] | None = None
        while time.monotonic() < deadline:
            last_job = store.get_xlsx_write_job(job_id_text)
            status = str((last_job or {}).get("status", "") or "").strip().lower()
            if status in {"success", "failed"}:
                return last_job or {}
            time.sleep(0.2)
        raise HandoverXlsxWriteQueueTimeoutError("容量表写入队列繁忙，请稍后重试")

    def wait_for_barrier(self, *, building: str, timeout_sec: float = 120.0) -> Dict[str, Any]:
        barrier = self.enqueue_barrier(building=building)
        return self.wait_for_job(
            building=building,
            job_id=str(barrier.get("job_id", "") or "").strip(),
            timeout_sec=timeout_sec,
        )

    def has_active_write_jobs(self, *, building: str) -> bool:
        building_text = str(building or "").strip()
        if not building_text:
            return False
        return self._store(building_text).has_active_xlsx_write_jobs()

    def recover_startup_jobs(self, buildings: list[str] | tuple[str, ...] | None = None) -> Dict[str, Any]:
        targets = [
            str(item or "").strip()
            for item in (buildings if buildings is not None else HANDOVER_BUILDINGS)
            if str(item or "").strip()
        ]
        recovered: Dict[str, Any] = {}
        total_pending = 0
        total_reset_running = 0
        for building in targets:
            try:
                result = self._store(building).recover_xlsx_write_jobs_for_startup()
                recovered[building] = result
                total_pending += int(result.get("pending", 0) or 0)
                total_reset_running += int(result.get("reset_running", 0) or 0)
                if int(result.get("pending", 0) or 0) > 0:
                    self._start_worker(building=building)
            except Exception as exc:  # noqa: BLE001
                recovered[building] = {"error": str(exc)}
                self.emit_log(f"[交接班][xlsx队列] 启动恢复失败 building={building}, error={exc}")
        self.emit_log(
            "[交接班][xlsx队列] 启动恢复完成 "
            f"buildings={len(targets)}, pending={total_pending}, reset_running={total_reset_running}"
        )
        return {
            "buildings": recovered,
            "pending": total_pending,
            "reset_running": total_reset_running,
        }

    def _run_worker(self, building: str, key: str) -> None:
        should_restart = False
        try:
            self._worker_loop(building)
        finally:
            with self._worker_guard:
                current = self._workers.get(key)
                if current is threading.current_thread():
                    self._workers.pop(key, None)
                    try:
                        should_restart = self._store(building).has_pending_xlsx_write_jobs()
                    except Exception:
                        should_restart = False
            self.emit_log(f"[交接班][xlsx队列] worker已退出 building={building}")
        if should_restart:
            self._start_worker(building=building)

    def _worker_loop(self, building: str) -> None:
        store = self._store(building)
        while True:
            job = store.claim_next_xlsx_write_job()
            if not isinstance(job, dict):
                return
            job_id = str(job.get("job_id", "") or "").strip()
            task_type = str(job.get("task_type", "") or "").strip()
            started = time.perf_counter()
            try:
                self.emit_log(
                    f"[交接班][xlsx队列] 开始 building={building}, job_id={job_id}, task_type={task_type}"
                )
                self._execute_job(building=building, job=job)
                store.finish_xlsx_write_job(job_id=job_id, success=True)
                self.emit_log(
                    f"[交接班][xlsx队列] 完成 building={building}, job_id={job_id}, "
                    f"task_type={task_type}, elapsed_ms={int((time.perf_counter() - started) * 1000)}"
                )
            except Exception as exc:  # noqa: BLE001
                store.finish_xlsx_write_job(job_id=job_id, success=False, error=str(exc))
                self.emit_log(
                    f"[交接班][xlsx队列] 失败 building={building}, job_id={job_id}, "
                    f"task_type={task_type}, error={exc}"
                )
            time.sleep(0.02)

    def _resource_guard(self, *, building: str, name: str):
        if self.job_service is not None and callable(getattr(self.job_service, "resource_guard", None)):
            return self.job_service.resource_guard(
                name=name,
                resource_keys=[f"handover_building:{building}"],
            )
        return contextlib.nullcontext()

    def _execute_job(self, *, building: str, job: Dict[str, Any]) -> None:
        task_type = str(job.get("task_type", "") or "").strip()
        payload = job.get("payload", {}) if isinstance(job.get("payload", {}), dict) else {}
        if task_type == "barrier":
            return
        if task_type == "review_excel_sync":
            self._execute_review_excel_sync(building=building, payload=payload)
            return
        if task_type == "capacity_overlay_sync":
            self._execute_capacity_overlay_sync(building=building, payload=payload)
            return
        raise ValueError(f"未知xlsx写入任务类型: {task_type}")

    def _execute_review_excel_sync(self, *, building: str, payload: Dict[str, Any]) -> None:
        from handover_log_module.service.review_document_state_service import ReviewDocumentStateService

        session_id = str(payload.get("session_id", "") or "").strip()
        target_revision = int(payload.get("target_revision", 0) or 0)
        with self._resource_guard(building=building, name=f"xlsx_review_sync:{building}:{session_id}"):
            service = ReviewDocumentStateService(
                self.config,
                parser=self.parser,
                writer=self.writer,
                emit_log=self.emit_log,
            )
            try:
                service.force_sync_session(
                    building=building,
                    session_id=session_id,
                    target_revision=target_revision,
                    reason="xlsx_queue",
                    reconcile_sync_job=False,
                )
            except Exception as exc:
                store = self._store(building)
                current = store.get_sync_state(session_id)
                store.update_sync_state(
                    session_id=session_id,
                    status="failed",
                    synced_revision=int(current.get("synced_revision", 0) or 0),
                    pending_revision=target_revision,
                    error=str(exc),
                )
                raise

    def _execute_capacity_overlay_sync(self, *, building: str, payload: Dict[str, Any]) -> None:
        from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService
        from handover_log_module.service.review_session_service import ReviewSessionService

        session_id = str(payload.get("session_id", "") or "").strip()
        overlay_scope = str(payload.get("overlay_scope", "") or "").strip().lower() or "full"
        tracked_cells = payload.get("tracked_cells", {}) if isinstance(payload.get("tracked_cells", {}), dict) else {}
        shared_110kv = payload.get("shared_110kv", {}) if isinstance(payload.get("shared_110kv", {}), dict) else {}
        cooling_pump_pressures = (
            payload.get("cooling_pump_pressures", {})
            if isinstance(payload.get("cooling_pump_pressures", {}), dict)
            else {}
        )
        review_service = ReviewSessionService(self.config)
        try:
            session = review_service.get_or_recover_session_by_id(session_id)
            if not isinstance(session, dict):
                raise ValueError(f"审核会话不存在: {session_id}")
            duty_date = str(session.get("duty_date", "") or "").strip()
            duty_shift = str(session.get("duty_shift", "") or "").strip().lower()
            if not duty_date or duty_shift not in {"day", "night"}:
                raise ValueError("容量表补写缺少日期或班次")
            with self._resource_guard(building=building, name=f"xlsx_capacity_overlay:{building}:{session_id}"):
                capacity_service = HandoverCapacityReportService(self.config)
                if overlay_scope == "substation_110kv":
                    sync_payload = capacity_service.sync_substation_110kv_for_existing_report_from_cells(
                        building=building,
                        duty_date=duty_date,
                        duty_shift=duty_shift,
                        handover_cells=tracked_cells,
                        capacity_output_file=str(session.get("capacity_output_file", "") or "").strip(),
                        shared_110kv=shared_110kv,
                        cooling_pump_pressures=cooling_pump_pressures,
                        emit_log=self.emit_log,
                    )
                else:
                    sync_payload = capacity_service.sync_overlay_for_existing_report_from_cells(
                        building=building,
                        duty_date=duty_date,
                        duty_shift=duty_shift,
                        handover_cells=tracked_cells,
                        capacity_output_file=str(session.get("capacity_output_file", "") or "").strip(),
                        shared_110kv=shared_110kv,
                        cooling_pump_pressures=cooling_pump_pressures,
                        emit_log=self.emit_log,
                    )
            sync_status = str(sync_payload.get("status", "") if isinstance(sync_payload, dict) else "").strip().lower()
            if sync_status == "ready":
                capacity_status = "success"
                capacity_error = ""
            elif sync_status == "pending_input":
                capacity_status = "pending_input"
                capacity_error = str(sync_payload.get("error", "")).strip()
            elif sync_status == "missing_file":
                capacity_status = "missing_file"
                capacity_error = str(sync_payload.get("error", "")).strip()
            else:
                capacity_status = "failed"
                capacity_error = str(sync_payload.get("error", "")).strip()
            updated = review_service.update_capacity_sync(
                session_id=session_id,
                capacity_sync=sync_payload if isinstance(sync_payload, dict) else {},
                capacity_status=capacity_status,
                capacity_error=capacity_error,
            )
            self.emit_log(
                "[交接班][容量报表][xlsx队列] 补写状态 "
                f"building={updated.get('building', '-')}, session_id={session_id}, status={sync_status or '-'}"
            )
        except Exception as exc:
            if session_id:
                try:
                    review_service.update_capacity_sync(
                        session_id=session_id,
                        capacity_sync={
                            "status": "failed",
                            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                            "error": str(exc),
                            "tracked_cells": list(HandoverCapacityReportService.tracked_cells()),
                        },
                        capacity_status="failed",
                        capacity_error=str(exc),
                    )
                except Exception:
                    pass
            raise
