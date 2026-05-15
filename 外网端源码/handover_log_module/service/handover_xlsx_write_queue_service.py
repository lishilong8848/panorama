from __future__ import annotations

import copy
import threading
import time
from typing import Any, Callable, Dict

from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore
from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService
from handover_log_module.service.review_document_parser import ReviewDocumentParser
from handover_log_module.service.review_document_state_service import (
    ReviewDocumentStateError,
    ReviewDocumentStateService,
)
from handover_log_module.service.review_document_writer import ReviewDocumentWriter
from handover_log_module.service.review_session_service import (
    ReviewSessionNotFoundError,
    ReviewSessionService,
)


class HandoverXlsxWriteQueueTimeoutError(TimeoutError):
    pass


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


class HandoverXlsxWriteQueueService:
    """Per-building persistent FIFO queue for handover review xlsx writes."""

    _worker_guard = threading.Lock()
    _workers: Dict[str, threading.Thread] = {}
    _recovered_buildings: set[str] = set()

    def __init__(
        self,
        config: Dict[str, Any],
        *,
        review_service: ReviewSessionService | None = None,
        document_state: ReviewDocumentStateService | None = None,
        parser: ReviewDocumentParser | None = None,
        writer: ReviewDocumentWriter | None = None,
        emit_log: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config if isinstance(config, dict) else {}
        self._raw_emit_log = emit_log if callable(emit_log) else print
        self.emit_log = self._emit_log_safely
        self.review_service = review_service or ReviewSessionService(self.config)
        self.document_state = document_state or ReviewDocumentStateService(
            self.config,
            parser=parser,
            writer=writer,
            emit_log=self.emit_log,
        )
        if hasattr(self.document_state, "emit_log"):
            self.document_state.emit_log = self.emit_log

    def _emit_log_safely(self, message: str) -> None:
        text = str(message or "")
        try:
            self._raw_emit_log(text)
            return
        except Exception as exc:  # noqa: BLE001
            # xlsx 写入队列是持久后台队列，可能长于触发它的任务生命周期。
            # 任务日志器关闭后，日志失败不能反过来把 Excel 同步标记为失败。
            try:
                print(f"{text} [日志降级输出: {type(exc).__name__}: {exc}]")
            except Exception:
                pass

    def _store(self, building: str) -> ReviewBuildingDocumentStore:
        return ReviewBuildingDocumentStore(config=self.config, building=building)

    @staticmethod
    def _extract_fixed_cells(document: Dict[str, Any]) -> Dict[str, str]:
        output: Dict[str, str] = {}
        fixed_blocks = document.get("fixed_blocks", []) if isinstance(document, dict) else []
        if not isinstance(fixed_blocks, list):
            return output
        for block in fixed_blocks:
            if not isinstance(block, dict):
                continue
            fields = block.get("fields", [])
            if not isinstance(fields, list):
                continue
            for field in fields:
                if not isinstance(field, dict):
                    continue
                cell = _text(field.get("cell")).upper()
                if cell:
                    output[cell] = _text(field.get("value"))
        return output

    @classmethod
    def _extract_capacity_tracked_cells(cls, document: Dict[str, Any]) -> Dict[str, str]:
        fixed_cells = cls._extract_fixed_cells(document)
        return {
            cell: _text(fixed_cells.get(cell, ""))
            for cell in HandoverCapacityReportService.tracked_cells()
        }

    def _recover_running_once(self, building: str) -> None:
        key = _text(building)
        if not key:
            return
        with self._worker_guard:
            if key in self._recovered_buildings:
                return
            self._recovered_buildings.add(key)
        recovered = self._store(key).recover_xlsx_write_jobs_for_startup()
        if recovered:
            self.emit_log(f"[交接班][xlsx队列] 启动恢复 running 任务 building={key}, count={recovered}")

    def _start_worker(self, building: str) -> None:
        building_text = _text(building)
        if not building_text:
            return
        self._recover_running_once(building_text)
        key = building_text
        with self._worker_guard:
            existing = self._workers.get(key)
            if existing is not None and existing.is_alive():
                return
            worker = threading.Thread(
                target=self._worker_loop,
                args=(building_text,),
                name=f"handover-xlsx-write-{building_text}",
                daemon=True,
            )
            self._workers[key] = worker
            worker.start()

    def enqueue_review_excel_sync(self, session: Dict[str, Any], *, target_revision: int | None = None) -> Dict[str, Any]:
        building = _text(session.get("building"))
        session_id = _text(session.get("session_id"))
        revision = int(target_revision or session.get("revision", 0) or 0)
        if not building or not session_id:
            return {"status": "failed", "error": "审核会话缺少楼栋或 session_id"}
        store = self._store(building)
        sync_state = store.update_sync_state(
            session_id=session_id,
            status="pending",
            synced_revision=store.get_sync_state(session_id).get("synced_revision", 0),
            pending_revision=revision,
            error="",
        )
        job = store.enqueue_xlsx_write_job(
            task_type="review_excel_sync",
            session_id=session_id,
            dedupe_key=session_id,
            payload={"session_id": session_id, "target_revision": revision},
        )
        self.emit_log(
            f"[交接班][xlsx队列] 已入队 building={building}, task=review_excel_sync, "
            f"session_id={session_id}, revision={revision}, job_id={job.get('job_id', '-')}"
        )
        self._start_worker(building)
        return sync_state

    def enqueue_capacity_overlay_sync(
        self,
        session: Dict[str, Any],
        *,
        tracked_cells: Dict[str, Any] | None = None,
        client_id: str = "",
    ) -> Dict[str, Any]:
        building = _text(session.get("building"))
        session_id = _text(session.get("session_id"))
        output_file = _text(session.get("capacity_output_file"))
        if not building or not session_id:
            return {"status": "failed", "error": "审核会话缺少楼栋或 session_id"}
        payload: Dict[str, Any] = {
            "session_id": session_id,
            "client_id": _text(client_id),
        }
        if tracked_cells is not None:
            payload["tracked_cells"] = {
                cell: _text((tracked_cells or {}).get(cell, ""))
                for cell in HandoverCapacityReportService.tracked_cells()
            }
        job = self._store(building).enqueue_xlsx_write_job(
            task_type="capacity_overlay_sync",
            session_id=session_id,
            dedupe_key=f"{session_id}|{output_file}",
            payload=payload,
        )
        self.emit_log(
            f"[交接班][xlsx队列] 已入队 building={building}, task=capacity_overlay_sync, "
            f"session_id={session_id}, job_id={job.get('job_id', '-')}"
        )
        self._start_worker(building)
        return job

    def enqueue_barrier(self, *, building: str, session_id: str = "", reason: str = "") -> Dict[str, Any]:
        building_text = _text(building)
        job = self._store(building_text).enqueue_xlsx_write_job(
            task_type="barrier",
            session_id=_text(session_id),
            dedupe_key="",
            payload={"reason": _text(reason), "session_id": _text(session_id)},
        )
        self.emit_log(
            f"[交接班][xlsx队列] barrier已入队 building={building_text}, "
            f"session_id={_text(session_id) or '-'}, reason={_text(reason) or '-'}, job_id={job.get('job_id', '-')}"
        )
        self._start_worker(building_text)
        return job

    def wait_for_job(self, *, building: str, job_id: str, timeout_sec: float = 120.0) -> Dict[str, Any]:
        building_text = _text(building)
        job_id_text = _text(job_id)
        deadline = time.monotonic() + max(0.1, float(timeout_sec or 120.0))
        store = self._store(building_text)
        self._start_worker(building_text)
        while time.monotonic() <= deadline:
            job = store.get_xlsx_write_job(job_id_text)
            status = _text(job.get("status") if isinstance(job, dict) else "").lower()
            if status in {"success", "failed"}:
                return job or {}
            time.sleep(0.2)
        raise HandoverXlsxWriteQueueTimeoutError("交接班文件写入队列繁忙，请稍后重试")

    def wait_for_barrier(
        self,
        *,
        building: str,
        session_id: str = "",
        reason: str = "",
        timeout_sec: float = 120.0,
    ) -> Dict[str, Any]:
        job = self.enqueue_barrier(building=building, session_id=session_id, reason=reason)
        return self.wait_for_job(building=building, job_id=_text(job.get("job_id")), timeout_sec=timeout_sec)

    def _worker_loop(self, building: str) -> None:
        store = self._store(building)
        empty_rounds = 0
        try:
            while True:
                job = store.claim_next_xlsx_write_job()
                if not job:
                    empty_rounds += 1
                    if empty_rounds >= 6:
                        self.emit_log(f"[交接班][xlsx队列] worker已退出 building={building}")
                        return
                    time.sleep(0.5)
                    continue
                empty_rounds = 0
                started = time.perf_counter()
                success = True
                error = ""
                try:
                    self.emit_log(
                        f"[交接班][xlsx队列] 开始 building={building}, job_id={job.get('job_id', '-')}, "
                        f"task_type={job.get('task_type', '-')}"
                    )
                    self._execute_job(building=building, job=job)
                except Exception as exc:  # noqa: BLE001
                    success = False
                    error = str(exc)
                    self.emit_log(
                        f"[交接班][xlsx队列] 失败 building={building}, job_id={job.get('job_id', '-')}, "
                        f"task_type={job.get('task_type', '-')}, error={error}"
                    )
                finally:
                    store.finish_xlsx_write_job(job_id=_text(job.get("job_id")), success=success, error=error)
                    elapsed_ms = int((time.perf_counter() - started) * 1000)
                    self.emit_log(
                        f"[交接班][xlsx队列] 完成 building={building}, job_id={job.get('job_id', '-')}, "
                        f"task_type={job.get('task_type', '-')}, status={'success' if success else 'failed'}, elapsed_ms={elapsed_ms}"
                    )
        finally:
            with self._worker_guard:
                current = self._workers.get(building)
                if current is threading.current_thread():
                    self._workers.pop(building, None)

    def _execute_job(self, *, building: str, job: Dict[str, Any]) -> None:
        task_type = _text(job.get("task_type")).lower()
        payload = job.get("payload", {}) if isinstance(job.get("payload", {}), dict) else {}
        if task_type == "barrier":
            return
        session_id = _text(payload.get("session_id") or job.get("session_id"))
        if not session_id:
            raise RuntimeError("xlsx 队列任务缺少 session_id")
        session = self.review_service.get_or_recover_session_by_id(session_id)
        if not isinstance(session, dict):
            raise ReviewSessionNotFoundError("review session not found")
        if task_type == "review_excel_sync":
            self.document_state.force_sync_session_dict(session, reason="xlsx_queue")
            return
        if task_type == "capacity_overlay_sync":
            self._execute_capacity_overlay(session=session, payload=payload)
            return
        raise RuntimeError(f"unsupported xlsx queue task_type: {task_type}")

    def _execute_capacity_overlay(self, *, session: Dict[str, Any], payload: Dict[str, Any]) -> None:
        building = _text(session.get("building"))
        session_id = _text(session.get("session_id"))
        document: Dict[str, Any] = {}
        try:
            document, session = self.document_state.load_document(session)
        except ReviewDocumentStateError:
            document = {}
        tracked_cells = payload.get("tracked_cells", {}) if isinstance(payload.get("tracked_cells", {}), dict) else {}
        if not tracked_cells:
            tracked_cells = self._extract_capacity_tracked_cells(document)
        else:
            tracked_cells = {
                cell: _text(tracked_cells.get(cell, ""))
                for cell in HandoverCapacityReportService.tracked_cells()
            }
        shared_state = self.review_service.get_substation_110kv_state(
            batch_key=_text(session.get("batch_key")),
            client_id=_text(payload.get("client_id")),
        )
        shared_110kv = (
            shared_state.get("shared_blocks", {}).get("substation_110kv", {})
            if isinstance(shared_state.get("shared_blocks", {}), dict)
            else {}
        )
        cooling_pump_pressures = (
            document.get("cooling_pump_pressures", {})
            if isinstance(document.get("cooling_pump_pressures", {}), dict)
            else {}
        )
        sync_payload = HandoverCapacityReportService(self.config).sync_overlay_for_existing_report_from_cells(
            building=building,
            duty_date=_text(session.get("duty_date")),
            duty_shift=_text(session.get("duty_shift")).lower(),
            handover_cells=tracked_cells,
            capacity_output_file=_text(session.get("capacity_output_file")),
            shared_110kv=copy.deepcopy(shared_110kv) if isinstance(shared_110kv, dict) else {},
            cooling_pump_pressures=copy.deepcopy(cooling_pump_pressures) if isinstance(cooling_pump_pressures, dict) else {},
            client_id=_text(payload.get("client_id")),
            emit_log=self.emit_log,
        )
        sync_status = _text(sync_payload.get("status") if isinstance(sync_payload, dict) else "").lower()
        capacity_status = "success" if sync_status == "ready" else (sync_status or "failed")
        capacity_error = "" if sync_status == "ready" else _text(sync_payload.get("error") if isinstance(sync_payload, dict) else "")
        self.review_service.update_capacity_sync(
            session_id=session_id,
            capacity_sync=sync_payload if isinstance(sync_payload, dict) else {},
            capacity_status=capacity_status,
            capacity_error=capacity_error,
        )
        if sync_status not in {"ready", "pending_input"}:
            raise RuntimeError(capacity_error or "容量报表补写失败")
