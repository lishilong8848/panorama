from __future__ import annotations

import copy
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict

from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore
from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService
from handover_log_module.service.handover_generation_guard import try_host_file_lock
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


def _output_key(value: Any) -> str:
    path = _text(value)
    return os.path.normcase(str(Path(path).resolve())) if path else ""


class HandoverXlsxWriteQueueService:
    """Bounded per-building workers; writes to the same workbook stay ordered."""

    _worker_guard = threading.Lock()
    _workers: Dict[tuple[str, int], threading.Thread] = {}

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
        return HandoverCapacityReportService.extract_tracked_cells_from_review_document(
            document if isinstance(document, dict) else {}
        )

    def _start_worker(self, building: str) -> None:
        building_text = _text(building)
        if not building_text:
            return
        store = self._store(building_text)
        db_key = _output_key(store.db_path)
        with self._worker_guard:
            for slot in range(2):
                key = (db_key, slot)
                existing = self._workers.get(key)
                if existing is not None and existing.is_alive():
                    continue
                worker = threading.Thread(
                    target=self._worker_loop, args=(building_text, store, key),
                    name=f"handover-xlsx-write-{building_text}-{slot}", daemon=True,
                )
                self._workers[key] = worker
                worker.start()

    def enqueue_review_excel_sync(
        self,
        session: Dict[str, Any],
        *,
        target_revision: int | None = None,
        force_all_regions: bool = False,
    ) -> Dict[str, Any]:
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
            payload={
                "session_id": session_id,
                "target_revision": revision,
                "force_all_regions": bool(force_all_regions),
            },
            dedupe_running_if_payload_matches=True,
            target_key=_output_key(session.get("output_file")),
        )
        self.emit_log(
            f"[交接班][xlsx队列] 已入队 building={building}, task=review_excel_sync, "
            f"session_id={session_id}, revision={revision}, "
            f"force_all_regions={bool(force_all_regions)}, job_id={job.get('job_id', '-')}"
        )
        self._start_worker(building)
        return {**sync_state, "job_id": job["job_id"]}

    def enqueue_capacity_overlay_sync(
        self,
        session: Dict[str, Any],
        *,
        tracked_cells: Dict[str, Any] | None = None,
        client_id: str = "",
        use_saved_shared_110kv: bool = False,
        expected_document_revision: int | None = None,
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
        if use_saved_shared_110kv:
            payload["use_saved_shared_110kv"] = True
        if expected_document_revision is not None:
            payload["expected_document_revision"] = expected_document_revision
        if tracked_cells is not None:
            payload["tracked_cells"] = {
                cell: _text((tracked_cells or {}).get(cell, ""))
                for cell in HandoverCapacityReportService.tracked_cells()
            }
        dedupe_key = f"{session_id}|{output_file}"
        if expected_document_revision is not None:
            # A delivery overlay must not be replaced by a pending autosave overlay.
            dedupe_key += f"|delivery:{expected_document_revision}"
        job = self._store(building).enqueue_xlsx_write_job(
            task_type="capacity_overlay_sync",
            session_id=session_id,
            dedupe_key=dedupe_key,
            payload=payload,
            dedupe_running_if_payload_matches=True,
            target_key=_output_key(output_file),
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

    def _worker_loop(self, building: str, store: ReviewBuildingDocumentStore, key: tuple[str, int]) -> None:
        db_key, slot = key
        try:
            while True:
                try:
                    # Only a slot lock owner can recover that slot's interrupted write.
                    with try_host_file_lock(f"{db_key}|{slot}", namespace="xlsx_queue") as locked:
                        if locked:
                            recovered = store.recover_xlsx_write_jobs_for_startup(worker_slot=slot)
                            if recovered:
                                self.emit_log(f"[交接班][xlsx队列] 恢复中断任务 building={building}, slot={slot}, count={recovered}")
                            while job := store.claim_next_xlsx_write_job(worker_slot=slot):
                                self._run_claimed_job(building=building, store=store, job=job)
                except Exception as exc:  # noqa: BLE001
                    self.emit_log(
                        f"[交接班][xlsx队列] worker领取任务异常，稍后重试 building={building}, error={exc}"
                    )
                    time.sleep(1.0)
                    continue
                time.sleep(0.5)
        finally:
            with self._worker_guard:
                current = self._workers.get(key)
                if current is threading.current_thread():
                    self._workers.pop(key, None)

    def _run_claimed_job(self, *, building: str, store: ReviewBuildingDocumentStore, job: Dict[str, Any]) -> None:
        started = time.perf_counter()
        job_id = job["job_id"]
        task_type = job["task_type"]
        error = ""
        success = True
        self.emit_log(f"[交接班][xlsx队列] 开始 building={building}, job_id={job_id}, task_type={task_type}")
        try:
            target_key = job.get("target_key")
            if target_key:
                deadline = time.monotonic() + 30
                while True:
                    with try_host_file_lock(target_key, namespace="xlsx_target") as locked:
                        if locked:
                            self._execute_job(building=building, job=job)
                            break
                    if time.monotonic() >= deadline:
                        raise HandoverXlsxWriteQueueTimeoutError("同一 Excel 正在写入，请稍后重试")
                    time.sleep(0.1)
            else:
                self._execute_job(building=building, job=job)
        except Exception as exc:  # noqa: BLE001
            success = False
            error = str(exc)
            self.emit_log(f"[交接班][xlsx队列] 失败 building={building}, job_id={job_id}, task_type={task_type}, error={error}")
        store.finish_xlsx_write_job(job_id=job_id, success=success, error=error)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        self.emit_log(
            f"[交接班][xlsx队列] 完成 building={building}, job_id={job_id}, "
            f"task_type={task_type}, status={'success' if success else 'failed'}, elapsed_ms={elapsed_ms}"
        )

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
        path_field = "output_file" if task_type == "review_excel_sync" else "capacity_output_file"
        if job.get("target_key") and job["target_key"] != _output_key(session.get(path_field)):
            raise RuntimeError("交接班文件已重新生成，已跳过旧文件写入任务")
        if task_type == "review_excel_sync":
            self.document_state.force_sync_session_dict(
                session,
                reason="xlsx_queue",
                force_all_regions=bool(payload.get("force_all_regions")),
                preserve_existing=True,
            )
            return
        if task_type == "capacity_overlay_sync":
            self._execute_capacity_overlay(session=session, payload=payload)
            return
        raise RuntimeError(f"unsupported xlsx queue task_type: {task_type}")

    def _execute_capacity_overlay(self, *, session: Dict[str, Any], payload: Dict[str, Any]) -> None:
        building = _text(session.get("building"))
        session_id = _text(session.get("session_id"))
        expected_revision = payload.get("expected_document_revision")
        document: Dict[str, Any] = {}
        try:
            document, session = self.document_state.load_document(session, preserve_existing=True)
        except ReviewDocumentStateError:
            if expected_revision is not None:
                raise
            document = {}
        if expected_revision is not None and int(session.get("revision", 0) or 0) != expected_revision:
            raise RuntimeError("审核文档已变更，未补写旧版本容量表，请重新提交")
        tracked_cells = payload.get("tracked_cells", {}) if isinstance(payload.get("tracked_cells", {}), dict) else {}
        if not tracked_cells:
            tracked_cells = self._extract_capacity_tracked_cells(document)
        else:
            tracked_cells = {
                cell: _text(tracked_cells.get(cell, ""))
                for cell in HandoverCapacityReportService.tracked_cells()
            }
        batch_key = _text(session.get("batch_key"))
        if payload.get("use_saved_shared_110kv"):
            shared_110kv = self.review_service.get_saved_substation_110kv_block(batch_key=batch_key)
        else:
            shared_state = self.review_service.get_substation_110kv_state(
                batch_key=batch_key, client_id=_text(payload.get("client_id")),
            )
            blocks = shared_state.get("shared_blocks", {})
            shared_110kv = blocks.get("substation_110kv", {}) if isinstance(blocks, dict) else {}
        cooling_pump_pressures = (
            document.get("cooling_pump_pressures", {})
            if isinstance(document.get("cooling_pump_pressures", {}), dict)
            else {}
        )
        capacity_service = HandoverCapacityReportService(self.config)
        inputs = dict(
            building=building, duty_date=_text(session.get("duty_date")),
            duty_shift=_text(session.get("duty_shift")).lower(), handover_cells=tracked_cells,
            shared_110kv=copy.deepcopy(shared_110kv) if isinstance(shared_110kv, dict) else {},
            cooling_pump_pressures=copy.deepcopy(cooling_pump_pressures),
            emit_log=self.emit_log,
        )
        output_file = _text(session.get("capacity_output_file"))
        sync = session.get("capacity_sync", {})
        if expected_revision is not None and not payload.get("use_saved_shared_110kv") and isinstance(sync, dict) and sync.get("status") == "ready":
            if capacity_service.download_snapshot_matches(
                sync.get("download_snapshot"), revision=expected_revision, output_file=output_file, **inputs,
            ):
                return

        def download_signature():
            try:
                return capacity_service.download_inputs_signature(**inputs)
            except Exception:  # A missing dependency disables reuse, not the existing write path.
                return ""

        download_signature_before = download_signature()
        sync_payload = capacity_service.sync_overlay_for_existing_report_from_cells(
            **inputs,
            capacity_output_file=output_file,
            client_id=_text(payload.get("client_id")),
        )
        sync_status = _text(sync_payload.get("status") if isinstance(sync_payload, dict) else "").lower()
        after = download_signature() if sync_status == "ready" and download_signature_before else ""
        if after and after == download_signature_before:
            sync_payload["download_snapshot"] = {
                "revision": int(session.get("revision", 0) or 0),
                "inputs": download_signature_before,
                "file": capacity_service._capacity_file_fingerprint(output_file),
                "verified_at": time.time(),
            }
        capacity_status = "success" if sync_status == "ready" else (sync_status or "failed")
        capacity_error = "" if sync_status == "ready" else _text(sync_payload.get("error") if isinstance(sync_payload, dict) else "")
        self.review_service.update_capacity_sync(
            session_id=session_id,
            capacity_sync=sync_payload if isinstance(sync_payload, dict) else {},
            capacity_status=capacity_status,
            capacity_error=capacity_error,
        )
        if sync_status == "pending_input" and expected_revision is not None:
            raise RuntimeError(f"{capacity_error or '容量报表缺少必填数据'}；请在审核页补齐并保存后再提交")
        if sync_status not in {"ready", "pending_input"}:
            raise RuntimeError(capacity_error or "容量报表补写失败")
        if expected_revision is not None and download_signature_before and after != download_signature_before:
            raise RuntimeError("容量报表补写期间共享数据已更新，请重新下载")
