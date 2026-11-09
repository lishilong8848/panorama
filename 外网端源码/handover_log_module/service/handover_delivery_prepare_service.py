from __future__ import annotations

import copy
import hashlib
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict

from handover_log_module.api.facade import load_handover_config
from handover_log_module.service.capacity_report_image_delivery_service import (
    CapacityReportImageDeliveryService,
    _runtime_root,
)
from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService
from handover_log_module.service.handover_summary_message_service import HandoverSummaryMessageService
from handover_log_module.service.handover_xlsx_write_queue_service import HandoverXlsxWriteQueueService
from handover_log_module.service.review_document_state_service import ReviewDocumentStateService
from handover_log_module.service.review_session_service import (
    ReviewSessionConflictError,
    ReviewSessionNotFoundError,
    ReviewSessionService,
)


class HandoverDeliveryPrepareError(RuntimeError):
    retryable = False


class HandoverDeliveryPrepareRetryableError(ReviewSessionConflictError):
    retryable = True


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _fingerprint(path: Path) -> tuple[int, int, int, str]:
    try:
        before = path.stat()
        digest = _digest(path)
        after = path.stat()
    except OSError as exc:
        raise HandoverDeliveryPrepareRetryableError("源文件读取失败，请重试") from exc
    first = (before.st_size, before.st_mtime_ns, before.st_ino)
    last = (after.st_size, after.st_mtime_ns, after.st_ino)
    if first != last:
        raise HandoverDeliveryPrepareRetryableError("源文件读取期间发生变化，请重试")
    return (*last, digest)


class HandoverDeliveryPrepareService:
    """Prepare immutable local artifacts; confirmation and delivery belong to the caller."""

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        # The complete runtime config carries paths and shared module settings.
        self.config = copy.deepcopy(runtime_config)
        if "_global_paths" not in self.config:
            self.config = load_handover_config(self.config)
        self.review_service = ReviewSessionService(self.config, initialize_generated_file_index=False)
        self.document_state = ReviewDocumentStateService(self.config)
        self.summary_service = HandoverSummaryMessageService(self.config)
        self.image_service = CapacityReportImageDeliveryService(
            self.config,
            review_service=self.review_service,
            summary_service=self.summary_service,
        )

    def _snapshot(self, session_id: str) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        session = self.review_service.get_session_by_id(session_id)
        if not isinstance(session, dict):
            raise ReviewSessionNotFoundError("审核会话不存在")
        session = copy.deepcopy(session)
        if not session.get("building") or session.get("session_id") != session_id:
            raise HandoverDeliveryPrepareError("审核会话缺少有效的楼栋或 session_id")
        state = self.document_state._store(session["building"]).get_document(session_id)
        if not isinstance(state, dict) or not isinstance(state.get("document"), dict):
            raise HandoverDeliveryPrepareError("审核文档尚未保存，不能准备交付成品")
        state = copy.deepcopy(state)
        if int(session.get("revision", 0) or 0) <= 0 or int(state.get("revision", 0) or 0) <= 0:
            raise HandoverDeliveryPrepareError("审核会话或文档缺少有效版本")
        source = str(state.get("source_excel_path", "") or "").strip()
        output = str(session.get("output_file", "") or "").strip()
        if not source or not output or Path(source).resolve() != Path(output).resolve():
            raise HandoverDeliveryPrepareRetryableError("审核文档和会话源文件不一致，请重试")
        shared = copy.deepcopy(self.review_service.get_saved_substation_110kv_block(batch_key=session.get("batch_key", "")))
        return session, state, shared

    @staticmethod
    def _version(snapshot: tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]) -> str:
        session, state, shared = snapshot
        return _json_digest({
            "session": {key: session.get(key) for key in (
                "session_id", "building", "batch_key", "duty_date", "duty_shift",
                "revision", "output_file", "capacity_output_file",
            )},
            "document_revision": state.get("revision"),
            "document": state.get("document"),
            "source_excel_path": state.get("source_excel_path"),
            "shared": shared,
        })

    def _check_version(self, session_id: str, version: str) -> Dict[str, Any]:
        try:
            snapshot = self._snapshot(session_id)
        except (ReviewSessionNotFoundError, HandoverDeliveryPrepareError) as exc:
            raise HandoverDeliveryPrepareRetryableError("审核会话或文档已变化，请重试") from exc
        if self._version(snapshot) != version:
            raise HandoverDeliveryPrepareRetryableError("审核会话或文档版本已变化，请重试")
        return snapshot[0]

    @staticmethod
    def _require_success(job: Dict[str, Any], label: str) -> None:
        if not isinstance(job, dict) or job.get("status") != "success":
            raise HandoverDeliveryPrepareError(f"{label}未成功，不能准备交付成品")

    def prepare(
        self,
        session_id: str,
        expected_revision: int | None = None,
        emit_log: Callable[[str], None] = print,
        runtime: Any = None,
        *,
        expected_document_revision: int | None = None,
    ) -> Dict[str, Any]:
        def check_cancelled() -> None:
            if runtime is not None:
                runtime.raise_if_cancelled()

        check_cancelled()
        session_id = str(session_id or "").strip()
        initial_session = self.review_service.get_session_by_id(session_id)
        if not isinstance(initial_session, dict):
            raise ReviewSessionNotFoundError("审核会话不存在")
        self.document_state.ensure_document_for_session(initial_session)
        snapshot = self._snapshot(session_id)
        session, state, shared = snapshot
        revision = int(session["revision"])
        document_revision = int(state["revision"])
        if expected_revision is not None and int(expected_revision) != revision:
            raise HandoverDeliveryPrepareRetryableError("审核会话版本与预期不一致，请重试")
        if expected_document_revision is not None and int(expected_document_revision) != document_revision:
            raise HandoverDeliveryPrepareRetryableError("审核文档已变更，请重新提交")
        version = self._version(snapshot)
        for key in ("output_file", "capacity_output_file"):
            value = str(session.get(key, "") or "").strip()
            if not value or not Path(value).is_file():
                raise HandoverDeliveryPrepareError(f"交付源文件不存在: {key}")

        queue = HandoverXlsxWriteQueueService(
            self.config, review_service=self.review_service,
            document_state=self.document_state, emit_log=emit_log,
        )
        building = session["building"]
        queued = queue.enqueue_review_excel_sync(
            session, target_revision=document_revision, force_all_regions=True,
        )
        if queued.get("status") == "failed":
            raise HandoverDeliveryPrepareError("日志Excel同步入队失败")
        try:
            # A successful barrier does not imply preceding jobs succeeded.
            self._require_success(queue.wait_for_barrier(
                building=building, session_id=session_id, reason="delivery_prepare_log",
            ), "日志队列屏障")
            check_cancelled()
            session = self._check_version(session_id, version)
            sync = self.document_state._store(building).get_sync_state(session_id)
            if (sync.get("status") != "synced"
                    or int(sync.get("synced_revision", 0) or 0) != document_revision
                    or int(sync.get("pending_revision", 0) or 0) != 0):
                raise HandoverDeliveryPrepareError("日志Excel尚未同步到当前文档版本")
            document, _ = self.document_state.load_document(session)
            cells = HandoverCapacityReportService.extract_tracked_cells_from_review_document(document)
            self._check_version(session_id, version)
            job = queue.enqueue_capacity_overlay_sync(session, tracked_cells=cells, use_saved_shared_110kv=True)
            if not job.get("job_id"):
                raise HandoverDeliveryPrepareError("容量Excel同步入队失败")
            self._require_success(queue.wait_for_job(
                building=building, job_id=str(job["job_id"]),
            ), "容量Excel同步")
            self._require_success(queue.wait_for_barrier(
                building=building, session_id=session_id, reason="delivery_prepare_capacity",
            ), "容量队列屏障")
        except TimeoutError as exc:
            raise HandoverDeliveryPrepareRetryableError("Excel同步等待超时，请重试") from exc
        check_cancelled()
        session = self._check_version(session_id, version)
        capacity_sync = session.get("capacity_sync", {})
        if capacity_sync.get("status") != "ready":
            raise HandoverDeliveryPrepareError("容量Excel补写未就绪")

        sources = {"log": Path(session["output_file"]), "capacity": Path(session["capacity_output_file"])}
        fingerprints = {key: _fingerprint(path) for key, path in sources.items()}
        session_root = _runtime_root(self.config) / "handover" / "delivery" / hashlib.sha256(
            session_id.encode("utf-8")
        ).hexdigest()
        session_root.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".prepare-", dir=session_root))
        names = {"log": "log.xlsx", "capacity": "capacity.xlsx", "image": "capacity.png", "text": "handover.txt"}
        paths = {key: staging / name for key, name in names.items()}

        def check_sources() -> None:
            for key, source in sources.items():
                if _fingerprint(source) != fingerprints[key]:
                    raise HandoverDeliveryPrepareRetryableError("源Excel在准备期间被改写，请重试")
            self._check_version(session_id, version)

        try:
            for key, source in sources.items():
                shutil.copy2(source, paths[key])
                if _digest(paths[key]) != fingerprints[key][-1]:
                    raise HandoverDeliveryPrepareRetryableError("源Excel复制期间发生变化，请重试")
            check_sources()
            signature = self.image_service.capacity_service.build_capacity_overlay_signature(
                building=building, duty_date=session.get("duty_date", ""),
                duty_shift=session.get("duty_shift", ""), handover_cells=cells,
                capacity_output_file=str(paths["capacity"]), shared_110kv=shared,
                cooling_pump_pressures=document.get("cooling_pump_pressures", {}),
                emit_log=emit_log,
            )
            if (not signature.get("valid") or not signature.get("signature")
                    or not signature.get("input_signature")
                    or signature["input_signature"] != capacity_sync.get("input_signature")):
                raise HandoverDeliveryPrepareError("容量Excel签名与已同步输入不一致")
            self.image_service._validate_capacity_workbook_ready_for_image(
                source_path=paths["capacity"], signature_info=signature, emit_log=emit_log,
            )
            check_cancelled()
            # This entry point owns both the FIFO screenshot queue and process file lock.
            self.image_service._render_capacity_report_image(
                source_path=paths["capacity"], output_path=paths["image"], emit_log=emit_log,
            )
            self.image_service._validate_rendered_image_content(paths["image"])
            check_cancelled()
            summary_session = copy.deepcopy(session)
            summary_session.update(output_file=str(paths["log"]), capacity_output_file=str(paths["capacity"]))
            summary_session["cooling_pump_pressures"] = copy.deepcopy(document.get("cooling_pump_pressures", {}))
            message = self.summary_service.build_for_session(summary_session, emit_log=emit_log)
            if (not isinstance(message, str) or not message.strip()
                    or "审核链接：" in message or "/handover/review/" in message):
                raise HandoverDeliveryPrepareError("交接文本生成失败")
            paths["text"].write_text(message, encoding="utf-8")
            hashes = {key: _digest(path) for key, path in paths.items()}
            if any(hashes[key] != fingerprints[key][-1] for key in sources):
                raise HandoverDeliveryPrepareRetryableError("成品源副本在生成期间被改写，请重试")
            check_cancelled()
            check_sources()
            destination = session_root / f"{revision}-{_json_digest(hashes)}"
            try:
                staging.rename(destination)
            except OSError:
                # Concurrent identical results may be reused, never overwritten.
                if not destination.is_dir() or any(
                    not (destination / names[key]).is_file()
                    or _digest(destination / names[key]) != hashes[key] for key in names
                ):
                    raise
            self._check_version(session_id, version)
            return {
                "session_id": session_id, "building": building,
                "batch_key": session.get("batch_key", ""), "revision": revision,
                "document_revision": document_revision,
                "paths": {key: str(destination / name) for key, name in names.items()},
                "hashes": hashes,
            }
        finally:
            # Only this invocation's private staging directory is disposable.
            if staging.exists():
                shutil.rmtree(staging)
