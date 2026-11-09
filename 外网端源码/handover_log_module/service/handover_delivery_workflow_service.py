from concurrent.futures import ThreadPoolExecutor
import hashlib
import uuid

from handover_log_module.api.facade import load_handover_config
from handover_log_module.service.handover_delivery_state_service import HandoverDeliveryStateService
from handover_log_module.service.review_session_service import ReviewSessionService
from handover_log_module.service.handover_attachment_store import HandoverAttachmentStore, attachment_record_lock


class HandoverDeliveryWorkflowService:
    BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")

    def __init__(self, config):
        self.runtime_config = config
        self.config = config if "_global_paths" in config else load_handover_config(config)
        self.state = HandoverDeliveryStateService(self.config)
        self.review = ReviewSessionService(self.config, initialize_generated_file_index=False)

    def prepare(self, session_id, generation_id, expected_revision, emit_log, runtime=None, *, expected_document_revision=None):
        from handover_log_module.service.handover_delivery_prepare_service import HandoverDeliveryPrepareService
        self.state.update(session_id, generation_id, status="preparing", artifacts_ready=False)
        try:
            result = HandoverDeliveryPrepareService(self.runtime_config).prepare(
                session_id, expected_revision=expected_revision, emit_log=emit_log, runtime=runtime,
                expected_document_revision=expected_document_revision,
            )
            session = self.review.get_session_by_id(session_id)
            if not session or int(session.get("revision", 0)) != result["revision"]:
                raise RuntimeError("审核内容在生成期间已变更，请重新生成")
            if runtime:
                runtime.raise_if_cancelled()
            with attachment_record_lock(f"prepare-admit:{session_id}", timeout=3):
                if self.state.get(session_id).get("generation_id") != generation_id:
                    raise RuntimeError("成品任务已被新版本替代")
                if not session.get("confirmed"):
                    session, _ = self.review.mark_confirmed(
                        building=session["building"], session_id=session_id, confirmed=True,
                        base_revision=result["revision"], compare_and_set=True,
                    )
                return self.state.update(
                    session_id, generation_id, status="ready", artifacts_ready=True,
                    paths=result["paths"], hashes=result["hashes"], revision=int(session["revision"]), error="",
                    document_revision=result["document_revision"],
                )
        except Exception as exc:
            if self.state.get(session_id).get("generation_id") == generation_id:
                self.state.update(session_id, generation_id, status="failed", artifacts_ready=False, error=str(exc))
            raise

    def prepare_sessions(self, sessions, emit_log, *, confirm_unconfirmed=False, runtime=None):
        def prepare_one(session):
            building, session_id = session["building"], session["session_id"]
            try:
                with attachment_record_lock(f"prepare-admit:{session_id}", timeout=3):
                    current = self.state.for_session(session)
                    if current.get("status") in self.state.ACTIVE or (
                        current.get("artifacts_ready") and self.state.artifacts_exist(current)
                    ):
                        return None
                    generation_id = uuid.uuid4().hex
                    self.state.begin(session, generation_id)
                self.prepare(session_id, generation_id, int(session["revision"]), emit_log, runtime=runtime)
                return None
            except Exception as exc:
                emit_log(f"[交接班][成品] {building} 生成未完成: {exc}")
                return {"building": building, "error": str(exc)}

        candidates = [s for s in sessions if s.get("building") in self.BUILDINGS
                      and (s.get("confirmed") or confirm_unconfirmed)]
        with ThreadPoolExecutor(max_workers=len(self.BUILDINGS), thread_name_prefix="handover-delivery") as pool:
            return [failure for failure in pool.map(prepare_one, candidates) if failure]

    def _current_batch(self, batch_key):
        rows = self.state.batch(batch_key)
        sessions = self.review.list_batch_sessions(batch_key)
        return {
            session["building"]: rows[session["building"]]
            for session in sessions
            if session.get("confirmed") and session["building"] in rows
            and rows[session["building"]].get("artifacts_ready")
            and rows[session["building"]].get("session_id") == session["session_id"]
            and rows[session["building"]].get("revision") == int(session.get("revision", 0))
        }

    def record_upload_error(self, batch_key, error):
        for row in self._current_batch(batch_key).values():
            try:
                self.state.update(row["session_id"], row["generation_id"], upload_error=error)
            except RuntimeError:
                continue

    def publish_ready_batch(self, batch_key, emit_log):
        if not self.config.get("daily_report_bitable_export", {}).get("enabled", True):
            return {"status": "skipped", "reason": "disabled", "uploaded_buildings": [],
                    "failed_buildings": [], "skipped_buildings": []}
        current = self._current_batch(batch_key)
        missing = [b for b in self.BUILDINGS if b not in current]
        if missing:
            emit_log(f"[交接班][班次附件] 本楼成品已就绪，等待其他楼: {','.join(missing)}")
            return {"status": "pending", "reason": "waiting_for_buildings", "missing_buildings": missing,
                    "pending_buildings": [{"building": b, "reason": "waiting_for_artifacts"} for b in missing],
                    "uploaded_buildings": [], "failed_buildings": [], "skipped_buildings": []}
        with attachment_record_lock(f"publish:{batch_key}"):
            manifests = self._current_batch(batch_key)
            if any(b not in manifests for b in self.BUILDINGS):
                return {"status": "pending", "reason": "generation_changed", "uploaded_buildings": [],
                        "failed_buildings": [], "skipped_buildings": []}
            date, shift = batch_key.split("|")
            store = HandoverAttachmentStore(self.config)
            record = store.ensure_record(date, shift)
            url = record["record_url"]
            token = f"bitable:{record['record_id']}"
            files, changed = {}, []
            unchanged_names = set()
            existing_names = set(record.get("attachment_names", []))
            for building in self.BUILDINGS:
                row = manifests[building]
                paths = row.get("paths", {})
                if not self.state.artifacts_exist(row):
                    raise RuntimeError(f"{building} 已准备成品缺失，请重新生成")
                pair = {f"{building}_handover.xlsx": paths["log"], f"{building}_capacity.xlsx": paths["capacity"]}
                if row.get("status") == "uploaded" and row.get("record_url") == url and pair.keys() <= existing_names:
                    unchanged_names.update(pair)
                    continue
                files.update(pair)
                changed.append(building)

            def is_current():
                current = self._current_batch(batch_key)
                return all(current.get(b, {}).get("generation_id") == manifests[b]["generation_id"] for b in self.BUILDINGS)

            if files:
                store.upload_files(
                    date, shift, files, is_current=is_current, expected_record_id=record["record_id"],
                    required_existing_names=unchanged_names,
                )
            if not is_current():
                return {"status": "pending", "reason": "generation_changed", "uploaded_buildings": [],
                        "failed_buildings": [], "skipped_buildings": []}
            previous = self.review.get_cloud_batch_fast(batch_key) or {}
            if previous.get("spreadsheet_token") != token:
                previous = {}
            meta = {
                **previous, "status": "success", "spreadsheet_token": token, "spreadsheet_url": url,
                "spreadsheet_title": "班次交接班附件",
            }
            self.review.register_cloud_batch(batch_key=batch_key, duty_date=date, duty_shift=shift, cloud_batch=meta)
            for building in changed:
                row = manifests[building]
                if self.state.get(row["session_id"]).get("generation_id") != row["generation_id"]:
                    continue
                session = self.review.get_session_by_id(row["session_id"]) or {}
                if int(session.get("revision", 0)) != row["revision"]:
                    continue
                updated = self.review.update_cloud_sheet_sync(session_id=row["session_id"], expected_revision=row["revision"], cloud_sheet_sync={
                    "status": "success", "spreadsheet_token": token, "spreadsheet_url": url,
                    "synced_revision": row["revision"],
                })
                if not updated:
                    continue
                self.state.update(row["session_id"], row["generation_id"], status="uploaded",
                                  artifacts_ready=True, record_url=url, upload_error="")
            signature = hashlib.sha256("|".join(manifests[b]["generation_id"] for b in self.BUILDINGS).encode()).hexdigest()
            return {**meta, "status": "ok", "record_id": record["record_id"], "record_url": url,
                    "attachment_generation_signature": signature,
                    "uploaded_buildings": changed, "failed_buildings": [], "skipped_buildings": []}
