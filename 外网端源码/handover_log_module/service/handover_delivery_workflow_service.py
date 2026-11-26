from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import uuid

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableRateLimited
from handover_log_module.api.facade import load_handover_config
from handover_log_module.service.handover_delivery_state_service import HandoverDeliveryStateService
from handover_log_module.service.handover_generation_guard import try_host_file_lock
from handover_log_module.service.review_session_service import ReviewSessionConflictError, ReviewSessionService
from handover_log_module.service.handover_attachment_store import HandoverAttachmentStore, attachment_record_lock


class HandoverDeliveryWorkflowService:
    BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")

    def __init__(self, config):
        self.runtime_config = config
        self.config = config if "_global_paths" in config else load_handover_config(config)
        self.state = HandoverDeliveryStateService(self.config)
        self.review = ReviewSessionService(self.config, initialize_generated_file_index=False)

    def prepare(self, session_id, generation_id, expected_revision, emit_log, runtime=None, *, expected_document_revision=None):
        from handover_log_module.service.handover_delivery_prepare_service import (
            HandoverDeliveryPrepareRetryableError, HandoverDeliveryPrepareService,
        )
        self.state.update(session_id, generation_id, status="preparing", artifacts_ready=False)
        try:
            result = HandoverDeliveryPrepareService(self.runtime_config).prepare(
                session_id, expected_revision=expected_revision, emit_log=emit_log, runtime=runtime,
                expected_document_revision=expected_document_revision,
            )
            session = self.review.get_session_by_id(session_id)
            if not session or int(session.get("revision", 0)) != result["revision"]:
                raise HandoverDeliveryPrepareRetryableError("审核内容在生成期间已变更，请重新生成")
            if runtime:
                runtime.raise_if_cancelled()
            with attachment_record_lock(f"prepare-admit:{session_id}", timeout=3):
                if self.state.get(session_id).get("generation_id") != generation_id:
                    raise HandoverDeliveryPrepareRetryableError("成品任务已被新版本替代")
                if not session.get("confirmed"):
                    try:
                        session, _ = self.review.mark_confirmed(
                            building=session["building"], session_id=session_id, confirmed=True,
                            base_revision=result["revision"], compare_and_set=True,
                        )
                    except ReviewSessionConflictError as exc:
                        raise HandoverDeliveryPrepareRetryableError(str(exc)) from exc
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
                    session = self.review.get_session_by_id(session_id)
                    if not session or (not confirm_unconfirmed and not session.get("confirmed")):
                        return None
                    current = self.state.for_session(session)
                    if current.get("status") in self.state.ACTIVE:
                        emit_log(f"[交接班][成品] {session_id} 已有任务在准备，等待现有任务")
                        return None
                    if current.get("artifacts_ready") and self.state.artifacts_exist(current):
                        return None
                    generation_id = uuid.uuid4().hex
                    self.state.begin(session, generation_id)
                self.prepare(session_id, generation_id, int(session["revision"]), emit_log, runtime=runtime)
                return None
            except Exception as exc:
                emit_log(f"[交接班][成品] {building} 生成未完成: {exc}")
                failure = {"building": building, "error": str(exc)}
                if getattr(exc, "retryable", False):
                    failure["retryable"] = True
                return failure

        candidates = [s for s in sessions if s.get("building") in self.BUILDINGS
                      and (s.get("confirmed") or confirm_unconfirmed)]

        def prepare_when_generation_idle(session):
            try:
                with try_host_file_lock(session["session_id"], namespace="generation") as locked:
                    if not locked:
                        return {"building": session["building"], "error": "本楼正在生成交接班文件", "retryable": True}
                    return prepare_one(session)
            except OSError as exc:
                return {"building": session["building"], "error": f"本楼文件锁暂不可用: {exc}", "retryable": True}

        with ThreadPoolExecutor(max_workers=len(self.BUILDINGS), thread_name_prefix="handover-delivery") as pool:
            return [failure for failure in pool.map(prepare_when_generation_idle, candidates) if failure]

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

    def record_upload_error(self, batch_key, error, *, building=""):
        for name, row in self._current_batch(batch_key).items():
            if (building and name != building) or row.get("status") == "uploaded":
                continue
            try:
                self.state.update(row["session_id"], row["generation_id"], upload_error=error)
            except RuntimeError:
                continue

    def publish_ready_batch(self, batch_key, emit_log, *, building=""):
        if not self.config.get("daily_report_bitable_export", {}).get("enabled", True):
            return {"status": "skipped", "reason": "disabled", "uploaded_buildings": [],
                    "failed_buildings": [], "skipped_buildings": []}
        if building and building not in self.BUILDINGS:
            raise ValueError("无效的交接班楼栋")
        requested = (building,) if building else self.BUILDINGS
        with attachment_record_lock(f"publish:{batch_key}"):
            manifests = self._current_batch(batch_key)
            candidates = [name for name in requested if name in manifests]
            if not candidates:
                return {"status": "pending", "reason": "waiting_for_artifacts", "missing_buildings": list(requested),
                        "pending_buildings": [{"building": name, "reason": "waiting_for_artifacts"} for name in requested],
                        "uploaded_buildings": [], "failed_buildings": [], "skipped_buildings": []}
            date, shift = batch_key.split("|")
            store = HandoverAttachmentStore(self.config)
            # batch 级 token 用稳定代理值（不绑定单一记录），供 H/110/abcdeh 补充同步「已同步过一次」门控；
            # 每楼各自的多维记录 token/url 存在 session 级 cloud_sheet_sync 与 details 里。
            token = f"bitable:{batch_key}"
            changed, failed = [], []
            verified = {}
            details: dict[str, dict[str, str]] = {}
            representative_url = ""

            def filenames(name):
                return (f"{name}_handover.xlsx", f"{name}_capacity.xlsx")

            for name in candidates:
                row = manifests[name]
                phase = "记录准备"
                try:
                    record = store.ensure_record(date, shift, name, preferred_record_url=row.get("record_url", ""))
                    url = record["record_url"]
                    building_token = f"bitable:{record['record_id']}"
                    details[name] = {"record_id": record["record_id"], "record_url": url,
                                     "spreadsheet_token": building_token, "spreadsheet_url": url}
                    existing_names = set(record.get("attachment_names", []))
                    if (row.get("status") == "uploaded" and row.get("record_url") == url
                            and set(filenames(name)) <= existing_names):
                        representative_url = representative_url or url
                        verified[name] = row["generation_id"]
                        continue
                    if row.get("record_url") and row["record_url"] != url:
                        emit_log(f"[交接班][班次附件] {batch_key} {name} 原记录与当前班次不匹配，改用正确记录补传")

                    def is_current():
                        current = self._current_batch(batch_key).get(name, {})
                        return current.get("generation_id") == row["generation_id"]

                    phase = "上传"
                    if not self.state.artifacts_exist(row):
                        raise RuntimeError(f"{name} 已准备成品缺失，请重新生成")
                    pair = dict(zip(filenames(name), (row["paths"]["log"], row["paths"]["capacity"])))
                    store.upload_files(
                        date, shift, pair, building=name, is_current=is_current,
                        expected_record_id=record["record_id"],
                    )
                    updated = self.review.update_cloud_sheet_sync(
                        session_id=row["session_id"], expected_revision=row["revision"], cloud_sheet_sync={
                            "status": "success", "spreadsheet_token": building_token, "spreadsheet_url": url,
                            "synced_revision": row["revision"],
                        },
                    )
                    if not updated:
                        raise RuntimeError("审核内容已变更，请重新提交")
                    self.state.update(row["session_id"], row["generation_id"], status="uploaded",
                                      artifacts_ready=True, record_url=url, upload_error="")
                    representative_url = representative_url or url
                    verified[name] = row["generation_id"]
                    changed.append(name)
                except Exception as exc:
                    failed.append({"building": name, "error": str(exc)})
                    emit_log(f"[交接班][班次附件] {batch_key} {name} {phase}未完成: {exc}")
                    try:
                        self.state.update(row["session_id"], row["generation_id"], status="ready",
                                          upload_error=str(exc))
                    except RuntimeError:
                        pass
                    if isinstance(exc, FeishuBitableRateLimited):
                        break
            current = self._current_batch(batch_key)

            completed_urls = {
                name: row["record_url"] for name, row in current.items()
                if row.get("status") == "uploaded" and row.get("record_url")
                and (name not in requested or verified.get(name) == row["generation_id"])
            }
            url_counts = Counter(completed_urls.values())
            completed = [name for name in self.BUILDINGS if url_counts[completed_urls.get(name)] == 1]
            missing = [name for name in self.BUILDINGS if name not in completed]
            status = "partial_failed" if failed and completed else "failed" if failed else (
                "pending" if any(name in missing for name in requested) else "ok"
            )
            previous = self.review.get_cloud_batch_fast(batch_key) or {}
            if previous.get("spreadsheet_token") != token:
                previous = {}
            meta = {
                **previous, "status": "success" if not missing else "prepared",
                "spreadsheet_token": token, "spreadsheet_url": representative_url,
                "spreadsheet_title": "班次交接班附件",
            }
            self.review.register_cloud_batch(batch_key=batch_key, duty_date=date, duty_shift=shift, cloud_batch=meta)
            signature = hashlib.sha256("|".join(current[b]["generation_id"] for b in self.BUILDINGS).encode()).hexdigest() if not missing else ""
            return {**meta, "status": status, "record_id": "", "record_url": representative_url,
                    "all_buildings_uploaded": not missing, "missing_buildings": missing,
                    "completed_buildings": completed, "details": details,
                    "pending_buildings": [{"building": name, "reason": "waiting_for_upload"} for name in missing
                                          if name not in {item["building"] for item in failed}],
                    "attachment_generation_signature": signature,
                    "uploaded_buildings": changed, "failed_buildings": failed, "skipped_buildings": [],
                    "error": "；".join(f"{item['building']}：{item['error']}" for item in failed)}
