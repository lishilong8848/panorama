from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Callable, Dict, List

from handover_log_module.core.shift_window import format_duty_date_text
from handover_log_module.repository.excel_reader import load_workbook_quietly
from handover_log_module.repository.shift_roster_repository import ShiftRosterRepository
from handover_log_module.service.handover_daily_report_bitable_export_service import (
    HandoverDailyReportBitableExportService,
)
from handover_log_module.service.handover_cabinet_shift_record_bitable_export_service import (
    HandoverCabinetShiftRecordBitableExportService,
)
from handover_log_module.service.handover_daily_report_state_service import HandoverDailyReportStateService
from handover_log_module.service.handover_110_station_upload_service import Handover110StationUploadService
from handover_log_module.service.handover_cloud_sheet_sync_service import HandoverCloudSheetSyncService
from handover_log_module.service.handover_summary_message_service import HandoverSummaryMessageService
from handover_log_module.service.review_document_state_service import (
    ReviewDocumentStateError,
    ReviewDocumentStateService,
)
from handover_log_module.service.review_session_service import ReviewSessionNotFoundError, ReviewSessionService
from handover_log_module.service.source_data_attachment_bitable_export_service import (
    SourceDataAttachmentBitableExportService,
)

_STATION_H_LONG_DAY_ROLE_BY_NAME = {
    "梅冰冰": "设施运维经理",
    "马进宇": "设施运维副经理",
    "曹李培": "综合管理",
    "王庆华": "暖通主管",
    "周海祥": "电气主管",
    "曹毅": "弱电主管",
    "明志勇": "安全&消防工程师",
    "高荣": "消防主管",
}


def _normalize_export_state(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    return {
        "status": str(payload.get("status", "")).strip().lower(),
        "reason": str(payload.get("reason", "")).strip(),
        "uploaded_count": int(payload.get("uploaded_count", 0) or 0),
        "error": str(payload.get("error", "")).strip(),
        "uploaded_at": str(payload.get("uploaded_at", "")).strip(),
        "record_id": str(payload.get("record_id", "")).strip(),
        "updated_at": str(payload.get("updated_at", "")).strip(),
        "uploaded_revision": int(payload.get("uploaded_revision", 0) or 0),
        "frozen_after_first_full_cloud_sync": bool(payload.get("frozen_after_first_full_cloud_sync", False)),
    }


def _normalize_cloud_sync_state(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    return {
        "attempted": bool(payload.get("attempted", False)),
        "success": bool(payload.get("success", False)),
        "status": str(payload.get("status", "")).strip().lower(),
        "spreadsheet_token": str(payload.get("spreadsheet_token", "")).strip(),
        "spreadsheet_url": str(payload.get("spreadsheet_url", "")).strip(),
        "spreadsheet_title": str(payload.get("spreadsheet_title", "")).strip(),
        "sheet_title": str(payload.get("sheet_title", "")).strip(),
        "synced_revision": int(payload.get("synced_revision", 0) or 0),
        "last_attempt_revision": int(payload.get("last_attempt_revision", 0) or 0),
        "prepared_at": str(payload.get("prepared_at", "")).strip(),
        "updated_at": str(payload.get("updated_at", "")).strip(),
        "error": str(payload.get("error", "")).strip(),
        "synced_row_count": int(payload.get("synced_row_count", 0) or 0),
        "synced_column_count": int(payload.get("synced_column_count", 0) or 0),
        "synced_merges": payload.get("synced_merges", []) if isinstance(payload.get("synced_merges", []), list) else [],
        "dynamic_merge_signature": str(payload.get("dynamic_merge_signature", "")).strip(),
    }


def _normalize_daily_report_export_state(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    return {
        "status": str(payload.get("status", "")).strip().lower(),
        "record_id": str(payload.get("record_id", "")).strip(),
        "record_url": str(payload.get("record_url", "")).strip(),
        "spreadsheet_url": str(payload.get("spreadsheet_url", "")).strip(),
        "updated_at": str(payload.get("updated_at", "")).strip(),
        "error": str(payload.get("error", "")).strip(),
        "error_code": str(payload.get("error_code", "")).strip(),
        "error_detail": str(payload.get("error_detail", "")).strip(),
    }


def _followup_status_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "blocked": "已阻塞",
        "disabled": "已禁用",
        "prepared": "已预建",
        "prepare_failed": "预建失败",
        "pending_upload": "待上传",
        "uploading": "上传中",
        "syncing": "同步中",
        "pending_review": "待确认后上传",
        "success": "成功",
        "ok": "成功",
        "skipped": "已跳过",
        "failed": "失败",
        "partial_failed": "部分失败",
        "idle": "未执行",
        "skipped_due_to_cloud_sync_not_ok": "云表未成功，已跳过",
        "ready": "已就绪",
    }
    return mapping.get(text, text or "-")


def _followup_reason_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "disabled": "配置已禁用",
        "missing_duty_context": "缺少班次上下文",
        "night_shift_disabled": "夜班上传已禁用",
        "await_all_confirmed": "等待楼栋确认",
        "already_uploaded": "已按当前版本完成上传",
        "missing_output_file": "缺少输出文件",
        "missing_sessions": "缺少审核会话",
        "cloud_batch_unavailable": "云表批次不可用",
        "pending_upload": "待上传",
        "session_not_found": "未找到审核会话",
        "missing_session_id": "缺少审核会话 ID",
        "list_existing_failed": "读取旧记录失败",
        "missing_source_file": "源数据文件不存在",
        "missing_source_file_cache": "源文件缓存不存在",
        "upload_error": "上传失败",
        "already_success": "已成功，无需重试",
        "pending_review": "待确认后上传",
        "missing_spreadsheet_url": "缺少云表链接",
        "invalid_duty_context": "班次上下文无效",
        "cloud_sync_pending": "等待云文档同步成功",
        "missing_document": "缺少审核文档",
        "record_sync_failed": "机柜记录写入失败",
        "created": "已新增记录",
        "updated": "已更新记录",
    }
    return mapping.get(text, text or "-")


class ReviewFollowupTriggerService:
    STATIC_SKIP_REASONS = {"disabled", "missing_duty_context", "night_shift_disabled"}

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config if isinstance(config, dict) else {}
        self._review_service = ReviewSessionService(self.config)
        self._source_data_attachment_export_service = SourceDataAttachmentBitableExportService(self.config)
        self._cloud_sheet_sync_service = HandoverCloudSheetSyncService(self.config)
        self._station_110_upload_service = Handover110StationUploadService(self.config)
        self._daily_report_state_service = HandoverDailyReportStateService(self.config)
        self._daily_report_bitable_export_service = HandoverDailyReportBitableExportService(self.config)
        self._cabinet_shift_record_export_service = HandoverCabinetShiftRecordBitableExportService(self.config)
        self._review_document_state_service = ReviewDocumentStateService(self.config)
        self._summary_message_service = HandoverSummaryMessageService(self.config)

    def evaluate(self, batch_status: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = batch_status if isinstance(batch_status, dict) else {}
        ready = bool(payload.get("ready_for_followup_upload", False))
        return {
            "ready_for_followup_upload": ready,
            "blocked_reason": "" if ready else "暂无已确认楼栋",
        }

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _ensure_external_network(self, emit_log: Callable[[str], None]) -> bool:
        emit_log("[交接班][确认后上传] 网络切换功能已移除，按当前网络继续执行")
        return False

    @staticmethod
    def _is_export_complete_for_revision(
        state: Dict[str, Any] | None,
        revision: int,
        *,
        static_skip_reasons: set[str] | None = None,
    ) -> bool:
        normalized = _normalize_export_state(state)
        status = str(normalized.get("status", "")).strip().lower()
        uploaded_revision = int(normalized.get("uploaded_revision", 0) or 0)
        if status in {"ok", "success", "skipped"} and bool(normalized.get("frozen_after_first_full_cloud_sync", False)):
            return True
        if status in {"ok", "success"} and uploaded_revision == int(revision or 0):
            return True
        if status == "skipped":
            reason = str(normalized.get("reason", "")).strip().lower()
            if reason == "already_uploaded":
                return True
            if static_skip_reasons and reason in static_skip_reasons:
                return True
            if uploaded_revision == int(revision or 0):
                return True
        return False

    @staticmethod
    def _is_cloud_sync_complete_for_revision(state: Dict[str, Any] | None, revision: int) -> bool:
        normalized = _normalize_cloud_sync_state(state)
        status = str(normalized.get("status", "")).strip().lower()
        synced_revision = int(normalized.get("synced_revision", 0) or 0)
        if status == "disabled":
            return True
        return status == "success" and synced_revision >= int(revision or 0)

    @staticmethod
    def _is_cloud_sync_failed(state: Dict[str, Any] | None) -> bool:
        status = str(_normalize_cloud_sync_state(state).get("status", "")).strip().lower()
        return status in {"failed", "prepare_failed"}

    def is_first_full_cloud_sync_completed(self, batch_key: str) -> bool:
        return bool(self._review_service.is_first_full_cloud_sync_completed(batch_key))

    @staticmethod
    def _empty_export_result() -> Dict[str, Any]:
        return {
            "uploaded_buildings": [],
            "skipped_buildings": [],
            "failed_buildings": [],
            "details": {},
        }

    def _existing_daily_report_record_export(self, sessions: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not sessions:
            return self._daily_report_export_state(status="idle")
        first = sessions[0]
        duty_date = str(first.get("duty_date", "")).strip()
        duty_shift = str(first.get("duty_shift", "")).strip().lower()
        state = self._daily_report_state_service.get_export_state(
            duty_date=duty_date,
            duty_shift=duty_shift,
        )
        normalized = _normalize_daily_report_export_state(state)
        if str(normalized.get("status", "")).strip().lower():
            return normalized
        return self._daily_report_export_state(status="idle")

    @staticmethod
    def _empty_cabinet_shift_record_export(status: str = "idle", reason: str = "") -> Dict[str, Any]:
        return {
            "status": str(status or "idle").strip().lower() or "idle",
            "reason": str(reason or "").strip(),
            "created_buildings": [],
            "updated_buildings": [],
            "skipped_buildings": [],
            "failed_buildings": [],
            "details": {},
        }

    def _existing_cabinet_shift_record_export(self, sessions: List[Dict[str, Any]]) -> Dict[str, Any]:
        normalized_sessions = [session for session in sessions if isinstance(session, dict)]
        if not normalized_sessions:
            return self._empty_cabinet_shift_record_export()
        details: Dict[str, Dict[str, Any]] = {}
        updated_buildings: List[str] = []
        skipped_buildings: List[Dict[str, str]] = []
        failed_buildings: List[Dict[str, str]] = []
        pending_buildings: List[Dict[str, str]] = []
        for session in normalized_sessions:
            building = str(session.get("building", "")).strip() or "-"
            revision = int(session.get("revision", 0) or 0)
            state = _normalize_export_state(session.get("cabinet_shift_record_export", {}))
            details[building] = state
            status = str(state.get("status", "")).strip().lower()
            reason = str(state.get("reason", "")).strip()
            if self._is_export_complete_for_revision(
                state,
                revision,
                static_skip_reasons={"disabled", "missing_duty_context"},
            ):
                if status == "skipped":
                    skipped_buildings.append({"building": building, "reason": reason or "skipped"})
                else:
                    updated_buildings.append(building)
                continue
            if status == "failed":
                failed_buildings.append({"building": building, "error": str(state.get("error", "")).strip()})
                continue
            pending_buildings.append({"building": building, "reason": reason or status or "pending_upload"})
        if failed_buildings and updated_buildings:
            status = "partial_failed"
        elif failed_buildings:
            status = "failed"
        elif pending_buildings:
            status = "pending"
        elif updated_buildings or skipped_buildings:
            status = "ok"
        else:
            status = "idle"
        return {
            "status": status,
            "reason": "",
            "created_buildings": [],
            "updated_buildings": updated_buildings,
            "skipped_buildings": skipped_buildings,
            "failed_buildings": failed_buildings,
            "pending_buildings": pending_buildings,
            "details": details,
        }

    def _run_cabinet_shift_record_export(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        cloud_result: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        target_batch = str(batch_key or "").strip()
        normalized_sessions = [session for session in sessions if isinstance(session, dict)]
        if not normalized_sessions:
            return self._empty_cabinet_shift_record_export(status="skipped", reason="missing_sessions")
        cloud_status = str(cloud_result.get("status", "")).strip().lower()
        cloud_skips = [item for item in (cloud_result.get("skipped_buildings", []) or []) if isinstance(item, dict)]
        cloud_disabled = bool(cloud_skips) and all(
            str(item.get("reason", "")).strip().lower() == "disabled" for item in cloud_skips
        )
        if cloud_status == "disabled" or cloud_disabled:
            emit_log("[交接班][机柜班次多维] 跳过: 云文档同步已禁用")
            return self._existing_cabinet_shift_record_export(normalized_sessions)
        if cloud_status != "ok" and not self._all_sessions_cloud_synced_current_revision(normalized_sessions):
            emit_log(f"[交接班][机柜班次多维] 跳过: 云表同步状态={_followup_status_text(cloud_status)}")
            return self._existing_cabinet_shift_record_export(normalized_sessions)

        candidates: List[Dict[str, Any]] = []
        skipped_buildings: List[Dict[str, str]] = []
        for session in normalized_sessions:
            building = str(session.get("building", "")).strip() or "-"
            session_id = str(session.get("session_id", "")).strip()
            revision = int(session.get("revision", 0) or 0)
            state = _normalize_export_state(session.get("cabinet_shift_record_export", {}))
            state_status = str(state.get("status", "")).strip().lower()
            state_reason = str(state.get("reason", "")).strip().lower()
            if (
                state_status == "skipped"
                and state_reason in {"disabled", "missing_duty_context"}
                and self._is_export_complete_for_revision(
                    state,
                    revision,
                    static_skip_reasons={"disabled", "missing_duty_context"},
                )
            ):
                skipped_buildings.append({"building": building, "reason": state_reason})
                continue
            if not self._is_cloud_sync_complete_for_revision(session.get("cloud_sheet_sync", {}), revision):
                skipped_buildings.append({"building": building, "reason": "cloud_sync_pending"})
                continue
            if not session_id:
                skipped_buildings.append({"building": building, "reason": "missing_session_id"})
                continue
            candidates.append(session)
        if not candidates:
            existing = self._existing_cabinet_shift_record_export(normalized_sessions)
            existing["skipped_buildings"] = list(existing.get("skipped_buildings", []) or []) + skipped_buildings
            return existing

        emit_log(
            f"[交接班][机柜班次多维] 开始 batch={target_batch}, sessions={len(candidates)}"
        )
        result = self._cabinet_shift_record_export_service.export_sessions(
            sessions=candidates,
            emit_log=emit_log,
        )
        details = result.get("details", {}) if isinstance(result.get("details", {}), dict) else {}
        for session in candidates:
            building = str(session.get("building", "")).strip() or "-"
            session_id = str(session.get("session_id", "")).strip()
            detail = details.get(building, {}) if isinstance(details.get(building, {}), dict) else {}
            if not session_id or not detail:
                continue
            try:
                self._review_service.update_cabinet_shift_record_export(
                    session_id=session_id,
                    cabinet_shift_record_export=detail,
                )
            except ReviewSessionNotFoundError as exc:
                emit_log(
                    f"[交接班][机柜班次多维] 状态回写失败: building={building}, session_id={session_id}, error={exc}"
                )
        result["skipped_buildings"] = list(skipped_buildings) + list(result.get("skipped_buildings", []) or [])
        return result

    def _all_sessions_cloud_synced_current_revision(self, sessions: List[Dict[str, Any]]) -> bool:
        normalized_sessions = [session for session in sessions if isinstance(session, dict)]
        if not normalized_sessions:
            return False
        for session in normalized_sessions:
            revision = int(session.get("revision", 0) or 0)
            cloud_state = _normalize_cloud_sync_state(session.get("cloud_sheet_sync", {}))
            if not self._is_cloud_sync_complete_for_revision(cloud_state, revision):
                return False
        return True

    def _maybe_mark_first_full_cloud_sync_completed(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        emit_log: Callable[[str], None],
    ) -> None:
        target_batch = str(batch_key or "").strip()
        if not target_batch or not sessions:
            return
        if self._review_service.is_first_full_cloud_sync_completed(target_batch):
            return
        if not self._all_sessions_cloud_synced_current_revision(sessions):
            return
        marked = self._review_service.mark_first_full_cloud_sync_completed(batch_key=target_batch)
        if isinstance(marked, dict) and bool(marked.get("first_full_cloud_sync_completed", False)):
            emit_log(f"[交接班][确认后上传] 已标记首次全量云表完成 batch={target_batch}")

    def trigger_after_single_confirm(
        self,
        *,
        batch_key: str,
        building: str,
        session_id: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        target_batch = str(batch_key or "").strip()
        target_building = str(building or "").strip()
        target_session_id = str(session_id or "").strip()
        emit_log(
            f"[交接班][确认后上传] 单楼确认后立即上传云文档: "
            f"batch={target_batch}, building={target_building}"
        )
        return self.trigger_single_building_cloud_sync(
            batch_key=target_batch,
            building=target_building,
            session_id=target_session_id,
            emit_log=emit_log,
        )

    def trigger_single_building_cloud_sync(
        self,
        *,
        batch_key: str,
        building: str,
        session_id: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        target_batch = str(batch_key or "").strip()
        target_building = str(building or "").strip()
        target_session_id = str(session_id or "").strip()
        session = self._resolve_session_for_cloud_sync(
            batch_key=target_batch,
            building=target_building,
            session_id=target_session_id,
        )
        if not isinstance(session, dict):
            return {
                "status": "failed",
                "batch_key": target_batch,
                "uploaded_buildings": [],
                "skipped_buildings": [],
                "failed_buildings": [{"building": target_building, "error": "session_not_found"}],
                "details": {},
                "blocked_reason": "",
                "cloud_sheet_sync": {
                    "status": "failed",
                    "uploaded_buildings": [],
                    "skipped_buildings": [],
                    "failed_buildings": [{"building": target_building, "error": "session_not_found"}],
                    "details": {},
                },
                "daily_report_record_export": self._existing_daily_report_record_export(
                    self._review_service.list_batch_sessions(target_batch)
                ),
                "cabinet_shift_record_export": self._existing_cabinet_shift_record_export(
                    self._review_service.list_batch_sessions(target_batch)
                ),
                "followup_progress": self._collect_followup_progress(
                    batch_key=target_batch,
                    sessions=self._review_service.list_batch_sessions(target_batch),
                    ready=True,
                ),
            }
        if not bool(session.get("confirmed", False)):
            sessions = self._review_service.list_batch_sessions(target_batch)
            return {
                "status": "blocked",
                "batch_key": target_batch,
                "uploaded_buildings": [],
                "skipped_buildings": [],
                "failed_buildings": [{"building": target_building, "error": "pending_review"}],
                "details": {},
                "blocked_reason": "pending_review",
                "cloud_sheet_sync": {
                    "status": "blocked",
                    "uploaded_buildings": [],
                    "skipped_buildings": [],
                    "failed_buildings": [{"building": target_building, "error": "pending_review"}],
                    "details": {},
                    "blocked_reason": "pending_review",
                },
                "daily_report_record_export": self._existing_daily_report_record_export(sessions),
                "cabinet_shift_record_export": self._existing_cabinet_shift_record_export(sessions),
                "followup_progress": self._collect_followup_progress(
                    batch_key=target_batch,
                    sessions=sessions,
                    ready=True,
                ),
            }

        export_result = self._run_session_followup_exports(
            batch_key=target_batch,
            sessions=[session],
            emit_log=emit_log,
        )
        refreshed_session = self._resolve_session_for_cloud_sync(
            batch_key=target_batch,
            building=target_building,
            session_id=target_session_id,
        ) or session
        cloud_result = self._run_cloud_sheet_upload(
            batch_key=target_batch,
            sessions=[refreshed_session],
            emit_log=emit_log,
        )
        refreshed_sessions = self._review_service.list_batch_sessions(target_batch)
        self._maybe_mark_first_full_cloud_sync_completed(
            batch_key=target_batch,
            sessions=refreshed_sessions,
            emit_log=emit_log,
        )
        single_sessions = [
            item
            for item in refreshed_sessions
            if isinstance(item, dict)
            and (
                str(item.get("session_id", "")).strip() == target_session_id
                or str(item.get("building", "")).strip() == target_building
            )
        ] or [session]
        cabinet_shift_record_export = self._run_cabinet_shift_record_export(
            batch_key=target_batch,
            sessions=single_sessions,
            cloud_result=cloud_result,
            emit_log=emit_log,
        )
        refreshed_sessions = self._review_service.list_batch_sessions(target_batch)
        daily_report_record_export = self._existing_daily_report_record_export(refreshed_sessions or [session])
        if self._all_sessions_cloud_synced_current_revision(refreshed_sessions):
            cloud_summary = self._summarize_cloud_sheet_sync(
                batch_key=target_batch,
                sessions=refreshed_sessions,
            )
            daily_report_record_export = self._run_daily_report_record_export(
                batch_key=target_batch,
                sessions=refreshed_sessions,
                cloud_result=cloud_summary,
                emit_log=emit_log,
            )
            refreshed_sessions = self._review_service.list_batch_sessions(target_batch)
        return self._compose_followup_result(
            batch_key=target_batch,
            export_result=export_result,
            cloud_result=cloud_result,
            daily_report_record_export=daily_report_record_export,
            cabinet_shift_record_export=cabinet_shift_record_export,
            sessions=refreshed_sessions or [session],
        )

    def upload_pending_cloud_sheets_for_duty(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        batch_key = self._review_service.build_batch_key(duty_date_text, duty_shift_text)
        return self.upload_pending_cloud_sheets_for_batch(batch_key=batch_key, emit_log=emit_log)

    def upload_pending_cloud_sheets_for_batch(
        self,
        *,
        batch_key: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        target_batch = str(batch_key or "").strip()
        sessions = self._review_service.list_batch_sessions(target_batch)
        if not target_batch or not sessions:
            cloud_result = {
                "status": "skipped",
                "spreadsheet_token": "",
                "spreadsheet_url": "",
                "spreadsheet_title": "",
                "uploaded_buildings": [],
                "skipped_buildings": [],
                "failed_buildings": [],
                "details": {},
            }
            return self._compose_followup_result(
                batch_key=target_batch,
                export_result=self._empty_export_result(),
                cloud_result=cloud_result,
                daily_report_record_export=self._existing_daily_report_record_export(sessions),
                cabinet_shift_record_export=self._existing_cabinet_shift_record_export(sessions),
                sessions=sessions,
            )

        forced_confirmed_buildings: List[str] = []
        force_confirm_failed: List[Dict[str, str]] = []
        for session in sessions:
            if not isinstance(session, dict):
                continue
            building = str(session.get("building", "")).strip()
            session_id = str(session.get("session_id", "")).strip()
            revision = int(session.get("revision", 0) or 0)
            if bool(session.get("confirmed", False)):
                continue
            cloud_status = str(
                _normalize_cloud_sync_state(session.get("cloud_sheet_sync", {})).get("status", "")
            ).strip().lower()
            if cloud_status in {"uploading", "syncing"}:
                continue
            try:
                self._review_service.mark_confirmed(
                    building=building,
                    session_id=session_id,
                    confirmed=True,
                    base_revision=revision,
                    confirmed_by="定时兜底确认",
                )
                forced_confirmed_buildings.append(building)
            except Exception as exc:  # noqa: BLE001
                force_confirm_failed.append({"building": building, "error": str(exc)})

        if forced_confirmed_buildings or force_confirm_failed:
            emit_log(
                "[交接班][云文档补上传] 定时兜底确认完成 "
                f"batch={target_batch}, confirmed={','.join(forced_confirmed_buildings) or '-'}, "
                f"failed={len(force_confirm_failed)}"
            )
            sessions = self._review_service.list_batch_sessions(target_batch)

        pending_sessions: List[Dict[str, Any]] = []
        skipped_buildings: List[Dict[str, str]] = []
        pending_keys: set[tuple[str, str]] = set()
        for session in sessions:
            if not isinstance(session, dict):
                continue
            building = str(session.get("building", "")).strip()
            session_id = str(session.get("session_id", "")).strip()
            revision = int(session.get("revision", 0) or 0)
            if not bool(session.get("confirmed", False)):
                continue
            cloud_status = str(
                _normalize_cloud_sync_state(session.get("cloud_sheet_sync", {})).get("status", "")
            ).strip().lower()
            if cloud_status in {"uploading", "syncing"}:
                skipped_buildings.append({"building": building, "reason": cloud_status})
                continue
            if self._is_cloud_sync_complete_for_revision(session.get("cloud_sheet_sync", {}), revision):
                skipped_buildings.append({"building": building, "reason": "already_uploaded"})
                continue
            pending_sessions.append(session)
            pending_keys.add((session_id, building))

        emit_log(
            f"[交接班][云文档补上传] 规划完成 batch={target_batch}, "
            f"待上传={len(pending_sessions)}, 已跳过={len(skipped_buildings)}"
        )
        if not pending_sessions:
            cloud_summary = self._summarize_cloud_sheet_sync(batch_key=target_batch, sessions=sessions)
            cloud_summary["skipped_buildings"] = skipped_buildings + list(
                cloud_summary.get("skipped_buildings", []) or []
            )
            cloud_summary["failed_buildings"] = force_confirm_failed + list(
                cloud_summary.get("failed_buildings", []) or []
            )
            cloud_summary = self._refresh_cloud_result_status(cloud_summary)
            return self._compose_followup_result(
                batch_key=target_batch,
                export_result=self._empty_export_result(),
                cloud_result=cloud_summary,
                daily_report_record_export=self._existing_daily_report_record_export(sessions),
                cabinet_shift_record_export=self._existing_cabinet_shift_record_export(sessions),
                sessions=sessions,
            )

        export_result = self._run_session_followup_exports(
            batch_key=target_batch,
            sessions=pending_sessions,
            emit_log=emit_log,
        )
        refreshed_sessions = self._review_service.list_batch_sessions(target_batch)
        refreshed_pending_sessions = [
            session
            for session in refreshed_sessions
            if (
                str(session.get("session_id", "")).strip(),
                str(session.get("building", "")).strip(),
            )
            in pending_keys
        ] or pending_sessions

        self._ensure_external_network(emit_log)
        cloud_result = self._run_cloud_sheet_upload(
            batch_key=target_batch,
            sessions=refreshed_pending_sessions,
            emit_log=emit_log,
        )
        cloud_result["skipped_buildings"] = skipped_buildings + list(cloud_result.get("skipped_buildings", []) or [])
        cloud_result["failed_buildings"] = force_confirm_failed + list(cloud_result.get("failed_buildings", []) or [])
        cloud_result = self._refresh_cloud_result_status(cloud_result)
        refreshed_sessions = self._review_service.list_batch_sessions(target_batch)
        self._maybe_mark_first_full_cloud_sync_completed(
            batch_key=target_batch,
            sessions=refreshed_sessions,
            emit_log=emit_log,
        )

        refreshed_pending_sessions = [
            session
            for session in refreshed_sessions
            if (
                str(session.get("session_id", "")).strip(),
                str(session.get("building", "")).strip(),
            )
            in pending_keys
        ] or refreshed_pending_sessions
        cabinet_shift_record_export = self._run_cabinet_shift_record_export(
            batch_key=target_batch,
            sessions=refreshed_pending_sessions,
            cloud_result=cloud_result,
            emit_log=emit_log,
        )
        refreshed_sessions = self._review_service.list_batch_sessions(target_batch)
        daily_report_record_export = self._existing_daily_report_record_export(refreshed_sessions)
        if self._all_sessions_cloud_synced_current_revision(refreshed_sessions):
            cloud_summary = self._summarize_cloud_sheet_sync(
                batch_key=target_batch,
                sessions=refreshed_sessions,
            )
            daily_report_record_export = self._run_daily_report_record_export(
                batch_key=target_batch,
                sessions=refreshed_sessions,
                cloud_result=cloud_summary,
                emit_log=emit_log,
            )
            refreshed_sessions = self._review_service.list_batch_sessions(target_batch)

        return self._compose_followup_result(
            batch_key=target_batch,
            export_result=export_result,
            cloud_result=cloud_result,
            daily_report_record_export=daily_report_record_export,
            cabinet_shift_record_export=cabinet_shift_record_export,
            sessions=refreshed_sessions or sessions,
        )

    def _resolve_session_for_cloud_sync(
        self,
        *,
        batch_key: str,
        building: str,
        session_id: str,
    ) -> Dict[str, Any] | None:
        target_session_id = str(session_id or "").strip()
        if target_session_id:
            session = self._review_service.get_session_by_id(target_session_id)
            if isinstance(session, dict):
                return session
        building_name = str(building or "").strip()
        if not building_name:
            return None
        for session in self._review_service.list_batch_sessions(batch_key):
            if str(session.get("building", "")).strip() == building_name:
                return session
        return None

    def _update_cloud_sheet_sync_resilient(
        self,
        *,
        batch_key: str,
        building: str,
        session_id: str,
        cloud_sheet_sync: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> bool:
        session = self._resolve_session_for_cloud_sync(
            batch_key=batch_key,
            building=building,
            session_id=session_id,
        )
        if not isinstance(session, dict):
            emit_log(
                f"[交接班][云表最终上传] 状态回写跳过: batch={batch_key}, building={building}, 原因={_followup_reason_text('session_not_found')}"
            )
            return False
        target_session_id = str(session.get("session_id", "")).strip()
        if not target_session_id:
            emit_log(
                f"[交接班][云表最终上传] 状态回写跳过: batch={batch_key}, building={building}, 原因={_followup_reason_text('missing_session_id')}"
            )
            return False
        try:
            self._review_service.update_cloud_sheet_sync(
                session_id=target_session_id,
                cloud_sheet_sync=cloud_sheet_sync,
            )
            return True
        except ReviewSessionNotFoundError as exc:
            emit_log(
                f"[交接班][云表最终上传] 状态回写失败: batch={batch_key}, building={building}, 错误={exc}"
            )
            return False

    def _summarize_cloud_sheet_sync(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        batch_meta = self._review_service.get_cloud_batch(batch_key) or {}
        uploaded_buildings: List[str] = []
        failed_buildings: List[Dict[str, str]] = []
        skipped_buildings: List[Dict[str, str]] = []
        pending_buildings: List[Dict[str, str]] = []
        for session in sessions:
            building = str(session.get("building", "")).strip()
            if not building:
                continue
            revision = int(session.get("revision", 0) or 0)
            cloud_state = _normalize_cloud_sync_state(session.get("cloud_sheet_sync", {}))
            status = str(cloud_state.get("status", "")).strip().lower()
            if status == "disabled":
                skipped_buildings.append({"building": building, "reason": "disabled"})
                continue
            if self._is_cloud_sync_complete_for_revision(cloud_state, revision):
                uploaded_buildings.append(building)
                continue
            if status in {"failed", "prepare_failed"}:
                failed_buildings.append({"building": building, "error": str(cloud_state.get("error", "")).strip()})
                continue
            pending_buildings.append({"building": building, "reason": status or "pending_upload"})

        if pending_buildings:
            status = "pending"
        elif failed_buildings and uploaded_buildings:
            status = "partial_failed"
        elif failed_buildings:
            status = "failed"
        elif uploaded_buildings:
            status = "ok"
        else:
            status = "skipped"
        return {
            "status": status,
            "spreadsheet_token": str(batch_meta.get("spreadsheet_token", "")).strip(),
            "spreadsheet_url": str(batch_meta.get("spreadsheet_url", "")).strip(),
            "spreadsheet_title": str(batch_meta.get("spreadsheet_title", "")).strip(),
            "uploaded_buildings": uploaded_buildings,
            "skipped_buildings": skipped_buildings,
            "failed_buildings": failed_buildings,
            "pending_buildings": pending_buildings,
            "details": {},
        }

    def _collect_followup_progress(self, *, batch_key: str, sessions: List[Dict[str, Any]], ready: bool) -> Dict[str, Any]:
        batch_status = self._review_service.get_batch_status(batch_key)
        confirmed_sessions = [
            session
            for session in sessions
            if isinstance(session, dict) and bool(session.get("confirmed", False))
        ]
        all_cloud_synced = self._all_sessions_cloud_synced_current_revision(sessions)
        daily_report_status = "idle"
        daily_report_pending = 0
        daily_report_failed = 0
        if sessions:
            first = sessions[0]
            duty_date = str(first.get("duty_date", "")).strip()
            duty_shift = str(first.get("duty_shift", "")).strip().lower()
            daily_report_state = self._daily_report_state_service.get_export_state(
                duty_date=duty_date,
                duty_shift=duty_shift,
            )
            daily_report_status = str(daily_report_state.get("status", "")).strip().lower() or "idle"
            if all_cloud_synced and daily_report_status not in {"success", "skipped"}:
                daily_report_pending = 1
            if all_cloud_synced and daily_report_status == "failed":
                daily_report_failed = 1

        attachment_pending_count = 0
        cabinet_pending_count = 0
        cabinet_failed_count = 0
        cloud_pending_count = 0
        failed_count = daily_report_failed
        for session in confirmed_sessions:
            revision = int(session.get("revision", 0) or 0)
            attachment_state = _normalize_export_state(session.get("source_data_attachment_export", {}))
            if not self._is_export_complete_for_revision(
                attachment_state,
                revision,
                static_skip_reasons=self.STATIC_SKIP_REASONS,
            ):
                attachment_pending_count += 1
            if str(attachment_state.get("status", "")).strip().lower() == "failed":
                failed_count += 1

            cabinet_state = _normalize_export_state(session.get("cabinet_shift_record_export", {}))
            if not self._is_export_complete_for_revision(
                cabinet_state,
                revision,
                static_skip_reasons={"disabled", "missing_duty_context"},
            ):
                cabinet_pending_count += 1
            if str(cabinet_state.get("status", "")).strip().lower() == "failed":
                cabinet_failed_count += 1
                failed_count += 1

            cloud_state = _normalize_cloud_sync_state(session.get("cloud_sheet_sync", {}))
            if not self._is_cloud_sync_complete_for_revision(cloud_state, revision):
                cloud_pending_count += 1
            if self._is_cloud_sync_failed(cloud_state):
                failed_count += 1

        pending_count = attachment_pending_count + cloud_pending_count + cabinet_pending_count + daily_report_pending
        if pending_count <= 0 and failed_count <= 0:
            status = "complete"
        elif pending_count > 0 and failed_count > 0:
            status = "partial_failed"
        elif failed_count > 0:
            status = "failed"
        else:
            status = "pending"
        can_resume_followup = bool(
            confirmed_sessions
            and str(batch_status.get("batch_key", "")).strip()
            and (pending_count > 0 or failed_count > 0)
        )
        return {
            "status": status,
            "can_resume_followup": can_resume_followup,
            "pending_count": pending_count,
            "failed_count": failed_count,
            "attachment_pending_count": attachment_pending_count,
            "cabinet_shift_record_pending_count": cabinet_pending_count,
            "cabinet_shift_record_failed_count": cabinet_failed_count,
            "cloud_pending_count": cloud_pending_count,
            "daily_report_status": daily_report_status,
        }

    def get_followup_progress(self, batch_key: str) -> Dict[str, Any]:
        target_batch = str(batch_key or "").strip()
        if not target_batch:
            return self._collect_followup_progress(batch_key="", sessions=[], ready=False)
        batch_status = self._review_service.get_batch_status(target_batch)
        sessions = self._review_service.list_batch_sessions(target_batch)
        return self._collect_followup_progress(
            batch_key=target_batch,
            sessions=sessions,
            ready=any(bool(session.get("confirmed", False)) for session in sessions if isinstance(session, dict)),
        )

    @staticmethod
    def _normalize_followup_result_cloud_payload(result: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = result if isinstance(result, dict) else {}
        return {
            "status": str(payload.get("status", "")).strip().lower() or "skipped",
            "spreadsheet_token": str(payload.get("spreadsheet_token", "")).strip(),
            "spreadsheet_url": str(payload.get("spreadsheet_url", "")).strip(),
            "spreadsheet_title": str(payload.get("spreadsheet_title", "")).strip(),
            "uploaded_buildings": list(payload.get("uploaded_buildings", []) or []),
            "skipped_buildings": [item for item in (payload.get("skipped_buildings", []) or []) if isinstance(item, dict)],
            "failed_buildings": [item for item in (payload.get("failed_buildings", []) or []) if isinstance(item, dict)],
            "pending_buildings": [item for item in (payload.get("pending_buildings", []) or []) if isinstance(item, dict)],
            "details": dict(payload.get("details", {})) if isinstance(payload.get("details", {}), dict) else {},
        }

    @classmethod
    def _refresh_cloud_result_status(cls, cloud_result: Dict[str, Any]) -> Dict[str, Any]:
        result = cloud_result if isinstance(cloud_result, dict) else {}
        normalized = cls._normalize_followup_result_cloud_payload(result)
        failed = list(normalized.get("failed_buildings", []) or [])
        uploaded = list(normalized.get("uploaded_buildings", []) or [])
        pending = list(normalized.get("pending_buildings", []) or [])
        skipped = list(normalized.get("skipped_buildings", []) or [])
        if failed and uploaded:
            status = "partial_failed"
        elif failed:
            status = "failed"
        elif pending:
            status = "pending"
        elif uploaded:
            status = "ok"
        elif skipped:
            status = "skipped"
        else:
            status = str(result.get("status", "") or "").strip().lower() or "skipped"
        result["status"] = status
        return result

    def _compose_followup_result(
        self,
        *,
        batch_key: str,
        export_result: Dict[str, Any],
        cloud_result: Dict[str, Any],
        daily_report_record_export: Dict[str, Any],
        cabinet_shift_record_export: Dict[str, Any],
        sessions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        normalized_cloud = self._normalize_followup_result_cloud_payload(cloud_result)
        cabinet_result = cabinet_shift_record_export if isinstance(cabinet_shift_record_export, dict) else {}
        combined_uploaded = sorted(
            set(
                export_result.get("uploaded_buildings", [])
                + list(normalized_cloud.get("uploaded_buildings", []) or [])
                + list(cabinet_result.get("created_buildings", []) or [])
                + list(cabinet_result.get("updated_buildings", []) or [])
            )
        )
        combined_failed_map: Dict[str, str] = {}
        failed_items = (
            export_result.get("failed_buildings", [])
            + list(normalized_cloud.get("failed_buildings", []) or [])
            + list(cabinet_result.get("failed_buildings", []) or [])
        )
        for item in failed_items:
            if not isinstance(item, dict):
                continue
            building = str(item.get("building", "")).strip()
            if not building:
                continue
            combined_failed_map[building] = str(item.get("error", "")).strip()
        combined_skipped = (
            export_result.get("skipped_buildings", [])
            + list(normalized_cloud.get("skipped_buildings", []) or [])
            + list(cabinet_result.get("skipped_buildings", []) or [])
        )
        followup_progress = self._collect_followup_progress(batch_key=batch_key, sessions=sessions, ready=True)
        if combined_failed_map and combined_uploaded:
            status = "partial_failed"
        elif combined_failed_map:
            status = "failed"
        elif combined_uploaded:
            status = "ok"
        else:
            status = "skipped"
        if followup_progress.get("status") == "complete" and status == "skipped":
            status = "ok"
        return {
            "status": status,
            "batch_key": str(batch_key or "").strip(),
            "uploaded_buildings": combined_uploaded,
            "skipped_buildings": combined_skipped,
            "failed_buildings": [
                {"building": building, "error": error}
                for building, error in combined_failed_map.items()
            ],
            "details": export_result.get("details", {}),
            "blocked_reason": "",
            "cloud_sheet_sync": normalized_cloud,
            "daily_report_record_export": daily_report_record_export,
            "cabinet_shift_record_export": cabinet_result,
            "followup_progress": followup_progress,
        }

    def _resolve_cloud_batch_meta(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        cloud_cfg = self.config.get("cloud_sheet_sync", {})
        if isinstance(cloud_cfg, dict) and not bool(cloud_cfg.get("enabled", True)):
            emit_log("[交接班][云表最终上传] 已跳过: 当前配置禁用云表同步")
            return {
                "batch_key": str(batch_key or "").strip(),
                "status": "disabled",
                "spreadsheet_token": "",
                "spreadsheet_url": "",
                "spreadsheet_title": "",
                "prepared_at": "",
                "updated_at": self._now_text(),
                "error": "",
            }

        existing = self._review_service.get_cloud_batch(batch_key)
        if isinstance(existing, dict) and str(existing.get("spreadsheet_token", "")).strip():
            status = str(existing.get("status", "")).strip().lower()
            if status in {"prepared", "success"}:
                validation = self._cloud_sheet_sync_service.validate_batch_spreadsheet(
                    batch_meta=existing,
                    emit_log=emit_log,
                )
                if bool(validation.get("valid", False)):
                    return existing
                emit_log(
                    "[交接班][云表预建] 已缓存云文档失效，将自动重新创建: "
                    f"batch={batch_key}, token={str(existing.get('spreadsheet_token', '')).strip()}"
                )

        if not sessions:
            return {
                "batch_key": str(batch_key or "").strip(),
                "status": "prepare_failed",
                "spreadsheet_token": "",
                "spreadsheet_url": "",
                "spreadsheet_title": "",
                "prepared_at": "",
                "updated_at": self._now_text(),
                "error": "missing_sessions",
            }

        first = sessions[0]
        duty_date = str(first.get("duty_date", "")).strip()
        duty_shift = str(first.get("duty_shift", "")).strip().lower()
        duty_date_text = format_duty_date_text(duty_date) if duty_date else ""
        shift_text = "白班" if duty_shift == "day" else "夜班"
        prepared = self._cloud_sheet_sync_service.prepare_batch_spreadsheet(
            duty_date=duty_date,
            duty_date_text=duty_date_text,
            shift_text=shift_text,
            emit_log=emit_log,
        )
        normalized = self._review_service.register_cloud_batch(
            batch_key=batch_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
            cloud_batch=prepared,
        )
        for session in sessions:
            session_id = str(session.get("session_id", "")).strip()
            building = str(session.get("building", "")).strip()
            if not session_id or not building:
                continue
            try:
                self._review_service.attach_cloud_batch_to_session(
                    session_id=session_id,
                    batch_key=batch_key,
                    building=building,
                )
            except Exception:  # noqa: BLE001
                continue
        return normalized

    def _build_cloud_items(
        self,
        sessions: List[Dict[str, Any]],
        *,
        emit_log: Callable[[str], None] = print,
        force_excel_sync: bool = False,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, str]], List[Dict[str, str]]]:
        upload_items: List[Dict[str, Any]] = []
        skipped_buildings: List[Dict[str, str]] = []
        failed_buildings: List[Dict[str, str]] = []
        for session in sessions:
            building = str(session.get("building", "")).strip()
            cloud_state = _normalize_cloud_sync_state(session.get("cloud_sheet_sync", {}))
            output_file = str(session.get("output_file", "")).strip()
            revision = int(session.get("revision", 0) or 0)
            if cloud_state["status"] == "disabled":
                skipped_buildings.append({"building": building, "reason": "disabled"})
                continue
            if cloud_state["status"] in {"uploading", "syncing"}:
                skipped_buildings.append({"building": building, "reason": cloud_state["status"]})
                continue
            if self._is_cloud_sync_complete_for_revision(cloud_state, revision):
                skipped_buildings.append({"building": building, "reason": "already_uploaded"})
                continue
            if not output_file:
                failed_buildings.append({"building": building, "error": "missing_output_file"})
                continue
            if force_excel_sync:
                try:
                    self._review_document_state_service.force_sync_session_dict(
                        session,
                        reason="cloud_upload",
                    )
                except ReviewDocumentStateError as exc:
                    emit_log(
                        "[交接班][云表最终上传] 同步最新审核内容失败 "
                        f"building={building}, session_id={session.get('session_id', '-')}, error={exc}"
                    )
                    failed_buildings.append({"building": building, "error": f"交接班 Excel 未同步到最新审核内容: {exc}"})
                    continue
            upload_items.append(
                {
                    "building": building,
                    "output_file": output_file,
                    "revision": revision,
                }
            )
        return upload_items, skipped_buildings, failed_buildings

    def _persist_cloud_sync_result(
        self,
        *,
        sessions: List[Dict[str, Any]],
        batch_meta: Dict[str, Any],
        cloud_result: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> None:
        batch_key = str(batch_meta.get("batch_key", "") or "").strip()
        uploaded = set(cloud_result.get("uploaded_buildings", []) or [])
        failed_map = {
            str(item.get("building", "")).strip(): str(item.get("error", "")).strip()
            for item in (cloud_result.get("failed_buildings", []) or [])
            if isinstance(item, dict)
        }
        spreadsheet_token = str(cloud_result.get("spreadsheet_token", "") or batch_meta.get("spreadsheet_token", "")).strip()
        spreadsheet_url = str(cloud_result.get("spreadsheet_url", "") or batch_meta.get("spreadsheet_url", "")).strip()
        spreadsheet_title = str(cloud_result.get("spreadsheet_title", "") or batch_meta.get("spreadsheet_title", "")).strip()
        prepared_at = str(batch_meta.get("prepared_at", "")).strip()
        for session in sessions:
            session_id = str(session.get("session_id", "")).strip()
            building = str(session.get("building", "")).strip()
            if not session_id or not building:
                continue
            previous = _normalize_cloud_sync_state(session.get("cloud_sheet_sync", {}))
            revision = int(session.get("revision", 0) or 0)
            if previous["status"] == "disabled":
                continue
            if building in uploaded:
                detail = (
                    cloud_result.get("details", {}).get(building, {})
                    if isinstance(cloud_result.get("details", {}), dict)
                    else {}
                )
                payload = {
                    **previous,
                    "attempted": True,
                    "success": True,
                    "status": "success",
                    "spreadsheet_token": spreadsheet_token,
                    "spreadsheet_url": spreadsheet_url,
                    "spreadsheet_title": spreadsheet_title,
                    "sheet_title": previous["sheet_title"] or building,
                    "synced_revision": revision,
                    "last_attempt_revision": revision,
                    "prepared_at": prepared_at,
                    "updated_at": self._now_text(),
                    "error": "",
                    "synced_row_count": int(detail.get("synced_row_count", detail.get("rows", 0)) or 0),
                    "synced_column_count": int(detail.get("synced_column_count", detail.get("cols", 0)) or 0),
                    "synced_merges": detail.get("synced_merges", previous.get("synced_merges", [])),
                    "dynamic_merge_signature": str(
                        detail.get("dynamic_merge_signature", previous.get("dynamic_merge_signature", ""))
                    ).strip(),
                }
                self._update_cloud_sheet_sync_resilient(
                    batch_key=batch_key,
                    building=building,
                    session_id=session_id,
                    cloud_sheet_sync=payload,
                    emit_log=emit_log,
                )
                continue
            if building in failed_map:
                payload = {
                    **previous,
                    "attempted": True,
                    "success": False,
                    "status": "failed",
                    "spreadsheet_token": spreadsheet_token or previous["spreadsheet_token"],
                    "spreadsheet_url": spreadsheet_url or previous["spreadsheet_url"],
                    "spreadsheet_title": spreadsheet_title or previous["spreadsheet_title"],
                    "sheet_title": previous["sheet_title"] or building,
                    "synced_revision": int(previous.get("synced_revision", 0) or 0),
                    "last_attempt_revision": revision,
                    "prepared_at": prepared_at or previous["prepared_at"],
                    "updated_at": self._now_text(),
                    "error": failed_map[building],
                    "synced_row_count": int(previous.get("synced_row_count", 0) or 0),
                    "synced_column_count": int(previous.get("synced_column_count", 0) or 0),
                    "synced_merges": previous.get("synced_merges", []),
                    "dynamic_merge_signature": str(previous.get("dynamic_merge_signature", "")).strip(),
                }
                self._update_cloud_sheet_sync_resilient(
                    batch_key=batch_key,
                    building=building,
                    session_id=session_id,
                    cloud_sheet_sync=payload,
                    emit_log=emit_log,
                )

    def _mark_cloud_sheet_uploading(
        self,
        *,
        sessions: List[Dict[str, Any]],
        batch_meta: Dict[str, Any],
        upload_items: List[Dict[str, Any]],
        emit_log: Callable[[str], None],
    ) -> None:
        batch_key = str(batch_meta.get("batch_key", "") or "").strip()
        uploading_buildings = {
            str(item.get("building", "")).strip()
            for item in upload_items
            if isinstance(item, dict) and str(item.get("building", "")).strip()
        }
        if not batch_key or not uploading_buildings:
            return
        spreadsheet_token = str(batch_meta.get("spreadsheet_token", "")).strip()
        spreadsheet_url = str(batch_meta.get("spreadsheet_url", "")).strip()
        spreadsheet_title = str(batch_meta.get("spreadsheet_title", "")).strip()
        prepared_at = str(batch_meta.get("prepared_at", "")).strip()
        for session in sessions:
            session_id = str(session.get("session_id", "")).strip()
            building = str(session.get("building", "")).strip()
            if not session_id or building not in uploading_buildings:
                continue
            previous = _normalize_cloud_sync_state(session.get("cloud_sheet_sync", {}))
            if previous["status"] == "disabled":
                continue
            revision = int(session.get("revision", 0) or 0)
            payload = {
                **previous,
                "attempted": True,
                "success": False,
                "status": "uploading",
                "spreadsheet_token": spreadsheet_token or previous["spreadsheet_token"],
                "spreadsheet_url": spreadsheet_url or previous["spreadsheet_url"],
                "spreadsheet_title": spreadsheet_title or previous["spreadsheet_title"],
                "sheet_title": previous["sheet_title"] or building,
                "synced_revision": int(previous.get("synced_revision", 0) or 0),
                "last_attempt_revision": revision,
                "prepared_at": prepared_at or previous["prepared_at"],
                "updated_at": self._now_text(),
                "error": "",
                "synced_row_count": int(previous.get("synced_row_count", 0) or 0),
                "synced_column_count": int(previous.get("synced_column_count", 0) or 0),
                "synced_merges": previous.get("synced_merges", []),
                "dynamic_merge_signature": str(previous.get("dynamic_merge_signature", "")).strip(),
            }
            self._update_cloud_sheet_sync_resilient(
                batch_key=batch_key,
                building=building,
                session_id=session_id,
                cloud_sheet_sync=payload,
                emit_log=emit_log,
            )

    @staticmethod
    def _station_h_split_people(text: Any) -> List[str]:
        raw = str(text or "").strip()
        if "：" in raw:
            raw = raw.split("：", 1)[1]
        elif ":" in raw:
            raw = raw.split(":", 1)[1]
        names = [item.strip() for item in re.split(r"[、,/，；;\s]+", raw) if item.strip()]
        return names

    @staticmethod
    def _station_h_number(value: Any) -> float | int:
        if value is None:
            return 0
        if isinstance(value, bool):
            return 0
        if isinstance(value, (int, float)):
            number = float(value)
        else:
            text = str(value or "").strip()
            if not text or text == "/":
                return 0
            text = text.replace(",", "").replace("，", "")
            match = re.search(r"-?\d+(?:\.\d+)?", text)
            if not match:
                return 0
            try:
                number = float(match.group(0))
            except ValueError:
                return 0
        return int(number) if number.is_integer() else number

    @staticmethod
    def _station_h_long_day_text(value: Any) -> str:
        names = ReviewFollowupTriggerService._station_h_split_people(value)
        if not names:
            return "常白岗：/"
        name_set = set(names)
        filtered_names = [
            name
            for name in _STATION_H_LONG_DAY_ROLE_BY_NAME
            if name in name_set
        ]
        return f"常白岗：{' '.join(filtered_names) if filtered_names else '/'}"

    @staticmethod
    def _station_h_fixed_cell_values(document: Dict[str, Any] | None) -> Dict[str, Any]:
        values: Dict[str, Any] = {}
        fixed_blocks = document.get("fixed_blocks", []) if isinstance(document, dict) else []
        if not isinstance(fixed_blocks, list):
            return values
        for block in fixed_blocks:
            if not isinstance(block, dict):
                continue
            fields = block.get("fields", [])
            if not isinstance(fields, list):
                continue
            for field in fields:
                if not isinstance(field, dict):
                    continue
                cell = str(field.get("cell", "") or "").strip().upper()
                if cell:
                    values[cell] = field.get("value", "")
        return values

    def _station_h_cabinet_values_from_session(self, session: Dict[str, Any]) -> Dict[str, Any]:
        source_cells = {"B12": "B13", "D12": "D13", "F12": "F13", "H12": "H13"}
        try:
            document, _synced_session = self._review_document_state_service.load_document(session)
            fixed_values = self._station_h_fixed_cell_values(document)
            if "B13" in fixed_values or "D13" in fixed_values:
                return {
                    target_cell: fixed_values.get(source_cell, "")
                    for target_cell, source_cell in source_cells.items()
                }
        except Exception:
            pass

        output_file = Path(str(session.get("output_file", "") or "").strip())
        if not output_file.exists():
            raise FileNotFoundError("交接班日志未生成")
        workbook = load_workbook_quietly(output_file, data_only=True)
        try:
            worksheet = workbook["交接班日志"] if "交接班日志" in workbook.sheetnames else workbook.active
            return {
                target_cell: worksheet[source_cell].value
                for target_cell, source_cell in source_cells.items()
            }
        finally:
            workbook.close()

    def _station_h_cabinet_totals(self, sessions: List[Dict[str, Any]]) -> Dict[str, Any]:
        by_building = {
            str(session.get("building", "")).strip(): session
            for session in sessions
            if isinstance(session, dict)
        }
        totals = {"B12": 0.0, "D12": 0.0, "F12": 0.0, "H12": 0.0}
        missing_buildings: List[str] = []
        failed: List[str] = []
        for building in HandoverCloudSheetSyncService.MANAGED_BUILDINGS:
            session = by_building.get(building, {})
            if not session:
                missing_buildings.append(building)
                continue
            try:
                values = self._station_h_cabinet_values_from_session(session)
                for target_cell in totals:
                    totals[target_cell] += float(self._station_h_number(values.get(target_cell, "")))
            except FileNotFoundError:
                missing_buildings.append(building)
            except Exception as exc:  # noqa: BLE001
                failed.append(f"{building}: {exc}")
        if missing_buildings:
            return {
                "ok": False,
                "status": "skipped",
                "reason": "waiting_building_outputs",
                "error": f"等待楼栋交接班日志生成: {','.join(missing_buildings)}",
            }
        if failed:
            return {
                "ok": False,
                "status": "failed",
                "reason": "parse_failed",
                "error": "; ".join(failed),
            }
        return {"ok": True, "totals": {cell: self._station_h_number(value) for cell, value in totals.items()}}

    def _build_station_h_cell_values(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        duty_date, duty_shift = self._review_service.parse_batch_key(batch_key)
        if not duty_date or duty_shift not in {"day", "night"}:
            return {"ok": False, "status": "skipped", "reason": "missing_duty_context", "error": "缺少日期/班次"}

        cabinet_result = self._station_h_cabinet_totals(sessions)
        if not bool(cabinet_result.get("ok", False)):
            return cabinet_result

        try:
            outdoor_state = self._review_service.get_outdoor_temperature_state(batch_key=batch_key)
            outdoor_block = (
                outdoor_state.get("shared_blocks", {}).get("outdoor_temperature", {})
                if isinstance(outdoor_state, dict)
                else {}
            )
            outdoor_cells = outdoor_block.get("cells", {}) if isinstance(outdoor_block, dict) else {}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "status": "failed", "reason": "outdoor_temperature_failed", "error": str(exc)}
        dry_bulb = str(outdoor_cells.get("B7", "") or "").strip()
        wet_bulb = str(outdoor_cells.get("D7", "") or "").strip()
        if not dry_bulb or not wet_bulb:
            return {
                "ok": False,
                "status": "failed",
                "reason": "missing_outdoor_temperature",
                "error": "缺少共享室外干球/湿球温度",
            }

        roster_repo = ShiftRosterRepository(self.config)
        try:
            assignment = roster_repo.query_assignment(
                building="H楼",
                duty_date=duty_date,
                duty_shift=duty_shift,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "status": "failed", "reason": "roster_failed", "error": str(exc)}
        current_names = self._station_h_split_people(assignment.current_people)
        next_names = self._station_h_split_people(assignment.next_people)
        current_first = current_names[0] if current_names else ""
        next_first = assignment.next_first_person or (next_names[0] if next_names else "")
        if not current_names or not next_names or not current_first or not next_first:
            return {
                "ok": False,
                "status": "failed",
                "reason": "missing_roster_people",
                "error": "H楼值班/接班人员未识别",
            }

        try:
            long_day_cells = roster_repo.query_long_day_cell_values(
                building="H楼",
                duty_date=duty_date,
                duty_shift=duty_shift,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][H楼云表] 常白岗查询失败，将使用空值: {exc}")
            long_day_cells = {}
        b4_value = "常白岗：/"
        f4_value = "常白岗：/"
        if duty_shift == "day":
            b4_value = self._station_h_long_day_text(long_day_cells.get("B4", ""))
        else:
            f4_value = self._station_h_long_day_text(long_day_cells.get("F4", ""))

        patrol_times = ("10:00", "15:00") if duty_shift == "day" else ("22：00", "3：00")
        shift_text = "白班" if duty_shift == "day" else "夜班"
        totals = dict(cabinet_result.get("totals", {}))
        cells: Dict[str, Any] = {
            "B2": format_duty_date_text(duty_date),
            "F2": shift_text,
            "C3": current_names[0] if len(current_names) >= 1 else "",
            "D3": current_names[1] if len(current_names) >= 2 else "",
            "G3": next_names[0] if len(next_names) >= 1 else "",
            "H3": next_names[1] if len(next_names) >= 2 else "",
            "B4": b4_value,
            "F4": f4_value,
            "B6": dry_bulb,
            "G6": wet_bulb,
            "B12": totals.get("B12", 0),
            "D12": totals.get("D12", 0),
            "F12": totals.get("F12", 0),
            "H12": totals.get("H12", 0),
            "B15": patrol_times[0],
            "B16": patrol_times[1],
            "H15": current_first,
            "H16": current_first,
        }
        for cell in ("H40", "H41", "H42", "H45", "H46", "H47", "H48"):
            cells[cell] = next_first
        return {"ok": True, "cells": cells}

    def _persist_station_h_sync_result(self, *, batch_key: str, sync_result: Dict[str, Any]) -> None:
        payload = dict(sync_result) if isinstance(sync_result, dict) else {}
        payload["updated_at"] = self._now_text()
        self._review_service.update_cloud_batch_extra_state(
            batch_key=batch_key,
            field="station_h_sync",
            value=payload,
        )

    def _attach_station_h_sync_result(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        cloud_result: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        result = cloud_result if isinstance(cloud_result, dict) else {}
        token = str(result.get("spreadsheet_token", "")).strip()
        if not token:
            result["station_h_sync"] = {"status": "skipped", "reason": "missing_spreadsheet_token"}
            return result
        cloud_status = str(result.get("status", "")).strip().lower()
        if cloud_status == "failed":
            result["station_h_sync"] = {"status": "skipped", "reason": "cloud_upload_failed"}
            return result
        if cloud_status not in {"ok", "skipped"}:
            result["station_h_sync"] = {"status": "skipped", "reason": "cloud_upload_not_complete"}
            return result
        batch_meta = self._review_service.get_cloud_batch(batch_key) or {}
        existing = batch_meta.get("station_h_sync", {}) if isinstance(batch_meta, dict) else {}
        if isinstance(existing, dict) and str(existing.get("status", "")).strip().lower() == "success":
            result["station_h_sync"] = {**existing, "reason": "already_synced_once"}
            return result

        cell_result = self._build_station_h_cell_values(batch_key=batch_key, sessions=sessions, emit_log=emit_log)
        if not bool(cell_result.get("ok", False)):
            station_result = {
                "status": str(cell_result.get("status", "failed") or "failed").strip().lower(),
                "reason": str(cell_result.get("reason", "") or "").strip(),
                "error": str(cell_result.get("error", "") or "").strip(),
            }
            result["station_h_sync"] = station_result
            if station_result["status"] != "skipped":
                self._persist_station_h_sync_result(batch_key=batch_key, sync_result=station_result)
                emit_log(
                    f"[交接班][H楼云表] 同步未完成 batch={batch_key}, "
                    f"status={station_result['status']}, error={station_result.get('error', '')}"
                )
            return result

        try:
            latest_batch_meta = self._review_service.get_cloud_batch(batch_key) or batch_meta
            station_result = self._cloud_sheet_sync_service.sync_station_h_sheet(
                batch_meta=latest_batch_meta,
                cell_values=cell_result.get("cells", {}),
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            station_result = {"status": "failed", "error": str(exc)}
            emit_log(f"[交接班][H楼云表] 同步异常 batch={batch_key}, error={exc}")
        result["station_h_sync"] = station_result
        status = str(station_result.get("status", "")).strip().lower()
        if status in {"success", "failed"}:
            self._persist_station_h_sync_result(batch_key=batch_key, sync_result=station_result)
        if status and status != "skipped":
            emit_log(f"[交接班][H楼云表] 跟随云文档上传完成 batch={batch_key}, status={status}")
        return result

    def _managed_building_cloud_sync_gaps(
        self,
        sessions: List[Dict[str, Any]],
    ) -> List[str]:
        by_building = {
            str(session.get("building", "")).strip(): session
            for session in sessions
            if isinstance(session, dict)
        }
        gaps: List[str] = []
        for building in HandoverCloudSheetSyncService.MANAGED_BUILDINGS:
            session = by_building.get(building)
            if not isinstance(session, dict):
                gaps.append(f"{building}:missing_session")
                continue
            revision = int(session.get("revision", 0) or 0)
            cloud_state = _normalize_cloud_sync_state(session.get("cloud_sheet_sync", {}))
            if not self._is_cloud_sync_complete_for_revision(cloud_state, revision):
                status = str(cloud_state.get("status", "")).strip().lower() or "pending"
                gaps.append(f"{building}:{status}")
        return gaps

    def _attach_station_h_sync_result_after_final_building_upload(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        cloud_result: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        result = cloud_result if isinstance(cloud_result, dict) else {}
        batch_meta = self._review_service.get_cloud_batch(batch_key) or {}
        existing = batch_meta.get("station_h_sync", {}) if isinstance(batch_meta, dict) else {}
        if isinstance(existing, dict) and str(existing.get("status", "")).strip().lower() == "success":
            result["station_h_sync"] = {**existing, "reason": "already_synced_once"}
            return result

        uploaded_buildings = [
            str(item or "").strip()
            for item in list(result.get("uploaded_buildings", []) or [])
            if str(item or "").strip()
        ]
        if not uploaded_buildings:
            result["station_h_sync"] = {"status": "skipped", "reason": "no_new_building_upload"}
            return result

        batch_sessions = self._review_service.list_batch_sessions(batch_key)
        gaps = self._managed_building_cloud_sync_gaps(batch_sessions)
        if gaps:
            result["station_h_sync"] = {
                "status": "skipped",
                "reason": "waiting_final_building_upload",
                "pending": gaps,
            }
            emit_log(
                f"[交接班][H楼云表] 等待最后楼栋云文档上传完成 batch={batch_key}, "
                f"pending={','.join(gaps)}"
            )
            return result

        return self._attach_station_h_sync_result(
            batch_key=batch_key,
            sessions=batch_sessions or sessions,
            cloud_result=result,
            emit_log=emit_log,
        )

    @staticmethod
    def _previous_abcdeh_work_content_duty(*, duty_date: str, duty_shift: str) -> tuple[str, str]:
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        if duty_shift_text == "night":
            return duty_date_text, "day"
        try:
            previous_day = datetime.strptime(duty_date_text, "%Y-%m-%d") - timedelta(days=1)
            return previous_day.strftime("%Y-%m-%d"), "night"
        except Exception:  # noqa: BLE001
            return "", ""

    @staticmethod
    def _format_abcdeh_work_items(items: List[Any]) -> str:
        normalized = [str(item or "").strip() for item in items if str(item or "").strip()]
        if not normalized:
            return "/"
        return "\n".join(f"{index}、{item}" for index, item in enumerate(normalized, 1))

    def _abcdeh_work_items_for_session(self, session: Dict[str, Any] | None) -> List[str]:
        payload = session if isinstance(session, dict) else {}
        output_file = str(payload.get("output_file", "") or "").strip()
        if not output_file:
            return []
        try:
            return self._summary_message_service.extract_work_items_from_output_file(output_file)
        except Exception:
            return []

    def _build_abcdeh_work_content(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        duty_date, duty_shift = self._review_service.parse_batch_key(batch_key)
        if not duty_date or duty_shift not in {"day", "night"}:
            return {"ok": False, "status": "skipped", "reason": "missing_duty_context", "error": "缺少日期/班次"}
        previous_date, previous_shift = self._previous_abcdeh_work_content_duty(
            duty_date=duty_date,
            duty_shift=duty_shift,
        )
        if not previous_date or previous_shift not in {"day", "night"}:
            return {"ok": False, "status": "skipped", "reason": "missing_previous_duty_context", "error": "缺少上班次日期/班次"}

        current_by_building = {
            str(session.get("building", "")).strip(): session
            for session in sessions
            if isinstance(session, dict)
        }
        work_content: Dict[str, Dict[str, str]] = {}
        missing_current: List[str] = []
        missing_previous: List[str] = []
        for building in HandoverCloudSheetSyncService.MANAGED_BUILDINGS:
            current_session = current_by_building.get(building, {})
            current_items = self._abcdeh_work_items_for_session(current_session)
            if not current_items:
                missing_current.append(building)

            previous_session = self._review_service.get_session_for_building_duty_fast(
                building,
                previous_date,
                previous_shift,
            )
            previous_items = self._abcdeh_work_items_for_session(previous_session)
            if not previous_items:
                missing_previous.append(building)

            work_content[building] = {
                "previous": self._format_abcdeh_work_items(previous_items),
                "current": self._format_abcdeh_work_items(current_items),
            }
        return {
            "ok": True,
            "work_content": work_content,
            "previous_batch_key": self._review_service.build_batch_key(previous_date, previous_shift),
            "missing_current": missing_current,
            "missing_previous": missing_previous,
        }

    def _persist_abcdeh_work_content_sync_result(self, *, batch_key: str, sync_result: Dict[str, Any]) -> None:
        payload = dict(sync_result) if isinstance(sync_result, dict) else {}
        payload["updated_at"] = self._now_text()
        self._review_service.update_cloud_batch_extra_state(
            batch_key=batch_key,
            field="abcdeh_work_content_sync",
            value=payload,
        )

    def _attach_abcdeh_work_content_sync_result_after_final_building_upload(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        cloud_result: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        result = cloud_result if isinstance(cloud_result, dict) else {}
        token = str(result.get("spreadsheet_token", "")).strip()
        if not token:
            result["abcdeh_work_content_sync"] = {"status": "skipped", "reason": "missing_spreadsheet_token"}
            return result
        cloud_status = str(result.get("status", "")).strip().lower()
        if cloud_status == "failed":
            result["abcdeh_work_content_sync"] = {"status": "skipped", "reason": "cloud_upload_failed"}
            return result
        if cloud_status not in {"ok", "skipped"}:
            result["abcdeh_work_content_sync"] = {"status": "skipped", "reason": "cloud_upload_not_complete"}
            return result

        batch_meta = self._review_service.get_cloud_batch(batch_key) or {}
        existing = batch_meta.get("abcdeh_work_content_sync", {}) if isinstance(batch_meta, dict) else {}
        if isinstance(existing, dict) and str(existing.get("status", "")).strip().lower() == "success":
            result["abcdeh_work_content_sync"] = {**existing, "reason": "already_synced_once"}
            return result

        uploaded_buildings = [
            str(item or "").strip()
            for item in list(result.get("uploaded_buildings", []) or [])
            if str(item or "").strip()
        ]
        if not uploaded_buildings:
            result["abcdeh_work_content_sync"] = {"status": "skipped", "reason": "no_new_building_upload"}
            return result

        batch_sessions = self._review_service.list_batch_sessions(batch_key)
        gaps = self._managed_building_cloud_sync_gaps(batch_sessions)
        if gaps:
            result["abcdeh_work_content_sync"] = {
                "status": "skipped",
                "reason": "waiting_final_building_upload",
                "pending": gaps,
            }
            emit_log(
                f"[交接班][ABCDEH工作内容云表] 等待最后楼栋云文档上传完成 batch={batch_key}, "
                f"pending={','.join(gaps)}"
            )
            return result

        content_result = self._build_abcdeh_work_content(
            batch_key=batch_key,
            sessions=batch_sessions or sessions,
        )
        if not bool(content_result.get("ok", False)):
            sync_result = {
                "status": str(content_result.get("status", "failed") or "failed").strip().lower(),
                "reason": str(content_result.get("reason", "") or "").strip(),
                "error": str(content_result.get("error", "") or "").strip(),
            }
            result["abcdeh_work_content_sync"] = sync_result
            if sync_result["status"] != "skipped":
                self._persist_abcdeh_work_content_sync_result(batch_key=batch_key, sync_result=sync_result)
            return result

        try:
            latest_batch_meta = self._review_service.get_cloud_batch(batch_key) or batch_meta
            sync_result = self._cloud_sheet_sync_service.sync_abcdeh_work_content_sheet(
                batch_meta=latest_batch_meta,
                work_content=content_result.get("work_content", {}),
                emit_log=emit_log,
            )
            sync_result["previous_batch_key"] = str(content_result.get("previous_batch_key", "") or "").strip()
            sync_result["missing_current"] = list(content_result.get("missing_current", []) or [])
            sync_result["missing_previous"] = list(content_result.get("missing_previous", []) or [])
        except Exception as exc:  # noqa: BLE001
            sync_result = {"status": "failed", "error": str(exc)}
            emit_log(f"[交接班][ABCDEH工作内容云表] 同步异常 batch={batch_key}, error={exc}")
        result["abcdeh_work_content_sync"] = sync_result
        status = str(sync_result.get("status", "")).strip().lower()
        if status in {"success", "failed"}:
            self._persist_abcdeh_work_content_sync_result(batch_key=batch_key, sync_result=sync_result)
        if status and status != "skipped":
            emit_log(f"[交接班][ABCDEH工作内容云表] 跟随云文档上传完成 batch={batch_key}, status={status}")
        return result

    def _attach_station_110_sync_result(
        self,
        *,
        batch_key: str,
        cloud_result: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        result = cloud_result if isinstance(cloud_result, dict) else {}
        token = str(result.get("spreadsheet_token", "")).strip()
        if not token:
            result["station_110_sync"] = {"status": "skipped", "reason": "missing_spreadsheet_token"}
            return result
        try:
            station_result = self._station_110_upload_service.sync_existing_upload_to_cloud(
                batch_key=batch_key,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            station_result = {"status": "failed", "error": str(exc)}
            emit_log(f"[交接班][110站云表] 同步异常 batch={batch_key}, error={exc}")
        result["station_110_sync"] = station_result
        station_status = str(station_result.get("status", "")).strip().lower()
        if station_status and station_status != "skipped":
            emit_log(f"[交接班][110站云表] 跟随云文档上传完成 batch={batch_key}, status={station_status}")
        return result

    def _attach_extra_cloud_sheet_sync_results(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        cloud_result: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        result = self._attach_station_h_sync_result_after_final_building_upload(
            batch_key=batch_key,
            sessions=sessions,
            cloud_result=cloud_result,
            emit_log=emit_log,
        )
        result = self._attach_abcdeh_work_content_sync_result_after_final_building_upload(
            batch_key=batch_key,
            sessions=sessions,
            cloud_result=result,
            emit_log=emit_log,
        )
        return self._attach_station_110_sync_result(
            batch_key=batch_key,
            cloud_result=result,
            emit_log=emit_log,
        )

    def _run_cloud_sheet_upload(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        with self._station_110_upload_service.batch_lock(batch_key):
            return self._run_cloud_sheet_upload_locked(
                batch_key=batch_key,
                sessions=sessions,
                emit_log=emit_log,
            )

    def _run_cloud_sheet_upload_locked(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        batch_meta = self._resolve_cloud_batch_meta(batch_key=batch_key, sessions=sessions, emit_log=emit_log)
        if str(batch_meta.get("status", "")).strip().lower() == "disabled":
            emit_log("[交接班][云表最终上传] 已跳过: 当前配置禁用云文档同步")
            return {
                "status": "skipped",
                "spreadsheet_token": "",
                "spreadsheet_url": "",
                "spreadsheet_title": "",
                "uploaded_buildings": [],
                "skipped_buildings": [{"building": str(item.get("building", "")).strip(), "reason": "disabled"} for item in sessions],
                "failed_buildings": [],
                "details": {},
            }

        batch_status = str(batch_meta.get("status", "")).strip().lower()
        batch_token = str(batch_meta.get("spreadsheet_token", "")).strip()
        if batch_status not in {"prepared", "success"} or not batch_token:
            upload_items, skipped_buildings, failed_buildings = self._build_cloud_items(
                sessions,
                emit_log=emit_log,
                force_excel_sync=False,
            )
            batch_error = str(batch_meta.get("error", "")).strip() or "cloud_batch_unavailable"
            emit_log(
                "[交接班][云表最终上传] 无法执行: "
                f"batch={batch_key}, 批次状态={_followup_status_text(batch_status)}, 原因={_followup_reason_text(batch_error)}"
            )
            failed_buildings = list(failed_buildings) + [
                {"building": str(item.get("building", "")).strip(), "error": batch_error}
                for item in upload_items
            ]
            result = {
                "status": "failed" if failed_buildings else "skipped",
                "spreadsheet_token": batch_token,
                "spreadsheet_url": str(batch_meta.get("spreadsheet_url", "")).strip(),
                "spreadsheet_title": str(batch_meta.get("spreadsheet_title", "")).strip(),
                "uploaded_buildings": [],
                "skipped_buildings": skipped_buildings,
                "failed_buildings": failed_buildings,
                "details": {},
            }
            self._persist_cloud_sync_result(
                sessions=sessions,
                batch_meta=batch_meta,
                cloud_result=result,
                emit_log=emit_log,
            )
            return self._attach_extra_cloud_sheet_sync_results(
                batch_key=batch_key,
                sessions=sessions,
                cloud_result=result,
                emit_log=emit_log,
            )

        upload_items, skipped_buildings, failed_buildings = self._build_cloud_items(
            sessions,
            emit_log=emit_log,
            force_excel_sync=True,
        )
        if not upload_items:
            status = "failed" if failed_buildings else "skipped"
            emit_log(
                f"[交接班][云表最终上传] 已跳过: batch={batch_key}, "
                f"状态={_followup_status_text(status)}, 已跳过={len(skipped_buildings)}, 已失败={len(failed_buildings)}"
            )
            result = {
                "status": status,
                "spreadsheet_token": str(batch_meta.get("spreadsheet_token", "")).strip(),
                "spreadsheet_url": str(batch_meta.get("spreadsheet_url", "")).strip(),
                "spreadsheet_title": str(batch_meta.get("spreadsheet_title", "")).strip(),
                "uploaded_buildings": [],
                "skipped_buildings": skipped_buildings,
                "failed_buildings": failed_buildings,
                "details": {},
            }
            return self._attach_extra_cloud_sheet_sync_results(
                batch_key=batch_key,
                sessions=sessions,
                cloud_result=result,
                emit_log=emit_log,
            )

        self._mark_cloud_sheet_uploading(
            sessions=sessions,
            batch_meta=batch_meta,
            upload_items=upload_items,
            emit_log=emit_log,
        )
        try:
            result = self._cloud_sheet_sync_service.sync_confirmed_buildings(
                batch_meta=batch_meta,
                building_items=upload_items,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][云表最终上传] 批量异常 batch={batch_key}, error={exc}")
            result = {
                "status": "failed",
                "spreadsheet_token": str(batch_meta.get("spreadsheet_token", "")).strip(),
                "spreadsheet_url": str(batch_meta.get("spreadsheet_url", "")).strip(),
                "spreadsheet_title": str(batch_meta.get("spreadsheet_title", "")).strip(),
                "uploaded_buildings": [],
                "skipped_buildings": [],
                "failed_buildings": [
                    {"building": str(item.get("building", "")).strip(), "error": str(exc)}
                    for item in upload_items
                    if isinstance(item, dict) and str(item.get("building", "")).strip()
                ],
                "details": {},
            }
        result["skipped_buildings"] = list(skipped_buildings) + list(result.get("skipped_buildings", []) or [])
        result["failed_buildings"] = list(failed_buildings) + list(result.get("failed_buildings", []) or [])
        uploaded = list(result.get("uploaded_buildings", []) or [])
        has_failed = bool(result["failed_buildings"])
        if has_failed and uploaded:
            result["status"] = "partial_failed"
        elif has_failed:
            result["status"] = "failed"
        elif uploaded:
            result["status"] = "ok"
        else:
            result["status"] = "skipped"
        self._persist_cloud_sync_result(
            sessions=sessions,
            batch_meta=batch_meta,
            cloud_result=result,
            emit_log=emit_log,
        )
        return self._attach_extra_cloud_sheet_sync_results(
            batch_key=batch_key,
            sessions=sessions,
            cloud_result=result,
            emit_log=emit_log,
        )

    def _daily_report_export_state(
        self,
        *,
        status: str,
        spreadsheet_url: str = "",
        record_id: str = "",
        record_url: str = "",
        error: str = "",
        error_code: str = "",
        error_detail: str = "",
    ) -> Dict[str, Any]:
        return _normalize_daily_report_export_state(
            {
                "status": str(status or "").strip().lower(),
                "record_id": str(record_id or "").strip(),
                "record_url": str(record_url or "").strip(),
                "spreadsheet_url": str(spreadsheet_url or "").strip(),
                "updated_at": self._now_text(),
                "error": str(error or "").strip(),
                "error_code": str(error_code or "").strip(),
                "error_detail": str(error_detail or "").strip(),
            }
        )

    def _build_daily_report_export_record(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        spreadsheet_url: str,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        try:
            export_result = self._daily_report_bitable_export_service.export_record(
                duty_date=duty_date,
                duty_shift=duty_shift,
                spreadsheet_url=spreadsheet_url,
                emit_log=emit_log,
            )
            return self._daily_report_export_state(
                status=str(export_result.get("status", "")).strip().lower() or "success",
                spreadsheet_url=spreadsheet_url,
                record_id=str(export_result.get("record_id", "")).strip(),
                record_url=str(export_result.get("record_url", "")).strip(),
                error=str(export_result.get("error", "")).strip(),
                error_code=str(export_result.get("error_code", "")).strip(),
                error_detail=str(export_result.get("error_detail", "")).strip(),
            )
        except Exception as exc:  # noqa: BLE001
            error_code = str(getattr(exc, "error_code", "") or "daily_report_export_failed").strip()
            error = str(getattr(exc, "user_message", "") or "").strip() or "日报多维写入失败。"
            error_detail = str(getattr(exc, "error_detail", "") or str(exc)).strip()
            return self._daily_report_export_state(
                status="failed",
                spreadsheet_url=spreadsheet_url,
                error=error,
                error_code=error_code,
                error_detail=error_detail,
            )

    def _run_daily_report_record_export(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        cloud_result: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        target_batch = str(batch_key or "").strip()
        if not sessions:
            return self._daily_report_export_state(status="skipped", error="missing_sessions")

        first_session = sessions[0] if isinstance(sessions[0], dict) else {}
        duty_date = str(first_session.get("duty_date", "")).strip()
        duty_shift = str(first_session.get("duty_shift", "")).strip().lower()
        cloud_status = str(cloud_result.get("status", "")).strip().lower()
        spreadsheet_url = str(cloud_result.get("spreadsheet_url", "")).strip()
        if not spreadsheet_url:
            batch_meta = self._review_service.get_cloud_batch(target_batch)
            if isinstance(batch_meta, dict):
                spreadsheet_url = str(batch_meta.get("spreadsheet_url", "")).strip()

        emit_log(f"[交接班][日报多维] 开始 batch={target_batch}")
        if cloud_status != "ok":
            emit_log(f"[交接班][日报多维] 跳过: 云表同步状态={_followup_status_text(cloud_status)}")
            state = self._daily_report_export_state(
                status="skipped_due_to_cloud_sync_not_ok",
                spreadsheet_url=spreadsheet_url,
                error=f"云表同步状态不是成功: {_followup_status_text(cloud_status)}",
            )
            return self._daily_report_state_service.update_export_state(
                duty_date=duty_date,
                duty_shift=duty_shift,
                daily_report_record_export=state,
            )

        if not spreadsheet_url:
            emit_log("[交接班][日报多维] 跳过: 缺少云表链接")
            state = self._daily_report_export_state(status="failed", error="缺少云表链接")
            return self._daily_report_state_service.update_export_state(
                duty_date=duty_date,
                duty_shift=duty_shift,
                daily_report_record_export=state,
            )

        state = self._build_daily_report_export_record(
            duty_date=duty_date,
            duty_shift=duty_shift,
            spreadsheet_url=spreadsheet_url,
            emit_log=emit_log,
        )
        return self._daily_report_state_service.update_export_state(
            duty_date=duty_date,
            duty_shift=duty_shift,
            daily_report_record_export=state,
        )

    def rewrite_daily_report_record(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        batch_key = self._daily_report_state_service.build_batch_key(duty_date_text, duty_shift_text)
        if not batch_key:
            return self._daily_report_export_state(
                status="failed",
                error=_followup_reason_text("invalid_duty_context"),
            )
        batch_meta = self._review_service.get_cloud_batch(batch_key)
        export_state = self._daily_report_state_service.get_export_state(
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
        )
        spreadsheet_url = str(export_state.get("spreadsheet_url", "")).strip()
        if not spreadsheet_url and isinstance(batch_meta, dict):
            spreadsheet_url = str(batch_meta.get("spreadsheet_url", "")).strip()
        if not spreadsheet_url:
            state = self._daily_report_export_state(
                status="failed",
                error=_followup_reason_text("missing_spreadsheet_url"),
            )
            return self._daily_report_state_service.update_export_state(
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                daily_report_record_export=state,
            )
        state = self._build_daily_report_export_record(
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
            spreadsheet_url=spreadsheet_url,
            emit_log=emit_log,
        )
        return self._daily_report_state_service.update_export_state(
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
            daily_report_record_export=state,
        )

    def retry_cloud_sheet_for_building(
        self,
        building: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        latest = self._review_service.get_latest_session(building)
        if not latest:
            return {
                "status": "failed",
                "batch_key": "",
                "session": None,
                "batch_status": self._review_service.get_batch_status(""),
                "cloud_sheet_sync": {
                    "status": "failed",
                    "uploaded_buildings": [],
                    "failed_buildings": [{"building": str(building or "").strip(), "error": "review session not found"}],
                    "skipped_buildings": [],
                    "details": {},
                },
            }

        batch_key = str(latest.get("batch_key", "")).strip()
        batch_status = self._review_service.get_batch_status(batch_key)
        if not bool(latest.get("confirmed", False)):
            return {
                "status": "blocked",
                "batch_key": batch_key,
                "session": latest,
                "batch_status": batch_status,
                "cloud_sheet_sync": {
                    "status": "blocked",
                    "uploaded_buildings": [],
                    "failed_buildings": [{"building": str(building or "").strip(), "error": "pending_review"}],
                    "skipped_buildings": [],
                    "details": {},
                    "blocked_reason": "pending_review",
                },
            }

        self._ensure_external_network(emit_log)
        cloud_result = self._run_cloud_sheet_upload(batch_key=batch_key, sessions=[latest], emit_log=emit_log)
        self._maybe_mark_first_full_cloud_sync_completed(
            batch_key=batch_key,
            sessions=self._review_service.list_batch_sessions(batch_key),
            emit_log=emit_log,
        )
        return {
            "status": str(cloud_result.get("status", "")).strip() or "failed",
            "batch_key": batch_key,
            "session": self._review_service.get_latest_session(building),
            "batch_status": self._review_service.get_batch_status(batch_key),
            "cloud_sheet_sync": cloud_result,
        }

    def retry_cloud_sheet_for_session(
        self,
        session_id: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        target_session_id = str(session_id or "").strip()
        session = self._review_service.get_session_by_id(target_session_id)
        if not session:
            return {
                "status": "failed",
                "batch_key": "",
                "session": None,
                "batch_status": self._review_service.get_batch_status(""),
                "cloud_sheet_sync": {
                    "status": "failed",
                    "uploaded_buildings": [],
                    "failed_buildings": [{"building": "", "error": "review session not found"}],
                    "skipped_buildings": [],
                    "details": {},
                },
            }

        batch_key = str(session.get("batch_key", "")).strip()
        batch_status = self._review_service.get_batch_status(batch_key)
        if not bool(session.get("confirmed", False)):
            return {
                "status": "blocked",
                "batch_key": batch_key,
                "session": session,
                "batch_status": batch_status,
                "cloud_sheet_sync": {
                    "status": "blocked",
                    "uploaded_buildings": [],
                    "failed_buildings": [{"building": str(session.get("building", "")).strip(), "error": "pending_review"}],
                    "skipped_buildings": [],
                    "details": {},
                    "blocked_reason": "pending_review",
                },
            }

        self._ensure_external_network(emit_log)
        cloud_result = self._run_cloud_sheet_upload(batch_key=batch_key, sessions=[session], emit_log=emit_log)
        self._maybe_mark_first_full_cloud_sync_completed(
            batch_key=batch_key,
            sessions=self._review_service.list_batch_sessions(batch_key),
            emit_log=emit_log,
        )
        return {
            "status": str(cloud_result.get("status", "")).strip() or "failed",
            "batch_key": batch_key,
            "session": self._review_service.get_session_by_id(target_session_id),
            "batch_status": self._review_service.get_batch_status(batch_key),
            "cloud_sheet_sync": cloud_result,
        }

    def force_update_cloud_sheet_for_session(
        self,
        session_id: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        target_session_id = str(session_id or "").strip()
        session = self._review_service.get_session_by_id(target_session_id)
        if not session:
            return {
                "status": "failed",
                "batch_key": "",
                "session": None,
                "batch_status": self._review_service.get_batch_status(""),
                "cloud_sheet_sync": {
                    "status": "failed",
                    "uploaded_buildings": [],
                    "failed_buildings": [{"building": "", "error": "review session not found"}],
                    "skipped_buildings": [],
                    "details": {},
                },
            }

        building = str(session.get("building", "")).strip()
        emit_log(f"[交接班][历史云表更新] 开始 building={building}, session={target_session_id}")
        cloud_result = self._run_cloud_sheet_upload(
            batch_key=str(session.get("batch_key", "")).strip(),
            sessions=[session],
            emit_log=emit_log,
        )
        self._maybe_mark_first_full_cloud_sync_completed(
            batch_key=str(session.get("batch_key", "")).strip(),
            sessions=self._review_service.list_batch_sessions(str(session.get("batch_key", "")).strip()),
            emit_log=emit_log,
        )
        status = str(cloud_result.get("status", "")).strip() or "failed"
        emit_log(f"[交接班][历史云表更新] 完成 building={building}, 状态={_followup_status_text(status)}")
        return {
            "status": status,
            "batch_key": str(session.get("batch_key", "")).strip(),
            "session": self._review_service.get_session_by_id(target_session_id),
            "batch_status": self._review_service.get_batch_status(str(session.get("batch_key", "")).strip()),
            "cloud_sheet_sync": cloud_result,
        }

    def retry_failed_cloud_sheet_in_batch(
        self,
        batch_key: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        target_batch_key = str(batch_key or "").strip()
        batch_status = self._review_service.get_batch_status(target_batch_key)
        sessions = self._review_service.list_batch_sessions(target_batch_key)
        retry_sessions = []
        skipped_buildings: List[Dict[str, str]] = []
        for session in sessions:
            cloud_state = _normalize_cloud_sync_state(session.get("cloud_sheet_sync", {}))
            building = str(session.get("building", "")).strip()
            status = str(cloud_state.get("status", "")).strip().lower()
            if status in {"failed", "prepare_failed"} and bool(session.get("confirmed", False)):
                retry_sessions.append(session)
            elif status in {"failed", "prepare_failed"}:
                skipped_buildings.append({"building": building, "reason": "pending_review"})
            else:
                skipped_buildings.append({"building": building, "reason": "already_success"})

        if not retry_sessions:
            return {
                "status": "skipped",
                "batch_key": target_batch_key,
                "batch_status": batch_status,
                "updated_sessions": sessions,
                "cloud_sheet_sync": {
                    "status": "skipped",
                    "spreadsheet_token": "",
                    "spreadsheet_url": "",
                    "spreadsheet_title": "",
                    "uploaded_buildings": [],
                    "skipped_buildings": skipped_buildings,
                    "failed_buildings": [],
                    "details": {},
                    "blocked_reason": "",
                },
            }

        self._ensure_external_network(emit_log)
        cloud_result = self._run_cloud_sheet_upload(
            batch_key=target_batch_key,
            sessions=retry_sessions,
            emit_log=emit_log,
        )
        cloud_result["skipped_buildings"] = skipped_buildings + list(cloud_result.get("skipped_buildings", []) or [])
        refreshed_sessions = self._review_service.list_batch_sessions(target_batch_key)
        self._maybe_mark_first_full_cloud_sync_completed(
            batch_key=target_batch_key,
            sessions=refreshed_sessions,
            emit_log=emit_log,
        )
        return {
            "status": str(cloud_result.get("status", "")).strip() or "failed",
            "batch_key": target_batch_key,
            "batch_status": self._review_service.get_batch_status(target_batch_key),
            "updated_sessions": refreshed_sessions,
            "cloud_sheet_sync": cloud_result,
        }

    def _run_session_followup_exports(
        self,
        *,
        batch_key: str,
        sessions: List[Dict[str, Any]],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        uploaded_buildings: List[str] = []
        skipped_buildings: List[Dict[str, str]] = []
        failed_buildings: List[Dict[str, str]] = []
        details: Dict[str, Dict[str, Any]] = {}

        attachment_existing_records: List[Dict[str, Any]] | None = None
        attachment_existing_records_error = ""
        attachment_globally_enabled = (
            str(
                self._source_data_attachment_export_service.build_deferred_state(duty_shift="day").get("reason", "")
            ).strip().lower()
            != "disabled"
        )
        needs_attachment_prefetch = False
        if attachment_globally_enabled:
            for session in sessions:
                attachment_state = _normalize_export_state(session.get("source_data_attachment_export", {}))
                revision = int(session.get("revision", 0) or 0)
                if attachment_state["uploaded_revision"] == revision and attachment_state["status"] in {"ok", "skipped"}:
                    continue
                if attachment_state["status"] == "skipped" and attachment_state["reason"].lower() in self.STATIC_SKIP_REASONS:
                    continue
                needs_attachment_prefetch = True
                break
        if needs_attachment_prefetch:
            try:
                attachment_existing_records = self._source_data_attachment_export_service.list_existing_records(
                    emit_log=emit_log
                )
            except Exception as exc:  # noqa: BLE001
                attachment_existing_records_error = str(exc)
                emit_log(f"[交接班][源数据附件] 旧记录读取失败: {exc}")

        for session in sessions:
            building = str(session.get("building", "")).strip() or "-"
            session_id = str(session.get("session_id", "")).strip()
            data_file = self._resolve_session_source_data_file(session)
            duty_date = str(session.get("duty_date", "")).strip()
            duty_shift = str(session.get("duty_shift", "")).strip().lower()
            revision = int(session.get("revision", 0) or 0)

            attachment_state = _normalize_export_state(session.get("source_data_attachment_export", {}))
            next_attachment_state = {
                "status": attachment_state["status"] or "failed",
                "reason": attachment_state["reason"],
                "uploaded_count": attachment_state["uploaded_count"],
                "error": attachment_state["error"],
                "uploaded_at": attachment_state["uploaded_at"],
                "uploaded_revision": attachment_state["uploaded_revision"],
            }

            if attachment_state["uploaded_revision"] == revision and attachment_state["status"] in {"ok", "skipped"}:
                attachment_result = {
                    "status": "skipped",
                    "reason": "already_uploaded",
                    "uploaded_count": attachment_state["uploaded_count"],
                    "error": "",
                }
            elif (
                attachment_state["status"] == "skipped"
                and attachment_state["reason"].lower() in self.STATIC_SKIP_REASONS
            ):
                next_attachment_state["uploaded_revision"] = revision
                next_attachment_state["uploaded_at"] = next_attachment_state["uploaded_at"] or self._now_text()
                self._review_service.update_source_data_attachment_export(
                    session_id=session_id,
                    source_data_attachment_export=next_attachment_state,
                )
                attachment_result = {
                    "status": "skipped",
                    "reason": attachment_state["reason"],
                    "uploaded_count": attachment_state["uploaded_count"],
                    "error": "",
                }
            else:
                if attachment_existing_records_error:
                    result = {
                        "status": "failed",
                        "reason": "list_existing_failed",
                        "uploaded_count": 0,
                        "error": attachment_existing_records_error,
                    }
                else:
                    result = self._source_data_attachment_export_service.run_from_source_file(
                        building=building,
                        duty_date=duty_date,
                        duty_shift=duty_shift,
                        data_file=data_file,
                        existing_records=attachment_existing_records,
                        emit_log=emit_log,
                    )
                result_status = str(result.get("status", "")).strip().lower() or "failed"
                next_attachment_state.update(
                    {
                        "status": result_status,
                        "reason": str(result.get("reason", "")).strip(),
                        "uploaded_count": int(result.get("uploaded_count", 0) or 0),
                        "error": str(result.get("error", "")).strip(),
                    }
                )
                if result_status in {"ok", "skipped"}:
                    next_attachment_state["uploaded_at"] = result.get("uploaded_at") or self._now_text()
                    next_attachment_state["uploaded_revision"] = revision
                else:
                    next_attachment_state["uploaded_at"] = ""
                    next_attachment_state["uploaded_revision"] = 0
                self._review_service.update_source_data_attachment_export(
                    session_id=session_id,
                    source_data_attachment_export=next_attachment_state,
                )
                attachment_result = {
                    "status": result_status,
                    "reason": next_attachment_state["reason"],
                    "uploaded_count": next_attachment_state["uploaded_count"],
                    "error": next_attachment_state["error"],
                }

            details[building] = {
                "source_data_attachment_export": attachment_result,
            }
            building_failed = attachment_result.get("status", "") == "failed"
            building_uploaded = attachment_result.get("status", "") == "ok"
            if building_failed:
                failed_buildings.append(
                    {
                        "building": building,
                        "error": attachment_result.get("error", "") or "未知错误",
                    }
                )
            elif building_uploaded:
                uploaded_buildings.append(building)
            else:
                reason = attachment_result.get("reason", "") or "skipped"
                skipped_buildings.append({"building": building, "reason": str(reason)})

        return {
            "uploaded_buildings": uploaded_buildings,
            "skipped_buildings": skipped_buildings,
            "failed_buildings": failed_buildings,
            "details": details,
        }

    @staticmethod
    def _resolve_session_source_data_file(session: Dict[str, Any]) -> str:
        data_file = str(session.get("data_file", "")).strip()
        if data_file:
            return data_file
        source_file_cache = session.get("source_file_cache", {})
        if not isinstance(source_file_cache, dict):
            return ""
        return str(source_file_cache.get("stored_path", "")).strip()

    def trigger_batch(self, batch_key: str, emit_log: Callable[[str], None] = print) -> Dict[str, Any]:
        batch_status = self._review_service.get_batch_status(batch_key)
        gate = self.evaluate(batch_status)
        if not gate.get("ready_for_followup_upload", False):
            blocked_sessions = self._review_service.list_batch_sessions(batch_key)
            return {
                "status": "blocked",
                "batch_key": str(batch_key or "").strip(),
                "uploaded_buildings": [],
                "skipped_buildings": [],
                "failed_buildings": [],
                "details": {},
                "blocked_reason": gate.get("blocked_reason", ""),
                "cloud_sheet_sync": {
                    "status": "blocked",
                    "uploaded_buildings": [],
                    "skipped_buildings": [],
                    "failed_buildings": [],
                    "details": {},
                    "blocked_reason": gate.get("blocked_reason", ""),
                },
                "daily_report_record_export": self._daily_report_export_state(
                    status="skipped",
                    error=gate.get("blocked_reason", ""),
                ),
                "cabinet_shift_record_export": self._existing_cabinet_shift_record_export(blocked_sessions),
                "followup_progress": self._collect_followup_progress(
                    batch_key=str(batch_key or "").strip(),
                    sessions=blocked_sessions,
                    ready=False,
                ),
            }

        sessions = self._review_service.list_batch_sessions(batch_key)
        confirmed_sessions = [
            session
            for session in sessions
            if isinstance(session, dict) and bool(session.get("confirmed", False))
        ]
        if not confirmed_sessions:
            blocked_reason = "暂无已确认楼栋"
            return {
                "status": "blocked",
                "batch_key": str(batch_key or "").strip(),
                "uploaded_buildings": [],
                "skipped_buildings": [],
                "failed_buildings": [],
                "details": {},
                "blocked_reason": blocked_reason,
                "cloud_sheet_sync": {
                    "status": "blocked",
                    "uploaded_buildings": [],
                    "skipped_buildings": [],
                    "failed_buildings": [],
                    "details": {},
                    "blocked_reason": blocked_reason,
                },
                "daily_report_record_export": self._daily_report_export_state(
                    status="skipped",
                    error=blocked_reason,
                ),
                "cabinet_shift_record_export": self._existing_cabinet_shift_record_export(sessions),
                "followup_progress": self._collect_followup_progress(
                    batch_key=str(batch_key or "").strip(),
                    sessions=sessions,
                    ready=False,
                ),
            }
        export_result = self._run_session_followup_exports(
            batch_key=batch_key,
            sessions=confirmed_sessions,
            emit_log=emit_log,
        )
        refreshed_sessions = self._review_service.list_batch_sessions(batch_key)
        refreshed_confirmed_sessions = [
            session
            for session in refreshed_sessions
            if isinstance(session, dict) and bool(session.get("confirmed", False))
        ] or confirmed_sessions
        emit_log("[交接班][确认后上传] 已跳过自动切回外网，按当前网络继续执行")
        cloud_result = self._run_cloud_sheet_upload(
            batch_key=batch_key,
            sessions=refreshed_confirmed_sessions,
            emit_log=emit_log,
        )
        emit_log(
            "[交接班][确认后上传][云表] "
            f"batch={batch_key}, 状态={_followup_status_text(cloud_result.get('status'))}, "
            f"已上传={len(cloud_result.get('uploaded_buildings', []) or [])}, "
            f"已跳过={len(cloud_result.get('skipped_buildings', []) or [])}, "
            f"已失败={len(cloud_result.get('failed_buildings', []) or [])}"
        )
        refreshed_sessions = self._review_service.list_batch_sessions(batch_key)
        self._maybe_mark_first_full_cloud_sync_completed(
            batch_key=batch_key,
            sessions=refreshed_sessions or sessions,
            emit_log=emit_log,
        )
        refreshed_confirmed_sessions = [
            session
            for session in refreshed_sessions
            if isinstance(session, dict) and bool(session.get("confirmed", False))
        ] or refreshed_confirmed_sessions
        cabinet_shift_record_export = self._run_cabinet_shift_record_export(
            batch_key=batch_key,
            sessions=refreshed_confirmed_sessions,
            cloud_result=cloud_result,
            emit_log=emit_log,
        )
        refreshed_sessions = self._review_service.list_batch_sessions(batch_key)
        daily_report_record_export = self._existing_daily_report_record_export(refreshed_sessions or sessions)
        if self._all_sessions_cloud_synced_current_revision(refreshed_sessions or sessions):
            cloud_summary = self._summarize_cloud_sheet_sync(
                batch_key=batch_key,
                sessions=refreshed_sessions or sessions,
            )
            daily_report_record_export = self._run_daily_report_record_export(
                batch_key=batch_key,
                sessions=refreshed_sessions or sessions,
                cloud_result=cloud_summary,
                emit_log=emit_log,
            )
        refreshed_sessions = self._review_service.list_batch_sessions(batch_key)
        return self._compose_followup_result(
            batch_key=batch_key,
            export_result=export_result,
            cloud_result=cloud_result,
            daily_report_record_export=daily_report_record_export,
            cabinet_shift_record_export=cabinet_shift_record_export,
            sessions=refreshed_sessions or sessions,
        )

    def continue_batch(self, batch_key: str, emit_log: Callable[[str], None] = print) -> Dict[str, Any]:
        target_batch = str(batch_key or "").strip()
        batch_status = self._review_service.get_batch_status(target_batch)
        gate = self.evaluate(batch_status)
        if not gate.get("ready_for_followup_upload", False):
            sessions = self._review_service.list_batch_sessions(target_batch)
            return {
                "status": "blocked",
                "batch_key": target_batch,
                "uploaded_buildings": [],
                "skipped_buildings": [],
                "failed_buildings": [],
                "details": {},
                "blocked_reason": gate.get("blocked_reason", ""),
                "cloud_sheet_sync": {
                    "status": "blocked",
                    "uploaded_buildings": [],
                    "skipped_buildings": [],
                    "failed_buildings": [],
                    "details": {},
                    "blocked_reason": gate.get("blocked_reason", ""),
                },
                "daily_report_record_export": self._daily_report_export_state(
                    status="skipped",
                    error=gate.get("blocked_reason", ""),
                ),
                "cabinet_shift_record_export": self._existing_cabinet_shift_record_export(sessions),
                "followup_progress": self._collect_followup_progress(
                    batch_key=target_batch,
                    sessions=sessions,
                    ready=False,
                ),
            }

        emit_log(f"[交接班][继续后续上传] 开始 batch={target_batch}")
        sessions = self._review_service.list_batch_sessions(target_batch)
        confirmed_sessions = [
            session
            for session in sessions
            if isinstance(session, dict) and bool(session.get("confirmed", False))
        ]
        if not confirmed_sessions:
            blocked_reason = "暂无已确认楼栋"
            return {
                "status": "blocked",
                "batch_key": target_batch,
                "uploaded_buildings": [],
                "skipped_buildings": [],
                "failed_buildings": [],
                "details": {},
                "blocked_reason": blocked_reason,
                "cloud_sheet_sync": {
                    "status": "blocked",
                    "uploaded_buildings": [],
                    "skipped_buildings": [],
                    "failed_buildings": [],
                    "details": {},
                    "blocked_reason": blocked_reason,
                },
                "daily_report_record_export": self._existing_daily_report_record_export(sessions),
                "cabinet_shift_record_export": self._existing_cabinet_shift_record_export(sessions),
                "followup_progress": self._collect_followup_progress(
                    batch_key=target_batch,
                    sessions=sessions,
                    ready=False,
                ),
            }
        export_result = self._run_session_followup_exports(
            batch_key=target_batch,
            sessions=confirmed_sessions,
            emit_log=emit_log,
        )
        refreshed_sessions = self._review_service.list_batch_sessions(target_batch)

        pending_cloud_sessions = [
            session
            for session in refreshed_sessions
            if isinstance(session, dict)
            if bool(session.get("confirmed", False))
            if not self._is_cloud_sync_complete_for_revision(
                session.get("cloud_sheet_sync", {}),
                int(session.get("revision", 0) or 0),
            )
        ]
        if pending_cloud_sessions:
            emit_log(
                f"[交接班][继续后续上传][云表] 开始 batch={target_batch}, 待处理楼栋={len(pending_cloud_sessions)}"
            )
            self._run_cloud_sheet_upload(
                batch_key=target_batch,
                sessions=pending_cloud_sessions,
                emit_log=emit_log,
            )
            refreshed_sessions = self._review_service.list_batch_sessions(target_batch)
        cloud_result = self._summarize_cloud_sheet_sync(
            batch_key=target_batch,
            sessions=refreshed_sessions,
        )
        self._maybe_mark_first_full_cloud_sync_completed(
            batch_key=target_batch,
            sessions=refreshed_sessions,
            emit_log=emit_log,
        )
        refreshed_confirmed_sessions = [
            session
            for session in refreshed_sessions
            if isinstance(session, dict) and bool(session.get("confirmed", False))
        ]
        cabinet_shift_record_export = self._run_cabinet_shift_record_export(
            batch_key=target_batch,
            sessions=refreshed_confirmed_sessions,
            cloud_result=cloud_result,
            emit_log=emit_log,
        )
        refreshed_sessions = self._review_service.list_batch_sessions(target_batch)

        daily_report_record_export = {}
        if refreshed_sessions:
            first_session = refreshed_sessions[0]
            duty_date = str(first_session.get("duty_date", "")).strip()
            duty_shift = str(first_session.get("duty_shift", "")).strip().lower()
            existing_daily_report = self._daily_report_state_service.get_export_state(
                duty_date=duty_date,
                duty_shift=duty_shift,
            )
            existing_daily_report = _normalize_daily_report_export_state(existing_daily_report)
            if str(existing_daily_report.get("status", "")).strip().lower() == "success":
                daily_report_record_export = existing_daily_report
            elif str(cloud_result.get("status", "")).strip().lower() == "ok":
                daily_report_record_export = self._run_daily_report_record_export(
                    batch_key=target_batch,
                    sessions=refreshed_sessions,
                    cloud_result=cloud_result,
                    emit_log=emit_log,
                )
            else:
                daily_report_record_export = existing_daily_report
        if not isinstance(daily_report_record_export, dict):
            daily_report_record_export = self._daily_report_export_state(status="idle")

        return self._compose_followup_result(
            batch_key=target_batch,
            export_result=export_result,
            cloud_result=cloud_result,
            daily_report_record_export=daily_report_record_export,
            cabinet_shift_record_export=cabinet_shift_record_export,
            sessions=refreshed_sessions,
        )

