from __future__ import annotations

import asyncio
import copy
import inspect
import time
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, Body, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse

from app.config.handover_segment_store import building_code_from_name, handover_building_segment_path
from app.config.settings_loader import (
    get_handover_building_segment,
    save_handover_building_segment,
    save_settings,
)
from app.shared.utils.frontend_cache import render_frontend_index_html, source_frontend_no_cache_headers
from handover_log_module.api.facade import load_handover_config
from handover_log_module.service.cabinet_power_defaults_service import CabinetPowerDefaultsService
from handover_log_module.service.footer_inventory_defaults_service import FooterInventoryDefaultsService
from handover_log_module.service.handover_daily_report_asset_service import HandoverDailyReportAssetService
from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService
from handover_log_module.service.handover_daily_report_screenshot_service import (
    HandoverDailyReportScreenshotService,
)
from handover_log_module.service.handover_daily_report_state_service import HandoverDailyReportStateService
from handover_log_module.service.review_document_parser import ReviewDocumentParser
from handover_log_module.service.review_document_state_service import (
    ReviewDocumentStateConflictError,
    ReviewDocumentStateError,
    ReviewDocumentStateService,
)
from handover_log_module.service.review_document_writer import ReviewDocumentWriter
from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService
from handover_log_module.service.review_session_service import (
    ReviewSessionConflictError,
    ReviewSessionNotFoundError,
    ReviewSessionService,
    ReviewSessionStoreUnavailableError,
)


router = APIRouter(tags=["handover_review"])


def _raise_review_store_http_error(
    exc: ReviewSessionStoreUnavailableError,
    *,
    saved_document: bool = False,
) -> None:
    detail = str(exc or "").strip() or "审核状态存储暂时不可用，请稍后重试"
    if saved_document:
        detail = f"当前文件已保存，但{detail}"
    raise HTTPException(status_code=503, detail=detail) from exc


def _empty_concurrency(current_revision: int = 0) -> Dict[str, Any]:
    return {
        "current_revision": int(current_revision or 0),
        "active_editor": None,
        "lease_expires_at": "",
        "is_editing_elsewhere": False,
        "client_holds_lock": False,
    }


def _get_session_concurrency_safe(
    service: ReviewSessionService,
    *,
    building: str,
    session_id: str,
    client_id: str = "",
    current_revision: int = 0,
    emit_log=None,
) -> Dict[str, Any]:
    getter = getattr(service, "get_session_concurrency", None)
    if not callable(getter):
        return _empty_concurrency(current_revision=current_revision)
    try:
        return getter(
            building=building,
            session_id=session_id,
            client_id=client_id,
        )
    except ReviewSessionStoreUnavailableError as exc:
        if callable(emit_log):
            emit_log(
                f"[交接班][审核并发] 已降级: building={building}, session_id={session_id}, 错误={exc}"
            )
        return _empty_concurrency(current_revision=current_revision)


def _handover_cfg(container) -> Dict[str, Any]:
    runtime_config = getattr(container, "runtime_config", None)
    if isinstance(runtime_config, dict):
        return load_handover_config(runtime_config)
    config_path = Path(str(getattr(container, "config_path", "") or "")).resolve()
    runtime_root = config_path.parent / ".runtime" if str(config_path) else Path(".runtime").resolve()
    return {"_global_paths": {"runtime_state_root": str(runtime_root)}}


def _build_review_services(container) -> tuple[ReviewSessionService, ReviewDocumentParser, ReviewDocumentWriter, ReviewFollowupTriggerService]:
    handover_cfg = _handover_cfg(container)
    return (
        ReviewSessionService(handover_cfg),
        ReviewDocumentParser(handover_cfg),
        ReviewDocumentWriter(handover_cfg),
        ReviewFollowupTriggerService(handover_cfg),
    )


def _build_review_document_state_service(
    container,
    *,
    parser: ReviewDocumentParser | None = None,
    writer: ReviewDocumentWriter | None = None,
) -> ReviewDocumentStateService:
    return ReviewDocumentStateService(
        _handover_cfg(container),
        parser=parser,
        writer=writer,
        emit_log=getattr(container, "add_system_log", print),
    )


def _empty_followup_progress() -> Dict[str, Any]:
    return {
        "status": "idle",
        "can_resume_followup": False,
        "pending_count": 0,
        "failed_count": 0,
        "attachment_pending_count": 0,
        "cloud_pending_count": 0,
        "daily_report_status": "idle",
    }


def _handover_resource_keys(*resource_keys: str, batch_key: str = "") -> list[str]:
    keys: list[str] = []
    for item in resource_keys:
        text = str(item or "").strip()
        if text and text not in keys:
            keys.append(text)
    batch_text = str(batch_key or "").strip()
    if batch_text:
        resource_key = f"handover_batch:{batch_text}"
        if resource_key not in keys:
            keys.append(resource_key)
    return keys


def _attach_followup_progress(
    followup: ReviewFollowupTriggerService,
    batch_status: Dict[str, Any] | None,
) -> Dict[str, Any]:
    payload = dict(batch_status or {})
    batch_key = str(payload.get("batch_key", "")).strip()
    payload["followup_progress"] = (
        followup.get_followup_progress(batch_key) if batch_key else _empty_followup_progress()
    )
    return payload


def _build_followup_failure_result(
    followup: ReviewFollowupTriggerService,
    *,
    batch_key: str,
    error: str,
) -> Dict[str, Any]:
    target_batch = str(batch_key or "").strip()
    return {
        "status": "failed",
        "batch_key": target_batch,
        "uploaded_buildings": [],
        "skipped_buildings": [],
        "failed_buildings": [],
        "details": {},
        "blocked_reason": "",
        "cloud_sheet_sync": {
            "status": "failed",
            "uploaded_buildings": [],
            "skipped_buildings": [],
            "failed_buildings": [],
            "details": {},
            "error": str(error or "").strip(),
        },
        "daily_report_record_export": {
            "status": "failed",
            "error": str(error or "").strip(),
        },
        "followup_progress": followup.get_followup_progress(target_batch) if target_batch else _empty_followup_progress(),
        "error": str(error or "").strip(),
    }


def _followup_status_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "ok": "成功",
        "success": "成功",
        "failed": "失败",
        "partial_failed": "部分失败",
        "blocked": "已阻塞",
        "skipped": "已跳过",
        "disabled": "已禁用",
        "ready_for_external": "等待外网继续",
        "pending_review": "待确认后上传",
    }
    return mapping.get(text, text or "-")


def _daily_report_stage_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "summary_sheet": "今日航图",
        "external_page": "外围页面",
        "unknown": "未知阶段",
    }
    return mapping.get(text, text or "-")


def _accepted_job_response(job) -> Dict[str, Any]:
    payload = job.to_dict() if hasattr(job, "to_dict") else dict(job or {})
    return {
        "ok": True,
        "accepted": True,
        "job": payload,
    }


def _start_handover_background_job(
    container,
    *,
    name: str,
    run_func,
    resource_keys: list[str] | tuple[str, ...] | None = None,
    priority: str = "manual",
    feature: str = "",
    submitted_by: str = "manual",
    worker_handler: str = "",
    worker_payload: Dict[str, Any] | None = None,
):
    job_service = container.job_service
    if worker_handler and hasattr(job_service, "start_worker_job"):
        return job_service.start_worker_job(
            name=name,
            worker_handler=worker_handler,
            worker_payload=worker_payload or {},
            resource_keys=resource_keys,
            priority=priority,
            feature=feature,
            submitted_by=submitted_by,
        )
    return job_service.start_job(
        name=name,
        run_func=run_func,
        resource_keys=resource_keys,
        priority=priority,
        feature=feature,
        submitted_by=submitted_by,
    )


def _build_daily_report_services(
    container,
) -> tuple[ReviewSessionService, HandoverDailyReportStateService, HandoverDailyReportAssetService, HandoverDailyReportScreenshotService]:
    handover_cfg = _handover_cfg(container)
    return (
        ReviewSessionService(handover_cfg),
        HandoverDailyReportStateService(handover_cfg),
        HandoverDailyReportAssetService(handover_cfg),
        HandoverDailyReportScreenshotService(handover_cfg),
    )


def _validate_daily_report_target_or_400(target: str) -> str:
    target_text = str(target or "").strip().lower()
    if target_text not in {"summary_sheet", "external_page"}:
        raise HTTPException(status_code=400, detail="target 参数错误")
    return target_text


def _load_daily_report_spreadsheet_url(
    review_service: ReviewSessionService,
    state_service: HandoverDailyReportStateService,
    *,
    duty_date: str,
    duty_shift: str,
) -> str:
    batch_key = state_service.build_batch_key(duty_date, duty_shift)
    export_state = state_service.get_export_state(duty_date=duty_date, duty_shift=duty_shift)
    spreadsheet_url = str(export_state.get("spreadsheet_url", "")).strip()
    if spreadsheet_url:
        return spreadsheet_url
    cloud_batch = review_service.get_cloud_batch(batch_key) or {}
    return str(cloud_batch.get("spreadsheet_url", "")).strip() if isinstance(cloud_batch, dict) else ""


def _build_daily_report_context_payload(
    *,
    review_service: ReviewSessionService,
    state_service: HandoverDailyReportStateService,
    asset_service: HandoverDailyReportAssetService,
    screenshot_service: HandoverDailyReportScreenshotService,
    duty_date: str,
    duty_shift: str,
) -> Dict[str, Any]:
    spreadsheet_url = _load_daily_report_spreadsheet_url(
        review_service,
        state_service,
        duty_date=duty_date,
        duty_shift=duty_shift,
    )
    try:
        screenshot_auth = screenshot_service.check_auth_status(
            emit_log=lambda *_args, **_kwargs: None,
            ensure_browser_running=False,
        )
    except Exception:
        screenshot_auth = state_service.get_screenshot_auth_state()
    capture_assets = asset_service.get_capture_assets_context(
        duty_date=duty_date,
        duty_shift=duty_shift,
    )
    return state_service.get_context(
        duty_date=duty_date,
        duty_shift=duty_shift,
        screenshot_auth=screenshot_auth,
        capture_assets=capture_assets,
        spreadsheet_url=spreadsheet_url,
    )



async def _build_daily_report_context_payload_async(
    *,
    review_service: ReviewSessionService,
    state_service: HandoverDailyReportStateService,
    asset_service: HandoverDailyReportAssetService,
    screenshot_service: HandoverDailyReportScreenshotService,
    duty_date: str,
    duty_shift: str,
) -> Dict[str, Any]:
    spreadsheet_url = _load_daily_report_spreadsheet_url(
        review_service,
        state_service,
        duty_date=duty_date,
        duty_shift=duty_shift,
    )
    try:
        if hasattr(screenshot_service, 'check_auth_status_async'):
            screenshot_auth = await screenshot_service.check_auth_status_async(
                emit_log=lambda *_args, **_kwargs: None,
                ensure_browser_running=False,
            )
        else:
            screenshot_auth = screenshot_service.check_auth_status(
                emit_log=lambda *_args, **_kwargs: None,
                ensure_browser_running=False,
            )
            if inspect.isawaitable(screenshot_auth):
                screenshot_auth = await screenshot_auth
    except Exception:
        screenshot_auth = state_service.get_screenshot_auth_state()
    capture_assets = asset_service.get_capture_assets_context(
        duty_date=duty_date,
        duty_shift=duty_shift,
    )
    return state_service.get_context(
        duty_date=duty_date,
        duty_shift=duty_shift,
        screenshot_auth=screenshot_auth,
        capture_assets=capture_assets,
        spreadsheet_url=spreadsheet_url,
    )


def _touch_daily_report_asset_rewrite_state(
    state_service: HandoverDailyReportStateService,
    *,
    duty_date: str,
    duty_shift: str,
) -> Dict[str, Any]:
    export_state = state_service.get_export_state(duty_date=duty_date, duty_shift=duty_shift)
    status = str(export_state.get("status", "")).strip().lower()
    if status in {"success", "pending_asset_rewrite"}:
        return state_service.mark_pending_asset_rewrite(duty_date=duty_date, duty_shift=duty_shift)
    return export_state


def _daily_report_error_message(error_code: str, *, fallback: str = "") -> str:
    code = str(error_code or "").strip()
    if code == "daily_report_url_field_invalid":
        return "日报链接字段写入失败，请检查飞书多维表“交接班日报”字段类型。"
    if code == "missing_spreadsheet_url":
        return "当前批次缺少云文档链接，无法重写日报记录。"
    if code == "missing_effective_asset":
        return "当前最终生效截图不完整，无法重写日报记录。"
    if code == "login_required":
        return "飞书截图登录态未就绪，请先完成登录。"
    if code == "target_page_not_open":
        return "目标网页当前没有在系统 Edge 中打开，请先打开对应页面后再重试。"
    if code == "summary_sheet_not_found":
        return "未找到今日航图页面，请确认当前飞书页面已打开且内容可见。"
    if code == "target_page_mismatch":
        return "当前打开页面与目标页面不一致，请重新打开对应飞书页面后重试。"
    if code == "capture_dom_unavailable":
        return "截图页面当前不可用，请稍后重试。"
    if code == "timeout":
        return "截图操作超时，请查看系统错误日志后重试。"
    return str(fallback or "").strip() or "操作失败，请查看系统错误日志。"


def _daily_report_failure_payload(raw: Dict[str, Any] | None = None, *, fallback_error_code: str = "", fallback_detail: str = "") -> Dict[str, str]:
    payload = raw if isinstance(raw, dict) else {}
    error_code = str(payload.get("error_code", "") or fallback_error_code or "").strip()
    error_detail = str(payload.get("error_detail", "") or fallback_detail or "").strip()
    error = str(payload.get("error", "") or "").strip()
    if not error and (error_code or error_detail):
        error = _daily_report_error_message(error_code, fallback=error_detail)
    return {
        "error": error,
        "error_code": error_code,
        "error_detail": error_detail,
    }


def _daily_report_capture_result_payload(raw: Dict[str, Any] | None = None, *, fallback_stage: str = "unknown", fallback_detail: str = "") -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    error_code = str(payload.get("error", "") or "").strip()
    error_detail = str(payload.get("error_detail", "") or fallback_detail or "").strip()
    error_message = str(payload.get("error_message", "") or "").strip()
    if not error_message:
        error_message = (
            _daily_report_error_message(error_code, fallback="")
            if error_code
            else "操作失败，请查看系统错误日志。"
        )
    return {
        "status": str(payload.get("status", "") or "failed").strip().lower() or "failed",
        "stage": str(payload.get("stage", "") or fallback_stage).strip().lower() or fallback_stage,
        "error": error_code,
        "error_detail": error_detail,
        "error_message": error_message,
        "path": str(payload.get("path", "") or "").strip(),
        "resolved_url": str(payload.get("resolved_url", "") or "").strip(),
        "resolved_page_id": str(payload.get("resolved_page_id", "") or "").strip(),
        "matched_mode": str(payload.get("matched_mode", "") or "").strip().lower(),
    }


def _persist_footer_inventory_defaults(container, *, building: str, document: Dict[str, Any]) -> int:
    defaults_service = FooterInventoryDefaultsService()
    rows = defaults_service.extract_rows_from_document(document)
    building_code = building_code_from_name(building)
    current_doc = get_handover_building_segment(building_code, container.config_path)
    current_data = current_doc.get("data", {}) if isinstance(current_doc.get("data", {}), dict) else {}
    updated_config = defaults_service.set_building_defaults(current_data, building, rows)
    saved_config, _document, aggregate_refresh_error = save_handover_building_segment(
        building_code,
        updated_config,
        base_revision=int(current_doc.get("revision", 0) or 0),
        config_path=container.config_path,
    )
    container.reload_config(saved_config)
    if aggregate_refresh_error:
        container.add_system_log(
            f"[交接班][审核模板默认] 楼栋分段已保存，但聚合配置刷新失败: building={building}, error={aggregate_refresh_error}"
    )
    return len(rows)


def _normalize_review_dirty_regions(raw: Any) -> Dict[str, bool]:
    if not isinstance(raw, dict):
        return {
            "fixed_blocks": True,
            "sections": True,
            "footer_inventory": True,
        }
    return {
        "fixed_blocks": bool(raw.get("fixed_blocks")),
        "sections": bool(raw.get("sections")),
        "footer_inventory": bool(raw.get("footer_inventory")),
    }


def _extract_review_fixed_cells(document: Dict[str, Any]) -> Dict[str, str]:
    output: Dict[str, str] = {}
    fixed_blocks = document.get("fixed_blocks", []) if isinstance(document, dict) else []
    if not isinstance(fixed_blocks, list):
        return output
    for block in fixed_blocks:
        if not isinstance(block, dict):
            continue
        for field in block.get("fields", []):
            if not isinstance(field, dict):
                continue
            cell_name = str(field.get("cell", "") or "").strip().upper()
            if not cell_name:
                continue
            output[cell_name] = str(field.get("value", "") or "").strip()
    return output


def _extract_capacity_tracked_cells(document: Dict[str, Any]) -> Dict[str, str]:
    fixed_cells = _extract_review_fixed_cells(document)
    return {
        cell: str(fixed_cells.get(cell, "") or "").strip()
        for cell in HandoverCapacityReportService.tracked_cells()
    }


def _should_sync_capacity_after_review_save(
    *,
    previous_session: Dict[str, Any] | None,
    dirty_regions: Dict[str, bool],
    tracked_cells: Dict[str, str],
) -> bool:
    if not bool((dirty_regions or {}).get("fixed_blocks")):
        return False
    previous = previous_session if isinstance(previous_session, dict) else {}
    previous_sync = previous.get("capacity_sync", {}) if isinstance(previous.get("capacity_sync", {}), dict) else {}
    previous_signature = str(previous_sync.get("input_signature", "") or "").strip()
    next_signature = HandoverCapacityReportService.capacity_input_signature(tracked_cells)
    if previous_signature != next_signature:
        return True
    status = str(previous_sync.get("status", "") or "").strip().lower()
    return status in {"pending_input", "missing_file", "failed"}


def _sync_capacity_overlay_after_review_save(
    *,
    container,
    review_service: ReviewSessionService,
    previous_session: Dict[str, Any],
    saved_session: Dict[str, Any],
    document: Dict[str, Any],
    dirty_regions: Dict[str, bool],
) -> Dict[str, Any]:
    if not callable(getattr(review_service, "update_capacity_sync", None)):
        return saved_session
    runtime_cfg = getattr(container, "runtime_config", None)
    if not isinstance(runtime_cfg, dict):
        return saved_session
    tracked_cells = _extract_capacity_tracked_cells(document)
    if not _should_sync_capacity_after_review_save(
        previous_session=previous_session,
        dirty_regions=dirty_regions,
        tracked_cells=tracked_cells,
    ):
        return saved_session

    building = str(saved_session.get("building", "")).strip()
    duty_date = str(saved_session.get("duty_date", "")).strip()
    duty_shift = str(saved_session.get("duty_shift", "")).strip().lower()
    if not building or not duty_date or duty_shift not in {"day", "night"}:
        return saved_session

    handover_cfg = _handover_cfg(container)
    capacity_service = HandoverCapacityReportService(handover_cfg)
    sync_payload = capacity_service.sync_overlay_for_existing_report_from_cells(
        building=building,
        duty_date=duty_date,
        duty_shift=duty_shift,
        handover_cells=tracked_cells,
        capacity_output_file=str(saved_session.get("capacity_output_file", "")).strip(),
        emit_log=container.add_system_log,
    )
    sync_status = str(sync_payload.get("status", "")).strip().lower()
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

    updated_session = review_service.update_capacity_sync(
        session_id=str(saved_session.get("session_id", "")).strip(),
        capacity_sync=sync_payload if isinstance(sync_payload, dict) else {},
        capacity_status=capacity_status,
        capacity_error=capacity_error,
    )
    container.add_system_log(
        "[交接班][容量报表][审核联动] 保存后补写状态 "
        f"building={updated_session.get('building', '-')}, "
        f"session_id={updated_session.get('session_id', '-')}, "
        f"status={sync_status or '-'}"
    )
    return updated_session


def _normalized_review_defaults_snapshot(
    *,
    footer_service: FooterInventoryDefaultsService,
    cabinet_service: CabinetPowerDefaultsService,
    config: Dict[str, Any],
    building: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    footer_rows = footer_service.get_building_defaults(config, building)
    cabinet_cells = cabinet_service.get_building_defaults(config, building)
    return (
        footer_service.normalize_rows(footer_rows if footer_rows is not None else []),
        cabinet_service.normalize_cells(cabinet_cells if cabinet_cells is not None else {}),
    )


def _persist_review_defaults(
    container,
    *,
    building: str,
    document: Dict[str, Any],
    dirty_regions: Dict[str, bool] | None = None,
) -> Dict[str, int | bool]:
    dirty = _normalize_review_dirty_regions(dirty_regions)
    state_service = _build_review_document_state_service(container)
    persisted = state_service.persist_defaults_from_document(
        building=building,
        document=document,
        dirty_regions=dirty,
    )
    result: Dict[str, int | bool | str] = {
        "footer_inventory_rows": int(persisted.get("footer_inventory_rows", 0) or 0),
        "cabinet_power_fields": int(persisted.get("cabinet_power_fields", 0) or 0),
        "defaults_updated": bool(persisted.get("defaults_updated", False)),
        "config_updated": False,
        "aggregate_refresh_error": "",
    }
    if not dirty.get("footer_inventory") and not dirty.get("fixed_blocks"):
        return result

    footer_service = FooterInventoryDefaultsService()
    cabinet_service = CabinetPowerDefaultsService()
    building_code = building_code_from_name(building)
    current_doc = get_handover_building_segment(building_code, container.config_path)
    current_data = (
        copy.deepcopy(current_doc.get("data", {}))
        if isinstance(current_doc.get("data", {}), dict)
        else {}
    )
    updated_data = copy.deepcopy(current_data)

    if dirty.get("footer_inventory"):
        updated_data = footer_service.set_building_defaults(
            updated_data,
            building,
            footer_service.extract_rows_from_document(document),
        )
    if dirty.get("fixed_blocks"):
        updated_data = cabinet_service.set_building_defaults(
            updated_data,
            building,
            cabinet_service.extract_cells_from_document(document),
        )

    if updated_data == current_data:
        return result

    try:
        saved_config, _document, aggregate_refresh_error = save_handover_building_segment(
            building_code,
            updated_data,
            base_revision=int(current_doc.get("revision", 0) or 0),
            config_path=container.config_path,
        )
        container.reload_config(saved_config)
        result["config_updated"] = True
        result["aggregate_refresh_error"] = str(aggregate_refresh_error or "").strip()
    except Exception as exc:  # noqa: BLE001
        result["config_updated"] = False
        result["config_error"] = str(exc)
    return result


def _resolve_building_or_404(service: ReviewSessionService, building_code: str) -> str:
    building = service.get_building_by_code(building_code)
    if not building:
        raise HTTPException(status_code=404, detail="未知楼栋页面")
    return building


def _load_latest_session_or_404(service: ReviewSessionService, building: str) -> Dict[str, Any]:
    try:
        session = service.get_latest_session(building)
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    if not isinstance(session, dict):
        raise HTTPException(status_code=404, detail="暂无可审核交接班文件")
    return session


def _normalize_duty_context(duty_date: str = "", duty_shift: str = "") -> tuple[str, str]:
    duty_date_text = str(duty_date or "").strip()
    duty_shift_text = str(duty_shift or "").strip().lower()
    if not duty_date_text or duty_shift_text not in {"day", "night"}:
        return "", ""
    return duty_date_text, duty_shift_text


def _load_target_session_or_404(
    service: ReviewSessionService,
    *,
    building: str,
    duty_date: str = "",
    duty_shift: str = "",
    session_id: str = "",
) -> Dict[str, Any]:
    session_id_text = str(session_id or "").strip()
    try:
        if session_id_text:
            session = service.get_session_by_id(session_id_text)
        else:
            duty_date_text, duty_shift_text = _normalize_duty_context(duty_date, duty_shift)
            if duty_date_text and duty_shift_text:
                session = service.get_session_for_building_duty(building, duty_date_text, duty_shift_text)
            else:
                session = service.get_latest_session(building)
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    if not isinstance(session, dict):
        raise HTTPException(status_code=404, detail="暂无可审核交接班文件")
    if str(session.get("building", "")).strip() != str(building or "").strip():
        raise HTTPException(status_code=404, detail="review session building mismatch")
    return session


def _shift_label(shift_code: str) -> str:
    normalized = str(shift_code or "").strip().lower()
    if normalized == "day":
        return "白班"
    if normalized == "night":
        return "夜班"
    return normalized or "-"


def _parse_base_revision_or_400(payload: Dict[str, Any]) -> int:
    try:
        return int((payload or {}).get("base_revision", 0) or 0)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="base_revision 参数错误") from exc


HISTORY_CLOUD_SUCCESS_LIMIT = 10
HISTORY_CLOUD_SUCCESS_RULE = "cloud_success_only"


def _empty_history_payload(
    *,
    latest_session_id: str = "",
    selected_session_id: str = "",
    error: str = "",
) -> Dict[str, Any]:
    latest_session_id_text = str(latest_session_id or "").strip()
    selected_session_id_text = str(selected_session_id or "").strip()
    return {
        "latest_session_id": latest_session_id_text,
        "selected_session_id": selected_session_id_text,
        "selected_is_latest": bool(
            latest_session_id_text
            and selected_session_id_text
            and latest_session_id_text == selected_session_id_text
        ),
        "selected_in_history_list": False,
        "selected_history_excluded_reason": "",
        "history_limit": HISTORY_CLOUD_SUCCESS_LIMIT,
        "history_rule": HISTORY_CLOUD_SUCCESS_RULE,
        "sessions": [],
        "degraded": bool(str(error or "").strip()),
        "error": str(error or "").strip(),
    }


def _build_history_payload(service: ReviewSessionService, *, building: str, selected_session_id: str) -> Dict[str, Any]:
    latest_session_id = service.get_latest_session_id(building)
    sessions = service.list_building_cloud_history_sessions(building, limit=HISTORY_CLOUD_SUCCESS_LIMIT)
    selected_session_id_text = str(selected_session_id or "").strip()
    history_sessions = []
    selected_in_history_list = False
    for item in sessions:
        session_id = str(item.get("session_id", "")).strip()
        is_latest = bool(latest_session_id and session_id == latest_session_id)
        if session_id == selected_session_id_text:
            selected_in_history_list = True
        history_sessions.append(
            {
                "session_id": session_id,
                "building": str(item.get("building", "")).strip(),
                "duty_date": str(item.get("duty_date", "")).strip(),
                "duty_shift": str(item.get("duty_shift", "")).strip().lower(),
                "revision": int(item.get("revision", 0) or 0),
                "confirmed": bool(item.get("confirmed", False)),
                "updated_at": str(item.get("updated_at", "")).strip(),
                "output_file": str(item.get("output_file", "")).strip(),
                "has_output_file": Path(str(item.get("output_file", "")).strip()).exists(),
                "is_latest": is_latest,
                "label": f"{'最新 ' if is_latest else ''}{str(item.get('duty_date', '')).strip()} / {_shift_label(str(item.get('duty_shift', '')).strip())}",
            }
        )
    selected_is_latest = bool(
        selected_session_id_text
        and latest_session_id
        and selected_session_id_text == latest_session_id
    )
    selected_history_excluded_reason = ""
    if selected_session_id_text and not selected_in_history_list:
        selected_session = service.get_session_by_id(selected_session_id_text)
        if isinstance(selected_session, dict):
            output_file = Path(str(selected_session.get("output_file", "")).strip())
            cloud_sync = ReviewSessionService._normalize_cloud_sheet_sync(selected_session.get("cloud_sheet_sync", {}))
            if (
                output_file.exists()
                and str(cloud_sync.get("status", "")).strip().lower() == "success"
                and str(cloud_sync.get("spreadsheet_url", "")).strip()
            ):
                selected_history_excluded_reason = "outside_limit"
            else:
                selected_history_excluded_reason = "not_cloud_success"
    return {
        "latest_session_id": latest_session_id,
        "selected_session_id": selected_session_id_text,
        "selected_is_latest": selected_is_latest,
        "selected_in_history_list": selected_in_history_list,
        "selected_history_excluded_reason": selected_history_excluded_reason,
        "history_limit": HISTORY_CLOUD_SUCCESS_LIMIT,
        "history_rule": HISTORY_CLOUD_SUCCESS_RULE,
        "sessions": history_sessions,
    }


def _build_history_payload_safe(
    service: ReviewSessionService,
    *,
    building: str,
    selected_session_id: str,
    emit_log=None,
) -> Dict[str, Any]:
    try:
        return _build_history_payload(
            service,
            building=building,
            selected_session_id=selected_session_id,
        )
    except Exception as exc:  # noqa: BLE001
        latest_session_id = ""
        try:
            latest_session_id = service.get_latest_session_id(building)
        except Exception:  # noqa: BLE001
            latest_session_id = ""
        if callable(emit_log):
            emit_log(
                f"[交接班][历史列表] 已降级: building={building}, 错误={type(exc).__name__}: {exc}"
            )
        return _empty_history_payload(
            latest_session_id=latest_session_id,
            selected_session_id=selected_session_id,
            error="history_unavailable",
        )


def _ensure_latest_session_actionable_or_400(service: ReviewSessionService, *, building: str, session_id: str) -> None:
    try:
        latest_session_id = service.get_latest_session_id(building)
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    if not latest_session_id:
        raise HTTPException(status_code=404, detail="暂无可审核交接班文件")
    if str(session_id or "").strip() != latest_session_id:
        raise HTTPException(status_code=400, detail="仅最新交接班日志支持确认、撤销确认和云表重试")


@router.get("/handover/review/{building_code}")
def handover_review_page(building_code: str, request: Request):
    container = request.app.state.container
    service, _, _, _ = _build_review_services(container)
    _resolve_building_or_404(service, building_code)
    if str(container.frontend_mode or "").strip().lower() == "source":
        return HTMLResponse(
            render_frontend_index_html(
                container.frontend_root,
                frontend_mode=container.frontend_mode,
                asset_base_path=str(getattr(request.app.state, "source_frontend_asset_prefix", "/assets")),
            ),
            headers=source_frontend_no_cache_headers(container.frontend_mode),
        )
    return FileResponse(
        container.frontend_root / "index.html",
        headers=source_frontend_no_cache_headers(container.frontend_mode),
    )


@router.get("/api/handover/review/batch/{batch_key}/status")
def handover_review_batch_status(batch_key: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    service, _, _, followup = _build_review_services(container)
    try:
        batch_status = service.get_batch_status(batch_key)
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    return {"ok": True, **_attach_followup_progress(followup, batch_status)}


@router.post("/api/handover/review/batch/{batch_key}/confirm-all")
def handover_review_confirm_all(batch_key: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container

    def _run(emit_log) -> Dict[str, Any]:
        service, _, _, followup = _build_review_services(container)
        emit_log(f"[交接班][审核一键全确认] 开始 batch={batch_key}")
        updated_sessions, batch_status = service.confirm_all_in_batch(batch_key=batch_key)
        emit_log(
            f"[交接班][审核一键全确认] batch={batch_key}, sessions={len(updated_sessions)}, all_confirmed={bool(batch_status.get('all_confirmed', False))}"
        )
        emit_log(f"[交接班][确认后上传] 开始 batch={batch_key}")
        try:
            followup_result = followup.trigger_batch(batch_key, emit_log=emit_log)
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][确认后上传] 失败 batch={batch_key}, 错误={exc}")
            followup_result = _build_followup_failure_result(
                followup,
                batch_key=batch_key,
                error=str(exc),
            )
        emit_log(
            f"[交接班][确认后上传] batch={batch_key}, 状态={_followup_status_text(followup_result.get('status'))}, "
            f"已上传={len(followup_result.get('uploaded_buildings', []))}, 已失败={len(followup_result.get('failed_buildings', []))}, "
            f"云表状态={_followup_status_text(followup_result.get('cloud_sheet_sync', {}).get('status', '-'))}"
        )
        refreshed_batch_status = _attach_followup_progress(followup, service.get_batch_status(batch_key))
        refreshed_sessions = service.list_batch_sessions(batch_key)
        return {
            "ok": True,
            "updated_sessions": refreshed_sessions or updated_sessions,
            "batch_status": refreshed_batch_status or _attach_followup_progress(followup, batch_status),
            "followup_result": followup_result,
        }

    job = _start_handover_background_job(
        container,
        name=f"交接班审核一键全确认-{batch_key}",
        run_func=_run,
        worker_handler="handover_confirm_all",
        worker_payload={"batch_key": batch_key},
        resource_keys=_handover_resource_keys(batch_key=batch_key),
        priority="manual",
        feature="handover_confirm_all",
        submitted_by="manual",
    )
    container.add_system_log(f"[任务] 已提交: 交接班审核一键全确认 batch={batch_key} ({job.job_id})")
    return _accepted_job_response(job)


@router.get("/api/handover/daily-report/context")
def handover_daily_report_context(
    request: Request,
    duty_date: str = "",
    duty_shift: str = "",
) -> Dict[str, Any]:
    duty_date_text, duty_shift_text = _normalize_duty_context(duty_date, duty_shift)
    if not duty_date_text or not duty_shift_text:
        raise HTTPException(status_code=400, detail="duty_date / duty_shift 参数错误")

    container = request.app.state.container
    review_service, state_service, asset_service, screenshot_service = _build_daily_report_services(container)
    return _build_daily_report_context_payload(
        review_service=review_service,
        state_service=state_service,
        asset_service=asset_service,
        screenshot_service=screenshot_service,
        duty_date=duty_date_text,
        duty_shift=duty_shift_text,
    )


@router.post("/api/handover/daily-report/screenshot-auth/open")
def handover_daily_report_open_screenshot_auth(
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    duty_date_text, duty_shift_text = _normalize_duty_context(
        str(payload.get("duty_date", "")).strip() if isinstance(payload, dict) else "",
        str(payload.get("duty_shift", "")).strip() if isinstance(payload, dict) else "",
    )
    if not duty_date_text or not duty_shift_text:
        raise HTTPException(status_code=400, detail="duty_date / duty_shift 参数错误")

    container = request.app.state.container
    batch_key = f"{duty_date_text}|{duty_shift_text}"

    def _run(emit_log) -> Dict[str, Any]:
        _, _, _, screenshot_service = _build_daily_report_services(container)
        result = screenshot_service.open_login_browser(emit_log=emit_log)
        return {
            "ok": bool(result.get("ok", False)),
            "status": str(result.get("status", "")).strip() or "failed",
            "message": str(result.get("message", "")).strip(),
            "profile_dir": str(result.get("profile_dir", "")).strip(),
        }

    job = _start_handover_background_job(
        container,
        name=f"日报截图登录态初始化-{batch_key}",
        run_func=_run,
        worker_handler="daily_report_auth_open",
        worker_payload={"duty_date": duty_date_text, "duty_shift": duty_shift_text},
        resource_keys=_handover_resource_keys("browser:controlled", batch_key=batch_key),
        priority="manual",
        feature="daily_report_auth_open",
        submitted_by="manual",
    )
    container.add_system_log(f"[任务] 已提交: 日报截图登录态初始化 batch={batch_key} ({job.job_id})")
    return _accepted_job_response(job)


@router.post("/api/handover/daily-report/screenshot-test")
def handover_daily_report_screenshot_test(
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    duty_date_text, duty_shift_text = _normalize_duty_context(
        str(payload.get("duty_date", "")).strip() if isinstance(payload, dict) else "",
        str(payload.get("duty_shift", "")).strip() if isinstance(payload, dict) else "",
    )
    if not duty_date_text or duty_shift_text not in {"day", "night"}:
        raise HTTPException(status_code=400, detail="invalid duty context")

    container = request.app.state.container
    batch_key = f"{duty_date_text}|{duty_shift_text}"

    def _run(emit_log) -> Dict[str, Any]:
        review_service, _state_service, asset_service, screenshot_service = _build_daily_report_services(container)
        cloud_batch = review_service.get_cloud_batch(batch_key) or {}
        spreadsheet_url = str(cloud_batch.get("spreadsheet_url", "")).strip() if isinstance(cloud_batch, dict) else ""
        summary_result = screenshot_service.capture_summary_sheet(
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
            emit_log=emit_log,
            prefer_existing_page=True,
            allow_open_fallback=True,
        )

        external_result = screenshot_service.capture_external_page(
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
            emit_log=emit_log,
            prefer_existing_page=True,
            allow_open_fallback=True,
        )

        statuses = {
            "summary": str(summary_result.get("status", "")).strip().lower(),
            "external": str(external_result.get("status", "")).strip().lower(),
        }
        if statuses["summary"] in {"ok", "skipped"} and statuses["external"] == "ok":
            overall_status = "ok"
        elif statuses["summary"] == "ok" or statuses["external"] == "ok":
            overall_status = "partial_failed"
        else:
            overall_status = "failed"

        return {
            "ok": overall_status != "failed",
            "status": overall_status,
            "batch_key": batch_key,
            "spreadsheet_url": spreadsheet_url,
            "summary_sheet_image": summary_result,
            "external_page_image": external_result,
            "capture_assets": asset_service.get_capture_assets_context(
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
            ),
        }

    job = _start_handover_background_job(
        container,
        name=f"日报截图测试-{batch_key}",
        run_func=_run,
        worker_handler="daily_report_screenshot_test",
        worker_payload={"duty_date": duty_date_text, "duty_shift": duty_shift_text},
        resource_keys=_handover_resource_keys("browser:controlled", batch_key=batch_key),
        priority="manual",
        feature="daily_report_screenshot_test",
        submitted_by="manual",
    )
    container.add_system_log(f"[任务] 已提交: 日报截图测试 batch={batch_key} ({job.job_id})")
    return _accepted_job_response(job)


@router.get("/api/handover/daily-report/capture-assets/file")
def handover_daily_report_capture_asset_file(
    request: Request,
    duty_date: str = "",
    duty_shift: str = "",
    target: str = "",
    variant: str = "effective",
    view: str = "full",
):
    duty_date_text, duty_shift_text = _normalize_duty_context(duty_date, duty_shift)
    if not duty_date_text or not duty_shift_text:
        raise HTTPException(status_code=400, detail="duty_date / duty_shift 参数错误")
    target_text = _validate_daily_report_target_or_400(target)
    variant_text = str(variant or "").strip().lower()
    if variant_text not in {"effective", "auto", "manual"}:
        raise HTTPException(status_code=400, detail="variant 参数错误")
    view_text = str(view or "").strip().lower() or "full"
    if view_text not in {"full", "thumb"}:
        raise HTTPException(status_code=400, detail="view 参数错误")

    container = request.app.state.container
    _, _, asset_service, _ = _build_daily_report_services(container)
    path = asset_service.get_asset_file_path(
        duty_date=duty_date_text,
        duty_shift=duty_shift_text,
        target=target_text,
        variant=variant_text,
        view=view_text,
    )
    if path is None or not path.exists():
        raise HTTPException(status_code=404, detail="截图文件不存在")
    suffix = str(path.suffix or "").strip().lower()
    media_type = "image/jpeg" if suffix in {".jpg", ".jpeg"} else "image/png"
    return FileResponse(path=path, media_type=media_type, filename=path.name)


@router.post("/api/handover/daily-report/capture-assets/recapture")
def handover_daily_report_recapture_asset(
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    duty_date_text, duty_shift_text = _normalize_duty_context(
        str(payload.get("duty_date", "")).strip() if isinstance(payload, dict) else "",
        str(payload.get("duty_shift", "")).strip() if isinstance(payload, dict) else "",
    )
    if not duty_date_text or not duty_shift_text:
        raise HTTPException(status_code=400, detail="duty_date / duty_shift 参数错误")
    target_text = _validate_daily_report_target_or_400(payload.get("target", "") if isinstance(payload, dict) else "")

    container = request.app.state.container
    batch_key = f"{duty_date_text}|{duty_shift_text}"

    def _run(emit_log) -> Dict[str, Any]:
        review_service, state_service, asset_service, screenshot_service = _build_daily_report_services(container)
        try:
            if target_text == "summary_sheet":
                result = _daily_report_capture_result_payload(
                    screenshot_service.capture_summary_sheet(
                        duty_date=duty_date_text,
                        duty_shift=duty_shift_text,
                        emit_log=emit_log,
                        prefer_existing_page=True,
                        allow_open_fallback=True,
                    )
                )
            else:
                result = _daily_report_capture_result_payload(
                    screenshot_service.capture_external_page(
                        duty_date=duty_date_text,
                        duty_shift=duty_shift_text,
                        emit_log=emit_log,
                        prefer_existing_page=True,
                        allow_open_fallback=True,
                    )
                )
        except Exception as exc:  # noqa: BLE001
            result = _daily_report_capture_result_payload(
                fallback_stage="unknown",
                fallback_detail=str(exc),
            )
            emit_log(
                f"[交接班][日报截图] 失败 batch={duty_date_text}|{duty_shift_text}, target={target_text}, "
                f"阶段={_daily_report_stage_text(result['stage'])}, 状态={_followup_status_text(result['status'])}, "
                f"错误={result['error_detail'] or result['error']}"
            )

        if str(result.get("status", "")).strip().lower() == "ok":
            _touch_daily_report_asset_rewrite_state(
                state_service,
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
            )
        context = _build_daily_report_context_payload(
            review_service=review_service,
            state_service=state_service,
            asset_service=asset_service,
            screenshot_service=screenshot_service,
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
        )
        return {
            "ok": str(result.get("status", "")).strip().lower() == "ok",
            "target": target_text,
            "result": result,
            "capture_assets": context.get("capture_assets", {}),
            "daily_report_record_export": context.get("daily_report_record_export", {}),
        }

    job = _start_handover_background_job(
        container,
        name=f"日报截图重截-{target_text}-{batch_key}",
        run_func=_run,
        worker_handler="daily_report_recapture",
        worker_payload={"duty_date": duty_date_text, "duty_shift": duty_shift_text, "target": target_text},
        resource_keys=_handover_resource_keys("browser:controlled", batch_key=batch_key),
        priority="manual",
        feature=f"daily_report_recapture_{target_text}",
        submitted_by="manual",
    )
    container.add_system_log(f"[任务] 已提交: 日报截图重截 target={target_text} batch={batch_key} ({job.job_id})")
    return _accepted_job_response(job)


@router.post("/api/handover/daily-report/capture-assets/upload")
async def handover_daily_report_upload_asset(
    request: Request,
    duty_date: str = Form(default=""),
    duty_shift: str = Form(default=""),
    target: str = Form(default=""),
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    duty_date_text, duty_shift_text = _normalize_duty_context(duty_date, duty_shift)
    if not duty_date_text or not duty_shift_text:
        raise HTTPException(status_code=400, detail="duty_date / duty_shift 参数错误")
    target_text = _validate_daily_report_target_or_400(target)
    content_type = str(file.content_type or "").strip().lower()
    allowed_types = {"image/png", "image/jpeg", "image/jpg", "image/webp"}
    if content_type and content_type not in allowed_types:
        raise HTTPException(status_code=400, detail="仅支持 png/jpg/jpeg/webp 图片")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="上传文件为空")

    container = request.app.state.container
    review_service, state_service, asset_service, screenshot_service = _build_daily_report_services(container)
    try:
        path = asset_service.save_manual_image(
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
            target=target_text,
            content=content,
            mime_type=content_type,
            original_name=str(file.filename or "").strip(),
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"图片处理失败: {exc}") from exc
    _touch_daily_report_asset_rewrite_state(
        state_service,
        duty_date=duty_date_text,
        duty_shift=duty_shift_text,
    )
    context = await _build_daily_report_context_payload_async(
        review_service=review_service,
        state_service=state_service,
        asset_service=asset_service,
        screenshot_service=screenshot_service,
        duty_date=duty_date_text,
        duty_shift=duty_shift_text,
    )
    return {
        "ok": True,
        "target": target_text,
        "result": {"status": "ok", "error": "", "path": str(path)},
        "capture_assets": context.get("capture_assets", {}),
        "daily_report_record_export": context.get("daily_report_record_export", {}),
    }


@router.delete("/api/handover/daily-report/capture-assets/manual")
def handover_daily_report_delete_manual_asset(
    request: Request,
    duty_date: str = "",
    duty_shift: str = "",
    target: str = "",
) -> Dict[str, Any]:
    duty_date_text, duty_shift_text = _normalize_duty_context(duty_date, duty_shift)
    if not duty_date_text or not duty_shift_text:
        raise HTTPException(status_code=400, detail="duty_date / duty_shift 参数错误")
    target_text = _validate_daily_report_target_or_400(target)

    container = request.app.state.container
    review_service, state_service, asset_service, screenshot_service = _build_daily_report_services(container)
    removed = asset_service.delete_manual_image(
        duty_date=duty_date_text,
        duty_shift=duty_shift_text,
        target=target_text,
    )
    if removed:
        _touch_daily_report_asset_rewrite_state(
            state_service,
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
        )
    context = _build_daily_report_context_payload(
        review_service=review_service,
        state_service=state_service,
        asset_service=asset_service,
        screenshot_service=screenshot_service,
        duty_date=duty_date_text,
        duty_shift=duty_shift_text,
    )
    return {
        "ok": True,
        "target": target_text,
        "removed": removed,
        "capture_assets": context.get("capture_assets", {}),
        "daily_report_record_export": context.get("daily_report_record_export", {}),
    }


@router.post("/api/handover/daily-report/record/rewrite")
def handover_daily_report_rewrite_record(
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    duty_date_text, duty_shift_text = _normalize_duty_context(
        str(payload.get("duty_date", "")).strip() if isinstance(payload, dict) else "",
        str(payload.get("duty_shift", "")).strip() if isinstance(payload, dict) else "",
    )
    if not duty_date_text or not duty_shift_text:
        raise HTTPException(status_code=400, detail="duty_date / duty_shift 参数错误")

    container = request.app.state.container
    batch_key = f"{duty_date_text}|{duty_shift_text}"

    def _run(emit_log) -> Dict[str, Any]:
        review_service, state_service, asset_service, screenshot_service = _build_daily_report_services(container)
        followup = ReviewFollowupTriggerService(_handover_cfg(container))
        logged_failure = False
        try:
            result = followup.rewrite_daily_report_record(
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            failure = _daily_report_failure_payload(
                fallback_error_code=str(getattr(exc, "error_code", "") or "daily_report_export_failed"),
                fallback_detail=str(getattr(exc, "error_detail", "") or str(exc)),
            )
            emit_log(
                f"[交接班][日报多维] 失败 batch={duty_date_text}|{duty_shift_text}, "
                f"原因={failure['error_detail'] or failure['error']}"
            )
            logged_failure = True
            result = state_service.update_export_state(
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                daily_report_record_export={
                    **state_service.get_export_state(duty_date=duty_date_text, duty_shift=duty_shift_text),
                    "status": "failed",
                    "error": failure["error"],
                    "error_code": failure["error_code"],
                    "error_detail": failure["error_detail"],
                },
            )
        failure = _daily_report_failure_payload(result)
        if (
            not logged_failure
            and str(result.get("status", "")).strip().lower() != "success"
            and (failure["error"] or failure["error_detail"])
        ):
            emit_log(
                f"[交接班][日报多维] 失败 batch={duty_date_text}|{duty_shift_text}, "
                f"原因={failure['error_detail'] or failure['error']}"
            )
        context = _build_daily_report_context_payload(
            review_service=review_service,
            state_service=state_service,
            asset_service=asset_service,
            screenshot_service=screenshot_service,
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
        )
        return {
            "ok": str(result.get("status", "")).strip().lower() == "success",
            "error": failure["error"],
            "error_code": failure["error_code"],
            "error_detail": failure["error_detail"],
            "daily_report_record_export": context.get("daily_report_record_export", {}),
            "capture_assets": context.get("capture_assets", {}),
        }

    job = _start_handover_background_job(
        container,
        name=f"日报多维重写-{batch_key}",
        run_func=_run,
        worker_handler="daily_report_record_rewrite",
        worker_payload={"duty_date": duty_date_text, "duty_shift": duty_shift_text},
        resource_keys=_handover_resource_keys("network:external", batch_key=batch_key),
        priority="manual",
        feature="daily_report_record_rewrite",
        submitted_by="manual",
    )
    container.add_system_log(f"[任务] 已提交: 日报多维重写 batch={batch_key} ({job.job_id})")
    return _accepted_job_response(job)


@router.get("/api/handover/review/{building_code}")
def handover_review_data(
    building_code: str,
    request: Request,
    duty_date: str = "",
    duty_shift: str = "",
    session_id: str = "",
    client_id: str = "",
) -> Dict[str, Any]:
    container = request.app.state.container
    service, parser, writer, _ = _build_review_services(container)
    document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
    building = _resolve_building_or_404(service, building_code)
    session = _load_target_session_or_404(
        service,
        building=building,
        duty_date=duty_date,
        duty_shift=duty_shift,
        session_id=session_id,
    )
    try:
        document, session = document_state.load_document(session)
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    try:
        batch_status = service.get_batch_status(session["batch_key"])
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    review_ui = parser.config.get("review_ui", {}) if isinstance(parser.config, dict) else {}
    return {
        "ok": True,
        "building": building,
        "session": session,
        "document": document,
        "batch_status": batch_status,
        "concurrency": _get_session_concurrency_safe(
            service,
            building=building,
            session_id=str(session.get("session_id", "")).strip(),
            client_id=str(client_id or "").strip(),
            current_revision=int(session.get("revision", 0) or 0),
            emit_log=container.add_system_log,
        ),
        "review_ui": review_ui if isinstance(review_ui, dict) else {},
        "history": _build_history_payload_safe(
            service,
            building=building,
            selected_session_id=str(session.get("session_id", "")).strip(),
            emit_log=container.add_system_log,
        ),
    }


@router.get("/api/handover/review/{building_code}/download")
def handover_review_download(building_code: str, request: Request, session_id: str = "") -> FileResponse:
    container = request.app.state.container
    service, parser, writer, _ = _build_review_services(container)
    document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
    building = _resolve_building_or_404(service, building_code)

    session_id_text = str(session_id or "").strip()
    if not session_id_text:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    target = _load_target_session_or_404(service, building=building, session_id=session_id_text)
    try:
        document_state.ensure_document_for_session(target)
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    output_file_text = str(target.get("output_file", "")).strip()
    if not output_file_text:
        raise HTTPException(status_code=409, detail="交接班文件不存在，无法同步最新审核内容")
    output_file = Path(output_file_text)
    if not output_file.exists() or not output_file.is_file():
        raise HTTPException(status_code=409, detail="交接班文件不存在，无法同步最新审核内容")
    try:
        document_state.force_sync_session_dict(target, reason="download")
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    container.add_system_log(
        f"[交接班][下载成品] building={building}, session_id={session_id_text}, file={output_file}"
    )
    return FileResponse(
        path=output_file,
        filename=output_file.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.get("/api/handover/review/{building_code}/capacity-download")
def handover_review_capacity_download(building_code: str, request: Request, session_id: str = "") -> FileResponse:
    container = request.app.state.container
    service, _, _, _ = _build_review_services(container)
    building = _resolve_building_or_404(service, building_code)

    session_id_text = str(session_id or "").strip()
    if not session_id_text:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    target = _load_target_session_or_404(service, building=building, session_id=session_id_text)

    output_file_text = str(target.get("capacity_output_file", "")).strip()
    if not output_file_text:
        raise HTTPException(status_code=404, detail="当前交接班容量报表尚未生成")
    output_file = Path(output_file_text)
    if not output_file.exists() or not output_file.is_file():
        raise HTTPException(status_code=404, detail="交接班容量报表文件不存在，请重新生成")
    capacity_sync = target.get("capacity_sync", {}) if isinstance(target.get("capacity_sync", {}), dict) else {}
    capacity_sync_status = str(capacity_sync.get("status", "")).strip().lower()
    if capacity_sync_status != "ready":
        detail = str(capacity_sync.get("error", "")).strip() or "容量报表待补写完成后才能下载"
        raise HTTPException(status_code=409, detail=detail)

    container.add_system_log(
        f"[交接班][下载容量报表] building={building}, session_id={session_id_text}, file={output_file}"
    )
    return FileResponse(
        path=output_file,
        filename=output_file.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.put("/api/handover/review/{building_code}")
def handover_review_save(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, parser, writer, _ = _build_review_services(container)
    document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
    building = _resolve_building_or_404(service, building_code)

    session_id = str(payload.get("session_id", "")).strip()
    base_revision = int(payload.get("base_revision", 0) or 0)
    document = payload.get("document", {})
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    if not isinstance(document, dict):
        raise HTTPException(status_code=400, detail="document 格式错误")
    target = _load_target_session_or_404(service, building=building, session_id=session_id)
    try:
        document_state.ensure_document_for_session(target)
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    batch_key = str(target.get("batch_key", "")).strip()
    dirty_regions = _normalize_review_dirty_regions(payload.get("dirty_regions"))
    save_started = time.perf_counter()
    write_elapsed_ms = 0
    defaults_elapsed_ms = 0
    capacity_elapsed_ms = 0
    session_elapsed_ms = 0
    queued_excel_sync = False
    with container.job_service.resource_guard(
        name=f"handover_save:{batch_key or building}:{session_id}",
        resource_keys=_handover_resource_keys(batch_key=batch_key),
    ):
        write_started = time.perf_counter()
        previous_document_state: Dict[str, Any] | None = None
        try:
            _saved_document_state, previous_document_state = document_state.save_document(
                session=target,
                document=document,
                base_revision=base_revision,
                dirty_regions=dirty_regions,
            )
        except ReviewDocumentStateConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ReviewDocumentStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        write_elapsed_ms = int((time.perf_counter() - write_started) * 1000)
        try:
            latest_session_id = service.get_latest_session_id(building)
        except ReviewSessionStoreUnavailableError as exc:
            document_state.restore_document(building=building, previous=previous_document_state)
            _raise_review_store_http_error(exc, saved_document=True)
        is_latest_session = bool(latest_session_id and latest_session_id == session_id)
        persisted_defaults = {"footer_inventory_rows": 0, "cabinet_power_fields": 0, "config_updated": False}
        if is_latest_session:
            defaults_started = time.perf_counter()
            try:
                persisted_defaults = _persist_review_defaults(
                    container,
                    building=building,
                    document=document,
                    dirty_regions=dirty_regions,
                )
            except Exception as exc:  # noqa: BLE001
                persisted_defaults = {
                    "footer_inventory_rows": 0,
                    "cabinet_power_fields": 0,
                    "config_updated": False,
                    "defaults_updated": False,
                    "error": str(exc),
                }
                container.add_system_log(
                    f"[交接班][审核模板默认] SQLite默认值写入失败，已保留审核文档保存结果: building={building}, error={exc}"
                )
            defaults_elapsed_ms = int((time.perf_counter() - defaults_started) * 1000)
        try:
            session_started = time.perf_counter()
            if is_latest_session:
                session, batch_status = service.touch_session_after_save(
                    building=building,
                    session_id=session_id,
                    base_revision=base_revision,
                )
            else:
                session, batch_status = service.touch_session_after_history_save(
                    building=building,
                    session_id=session_id,
                    base_revision=base_revision,
                )
            session_elapsed_ms = int((time.perf_counter() - session_started) * 1000)
        except ReviewSessionConflictError as exc:
            document_state.restore_document(building=building, previous=previous_document_state)
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ReviewSessionNotFoundError as exc:
            document_state.restore_document(building=building, previous=previous_document_state)
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ReviewSessionStoreUnavailableError as exc:
            document_state.restore_document(building=building, previous=previous_document_state)
            _raise_review_store_http_error(exc, saved_document=True)
        try:
            capacity_started = time.perf_counter()
            session = _sync_capacity_overlay_after_review_save(
                container=container,
                review_service=service,
                previous_session=target,
                saved_session=session,
                document=document,
                dirty_regions=dirty_regions,
            )
            capacity_elapsed_ms = int((time.perf_counter() - capacity_started) * 1000)
        except ReviewSessionStoreUnavailableError as exc:
            _raise_review_store_http_error(exc, saved_document=True)
        except ReviewSessionNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        try:
            excel_sync = document_state.enqueue_excel_sync(
                session=session,
                target_revision=int(session.get("revision", 0) or 0),
            )
            queued_excel_sync = str(excel_sync.get("status", "")).strip().lower() not in {"", "failed", "unknown"}
        except Exception as exc:  # noqa: BLE001
            excel_sync = {
                "status": "failed",
                "synced_revision": int(session.get("revision", 0) or 0) - 1 if int(session.get("revision", 0) or 0) > 0 else 0,
                "pending_revision": int(session.get("revision", 0) or 0),
                "error": f"后台Excel同步排队失败: {exc}",
                "updated_at": "",
            }
            container.add_system_log(
                f"[交接班][审核SQLite] 后台Excel同步排队失败，已保留审核文档保存结果: "
                f"building={building}, session_id={session_id}, error={exc}"
            )
        session = dict(session)
        session["excel_sync"] = excel_sync if isinstance(excel_sync, dict) else document_state.attach_excel_sync(session).get("excel_sync", {})

    total_elapsed_ms = int((time.perf_counter() - save_started) * 1000)
    if is_latest_session:
        container.add_system_log(
            f"[交接班][审核保存] building={building}, session_id={session_id}, revision={session.get('revision', '-')}, "
            f"SQLite保存耗时={write_elapsed_ms}ms, 默认值耗时={defaults_elapsed_ms}ms, "
            f"容量补写耗时={capacity_elapsed_ms}ms, 状态更新耗时={session_elapsed_ms}ms, 总耗时={total_elapsed_ms}ms"
        )
        if isinstance(persisted_defaults, dict) and persisted_defaults.get("defaults_updated"):
            container.add_system_log(
                f"[交接班][审核模板默认] 已写入楼栋SQLite默认值: building={building}, "
                f"cabinet_power_fields={persisted_defaults.get('cabinet_power_fields', 0)}, "
                f"footer_inventory_rows={persisted_defaults.get('footer_inventory_rows', 0)}"
            )
        else:
            container.add_system_log(
                f"[交接班][审核模板默认] 楼栋SQLite默认值无变化，已跳过写入: building={building}, "
                f"cabinet_power_fields={persisted_defaults.get('cabinet_power_fields', 0) if isinstance(persisted_defaults, dict) else 0}, "
                f"footer_inventory_rows={persisted_defaults.get('footer_inventory_rows', 0) if isinstance(persisted_defaults, dict) else 0}"
            )
        if isinstance(persisted_defaults, dict) and persisted_defaults.get("config_updated"):
            aggregate_refresh_error = str(persisted_defaults.get("aggregate_refresh_error", "") or "").strip()
            if aggregate_refresh_error:
                container.add_system_log(
                    f"[交接班][审核模板默认] 已回写楼栋分段默认值，但聚合配置刷新失败: "
                    f"building={building}, error={aggregate_refresh_error}"
                )
            else:
                container.add_system_log(
                    f"[交接班][审核模板默认] 已回写楼栋分段默认值: building={building}"
                )
        elif isinstance(persisted_defaults, dict) and str(persisted_defaults.get("config_error", "") or "").strip():
            container.add_system_log(
                f"[交接班][审核模板默认] 楼栋分段默认值回写失败，已保留SQLite默认值: "
                f"building={building}, error={persisted_defaults.get('config_error', '')}"
            )
    else:
        container.add_system_log(
            f"[交接班][历史模式保存] building={building}, session_id={session_id}, revision={session.get('revision', '-')}, "
            f"SQLite保存耗时={write_elapsed_ms}ms, 容量补写耗时={capacity_elapsed_ms}ms, "
            f"状态更新耗时={session_elapsed_ms}ms, 总耗时={total_elapsed_ms}ms"
        )
        container.add_system_log(
            f"[交接班][审核模板默认] 已跳过历史模式默认值更新: building={building}, session_id={session_id}"
        )
    return {
        "ok": True,
        "session": session,
        "revision": int(session.get("revision", 0) or 0),
        "updated_at": str(session.get("updated_at", "")).strip(),
        "output_file": str(session.get("output_file", "")).strip(),
        "concurrency": _get_session_concurrency_safe(
            service,
            building=building,
            session_id=str(session.get("session_id", "")).strip(),
            client_id=str(payload.get("client_id", "")).strip(),
            current_revision=int(session.get("revision", 0) or 0),
            emit_log=container.add_system_log,
        ),
        "batch_status": batch_status,
        "save_profile": {
            "write_ms": int(write_elapsed_ms or 0),
            "sqlite_save_ms": int(write_elapsed_ms or 0),
            "defaults_ms": int(defaults_elapsed_ms or 0),
            "capacity_sync_ms": int(capacity_elapsed_ms or 0),
            "session_ms": int(session_elapsed_ms or 0),
            "queued_excel_sync": bool(queued_excel_sync),
            "excel_sync_status": str(session.get("excel_sync", {}).get("status", "") if isinstance(session.get("excel_sync", {}), dict) else ""),
            "total_ms": int(total_elapsed_ms or 0),
        },
        "history": _build_history_payload_safe(
            service,
            building=building,
            selected_session_id=str(session.get("session_id", "")).strip(),
            emit_log=container.add_system_log,
        ),
    }


@router.post("/api/handover/review/{building_code}/confirm")
def handover_review_confirm(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, _, _, followup = _build_review_services(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "")).strip()
    base_revision = _parse_base_revision_or_400(payload)
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    _load_target_session_or_404(service, building=building, session_id=session_id)
    _ensure_latest_session_actionable_or_400(service, building=building, session_id=session_id)
    target_session = _load_target_session_or_404(service, building=building, session_id=session_id)
    target_batch_key = str(target_session.get("batch_key", "")).strip()
    with container.job_service.resource_guard(
        name=f"handover_confirm:{target_batch_key}:{building}",
        resource_keys=_handover_resource_keys(batch_key=target_batch_key),
    ):
        try:
            session, batch_status = service.mark_confirmed(
                building=building,
                session_id=session_id,
                confirmed=True,
                base_revision=base_revision,
            )
        except ReviewSessionConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ReviewSessionNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ReviewSessionStoreUnavailableError as exc:
            _raise_review_store_http_error(exc)

        container.add_system_log(
            f"[交接班][审核确认] building={building}, batch={session.get('batch_key', '-')}"
        )
        target_batch_key = str(session.get("batch_key", "")).strip()
        try:
            followup_result = followup.trigger_after_single_confirm(
                batch_key=target_batch_key,
                building=building,
                session_id=str(session.get("session_id", "")).strip(),
                emit_log=container.add_system_log,
            )
        except Exception as exc:  # noqa: BLE001
            container.add_system_log(f"[交接班][确认后上传] 失败 batch={target_batch_key}, 错误={exc}")
            followup_result = _build_followup_failure_result(
                followup,
                batch_key=target_batch_key,
                error=str(exc),
            )
        container.add_system_log(
            f"[交接班][确认后上传] batch={session.get('batch_key', '-')}, 状态={_followup_status_text(followup_result.get('status'))}, "
            f"已上传={len(followup_result.get('uploaded_buildings', []))}, 已失败={len(followup_result.get('failed_buildings', []))}, "
            f"云表状态={_followup_status_text(followup_result.get('cloud_sheet_sync', {}).get('status', '-'))}"
        )
        latest_session = service.get_session_by_id(session_id) or session
        latest_batch_status = _attach_followup_progress(followup, service.get_batch_status(target_batch_key))
        return {
            "ok": True,
            "session": latest_session,
            "concurrency": _get_session_concurrency_safe(
                service,
                building=building,
                session_id=str(latest_session.get("session_id", "")).strip(),
                client_id=str(payload.get("client_id", "")).strip(),
                current_revision=int(latest_session.get("revision", 0) or 0),
                emit_log=container.add_system_log,
            ),
            "batch_status": latest_batch_status or _attach_followup_progress(followup, batch_status),
            "followup_result": followup_result,
            "history": _build_history_payload_safe(
                service,
                building=building,
                selected_session_id=str(latest_session.get("session_id", "")).strip(),
                emit_log=container.add_system_log,
            ),
        }


@router.post("/api/handover/review/{building_code}/unconfirm")
def handover_review_unconfirm(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, _, _, _ = _build_review_services(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "")).strip()
    base_revision = _parse_base_revision_or_400(payload)
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    _load_target_session_or_404(service, building=building, session_id=session_id)
    _ensure_latest_session_actionable_or_400(service, building=building, session_id=session_id)
    target_session = _load_target_session_or_404(service, building=building, session_id=session_id)
    target_batch_key = str(target_session.get("batch_key", "")).strip()
    with container.job_service.resource_guard(
        name=f"handover_unconfirm:{target_batch_key}:{building}",
        resource_keys=_handover_resource_keys(batch_key=target_batch_key),
    ):
        try:
            session, batch_status = service.mark_confirmed(
                building=building,
                session_id=session_id,
                confirmed=False,
                base_revision=base_revision,
            )
        except ReviewSessionConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ReviewSessionNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ReviewSessionStoreUnavailableError as exc:
            _raise_review_store_http_error(exc)
        container.add_system_log(
            f"[交接班][审核撤销] building={building}, batch={session.get('batch_key', '-')}"
        )
        return {
            "ok": True,
            "session": session,
            "concurrency": _get_session_concurrency_safe(
                service,
                building=building,
                session_id=str(session.get("session_id", "")).strip(),
                client_id=str(payload.get("client_id", "")).strip(),
                current_revision=int(session.get("revision", 0) or 0),
                emit_log=container.add_system_log,
            ),
            "batch_status": batch_status,
            "history": _build_history_payload_safe(
                service,
                building=building,
                selected_session_id=str(session.get("session_id", "")).strip(),
                emit_log=container.add_system_log,
            ),
        }


@router.post("/api/handover/review/{building_code}/lock/claim")
def handover_review_lock_claim(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, _, _, _ = _build_review_services(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "")).strip()
    client_id = str(payload.get("client_id", "")).strip()
    holder_label = str(payload.get("holder_label", "")).strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id 不能为空")
    _load_target_session_or_404(service, building=building, session_id=session_id)
    try:
        concurrency = service.claim_session_lock(
            building=building,
            session_id=session_id,
            client_id=client_id,
            holder_label=holder_label,
        )
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    return {"ok": True, "concurrency": concurrency, "accepted": bool(concurrency.get("acquired", False))}


@router.post("/api/handover/review/{building_code}/lock/heartbeat")
def handover_review_lock_heartbeat(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, _, _, _ = _build_review_services(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "")).strip()
    client_id = str(payload.get("client_id", "")).strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id 不能为空")
    _load_target_session_or_404(service, building=building, session_id=session_id)
    try:
        concurrency = service.heartbeat_session_lock(
            building=building,
            session_id=session_id,
            client_id=client_id,
        )
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    return {"ok": True, "concurrency": concurrency, "accepted": bool(concurrency.get("renewed", False))}


@router.post("/api/handover/review/{building_code}/lock/release")
def handover_review_lock_release(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, _, _, _ = _build_review_services(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "")).strip()
    client_id = str(payload.get("client_id", "")).strip()
    if not session_id or not client_id:
        return {"ok": True, "concurrency": {"current_revision": 0, "active_editor": None, "lease_expires_at": "", "is_editing_elsewhere": False, "client_holds_lock": False}, "released": False}
    try:
        concurrency = service.release_session_lock(
            building=building,
            session_id=session_id,
            client_id=client_id,
        )
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    return {"ok": True, "concurrency": concurrency, "released": bool(concurrency.get("released", False))}


@router.post("/api/handover/review/{building_code}/cloud-sync/retry")
def handover_review_retry_cloud_sync(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, _, _, _ = _build_review_services(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "")).strip() if isinstance(payload, dict) else ""
    target = _load_target_session_or_404(service, building=building, session_id=session_id) if session_id else _load_latest_session_or_404(service, building)
    _ensure_latest_session_actionable_or_400(
        service,
        building=building,
        session_id=str(target.get("session_id", "")).strip(),
    )
    target_batch_key = str(target.get("batch_key", "")).strip()

    def _run(emit_log) -> Dict[str, Any]:
        service, _, _, followup = _build_review_services(container)
        if session_id:
            result = followup.retry_cloud_sheet_for_session(session_id, emit_log=emit_log)
        else:
            result = followup.retry_cloud_sheet_for_building(building, emit_log=emit_log)
        session = result.get("session") if isinstance(result.get("session"), dict) else target
        batch_status = (
            result.get("batch_status")
            if isinstance(result.get("batch_status"), dict)
            else service.get_batch_status(str(session.get("batch_key", "")).strip())
        )
        emit_log(
            f"[交接班][云表重试] building={building}, batch={session.get('batch_key', '-')}, 状态={_followup_status_text(result.get('status'))}"
        )
        return {
            "ok": str(result.get("status", "")).strip().lower() in {"ok", "success"},
            "session": session,
            "batch_status": batch_status,
            "cloud_sheet_sync": result.get("cloud_sheet_sync", {}),
            "status": result.get("status", ""),
            "history": _build_history_payload_safe(
                service,
                building=building,
                selected_session_id=str(session.get("session_id", "")).strip(),
                emit_log=emit_log,
            ),
        }

    job = _start_handover_background_job(
        container,
        name=f"交接班云表重试-{building}-{target_batch_key}",
        run_func=_run,
        worker_handler="handover_cloud_retry_single",
        worker_payload={
            "building": building,
            "session_id": session_id or "",
            "batch_key": target_batch_key,
        },
        resource_keys=_handover_resource_keys("network:external", batch_key=target_batch_key),
        priority="manual",
        feature="handover_cloud_retry_single",
        submitted_by="manual",
    )
    container.add_system_log(
        f"[任务] 已提交: 交接班云表重试 building={building} batch={target_batch_key} ({job.job_id})"
    )
    return _accepted_job_response(job)


@router.post("/api/handover/review/{building_code}/cloud-sync/update")
def handover_review_update_cloud_sync(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, _, _, followup = _build_review_services(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "")).strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    target = _load_target_session_or_404(service, building=building, session_id=session_id)
    if str(target.get("building", "")).strip() != building:
        raise HTTPException(status_code=400, detail="session building mismatch")
    target_batch_key = str(target.get("batch_key", "")).strip()
    with container.job_service.resource_guard(
        name=f"handover_cloud_update:{target_batch_key}:{building}",
        resource_keys=_handover_resource_keys("network:external", batch_key=target_batch_key),
    ):
        result = followup.force_update_cloud_sheet_for_session(session_id, emit_log=container.add_system_log)
        refreshed_session = result.get("session") if isinstance(result.get("session"), dict) else target
        batch_status = (
            result.get("batch_status")
            if isinstance(result.get("batch_status"), dict)
            else service.get_batch_status(str(refreshed_session.get("batch_key", "")).strip())
        )
        return {
            "ok": str(result.get("status", "")).strip().lower() in {"ok", "success"},
            "status": result.get("status", ""),
            "session": refreshed_session,
            "batch_status": batch_status,
            "cloud_sheet_sync": result.get("cloud_sheet_sync", {}),
            "history": _build_history_payload_safe(
                service,
                building=building,
                selected_session_id=str(refreshed_session.get("session_id", "")).strip(),
                emit_log=container.add_system_log,
            ),
        }


@router.post("/api/handover/review/batch/{batch_key}/cloud-sync/retry")
def handover_review_retry_cloud_sync_batch(
    batch_key: str,
    request: Request,
) -> Dict[str, Any]:
    container = request.app.state.container

    def _run(emit_log) -> Dict[str, Any]:
        service, _, _, followup = _build_review_services(container)
        result = followup.retry_failed_cloud_sheet_in_batch(batch_key, emit_log=emit_log)
        emit_log(
            f"[交接班][云表批量重试] batch={batch_key}, 状态={_followup_status_text(result.get('status'))}"
        )
        batch_status = (
            result.get("batch_status")
            if isinstance(result.get("batch_status"), dict)
            else service.get_batch_status(batch_key)
        )
        updated_sessions = (
            result.get("updated_sessions")
            if isinstance(result.get("updated_sessions"), list)
            else service.list_batch_sessions(batch_key)
        )
        status = str(result.get("status", "")).strip().lower()
        return {
            "ok": status != "blocked",
            "status": result.get("status", ""),
            "batch_key": batch_key,
            "batch_status": batch_status,
            "updated_sessions": updated_sessions,
            "cloud_sheet_sync": result.get("cloud_sheet_sync", {}),
        }

    job = _start_handover_background_job(
        container,
        name=f"交接班云表批量重试-{batch_key}",
        run_func=_run,
        worker_handler="handover_cloud_retry_batch",
        worker_payload={"batch_key": batch_key},
        resource_keys=_handover_resource_keys("network:external", batch_key=batch_key),
        priority="manual",
        feature="handover_cloud_retry_batch",
        submitted_by="manual",
    )
    container.add_system_log(f"[任务] 已提交: 交接班云表批量重试 batch={batch_key} ({job.job_id})")
    return _accepted_job_response(job)
