from __future__ import annotations

import asyncio
import contextlib
import copy
import inspect
import threading
import time
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, BackgroundTasks, Body, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse

from app.config.handover_segment_store import building_code_from_name, handover_building_segment_path
from app.config.settings_loader import (
    get_handover_building_segment,
    load_settings,
    save_handover_building_segment,
    save_settings,
)
from app.shared.utils.frontend_cache import render_frontend_index_html, source_frontend_no_cache_headers
from handover_log_module.api.facade import load_handover_config
from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore
from handover_log_module.service.cabinet_power_defaults_service import CabinetPowerDefaultsService
from handover_log_module.service.cooling_pump_pressure_defaults_service import CoolingPumpPressureDefaultsService
from handover_log_module.service.footer_inventory_defaults_service import FooterInventoryDefaultsService
from handover_log_module.service.handover_daily_report_asset_service import HandoverDailyReportAssetService
from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService
from handover_log_module.service.handover_daily_report_screenshot_service import (
    HandoverDailyReportScreenshotService,
)
from handover_log_module.service.handover_daily_report_state_service import HandoverDailyReportStateService
from handover_log_module.service.handover_xlsx_write_queue_service import (
    HandoverXlsxWriteQueueService,
    HandoverXlsxWriteQueueTimeoutError,
)
from handover_log_module.service.capacity_report_image_delivery_service import (
    CapacityReportImageDeliveryService,
)
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
_REVIEW_DEFAULT_CONFIG_LOCK_GUARD = threading.Lock()
_REVIEW_DEFAULT_CONFIG_LOCKS: dict[str, threading.RLock] = {}
_REVIEW_DOCUMENT_CACHE_GUARD = threading.Lock()
_REVIEW_DOCUMENT_CACHE: dict[str, dict[str, Any]] = {}
_REVIEW_DOCUMENT_WARMUPS_INFLIGHT: set[str] = set()
_REVIEW_BOOTSTRAP_CACHE: dict[str, dict[str, Any]] = {}
_REVIEW_HISTORY_CACHE: dict[str, dict[str, Any]] = {}
_REVIEW_HISTORY_CACHE_TTL_SEC = 15.0
_SUBSTATION_110KV_COMPARE_KEYS = ("line_voltage", "current", "power_kw", "power_factor", "load_rate")


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


def _self_held_concurrency(
    concurrency: Dict[str, Any] | None,
    *,
    current_revision: int,
) -> Dict[str, Any]:
    payload = dict(concurrency or {}) if isinstance(concurrency, dict) else {}
    payload["current_revision"] = int(current_revision or 0)
    payload["is_editing_elsewhere"] = False
    payload["client_holds_lock"] = True
    if not isinstance(payload.get("active_editor"), dict):
        payload["active_editor"] = None
    payload["lease_expires_at"] = str(payload.get("lease_expires_at", "") or "").strip()
    return payload


def _review_default_config_lock(building_code: str) -> threading.RLock:
    key = str(building_code or "").strip().lower() or "unknown"
    with _REVIEW_DEFAULT_CONFIG_LOCK_GUARD:
        lock = _REVIEW_DEFAULT_CONFIG_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _REVIEW_DEFAULT_CONFIG_LOCKS[key] = lock
    return lock


def _review_document_cache_key(*, building: str, session_id: str) -> str:
    return f"{str(building or '').strip()}|{str(session_id or '').strip()}"


def _review_document_signature(
    session: Dict[str, Any],
    *,
    revision_override: int | None = None,
) -> Dict[str, Any]:
    output_file = str(session.get("output_file", "") or "").strip()
    source_excel_mtime = ""
    source_excel_size = 0
    if output_file:
        try:
            output_path = Path(output_file)
            if output_path.exists() and output_path.is_file():
                stat = output_path.stat()
                source_excel_mtime = str(
                    getattr(stat, "st_mtime_ns", None) or int(getattr(stat, "st_mtime", 0) or 0)
                )
                source_excel_size = int(getattr(stat, "st_size", 0) or 0)
        except Exception:  # noqa: BLE001
            source_excel_mtime = ""
            source_excel_size = 0
    return {
        "session_id": str(session.get("session_id", "") or "").strip(),
        "revision": int(
            revision_override
            if revision_override is not None
            else int(session.get("revision", 0) or 0)
        ),
        "output_file": output_file,
        "source_excel_mtime": source_excel_mtime,
        "source_excel_size": source_excel_size,
    }


def _review_document_cache_get(
    *,
    building: str,
    signature: Dict[str, Any],
) -> Dict[str, Any] | None:
    key = _review_document_cache_key(
        building=building,
        session_id=str(signature.get("session_id", "")).strip(),
    )
    with _REVIEW_DOCUMENT_CACHE_GUARD:
        cached = _REVIEW_DOCUMENT_CACHE.get(key)
        if not isinstance(cached, dict):
            return None
        cached_signature = cached.get("signature", {})
        if not isinstance(cached_signature, dict):
            return None
        if cached_signature != signature:
            return None
        document = cached.get("document", {})
    return copy.deepcopy(document if isinstance(document, dict) else {})


def _review_document_cache_put(
    *,
    building: str,
    signature: Dict[str, Any],
    document: Dict[str, Any],
) -> None:
    key = _review_document_cache_key(
        building=building,
        session_id=str(signature.get("session_id", "")).strip(),
    )
    payload = {
        "signature": dict(signature if isinstance(signature, dict) else {}),
        "document": copy.deepcopy(document if isinstance(document, dict) else {}),
        "updated_at": time.time(),
    }
    with _REVIEW_DOCUMENT_CACHE_GUARD:
        _REVIEW_DOCUMENT_CACHE[key] = payload


def _review_bootstrap_signature(
    session: Dict[str, Any],
    *,
    revision_override: int | None = None,
) -> Dict[str, Any]:
    signature = _review_document_signature(session, revision_override=revision_override)
    signature["updated_at"] = str(session.get("updated_at", "") or "").strip()
    return signature


def _review_bootstrap_cache_get(
    *,
    building: str,
    signature: Dict[str, Any],
) -> Dict[str, Any] | None:
    key = _review_document_cache_key(
        building=building,
        session_id=str(signature.get("session_id", "")).strip(),
    )
    with _REVIEW_DOCUMENT_CACHE_GUARD:
        cached = _REVIEW_BOOTSTRAP_CACHE.get(key)
        if not isinstance(cached, dict):
            return None
        cached_signature = cached.get("signature", {})
        if not isinstance(cached_signature, dict) or cached_signature != signature:
            return None
        payload = cached.get("payload", {})
    return copy.deepcopy(payload if isinstance(payload, dict) else {})


def _review_bootstrap_cache_put(
    *,
    building: str,
    signature: Dict[str, Any],
    payload: Dict[str, Any],
) -> None:
    key = _review_document_cache_key(
        building=building,
        session_id=str(signature.get("session_id", "")).strip(),
    )
    with _REVIEW_DOCUMENT_CACHE_GUARD:
        _REVIEW_BOOTSTRAP_CACHE[key] = {
            "signature": dict(signature if isinstance(signature, dict) else {}),
            "payload": copy.deepcopy(payload if isinstance(payload, dict) else {}),
            "updated_at": time.time(),
        }


def _review_history_cache_key(*, building: str, selected_session_id: str) -> str:
    return f"{str(building or '').strip()}|{str(selected_session_id or '').strip()}"


def _review_history_cache_get(
    *,
    building: str,
    selected_session_id: str,
) -> Dict[str, Any] | None:
    key = _review_history_cache_key(building=building, selected_session_id=selected_session_id)
    with _REVIEW_DOCUMENT_CACHE_GUARD:
        cached = _REVIEW_HISTORY_CACHE.get(key)
        if not isinstance(cached, dict):
            return None
        updated_at = float(cached.get("updated_at", 0.0) or 0.0)
        if updated_at <= 0 or (time.time() - updated_at) > _REVIEW_HISTORY_CACHE_TTL_SEC:
            _REVIEW_HISTORY_CACHE.pop(key, None)
            return None
        payload = cached.get("payload", {})
    return copy.deepcopy(payload if isinstance(payload, dict) else {})


def _review_history_cache_put(
    *,
    building: str,
    selected_session_id: str,
    payload: Dict[str, Any],
) -> None:
    key = _review_history_cache_key(building=building, selected_session_id=selected_session_id)
    with _REVIEW_DOCUMENT_CACHE_GUARD:
        _REVIEW_HISTORY_CACHE[key] = {
            "payload": copy.deepcopy(payload if isinstance(payload, dict) else {}),
            "updated_at": time.time(),
        }


def _review_history_cache_invalidate(*, building: str) -> None:
    prefix = f"{str(building or '').strip()}|"
    with _REVIEW_DOCUMENT_CACHE_GUARD:
        for key in [item for item in _REVIEW_HISTORY_CACHE.keys() if item.startswith(prefix)]:
            _REVIEW_HISTORY_CACHE.pop(key, None)


def _review_history_cache_invalidate_sessions(sessions: Any) -> None:
    if not isinstance(sessions, list):
        return
    buildings = {
        str(item.get("building", "")).strip()
        for item in sessions
        if isinstance(item, dict) and str(item.get("building", "")).strip()
    }
    for building in buildings:
        _review_history_cache_invalidate(building=building)


def _load_review_document_cached(
    document_state,
    session: Dict[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    if not isinstance(document_state, ReviewDocumentStateService):
        return document_state.load_document(session)
    building = str(session.get("building", "") or "").strip()
    signature = _review_document_signature(session)
    cached_document = _review_document_cache_get(building=building, signature=signature)
    if isinstance(cached_document, dict):
        session_with_sync = document_state.attach_excel_sync(dict(session))
        session_with_sync["revision"] = int(signature.get("revision", 0) or 0)
        attach_pump = getattr(document_state, "attach_cooling_pump_pressures", None)
        if callable(attach_pump):
            try:
                cached_document = attach_pump(document=cached_document, session=session_with_sync)
            except Exception:  # noqa: BLE001
                pass
        return cached_document, session_with_sync
    document, session_with_sync = document_state.load_document(session)
    resolved_signature = _review_document_signature(
        session_with_sync if isinstance(session_with_sync, dict) else session,
        revision_override=int(
            (session_with_sync or {}).get("revision", session.get("revision", 0))
            if isinstance(session_with_sync, dict)
            else int(session.get("revision", 0) or 0)
        ),
    )
    _review_document_cache_put(
        building=building,
        signature=resolved_signature,
        document=document if isinstance(document, dict) else {},
    )
    return document, session_with_sync


def _review_ui_payload(parser: ReviewDocumentParser | None) -> Dict[str, Any]:
    parser_config = getattr(parser, "config", {}) if parser is not None else {}
    review_ui = parser_config.get("review_ui", {}) if isinstance(parser_config, dict) else {}
    return review_ui if isinstance(review_ui, dict) else {}


def _build_review_bootstrap_payload(
    *,
    building: str,
    parser: ReviewDocumentParser,
    document_state: ReviewDocumentStateService,
    session: Dict[str, Any],
) -> tuple[Dict[str, Any], bool]:
    signature = _review_bootstrap_signature(session)
    cached = _review_bootstrap_cache_get(building=building, signature=signature)
    if isinstance(cached, dict) and bool(cached):
        attach_pump = getattr(document_state, "attach_cooling_pump_pressures", None)
        if callable(attach_pump) and isinstance(cached.get("document", {}), dict):
            try:
                cached["document"] = attach_pump(
                    document=cached.get("document", {}),
                    session=cached.get("session", session) if isinstance(cached.get("session", {}), dict) else session,
                )
            except Exception:  # noqa: BLE001
                pass
        return cached, True
    document, session_with_sync = _load_review_document_cached(document_state, session)
    resolved_signature = _review_bootstrap_signature(
        session_with_sync if isinstance(session_with_sync, dict) else session,
        revision_override=int(
            (session_with_sync or {}).get("revision", session.get("revision", 0))
            if isinstance(session_with_sync, dict)
            else int(session.get("revision", 0) or 0)
        ),
    )
    payload = {
        "ok": True,
        "building": building,
        "session": session_with_sync if isinstance(session_with_sync, dict) else dict(session),
        "document": document if isinstance(document, dict) else {},
        "review_ui": _review_ui_payload(parser),
        "prepared_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "snapshot_revision": int(resolved_signature.get("revision", 0) or 0),
    }
    _review_bootstrap_cache_put(
        building=building,
        signature=resolved_signature,
        payload=payload,
    )
    return payload, False


def warm_latest_review_documents(
    container,
    *,
    preferred_building: str = "",
    reason: str = "",
    target_only: bool = False,
) -> None:
    service, parser, writer, _ = _build_review_services(container)
    document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
    preferred = str(preferred_building or "").strip()
    list_buildings = getattr(service, "list_buildings", None)
    if target_only and preferred:
        buildings = [preferred]
    elif callable(list_buildings):
        buildings = list_buildings()
    elif preferred:
        buildings = [preferred]
    else:
        buildings = []
    if preferred and preferred in buildings:
        buildings = [preferred, *[item for item in buildings if item != preferred]]
    warmed = 0
    for building in buildings:
        try:
            latest_session_getter = getattr(service, "get_latest_session_fast", None)
            if callable(latest_session_getter):
                session = latest_session_getter(building)
            else:
                session = service.get_latest_session(building)
        except ReviewSessionStoreUnavailableError:
            continue
        if not isinstance(session, dict):
            continue
        try:
            _payload, from_cache = _build_review_bootstrap_payload(
                building=building,
                parser=parser,
                document_state=document_state,
                session=session,
            )
            if not from_cache:
                warmed += 1
        except ReviewDocumentStateError as exc:
            container.add_system_log(
                f"[交接班][审核预热] 最新审核首屏快照预热失败: building={building}, error={exc}"
            )
        except Exception as exc:  # noqa: BLE001
            container.add_system_log(
                f"[交接班][审核预热] 最新审核首屏快照预热异常: building={building}, error={exc}"
            )
    if warmed > 0:
        container.add_system_log(
            f"[交接班][审核预热] 最新审核首屏快照预热完成: refreshed={warmed}, "
            f"preferred={preferred or '-'}, reason={str(reason or '').strip() or '-'}"
        )


def schedule_latest_review_documents_warmup(
    container,
    *,
    preferred_building: str = "",
    reason: str = "",
    target_only: bool = False,
) -> None:
    warmup_key = "latest_review_documents"
    with _REVIEW_DOCUMENT_CACHE_GUARD:
        if warmup_key in _REVIEW_DOCUMENT_WARMUPS_INFLIGHT:
            return
        _REVIEW_DOCUMENT_WARMUPS_INFLIGHT.add(warmup_key)

    def _runner() -> None:
        try:
            try:
                warm_latest_review_documents(
                    container,
                    preferred_building=preferred_building,
                    reason=reason,
                    target_only=target_only,
                )
            except Exception as exc:  # noqa: BLE001
                container.add_system_log(f"[交接班][审核预热] 后台预热线程执行失败: {exc}")
        finally:
            with _REVIEW_DOCUMENT_CACHE_GUARD:
                _REVIEW_DOCUMENT_WARMUPS_INFLIGHT.discard(warmup_key)

    threading.Thread(
        target=_runner,
        name="handover-review-latest-warmup",
        daemon=True,
    ).start()


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
    except ReviewSessionNotFoundError:
        return _empty_concurrency(current_revision=current_revision)


def _ensure_session_lock_held_or_409(
    service: ReviewSessionService,
    *,
    building: str,
    session_id: str,
    client_id: str,
) -> Dict[str, Any]:
    client_id_text = str(client_id or "").strip()
    if not client_id_text:
        raise HTTPException(status_code=400, detail="client_id 不能为空")
    try:
        concurrency = service.get_session_concurrency(
            building=building,
            session_id=session_id,
            client_id=client_id_text,
        )
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    except ReviewSessionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if not bool(concurrency.get("client_holds_lock", False)):
        raise HTTPException(status_code=409, detail="当前审核页正在其他终端编辑，请等待或刷新后重试")
    return concurrency


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


_ORIGINAL_BUILD_REVIEW_SERVICES = _build_review_services


def _build_review_session_service(container) -> ReviewSessionService:
    if _build_review_services is not _ORIGINAL_BUILD_REVIEW_SERVICES:
        service, _, _, _ = _build_review_services(container)
        return service
    return ReviewSessionService(_handover_cfg(container))


def _build_review_followup_service(container) -> ReviewFollowupTriggerService:
    if _build_review_services is not _ORIGINAL_BUILD_REVIEW_SERVICES:
        _, _, _, followup = _build_review_services(container)
        return followup
    return ReviewFollowupTriggerService(_handover_cfg(container))


def _build_review_ui_config(container) -> Dict[str, Any]:
    if _build_review_services is not _ORIGINAL_BUILD_REVIEW_SERVICES:
        _, parser, _, _ = _build_review_services(container)
        parser_config = getattr(parser, "config", {}) if parser is not None else {}
        review_ui = parser_config.get("review_ui", {}) if isinstance(parser_config, dict) else {}
        return review_ui if isinstance(review_ui, dict) else {}
    handover_cfg = _handover_cfg(container)
    review_ui = handover_cfg.get("review_ui", {}) if isinstance(handover_cfg, dict) else {}
    return review_ui if isinstance(review_ui, dict) else {}


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


def _build_xlsx_write_queue_service(
    container,
    *,
    parser: ReviewDocumentParser | None = None,
    writer: ReviewDocumentWriter | None = None,
) -> HandoverXlsxWriteQueueService:
    return HandoverXlsxWriteQueueService(
        _handover_cfg(container),
        emit_log=getattr(container, "add_system_log", print),
        job_service=getattr(container, "job_service", None),
        parser=parser,
        writer=writer,
    )


def _resource_guard_or_null(container, *, name: str, resource_keys: list[str]):
    job_service = getattr(container, "job_service", None)
    guard = getattr(job_service, "resource_guard", None)
    if callable(guard):
        return guard(name=name, resource_keys=resource_keys)
    return contextlib.nullcontext()


def _attach_excel_sync_safe(document_state, session: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = dict(session or {})
    attach = getattr(document_state, "attach_excel_sync", None)
    if callable(attach):
        try:
            result = attach(payload)
            if isinstance(result, dict):
                return result
        except Exception:  # noqa: BLE001
            return payload
    return payload


def _attach_excel_sync_from_store(container, session: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = dict(session or {})
    if _build_review_services is not _ORIGINAL_BUILD_REVIEW_SERVICES:
        return _attach_excel_sync_safe(
            _build_review_document_state_service(container),
            payload,
        )
    building = str(payload.get("building", "") or "").strip()
    session_id = str(payload.get("session_id", "") or "").strip()
    if not building or not session_id:
        return payload
    try:
        sync_state = ReviewBuildingDocumentStore(
            config=_handover_cfg(container),
            building=building,
        ).get_sync_state(session_id)
    except Exception:  # noqa: BLE001
        return payload
    if not isinstance(sync_state, dict):
        return payload
    if str(sync_state.get("status", "")).strip().lower() == "unknown":
        revision = int(payload.get("revision", 0) or 0)
        sync_state = {
            "status": "unknown",
            "synced_revision": 0,
            "pending_revision": revision,
            "error": "",
            "updated_at": "",
        }
    payload["excel_sync"] = sync_state
    return payload


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


def _handover_resource_keys(*resource_keys: str, batch_key: str = "", building: str = "") -> list[str]:
    keys: list[str] = []
    for item in resource_keys:
        text = str(item or "").strip()
        if text and text not in keys:
            keys.append(text)
    building_text = str(building or "").strip()
    if building_text:
        resource_key = f"handover_building:{building_text}"
        if resource_key not in keys:
            keys.append(resource_key)
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
    load_settings(container.config_path)
    current_doc = get_handover_building_segment(building_code, container.config_path)
    current_data = current_doc.get("data", {}) if isinstance(current_doc.get("data", {}), dict) else {}
    updated_config = defaults_service.set_building_defaults(current_data, building, rows)
    saved_config, _document, aggregate_refresh_error = save_handover_building_segment(
        building_code,
        updated_config,
        base_revision=int(current_doc.get("revision", 0) or 0),
        config_path=container.config_path,
    )
    _apply_container_config_snapshot(container, saved_config, mode="light")
    if aggregate_refresh_error:
        container.add_system_log(
            f"[交接班][审核模板默认] 楼栋分段已保存，但聚合配置刷新失败: building={building}, error={aggregate_refresh_error}"
        )
    return len(rows)


def _apply_container_config_snapshot(container, saved_config: Dict[str, Any], *, mode: str = "light") -> None:
    apply_snapshot = getattr(container, "apply_config_snapshot", None)
    if callable(apply_snapshot):
        apply_snapshot(saved_config, mode=mode)
        return
    container.reload_config(saved_config)


def _normalize_review_dirty_regions(raw: Any) -> Dict[str, bool]:
    if not isinstance(raw, dict):
        return {
            "fixed_blocks": True,
            "sections": True,
            "footer_inventory": True,
            "cooling_pump_pressures": True,
        }
    return {
        "fixed_blocks": bool(raw.get("fixed_blocks")),
        "sections": bool(raw.get("sections")),
        "footer_inventory": bool(raw.get("footer_inventory")),
        "cooling_pump_pressures": bool(raw.get("cooling_pump_pressures")),
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
    shared_110kv: Dict[str, Any] | None = None,
    cooling_pump_pressures: Dict[str, Any] | None = None,
) -> bool:
    dirty = dirty_regions or {}
    if not bool(dirty.get("fixed_blocks")) and not bool(dirty.get("cooling_pump_pressures")):
        return False
    previous = previous_session if isinstance(previous_session, dict) else {}
    previous_sync = previous.get("capacity_sync", {}) if isinstance(previous.get("capacity_sync", {}), dict) else {}
    previous_signature = str(previous_sync.get("input_signature", "") or "").strip()
    next_signature = _capacity_input_signature_for_review(
        tracked_cells=tracked_cells,
        shared_110kv=shared_110kv,
        cooling_pump_pressures=cooling_pump_pressures,
    )
    if previous_signature != next_signature:
        return True
    status = str(previous_sync.get("status", "") or "").strip().lower()
    return status in {"pending_input", "missing_file", "failed"}


def _capacity_input_signature_for_review(
    *,
    tracked_cells: Dict[str, str],
    shared_110kv: Dict[str, Any] | None = None,
    cooling_pump_pressures: Dict[str, Any] | None = None,
) -> str:
    shared_signature = ""
    if isinstance(shared_110kv, dict):
        shared_payload = ReviewSessionService.normalize_substation_110kv_payload(shared_110kv)
        parts = []
        for row in shared_payload.get("rows", []):
            if not isinstance(row, dict):
                continue
            parts.append(
                "{row_id}:{line_voltage},{current},{power_kw},{power_factor},{load_rate}".format(
                    row_id=str(row.get("row_id", "") or "").strip(),
                    line_voltage=str(row.get("line_voltage", "") or "").strip(),
                    current=str(row.get("current", "") or "").strip(),
                    power_kw=str(row.get("power_kw", "") or "").strip(),
                    power_factor=str(row.get("power_factor", "") or "").strip(),
                    load_rate=str(row.get("load_rate", "") or "").strip(),
                )
            )
        shared_signature = f"rev={int(shared_payload.get('revision', 0) or 0)};" + "|".join(parts)
    cooling_signature = CoolingPumpPressureDefaultsService.signature(cooling_pump_pressures or {})
    return HandoverCapacityReportService.capacity_input_signature(
        tracked_cells,
        shared_110kv_signature=shared_signature,
        cooling_pump_signature=cooling_signature,
    )


def _build_pending_capacity_sync_payload(tracked_cells: Dict[str, str]) -> Dict[str, Any]:
    normalized_cells = {
        str(cell or "").strip().upper(): str(value or "").strip()
        for cell, value in (tracked_cells or {}).items()
        if str(cell or "").strip()
    }
    return {
        "status": "pending",
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "error": "",
        "tracked_cells": list(HandoverCapacityReportService.tracked_cells()),
        "input_signature": HandoverCapacityReportService.capacity_input_signature(normalized_cells),
    }


def _build_pending_capacity_sync_payload_for_review(
    *,
    tracked_cells: Dict[str, str],
    shared_110kv: Dict[str, Any] | None = None,
    cooling_pump_pressures: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload = _build_pending_capacity_sync_payload(tracked_cells)
    payload["input_signature"] = _capacity_input_signature_for_review(
        tracked_cells=tracked_cells,
        shared_110kv=shared_110kv,
        cooling_pump_pressures=cooling_pump_pressures,
    )
    return payload


def _queue_capacity_overlay_after_review_save(
    *,
    container,
    background_tasks: BackgroundTasks,
    review_service: ReviewSessionService,
    previous_session: Dict[str, Any],
    saved_session: Dict[str, Any],
    document: Dict[str, Any],
    dirty_regions: Dict[str, bool],
) -> tuple[Dict[str, Any], bool]:
    if not callable(getattr(review_service, "update_capacity_sync", None)):
        return saved_session, False
    tracked_cells = _extract_capacity_tracked_cells(document)
    shared_110kv = {}
    try:
        shared_110kv = review_service.get_substation_110kv(str(saved_session.get("batch_key", "")).strip())
    except Exception:
        shared_110kv = {}
    cooling_pump_pressures = (
        document.get("cooling_pump_pressures", {}) if isinstance(document.get("cooling_pump_pressures", {}), dict) else {}
    )
    if not _should_sync_capacity_after_review_save(
        previous_session=previous_session,
        dirty_regions=dirty_regions,
        tracked_cells=tracked_cells,
        shared_110kv=shared_110kv,
        cooling_pump_pressures=cooling_pump_pressures,
    ):
        return saved_session, False
    pending_payload = _build_pending_capacity_sync_payload_for_review(
        tracked_cells=tracked_cells,
        shared_110kv=shared_110kv,
        cooling_pump_pressures=cooling_pump_pressures,
    )
    updated_session = review_service.update_capacity_sync(
        session_id=str(saved_session.get("session_id", "")).strip(),
        capacity_sync=pending_payload,
        capacity_status="pending",
        capacity_error="",
    )
    del background_tasks
    _build_xlsx_write_queue_service(container).enqueue_capacity_overlay_sync(
        building=str(updated_session.get("building", "")).strip() or str(saved_session.get("building", "")).strip(),
        session_id=str(updated_session.get("session_id", "")).strip() or str(saved_session.get("session_id", "")).strip(),
        tracked_cells=copy.deepcopy(tracked_cells),
        shared_110kv=copy.deepcopy(shared_110kv),
        cooling_pump_pressures=copy.deepcopy(cooling_pump_pressures),
        capacity_output_file=str(updated_session.get("capacity_output_file", "") or saved_session.get("capacity_output_file", "") or "").strip(),
    )
    return updated_session, True


def _ensure_capacity_overlay_queue_drained_for_session(
    *,
    container,
    review_service: ReviewSessionService,
    building: str,
    session_id: str,
    timeout_sec: float = 120.0,
) -> Dict[str, Any]:
    target = _load_target_session_or_404(review_service, building=building, session_id=session_id)
    queue_service = _build_xlsx_write_queue_service(container)
    output_file_text = str(target.get("output_file", "") or "").strip()
    capacity_output_file_text = str(target.get("capacity_output_file", "") or "").strip()
    output_file_exists = False
    if output_file_text:
        try:
            output_file_exists = Path(output_file_text).exists()
        except Exception:
            output_file_exists = False
    if output_file_exists and capacity_output_file_text:
        service, parser, writer, _ = _build_review_services(container)
        del service
        document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
        document, session_with_sync = document_state.load_document(target)
        tracked_cells = _extract_capacity_tracked_cells(document)
        try:
            shared_110kv = review_service.get_substation_110kv(str(target.get("batch_key", "")).strip())
        except Exception:
            shared_110kv = {}
        cooling_pump_pressures = (
            document.get("cooling_pump_pressures", {})
            if isinstance(document.get("cooling_pump_pressures", {}), dict)
            else {}
        )
        next_signature = _capacity_input_signature_for_review(
            tracked_cells=tracked_cells,
            shared_110kv=shared_110kv,
            cooling_pump_pressures=cooling_pump_pressures,
        )
        capacity_sync = target.get("capacity_sync", {}) if isinstance(target.get("capacity_sync", {}), dict) else {}
        current_signature = str(capacity_sync.get("input_signature", "") or "").strip()
        current_status = str(capacity_sync.get("status", "") or "").strip().lower()
        queue_service = _build_xlsx_write_queue_service(container, parser=parser, writer=writer)
        if current_signature != next_signature or current_status in {"", "failed", "missing_file", "pending_input"}:
            pending_payload = _build_pending_capacity_sync_payload_for_review(
                tracked_cells=tracked_cells,
                shared_110kv=shared_110kv,
                cooling_pump_pressures=cooling_pump_pressures,
            )
            updated_session = review_service.update_capacity_sync(
                session_id=session_id,
                capacity_sync=pending_payload,
                capacity_status="pending",
                capacity_error="",
            )
            queue_service.enqueue_capacity_overlay_sync(
                building=building,
                session_id=session_id,
                tracked_cells=tracked_cells,
                shared_110kv=shared_110kv,
                cooling_pump_pressures=cooling_pump_pressures,
                capacity_output_file=str(updated_session.get("capacity_output_file", "") or target.get("capacity_output_file", "") or "").strip(),
            )
    queue_service.wait_for_barrier(building=building, timeout_sec=timeout_sec)
    return _load_target_session_or_404(review_service, building=building, session_id=session_id)


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
        "cooling_pump_pressure_fields": int(persisted.get("cooling_pump_pressure_fields", 0) or 0),
        "defaults_updated": bool(persisted.get("defaults_updated", False)),
        "config_updated": False,
        "aggregate_refresh_error": "",
        "config_sync_required": False,
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

    result["config_sync_required"] = True
    result["config_building_code"] = building_code
    result["config_data"] = updated_data
    return result


def _persist_review_defaults_config_async(
    container,
    *,
    building: str,
    building_code: str,
    updated_data: Dict[str, Any],
) -> None:
    lock = _review_default_config_lock(building_code)
    with lock:
        try:
            current_doc = get_handover_building_segment(building_code, container.config_path)
            current_data = (
                copy.deepcopy(current_doc.get("data", {}))
                if isinstance(current_doc.get("data", {}), dict)
                else {}
            )
            if updated_data == current_data:
                container.add_system_log(
                    f"[交接班][审核模板默认] 楼栋分段默认值无变化，已跳过后台回写: building={building}"
                )
                return
            saved_config, _document, aggregate_refresh_error = save_handover_building_segment(
                building_code,
                updated_data,
                base_revision=int(current_doc.get("revision", 0) or 0),
                config_path=container.config_path,
            )
            _apply_container_config_snapshot(container, saved_config, mode="light")
            aggregate_refresh_error_text = str(aggregate_refresh_error or "").strip()
            if aggregate_refresh_error_text:
                container.add_system_log(
                    f"[交接班][审核模板默认] 楼栋分段默认值后台回写成功，但聚合配置刷新失败: "
                    f"building={building}, error={aggregate_refresh_error_text}"
                )
            else:
                container.add_system_log(
                    f"[交接班][审核模板默认] 楼栋分段默认值后台回写成功: building={building}"
                )
        except Exception as exc:  # noqa: BLE001
            container.add_system_log(
                f"[交接班][审核模板默认] 楼栋分段默认值后台回写失败: building={building}, error={exc}"
            )


def _resolve_building_or_404(service: ReviewSessionService, building_code: str) -> str:
    building = service.get_building_by_code(building_code)
    if not building:
        raise HTTPException(status_code=404, detail="未知楼栋页面")
    return building


def _load_latest_session_or_404(service: ReviewSessionService, building: str) -> Dict[str, Any]:
    try:
        latest_session_id = _safe_latest_session_id(service, building=building)
        if latest_session_id:
            session = service.get_session_by_id(latest_session_id)
        else:
            latest_session_getter = getattr(service, "get_latest_session_fast", None)
            if callable(latest_session_getter):
                session = latest_session_getter(building)
            else:
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
            recover_session = getattr(service, "get_or_recover_session_by_id", None)
            if callable(recover_session):
                session = recover_session(session_id_text)
            else:
                session = service.get_session_by_id(session_id_text)
        else:
            duty_date_text, duty_shift_text = _normalize_duty_context(duty_date, duty_shift)
            if duty_date_text and duty_shift_text:
                session_getter = getattr(service, "get_session_for_building_duty_fast", None)
                if callable(session_getter):
                    session = session_getter(building, duty_date_text, duty_shift_text)
                else:
                    session = service.get_session_for_building_duty(building, duty_date_text, duty_shift_text)
            else:
                latest_session_id = _safe_latest_session_id(service, building=building)
                if latest_session_id:
                    session = service.get_session_by_id(latest_session_id)
                else:
                    latest_session_getter = getattr(service, "get_latest_session_fast", None)
                    if callable(latest_session_getter):
                        session = latest_session_getter(building)
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
    latest_getter = getattr(service, "get_latest_session_id_fast", None)
    if not callable(latest_getter):
        latest_getter = getattr(service, "get_latest_session_id", None)
    latest_session_id = str(latest_getter(building) if callable(latest_getter) else "").strip()
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
    cached = _review_history_cache_get(
        building=building,
        selected_session_id=selected_session_id,
    )
    if isinstance(cached, dict) and cached:
        return cached
    try:
        payload = _build_history_payload(
            service,
            building=building,
            selected_session_id=selected_session_id,
        )
        _review_history_cache_put(
            building=building,
            selected_session_id=selected_session_id,
            payload=payload,
        )
        return payload
    except Exception as exc:  # noqa: BLE001
        latest_session_id = ""
        try:
            latest_getter = getattr(service, "get_latest_session_id_fast", None)
            if not callable(latest_getter):
                latest_getter = getattr(service, "get_latest_session_id", None)
            latest_session_id = str(latest_getter(building) if callable(latest_getter) else "").strip()
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


def _safe_latest_session_id(
    service: ReviewSessionService,
    *,
    building: str,
    emit_log=None,
) -> str:
    try:
        latest_getter = getattr(service, "get_latest_session_id_fast", None)
        if not callable(latest_getter):
            latest_getter = getattr(service, "get_latest_session_id", None)
        return str(latest_getter(building) if callable(latest_getter) else "").strip()
    except ReviewSessionStoreUnavailableError as exc:
        if callable(emit_log):
            emit_log(f"[交接班][审核状态] latest_session 已降级: building={building}, 错误={exc}")
        return ""
    except Exception as exc:  # noqa: BLE001
        if callable(emit_log):
            emit_log(
                f"[交接班][审核状态] latest_session 查询异常已降级: building={building}, "
                f"错误={type(exc).__name__}: {exc}"
            )
        return ""


def _present_review_cloud_sheet_state(raw: Any) -> Dict[str, Any]:
    payload = ReviewSessionService._normalize_cloud_sheet_sync(raw)
    status = str(payload.get("status", "")).strip().lower()
    url = str(payload.get("spreadsheet_url", "")).strip()
    error = str(payload.get("error", "")).strip()
    attempted = bool(payload.get("attempted"))
    text = "云表未执行"
    tone = "neutral"
    reason_code = status or "idle"
    if status == "success":
        text = "云表已同步"
        tone = "success"
    elif status == "pending_upload":
        text = "云表待最终上传"
        tone = "warning"
    elif status == "prepare_failed":
        text = "云表预建失败"
        tone = "danger"
    elif status == "failed":
        text = "云表最终上传失败"
        tone = "danger"
    elif status == "disabled":
        text = "云表未启用"
        tone = "neutral"
    elif status == "skipped":
        text = "云表未执行"
        tone = "neutral"
    elif attempted:
        text = "云表已尝试同步"
        tone = "info"
        reason_code = "attempted"
    return {
        "status": status or "idle",
        "text": text,
        "tone": tone,
        "reason_code": reason_code,
        "url": url,
        "error": error,
    }


def _present_review_excel_sync_state(raw: Any) -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    status = str(payload.get("status", "")).strip().lower()
    error = str(payload.get("error", "")).strip()
    synced_revision = int(payload.get("synced_revision", 0) or 0)
    pending_revision = int(payload.get("pending_revision", 0) or 0)
    text = "交接班文件同步状态未知"
    tone = "neutral"
    reason_code = status or "unknown"
    if status == "synced":
        text = "交接班文件已同步"
        tone = "success"
    elif status in {"pending", "syncing"}:
        text = "后台正在同步交接班文件"
        tone = "info"
    elif status == "failed":
        text = "后台Excel同步失败"
        tone = "danger"
    elif status == "unknown":
        text = "交接班文件待同步"
        tone = "warning"
    return {
        "status": status or "unknown",
        "text": text,
        "tone": tone,
        "reason_code": reason_code,
        "error": error,
        "synced_revision": synced_revision,
        "pending_revision": pending_revision,
    }


def _present_review_capacity_state(raw: Any) -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    status = str(payload.get("status", "")).strip().lower()
    error = str(payload.get("error", "")).strip()
    tracked_cells = payload.get("tracked_cells") if isinstance(payload.get("tracked_cells"), list) else []
    normalized_tracked_cells = [
        str(item or "").strip().upper()
        for item in tracked_cells
        if str(item or "").strip()
    ]
    text = "容量报表待补写"
    tone = "warning"
    reason_code = status or "failed"
    if status == "ready":
        text = "容量报表已就绪"
        tone = "success"
    elif status == "pending":
        text = "容量报表后台补写中"
        tone = "info"
    elif status == "pending_input":
        text = "容量关联字段待补全"
        tone = "warning"
    elif status == "missing_file":
        text = "容量报表文件缺失"
        tone = "warning"
    elif status == "failed":
        text = error or "容量报表待补写完成后才能下载"
        tone = "danger"
    return {
        "status": status or "failed",
        "text": text,
        "tone": tone,
        "reason_code": reason_code,
        "error": error,
        "tracked_cells": normalized_tracked_cells,
        "updated_at": str(payload.get("updated_at", "")).strip(),
    }


def _review_action(
    *,
    allowed: bool,
    label: str,
    disabled_reason: str = "",
    visible: bool = True,
    tone: str = "neutral",
    variant: str = "secondary",
    pending: bool = False,
) -> Dict[str, Any]:
    return {
        "allowed": bool(allowed),
        "visible": bool(visible),
        "pending": bool(pending),
        "label": str(label or "").strip() or "-",
        "disabled_reason": str(disabled_reason or "").strip(),
        "tone": str(tone or "neutral").strip() or "neutral",
        "variant": str(variant or "secondary").strip() or "secondary",
    }


def _review_badge(
    *,
    code: str,
    text: str,
    tone: str = "neutral",
    emphasis: str = "soft",
    icon: str = "dot",
) -> Dict[str, Any]:
    return {
        "code": str(code or "").strip().lower() or "unknown",
        "text": str(text or "").strip() or "-",
        "tone": str(tone or "neutral").strip() or "neutral",
        "emphasis": str(emphasis or "soft").strip() or "soft",
        "icon": str(icon or "dot").strip() or "dot",
    }


def _review_display_item(
    *,
    status: str,
    text: str,
    tone: str = "neutral",
    reason_code: str = "",
    detail_text: str = "",
) -> Dict[str, Any]:
    return {
        "status": str(status or "").strip().lower() or "unknown",
        "text": str(text or "").strip() or "-",
        "tone": str(tone or "neutral").strip() or "neutral",
        "reason_code": str(reason_code or status or "").strip().lower() or "unknown",
        "detail_text": str(detail_text or "").strip(),
    }


def _build_review_confirm_feedback(
    *,
    confirmed: bool,
    followup_result: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload = followup_result if isinstance(followup_result, dict) else {}
    followup_status = str(payload.get("status", "")).strip().lower()
    cloud_status = (
        str(
            (payload.get("cloud_sheet_sync", {}) or {}).get("status", "")
            if isinstance(payload.get("cloud_sheet_sync", {}), dict)
            else ""
        )
        .strip()
        .lower()
    )
    if not confirmed:
        return _review_display_item(
            status="unconfirmed",
            text="已撤销确认",
            tone="info",
            reason_code="unconfirmed",
            detail_text="当前楼栋已撤销确认。",
        )
    if followup_status == "await_all_confirmed":
        return _review_display_item(
            status="await_all_confirmed",
            text="等待五个楼栋全部确认",
            tone="info",
            reason_code="await_all_confirmed",
            detail_text="当前批次尚未全部确认，后续上传将在全部确认后执行。",
        )
    if followup_status in {"ok", "success"}:
        return _review_display_item(
            status="followup_started",
            text="已触发首次全量上传",
            tone="success",
            reason_code="followup_started",
            detail_text="当前楼栋确认完成，已触发整批后续上传。",
        )
    if cloud_status in {"ok", "success"}:
        return _review_display_item(
            status="single_building_retry_started",
            text="已进入单楼云表重传",
            tone="success",
            reason_code="single_building_retry_started",
            detail_text="当前楼栋确认完成，已触发当前楼栋云表重传。",
        )
    return _review_display_item(
        status="confirmed",
        text="已确认当前楼栋",
        tone="success",
        reason_code="confirmed",
        detail_text="当前楼栋已确认。",
    )


def _build_review_cloud_retry_feedback(result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = result if isinstance(result, dict) else {}
    status = str(payload.get("status", "")).strip().lower()
    if status in {"ok", "success"}:
        return _review_display_item(
            status="success",
            text="云表上传成功",
            tone="success",
            reason_code="success",
            detail_text="云表上传已完成。",
        )
    if status == "blocked":
        blocked_reason = ""
        cloud_sheet_sync = payload.get("cloud_sheet_sync", {})
        if isinstance(cloud_sheet_sync, dict):
            blocked_reason = str(cloud_sheet_sync.get("blocked_reason", "")).strip()
        return _review_display_item(
            status="blocked",
            text=blocked_reason or "当前批次尚未全部确认，不能重试云表上传。",
            tone="warning",
            reason_code="blocked",
            detail_text=blocked_reason or "当前批次尚未全部确认，不能重试云表上传。",
        )
    return _review_display_item(
        status="failed",
        text="云表上传失败",
        tone="danger",
        reason_code="failed",
        detail_text="云表上传失败。",
    )


def _build_review_history_cloud_update_feedback(result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = result if isinstance(result, dict) else {}
    status = str(payload.get("status", "")).strip().lower()
    if status in {"ok", "success"}:
        return _review_display_item(
            status="success",
            text="历史云文档已更新",
            tone="success",
            reason_code="success",
            detail_text="当前历史交接班对应的云文档已更新。",
        )
    return _review_display_item(
        status="failed",
        text="历史云文档更新失败",
        tone="danger",
        reason_code="failed",
        detail_text="历史云文档更新失败。",
    )


def _build_review_document_state(
    *,
    session: Dict[str, Any] | None,
    client_session_id: str = "",
    client_revision: int = 0,
    latest_session_id: str = "",
) -> Dict[str, Any]:
    session_payload = session if isinstance(session, dict) else {}
    server_session_id = str(session_payload.get("session_id", "")).strip()
    if not server_session_id:
        return {
            **_review_display_item(
                status="unavailable",
                text="暂无审核内容",
                tone="neutral",
                reason_code="unavailable",
                detail_text="当前没有可同步的审核文档。",
            ),
            "should_reload_document": False,
        }
    client_session_id_text = str(client_session_id or "").strip()
    try:
        client_revision_value = int(client_revision or 0)
    except Exception:  # noqa: BLE001
        client_revision_value = 0
    server_revision = int(session_payload.get("revision", 0) or 0)
    latest_session_id_text = str(latest_session_id or "").strip()
    if client_session_id_text and client_session_id_text != server_session_id:
        status = "latest_session_changed" if latest_session_id_text and server_session_id == latest_session_id_text else "session_changed"
        text = "检测到最新交接班日志" if status == "latest_session_changed" else "检测到审核记录已切换"
        detail = "正在切换到最新交接班日志..." if status == "latest_session_changed" else "正在同步最新审核内容..."
        return {
            **_review_display_item(
                status=status,
                text=text,
                tone="warning",
                reason_code=status,
                detail_text=detail,
            ),
            "should_reload_document": True,
        }
    if client_revision_value > 0 and client_revision_value != server_revision:
        return {
            **_review_display_item(
                status="revision_changed",
                text="审核内容已更新",
                tone="warning",
                reason_code="revision_changed",
                detail_text="检测到审核内容更新，正在同步最新内容...",
            ),
            "should_reload_document": True,
        }
    return {
        **_review_display_item(
            status="current",
            text="审核内容已同步",
            tone="success",
            reason_code="current",
            detail_text="当前文档与后端版本一致。",
        ),
        "should_reload_document": False,
    }


def _build_review_display_state(
    *,
    building: str,
    session: Dict[str, Any] | None,
    batch_status: Dict[str, Any] | None,
    concurrency: Dict[str, Any] | None,
    history: Dict[str, Any] | None = None,
    defaults_sync: Dict[str, Any] | None = None,
    save_status: Dict[str, Any] | None = None,
    latest_session_id: str = "",
    client_session_id: str = "",
    client_revision: int = 0,
) -> Dict[str, Any]:
    session_payload = session if isinstance(session, dict) else {}
    batch_payload = batch_status if isinstance(batch_status, dict) else {}
    concurrency_payload = concurrency if isinstance(concurrency, dict) else _empty_concurrency()
    history_payload = history if isinstance(history, dict) else {}
    defaults_sync_payload = defaults_sync if isinstance(defaults_sync, dict) else {}
    save_status_payload = save_status if isinstance(save_status, dict) else {}
    selected_session_id = str(session_payload.get("session_id", "")).strip()
    latest_session_id_text = str(
        latest_session_id
        or history_payload.get("latest_session_id", "")
        or ""
    ).strip()
    is_history_mode = bool(
        selected_session_id
        and latest_session_id_text
        and selected_session_id != latest_session_id_text
    )
    cloud_sheet_state = _present_review_cloud_sheet_state(session_payload.get("cloud_sheet_sync", {}))
    excel_sync_state = _present_review_excel_sync_state(session_payload.get("excel_sync", {}))
    capacity_state = _present_review_capacity_state(session_payload.get("capacity_sync", {}))
    confirmed = bool(session_payload.get("confirmed", False))
    has_output_file = bool(str(session_payload.get("output_file", "")).strip())
    has_capacity_file = bool(str(session_payload.get("capacity_output_file", "")).strip())
    all_confirmed = bool(batch_payload.get("all_confirmed", False))
    remote_editor_active = bool(concurrency_payload.get("is_editing_elsewhere", False))
    active_editor = (
        concurrency_payload.get("active_editor", {})
        if isinstance(concurrency_payload.get("active_editor"), dict)
        else {}
    )
    active_editor_label = str(active_editor.get("holder_label", "")).strip()
    lock_state = {
        "status": "free",
        "text": "",
        "tone": "neutral",
        "reason_code": "free",
    }
    if bool(concurrency_payload.get("client_holds_lock", False)):
        lock_state = {
            "status": "held_by_self",
            "text": "本端编辑中",
            "tone": "info",
            "reason_code": "held_by_self",
        }
    elif remote_editor_active and active_editor_label:
        lock_state = {
            "status": "held_by_other",
            "text": "其他终端编辑中",
            "tone": "warning",
            "reason_code": "held_by_other",
        }
    status_banners: list[dict[str, Any]] = []
    if is_history_mode:
        status_banners.append(
            {
                "code": "history_mode",
                "text": "当前为历史模式：只更新当前历史记录，不改模板默认值；如需同步云文档，请手动点击“更新云文档”。",
                "tone": "info",
            }
        )
    if remote_editor_active and active_editor_label:
        status_banners.append(
            {
                "code": "other_editor_active",
                "text": f"其他用户正在编辑：{active_editor_label}。如需保存或确认，请稍等。",
                "tone": "warning",
            }
        )
    if cloud_sheet_state["error"]:
        status_banners.append(
            {
                "code": "cloud_sheet_error",
                "text": f"云表同步失败: {cloud_sheet_state['error']}",
                "tone": "danger",
            }
        )
    if excel_sync_state["status"] == "failed":
        status_banners.append(
            {
                "code": "excel_sync_failed",
                "text": f"后台Excel同步失败：{excel_sync_state['error'] or '下载前会自动重试同步'}",
                "tone": "danger",
            }
        )
    elif excel_sync_state["status"] in {"pending", "syncing"}:
        status_banners.append(
            {
                "code": "excel_sync_pending",
                "text": "后台正在同步交接班Excel，页面内容已保存到本楼SQLite；下载时会强制写入最新内容。",
                "tone": "info",
            }
        )
    defaults_sync_state = _review_display_item(
        status=str(defaults_sync_payload.get("status", "")).strip().lower() or "skipped",
        text=str(defaults_sync_payload.get("state_text", "")).strip() or "默认值未更新",
        tone=str(defaults_sync_payload.get("tone", "")).strip() or "neutral",
        reason_code=str(defaults_sync_payload.get("status", "")).strip().lower() or "skipped",
        detail_text=str(defaults_sync_payload.get("detail_text", "")).strip(),
    )
    if defaults_sync_state["status"] == "queued":
        status_banners.append(
            {
                "code": "defaults_sync_pending",
                "text": defaults_sync_state["detail_text"] or "楼栋默认值后台回写中。",
                "tone": "info",
            }
        )
    elif defaults_sync_state["status"] == "failed":
        status_banners.append(
            {
                "code": "defaults_sync_failed",
                "text": defaults_sync_state["detail_text"] or "楼栋默认值后台回写失败。",
                "tone": "danger",
            }
        )
    save_allowed = bool(session_payload) and not remote_editor_active
    save_disabled_reason = ""
    if not session_payload:
        save_disabled_reason = "暂无可保存的交接班记录"
    elif remote_editor_active:
        save_disabled_reason = "当前审核页正在其他终端编辑，请等待或刷新后重试"
    confirm_allowed = bool(session_payload) and not is_history_mode and not remote_editor_active
    confirm_disabled_reason = ""
    if not session_payload:
        confirm_disabled_reason = "暂无可确认的交接班记录"
    elif is_history_mode:
        confirm_disabled_reason = "仅最新交接班日志支持确认、撤销确认和云表重试"
    elif remote_editor_active:
        confirm_disabled_reason = "当前审核页正在其他终端编辑，请等待或刷新后重试"
    retry_allowed = bool(session_payload) and (not is_history_mode) and confirmed and all_confirmed and str(cloud_sheet_state["status"]) in {"failed", "prepare_failed"}
    retry_disabled_reason = ""
    if not session_payload:
        retry_disabled_reason = "暂无可重试的交接班记录"
    elif is_history_mode:
        retry_disabled_reason = "历史模式不支持重试当前云表上传"
    elif not confirmed:
        retry_disabled_reason = "当前楼栋尚未确认，不能重试云表上传"
    elif not all_confirmed:
        retry_disabled_reason = "当前批次尚未全部确认，不能重试云表上传"
    elif str(cloud_sheet_state["status"]) not in {"failed", "prepare_failed"}:
        retry_disabled_reason = "当前云表状态无需重试"
    update_history_allowed = bool(session_payload) and is_history_mode and not remote_editor_active
    update_history_disabled_reason = ""
    if not session_payload:
        update_history_disabled_reason = "暂无可更新的历史交接班记录"
    elif not is_history_mode:
        update_history_disabled_reason = "仅历史模式支持更新云文档"
    elif remote_editor_active:
        update_history_disabled_reason = "当前审核页正在其他终端编辑，请等待或刷新后重试"
    return_to_latest_allowed = bool(session_payload) and is_history_mode and bool(latest_session_id_text)
    return_to_latest_disabled_reason = ""
    if not session_payload:
        return_to_latest_disabled_reason = "暂无可切换的交接班记录"
    elif not is_history_mode:
        return_to_latest_disabled_reason = "当前已经是最新交接班日志"
    elif not latest_session_id_text:
        return_to_latest_disabled_reason = "当前无法定位最新交接班日志"
    download_allowed = bool(session_payload) and has_output_file
    download_disabled_reason = ""
    if not session_payload:
        download_disabled_reason = "当前没有可下载的交接班文件"
    elif not has_output_file:
        download_disabled_reason = "当前没有可下载的交接班文件"
    capacity_allowed = bool(session_payload) and has_capacity_file and str(capacity_state["status"]) == "ready"
    capacity_disabled_reason = ""
    if not session_payload:
        capacity_disabled_reason = "当前没有可下载的交接班容量报表"
    elif not has_capacity_file:
        capacity_disabled_reason = "当前没有可下载的交接班容量报表"
    elif str(capacity_state["status"]) != "ready":
        capacity_disabled_reason = capacity_state["error"] or "容量报表待补写完成后才能下载"
    capacity_image_delivery = (
        session_payload.get("capacity_image_delivery", {})
        if isinstance(session_payload, dict) and isinstance(session_payload.get("capacity_image_delivery", {}), dict)
        else {}
    )
    capacity_image_sending = str(capacity_image_delivery.get("status", "") or "").strip().lower() == "sending"
    capacity_image_send_allowed = capacity_allowed and not capacity_image_sending
    capacity_image_send_disabled_reason = capacity_disabled_reason
    if capacity_image_sending:
        capacity_image_send_disabled_reason = "容量表图片正在发送中，请等待发送完成"
    history_limit = max(1, int(history_payload.get("history_limit", HISTORY_CLOUD_SUCCESS_LIMIT) or HISTORY_CLOUD_SUCCESS_LIMIT))
    history_hint_rows = [f"仅显示最近 {history_limit} 条已成功上云的交接班日志。"]
    if session_payload and not bool(history_payload.get("selected_in_history_list", False)):
        excluded_reason = str(history_payload.get("selected_history_excluded_reason", "")).strip().lower()
        if excluded_reason == "outside_limit":
            history_hint_rows.append(f"当前查看记录已成功上云，但不在最近 {history_limit} 条历史范围内。")
        elif excluded_reason == "not_cloud_success":
            history_hint_rows.append("当前查看记录尚未成功上云，因此不在历史列表中。")
    history_hint = " ".join([item for item in history_hint_rows if str(item or "").strip()]).strip()
    save_state = _review_display_item(
        status=str(save_status_payload.get("status", "")).strip().lower() or ("blocked" if remote_editor_active else ("history_ready" if is_history_mode else "ready")),
        text=str(save_status_payload.get("state_text", "")).strip() or ("其他终端编辑中，当前只读" if remote_editor_active else ("历史交接班日志可编辑" if is_history_mode else "当前交接班日志可编辑")),
        tone=str(save_status_payload.get("tone", "")).strip() or ("warning" if remote_editor_active else ("info" if is_history_mode else "success")),
        reason_code=str(save_status_payload.get("reason_code", "")).strip().lower() or ("remote_editor_active" if remote_editor_active else ("history_mode" if is_history_mode else "ready")),
        detail_text=str(save_status_payload.get("detail_text", "")).strip() or ("当前审核页正在其他终端编辑，请等待或刷新后重试" if remote_editor_active else ""),
    )
    confirm_badge = _review_badge(
        code="confirm",
        text="可编辑" if is_history_mode else ("已确认" if confirmed else "待确认"),
        tone="neutral" if is_history_mode else ("success" if confirmed else "warning"),
        emphasis="outline" if is_history_mode else ("solid" if confirmed else "soft"),
        icon="file" if is_history_mode else ("check" if confirmed else "warn"),
    )
    if remote_editor_active:
        save_badge = _review_badge(
            code="save",
            text="只读",
            tone="warning",
            emphasis="soft",
            icon="warn",
        )
    elif is_history_mode:
        save_badge = _review_badge(
            code="save",
            text="历史可编辑",
            tone="info",
            emphasis="soft",
            icon="file",
        )
    else:
        save_badge = _review_badge(
            code="save",
            text="已保存",
            tone="success",
            emphasis="soft",
            icon="check",
        )
    download_state = _review_display_item(
        status="ready" if download_allowed else ("missing_file" if session_payload else "unavailable"),
        text="交接班文件可下载" if download_allowed else download_disabled_reason,
        tone="success" if download_allowed else "warning",
        reason_code="ready" if download_allowed else ("missing_file" if session_payload else "unavailable"),
        detail_text="" if download_allowed else download_disabled_reason,
    )
    capacity_download_state = _review_display_item(
        status="ready" if capacity_allowed else (str(capacity_state["status"]) or ("missing_file" if session_payload else "unavailable")),
        text="容量报表可下载" if capacity_allowed else (capacity_disabled_reason or capacity_state["text"]),
        tone="success" if capacity_allowed else str(capacity_state["tone"] or "warning"),
        reason_code="ready" if capacity_allowed else (str(capacity_state["reason_code"]) or ("missing_file" if session_payload else "unavailable")),
        detail_text="" if capacity_allowed else (capacity_disabled_reason or capacity_state["error"] or capacity_state["text"]),
    )
    confirm_state = _review_display_item(
        status="confirmed" if confirmed else ("history_mode" if is_history_mode else ("blocked" if remote_editor_active else "pending_confirm")),
        text="当前楼栋已确认" if confirmed else ("历史模式不可确认" if is_history_mode else ("其他终端编辑中" if remote_editor_active else "当前楼栋待确认")),
        tone="success" if confirmed else ("neutral" if is_history_mode else "warning"),
        reason_code="confirmed" if confirmed else ("history_mode" if is_history_mode else ("remote_editor_active" if remote_editor_active else "pending_confirm")),
        detail_text="" if confirmed else confirm_disabled_reason,
    )
    document_state = _build_review_document_state(
        session=session_payload,
        client_session_id=client_session_id,
        client_revision=client_revision,
        latest_session_id=latest_session_id_text,
    )
    header_badges: list[dict[str, Any]] = [
        _review_badge(
            code="mode",
            text="历史记录" if is_history_mode else "当前记录",
            tone="warning" if is_history_mode else "info",
            emphasis="outline",
            icon="clock",
        ),
        save_badge,
        confirm_badge,
    ]
    if str(lock_state.get("text", "")).strip():
        header_badges.append(
            _review_badge(
                code="lock",
                text=str(lock_state.get("text", "")).strip(),
                tone=str(lock_state.get("tone", "neutral")).strip() or "neutral",
                emphasis="outline",
                icon="warn",
            )
        )
    elif str(cloud_sheet_state.get("tone", "")).strip() in {"warning", "danger"} and str(cloud_sheet_state.get("text", "")).strip():
        header_badges.append(
            _review_badge(
                code="cloud_sheet",
                text=str(cloud_sheet_state.get("text", "")).strip(),
                tone=str(cloud_sheet_state.get("tone", "neutral")).strip() or "neutral",
                emphasis="outline",
                icon="link",
            )
        )
    return {
        "mode": {
            "code": "history" if is_history_mode else "latest",
            "text": "历史记录" if is_history_mode else "当前记录",
            "tone": "warning" if is_history_mode else "info",
            "emphasis": "outline",
            "icon": "clock",
        },
        "header_badges": header_badges,
        "confirm_badge": confirm_badge,
        "lock_state": lock_state,
        "history_hint": history_hint,
        "save_state": save_state,
        "download_state": download_state,
        "capacity_download_state": capacity_download_state,
        "confirm_state": confirm_state,
        "document_state": document_state,
        "cloud_sheet": cloud_sheet_state,
        "excel_sync": excel_sync_state,
        "capacity_sync": capacity_state,
        "defaults_sync": defaults_sync_state,
        "status_banners": status_banners,
        "actions": {
            "refresh": _review_action(
                allowed=bool(session_payload),
                visible=True,
                label="刷新",
                disabled_reason="" if session_payload else "暂无可刷新的交接班记录",
                tone="neutral",
                variant="secondary",
            ),
            "save": _review_action(
                allowed=save_allowed,
                visible=bool(session_payload),
                label="保存",
                disabled_reason=save_disabled_reason,
                tone="primary",
                variant="primary",
            ),
            "download": _review_action(
                allowed=download_allowed,
                visible=bool(session_payload),
                label="下载交接班日志",
                disabled_reason=download_disabled_reason,
                tone="neutral",
                variant="secondary",
            ),
            "capacity_download": _review_action(
                allowed=capacity_allowed,
                visible=bool(session_payload),
                label="下载交接班容量报表",
                disabled_reason=capacity_disabled_reason,
                tone="neutral",
                variant="secondary",
            ),
            "capacity_image_send": _review_action(
                allowed=capacity_image_send_allowed,
                visible=bool(session_payload),
                label="发送容量表图片",
                disabled_reason=capacity_image_send_disabled_reason,
                tone="neutral",
                variant="secondary",
                pending=capacity_image_sending,
            ),
            "confirm": _review_action(
                allowed=confirm_allowed,
                visible=bool(session_payload) and not is_history_mode,
                label="已确认（可取消）" if confirmed else "确认当前楼栋",
                disabled_reason=confirm_disabled_reason,
                tone="success" if confirmed else "warning",
                variant="success" if confirmed else "warning",
            ),
            "retry_cloud_sync": _review_action(
                allowed=retry_allowed,
                visible=bool(session_payload) and not is_history_mode,
                label="重试云表上传",
                disabled_reason=retry_disabled_reason,
                tone="warning",
                variant="warning",
            ),
            "update_history_cloud_sync": _review_action(
                allowed=update_history_allowed,
                visible=bool(session_payload) and is_history_mode,
                label="更新云文档",
                disabled_reason=update_history_disabled_reason,
                tone="warning",
                variant="warning",
            ),
            "return_to_latest": _review_action(
                allowed=return_to_latest_allowed,
                visible=bool(session_payload) and is_history_mode,
                label="返回最新",
                disabled_reason=return_to_latest_disabled_reason,
                tone="neutral",
                variant="secondary",
            ),
        },
    }


def _attach_review_display_state(
    payload: Dict[str, Any],
    *,
    service: ReviewSessionService,
    building: str,
    client_id: str = "",
    client_session_id: str = "",
    client_revision: int = 0,
    emit_log=None,
    include_concurrency: bool = True,
) -> Dict[str, Any]:
    response = dict(payload or {})
    session = response.get("session") if isinstance(response.get("session"), dict) else {}
    if not session:
        response["display_state"] = _build_review_display_state(
            building=building,
            session={},
            batch_status=response.get("batch_status", {}),
            concurrency=_empty_concurrency(),
            history=response.get("history", {}),
            defaults_sync=response.get("defaults_sync", {}),
            save_status=response.get("save_status", {}),
            latest_session_id="",
            client_session_id=client_session_id,
            client_revision=client_revision,
        )
        return response
    current_revision = int(session.get("revision", 0) or 0)
    if include_concurrency:
        concurrency = (
            response.get("concurrency")
            if isinstance(response.get("concurrency"), dict)
            else _get_session_concurrency_safe(
                service,
                building=building,
                session_id=str(session.get("session_id", "")).strip(),
                client_id=str(client_id or "").strip(),
                current_revision=current_revision,
                emit_log=emit_log,
            )
        )
    else:
        concurrency = response.get("concurrency") if isinstance(response.get("concurrency"), dict) else {}
    latest_session_id = str(response.get("latest_session_id", "") or "").strip()
    if not latest_session_id:
        latest_session_id = _safe_latest_session_id(service, building=building, emit_log=emit_log)
    if latest_session_id:
        response["latest_session_id"] = latest_session_id
    batch_key = str(session.get("batch_key", "") or "").strip()
    if batch_key:
        try:
            shared_110kv = service.get_substation_110kv(batch_key)
            response["shared_blocks"] = {
                **(
                    response.get("shared_blocks", {})
                    if isinstance(response.get("shared_blocks", {}), dict)
                    else {}
                ),
                "substation_110kv": shared_110kv,
            }
            response["shared_block_locks"] = {
                **(
                    response.get("shared_block_locks", {})
                    if isinstance(response.get("shared_block_locks", {}), dict)
                    else {}
                ),
                "substation_110kv": service.get_substation_110kv_lock(
                    batch_key=batch_key,
                    client_id=str(client_id or "").strip(),
                ),
            }
        except Exception as exc:  # noqa: BLE001
            if callable(emit_log):
                emit_log(
                    f"[交接班][110KV共享] 状态附加失败 building={building}, batch={batch_key}, error={exc}"
                )
    if include_concurrency:
        response["concurrency"] = concurrency
    response["display_state"] = _build_review_display_state(
        building=building,
        session=session,
        batch_status=response.get("batch_status", {}),
        concurrency=concurrency,
        history=response.get("history", {}),
        defaults_sync=response.get("defaults_sync", {}),
        save_status=response.get("save_status", {}),
        latest_session_id=latest_session_id,
        client_session_id=client_session_id,
        client_revision=client_revision,
    )
    return response


def _ensure_latest_session_actionable_or_400(service: ReviewSessionService, *, building: str, session_id: str) -> None:
    latest_session_id = _safe_latest_session_id(service, building=building)
    if not latest_session_id:
        raise HTTPException(status_code=404, detail="暂无可审核交接班文件")
    if str(session_id or "").strip() != latest_session_id:
        raise HTTPException(status_code=400, detail="仅最新交接班日志支持确认、撤销确认和云表重试")


@router.get("/handover/review/{building_code}")
def handover_review_page(building_code: str, request: Request):
    container = request.app.state.container
    service = _build_review_session_service(container)
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
    service = _build_review_session_service(container)
    followup = _build_review_followup_service(container)
    try:
        batch_status = service.get_batch_status(batch_key)
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    return {"ok": True, **_attach_followup_progress(followup, batch_status)}


@router.post("/api/handover/review/batch/{batch_key}/confirm-all")
def handover_review_confirm_all(batch_key: str, request: Request) -> Dict[str, Any]:
    container = request.app.state.container

    def _run(emit_log) -> Dict[str, Any]:
        service = _build_review_session_service(container)
        followup = _build_review_followup_service(container)
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
        _review_history_cache_invalidate_sessions(refreshed_sessions or updated_sessions)
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
        document, session = _load_review_document_cached(document_state, session)
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    try:
        batch_status = service.get_batch_status(session["batch_key"])
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    review_ui = parser.config.get("review_ui", {}) if isinstance(parser.config, dict) else {}
    response = {
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
    return _attach_review_display_state(
        response,
        service=service,
        building=building,
        client_id=str(client_id or "").strip(),
        emit_log=container.add_system_log,
    )


@router.get("/api/handover/review/{building_code}/bootstrap")
def handover_review_bootstrap(
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
        payload, _from_cache = _build_review_bootstrap_payload(
            building=building,
            parser=parser,
            document_state=document_state,
            session=session,
        )
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _attach_review_display_state(
        payload,
        service=service,
        building=building,
        client_id=str(client_id or "").strip(),
        emit_log=container.add_system_log,
        include_concurrency=False,
    )


@router.get("/api/handover/review/{building_code}/status")
def handover_review_status(
    building_code: str,
    request: Request,
    duty_date: str = "",
    duty_shift: str = "",
    session_id: str = "",
    client_id: str = "",
    client_session_id: str = "",
    client_revision: int = 0,
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    session = _load_target_session_or_404(
        service,
        building=building,
        duty_date=duty_date,
        duty_shift=duty_shift,
        session_id=session_id,
    )
    session = _attach_excel_sync_from_store(container, session)
    try:
        batch_status = service.get_batch_status(session["batch_key"])
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    review_ui = _build_review_ui_config(container)
    poll_interval_sec = 5
    if isinstance(review_ui, dict):
        try:
            poll_interval_sec = max(1, int(review_ui.get("poll_interval_sec", 5) or 5))
        except Exception:  # noqa: BLE001
            poll_interval_sec = 5
    response = {
        "ok": True,
        "building": building,
        "session": session,
        "batch_status": batch_status,
        "concurrency": _get_session_concurrency_safe(
            service,
            building=building,
            session_id=str(session.get("session_id", "")).strip(),
            client_id=str(client_id or "").strip(),
            current_revision=int(session.get("revision", 0) or 0),
            emit_log=container.add_system_log,
        ),
        "review_ui": {"poll_interval_sec": poll_interval_sec},
    }
    return _attach_review_display_state(
        response,
        service=service,
        building=building,
        client_id=str(client_id or "").strip(),
        client_session_id=str(client_session_id or "").strip(),
        client_revision=int(client_revision or 0),
        emit_log=container.add_system_log,
    )


@router.get("/api/handover/review/{building_code}/history")
def handover_review_history(
    building_code: str,
    request: Request,
    duty_date: str = "",
    duty_shift: str = "",
    session_id: str = "",
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    session = _load_target_session_or_404(
        service,
        building=building,
        duty_date=duty_date,
        duty_shift=duty_shift,
        session_id=session_id,
    )
    return {
        "ok": True,
        "building": building,
        "session_id": str(session.get("session_id", "")).strip(),
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
        queue_service = _build_xlsx_write_queue_service(container, parser=parser, writer=writer)
        queue_service.enqueue_review_excel_sync(
            target,
            target_revision=int(target.get("revision", 0) or 0),
        )
        queue_service.wait_for_barrier(building=building, timeout_sec=120.0)
        sync_state = _attach_excel_sync_safe(document_state, target).get("excel_sync", {})
        if str(sync_state.get("status", "") or "").strip().lower() == "failed":
            raise ReviewDocumentStateError(str(sync_state.get("error", "") or "").strip() or "交接班Excel同步失败")
    except HandoverXlsxWriteQueueTimeoutError as exc:
        raise HTTPException(status_code=409, detail="交接班文件写入队列繁忙，请稍后重试") from exc
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
    try:
        target = _ensure_capacity_overlay_queue_drained_for_session(
            container=container,
            review_service=service,
            building=building,
            session_id=session_id_text,
            timeout_sec=120.0,
        )
    except HandoverXlsxWriteQueueTimeoutError as exc:
        container.add_system_log(
            "[交接班][下载容量报表] 等待xlsx写入队列超时 "
            f"building={building}, session={session_id_text}, error={exc}"
        )
        raise HTTPException(status_code=409, detail="容量表写入队列繁忙，请稍后重试") from exc
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    queue_service = _build_xlsx_write_queue_service(container)
    for attempt in range(3):
        needs_queue_wait = False
        with _resource_guard_or_null(
            container,
            name=f"handover_capacity_download:{building}:{session_id_text}",
            resource_keys=_handover_resource_keys(building=building),
        ):
            if queue_service.has_active_write_jobs(building=building):
                needs_queue_wait = True
                container.add_system_log(
                    "[交接班][下载容量报表] 获取下载锁后发现xlsx队列仍有写入任务，继续等待 "
                    f"building={building}, session={session_id_text}, attempt={attempt + 1}"
                )
            else:
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
        if needs_queue_wait:
            try:
                queue_service.wait_for_barrier(building=building, timeout_sec=120.0)
            except HandoverXlsxWriteQueueTimeoutError as exc:
                container.add_system_log(
                    "[交接班][下载容量报表] 下载锁复查后等待xlsx队列超时 "
                    f"building={building}, session={session_id_text}, error={exc}"
                )
                raise HTTPException(status_code=409, detail="容量表写入队列繁忙，请稍后重试") from exc
    raise HTTPException(status_code=409, detail="容量表写入队列繁忙，请稍后重试")


@router.post("/api/handover/review/{building_code}/capacity-image/send")
def handover_review_capacity_image_send(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, _, _, _ = _build_review_services(container)
    building = _resolve_building_or_404(service, building_code)

    payload_dict = payload if isinstance(payload, dict) else {}
    session_id_text = str(payload_dict.get("session_id", "") or "").strip()
    if not session_id_text:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    target = _load_target_session_or_404(service, building=building, session_id=session_id_text)
    container.add_system_log(
        "[交接班][容量表图片发送] 同步接口已命中 "
        f"building={building}, session={session_id_text}, "
        f"source_file={str(target.get('capacity_output_file', '') or '').strip() or '-'}"
    )
    try:
        target = _ensure_capacity_overlay_queue_drained_for_session(
            container=container,
            review_service=service,
            building=building,
            session_id=session_id_text,
            timeout_sec=120.0,
        )
    except HandoverXlsxWriteQueueTimeoutError as exc:
        container.add_system_log(
            "[交接班][容量表图片发送] 等待xlsx写入队列超时 "
            f"building={building}, session={session_id_text}, error={exc}"
        )
        raise HTTPException(status_code=409, detail="容量表写入队列繁忙，请稍后重试") from exc
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    delivery_service = CapacityReportImageDeliveryService(
        _handover_cfg(container),
        config_path=getattr(container, "config_path", None),
    )
    queue_service = _build_xlsx_write_queue_service(container)
    for attempt in range(3):
        needs_queue_wait = False
        with _resource_guard_or_null(
            container,
            name=f"handover_capacity_image_send:{building}:{session_id_text}",
            resource_keys=_handover_resource_keys(building=building),
        ):
            if queue_service.has_active_write_jobs(building=building):
                needs_queue_wait = True
                container.add_system_log(
                    "[交接班][容量表图片发送] 获取发送锁后发现xlsx队列仍有写入任务，继续等待 "
                    f"building={building}, session={session_id_text}, attempt={attempt + 1}"
                )
            else:
                try:
                    delivery_service.begin_delivery(target, building=building, source="manual")
                except FileNotFoundError as exc:
                    container.add_system_log(
                        "[交接班][容量表图片发送] 同步接口预检失败 "
                        f"building={building}, session={session_id_text}, error={exc}"
                    )
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
                except ValueError as exc:
                    container.add_system_log(
                        "[交接班][容量表图片发送] 同步接口预检失败 "
                        f"building={building}, session={session_id_text}, error={exc}"
                    )
                    raise HTTPException(status_code=409, detail=str(exc)) from exc

                try:
                    latest_session = _load_target_session_or_404(service, building=building, session_id=session_id_text)
                    result = delivery_service.send_for_session(
                        latest_session,
                        building=building,
                        source="manual",
                        emit_log=container.add_system_log,
                    )
                except Exception as exc:  # noqa: BLE001
                    try:
                        delivery_service.mark_failed(session_id=session_id_text, error=f"发送容量表图片失败: {exc}", source="manual")
                    except Exception:
                        pass
                    container.add_system_log(
                        "[交接班][容量表图片发送] 同步接口发送异常 "
                        f"building={building}, session={session_id_text}, error={exc}"
                    )
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
                container.add_system_log(
                    f"[交接班][容量表图片发送] 同步发送完成 building={building}, session={session_id_text}, status={result.get('status', '-')}"
                )
                return result
        if needs_queue_wait:
            try:
                queue_service.wait_for_barrier(building=building, timeout_sec=120.0)
            except HandoverXlsxWriteQueueTimeoutError as exc:
                container.add_system_log(
                    "[交接班][容量表图片发送] 发送锁复查后等待xlsx队列超时 "
                    f"building={building}, session={session_id_text}, error={exc}"
                )
                raise HTTPException(status_code=409, detail="容量表写入队列繁忙，请稍后重试") from exc
    raise HTTPException(status_code=409, detail="容量表写入队列繁忙，请稍后重试")


@router.put("/api/handover/review/{building_code}")
def handover_review_save(
    building_code: str,
    request: Request,
    background_tasks: BackgroundTasks,
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
    lock_concurrency = _ensure_session_lock_held_or_409(
        service,
        building=building,
        session_id=session_id,
        client_id=str(payload.get("client_id", "")).strip(),
    )
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
    queued_capacity_sync = False
    defaults_config_async = False
    defaults_config_status = "skipped"
    with container.job_service.resource_guard(
        name=f"handover_save:{batch_key or building}:{session_id}",
        resource_keys=_handover_resource_keys(building=building),
    ):
        write_started = time.perf_counter()
        previous_document_state: Dict[str, Any] | None = None
        try:
            _saved_document_state, previous_document_state = document_state.save_document(
                session=target,
                document=document,
                base_revision=base_revision,
                dirty_regions=dirty_regions,
                ensure_ready=False,
            )
            if isinstance(_saved_document_state, dict):
                if isinstance(_saved_document_state.get("document", {}), dict):
                    document = _saved_document_state.get("document", {})
                _review_document_cache_put(
                    building=building,
                    signature=_review_document_signature(
                        target,
                        revision_override=int(_saved_document_state.get("revision", 0) or 0),
                    ),
                    document=_saved_document_state.get("document", {})
                    if isinstance(_saved_document_state.get("document", {}), dict)
                    else document,
                )
        except ReviewDocumentStateConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ReviewDocumentStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        write_elapsed_ms = int((time.perf_counter() - write_started) * 1000)
        try:
            latest_session_id = _safe_latest_session_id(
                service,
                building=building,
                emit_log=container.add_system_log,
            )
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
                    "config_sync_required": False,
                    "error": str(exc),
                }
                container.add_system_log(
                    f"[交接班][审核模板默认] SQLite默认值写入失败，已保留审核文档保存结果: building={building}, error={exc}"
                )
            defaults_elapsed_ms = int((time.perf_counter() - defaults_started) * 1000)
            if isinstance(persisted_defaults, dict) and bool(persisted_defaults.get("config_sync_required", False)):
                defaults_config_async = True
                defaults_config_status = "queued"
                background_tasks.add_task(
                    _persist_review_defaults_config_async,
                    container,
                    building=building,
                    building_code=str(persisted_defaults.get("config_building_code", "")).strip(),
                    updated_data=copy.deepcopy(
                        persisted_defaults.get("config_data", {})
                        if isinstance(persisted_defaults.get("config_data", {}), dict)
                        else {}
                    ),
                )
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
            session, queued_capacity_sync = _queue_capacity_overlay_after_review_save(
                container=container,
                background_tasks=background_tasks,
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
        _review_history_cache_invalidate(building=building)
        _review_bootstrap_cache_put(
            building=building,
            signature=_review_bootstrap_signature(session),
            payload={
                "ok": True,
                "building": building,
                "session": copy.deepcopy(session),
                "document": copy.deepcopy(document if isinstance(document, dict) else {}),
                "review_ui": _review_ui_payload(parser),
                "prepared_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "snapshot_revision": int(session.get("revision", 0) or 0),
            },
        )

    total_elapsed_ms = int((time.perf_counter() - save_started) * 1000)
    if is_latest_session:
        container.add_system_log(
            f"[交接班][审核保存] building={building}, session_id={session_id}, revision={session.get('revision', '-')}, "
            f"SQLite保存耗时={write_elapsed_ms}ms, 默认值耗时={defaults_elapsed_ms}ms, "
            f"容量补写排队耗时={capacity_elapsed_ms}ms, 状态更新耗时={session_elapsed_ms}ms, 总耗时={total_elapsed_ms}ms"
        )
        if isinstance(persisted_defaults, dict) and persisted_defaults.get("defaults_updated"):
            container.add_system_log(
                f"[交接班][审核模板默认] 已写入楼栋SQLite默认值: building={building}, "
                f"cabinet_power_fields={persisted_defaults.get('cabinet_power_fields', 0)}, "
                f"footer_inventory_rows={persisted_defaults.get('footer_inventory_rows', 0)}, "
                f"cooling_pump_pressure_fields={persisted_defaults.get('cooling_pump_pressure_fields', 0)}"
            )
        else:
            container.add_system_log(
                f"[交接班][审核模板默认] 楼栋SQLite默认值无变化，已跳过写入: building={building}, "
                f"cabinet_power_fields={persisted_defaults.get('cabinet_power_fields', 0) if isinstance(persisted_defaults, dict) else 0}, "
                f"footer_inventory_rows={persisted_defaults.get('footer_inventory_rows', 0) if isinstance(persisted_defaults, dict) else 0}, "
                f"cooling_pump_pressure_fields={persisted_defaults.get('cooling_pump_pressure_fields', 0) if isinstance(persisted_defaults, dict) else 0}"
            )
        if defaults_config_status == "queued":
            container.add_system_log(
                f"[交接班][审核模板默认] 楼栋分段默认值已进入后台回写队列: building={building}"
            )
    else:
        container.add_system_log(
            f"[交接班][历史模式保存] building={building}, session_id={session_id}, revision={session.get('revision', '-')}, "
            f"SQLite保存耗时={write_elapsed_ms}ms, 容量补写排队耗时={capacity_elapsed_ms}ms, "
            f"状态更新耗时={session_elapsed_ms}ms, 总耗时={total_elapsed_ms}ms"
        )
        container.add_system_log(
            f"[交接班][审核模板默认] 已跳过历史模式默认值更新: building={building}, session_id={session_id}"
        )
    response = {
        "ok": True,
        "session": session,
        "revision": int(session.get("revision", 0) or 0),
        "updated_at": str(session.get("updated_at", "")).strip(),
        "output_file": str(session.get("output_file", "")).strip(),
        "latest_session_id": str(latest_session_id or "").strip(),
        "apply_mode": "business_only",
        "runtime_apply": {
            "apply_mode": "business_only",
            "reload_performed": False,
            "applied_services": [
                "review_sqlite",
                "building_defaults_sqlite",
                "capacity_overlay_queue",
                "excel_sync_queue",
            ],
        },
        "save_status": {
            "status": "saved",
            "state_text": "审核内容已保存",
            "summary_text": "SQLite 已提交，后续同步状态以后端返回为准。",
            "detail_text": f"本次保存总耗时 {int(total_elapsed_ms or 0)} ms",
            "tone": "success",
            "reason_code": "saved",
        },
        "defaults_sync": {
            "status": str(defaults_config_status or "skipped"),
            "async": bool(defaults_config_async),
            "state_text": (
                "默认值后台回写中"
                if str(defaults_config_status or "").strip() == "queued"
                else (
                    "默认值回写失败"
                    if str(defaults_config_status or "").strip() == "failed"
                    else (
                        "默认值已固化"
                        if is_latest_session
                        else "历史模式未回写默认值"
                    )
                )
            ),
            "tone": (
                "info"
                if str(defaults_config_status or "").strip() == "queued"
                else ("danger" if str(defaults_config_status or "").strip() == "failed" else "success")
            ),
            "detail_text": (
                "已进入后台回写队列"
                if str(defaults_config_status or "").strip() == "queued"
                else (
                    "后台回写失败，请查看系统日志"
                    if str(defaults_config_status or "").strip() == "failed"
                    else (
                        "最新会话的默认值已写入楼栋主源"
                        if is_latest_session
                        else "仅最新会话会更新默认值"
                    )
                )
            ),
        },
        "concurrency": _self_held_concurrency(
            lock_concurrency,
            current_revision=int(session.get("revision", 0) or 0),
        ),
        "batch_status": batch_status,
        "save_profile": {
            "write_ms": int(write_elapsed_ms or 0),
            "sqlite_save_ms": int(write_elapsed_ms or 0),
            "defaults_ms": int(defaults_elapsed_ms or 0),
            "capacity_sync_ms": int(capacity_elapsed_ms or 0),
            "queued_capacity_sync": bool(queued_capacity_sync),
            "capacity_sync_status": str(session.get("capacity_sync", {}).get("status", "") if isinstance(session.get("capacity_sync", {}), dict) else ""),
            "session_ms": int(session_elapsed_ms or 0),
            "queued_excel_sync": bool(queued_excel_sync),
            "excel_sync_status": str(session.get("excel_sync", {}).get("status", "") if isinstance(session.get("excel_sync", {}), dict) else ""),
            "defaults_config_async": bool(defaults_config_async),
            "defaults_config_status": str(defaults_config_status or "skipped"),
            "total_ms": int(total_elapsed_ms or 0),
        },
    }
    return _attach_review_display_state(
        response,
        service=service,
        building=building,
        client_id=str(payload.get("client_id", "")).strip(),
        emit_log=container.add_system_log,
    )


@router.post("/api/handover/review/{building_code}/confirm")
def handover_review_confirm(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    followup = _build_review_followup_service(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "")).strip()
    base_revision = _parse_base_revision_or_400(payload)
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    _load_target_session_or_404(service, building=building, session_id=session_id)
    _ensure_latest_session_actionable_or_400(service, building=building, session_id=session_id)
    _ensure_session_lock_held_or_409(
        service,
        building=building,
        session_id=session_id,
        client_id=str(payload.get("client_id", "")).strip(),
    )
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
        latest_session = _attach_excel_sync_from_store(container, latest_session)
        _review_history_cache_invalidate(building=building)
        latest_batch_status = _attach_followup_progress(followup, service.get_batch_status(target_batch_key))
        response = {
            "ok": True,
            "session": latest_session,
            "latest_session_id": _safe_latest_session_id(service, building=building, emit_log=container.add_system_log),
            "operation_feedback": _build_review_confirm_feedback(
                confirmed=bool(latest_session.get("confirmed", False)),
                followup_result=followup_result,
            ),
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
        }
        return _attach_review_display_state(
            response,
            service=service,
            building=building,
            client_id=str(payload.get("client_id", "")).strip(),
            emit_log=container.add_system_log,
        )


@router.post("/api/handover/review/{building_code}/unconfirm")
def handover_review_unconfirm(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "")).strip()
    base_revision = _parse_base_revision_or_400(payload)
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    _load_target_session_or_404(service, building=building, session_id=session_id)
    _ensure_latest_session_actionable_or_400(service, building=building, session_id=session_id)
    _ensure_session_lock_held_or_409(
        service,
        building=building,
        session_id=session_id,
        client_id=str(payload.get("client_id", "")).strip(),
    )
    target_session = _load_target_session_or_404(service, building=building, session_id=session_id)
    target_batch_key = str(target_session.get("batch_key", "")).strip()
    with container.job_service.resource_guard(
        name=f"handover_unconfirm:{target_batch_key}:{building}",
        resource_keys=_handover_resource_keys(building=building),
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
        session = _attach_excel_sync_from_store(container, session)
        _review_history_cache_invalidate(building=building)
        response = {
            "ok": True,
            "session": session,
            "latest_session_id": _safe_latest_session_id(service, building=building, emit_log=container.add_system_log),
            "operation_feedback": _build_review_confirm_feedback(
                confirmed=False,
                followup_result=None,
            ),
            "concurrency": _get_session_concurrency_safe(
                service,
                building=building,
                session_id=str(session.get("session_id", "")).strip(),
                client_id=str(payload.get("client_id", "")).strip(),
                current_revision=int(session.get("revision", 0) or 0),
                emit_log=container.add_system_log,
            ),
            "batch_status": batch_status,
        }
        return _attach_review_display_state(
            response,
            service=service,
            building=building,
            client_id=str(payload.get("client_id", "")).strip(),
            emit_log=container.add_system_log,
        )


@router.post("/api/handover/review/{building_code}/lock/claim")
def handover_review_lock_claim(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
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
    service = _build_review_session_service(container)
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
    service = _build_review_session_service(container)
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


def _sync_substation_110kv_to_batch_capacity_reports(
    *,
    container,
    review_service: ReviewSessionService,
    parser: ReviewDocumentParser,
    writer: ReviewDocumentWriter,
    shared_110kv: Dict[str, Any],
    emit_log,
) -> Dict[str, Any]:
    batch_key = str(shared_110kv.get("batch_key", "") or "").strip()
    if not batch_key:
        return {"updated": 0, "failed": 0, "errors": []}
    document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
    queue_service = _build_xlsx_write_queue_service(container, parser=parser, writer=writer)
    sessions = review_service.list_batch_sessions(batch_key)
    updated = 0
    failed = 0
    errors: list[dict[str, str]] = []
    for session in sessions:
        building = str(session.get("building", "") or "").strip()
        session_id = str(session.get("session_id", "") or "").strip()
        capacity_file = str(session.get("capacity_output_file", "") or "").strip()
        if not capacity_file:
            continue
        try:
            document, session_with_sync = document_state.load_document(session)
            tracked_cells = _extract_capacity_tracked_cells(document)
            cooling_pump_pressures = (
                document.get("cooling_pump_pressures", {})
                if isinstance(document.get("cooling_pump_pressures", {}), dict)
                else {}
            )
            pending_payload = _build_pending_capacity_sync_payload_for_review(
                tracked_cells=tracked_cells,
                shared_110kv=shared_110kv,
                cooling_pump_pressures=cooling_pump_pressures,
            )
            updated_session = review_service.update_capacity_sync(
                session_id=session_id,
                capacity_sync=pending_payload,
                capacity_status="pending",
                capacity_error="",
            )
            queue_service.enqueue_capacity_overlay_sync(
                building=building,
                session_id=session_id,
                tracked_cells=tracked_cells,
                shared_110kv=shared_110kv,
                cooling_pump_pressures=cooling_pump_pressures,
                capacity_output_file=str(updated_session.get("capacity_output_file", "") or capacity_file).strip(),
            )
            updated += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            errors.append({"building": building, "session_id": session_id, "error": str(exc)})
            if callable(emit_log):
                emit_log(
                    f"[交接班][110KV共享] 容量表补写失败 building={building}, session_id={session_id}, error={exc}"
                )
    if callable(emit_log):
        emit_log(
            f"[交接班][110KV共享] 本班容量表补写已入队 batch={batch_key}, queued={updated}, failed={failed}"
        )
    return {"updated": updated, "failed": failed, "errors": errors}


def _substation_110kv_value_signature(payload: Dict[str, Any] | None, *, batch_key: str = "") -> tuple[tuple[str, str, str, str, str, str], ...]:
    normalized = ReviewSessionService.normalize_substation_110kv_payload(payload, batch_key=batch_key)
    signature: list[tuple[str, str, str, str, str, str]] = []
    for row in normalized.get("rows", []):
        if not isinstance(row, dict):
            continue
        signature.append(
            (
                str(row.get("row_id", "") or "").strip(),
                *(str(row.get(key, "") or "").strip() for key in _SUBSTATION_110KV_COMPARE_KEYS),
            )
        )
    return tuple(signature)


def _shared_110kv_lock_response(
    *,
    service: ReviewSessionService,
    building: str,
    session_id: str,
    client_id: str,
    operation,
) -> Dict[str, Any]:
    target = _load_target_session_or_404(service, building=building, session_id=session_id)
    batch_key = str(target.get("batch_key", "") or "").strip()
    try:
        lock_state = operation(batch_key)
        block = service.get_substation_110kv(batch_key)
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    return {
        "ok": True,
        "shared_blocks": {"substation_110kv": block},
        "shared_block_locks": {"substation_110kv": lock_state},
        "accepted": bool(lock_state.get("acquired", lock_state.get("renewed", True))),
    }


@router.post("/api/handover/review/{building_code}/shared-blocks/110kv/lock/claim")
def handover_review_shared_110kv_lock_claim(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "") or "").strip()
    client_id = str(payload.get("client_id", "") or "").strip()
    holder_label = str(payload.get("holder_label", "") or "").strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id 不能为空")
    return _shared_110kv_lock_response(
        service=service,
        building=building,
        session_id=session_id,
        client_id=client_id,
        operation=lambda batch_key: service.claim_substation_110kv_lock(
            batch_key=batch_key,
            building=building,
            client_id=client_id,
            holder_label=holder_label,
        ),
    )


@router.post("/api/handover/review/{building_code}/shared-blocks/110kv/lock/heartbeat")
def handover_review_shared_110kv_lock_heartbeat(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "") or "").strip()
    client_id = str(payload.get("client_id", "") or "").strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id 不能为空")
    return _shared_110kv_lock_response(
        service=service,
        building=building,
        session_id=session_id,
        client_id=client_id,
        operation=lambda batch_key: service.heartbeat_substation_110kv_lock(
            batch_key=batch_key,
            client_id=client_id,
        ),
    )


@router.post("/api/handover/review/{building_code}/shared-blocks/110kv/lock/release")
def handover_review_shared_110kv_lock_release(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "") or "").strip()
    client_id = str(payload.get("client_id", "") or "").strip()
    if not session_id or not client_id:
        return {
            "ok": True,
            "shared_block_locks": {"substation_110kv": _empty_concurrency()},
            "released": False,
        }
    response = _shared_110kv_lock_response(
        service=service,
        building=building,
        session_id=session_id,
        client_id=client_id,
        operation=lambda batch_key: service.release_substation_110kv_lock(
            batch_key=batch_key,
            client_id=client_id,
        ),
    )
    response["released"] = bool(response.get("shared_block_locks", {}).get("substation_110kv", {}).get("released", False))
    return response


@router.put("/api/handover/review/{building_code}/shared-blocks/110kv")
def handover_review_shared_110kv_save(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, parser, writer, _ = _build_review_services(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "") or "").strip()
    client_id = str(payload.get("client_id", "") or "").strip()
    base_revision = int(payload.get("base_revision", 0) or 0)
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id 不能为空")
    target = _load_target_session_or_404(service, building=building, session_id=session_id)
    batch_key = str(target.get("batch_key", "") or "").strip()
    rows = payload.get("rows", [])
    submitted_110kv = ReviewSessionService.normalize_substation_110kv_payload(
        {"batch_key": batch_key, "rows": rows if isinstance(rows, list) else []},
        batch_key=batch_key,
    )
    try:
        current_110kv = service.get_substation_110kv(batch_key)
        if _substation_110kv_value_signature(submitted_110kv, batch_key=batch_key) == _substation_110kv_value_signature(
            current_110kv,
            batch_key=batch_key,
        ):
            sync_result = {"updated": 0, "failed": 0, "errors": [], "no_change": True}
            lock_state = service.get_substation_110kv_lock(batch_key=batch_key, client_id=client_id)
            container.add_system_log(
                f"[交接班][110KV共享] 内容无变化，跳过保存和容量补写 building={building}, "
                f"batch={batch_key}, revision={current_110kv.get('revision', 0)}"
            )
            return {
                "ok": True,
                "building": building,
                "session": _attach_excel_sync_from_store(container, service.get_session_by_id(session_id) or target),
                "shared_blocks": {"substation_110kv": current_110kv},
                "shared_block_locks": {"substation_110kv": lock_state},
                "capacity_sync_result": sync_result,
                "no_change": True,
            }
        shared_110kv = service.save_substation_110kv(
            batch_key=batch_key,
            building=building,
            client_id=client_id,
            base_revision=base_revision,
            rows=submitted_110kv.get("rows", []),
        )
    except ValueError as exc:
        reason = str(exc)
        if reason == "shared_block_revision_conflict":
            raise HTTPException(status_code=409, detail="110KV变电站内容已被其他楼栋更新，请刷新后重试") from exc
        if reason == "shared_block_lock_required":
            raise HTTPException(status_code=409, detail="110KV变电站正在其他楼栋或终端编辑，请稍后重试") from exc
        raise HTTPException(status_code=400, detail=reason or "110KV变电站保存失败") from exc
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    sync_result = _sync_substation_110kv_to_batch_capacity_reports(
        container=container,
        review_service=service,
        parser=parser,
        writer=writer,
        shared_110kv=shared_110kv,
        emit_log=container.add_system_log,
    )
    lock_state = service.get_substation_110kv_lock(batch_key=batch_key, client_id=client_id)
    container.add_system_log(
        f"[交接班][110KV共享] 保存完成 building={building}, batch={batch_key}, "
        f"revision={shared_110kv.get('revision', 0)}, 容量补写={sync_result.get('updated', 0)}/{sync_result.get('failed', 0)}"
    )
    return {
        "ok": True,
        "building": building,
        "session": _attach_excel_sync_from_store(container, service.get_session_by_id(session_id) or target),
        "shared_blocks": {"substation_110kv": shared_110kv},
        "shared_block_locks": {"substation_110kv": lock_state},
        "capacity_sync_result": sync_result,
    }


@router.post("/api/handover/review/{building_code}/cloud-sync/retry")
def handover_review_retry_cloud_sync(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
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
        service = _build_review_session_service(container)
        followup = _build_review_followup_service(container)
        if session_id:
            result = followup.retry_cloud_sheet_for_session(session_id, emit_log=emit_log)
        else:
            result = followup.retry_cloud_sheet_for_building(building, emit_log=emit_log)
        session = result.get("session") if isinstance(result.get("session"), dict) else target
        session = _attach_excel_sync_from_store(container, session)
        batch_status = (
            result.get("batch_status")
            if isinstance(result.get("batch_status"), dict)
            else service.get_batch_status(str(session.get("batch_key", "")).strip())
        )
        emit_log(
            f"[交接班][云表重试] building={building}, batch={session.get('batch_key', '-')}, 状态={_followup_status_text(result.get('status'))}"
        )
        response = {
            "ok": str(result.get("status", "")).strip().lower() in {"ok", "success"},
            "session": session,
            "batch_status": batch_status,
            "cloud_sheet_sync": result.get("cloud_sheet_sync", {}),
            "status": result.get("status", ""),
            "operation_feedback": _build_review_cloud_retry_feedback(result),
            "latest_session_id": _safe_latest_session_id(service, building=building, emit_log=emit_log),
        }
        return _attach_review_display_state(
            response,
            service=service,
            building=building,
            emit_log=emit_log,
        )

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
    service = _build_review_session_service(container)
    followup = _build_review_followup_service(container)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "")).strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    target = _load_target_session_or_404(service, building=building, session_id=session_id)
    if str(target.get("building", "")).strip() != building:
        raise HTTPException(status_code=400, detail="session building mismatch")
    _ensure_session_lock_held_or_409(
        service,
        building=building,
        session_id=session_id,
        client_id=str(payload.get("client_id", "")).strip(),
    )
    target_batch_key = str(target.get("batch_key", "")).strip()
    with container.job_service.resource_guard(
        name=f"handover_cloud_update:{target_batch_key}:{building}",
        resource_keys=_handover_resource_keys("network:external", batch_key=target_batch_key),
    ):
        result = followup.force_update_cloud_sheet_for_session(session_id, emit_log=container.add_system_log)
        refreshed_session = result.get("session") if isinstance(result.get("session"), dict) else target
        refreshed_session = _attach_excel_sync_from_store(container, refreshed_session)
        _review_history_cache_invalidate(building=building)
        batch_status = (
            result.get("batch_status")
            if isinstance(result.get("batch_status"), dict)
            else service.get_batch_status(str(refreshed_session.get("batch_key", "")).strip())
        )
        response = {
            "ok": str(result.get("status", "")).strip().lower() in {"ok", "success"},
            "status": result.get("status", ""),
            "session": refreshed_session,
            "batch_status": batch_status,
            "cloud_sheet_sync": result.get("cloud_sheet_sync", {}),
            "operation_feedback": _build_review_history_cloud_update_feedback(result),
            "latest_session_id": _safe_latest_session_id(service, building=building, emit_log=container.add_system_log),
        }
        return _attach_review_display_state(
            response,
            service=service,
            building=building,
            client_id=str(payload.get("client_id", "")).strip(),
            emit_log=container.add_system_log,
        )


@router.post("/api/handover/review/batch/{batch_key}/cloud-sync/retry")
def handover_review_retry_cloud_sync_batch(
    batch_key: str,
    request: Request,
) -> Dict[str, Any]:
    container = request.app.state.container

    def _run(emit_log) -> Dict[str, Any]:
        service = _build_review_session_service(container)
        followup = _build_review_followup_service(container)
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
        _review_history_cache_invalidate_sessions(updated_sessions)
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
