from __future__ import annotations

import asyncio
import copy
import inspect
import threading
import time
from datetime import datetime, timedelta
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
from handover_log_module.service.footer_inventory_defaults_service import FooterInventoryDefaultsService
from handover_log_module.service.handover_daily_report_asset_service import HandoverDailyReportAssetService
from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService
from handover_log_module.service.handover_xlsx_write_queue_service import (
    HandoverXlsxWriteQueueService,
    HandoverXlsxWriteQueueTimeoutError,
)
from handover_log_module.service.capacity_report_image_delivery_service import CapacityReportImageDeliveryService
from handover_log_module.service.handover_daily_report_screenshot_service import (
    HandoverDailyReportScreenshotService,
)
from handover_log_module.service.handover_daily_report_state_service import HandoverDailyReportStateService
from handover_log_module.service.handover_110_station_upload_service import Handover110StationUploadService
from handover_log_module.service.event_category_payload_builder import EventCategoryPayloadBuilder
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
_STATION_110_UPLOAD_MAX_BYTES = 50 * 1024 * 1024
_UPLOAD_READ_CHUNK_BYTES = 1024 * 1024
_HEALTH_COMPONENT_CACHE_ATTR = "_health_component_cache"
_HEALTH_COMPONENT_CACHE_LOCK_ATTR = "_health_component_cache_lock"
_HANDOVER_REVIEW_HEALTH_CACHE_STATUS_PREFIX = "handover_review_status:"
_HANDOVER_REVIEW_HEALTH_CACHE_ACCESS_PREFIX = "handover_review_access:"


async def _read_upload_file_limited(file: UploadFile, *, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await file.read(_UPLOAD_READ_CHUNK_BYTES)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(status_code=413, detail="上传文件过大，110站文件最大支持50MB")
        chunks.append(chunk)
    return b"".join(chunks)


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


def _request_runtime_status_refresh(container, *, reason: str) -> None:
    coordinator = getattr(container, "runtime_status_coordinator", None)
    if coordinator is None:
        return
    try:
        request_refresh = getattr(coordinator, "request_refresh", None)
        if callable(request_refresh):
            request_refresh(reason=reason)
    except Exception:
        return


def _publish_handover_review_status_cache(
    request: Request,
    container,
    *,
    service: ReviewSessionService,
    batch_status: Dict[str, Any],
    reason: str,
) -> None:
    status_payload = copy.deepcopy(batch_status if isinstance(batch_status, dict) else {})
    batch_key = str(status_payload.get("batch_key", "") or "").strip()
    if not batch_key:
        return
    if "followup_progress" not in status_payload:
        try:
            status_payload = _attach_followup_progress(
                _build_review_followup_service(container),
                status_payload,
            )
        except Exception:
            pass
    duty_date = str(status_payload.get("duty_date", "") or "").strip()
    duty_shift = str(status_payload.get("duty_shift", "") or "").strip().lower()
    if (not duty_date or not duty_shift) and hasattr(service, "parse_batch_key"):
        duty_date, duty_shift = service.parse_batch_key(batch_key)

    cache_keys: set[str] = set()
    if duty_date and duty_shift:
        cache_keys.add(f"handover_review_status:{duty_date}:{duty_shift}")
    try:
        latest_status = service.get_latest_batch_status()
        if str(latest_status.get("batch_key", "") or "").strip() == batch_key:
            cache_keys.add("handover_review_status:latest")
    except Exception:
        cache_keys.add("handover_review_status:latest")

    app_state = getattr(getattr(request, "app", None), "state", None)
    if app_state is None:
        _request_runtime_status_refresh(container, reason=reason)
        return
    cache = getattr(app_state, _HEALTH_COMPONENT_CACHE_ATTR, None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(app_state, _HEALTH_COMPONENT_CACHE_ATTR, cache)
    cache_lock = getattr(app_state, _HEALTH_COMPONENT_CACHE_LOCK_ATTR, None)

    def _update_cache() -> int:
        removed = 0
        for raw_key in list(cache.keys()):
            key = str(raw_key or "")
            if key.startswith(_HANDOVER_REVIEW_HEALTH_CACHE_ACCESS_PREFIX):
                cache.pop(raw_key, None)
                removed += 1
                continue
            if key.startswith(_HANDOVER_REVIEW_HEALTH_CACHE_STATUS_PREFIX) and key not in cache_keys:
                cache.pop(raw_key, None)
                removed += 1
        entry = {
            "ts": time.monotonic(),
            "value": copy.deepcopy(status_payload),
            "ready": True,
            "refreshing": False,
        }
        for key in cache_keys:
            cache[key] = copy.deepcopy(entry)
        return removed

    try:
        if cache_lock is not None and hasattr(cache_lock, "__enter__"):
            with cache_lock:
                removed_count = _update_cache()
        else:
            removed_count = _update_cache()
        if callable(getattr(container, "add_system_log", None)):
            container.add_system_log(
                f"[交接班][审核状态缓存] 已刷新 batch={batch_key}, keys={len(cache_keys)}, "
                f"removed={removed_count}, reason={reason}"
            )
    except Exception as exc:  # noqa: BLE001
        if callable(getattr(container, "add_system_log", None)):
            container.add_system_log(f"[交接班][审核状态缓存] 刷新失败 batch={batch_key}, error={exc}")
    finally:
        _request_runtime_status_refresh(container, reason=reason)


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
        "document_revision": int(resolved_signature.get("revision", 0) or 0),
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


def _get_substation_110kv_state_safe(
    service: ReviewSessionService,
    *,
    batch_key: str,
    client_id: str = "",
    emit_log=None,
) -> Dict[str, Any]:
    getter = getattr(service, "get_substation_110kv_state", None)
    if not callable(getter):
        return {"shared_blocks": {}, "shared_block_locks": {}}
    try:
        state = getter(
            batch_key=str(batch_key or "").strip(),
            client_id=str(client_id or "").strip(),
        )
    except ReviewSessionStoreUnavailableError as exc:
        if callable(emit_log):
            emit_log(f"[交接班][110KV共享] 状态读取失败: batch={batch_key}, error={exc}")
        return {"shared_blocks": {}, "shared_block_locks": {}}
    return state if isinstance(state, dict) else {"shared_blocks": {}, "shared_block_locks": {}}


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


def _build_station_110_upload_service(container) -> Handover110StationUploadService:
    return Handover110StationUploadService(_handover_cfg(container))


def _build_review_ui_config(container) -> Dict[str, Any]:
    if _build_review_services is not _ORIGINAL_BUILD_REVIEW_SERVICES:
        _, parser, _, _ = _build_review_services(container)
        parser_config = getattr(parser, "config", {}) if parser is not None else {}
        review_ui = parser_config.get("review_ui", {}) if isinstance(parser_config, dict) else {}
        return review_ui if isinstance(review_ui, dict) else {}
    handover_cfg = _handover_cfg(container)
    review_ui = handover_cfg.get("review_ui", {}) if isinstance(handover_cfg, dict) else {}
    return review_ui if isinstance(review_ui, dict) else {}


def _fixed_cell_value_from_review_document(document: Dict[str, Any] | None, cell_name: str) -> str:
    target = str(cell_name or "").strip().upper()
    if not target or not isinstance(document, dict):
        return ""
    fixed_blocks = document.get("fixed_blocks", [])
    if not isinstance(fixed_blocks, list):
        return ""
    for block in fixed_blocks:
        fields = block.get("fields", []) if isinstance(block, dict) else []
        if not isinstance(fields, list):
            continue
        for field in fields:
            if not isinstance(field, dict):
                continue
            if str(field.get("cell", "") or "").strip().upper() != target:
                continue
            return str(field.get("value", "") or "").strip()
    return ""


def _is_current_handover_duty_context(*, duty_date: str, duty_shift: str) -> bool:
    cursor = datetime.now()
    second_of_day = cursor.hour * 3600 + cursor.minute * 60 + cursor.second
    if second_of_day < 9 * 3600:
        current_date = (cursor.date() - timedelta(days=1)).strftime("%Y-%m-%d")
        current_shift = "night"
    elif second_of_day < 18 * 3600:
        current_date = cursor.strftime("%Y-%m-%d")
        current_shift = "day"
    else:
        current_date = cursor.strftime("%Y-%m-%d")
        current_shift = "night"
    return (
        str(duty_date or "").strip() == current_date
        and str(duty_shift or "").strip().lower() == current_shift
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


def _review_document_revision_from_store(container, session: Dict[str, Any] | None, *, fallback: int = 0) -> int:
    payload = session if isinstance(session, dict) else {}
    fallback_revision = int(fallback or payload.get("document_revision", 0) or payload.get("revision", 0) or 0)
    building = str(payload.get("building", "") or "").strip()
    session_id = str(payload.get("session_id", "") or "").strip()
    if not building or not session_id:
        return fallback_revision
    try:
        state = ReviewBuildingDocumentStore(
            config=_handover_cfg(container),
            building=building,
        ).get_document(session_id)
    except Exception:  # noqa: BLE001
        return fallback_revision
    if not isinstance(state, dict):
        return fallback_revision
    try:
        return int(state.get("revision", fallback_revision) or fallback_revision)
    except Exception:  # noqa: BLE001
        return fallback_revision


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


def _build_followup_queued_result(
    followup: ReviewFollowupTriggerService,
    *,
    batch_key: str,
    job=None,
) -> Dict[str, Any]:
    target_batch = str(batch_key or "").strip()
    job_payload = job.to_dict() if hasattr(job, "to_dict") else dict(job or {})
    return {
        "status": "queued",
        "batch_key": target_batch,
        "uploaded_buildings": [],
        "skipped_buildings": [],
        "failed_buildings": [],
        "details": {},
        "blocked_reason": "",
        "cloud_sheet_sync": {
            "status": "queued",
            "uploaded_buildings": [],
            "skipped_buildings": [],
            "failed_buildings": [],
            "details": {},
            "blocked_reason": "",
        },
        "daily_report_record_export": {
            "status": "queued",
            "error": "",
        },
        "followup_progress": followup.get_followup_progress(target_batch) if target_batch else _empty_followup_progress(),
        "job": job_payload,
    }


def _build_followup_await_all_result(
    followup: ReviewFollowupTriggerService,
    *,
    batch_key: str,
    blocked_reason: str = "",
) -> Dict[str, Any]:
    target_batch = str(batch_key or "").strip()
    reason = str(blocked_reason or "").strip() or "暂无已确认楼栋"
    return {
        "status": "await_all_confirmed",
        "batch_key": target_batch,
        "uploaded_buildings": [],
        "skipped_buildings": [],
        "failed_buildings": [],
        "details": {},
        "blocked_reason": reason,
        "cloud_sheet_sync": {
            "status": "await_all_confirmed",
            "uploaded_buildings": [],
            "skipped_buildings": [],
            "failed_buildings": [],
            "details": {},
            "blocked_reason": reason,
        },
        "daily_report_record_export": {
            "status": "idle",
            "error": "",
        },
        "followup_progress": followup.get_followup_progress(target_batch) if target_batch else _empty_followup_progress(),
    }


def _followup_status_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "ok": "成功",
        "success": "成功",
        "queued": "已提交",
        "failed": "失败",
        "partial_failed": "部分失败",
        "blocked": "已阻塞",
        "skipped": "已跳过",
        "disabled": "已禁用",
        "ready_for_external": "等待外网继续",
        "pending_review": "待确认后上传",
        "pending_upload": "待上传",
        "uploading": "上传中",
        "syncing": "同步中",
    }
    return mapping.get(text, text or "-")


def _start_handover_followup_job_after_confirm(
    container,
    *,
    batch_key: str,
    building: str = "",
    session_id: str = "",
    submitted_by: str = "confirm",
    dedupe_key: str = "",
    task_label: str = "确认后上传",
) -> Dict[str, Any]:
    target_batch = str(batch_key or "").strip()
    target_building = str(building or "").strip()
    target_session_id = str(session_id or "").strip()
    label_text = str(task_label or "").strip() or "确认后上传"
    followup = _build_review_followup_service(container)
    if not target_batch:
        return _build_followup_failure_result(followup, batch_key=target_batch, error="batch_key 不能为空")

    def _run(emit_log) -> Dict[str, Any]:
        service = _build_review_followup_service(container)
        scope_text = f"building={target_building}" if target_building else "batch=all"
        emit_log(f"[交接班][{label_text}] 后台任务开始 batch={target_batch}, {scope_text}")
        if target_building:
            result = service.trigger_after_single_confirm(
                batch_key=target_batch,
                building=target_building,
                session_id=target_session_id,
                emit_log=emit_log,
            )
        else:
            result = service.continue_batch(target_batch, emit_log=emit_log)
        emit_log(
            f"[交接班][{label_text}] 后台任务完成 batch={target_batch}, "
            f"{scope_text}, "
            f"状态={_followup_status_text(result.get('status'))}, "
            f"已上传={len(result.get('uploaded_buildings', []) or [])}, "
            f"已失败={len(result.get('failed_buildings', []) or [])}, "
            f"云表状态={_followup_status_text((result.get('cloud_sheet_sync', {}) or {}).get('status', '-'))}"
        )
        return result

    job = _start_handover_background_job(
        container,
        name=f"交接班{label_text}-{target_batch}{('-' + target_building) if target_building else ''}",
        run_func=_run,
        worker_handler="handover_followup_continue",
        worker_payload={
            "batch_key": target_batch,
            "building": target_building,
            "session_id": target_session_id,
        },
        resource_keys=["network:external", f"handover_followup:{target_batch}"],
        priority="manual",
        feature="handover_followup_continue",
        submitted_by=submitted_by,
        dedupe_key=str(dedupe_key or "").strip()
        or f"handover_followup_continue:{target_batch}:{target_building or 'batch'}:{target_session_id or '-'}",
    )
    job_id = str(getattr(job, "job_id", "") or (job.get("job_id", "") if isinstance(job, dict) else "")).strip()
    container.add_system_log(
        f"[任务] 已提交: 交接班{label_text} batch={target_batch}, building={target_building or '全部'} ({job_id or '-'})"
    )
    return _build_followup_queued_result(followup, batch_key=target_batch, job=job)


def _maybe_start_handover_followup_job_after_review_save(
    container,
    *,
    followup: ReviewFollowupTriggerService,
    session: Dict[str, Any],
    batch_status: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(session, dict) or not bool(session.get("confirmed", False)):
        return {}
    target_batch = str(session.get("batch_key", "") or "").strip()
    if not target_batch:
        return {}
    cloud_state = ReviewSessionService._normalize_cloud_sheet_sync(session.get("cloud_sheet_sync", {}))
    if str(cloud_state.get("status", "")).strip().lower() == "disabled":
        return {}
    revision = int(session.get("revision", 0) or 0)
    synced_revision = int(cloud_state.get("synced_revision", 0) or 0)
    if str(cloud_state.get("status", "")).strip().lower() == "success" and synced_revision == revision:
        return {}
    session_id = str(session.get("session_id", "") or "").strip()
    building = str(session.get("building", "") or "").strip()
    dedupe_key = f"handover_followup_continue:{target_batch}:review_save:{session_id or building}:{revision}"
    try:
        result = _start_handover_followup_job_after_confirm(
            container,
            batch_key=target_batch,
            building=building,
            session_id=session_id,
            submitted_by="review_save",
            dedupe_key=dedupe_key,
            task_label="审核保存后云文档重传",
        )
        container.add_system_log(
            f"[交接班][审核保存后上传] 已排队云文档重传: "
            f"batch={target_batch}, building={building or '-'}, revision={revision}"
        )
        return result
    except Exception as exc:  # noqa: BLE001
        container.add_system_log(
            f"[交接班][审核保存后上传] 任务提交失败，已保留待重传状态: "
            f"batch={target_batch}, building={building or '-'}, revision={revision}, 错误={exc}"
        )
        return _build_followup_failure_result(followup, batch_key=target_batch, error=str(exc))


def _daily_report_stage_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "summary_sheet": "日报截图",
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
    dedupe_key: str = "",
):
    job_service = container.job_service
    if worker_handler and hasattr(job_service, "start_worker_job"):
        kwargs = {
            "name": name,
            "worker_handler": worker_handler,
            "worker_payload": worker_payload or {},
            "resource_keys": resource_keys,
            "priority": priority,
            "feature": feature,
            "submitted_by": submitted_by,
        }
        try:
            if "dedupe_key" in inspect.signature(job_service.start_worker_job).parameters:
                kwargs["dedupe_key"] = dedupe_key
        except (TypeError, ValueError):
            kwargs["dedupe_key"] = dedupe_key
        return job_service.start_worker_job(**kwargs)
    kwargs = {
        "name": name,
        "run_func": run_func,
        "resource_keys": resource_keys,
        "priority": priority,
        "feature": feature,
        "submitted_by": submitted_by,
    }
    try:
        if "dedupe_key" in inspect.signature(job_service.start_job).parameters:
            kwargs["dedupe_key"] = dedupe_key
    except (TypeError, ValueError):
        kwargs["dedupe_key"] = dedupe_key
    return job_service.start_job(**kwargs)


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
    if target_text != "summary_sheet":
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
        return "未找到日报截图页面，请确认页面可正常访问。"
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
            "capacity_room_inputs": True,
        }
    return {
        "fixed_blocks": bool(raw.get("fixed_blocks")),
        "sections": bool(raw.get("sections")),
        "footer_inventory": bool(raw.get("footer_inventory")),
        "cooling_pump_pressures": bool(raw.get("cooling_pump_pressures")),
        "capacity_room_inputs": bool(raw.get("capacity_room_inputs")),
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
    return HandoverCapacityReportService.extract_tracked_cells_from_review_document(
        document if isinstance(document, dict) else {}
    )


def _should_sync_capacity_after_review_save(
    *,
    previous_session: Dict[str, Any] | None,
    dirty_regions: Dict[str, bool],
    tracked_cells: Dict[str, str],
) -> bool:
    dirty = dirty_regions or {}
    if (
        not bool(dirty.get("fixed_blocks"))
        and not bool(dirty.get("cooling_pump_pressures"))
        and not bool(dirty.get("capacity_room_inputs"))
    ):
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
    tracked_cells = _extract_capacity_tracked_cells(document)
    if not _should_sync_capacity_after_review_save(
        previous_session=previous_session,
        dirty_regions=dirty_regions,
        tracked_cells=tracked_cells,
    ):
        return saved_session
    return _sync_capacity_overlay_for_saved_session(
        container=container,
        review_service=review_service,
        saved_session=saved_session,
        tracked_cells=tracked_cells,
        shared_110kv=(
            review_service.get_substation_110kv_state(
                batch_key=str(saved_session.get("batch_key", "")).strip(),
            ).get("shared_blocks", {}).get("substation_110kv", {})
            if str(saved_session.get("batch_key", "")).strip()
            else {}
        ),
        cooling_pump_pressures=(
            document.get("cooling_pump_pressures", {})
            if isinstance(document.get("cooling_pump_pressures", {}), dict)
            else {}
        ),
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


def _sync_capacity_overlay_for_saved_session(
    *,
    container,
    review_service: ReviewSessionService,
    saved_session: Dict[str, Any],
    tracked_cells: Dict[str, str],
    shared_110kv: Dict[str, Any] | None = None,
    cooling_pump_pressures: Dict[str, Any] | None = None,
    client_id: str = "",
) -> Dict[str, Any]:
    if not callable(getattr(review_service, "update_capacity_sync", None)):
        return saved_session
    runtime_cfg = getattr(container, "runtime_config", None)
    if not isinstance(runtime_cfg, dict):
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
        shared_110kv=shared_110kv,
        cooling_pump_pressures=cooling_pump_pressures,
        client_id=client_id,
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


def _build_xlsx_write_queue_service(
    container,
    *,
    review_service: ReviewSessionService | None = None,
    document_state: ReviewDocumentStateService | None = None,
    parser: ReviewDocumentParser | None = None,
    writer: ReviewDocumentWriter | None = None,
) -> HandoverXlsxWriteQueueService:
    return HandoverXlsxWriteQueueService(
        _handover_cfg(container),
        review_service=review_service,
        document_state=document_state,
        parser=parser,
        writer=writer,
        emit_log=container.add_system_log,
    )


def _first_existing_path(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        try:
            if Path(text).exists():
                return text
        except Exception:  # noqa: BLE001
            continue
    return ""


def _first_cached_file(entries: Any, building: str) -> str:
    target = str(building or "").strip()
    rows = entries if isinstance(entries, list) else []
    for item in rows:
        if not isinstance(item, dict):
            continue
        if str(item.get("building", "") or "").strip() != target:
            continue
        path_text = _first_existing_path(item.get("file_path"))
        if path_text:
            return path_text
    return ""


def _derive_capacity_source_from_handover_source(handover_source: Any) -> str:
    text = str(handover_source or "").strip()
    if not text:
        return ""
    candidates: list[str] = []
    if "交接班日志源文件" in text:
        candidates.append(text.replace("交接班日志源文件", "交接班容量报表源文件"))
    try:
        path = Path(text)
        name = path.name.replace("交接班日志源文件", "交接班容量报表源文件")
        parent_parts = [
            "交接班容量报表源文件" if part == "交接班日志源文件" else part
            for part in path.parent.parts
        ]
        if parent_parts:
            candidates.append(str(Path(*parent_parts) / name))
    except Exception:  # noqa: BLE001
        pass
    return _first_existing_path(*candidates)


def _resolve_regenerate_source_files(container, session: Dict[str, Any], *, building: str) -> tuple[str, str]:
    duty_date = str(session.get("duty_date", "") or "").strip()
    duty_shift = str(session.get("duty_shift", "") or "").strip().lower()
    source_cache = session.get("source_file_cache", {}) if isinstance(session.get("source_file_cache", {}), dict) else {}
    handover_source = _first_existing_path(
        session.get("data_file"),
        source_cache.get("stored_path"),
    )
    capacity_source = _first_existing_path(session.get("capacity_source_file"))
    capacity_source = capacity_source or _derive_capacity_source_from_handover_source(handover_source)
    bridge_service = getattr(container, "shared_bridge_service", None)
    if bridge_service is not None and duty_date and duty_shift and (not handover_source or not capacity_source):
        try:
            if not handover_source:
                handover_source = _first_cached_file(
                    bridge_service.get_handover_by_date_cache_entries(
                        duty_date=duty_date,
                        duty_shift=duty_shift,
                        buildings=[building],
                    ),
                    building,
                )
        except Exception as exc:  # noqa: BLE001
            container.add_system_log(f"[交接班][审核重生成] 读取交接班共享缓存失败 building={building}, error={exc}")
        try:
            if not capacity_source:
                capacity_source = _first_cached_file(
                    bridge_service.get_handover_capacity_by_date_cache_entries(
                        duty_date=duty_date,
                        duty_shift=duty_shift,
                        buildings=[building],
                    ),
                    building,
                )
        except Exception as exc:  # noqa: BLE001
            container.add_system_log(f"[交接班][审核重生成] 读取容量共享缓存失败 building={building}, error={exc}")
    return handover_source, capacity_source


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
    if not _should_sync_capacity_after_review_save(
        previous_session=previous_session,
        dirty_regions=dirty_regions,
        tracked_cells=tracked_cells,
    ):
        return saved_session, False
    pending_payload = _build_pending_capacity_sync_payload(tracked_cells)
    updated_session = review_service.update_capacity_sync(
        session_id=str(saved_session.get("session_id", "")).strip(),
        capacity_sync=pending_payload,
        capacity_status="pending",
        capacity_error="",
    )
    queue_service = _build_xlsx_write_queue_service(container, review_service=review_service)
    queue_service.enqueue_capacity_overlay_sync(
        updated_session,
        tracked_cells=copy.deepcopy(tracked_cells),
    )
    return updated_session, True


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
        "capacity_room_rows": int(persisted.get("capacity_room_rows", 0) or 0),
        "cooling_pump_pressure_rows": int(persisted.get("cooling_pump_pressure_rows", 0) or 0),
        "attention_handover_rows": int(persisted.get("attention_handover_rows", 0) or 0),
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


def _parse_hms_seconds(value: Any, fallback: str) -> int:
    text = str(value or fallback or "").strip()
    parts = text.split(":")
    try:
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
        second = int(parts[2]) if len(parts) > 2 else 0
    except Exception:  # noqa: BLE001
        hour, minute, second = [int(part) for part in str(fallback or "00:00:00").split(":")[:3]]
    hour = max(0, min(23, hour))
    minute = max(0, min(59, minute))
    second = max(0, min(59, second))
    return hour * 3600 + minute * 60 + second


def _current_handover_duty_context(config: Dict[str, Any] | None = None, now: datetime | None = None) -> tuple[str, str]:
    _ = config
    day_start = _parse_hms_seconds("09:00:00", "09:00:00")
    night_start = _parse_hms_seconds("18:00:00", "18:00:00")
    current = now or datetime.now()
    second_of_day = current.hour * 3600 + current.minute * 60 + current.second
    if second_of_day < day_start:
        return (current - timedelta(days=1)).strftime("%Y-%m-%d"), "night"
    if second_of_day < night_start:
        return current.strftime("%Y-%m-%d"), "day"
    return current.strftime("%Y-%m-%d"), "night"


def _resolve_review_duty_context(
    service: ReviewSessionService,
    *,
    duty_date: str = "",
    duty_shift: str = "",
    session_id: str = "",
    config: Dict[str, Any] | None = None,
) -> tuple[str, str]:
    if str(session_id or "").strip():
        return "", ""
    duty_date_text, duty_shift_text = _normalize_duty_context(duty_date, duty_shift)
    if duty_date_text and duty_shift_text:
        return duty_date_text, duty_shift_text
    return _current_handover_duty_context(config or getattr(service, "config", {}) or {})


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


def _pending_review_payload(
    service: ReviewSessionService,
    *,
    building: str,
    duty_date: str,
    duty_shift: str,
    review_ui: Dict[str, Any] | None = None,
    emit_log=None,
) -> Dict[str, Any]:
    batch_key = service.build_batch_key(duty_date, duty_shift)
    try:
        batch_status = service.get_batch_status(batch_key)
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    latest_session_id = _safe_latest_session_id(service, building=building, emit_log=emit_log)
    shift_text = _shift_label(duty_shift)
    message = f"{duty_date} {shift_text}交接班数据尚未生成，请在数据生成后再来查看。"
    response = {
        "ok": True,
        "building": building,
        "session": None,
        "document": {},
        "batch_status": batch_status,
        "concurrency": _empty_concurrency(),
        "review_ui": review_ui if isinstance(review_ui, dict) else {},
        "history": _empty_history_payload(
            latest_session_id=latest_session_id,
            selected_session_id="",
        ),
        "latest_session_id": latest_session_id,
        "review_context": {
            "status": "waiting_generation",
            "ready": False,
            "duty_date": duty_date,
            "duty_shift": duty_shift,
            "duty_shift_text": shift_text,
            "batch_key": batch_key,
            "message": message,
        },
    }
    response = _attach_review_display_state(
        response,
        service=service,
        building=building,
        include_concurrency=False,
    )
    display_state = response.get("display_state", {}) if isinstance(response.get("display_state", {}), dict) else {}
    banners = list(display_state.get("status_banners", []) if isinstance(display_state.get("status_banners", []), list) else [])
    banners.append({"code": "waiting_generation", "text": message, "tone": "warning"})
    display_state["status_banners"] = banners
    response["display_state"] = display_state
    return response


def _load_review_session_or_pending(
    service: ReviewSessionService,
    *,
    building: str,
    duty_date: str = "",
    duty_shift: str = "",
    session_id: str = "",
    config: Dict[str, Any] | None = None,
    review_ui: Dict[str, Any] | None = None,
    emit_log=None,
) -> tuple[Dict[str, Any] | None, Dict[str, Any] | None]:
    resolved_date, resolved_shift = _resolve_review_duty_context(
        service,
        duty_date=duty_date,
        duty_shift=duty_shift,
        session_id=session_id,
        config=config,
    )
    try:
        session = _load_target_session_or_404(
            service,
            building=building,
            duty_date=resolved_date or duty_date,
            duty_shift=resolved_shift or duty_shift,
            session_id=session_id,
        )
        return session, None
    except HTTPException as exc:
        if (
            exc.status_code == 404
            and not str(session_id or "").strip()
            and resolved_date
            and resolved_shift
        ):
            return None, _pending_review_payload(
                service,
                building=building,
                duty_date=resolved_date,
                duty_shift=resolved_shift,
                review_ui=review_ui,
                emit_log=emit_log,
            )
        raise


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


def _present_review_cloud_sheet_state(raw: Any, *, revision: int = 0) -> Dict[str, Any]:
    payload = ReviewSessionService._normalize_cloud_sheet_sync(raw)
    status = str(payload.get("status", "")).strip().lower()
    url = str(payload.get("spreadsheet_url", "")).strip()
    error = str(payload.get("error", "")).strip()
    attempted = bool(payload.get("attempted"))
    synced_revision = int(payload.get("synced_revision", 0) or 0)
    last_attempt_revision = int(payload.get("last_attempt_revision", 0) or 0)
    current_revision = int(revision or 0)
    cloud_revision_stale = (
        current_revision > 0
        and synced_revision > 0
        and synced_revision < current_revision
        and status in {"success", "pending_upload"}
    )
    text = "云表未执行"
    tone = "neutral"
    reason_code = status or "idle"
    if status == "success" and cloud_revision_stale:
        text = "云表内容已修改，待重新上传"
        tone = "warning"
        reason_code = "stale"
    elif status == "success":
        text = "云表已同步"
        tone = "success"
    elif status in {"uploading", "syncing"}:
        text = "云表上传中"
        tone = "info"
    elif status == "pending_upload":
        text = "云表内容已修改，待重新上传" if cloud_revision_stale else "云表待最终上传"
        tone = "warning"
        reason_code = "stale_pending_upload" if cloud_revision_stale else reason_code
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
        "synced_revision": synced_revision,
        "last_attempt_revision": last_attempt_revision,
        "current_revision": current_revision,
    }


def _ensure_cloud_sheet_not_uploading_or_409(session: Dict[str, Any], *, action: str = "操作确认状态") -> None:
    cloud_sync = session.get("cloud_sheet_sync", {}) if isinstance(session, dict) else {}
    if not isinstance(cloud_sync, dict):
        cloud_sync = {}
    status = str(cloud_sync.get("status", "") or "").strip().lower()
    if status in {"uploading", "syncing"}:
        raise HTTPException(status_code=409, detail=f"当前楼栋云文档上传中，请等待上传完成后再{action}")


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
) -> Dict[str, Any]:
    return {
        "allowed": bool(allowed),
        "visible": bool(visible),
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
            text="等待当前楼栋确认",
            tone="info",
            reason_code="await_all_confirmed",
            detail_text="当前楼栋尚未确认，确认后会上传当前楼栋云文档。",
        )
    if followup_status == "queued":
        return _review_display_item(
            status="followup_queued",
            text="已确认当前楼栋，后续上传任务已提交",
            tone="success",
            reason_code="followup_queued",
            detail_text="当前楼栋确认完成，云文档上传将在后台继续执行。",
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
        if blocked_reason == "pending_review":
            blocked_reason = "当前楼栋尚未确认，不能重试云表上传。"
        return _review_display_item(
            status="blocked",
            text=blocked_reason or "当前楼栋暂不能重试云表上传。",
            tone="warning",
            reason_code="blocked",
            detail_text=blocked_reason or "当前楼栋暂不能重试云表上传。",
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
    try:
        server_revision = int(
            session_payload.get("document_revision")
            or session_payload.get("snapshot_revision")
            or session_payload.get("revision", 0)
            or 0
        )
    except Exception:  # noqa: BLE001
        server_revision = 0
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
    cloud_sheet_state = _present_review_cloud_sheet_state(
        session_payload.get("cloud_sheet_sync", {}),
        revision=int(session_payload.get("revision", 0) or 0),
    )
    cloud_sheet_uploading = str(cloud_sheet_state["status"]).strip().lower() in {"uploading", "syncing"}
    excel_sync_state = _present_review_excel_sync_state(session_payload.get("excel_sync", {}))
    capacity_state = _present_review_capacity_state(session_payload.get("capacity_sync", {}))
    capacity_image_delivery = (
        session_payload.get("capacity_image_delivery", {})
        if isinstance(session_payload.get("capacity_image_delivery", {}), dict)
        else {}
    )
    capacity_image_sending = str(capacity_image_delivery.get("status", "") or "").strip().lower() == "sending"
    confirmed = bool(session_payload.get("confirmed", False))
    has_output_file = bool(str(session_payload.get("output_file", "")).strip())
    has_capacity_file = bool(str(session_payload.get("capacity_output_file", "")).strip())
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
    elif cloud_sheet_uploading:
        status_banners.append(
            {
                "code": "cloud_sheet_uploading",
                "text": "当前楼栋云文档上传中，上传完成前不能修改确认状态。",
                "tone": "info",
            }
        )
    elif bool(session_payload.get("confirmed", False)) and str(cloud_sheet_state.get("reason_code", "")).strip() in {
        "stale",
        "stale_pending_upload",
    }:
        status_banners.append(
            {
                "code": "cloud_sheet_stale",
                "text": "审核内容已修改，云文档需要重新上传；系统会在空闲时自动处理。",
                "tone": "warning",
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
    confirm_allowed = bool(session_payload) and not is_history_mode and not remote_editor_active and not cloud_sheet_uploading
    confirm_disabled_reason = ""
    if not session_payload:
        confirm_disabled_reason = "暂无可确认的交接班记录"
    elif is_history_mode:
        confirm_disabled_reason = "仅最新交接班日志支持确认、撤销确认和云表重试"
    elif remote_editor_active:
        confirm_disabled_reason = "当前审核页正在其他终端编辑，请等待或刷新后重试"
    elif cloud_sheet_uploading:
        confirm_disabled_reason = "当前楼栋云文档上传中，请等待上传完成后再操作确认状态"
    retry_allowed = bool(session_payload) and (not is_history_mode) and confirmed and str(cloud_sheet_state["status"]) in {"failed", "prepare_failed"}
    retry_disabled_reason = ""
    if not session_payload:
        retry_disabled_reason = "暂无可重试的交接班记录"
    elif is_history_mode:
        retry_disabled_reason = "历史模式不支持重试当前云表上传"
    elif not confirmed:
        retry_disabled_reason = "当前楼栋尚未确认，不能重试云表上传"
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
    capacity_allowed = bool(session_payload) and has_capacity_file
    capacity_disabled_reason = ""
    if not session_payload:
        capacity_disabled_reason = "当前没有可下载的交接班容量报表"
    elif not has_capacity_file:
        capacity_disabled_reason = "当前没有可下载的交接班容量报表"
    elif str(capacity_state["status"]) != "ready":
        capacity_disabled_reason = ""
    capacity_image_send_allowed = bool(session_payload) and has_capacity_file and not is_history_mode and not remote_editor_active and not capacity_image_sending
    capacity_image_send_disabled_reason = ""
    if not session_payload:
        capacity_image_send_disabled_reason = "当前没有可发送的容量报表"
    elif is_history_mode:
        capacity_image_send_disabled_reason = "历史交接班日志不支持发送容量表图片"
    elif remote_editor_active:
        capacity_image_send_disabled_reason = "当前审核页正在其他终端编辑，请等待或刷新后重试"
    elif not has_capacity_file:
        capacity_image_send_disabled_reason = "当前没有可发送的容量报表"
    elif capacity_image_sending:
        capacity_image_send_disabled_reason = "容量表图片正在发送中，请等待发送完成"
    regenerate_allowed = bool(session_payload) and not is_history_mode and not remote_editor_active and not cloud_sheet_uploading and not confirmed
    regenerate_disabled_reason = ""
    if not session_payload:
        regenerate_disabled_reason = "暂无可重新生成的交接班记录"
    elif is_history_mode:
        regenerate_disabled_reason = "历史交接班日志不支持重新生成"
    elif remote_editor_active:
        regenerate_disabled_reason = "当前审核页正在其他终端编辑，请等待或刷新后重试"
    elif cloud_sheet_uploading:
        regenerate_disabled_reason = "当前楼栋云文档上传中，请等待上传完成后再重新生成"
    elif confirmed:
        regenerate_disabled_reason = "当前楼栋已确认，请先撤销确认后再重新生成"
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
        status="ready" if capacity_allowed and str(capacity_state["status"]) == "ready" else ("needs_sync" if capacity_allowed else (str(capacity_state["status"]) or ("missing_file" if session_payload else "unavailable"))),
        text="容量报表可下载" if capacity_allowed and str(capacity_state["status"]) == "ready" else ("容量报表可下载，下载前会同步最新字段" if capacity_allowed else (capacity_disabled_reason or capacity_state["text"])),
        tone="success" if capacity_allowed and str(capacity_state["status"]) == "ready" else ("warning" if capacity_allowed else str(capacity_state["tone"] or "warning")),
        reason_code="ready" if capacity_allowed and str(capacity_state["status"]) == "ready" else ("needs_sync" if capacity_allowed else (str(capacity_state["reason_code"]) or ("missing_file" if session_payload else "unavailable"))),
        detail_text="" if capacity_allowed and str(capacity_state["status"]) == "ready" else ("点击下载时会先补写审核页字段，失败则不会下载旧文件" if capacity_allowed else (capacity_disabled_reason or capacity_state["error"] or capacity_state["text"])),
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
                visible=bool(session_payload) and not is_history_mode,
                label="发送容量表图片",
                disabled_reason=capacity_image_send_disabled_reason,
                tone="neutral",
                variant="secondary",
            ),
            "regenerate": _review_action(
                allowed=regenerate_allowed,
                visible=bool(session_payload) and not is_history_mode,
                label="重新生成交接班及容量表",
                disabled_reason=regenerate_disabled_reason,
                tone="warning",
                variant="warning",
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
    session_revision = int(session.get("revision", 0) or 0)
    try:
        document_revision = int(
            response.get("document_revision")
            or response.get("snapshot_revision")
            or session.get("document_revision")
            or session.get("snapshot_revision")
            or session_revision
            or 0
        )
    except Exception:  # noqa: BLE001
        document_revision = session_revision
    current_revision = int(document_revision or session_revision or 0)
    session = dict(session)
    session["document_revision"] = current_revision
    session["session_revision"] = session_revision
    response["session"] = session
    response["document_revision"] = current_revision
    response["snapshot_revision"] = current_revision
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
    if isinstance(concurrency, dict):
        concurrency = dict(concurrency)
        concurrency["current_revision"] = current_revision
    latest_session_id = str(response.get("latest_session_id", "") or "").strip()
    if not latest_session_id:
        latest_session_id = _safe_latest_session_id(service, building=building, emit_log=emit_log)
    if latest_session_id:
        response["latest_session_id"] = latest_session_id
    if include_concurrency:
        response["concurrency"] = concurrency
    batch_key = str(session.get("batch_key", "") or "").strip()
    if batch_key:
        try:
            outdoor_state = service.get_outdoor_temperature_state(
                batch_key=batch_key,
                client_id=str(client_id or "").strip(),
                preferred_document=(
                    response.get("document", {})
                    if isinstance(response.get("document", {}), dict)
                    else {}
                ),
                preferred_session=session,
            )
        except ReviewSessionStoreUnavailableError as exc:
            _raise_review_store_http_error(exc)
        except Exception as exc:  # noqa: BLE001
            if callable(emit_log):
                emit_log(f"[交接班][室外温湿度共享] 读取共享状态失败: batch={batch_key}, error={exc}")
            outdoor_state = {"shared_blocks": {}, "shared_block_locks": {}}
        outdoor_blocks = outdoor_state.get("shared_blocks", {}) if isinstance(outdoor_state, dict) else {}
        outdoor_locks = outdoor_state.get("shared_block_locks", {}) if isinstance(outdoor_state, dict) else {}
        outdoor_block = (
            outdoor_blocks.get("outdoor_temperature", {})
            if isinstance(outdoor_blocks, dict)
            else {}
        )
        if isinstance(response.get("document", {}), dict) and isinstance(outdoor_block, dict):
            response["document"], _changed = ReviewSessionService.apply_outdoor_temperature_to_document(
                copy.deepcopy(response.get("document", {})),
                outdoor_block.get("cells", {}) if isinstance(outdoor_block.get("cells", {}), dict) else {},
            )
        shared_state = _get_substation_110kv_state_safe(
            service,
            batch_key=batch_key,
            client_id=str(client_id or "").strip(),
            emit_log=emit_log,
        )
        shared_blocks = shared_state.get("shared_blocks", {}) if isinstance(shared_state, dict) else {}
        shared_locks = shared_state.get("shared_block_locks", {}) if isinstance(shared_state, dict) else {}
        response["shared_blocks"] = {
            **(response.get("shared_blocks", {}) if isinstance(response.get("shared_blocks", {}), dict) else {}),
            **(outdoor_blocks if isinstance(outdoor_blocks, dict) else {}),
            **(shared_blocks if isinstance(shared_blocks, dict) else {}),
        }
        response["shared_block_locks"] = {
            **(
                response.get("shared_block_locks", {})
                if isinstance(response.get("shared_block_locks", {}), dict)
                else {}
            ),
            **(outdoor_locks if isinstance(outdoor_locks, dict) else {}),
            **(shared_locks if isinstance(shared_locks, dict) else {}),
        }
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
    if str(building_code or "").strip().lower() != "110":
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


@router.get("/api/handover/review/110-station/status")
def handover_review_110_station_status(
    request: Request,
    duty_date: str = "",
    duty_shift: str = "",
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_station_110_upload_service(container)
    try:
        return service.status(duty_date=duty_date, duty_shift=duty_shift)
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/handover/review/110-station/parse")
async def handover_review_110_station_parse(
    request: Request,
    duty_date: str = Form(default=""),
    duty_shift: str = Form(default=""),
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    content = await _read_upload_file_limited(file, max_bytes=_STATION_110_UPLOAD_MAX_BYTES)
    container = request.app.state.container
    service = _build_station_110_upload_service(container)
    try:
        return service.parse(
            duty_date=duty_date,
            duty_shift=duty_shift,
            filename=str(file.filename or "").strip(),
            content=content,
            emit_log=container.add_system_log,
        )
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        container.add_system_log(f"[交接班][110站解析] 失败: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/api/handover/review/110-station/upload")
async def handover_review_110_station_upload(
    request: Request,
    duty_date: str = Form(default=""),
    duty_shift: str = Form(default=""),
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    content = await _read_upload_file_limited(file, max_bytes=_STATION_110_UPLOAD_MAX_BYTES)
    container = request.app.state.container
    service = _build_station_110_upload_service(container)
    try:
        return service.upload(
            duty_date=duty_date,
            duty_shift=duty_shift,
            filename=str(file.filename or "").strip(),
            content=content,
            emit_log=container.add_system_log,
        )
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        container.add_system_log(f"[交接班][110站上传] 失败: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/api/handover/review/110-station/cloud-sync/retry")
def handover_review_110_station_cloud_sync_retry(
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    body = payload if isinstance(payload, dict) else {}
    container = request.app.state.container
    service = _build_station_110_upload_service(container)
    try:
        return service.retry_cloud_sync(
            duty_date=str(body.get("duty_date", "")).strip(),
            duty_shift=str(body.get("duty_shift", "")).strip(),
            emit_log=container.add_system_log,
        )
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    except Exception as exc:  # noqa: BLE001
        container.add_system_log(f"[交接班][110站云表] 重试失败: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
        try:
            followup_result = _start_handover_followup_job_after_confirm(
                container,
                batch_key=batch_key,
                submitted_by="confirm_all",
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][确认后上传] 任务提交失败 batch={batch_key}, 错误={exc}")
            followup_result = _build_followup_failure_result(
                followup,
                batch_key=batch_key,
                error=str(exc),
            )
        emit_log(
            f"[交接班][确认后上传] batch={batch_key}, 状态={_followup_status_text(followup_result.get('status'))}, "
            f"云表状态={_followup_status_text(followup_result.get('cloud_sheet_sync', {}).get('status', '-'))}"
        )
        refreshed_batch_status = _attach_followup_progress(followup, service.get_batch_status(batch_key))
        refreshed_sessions = service.list_batch_sessions(batch_key)
        _review_history_cache_invalidate_sessions(refreshed_sessions or updated_sessions)
        _publish_handover_review_status_cache(
            request,
            container,
            service=service,
            batch_status=refreshed_batch_status or _attach_followup_progress(followup, batch_status),
            reason="confirm_all",
        )
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
        worker_handler="",
        worker_payload={},
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
        emit_log("[交接班][日报截图] 单截图公开页面模式，无需初始化飞书截图登录态")
        return {
            "ok": True,
            "status": "skipped",
            "message": "单截图公开页面模式，无需初始化飞书截图登录态",
            "profile_dir": "",
        }

    job = _start_handover_background_job(
        container,
        name=f"日报截图登录态跳过-{batch_key}",
        run_func=_run,
        worker_handler="daily_report_auth_open",
        worker_payload={"duty_date": duty_date_text, "duty_shift": duty_shift_text},
        resource_keys=_handover_resource_keys("browser:controlled", batch_key=batch_key),
        priority="manual",
        feature="daily_report_auth_open",
        submitted_by="manual",
    )
    container.add_system_log(f"[任务] 已提交: 日报截图登录态跳过 batch={batch_key} ({job.job_id})")
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
        summary_result = screenshot_service.capture_daily_report_page(
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
            emit_log=emit_log,
        )
        overall_status = "ok" if str(summary_result.get("status", "")).strip().lower() in {"ok", "skipped"} else "failed"

        return {
            "ok": overall_status != "failed",
            "status": overall_status,
            "batch_key": batch_key,
            "spreadsheet_url": spreadsheet_url,
            "summary_sheet_image": summary_result,
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
            result = _daily_report_capture_result_payload(
                screenshot_service.capture_daily_report_page(
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    emit_log=emit_log,
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
    handover_cfg = _handover_cfg(container)
    service, parser, writer, _ = _build_review_services(container)
    document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
    building = _resolve_building_or_404(service, building_code)
    review_ui = parser.config.get("review_ui", {}) if isinstance(parser.config, dict) else {}
    session, pending_payload = _load_review_session_or_pending(
        service,
        building=building,
        duty_date=duty_date,
        duty_shift=duty_shift,
        session_id=session_id,
        config=handover_cfg,
        review_ui=review_ui if isinstance(review_ui, dict) else {},
        emit_log=container.add_system_log,
    )
    if pending_payload is not None:
        return pending_payload
    try:
        document, session = _load_review_document_cached(document_state, session)
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    try:
        batch_status = service.get_batch_status(session["batch_key"])
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
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
    handover_cfg = _handover_cfg(container)
    service, parser, writer, _ = _build_review_services(container)
    document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
    building = _resolve_building_or_404(service, building_code)
    review_ui = _review_ui_payload(parser)
    session, pending_payload = _load_review_session_or_pending(
        service,
        building=building,
        duty_date=duty_date,
        duty_shift=duty_shift,
        session_id=session_id,
        config=handover_cfg,
        review_ui=review_ui,
        emit_log=container.add_system_log,
    )
    if pending_payload is not None:
        return pending_payload
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


@router.post("/api/handover/review/{building_code}/sections/events/refresh")
def handover_review_refresh_event_sections(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    container = request.app.state.container
    handover_cfg = _handover_cfg(container)
    service, parser, writer, _ = _build_review_services(container)
    document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
    building = _resolve_building_or_404(service, building_code)
    session_id = str(payload.get("session_id", "") or "").strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    target = _load_target_session_or_404(service, building=building, session_id=session_id)
    try:
        document, session = document_state.load_document(target)
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    duty_date_text = str(session.get("duty_date", "") or "").strip()
    duty_shift_text = str(session.get("duty_shift", "") or "").strip().lower()
    if not duty_date_text or duty_shift_text not in {"day", "night"}:
        raise HTTPException(status_code=400, detail="当前审核记录缺少日期或班次，无法刷新事件分类")

    follower_text = _fixed_cell_value_from_review_document(document, "C3")
    builder = EventCategoryPayloadBuilder(handover_cfg)
    started = time.perf_counter()
    try:
        sections = builder.build(
            building=building,
            duty_date=duty_date_text,
            duty_shift=duty_shift_text,
            follower_text=follower_text,
            is_current_duty_context=_is_current_handover_duty_context(
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
            ),
            emit_log=container.add_system_log,
        )
    except Exception as exc:  # noqa: BLE001
        container.add_system_log(
            f"[交接班][审核页事件刷新] 失败 building={building}, session={session_id}, error={exc}"
        )
        raise HTTPException(status_code=400, detail=f"刷新事件分类失败: {exc}") from exc
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    event_sections = sections if isinstance(sections, dict) else {}
    row_counts = {
        str(name): len(rows) if isinstance(rows, list) else 0
        for name, rows in event_sections.items()
    }
    container.add_system_log(
        f"[交接班][审核页事件刷新] 完成 building={building}, session={session_id}, "
        f"sections={row_counts}, elapsed_ms={elapsed_ms}"
    )
    return {
        "ok": True,
        "building": building,
        "session_id": session_id,
        "duty_date": duty_date_text,
        "duty_shift": duty_shift_text,
        "sections": event_sections,
        "row_counts": row_counts,
        "elapsed_ms": elapsed_ms,
    }


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
    handover_cfg = _handover_cfg(container)
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    review_ui = _build_review_ui_config(container)
    session, pending_payload = _load_review_session_or_pending(
        service,
        building=building,
        duty_date=duty_date,
        duty_shift=duty_shift,
        session_id=session_id,
        config=handover_cfg,
        review_ui=review_ui,
        emit_log=container.add_system_log,
    )
    if pending_payload is not None:
        return pending_payload
    session = _attach_excel_sync_from_store(container, session)
    session_revision = int(session.get("revision", 0) or 0)
    document_revision = _review_document_revision_from_store(
        container,
        session,
        fallback=session_revision,
    )
    session = dict(session)
    session["document_revision"] = document_revision
    session["session_revision"] = session_revision
    try:
        batch_status = service.get_batch_status(session["batch_key"])
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
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
            current_revision=document_revision,
            emit_log=container.add_system_log,
        ),
        "review_ui": {"poll_interval_sec": poll_interval_sec},
        "document_revision": document_revision,
        "snapshot_revision": document_revision,
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
    existing_document_state: Dict[str, Any] | None = None
    try:
        ensured_state = document_state.ensure_document_for_session(target)
        existing_document_state = ensured_state if isinstance(ensured_state, dict) else None
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    output_file_text = str(target.get("output_file", "")).strip()
    if not output_file_text:
        raise HTTPException(status_code=409, detail="交接班文件不存在，无法同步最新审核内容")
    output_file = Path(output_file_text)
    if not output_file.exists() or not output_file.is_file():
        raise HTTPException(status_code=409, detail="交接班文件不存在，无法同步最新审核内容")
    download_warning = ""
    try:
        state = existing_document_state
        if not isinstance(state, dict) or not callable(getattr(document_state, "attach_excel_sync", None)):
            document_state.force_sync_session_dict(target, reason="download")
        else:
            document_revision = int(state.get("revision", target.get("revision", 0)) or 0)
            sync_state = document_state.attach_excel_sync(target).get("excel_sync", {})
            synced_revision = int(sync_state.get("synced_revision", 0) or 0) if isinstance(sync_state, dict) else 0
            sync_status = str(sync_state.get("status", "") if isinstance(sync_state, dict) else "").strip().lower()
            if sync_status not in {"synced", "success"} or synced_revision < document_revision:
                queue_service = _build_xlsx_write_queue_service(
                    container,
                    review_service=service,
                    document_state=document_state,
                )
                queue_service.enqueue_review_excel_sync(target, target_revision=document_revision)
                barrier = queue_service.wait_for_barrier(
                    building=building,
                    session_id=session_id_text,
                    reason="download",
                    timeout_sec=120,
                )
                if str(barrier.get("status", "")).strip().lower() != "success":
                    download_warning = str(barrier.get("error", "") or "").strip() or "交接班文件写入队列失败"
                latest_sync = document_state.attach_excel_sync(target).get("excel_sync", {})
                latest_status = str(latest_sync.get("status", "") if isinstance(latest_sync, dict) else "").strip().lower()
                latest_synced_revision = int(latest_sync.get("synced_revision", 0) or 0) if isinstance(latest_sync, dict) else 0
                if latest_status not in {"synced", "success"} or latest_synced_revision < document_revision:
                    download_warning = (
                        str(latest_sync.get("error", "") if isinstance(latest_sync, dict) else "").strip()
                        or f"交接班文件尚未同步到最新版本: synced={latest_synced_revision}, target={document_revision}"
                    )
    except ReviewDocumentStateError as exc:
        download_warning = str(exc)
    except HandoverXlsxWriteQueueTimeoutError as exc:
        download_warning = str(exc)
    except Exception as exc:  # noqa: BLE001
        download_warning = str(exc)
    if download_warning:
        container.add_system_log(
            "[交接班][下载成品] 同步最新审核内容失败，已阻止下载旧交接班文件 "
            f"building={building}, session_id={session_id_text}, file={output_file}, error={download_warning}"
        )
        raise HTTPException(status_code=409, detail=download_warning)

    container.add_system_log(
        f"[交接班][下载成品] building={building}, session_id={session_id_text}, file={output_file}, "
        "warning=no"
    )
    return FileResponse(
        path=output_file,
        filename=output_file.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.get("/api/handover/review/{building_code}/capacity-download")
def handover_review_capacity_download(
    building_code: str,
    request: Request,
    session_id: str = "",
    client_id: str = "",
) -> FileResponse:
    container = request.app.state.container
    service, parser, writer, _ = _build_review_services(container)
    document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
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
    try:
        document, target = document_state.load_document(target)
    except ReviewDocumentStateError as exc:
        container.add_system_log(
            "[交接班][下载容量报表] 审核文档读取失败，已阻止下载旧容量表 "
            f"building={building}, session_id={session_id_text}, error={exc}"
        )
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    tracked_cells = _extract_capacity_tracked_cells(document)
    try:
        queue_service = _build_xlsx_write_queue_service(
            container,
            review_service=service,
            document_state=document_state,
        )
        queue_service.enqueue_capacity_overlay_sync(
            target,
            tracked_cells=tracked_cells,
            client_id=str(client_id or "").strip(),
        )
        barrier = queue_service.wait_for_barrier(
            building=building,
            session_id=session_id_text,
            reason="capacity_download",
            timeout_sec=120,
        )
        if str(barrier.get("status", "")).strip().lower() != "success":
            detail = str(barrier.get("error", "") or "").strip() or "容量报表写入队列失败"
            container.add_system_log(
                f"[交接班][下载容量报表] 补写失败，已阻止下载旧容量表 building={building}, session_id={session_id_text}, error={detail}"
            )
            raise HTTPException(status_code=409, detail=detail)
    except HandoverXlsxWriteQueueTimeoutError as exc:
        container.add_system_log(
            f"[交接班][下载容量报表] 补写等待超时，已阻止下载旧容量表 building={building}, session_id={session_id_text}, error={exc}"
        )
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        container.add_system_log(
            f"[交接班][下载容量报表] 补写异常，已阻止下载旧容量表 building={building}, session_id={session_id_text}, error={exc}"
        )
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    get_session_by_id = getattr(service, "get_session_by_id", None)
    refreshed_target = get_session_by_id(session_id_text) if callable(get_session_by_id) else None
    refreshed_target = refreshed_target or target
    sync_payload = refreshed_target.get("capacity_sync", {}) if isinstance(refreshed_target.get("capacity_sync", {}), dict) else {}
    sync_status = str(sync_payload.get("status", "")).strip().lower()
    if sync_status != "ready":
        detail = str(sync_payload.get("error", "") if isinstance(sync_payload, dict) else "").strip() or "容量报表部分字段补写失败"
        container.add_system_log(
            f"[交接班][下载容量报表] 补写未就绪，已阻止下载旧容量表 building={building}, session_id={session_id_text}, status={sync_status or '-'}, error={detail}"
        )
        raise HTTPException(status_code=409, detail=detail)
    target = refreshed_target
    refreshed_output_file_text = str(target.get("capacity_output_file", "") or output_file_text).strip()
    output_file = Path(refreshed_output_file_text)
    if not refreshed_output_file_text or not output_file.exists() or not output_file.is_file():
        detail = "容量报表补写完成但文件不存在，请重新生成"
        container.add_system_log(
            f"[交接班][下载容量报表] {detail} building={building}, session_id={session_id_text}, file={refreshed_output_file_text or '-'}"
        )
        raise HTTPException(status_code=409, detail=detail)

    container.add_system_log(
        f"[交接班][下载容量报表] building={building}, session_id={session_id_text}, "
        f"status={sync_status}, file={output_file}"
    )
    return FileResponse(
        path=output_file,
        filename=output_file.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.post("/api/handover/review/{building_code}/capacity-image/send")
def handover_review_capacity_image_send(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, parser, writer, _ = _build_review_services(container)
    document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
    building = _resolve_building_or_404(service, building_code)

    session_id_text = str(payload.get("session_id", "") or "").strip()
    client_id = str(payload.get("client_id", "") or "").strip()
    if not session_id_text:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    _ensure_latest_session_actionable_or_400(service, building=building, session_id=session_id_text)
    target = _load_target_session_or_404(service, building=building, session_id=session_id_text)
    if not str(target.get("capacity_output_file", "") or "").strip():
        return {
            "ok": False,
            "status": "failed",
            "error": "当前交接班容量报表尚未生成",
            "building": building,
            "session_id": session_id_text,
            "successful_recipients": [],
            "failed_recipients": [],
            "capacity_image_delivery": target.get("capacity_image_delivery", {}),
            "review_link_delivery": target.get("review_link_delivery", {}),
        }

    try:
        document_state.ensure_document_for_session(target)
        queue_service = _build_xlsx_write_queue_service(
            container,
            review_service=service,
            document_state=document_state,
        )
        state = document_state.ensure_document_for_session(target)
        queue_service.enqueue_review_excel_sync(target, target_revision=int(state.get("revision", target.get("revision", 0)) or 0))
        barrier = queue_service.wait_for_barrier(
            building=building,
            session_id=session_id_text,
            reason="capacity_image_review_sync",
            timeout_sec=120,
        )
        if str(barrier.get("status", "")).strip().lower() != "success":
            raise ReviewDocumentStateError(str(barrier.get("error", "") or "").strip() or "交接班文件写入队列失败")
        latest_sync = document_state.attach_excel_sync(target).get("excel_sync", {})
        latest_status = str(latest_sync.get("status", "") if isinstance(latest_sync, dict) else "").strip().lower()
        latest_synced_revision = int(latest_sync.get("synced_revision", 0) or 0) if isinstance(latest_sync, dict) else 0
        target_revision = int(state.get("revision", target.get("revision", 0)) or 0)
        if latest_status not in {"synced", "success"} or latest_synced_revision < target_revision:
            raise ReviewDocumentStateError(
                str(latest_sync.get("error", "") if isinstance(latest_sync, dict) else "").strip()
                or f"交接班文件尚未同步到最新版本: synced={latest_synced_revision}, target={target_revision}"
            )
        document, target_with_document = document_state.load_document(target)
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except HandoverXlsxWriteQueueTimeoutError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    tracked_cells = _extract_capacity_tracked_cells(document)
    shared_state = _get_substation_110kv_state_safe(
        service,
        batch_key=str(target.get("batch_key", "")).strip(),
        client_id=client_id,
        emit_log=container.add_system_log,
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

    current_session = dict(target)
    current_session.update(target_with_document if isinstance(target_with_document, dict) else {})
    current_session["cooling_pump_pressures"] = cooling_pump_pressures if isinstance(cooling_pump_pressures, dict) else {}

    def _ensure_capacity_ready_for_send() -> Dict[str, Any]:
        nonlocal current_session
        queue_service = _build_xlsx_write_queue_service(
            container,
            review_service=service,
            document_state=document_state,
        )
        queue_service.enqueue_capacity_overlay_sync(
            current_session,
            tracked_cells=tracked_cells,
            client_id=client_id,
        )
        barrier = queue_service.wait_for_barrier(
            building=building,
            session_id=session_id_text,
            reason="capacity_image_overlay",
            timeout_sec=120,
        )
        if str(barrier.get("status", "")).strip().lower() != "success":
            raise RuntimeError(str(barrier.get("error", "") or "").strip() or "容量报表写入队列失败")
        latest = service.get_session_by_id(session_id_text)
        if isinstance(latest, dict):
            current_session.update(latest)
        return current_session

    try:
        current_session = _ensure_capacity_ready_for_send()
        document, target_with_document = document_state.load_document(current_session)
        tracked_cells = _extract_capacity_tracked_cells(document)
        shared_state = _get_substation_110kv_state_safe(
            service,
            batch_key=str(current_session.get("batch_key", "")).strip(),
            client_id=client_id,
            emit_log=container.add_system_log,
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
        current_session.update(target_with_document if isinstance(target_with_document, dict) else {})
        current_session["cooling_pump_pressures"] = cooling_pump_pressures if isinstance(cooling_pump_pressures, dict) else {}
    except HandoverXlsxWriteQueueTimeoutError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    delivery_service = CapacityReportImageDeliveryService(
        _handover_cfg(container),
        config_path=getattr(container, "config_path", None),
        review_service=service,
    )
    try:
        result = delivery_service.send_for_session(
            current_session,
            building=building,
            handover_cells=tracked_cells,
            shared_110kv=shared_110kv if isinstance(shared_110kv, dict) else {},
            cooling_pump_pressures=cooling_pump_pressures if isinstance(cooling_pump_pressures, dict) else {},
            client_id=client_id,
            ensure_capacity_ready=_ensure_capacity_ready_for_send,
            emit_log=container.add_system_log,
        )
    except Exception as exc:  # noqa: BLE001
        container.add_system_log(
            f"[交接班][容量表图片发送] 接口异常 building={building}, session_id={session_id_text}, error={exc}"
        )
        return {
            "ok": False,
            "status": "failed",
            "error": str(exc),
            "building": building,
            "session_id": session_id_text,
            "successful_recipients": [],
            "failed_recipients": [],
            "capacity_image_delivery": current_session.get("capacity_image_delivery", {}),
            "review_link_delivery": current_session.get("review_link_delivery", {}),
        }
    return result


@router.post("/api/handover/review/{building_code}/regenerate")
def handover_review_regenerate(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    session_id_text = str(payload.get("session_id", "") or "").strip()
    client_id = str(payload.get("client_id", "") or "").strip()
    if not session_id_text:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    _ensure_latest_session_actionable_or_400(service, building=building, session_id=session_id_text)
    target = _load_target_session_or_404(service, building=building, session_id=session_id_text)
    if bool(target.get("confirmed", False)):
        raise HTTPException(status_code=409, detail="当前楼栋已确认，请先撤销确认后再重新生成")
    cloud_sync = target.get("cloud_sheet_sync", {}) if isinstance(target.get("cloud_sheet_sync", {}), dict) else {}
    if str(cloud_sync.get("status", "") or "").strip().lower() in {"uploading", "syncing"}:
        raise HTTPException(status_code=409, detail="当前楼栋云文档上传中，请等待完成后再重新生成")

    duty_date = str(target.get("duty_date", "") or "").strip()
    duty_shift = str(target.get("duty_shift", "") or "").strip().lower()
    batch_key = str(target.get("batch_key", "") or "").strip()
    handover_source, capacity_source = _resolve_regenerate_source_files(
        container,
        target,
        building=building,
    )
    if not handover_source:
        raise HTTPException(status_code=409, detail="缺少可用于重新生成的交接班源文件")
    if not capacity_source:
        raise HTTPException(status_code=409, detail="缺少可用于重新生成的交接班容量报表源文件")

    worker_payload = {
        "building": building,
        "session_id": session_id_text,
        "duty_date": duty_date,
        "duty_shift": duty_shift,
        "data_file": handover_source,
        "capacity_source_file": capacity_source,
        "client_id": client_id,
    }

    def _run(emit_log):
        from app.worker.task_handlers import handle_handover_review_regenerate

        return handle_handover_review_regenerate(
            getattr(container, "runtime_config", {}),
            worker_payload,
            emit_log,
        )

    job = _start_handover_background_job(
        container,
        name=f"重新生成交接班及容量表 {building}",
        run_func=_run,
        worker_handler="handover_review_regenerate",
        worker_payload=worker_payload,
        resource_keys=_handover_resource_keys("shared_bridge:handover", building=building, batch_key=batch_key),
        priority="manual",
        feature="handover_review_regenerate",
        submitted_by="manual",
        dedupe_key=f"handover_review_regenerate:{session_id_text}",
    )
    container.add_system_log(
        f"[任务] 已提交: 重新生成交接班及容量表 building={building}, session={session_id_text} ({job.job_id})"
    )
    return _accepted_job_response(job)


@router.put("/api/handover/review/{building_code}")
def handover_review_save(
    building_code: str,
    request: Request,
    background_tasks: BackgroundTasks,
    payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    container = request.app.state.container
    service, parser, writer, followup = _build_review_services(container)
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
        session_base_revision = int(payload.get("session_base_revision") or target.get("revision", 0) or base_revision or 0)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="session_base_revision 参数错误") from exc
    lock_concurrency = _ensure_session_lock_held_or_409(
        service,
        building=building,
        session_id=session_id,
        client_id=str(payload.get("client_id", "")).strip(),
    )
    initial_document_state: Dict[str, Any] | None = None
    try:
        ensured_document_state = document_state.ensure_document_for_session(target)
        initial_document_state = ensured_document_state if isinstance(ensured_document_state, dict) else None
    except ReviewDocumentStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    batch_key = str(target.get("batch_key", "")).strip()
    dirty_regions = _normalize_review_dirty_regions(payload.get("dirty_regions"))
    previous_document = (
        initial_document_state.get("document", {})
        if isinstance(initial_document_state, dict) and isinstance(initial_document_state.get("document", {}), dict)
        else {}
    )
    previous_outdoor_cells = ReviewSessionService.extract_outdoor_temperature_cells_from_document(previous_document)
    incoming_outdoor_cells = ReviewSessionService.extract_outdoor_temperature_cells_from_document(document)
    has_outdoor_dirty_flag = "shared_outdoor_temperature_dirty" in payload
    outdoor_cells_changed = (
        bool(payload.get("shared_outdoor_temperature_dirty"))
        if has_outdoor_dirty_flag
        else (
            bool(dirty_regions.get("fixed_blocks")) and any(
                str(incoming_outdoor_cells.get(cell, "") if incoming_outdoor_cells.get(cell) is not None else "")
                != str(previous_outdoor_cells.get(cell, "") if previous_outdoor_cells.get(cell) is not None else "")
                for cell in ("B7", "D7")
            )
        )
    )
    outdoor_shared_publish_cells: Dict[str, str] | None = None
    outdoor_batch_sync_result: Dict[str, Any] = {}
    if outdoor_cells_changed:
        outdoor_shared_publish_cells = dict(incoming_outdoor_cells)
    elif batch_key:
        try:
            outdoor_state = service.get_outdoor_temperature_state(
                batch_key=batch_key,
                client_id=str(payload.get("client_id", "")).strip(),
                preferred_document=document,
                preferred_session=target,
            )
            outdoor_block = outdoor_state.get("shared_blocks", {}).get("outdoor_temperature", {})
            outdoor_cells = outdoor_block.get("cells", {}) if isinstance(outdoor_block, dict) else {}
            document, _outdoor_applied = ReviewSessionService.apply_outdoor_temperature_to_document(
                document,
                outdoor_cells if isinstance(outdoor_cells, dict) else {},
            )
        except ReviewSessionStoreUnavailableError as exc:
            _raise_review_store_http_error(exc)
        except Exception as exc:  # noqa: BLE001
            container.add_system_log(
                f"[交接班][室外温湿度共享] 保存前套用共享值失败，继续保存当前文档: "
                f"building={building}, session_id={session_id}, error={exc}"
            )
    save_started = time.perf_counter()
    write_elapsed_ms = 0
    defaults_elapsed_ms = 0
    capacity_elapsed_ms = 0
    session_elapsed_ms = 0
    queued_excel_sync = False
    queued_capacity_sync = False
    defaults_config_async = False
    defaults_config_status = "skipped"
    saved_document_revision = int(base_revision or 0)
    auto_cloud_sync_result: Dict[str, Any] = {}
    with container.job_service.resource_guard(
        name=f"handover_save:{batch_key or building}:{session_id}",
        resource_keys=_handover_resource_keys(building=building, batch_key=batch_key),
    ):
        write_started = time.perf_counter()
        previous_document_state: Dict[str, Any] | None = None
        saved_document_state_for_cache: Dict[str, Any] | None = None
        try:
            _saved_document_state, previous_document_state = document_state.save_document(
                session=target,
                document=document,
                base_revision=base_revision,
                dirty_regions=dirty_regions,
                ensure_ready=False,
            )
            if isinstance(_saved_document_state, dict):
                saved_document_state_for_cache = _saved_document_state
                saved_document_revision = int(_saved_document_state.get("revision", saved_document_revision) or saved_document_revision)
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
        persisted_defaults = {
            "footer_inventory_rows": 0,
            "cabinet_power_fields": 0,
            "capacity_room_rows": 0,
            "config_updated": False,
        }
        try:
            session_started = time.perf_counter()
            if is_latest_session:
                session, batch_status = service.touch_session_after_save(
                    building=building,
                    session_id=session_id,
                    base_revision=session_base_revision,
                )
            else:
                session, batch_status = service.touch_session_after_history_save(
                    building=building,
                    session_id=session_id,
                    base_revision=session_base_revision,
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
        if outdoor_shared_publish_cells is not None and batch_key:
            try:
                shared_save = service.save_outdoor_temperature(
                    batch_key=batch_key,
                    building=building,
                    client_id=str(payload.get("client_id", "")).strip(),
                    cells=outdoor_shared_publish_cells,
                )
                outdoor_batch_sync_result = service.sync_outdoor_temperature_to_batch_documents(
                    batch_key=batch_key,
                    cells=outdoor_shared_publish_cells,
                    source_session_id=session_id,
                )
                if isinstance(outdoor_batch_sync_result.get("batch_status", {}), dict):
                    batch_status = outdoor_batch_sync_result.get("batch_status", batch_status)
                _review_history_cache_invalidate_sessions(
                    outdoor_batch_sync_result.get("updated_sessions", [])
                    if isinstance(outdoor_batch_sync_result.get("updated_sessions", []), list)
                    else []
                )
                container.add_system_log(
                    f"[交接班][室外温湿度共享] 已同步五楼共享温湿度: "
                    f"building={building}, session_id={session_id}, "
                    f"B7={outdoor_shared_publish_cells.get('B7', '') or '-'}, "
                    f"D7={outdoor_shared_publish_cells.get('D7', '') or '-'}, "
                    f"shared_revision={shared_save.get('shared_blocks', {}).get('outdoor_temperature', {}).get('revision', '-')}, "
                    f"其他楼更新={int(outdoor_batch_sync_result.get('updated_count', 0) or 0)}"
                )
            except ReviewSessionStoreUnavailableError as exc:
                _raise_review_store_http_error(exc, saved_document=True)
            except Exception as exc:  # noqa: BLE001
                outdoor_batch_sync_result = {"status": "failed", "error": str(exc)}
                container.add_system_log(
                    f"[交接班][室外温湿度共享] 共享同步失败，已保留当前楼保存结果: "
                    f"building={building}, session_id={session_id}, error={exc}"
                )
        saved_document_for_response = document if isinstance(document, dict) else {}
        if isinstance(saved_document_state_for_cache, dict):
            state_document = saved_document_state_for_cache.get("document", {})
            if isinstance(state_document, dict):
                saved_document_for_response = state_document
            _review_document_cache_put(
                building=building,
                signature=_review_document_signature(
                    session,
                    revision_override=int(saved_document_state_for_cache.get("revision", 0) or 0),
                ),
                document=saved_document_for_response,
            )
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
                    "capacity_room_rows": 0,
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
            xlsx_queue_service = _build_xlsx_write_queue_service(
                container,
                review_service=service,
                document_state=document_state,
            )
            excel_sync = xlsx_queue_service.enqueue_review_excel_sync(
                session=session,
                target_revision=saved_document_revision,
            )
            queued_excel_sync = str(excel_sync.get("status", "")).strip().lower() not in {"", "failed", "unknown"}
            outdoor_updated_sessions = (
                outdoor_batch_sync_result.get("updated_sessions", [])
                if isinstance(outdoor_batch_sync_result.get("updated_sessions", []), list)
                else []
            )
            for outdoor_session in outdoor_updated_sessions:
                if not isinstance(outdoor_session, dict):
                    continue
                outdoor_building = str(outdoor_session.get("building", "") or "").strip()
                outdoor_session_id = str(outdoor_session.get("session_id", "") or "").strip()
                if not outdoor_building or not outdoor_session_id:
                    continue
                try:
                    patched_state = ReviewBuildingDocumentStore(
                        config=_handover_cfg(container),
                        building=outdoor_building,
                    ).get_document(outdoor_session_id)
                    patched_revision = (
                        int(patched_state.get("revision", 0) or 0)
                        if isinstance(patched_state, dict)
                        else int(outdoor_session.get("revision", 0) or 0)
                    )
                    xlsx_queue_service.enqueue_review_excel_sync(
                        session=outdoor_session,
                        target_revision=patched_revision,
                    )
                    xlsx_queue_service.enqueue_capacity_overlay_sync(outdoor_session)
                except Exception as exc:  # noqa: BLE001
                    container.add_system_log(
                        f"[交接班][室外温湿度共享] 其他楼后台同步排队失败: "
                        f"building={outdoor_building}, session_id={outdoor_session_id}, error={exc}"
                    )
        except Exception as exc:  # noqa: BLE001
            excel_sync = {
                "status": "failed",
                "synced_revision": saved_document_revision - 1 if saved_document_revision > 0 else 0,
                "pending_revision": saved_document_revision,
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
            signature=_review_bootstrap_signature(session, revision_override=saved_document_revision),
            payload={
                "ok": True,
                "building": building,
                "session": copy.deepcopy(session),
                "document": copy.deepcopy(saved_document_for_response if isinstance(saved_document_for_response, dict) else {}),
                "review_ui": _review_ui_payload(parser),
                "prepared_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "document_revision": saved_document_revision,
                "snapshot_revision": saved_document_revision,
            },
        )

    if is_latest_session:
        auto_cloud_sync_result = _maybe_start_handover_followup_job_after_review_save(
            container,
            followup=followup,
            session=session,
            batch_status=batch_status,
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
                f"capacity_room_rows={persisted_defaults.get('capacity_room_rows', 0)}, "
                f"footer_inventory_rows={persisted_defaults.get('footer_inventory_rows', 0)}"
            )
        else:
            container.add_system_log(
                f"[交接班][审核模板默认] 楼栋SQLite默认值无变化，已跳过写入: building={building}, "
                f"cabinet_power_fields={persisted_defaults.get('cabinet_power_fields', 0) if isinstance(persisted_defaults, dict) else 0}, "
                f"capacity_room_rows={persisted_defaults.get('capacity_room_rows', 0) if isinstance(persisted_defaults, dict) else 0}, "
                f"footer_inventory_rows={persisted_defaults.get('footer_inventory_rows', 0) if isinstance(persisted_defaults, dict) else 0}"
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
        "document": copy.deepcopy(saved_document_for_response if isinstance(saved_document_for_response, dict) else {}),
        "revision": int(session.get("revision", 0) or 0),
        "session_revision": int(session.get("revision", 0) or 0),
        "document_revision": saved_document_revision,
        "snapshot_revision": saved_document_revision,
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
        "auto_cloud_sync": auto_cloud_sync_result,
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
            "outdoor_temperature_shared": {
                "changed": bool(outdoor_shared_publish_cells is not None),
                "updated_sessions": int(outdoor_batch_sync_result.get("updated_count", 0) or 0)
                if isinstance(outdoor_batch_sync_result, dict)
                else 0,
                "error": str(outdoor_batch_sync_result.get("error", "") or "")
                if isinstance(outdoor_batch_sync_result, dict)
                else "",
            },
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
    _ensure_cloud_sheet_not_uploading_or_409(target_session, action="操作确认状态")
    target_batch_key = str(target_session.get("batch_key", "")).strip()
    with container.job_service.resource_guard(
        name=f"handover_confirm:{target_batch_key}:{building}",
        resource_keys=_handover_resource_keys(batch_key=target_batch_key),
    ):
        try:
            parser = ReviewDocumentParser(_handover_cfg(container))
            writer = ReviewDocumentWriter(_handover_cfg(container))
            document_state = _build_review_document_state_service(container, parser=parser, writer=writer)
            state = document_state.ensure_document_for_session(target_session)
            queue_service = _build_xlsx_write_queue_service(
                container,
                review_service=service,
                document_state=document_state,
                parser=parser,
                writer=writer,
            )
            queue_service.enqueue_review_excel_sync(
                target_session,
                target_revision=int(state.get("revision", target_session.get("revision", 0)) or 0),
            )
            barrier = queue_service.wait_for_barrier(
                building=building,
                session_id=session_id,
                reason="confirm",
                timeout_sec=120,
            )
            if str(barrier.get("status", "")).strip().lower() != "success":
                raise HTTPException(
                    status_code=409,
                    detail=str(barrier.get("error", "") or "").strip() or "交接班文件写入队列繁忙，请稍后重试",
                )
            latest_sync = document_state.attach_excel_sync(target_session).get("excel_sync", {})
            latest_status = str(latest_sync.get("status", "") if isinstance(latest_sync, dict) else "").strip().lower()
            latest_synced_revision = int(latest_sync.get("synced_revision", 0) or 0) if isinstance(latest_sync, dict) else 0
            target_revision = int(state.get("revision", target_session.get("revision", 0)) or 0)
            if latest_status not in {"synced", "success"} or latest_synced_revision < target_revision:
                raise HTTPException(
                    status_code=409,
                    detail=str(latest_sync.get("error", "") if isinstance(latest_sync, dict) else "").strip()
                    or f"交接班文件尚未同步到最新版本: synced={latest_synced_revision}, target={target_revision}",
                )
        except HandoverXlsxWriteQueueTimeoutError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ReviewDocumentStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
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
            followup_result = _start_handover_followup_job_after_confirm(
                container,
                batch_key=target_batch_key,
                building=building,
                session_id=str(session.get("session_id", "") or session_id).strip(),
                submitted_by="confirm",
            )
        except Exception as exc:  # noqa: BLE001
            container.add_system_log(f"[交接班][确认后上传] 任务提交失败 batch={target_batch_key}, building={building}, 错误={exc}")
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
        _publish_handover_review_status_cache(
            request,
            container,
            service=service,
            batch_status=latest_batch_status or _attach_followup_progress(followup, batch_status),
            reason="confirm",
        )
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
    _ensure_cloud_sheet_not_uploading_or_409(target_session, action="操作确认状态")
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
        latest_batch_status = _attach_followup_progress(
            _build_review_followup_service(container),
            service.get_batch_status(target_batch_key),
        )
        _publish_handover_review_status_cache(
            request,
            container,
            service=service,
            batch_status=latest_batch_status or batch_status,
            reason="unconfirm",
        )
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
            "batch_status": latest_batch_status or batch_status,
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


def _substation_110kv_session_from_payload(
    service: ReviewSessionService,
    *,
    building: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    session_id = str((payload or {}).get("session_id", "")).strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id 不能为空")
    return _load_target_session_or_404(service, building=building, session_id=session_id)


def _substation_110kv_rows_from_payload(payload: Dict[str, Any]) -> list[dict[str, Any]]:
    rows = (payload or {}).get("rows", [])
    if not isinstance(rows, list):
        return []
    return [dict(item) for item in rows if isinstance(item, dict)]


@router.post("/api/handover/review/{building_code}/shared-blocks/110kv/lock/claim")
def handover_review_110kv_lock_claim(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    client_id = str(payload.get("client_id", "")).strip()
    holder_label = str(payload.get("holder_label", "")).strip()
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id 不能为空")
    session = _substation_110kv_session_from_payload(service, building=building, payload=payload)
    try:
        state = service.claim_substation_110kv_lock(
            batch_key=str(session.get("batch_key", "")).strip(),
            building=building,
            client_id=client_id,
            holder_label=holder_label,
        )
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    lock = state.get("shared_block_locks", {}).get("substation_110kv", {}) if isinstance(state, dict) else {}
    return {
        "ok": True,
        **(state if isinstance(state, dict) else {}),
        "accepted": bool(lock.get("acquired", False) or lock.get("client_holds_lock", False)),
    }


@router.post("/api/handover/review/{building_code}/shared-blocks/110kv/lock/heartbeat")
def handover_review_110kv_lock_heartbeat(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    client_id = str(payload.get("client_id", "")).strip()
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id 不能为空")
    session = _substation_110kv_session_from_payload(service, building=building, payload=payload)
    try:
        state = service.heartbeat_substation_110kv_lock(
            batch_key=str(session.get("batch_key", "")).strip(),
            client_id=client_id,
        )
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    lock = state.get("shared_block_locks", {}).get("substation_110kv", {}) if isinstance(state, dict) else {}
    return {
        "ok": True,
        **(state if isinstance(state, dict) else {}),
        "accepted": bool(lock.get("renewed", False)),
    }


@router.post("/api/handover/review/{building_code}/shared-blocks/110kv/lock/release")
def handover_review_110kv_lock_release(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    client_id = str(payload.get("client_id", "")).strip()
    session_id = str(payload.get("session_id", "")).strip()
    if not client_id or not session_id:
        return {"ok": True, "shared_blocks": {}, "shared_block_locks": {}, "released": False}
    session = _substation_110kv_session_from_payload(service, building=building, payload=payload)
    try:
        state = service.release_substation_110kv_lock(
            batch_key=str(session.get("batch_key", "")).strip(),
            client_id=client_id,
        )
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    lock = state.get("shared_block_locks", {}).get("substation_110kv", {}) if isinstance(state, dict) else {}
    return {
        "ok": True,
        **(state if isinstance(state, dict) else {}),
        "released": bool(lock.get("released", False)),
    }


@router.post("/api/handover/review/{building_code}/shared-blocks/110kv/dirty")
def handover_review_110kv_dirty(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    client_id = str(payload.get("client_id", "")).strip()
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id 不能为空")
    session = _substation_110kv_session_from_payload(service, building=building, payload=payload)
    try:
        state = service.mark_substation_110kv_dirty(
            batch_key=str(session.get("batch_key", "")).strip(),
            building=building,
            client_id=client_id,
            rows=_substation_110kv_rows_from_payload(payload),
        )
    except ReviewSessionConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    return {"ok": True, **(state if isinstance(state, dict) else {})}


@router.put("/api/handover/review/{building_code}/shared-blocks/110kv")
def handover_review_110kv_save(
    building_code: str,
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    service = _build_review_session_service(container)
    building = _resolve_building_or_404(service, building_code)
    client_id = str(payload.get("client_id", "")).strip()
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id 不能为空")
    session = _substation_110kv_session_from_payload(service, building=building, payload=payload)
    base_revision = _parse_base_revision_or_400(payload)
    try:
        state = service.save_substation_110kv(
            batch_key=str(session.get("batch_key", "")).strip(),
            building=building,
            client_id=client_id,
            rows=_substation_110kv_rows_from_payload(payload),
            base_revision=base_revision,
        )
    except ReviewSessionConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ReviewSessionStoreUnavailableError as exc:
        _raise_review_store_http_error(exc)
    block = state.get("shared_blocks", {}).get("substation_110kv", {}) if isinstance(state, dict) else {}
    container.add_system_log(
        f"[交接班][110KV共享] 保存完成 building={building}, batch={session.get('batch_key', '-')}, "
        f"revision={block.get('revision', 0)}, no_change={bool(state.get('no_change', False))}"
    )
    return {"ok": True, **(state if isinstance(state, dict) else {})}


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
