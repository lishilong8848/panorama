from __future__ import annotations

import threading
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

from app.modules.alarm_rule_export.service.alarm_rule_export_service import (
    list_alarm_rule_export_files,
    resolve_alarm_rule_export_file,
)
from app.modules.system_screenshot_capture.service.system_screenshot_capture_service import (
    list_system_screenshot_files,
    resolve_system_screenshot_file,
    run_system_screenshot_capture,
)
from app.modules.internal_bridge_http.service.internal_bridge_http_runner import InternalBridgeHttpTaskRunner


router = APIRouter(prefix="/api/internal-bridge", tags=["internal-bridge"])

_RUNNER_ATTR = "_internal_bridge_http_runner"


def _client_host(request: Request) -> str:
    client = request.client
    return str(client.host if client else "").strip()


def _bridge_config(request: Request) -> Dict[str, Any]:
    container = getattr(request.app.state, "container", None)
    runtime_config = getattr(container, "runtime_config", {}) if container is not None else {}
    common_cfg = getattr(container, "config", {}).get("common", {}) if container is not None and isinstance(getattr(container, "config", {}), dict) else {}
    cfg = {}
    if isinstance(common_cfg, dict) and isinstance(common_cfg.get("internal_bridge_http"), dict):
        cfg.update(common_cfg.get("internal_bridge_http") or {})
    if isinstance(runtime_config, dict) and isinstance(runtime_config.get("internal_bridge_http"), dict):
        cfg.update(runtime_config.get("internal_bridge_http") or {})
    return cfg


def _require_enabled_and_authorized(request: Request) -> Dict[str, Any]:
    cfg = _bridge_config(request)
    if not bool(cfg.get("enabled", False)):
        raise HTTPException(status_code=503, detail="内网端 HTTP 桥接未启用")
    allowed_ips = cfg.get("allowed_client_ips", [])
    if isinstance(allowed_ips, list):
        normalized_ips = {str(item or "").strip() for item in allowed_ips if str(item or "").strip()}
    else:
        normalized_ips = set()
    host = _client_host(request)
    if normalized_ips and host not in normalized_ips:
        raise HTTPException(status_code=403, detail="当前客户端 IP 不允许调用内网桥接接口")
    return cfg


def _runner(request: Request) -> InternalBridgeHttpTaskRunner:
    container = getattr(request.app.state, "container", None)
    if container is None:
        raise HTTPException(status_code=503, detail="运行容器未初始化")
    service = getattr(container, "shared_bridge_service", None)
    if service is None:
        raise HTTPException(status_code=503, detail="内网共享桥接服务未初始化")
    existing = getattr(request.app.state, _RUNNER_ATTR, None)
    if isinstance(existing, InternalBridgeHttpTaskRunner):
        return existing
    runner = InternalBridgeHttpTaskRunner(
        runtime_service=service,
        emit_log=lambda text: container.add_system_log(text, suppress_alert_upload=True),
    )
    setattr(request.app.state, _RUNNER_ATTR, runner)
    return runner


def _runtime_config(request: Request) -> Dict[str, Any]:
    container = getattr(request.app.state, "container", None)
    runtime_config = getattr(container, "runtime_config", {}) if container is not None else {}
    return runtime_config if isinstance(runtime_config, dict) else {}


def _alarm_rule_export_state_file(config: Dict[str, Any]) -> str | None:
    feature_cfg = config.get("alarm_rule_export", {}) if isinstance(config, dict) else {}
    if not isinstance(feature_cfg, dict):
        return None
    return str(feature_cfg.get("state_file", "") or "").strip() or None


def _system_screenshot_state_file(config: Dict[str, Any]) -> str | None:
    feature_cfg = config.get("system_screenshot_capture", {}) if isinstance(config, dict) else {}
    if not isinstance(feature_cfg, dict):
        return None
    return str(feature_cfg.get("state_file", "") or "").strip() or None


@router.get("/health")
def internal_bridge_health(request: Request) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    return _runner(request).health()


@router.post("/tasks")
def create_internal_bridge_task(
    request: Request,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    try:
        return _runner(request).create_task(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/alarm-events/window-query")
def create_alarm_event_window_query_task(
    request: Request,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    body = payload if isinstance(payload, dict) else {}
    try:
        return _runner(request).create_task(
            {
                "task_type": "create_alarm_event_window_query_task",
                "get_or_create_name": "get_or_create_alarm_event_window_query_task",
                "create_name": "create_alarm_event_window_query_task",
                "payload": {
                    "buildings": body.get("buildings", []),
                    "query_start": str(body.get("query_start", "") or "").strip(),
                    "query_end": str(body.get("query_end", "") or "").strip(),
                    "duty_date": str(body.get("duty_date", "") or "").strip(),
                    "duty_shift": str(body.get("duty_shift", "") or "").strip().lower(),
                    "requested_by": str(body.get("requested_by", "") or "").strip() or "handover_alarm_window",
                },
                "requested_by": str(body.get("requested_by", "") or "").strip() or "handover_alarm_window",
            }
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/tasks")
def list_internal_bridge_tasks(
    request: Request,
    status: str = "",
    limit: int = 100,
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    try:
        tasks = _runner(request).list_tasks(status=status, limit=limit)
        return {"ok": True, "tasks": tasks}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/tasks/{task_id}")
def get_internal_bridge_task(
    task_id: str,
    request: Request,
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    task = _runner(request).get_task(task_id)
    if not isinstance(task, dict):
        raise HTTPException(status_code=404, detail="内网桥接任务不存在")
    return task


@router.post("/tasks/{task_id}/cancel")
def cancel_internal_bridge_task(
    task_id: str,
    request: Request,
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    cancelled = _runner(request).cancel_task(task_id)
    return {"ok": bool(cancelled), "task_id": str(task_id or "").strip(), "status": "cancelled" if cancelled else "not_found"}


@router.get("/source-index")
def query_internal_source_index(
    request: Request,
    source_family: str = "",
    bucket_or_date: str = "",
    building: str = "",
    bucket_kind: str = "",
    duty_shift: str = "",
    status: str = "ready",
    limit: int = 50,
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    entries = _runner(request).list_source_index(
        source_family=source_family,
        bucket_or_date=bucket_or_date,
        building=building,
        bucket_kind=bucket_kind,
        duty_shift=duty_shift,
        status=status,
        limit=limit,
    )
    recovering = _runner(request).source_index_recovery_active(
        source_family=source_family,
        bucket_or_date=bucket_or_date,
        building=building,
        bucket_kind=bucket_kind,
        duty_shift=duty_shift,
    )
    return {"ok": True, "entries": entries, "recovering": recovering}


@router.get("/alarm-rule-export/files")
def list_internal_alarm_rule_export_files(
    request: Request,
    period: str = "",
    building: str = "",
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    config = _runtime_config(request)
    return {
        "ok": True,
        **list_alarm_rule_export_files(
            config=config,
            period=period or None,
            building=building,
            state_file=_alarm_rule_export_state_file(config),
        ),
    }


@router.get("/alarm-rule-export/files/download")
def download_internal_alarm_rule_export_file(
    request: Request,
    period: str,
    building: str,
    file_name: str,
) -> FileResponse:
    _require_enabled_and_authorized(request)
    config = _runtime_config(request)
    try:
        path, _metadata = resolve_alarm_rule_export_file(
            config=config,
            period=period,
            building=building,
            file_name=file_name,
            state_file=_alarm_rule_export_state_file(config),
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(path=str(path), filename=path.name)


@router.get("/system-screenshots/files")
def list_internal_system_screenshot_files(
    request: Request,
    capture_date: str = "",
    target_key: str = "",
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    config = _runtime_config(request)
    return {
        "ok": True,
        **list_system_screenshot_files(
            config=config,
            capture_date=capture_date or None,
            target_key=target_key,
            state_file=_system_screenshot_state_file(config),
        ),
    }


@router.get("/system-screenshots/files/download")
def download_internal_system_screenshot_file(
    request: Request,
    capture_date: str,
    target_key: str,
    file_name: str = "",
) -> FileResponse:
    _require_enabled_and_authorized(request)
    config = _runtime_config(request)
    try:
        path, metadata = resolve_system_screenshot_file(
            config=config,
            capture_date=capture_date,
            target_key=target_key,
            file_name=file_name,
            state_file=_system_screenshot_state_file(config),
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(
        path=str(path),
        filename=path.name,
        media_type=str(metadata.get("content_type", "") or "image/png"),
    )


@router.post("/system-screenshots/run")
def run_internal_system_screenshot_capture(
    request: Request,
    payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    body = payload if isinstance(payload, dict) else {}
    config = _runtime_config(request)
    container = getattr(request.app.state, "container", None)
    raw_emit_log = getattr(container, "add_system_log", None)
    capture_date = str(body.get("capture_date", "") or "").strip() or None
    force = bool(body.get("force", False))
    wait = bool(body.get("wait", False))

    def emit_log(text: str) -> None:
        if not callable(raw_emit_log):
            return
        try:
            raw_emit_log(text, suppress_alert_upload=True)
        except TypeError:
            raw_emit_log(text)

    def _run_once() -> Dict[str, Any]:
        lock = getattr(container, "_system_screenshot_capture_run_lock", None)
        acquired = False
        if lock is not None and hasattr(lock, "acquire"):
            acquired = bool(lock.acquire(blocking=False))
            if not acquired:
                return {"status": "running", "message": "系统截图采集已有运行实例"}
        try:
            return run_system_screenshot_capture(
                config=config,
                capture_date=capture_date,
                force=force,
                emit_log=emit_log,
            )
        finally:
            if acquired:
                lock.release()

    if wait:
        try:
            result = _run_once()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return {"ok": True, **result}

    def _worker() -> None:
        try:
            result = _run_once()
            emit_log(
                "[系统截图采集] HTTP 后台检查完成: "
                f"date={capture_date or '-'}, status={result.get('status', '-')}"
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[系统截图采集] HTTP 后台检查失败: date={capture_date or '-'}, error={exc}")

    threading.Thread(target=_worker, name="internal-http-system-screenshot-capture", daemon=True).start()
    return {"ok": True, "status": "accepted", "capture_date": capture_date or "", "force": force}


@router.post("/source-index/batch")
def query_internal_source_index_batch(
    request: Request,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    queries = payload.get("queries", []) if isinstance(payload, dict) else []
    default_limit = int(payload.get("default_limit", 50) or 50) if isinstance(payload, dict) else 50
    results = _runner(request).list_source_index_batch(
        queries if isinstance(queries, list) else [],
        default_limit=default_limit,
    )
    return {"ok": True, "results": results}


@router.post("/source-cache/refresh-latest")
def refresh_internal_latest_source_cache(
    request: Request,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    _require_enabled_and_authorized(request)
    source_family = str(payload.get("source_family", "") or "") if isinstance(payload, dict) else ""
    buildings = payload.get("buildings", []) if isinstance(payload, dict) else []
    try:
        return _runner(request).refresh_latest_source_cache(
            source_family=source_family,
            buildings=buildings if isinstance(buildings, list) else [],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
