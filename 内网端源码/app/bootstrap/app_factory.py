from __future__ import annotations

import asyncio
import copy
from contextlib import asynccontextmanager
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from ipaddress import ip_address
from typing import Any, Dict

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from app.bootstrap.container import build_container
from app.config.config_adapter import normalize_role_mode, adapt_runtime_config
from app.config.settings_loader import save_settings
from app.modules.handover_review.api.routes import router as handover_review_router
from app.modules.feishu.api.routes import router as feishu_router
from app.modules.notify.api.routes import router as notify_router
from app.modules.ocr.api.routes import router as ocr_router
from app.modules.report_pipeline.api.routes import (
    router as pipeline_router,
    schedule_handover_review_access_startup_probe,
)
from app.modules.report_pipeline.service.shared_bridge_waiting_job_helper import (
    start_waiting_bridge_job,
)
from app.modules.shared_bridge.api.routes import router as shared_bridge_router
from app.modules.shared_bridge.service.runtime_status_coordinator import RuntimeStatusCoordinator
from app.modules.scheduler.api.handover_routes import router as handover_scheduler_router
from app.modules.scheduler.api.day_metric_upload_routes import router as day_metric_upload_scheduler_router
from app.modules.scheduler.api.alarm_event_upload_routes import router as alarm_event_upload_scheduler_router
from app.modules.scheduler.api.monthly_change_report_routes import router as monthly_change_report_scheduler_router
from app.modules.scheduler.api.monthly_event_report_routes import router as monthly_event_report_scheduler_router
from app.modules.scheduler.api.routes import router as scheduler_router
from app.modules.scheduler.api.wet_bulb_collection_routes import router as wet_bulb_collection_scheduler_router
from app.modules.sheet_import.api.routes import router as sheet_import_router
from app.modules.user.api.routes import router as user_router
from app.modules.websocket.api.log_stream_routes import router as logs_router
from app.shared.utils.frontend_cache import (
    render_frontend_index_html,
    resolve_source_frontend_asset_path,
    source_frontend_no_cache_headers,
)
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from handover_log_module.api.facade import load_handover_config
from handover_log_module.service.handover_daily_report_screenshot_service import (
    HandoverDailyReportScreenshotService,
)
from handover_log_module.service.monthly_change_report_service import MonthlyChangeReportService
from handover_log_module.service.monthly_event_report_service import MonthlyEventReportService
from pipeline_utils import get_app_dir, get_app_root_dir


_EXTERNAL_REVIEW_ALLOWED_PREFIXES = (
    "/handover/review/",
    "/api/handover/review/",
    "/assets/",
    "/assets-src/",
)
_EXTERNAL_REVIEW_ALLOWED_EXACT = {
    "/favicon.ico",
}

_ROLE_SELECTION_ALLOWED_EXACT = {
    "/",
    "/index.html",
    "/favicon.ico",
    "/api/health/bootstrap",
    "/api/handover/daily-report/context",
    "/api/logs/system",
    "/api/runtime/activate-startup",
    "/api/runtime/exit-current",
}
_ROLE_SELECTION_ALLOWED_PREFIXES = (
    "/assets/",
    "/assets-src/",
)


class _SourceNoCacheStaticFiles(StaticFiles):
    def __init__(self, *args, frontend_mode: str = "", **kwargs):
        super().__init__(*args, **kwargs)
        self._frontend_mode = str(frontend_mode or "").strip().lower()

    def file_response(self, full_path, stat_result, scope, status_code=200):  # noqa: ANN001
        response = super().file_response(full_path, stat_result, scope, status_code)
        for key, value in source_frontend_no_cache_headers(self._frontend_mode).items():
            response.headers[key] = value
        return response


def _is_loopback_client(host: str) -> bool:
    raw = str(host or "").strip()
    if not raw:
        return False
    lowered = raw.lower()
    if lowered in {"127.0.0.1", "::1", "localhost"}:
        return True
    try:
        return ip_address(raw).is_loopback
    except ValueError:
        return False


def _is_private_or_link_local_host(host: str) -> bool:
    raw = str(host or "").strip()
    if not raw:
        return False
    try:
        parsed = ip_address(raw)
    except ValueError:
        return False
    return bool(parsed.is_private or parsed.is_link_local)


def _is_lan_console_client(request: Request) -> bool:
    client_host = request.client.host if request.client else ""
    if _is_loopback_client(client_host):
        return True
    request_host = str(request.url.hostname or "").strip()
    if not _is_private_or_link_local_host(client_host):
        return False
    if not request_host:
        return True
    return _is_private_or_link_local_host(request_host) or request_host == client_host


def _is_externally_allowed_path(path: str) -> bool:
    text = str(path or "").strip() or "/"
    if text in _EXTERNAL_REVIEW_ALLOWED_EXACT:
        return True
    return any(text.startswith(prefix) for prefix in _EXTERNAL_REVIEW_ALLOWED_PREFIXES)


def _is_role_selection_allowed_path(path: str) -> bool:
    text = str(path or "").strip() or "/"
    if text in _ROLE_SELECTION_ALLOWED_EXACT:
        return True
    return any(text.startswith(prefix) for prefix in _ROLE_SELECTION_ALLOWED_PREFIXES)


def _install_windows_asyncio_exception_filter(container) -> None:
    if os.name != "nt":
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return

    previous_handler = loop.get_exception_handler()

    def _handler(loop_obj, context):  # noqa: ANN001
        exception = context.get("exception")
        handle_text = str(context.get("handle", "") or "")
        if (
            isinstance(exception, ConnectionResetError)
            and getattr(exception, "winerror", None) == 10054
            and "_ProactorBasePipeTransport._call_connection_lost" in handle_text
        ):
            return
        if previous_handler is not None:
            previous_handler(loop_obj, context)
            return
        loop_obj.default_exception_handler(context)

    loop.set_exception_handler(_handler)
    container.add_system_log("[启动] 已启用 Windows asyncio 断连噪音过滤")


def _register_common_routes(app: FastAPI) -> None:
    app.include_router(pipeline_router)
    app.include_router(logs_router)
    app.include_router(shared_bridge_router)
    app.include_router(user_router)


def _register_external_role_routes(app: FastAPI) -> None:
    app.include_router(sheet_import_router)
    app.include_router(handover_review_router)
    app.include_router(scheduler_router)
    app.include_router(handover_scheduler_router)
    app.include_router(day_metric_upload_scheduler_router)
    app.include_router(alarm_event_upload_scheduler_router)
    app.include_router(wet_bulb_collection_scheduler_router)
    app.include_router(monthly_change_report_scheduler_router)
    app.include_router(monthly_event_report_scheduler_router)
    app.include_router(feishu_router)
    app.include_router(notify_router)
    app.include_router(ocr_router)

def _initialize_handover_daily_report_auth(container) -> None:
    existing_thread = getattr(container, "_handover_daily_report_auth_init_thread", None)
    if isinstance(existing_thread, threading.Thread) and existing_thread.is_alive():
        try:
            container.add_system_log("[交接班][日报截图登录] 启动自动初始化已在后台执行，跳过重复触发")
        except Exception:  # noqa: BLE001
            pass
        return

    def _runner() -> None:
        try:
            handover_cfg = load_handover_config(container.runtime_config)
            export_cfg = handover_cfg.get("daily_report_bitable_export", {})
            if not isinstance(export_cfg, dict):
                export_cfg = {}
            if not bool(export_cfg.get("enabled", True)):
                container.add_system_log("[交接班][日报截图登录] 启动自动初始化已跳过：日报多维导出已禁用")
                return
            screenshot_service = HandoverDailyReportScreenshotService(handover_cfg)
            result = screenshot_service.open_login_browser(emit_log=container.add_system_log)
            message = str(result.get("message", "") or "").strip() or "已触发自动初始化"
            container.add_system_log(f"[交接班][日报截图登录] 启动自动初始化: {message}")
        except Exception as exc:  # noqa: BLE001
            try:
                container.add_system_log(f"[交接班][日报截图登录] 启动自动初始化失败：{exc}")
            except Exception:  # noqa: BLE001
                pass
        finally:
            try:
                setattr(container, "_handover_daily_report_auth_init_thread", None)
            except Exception:  # noqa: BLE001
                pass

    thread = threading.Thread(
        target=_runner,
        name="handover-daily-report-auth-init",
        daemon=True,
    )
    setattr(container, "_handover_daily_report_auth_init_thread", thread)
    thread.start()
    container.add_system_log("[交接班][日报截图登录] 启动自动初始化已转入后台，不阻塞外网页面进入")


def create_app(*, enable_lifespan: bool = True) -> FastAPI:
    container = build_container()
    startup_runtime_activation_lock = threading.Lock()

    def _role_label(role_mode: str) -> str:
        role = normalize_role_mode(role_mode)
        if role == "internal":
            return "内网端"
        if role == "external":
            return "外网端"
        return ""

    def _ensure_config_dict_path(root: Dict[str, Any], *path: str) -> Dict[str, Any]:
        current = root
        for key in path:
            next_value = current.get(key)
            if not isinstance(next_value, dict):
                next_value = {}
                current[key] = next_value
            current = next_value
        return current

    def _positive_int(value: Any, fallback: int) -> int:
        try:
            number = int(value)
        except Exception:
            return int(fallback)
        return number if number > 0 else int(fallback)

    def _apply_shared_bridge_role_patch(shared_bridge_cfg: Dict[str, Any], role_mode: str, payload: Dict[str, Any]) -> None:
        bridge_payload = payload.get("shared_bridge", {}) if isinstance(payload.get("shared_bridge", {}), dict) else {}
        role_root_key = "internal_root_dir" if role_mode == "internal" else "external_root_dir"
        existing_root = str(
            shared_bridge_cfg.get(role_root_key)
            or shared_bridge_cfg.get("root_dir")
            or "",
        ).strip()
        requested_root = str(
            bridge_payload.get(role_root_key)
            or bridge_payload.get("root_dir")
            or existing_root
            or "",
        ).strip()
        if not requested_root:
            raise ValueError(f"{_role_label(role_mode)}共享目录不能为空")
        shared_bridge_cfg["enabled"] = True
        shared_bridge_cfg[role_root_key] = requested_root
        shared_bridge_cfg["root_dir"] = requested_root

        defaults = {
            "poll_interval_sec": 2,
            "heartbeat_interval_sec": 5,
            "claim_lease_sec": 30,
            "stale_task_timeout_sec": 1800,
            "artifact_retention_days": 7,
            "sqlite_busy_timeout_ms": 5000,
        }
        for key, fallback in defaults.items():
            if key in bridge_payload or key not in shared_bridge_cfg:
                shared_bridge_cfg[key] = _positive_int(bridge_payload.get(key), int(shared_bridge_cfg.get(key, fallback) or fallback))

    def _persist_last_started_role_mode(role_mode: str) -> None:
        normalized = normalize_role_mode(role_mode)
        if normalized not in {"internal", "external"}:
            return
        merged = copy.deepcopy(container.config if isinstance(container.config, dict) else {})
        deployment = _ensure_config_dict_path(merged, "common", "deployment")
        if deployment.get("last_started_role_mode") == normalized:
            return
        deployment["last_started_role_mode"] = normalized
        saved = save_settings(merged, container.config_path)
        container.config = copy.deepcopy(saved)
        container.runtime_config = adapt_runtime_config(container.config)

    def _persist_startup_role_selection(role_mode: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = normalize_role_mode(role_mode)
        if normalized not in {"internal", "external"}:
            raise ValueError("请选择有效角色: internal 或 external")

        merged = copy.deepcopy(container.config if isinstance(container.config, dict) else {})
        deployment = _ensure_config_dict_path(merged, "common", "deployment")
        shared_bridge_cfg = _ensure_config_dict_path(merged, "common", "shared_bridge")

        deployment["role_mode"] = normalized
        deployment["last_started_role_mode"] = normalized
        deployment["node_label"] = _role_label(normalized)
        _apply_shared_bridge_role_patch(shared_bridge_cfg, normalized, payload)

        if merged == (container.config if isinstance(container.config, dict) else {}):
            saved = copy.deepcopy(container.config if isinstance(container.config, dict) else merged)
        else:
            saved = save_settings(merged, container.config_path)
        apply_snapshot = getattr(container, "apply_config_snapshot", None)
        if callable(apply_snapshot):
            apply_snapshot(saved, mode="light")
        else:
            container.config = copy.deepcopy(saved)
            container.runtime_config = adapt_runtime_config(container.config)
        return {
            "role_mode": normalized,
            "node_label": deployment["node_label"],
            "shared_bridge_root": str(shared_bridge_cfg.get("root_dir", "") or "").strip(),
        }

    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        _install_windows_asyncio_exception_filter(container)
        _app.state.runtime_services_activated = False
        _app.state.runtime_activation_phase = "idle"
        _app.state.runtime_activation_error = ""
        _app.state.runtime_activation_step = ""
        _app.state.runtime_activation_started_at = ""
        _app.state.runtime_activation_worker = None
        _app.state.startup_role_confirmed = False
        _app.state.startup_role_user_exited = False
        initial_role_mode = _deployment_role_mode()
        if initial_role_mode in {"internal", "external"}:
            container.add_system_log(f"[启动] 当前固定为{_role_label(initial_role_mode)}，将自动进入对应系统")
        else:
            container.add_system_log("[启动] 当前未配置固定端角色，保留最小控制台壳")
        runtime_status_coordinator = getattr(container, "runtime_status_coordinator", None)
        if runtime_status_coordinator is not None:
            try:
                runtime_status_coordinator.start()
            except Exception as exc:  # noqa: BLE001
                container.add_system_log(f"[运行状态] 启动状态快照后台线程失败: {exc}")
        if initial_role_mode in {"internal", "external"}:
            def _auto_activate_fixed_role() -> None:
                try:
                    _activate_runtime_services(source=f"{_role_label(initial_role_mode)}固定启动")
                except Exception as exc:  # noqa: BLE001
                    app.state.runtime_activation_phase = "failed"
                    app.state.runtime_activation_error = str(exc)
                    app.state.runtime_activation_step = "failed"
                    try:
                        container.add_system_log(f"[启动] 固定端自动进入失败: {exc}")
                    except Exception:  # noqa: BLE001
                        pass

            worker = threading.Thread(
                target=_auto_activate_fixed_role,
                name="fixed-role-runtime-activation",
                daemon=True,
            )
            _app.state.runtime_activation_worker = worker
            worker.start()

        try:
            yield
        finally:
            runtime_status_coordinator = getattr(container, "runtime_status_coordinator", None)
            if runtime_status_coordinator is not None:
                try:
                    runtime_status_coordinator.stop()
                except Exception:  # noqa: BLE001
                    pass
            if container.scheduler:
                container.stop_scheduler(source="关闭自动")
            if container.handover_scheduler_manager:
                container.stop_handover_scheduler(source="关闭自动")
            if container.wet_bulb_collection_scheduler:
                container.stop_wet_bulb_collection_scheduler(source="关闭自动")
            if container.day_metric_upload_scheduler:
                container.stop_day_metric_upload_scheduler(source="关闭自动")
            if container.alarm_event_upload_scheduler:
                container.stop_alarm_event_upload_scheduler(source="关闭自动")
            if container.monthly_change_report_scheduler:
                container.stop_monthly_change_report_scheduler(source="关闭自动")
            if container.monthly_event_report_scheduler:
                container.stop_monthly_event_report_scheduler(source="关闭自动")
            if container.updater_service:
                container.stop_updater(source="关闭自动")
            if container.alert_log_uploader:
                container.stop_alert_log_uploader(source="关闭自动")
            if container.shared_bridge_service:
                container.stop_shared_bridge(source="关闭自动")
            container.job_service.shutdown_task_engine()
            container.add_system_log("Web控制台已关闭")

    app = FastAPI(
        title="全景平台月报 Web 控制台",
        version="3.0.0",
        lifespan=_lifespan if enable_lifespan else None,
    )
    app.state.container = container
    app.state.started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    app.state.source_frontend_asset_version = datetime.now().strftime("%Y%m%d%H%M%S")
    app.state.source_frontend_asset_prefix = f"/assets-src/{app.state.source_frontend_asset_version}"
    app.state.runtime_services_activated = False
    app.state.runtime_activation_phase = "idle"
    app.state.runtime_activation_error = ""
    app.state.runtime_activation_step = ""
    app.state.runtime_activation_started_at = ""
    app.state.runtime_activation_worker = None
    app.state.startup_role_confirmed = False
    app.state.startup_role_user_exited = False
    runtime_state_root = resolve_runtime_state_root(
        runtime_config=container.runtime_config,
        app_dir=get_app_dir(),
    )
    container.runtime_status_coordinator = RuntimeStatusCoordinator(
        container=container,
        runtime_state_root=runtime_state_root,
        app_state_getter=lambda: {
            "runtime_activated": bool(getattr(app.state, "runtime_services_activated", False)),
            "activation_phase": str(getattr(app.state, "runtime_activation_phase", "") or "").strip(),
            "activation_error": str(getattr(app.state, "runtime_activation_error", "") or "").strip(),
            "activation_step": str(getattr(app.state, "runtime_activation_step", "") or "").strip(),
            "startup_role_confirmed": bool(getattr(app.state, "startup_role_confirmed", False)),
            "startup_role_user_exited": bool(getattr(app.state, "startup_role_user_exited", False)),
            "started_at": str(getattr(app.state, "started_at", "") or "").strip(),
        },
        emit_log=lambda text: container.add_system_log(text, suppress_alert_upload=True),
        refresh_interval_sec=10.0,
    )
    container.runtime_activation_progress_callback = lambda step: setattr(
        app.state,
        "runtime_activation_step",
        str(step or "").strip(),
    )

    @app.middleware("http")
    async def restrict_external_access(request: Request, call_next):
        path = str(request.url.path or "").strip() or "/"
        if (
            _is_lan_console_client(request)
            or _is_externally_allowed_path(path)
            or (
                not bool(getattr(app.state, "runtime_services_activated", False))
                and _is_role_selection_allowed_path(path)
            )
        ):
            return await call_next(request)
        return Response(status_code=404)

    @app.middleware("http")
    async def guard_role_selection_only_runtime(request: Request, call_next):
        path = str(request.url.path or "").strip() or "/"
        if _is_role_selection_allowed_path(path):
            return await call_next(request)
        if not path.startswith("/api/"):
            return await call_next(request)
        if bool(getattr(app.state, "runtime_services_activated", False)):
            return await call_next(request)
        return JSONResponse(
            content={"detail": "当前未进入内网端或外网端，请先在角色选择页进入系统。"},
            status_code=409,
        )

    _INTERNAL_BLOCKED_PREFIXES = (
        "/api/jobs/",
        "/api/handover/",
        "/handover/review/",
        "/api/scheduler/",
        "/api/handover-scheduler/",
        "/api/wet-bulb-collection-scheduler/",
    )

    @app.middleware("http")
    async def guard_internal_role_routes(request: Request, call_next):
        role_mode = _deployment_role_mode()
        path = str(request.url.path or "").strip() or "/"
        if path == "/api/handover/daily-report/context":
            return await call_next(request)
        if role_mode == "internal" and any(path.startswith(prefix) for prefix in _INTERNAL_BLOCKED_PREFIXES):
            return JSONResponse(
                content={"detail": "当前为内网端，本地管理页不提供该业务入口，请在外网端发起。"},
                status_code=409,
            )
        return await call_next(request)

    def _deployment_role_mode() -> str:
        snapshot = container.deployment_snapshot() if hasattr(container, "deployment_snapshot") else {}
        if not isinstance(snapshot, dict):
            return ""
        text = str(snapshot.get("role_mode", "") or "").strip().lower()
        if text in {"internal", "external"}:
            return text
        return ""

    def _activate_runtime_services(source: str = "启动角色确认") -> Dict[str, Any]:
        role_mode = _deployment_role_mode()
        if role_mode not in {"internal", "external"}:
            app.state.runtime_activation_phase = "failed"
            app.state.runtime_activation_error = "请先选择有效角色后再启动后台运行时"
            app.state.startup_role_confirmed = False
            return {
                "ok": False,
                "activated": False,
                "already_active": False,
                "role_mode": role_mode,
                "error": app.state.runtime_activation_error,
            }
        if bool(getattr(app.state, "runtime_services_activated", False)):
            app.state.startup_role_confirmed = True
            app.state.startup_role_user_exited = False
            try:
                _persist_last_started_role_mode(role_mode)
            except Exception as exc:  # noqa: BLE001
                container.add_system_log(f"[启动] 回写最近成功启动角色失败：{exc}")
            return {
                "ok": True,
                "activated": True,
                "already_active": True,
                "role_mode": role_mode,
            }

        app.state.runtime_activation_phase = "activating"
        app.state.runtime_activation_error = ""
        app.state.runtime_activation_step = "starting_runtime_services"
        app.state.runtime_activation_started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            result = container.start_role_runtime_services(source=source)
            if role_mode == "external":
                container.add_system_log(
                    f"[调度] 执行器绑定完成: {container.scheduler_executor_name()}, "
                    f"executor_bound={container.is_scheduler_executor_bound()}"
                )
                container.add_system_log(
                    f"[交接班调度] 执行器绑定完成: {container.handover_scheduler_executor_name()}, "
                    f"executor_bound={container.is_handover_scheduler_executor_bound()}"
                )
                container.add_system_log(
                    f"[湿球温度定时采集调度] 执行器绑定完成: {container.wet_bulb_collection_scheduler_executor_name()}, "
                    f"executor_bound={container.is_wet_bulb_collection_scheduler_executor_bound()}"
                )
                container.add_system_log(
                    f"[12项独立上传调度] 执行器绑定完成: {container.day_metric_upload_scheduler_executor_name()}, "
                    f"executor_bound={container.is_day_metric_upload_scheduler_executor_bound()}"
                )
                container.add_system_log(
                    f"[告警信息上传调度] 执行器绑定完成: {container.alarm_event_upload_scheduler_executor_name()}, "
                    f"executor_bound={container.is_alarm_event_upload_scheduler_executor_bound()}"
                )
                container.add_system_log(
                    f"[月度变更统计表调度] 执行器绑定完成: {container.monthly_change_report_scheduler_executor_name()}, "
                    f"executor_bound={container.is_monthly_change_report_scheduler_executor_bound()}"
                )
                container.add_system_log(
                    f"[月度事件统计表调度] 执行器绑定完成: {container.monthly_event_report_scheduler_executor_name()}, "
                    f"executor_bound={container.is_monthly_event_report_scheduler_executor_bound()}"
                )
                container.add_system_log(
                    "[访问控制] 已启用局域网页面隔离：仅对外开放 /handover/review/*、/api/handover/review/* 与 /assets/*"
                )
                app.state.runtime_activation_step = "initializing_handover_daily_report_auth"
                _initialize_handover_daily_report_auth(container)
                app.state.runtime_activation_step = "probing_handover_review_access"
                schedule_handover_review_access_startup_probe(container)
            app.state.runtime_services_activated = True
            app.state.runtime_activation_phase = "activated"
            app.state.runtime_activation_error = ""
            app.state.runtime_activation_step = "activated"
            app.state.startup_role_confirmed = True
            app.state.startup_role_user_exited = False
            runtime_status_coordinator = getattr(container, "runtime_status_coordinator", None)
            if runtime_status_coordinator is not None:
                try:
                    runtime_status_coordinator.request_refresh(reason="runtime_activated")
                except Exception:  # noqa: BLE001
                    pass
            if role_mode == "external":
                try:
                    from app.modules.handover_review.api.routes import (
                        schedule_latest_review_documents_warmup,
                    )

                    schedule_latest_review_documents_warmup(
                        container,
                        reason="external_runtime_activated",
                    )
                except Exception as exc:  # noqa: BLE001
                    container.add_system_log(f"[交接班][审核预热] 运行时启动后预热失败: {exc}")
            try:
                _persist_last_started_role_mode(role_mode)
            except Exception as exc:  # noqa: BLE001
                container.add_system_log(f"[启动] 回写最近成功启动角色失败：{exc}")
            return {
                "ok": True,
                "activated": True,
                "already_active": False,
                "role_mode": role_mode,
                **(result if isinstance(result, dict) else {}),
            }
        except Exception as exc:  # noqa: BLE001
            app.state.runtime_activation_phase = "failed"
            app.state.runtime_activation_error = str(exc)
            app.state.runtime_activation_step = "failed"
            app.state.startup_role_confirmed = False
            return {
                "ok": False,
                "activated": False,
                "already_active": False,
                "role_mode": role_mode,
                "error": str(exc),
            }

    def _resolve_bridge_runtime():
        bridge_snapshot = container.shared_bridge_snapshot() if hasattr(container, "shared_bridge_snapshot") else {}
        bridge_enabled = isinstance(bridge_snapshot, dict) and bool(bridge_snapshot.get("enabled", False))
        bridge_root = str(bridge_snapshot.get("root_dir", "") or "").strip() if isinstance(bridge_snapshot, dict) else ""
        bridge_service = getattr(container, "shared_bridge_service", None)
        if not bridge_enabled or not bridge_root:
            return None, "shared_bridge_disabled"
        if bridge_service is None:
            return None, "shared_bridge_service_unavailable"
        return bridge_service, ""

    def _bridge_runtime_error_text(error_code: str) -> str:
        text = str(error_code or "").strip().lower()
        if text == "shared_bridge_disabled":
            return "共享桥接未启用或共享目录未配置"
        if text == "shared_bridge_service_unavailable":
            return "共享桥接服务不可用"
        return str(error_code or "").strip() or "-"

    def _current_hour_bucket() -> str:
        return datetime.now().strftime("%Y-%m-%d %H")

    @app.post("/api/runtime/activate-startup", response_model=None)
    async def activate_startup_runtime(request: Request) -> JSONResponse:
        payload: Dict[str, Any] = {}
        try:
            incoming = await request.json()
            if isinstance(incoming, dict):
                payload = incoming
        except Exception:
            payload = {}

        source = str(payload.get("source", "") or "").strip() or "启动角色确认"
        startup_handoff_nonce = str(payload.get("startup_handoff_nonce", "") or "").strip()

        def _activate_from_payload() -> Dict[str, Any]:
            with startup_runtime_activation_lock:
                requested_role = normalize_role_mode(
                    payload.get("role_mode") or payload.get("target_role_mode") or ""
                )
                saved_role: Dict[str, Any] | None = None
                if requested_role in {"internal", "external"}:
                    current_role = _deployment_role_mode()
                    if bool(getattr(app.state, "runtime_services_activated", False)) and current_role != requested_role:
                        container.stop_role_runtime_services(source=f"{source}-切换角色前停止当前系统")
                        app.state.runtime_services_activated = False
                        app.state.runtime_activation_phase = "idle"
                        app.state.runtime_activation_error = ""
                        app.state.startup_role_confirmed = False
                    saved_role = _persist_startup_role_selection(requested_role, payload)

                result = _activate_runtime_services(source)
                if saved_role is not None:
                    result["saved_role"] = saved_role
                result["phase"] = str(getattr(app.state, "runtime_activation_phase", "") or "").strip()
                result["step"] = str(getattr(app.state, "runtime_activation_step", "") or "").strip()
                return result

        def _activation_worker_alive() -> bool:
            worker = getattr(app.state, "runtime_activation_worker", None)
            return isinstance(worker, threading.Thread) and worker.is_alive()

        def _run_activation_in_background() -> None:
            try:
                result = _activate_from_payload()
                if bool(result.get("ok", False)):
                    get_startup_role_handoff = getattr(container, "get_startup_role_handoff", None)
                    clear_startup_role_handoff = getattr(container, "clear_startup_role_handoff", None)
                    if callable(get_startup_role_handoff) and callable(clear_startup_role_handoff):
                        handoff = get_startup_role_handoff()
                        handoff_nonce = str(handoff.get("nonce", "") or "").strip() if isinstance(handoff, dict) else ""
                        if handoff_nonce and (
                            source == "startup_role_resume_after_restart"
                            or (startup_handoff_nonce and startup_handoff_nonce == handoff_nonce)
                        ):
                            clear_startup_role_handoff()
            except Exception as exc:  # noqa: BLE001
                app.state.runtime_activation_phase = "failed"
                app.state.runtime_activation_error = str(exc)
                app.state.runtime_activation_step = "failed"
                try:
                    container.add_system_log(f"[启动] 启动角色后台激活失败: {exc}")
                except Exception:  # noqa: BLE001
                    pass
            finally:
                app.state.runtime_activation_worker = None

        def _start_activation() -> Dict[str, Any]:
            with startup_runtime_activation_lock:
                requested_role = normalize_role_mode(
                    payload.get("role_mode") or payload.get("target_role_mode") or ""
                )
                current_role = _deployment_role_mode()
                effective_role = requested_role if requested_role in {"internal", "external"} else current_role
                current_phase = str(getattr(app.state, "runtime_activation_phase", "") or "").strip().lower()
                if effective_role in {"internal", "external"}:
                    if bool(getattr(app.state, "runtime_services_activated", False)) and current_role == effective_role:
                        return {
                            "ok": True,
                            "activated": True,
                            "already_active": True,
                            "pending": False,
                            "role_mode": effective_role,
                            "phase": current_phase or "activated",
                            "step": str(getattr(app.state, "runtime_activation_step", "") or "").strip(),
                        }
                    if current_role == effective_role and current_phase == "activating" and _activation_worker_alive():
                        return {
                            "ok": True,
                            "activated": False,
                            "already_active": False,
                            "pending": True,
                            "role_mode": effective_role,
                            "phase": current_phase,
                            "step": str(getattr(app.state, "runtime_activation_step", "") or "").strip(),
                        }
                if current_phase == "activating" and not _activation_worker_alive():
                    app.state.runtime_activation_phase = "idle"
                    app.state.runtime_activation_error = ""
                    app.state.runtime_activation_step = ""
                app.state.runtime_activation_phase = "activating"
                app.state.runtime_activation_error = ""
                app.state.runtime_activation_step = "queued"
                app.state.runtime_activation_started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                worker = threading.Thread(
                    target=_run_activation_in_background,
                    name="startup-runtime-activation",
                    daemon=True,
                )
                app.state.runtime_activation_worker = worker
                worker.start()
                return {
                    "ok": True,
                    "activated": False,
                    "already_active": False,
                    "pending": True,
                    "role_mode": effective_role,
                    "phase": "activating",
                    "step": str(getattr(app.state, "runtime_activation_step", "") or "").strip(),
                }

        try:
            result = await asyncio.to_thread(_start_activation)
            return JSONResponse(content=result)
        except Exception as exc:  # noqa: BLE001
            return JSONResponse(
                content={
                    "ok": False,
                    "activated": False,
                    "phase": "failed",
                    "error": str(exc),
                    "role_mode": _deployment_role_mode(),
                }
            )

    @app.post("/api/runtime/exit-current", response_model=None)
    async def exit_current_runtime(request: Request) -> JSONResponse:
        payload: Dict[str, Any] = {}
        try:
            incoming = await request.json()
            if isinstance(incoming, dict):
                payload = incoming
        except Exception:
            payload = {}
        source = str(payload.get("source", "") or "").strip() or "退出当前系统"
        try:
            result = await asyncio.to_thread(container.stop_role_runtime_services, source)
            app.state.runtime_services_activated = False
            app.state.runtime_activation_phase = "idle"
            app.state.runtime_activation_error = ""
            app.state.startup_role_confirmed = False
            app.state.startup_role_user_exited = True
            clear_startup_role_handoff = getattr(container, "clear_startup_role_handoff", None)
            if callable(clear_startup_role_handoff):
                clear_startup_role_handoff()
            runtime_status_coordinator = getattr(container, "runtime_status_coordinator", None)
            if runtime_status_coordinator is not None:
                try:
                    runtime_status_coordinator.request_refresh(reason="runtime_exited")
                except Exception:  # noqa: BLE001
                    pass
            return JSONResponse(
                content={
                    "ok": True,
                    "deactivated": True,
                    "role_mode": _deployment_role_mode(),
                    **(result if isinstance(result, dict) else {}),
                }
            )
        except Exception as exc:  # noqa: BLE001
            app.state.runtime_activation_phase = "failed"
            app.state.runtime_activation_error = str(exc)
            return JSONResponse(
                content={
                    "ok": False,
                    "deactivated": False,
                    "error": str(exc),
                    "role_mode": _deployment_role_mode(),
                },
                status_code=500,
            )

    def _format_bucket_age_hours_text(value: Any) -> str:
        try:
            age_hours = float(value)
        except (TypeError, ValueError):
            return ""
        if age_hours <= 0:
            return "0 小时"
        rounded = round(age_hours, 1)
        if rounded.is_integer():
            return f"{int(rounded)} 小时"
        return f"{rounded:.1f} 小时"

    def _source_cache_wait_text(feature_name: str) -> str:
        return f"等待最新共享文件更新：{feature_name}源文件尚未登记或尚未完成下载"

    def _build_latest_cache_wait_text(feature_name: str, selection: Dict[str, Any]) -> str:
        best_bucket_key = str(selection.get("best_bucket_key", "") or "").strip() if isinstance(selection, dict) else ""
        best_bucket_age_hours = selection.get("best_bucket_age_hours") if isinstance(selection, dict) else None
        is_best_bucket_too_old = bool(selection.get("is_best_bucket_too_old", False)) if isinstance(selection, dict) else False
        missing_buildings = [
            str(item or "").strip()
            for item in (selection.get("missing_buildings", []) if isinstance(selection, dict) else [])
            if str(item or "").strip()
        ]
        stale_buildings = [
            str(item or "").strip()
            for item in (selection.get("stale_buildings", []) if isinstance(selection, dict) else [])
            if str(item or "").strip()
        ]
        blocked_buildings = [
            {
                "building": str(item.get("building", "") or "").strip(),
                "reason": str(item.get("reason", "") or "").strip(),
            }
            for item in (selection.get("blocked_buildings", []) if isinstance(selection, dict) else [])
            if isinstance(item, dict) and str(item.get("building", "") or "").strip()
        ]
        if is_best_bucket_too_old:
            age_text = _format_bucket_age_hours_text(best_bucket_age_hours)
            bucket_text = best_bucket_key or "未知时间桶"
            if age_text:
                return f"等待最新共享文件更新：{feature_name}源文件当前最新时间桶 {bucket_text} 距现在约 {age_text}，已超过 3 小时。"
            return f"等待最新共享文件更新：{feature_name}源文件当前最新时间桶 {bucket_text} 已超过 3 小时。"
        if stale_buildings:
            return (
                f"等待过旧楼栋共享文件更新：{feature_name}源文件已有回退版本，但以下楼栋较最新时间桶落后超过 3 桶："
                + " / ".join(stale_buildings)
            )
        if blocked_buildings:
            blocked_text = " / ".join(
                f"{item['building']} {item['reason']}".strip()
                for item in blocked_buildings
            ).strip()
            if blocked_text:
                return f"等待内网恢复：{blocked_text}"
        if missing_buildings:
            return (
                f"等待缺失楼栋共享文件补齐：{feature_name}源文件尚未登记或文件不可访问，缺失楼栋："
                + " / ".join(missing_buildings)
            )
        return _source_cache_wait_text(feature_name)

    def _start_external_cache_job(
        *,
        name: str,
        feature: str,
        resource_key: str,
        run_func,
        dedupe_key: str = "",
    ):
        job_kwargs = {
            "name": name,
            "run_func": run_func,
            "resource_keys": [resource_key],
            "priority": "scheduler",
            "feature": feature,
            "submitted_by": "scheduler",
        }
        dedupe_text = str(dedupe_key or "").strip()
        if dedupe_text:
            job_kwargs["dedupe_key"] = dedupe_text
        return container.job_service.start_job(**job_kwargs)

    def scheduler_callback(source: str) -> tuple[bool, str]:
        from app.modules.notify.service.webhook_notify_service import WebhookNotifyService
        from app.modules.report_pipeline.service.monthly_cache_continue_service import run_monthly_from_file_items
        from app.modules.report_pipeline.service.orchestrator_service import OrchestratorService

        runtime_config = container.runtime_config
        notify = WebhookNotifyService(runtime_config)
        role_mode = _deployment_role_mode()

        if role_mode == "internal":
            container.add_system_log("[调度] 当前为内网端，自动流程调度跳过；请在外网端启用该调度")
            return True, "internal_role_skip"

        if role_mode == "external":
            bridge_service, bridge_error = _resolve_bridge_runtime()
            if bridge_service is None:
                detail = f"共享桥接未就绪，外网端无法执行自动流程调度：{_bridge_runtime_error_text(bridge_error)}"
                container.add_system_log(f"[调度] {detail}")
                return False, detail
            try:
                target_buildings = bridge_service.get_source_cache_buildings()
                selection = bridge_service.get_latest_source_cache_selection(
                    source_family="monthly_report_family",
                    buildings=target_buildings,
                )
                cached_entries = list(selection.get("selected_entries", [])) if isinstance(selection, dict) else []
                if not bool(selection.get("can_proceed", False)) or len(cached_entries) < len(target_buildings):
                    dedupe_key = f"auto_once:scheduler:shared_bridge:{str(selection.get('best_bucket_key', '') or '').strip()}"
                    job, bridge_task = start_waiting_bridge_job(
                        job_service=container.job_service,
                        bridge_service=bridge_service,
                        name=source,
                        worker_handler="auto_once",
                        worker_payload={"source": source},
                        resource_keys=["shared_bridge:monthly_report"],
                        priority="scheduler",
                        feature="auto_once",
                        dedupe_key=dedupe_key,
                        submitted_by="scheduler",
                        bridge_get_or_create_name="get_or_create_monthly_auto_once_task",
                        bridge_create_name="create_monthly_auto_once_task",
                        bridge_kwargs={"requested_by": "scheduler", "source": source},
                    )
                    detail = _build_latest_cache_wait_text("月报", selection if isinstance(selection, dict) else {})
                    accepted_detail = (
                        f"{detail} 已受理共享桥接任务 task_id={str(bridge_task.get('task_id', '') or '-').strip() or '-'}, job_id={job.job_id}"
                    )
                    container.add_system_log(f"[调度] {accepted_detail}")
                    return True, accepted_detail

                def _run_from_cache(emit_log):
                    file_items = [
                        {
                            "building": str(item.get("building", "") or "").strip(),
                            "file_path": str(item.get("file_path", "") or "").strip(),
                            "upload_date": str(
                                item.get("metadata", {}).get("upload_date", "")
                                or item.get("duty_date", "")
                                or _current_hour_bucket()[:10]
                            ).strip(),
                        }
                        for item in cached_entries
                    ]
                    return run_monthly_from_file_items(
                        runtime_config,
                        file_items=file_items,
                        emit_log=emit_log,
                        source_label="月报共享文件",
                    )

                job = _start_external_cache_job(
                    name="自动流程调度-月报共享文件",
                    feature="monthly_cache_latest",
                    resource_key="shared_bridge:monthly_report",
                    run_func=_run_from_cache,
                )
                detail = f"已提交月报共享文件继续处理任务 job_id={job.job_id}"
                container.add_system_log(f"[调度] {detail}")
                return True, detail
            except Exception as exc:  # noqa: BLE001
                notify.send_failure(stage=source, detail=str(exc), emit_log=container.add_system_log)
                return False, str(exc)

        orchestrator = OrchestratorService(runtime_config)

        def _run(emit_log):
            try:
                auto_result = orchestrator.run_auto_once(emit_log, source=source)
                return {
                    "auto_result": auto_result,
                }
            except Exception as exc:  # noqa: BLE001
                notify.send_failure(stage=source, detail=str(exc), emit_log=emit_log)
                raise

        try:
            job = container.job_service.start_job(
                name=source,
                run_func=_run,
                resource_keys=["network:pipeline"],
                priority="scheduler",
            )
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
        done = container.job_service.wait_job(job.job_id)
        if done.status == "success":
            return True, "ok"
        return False, done.error or done.summary or "任务失败"

    def handover_scheduler_callback(slot: str, source: str) -> tuple[bool, str]:
        from app.modules.notify.service.webhook_notify_service import WebhookNotifyService
        from app.modules.report_pipeline.service.orchestrator_service import OrchestratorService

        runtime_config = container.runtime_config
        notify = WebhookNotifyService(runtime_config)
        role_mode = _deployment_role_mode()

        now = datetime.now()
        if slot == "morning":
            duty_date = (now.date() - timedelta(days=1)).strftime("%Y-%m-%d")
            duty_shift = "night"
            slot_name = "上午"
        elif slot == "afternoon":
            duty_date = now.date().strftime("%Y-%m-%d")
            duty_shift = "day"
            slot_name = "下午"
        else:
            return False, f"未知交接班调度时段: {slot}"

        job_name = f"{source}-交接班定时（{slot_name}）"

        if role_mode == "internal":
            container.add_system_log("[交接班调度] 当前为内网端，调度跳过；请在外网端启用该调度")
            return True, "internal_role_skip"

        if role_mode == "external":
            bridge_service, bridge_error = _resolve_bridge_runtime()
            if bridge_service is None:
                detail = f"共享桥接未就绪，外网端无法执行交接班调度：{_bridge_runtime_error_text(bridge_error)}"
                container.add_system_log(f"[交接班调度] {detail}")
                return False, detail
            try:
                target_buildings = bridge_service.get_source_cache_buildings()
                selection = bridge_service.get_latest_source_cache_selection(
                    source_family="handover_log_family",
                    buildings=target_buildings,
                )
                cached_entries = list(selection.get("selected_entries", [])) if isinstance(selection, dict) else []
                capacity_entries = bridge_service.get_handover_capacity_by_date_cache_entries(
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    buildings=target_buildings,
                )
                if (
                    not bool(selection.get("can_proceed", False))
                    or len(cached_entries) < len(target_buildings)
                    or len(capacity_entries) < len(target_buildings)
                ):
                    dedupe_key = f"handover:scheduler:{slot}:{duty_date}:{duty_shift}"
                    job, bridge_task = start_waiting_bridge_job(
                        job_service=container.job_service,
                        bridge_service=bridge_service,
                        name=job_name,
                        worker_handler="handover_from_download",
                        worker_payload={"buildings": target_buildings, "end_time": None, "duty_date": duty_date, "duty_shift": duty_shift},
                        resource_keys=["shared_bridge:handover"],
                        priority="scheduler",
                        feature="handover_from_download",
                        dedupe_key=dedupe_key,
                        submitted_by="scheduler",
                        bridge_get_or_create_name="get_or_create_handover_from_download_task",
                        bridge_create_name="create_handover_from_download_task",
                        bridge_kwargs={
                            "buildings": target_buildings,
                            "end_time": None,
                            "duty_date": duty_date,
                            "duty_shift": duty_shift,
                            "requested_by": "scheduler",
                        },
                    )
                    detail = _build_latest_cache_wait_text("交接班", selection if isinstance(selection, dict) else {})
                    accepted_detail = (
                        f"{detail} 已受理共享桥接任务 task_id={str(bridge_task.get('task_id', '') or '-').strip() or '-'}, job_id={job.job_id}"
                    )
                    container.add_system_log(f"[交接班调度] {accepted_detail}")
                    return True, accepted_detail

                def _run_from_cache(emit_log):
                    orchestrator = OrchestratorService(runtime_config)
                    building_files = [
                        (
                            str(item.get("building", "") or "").strip(),
                            str(item.get("file_path", "") or "").strip(),
                        )
                        for item in cached_entries
                    ]
                    capacity_building_files = [
                        (
                            str(item.get("building", "") or "").strip(),
                            str(item.get("file_path", "") or "").strip(),
                        )
                        for item in capacity_entries
                    ]
                    return orchestrator.run_handover_from_files(
                        building_files=building_files,
                        capacity_building_files=capacity_building_files,
                        end_time=None,
                        duty_date=duty_date,
                        duty_shift=duty_shift,
                        emit_log=emit_log,
                    )

                job = _start_external_cache_job(
                    name=f"{job_name}-共享文件",
                    feature="handover_cache_latest",
                    resource_key="shared_bridge:handover",
                    run_func=_run_from_cache,
                )
                detail = (
                    "已提交交接班共享文件继续处理任务 "
                    f"job_id={job.job_id}, duty_date={duty_date}, duty_shift={duty_shift}"
                )
                container.add_system_log(f"[交接班调度] {detail}")
                return True, detail
            except Exception as exc:  # noqa: BLE001
                notify.send_failure(stage=f"{source}-交接班日志", detail=str(exc), emit_log=container.add_system_log)
                return False, str(exc)

        orchestrator = OrchestratorService(runtime_config)

        def _run(emit_log):
            try:
                result = orchestrator.run_handover_from_download(
                    buildings=None,
                    end_time=None,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    emit_log=emit_log,
                )
                failure_summary = orchestrator.build_handover_download_failure_summary(result)
                if failure_summary:
                    emit_log(
                        "[交接班调度] 失败汇总告警: "
                        f"buildings={str(failure_summary.get('building', '') or '-').strip() or '-'}, "
                        f"detail={str(failure_summary.get('detail', '') or '-').strip() or '-'}"
                    )
                    notify.send_failure(
                        stage=f"{source}-交接班日志",
                        detail=str(failure_summary.get('detail', '') or '').strip() or "交接班内网下载存在失败楼栋",
                        building=str(failure_summary.get('building', '') or '').strip() or None,
                        emit_log=emit_log,
                    )
                return result
            except Exception as exc:  # noqa: BLE001
                notify.send_failure(stage=f"{source}-交接班日志", detail=str(exc), emit_log=emit_log)
                raise

        try:
            job = container.job_service.start_job(
                name=job_name,
                run_func=_run,
                resource_keys=["network:pipeline"],
                priority="scheduler",
            )
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
        done = container.job_service.wait_job(job.job_id)
        if done.status == "success":
            return True, "ok"
        return False, done.error or done.summary or "任务失败"

    def wet_bulb_collection_scheduler_callback(source: str) -> tuple[bool, str]:
        from app.modules.notify.service.webhook_notify_service import WebhookNotifyService
        from app.modules.report_pipeline.service.orchestrator_service import OrchestratorService
        from handover_log_module.service.wet_bulb_collection_service import WetBulbCollectionService

        runtime_config = container.runtime_config
        role_mode = _deployment_role_mode()

        if role_mode == "internal":
            container.add_system_log("[湿球温度定时采集调度] 当前为内网端，调度跳过；请在外网端启用该调度")
            return True, "internal_role_skip"

        if role_mode == "external":
            bridge_service, bridge_error = _resolve_bridge_runtime()
            if bridge_service is None:
                detail = f"共享桥接未就绪，外网端无法执行湿球温度调度：{_bridge_runtime_error_text(bridge_error)}"
                container.add_system_log(f"[湿球温度定时采集调度] {detail}")
                container.record_wet_bulb_collection_external_run(
                    status="failed",
                    source="scheduler",
                    detail=detail,
                    duration_ms=0,
                )
                return False, detail
            try:
                target_buildings = bridge_service.get_source_cache_buildings()
                selection = bridge_service.get_latest_source_cache_selection(
                    source_family="handover_log_family",
                    buildings=target_buildings,
                )
                cached_entries = list(selection.get("selected_entries", [])) if isinstance(selection, dict) else []
                if not bool(selection.get("can_proceed", False)) or len(cached_entries) < len(target_buildings):
                    dedupe_key = f"wet_bulb:scheduler:shared_bridge:{str(selection.get('best_bucket_key', '') or '').strip()}"
                    job, bridge_task = start_waiting_bridge_job(
                        job_service=container.job_service,
                        bridge_service=bridge_service,
                        name=source,
                        worker_handler="wet_bulb_collection_run",
                        worker_payload={"source": source},
                        resource_keys=["shared_bridge:wet_bulb"],
                        priority="scheduler",
                        feature="wet_bulb_collection_run",
                        dedupe_key=dedupe_key,
                        submitted_by="scheduler",
                        bridge_get_or_create_name="get_or_create_wet_bulb_collection_task",
                        bridge_create_name="create_wet_bulb_collection_task",
                        bridge_kwargs={"buildings": target_buildings, "requested_by": "scheduler"},
                    )
                    detail = _build_latest_cache_wait_text("湿球温度", selection if isinstance(selection, dict) else {})
                    accepted_detail = (
                        f"{detail} 已受理共享桥接任务 task_id={str(bridge_task.get('task_id', '') or '-').strip() or '-'}, job_id={job.job_id}"
                    )
                    container.add_system_log(f"[湿球温度定时采集调度] {accepted_detail}")
                    container.record_wet_bulb_collection_external_run(
                        status="accepted",
                        source="scheduler",
                        detail=accepted_detail,
                        duration_ms=0,
                    )
                    return True, accepted_detail

                def _run_from_cache(emit_log):
                    service = WetBulbCollectionService(runtime_config)
                    source_units = [
                        {
                            "building": str(item.get("building", "") or "").strip(),
                            "file_path": str(item.get("file_path", "") or "").strip(),
                        }
                        for item in cached_entries
                    ]
                    return service.continue_from_source_units(source_units=source_units, emit_log=emit_log)

                job = _start_external_cache_job(
                    name="湿球温度定时采集-共享文件",
                    feature="wet_bulb_cache_latest",
                    resource_key="shared_bridge:wet_bulb",
                    run_func=_run_from_cache,
                )
                detail = f"已提交湿球温度共享文件继续处理任务 job_id={job.job_id}"
                container.add_system_log(f"[湿球温度定时采集调度] {detail}")
                container.record_wet_bulb_collection_external_run(
                    status="success",
                    source="scheduler",
                    detail=detail,
                    duration_ms=0,
                )
                return True, detail
            except Exception as exc:  # noqa: BLE001
                error_text = str(exc)
                container.add_system_log(f"[湿球温度定时采集调度] 共享文件继续处理任务提交失败：{error_text}")
                container.record_wet_bulb_collection_external_run(
                    status="failed",
                    source="scheduler",
                    detail=error_text,
                    duration_ms=0,
                )
                return False, error_text

        orchestrator = OrchestratorService(runtime_config)
        notify = WebhookNotifyService(runtime_config)

        def _run(emit_log):
            started_at = datetime.now()
            try:
                result = orchestrator.run_wet_bulb_collection(emit_log, source=source)
                duration_ms = int((datetime.now() - started_at).total_seconds() * 1000)
                container.record_wet_bulb_collection_external_run(
                    status=str(result.get("status", "ok")),
                    source="scheduler",
                    detail=str(result.get("summary", "") or ""),
                    duration_ms=duration_ms,
                )
                return result
            except Exception as exc:  # noqa: BLE001
                duration_ms = int((datetime.now() - started_at).total_seconds() * 1000)
                container.record_wet_bulb_collection_external_run(
                    status="failed",
                    source="scheduler",
                    detail=str(exc),
                    duration_ms=duration_ms,
                )
                notify.send_failure(stage=source, detail=str(exc), emit_log=emit_log)
                raise

        try:
            job = container.job_service.start_job(
                name=source,
                run_func=_run,
                resource_keys=["network:pipeline"],
                priority="scheduler",
            )
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
        done = container.job_service.wait_job(job.job_id)
        if done.status == "success":
            return True, "ok"
        return False, done.error or done.summary or "任务失败"

    def day_metric_upload_scheduler_callback(source: str) -> tuple[bool, str]:
        from handover_log_module.service.day_metric_standalone_upload_service import DayMetricStandaloneUploadService

        role_mode = _deployment_role_mode()
        if role_mode == "internal":
            container.add_system_log("[12项独立上传调度] 当前为内网端，调度跳过；请在外网端启用该调度")
            return True, "internal_role_skip"
        if role_mode != "external":
            detail = "当前未确认有效角色，无法执行12项独立上传调度"
            container.add_system_log(f"[12项独立上传调度] {detail}")
            return False, detail

        bridge_service, bridge_error = _resolve_bridge_runtime()
        if bridge_service is None:
            detail = f"共享桥接未就绪，外网端无法执行12项独立上传调度：{_bridge_runtime_error_text(bridge_error)}"
            container.add_system_log(f"[12项独立上传调度] {detail}")
            return False, detail

        target_date = datetime.now().strftime("%Y-%m-%d")
        target_buildings = [item for item in bridge_service.get_source_cache_buildings() if str(item or "").strip()]
        if not target_buildings:
            detail = "共享桥接未配置可用楼栋，无法执行12项独立上传调度"
            container.add_system_log(f"[12项独立上传调度] {detail}")
            return False, detail

        try:
            cached_entries = [
                item
                for item in bridge_service.get_day_metric_by_date_cache_entries(
                    selected_dates=[target_date],
                    buildings=target_buildings,
                )
                if str(item.get("file_path", "") or "").strip()
                and os.path.exists(str(item.get("file_path", "") or "").strip())
            ]
            expected_count = len(target_buildings)
            if len(cached_entries) < expected_count:
                try:
                    container.add_system_log("[12项独立上传调度] 按日缓存未齐全，先尝试直接复用现有交接班日志源文件")
                    bridge_service.fill_day_metric_history(
                        selected_dates=[target_date],
                        building_scope="all_enabled",
                        building=None,
                        emit_log=container.add_system_log,
                    )
                    cached_entries = [
                        item
                        for item in bridge_service.get_day_metric_by_date_cache_entries(
                            selected_dates=[target_date],
                            buildings=target_buildings,
                        )
                        if str(item.get("file_path", "") or "").strip()
                        and os.path.exists(str(item.get("file_path", "") or "").strip())
                    ]
                except Exception as exc:  # noqa: BLE001
                    container.add_system_log(f"[12项独立上传调度] 直接复用交接班日志源文件失败，继续等待内网补采同步: {exc}")
            if len(cached_entries) < expected_count:
                dedupe_key = f"day_metric_upload:scheduler:{target_date}:{'|'.join(sorted(target_buildings))}"
                job, bridge_task = start_waiting_bridge_job(
                    job_service=container.job_service,
                    bridge_service=bridge_service,
                    name=source,
                    worker_handler="day_metric_from_download",
                    worker_payload={"selected_dates": [target_date], "building_scope": "all_enabled", "building": None},
                    resource_keys=["shared_bridge:day_metric"],
                    priority="scheduler",
                    feature="day_metric_from_download",
                    dedupe_key=dedupe_key,
                    submitted_by="scheduler",
                    bridge_get_or_create_name="get_or_create_day_metric_from_download_task",
                    bridge_create_name="create_day_metric_from_download_task",
                    bridge_kwargs={
                        "selected_dates": [target_date],
                        "building_scope": "all_enabled",
                        "building": None,
                        "requested_by": "scheduler",
                    },
                )
                accepted_detail = (
                    "等待内网补采同步：12项当日源文件尚未全部到位。"
                    f" 已受理共享桥接任务 task_id={str(bridge_task.get('task_id', '') or '-').strip() or '-'},"
                    f" date={target_date}, job_id={job.job_id}"
                )
                container.add_system_log(f"[12项独立上传调度] {accepted_detail}")
                return True, accepted_detail

            def _run_from_cache(emit_log):
                source_units = [
                    {
                        "duty_date": str(item.get("duty_date", "") or "").strip(),
                        "building": str(item.get("building", "") or "").strip(),
                        "source_file": str(item.get("file_path", "") or "").strip(),
                    }
                    for item in cached_entries
                ]
                service = DayMetricStandaloneUploadService(container.runtime_config)
                return service.continue_from_source_files(
                    selected_dates=[target_date],
                    buildings=target_buildings,
                    source_units=source_units,
                    building_scope="all_enabled",
                    building=None,
                    emit_log=emit_log,
                )

            dedupe_key = f"day_metric_cache_by_date:scheduler:{target_date}:{'|'.join(sorted(target_buildings))}"
            job = _start_external_cache_job(
                name="12项独立上传-使用共享文件",
                feature="day_metric_cache_by_date",
                resource_key="shared_bridge:day_metric",
                run_func=_run_from_cache,
                dedupe_key=dedupe_key,
            )
            detail = f"已提交12项独立上传共享文件处理任务 job_id={job.job_id}, date={target_date}"
            container.add_system_log(f"[12项独立上传调度] {detail}")
            return True, detail
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            container.add_system_log(f"[12项独立上传调度] 提交失败：{error_text}")
            return False, error_text

    def alarm_event_upload_scheduler_callback(source: str) -> tuple[bool, str]:
        role_mode = _deployment_role_mode()
        if role_mode == "internal":
            container.add_system_log("[告警信息上传调度] 当前为内网端，调度跳过；请在外网端启用该调度")
            return True, "internal_role_skip"
        if role_mode != "external":
            detail = "当前未确认有效角色，无法执行告警信息上传调度"
            container.add_system_log(f"[告警信息上传调度] {detail}")
            return False, detail

        bridge_service, bridge_error = _resolve_bridge_runtime()
        if bridge_service is None:
            detail = f"共享桥接未就绪，外网端无法执行告警信息上传调度：{_bridge_runtime_error_text(bridge_error)}"
            container.add_system_log(f"[告警信息上传调度] {detail}")
            return False, detail

        def _run(emit_log):
            def _combined_log(line: str) -> None:
                text = str(line or "").strip()
                if text:
                    emit_log(text)

            result = bridge_service.upload_alarm_event_source_cache_full_to_bitable(emit_log=_combined_log)
            accepted = bool(result.get("accepted"))
            reason = str(result.get("reason", "") or "").strip()
            if not accepted:
                error_text = str(result.get("error", "") or "").strip() or "告警信息文件上传失败"
                raise RuntimeError(error_text)
            if reason == "partial_completed":
                failed_entries = ", ".join(
                    str(item or "").strip() for item in result.get("failed_entries", []) or [] if str(item or "").strip()
                )
                raise RuntimeError(f"存在失败楼栋，请查看日志{f'：{failed_entries}' if failed_entries else ''}")
            return result

        try:
            job = _start_external_cache_job(
                name="使用共享文件上传60天-全部楼栋",
                feature="alarm_event_upload",
                resource_key="alarm_upload:global",
                run_func=_run,
                dedupe_key="alarm_event_upload:full",
            )
            detail = f"已提交告警信息共享文件上传任务 job_id={job.job_id}, scope=全部楼栋"
            container.add_system_log(f"[告警信息上传调度] {detail}")
            return True, detail
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            container.add_system_log(f"[告警信息上传调度] 提交失败：{error_text}")
            return False, error_text

    def monthly_event_report_scheduler_callback(source: str) -> tuple[bool, str]:
        role_mode = _deployment_role_mode()
        if role_mode == "internal":
            container.add_system_log("[月度事件统计表调度] 当前为内网端，调度跳过；请在外网端启用该调度")
            return True, "internal_role_skip"

        runtime_config = container.runtime_config
        service = MonthlyEventReportService(runtime_config)
        _, _, target_month = service.target_month_window(datetime.now())
        dedupe_key = service.dedupe_key("all", target_month=target_month)

        def _run(emit_log):
            return service.run(
                scope="all",
                emit_log=emit_log,
                source=source,
            )

        try:
            job = container.job_service.start_job(
                name="月度事件统计表处理-全部楼栋",
                run_func=_run,
                resource_keys=["monthly_event_report:global"],
                priority="scheduler",
                feature="monthly_event_report",
                submitted_by="scheduler",
                dedupe_key=dedupe_key,
            )
            detail = f"已提交月度事件统计表任务 job_id={job.job_id}, target_month={target_month}"
            container.add_system_log(f"[月度事件统计表调度] {detail}")
            return True, detail
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def monthly_change_report_scheduler_callback(source: str) -> tuple[bool, str]:
        role_mode = _deployment_role_mode()
        if role_mode == "internal":
            container.add_system_log("[月度变更统计表调度] 当前为内网端，调度跳过；请在外网端启用该调度")
            return True, "internal_role_skip"

        runtime_config = container.runtime_config
        service = MonthlyChangeReportService(runtime_config)
        _, _, target_month = service.target_month_window(datetime.now())
        dedupe_key = service.dedupe_key("all", target_month=target_month)

        def _run(emit_log):
            return service.run(
                scope="all",
                emit_log=emit_log,
                source=source,
            )

        try:
            job = container.job_service.start_job(
                name="月度变更统计表处理-全部楼栋",
                run_func=_run,
                resource_keys=["monthly_change_report:global"],
                priority="scheduler",
                feature="monthly_change_report",
                submitted_by="scheduler",
                dedupe_key=dedupe_key,
            )
            detail = f"已提交月度变更统计表任务 job_id={job.job_id}, target_month={target_month}"
            container.add_system_log(f"[月度变更统计表调度] {detail}")
            return True, detail
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def updater_restart_callback(context: dict) -> tuple[bool, str]:
        wrote_startup_handoff = False
        try:
            write_startup_role_handoff = getattr(container, "write_startup_role_handoff", None)
            clear_startup_role_handoff = getattr(container, "clear_startup_role_handoff", None)
            deployment_snapshot = container.deployment_snapshot() if hasattr(container, "deployment_snapshot") else {}
            if not isinstance(deployment_snapshot, dict):
                deployment_snapshot = {}
            target_role_mode = normalize_role_mode(deployment_snapshot.get("role_mode"))
            if callable(write_startup_role_handoff) and target_role_mode in {"internal", "external"}:
                write_startup_role_handoff(
                    target_role_mode=target_role_mode,
                    source="updater_restart",
                    reason=str((context or {}).get("reason", "") or "updater_apply").strip(),
                    source_startup_time=str(getattr(app.state, "started_at", "") or "").strip(),
                )
                wrote_startup_handoff = True
            restart_exit_code = str(os.environ.get("QJPT_RESTART_EXIT_CODE", "") or "").strip()
            if restart_exit_code:
                try:
                    exit_code = int(restart_exit_code)
                except Exception as exc:  # noqa: BLE001
                    return False, f"无效的重启退出码: {exc}"

                container.add_system_log("[更新] 已安排在当前窗口内重启")

                def _exit_later() -> None:
                    time.sleep(1)
                    os._exit(exit_code)

                threading.Thread(target=_exit_later, daemon=True, name="updater-restart").start()
                return True, "same_console_restart_scheduled"

            app_dir = get_app_dir()
            app_root_dir = get_app_root_dir(app_dir)
            launcher_bat = app_root_dir / "启动程序.bat"
            portable_launcher = app_dir / "portable_launcher.py"
            if sys.platform.startswith("win") and launcher_bat.exists() and app_root_dir != app_dir:
                cmd = ["cmd.exe", "/c", str(launcher_bat)]
                popen_kwargs = {"cwd": str(app_root_dir)}
            elif (
                not getattr(sys, "frozen", False)
                and not str(os.environ.get("QJPT_PORTABLE_LAUNCHER", "") or "").strip()
                and portable_launcher.exists()
            ):
                cmd = [sys.executable, str(portable_launcher)]
                popen_kwargs = {"cwd": str(app_dir)}
            elif sys.platform.startswith("win") and launcher_bat.exists():
                cmd = ["cmd.exe", "/c", str(launcher_bat)]
                popen_kwargs = {"cwd": str(app_root_dir)}
            elif getattr(sys, "frozen", False):
                cmd = [sys.executable]
                popen_kwargs = {"cwd": str(app_dir)}
            else:
                cmd = [sys.executable, str(app_dir / "main.py")]
                popen_kwargs = {"cwd": str(app_dir)}

            child_env = dict(os.environ)
            child_env["QJPT_DISABLE_BROWSER_AUTO_OPEN"] = "1"
            popen_kwargs["env"] = child_env

            subprocess.Popen(cmd, **popen_kwargs)
            container.add_system_log(f"[更新] 已安排当前控制台重启: {' '.join(cmd)}")

            def _exit_later() -> None:
                time.sleep(1)
                os._exit(0)

            threading.Thread(target=_exit_later, daemon=True, name="updater-restart").start()
            return True, "restart_scheduled"
        except Exception as exc:  # noqa: BLE001
            if wrote_startup_handoff and callable(clear_startup_role_handoff):
                clear_startup_role_handoff()
            return False, str(exc)
    setter = getattr(container, "set_scheduler_callback", None)
    if callable(setter):
        setter(scheduler_callback)
    setter = getattr(container, "set_handover_scheduler_callback", None)
    if callable(setter):
        setter(handover_scheduler_callback)
    setter = getattr(container, "set_wet_bulb_collection_scheduler_callback", None)
    if callable(setter):
        setter(wet_bulb_collection_scheduler_callback)
    setter = getattr(container, "set_day_metric_upload_scheduler_callback", None)
    if callable(setter):
        setter(day_metric_upload_scheduler_callback)
    setter = getattr(container, "set_alarm_event_upload_scheduler_callback", None)
    if callable(setter):
        setter(alarm_event_upload_scheduler_callback)
    setter = getattr(container, "set_monthly_change_report_scheduler_callback", None)
    if callable(setter):
        setter(monthly_change_report_scheduler_callback)
    setter = getattr(container, "set_monthly_event_report_scheduler_callback", None)
    if callable(setter):
        setter(monthly_event_report_scheduler_callback)
    setter = getattr(container, "set_updater_restart_callback", None)
    if callable(setter):
        setter(updater_restart_callback)
    @app.get("/", response_class=HTMLResponse)
    @app.get("/login", response_class=HTMLResponse)
    @app.get("/index.html", response_class=HTMLResponse)
    @app.get("/internal", response_class=HTMLResponse)
    @app.get("/internal/status", response_class=HTMLResponse)
    @app.get("/internal/config", response_class=HTMLResponse)
    @app.get("/external", response_class=HTMLResponse)
    @app.get("/external/status", response_class=HTMLResponse)
    @app.get("/external/dashboard", response_class=HTMLResponse)
    @app.get("/external/config", response_class=HTMLResponse)
    def index() -> Response:
        if str(container.frontend_mode or "").strip().lower() == "source":
            return HTMLResponse(
                render_frontend_index_html(
                    container.frontend_root,
                    frontend_mode=container.frontend_mode,
                    asset_base_path=app.state.source_frontend_asset_prefix,
                ),
                headers=source_frontend_no_cache_headers(container.frontend_mode),
            )
        return FileResponse(
            container.frontend_root / "index.html",
            headers=source_frontend_no_cache_headers(container.frontend_mode),
        )

    @app.get("/favicon.ico")
    def favicon() -> Response:
        candidates = [
            container.frontend_root / "favicon.ico",
            container.frontend_assets_dir / "favicon.ico",
        ]
        for path in candidates:
            if path.exists():
                return FileResponse(path)
        return Response(status_code=204)

    if container.frontend_assets_dir.exists():
        @app.get("/assets-src/{asset_version}/{asset_path:path}")
        def source_frontend_asset(asset_version: str, asset_path: str) -> Response:
            if str(container.frontend_mode or "").strip().lower() != "source":
                return Response(status_code=404)
            if str(asset_version or "").strip() != str(app.state.source_frontend_asset_version or "").strip():
                return Response(status_code=404)
            path = resolve_source_frontend_asset_path(container.frontend_assets_dir, asset_path)
            if path is None:
                return Response(status_code=404)
            return FileResponse(
                path,
                headers=source_frontend_no_cache_headers(container.frontend_mode),
            )

        app.mount(
            "/assets",
            _SourceNoCacheStaticFiles(
                directory=container.frontend_assets_dir,
                frontend_mode=container.frontend_mode,
            ),
            name="assets",
        )

    role_mode = _deployment_role_mode()

    _register_common_routes(app)

    if role_mode == "external":
        _register_external_role_routes(app)

    return app



