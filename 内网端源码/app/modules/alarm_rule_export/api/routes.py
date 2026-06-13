from __future__ import annotations

import asyncio
import threading
from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.modules.alarm_rule_export.service.alarm_rule_export_service import (
    alarm_rule_export_status,
    list_alarm_rule_export_sites,
    reset_alarm_rule_export_building,
    run_alarm_rule_export,
)


router = APIRouter(prefix="/api/alarm-rule-export", tags=["alarm-rule-export"])


class AlarmRuleExportRunRequest(BaseModel):
    period: str | None = Field(default=None, description="YYYY-MM; defaults to current month")
    buildings: str | None = Field(default=None, description="Comma separated building names")
    parallel: int = Field(default=5, ge=1, le=5)
    keep_open_sec: int = Field(default=1, ge=0)
    headless: bool = False


class AlarmRuleExportResetRequest(BaseModel):
    building: str
    period: str | None = None


def _container(request: Request) -> Any:
    return getattr(request.app.state, "container", None)


def _runtime(request: Request) -> Dict[str, Any]:
    runtime = getattr(request.app.state, "alarm_rule_export_runtime", None)
    if not isinstance(runtime, dict):
        runtime = {
            "lock": threading.Lock(),
            "running": False,
            "started_at": "",
            "finished_at": "",
            "last_result": None,
            "last_error": "",
            "thread": None,
        }
        request.app.state.alarm_rule_export_runtime = runtime
    return runtime


@router.get("/sites")
def get_alarm_rule_export_sites(request: Request) -> Dict[str, Any]:
    container = _container(request)
    config = getattr(container, "runtime_config", None) or getattr(container, "config", {}) or {}
    sites = list_alarm_rule_export_sites(config=config)
    return {"ok": True, "sites": sites}


@router.get("/status")
def get_alarm_rule_export_status(request: Request, period: str | None = None) -> Dict[str, Any]:
    container = _container(request)
    config = getattr(container, "runtime_config", None) or getattr(container, "config", {}) or {}
    feature_cfg = config.get("alarm_rule_export", {}) if isinstance(config, dict) else {}
    if not isinstance(feature_cfg, dict):
        feature_cfg = {}
    runtime = _runtime(request)
    status = alarm_rule_export_status(
        config=config,
        period=period,
        state_file=str(feature_cfg.get("state_file", "") or "") or None,
    )
    return {
        "ok": True,
        **status,
        "running": bool(runtime.get("running")),
        "started_at": str(runtime.get("started_at") or ""),
        "finished_at": str(runtime.get("finished_at") or ""),
        "last_result": runtime.get("last_result"),
        "last_error": str(runtime.get("last_error") or ""),
    }


@router.post("/run")
def run_alarm_rule_export_now(request: Request, payload: AlarmRuleExportRunRequest | None = None) -> Dict[str, Any]:
    payload = payload or AlarmRuleExportRunRequest()
    container = _container(request)
    config = getattr(container, "runtime_config", None) or getattr(container, "config", {}) or {}
    runtime = _runtime(request)

    with runtime["lock"]:
        thread = runtime.get("thread")
        if bool(runtime.get("running")) and isinstance(thread, threading.Thread) and thread.is_alive():
            return {
                "ok": True,
                "accepted": False,
                "reason": "already_running",
                "started_at": str(runtime.get("started_at") or ""),
            }
        runtime.update(
            {
                "running": True,
                "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "finished_at": "",
                "last_result": None,
                "last_error": "",
            }
        )

    def _worker() -> None:
        try:
            feature_cfg = config.get("alarm_rule_export", {}) if isinstance(config, dict) else {}
            if not isinstance(feature_cfg, dict):
                feature_cfg = {}
            if container is not None:
                container.add_system_log(
                    f"[告警规则导出] 手动启动: period={payload.period or 'current'}, buildings={payload.buildings or '全部'}"
                )
            code = asyncio.run(
                run_alarm_rule_export(
                    config=config,
                    buildings=payload.buildings,
                    period=payload.period,
                    parallel=payload.parallel,
                    state_file=str(feature_cfg.get("state_file", "") or "") or None,
                    download_root=str(feature_cfg.get("download_root", "") or "") or None,
                    screenshots_dir=str(feature_cfg.get("screenshots_dir", "") or "") or None,
                    keep_open_sec=payload.keep_open_sec,
                    headless=payload.headless,
                    generate_poll_interval_sec=float(feature_cfg.get("generate_poll_interval_sec", 3600) or 3600),
                    export_generate_timeout_ms=int(feature_cfg.get("export_generate_timeout_ms", 172800000) or 172800000),
                )
            )
            runtime["last_result"] = {"exit_code": code}
            if container is not None:
                container.add_system_log(f"[告警规则导出] 手动运行完成: exit_code={code}")
        except Exception as exc:  # noqa: BLE001
            runtime["last_error"] = str(exc)
            if container is not None:
                container.add_system_log(f"[告警规则导出] 手动运行失败: {exc}")
        finally:
            runtime["running"] = False
            runtime["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    thread = threading.Thread(target=_worker, name="alarm-rule-export-api-run", daemon=True)
    runtime["thread"] = thread
    thread.start()

    return {"ok": True, "accepted": True, "period": payload.period, "buildings": payload.buildings}


@router.post("/reset-building")
def reset_alarm_rule_export(request: Request, payload: AlarmRuleExportResetRequest) -> Dict[str, Any]:
    container = _container(request)
    config = getattr(container, "runtime_config", None) or getattr(container, "config", {}) or {}
    feature_cfg = config.get("alarm_rule_export", {}) if isinstance(config, dict) else {}
    if not isinstance(feature_cfg, dict):
        feature_cfg = {}
    result = reset_alarm_rule_export_building(
        config=config,
        building=payload.building,
        period=payload.period,
        state_file=str(feature_cfg.get("state_file", "") or "") or None,
    )
    if container is not None:
        container.add_system_log(
            f"[告警规则导出] 已清理重导状态: building={payload.building}, period={result.get('period')}, removed={result.get('removed_downloaded_records')}"
        )
    return {"ok": True, **result}
