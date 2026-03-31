from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Body, HTTPException, Request


router = APIRouter(prefix="/api/updater", tags=["updater"])


_UPDATER_RESULT_TEXT = {
    "": "-",
    "disabled": "已禁用",
    "up_to_date": "已经是最新版本",
    "update_available": "发现可用更新",
    "downloading_patch": "补丁下载中",
    "applying_patch": "补丁应用中",
    "dependency_checking": "运行依赖检查中",
    "dependency_syncing": "运行依赖同步中",
    "dependency_rollback": "依赖同步失败，正在回滚",
    "updated": "补丁已应用",
    "updated_restart_scheduled": "更新后将自动重启",
    "queued_busy": "任务结束后自动更新",
    "restart_pending": "等待重启生效",
    "ahead_of_remote": "本地版本高于远端正式版本",
    "ahead_of_mirror": "本地版本高于共享目录批准版本",
    "mirror_pending_publish": "等待外网端发布批准版本",
    "failed": "更新失败",
}

_UPDATER_QUEUE_TEXT = {
    "": "未排队",
    "none": "未排队",
    "queued": "已排队",
}


def _format_updater_result(raw: Any) -> str:
    key = str(raw or "").strip()
    return _UPDATER_RESULT_TEXT.get(key, key or "-")


def _format_updater_queue(raw: Any) -> str:
    key = str(raw or "").strip()
    return _UPDATER_QUEUE_TEXT.get(key, key or "-")


def _runtime_payload(container) -> Dict[str, Any]:
    runtime = container.updater_snapshot()
    runtime_cfg = container.runtime_config.get("updater", {}) if isinstance(container.runtime_config, dict) else {}
    if not isinstance(runtime_cfg, dict):
        runtime_cfg = {}
    return {
        "enabled": bool(runtime_cfg.get("enabled", True)),
        "running": bool(runtime.get("running", False)),
        "last_check_at": str(runtime.get("last_check_at", "")),
        "last_result": str(runtime.get("last_result", "")),
        "last_error": str(runtime.get("last_error", "")),
        "local_version": str(runtime.get("local_version", "")),
        "remote_version": str(runtime.get("remote_version", "")),
        "local_release_revision": int(runtime.get("local_release_revision", 0) or 0),
        "remote_release_revision": int(runtime.get("remote_release_revision", 0) or 0),
        "source_kind": str(runtime.get("source_kind", "remote") or "remote"),
        "source_label": str(runtime.get("source_label", "远端正式更新源") or "远端正式更新源"),
        "state_path": str(runtime.get("state_path", "")),
        "update_available": bool(runtime.get("update_available", False)),
        "force_apply_available": bool(runtime.get("force_apply_available", False)),
        "restart_required": bool(runtime.get("restart_required", False)),
        "dependency_sync_status": str(runtime.get("dependency_sync_status", "idle")),
        "dependency_sync_error": str(runtime.get("dependency_sync_error", "")),
        "dependency_sync_at": str(runtime.get("dependency_sync_at", "")),
        "queued_apply": dict(runtime.get("queued_apply", {})),
        "mirror_ready": bool(runtime.get("mirror_ready", False)),
        "mirror_version": str(runtime.get("mirror_version", "")),
        "mirror_manifest_path": str(runtime.get("mirror_manifest_path", "")),
        "last_publish_at": str(runtime.get("last_publish_at", "")),
        "last_publish_error": str(runtime.get("last_publish_error", "")),
    }


@router.get("/status")
def updater_status(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    return {"ok": True, "runtime": _runtime_payload(container)}


@router.post("/check")
def updater_check(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        with container.job_service.resource_guard(name="updater_check", resource_keys=["updater:global"]):
            service = container.ensure_updater_service()
            result = service.check_now()
        container.add_system_log(f"[更新] 手动检查完成: {_format_updater_result(result.get('last_result', '-'))}")
        return {"ok": True, "result": result, "runtime": _runtime_payload(container)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"手动检查更新失败: {exc}") from exc


@router.post("/apply")
def updater_apply(
    request: Request,
    payload: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        with container.job_service.resource_guard(name="updater_apply", resource_keys=["updater:global"]):
            service = container.ensure_updater_service()
            mode = str(payload.get("mode", "normal") or "normal").strip().lower()
            queue_if_busy = bool(payload.get("queue_if_busy", False))
            result = service.apply_now(mode=mode, queue_if_busy=queue_if_busy)
        container.add_system_log(
            "[更新] 手动应用完成: "
            f"{_format_updater_result(result.get('last_result', '-'))}，"
            f"队列状态={_format_updater_queue(result.get('queue_status', '-'))}"
        )
        return {"ok": True, "result": result, "runtime": _runtime_payload(container)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"应用更新失败: {exc}") from exc


@router.post("/restart")
def updater_restart(request: Request) -> Dict[str, Any]:
    container = request.app.state.container
    try:
        with container.job_service.resource_guard(name="updater_restart", resource_keys=["updater:global"]):
            service = container.ensure_updater_service()
            result = service.restart_now()
        container.add_system_log(f"[更新] 手动重启触发: {_format_updater_result(result.get('last_result', '-'))}")
        return {"ok": True, "result": result, "runtime": _runtime_payload(container)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"触发重启失败: {exc}") from exc
