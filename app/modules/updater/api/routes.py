from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Body, HTTPException, Request
from app.config.config_adapter import normalize_role_mode


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
    "git_fetching": "Git 远端检查中",
    "git_pulling": "Git 拉取中",
    "dirty_worktree": "检测到本地改动，已阻止更新",
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
    service = getattr(container, "updater_service", None)
    runtime_enabled = runtime.get("enabled")
    if runtime_enabled is None:
        runtime_enabled = getattr(service, "enabled", None)
    if runtime_enabled is None:
        runtime_enabled = bool(runtime_cfg.get("enabled", True))
    return {
        "enabled": bool(runtime_enabled),
        "disabled_reason": str(runtime.get("disabled_reason", "") or ""),
        "running": bool(runtime.get("running", False)),
        "last_check_at": str(runtime.get("last_check_at", "")),
        "last_result": str(runtime.get("last_result", "")),
        "last_error": str(runtime.get("last_error", "")),
        "local_version": str(runtime.get("local_version", "")),
        "remote_version": str(runtime.get("remote_version", "")),
        "update_mode": str(runtime.get("update_mode", "patch_zip") or "patch_zip"),
        "app_root_dir": str(runtime.get("app_root_dir", "") or ""),
        "persistent_user_data_dir": str(runtime.get("persistent_user_data_dir", "") or ""),
        "git_available": bool(runtime.get("git_available", False)),
        "git_repo_detected": bool(runtime.get("git_repo_detected", False)),
        "branch": str(runtime.get("branch", "") or ""),
        "local_commit": str(runtime.get("local_commit", "") or ""),
        "remote_commit": str(runtime.get("remote_commit", "") or ""),
        "worktree_dirty": bool(runtime.get("worktree_dirty", False)),
        "dirty_files": list(runtime.get("dirty_files", []) or []),
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
        "internal_peer": dict(runtime.get("internal_peer", {}))
        if isinstance(runtime.get("internal_peer", {}), dict)
        else {},
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


def _resolve_container_role_mode(container) -> str:
    deployment_snapshot = getattr(container, "deployment_snapshot", None)
    if callable(deployment_snapshot):
        try:
            snapshot = deployment_snapshot()
        except Exception:  # noqa: BLE001
            snapshot = {}
        if isinstance(snapshot, dict):
            role_mode = normalize_role_mode(snapshot.get("role_mode"))
            if role_mode in {"internal", "external"}:
                return role_mode

    config = container.config if isinstance(getattr(container, "config", None), dict) else {}
    common = config.get("common", {}) if isinstance(config, dict) else {}
    deployment = common.get("deployment", {}) if isinstance(common, dict) else {}
    if not isinstance(deployment, dict):
        deployment = config.get("deployment", {}) if isinstance(config, dict) else {}
    role_mode = normalize_role_mode(deployment.get("role_mode") if isinstance(deployment, dict) else "")
    return role_mode


def _require_external_role(container) -> None:
    role_mode = _resolve_container_role_mode(container)
    if role_mode != "external":
        raise HTTPException(status_code=403, detail="仅外网端可下发内网更新命令")


def _submit_internal_peer_command(request: Request, *, action: str) -> Dict[str, Any]:
    container = request.app.state.container
    _require_external_role(container)
    try:
        with container.job_service.resource_guard(
            name=f"updater_internal_peer_{action}",
            resource_keys=["updater:global"],
        ):
            service = container.ensure_updater_service()
            result = service.submit_internal_peer_command(action=action)
        container.add_system_log(
            "[更新] 外网下发内网远程更新命令: "
            f"action={action}, accepted={bool(result.get('accepted', False))}, "
            f"already_pending={bool(result.get('already_pending', False))}"
        )
        return {"ok": True, "result": result, "runtime": _runtime_payload(container)}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"下发内网更新命令失败: {exc}") from exc


@router.post("/internal-peer/check")
def updater_internal_peer_check(request: Request) -> Dict[str, Any]:
    return _submit_internal_peer_command(request, action="check")


@router.post("/internal-peer/apply")
def updater_internal_peer_apply(request: Request) -> Dict[str, Any]:
    return _submit_internal_peer_command(request, action="apply")
