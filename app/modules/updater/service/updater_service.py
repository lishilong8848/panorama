from __future__ import annotations

import json
import os
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

from pipeline_utils import get_app_dir

from app.config.config_adapter import normalize_role_mode, resolve_shared_bridge_paths
from app.modules.updater.core.versioning import (
    compare_versions,
    load_local_build_meta,
    normalize_local_version,
    normalize_remote_version,
)
from app.modules.updater.repository.updater_state_store import UpdaterStateStore
from app.modules.updater.service.manifest_client import (
    ManifestClient,
    SharedMirrorManifestClient,
    SharedMirrorPendingError,
)
from app.modules.updater.service.runtime_dependency_sync_service import RuntimeDependencySyncService
from app.modules.updater.service.update_applier import UpdateApplier


_SOURCE_RUN_DISABLE_UPDATER_ENV = "QJPT_DISABLE_UPDATER_IN_SOURCE_RUN"


def _updater_disabled_reason_from_env() -> str:
    raw = str(os.environ.get(_SOURCE_RUN_DISABLE_UPDATER_ENV, "") or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return "source_python_run"
    return ""


def _default_node_id(role_mode: Any) -> str:
    role = normalize_role_mode(role_mode)
    machine_id = f"{uuid.getnode():012x}"
    if role not in {"internal", "external"}:
        role = "unselected"
    return f"{role}-{machine_id}"


class UpdaterService:
    def __init__(
        self,
        *,
        config: Dict[str, Any],
        emit_log: Callable[[str], None],
        restart_callback: Callable[[Dict[str, Any]], Tuple[bool, str]] | None = None,
        is_busy: Callable[[], bool] | None = None,
    ) -> None:
        updater_cfg = config.get("updater", {})
        paths_cfg = config.get("paths", {})
        deployment_cfg = config.get("deployment", {})
        shared_bridge_cfg = config.get("shared_bridge", {})
        if not isinstance(updater_cfg, dict):
            updater_cfg = {}
        if not isinstance(paths_cfg, dict):
            paths_cfg = {}
        if not isinstance(deployment_cfg, dict):
            deployment_cfg = {}
        if not isinstance(shared_bridge_cfg, dict):
            shared_bridge_cfg = {}

        self.cfg = {
            "enabled": bool(updater_cfg.get("enabled", True)),
            "check_interval_sec": max(30, int(updater_cfg.get("check_interval_sec", 3600))),
            "auto_apply": bool(updater_cfg.get("auto_apply", False)),
            "auto_restart": bool(updater_cfg.get("auto_restart", True)),
            "allow_downgrade": bool(updater_cfg.get("allow_downgrade", False)),
            "gitee_repo": str(updater_cfg.get("gitee_repo", "") or "").strip(),
            "gitee_branch": str(updater_cfg.get("gitee_branch", "master") or "master").strip(),
            "gitee_manifest_path": str(
                updater_cfg.get("gitee_manifest_path", "updates/latest_patch.json") or "updates/latest_patch.json"
            ).strip(),
            "request_timeout_sec": max(3, int(updater_cfg.get("request_timeout_sec", 20))),
            "download_retry_count": max(1, int(updater_cfg.get("download_retry_count", 5))),
            "state_file": str(updater_cfg.get("state_file", "updater_state.json") or "updater_state.json").strip(),
            "download_dir": str(
                updater_cfg.get("download_dir", "runtime_state/updater/downloads") or "runtime_state/updater/downloads"
            ).strip(),
            "backup_dir": str(
                updater_cfg.get("backup_dir", "runtime_state/updater/backups") or "runtime_state/updater/backups"
            ).strip(),
            "max_backups": max(1, int(updater_cfg.get("max_backups", 3))),
        }
        self.disabled_reason = _updater_disabled_reason_from_env()
        if self.disabled_reason:
            self.cfg["enabled"] = False
        self.runtime_state_root = str(paths_cfg.get("runtime_state_root", "") or "").strip()
        self.emit_log = emit_log
        self.restart_callback = restart_callback
        self.is_busy = is_busy or (lambda: False)
        self.role_mode = normalize_role_mode(deployment_cfg.get("role_mode"))
        self.node_id = str(deployment_cfg.get("node_id", "") or "").strip() or _default_node_id(self.role_mode)
        resolved_shared_bridge = resolve_shared_bridge_paths(shared_bridge_cfg, deployment_cfg.get("role_mode"))
        self.shared_bridge_enabled = bool(resolved_shared_bridge.get("enabled", False))
        self.shared_bridge_root = str(resolved_shared_bridge.get("root_dir", "") or "").strip()
        self.source_kind = "shared_mirror" if self.role_mode == "internal" else "remote"
        self.source_label = "共享目录更新源" if self.source_kind == "shared_mirror" else "远端正式更新源"

        self.app_dir = get_app_dir()
        self.state_path = self._resolve_state_path(self.cfg["state_file"])
        self.download_dir = self._resolve_any_path(self.cfg["download_dir"])
        self.backup_dir = self._resolve_any_path(self.cfg["backup_dir"])
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        self.state_store = UpdaterStateStore(self.state_path)
        self.state = self.state_store.load()

        self.shared_mirror_client = SharedMirrorManifestClient(self.shared_bridge_root) if self.shared_bridge_root else None
        if self.source_kind == "shared_mirror":
            self.client = self.shared_mirror_client
        else:
            self.client = ManifestClient(
                repo_url=self.cfg["gitee_repo"],
                branch=self.cfg["gitee_branch"],
                manifest_path=self.cfg["gitee_manifest_path"],
                timeout_sec=self.cfg["request_timeout_sec"],
                retry_count=self.cfg["download_retry_count"],
            )
        self.applier = UpdateApplier(
            app_dir=self.app_dir,
            emit_log=self.emit_log,
            runtime_state_root=self.runtime_state_root,
        )
        self.dependency_sync_service = RuntimeDependencySyncService(
            app_dir=self.app_dir,
            runtime_state_root=self.runtime_state_root,
            emit_log=self.emit_log,
            python_executable=sys.executable,
        )

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._work_lock = threading.Lock()
        self._last_shared_mirror_signal: tuple[str, ...] = ()
        mirror_runtime = self._mirror_runtime_snapshot()
        self.state.setdefault("source_kind", self.source_kind)
        self.state.setdefault("source_label", self.source_label)
        self.state.setdefault("enabled", bool(self.cfg["enabled"]))
        self.state.setdefault("disabled_reason", self.disabled_reason)
        self.state.setdefault("mirror_ready", mirror_runtime.get("mirror_ready", False))
        self.state.setdefault("mirror_version", mirror_runtime.get("mirror_version", ""))
        self.state.setdefault("mirror_manifest_path", mirror_runtime.get("mirror_manifest_path", ""))
        self.state.setdefault("last_publish_at", mirror_runtime.get("last_publish_at", ""))
        self.state.setdefault("last_publish_error", mirror_runtime.get("last_publish_error", ""))
        if not bool(self.cfg["enabled"]):
            self.state.update(
                {
                    "enabled": False,
                    "disabled_reason": self.disabled_reason,
                    "last_result": "disabled",
                    "last_error": "",
                    "update_available": False,
                    "force_apply_available": False,
                    "restart_required": False,
                    "queued_apply": self._empty_queue_payload(),
                }
            )
        self._persist_state()
        self.runtime: Dict[str, Any] = {
            "enabled": bool(self.state.get("enabled", self.cfg["enabled"])),
            "disabled_reason": str(self.state.get("disabled_reason", self.disabled_reason) or self.disabled_reason),
            "running": False,
            "last_check_at": str(self.state.get("last_check_at", "")),
            "last_result": str(self.state.get("last_result", "")),
            "last_error": str(self.state.get("last_error", "")),
            "local_version": str(self.state.get("local_version", "")),
            "remote_version": str(self.state.get("remote_version", "")),
            "source_kind": str(self.state.get("source_kind", self.source_kind) or self.source_kind),
            "source_label": str(self.state.get("source_label", self.source_label) or self.source_label),
            "local_release_revision": int(self.state.get("local_release_revision", 0) or 0),
            "remote_release_revision": int(self.state.get("remote_release_revision", 0) or 0),
            "update_available": bool(self.state.get("update_available", False)),
            "force_apply_available": bool(self.state.get("force_apply_available", False)),
            "restart_required": bool(self.state.get("restart_required", False)),
            "dependency_sync_status": str(self.state.get("dependency_sync_status", "idle") or "idle"),
            "dependency_sync_error": str(self.state.get("dependency_sync_error", "") or ""),
            "dependency_sync_at": str(self.state.get("dependency_sync_at", "") or ""),
            "queued_apply": dict(self.state.get("queued_apply", {})),
            "state_path": str(self.state_path),
            "mirror_ready": bool(self.state.get("mirror_ready", mirror_runtime.get("mirror_ready", False))),
            "mirror_version": str(self.state.get("mirror_version", mirror_runtime.get("mirror_version", "")) or ""),
            "mirror_manifest_path": str(
                self.state.get("mirror_manifest_path", mirror_runtime.get("mirror_manifest_path", "")) or ""
            ),
            "last_publish_at": str(self.state.get("last_publish_at", mirror_runtime.get("last_publish_at", "")) or ""),
            "last_publish_error": str(
                self.state.get("last_publish_error", mirror_runtime.get("last_publish_error", "")) or ""
            ),
        }

    def _log(self, text: str) -> None:
        self.emit_log(f"[Updater] {text}")

    @staticmethod
    def _disabled_reason_text(raw: Any) -> str:
        key = str(raw or "").strip().lower()
        if key == "source_python_run":
            return "当前为 Python 本地源码运行，已跳过更新。"
        return "当前运行模式已禁用更新。"

    @staticmethod
    def _result_text(raw: Any) -> str:
        key = str(raw or "").strip()
        mapping = {
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
        return mapping.get(key, key or "-")

    @staticmethod
    def _queue_reason_text(raw: Any) -> str:
        key = str(raw or "").strip()
        mapping = {
            "": "-",
            "active_job_running": "当前仍有任务在运行",
        }
        return mapping.get(key, key or "-")

    @staticmethod
    def _dependency_status_text(raw: Any) -> str:
        key = str(raw or "").strip()
        mapping = {
            "": "-",
            "idle": "无需同步",
            "running": "同步中",
            "success": "成功",
            "failed": "失败",
            "rolled_back": "失败后已回滚",
            "skipped": "已跳过",
        }
        return mapping.get(key, key or "-")

    @staticmethod
    def _apply_mode_text(raw: Any) -> str:
        key = str(raw or "").strip().lower()
        if key == "force_remote":
            return "按远端正式版本覆盖"
        return "正常更新"

    def _resolve_runtime_root(self) -> Path:
        text = self.runtime_state_root
        if text:
            path = Path(text)
            if not path.is_absolute():
                path = self.app_dir / path
        else:
            path = self.app_dir / ".runtime"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _resolve_state_path(self, state_file: str) -> Path:
        path = Path(state_file)
        if path.is_absolute():
            return path
        return self._resolve_runtime_root() / path

    def _resolve_any_path(self, value: str) -> Path:
        path = Path(str(value or "").strip())
        if path.is_absolute():
            return path
        return self.app_dir / path

    def _mirror_runtime_snapshot(self) -> Dict[str, Any]:
        if not self.shared_mirror_client:
            return {
                "mirror_ready": False,
                "mirror_version": "",
                "mirror_manifest_path": "",
                "last_publish_at": "",
                "last_publish_error": "",
            }
        snapshot = self.shared_mirror_client.get_runtime_snapshot()
        return {
            "mirror_ready": bool(snapshot.get("mirror_ready", False)),
            "mirror_version": str(snapshot.get("mirror_version", "") or "").strip(),
            "mirror_manifest_path": str(snapshot.get("mirror_manifest_path", "") or "").strip(),
            "last_publish_at": str(snapshot.get("last_publish_at", "") or "").strip(),
            "last_publish_error": str(snapshot.get("last_publish_error", "") or "").strip(),
        }

    def _sync_mirror_runtime(self) -> Dict[str, Any]:
        snapshot = self._mirror_runtime_snapshot()
        self._set_runtime_and_state(
            source_kind=self.source_kind,
            source_label=self.source_label,
            mirror_ready=bool(snapshot.get("mirror_ready", False)),
            mirror_version=str(snapshot.get("mirror_version", "") or "").strip(),
            mirror_manifest_path=str(snapshot.get("mirror_manifest_path", "") or "").strip(),
            last_publish_at=str(snapshot.get("last_publish_at", "") or "").strip(),
            last_publish_error=str(snapshot.get("last_publish_error", "") or "").strip(),
        )
        return snapshot

    def _shared_mirror_watch_signal(self) -> tuple[str, ...]:
        if self.source_kind != "shared_mirror" or not self.shared_mirror_client:
            return ()
        parts: list[str] = []
        # Only use the final publish-state file as the immediate trigger.
        # The manifest may appear slightly earlier during publishing, and
        # watching it would widen the window for reacting to a half-published mirror.
        for path in (self.shared_mirror_client.publish_state_path,):
            try:
                if path.exists():
                    stat = path.stat()
                    parts.append(f"{path.name}:{stat.st_mtime_ns}:{stat.st_size}")
                else:
                    parts.append(f"{path.name}:missing")
            except Exception as exc:  # noqa: BLE001
                parts.append(f"{path.name}:error:{type(exc).__name__}")
        return tuple(parts)

    def _sync_shared_mirror_watch_signal(self) -> tuple[str, ...]:
        signal = self._shared_mirror_watch_signal()
        self._last_shared_mirror_signal = signal
        return signal

    def _consume_shared_mirror_watch_trigger(self) -> bool:
        signal = self._shared_mirror_watch_signal()
        if not signal:
            return False
        if not self._last_shared_mirror_signal:
            self._last_shared_mirror_signal = signal
            return False
        if signal != self._last_shared_mirror_signal:
            self._last_shared_mirror_signal = signal
            return True
        return False

    @property
    def enabled(self) -> bool:
        return bool(self.cfg["enabled"])

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> Dict[str, Any]:
        if not self.enabled:
            self._set_runtime_and_state(
                enabled=False,
                disabled_reason=self.disabled_reason,
                last_result="disabled",
                last_error="",
                source_kind=self.source_kind,
                source_label=self.source_label,
                update_available=False,
                force_apply_available=False,
                restart_required=False,
                dependency_sync_status="idle",
                dependency_sync_error="",
                queued_apply=self._empty_queue_payload(),
                running=False,
            )
            self._log(f"更新服务未启用: {self._disabled_reason_text(self.disabled_reason)}")
            return {"started": False, "running": False, "reason": "disabled"}
        if self.source_kind == "shared_mirror" and not self.shared_mirror_client:
            self._set_runtime_and_state(
                last_result="failed",
                last_error="共享目录未配置，内网端无法检查离线更新。",
                source_kind=self.source_kind,
                source_label=self.source_label,
                running=False,
            )
            self._log("更新服务启动失败: 内网端未配置共享目录更新源")
            return {"started": False, "running": False, "reason": "misconfigured"}
        if self.is_running():
            return {"started": False, "running": True, "reason": "already_running"}

        self._sync_mirror_runtime()

        try:
            startup_result = self._run_check(apply_update=None, force_remote=False)
            self._log(
                "启动检查完成: "
                f"结果={self._result_text(startup_result.get('last_result', '-'))}, "
                f"本地版本={startup_result.get('local_version', '-')}, "
                f"远端版本={startup_result.get('remote_version', '-')}"
            )
        except Exception as exc:  # noqa: BLE001
            self._record_failure("启动检查失败", exc)
        self._sync_shared_mirror_watch_signal()

        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="qjpt-updater")
        self._thread.start()
        return {"started": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None
        self._set_runtime(running=False)
        return {"stopped": True, "running": False, "reason": "stopped"}

    def get_runtime_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self.runtime)

    def check_now(self) -> Dict[str, Any]:
        if not self.enabled:
            self._set_runtime_and_state(
                enabled=False,
                disabled_reason=self.disabled_reason,
                last_result="disabled",
                last_error="",
                update_available=False,
                force_apply_available=False,
                restart_required=False,
                queued_apply=self._empty_queue_payload(),
                running=False,
            )
            return self._build_result_payload(
                last_result="disabled",
                queue_status="none",
                message=self._disabled_reason_text(self.disabled_reason),
            )
        return self._run_check(apply_update=None, force_remote=False)

    def apply_now(self, *, mode: str = "normal", queue_if_busy: bool = False) -> Dict[str, Any]:
        if not self.enabled:
            self._set_runtime_and_state(
                enabled=False,
                disabled_reason=self.disabled_reason,
                last_result="disabled",
                last_error="",
                update_available=False,
                force_apply_available=False,
                restart_required=False,
                queued_apply=self._empty_queue_payload(),
                running=False,
            )
            return self._build_result_payload(
                last_result="disabled",
                queue_status="none",
                message=self._disabled_reason_text(self.disabled_reason),
            )
        normalized_mode = "force_remote" if str(mode or "").strip().lower() == "force_remote" else "normal"
        if queue_if_busy and self.is_busy():
            queue_payload = self._queue_apply(mode=normalized_mode, reason="active_job_running")
            return self._build_result_payload(last_result="queued_busy", queue_status="queued", **queue_payload)
        return self._run_check(apply_update=True, force_remote=normalized_mode == "force_remote")

    def restart_now(self) -> Dict[str, Any]:
        if not self.enabled:
            self._set_runtime_and_state(
                enabled=False,
                disabled_reason=self.disabled_reason,
                last_result="disabled",
                last_error="",
                update_available=False,
                force_apply_available=False,
                restart_required=False,
                queued_apply=self._empty_queue_payload(),
                running=False,
            )
            return self._build_result_payload(
                last_result="disabled",
                queue_status="none",
                message=self._disabled_reason_text(self.disabled_reason),
            )
        if not bool(self.state.get("restart_required", False)):
            return self._build_result_payload(last_result=str(self.state.get("last_result", "")), queue_status="none")
        if not callable(self.restart_callback):
            raise RuntimeError("当前运行模式不支持自动重启，请手动重启程序。")
        ok, detail = self.restart_callback(
            {
                "reason": "manual_restart",
                "target_version": str(self.state.get("local_version", "") or "").strip(),
                "target_release_revision": int(self.state.get("local_release_revision", 0) or 0),
            }
        )
        if not ok:
            raise RuntimeError(detail or "自动重启失败，请手动重启程序。")
        self._set_runtime_and_state(
            last_result="updated_restart_scheduled",
            last_error="",
            restart_required=False,
        )
        return self._build_result_payload(last_result="updated_restart_scheduled", queue_status="none")

    def _empty_queue_payload(self) -> Dict[str, Any]:
        return {
            "queued": False,
            "mode": "",
            "queued_at": "",
            "reason": "",
        }

    def _persist_state(self) -> None:
        self.state_store.save(self.state)

    def _set_runtime(self, **kwargs: Any) -> None:
        with self._lock:
            self.runtime.update(kwargs)

    def _set_runtime_and_state(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            self.state[key] = value
        self._persist_state()
        self._set_runtime(**kwargs)

    def _build_result_payload(self, *, last_result: str, queue_status: str = "none", **extra: Any) -> Dict[str, Any]:
        payload = {
            "ok": last_result != "failed",
            "enabled": bool(self.state.get("enabled", self.cfg["enabled"])),
            "disabled_reason": str(self.state.get("disabled_reason", self.disabled_reason) or self.disabled_reason),
            "last_result": last_result,
            "queue_status": queue_status,
            "restart_required": bool(self.state.get("restart_required", False)),
            "force_apply_available": bool(self.state.get("force_apply_available", False)),
            "local_version": str(self.state.get("local_version", "")),
            "remote_version": str(self.state.get("remote_version", "")),
            "source_kind": str(self.state.get("source_kind", self.source_kind) or self.source_kind),
            "source_label": str(self.state.get("source_label", self.source_label) or self.source_label),
            "local_release_revision": int(self.state.get("local_release_revision", 0) or 0),
            "remote_release_revision": int(self.state.get("remote_release_revision", 0) or 0),
            "dependency_sync_status": str(self.state.get("dependency_sync_status", "idle") or "idle"),
            "dependency_sync_error": str(self.state.get("dependency_sync_error", "") or ""),
            "last_error": str(self.state.get("last_error", "")),
            "last_check_at": str(self.state.get("last_check_at", "")),
            "queued_apply": dict(self.state.get("queued_apply", self._empty_queue_payload())),
            "mirror_ready": bool(self.state.get("mirror_ready", False)),
            "mirror_version": str(self.state.get("mirror_version", "") or ""),
            "mirror_manifest_path": str(self.state.get("mirror_manifest_path", "") or ""),
            "last_publish_at": str(self.state.get("last_publish_at", "") or ""),
            "last_publish_error": str(self.state.get("last_publish_error", "") or ""),
            "message": str(extra.pop("message", "") or "").strip(),
        }
        payload.update(extra)
        return payload

    def _queue_apply(self, *, mode: str, reason: str) -> Dict[str, Any]:
        queue_payload = {
            "queued": True,
            "mode": mode,
            "queued_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "reason": reason,
        }
        self._set_runtime_and_state(
            queued_apply=queue_payload,
            last_result="queued_busy",
            last_error="",
        )
        self._log(
            "更新请求已排队: "
            f"模式={self._apply_mode_text(mode)}, 原因={self._queue_reason_text(reason)}"
        )
        return {"queued_apply": queue_payload, "message": "当前仍有任务在运行，更新将在任务结束后自动执行。"}

    def _clear_queue(self) -> None:
        self._set_runtime_and_state(queued_apply=self._empty_queue_payload())

    def _resolve_manifest_patch_ref(self, remote_manifest: Dict[str, Any]) -> str:
        return (
            str(remote_manifest.get("zip_relpath", "") or "").strip()
            or str(remote_manifest.get("zip_url", "") or "").strip()
        )

    def _ensure_publishable_patch_zip(self, remote_manifest: Dict[str, Any]) -> Path:
        patch_ref = self._resolve_manifest_patch_ref(remote_manifest)
        zip_name = Path(str(patch_ref or "").split("?")[0]).name or "patch.zip"
        zip_path = self.download_dir / zip_name
        if zip_path.exists():
            return zip_path
        if not isinstance(self.client, ManifestClient):
            raise RuntimeError("当前更新源不支持重新下载批准补丁。")
        zip_url = str(remote_manifest.get("zip_url", "") or "").strip()
        if not zip_url:
            raise RuntimeError("更新清单缺少 zip_url，无法重新下载批准补丁。")
        zip_sha = str(remote_manifest.get("zip_sha256", "") or "").strip()
        self._log(f"共享镜像缺少本地补丁包，开始重新下载用于发布: {zip_url}")
        self.client.download_patch(zip_url, zip_path, expected_sha256=zip_sha)
        return zip_path

    def _try_publish_shared_mirror(
        self,
        *,
        remote_manifest: Dict[str, Any],
        approved_local_version: str,
        approved_release_revision: int,
        patch_zip: Path | None = None,
    ) -> Dict[str, Any]:
        if self.role_mode != "external" or not self.shared_mirror_client or not self.shared_bridge_root:
            return {"published": False, "reason": "not_external"}
        try:
            current_snapshot = self.shared_mirror_client.get_runtime_snapshot()
            current_release_revision = int(current_snapshot.get("mirror_release_revision", 0) or 0)
            current_version = str(current_snapshot.get("mirror_version", "") or "").strip()
            if (
                bool(current_snapshot.get("mirror_ready", False))
                and current_release_revision == int(approved_release_revision or 0)
                and current_version == str(approved_local_version or "").strip()
                and not str(current_snapshot.get("last_publish_error", "") or "").strip()
            ):
                self._sync_mirror_runtime()
                return {"published": False, "reason": "already_published"}
            publish_zip = patch_zip if patch_zip is not None else self._ensure_publishable_patch_zip(remote_manifest)
            result = self.shared_mirror_client.publish_approved_update(
                remote_manifest=remote_manifest,
                patch_zip=publish_zip,
                expected_sha256=str(remote_manifest.get("zip_sha256", "") or "").strip(),
                published_by_role=self.role_mode,
                published_by_node_id=self.node_id,
                approved_local_version=approved_local_version,
                approved_release_revision=approved_release_revision,
            )
            self._sync_mirror_runtime()
            self._log(
                "已将当前批准版本发布到共享目录: "
                f"版本={approved_local_version or '-'}, 发布路径={result.get('manifest_path', '-')}"
            )
            return {"published": True, "result": result}
        except Exception as exc:  # noqa: BLE001
            if self.shared_mirror_client:
                self.shared_mirror_client.record_publish_error(
                    str(exc),
                    published_by_role=self.role_mode,
                    published_by_node_id=self.node_id,
                )
            self._sync_mirror_runtime()
            self._log(f"共享目录更新镜像发布失败: {exc}")
            return {"published": False, "reason": "publish_failed", "error": str(exc)}

    def _persist_local_build_meta_from_remote(self, remote_manifest: Dict[str, Any]) -> None:
        if not isinstance(remote_manifest, dict):
            return
        local_meta = load_local_build_meta(self.app_dir)
        meta_path_text = str(local_meta.get("_meta_path", "") or "").strip()
        meta_path = Path(meta_path_text) if meta_path_text else (self.app_dir / "build_meta.json")

        target_version = str(remote_manifest.get("target_version", "") or "").strip()
        target_display = str(remote_manifest.get("target_display_version", "") or "").strip()
        if not target_version and not target_display:
            return

        payload = {
            "app_name": str(local_meta.get("app_name", "QJPT") or "QJPT"),
            "build_id": target_version,
            "major_version": int(remote_manifest.get("major_version", 0) or 0),
            "patch_version": int(remote_manifest.get("target_patch_version", 0) or 0),
            "release_revision": int(remote_manifest.get("target_release_revision", 0) or 0),
            "display_version": target_display,
            "created_at": str(remote_manifest.get("created_at", "") or "").strip()
            or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated_from_manifest_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            self._log(f"写入本地版本元数据失败: {exc}")

    def _record_failure(self, prefix: str, exc: Exception) -> None:
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_text = str(exc)
        self._set_runtime_and_state(
            last_check_at=now_text,
            last_result="failed",
            last_error=error_text,
            update_available=False,
        )
        self._log(f"{prefix}: {error_text}")

    def _should_auto_restart(self) -> bool:
        if not bool(self.cfg.get("auto_restart", False)):
            return False
        if bool(getattr(sys, "frozen", False)):
            return True
        return bool(os.environ.get("QJPT_PORTABLE_LAUNCHER") or os.environ.get("QJPT_RESTART_EXIT_CODE"))

    def _apply_restart_strategy(self, *, local_version: str, local_release_revision: int) -> tuple[str, bool]:
        if not self._should_auto_restart():
            return "restart_pending", True
        if not callable(self.restart_callback):
            return "restart_pending", True
        ok, detail = self.restart_callback(
            {
                "reason": "updater_applied",
                "target_version": local_version,
                "target_release_revision": local_release_revision,
            }
        )
        if ok:
            self._log(detail or "补丁应用完成，已安排自动重启。")
            return "updated_restart_scheduled", False
        self._log(f"自动重启失败，等待手动重启: {detail or '-'}")
        return "restart_pending", True

    def _resolve_structured_dependency_packages(self, patch_meta: Dict[str, Any]) -> list[Dict[str, Any]]:
        raw_packages = patch_meta.get("required_packages", [])
        if not isinstance(raw_packages, list):
            return []
        structured = [item for item in raw_packages if isinstance(item, dict)]
        return structured if structured else []

    def _sync_patch_dependencies(self, applied: Dict[str, Any], patch_meta: Dict[str, Any]) -> Dict[str, Any]:
        dependency_manifest_path = str(patch_meta.get("dependency_manifest_path", "") or "").strip()
        if dependency_manifest_path:
            lock_path = self.app_dir / Path(dependency_manifest_path)
            if lock_path.exists():
                self._set_runtime_and_state(
                    last_result="dependency_checking",
                    dependency_sync_status="running",
                    dependency_sync_error="",
                    dependency_sync_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                self._log(f"开始按依赖锁文件补齐运行依赖: {lock_path}")
                self._set_runtime_and_state(last_result="dependency_syncing")
                result = self.dependency_sync_service.sync_from_lock_file(lock_path)
                result["lock_path"] = str(lock_path)
                return result

        structured_packages = self._resolve_structured_dependency_packages(patch_meta)
        if structured_packages:
            self._set_runtime_and_state(
                last_result="dependency_checking",
                dependency_sync_status="running",
                dependency_sync_error="",
                dependency_sync_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            self._log("开始按补丁元数据补齐运行依赖")
            self._set_runtime_and_state(last_result="dependency_syncing")
            return self.dependency_sync_service.sync_required_packages(structured_packages, exact_versions=True)

        return {
            "status": "skipped",
            "installed": 0,
            "checked": 0,
            "packages": [],
            "exact_versions": False,
        }

    def _rollback_after_dependency_failure(
        self,
        *,
        applied: Dict[str, Any],
        remote: Dict[str, Any],
        remote_version_text: str,
        remote_release_revision: int,
        error_text: str,
    ) -> Dict[str, Any]:
        backup_path = str(applied.get("backup", "") or "").strip()
        rollback_detail = ""
        if backup_path:
            self._set_runtime_and_state(last_result="dependency_rollback")
            try:
                rollback_result = self.applier.restore_backup_snapshot(backup_path)
                rollback_detail = (
                    "已回滚到旧版本，"
                    f"恢复文件={rollback_result.get('restored', 0)}, "
                    f"移除新增={rollback_result.get('removed_created', 0)}"
                )
                self._log(f"依赖安装失败，{rollback_detail}")
            except Exception as rollback_exc:  # noqa: BLE001
                rollback_detail = f"回滚失败: {rollback_exc}"
                self._log(rollback_detail)
        local_after_rollback = normalize_local_version(load_local_build_meta(self.app_dir))
        local_after_text = local_after_rollback.get("display_version") or local_after_rollback.get("build_id") or "-"
        update_available = compare_versions(local_after_rollback, remote) < 0
        force_apply_available = bool(remote.get("build_id") or remote.get("display_version")) and compare_versions(local_after_rollback, remote) > 0
        dependency_status = "rolled_back" if rollback_detail and not rollback_detail.startswith("回滚失败") else "failed"
        dependency_message = "更新失败：运行依赖安装失败，已自动回滚到旧版本。"
        if dependency_status == "failed":
            dependency_message = "更新失败：运行依赖安装失败，且自动回滚失败，请检查系统日志。"

        self._set_runtime_and_state(
            last_result="failed",
            last_error=error_text,
            local_version=str(local_after_text),
            remote_version=str(remote_version_text),
            local_release_revision=int(local_after_rollback.get("release_revision", 0) or 0),
            remote_release_revision=remote_release_revision,
            update_available=update_available,
            force_apply_available=force_apply_available,
            restart_required=False,
            dependency_sync_status=dependency_status,
            dependency_sync_error=error_text,
            dependency_sync_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return self._build_result_payload(
            last_result="failed",
            message=dependency_message,
            rollback_detail=rollback_detail,
        )

    def _check_once(self, *, apply_update: bool | None, force_remote: bool) -> Dict[str, Any]:
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        local = normalize_local_version(load_local_build_meta(self.app_dir))
        local_version_text = local.get("display_version") or local.get("build_id") or "-"
        local_release_revision = int(local.get("release_revision", 0) or 0)
        force_remote = bool(force_remote and self.source_kind == "remote")
        self._sync_mirror_runtime()

        try:
            remote_manifest = self.client.fetch_latest_manifest()
        except SharedMirrorPendingError:
            self._set_runtime_and_state(
                last_check_at=now_text,
                last_result="mirror_pending_publish",
                last_error="",
                local_version=str(local_version_text),
                remote_version="",
                source_kind=self.source_kind,
                source_label=self.source_label,
                local_release_revision=local_release_revision,
                remote_release_revision=0,
                update_available=False,
                force_apply_available=False,
            )
            return self._build_result_payload(
                last_result="mirror_pending_publish",
                message="共享目录中还没有已批准的更新版本，等待外网端发布后会自动跟随更新。",
            )

        remote = normalize_remote_version(remote_manifest)
        remote_version_text = remote.get("display_version") or remote.get("build_id") or "-"
        remote_release_revision = int(remote.get("release_revision", 0) or 0)

        cmp = compare_versions(local, remote)
        allow_downgrade = bool(self.cfg["allow_downgrade"]) and self.source_kind == "remote"
        should_apply = bool(self.cfg["auto_apply"]) if apply_update is None else bool(apply_update)
        update_available = cmp < 0
        can_force_remote = self.source_kind == "remote" and bool(remote.get("build_id") or remote.get("display_version"))
        force_apply_available = can_force_remote and cmp > 0

        self.state["last_check_at"] = now_text
        self.state["local_version"] = str(local_version_text)
        self.state["remote_version"] = str(remote_version_text)
        self.state["source_kind"] = self.source_kind
        self.state["source_label"] = self.source_label
        self.state["local_release_revision"] = local_release_revision
        self.state["remote_release_revision"] = remote_release_revision
        self.state["update_available"] = update_available
        self.state["force_apply_available"] = force_apply_available
        self._persist_state()
        self._set_runtime(
            last_check_at=now_text,
            local_version=str(local_version_text),
            remote_version=str(remote_version_text),
            source_kind=self.source_kind,
            source_label=self.source_label,
            local_release_revision=local_release_revision,
            remote_release_revision=remote_release_revision,
            update_available=update_available,
            force_apply_available=force_apply_available,
        )

        if cmp == 0 and not force_remote:
            result_key = "restart_pending" if bool(self.state.get("restart_required", False)) else "up_to_date"
            self._set_runtime_and_state(last_result=result_key, last_error="")
            if self.source_kind == "remote" and self.role_mode == "external":
                self._try_publish_shared_mirror(
                    remote_manifest=remote_manifest,
                    approved_local_version=str(local_version_text),
                    approved_release_revision=local_release_revision,
                )
            return self._build_result_payload(
                last_result=result_key,
                message="补丁已应用，等待重启生效。" if result_key == "restart_pending" else "当前已经是最新版本。",
            )

        if cmp > 0 and not allow_downgrade and not force_remote:
            result_key = "ahead_of_mirror" if self.source_kind == "shared_mirror" else "ahead_of_remote"
            message = (
                "本地版本高于共享目录中的批准版本，内网端不会自动回退。"
                if result_key == "ahead_of_mirror"
                else "本地版本高于远端正式版本，点击“开始更新”将按远端正式版本重新覆盖。"
            )
            self._set_runtime_and_state(last_result=result_key, last_error="", force_apply_available=force_apply_available)
            return self._build_result_payload(last_result=result_key, message=message)

        if not should_apply:
            self._set_runtime_and_state(last_result="update_available", last_error="")
            return self._build_result_payload(
                last_result="update_available",
                message=(
                    "已检测到共享目录中的批准版本。"
                    if self.source_kind == "shared_mirror"
                    else "检测到可用更新。"
                ),
            )

        patch_ref = self._resolve_manifest_patch_ref(remote_manifest)
        zip_sha = str(remote_manifest.get("zip_sha256", "") or "").strip()
        if not patch_ref:
            raise RuntimeError("更新清单缺少 zip_url / zip_relpath。")

        zip_name = Path(str(patch_ref).split("?")[0]).name or "patch.zip"
        zip_path = self.download_dir / zip_name
        self._set_runtime_and_state(
            last_result="downloading_patch",
            last_error="",
            dependency_sync_status="idle",
            dependency_sync_error="",
        )
        self._log(
            "开始下载更新补丁: "
            f"模式={self._apply_mode_text('force_remote' if force_remote else 'normal')}, "
            f"来源={self.source_label}, 标识={patch_ref}"
        )
        self.client.download_patch(patch_ref, zip_path, expected_sha256=zip_sha)

        self._set_runtime_and_state(last_result="applying_patch")
        applied = self.applier.apply_patch_zip(
            zip_path=zip_path,
            backup_root=self.backup_dir,
            max_backups=int(self.cfg["max_backups"]),
        )
        patch_meta = applied.get("patch_meta", {})
        if not isinstance(patch_meta, dict):
            patch_meta = {}

        try:
            dependency_result = self._sync_patch_dependencies(applied, patch_meta)
        except Exception as dependency_exc:  # noqa: BLE001
            return self._rollback_after_dependency_failure(
                applied=applied,
                remote=remote,
                remote_version_text=str(remote_version_text),
                remote_release_revision=remote_release_revision,
                error_text=str(dependency_exc),
            )

        self._persist_local_build_meta_from_remote(remote_manifest)
        refreshed_local = normalize_local_version(load_local_build_meta(self.app_dir))
        refreshed_local_text = refreshed_local.get("display_version") or refreshed_local.get("build_id") or "-"
        refreshed_local_release_revision = int(refreshed_local.get("release_revision", 0) or 0)
        refreshed_update_available = compare_versions(refreshed_local, remote) < 0
        final_result = "updated"
        restart_required = False

        if bool(self.cfg.get("auto_restart", False)):
            final_result, restart_required = self._apply_restart_strategy(
                local_version=str(refreshed_local_text),
                local_release_revision=refreshed_local_release_revision,
            )

        dependency_status = "success" if str(dependency_result.get("status", "")).strip() == "success" else "idle"
        final_message = "补丁已应用完成。"
        if final_result == "updated_restart_scheduled":
            final_message = "补丁已应用并完成运行依赖同步，程序将自动重启。"
        elif final_result == "restart_pending":
            final_message = "补丁已应用并完成运行依赖同步，请重启程序使更新生效。"

        self._set_runtime_and_state(
            last_result=final_result,
            last_error="",
            local_version=str(refreshed_local_text),
            remote_version=str(remote_version_text),
            source_kind=self.source_kind,
            source_label=self.source_label,
            local_release_revision=refreshed_local_release_revision,
            remote_release_revision=remote_release_revision,
            update_available=refreshed_update_available,
            force_apply_available=bool(remote.get("build_id") or remote.get("display_version")) and compare_versions(refreshed_local, remote) > 0 and self.source_kind == "remote",
            last_updated_at=now_text,
            restart_required=restart_required,
            last_applied_release_revision=refreshed_local_release_revision,
            dependency_sync_status=dependency_status,
            dependency_sync_error="",
            dependency_sync_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        if self.source_kind == "remote" and self.role_mode == "external":
            self._try_publish_shared_mirror(
                remote_manifest=remote_manifest,
                approved_local_version=str(refreshed_local_text),
                approved_release_revision=refreshed_local_release_revision,
                patch_zip=zip_path,
            )
        self._log(
            "补丁更新完成: "
            f"结果={self._result_text(final_result)}, 替换文件={applied.get('replaced', 0)}, "
            f"删除文件={applied.get('deleted', 0)}, 依赖状态={self._dependency_status_text(dependency_status)}"
        )
        return self._build_result_payload(
            last_result=final_result,
            message=final_message,
            applied=applied,
            dependency_result=dependency_result,
        )

    def _run_check(self, *, apply_update: bool | None, force_remote: bool) -> Dict[str, Any]:
        locked = self._work_lock.acquire(blocking=False)
        if not locked:
            raise RuntimeError("更新检查正在进行，请稍后重试。")
        try:
            return self._check_once(apply_update=apply_update, force_remote=force_remote)
        finally:
            self._sync_shared_mirror_watch_signal()
            self._work_lock.release()

    def _try_process_queued_apply(self) -> None:
        queued_apply = self.state.get("queued_apply", {})
        if not isinstance(queued_apply, dict) or not bool(queued_apply.get("queued", False)):
            return
        if self.is_busy():
            return
        mode = "force_remote" if str(queued_apply.get("mode", "")).strip().lower() == "force_remote" else "normal"
        self._log(f"开始处理排队更新: 模式={self._apply_mode_text(mode)}")
        self._clear_queue()
        try:
            self._run_check(apply_update=True, force_remote=mode == "force_remote")
        except Exception as exc:  # noqa: BLE001
            self._record_failure("排队更新失败", exc)

    def _loop(self) -> None:
        self._set_runtime(running=True)
        self._log(f"更新线程已启动: interval={self.cfg['check_interval_sec']}s")
        next_check_monotonic = time.monotonic() + int(self.cfg["check_interval_sec"])
        while not self._stop.wait(1):
            self._try_process_queued_apply()
            if self._consume_shared_mirror_watch_trigger():
                self._log("检测到共享目录批准版本变化，立即检查更新")
                next_check_monotonic = time.monotonic() + int(self.cfg["check_interval_sec"])
                try:
                    self._run_check(apply_update=None, force_remote=False)
                except Exception as exc:  # noqa: BLE001
                    self._record_failure("共享目录变更触发检查失败", exc)
                continue
            if time.monotonic() < next_check_monotonic:
                continue
            next_check_monotonic = time.monotonic() + int(self.cfg["check_interval_sec"])
            try:
                self._run_check(apply_update=None, force_remote=False)
            except Exception as exc:  # noqa: BLE001
                self._record_failure("周期检查失败", exc)
        self._set_runtime(running=False)
        self._log("更新线程已停止")

