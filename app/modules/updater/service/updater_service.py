from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
import hashlib
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

from pipeline_utils import DEFAULT_CONFIG_FILENAME, get_app_dir, get_app_root_dir, get_persistent_user_data_dir

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
from app.modules.updater.service.remote_control_store import (
    UpdaterRemoteControlStore,
    empty_internal_peer_snapshot,
)
from app.modules.updater.service.runtime_dependency_sync_service import RuntimeDependencySyncService
from app.modules.updater.service.update_applier import UpdateApplier


_SOURCE_RUN_DISABLE_UPDATER_ENV = "QJPT_DISABLE_UPDATER_IN_SOURCE_RUN"
_SOURCE_RUN_GIT_PULL_ENV = "QJPT_ENABLE_GIT_PULL_IN_SOURCE_RUN"
_SOURCE_APPROVED_UPDATE_MODE = "shared_approved_source"
_SOURCE_APPROVED_SOURCE_KIND = "shared_approved_source"
_INTERNAL_PEER_HEARTBEAT_INTERVAL_SEC = 5
_INTERNAL_PEER_HEARTBEAT_TIMEOUT_SEC = 15
_GIT_TRACKED_STATUS_TIMEOUT_SEC = 30
_SOURCE_SNAPSHOT_ZIP_NAME = "source_snapshot.zip"
_SOURCE_MANIFEST_NAME = "source_manifest.json"
_SOURCE_PUBLISH_STATE_NAME = "source_publish_state.json"
_SOURCE_SNAPSHOT_SCOPE_PY_ONLY = "py_only"
_SOURCE_GIT_HEAD_WATCH_INTERVAL_SEC = 10
_SOURCE_SNAPSHOT_EXCLUDED_DIRS = {
    ".git",
    ".hg",
    ".svn",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".runtime",
    ".venv",
    "__pycache__",
    "build_output",
    "config_segments",
    "dist",
    "htmlcov",
    "logs",
    "node_modules",
    "runtime_state",
    "share",
    "user_data",
    "venv",
}
_SOURCE_SNAPSHOT_EXCLUDED_PREFIXES = (
    "runtime/python/",
)
_SOURCE_SNAPSHOT_EXCLUDED_FILES = {
    DEFAULT_CONFIG_FILENAME,
    ".env",
}
_GIT_DIRTY_ALLOWLIST = {
    DEFAULT_CONFIG_FILENAME,
    "runtime_dependency_lock.json",
}
_GIT_DIRTY_ALLOWLIST_PREFIXES = (
    "config_segments/",
    "user_data/",
)


def _normalize_git_status_path(raw_path: Any) -> str:
    text = str(raw_path or "").strip().replace("\\", "/")
    if " -> " in text:
        text = text.split(" -> ", 1)[1].strip()
    while text.startswith("./"):
        text = text[2:]
    return text.lstrip("/")


def _is_ignorable_git_dirty_path(raw_path: Any) -> bool:
    normalized = _normalize_git_status_path(raw_path)
    if not normalized:
        return False
    if normalized in _GIT_DIRTY_ALLOWLIST:
        return True
    return any(normalized.startswith(prefix) for prefix in _GIT_DIRTY_ALLOWLIST_PREFIXES)


def _updater_disabled_reason_from_env() -> str:
    raw = str(os.environ.get(_SOURCE_RUN_DISABLE_UPDATER_ENV, "") or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return "source_python_run"
    return ""


def _source_run_git_pull_enabled_from_env() -> bool:
    raw = str(os.environ.get(_SOURCE_RUN_GIT_PULL_ENV, "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _short_git_commit(raw: Any) -> str:
    text = str(raw or "").strip()
    if len(text) >= 7:
        return text[:7]
    return text


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest().lower()


def _normalize_zip_relpath(raw_path: Path | str) -> str:
    return str(raw_path).replace("\\", "/").lstrip("/")


def _is_source_snapshot_excluded(rel_path: Path, *, is_dir: bool = False) -> bool:
    rel_text = _normalize_zip_relpath(rel_path)
    if not rel_text:
        return True
    parts = set(rel_path.parts)
    if any(part in _SOURCE_SNAPSHOT_EXCLUDED_DIRS for part in parts):
        return True
    if rel_path.name in _SOURCE_SNAPSHOT_EXCLUDED_FILES:
        return True
    if rel_path.name.startswith(f"{DEFAULT_CONFIG_FILENAME}.backup"):
        return True
    if any(rel_text == prefix.rstrip("/") or rel_text.startswith(prefix) for prefix in _SOURCE_SNAPSHOT_EXCLUDED_PREFIXES):
        return True
    if is_dir and rel_path.name in _SOURCE_SNAPSHOT_EXCLUDED_DIRS:
        return True
    return False


def _is_python_source_relpath(rel_path: Path | str) -> bool:
    rel = Path(str(rel_path).replace("\\", "/"))
    rel_text = _normalize_zip_relpath(rel)
    if not rel_text or rel.suffix.lower() != ".py":
        return False
    return not _is_source_snapshot_excluded(rel)


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
        self.runtime_state_root = str(paths_cfg.get("runtime_state_root", "") or "").strip()
        self.emit_log = emit_log
        self.restart_callback = restart_callback
        self.is_busy = is_busy or (lambda: False)
        self.role_mode = normalize_role_mode(deployment_cfg.get("role_mode"))
        self.node_id = str(deployment_cfg.get("node_id", "") or "").strip() or _default_node_id(self.role_mode)
        resolved_shared_bridge = resolve_shared_bridge_paths(shared_bridge_cfg, deployment_cfg.get("role_mode"))
        self.shared_bridge_enabled = bool(resolved_shared_bridge.get("enabled", False))
        self.shared_bridge_root = str(resolved_shared_bridge.get("root_dir", "") or "").strip()
        self.app_dir = get_app_dir()
        self.app_root_dir = get_app_root_dir(self.app_dir)
        self.persistent_user_data_dir = get_persistent_user_data_dir(self.app_dir)
        self.source_run_git_pull_enabled = _source_run_git_pull_enabled_from_env()
        if self.source_run_git_pull_enabled and self.role_mode == "internal":
            self.update_mode = _SOURCE_APPROVED_UPDATE_MODE
        elif self.source_run_git_pull_enabled:
            self.update_mode = "git_pull"
        else:
            self.update_mode = "patch_zip"
        self.git_available = bool(shutil.which("git")) if self.update_mode == "git_pull" else False
        self.git_repo_detected = bool((self.app_dir / ".git").exists()) if self.update_mode == "git_pull" else False
        self.git_repo_url = ""
        self.git_branch = ""
        self.git_remote_name = ""
        self._last_seen_git_head = ""
        if self.update_mode == "git_pull":
            self.source_kind = "git_remote"
            self.source_label = "Git 仓库更新源"
        elif self.update_mode == _SOURCE_APPROVED_UPDATE_MODE:
            self.source_kind = _SOURCE_APPROVED_SOURCE_KIND
            self.source_label = "共享目录批准源码"
        else:
            self.source_kind = "shared_mirror" if self.role_mode == "internal" else "remote"
            self.source_label = "共享目录更新源" if self.source_kind == "shared_mirror" else "远端正式更新源"
        self.disabled_reason = self._resolve_disabled_reason()
        if self.disabled_reason:
            self.cfg["enabled"] = False

        self.state_path = self._resolve_state_path(self.cfg["state_file"])
        self.download_dir = self._resolve_any_path(self.cfg["download_dir"])
        self.backup_dir = self._resolve_any_path(self.cfg["backup_dir"])
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        self.state_store = UpdaterStateStore(self.state_path)
        self.state = self.state_store.load()

        self.shared_mirror_client = SharedMirrorManifestClient(self.shared_bridge_root) if self.shared_bridge_root else None
        self.remote_control_store = UpdaterRemoteControlStore(self.shared_bridge_root) if self.shared_bridge_root else None
        if self.update_mode == "git_pull":
            self.client = None
        elif self.update_mode == _SOURCE_APPROVED_UPDATE_MODE:
            self.client = None
        elif self.source_kind == "shared_mirror":
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
        self._last_internal_peer_status_sync_monotonic = 0.0
        mirror_runtime = self._initial_mirror_runtime_snapshot()
        internal_peer_runtime = self._initial_internal_peer_runtime_snapshot()
        self.state.setdefault("source_kind", self.source_kind)
        self.state.setdefault("source_label", self.source_label)
        self.state.setdefault("update_mode", self.update_mode)
        self.state.setdefault("app_root_dir", str(self.app_root_dir))
        self.state.setdefault("persistent_user_data_dir", str(self.persistent_user_data_dir))
        self.state.setdefault("branch", "")
        self.state.setdefault("local_commit", "")
        self.state.setdefault("remote_commit", "")
        self.state.setdefault("worktree_dirty", False)
        self.state.setdefault("dirty_files", [])
        self.state.setdefault("last_published_commit", "")
        self.state.setdefault("last_publish_attempt_commit", "")
        self.state.setdefault("last_publish_deferred_commit", "")
        self.state.setdefault("last_publish_command_id", "")
        self.state.setdefault("last_internal_apply_completed_commit", "")
        self.state.setdefault("last_internal_apply_failed_commit", "")
        self.state.setdefault("enabled", bool(self.cfg["enabled"]))
        self.state.setdefault("disabled_reason", self.disabled_reason)
        self.state.setdefault("mirror_ready", mirror_runtime.get("mirror_ready", False))
        self.state.setdefault("mirror_version", mirror_runtime.get("mirror_version", ""))
        self.state.setdefault("mirror_manifest_path", mirror_runtime.get("mirror_manifest_path", ""))
        self.state.setdefault("last_publish_at", mirror_runtime.get("last_publish_at", ""))
        self.state.setdefault("last_publish_error", mirror_runtime.get("last_publish_error", ""))
        self.state.setdefault("approved_commit", mirror_runtime.get("approved_commit", ""))
        self.state.setdefault("approved_manifest", mirror_runtime.get("approved_manifest", {}))
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
            "update_mode": str(self.state.get("update_mode", self.update_mode) or self.update_mode),
            "app_root_dir": str(self.state.get("app_root_dir", str(self.app_root_dir)) or str(self.app_root_dir)),
            "persistent_user_data_dir": str(
                self.state.get("persistent_user_data_dir", str(self.persistent_user_data_dir))
                or str(self.persistent_user_data_dir)
            ),
            "git_available": False,
            "git_repo_detected": False,
            "branch": str(self.state.get("branch", "") or ""),
            "local_commit": str(self.state.get("local_commit", "") or ""),
            "remote_commit": str(self.state.get("remote_commit", "") or ""),
            "worktree_dirty": bool(self.state.get("worktree_dirty", False)),
            "dirty_files": list(self.state.get("dirty_files", []) or []),
            "last_published_commit": str(self.state.get("last_published_commit", "") or ""),
            "last_publish_attempt_commit": str(self.state.get("last_publish_attempt_commit", "") or ""),
            "last_publish_deferred_commit": str(self.state.get("last_publish_deferred_commit", "") or ""),
            "last_publish_command_id": str(self.state.get("last_publish_command_id", "") or ""),
            "last_internal_apply_completed_commit": str(
                self.state.get("last_internal_apply_completed_commit", "") or ""
            ),
            "last_internal_apply_failed_commit": str(self.state.get("last_internal_apply_failed_commit", "") or ""),
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
            "approved_commit": str(self.state.get("approved_commit", mirror_runtime.get("approved_commit", "")) or ""),
            "approved_manifest": dict(
                self.state.get("approved_manifest", mirror_runtime.get("approved_manifest", {})) or {}
            ),
            "internal_peer": internal_peer_runtime,
        }

    def _log(self, text: str) -> None:
        self.emit_log(f"[Updater] {text}")

    def _initial_mirror_runtime_snapshot(self) -> Dict[str, Any]:
        return {
            "mirror_ready": bool(self.state.get("mirror_ready", False)),
            "mirror_version": str(self.state.get("mirror_version", "") or "").strip(),
            "mirror_manifest_path": str(self.state.get("mirror_manifest_path", "") or "").strip(),
            "last_publish_at": str(self.state.get("last_publish_at", "") or "").strip(),
            "last_publish_error": str(self.state.get("last_publish_error", "") or "").strip(),
            "approved_commit": str(self.state.get("approved_commit", "") or "").strip(),
            "approved_manifest": dict(self.state.get("approved_manifest", {}) or {}),
        }

    def _initial_internal_peer_runtime_snapshot(self) -> Dict[str, Any]:
        return empty_internal_peer_snapshot(available=bool(self.role_mode == "external" and self.remote_control_store and self.shared_bridge_root))

    def _safe_sync_mirror_runtime(self) -> Dict[str, Any]:
        try:
            return self._sync_mirror_runtime()
        except Exception as exc:  # noqa: BLE001
            self._log(f"共享更新状态刷新失败，不阻断主流程: {exc}")
            return self._initial_mirror_runtime_snapshot()

    def _safe_sync_internal_peer_runtime(self) -> Dict[str, Any]:
        try:
            return self._sync_internal_peer_runtime()
        except Exception as exc:  # noqa: BLE001
            self._log(f"内网端更新状态刷新失败，不阻断主流程: {exc}")
            return self._initial_internal_peer_runtime_snapshot()

    def _safe_sync_shared_mirror_watch_signal(self) -> tuple[str, ...]:
        try:
            return self._sync_shared_mirror_watch_signal()
        except Exception as exc:  # noqa: BLE001
            self._log(f"共享更新监听状态刷新失败，不阻断主流程: {exc}")
            return ()

    def _safe_sync_git_runtime(self, *, fetch_remote: bool = False) -> Dict[str, Any]:
        try:
            return self._sync_git_runtime(fetch_remote=fetch_remote)
        except Exception as exc:  # noqa: BLE001
            self._log(f"Git 更新状态刷新失败，不阻断主流程: {exc}")
            return {}

    def _local_version_snapshot(self) -> tuple[str, int]:
        local_version = str(self.state.get("local_version", "") or self.runtime.get("local_version", "") or "").strip()
        local_revision = int(
            self.state.get("local_release_revision", self.runtime.get("local_release_revision", 0)) or 0
        )
        if local_version or local_revision > 0:
            return local_version, local_revision
        local = normalize_local_version(load_local_build_meta(self.app_dir))
        return (
            str(local.get("display_version") or local.get("build_id") or "").strip(),
            int(local.get("release_revision", 0) or 0),
        )

    def _internal_peer_runtime_snapshot(self) -> Dict[str, Any]:
        if self.role_mode != "external" or not self.remote_control_store or not self.shared_bridge_root:
            return empty_internal_peer_snapshot(available=False)
        snapshot = self.remote_control_store.build_internal_peer_snapshot(
            heartbeat_timeout_sec=_INTERNAL_PEER_HEARTBEAT_TIMEOUT_SEC,
        )
        snapshot["available"] = True
        return snapshot

    def _sync_internal_peer_runtime(self) -> Dict[str, Any]:
        snapshot = self._internal_peer_runtime_snapshot()
        self._set_runtime(internal_peer=snapshot)
        return snapshot

    def _write_internal_peer_status(
        self,
        *,
        online: bool,
        command: Dict[str, Any] | None = None,
        force: bool = False,
    ) -> None:
        if self.role_mode != "internal" or not self.remote_control_store or not self.shared_bridge_root:
            return
        now_monotonic = time.monotonic()
        if not force and (now_monotonic - self._last_internal_peer_status_sync_monotonic) < _INTERNAL_PEER_HEARTBEAT_INTERVAL_SEC:
            return
        current_status = self.remote_control_store.load_status()
        local_version, local_revision = self._local_version_snapshot()
        payload = {
            **current_status,
            "available": True,
            "online": bool(online),
            "heartbeat_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "node_id": self.node_id,
            "node_label": "内网端",
            "local_version": local_version,
            "local_commit": str(self.state.get("local_commit", "") or self.runtime.get("local_commit", "") or "").strip(),
            "local_release_revision": local_revision,
            "last_check_at": str(self.state.get("last_check_at", "") or ""),
            "last_result": str(self.state.get("last_result", "") or ""),
            "last_error": str(self.state.get("last_error", "") or ""),
            "update_available": bool(self.state.get("update_available", False)),
            "restart_required": bool(self.state.get("restart_required", False)),
            "queued_apply": dict(self.state.get("queued_apply", self._empty_queue_payload()) or self._empty_queue_payload()),
        }
        if isinstance(command, dict):
            payload.update(
                {
                    "last_command_id": str(command.get("command_id", "") or "").strip(),
                    "last_command_action": str(command.get("action", "") or "").strip().lower(),
                    "last_command_status": str(command.get("status", "") or "").strip().lower(),
                    "last_command_message": str(command.get("message", "") or "").strip(),
                    "last_command_source_commit": str(command.get("source_commit", "") or "").strip(),
                }
            )
        self.remote_control_store.write_status(payload)
        self._last_internal_peer_status_sync_monotonic = now_monotonic

    def submit_internal_peer_command(self, *, action: str) -> Dict[str, Any]:
        return self._submit_internal_peer_command(action=action)

    def _submit_internal_peer_command(self, *, action: str, source_commit: str = "") -> Dict[str, Any]:
        action_text = str(action or "").strip().lower()
        if self.role_mode != "external":
            raise RuntimeError("当前仅支持外网端下发内网更新命令。")
        if action_text not in {"check", "apply", "restart"}:
            raise RuntimeError("仅支持下发检查更新、开始更新或重启生效命令。")
        if not self.remote_control_store or not self.shared_bridge_root:
            raise RuntimeError("共享目录未配置，无法向内网端下发更新命令。")
        result = self.remote_control_store.submit_command(
            command_id=uuid.uuid4().hex,
            action=action_text,
            requested_by_node_id=self.node_id,
            requested_by_role=self.role_mode or "external",
            source_commit=source_commit,
        )
        internal_peer = self._sync_internal_peer_runtime()
        command = result.get("command", {}) if isinstance(result.get("command", {}), dict) else {}
        if result.get("accepted"):
            message = "已下发内网端检查更新命令，等待内网端执行。"
            if action_text == "apply":
                message = "已下发内网端开始更新命令，等待内网端执行。"
            elif action_text == "restart":
                message = "已下发内网端重启生效命令，等待内网端执行。"
            self._log(
                "已下发内网远程更新命令: "
                f"action={action_text}, command_id={str(command.get('command_id', '') or '-').strip() or '-'}"
            )
        else:
            pending_action = str(command.get("action", "") or "").strip().lower()
            message = "已有待执行的内网端更新命令，请等待其完成。"
            if pending_action == "check":
                message = "已有待执行的内网端检查更新命令，请等待其完成。"
            elif pending_action == "apply":
                message = "已有待执行的内网端开始更新命令，请等待其完成。"
            elif pending_action == "restart":
                message = "已有待执行的内网端重启生效命令，请等待其完成。"
        return {
            **result,
            "action": action_text,
            "message": message,
            "internal_peer": internal_peer,
        }

    def _resolve_disabled_reason(self) -> str:
        env_reason = _updater_disabled_reason_from_env()
        if env_reason:
            return env_reason
        if self.update_mode != "git_pull":
            return ""
        if not self.git_available:
            return "git_not_installed"
        if not self.git_repo_detected:
            return "git_repo_missing"
        return ""

    def _run_git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args],
            cwd=str(self.app_dir),
            text=True,
            capture_output=True,
            check=False,
            timeout=_GIT_TRACKED_STATUS_TIMEOUT_SEC,
        )

    def _detect_git_identity(self, branch_hint: str = "") -> Dict[str, str]:
        if self.update_mode != "git_pull" or not self.git_available or not self.git_repo_detected:
            return {"branch": "", "remote_name": "", "remote_url": ""}
        branch = str(branch_hint or "").strip()
        if not branch:
            branch_ret = self._run_git("rev-parse", "--abbrev-ref", "HEAD")
            if branch_ret.returncode == 0:
                branch = str(branch_ret.stdout or "").strip()
        if not branch or branch == "HEAD":
            branch = str(self.git_branch or "").strip()
        remote_name = ""
        if branch:
            branch_remote_ret = self._run_git("config", "--get", f"branch.{branch}.remote")
            if branch_remote_ret.returncode == 0:
                remote_name = str(branch_remote_ret.stdout or "").strip()
        if not remote_name:
            remotes_ret = self._run_git("remote")
            if remotes_ret.returncode == 0:
                remotes = [str(line or "").strip() for line in (remotes_ret.stdout or "").splitlines() if str(line or "").strip()]
                if "origin" in remotes:
                    remote_name = "origin"
                elif remotes:
                    remote_name = remotes[0]
        remote_url = ""
        if remote_name:
            remote_url_ret = self._run_git("remote", "get-url", remote_name)
            if remote_url_ret.returncode == 0:
                remote_url = str(remote_url_ret.stdout or "").strip()
        return {
            "branch": str(branch or "").strip(),
            "remote_name": str(remote_name or "").strip(),
            "remote_url": str(remote_url or "").strip(),
        }

    def _git_tracked_status(self) -> Dict[str, Any]:
        if self.update_mode != "git_pull":
            return {
                "update_mode": self.update_mode,
                "branch": "",
                "remote_name": "",
                "remote_url": "",
                "local_commit": "",
                "remote_commit": "",
                "worktree_dirty": False,
                "dirty_files": [],
            }
        snapshot = {
            "update_mode": "git_pull",
            "branch": "",
            "remote_name": "",
            "remote_url": "",
            "local_commit": "",
            "remote_commit": "",
            "worktree_dirty": False,
            "dirty_files": [],
        }
        if not self.git_available or not self.git_repo_detected:
            return snapshot
        identity = self._detect_git_identity(self.git_branch)
        snapshot["branch"] = str(identity.get("branch", "") or "").strip()
        snapshot["remote_name"] = str(identity.get("remote_name", "") or "").strip()
        snapshot["remote_url"] = str(identity.get("remote_url", "") or "").strip()
        local_commit_ret = self._run_git("rev-parse", "HEAD")
        if local_commit_ret.returncode == 0:
            snapshot["local_commit"] = str(local_commit_ret.stdout or "").strip()
        dirty_ret = self._run_git("status", "--porcelain", "--untracked-files=no")
        if dirty_ret.returncode == 0:
            dirty_lines = [
                _normalize_git_status_path(line[3:].strip())
                for line in (dirty_ret.stdout or "").splitlines()
                if str(line or "").strip()
            ]
            blocking_dirty_lines = [line for line in dirty_lines if line and not _is_ignorable_git_dirty_path(line)]
            snapshot["dirty_files"] = blocking_dirty_lines
            snapshot["worktree_dirty"] = bool(blocking_dirty_lines)
        remote_name = str(snapshot.get("remote_name", "") or self.git_remote_name or "").strip()
        branch = str(snapshot.get("branch", "") or self.git_branch or "").strip()
        remote_commit_ret = self._run_git("rev-parse", f"{remote_name}/{branch}") if remote_name and branch else None
        if remote_commit_ret and remote_commit_ret.returncode == 0:
            snapshot["remote_commit"] = str(remote_commit_ret.stdout or "").strip()
        return snapshot

    def _sync_git_runtime(self, *, fetch_remote: bool = False) -> Dict[str, Any]:
        snapshot = self._git_tracked_status()
        if self.update_mode != "git_pull":
            return snapshot
        remote_name = str(snapshot.get("remote_name", "") or self.git_remote_name or "").strip()
        branch = str(snapshot.get("branch", "") or self.git_branch or "").strip()
        local = normalize_local_version(load_local_build_meta(self.app_dir))
        local_version_text = local.get("display_version") or local.get("build_id") or "-"
        local_release_revision = int(local.get("release_revision", 0) or 0)
        if fetch_remote and self.git_available and self.git_repo_detected and remote_name and branch:
            fetch_ret = self._run_git("fetch", remote_name, branch)
            if fetch_ret.returncode != 0:
                raise RuntimeError((fetch_ret.stderr or fetch_ret.stdout or "git fetch 失败").strip())
            snapshot = self._git_tracked_status()
        elif fetch_remote and self.git_available and self.git_repo_detected:
            raise RuntimeError("当前未配置 Git 更新仓库地址。")
        self.git_repo_detected = bool((self.app_dir / ".git").exists())
        self.git_branch = str(snapshot.get("branch", "") or self.git_branch).strip() or self.git_branch
        self.git_remote_name = str(snapshot.get("remote_name", "") or self.git_remote_name).strip() or self.git_remote_name
        self.git_repo_url = str(snapshot.get("remote_url", "") or self.git_repo_url).strip()
        self._set_runtime(
            update_mode="git_pull",
            app_root_dir=str(self.app_root_dir),
            persistent_user_data_dir=str(self.persistent_user_data_dir),
            git_available=self.git_available,
            git_repo_detected=self.git_repo_detected,
            source_kind=self.source_kind,
            source_label=self.source_label,
            local_version=str(local_version_text),
            remote_version=_short_git_commit(str(snapshot.get("remote_commit", "") or "")),
            local_release_revision=local_release_revision,
            remote_release_revision=local_release_revision,
            branch=str(snapshot.get("branch", "") or self.git_branch or ""),
            local_commit=str(snapshot.get("local_commit", "") or ""),
            remote_commit=str(snapshot.get("remote_commit", "") or ""),
            worktree_dirty=bool(snapshot.get("worktree_dirty", False)),
            dirty_files=list(snapshot.get("dirty_files", [])),
        )
        self.state.update(
            {
                "update_mode": "git_pull",
                "app_root_dir": str(self.app_root_dir),
                "persistent_user_data_dir": str(self.persistent_user_data_dir),
                "git_available": self.git_available,
                "git_repo_detected": self.git_repo_detected,
                "source_kind": self.source_kind,
                "source_label": self.source_label,
                "local_version": str(local_version_text),
                "remote_version": _short_git_commit(str(snapshot.get("remote_commit", "") or "")),
                "local_release_revision": local_release_revision,
                "remote_release_revision": local_release_revision,
                "branch": str(snapshot.get("branch", "") or self.git_branch or ""),
                "local_commit": str(snapshot.get("local_commit", "") or ""),
                "remote_commit": str(snapshot.get("remote_commit", "") or ""),
                "worktree_dirty": bool(snapshot.get("worktree_dirty", False)),
                "dirty_files": list(snapshot.get("dirty_files", [])),
            }
        )
        self._persist_state()
        return snapshot

    @staticmethod
    def _disabled_reason_text(raw: Any) -> str:
        key = str(raw or "").strip().lower()
        if key == "source_python_run":
            return "当前为源码直跑模式，请先执行 git pull 后重启程序。"
        if key == "git_not_installed":
            return "当前电脑未安装 Git，无法执行代码拉取更新。"
        if key == "git_repo_missing":
            return "当前代码目录不是 Git 工作区，无法执行代码拉取更新。"
        if key == "git_remote_missing":
            return "当前未配置 Git 更新仓库地址。"
        return "当前运行模式不支持应用内更新。"

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
            "git_fetching": "Git 远端检查中",
            "git_pulling": "Git 拉取中",
            "dirty_worktree": "检测到本地改动，已阻止更新",
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
                "approved_commit": "",
                "approved_manifest": {},
            }
        if self.update_mode in {"git_pull", _SOURCE_APPROVED_UPDATE_MODE}:
            return self._source_approved_runtime_snapshot()
        snapshot = self.shared_mirror_client.get_runtime_snapshot()
        return {
            "mirror_ready": bool(snapshot.get("mirror_ready", False)),
            "mirror_version": str(snapshot.get("mirror_version", "") or "").strip(),
            "mirror_manifest_path": str(snapshot.get("mirror_manifest_path", "") or "").strip(),
            "last_publish_at": str(snapshot.get("last_publish_at", "") or "").strip(),
            "last_publish_error": str(snapshot.get("last_publish_error", "") or "").strip(),
            "approved_commit": "",
            "approved_manifest": {},
        }

    @property
    def _source_approved_root(self) -> Path:
        if self.shared_mirror_client:
            return self.shared_mirror_client.approved_root
        return Path(self.shared_bridge_root) / "updater" / "approved"

    @property
    def _source_manifest_path(self) -> Path:
        return self._source_approved_root / _SOURCE_MANIFEST_NAME

    @property
    def _source_publish_state_path(self) -> Path:
        return self._source_approved_root / _SOURCE_PUBLISH_STATE_NAME

    def _default_source_publish_state(self) -> Dict[str, Any]:
        return {
            "mirror_ready": False,
            "mirror_version": "",
            "mirror_release_revision": 0,
            "last_publish_at": "",
            "last_publish_error": "",
            "mirror_manifest_path": str(self._source_manifest_path),
            "published_by_role": "",
            "published_by_node_id": "",
            "zip_relpath": "",
            "approved_commit": "",
        }

    def _load_source_publish_state(self) -> Dict[str, Any]:
        payload = self._default_source_publish_state()
        try:
            if self._source_publish_state_path.exists():
                loaded = json.loads(self._source_publish_state_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    payload.update(loaded)
        except Exception:  # noqa: BLE001
            pass
        payload["mirror_manifest_path"] = str(self._source_manifest_path)
        return payload

    def _source_approved_runtime_snapshot(self) -> Dict[str, Any]:
        state = self._load_source_publish_state()
        manifest: Dict[str, Any] = {}
        if self._source_manifest_path.exists():
            try:
                loaded = json.loads(self._source_manifest_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    manifest = loaded
            except Exception as exc:  # noqa: BLE001
                state["last_publish_error"] = f"读取源码批准清单失败: {exc}"
                state["mirror_ready"] = False
        approved_commit = str(manifest.get("source_commit", state.get("approved_commit", "")) or "").strip()
        version = str(
            manifest.get("display_version")
            or manifest.get("target_display_version")
            or state.get("mirror_version", "")
            or _short_git_commit(approved_commit)
            or ""
        ).strip()
        return {
            "mirror_ready": bool(state.get("mirror_ready", False)) and bool(manifest),
            "mirror_version": version,
            "mirror_manifest_path": str(self._source_manifest_path),
            "last_publish_at": str(state.get("last_publish_at", "") or manifest.get("created_at", "") or "").strip(),
            "last_publish_error": str(state.get("last_publish_error", "") or "").strip(),
            "approved_commit": approved_commit,
            "approved_manifest": manifest,
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
            approved_commit=str(snapshot.get("approved_commit", "") or "").strip(),
            approved_manifest=dict(snapshot.get("approved_manifest", {}) or {}),
        )
        return snapshot

    def _git_tracked_python_files(self) -> List[Path]:
        if self.update_mode != "git_pull" or not self.git_available or not self.git_repo_detected:
            return []
        ret = self._run_git("ls-files", "*.py")
        if ret.returncode != 0:
            raise RuntimeError((ret.stderr or ret.stdout or "git ls-files 失败").strip())
        output: List[Path] = []
        seen: set[str] = set()
        for raw in (ret.stdout or "").splitlines():
            normalized = _normalize_git_status_path(raw)
            if not normalized:
                continue
            rel = Path(*Path(normalized).parts)
            rel_text = _normalize_zip_relpath(rel)
            if rel_text in seen or not _is_python_source_relpath(rel):
                continue
            path = self.app_dir / rel
            if not path.is_file():
                continue
            seen.add(rel_text)
            output.append(rel)
        output.sort(key=lambda item: _normalize_zip_relpath(item))
        return output

    def _shared_mirror_watch_signal(self) -> tuple[str, ...]:
        if self.source_kind not in {"shared_mirror", _SOURCE_APPROVED_SOURCE_KIND} or not self.shared_mirror_client:
            return ()
        parts: list[str] = []
        # Only use the final publish-state file as the immediate trigger.
        # The manifest may appear slightly earlier during publishing, and
        # watching it would widen the window for reacting to a half-published mirror.
        watch_path = (
            self._source_publish_state_path
            if self.source_kind == _SOURCE_APPROVED_SOURCE_KIND
            else self.shared_mirror_client.publish_state_path
        )
        for path in (watch_path,):
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
        if self.source_kind in {"shared_mirror", _SOURCE_APPROVED_SOURCE_KIND} and not self.shared_mirror_client:
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

        if self.update_mode == "git_pull":
            self._set_runtime_and_state(
                enabled=True,
                disabled_reason="",
                last_error="",
                source_kind=self.source_kind,
                source_label=self.source_label,
                update_mode=self.update_mode,
                dependency_sync_status=str(self.state.get("dependency_sync_status", "idle") or "idle"),
                dependency_sync_error=str(self.state.get("dependency_sync_error", "") or ""),
                queued_apply=dict(self.state.get("queued_apply", self._empty_queue_payload()) or self._empty_queue_payload()),
            )
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, daemon=True, name="qjpt-updater")
            self._thread.start()
            return {"started": True, "running": True, "reason": "started"}

        if self.update_mode == _SOURCE_APPROVED_UPDATE_MODE:
            self._set_runtime_and_state(
                enabled=True,
                disabled_reason="",
                last_error="",
                source_kind=self.source_kind,
                source_label=self.source_label,
                update_mode=self.update_mode,
                dependency_sync_status=str(self.state.get("dependency_sync_status", "idle") or "idle"),
                dependency_sync_error=str(self.state.get("dependency_sync_error", "") or ""),
                queued_apply=dict(self.state.get("queued_apply", self._empty_queue_payload()) or self._empty_queue_payload()),
            )
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, daemon=True, name="qjpt-updater")
            self._thread.start()
            self._log("内网端共享目录批准源码更新模式已启用；启动阶段不会自动应用更新。")
            return {"started": True, "running": True, "reason": "started"}

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
        if self.role_mode == "internal":
            self._write_internal_peer_status(online=False, force=True)
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
        if self.is_busy():
            raise RuntimeError("当前仍有任务在运行，暂不能重启生效。")
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
            "update_available": bool(self.state.get("update_available", False)),
            "force_apply_available": bool(self.state.get("force_apply_available", False)),
            "local_version": str(self.state.get("local_version", "")),
            "remote_version": str(self.state.get("remote_version", "")),
            "source_kind": str(self.state.get("source_kind", self.source_kind) or self.source_kind),
            "source_label": str(self.state.get("source_label", self.source_label) or self.source_label),
            "update_mode": str(self.state.get("update_mode", self.update_mode) or self.update_mode),
            "app_root_dir": str(self.state.get("app_root_dir", str(self.app_root_dir)) or str(self.app_root_dir)),
            "persistent_user_data_dir": str(
                self.state.get("persistent_user_data_dir", str(self.persistent_user_data_dir))
                or str(self.persistent_user_data_dir)
            ),
            "git_available": bool(self.state.get("git_available", self.git_available)),
            "git_repo_detected": bool(self.state.get("git_repo_detected", self.git_repo_detected)),
            "branch": str(self.state.get("branch", "") or ""),
            "local_commit": str(self.state.get("local_commit", "") or ""),
            "remote_commit": str(self.state.get("remote_commit", "") or ""),
            "worktree_dirty": bool(self.state.get("worktree_dirty", False)),
            "dirty_files": list(self.state.get("dirty_files", []) or []),
            "last_published_commit": str(self.state.get("last_published_commit", "") or ""),
            "last_publish_attempt_commit": str(self.state.get("last_publish_attempt_commit", "") or ""),
            "last_publish_deferred_commit": str(self.state.get("last_publish_deferred_commit", "") or ""),
            "last_publish_command_id": str(self.state.get("last_publish_command_id", "") or ""),
            "last_internal_apply_completed_commit": str(
                self.state.get("last_internal_apply_completed_commit", "") or ""
            ),
            "last_internal_apply_failed_commit": str(self.state.get("last_internal_apply_failed_commit", "") or ""),
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
            "approved_commit": str(self.state.get("approved_commit", "") or ""),
            "approved_manifest": dict(self.state.get("approved_manifest", {}) or {}),
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

    def _queue_remote_apply(
        self,
        *,
        mode: str,
        reason: str,
        command: Dict[str, Any],
    ) -> Dict[str, Any]:
        queue_payload = {
            "queued": True,
            "mode": mode,
            "queued_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "reason": reason,
            "command_id": str(command.get("command_id", "") or "").strip(),
            "source_commit": str(command.get("source_commit", "") or "").strip(),
        }
        self._set_runtime_and_state(
            queued_apply=queue_payload,
            last_result="queued_busy",
            last_error="",
        )
        self._log(
            "远程更新请求已排队: "
            f"模式={self._apply_mode_text(mode)}, command_id={queue_payload['command_id'] or '-'}"
        )
        return {"queued_apply": queue_payload, "message": "当前仍有任务在运行，更新将在任务结束后自动执行。"}

    def _clear_queue(self) -> None:
        self._set_runtime_and_state(queued_apply=self._empty_queue_payload())

    def _apply_update_and_restart_if_needed(
        self,
        *,
        mode: str = "normal",
        queue_if_busy: bool = False,
        command: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized_mode = "force_remote" if str(mode or "").strip().lower() == "force_remote" else "normal"
        if queue_if_busy and self.is_busy():
            if isinstance(command, dict) and str(command.get("command_id", "") or "").strip():
                return self._build_result_payload(
                    last_result="queued_busy",
                    queue_status="queued",
                    **self._queue_remote_apply(mode=normalized_mode, reason="active_job_running", command=command),
                )
            queue_payload = self._queue_apply(mode=normalized_mode, reason="active_job_running")
            return self._build_result_payload(last_result="queued_busy", queue_status="queued", **queue_payload)
        result = self._run_check(apply_update=True, force_remote=normalized_mode == "force_remote")
        result_key = str(result.get("last_result", "") or "").strip().lower()
        if result_key == "restart_pending" or bool(result.get("restart_required", False)):
            return self.restart_now()
        return result

    def _resolve_manifest_patch_ref(self, remote_manifest: Dict[str, Any]) -> str:
        zip_relpath = str(remote_manifest.get("zip_relpath", "") or "").strip()
        zip_url = str(remote_manifest.get("zip_url", "") or "").strip()
        if isinstance(self.client, SharedMirrorManifestClient):
            return zip_relpath or zip_url
        return zip_url or zip_relpath

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

    def _build_source_snapshot_zip(self, *, manifest: Dict[str, Any], zip_path: Path) -> Dict[str, Any]:
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        if zip_path.exists():
            zip_path.unlink()
        source_files = self._git_tracked_python_files()
        if not source_files:
            raise RuntimeError("当前 Git 工作区未找到可同步的 .py 文件。")
        file_entries: List[Dict[str, Any]] = []
        for rel in source_files:
            path = self.app_dir / rel
            file_entries.append(
                {
                    "path": _normalize_zip_relpath(rel),
                    "sha256": _sha256_file(path),
                    "size": int(path.stat().st_size),
                }
            )
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
            embedded_manifest = dict(manifest if isinstance(manifest, dict) else {})
            embedded_manifest.pop("sha256", None)
            embedded_manifest.pop("zip_sha256", None)
            embedded_manifest.pop("zip_size", None)
            embedded_manifest["scope"] = _SOURCE_SNAPSHOT_SCOPE_PY_ONLY
            embedded_manifest["files"] = list(file_entries)
            embedded_manifest["deleted_files"] = []
            archive.writestr(_SOURCE_MANIFEST_NAME, json.dumps(embedded_manifest, ensure_ascii=False, indent=2))
            for rel in source_files:
                path = self.app_dir / rel
                rel_text = _normalize_zip_relpath(rel)
                archive.write(path, rel_text)
        return {
            "zip_path": str(zip_path),
            "included_files": len(source_files),
            "zip_size": int(zip_path.stat().st_size),
            "files": file_entries,
            "scope": _SOURCE_SNAPSHOT_SCOPE_PY_ONLY,
        }

    def _write_source_publish_error(self, error_text: str) -> None:
        if not self.shared_bridge_root:
            return
        payload = self._load_source_publish_state()
        payload.update(
            {
                "mirror_ready": False,
                "last_publish_error": str(error_text or "").strip(),
                "last_publish_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "published_by_role": self.role_mode,
                "published_by_node_id": self.node_id,
                "mirror_manifest_path": str(self._source_manifest_path),
            }
        )
        self._source_publish_state_path.parent.mkdir(parents=True, exist_ok=True)
        self._source_publish_state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def publish_approved_source_snapshot(self) -> Dict[str, Any]:
        if self.role_mode != "external":
            raise RuntimeError("仅外网端可发布内网批准源码版本。")
        if self.update_mode != "git_pull":
            raise RuntimeError("当前不是 Git 源码更新模式，无法发布源码快照。")
        if not self.shared_bridge_root or not self.shared_mirror_client:
            raise RuntimeError("共享目录未配置，无法发布内网批准源码版本。")

        try:
            git_snapshot = self._sync_git_runtime(fetch_remote=False)
            local_commit = str(git_snapshot.get("local_commit", "") or self.state.get("local_commit", "") or "").strip()
            branch = str(git_snapshot.get("branch", "") or self.state.get("branch", "") or "").strip()
            if not local_commit:
                raise RuntimeError("当前 Git 本地提交未知，请先检查代码更新。")
            if bool(git_snapshot.get("worktree_dirty", False)):
                dirty_files = list(git_snapshot.get("dirty_files", []) or [])
                raise RuntimeError(
                    "检测到本地代码改动，已阻止发布内网批准源码版本。"
                    + (f" 脏文件={','.join(dirty_files[:5])}" if dirty_files else "")
                )

            local = normalize_local_version(load_local_build_meta(self.app_dir))
            local_version_text = str(local.get("display_version") or local.get("build_id") or "").strip()
            local_release_revision = int(local.get("release_revision", 0) or 0)
            published_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            manifest = {
                "format": "source_snapshot",
                "scope": _SOURCE_SNAPSHOT_SCOPE_PY_ONLY,
                "source_commit": local_commit,
                "branch": branch,
                "created_at": published_at,
                "zip_relpath": _SOURCE_SNAPSHOT_ZIP_NAME,
                "display_version": local_version_text,
                "release_revision": local_release_revision,
                "target_display_version": local_version_text,
                "target_release_revision": local_release_revision,
                "published_by_role": self.role_mode,
                "published_by_node_id": self.node_id,
                "exclude_policy_version": 1,
            }
            staging_zip = self.download_dir / f"source_snapshot_{_short_git_commit(local_commit) or int(time.time())}.zip"
            build_result = self._build_source_snapshot_zip(manifest=manifest, zip_path=staging_zip)
            manifest["files"] = list(build_result.get("files", []) or [])
            manifest["deleted_files"] = []
            actual_sha = _sha256_file(staging_zip)
            manifest["sha256"] = actual_sha
            manifest["zip_sha256"] = actual_sha
            manifest["zip_size"] = int(staging_zip.stat().st_size)

            approved_root = self._source_approved_root
            staging_root = approved_root.parent / "staging"
            approved_root.mkdir(parents=True, exist_ok=True)
            staging_root.mkdir(parents=True, exist_ok=True)
            staging_approved_zip = staging_root / _SOURCE_SNAPSHOT_ZIP_NAME
            approved_zip = approved_root / _SOURCE_SNAPSHOT_ZIP_NAME
            approved_zip_tmp = approved_root / f".{_SOURCE_SNAPSHOT_ZIP_NAME}.tmp"
            manifest_tmp = approved_root / f".{_SOURCE_MANIFEST_NAME}.tmp"
            state_tmp = approved_root / f".{_SOURCE_PUBLISH_STATE_NAME}.tmp"
            shutil.copy2(staging_zip, staging_approved_zip)
            shutil.copy2(staging_approved_zip, approved_zip_tmp)
            os.replace(approved_zip_tmp, approved_zip)
            manifest_tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(manifest_tmp, self._source_manifest_path)
            publish_state = {
                "mirror_ready": True,
                "mirror_version": local_version_text or _short_git_commit(local_commit),
                "mirror_release_revision": local_release_revision,
                "last_publish_at": published_at,
                "last_publish_error": "",
                "mirror_manifest_path": str(self._source_manifest_path),
                "published_by_role": self.role_mode,
                "published_by_node_id": self.node_id,
                "zip_relpath": _SOURCE_SNAPSHOT_ZIP_NAME,
                "approved_commit": local_commit,
            }
            state_tmp.write_text(json.dumps(publish_state, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(state_tmp, self._source_publish_state_path)
            staging_approved_zip.unlink(missing_ok=True)
            self._sync_mirror_runtime()
            self._log(
                "已发布内网批准源码版本到共享目录: "
                f"commit={_short_git_commit(local_commit)}, files={build_result.get('included_files', 0)}, "
                f"path={self._source_manifest_path}"
            )
            return {
                "published": True,
                "manifest_path": str(self._source_manifest_path),
                "zip_path": str(approved_zip),
                "sha256": actual_sha,
                "source_commit": local_commit,
                "branch": branch,
                "display_version": local_version_text,
                "release_revision": local_release_revision,
                "included_files": int(build_result.get("included_files", 0) or 0),
            }
        except Exception as exc:  # noqa: BLE001
            self._write_source_publish_error(str(exc))
            self._sync_mirror_runtime()
            self._log(f"内网批准源码版本发布失败: {exc}")
            raise

    def _schedule_external_restart_after_source_publish(self, *, source_commit: str) -> bool:
        if not callable(self.restart_callback):
            self._set_runtime_and_state(restart_required=True)
            self._log("外网端源码已发布，当前未绑定自动重启回调，请手动重启外网端。")
            return False
        ok, detail = self.restart_callback(
            {
                "reason": "source_git_head_synced",
                "target_version": _short_git_commit(source_commit),
                "target_release_revision": int(self.state.get("local_release_revision", 0) or 0),
            }
        )
        if ok:
            self._set_runtime_and_state(last_result="updated_restart_scheduled", last_error="", restart_required=False)
            self._log(detail or "外网端源码同步发布完成，已安排当前窗口重启。")
            return True
        self._set_runtime_and_state(restart_required=True, last_error=str(detail or "外网端自动重启失败"))
        self._log(f"外网端源码同步发布完成，但自动重启失败，请手动重启: {detail or '-'}")
        return False

    def _auto_publish_git_head_to_internal(self) -> Dict[str, Any]:
        if self.role_mode != "external" or self.update_mode != "git_pull":
            return {"accepted": False, "reason": "not_external_git_pull"}
        if not self.shared_bridge_root or not self.remote_control_store:
            return {"accepted": False, "reason": "shared_bridge_unavailable"}
        git_snapshot = self._sync_git_runtime(fetch_remote=False)
        local_commit = str(git_snapshot.get("local_commit", "") or "").strip()
        if not local_commit:
            return {"accepted": False, "reason": "missing_local_commit"}
        if bool(git_snapshot.get("worktree_dirty", False)):
            dirty_files = list(git_snapshot.get("dirty_files", []) or [])
            error_text = "检测到本地代码改动，已阻止自动同步内网端。"
            if dirty_files:
                error_text += f" 脏文件={','.join(str(item) for item in dirty_files[:5])}"
            self._set_runtime_and_state(
                last_result="dirty_worktree",
                last_error=error_text,
                update_available=False,
                local_commit=local_commit,
            )
            self._log(error_text)
            return {"accepted": False, "reason": "dirty_worktree", "error": error_text}
        last_published_commit = str(self.state.get("last_published_commit", "") or "").strip()
        if last_published_commit == local_commit:
            return {"accepted": False, "reason": "already_published", "source_commit": local_commit}

        active_command = self.remote_control_store.load_command()
        if self.remote_control_store.is_active_command(active_command):
            self._set_runtime_and_state(
                last_publish_deferred_commit=local_commit,
                last_publish_command_id=str(active_command.get("command_id", "") or "").strip(),
                last_result="source_publishing",
                last_error="已有内网端更新命令待执行，暂不覆盖共享源码包。",
            )
            return {
                "accepted": False,
                "reason": "internal_command_active",
                "source_commit": local_commit,
                "command": active_command,
            }

        result = self.publish_approved_source_snapshot()
        source_commit = str(result.get("source_commit", "") or local_commit).strip()
        command_result = self._submit_internal_peer_command(action="apply", source_commit=source_commit)
        command = command_result.get("command", {}) if isinstance(command_result.get("command", {}), dict) else {}
        command_accepted = bool(command_result.get("accepted", False))
        state_update = {
            "last_publish_attempt_commit": source_commit,
            "last_publish_command_id": str(command.get("command_id", "") or "").strip(),
            "last_result": "source_publishing",
            "last_error": "",
        }
        if command_accepted:
            state_update["last_published_commit"] = source_commit
        self._set_runtime_and_state(**state_update)
        self._log(
            "外网端检测到 Git HEAD 变化，已发布 .py 同步包并下发内网应用命令: "
            f"commit={_short_git_commit(source_commit)}, accepted={command_accepted}"
        )
        if command_accepted:
            self._schedule_external_restart_after_source_publish(source_commit=source_commit)
        return {
            "accepted": command_accepted,
            "reason": "published",
            "source_commit": source_commit,
            "publish": result,
            "command": command_result,
        }

    def _try_auto_publish_git_head_to_internal(self) -> None:
        try:
            result = self._auto_publish_git_head_to_internal()
            if str(result.get("reason", "") or "").strip() in {"published", "dirty_worktree"}:
                self._safe_sync_internal_peer_runtime()
        except Exception as exc:  # noqa: BLE001
            self._record_failure("自动同步内网端源码失败", exc)

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

    def _check_git_once(self, *, apply_update: bool | None) -> Dict[str, Any]:
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        local = normalize_local_version(load_local_build_meta(self.app_dir))
        local_version_text = local.get("display_version") or local.get("build_id") or "-"
        local_release_revision = int(local.get("release_revision", 0) or 0)
        self._set_runtime_and_state(
            last_result="git_fetching",
            last_error="",
            source_kind=self.source_kind,
            source_label=self.source_label,
            update_mode=self.update_mode,
        )
        git_snapshot = self._sync_git_runtime(fetch_remote=True)
        branch = str(git_snapshot.get("branch", "") or self.git_branch or "").strip()
        remote_name = str(git_snapshot.get("remote_name", "") or self.git_remote_name or "").strip()
        local_commit = str(git_snapshot.get("local_commit", "") or "").strip()
        remote_commit = str(git_snapshot.get("remote_commit", "") or "").strip()
        worktree_dirty = bool(git_snapshot.get("worktree_dirty", False))
        dirty_files = list(git_snapshot.get("dirty_files", []) or [])
        update_available = bool(local_commit and remote_commit and local_commit != remote_commit)

        self._set_runtime_and_state(
            last_check_at=now_text,
            local_version=str(local_version_text),
            remote_version=_short_git_commit(remote_commit),
            source_kind=self.source_kind,
            source_label=self.source_label,
            local_release_revision=local_release_revision,
            remote_release_revision=local_release_revision,
            update_available=update_available,
            force_apply_available=False,
            branch=branch,
            local_commit=local_commit,
            remote_commit=remote_commit,
            worktree_dirty=worktree_dirty,
            dirty_files=dirty_files,
        )

        if not apply_update:
            result_key = "up_to_date"
            message = "当前 Git 工作区已经是最新提交。"
            if update_available:
                result_key = "update_available"
                message = "检测到 Git 仓库有可拉取更新。"
            self._set_runtime_and_state(last_result=result_key, last_error="")
            return self._build_result_payload(last_result=result_key, message=message)

        if worktree_dirty:
            self._set_runtime_and_state(last_result="dirty_worktree", last_error="", update_available=update_available)
            dirty_preview = "，".join(dirty_files[:5])
            if len(dirty_files) > 5:
                dirty_preview = f"{dirty_preview} 等 {len(dirty_files)} 项"
            return self._build_result_payload(
                last_result="dirty_worktree",
                message=(
                    f"检测到本地已修改文件，已阻止自动更新：{dirty_preview}"
                    if dirty_preview
                    else "检测到本地已修改文件，已阻止自动更新。"
                ),
            )

        if not update_available:
            result_key = "restart_pending" if bool(self.state.get("restart_required", False)) else "up_to_date"
            self._set_runtime_and_state(last_result=result_key, last_error="")
            return self._build_result_payload(
                last_result=result_key,
                message="代码已拉取完成，等待重启生效。" if result_key == "restart_pending" else "当前 Git 工作区已经是最新提交。",
            )

        self._set_runtime_and_state(last_result="git_pulling", last_error="")
        self._log(
            "开始执行 Git 拉取更新: "
            f"remote={remote_name or self.git_remote_name or '-'}, branch={branch or self.git_branch}, "
            f"local={_short_git_commit(local_commit) or '-'}, "
            f"remote={_short_git_commit(remote_commit) or '-'}"
        )
        pull_ret = self._run_git("pull", "--ff-only", remote_name or self.git_remote_name or "origin", branch or self.git_branch)
        if pull_ret.returncode != 0:
            raise RuntimeError((pull_ret.stderr or pull_ret.stdout or "git pull 失败").strip())

        try:
            dependency_result = self._sync_patch_dependencies({}, {"dependency_manifest_path": "runtime_dependency_lock.json"})
        except Exception as dependency_exc:  # noqa: BLE001
            self._set_runtime_and_state(
                last_result="failed",
                last_error=str(dependency_exc),
                dependency_sync_status="failed",
                dependency_sync_error=str(dependency_exc),
                dependency_sync_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            return self._build_result_payload(
                last_result="failed",
                message=f"Git 拉取成功，但运行依赖同步失败: {dependency_exc}",
            )

        refreshed_local = normalize_local_version(load_local_build_meta(self.app_dir))
        refreshed_local_text = refreshed_local.get("display_version") or refreshed_local.get("build_id") or "-"
        refreshed_local_release_revision = int(refreshed_local.get("release_revision", 0) or 0)
        refreshed_git_snapshot = self._sync_git_runtime(fetch_remote=False)
        refreshed_remote_commit = str(refreshed_git_snapshot.get("remote_commit", "") or "").strip()
        final_result = "restart_pending"
        restart_required = True
        dependency_status = "success" if str(dependency_result.get("status", "")).strip() == "success" else "idle"
        final_message = "代码已拉取完成并完成运行依赖同步，请重启程序使新代码生效。"
        self._set_runtime_and_state(
            last_result=final_result,
            last_error="",
            local_version=str(refreshed_local_text),
            remote_version=_short_git_commit(refreshed_remote_commit),
            local_release_revision=refreshed_local_release_revision,
            remote_release_revision=refreshed_local_release_revision,
            update_available=False,
            force_apply_available=False,
            last_updated_at=now_text,
            restart_required=restart_required,
            last_applied_release_revision=refreshed_local_release_revision,
            dependency_sync_status=dependency_status,
            dependency_sync_error="",
            dependency_sync_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        self._log(
            "Git 更新完成: "
            f"结果={self._result_text(final_result)}, branch={branch or self.git_branch}, "
            f"commit={_short_git_commit(str(refreshed_git_snapshot.get('local_commit', '') or '')) or '-'}"
        )
        return self._build_result_payload(
            last_result=final_result,
            message=final_message,
            dependency_result=dependency_result,
        )

    def _fetch_source_manifest(self) -> Dict[str, Any]:
        if not self.shared_bridge_root:
            raise SharedMirrorPendingError("共享目录未配置，无法检查批准源码版本。")
        state = self._load_source_publish_state()
        if not self._source_publish_state_path.exists():
            raise SharedMirrorPendingError("暂无外网端发布的批准源码版本。")
        if not bool(state.get("mirror_ready", False)):
            raise SharedMirrorPendingError("共享目录批准源码版本尚未发布完成。")
        error_text = str(state.get("last_publish_error", "") or "").strip()
        if error_text:
            raise SharedMirrorPendingError(f"共享目录批准源码版本发布异常: {error_text}")
        if not self._source_manifest_path.exists():
            raise SharedMirrorPendingError("共享目录批准源码清单不存在。")
        try:
            payload = json.loads(self._source_manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"读取共享目录批准源码清单失败: {exc}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("共享目录批准源码清单格式错误。")
        if str(payload.get("format", "") or "").strip() != "source_snapshot":
            raise RuntimeError("共享目录批准源码清单不是 source_snapshot 格式。")
        zip_relpath = str(payload.get("zip_relpath", "") or _SOURCE_SNAPSHOT_ZIP_NAME).strip()
        zip_path = self._source_approved_root / Path(zip_relpath).name
        if not zip_path.exists():
            raise SharedMirrorPendingError("共享目录批准源码包尚未就绪。")
        expected_sha = str(payload.get("sha256", payload.get("zip_sha256", "")) or "").strip().lower()
        if expected_sha:
            actual_sha = _sha256_file(zip_path)
            if actual_sha != expected_sha:
                raise RuntimeError("批准版本校验失败。")
        payload["zip_relpath"] = Path(zip_relpath).name
        payload["zip_path"] = str(zip_path)
        return payload

    def _local_source_commit(self) -> str:
        if self.git_available and self.git_repo_detected:
            try:
                snapshot = self._git_tracked_status()
                commit = str(snapshot.get("local_commit", "") or "").strip()
                if commit:
                    return commit
            except Exception:  # noqa: BLE001
                pass
        return str(self.state.get("local_commit", "") or self.runtime.get("local_commit", "") or "").strip()

    def _check_source_approved_once(self, *, apply_update: bool | None) -> Dict[str, Any]:
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        local = normalize_local_version(load_local_build_meta(self.app_dir))
        local_version_text = local.get("display_version") or local.get("build_id") or "-"
        local_release_revision = int(local.get("release_revision", 0) or 0)
        self._sync_mirror_runtime()
        local_commit = self._local_source_commit()
        try:
            manifest = self._fetch_source_manifest()
        except SharedMirrorPendingError as exc:
            self._set_runtime_and_state(
                last_check_at=now_text,
                last_result="mirror_pending_publish",
                last_error="",
                local_version=str(local_version_text),
                remote_version="",
                local_release_revision=local_release_revision,
                remote_release_revision=0,
                local_commit=local_commit,
                update_available=False,
                force_apply_available=False,
                source_kind=self.source_kind,
                source_label=self.source_label,
                update_mode=self.update_mode,
            )
            return self._build_result_payload(last_result="mirror_pending_publish", message=str(exc))

        approved_commit = str(manifest.get("source_commit", "") or "").strip()
        approved_version = str(
            manifest.get("display_version")
            or manifest.get("target_display_version")
            or _short_git_commit(approved_commit)
            or ""
        ).strip()
        approved_revision = int(manifest.get("release_revision", manifest.get("target_release_revision", 0)) or 0)
        update_available = bool(approved_commit and approved_commit != local_commit)
        self._set_runtime_and_state(
            last_check_at=now_text,
            local_version=str(local_version_text),
            remote_version=approved_version or _short_git_commit(approved_commit),
            local_release_revision=local_release_revision,
            remote_release_revision=approved_revision,
            local_commit=local_commit,
            remote_commit=approved_commit,
            approved_commit=approved_commit,
            approved_manifest=manifest,
            update_available=update_available,
            force_apply_available=False,
            source_kind=self.source_kind,
            source_label=self.source_label,
            update_mode=self.update_mode,
        )

        if not apply_update:
            result_key = "update_available" if update_available else "up_to_date"
            if bool(self.state.get("restart_required", False)):
                result_key = "restart_pending"
            self._set_runtime_and_state(last_result=result_key, last_error="")
            return self._build_result_payload(
                last_result=result_key,
                message="检测到共享目录批准源码版本。" if update_available else "当前已是共享目录批准源码版本。",
            )

        if not update_available:
            result_key = "restart_pending" if bool(self.state.get("restart_required", False)) else "up_to_date"
            self._set_runtime_and_state(last_result=result_key, last_error="")
            return self._build_result_payload(last_result=result_key, message="当前已是共享目录批准源码版本。")

        zip_path = Path(str(manifest.get("zip_path", "") or ""))
        if not zip_path.exists():
            raise RuntimeError("共享目录批准源码包不存在。")
        expected_sha = str(manifest.get("sha256", manifest.get("zip_sha256", "")) or "").strip().lower()
        local_zip = self.download_dir / _SOURCE_SNAPSHOT_ZIP_NAME
        self._set_runtime_and_state(last_result="downloading_patch", last_error="")
        shutil.copy2(zip_path, local_zip)
        if expected_sha and _sha256_file(local_zip) != expected_sha:
            local_zip.unlink(missing_ok=True)
            raise RuntimeError("批准版本校验失败。")

        self._set_runtime_and_state(last_result="applying_patch")
        applied = self.applier.apply_source_snapshot_zip(
            zip_path=local_zip,
            backup_root=self.backup_dir,
            max_backups=int(self.cfg["max_backups"]),
        )
        try:
            dependency_result = self._sync_patch_dependencies(applied, {"dependency_manifest_path": "runtime_dependency_lock.json"})
        except Exception as dependency_exc:  # noqa: BLE001
            return self._rollback_after_dependency_failure(
                applied=applied,
                remote={
                    "build_id": approved_version or approved_commit,
                    "display_version": approved_version or _short_git_commit(approved_commit),
                    "release_revision": approved_revision,
                },
                remote_version_text=approved_version or _short_git_commit(approved_commit),
                remote_release_revision=approved_revision,
                error_text=str(dependency_exc),
            )

        refreshed_local = normalize_local_version(load_local_build_meta(self.app_dir))
        refreshed_local_text = (
            str(manifest.get("display_version") or manifest.get("target_display_version") or "").strip()
            or refreshed_local.get("display_version")
            or refreshed_local.get("build_id")
            or "-"
        )
        refreshed_revision = int(
            manifest.get("release_revision", manifest.get("target_release_revision", 0))
            or refreshed_local.get("release_revision", 0)
            or 0
        )
        dependency_status = "success" if str(dependency_result.get("status", "")).strip() == "success" else "idle"
        self._set_runtime_and_state(
            last_result="restart_pending",
            last_error="",
            local_version=str(refreshed_local_text),
            remote_version=approved_version or _short_git_commit(approved_commit),
            local_release_revision=refreshed_revision,
            remote_release_revision=approved_revision,
            local_commit=approved_commit,
            remote_commit=approved_commit,
            approved_commit=approved_commit,
            approved_manifest=manifest,
            update_available=False,
            force_apply_available=False,
            last_updated_at=now_text,
            restart_required=True,
            last_applied_release_revision=refreshed_revision,
            dependency_sync_status=dependency_status,
            dependency_sync_error="",
            dependency_sync_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        self._write_internal_peer_status(online=True, force=True)
        self._log(
            "共享目录批准源码应用完成: "
            f"commit={_short_git_commit(approved_commit)}, replaced={applied.get('replaced', 0)}, "
            f"deleted={applied.get('deleted', 0)}"
        )
        return self._build_result_payload(
            last_result="restart_pending",
            message="共享目录批准源码已应用并完成运行依赖同步，请重启程序使更新生效。",
            applied=applied,
            dependency_result=dependency_result,
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
            if self.update_mode == "git_pull":
                return self._check_git_once(apply_update=apply_update)
            if self.update_mode == _SOURCE_APPROVED_UPDATE_MODE:
                return self._check_source_approved_once(apply_update=apply_update)
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
        command_id = str(queued_apply.get("command_id", "") or "").strip()
        source_commit = str(queued_apply.get("source_commit", "") or "").strip()
        self._log(f"开始处理排队更新: 模式={self._apply_mode_text(mode)}")
        self._clear_queue()
        try:
            result = self._apply_update_and_restart_if_needed(mode=mode, queue_if_busy=False)
            result_key = str(result.get("last_result", "") or "").strip().lower()
            if command_id and self.remote_control_store is not None:
                command = self.remote_control_store.update_command(
                    command_id=command_id,
                    status="completed",
                    message="内网端排队更新已执行完成",
                )
                if source_commit:
                    self._set_runtime_and_state(last_internal_apply_completed_commit=source_commit)
                self._write_internal_peer_status(
                    online=True,
                    command=command if isinstance(command, dict) else None,
                    force=True,
                )
            if result_key == "failed":
                raise RuntimeError(str(result.get("last_error", "") or result.get("message", "") or "排队更新失败"))
        except Exception as exc:  # noqa: BLE001
            if command_id and self.remote_control_store is not None:
                failed = self.remote_control_store.update_command(
                    command_id=command_id,
                    status="failed",
                    message=str(exc),
                )
                if source_commit:
                    self._set_runtime_and_state(last_internal_apply_failed_commit=source_commit)
                self._write_internal_peer_status(
                    online=True,
                    command=failed if isinstance(failed, dict) else None,
                    force=True,
                )
            self._record_failure("排队更新失败", exc)

    def _try_process_internal_peer_command(self) -> None:
        if self.role_mode != "internal" or not self.remote_control_store or not self.shared_bridge_root:
            return
        command = self.remote_control_store.load_command()
        if str(command.get("status", "")).strip().lower() != "pending":
            return
        command_id = str(command.get("command_id", "") or "").strip()
        action = str(command.get("action", "") or "").strip().lower()
        if not command_id:
            return

        accepted = self.remote_control_store.update_command(
            command_id=command_id,
            status="accepted",
            message="内网端已接收命令",
        )
        if not isinstance(accepted, dict):
            return
        self._write_internal_peer_status(online=True, command=accepted, force=True)
        self._log(
            "接收到外网远程更新命令: "
            f"action={action or '-'}, command_id={command_id}"
        )

        running = self.remote_control_store.update_command(
            command_id=command_id,
            status="running",
            message="内网端执行中",
        )
        if isinstance(running, dict):
            self._write_internal_peer_status(online=True, command=running, force=True)
        active_command = running if isinstance(running, dict) else accepted

        try:
            if action == "check":
                result = self.check_now()
            elif action == "apply":
                result = self._apply_update_and_restart_if_needed(
                    mode="normal",
                    queue_if_busy=True,
                    command=active_command,
                )
            elif action == "restart":
                result = self.restart_now()
            else:
                raise RuntimeError(f"不支持的远程命令动作: {action or '-'}")
            result_key = str(result.get("last_result", "") or "").strip().lower()
            queue_status = str(result.get("queue_status", "") or "").strip().lower()
            if result_key == "failed":
                raise RuntimeError(str(result.get("last_error", "") or result.get("message", "") or "远程命令执行失败"))

            if action == "apply" and result_key == "queued_busy":
                queued = self.remote_control_store.update_command(
                    command_id=command_id,
                    status="running",
                    message="已排队，等待当前任务结束后执行",
                )
                self._write_internal_peer_status(
                    online=True,
                    command=queued if isinstance(queued, dict) else active_command,
                    force=True,
                )
                self._log(
                    "内网远程更新命令已排队: "
                    f"action={action or '-'}, command_id={command_id}"
                )
                return
            elif action == "apply" and queue_status == "queued":
                completion_message = "内网端更新命令已排队执行"
            elif action == "apply":
                completion_message = "内网端开始更新命令执行完成"
            elif action == "restart":
                completion_message = "内网端重启生效命令执行完成"
            else:
                completion_message = "内网端检查更新命令执行完成"

            completed = self.remote_control_store.update_command(
                command_id=command_id,
                status="completed",
                message=completion_message,
            )
            command_source_commit = str(active_command.get("source_commit", "") or "").strip()
            if action == "apply" and command_source_commit:
                self._set_runtime_and_state(last_internal_apply_completed_commit=command_source_commit)
            self._write_internal_peer_status(
                online=True,
                command=completed if isinstance(completed, dict) else active_command,
                force=True,
            )
            self._log(
                "内网远程更新命令执行完成: "
                f"action={action or '-'}, command_id={command_id}, result={result_key or '-'}"
            )
        except Exception as exc:  # noqa: BLE001
            command_source_commit = str(active_command.get("source_commit", "") or "").strip()
            if action == "apply" and command_source_commit:
                self._set_runtime_and_state(last_internal_apply_failed_commit=command_source_commit)
            failed = self.remote_control_store.update_command(
                command_id=command_id,
                status="failed",
                message=str(exc),
            )
            self._write_internal_peer_status(
                online=True,
                command=failed if isinstance(failed, dict) else active_command,
                force=True,
            )
            self._log(
                "内网远程更新命令执行失败: "
                f"action={action or '-'}, command_id={command_id}, error={exc}"
            )

    def _loop(self) -> None:
        self._set_runtime(running=True)
        self._safe_sync_mirror_runtime()
        self._safe_sync_shared_mirror_watch_signal()
        if self.update_mode == "git_pull":
            git_snapshot = self._safe_sync_git_runtime(fetch_remote=False)
            self._last_seen_git_head = str(git_snapshot.get("local_commit", "") or "").strip()
        if self.role_mode == "internal":
            self._write_internal_peer_status(online=True, force=True)
        elif self.role_mode == "external":
            self._safe_sync_internal_peer_runtime()
        if self.update_mode == "git_pull":
            self._log("更新线程已启动: 源码直跑模式仅处理手动拉取、排队请求和远程命令，不会自动拉取代码。")
        else:
            self._log(f"更新线程已启动: interval={self.cfg['check_interval_sec']}s")
        next_check_monotonic = time.monotonic() + int(self.cfg["check_interval_sec"])
        next_git_head_check_monotonic = time.monotonic() + _SOURCE_GIT_HEAD_WATCH_INTERVAL_SEC
        while not self._stop.wait(1):
            if self.role_mode == "internal":
                self._write_internal_peer_status(online=True)
                self._try_process_internal_peer_command()
            elif self.role_mode == "external":
                self._safe_sync_internal_peer_runtime()
            self._try_process_queued_apply()
            if self.update_mode == "git_pull":
                if (
                    self.role_mode == "external"
                    and time.monotonic() >= next_git_head_check_monotonic
                ):
                    next_git_head_check_monotonic = time.monotonic() + _SOURCE_GIT_HEAD_WATCH_INTERVAL_SEC
                    git_snapshot = self._safe_sync_git_runtime(fetch_remote=False)
                    current_head = str(git_snapshot.get("local_commit", "") or "").strip()
                    last_published_commit = str(self.state.get("last_published_commit", "") or "").strip()
                    if current_head and (
                        current_head != self._last_seen_git_head
                        or last_published_commit != current_head
                    ):
                        self._last_seen_git_head = current_head
                        self._try_auto_publish_git_head_to_internal()
                continue
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

