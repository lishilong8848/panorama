from __future__ import annotations

from pathlib import Path

import app.modules.updater.service.updater_service as updater_service_module
from app.modules.updater.service.updater_service import UpdaterService


def _build_config(tmp_path: Path, *, shared_root: Path) -> dict:
    return {
        "paths": {
            "runtime_state_root": str(tmp_path / ".runtime"),
        },
        "deployment": {
            "role_mode": "internal",
            "node_id": "internal-node",
        },
        "shared_bridge": {
            "enabled": True,
            "root_dir": str(shared_root),
        },
        "updater": {
            "enabled": True,
            "auto_apply": False,
            "auto_restart": False,
            "gitee_repo": "https://example.invalid/repo.git",
            "gitee_branch": "master",
            "gitee_manifest_path": "updates/latest_patch.json",
            "download_retry_count": 1,
            "request_timeout_sec": 5,
        },
    }


def _write_build_meta(app_dir: Path) -> None:
    app_dir.mkdir(parents=True, exist_ok=True)
    app_dir.joinpath("build_meta.json").write_text(
        (
            "{\n"
            '  "build_id": "QJPT_V3",\n'
            '  "major_version": 3,\n'
            '  "patch_version": 1,\n'
            '  "release_revision": 1,\n'
            '  "display_version": "V3.1.20260411",\n'
            '  "created_at": "2026-04-11 00:00:00"\n'
            "}\n"
        ),
        encoding="utf-8",
    )


def test_internal_peer_pending_check_command_consumed_and_completed(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir)
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    service = UpdaterService(
        config=_build_config(tmp_path, shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    service.remote_control_store.submit_command(
        command_id="cmd-check",
        action="check",
        requested_by_node_id="external-node",
        requested_by_role="external",
    )
    service.check_now = lambda: {"last_result": "up_to_date", "queue_status": "none"}  # type: ignore[method-assign]

    service._try_process_internal_peer_command()

    command = service.remote_control_store.load_command()
    assert command["command_id"] == "cmd-check"
    assert command["status"] == "completed"
    assert "检查更新命令执行完成" in command["message"]


def test_internal_peer_pending_apply_command_stays_running_when_queued(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir)
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    service = UpdaterService(
        config=_build_config(tmp_path, shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: True,
    )

    service.remote_control_store.submit_command(
        command_id="cmd-apply",
        action="apply",
        requested_by_node_id="external-node",
        requested_by_role="external",
        source_commit="queued123",
    )

    service._try_process_internal_peer_command()

    command = service.remote_control_store.load_command()
    assert command["command_id"] == "cmd-apply"
    assert command["status"] == "running"
    assert "已排队" in command["message"]
    queued_apply = service.get_runtime_snapshot()["queued_apply"]
    assert queued_apply["queued"] is True
    assert queued_apply["command_id"] == "cmd-apply"
    assert queued_apply["source_commit"] == "queued123"


def test_internal_peer_apply_command_restarts_when_apply_requires_restart(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir)
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    restart_calls: list[str] = []
    service = UpdaterService(
        config=_build_config(tmp_path, shared_root=shared_root),
        emit_log=lambda _text: None,
        restart_callback=lambda _context: (restart_calls.append("restart") or (True, "restart scheduled")),
        is_busy=lambda: False,
    )

    service.remote_control_store.submit_command(
        command_id="cmd-apply-restart",
        action="apply",
        requested_by_node_id="external-node",
        requested_by_role="external",
    )
    service._apply_update_and_restart_if_needed = lambda **_kwargs: service.restart_now()  # type: ignore[method-assign]
    service.restart_now = lambda: (restart_calls.append("restart_now") or {"last_result": "updated_restart_scheduled", "queue_status": "none"})  # type: ignore[method-assign]

    service._try_process_internal_peer_command()

    command = service.remote_control_store.load_command()
    assert command["command_id"] == "cmd-apply-restart"
    assert command["status"] == "completed"
    assert "开始更新命令执行完成" in command["message"]
    assert restart_calls == ["restart_now"]


def test_queued_remote_apply_restarts_and_marks_command_completed(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir)
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    restart_calls: list[str] = []
    service = UpdaterService(
        config=_build_config(tmp_path, shared_root=shared_root),
        emit_log=lambda _text: None,
        restart_callback=lambda _context: (restart_calls.append("restart") or (True, "restart scheduled")),
        is_busy=lambda: False,
    )
    service.remote_control_store.submit_command(
        command_id="cmd-queued-apply",
        action="apply",
        requested_by_node_id="external-node",
        requested_by_role="external",
        source_commit="queued-commit",
    )
    service.state["queued_apply"] = {
        "queued": True,
        "mode": "normal",
        "queued_at": "2026-04-21 12:30:00",
        "reason": "active_job_running",
        "command_id": "cmd-queued-apply",
        "source_commit": "queued-commit",
    }
    service._run_check = lambda **_kwargs: {"last_result": "restart_pending", "restart_required": True}  # type: ignore[method-assign]
    service.restart_now = lambda: (restart_calls.append("restart_now") or {"last_result": "updated_restart_scheduled", "queue_status": "none"})  # type: ignore[method-assign]

    service._try_process_queued_apply()

    command = service.remote_control_store.load_command()
    assert command["status"] == "completed"
    assert "排队更新已执行完成" in command["message"]
    assert service.get_runtime_snapshot()["last_internal_apply_completed_commit"] == "queued-commit"
    assert restart_calls == ["restart_now"]


def test_external_runtime_snapshot_does_not_sync_internal_peer_inline(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir)
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    (app_dir / ".git").mkdir()
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: "C:/Git/bin/git.exe")
    monkeypatch.setattr(
        updater_service_module.UpdaterService,
        "_detect_git_identity",
        lambda self, branch_hint="": {"branch": "master", "remote_name": "origin", "remote_url": "https://example.invalid/repo.git"},
    )
    service = UpdaterService(
        config={
            "paths": {"runtime_state_root": str(tmp_path / ".runtime")},
            "deployment": {"role_mode": "external", "node_id": "external-node"},
            "shared_bridge": {"enabled": True, "root_dir": str(shared_root)},
            "updater": {"enabled": True},
        },
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    monkeypatch.setattr(service, "_sync_internal_peer_runtime", lambda: (_ for _ in ()).throw(AssertionError("should not sync inline")))

    snapshot = service.get_runtime_snapshot()

    assert snapshot["running"] is False


def test_external_stop_does_not_sync_internal_peer_inline(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir)
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    (app_dir / ".git").mkdir()
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: "C:/Git/bin/git.exe")
    monkeypatch.setattr(
        updater_service_module.UpdaterService,
        "_detect_git_identity",
        lambda self, branch_hint="": {"branch": "master", "remote_name": "origin", "remote_url": "https://example.invalid/repo.git"},
    )
    service = UpdaterService(
        config={
            "paths": {"runtime_state_root": str(tmp_path / ".runtime")},
            "deployment": {"role_mode": "external", "node_id": "external-node"},
            "shared_bridge": {"enabled": True, "root_dir": str(shared_root)},
            "updater": {"enabled": True},
        },
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    monkeypatch.setattr(service, "_sync_internal_peer_runtime", lambda: (_ for _ in ()).throw(AssertionError("should not sync inline")))

    result = service.stop()

    assert result["stopped"] is True
