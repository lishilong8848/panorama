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


def test_internal_peer_pending_apply_command_completed_when_queued(tmp_path: Path, monkeypatch) -> None:
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
        command_id="cmd-apply",
        action="apply",
        requested_by_node_id="external-node",
        requested_by_role="external",
    )
    service.apply_now = lambda **_kwargs: {"last_result": "queued_busy", "queue_status": "queued"}  # type: ignore[method-assign]

    service._try_process_internal_peer_command()

    command = service.remote_control_store.load_command()
    assert command["command_id"] == "cmd-apply"
    assert command["status"] == "completed"
    assert "已加入内网端更新队列" in command["message"]
