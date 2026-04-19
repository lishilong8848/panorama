from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import app.modules.updater.service.updater_service as updater_service_module
from app.modules.updater.service.updater_service import UpdaterService


def _build_config(tmp_path: Path) -> dict:
    return {
        "paths": {
            "runtime_state_root": str(tmp_path / ".runtime"),
        },
        "updater": {
            "enabled": True,
            "auto_apply": False,
            "auto_restart": False,
            "gitee_repo": "https://example.invalid/repo.git",
            "gitee_branch": "master",
            "gitee_manifest_path": "updates/latest_patch.json",
        },
    }


@pytest.fixture(autouse=True)
def _clear_source_run_disable_env(monkeypatch) -> None:
    monkeypatch.delenv(updater_service_module._SOURCE_RUN_DISABLE_UPDATER_ENV, raising=False)


def test_apply_now_queues_when_job_is_busy(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    service = UpdaterService(
        config=_build_config(tmp_path),
        emit_log=lambda _text: None,
        is_busy=lambda: True,
    )

    result = service.apply_now(mode="normal", queue_if_busy=True)

    assert result["last_result"] == "queued_busy"
    assert result["queue_status"] == "queued"
    assert result["queued_apply"]["queued"] is True
    assert result["queued_apply"]["mode"] == "normal"


def test_force_remote_apply_can_override_ahead_of_remote(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    (tmp_path / "build_meta.json").write_text(
        (
            "{\n"
            '  "build_id": "QJPT_V3",\n'
            '  "major_version": 3,\n'
            '  "patch_version": 50,\n'
            '  "release_revision": 50,\n'
            '  "display_version": "V3.50.20260326",\n'
            '  "created_at": "2026-03-26 10:00:00"\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    service = UpdaterService(
        config=_build_config(tmp_path),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )
    service.client.fetch_latest_manifest = lambda: {
        "target_version": "QJPT_V3",
        "major_version": 3,
        "target_patch_version": 49,
        "target_release_revision": 51,
        "target_display_version": "V3.49.20260326",
        "zip_url": "https://example.invalid/QJPT_patch_only.zip",
        "zip_sha256": "",
        "created_at": "2026-03-26 11:00:00",
    }
    service.client.download_patch = lambda zip_url, zip_path, expected_sha256="": zip_path.write_bytes(b"patch")
    service.applier.apply_patch_zip = lambda **_kwargs: {"replaced": 3, "deleted": 1, "backup": str(tmp_path / "backup")}

    result = service.apply_now(mode="force_remote", queue_if_busy=False)

    assert result["last_result"] == "updated"
    assert result["local_release_revision"] == 51
    assert service.get_runtime_snapshot()["restart_required"] is False


def test_check_now_does_not_expose_force_apply_when_already_up_to_date(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    (tmp_path / "build_meta.json").write_text(
        (
            "{\n"
            '  "build_id": "QJPT_V3",\n'
            '  "major_version": 3,\n'
            '  "patch_version": 54,\n'
            '  "release_revision": 54,\n'
            '  "display_version": "V3.54.20260327",\n'
            '  "created_at": "2026-03-27 10:00:00"\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    service = UpdaterService(
        config=_build_config(tmp_path),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )
    service.client.fetch_latest_manifest = lambda: {
        "target_version": "QJPT_V3",
        "major_version": 3,
        "target_patch_version": 54,
        "target_release_revision": 54,
        "target_display_version": "V3.54.20260327",
        "zip_url": "https://example.invalid/QJPT_patch_only.zip",
        "zip_sha256": "",
        "created_at": "2026-03-27 10:00:00",
    }

    result = service.check_now()

    assert result["last_result"] == "up_to_date"
    assert result["force_apply_available"] is False


def test_source_python_run_disables_updater_service(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_DISABLE_UPDATER_ENV, "1")
    service = UpdaterService(
        config=_build_config(tmp_path),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    result = service.check_now()

    assert service.enabled is False
    assert result["enabled"] is False
    assert result["disabled_reason"] == "source_python_run"
    assert result["last_result"] == "disabled"
    assert "本地源码运行" in result["message"]


def test_remote_manifest_prefers_zip_url_over_zip_relpath(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    service = UpdaterService(
        config=_build_config(tmp_path),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    patch_ref = service._resolve_manifest_patch_ref(  # noqa: SLF001
        {
            "zip_relpath": "updates/patches/QJPT_patch_only_p224_r224.zip",
            "zip_url": "https://example.invalid/updates/patches/QJPT_patch_only_p224_r224.zip",
        }
    )

    assert patch_ref == "https://example.invalid/updates/patches/QJPT_patch_only_p224_r224.zip"


def test_qjpt_v3_code_git_mode_blocks_apply_when_worktree_is_dirty(tmp_path: Path, monkeypatch) -> None:
    code_dir = tmp_path / "QJPT_V3_code"
    code_dir.mkdir(parents=True, exist_ok=True)
    (code_dir / ".git").mkdir()
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: code_dir)
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda name: "C:/Git/cmd/git.exe" if name == "git" else None)

    def _fake_git_run(self, *args):  # noqa: ANN001
        key = tuple(args)
        if key == ("rev-parse", "--abbrev-ref", "HEAD"):
            return subprocess.CompletedProcess(["git", *args], 0, stdout="master\n", stderr="")
        if key == ("rev-parse", "HEAD"):
            return subprocess.CompletedProcess(["git", *args], 0, stdout="1111111222222333333444444555555666667777\n", stderr="")
        if key == ("status", "--porcelain", "--untracked-files=no"):
            return subprocess.CompletedProcess(["git", *args], 0, stdout=" M app/bootstrap/app_factory.py\n", stderr="")
        if key == ("rev-parse", "origin/master"):
            return subprocess.CompletedProcess(["git", *args], 0, stdout="9999999aaaaaaabbbbbbbcccccccdddddddeeeeeee\n", stderr="")
        if key == ("fetch", "origin", "master"):
            return subprocess.CompletedProcess(["git", *args], 0, stdout="", stderr="")
        raise AssertionError(f"unexpected git command: {args}")

    monkeypatch.setattr(UpdaterService, "_run_git", _fake_git_run)
    service = UpdaterService(
        config=_build_config(code_dir),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    result = service.apply_now(mode="normal", queue_if_busy=False)

    assert service.enabled is True
    assert result["last_result"] == "dirty_worktree"
    assert result["worktree_dirty"] is True
    assert result["branch"] == "master"
    assert result["local_commit"].startswith("1111111")
    assert result["remote_commit"].startswith("9999999")
    assert "已阻止自动更新" in result["message"]


def test_qjpt_v3_code_git_mode_is_disabled_when_git_repo_missing(tmp_path: Path, monkeypatch) -> None:
    code_dir = tmp_path / "QJPT_V3_code"
    code_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: code_dir)
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda name: "C:/Git/cmd/git.exe" if name == "git" else None)

    service = UpdaterService(
        config=_build_config(code_dir),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )
    result = service.check_now()

    assert service.enabled is False
    assert result["disabled_reason"] == "git_repo_missing"
    assert result["update_mode"] == "git_pull"
    assert "Git 工作区" in result["message"]
