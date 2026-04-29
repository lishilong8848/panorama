from __future__ import annotations
from pathlib import Path
import hashlib
import json
import zipfile

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
    monkeypatch.delenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, raising=False)


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


def test_source_python_run_uses_manual_git_pull_mode(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    (tmp_path / ".git").mkdir()
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: "C:/Git/bin/git.exe")
    responses = {
        ("rev-parse", "--abbrev-ref", "HEAD"): type("R", (), {"returncode": 0, "stdout": "master\n", "stderr": ""})(),
        ("config", "--get", "branch.master.remote"): type("R", (), {"returncode": 0, "stdout": "origin\n", "stderr": ""})(),
        ("remote", "get-url", "origin"): type("R", (), {"returncode": 0, "stdout": "https://example.invalid/repo.git\n", "stderr": ""})(),
        ("rev-parse", "HEAD"): type("R", (), {"returncode": 0, "stdout": "abcdef123456\n", "stderr": ""})(),
        ("status", "--porcelain", "--untracked-files=no"): type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
        ("rev-parse", "origin/master"): type("R", (), {"returncode": 0, "stdout": "abcdef123456\n", "stderr": ""})(),
        ("fetch", "origin", "master"): type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
    }

    def fake_run_git(self, *args):  # noqa: ANN001
        return responses[tuple(args)]

    monkeypatch.setattr(UpdaterService, "_run_git", fake_run_git, raising=False)
    service = UpdaterService(
        config=_build_config(tmp_path),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    result = service.check_now()

    assert service.enabled is True
    assert service.update_mode == "git_pull"
    assert service.source_kind == "git_remote"
    assert service.source_label == "Git 仓库更新源"
    assert result["enabled"] is True
    assert result["disabled_reason"] == ""
    assert result["last_result"] == "up_to_date"
    assert "最新提交" in result["message"]


def test_git_pull_init_does_not_touch_shared_snapshot_paths(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    (tmp_path / ".git").mkdir()
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: "C:/Git/bin/git.exe")
    monkeypatch.setattr(
        UpdaterService,
        "_mirror_runtime_snapshot",
        lambda self: (_ for _ in ()).throw(AssertionError("constructor should not load shared mirror snapshot")),
        raising=False,
    )
    monkeypatch.setattr(
        UpdaterService,
        "_internal_peer_runtime_snapshot",
        lambda self: (_ for _ in ()).throw(AssertionError("constructor should not load internal peer snapshot")),
        raising=False,
    )

    service = UpdaterService(
        config=_build_role_config(tmp_path, role_mode="external", shared_root=tmp_path / "shared"),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    snapshot = dict(service.runtime)
    assert service.update_mode == "git_pull"
    assert snapshot["mirror_ready"] is False
    assert snapshot["internal_peer"]["available"] is True


def test_git_pull_start_does_not_block_on_shared_snapshot_refresh(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    (tmp_path / ".git").mkdir()
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: "C:/Git/bin/git.exe")

    service = UpdaterService(
        config=_build_role_config(tmp_path, role_mode="external", shared_root=tmp_path / "shared"),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    monkeypatch.setattr(service, "_sync_mirror_runtime", lambda: (_ for _ in ()).throw(RuntimeError("shared mirror unavailable")))
    monkeypatch.setattr(service, "_sync_shared_mirror_watch_signal", lambda: (_ for _ in ()).throw(RuntimeError("watch unavailable")))
    monkeypatch.setattr(service, "_sync_git_runtime", lambda fetch_remote=False: {})

    result = service.start()
    if service._thread and service._thread.is_alive():
        service._thread.join(timeout=1)

    assert result["started"] is True
    assert result["running"] is True


def test_git_dirty_worktree_ignores_user_mutable_tracked_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    (tmp_path / ".git").mkdir()
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: "C:/Git/bin/git.exe")
    responses = {
        ("rev-parse", "--abbrev-ref", "HEAD"): type("R", (), {"returncode": 0, "stdout": "master\n", "stderr": ""})(),
        ("config", "--get", "branch.master.remote"): type("R", (), {"returncode": 0, "stdout": "origin\n", "stderr": ""})(),
        ("remote", "get-url", "origin"): type("R", (), {"returncode": 0, "stdout": "https://example.invalid/repo.git\n", "stderr": ""})(),
        ("rev-parse", "HEAD"): type("R", (), {"returncode": 0, "stdout": "abcdef123456\n", "stderr": ""})(),
        ("status", "--porcelain", "--untracked-files=no"): type(
            "R",
            (),
            {
                "returncode": 0,
                "stdout": " M 表格计算配置.json\n M config_segments/handover/common.json\n",
                "stderr": "",
            },
        )(),
        ("rev-parse", "origin/master"): type("R", (), {"returncode": 0, "stdout": "9999999aaaaaaa\n", "stderr": ""})(),
        ("fetch", "origin", "master"): type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
    }


def _build_role_config(tmp_path: Path, *, role_mode: str, shared_root: Path) -> dict:
    cfg = _build_config(tmp_path)
    cfg.update(
        {
            "deployment": {"role_mode": role_mode},
            "shared_bridge": {
                "enabled": True,
                "root_dir": str(shared_root),
            },
        }
    )
    return cfg


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest().lower()

    def fake_run_git(self, *args):  # noqa: ANN001
        return responses[tuple(args)]

    monkeypatch.setattr(UpdaterService, "_run_git", fake_run_git, raising=False)
    service = UpdaterService(
        config=_build_config(tmp_path),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    snapshot = service.check_now()

    assert snapshot["last_result"] == "update_available"
    assert snapshot["worktree_dirty"] is False
    assert snapshot["dirty_files"] == []


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


def test_internal_source_run_uses_shared_approved_source_without_git(tmp_path: Path, monkeypatch) -> None:
    shared_root = tmp_path / "share"
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path / "app")
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: None)
    (tmp_path / "app").mkdir()

    service = UpdaterService(
        config=_build_role_config(tmp_path, role_mode="internal", shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )
    result = service.check_now()

    assert service.update_mode == updater_service_module._SOURCE_APPROVED_UPDATE_MODE
    assert service.source_kind == updater_service_module._SOURCE_APPROVED_SOURCE_KIND
    assert result["last_result"] == "mirror_pending_publish"
    assert result["enabled"] is True


def test_external_publish_approved_source_snapshot_excludes_user_config(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "share"
    app_dir.mkdir()
    (app_dir / ".git").mkdir()
    (app_dir / "main.py").write_text("print('ok')\n", encoding="utf-8")
    (app_dir / "表格计算配置.json").write_text('{"secret": "keep"}', encoding="utf-8")
    (app_dir / "config_segments").mkdir()
    (app_dir / "config_segments" / "handover.json").write_text('{"keep": true}', encoding="utf-8")
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: "C:/Git/bin/git.exe")

    responses = {
        ("rev-parse", "--abbrev-ref", "HEAD"): type("R", (), {"returncode": 0, "stdout": "master\n", "stderr": ""})(),
        ("config", "--get", "branch.master.remote"): type("R", (), {"returncode": 0, "stdout": "origin\n", "stderr": ""})(),
        ("remote", "get-url", "origin"): type("R", (), {"returncode": 0, "stdout": "https://example.invalid/repo.git\n", "stderr": ""})(),
        ("rev-parse", "HEAD"): type("R", (), {"returncode": 0, "stdout": "abcdef123456\n", "stderr": ""})(),
        ("status", "--porcelain", "--untracked-files=no"): type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
        ("rev-parse", "origin/master"): type("R", (), {"returncode": 0, "stdout": "abcdef123456\n", "stderr": ""})(),
        ("ls-files", "*.py"): type("R", (), {"returncode": 0, "stdout": "main.py\n", "stderr": ""})(),
    }

    def fake_run_git(self, *args):  # noqa: ANN001
        return responses[tuple(args)]

    monkeypatch.setattr(UpdaterService, "_run_git", fake_run_git, raising=False)
    service = UpdaterService(
        config=_build_role_config(tmp_path, role_mode="external", shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    result = service.publish_approved_source_snapshot()
    zip_path = Path(result["zip_path"])

    assert result["published"] is True
    assert (shared_root / "updater" / "approved" / "source_manifest.json").exists()
    with zipfile.ZipFile(zip_path, "r") as archive:
        names = set(archive.namelist())
        embedded_manifest = json.loads(archive.read("source_manifest.json").decode("utf-8"))
    assert "main.py" in names
    assert "表格计算配置.json" not in names
    assert "config_segments/handover.json" not in names
    assert embedded_manifest["scope"] == updater_service_module._SOURCE_SNAPSHOT_SCOPE_PY_ONLY
    assert embedded_manifest["files"][0]["path"] == "main.py"


def test_external_auto_publish_git_head_syncs_py_only_and_restarts(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "share"
    app_dir.mkdir()
    (app_dir / ".git").mkdir()
    (app_dir / "main.py").write_text("print('new')\n", encoding="utf-8")
    (app_dir / "untracked.py").write_text("print('skip')\n", encoding="utf-8")
    (app_dir / "readme.md").write_text("skip\n", encoding="utf-8")
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: "C:/Git/bin/git.exe")

    responses = {
        ("rev-parse", "--abbrev-ref", "HEAD"): type("R", (), {"returncode": 0, "stdout": "master\n", "stderr": ""})(),
        ("config", "--get", "branch.master.remote"): type("R", (), {"returncode": 0, "stdout": "origin\n", "stderr": ""})(),
        ("remote", "get-url", "origin"): type("R", (), {"returncode": 0, "stdout": "https://example.invalid/repo.git\n", "stderr": ""})(),
        ("rev-parse", "HEAD"): type("R", (), {"returncode": 0, "stdout": "1234567890abcdef\n", "stderr": ""})(),
        ("status", "--porcelain", "--untracked-files=no"): type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
        ("rev-parse", "origin/master"): type("R", (), {"returncode": 0, "stdout": "1234567890abcdef\n", "stderr": ""})(),
        ("ls-files", "*.py"): type("R", (), {"returncode": 0, "stdout": "main.py\n", "stderr": ""})(),
    }

    def fake_run_git(self, *args):  # noqa: ANN001
        return responses[tuple(args)]

    restart_calls: list[dict] = []
    monkeypatch.setattr(UpdaterService, "_run_git", fake_run_git, raising=False)
    service = UpdaterService(
        config=_build_role_config(tmp_path, role_mode="external", shared_root=shared_root),
        emit_log=lambda _text: None,
        restart_callback=lambda context: (restart_calls.append(dict(context)) or (True, "restart scheduled")),
        is_busy=lambda: False,
    )

    result = service._auto_publish_git_head_to_internal()

    assert result["accepted"] is True
    assert result["source_commit"] == "1234567890abcdef"
    assert restart_calls and restart_calls[0]["reason"] == "source_git_head_synced"
    command = service.remote_control_store.load_command()
    assert command["action"] == "apply"
    assert command["status"] == "pending"
    assert command["source_commit"] == "1234567890abcdef"
    assert service.get_runtime_snapshot()["last_published_commit"] == "1234567890abcdef"
    with zipfile.ZipFile(shared_root / "updater" / "approved" / "source_snapshot.zip", "r") as archive:
        names = set(archive.namelist())
    assert "main.py" in names
    assert "untracked.py" not in names
    assert "readme.md" not in names


def test_external_auto_publish_does_not_publish_or_mark_commit_done_when_internal_command_pending(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "share"
    app_dir.mkdir()
    (app_dir / ".git").mkdir()
    (app_dir / "main.py").write_text("print('new')\n", encoding="utf-8")
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: "C:/Git/bin/git.exe")

    responses = {
        ("rev-parse", "--abbrev-ref", "HEAD"): type("R", (), {"returncode": 0, "stdout": "master\n", "stderr": ""})(),
        ("config", "--get", "branch.master.remote"): type("R", (), {"returncode": 0, "stdout": "origin\n", "stderr": ""})(),
        ("remote", "get-url", "origin"): type("R", (), {"returncode": 0, "stdout": "https://example.invalid/repo.git\n", "stderr": ""})(),
        ("rev-parse", "HEAD"): type("R", (), {"returncode": 0, "stdout": "pending123456\n", "stderr": ""})(),
        ("status", "--porcelain", "--untracked-files=no"): type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})(),
        ("rev-parse", "origin/master"): type("R", (), {"returncode": 0, "stdout": "pending123456\n", "stderr": ""})(),
        ("ls-files", "*.py"): type("R", (), {"returncode": 0, "stdout": "main.py\n", "stderr": ""})(),
    }

    def fake_run_git(self, *args):  # noqa: ANN001
        return responses[tuple(args)]

    monkeypatch.setattr(UpdaterService, "_run_git", fake_run_git, raising=False)
    service = UpdaterService(
        config=_build_role_config(tmp_path, role_mode="external", shared_root=shared_root),
        emit_log=lambda _text: None,
        restart_callback=lambda _context: (True, "restart scheduled"),
        is_busy=lambda: False,
    )
    service.remote_control_store.submit_command(
        command_id="existing-command",
        action="check",
        requested_by_node_id="external-node",
        requested_by_role="external",
    )
    approved_root = shared_root / "updater" / "approved"
    approved_root.mkdir(parents=True, exist_ok=True)
    snapshot_zip = approved_root / "source_snapshot.zip"
    snapshot_zip.write_bytes(b"old-package")
    old_mtime = snapshot_zip.stat().st_mtime_ns

    result = service._auto_publish_git_head_to_internal()

    assert result["accepted"] is False
    assert result["reason"] == "internal_command_active"
    assert service.get_runtime_snapshot().get("last_published_commit", "") == ""
    assert service.get_runtime_snapshot()["last_publish_deferred_commit"] == "pending123456"
    assert snapshot_zip.read_bytes() == b"old-package"
    assert snapshot_zip.stat().st_mtime_ns == old_mtime


def test_internal_apply_py_only_source_snapshot_only_deletes_py_files(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "share"
    approved_root = shared_root / "updater" / "approved"
    approved_root.mkdir(parents=True)
    app_dir.mkdir()
    (app_dir / "keep.txt").write_text("keep\n", encoding="utf-8")
    (app_dir / "old.py").write_text("old\n", encoding="utf-8")
    (app_dir / "main.py").write_text("old-main\n", encoding="utf-8")
    manifest = {
        "format": "source_snapshot",
        "scope": updater_service_module._SOURCE_SNAPSHOT_SCOPE_PY_ONLY,
        "source_commit": "pyonly123",
        "branch": "master",
        "created_at": "2026-04-21 12:00:00",
        "zip_relpath": "source_snapshot.zip",
        "display_version": "pyonly",
        "release_revision": 1000,
        "files": [{"path": "main.py"}],
    }
    zip_path = approved_root / "source_snapshot.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("source_manifest.json", json.dumps(manifest, ensure_ascii=False))
        archive.writestr("main.py", "new-main\n")
        archive.writestr("keep.txt", "should-not-write\n")
    manifest["sha256"] = _sha256(zip_path)
    manifest["zip_size"] = zip_path.stat().st_size
    (approved_root / "source_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    (approved_root / "source_publish_state.json").write_text(
        json.dumps(
            {
                "mirror_ready": True,
                "mirror_version": "pyonly",
                "mirror_release_revision": 1000,
                "last_publish_at": "2026-04-21 12:00:00",
                "last_publish_error": "",
                "zip_relpath": "source_snapshot.zip",
                "approved_commit": "pyonly123",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: None)
    service = UpdaterService(
        config=_build_role_config(tmp_path, role_mode="internal", shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    result = service.apply_now(mode="normal", queue_if_busy=False)

    assert result["last_result"] == "restart_pending"
    assert (app_dir / "main.py").read_text(encoding="utf-8") == "new-main\n"
    assert not (app_dir / "old.py").exists()
    assert (app_dir / "keep.txt").read_text(encoding="utf-8") == "keep\n"


def test_internal_apply_approved_source_snapshot_preserves_user_config(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "share"
    approved_root = shared_root / "updater" / "approved"
    approved_root.mkdir(parents=True)
    app_dir.mkdir()
    (app_dir / "old.py").write_text("old\n", encoding="utf-8")
    (app_dir / "表格计算配置.json").write_text('{"secret": "keep"}', encoding="utf-8")
    manifest = {
        "format": "source_snapshot",
        "source_commit": "fedcba987654",
        "branch": "master",
        "created_at": "2026-04-19 12:00:00",
        "zip_relpath": "source_snapshot.zip",
        "display_version": "V3.test",
        "release_revision": 999,
    }
    zip_path = approved_root / "source_snapshot.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("source_manifest.json", json.dumps(manifest, ensure_ascii=False))
        archive.writestr("new.py", "new\n")
        archive.writestr("表格计算配置.json", '{"secret": "overwrite"}')
    manifest["sha256"] = _sha256(zip_path)
    manifest["zip_size"] = zip_path.stat().st_size
    (approved_root / "source_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    (approved_root / "source_publish_state.json").write_text(
        json.dumps(
            {
                "mirror_ready": True,
                "mirror_version": "V3.test",
                "mirror_release_revision": 999,
                "last_publish_at": "2026-04-19 12:00:00",
                "last_publish_error": "",
                "zip_relpath": "source_snapshot.zip",
                "approved_commit": "fedcba987654",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)
    monkeypatch.setenv(updater_service_module._SOURCE_RUN_GIT_PULL_ENV, "1")
    monkeypatch.setattr(updater_service_module.shutil, "which", lambda _name: None)
    service = UpdaterService(
        config=_build_role_config(tmp_path, role_mode="internal", shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    result = service.apply_now(mode="normal", queue_if_busy=False)

    assert result["last_result"] == "restart_pending"
    assert (app_dir / "new.py").exists()
    assert not (app_dir / "old.py").exists()
    assert json.loads((app_dir / "表格计算配置.json").read_text(encoding="utf-8"))["secret"] == "keep"
    assert service.get_runtime_snapshot()["restart_required"] is True
