from __future__ import annotations

import hashlib
from pathlib import Path

import app.modules.updater.service.updater_service as updater_service_module
from app.modules.updater.service.manifest_client import SharedMirrorManifestClient
from app.modules.updater.service.updater_service import UpdaterService


def _build_config(tmp_path: Path, *, role_mode: str, shared_root: Path) -> dict:
    return {
        "paths": {
            "runtime_state_root": str(tmp_path / ".runtime"),
        },
        "deployment": {
            "role_mode": role_mode,
            "node_id": f"{role_mode}-node",
        },
        "shared_bridge": {
            "enabled": role_mode in {"internal", "external"},
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


def _write_build_meta(app_dir: Path, *, release_revision: int, display_version: str) -> None:
    app_dir.mkdir(parents=True, exist_ok=True)
    patch_version = release_revision
    app_dir.joinpath("build_meta.json").write_text(
        (
            "{\n"
            '  "build_id": "QJPT_V3",\n'
            '  "major_version": 3,\n'
            f'  "patch_version": {patch_version},\n'
            f'  "release_revision": {release_revision},\n'
            f'  "display_version": "{display_version}",\n'
            '  "created_at": "2026-03-28 10:00:00"\n'
            "}\n"
        ),
        encoding="utf-8",
    )


def _publish_shared_version(shared_root: Path, *, release_revision: int, display_version: str, body: bytes) -> None:
    patch_zip = shared_root.parent / f"QJPT_patch_only_p{release_revision}_r{release_revision}.zip"
    patch_zip.write_bytes(body)
    expected_sha = hashlib.sha256(body).hexdigest()
    client = SharedMirrorManifestClient(shared_root)
    client.publish_approved_update(
        remote_manifest={
            "target_version": "QJPT_V3",
            "major_version": 3,
            "target_display_version": display_version,
            "target_release_revision": release_revision,
            "target_patch_version": release_revision,
            "zip_url": patch_zip.name,
            "zip_sha256": expected_sha,
        },
        patch_zip=patch_zip,
        expected_sha256=expected_sha,
        published_by_role="external",
        published_by_node_id="external-node",
        approved_local_version=display_version,
        approved_release_revision=release_revision,
    )


def test_internal_shared_mirror_pending_publish(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir, release_revision=54, display_version="V3.54.20260328")
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)

    service = UpdaterService(
        config=_build_config(tmp_path, role_mode="internal", shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    result = service.check_now()

    assert result["last_result"] == "mirror_pending_publish"
    assert result["source_kind"] == "shared_mirror"
    assert result["mirror_ready"] is False


def test_external_up_to_date_publishes_shared_mirror(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir, release_revision=60, display_version="V3.60.20260328")
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)

    service = UpdaterService(
        config=_build_config(tmp_path, role_mode="external", shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    body = b"approved-external-patch"
    expected_sha = hashlib.sha256(body).hexdigest()
    service.client.fetch_latest_manifest = lambda: {
        "target_version": "QJPT_V3",
        "major_version": 3,
        "target_display_version": "V3.60.20260328",
        "target_release_revision": 60,
        "target_patch_version": 60,
        "zip_url": "https://example.invalid/QJPT_patch_only_p60_r60.zip",
        "zip_sha256": expected_sha,
        "created_at": "2026-03-28 10:30:00",
    }
    service.client.download_patch = lambda zip_url, zip_path, expected_sha256="": zip_path.write_bytes(body)

    result = service.check_now()

    assert result["last_result"] == "up_to_date"
    assert service.get_runtime_snapshot()["mirror_ready"] is True
    assert (shared_root / "updater" / "approved" / "latest_patch.json").exists()


def test_internal_apply_now_uses_shared_mirror(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir, release_revision=60, display_version="V3.60.20260328")
    _publish_shared_version(shared_root, release_revision=61, display_version="V3.61.20260328", body=b"internal-mirror-patch")
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)

    service = UpdaterService(
        config=_build_config(tmp_path, role_mode="internal", shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )
    service.applier.apply_patch_zip = lambda **_kwargs: {
        "replaced": 5,
        "deleted": 0,
        "backup": str(tmp_path / "backup"),
        "patch_meta": {},
    }

    result = service.apply_now(mode="normal", queue_if_busy=False)

    assert result["last_result"] == "updated"
    assert result["source_kind"] == "shared_mirror"
    assert result["local_release_revision"] == 61
    assert service.get_runtime_snapshot()["mirror_ready"] is True


def test_internal_ahead_of_shared_mirror_does_not_downgrade(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir, release_revision=62, display_version="V3.62.20260328")
    _publish_shared_version(shared_root, release_revision=61, display_version="V3.61.20260328", body=b"older-mirror-patch")
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)

    service = UpdaterService(
        config=_build_config(tmp_path, role_mode="internal", shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    result = service.check_now()

    assert result["last_result"] == "ahead_of_mirror"
    assert "不会自动回退" in result["message"]


def test_external_publish_then_internal_auto_follow_end_to_end(tmp_path: Path, monkeypatch) -> None:
    shared_root = tmp_path / "shared"
    external_app_dir = tmp_path / "external-app"
    internal_app_dir = tmp_path / "internal-app"
    body = b"approved-shared-mirror-patch"
    expected_sha = hashlib.sha256(body).hexdigest()

    _write_build_meta(external_app_dir, release_revision=69, display_version="V3.69.20260328")
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: external_app_dir)
    external_service = UpdaterService(
        config=_build_config(tmp_path / "ext", role_mode="external", shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )
    external_service.client.fetch_latest_manifest = lambda: {
        "target_version": "QJPT_V3",
        "major_version": 3,
        "target_display_version": "V3.70.20260328",
        "target_release_revision": 70,
        "target_patch_version": 70,
        "zip_url": "https://example.invalid/QJPT_patch_only_p70_r70.zip",
        "zip_sha256": expected_sha,
        "created_at": "2026-03-28 16:20:00",
    }
    external_service.client.download_patch = lambda zip_url, zip_path, expected_sha256="": zip_path.write_bytes(body)
    external_service.applier.apply_patch_zip = lambda **_kwargs: {
        "replaced": 8,
        "deleted": 0,
        "backup": str(tmp_path / "external-backup"),
        "patch_meta": {},
    }

    external_result = external_service.apply_now(mode="normal", queue_if_busy=False)

    approved_manifest = shared_root / "updater" / "approved" / "latest_patch.json"
    assert external_result["last_result"] == "updated"
    assert approved_manifest.exists()
    assert external_service.get_runtime_snapshot()["mirror_ready"] is True
    assert external_service.get_runtime_snapshot()["mirror_version"] == "V3.70.20260328"

    _write_build_meta(internal_app_dir, release_revision=69, display_version="V3.69.20260328")
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: internal_app_dir)
    internal_config = _build_config(tmp_path / "int", role_mode="internal", shared_root=shared_root)
    internal_config["updater"]["auto_apply"] = True
    internal_service = UpdaterService(
        config=internal_config,
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )
    internal_service.applier.apply_patch_zip = lambda **_kwargs: {
        "replaced": 8,
        "deleted": 0,
        "backup": str(tmp_path / "internal-backup"),
        "patch_meta": {},
    }

    internal_result = internal_service.check_now()

    assert internal_result["last_result"] == "updated"
    assert internal_result["source_kind"] == "shared_mirror"
    assert internal_result["mirror_ready"] is True
    assert internal_result["mirror_version"] == "V3.70.20260328"
    assert internal_result["local_release_revision"] == 70
    assert internal_service.get_runtime_snapshot()["local_version"] == "V3.70.20260328"


def test_internal_loop_triggers_immediate_check_when_shared_mirror_changes(tmp_path: Path, monkeypatch) -> None:
    app_dir = tmp_path / "internal-app"
    shared_root = tmp_path / "shared"
    _write_build_meta(app_dir, release_revision=69, display_version="V3.69.20260328")
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: app_dir)

    service = UpdaterService(
        config=_build_config(tmp_path / "int", role_mode="internal", shared_root=shared_root),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )
    service._sync_shared_mirror_watch_signal()
    run_check_calls: list[dict[str, object]] = []

    def _fake_run_check(*, apply_update, force_remote):  # noqa: ANN001
        run_check_calls.append(
            {
                "apply_update": apply_update,
                "force_remote": force_remote,
            }
        )
        return {"last_result": "updated"}

    class _StopAfterPublish:
        def __init__(self) -> None:
            self.calls = 0

        def wait(self, _timeout: int) -> bool:
            self.calls += 1
            if self.calls == 1:
                _publish_shared_version(
                    shared_root,
                    release_revision=70,
                    display_version="V3.70.20260328",
                    body=b"approved-after-loop-start",
                )
                return False
            return True

    service._stop = _StopAfterPublish()
    service._run_check = _fake_run_check  # type: ignore[method-assign]

    service._loop()

    assert run_check_calls == [{"apply_update": None, "force_remote": False}]
