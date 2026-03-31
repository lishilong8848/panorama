from __future__ import annotations

from pathlib import Path

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


def test_dependency_failure_rolls_back_patch(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    build_meta_path = tmp_path / "build_meta.json"
    build_meta_path.write_text(
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
        "target_patch_version": 51,
        "target_release_revision": 51,
        "target_display_version": "V3.51.20260326",
        "zip_url": "https://example.invalid/QJPT_patch_only.zip",
        "zip_sha256": "",
        "created_at": "2026-03-26 11:00:00",
    }
    service.client.download_patch = lambda zip_url, zip_path, expected_sha256="": zip_path.write_bytes(b"patch")
    service.applier.apply_patch_zip = lambda **_kwargs: {
        "replaced": 3,
        "deleted": 1,
        "backup": str(tmp_path / "backup"),
        "patch_meta": {
            "required_packages": [
                {"package": "fastapi", "version": "0.116.0", "import_name": "fastapi"},
            ]
        },
    }
    service.applier.restore_backup_snapshot = lambda backup_path: {
        "restored": 4,
        "removed_created": 1,
        "backup": str(backup_path),
    }
    service.dependency_sync_service.sync_required_packages = lambda _packages, *, exact_versions: (_ for _ in ()).throw(
        RuntimeError("pip install failed")
    )

    result = service.apply_now(mode="normal", queue_if_busy=False)

    assert result["ok"] is False
    assert result["last_result"] == "failed"
    assert result["dependency_sync_status"] == "rolled_back"
    assert "已自动回滚到旧版本" in result["message"]
