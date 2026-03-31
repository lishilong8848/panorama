from __future__ import annotations

import zipfile
from pathlib import Path

from app.modules.updater.service.update_applier import UpdateApplier


def test_update_applier_uses_runtime_temp_and_cleans_up(tmp_path: Path) -> None:
    app_dir = tmp_path / "app"
    app_dir.mkdir(parents=True, exist_ok=True)
    runtime_root = tmp_path / ".runtime"
    patch_zip = tmp_path / "patch.zip"
    backup_root = tmp_path / "backups"

    with zipfile.ZipFile(patch_zip, "w") as archive:
        archive.writestr("foo.txt", "patched")

    applier = UpdateApplier(
        app_dir=app_dir,
        runtime_state_root=str(runtime_root),
        emit_log=lambda _message: None,
    )

    result = applier.apply_patch_zip(
        zip_path=patch_zip,
        backup_root=backup_root,
        max_backups=3,
    )

    assert (app_dir / "foo.txt").read_text(encoding="utf-8") == "patched"
    assert Path(str(result["backup"])).exists()

    temp_kind_root = runtime_root / "temp" / "updater_patch_apply"
    if temp_kind_root.exists():
        assert not any(temp_kind_root.iterdir())


def test_update_applier_can_restore_backup_snapshot(tmp_path: Path) -> None:
    app_dir = tmp_path / "app"
    app_dir.mkdir(parents=True, exist_ok=True)
    runtime_root = tmp_path / ".runtime"
    patch_zip = tmp_path / "patch.zip"
    backup_root = tmp_path / "backups"

    original_file = app_dir / "foo.txt"
    original_file.write_text("old", encoding="utf-8")
    removed_file = app_dir / "obsolete.txt"
    removed_file.write_text("legacy", encoding="utf-8")

    with zipfile.ZipFile(patch_zip, "w") as archive:
        archive.writestr("foo.txt", "new")
        archive.writestr("added.txt", "created")
        archive.writestr(
            "patch_meta.json",
            '{"deleted_files": ["obsolete.txt"]}',
        )

    applier = UpdateApplier(
        app_dir=app_dir,
        runtime_state_root=str(runtime_root),
        emit_log=lambda _message: None,
    )

    result = applier.apply_patch_zip(
        zip_path=patch_zip,
        backup_root=backup_root,
        max_backups=3,
    )

    assert original_file.read_text(encoding="utf-8") == "new"
    assert not removed_file.exists()
    assert (app_dir / "added.txt").exists()

    rollback_result = applier.restore_backup_snapshot(Path(str(result["backup"])))

    assert rollback_result["restored"] >= 2
    assert original_file.read_text(encoding="utf-8") == "old"
    assert removed_file.read_text(encoding="utf-8") == "legacy"
    assert not (app_dir / "added.txt").exists()
