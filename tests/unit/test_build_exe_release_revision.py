from __future__ import annotations

from pathlib import Path

from scripts import build_exe as module


def test_write_build_meta_includes_release_revision(tmp_path: Path) -> None:
    payload = module._write_build_meta(  # noqa: SLF001
        tmp_path,
        build_id="QJPT_V3",
        major_version=3,
        patch_version=48,
        release_revision=77,
        venv_hash="abc",
    )

    assert payload["release_revision"] == 77
    stored = module._read_json(tmp_path / "build_meta.json")  # noqa: SLF001
    assert stored["release_revision"] == 77


def test_write_latest_manifest_includes_target_release_revision(tmp_path: Path, monkeypatch) -> None:
    patch_dir = tmp_path / "patch"
    patch_dir.mkdir(parents=True, exist_ok=True)
    patch_zip = patch_dir / "QJPT_patch_only.zip"
    patch_zip.write_bytes(b"patch")
    monkeypatch.setattr(module, "_sha256_file", lambda _path: "sha256")

    _, manifest = module._write_latest_manifest(  # noqa: SLF001
        patch_dir=patch_dir,
        patch_zip=patch_zip,
        repo_url="https://gitee.com/example/repo.git",
        branch="master",
        subdir="updates/patches",
        build_meta={
            "build_id": "QJPT_V3",
            "major_version": 3,
            "patch_version": 48,
            "release_revision": 77,
            "display_version": "V3.48.20260326",
        },
        patch_meta={
            "target_patch_version": 48,
        },
    )

    assert manifest["target_release_revision"] == 77
