from __future__ import annotations

from pathlib import Path

from scripts import build_exe as module


def test_write_runtime_dependency_lock_uses_exact_versions(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(module.importlib.metadata, "version", lambda package: f"{package}-1.0.0")

    payload = module._write_runtime_dependency_lock(tmp_path, python_version="3.11.9")  # noqa: SLF001

    stored = module._read_json(tmp_path / "runtime_dependency_lock.json")  # noqa: SLF001
    assert stored["python_version"] == "3.11.9"
    assert payload["packages"] == stored["packages"]
    assert all(item["version"].endswith("-1.0.0") for item in stored["packages"])


def test_latest_manifest_includes_dependency_metadata(tmp_path: Path, monkeypatch) -> None:
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
            "dependency_manifest_path": "runtime_dependency_lock.json",
            "dependency_install_policy": "online_only_exact",
        },
    )

    assert manifest["dependency_manifest_path"] == "runtime_dependency_lock.json"
    assert manifest["dependency_install_policy"] == "online_only_exact"

