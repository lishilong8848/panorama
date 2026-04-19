from __future__ import annotations

import subprocess
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
    assert manifest["zip_relpath"] == "updates/patches/QJPT_patch_only.zip"


def test_should_exclude_npm_cache_from_patch() -> None:
    assert module._should_exclude_from_patch(Path(".npm-cache/_cacache/demo")) is True  # noqa: SLF001
    assert module._should_exclude_from_patch(Path("node_modules/demo/index.js")) is True  # noqa: SLF001


def test_materialize_git_release_tree_rewrites_origin(tmp_path, monkeypatch) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-b", "master"], cwd=repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.name", "tester"], cwd=repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "tester@example.com"], cwd=repo_dir, check=True, capture_output=True, text=True)
    (repo_dir / "main.py").write_text("print('ok')\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo_dir, check=True, capture_output=True, text=True)
    target_dir = tmp_path / "release" / "QJPT_V3_code"
    monkeypatch.setattr(module, "PROJECT_ROOT", repo_dir)

    source_branch, target_branch = module._materialize_git_release_tree(  # noqa: SLF001
        target_dir,
        repo_url="https://example.invalid/repo.git",
        preferred_branch="master",
    )

    origin_url = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=target_dir,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert source_branch == "master"
    assert target_branch == "master"
    assert (target_dir / ".git").exists()
    assert origin_url == "https://example.invalid/repo.git"
