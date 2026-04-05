from __future__ import annotations

import io
import zipfile
from pathlib import Path

from scripts import build_exe as module


class _FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def _make_zip_bytes() -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("python.exe", b"fake-python")
    return bio.getvalue()


def test_download_embed_zip_redownloads_invalid_cached_zip(tmp_path: Path, monkeypatch) -> None:
    build_dir = tmp_path / "build_output"
    monkeypatch.setattr(module, "BUILD_DIR", build_dir)
    monkeypatch.setattr(module, "EMBED_ZIP_MIRRORS", ["https://mirror.invalid/python"])
    cache_dir = build_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached_zip = cache_dir / module._embed_zip_name("3.11.9")
    cached_zip.write_text("not-a-zip", encoding="utf-8")

    zip_bytes = _make_zip_bytes()

    def fake_urlopen(url: str, timeout: int = 45):
        assert "3.11.9" in url
        assert timeout == 45
        return _FakeResponse(zip_bytes)

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)

    result = module._download_embed_zip("3.11.9")

    assert result == cached_zip
    assert zipfile.is_zipfile(result)


def test_ensure_local_git_identity_sets_defaults_when_missing(monkeypatch, tmp_path: Path) -> None:
    calls: list[list[str]] = []

    class _Result:
        def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run_cmd(args: list[str], cwd: Path | None = None):
        calls.append(list(args))
        if args == ["git", "config", "user.name"]:
            return _Result(returncode=1)
        if args == ["git", "config", "user.email"]:
            return _Result(returncode=1)
        if args == ["git", "config", "user.name", "QJPT Builder"]:
            return _Result(returncode=0)
        if args == ["git", "config", "user.email", "qjpt-builder@localhost"]:
            return _Result(returncode=0)
        raise AssertionError(f"unexpected args: {args}")

    monkeypatch.setattr(module, "_run_cmd", fake_run_cmd)

    module._ensure_local_git_identity(tmp_path)

    assert ["git", "config", "user.name"] in calls
    assert ["git", "config", "user.email"] in calls
    assert ["git", "config", "user.name", "QJPT Builder"] in calls
    assert ["git", "config", "user.email", "qjpt-builder@localhost"] in calls


def test_critical_release_sync_files_include_updater_paths() -> None:
    sync_paths = {str(path).replace("\\", "/") for path in module.CRITICAL_RELEASE_SYNC_FILES}

    assert "app/config/config_merge_guard.py" in sync_paths
    assert "app/modules/updater/api/routes.py" in sync_paths
    assert "app/modules/network/service/network_stability.py" in sync_paths
    assert "app/shared/runtime_dependency_spec.py" in sync_paths
    assert "app/modules/updater/service/runtime_dependency_sync_service.py" in sync_paths
    assert "app/modules/updater/service/updater_service.py" in sync_paths
    assert "app/modules/updater/service/update_applier.py" in sync_paths
    assert "app/modules/updater/core/versioning.py" in sync_paths
    assert "app/modules/updater/repository/updater_state_store.py" in sync_paths
    assert "web/frontend/src/updater_text.js" in sync_paths


def test_smoke_import_modules_include_updater_modules() -> None:
    assert "app.config.config_merge_guard" in module.SMOKE_IMPORT_MODULES
    assert "app.bootstrap.app_factory" in module.SMOKE_IMPORT_MODULES
    assert "app.modules.network.service.network_stability" in module.SMOKE_IMPORT_MODULES
    assert "app.modules.updater.service.runtime_dependency_sync_service" in module.SMOKE_IMPORT_MODULES
    assert "app.modules.updater.service.updater_service" in module.SMOKE_IMPORT_MODULES
    assert "app.modules.updater.service.update_applier" in module.SMOKE_IMPORT_MODULES
    assert "app.modules.updater.core.versioning" in module.SMOKE_IMPORT_MODULES


def test_patch_build_excludes_temp_and_backup_artifacts() -> None:
    assert module._should_exclude(Path(".tmp_runtime_tests/shared_bridge_monthly_runtime/artifact.xlsx"), include_venv=False)
    assert module._should_exclude(Path(".tmp_fix_bridge_store.py"), include_venv=False)
    assert module._should_exclude(Path("output/run.log"), include_venv=False)
    assert module._should_exclude(Path("表格计算配置.backup.20260403-081043.json"), include_venv=False)
    assert module._should_exclude(Path("表格计算部分代码.py.recovered.tmp_keep"), include_venv=False)
    assert module._should_exclude_from_patch(Path(".tmp_runtime_tests/shared_bridge_monthly_runtime/artifact.xlsx"))
    assert module._should_exclude_from_patch(Path("表格计算配置.backup.20260403-081043.json"))


def test_ensure_release_tree_imports_raises_when_import_check_fails(monkeypatch, tmp_path: Path) -> None:
    class _Result:
        def __init__(self) -> None:
            self.returncode = 1
            self.stdout = "out"
            self.stderr = "err"

    monkeypatch.setattr(module, "_run_cmd", lambda args, cwd=None: _Result())

    try:
        module._ensure_release_tree_imports(tmp_path)
    except RuntimeError as exc:
        assert "全量目录源码导入校验失败" in str(exc)
        assert str(tmp_path) in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_ensure_embedded_runtime_bootstrap_imports_raises_when_import_check_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime_python = tmp_path / "runtime" / "python" / "python.exe"
    runtime_python.parent.mkdir(parents=True, exist_ok=True)
    runtime_python.write_text("", encoding="utf-8")

    class _Result:
        def __init__(self) -> None:
            self.returncode = 1
            self.stdout = "boot-out"
            self.stderr = "boot-err"

    monkeypatch.setattr(module, "_run_cmd", lambda args, cwd=None: _Result())

    try:
        module._ensure_embedded_runtime_bootstrap_imports(tmp_path)
    except RuntimeError as exc:
        assert "lite 包启动链导入校验失败" in str(exc)
        assert str(tmp_path) in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
