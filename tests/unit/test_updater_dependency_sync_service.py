from __future__ import annotations

import subprocess
from pathlib import Path

from app.modules.updater.service.runtime_dependency_sync_service import RuntimeDependencySyncService


def test_sync_required_packages_installs_missing_and_mismatched_versions(monkeypatch, tmp_path: Path) -> None:
    service = RuntimeDependencySyncService(app_dir=tmp_path, runtime_state_root=str(tmp_path / ".runtime"))
    installed_versions = {
        "fastapi": "",
        "uvicorn": "0.34.0",
    }
    import_ready = {
        "fastapi": False,
        "uvicorn": True,
    }
    install_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(service, "ensure_pip_available", lambda: None)
    monkeypatch.setattr(service, "_find_import", lambda import_name: bool(import_ready.get(import_name, False)))
    monkeypatch.setattr(service, "_installed_version", lambda package: str(installed_versions.get(package, "")))

    def fake_install(package: str, version: str = "") -> None:
        install_calls.append((package, version))
        installed_versions[package] = version
        if package == "fastapi":
            import_ready["fastapi"] = True

    monkeypatch.setattr(service, "_install_package", fake_install)

    result = service.sync_required_packages(
        [
            {"package": "fastapi", "import_name": "fastapi", "version": "0.116.0"},
            {"package": "uvicorn", "import_name": "uvicorn", "version": "0.35.0"},
        ],
        exact_versions=True,
    )

    assert install_calls == [("fastapi", "0.116.0"), ("uvicorn", "0.35.0")]
    assert result["installed"] == 2
    assert result["status"] == "success"


def test_sync_from_lock_file_uses_exact_versions(monkeypatch, tmp_path: Path) -> None:
    lock_path = tmp_path / "runtime_dependency_lock.json"
    lock_path.write_text(
        (
            "{\n"
            '  "python_version": "3.11.9",\n'
            '  "generated_at": "2026-03-26 18:30:00",\n'
            '  "packages": [\n'
            '    {"package": "fastapi", "version": "0.116.0", "import_name": "fastapi"}\n'
            "  ]\n"
            "}\n"
        ),
        encoding="utf-8",
    )
    service = RuntimeDependencySyncService(app_dir=tmp_path)
    captured: dict[str, object] = {}

    def fake_sync(packages, *, exact_versions):
        captured["packages"] = packages
        captured["exact_versions"] = exact_versions
        return {"status": "success", "installed": 0, "checked": 1, "packages": []}

    monkeypatch.setattr(service, "sync_required_packages", fake_sync)

    result = service.sync_from_lock_file(lock_path)

    assert captured["exact_versions"] is True
    assert result["lock_path"] == str(lock_path)


def test_format_install_failure_includes_user_friendly_advice(tmp_path: Path) -> None:
    service = RuntimeDependencySyncService(app_dir=tmp_path)

    detail = (
        "WARNING: Retrying after connection broken by "
        "'ProxyError(\"Cannot connect to proxy.\", TimeoutError(\"_ssl.c:989: The handshake operation timed out\"))'\n"
        "ERROR: Could not find a version that satisfies the requirement fastapi==0.116.1"
    )
    message = service._format_install_failure("fastapi==0.116.1", detail)  # noqa: SLF001

    assert "安装依赖失败 fastapi==0.116.1" in message
    assert "代理连接失败" in message


def test_find_import_uses_real_import_not_only_spec(monkeypatch, tmp_path: Path) -> None:
    service = RuntimeDependencySyncService(app_dir=tmp_path)

    def fake_probe(_code: str, *args: str):  # noqa: ANN001
        import_name = args[0]
        if import_name == "fastapi":
            return subprocess.CompletedProcess(["python"], 1, stdout="", stderr="ModuleNotFoundError: sniffio")
        return subprocess.CompletedProcess(["python"], 0, stdout="", stderr="")

    monkeypatch.setattr(service, "_run_python_probe", fake_probe)

    assert service._find_import("fastapi") is False  # noqa: SLF001


def test_installed_version_uses_target_python_probe(monkeypatch, tmp_path: Path) -> None:
    service = RuntimeDependencySyncService(app_dir=tmp_path)

    def fake_probe(_code: str, *args: str):  # noqa: ANN001
        package = args[0]
        if package == "fastapi":
            return subprocess.CompletedProcess(["python"], 0, stdout="0.128.5\n", stderr="")
        return subprocess.CompletedProcess(["python"], 1, stdout="", stderr="boom")

    monkeypatch.setattr(service, "_run_python_probe", fake_probe)

    assert service._installed_version("fastapi") == "0.128.5"  # noqa: SLF001
    assert service._installed_version("unknown") == ""  # noqa: SLF001
