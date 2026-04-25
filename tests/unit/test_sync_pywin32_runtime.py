from __future__ import annotations

from pathlib import Path

import pytest

from scripts.sync_pywin32_runtime import PYWIN32_ITEMS, sync_pywin32_runtime


def _build_fake_pywin32_site_packages(site_packages: Path) -> None:
    site_packages.mkdir(parents=True, exist_ok=True)
    for name in PYWIN32_ITEMS:
        target = site_packages / name
        if "." in name and not name.endswith(".dist-info"):
            target.write_text(f"{name}\n", encoding="utf-8")
        else:
            target.mkdir(parents=True, exist_ok=True)
            (target / "__init__.py").write_text(f"# {name}\n", encoding="utf-8")


def test_sync_pywin32_runtime_copies_to_target_project_runtime(tmp_path: Path) -> None:
    source_site_packages = tmp_path / "source" / "Lib" / "site-packages"
    target_project = tmp_path / "internal_project"
    _build_fake_pywin32_site_packages(source_site_packages)

    result = sync_pywin32_runtime(
        source=str(source_site_packages),
        target_project=str(target_project),
        verify=False,
        emit_log=lambda _text: None,
    )

    target_site_packages = target_project / "runtime" / "python" / "Lib" / "site-packages"
    assert result["target_site_packages"] == str(target_site_packages.resolve())
    assert (target_site_packages / "pythoncom.py").read_text(encoding="utf-8") == "pythoncom.py\n"
    assert (target_site_packages / "win32com" / "__init__.py").exists()
    assert result["verified"] is False
    assert result["dry_run"] is False


def test_sync_pywin32_runtime_accepts_direct_target_site_packages(tmp_path: Path) -> None:
    source_site_packages = tmp_path / "source" / "Lib" / "site-packages"
    target_site_packages = tmp_path / "share" / "runtime" / "python" / "Lib" / "site-packages"
    _build_fake_pywin32_site_packages(source_site_packages)

    sync_pywin32_runtime(
        source=str(source_site_packages),
        target_site_packages=str(target_site_packages),
        verify=False,
        emit_log=lambda _text: None,
    )

    assert (target_site_packages / "pywin32_system32" / "__init__.py").exists()
    assert (target_site_packages / "PyWin32.chm").read_text(encoding="utf-8") == "PyWin32.chm\n"


def test_sync_pywin32_runtime_missing_source_item_fails(tmp_path: Path) -> None:
    source_site_packages = tmp_path / "source" / "Lib" / "site-packages"
    target_project = tmp_path / "internal_project"
    _build_fake_pywin32_site_packages(source_site_packages)
    (source_site_packages / "pythoncom.py").unlink()

    with pytest.raises(RuntimeError, match="pythoncom.py"):
        sync_pywin32_runtime(
            source=str(source_site_packages),
            target_project=str(target_project),
            verify=False,
            emit_log=lambda _text: None,
        )


def test_sync_pywin32_runtime_cleans_stale_target_item(tmp_path: Path) -> None:
    source_site_packages = tmp_path / "source" / "Lib" / "site-packages"
    target_site_packages = tmp_path / "target" / "Lib" / "site-packages"
    _build_fake_pywin32_site_packages(source_site_packages)
    (target_site_packages / "win32").mkdir(parents=True, exist_ok=True)
    (target_site_packages / "win32" / "stale.py").write_text("old\n", encoding="utf-8")

    sync_pywin32_runtime(
        source=str(source_site_packages),
        target_site_packages=str(target_site_packages),
        verify=False,
        emit_log=lambda _text: None,
    )

    assert not (target_site_packages / "win32" / "stale.py").exists()
    assert (target_site_packages / "win32" / "__init__.py").exists()


def test_sync_pywin32_runtime_dry_run_does_not_write(tmp_path: Path) -> None:
    source_site_packages = tmp_path / "source" / "Lib" / "site-packages"
    target_project = tmp_path / "internal_project"
    _build_fake_pywin32_site_packages(source_site_packages)

    result = sync_pywin32_runtime(
        source=str(source_site_packages),
        target_project=str(target_project),
        verify=False,
        dry_run=True,
        emit_log=lambda _text: None,
    )

    assert result["dry_run"] is True
    assert not (target_project / "runtime").exists()
