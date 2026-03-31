from __future__ import annotations

from scripts import build_exe as module


def test_build_versioned_patch_zip_name_uses_patch_and_release_revision() -> None:
    name = module._build_versioned_patch_zip_name(target_patch_version=58, target_release_revision=91)  # noqa: SLF001

    assert name == "QJPT_patch_only_p58_r91.zip"


def test_build_launcher_content_uses_project_local_python_only() -> None:
    content = module._build_launcher_content(  # noqa: SLF001
        code_dir_expr='cd /d "%~dp0"',
        pip_index_url="https://example.com/simple",
        pip_trusted_host="example.com",
    )

    assert "%CD%\\runtime\\python\\python.exe" in content
    assert "%CD%\\.venv\\Scripts\\python.exe" in content
    assert "where python" not in content
    assert "where py" not in content
