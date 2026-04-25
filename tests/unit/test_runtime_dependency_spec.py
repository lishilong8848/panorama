from __future__ import annotations

from app.shared.runtime_dependency_spec import (
    build_runtime_dependency_lock,
    normalized_runtime_dependency_specs,
)


def test_runtime_dependency_spec_entries_are_complete() -> None:
    specs = normalized_runtime_dependency_specs()

    assert specs
    assert {"package": "fastapi", "import_name": "fastapi"} in specs
    assert {"package": "pywin32", "import_name": "pythoncom"} in specs
    assert {"package": "pywin32", "import_name": "win32com.client"} in specs
    assert {"package": "python-multipart", "import_name": "multipart"} in specs


def test_build_runtime_dependency_lock_contains_exact_versions() -> None:
    payload = build_runtime_dependency_lock(
        package_versions={spec["package"]: "1.2.3" for spec in normalized_runtime_dependency_specs()},
        python_version="3.11.9",
        generated_at="2026-03-26 18:30:00",
    )

    assert payload["python_version"] == "3.11.9"
    assert len(payload["packages"]) == len(normalized_runtime_dependency_specs())
    assert payload["packages"][0]["version"] == "1.2.3"
