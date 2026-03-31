from __future__ import annotations

from pathlib import Path

import main as app_main


def test_ensure_runtime_dependencies_passes_runtime_state_root(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    captured: dict[str, object] = {}

    class _FakeRuntimeDependencySyncService:
        def __init__(self, *, app_dir, runtime_state_root, emit_log, python_executable):  # noqa: ANN001
            captured["app_dir"] = app_dir
            captured["runtime_state_root"] = runtime_state_root
            captured["emit_log"] = emit_log
            captured["python_executable"] = python_executable

        def ensure_startup_dependencies(self):
            return {"installed": 0}

    monkeypatch.setattr(app_main, "RuntimeDependencySyncService", _FakeRuntimeDependencySyncService)

    app_main._ensure_runtime_dependencies({"paths": {"runtime_state_root": str(runtime_root)}})

    assert captured["app_dir"] == app_main.PROJECT_ROOT
    assert captured["runtime_state_root"] == str(runtime_root)
