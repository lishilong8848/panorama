from __future__ import annotations

import pytest

import main as main_module


def test_main_uses_shared_runtime_dependency_service(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeService:
        def __init__(self, **kwargs):
            captured["init"] = kwargs

        def ensure_startup_dependencies(self):
            captured["called"] = True
            return {"installed": 1}

    monkeypatch.setattr(main_module, "RuntimeDependencySyncService", FakeService)

    main_module._ensure_runtime_dependencies({"paths": {"runtime_state_root": ".runtime"}})  # noqa: SLF001

    assert captured["called"] is True
    assert captured["init"]["python_executable"] == main_module.sys.executable


def test_main_exits_cleanly_when_dependency_bootstrap_fails(monkeypatch, capsys) -> None:
    monkeypatch.setattr(main_module, "_ensure_runtime_dependencies", lambda _cfg=None: (_ for _ in ()).throw(RuntimeError("代理连接失败")))

    with pytest.raises(SystemExit) as excinfo:
        main_module.main(["--no-open-browser"])

    captured = capsys.readouterr()
    assert excinfo.value.code == 1
    assert "运行依赖准备失败" in captured.out
    assert "代理连接失败" in captured.out


def test_source_run_marks_updater_disabled(monkeypatch) -> None:
    monkeypatch.delenv(main_module._SOURCE_RUN_DISABLE_UPDATER_ENV, raising=False)
    monkeypatch.delenv(main_module._PORTABLE_LAUNCHER_ENV, raising=False)
    monkeypatch.setattr(main_module.sys, "frozen", False, raising=False)

    changed = main_module._apply_source_run_runtime_flags()

    assert changed is True
    assert main_module.os.environ[main_module._SOURCE_RUN_DISABLE_UPDATER_ENV] == "1"


def test_portable_launcher_keeps_updater_enabled(monkeypatch) -> None:
    monkeypatch.delenv(main_module._SOURCE_RUN_DISABLE_UPDATER_ENV, raising=False)
    monkeypatch.setenv(main_module._PORTABLE_LAUNCHER_ENV, "1")
    monkeypatch.setattr(main_module.sys, "frozen", False, raising=False)

    changed = main_module._apply_source_run_runtime_flags()

    assert changed is False
    assert main_module.os.environ.get(main_module._SOURCE_RUN_DISABLE_UPDATER_ENV, "") == ""
