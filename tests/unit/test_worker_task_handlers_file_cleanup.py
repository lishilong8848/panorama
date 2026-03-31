from __future__ import annotations

from pathlib import Path

from app.worker import task_handlers


def _touch_file(root: Path, name: str = "input.xlsx") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    path.write_bytes(b"demo")
    return path


def test_manual_upload_handler_cleans_temp_dir(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    cleanup_dir = runtime_root / "temp" / "manual_upload"
    file_path = _touch_file(cleanup_dir)
    captured = {}
    monkeypatch.setattr(task_handlers, "get_app_dir", lambda: tmp_path)

    class _FakeCalculationService:
        def __init__(self, config):  # noqa: ANN001
            captured["config"] = config

        def run_manual_upload(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            return {"status": "ok"}

    monkeypatch.setattr(task_handlers, "CalculationService", _FakeCalculationService)
    result = task_handlers.handle_manual_upload(
        {"paths": {"runtime_state_root": str(tmp_path / ".runtime")}},
        {
            "building": "A楼",
            "file_path": str(file_path),
            "upload_date": "2026-03-24",
            "switch_external_before_upload": True,
            "cleanup_dir": str(cleanup_dir),
        },
        lambda _message: None,
    )

    assert result["status"] == "ok"
    assert captured["building"] == "A楼"
    assert not cleanup_dir.exists()


def test_handover_from_file_handler_cleans_temp_dir(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    cleanup_dir = runtime_root / "temp" / "handover_from_file"
    file_path = _touch_file(cleanup_dir)
    captured = {}
    monkeypatch.setattr(task_handlers, "get_app_dir", lambda: tmp_path)

    class _FakeOrchestratorService:
        def __init__(self, config):  # noqa: ANN001
            captured["config"] = config

        def run_handover_from_file(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            return {"status": "ok"}

    monkeypatch.setattr(task_handlers, "OrchestratorService", _FakeOrchestratorService)
    result = task_handlers.handle_handover_from_file(
        {"paths": {"runtime_state_root": str(tmp_path / ".runtime")}},
        {
            "building": "A楼",
            "file_path": str(file_path),
            "duty_date": "2026-03-24",
            "duty_shift": "night",
            "cleanup_dir": str(cleanup_dir),
        },
        lambda _message: None,
    )

    assert result["status"] == "ok"
    assert captured["building"] == "A楼"
    assert captured["duty_shift"] == "night"
    assert not cleanup_dir.exists()


def test_sheet_import_handler_cleans_temp_dir(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    cleanup_dir = runtime_root / "temp" / "sheet_import"
    file_path = _touch_file(cleanup_dir)
    captured = {}
    monkeypatch.setattr(task_handlers, "get_app_dir", lambda: tmp_path)

    class _FakeSheetImportService:
        def __init__(self, config):  # noqa: ANN001
            captured["config"] = config

        def run(self, xlsx_path, switch_external_before_upload, emit_log):  # noqa: ANN001
            captured["xlsx_path"] = xlsx_path
            captured["switch_external_before_upload"] = switch_external_before_upload
            return {"status": "ok", "failed_count": 0}

    monkeypatch.setattr(task_handlers, "SheetImportService", _FakeSheetImportService)
    result = task_handlers.handle_sheet_import(
        {"paths": {"runtime_state_root": str(tmp_path / ".runtime")}},
        {
            "xlsx_path": str(file_path),
            "switch_external_before_upload": True,
            "cleanup_dir": str(cleanup_dir),
        },
        lambda _message: None,
    )

    assert result["status"] == "ok"
    assert captured["switch_external_before_upload"] is True
    assert not cleanup_dir.exists()


def test_day_metric_from_file_handler_keeps_temp_file(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    keep_dir = runtime_root / "temp" / "day_metric_from_file"
    file_path = _touch_file(keep_dir)
    captured = {}

    class _FakeOrchestratorService:
        def __init__(self, config):  # noqa: ANN001
            captured["config"] = config

        def run_day_metric_from_file(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            return {"status": "ok"}

    monkeypatch.setattr(task_handlers, "OrchestratorService", _FakeOrchestratorService)
    result = task_handlers.handle_day_metric_from_file(
        {"paths": {"runtime_state_root": str(tmp_path / ".runtime")}},
        {
            "building": "A楼",
            "duty_date": "2026-03-24",
            "file_path": str(file_path),
        },
        lambda _message: None,
    )

    assert result["status"] == "ok"
    assert captured["building"] == "A楼"
    assert file_path.exists()
