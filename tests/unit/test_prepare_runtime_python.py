from __future__ import annotations

from pathlib import Path

from scripts.prepare_runtime_python import prepare_runtime_python, resolve_source_runtime_root


def _build_fake_runtime(root: Path) -> None:
    (root / "Lib" / "encodings").mkdir(parents=True, exist_ok=True)
    (root / "DLLs").mkdir(parents=True, exist_ok=True)
    (root / "__pycache__").mkdir(parents=True, exist_ok=True)
    (root / "python.exe").write_text("exe", encoding="utf-8")
    (root / "python313.dll").write_text("dll", encoding="utf-8")
    (root / "Lib" / "os.py").write_text("print('ok')", encoding="utf-8")
    (root / "Lib" / "os.pyc").write_text("compiled", encoding="utf-8")
    (root / "__pycache__" / "cache.pyc").write_text("compiled", encoding="utf-8")
    (root / "pyvenv.cfg").write_text("home = C:\\Python313", encoding="utf-8")


def test_prepare_runtime_python_copies_runtime_and_skips_ignored_files(tmp_path: Path) -> None:
    source_root = tmp_path / "python313"
    target_root = tmp_path / "project" / "runtime" / "python"
    _build_fake_runtime(source_root)

    result = prepare_runtime_python(
        source_root=source_root,
        target_root=target_root,
        clear_target=True,
        dry_run=False,
        emit_log=lambda _text: None,
    )

    assert result["copied_files"] >= 3
    assert (target_root / "python.exe").exists()
    assert (target_root / "python313.dll").exists()
    assert (target_root / "Lib" / "os.py").exists()
    assert not (target_root / "Lib" / "os.pyc").exists()
    assert not (target_root / "__pycache__").exists()
    assert not (target_root / "pyvenv.cfg").exists()
    assert (target_root / ".qjpt_runtime.json").exists()


def test_prepare_runtime_python_dry_run_does_not_write_files(tmp_path: Path) -> None:
    source_root = tmp_path / "python313"
    target_root = tmp_path / "project" / "runtime" / "python"
    _build_fake_runtime(source_root)

    result = prepare_runtime_python(
        source_root=source_root,
        target_root=target_root,
        clear_target=True,
        dry_run=True,
        emit_log=lambda _text: None,
    )

    assert result["copied_files"] >= 3
    assert not target_root.exists()


def test_resolve_source_runtime_root_accepts_python_executable_path(tmp_path: Path, monkeypatch) -> None:
    source_root = tmp_path / "python313"
    _build_fake_runtime(source_root)
    monkeypatch.setattr("scripts.prepare_runtime_python.sys.base_prefix", str(tmp_path / "missing"))
    monkeypatch.setattr("scripts.prepare_runtime_python.sys.prefix", str(tmp_path / "missing2"))
    monkeypatch.setattr("scripts.prepare_runtime_python.sys.executable", str(source_root / "python.exe"))

    resolved = resolve_source_runtime_root()

    assert resolved == source_root.resolve()
