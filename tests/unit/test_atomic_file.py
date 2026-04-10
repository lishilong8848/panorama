from __future__ import annotations

import os
from pathlib import Path

import pytest

import app.shared.utils.atomic_file as atomic_file_module
from app.shared.utils.atomic_file import atomic_write_text


def test_atomic_write_text_retries_replace_on_windows_permission_error(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "state.json"
    attempts = {"count": 0}
    original_replace = os.replace

    def flaky_replace(src, dst):  # noqa: ANN001, ANN202
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise PermissionError(5, "Access is denied")
        return original_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky_replace)

    atomic_write_text(target, '{"ok": true}', encoding="utf-8")

    assert target.read_text(encoding="utf-8") == '{"ok": true}'
    assert attempts["count"] == 2


def test_atomic_write_text_strict_mode_does_not_fallback_to_direct_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "state.json"
    fallback_called = {"value": False}

    def always_fail_replace(_src, _dst):  # noqa: ANN001, ANN202
        raise PermissionError(5, "Access is denied")

    def mark_fallback(*_args, **_kwargs):  # noqa: ANN001, ANN202
        fallback_called["value"] = True

    monkeypatch.setattr(os, "replace", always_fail_replace)
    monkeypatch.setattr(atomic_file_module, "_overwrite_from_temp_with_retry", mark_fallback)

    with pytest.raises(PermissionError):
        atomic_write_text(
            target,
            '{"ok": true}',
            encoding="utf-8",
            allow_overwrite_fallback=False,
        )

    assert fallback_called["value"] is False
    assert not target.exists()
