from __future__ import annotations

import os
from pathlib import Path

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
