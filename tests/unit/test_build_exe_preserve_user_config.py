from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import build_exe as module  # noqa: E402


def test_capture_and_restore_existing_user_config(tmp_path: Path) -> None:
    source_dir = tmp_path / "release" / module.RELEASE_CODE_DIR_NAME
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / module.USER_CONFIG_FILE_NAME
    source_file.write_text("user-config", encoding="utf-8")

    payload = module._capture_existing_user_config(source_dir)

    target_dir = tmp_path / "stage" / module.RELEASE_CODE_DIR_NAME
    restored = module._restore_existing_user_config(target_dir, payload)

    assert restored is True
    assert (target_dir / module.USER_CONFIG_FILE_NAME).read_text(encoding="utf-8") == "user-config"


def test_sync_stage_to_release_preserves_user_config_and_syncs_new_code(tmp_path: Path) -> None:
    release_dir = tmp_path / "release"
    stage_dir = tmp_path / "stage"
    (release_dir / module.USER_CONFIG_FILE_NAME).parent.mkdir(parents=True, exist_ok=True)
    (release_dir / module.USER_CONFIG_FILE_NAME).write_text("user-config", encoding="utf-8")
    (release_dir / ".runtime" / "state.json").parent.mkdir(parents=True, exist_ok=True)
    (release_dir / ".runtime" / "state.json").write_text("runtime-state", encoding="utf-8")
    (release_dir / "runtime" / "python" / "python.exe").parent.mkdir(parents=True, exist_ok=True)
    (release_dir / "runtime" / "python" / "python.exe").write_text("embedded-python", encoding="utf-8")

    new_module = stage_dir / "app" / "config" / "config_merge_guard.py"
    new_module.parent.mkdir(parents=True, exist_ok=True)
    new_module.write_text("x = 1\n", encoding="utf-8")
    (stage_dir / module.USER_CONFIG_FILE_NAME).write_text("default-config", encoding="utf-8")

    result = module._sync_stage_to_release(stage_dir, release_dir, preserve_user_data=True)

    assert result["copied"] >= 1
    assert (release_dir / "app" / "config" / "config_merge_guard.py").exists()
    assert (release_dir / module.USER_CONFIG_FILE_NAME).read_text(encoding="utf-8") == "user-config"
    assert (release_dir / ".runtime" / "state.json").read_text(encoding="utf-8") == "runtime-state"
    assert (release_dir / "runtime" / "python" / "python.exe").read_text(encoding="utf-8") == "embedded-python"
