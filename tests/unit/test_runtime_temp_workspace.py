from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import os

from app.shared.utils.runtime_temp_workspace import (
    cleanup_runtime_temp_dir,
    create_runtime_temp_dir,
    prune_stale_runtime_temp_dirs,
    resolve_runtime_state_root,
    resolve_runtime_temp_root,
)


def test_runtime_temp_workspace_defaults_to_project_runtime_root(tmp_path: Path) -> None:
    runtime_root = resolve_runtime_state_root(app_dir=tmp_path)
    temp_root = resolve_runtime_temp_root(app_dir=tmp_path)
    workspace = create_runtime_temp_dir(kind="handover_from_files", app_dir=tmp_path)

    assert runtime_root == tmp_path / ".runtime"
    assert temp_root == tmp_path / ".runtime" / "temp"
    assert workspace.parent.parent == temp_root
    assert workspace.parent.name == "handover_from_files"

    cleanup_runtime_temp_dir(workspace, app_dir=tmp_path)
    assert not workspace.exists()


def test_runtime_temp_workspace_respects_user_defined_runtime_root(tmp_path: Path) -> None:
    custom_root = tmp_path / "custom_runtime_root"
    runtime_config = {"paths": {"runtime_state_root": str(custom_root)}}

    workspace = create_runtime_temp_dir(
        kind="manual_upload",
        runtime_config=runtime_config,
        app_dir=tmp_path,
    )

    assert custom_root in workspace.parents
    assert workspace.parent.name == "manual_upload"

    cleanup_runtime_temp_dir(workspace, runtime_config=runtime_config, app_dir=tmp_path)
    assert not workspace.exists()


def test_prune_stale_runtime_temp_dirs_only_removes_old_workspaces(tmp_path: Path) -> None:
    temp_root = resolve_runtime_temp_root(app_dir=tmp_path)
    stale = temp_root / "sheet_import" / "20260323_010101_deadbeef"
    fresh = temp_root / "sheet_import" / "20260323_020202_feedface"
    stale.mkdir(parents=True, exist_ok=True)
    fresh.mkdir(parents=True, exist_ok=True)

    cutoff = datetime.now() - timedelta(hours=96)
    timestamp = cutoff.timestamp()
    os.utime(stale, (timestamp, timestamp))

    removed = prune_stale_runtime_temp_dirs(app_dir=tmp_path, older_than_hours=72)

    assert removed == 1
    assert not stale.exists()
    assert fresh.exists()
