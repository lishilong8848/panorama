from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.bootstrap.container import AppContainer
from app.config.config_adapter import resolve_shared_bridge_paths


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "shared_bridge_root_resolution"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_resolve_shared_bridge_paths_uses_internal_root_for_internal_role() -> None:
    resolved = resolve_shared_bridge_paths(
        {
            "enabled": True,
            "root_dir": r"D:\legacy-share",
            "internal_root_dir": r"D:\internal-share",
            "external_root_dir": r"\\172.16.1.2\share",
        },
        "internal",
    )

    assert resolved["root_dir"] == r"D:\internal-share"
    assert resolved["internal_root_dir"] == r"D:\internal-share"
    assert resolved["external_root_dir"] == r"\\172.16.1.2\share"


def test_resolve_shared_bridge_paths_uses_external_root_for_external_role() -> None:
    resolved = resolve_shared_bridge_paths(
        {
            "enabled": True,
            "root_dir": r"D:\legacy-share",
            "internal_root_dir": r"D:\internal-share",
            "external_root_dir": r"\\172.16.1.2\share",
        },
        "external",
    )

    assert resolved["root_dir"] == r"\\172.16.1.2\share"


def test_resolve_shared_bridge_paths_falls_back_to_legacy_root() -> None:
    resolved = resolve_shared_bridge_paths(
        {
            "enabled": True,
            "root_dir": r"D:\legacy-share",
        },
        "external",
    )

    assert resolved["root_dir"] == r"D:\legacy-share"
    assert resolved["internal_root_dir"] == r"D:\legacy-share"
    assert resolved["external_root_dir"] == r"D:\legacy-share"


def test_app_container_shared_bridge_snapshot_uses_role_resolved_root_without_runtime_service(work_dir: Path) -> None:
    container = AppContainer(
        config={
            "common": {
                "deployment": {"role_mode": "external", "node_id": "node-1", "node_label": "外网端"},
                "shared_bridge": {
                    "enabled": True,
                    "root_dir": r"D:\legacy-share",
                    "internal_root_dir": r"D:\internal-share",
                    "external_root_dir": r"\\172.16.1.2\share",
                },
            }
        },
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {
                "enabled": True,
                "root_dir": r"D:\legacy-share",
                "internal_root_dir": r"D:\internal-share",
                "external_root_dir": r"\\172.16.1.2\share",
            },
        },
        config_path=work_dir / "config.json",
        frontend_mode="source",
        frontend_root=work_dir,
        frontend_assets_dir=work_dir,
        job_service=SimpleNamespace(),
    )

    snapshot = container.shared_bridge_snapshot()

    assert snapshot["enabled"] is True
    assert snapshot["role_mode"] == "external"
    assert snapshot["root_dir"] == r"\\172.16.1.2\share"
    assert snapshot["db_status"] == "stopped"
    assert snapshot["agent_status"] == "stopped"
