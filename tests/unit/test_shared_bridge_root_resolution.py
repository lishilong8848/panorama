from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.bootstrap.container import AppContainer
from app.config.config_adapter import resolve_shared_bridge_paths
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.shared.utils import file_utils


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


def test_app_container_shared_root_diagnostic_treats_mapped_drive_and_unc_as_same_share(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
) -> None:
    monkeypatch.setattr(file_utils, "_mapped_drive_unc_root", lambda drive: r"\\172.16.1.2\share" if drive.upper() == "Z:" else "")
    container = AppContainer(
        config={
            "common": {
                "deployment": {"role_mode": "external", "node_id": "node-1", "node_label": "外网端"},
                "shared_bridge": {
                    "enabled": True,
                    "root_dir": r"D:\legacy-share",
                    "internal_root_dir": r"\\172.16.1.2\share",
                    "external_root_dir": "Z:\\",
                },
            }
        },
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {
                "enabled": True,
                "root_dir": r"D:\legacy-share",
                "internal_root_dir": r"\\172.16.1.2\share",
                "external_root_dir": "Z:\\",
            },
        },
        config_path=work_dir / "config.json",
        frontend_mode="source",
        frontend_root=work_dir,
        frontend_assets_dir=work_dir,
        job_service=SimpleNamespace(),
    )
    container.updater_service = SimpleNamespace(shared_bridge_root=r"\\172.16.1.2\share")

    payload = container.shared_root_diagnostic_snapshot(
        shared_bridge_snapshot={"role_mode": "external", "root_dir": "Z:\\"},
        updater_snapshot={"source_kind": "remote"},
    )

    assert payload["status"] == "alias_match"
    assert payload["status_text"] == "路径写法不同但目录一致"
    assert payload["paths"][0]["canonical_path"] == r"\\172.16.1.2\share"
    assert payload["paths"][1]["canonical_path"] == r"\\172.16.1.2\share"
    assert payload["paths"][2]["canonical_path"] == r"\\172.16.1.2\share"
    assert payload["paths"][3]["canonical_path"] == r"\\172.16.1.2\share"


def test_shared_bridge_runtime_diagnose_shared_root_initializes_required_dirs_and_counts_files(work_dir: Path) -> None:
    shared_root = work_dir / "share-root"
    runtime_config = {
        "deployment": {"role_mode": "external"},
        "shared_bridge": {"enabled": True, "root_dir": str(shared_root)},
        "internal_source_cache": {"enabled": True},
        "internal_source_sites": [{"building": "A楼", "enabled": True}],
    }
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    ready_path = shared_root / "交接班日志源文件" / "202604" / "20260409--白班" / "20260409--白班--交接班日志源文件--A楼.xlsx"
    ready_path.parent.mkdir(parents=True, exist_ok=True)
    ready_path.write_bytes(b"test")
    store.upsert_source_cache_entry(
        source_family="handover_log_family",
        building="A楼",
        bucket_kind="date",
        bucket_key="2026-04-09",
        duty_date="2026-04-09",
        duty_shift="day",
        downloaded_at="2026-04-09 09:00:00",
        relative_path=ready_path.relative_to(shared_root).as_posix(),
        status="ready",
        file_hash="abc",
        size_bytes=4,
        metadata={},
    )

    service = SharedBridgeRuntimeService(
        runtime_config=runtime_config,
        app_version="test",
    )

    payload = service.diagnose_shared_root(initialize=True)

    assert payload["status"] == "success"
    assert payload["summary"]["ready_entry_count"] == 1
    assert payload["summary"]["accessible_ready_count"] == 1
    assert any(item["key"] == "bridge_db" and item["exists"] for item in payload["directories"])
    assert any(item["key"] == "tmp_source_cache" and item["exists"] for item in payload["directories"])
    family = next(item for item in payload["families"] if item["key"] == "handover_log_family")
    assert family["ready_entry_count"] == 1
    assert family["accessible_ready_count"] == 1
