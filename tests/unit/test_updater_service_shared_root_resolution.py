from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest

from app.modules.updater.service.updater_service import UpdaterService


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "updater_service_shared_root_resolution"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _build_config(work_dir: Path, *, role_mode: str) -> dict:
    return {
        "paths": {
            "runtime_state_root": str(work_dir / ".runtime"),
        },
        "deployment": {
            "role_mode": role_mode,
            "node_id": f"{role_mode}-node",
        },
        "shared_bridge": {
            "enabled": True,
            "root_dir": r"D:\legacy-share",
            "internal_root_dir": r"D:\internal-share",
            "external_root_dir": r"\\172.16.1.2\share",
        },
        "updater": {
            "enabled": True,
            "auto_apply": False,
            "auto_restart": False,
            "gitee_repo": "https://example.invalid/repo.git",
            "gitee_branch": "master",
            "gitee_manifest_path": "updates/latest_patch.json",
            "download_retry_count": 1,
            "request_timeout_sec": 5,
        },
    }


def test_updater_service_uses_internal_shared_root_for_internal_role(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
) -> None:
    monkeypatch.setattr(UpdaterService, "_mirror_runtime_snapshot", lambda self: {})
    service = UpdaterService(
        config=_build_config(work_dir, role_mode="internal"),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    assert service.shared_bridge_root == r"D:\internal-share"
    assert service.source_kind == "shared_mirror"


def test_updater_service_uses_external_shared_root_for_external_role(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
) -> None:
    monkeypatch.setattr(UpdaterService, "_mirror_runtime_snapshot", lambda self: {})
    service = UpdaterService(
        config=_build_config(work_dir, role_mode="external"),
        emit_log=lambda _text: None,
        is_busy=lambda: False,
    )

    assert service.shared_bridge_root == r"\\172.16.1.2\share"
    assert service.source_kind == "remote"
