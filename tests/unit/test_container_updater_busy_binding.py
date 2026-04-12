from __future__ import annotations

from pathlib import Path

import app.modules.updater.service.updater_service as updater_service_module
from app.bootstrap.container import AppContainer


class _FakeJobService:
    def has_incomplete_jobs(self) -> bool:
        return True

    def has_running_jobs(self) -> bool:
        return False


def test_updater_service_uses_running_jobs_instead_of_waiting_jobs(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(updater_service_module, "get_app_dir", lambda: tmp_path)
    container = AppContainer(
        config={},
        runtime_config={
            "paths": {"runtime_state_root": str(tmp_path / ".runtime")},
            "updater": {
                "enabled": True,
                "auto_apply": False,
                "auto_restart": False,
                "gitee_repo": "https://example.invalid/repo.git",
                "gitee_branch": "master",
                "gitee_manifest_path": "updates/latest_patch.json",
            },
        },
        config_path=tmp_path / "config.json",
        frontend_mode="source",
        frontend_root=tmp_path,
        frontend_assets_dir=tmp_path,
        job_service=_FakeJobService(),
    )

    service = container._build_updater_service()

    assert service.is_busy() is False
