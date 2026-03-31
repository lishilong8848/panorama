from __future__ import annotations

from pathlib import Path

from scripts.build_exe import CRITICAL_RELEASE_SYNC_FILES, _sync_critical_stage_files_to_release


def test_sync_critical_stage_files_to_release_overwrites_latest_service_files(tmp_path: Path) -> None:
    stage_code_dir = tmp_path / "stage" / "QJPT_V3_code"
    release_code_dir = tmp_path / "release" / "QJPT_V3_code"

    for rel in CRITICAL_RELEASE_SYNC_FILES:
        src = stage_code_dir / rel
        dst = release_code_dir / rel
        src.parent.mkdir(parents=True, exist_ok=True)
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.write_text(f"latest::{rel.as_posix()}", encoding="utf-8")
        dst.write_text(f"stale::{rel.as_posix()}", encoding="utf-8")

    result = _sync_critical_stage_files_to_release(stage_code_dir, release_code_dir)

    assert result["copied"] == len(CRITICAL_RELEASE_SYNC_FILES)
    assert result["skipped"] == 0
    for rel in CRITICAL_RELEASE_SYNC_FILES:
        assert (release_code_dir / rel).read_text(encoding="utf-8") == f"latest::{rel.as_posix()}"


def test_critical_release_sync_files_include_portable_launcher() -> None:
    assert Path("portable_launcher.py") in CRITICAL_RELEASE_SYNC_FILES


def test_critical_release_sync_files_include_system_alert_log_upload_service() -> None:
    assert Path("app/modules/report_pipeline/service/system_alert_log_upload_service.py") in CRITICAL_RELEASE_SYNC_FILES


def test_critical_release_sync_files_include_runtime_log_pipeline_files() -> None:
    required = {
        Path("app/bootstrap/container.py"),
        Path("app/modules/report_pipeline/service/job_service.py"),
        Path("app/modules/websocket/service/log_stream_service.py"),
        Path("handover_log_module/service/review_session_service.py"),
        Path("main.py"),
        Path("web/frontend/src/config_api_utils.js"),
        Path("web/frontend/src/index.html"),
        Path("web/frontend/src/app_lifecycle.js"),
        Path("web/frontend/src/runtime_health_config_actions.js"),
        Path("web/frontend/src/runtime_resume_actions.js"),
        Path("web/frontend/src/log_stream.js"),
        Path("handover_log_module/service/handover_daily_report_bitable_export_service.py"),
    }
    assert required.issubset(set(CRITICAL_RELEASE_SYNC_FILES))
