from __future__ import annotations

from app.modules.report_pipeline.service.resume_checkpoint_store import (
    build_checkpoint,
    sync_summary_from_checkpoint,
)


def test_build_checkpoint_sets_core_fields():
    cp = build_checkpoint(
        source_name="多日期手动流程",
        run_save_dir="D:\\QLDownload\\run_x",
        selected_dates=["2026-03-01"],
    )
    assert cp["source"] == "多日期手动流程"
    assert cp["run_save_dir"] == "D:\\QLDownload\\run_x"
    assert cp["selected_dates"] == ["2026-03-01"]
    assert cp["stage"] == "downloading"
    assert isinstance(cp.get("run_id", ""), str) and cp["run_id"]


def test_sync_summary_from_checkpoint_updates_counts():
    checkpoint = {
        "run_id": "r1",
        "file_items": [
            {"status": "uploaded"},
            {"status": "pending"},
            {"status": "upload_failed"},
            {"status": "file_missing"},
        ],
        "summary": {},
    }
    summary = {}
    sync_summary_from_checkpoint(summary, checkpoint)
    assert summary["resume_run_id"] == "r1"
    assert summary["pending_upload_count"] == 2
    assert summary["upload_failed_count"] == 1
    assert summary["uploaded_count"] == 1
    assert summary["file_missing_count"] == 1
    assert summary["pending_resume"] is True
