from __future__ import annotations

import json
from pathlib import Path

from app.modules.report_pipeline.service.resume_checkpoint_store import (
    build_checkpoint,
    list_pending_upload_runs_internal,
    load_resume_index,
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


def test_list_pending_upload_runs_internal_tolerates_network_checkpoint_path_error(tmp_path, monkeypatch):
    runtime_root = tmp_path / ".runtime"
    resume_root = runtime_root / "pipeline_resume"
    resume_root.mkdir(parents=True, exist_ok=True)
    index_path = resume_root / "index.json"
    network_checkpoint = r"Z:\tmp\source_cache\monthly_latest\2026-04-05 13\A楼\_pipeline_checkpoint.json"
    index_path.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "run_id": "run-1",
                        "checkpoint_path": network_checkpoint,
                        "updated_at": "2026-04-12 12:00:00",
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    original_exists = Path.exists

    def fake_exists(self):
        if str(self) == network_checkpoint:
            raise OSError(64, "指定的网络名不再可用。")
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", fake_exists)

    result = list_pending_upload_runs_internal(
        retention_days=7,
        app_dir=tmp_path,
        root_dir="pipeline_resume",
        index_file="index.json",
        runtime_state_root=str(runtime_root),
    )

    assert result == []
    index_obj = load_resume_index(
        app_dir=tmp_path,
        root_dir="pipeline_resume",
        index_file="index.json",
        runtime_state_root=str(runtime_root),
    )
    assert index_obj["items"] == []
