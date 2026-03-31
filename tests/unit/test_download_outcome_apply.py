from __future__ import annotations

from dataclasses import dataclass

from app.modules.report_pipeline.service.download_result_utils import apply_download_outcomes


@dataclass
class _Outcome:
    success: bool
    file_path: str = ""
    error: str = ""


def test_apply_download_outcomes_records_success_and_failure():
    final_outcome_by_key = {
        ("2026-03-01", "A楼"): (object(), _Outcome(success=True, file_path="a.xlsx")),
        ("2026-03-01", "B楼"): (object(), _Outcome(success=False, error="timeout")),
    }
    date_result_by_date = {
        "2026-03-01": {
            "date": "2026-03-01",
            "start_time": "2026-03-01 00:00:00",
            "end_time": "2026-03-02 00:00:00",
            "success_buildings": [],
            "failed_buildings": [],
        }
    }
    summary = {"file_items": [], "success_dates": [], "failed_dates": []}
    checkpoint = {"file_items": []}
    notified: list[tuple[str, str, str]] = []

    apply_download_outcomes(
        final_outcome_by_key=final_outcome_by_key,
        date_result_by_date=date_result_by_date,
        summary=summary,
        checkpoint=checkpoint,
        notify_failure=lambda d, b, e: notified.append((d, b, e)),
    )

    assert summary["total_files"] == 1
    assert len(summary["file_items"]) == 1
    assert summary["file_items"][0]["building"] == "A楼"
    assert summary["success_dates"] == ["2026-03-01"]
    assert summary["failed_dates"] == []
    assert len(checkpoint["file_items"]) == 1
    assert notified == [("2026-03-01", "B楼", "timeout")]


def test_apply_download_outcomes_marks_date_failed_when_no_success():
    final_outcome_by_key = {
        ("2026-03-02", "A楼"): (object(), _Outcome(success=False, error="no data")),
    }
    date_result_by_date = {
        "2026-03-02": {
            "date": "2026-03-02",
            "start_time": "2026-03-02 00:00:00",
            "end_time": "2026-03-03 00:00:00",
            "success_buildings": [],
            "failed_buildings": [],
        }
    }
    summary = {"file_items": [], "success_dates": [], "failed_dates": []}
    checkpoint = {"file_items": []}

    apply_download_outcomes(
        final_outcome_by_key=final_outcome_by_key,
        date_result_by_date=date_result_by_date,
        summary=summary,
        checkpoint=checkpoint,
        notify_failure=lambda _d, _b, _e: None,
    )

    assert summary["total_files"] == 0
    assert summary["success_dates"] == []
    assert summary["failed_dates"] == ["2026-03-02"]
