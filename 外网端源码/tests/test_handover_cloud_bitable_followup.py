from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService


BATCH = "2026-10-26|day"
URL = "https://example.test/sheets/current"


def sessions():
    return [
        {"building": "A楼", "session_id": "A|" + BATCH, "duty_date": "2026-10-26", "duty_shift": "day",
         "revision": 1, "confirmed": True, "cloud_sheet_sync": {"status": "success", "synced_revision": 1}},
        {"building": "B楼", "session_id": "B|" + BATCH, "duty_date": "2026-10-26", "duty_shift": "day",
         "revision": 1, "confirmed": False, "cloud_sheet_sync": {"status": "pending_upload"}},
    ]


def service_for(rows):
    service = object.__new__(ReviewFollowupTriggerService)
    service._review_service = SimpleNamespace(
        list_batch_sessions=lambda _: rows,
        get_session_by_id=lambda _: rows[0],
        get_latest_session=lambda _: rows[0],
        get_batch_status=lambda _: {},
        get_cloud_batch=lambda _: {"spreadsheet_url": URL},
        update_cabinet_shift_record_export=Mock(),
    )
    service._maybe_mark_first_full_cloud_sync_completed = lambda **_: None
    service._ensure_external_network = lambda _: None
    service._run_session_followup_exports = lambda **_: {}
    service._run_cloud_sheet_upload = lambda **_: {"status": "ok", "spreadsheet_url": URL, "uploaded_buildings": ["A楼"]}
    service._summarize_cloud_sheet_sync = lambda **_: {"status": "partial_failed", "spreadsheet_url": URL}
    service._compose_followup_result = lambda **kwargs: kwargs
    service._existing_cabinet_shift_record_export = lambda _: {"status": "pending"}
    service._existing_daily_report_record_export = lambda _: {"status": "success", "record_id": "deleted_remote"}
    service._run_cabinet_shift_record_export = Mock(return_value={"status": "ok"})
    service._run_daily_report_record_export = Mock(return_value={"status": "success"})
    return service


def test_single_successful_building_also_syncs_daily_link_before_other_buildings_finish():
    rows = sessions()
    service = service_for(rows)
    result = service.trigger_single_building_cloud_sync(
        batch_key=BATCH, building="A楼", session_id=rows[0]["session_id"], emit_log=lambda _: None,
    )
    service._run_cabinet_shift_record_export.assert_called_once()
    service._run_daily_report_record_export.assert_called_once()
    assert result["daily_report_record_export"]["status"] == "success"


def test_cabinet_export_is_per_successful_building_not_whole_batch_status():
    rows = sessions()
    service = service_for(rows)
    del service._run_cabinet_shift_record_export
    exporter = Mock(return_value={"status": "ok", "details": {"A楼": {"status": "ok", "record_id": "new_a"}}})
    service._cabinet_shift_record_export_service = SimpleNamespace(export_sessions=exporter)
    service._run_cabinet_shift_record_export(
        batch_key=BATCH, sessions=rows,
        cloud_result={"status": "partial_failed", "uploaded_buildings": ["A楼"]}, emit_log=lambda _: None,
    )
    exporter.assert_called_once()
    assert [s["building"] for s in exporter.call_args.kwargs["sessions"]] == ["A楼"]


def test_daily_link_can_sync_when_another_building_or_extra_sheet_failed():
    rows = sessions()
    service = service_for(rows)
    del service._run_daily_report_record_export
    exporter = Mock(return_value={"status": "success", "record_id": "daily_a"})
    service._daily_report_bitable_export_service = SimpleNamespace(export_record=exporter)
    service._daily_report_state_service = SimpleNamespace(update_export_state=lambda **kw: kw["daily_report_record_export"])
    result = service._run_daily_report_record_export(
        batch_key=BATCH, sessions=rows,
        cloud_result={"status": "partial_failed", "spreadsheet_url": URL}, emit_log=lambda _: None,
    )
    exporter.assert_called_once()
    assert result["status"] == "success"


@pytest.mark.parametrize("entry", ["retry_cloud_sheet_for_session", "retry_cloud_sheet_for_building", "force_update_cloud_sheet_for_session"])
def test_cloud_retry_and_historical_update_also_reconcile_bitable_records(entry):
    rows = sessions()
    service = service_for(rows)
    getattr(service, entry)("A楼" if entry.endswith("building") else rows[0]["session_id"], emit_log=lambda _: None)
    service._run_cabinet_shift_record_export.assert_called_once()
    service._run_daily_report_record_export.assert_called_once()


def test_batch_retry_checks_records_even_when_cloud_already_succeeded():
    service = service_for(sessions())
    service.retry_failed_cloud_sheet_in_batch(BATCH, emit_log=lambda _: None)
    service._run_cabinet_shift_record_export.assert_called_once()
    service._run_daily_report_record_export.assert_called_once()


def test_daily_failure_is_reported_even_if_cloud_upload_succeeded():
    service = object.__new__(ReviewFollowupTriggerService)
    service._collect_followup_progress = lambda **_: {"status": "partial_failed"}
    result = service._compose_followup_result(
        batch_key=BATCH, export_result={}, cloud_result={"status": "ok", "uploaded_buildings": ["A楼"]},
        daily_report_record_export={"status": "failed", "error": "link failed", "error_detail": "timeout"},
        cabinet_shift_record_export={}, sessions=sessions(),
    )
    assert result["status"] == "partial_failed"
    assert any("timeout" in item["error"] for item in result["failed_buildings"])


def test_daily_link_is_not_created_when_no_current_revision_was_uploaded():
    rows = sessions()
    rows[0]["revision"] = 2
    service = service_for(rows)
    del service._run_daily_report_record_export
    service._daily_report_bitable_export_service = SimpleNamespace(export_record=Mock())
    service._run_daily_report_record_export(
        batch_key=BATCH, sessions=rows, cloud_result={"status": "ok", "spreadsheet_url": URL}, emit_log=lambda _: None,
    )
    service._daily_report_bitable_export_service.export_record.assert_not_called()


def test_continue_does_not_skip_daily_reconciliation_based_on_cached_success():
    rows = sessions()[:1]
    service = service_for(rows)
    service.evaluate = lambda _: {"ready_for_followup_upload": True}
    service._attach_extra_cloud_sheet_sync_results = lambda **kw: kw["cloud_result"]
    service._daily_report_state_service = SimpleNamespace(get_export_state=lambda **_: {"status": "success", "record_id": "deleted_remote"})
    service.continue_batch(BATCH, emit_log=lambda _: None)
    service._run_cabinet_shift_record_export.assert_called_once()
    service._run_daily_report_record_export.assert_called_once()


def test_partial_failure_feedback_names_bitable_failure():
    from app.modules.handover_review.api.routes import _build_review_cloud_retry_feedback, _build_review_history_cloud_update_feedback

    result = {"status": "partial_failed", "failed_buildings": [{"building": "日报链接多维", "error": "timeout"}]}
    for build in (_build_review_cloud_retry_feedback, _build_review_history_cloud_update_feedback):
        feedback = build(result)
        assert feedback["status"] == "partial_failed"
        assert "timeout" in feedback["detail_text"]
