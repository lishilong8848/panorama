from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService


def test_no_pending_cloud_upload_reconciles_missing_bitable_exports():
    sessions = [
        {
            "building": "A楼",
            "session_id": "A楼|2026-08-16|day",
            "revision": 1,
            "confirmed": True,
            "cloud_sheet_sync": {"status": "success", "synced_revision": 1},
        }
    ]

    class ReviewService:
        @staticmethod
        def list_batch_sessions(_batch_key):
            return sessions

    service = object.__new__(ReviewFollowupTriggerService)
    service._review_service = ReviewService()
    service._summarize_cloud_sheet_sync = lambda **_kwargs: {"status": "ok", "skipped_buildings": [], "failed_buildings": []}
    service._refresh_cloud_result_status = lambda value: value
    service._all_sessions_cloud_synced_current_revision = lambda _sessions: True
    service._existing_cabinet_shift_record_export = lambda _sessions: {"status": "pending"}
    service._existing_daily_report_record_export = lambda _sessions: {"status": "idle"}
    service._attach_extra_cloud_sheet_sync_results = lambda **kwargs: kwargs["cloud_result"]
    service._empty_export_result = lambda: {}
    service._compose_followup_result = lambda **kwargs: kwargs
    calls = []
    service._run_cabinet_shift_record_export = lambda **_kwargs: calls.append("cabinet") or {"status": "ok"}
    service._run_daily_report_record_export = lambda **_kwargs: calls.append("daily") or {"status": "success"}

    result = service.upload_pending_cloud_sheets_for_batch(
        batch_key="2026-08-16|day",
        emit_log=lambda _line: None,
    )

    assert calls == ["cabinet", "daily"]
    assert result["cabinet_shift_record_export"]["status"] == "ok"
    assert result["daily_report_record_export"]["status"] == "success"
