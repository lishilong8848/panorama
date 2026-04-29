from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService


def test_mark_cloud_sheet_uploading_updates_only_uploading_buildings(monkeypatch):
    service = ReviewFollowupTriggerService({})
    captured = []

    monkeypatch.setattr(
        service,
        "_update_cloud_sheet_sync_resilient",
        lambda **kwargs: captured.append(kwargs),
    )

    service._mark_cloud_sheet_uploading(
        sessions=[
            {
                "session_id": "A楼|2026-03-24|day",
                "building": "A楼",
                "revision": 2,
                "cloud_sheet_sync": {"status": "pending_upload"},
            },
            {
                "session_id": "B楼|2026-03-24|day",
                "building": "B楼",
                "revision": 3,
                "cloud_sheet_sync": {"status": "success", "synced_revision": 3},
            },
        ],
        batch_meta={
            "batch_key": "2026-03-24|day",
            "spreadsheet_token": "token-1",
            "spreadsheet_url": "https://example.com/sheet",
            "spreadsheet_title": "交接班",
            "prepared_at": "2026-03-24 08:00:00",
        },
        upload_items=[{"building": "A楼"}],
        emit_log=lambda _msg: None,
    )

    assert [item["building"] for item in captured] == ["A楼"]
    payload = captured[0]["cloud_sheet_sync"]
    assert payload["status"] == "uploading"
    assert payload["last_attempt_revision"] == 2
    assert payload["spreadsheet_token"] == "token-1"
