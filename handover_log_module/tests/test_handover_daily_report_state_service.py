from pathlib import Path

from handover_log_module.service import handover_daily_report_state_service as state_module


def test_daily_report_state_service_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(state_module, "resolve_runtime_state_root", lambda **_kwargs: Path(tmp_path))
    service = state_module.HandoverDailyReportStateService({"_global_paths": {}})

    export_state = service.update_export_state(
        duty_date="2026-03-24",
        duty_shift="night",
        daily_report_record_export={
            "status": "success",
            "record_id": "rec_1",
            "spreadsheet_url": "https://example.com/sheet",
            "summary_screenshot_source_used": "auto",
            "external_screenshot_source_used": "manual",
        },
    )
    auth_state = service.update_screenshot_auth_state(
        {
            "status": "ready",
            "profile_dir": str(tmp_path / "profile"),
        }
    )

    assert export_state["status"] == "success"
    assert export_state["summary_screenshot_source_used"] == "auto"
    assert export_state["external_screenshot_source_used"] == "manual"
    assert service.get_export_state(duty_date="2026-03-24", duty_shift="night")["record_id"] == "rec_1"
    assert auth_state["status"] == "ready"

    context = service.get_context(
        duty_date="2026-03-24",
        duty_shift="night",
        screenshot_auth=service.get_screenshot_auth_state(),
        capture_assets={},
        spreadsheet_url="https://example.com/sheet",
    )

    assert context["batch_key"] == "2026-03-24|night"
    assert context["daily_report_record_export"]["spreadsheet_url"] == "https://example.com/sheet"
    assert context["daily_report_record_export"]["summary_screenshot_source_used"] == "auto"
    assert context["screenshot_auth"]["status"] == "ready"
    assert context["display"]["auth"]["text"] == "已登录"
    assert context["display"]["export"]["text"] == "日报多维记录已写入"
    assert context["display"]["capture_assets"]["summary_sheet_image"]["title"] == "今日航图截图"
    assert context["display"]["actions"]["open_auth"]["allowed"] is True
    assert context["display"]["actions"]["rewrite_record"]["allowed"] is False
    assert context["display"]["actions"]["rewrite_record"]["reason_code"] == "missing_effective_assets"
    assert context["display"]["capture_assets"]["summary_sheet_image"]["actions"]["preview"]["allowed"] is False
