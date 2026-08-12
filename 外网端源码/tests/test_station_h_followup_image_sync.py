from __future__ import annotations

from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService


class _ReviewService:
    def __init__(self) -> None:
        self.persisted = []

    @staticmethod
    def get_cloud_batch(_batch_key):
        return {"spreadsheet_token": "spreadsheet-token"}

    @staticmethod
    def parse_batch_key(_batch_key):
        return "2026-08-12", "day"

    def update_cloud_batch_extra_state(self, *, batch_key, field, value):
        self.persisted.append({"batch_key": batch_key, "field": field, "value": value})


class _DutyFocusService:
    @staticmethod
    def build_status(**_kwargs):
        return {"print_ready": False}

    @staticmethod
    def build_image_document(**_kwargs):
        raise RuntimeError("签名缺失")


class _CloudSheetSyncService:
    def __init__(self) -> None:
        self.image_path = "not-called"

    def sync_station_h_sheet(self, *, duty_focus_image_path, **_kwargs):
        self.image_path = duty_focus_image_path
        return {
            "status": "success",
            "duty_focus_image_synced": False,
            "synced_row_count": 50,
            "synced_column_count": 20,
        }


def test_station_h_values_continue_when_duty_focus_image_generation_fails():
    service = object.__new__(ReviewFollowupTriggerService)
    service._review_service = _ReviewService()
    service._station_h_duty_focus_service = _DutyFocusService()
    service._cloud_sheet_sync_service = _CloudSheetSyncService()
    service._build_station_h_cell_values = lambda **_kwargs: {
        "ok": True,
        "cells": {"B3": "张三"},
        "selection": {},
    }
    logs = []

    result = service._attach_station_h_sync_result(
        batch_key="2026-08-12|day",
        sessions=[],
        cloud_result={"status": "skipped", "spreadsheet_token": "spreadsheet-token"},
        emit_log=logs.append,
    )

    station_result = result["station_h_sync"]
    assert service._cloud_sheet_sync_service.image_path is None
    assert station_result["status"] == "partial_failed"
    assert station_result["h_values_status"] == "success"
    assert station_result["duty_focus_image_status"] == "failed"
    assert "签名缺失" in station_result["duty_focus_image_error"]
    assert service._review_service.persisted[-1]["value"]["status"] == "partial_failed"
    assert any("H楼正文继续同步" in line for line in logs)


def test_station_h_success_persists_only_after_image_is_synced(tmp_path):
    class _SuccessfulDutyFocusService:
        @staticmethod
        def build_status(**_kwargs):
            return {"print_ready": True}

        @staticmethod
        def build_image_document(**_kwargs):
            image_path = tmp_path / "focus.png"
            image_path.write_bytes(b"png")
            return {"path": image_path, "generated_at": "2026-08-12 10:30:00"}

    class _SuccessfulCloudSheetSyncService(_CloudSheetSyncService):
        def sync_station_h_sheet(self, *, duty_focus_image_path, **_kwargs):
            self.image_path = duty_focus_image_path
            return {"status": "success", "duty_focus_image_synced": True}

    service = object.__new__(ReviewFollowupTriggerService)
    service._review_service = _ReviewService()
    service._station_h_duty_focus_service = _SuccessfulDutyFocusService()
    service._cloud_sheet_sync_service = _SuccessfulCloudSheetSyncService()
    service._build_station_h_cell_values = lambda **_kwargs: {
        "ok": True,
        "cells": {"B3": "张三"},
        "selection": {},
    }

    result = service._attach_station_h_sync_result(
        batch_key="2026-08-12|day",
        sessions=[],
        cloud_result={"status": "skipped", "spreadsheet_token": "spreadsheet-token"},
        emit_log=lambda _line: None,
    )

    station_result = result["station_h_sync"]
    assert service._cloud_sheet_sync_service.image_path == tmp_path / "focus.png"
    assert station_result["status"] == "success"
    assert station_result["duty_focus_image_status"] == "success"
    assert station_result["duty_focus_image_generated_at"] == "2026-08-12 10:30:00"
    assert service._review_service.persisted[-1]["value"]["status"] == "success"
