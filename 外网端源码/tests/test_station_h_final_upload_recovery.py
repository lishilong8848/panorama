import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.modules.report_pipeline.service.job_service import JobService, JobState
from handover_log_module.service import review_followup_trigger_service as module
from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService


BATCH = "2026-10-06|day"
TOKEN = "current-cloud"


def ready_sessions():
    return [{
        "building": b, "session_id": f"{b}|{BATCH}", "revision": 1, "confirmed": True,
        "cloud_sheet_sync": {"status": "success", "synced_revision": 1, "spreadsheet_token": TOKEN},
        "source_data_attachment_export": {"status": "success", "uploaded_revision": 1},
        "cabinet_shift_record_export": {"status": "success", "uploaded_revision": 1},
    } for b in ("A楼", "B楼", "C楼", "D楼", "E楼")]


class ReviewState:
    def __init__(self):
        self.sessions = ready_sessions()
        self.meta = {"spreadsheet_token": TOKEN, "status": "prepared"}

    def get_cloud_batch(self, _batch):
        return dict(self.meta)

    def list_batch_sessions(self, _batch):
        return self.sessions

    def update_cloud_batch_extra_state(self, *, batch_key, field, value):
        assert batch_key == BATCH
        self.meta[field] = dict(value)

    def get_batch_status(self, _batch):
        return {"batch_key": BATCH, "ready_for_followup_upload": True}

    @staticmethod
    def parse_batch_key(_batch):
        return "2026-10-06", "day"


def service_with_state():
    service = ReviewFollowupTriggerService.__new__(ReviewFollowupTriggerService)
    service._review_service = ReviewState()
    service._build_station_h_cell_values = lambda **_kw: {"ok": True, "cells": {"B2": "2026-10-06"}, "selection": {}}
    service._station_h_duty_focus_service = SimpleNamespace(
        build_status=lambda **_kw: {}, build_image_document=lambda **_kw: {"path": "focus.png"},
    )
    service._cloud_sheet_sync_service = SimpleNamespace(sync_station_h_sheet=Mock(side_effect=lambda **_kw: {
        "status": "success", "h_values_status": "success", "duty_focus_image_synced": True,
    }))
    lock = threading.RLock()
    service._station_110_upload_service = SimpleNamespace(batch_lock=lambda _batch: lock)
    service._resolve_cloud_batch_meta = lambda **_kw: service._review_service.get_cloud_batch(BATCH)
    for method in ("_attach_abcdeh_work_content_sync_result_after_final_building_upload",
                   "_attach_station_110_transformer_bitable_result", "_attach_station_110_sync_result"):
        setattr(service, method, lambda **kw: kw["cloud_result"])
    return service


def test_final_building_rechecks_fresh_batch_instead_of_stale_parent_status():
    service = service_with_state()
    incoming = {"status": "failed", "uploaded_buildings": [], "failed_buildings": [{"building": "附件", "error": "old failure"}]}
    result = service._attach_extra_cloud_sheet_sync_results(
        batch_key=BATCH, sessions=[], cloud_result=incoming, emit_log=lambda _: None,
    )
    assert result["status"] == "failed"
    assert result["station_h_sync"]["status"] == "success"
    assert service._cloud_sheet_sync_service.sync_station_h_sheet.call_count == 1
    assert service._cloud_sheet_sync_service.sync_station_h_sheet.call_args.kwargs["batch_meta"]["spreadsheet_token"] == TOKEN


def test_successful_h_is_not_uploaded_again_after_final_confirm():
    service = service_with_state()
    for _ in range(2):
        result = service._attach_extra_cloud_sheet_sync_results(
            batch_key=BATCH, sessions=[], cloud_result={"status": "ok", "spreadsheet_token": TOKEN}, emit_log=lambda _: None,
        )
        assert result["station_h_sync"]["status"] == "success"
    assert result["station_h_sync"]["reason"] == "already_synced_once"
    assert service._cloud_sheet_sync_service.sync_station_h_sheet.call_count == 1


def test_failed_h_is_retried_then_success_is_kept_once():
    service = service_with_state()
    uploader = service._cloud_sheet_sync_service.sync_station_h_sheet
    uploader.side_effect = [TimeoutError("H upload timeout"), {
        "status": "success", "h_values_status": "success", "duty_focus_image_synced": True,
    }]
    for expected_status in ("failed", "success", "success"):
        result = service._attach_extra_cloud_sheet_sync_results(
            batch_key=BATCH, sessions=[], cloud_result={"status": "ok", "spreadsheet_token": TOKEN}, emit_log=lambda _: None,
        )
        assert result["station_h_sync"]["status"] == expected_status
        assert service._review_service.meta["station_h_sync"]["status"] == expected_status
    assert uploader.call_count == 2


def test_old_cloud_success_does_not_suppress_new_target_upload():
    service = service_with_state()
    service._review_service.meta["station_h_sync"] = {"status": "success", "spreadsheet_token": "old-cloud"}
    result = service._attach_extra_cloud_sheet_sync_results(
        batch_key=BATCH, sessions=[], cloud_result={"status": "ok", "spreadsheet_token": TOKEN}, emit_log=lambda _: None,
    )
    assert result["station_h_sync"]["spreadsheet_token"] == TOKEN
    assert service._cloud_sheet_sync_service.sync_station_h_sheet.call_count == 1


def test_missing_building_cloud_upload_still_blocks_final_h_trigger():
    service = service_with_state()
    service._review_service.sessions[-1]["cloud_sheet_sync"] = {"status": "pending_upload"}
    result = service._attach_extra_cloud_sheet_sync_results(
        batch_key=BATCH, sessions=[], cloud_result={"status": "ok", "spreadsheet_token": TOKEN}, emit_log=lambda _: None,
    )
    assert result["station_h_sync"]["reason"] == "waiting_final_building_upload"
    service._cloud_sheet_sync_service.sync_station_h_sheet.assert_not_called()


def test_image_retry_preserves_already_uploaded_body():
    service = service_with_state()
    service._station_h_duty_focus_service.build_image_document = Mock(side_effect=RuntimeError("missing signature"))
    first = service._attach_extra_cloud_sheet_sync_results(
        batch_key=BATCH, sessions=[], cloud_result={"status": "ok", "spreadsheet_token": TOKEN}, emit_log=lambda _: None,
    )
    assert first["station_h_sync"]["status"] == "partial_failed"
    assert first["station_h_sync"]["h_values_status"] == "success"
    service._station_h_duty_focus_service.build_image_document = lambda **_kw: {"path": "focus.png"}
    second = service._attach_extra_cloud_sheet_sync_results(
        batch_key=BATCH, sessions=[], cloud_result={"status": "ok", "spreadsheet_token": TOKEN}, emit_log=lambda _: None,
    )
    assert second["station_h_sync"]["status"] == "success"
    calls = service._cloud_sheet_sync_service.sync_station_h_sheet.call_args_list
    assert [call.kwargs["skip_values"] for call in calls] == [False, True]


def test_failed_h_survives_result_normalization_and_marks_job_partial_failure():
    service = service_with_state()
    service._collect_followup_progress = lambda **_kw: {"status": "partial_failed"}
    result = service._compose_followup_result(
        batch_key=BATCH, export_result={}, cloud_result={
            "status": "ok", "uploaded_buildings": ["A楼", "B楼", "C楼", "D楼", "E楼"],
            "station_h_sync": {"status": "failed", "error": "H upload timeout"},
        }, daily_report_record_export={}, cabinet_shift_record_export={}, sessions=service._review_service.sessions,
    )
    assert result["status"] == "partial_failed"
    assert result["cloud_sheet_sync"]["status"] == "ok"
    assert result["cloud_sheet_sync"]["station_h_sync"]["error"] == "H upload timeout"
    assert result["failed_buildings"] == [{"building": "H楼", "error": "H upload timeout"}]
    job = JobState(job_id="h-failure", name="cloud catchup", status="success", result=result)
    assert JobService._job_failure_status(job) == "partial_failed"
    assert "H楼: H upload timeout" in JobService._job_failure_detail(job)


def test_h_partial_image_failure_is_visible_in_pending_progress():
    service = service_with_state()
    service._review_service.meta.update({
        "station_h_sync": {"status": "partial_failed", "error": "image timeout"},
        "abcdeh_work_content_sync": {"status": "success"}, "station_110_transformer_bitable": {"status": "success"},
    })
    service._daily_report_state_service = SimpleNamespace(get_export_state=lambda **_kw: {"status": "success"})
    progress = service._collect_followup_progress(batch_key=BATCH, sessions=service._review_service.sessions, ready=True)
    assert progress["extra_sheet_failed_count"] == 1
    assert progress["failed_items"][0]["type"] == "station_h_sync"
    assert progress["failed_items"][0]["tone"] == "danger"


@pytest.mark.parametrize("temperature", [{}, {"B7": 0, "D7": 0}])
def test_missing_shared_temperature_does_not_block_h_body(monkeypatch, temperature):
    service = service_with_state()
    del service._build_station_h_cell_values
    service.config = {}
    service._station_h_cabinet_totals = lambda _sessions: {"ok": True, "totals": {}}
    service._review_service.get_outdoor_temperature_state = lambda **_kw: {"shared_blocks": {"outdoor_temperature": {"cells": temperature}}}
    service._station_h_review_selection_service = SimpleNamespace(resolve_selection=lambda **_kw: {
        "current_people": ["甲", "乙"], "next_people": ["丙", "丁"], "long_day_people": [], "resolved_source": "current",
    })
    monkeypatch.setattr(module, "ShiftRosterRepository", lambda _cfg: Mock())
    result = service._build_station_h_cell_values(batch_key=BATCH, sessions=[], emit_log=lambda _: None)
    assert result["ok"] is True
    assert result["cells"]["B6"] == ("0" if temperature else "")
    assert result["cells"]["G6"] == ("0" if temperature else "")


def test_generation_and_final_confirm_share_batch_lock_and_upload_once():
    service = service_with_state()
    calls = []

    def slow_upload(**_kwargs):
        calls.append(1)
        time.sleep(0.1)
        return {"status": "success", "h_values_status": "success", "duty_focus_image_synced": True}

    service._cloud_sheet_sync_service.sync_station_h_sheet = slow_upload
    with ThreadPoolExecutor(max_workers=2) as pool:
        generation = pool.submit(service.trigger_station_h_sync_after_generation, batch_key=BATCH, emit_log=lambda _: None)
        final = pool.submit(service._attach_extra_cloud_sheet_sync_results, batch_key=BATCH, sessions=[],
                            cloud_result={"status": "ok", "spreadsheet_token": TOKEN}, emit_log=lambda _: None)
        assert generation.result()["station_h_sync"]["status"] == "success"
        assert final.result()["station_h_sync"]["status"] == "success"
    assert len(calls) == 1


def test_catchup_uploads_missing_h_before_unrelated_export_failure():
    service = service_with_state()
    service._existing_cabinet_shift_record_export = lambda _sessions: {"status": "pending"}
    service._existing_daily_report_record_export = lambda _sessions: {"status": "success"}
    service._run_cabinet_shift_record_export = Mock(side_effect=TimeoutError("cabinet export timeout"))
    with pytest.raises(TimeoutError, match="cabinet export timeout"):
        service.upload_pending_cloud_sheets_for_batch(batch_key=BATCH, emit_log=lambda _: None)
    assert service._review_service.meta["station_h_sync"]["status"] == "success"
    assert service._cloud_sheet_sync_service.sync_station_h_sheet.call_count == 1
