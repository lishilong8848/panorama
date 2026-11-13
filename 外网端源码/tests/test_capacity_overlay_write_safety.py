from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from openpyxl import Workbook, load_workbook

from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService


def test_capacity_download_snapshot_requires_current_inputs_and_file(tmp_path, monkeypatch):
    path = tmp_path / "capacity.xlsx"
    path.write_bytes(b"first")
    service = HandoverCapacityReportService({})
    live = {"inputs": "same"}
    monkeypatch.setattr(service, "download_inputs_signature", lambda **_: live["inputs"])
    monkeypatch.setattr(
        "handover_log_module.service.handover_capacity_report_service.time",
        type("Clock", (), {"time": staticmethod(lambda: 110)})(),
    )
    snapshot = {"revision": 7, "inputs": "same", "file": service._capacity_file_fingerprint(str(path)), "verified_at": 100}
    matches = service.download_snapshot_matches
    assert matches(snapshot, revision=7, output_file=str(path))
    assert not matches(snapshot, revision=8, output_file=str(path))
    assert not matches({**snapshot, "verified_at": 1}, revision=7, output_file=str(path))
    live["inputs"] = "changed"
    assert not matches(snapshot, revision=7, output_file=str(path))
    live["inputs"] = "same"
    path.write_bytes(b"new file")
    assert not matches(snapshot, revision=7, output_file=str(path))
from handover_log_module.service.handover_delivery_state_service import HandoverDeliveryStateService
from handover_log_module.service.handover_xlsx_write_queue_service import HandoverXlsxWriteQueueService


def test_incomplete_overlay_does_not_modify_existing_report(tmp_path) -> None:
    path = tmp_path / "capacity.xlsx"
    workbook = Workbook()
    workbook.active["A1"] = "original"
    workbook.save(path)
    workbook.close()

    result = HandoverCapacityReportService({}).sync_overlay_for_existing_report_from_cells(
        building="A楼",
        duty_date="2026-08-13",
        duty_shift="day",
        handover_cells={},
        capacity_output_file=str(path),
        emit_log=lambda _text: None,
    )

    assert result["status"] == "pending_input"
    checked = load_workbook(path)
    try:
        assert checked.active["A1"].value == "original"
    finally:
        checked.close()


def test_delivery_overlay_keeps_its_inputs_when_an_autosave_overlay_is_queued(tmp_path, monkeypatch):
    queue = HandoverXlsxWriteQueueService({"_global_paths": {"runtime_state_root": str(tmp_path)}})
    monkeypatch.setattr(queue, "_start_worker", lambda _building: None)
    session = {"building": "A楼", "session_id": "A楼|2026-11-09|night", "capacity_output_file": "capacity.xlsx"}
    delivery = queue.enqueue_capacity_overlay_sync(
        session, tracked_cells={"H6": "12"}, use_saved_shared_110kv=True, expected_document_revision=3,
    )
    autosave = queue.enqueue_capacity_overlay_sync(session, tracked_cells={"H6": "13"})
    assert delivery["job_id"] != autosave["job_id"]
    saved = queue._store("A楼").get_xlsx_write_job(delivery["job_id"])
    assert saved["payload"]["tracked_cells"]["H6"] == "12"
    assert saved["payload"]["expected_document_revision"] == 3


def test_delivery_pending_inputs_report_missing_cells_without_modifying_capacity(tmp_path):
    path = tmp_path / "capacity.xlsx"
    path.write_bytes(b"unmodified workbook")
    session = {"building": "A楼", "session_id": "A楼|2026-11-09|night", "revision": 3,
               "capacity_output_file": str(path)}
    review = SimpleNamespace(get_saved_substation_110kv_block=lambda **_: {}, update_capacity_sync=Mock())
    document_state = SimpleNamespace(load_document=lambda _, **_kwargs: ({}, session))
    queue = HandoverXlsxWriteQueueService({}, review_service=review, document_state=document_state)

    with pytest.raises(RuntimeError, match="缺少H6.*补齐并保存"):
        queue._execute_capacity_overlay(session=session, payload={
            "use_saved_shared_110kv": True, "expected_document_revision": 3,
        })
    assert review.update_capacity_sync.call_args.kwargs["capacity_sync"]["status"] == "pending_input"
    assert path.read_bytes() == b"unmodified workbook"

    review.update_capacity_sync.reset_mock()
    with pytest.raises(RuntimeError, match="审核文档已变更"):
        queue._execute_capacity_overlay(session=session, payload={"expected_document_revision": 2})
    review.update_capacity_sync.assert_not_called()


def test_prepare_failure_does_not_display_as_attachment_upload_failure():
    prepare_error = {"status": "failed", "error": "capacity input missing", "artifacts_ready": False}
    assert HandoverDeliveryStateService.sync_view(prepare_error, {})["status"] == "idle"
    upload_error = {"status": "ready", "upload_error": "remote upload failed", "artifacts_ready": True}
    assert HandoverDeliveryStateService.sync_view(upload_error, {})["error"] == "remote upload failed"
