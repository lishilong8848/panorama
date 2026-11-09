from __future__ import annotations

from copy import deepcopy
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

import pytest
from openpyxl import Workbook

from handover_log_module.service.handover_110_station_upload_service import Handover110StationUploadService
from handover_log_module.service.handover_attachment_delivery_service import HandoverAttachmentDeliveryService
from handover_log_module.service.handover_cloud_sheet_sync_service import HandoverCloudSheetSyncService
from handover_log_module.service.handover_daily_report_bitable_export_service import HandoverDailyReportBitableExportService
from handover_log_module.service.handover_delivery_state_service import HandoverDeliveryStateService


DUTY = {"duty_date": "2026-11-09", "duty_shift": "day"}
BATCH = "2026-11-09|day"
BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")
ATTACHMENTS = "交接班日志"


def _workbook(note):
    book = Workbook()
    book.active["A1"] = "=1+2"
    sheet = book.create_sheet("110kV")
    for index, label in enumerate(("阿开", "阿家", "1号主变", "2号主变", "3号主变", "4号主变"), start=1):
        sheet.cell(index, 4, label)
        sheet.cell(index, 5, 100 + index)
    book.create_sheet("附页")["A1"] = note
    buffer = BytesIO()
    book.save(buffer)
    book.close()
    return buffer.getvalue()


@pytest.fixture
def offline_delivery(tmp_path, monkeypatch):
    forbidden = Mock(side_effect=AssertionError("Live client, credentials or network access"))
    monkeypatch.setattr(HandoverDailyReportBitableExportService, "_new_client", forbidden)
    monkeypatch.setattr(HandoverCloudSheetSyncService, "_build_client", forbidden)
    monkeypatch.setattr("socket.socket.connect", forbidden)
    monkeypatch.setattr("socket.socket.connect_ex", forbidden)
    config = {
        "_global_paths": {"runtime_state_root": str(tmp_path / "runtime")},
        "daily_report_bitable_export": {"target": {"app_token": "test-base", "table_id": "test-table"}},
    }
    record = {"record_id": "recBatch", "fields": {
        "日期": DUTY["duty_date"],
        "班次": "白班",
        ATTACHMENTS: [{"name": f"{b}_{role}.xlsx", "file_token": f"{b}-{role}"}
                      for b in BUILDINGS for role in ("handover", "capacity")],
    }}
    known = {item["file_token"]: item for item in record["fields"][ATTACHMENTS]}
    uploaded = []
    client = Mock()
    client.list_fields.return_value = [{"field_name": ATTACHMENTS, "type": 17}]
    client.list_records.side_effect = lambda **_: [deepcopy(record)]
    client.get_record_by_id.side_effect = lambda **_: deepcopy(record)
    client.batch_create_records.side_effect = AssertionError("Unexpected new record")
    client.batch_delete_records.side_effect = AssertionError("Unexpected record deletion")

    def upload_attachment(*, file_name, content, mime_type):
        token = f"110-{len(uploaded) + 1}"
        uploaded.append((file_name, content))
        known[token] = {"name": file_name, "file_token": token}
        return token

    def update_record(*, table_id, record_id, fields):
        assert record_id == record["record_id"]
        record["fields"].update({**fields, ATTACHMENTS: [known[item["file_token"]] for item in fields[ATTACHMENTS]]})

    client.upload_attachment_bytes.side_effect = upload_attachment
    client.update_record.side_effect = update_record
    service = Handover110StationUploadService(config)
    service._attachment_delivery_service = HandoverAttachmentDeliveryService(
        config, client=client, output_root=tmp_path / "attachments",
    )
    yield service, HandoverDeliveryStateService(config), client, record, uploaded
    forbidden.assert_not_called()
    client.batch_create_records.assert_not_called()
    client.batch_delete_records.assert_not_called()


def _set_building(state, building, status="uploaded", ready=True, *, batch=BATCH):
    session_id = f"{building}|{batch}"
    state.begin({"session_id": session_id, "batch_key": batch, "building": building}, "generation")
    state.update(session_id, "generation", status=status, artifacts_ready=ready)


def _register_batch(service, *, token="bitable:recBatch"):
    return service._review_service.register_cloud_batch(
        batch_key=BATCH, **DUTY, cloud_batch={
            "status": "prepared", "spreadsheet_token": token, "spreadsheet_url": "https://example.test/recBatch",
        },
    )


@pytest.mark.parametrize("last_manifest", [None, ("ready", True), ("failed", True), ("uploaded", False)])
def test_110_waits_for_uploaded_and_ready_in_the_same_batch(offline_delivery, last_manifest):
    service, state, client, _, _ = offline_delivery
    for building in BUILDINGS[:-1]:
        _set_building(state, building)
    _set_building(state, "E楼", batch="2026-11-09|night")
    if last_manifest:
        _set_building(state, "E楼", *last_manifest)
    original = _workbook("完整原件")

    result = service.upload(**DUTY, filename="110.xlsx", content=original)

    upload = result["upload"]
    assert result["ok"] and upload["status"] == "success"
    assert upload["cloud_sync"]["status"] == "pending"
    assert upload["cloud_sync"]["missing_buildings"] == ["E楼"]
    assert Path(upload["stored_path"]).read_bytes() == original
    assert service.status(**DUTY)["upload"] == upload
    shared = service._state_store.get_shared_block(batch_key=BATCH, block_id="substation_110kv")
    assert shared["payload"]["rows"] == upload["parsed_110kv_rows"]
    assert len(shared["payload"]["rows"]) == 6
    assert not service.retry_cloud_sync(**DUTY)["ok"]
    followup = service.sync_existing_upload_to_cloud(batch_key=BATCH)
    assert (followup["status"], followup["reason"]) == ("pending", "waiting_for_buildings")
    assert client.mock_calls == []
    assert not result["cloud_batch"].get("spreadsheet_token")


def test_110_followup_sends_original_and_reupload_replaces_it(offline_delivery):
    service, state, client, record, uploaded = offline_delivery
    base_attachments = deepcopy(record["fields"][ATTACHMENTS])
    original = _workbook("第一版")
    first = service.upload(**DUTY, filename="第一版.xlsm", content=original)["upload"]
    assert first["cloud_sync"]["status"] == "pending"
    _register_batch(service)
    for building in BUILDINGS:
        _set_building(state, building)

    synced = service.sync_existing_upload_to_cloud(batch_key=BATCH)

    assert synced["status"] == "success"
    assert synced["upload"]["cloud_sync"]["source_file"] == first["stored_path"]
    assert uploaded == [("110_original.xlsm", original)]
    assert service.sync_existing_upload_to_cloud(batch_key=BATCH)["reason"] == "already_synced_same_source"

    revised = _workbook("第二版")
    second = service.upload(**DUTY, filename="第二版.xlsx", content=revised)["upload"]
    assert second["cloud_sync"]["status"] == "success"
    assert second["cloud_sync"]["spreadsheet_token"] == "bitable:recBatch"
    assert second["stored_path"] != first["stored_path"]
    assert Path(first["stored_path"]).read_bytes() == original
    assert Path(second["stored_path"]).read_bytes() == revised
    assert uploaded == [("110_original.xlsm", original), ("110_original.xlsx", revised)]
    assert record["fields"][ATTACHMENTS] == base_attachments + [
        {"name": "110_original.xlsx", "file_token": "110-2"},
    ]
    assert client.update_record.call_count == 2


def test_110_prior_sheet_sync_does_not_skip_delivery_to_new_attachment_record(offline_delivery):
    service, state, _, _, uploaded = offline_delivery
    original = _workbook("旧云表曾同步")
    upload = service.upload(**DUTY, filename="110.xlsx", content=original)["upload"]
    service._save_state(BATCH, {**upload, "cloud_sync": {
        "status": "success", "spreadsheet_token": "legacy-sheet-token", "source_file": upload["stored_path"],
    }})
    _register_batch(service)
    for building in BUILDINGS:
        _set_building(state, building)

    result = service.sync_existing_upload_to_cloud(batch_key=BATCH)

    assert result["status"] == "success"
    assert result["upload"]["cloud_sync"]["spreadsheet_token"] == "bitable:recBatch"
    assert uploaded == [("110_original.xlsx", original)]


def test_110_attachment_transfer_failure_preserves_existing_record(offline_delivery):
    service, state, client, record, _ = offline_delivery
    for building in BUILDINGS:
        _set_building(state, building)
    cached = _register_batch(service)
    first = service.upload(**DUTY, filename="110.xlsx", content=_workbook("已交付"))["upload"]
    assert first["cloud_sync"]["status"] == "success"
    old_record = deepcopy(record)
    client.reset_mock()
    transfer = client.upload_attachment_bytes.side_effect
    client.upload_attachment_bytes.side_effect = OSError("mock upload failure")

    revised = _workbook("待补传")
    result = service.upload(**DUTY, filename="110.xlsx", content=revised)

    assert result["ok"] and result["upload"]["status"] == "success"
    assert result["upload"]["cloud_sync"]["status"] == "failed"
    assert result["upload"]["cloud_sync"]["error"] == "mock upload failure"
    assert Path(result["upload"]["stored_path"]).read_bytes() == revised
    assert result["cloud_batch"] == cached
    assert record == old_record
    client.update_record.assert_not_called()

    client.upload_attachment_bytes.side_effect = transfer
    assert service.retry_cloud_sync(**DUTY)["ok"]
    assert record["fields"][ATTACHMENTS][:-1] == old_record["fields"][ATTACHMENTS][:-1]
    assert record["fields"][ATTACHMENTS][-1] == {"name": "110_original.xlsx", "file_token": "110-2"}


def test_110_never_creates_record_from_legacy_cloud_metadata(offline_delivery):
    service, state, client, _, _ = offline_delivery
    for building in BUILDINGS:
        _set_building(state, building)
    _register_batch(service, token="legacy-sheet-token")

    result = service.upload(**DUTY, filename="110.xlsx", content=_workbook("待补传"))

    assert result["upload"]["cloud_sync"]["status"] == "pending"
    assert result["upload"]["cloud_sync"]["reason"] == "waiting_for_attachment_record"
    assert result["cloud_batch"] == {}
    assert client.mock_calls == []
