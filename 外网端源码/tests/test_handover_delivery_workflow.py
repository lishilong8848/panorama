from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from concurrent.futures import ThreadPoolExecutor

import pytest

from handover_log_module.service import handover_delivery_workflow_service as workflow_module
from handover_log_module.service.handover_attachment_store import HandoverAttachmentStore
from handover_log_module.service.handover_delivery_state_service import HandoverDeliveryStateService
from handover_log_module.service.handover_attachment_delivery_service import HandoverAttachmentDeliveryService
from handover_log_module.service import review_followup_trigger_service as followup_module
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableRateLimited


BATCH = "2026-11-09|day"
DATE, SHIFT = BATCH.split("|")
FIELD = HandoverAttachmentStore.FIELD


@pytest.fixture
def bitable():
    client = Mock()
    client.list_fields.return_value = [{"field_name": FIELD, "type": 17}]
    record = {"record_id": "recTest", "fields": {"日期": DATE, "班次": "白班"}}
    records = []
    known = {}

    def create(**_):
        records.append(record)
        return [{"data": {"records": [deepcopy(record)]}}]

    def upload(*, file_name, content, **_):
        token = f"token:{file_name}:{content.hex()}"
        known[token] = {"name": file_name, "file_token": token}
        return token

    def update(*, record_id, fields, **_):
        assert record_id == record["record_id"]
        fields = deepcopy(fields)
        if FIELD in fields:
            fields[FIELD] = [known[item["file_token"]] for item in fields[FIELD]]
        record["fields"].update(fields)

    client.batch_create_records.side_effect = create
    client.list_records.side_effect = lambda **_: deepcopy(records)
    client.get_record_by_id.side_effect = lambda **_: deepcopy(record)
    client.upload_attachment_bytes.side_effect = upload
    client.update_record.side_effect = update
    client.batch_delete_records.side_effect = AssertionError("must not delete records")
    return HandoverAttachmentStore({"daily_report_bitable_export": {"target": {
        "app_token": "test-base", "table_id": "test-table",
    }}}, client=client), client, record


def test_attachment_update_replaces_only_same_deliverable_and_reuses_record(bitable, tmp_path):
    store, client, record = bitable
    local = tmp_path / "data.xlsx"
    local.write_bytes(b"original")
    created = store.ensure_record(DATE, SHIFT)
    store.upload_files(DATE, SHIFT, {"A楼_handover.xlsx": local, "H_handover.xlsx": local})
    local.write_bytes(b"revised")
    store.upload_files(DATE, SHIFT, {"A楼_handover.xlsx": local})
    assert store.ensure_record(DATE, SHIFT)["record_id"] == created["record_id"]
    assert record["fields"]["交接班日报"]["link"] == created["record_url"]
    assert {item["name"]: item["file_token"].split(":")[-1] for item in record["fields"][FIELD]} == {
        "H_handover.xlsx": b"original".hex(), "A楼_handover.xlsx": b"revised".hex(),
    }
    client.batch_create_records.assert_called_once()
    client.batch_delete_records.assert_not_called()


def test_failed_transfer_or_stale_generation_never_updates_attachment_field(bitable, tmp_path):
    store, client, record = bitable
    local = tmp_path / "data.xlsx"
    local.write_bytes(b"content")
    store.upload_files(DATE, SHIFT, {"A楼_handover.xlsx": local})
    original = deepcopy(record)
    client.update_record.reset_mock()
    with pytest.raises(RuntimeError, match="旧版本"):
        store.upload_files(DATE, SHIFT, {"A楼_handover.xlsx": local}, is_current=lambda: False)
    client.upload_attachment_bytes.side_effect = RuntimeError("simulated upload failure")
    with pytest.raises(RuntimeError, match="simulated upload failure"):
        store.upload_files(DATE, SHIFT, {"A楼_handover.xlsx": local})
    assert record == original
    client.update_record.assert_not_called()


def test_attachment_does_not_replace_record_moved_to_another_shift(bitable, tmp_path):
    store, client, record = bitable
    local = tmp_path / "data.xlsx"
    local.write_bytes(b"content")
    existing = store.ensure_record(DATE, SHIFT)
    record["fields"]["班次"] = "夜班"
    client.update_record.reset_mock()
    with pytest.raises(RuntimeError, match="日期或班次已变化"):
        store.upload_files(DATE, SHIFT, {"H_handover.xlsx": local}, expected_record_id=existing["record_id"])
    client.update_record.assert_not_called()


def test_cooldown_is_shared_by_stores_for_the_same_table(bitable, tmp_path):
    store, client, _ = bitable
    config = {"daily_report_bitable_export": {"target": {"app_token": str(tmp_path), "table_id": "test-table"}}}
    first = HandoverAttachmentStore(config, client=client)
    second = HandoverAttachmentStore(config, client=client)
    client.list_fields.side_effect = FeishuBitableRateLimited("rate limited", retry_after=30)
    with pytest.raises(FeishuBitableRateLimited, match="rate limited"):
        first.ensure_record(DATE, SHIFT)
    with pytest.raises(FeishuBitableRateLimited, match="冷却"):
        second.ensure_record(DATE, SHIFT)
    client.list_fields.assert_called_once()


def test_each_floor_publishes_independently_and_only_reuploads_changed_pair(tmp_path, monkeypatch, bitable):
    store, client, record = bitable
    workflow = object.__new__(workflow_module.HandoverDeliveryWorkflowService)
    workflow.config = {}
    workflow.state = HandoverDeliveryStateService({"paths": {"runtime_state_root": tmp_path}})
    sessions = [{"building": building, "session_id": f"{building}|{BATCH}", "batch_key": BATCH,
                 "revision": 1, "confirmed": True} for building in workflow.BUILDINGS]
    workflow.review = SimpleNamespace(
        list_batch_sessions=lambda _: sessions,
        get_session_by_id=lambda target: next(s for s in sessions if s["session_id"] == target),
        get_cloud_batch_fast=lambda _: {},
        register_cloud_batch=Mock(), update_cloud_sheet_sync=Mock(),
    )
    monkeypatch.setattr(workflow_module, "HandoverAttachmentStore", lambda _: store)
    artifacts = {}
    for key in ("log", "capacity", "image", "text"):
        path = tmp_path / key
        path.write_bytes(key.encode())
        artifacts[key] = str(path)

    def ready(session, generation):
        workflow.state.begin(session, generation)
        workflow.state.update(session["session_id"], generation, status="ready",
                              artifacts_ready=True, revision=session["revision"], paths=artifacts)

    assert workflow.publish_ready_batch(BATCH, lambda _: None, building="A楼")["status"] == "pending"
    ready(sessions[0], "first")
    first = workflow.publish_ready_batch(BATCH, lambda _: None, building="A楼")
    assert first["uploaded_buildings"] == ["A楼"]
    assert first["missing_buildings"] == list(workflow.BUILDINGS[1:])
    assert not first["all_buildings_uploaded"]
    assert len(record["fields"][FIELD]) == 2

    for row in sessions[1:]:
        ready(row, "first")
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(
            lambda b: workflow.publish_ready_batch(BATCH, lambda _: None, building=b), workflow.BUILDINGS[1:],
        ))
    assert sum(result["all_buildings_uploaded"] for result in results) == 1
    assert len(record["fields"][FIELD]) == 10
    assert all(workflow.state.for_session(row)["status"] == "uploaded" for row in sessions)

    sessions[0]["revision"] = 2
    assert workflow.state.for_session(sessions[0])["status"] == "stale"
    assert workflow.publish_ready_batch(BATCH, lambda _: None, building="A楼")["status"] == "pending"
    ready(sessions[0], "second")
    client.upload_attachment_bytes.reset_mock()
    assert workflow.publish_ready_batch(BATCH, lambda _: None)["uploaded_buildings"] == ["A楼"]
    assert client.upload_attachment_bytes.call_count == 2
    assert len(record["fields"][FIELD]) == 10
    client.batch_create_records.assert_called_once()

    original = deepcopy(record["fields"][FIELD])
    sessions[0]["revision"] = 3
    ready(sessions[0], "third")
    client.upload_attachment_bytes.side_effect = OSError("mock upload failure")
    failed = workflow.publish_ready_batch(BATCH, lambda _: None, building="A楼")
    assert failed["status"] == "partial_failed"
    assert failed["failed_buildings"][0]["building"] == "A楼"
    assert record["fields"][FIELD] == original
    assert workflow.state.for_session(sessions[0])["upload_error"] == "mock upload failure"
    assert all(workflow.state.for_session(row)["status"] == "uploaded" for row in sessions[1:])


@pytest.mark.parametrize("complete", [False, True])
def test_batch_supplements_only_run_when_all_five_uploaded(complete, tmp_path, monkeypatch):
    service = object.__new__(followup_module.ReviewFollowupTriggerService)
    service.config = {"paths": {"runtime_state_root": tmp_path}}
    service._daily_report_state_service = SimpleNamespace(update_export_state=Mock())
    service._review_service = SimpleNamespace(list_batch_sessions=lambda _: [])
    service._managed_building_cloud_sync_gaps = lambda _: []
    service._maybe_mark_first_full_cloud_sync_completed = Mock()
    service._attach_extra_cloud_sheet_sync_results = Mock(side_effect=lambda **kw: kw["cloud_result"])
    service._run_cabinet_shift_record_export = Mock(return_value={})
    publish = Mock(return_value={"status": "ok", "all_buildings_uploaded": complete, "record_id": "recTest",
                                 "record_url": "https://example.test/base?record=recTest"})
    monkeypatch.setattr(followup_module, "HandoverDeliveryWorkflowService", lambda _: SimpleNamespace(publish_ready_batch=publish))
    service.publish_attachment_delivery(batch_key=BATCH, building="A楼")
    assert publish.call_args.kwargs["building"] == "A楼"
    assert service._attach_extra_cloud_sheet_sync_results.called is complete


@pytest.mark.parametrize("shift", ["day", "night"])
def test_scheduled_confirmation_prepares_unconfirmed_buildings_before_publishing(monkeypatch, shift):
    service = object.__new__(followup_module.ReviewFollowupTriggerService)
    service.config = {}
    service._cloud_sheet_sync_service = HandoverAttachmentDeliveryService({})
    batch_key = f"{DATE}|{shift}"
    rows = [{"building": building, "confirmed": False} for building in workflow_module.HandoverDeliveryWorkflowService.BUILDINGS]
    service._review_service = SimpleNamespace(
        list_batch_sessions=lambda key: rows if key == batch_key else [],
        build_batch_key=lambda duty_date, duty_shift: f"{duty_date}|{duty_shift}",
        mark_confirmed=Mock(side_effect=AssertionError("cannot confirm before artifact preparation")),
    )
    calls = []

    def prepare(sessions, emit, *, confirm_unconfirmed):
        assert sessions == rows and confirm_unconfirmed is True
        calls.append("prepare")
        return []

    def publish(*, batch_key, emit_log):
        assert batch_key == f"{DATE}|{shift}"
        calls.append("publish")
        return {"status": "ok", "record_url": "https://example.test/base?record=recTest"}

    monkeypatch.setattr(followup_module, "HandoverDeliveryWorkflowService",
                        lambda _: SimpleNamespace(prepare_sessions=prepare))
    service.publish_attachment_delivery = publish
    result = service.upload_pending_cloud_sheets_for_duty(duty_date=DATE, duty_shift=shift, emit_log=lambda _: None)
    assert calls == ["prepare", "publish"]
    assert result["cloud_sheet_sync"]["status"] == "ok"
    service._review_service.mark_confirmed.assert_not_called()


def test_prepared_download_requires_current_generation(monkeypatch, tmp_path):
    from fastapi import HTTPException
    from app.modules.handover_review.api import routes

    config = {"paths": {"runtime_state_root": tmp_path}}
    session = {"building": "A楼", "session_id": f"A楼|{BATCH}", "batch_key": BATCH,
               "revision": 2, "confirmed": True, "output_file": "审核日志.xlsx"}
    state = HandoverDeliveryStateService(config)
    state.begin(session, "new")
    service = SimpleNamespace(config=config)
    monkeypatch.setattr(routes, "_build_review_session_service", lambda _: service)
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *_: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *_, **__: session)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=object())))

    with pytest.raises(HTTPException) as pending:
        routes.handover_review_delivery_download("a", request, session["session_id"], "new")
    assert pending.value.status_code == 409
    artifacts = {}
    for name in ("log", "capacity", "image", "text"):
        path = tmp_path / name
        path.write_bytes(b"prepared")
        artifacts[name] = str(path)
    state.update(session["session_id"], "new", status="ready", artifacts_ready=True, revision=2, paths=artifacts)
    with pytest.raises(HTTPException) as stale:
        routes.handover_review_delivery_download("a", request, session["session_id"], "old")
    assert stale.value.status_code == 409
    response = routes.handover_review_delivery_download("a", request, session["session_id"], "new")
    assert Path(response.path) == Path(artifacts["log"])


def test_batch_retry_migrates_legacy_success_instead_of_skipping(monkeypatch, tmp_path):
    service = object.__new__(followup_module.ReviewFollowupTriggerService)
    service.config = {"paths": {"runtime_state_root": tmp_path}}
    service._cloud_sheet_sync_service = HandoverAttachmentDeliveryService(service.config)
    rows = [{"building": "A楼", "session_id": f"A楼|{BATCH}", "confirmed": True, "revision": 3,
             "cloud_sheet_sync": {"status": "success", "synced_revision": 3, "spreadsheet_token": "old_sheet"}}]
    service._review_service = SimpleNamespace(
        list_batch_sessions=lambda _: rows, get_batch_status=lambda _: {},
        get_cloud_batch=lambda _: {"spreadsheet_token": "old_sheet", "spreadsheet_url": "https://example.test/sheets/old"},
    )
    prepare = Mock(return_value=[])
    monkeypatch.setattr(followup_module, "HandoverDeliveryWorkflowService", lambda _: SimpleNamespace(prepare_sessions=prepare))
    service.publish_attachment_delivery = Mock(return_value={"status": "pending", "missing_buildings": ["B楼"]})
    emit = lambda _: None

    summary = service._summarize_cloud_sheet_sync(batch_key=BATCH, sessions=rows)
    assert summary["uploaded_buildings"] == [] and summary["spreadsheet_url"] == ""
    result = service.retry_failed_cloud_sheet_in_batch(BATCH, emit)
    prepare.assert_called_once_with(rows, emit)
    assert result["status"] == "pending"


def test_supplement_reuploads_after_building_generation_changes():
    check = followup_module.ReviewFollowupTriggerService._station_h_sync_complete
    state = {"status": "success", "spreadsheet_token": "bitable:rec", "source_signature": "old"}
    assert check(state, "bitable:rec", "old")
    assert not check(state, "bitable:rec", "new")


def test_h_attachment_creates_batch_and_replaces_only_h_files(bitable, tmp_path):
    from PIL import Image

    store, client, record = bitable
    source = tmp_path / "local.xlsx"
    source.write_bytes(b"mock prepared Excel")
    service = HandoverAttachmentDeliveryService(
        {"cloud_sheet_sync": {"enabled": False}, "daily_report_bitable_export": {"enabled": True}},
        store=store, output_root=tmp_path / "supplement",
    )
    image = tmp_path / "focus.png"
    Image.new("RGB", (200, 300), "white").save(image)
    meta = {"batch_key": BATCH}

    sent = service.sync_station_h_sheet(
        batch_meta=meta, cell_values={"B2": DATE, "F2": "白班"}, duty_focus_image_path=image,
    )

    assert sent["status"] == "success" and sent["duty_focus_image_synced"]
    assert {item["name"] for item in record["fields"][FIELD]} == {"H_handover.xlsx", "H_duty_focus.png"}
    client.batch_create_records.assert_called_once()
    store.upload_files(DATE, SHIFT, {"A楼_handover.xlsx": source, "110_original.xlsx": source})
    first = {item["name"]: item["file_token"] for item in record["fields"][FIELD]}
    second = service.sync_station_h_sheet(
        batch_meta=meta, cell_values={"B2": DATE, "F2": "白班", "C3": "新值班人员"},
        duty_focus_image_path=image,
    )
    assert second["record_id"] == sent["record_id"]
    latest = {item["name"]: item["file_token"] for item in record["fields"][FIELD]}
    assert set(latest) == set(first)
    assert latest["H_handover.xlsx"] != first["H_handover.xlsx"]
    assert latest["A楼_handover.xlsx"] == first["A楼_handover.xlsx"]
    assert latest["110_original.xlsx"] == first["110_original.xlsx"]
    client.batch_create_records.assert_called_once()
    client.update_record.reset_mock()
    stale = service.sync_station_h_sheet(
        batch_meta=meta, cell_values={"B2": DATE, "F2": "白班"}, duty_focus_image_path=image, is_current=lambda: False,
    )
    assert stale["status"] == "failed" and "版本已更新" in stale["error"]
    client.update_record.assert_not_called()
    client.update_record.reset_mock()
    rejected = service.sync_station_h_sheet(batch_meta={**meta, "spreadsheet_token": "old_sheet"}, cell_values={"B2": DATE})
    assert rejected["status"] == "failed"
    client.update_record.assert_not_called()


def test_manual_h_upload_uses_saved_content_without_other_building_sessions(bitable, tmp_path, monkeypatch):
    from PIL import Image
    from openpyxl import load_workbook

    store, client, record = bitable
    service = object.__new__(followup_module.ReviewFollowupTriggerService)
    service.config = {}
    service._review_service = SimpleNamespace(
        parse_batch_key=lambda batch: tuple(batch.split("|")),
        list_batch_sessions=Mock(return_value=[]),
        get_cloud_batch=Mock(return_value=None),
        get_outdoor_temperature_state=Mock(return_value={}),
        register_cloud_batch=Mock(),
    )
    selection = {
        "current_people": ["值班甲", "值班乙"], "next_people": ["接班甲", "接班乙"],
        "long_day_people": ["长白甲", "长白乙"], "resolved_source": "current",
        "training_rows": [{"cells": {"B": "本班已保存培训"}}],
    }
    service._station_h_review_selection_service = SimpleNamespace(
        get_selection=Mock(return_value=selection), resolve_selection=Mock(return_value=selection),
    )
    service._cloud_sheet_sync_service = HandoverAttachmentDeliveryService(
        {}, store=store, output_root=tmp_path / "supplement",
    )
    image = tmp_path / "focus.png"
    Image.new("RGB", (200, 300), "white").save(image)
    service._station_h_duty_focus_service = SimpleNamespace(
        build_status=Mock(return_value={"checks": {"H19": "20℃/18℃"}}),
        build_image_document=Mock(return_value={"path": image}),
    )
    training = Mock(side_effect=AssertionError("manual upload must not refresh training"))
    monkeypatch.setattr(followup_module, "TrainingManagementPayloadBuilder", training)

    result = service.publish_station_h_attachment(batch_key=BATCH, historical=True, emit_log=Mock())

    assert result["status"] == "success", result
    assert "机柜汇总已留空" in result["warning"]
    service._station_h_duty_focus_service.build_image_document.assert_called_once()
    assert service._station_h_duty_focus_service.build_image_document.call_args.kwargs["force"] is True
    assert service._station_h_duty_focus_service.build_status.call_args.kwargs["historical"] is True
    service._review_service.register_cloud_batch.assert_called_once()
    assert service._review_service.register_cloud_batch.call_args.kwargs["update_latest"] is False
    assert {item["name"] for item in record["fields"][FIELD]} == {"H_handover.xlsx", "H_duty_focus.png"}
    training.assert_not_called()
    workbook = load_workbook(result["source_file"])
    try:
        sheet = workbook["H楼"]
        assert sheet["C3"].value == "值班甲"
        assert sheet["D3"].value == "值班乙"
        assert sheet["B31"].value == "本班已保存培训"
        assert sheet["B12"].value is None
        assert len(sheet._images) == 1
    finally:
        workbook.close()


def test_station_h_upload_submits_selected_historical_shift_only(monkeypatch):
    from app.modules.handover_review.api import routes

    start = Mock(return_value={"job_id": "job-h"})
    selected = {"current_people": ["值班甲"]}
    selection_service = SimpleNamespace(get_selection=Mock(return_value=selected))
    monkeypatch.setattr(routes, "_handover_cfg", lambda _: {})
    monkeypatch.setattr(routes, "_start_handover_background_job", start)
    monkeypatch.setattr(routes, "_build_station_h_review_selection_service", lambda _: selection_service)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=object())))

    payload = {"duty_date": "2026-11-08", "duty_shift": "night"}
    result = routes.handover_review_station_h_upload(request, payload)

    assert result["job"]["job_id"] == "job-h"
    assert start.call_args.kwargs["worker_payload"] == {
        "batch_key": "2026-11-08|night", "manual_upload": True, "historical": True,
    }
    dedupe_key = start.call_args.kwargs["dedupe_key"]
    assert dedupe_key.startswith("handover:station-h-upload:2026-11-08|night:")
    routes.handover_review_station_h_upload(request, payload)
    assert start.call_args.kwargs["dedupe_key"] == dedupe_key
    selected["current_people"] = ["值班乙"]
    routes.handover_review_station_h_upload(request, payload)
    assert start.call_args.kwargs["dedupe_key"] != dedupe_key
    assert start.call_args.kwargs["resource_keys"] == ["handover_station_h:2026-11-08|night"]
    with pytest.raises(routes.HTTPException) as error:
        routes.handover_review_station_h_upload(request, {"duty_date": "2026-11-31", "duty_shift": "night"})
    assert error.value.status_code == 400
    assert start.call_count == 3


def test_ae_upload_supplements_no_longer_trigger_h_attachment():
    service = object.__new__(followup_module.ReviewFollowupTriggerService)
    service._cloud_sheet_sync_service = object.__new__(HandoverAttachmentDeliveryService)
    service._attach_station_h_sync_result_after_final_building_upload = Mock()
    passthrough = lambda **kwargs: kwargs["cloud_result"]
    service._attach_abcdeh_work_content_sync_result_after_final_building_upload = Mock(side_effect=passthrough)
    service._attach_station_110_transformer_bitable_result = Mock(side_effect=passthrough)
    service._attach_station_110_sync_result = Mock(side_effect=passthrough)

    service._attach_extra_cloud_sheet_sync_results_locked(
        batch_key=BATCH, sessions=[], cloud_result={"status": "ok"}, emit_log=Mock(),
    )
    service._attach_station_h_sync_result_after_final_building_upload.assert_not_called()
    service._attach_abcdeh_work_content_sync_result_after_final_building_upload.assert_called_once()
    service._attach_station_110_transformer_bitable_result.assert_called_once()
    service._attach_station_110_sync_result.assert_called_once()
