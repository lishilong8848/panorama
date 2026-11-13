from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import openpyxl
import pytest

from app.modules.report_pipeline.service import energy_report_upload_service as module
from app.worker.task_handlers import handle_energy_report_upload


def test_wiki_target_uses_underlying_bitable_token_for_attachment(monkeypatch):
    resolver = Mock()
    resolver.resolve.return_value = {"app_token": "base-object-token", "wiki_obj_type": "bitable"}
    monkeypatch.setattr(module, "require_feishu_auth_settings", lambda _config: {"app_id": "test", "app_secret": "test"})
    monkeypatch.setattr(module, "BitableTargetResolver", lambda **_auth: resolver)
    factory = Mock()
    monkeypatch.setattr(module, "FeishuBitableClient", factory)
    module.EnergyReportUploadService({}).client()
    assert resolver.resolve.call_args.args[0]["wiki_url"].endswith(f"/wiki/{module.TARGET_TOKEN}?table={module.TABLE_ID}")
    assert factory.call_args.kwargs["app_token"] == "base-object-token"


def test_wiki_target_rejects_invalid_or_unresolved_object(monkeypatch):
    resolver = Mock()
    resolver.resolve.return_value = {"app_token": module.TARGET_TOKEN, "wiki_obj_type": "bitable"}
    monkeypatch.setattr(module, "require_feishu_auth_settings", lambda _config: {"app_id": "test", "app_secret": "test"})
    monkeypatch.setattr(module, "BitableTargetResolver", lambda **_auth: resolver)
    factory = Mock()
    monkeypatch.setattr(module, "FeishuBitableClient", factory)
    with pytest.raises(RuntimeError, match="未上传附件"):
        module.EnergyReportUploadService({}).client()
    factory.assert_not_called()


@pytest.fixture
def clients(tmp_path):
    fields = [{"field_name": "文本", "type": 1, "is_primary": True}]
    rows = []
    bridge = Mock()
    bitable = Mock(LIST_FIELD_URL="/apps/{app_token}/{table_id}/fields", app_token="test-app")
    bitable.list_fields.side_effect = lambda _: list(fields)
    bitable._post_json.side_effect = lambda _, payload: fields.append(payload)
    bitable.list_records.side_effect = lambda *_, **__: list(rows)
    bitable.upload_attachment_bytes.side_effect = lambda name, *_: name
    bitable.create_record.side_effect = lambda _, fields, **kw: rows.append(
        {"record_id": f"test{len(rows)}", "fields": fields},
    )
    bitable.update_record.side_effect = lambda _, record_id, fields: next(
        row for row in rows if row["record_id"] == record_id
    )["fields"].update(fields)
    workbook = openpyxl.Workbook()
    workbook.active.append(["2026-10"])
    template = tmp_path / "sample.xlsx"
    workbook.save(template)
    workbook.close()

    def sources(queries, **_):
        result = []
        for q in queries:
            relative = Path(q["source_family"]) / f'{q["building"]}.xlsx'
            path = tmp_path / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(template.read_bytes())
            result.append({"ok": True, "entries": [{
                **q, "relative_path": relative.as_posix(), "bucket_key": "2026-10", "file_verified": True,
            }]})
        return result

    bridge.source_index_batch.side_effect = sources
    service = module.EnergyReportUploadService(
        {"paths": {"runtime_state_root": str(tmp_path)}, "shared_bridge": {"external_root_dir": str(tmp_path)}},
        internal=bridge, bitable=bitable,
    )
    return service, bridge, bitable, rows


def test_monthly_schedule_is_due_at_day_one_and_crosses_year():
    assert module.due_period(datetime(2027, 1, 1, 9, 4, 59)) == ""
    assert module.due_period(datetime(2027, 1, 1, 9, 5)) == "2026-12"
    assert module.due_period(datetime(2027, 1, 3, 0, 0)) == "2026-12"


def test_upload_creates_five_records_and_skips_identical_rerun(clients):
    service, bridge, bitable, rows = clients
    first = service.run("2026-10", log=lambda _: None)
    assert first["updated_buildings"] == 5
    assert len(bridge.source_index_batch.call_args.args[0]) == 10
    assert bitable.upload_attachment_bytes.call_count == 10
    assert {row["fields"]["楼栋"] for row in rows} == set(module.BUILDINGS)
    assert all(
        row["fields"]["月份"] == "2026-10" and
        all(len(row["fields"][name]) == 1 for name in module.ATTACHMENTS.values()) for row in rows
    )
    assert module.completed(service.config, "2026-10")
    assert service.run("2026-10", log=lambda _: None)["updated_buildings"] == 0
    assert bitable.upload_attachment_bytes.call_count == 10
    assert bitable.create_record.call_count == 5
    bitable.update_record.assert_not_called()


def test_worker_reuses_serializable_preflight_manifest(clients, monkeypatch):
    import json

    service, bridge, bitable, rows = clients
    paths = service.source_paths("2026-10", request_missing=True)
    payload = json.loads(json.dumps({"period": "2026-10", "source_manifest": module.source_manifest(paths)}))
    bridge.source_index_batch.side_effect = AssertionError("worker must not query index again")
    monkeypatch.setattr(module, "EnergyReportUploadService", lambda _: service)
    result = handle_energy_report_upload(service.config, payload, lambda _: None)
    assert result["status"] == "success"
    bridge.source_index_batch.assert_called_once()
    assert len(rows) == 5 and bitable.upload_attachment_bytes.call_count == 10


def test_changed_files_wait_and_request_only_targeted_repair(clients, monkeypatch):
    from unittest.mock import call

    service, bridge, bitable, _ = clients
    paths = service.source_paths("2026-10", request_missing=True)
    manifest = module.source_manifest(paths)
    paths["A楼", "pue_confirmation_family"].unlink()
    paths["E楼", "utility_feeder_deviation_family"].write_bytes(b"broken")
    bridge.refresh_latest_source_cache.side_effect = module.InternalBridgeHttpError("内网端 HTTP 请求失败: timed out")
    monkeypatch.setattr(module, "EnergyReportUploadService", lambda _: service)
    result = handle_energy_report_upload(service.config, {
        "period": "2026-10", "source_manifest": manifest,
    }, lambda _: None)
    assert result["status"] == "waiting_resource"
    assert "A楼" in result["detail"] and "E楼" in result["detail"]
    assert bridge.refresh_latest_source_cache.call_args_list == [
        call(source_family="pue_confirmation_family", buildings=["A楼"], target_bucket_key="2026-10"),
        call(source_family="utility_feeder_deviation_family", buildings=["E楼"], target_bucket_key="2026-10", force_refresh=True),
    ]
    bridge.source_index_batch.assert_called_once()
    bitable.list_fields.assert_not_called()
    bitable.upload_attachment_bytes.assert_not_called()


@pytest.mark.parametrize("message", [
    "内网端 HTTP 请求失败: timed out",
    "内网端 source-index/batch 正在处理其他请求; retry_after_sec=60",
])
def test_legacy_worker_with_no_manifest_defers_transient_index_errors(clients, monkeypatch, message):
    service, bridge, bitable, _ = clients
    bridge.source_index_batch.side_effect = module.InternalBridgeHttpError(message)
    monkeypatch.setattr(module, "EnergyReportUploadService", lambda _: service)
    assert handle_energy_report_upload(service.config, {"period": "2026-10"}, lambda _: None)["status"] == "waiting_resource"
    bridge.refresh_latest_source_cache.assert_not_called()
    bitable.list_fields.assert_not_called()


def test_no_source_retry_on_nontransient_bridge_or_write_error(clients, monkeypatch):
    service, bridge, bitable, _ = clients
    monkeypatch.setattr(module, "EnergyReportUploadService", lambda _: service)
    sources = bridge.source_index_batch.side_effect
    bridge.source_index_batch.side_effect = module.InternalBridgeHttpError("内网端 HTTP 返回 403: forbidden")
    with pytest.raises(module.InternalBridgeHttpError, match="403"):
        handle_energy_report_upload(service.config, {"period": "2026-10"}, lambda _: None)
    bridge.source_index_batch.side_effect = sources
    manifest = module.source_manifest(service.source_paths("2026-10", request_missing=True))
    bitable.upload_attachment_bytes.side_effect = TimeoutError("remote write outcome unknown")
    with pytest.raises(TimeoutError, match="remote write outcome unknown"):
        handle_energy_report_upload(service.config, {"period": "2026-10", "source_manifest": manifest}, lambda _: None)
    assert not module._source_retry_path(service.config, "2026-10").exists()


def test_incomplete_manifest_never_writes_to_feishu(clients):
    service, bridge, bitable, _ = clients
    manifest = module.source_manifest(service.source_paths("2026-10"))[:-1]
    with pytest.raises(ValueError, match="五楼十份"):
        service.run("2026-10", manifest=manifest, log=lambda _: None)
    bridge.source_index_batch.assert_called_once()
    bitable.list_fields.assert_not_called()


def test_valid_replacement_waits_for_fresh_index_without_redownloading(clients):
    service, bridge, bitable, _ = clients
    paths = service.source_paths("2026-10", request_missing=True)
    manifest = module.source_manifest(paths)
    path = paths["E楼", "utility_feeder_deviation_family"]
    workbook = openpyxl.load_workbook(path)
    try:
        workbook.active.append(["replaced while queued", "new data" * 20])
        workbook.save(path)
    finally:
        workbook.close()
    with pytest.raises(module.EnergyReportSourcePending, match="已变化"):
        service.run("2026-10", manifest=manifest, log=lambda _: None)
    bridge.source_index_batch.assert_called_once()
    bridge.refresh_latest_source_cache.assert_not_called()
    bitable.list_fields.assert_not_called()


def test_changed_pair_updates_only_existing_building(clients):
    service, _, bitable, rows = clients
    service.run("2026-10", log=lambda _: None)
    rows[0]["fields"]["同步标识"] = "old"
    assert service.run("2026-10", log=lambda _: None)["updated_buildings"] == 1
    assert bitable.create_record.call_count == 5
    assert bitable.update_record.call_args.args[:2] == (module.TABLE_ID, rows[0]["record_id"])
    assert bitable.upload_attachment_bytes.call_count == 12


def test_failed_upload_retries_without_recreating_completed_building(clients):
    service, _, bitable, rows = clients
    def upload(name, *_):
        if name == "B楼.xlsx":
            raise RuntimeError("upload unavailable")
        return name

    bitable.upload_attachment_bytes.side_effect = upload
    with pytest.raises(RuntimeError, match="upload unavailable"):
        service.run("2026-10", log=lambda _: None)
    assert [row["fields"]["楼栋"] for row in rows] == ["A楼"]
    assert not module.completed(service.config, "2026-10")
    bitable.upload_attachment_bytes.side_effect = lambda name, *_: name
    assert service.run("2026-10", log=lambda _: None)["updated_buildings"] == 4
    assert bitable.create_record.call_count == 5
    assert module.completed(service.config, "2026-10")


@pytest.mark.parametrize("bucket_key", ["2026-10-31", "2026-09", ""])
def test_wrong_period_or_missing_source_does_not_modify_feishu(clients, bucket_key):
    service, bridge, bitable, _ = clients
    source_result = bridge.source_index_batch.side_effect

    def incomplete(queries):
        results = source_result(queries)
        results[-1]["entries"][0]["bucket_key"] = bucket_key
        return results

    bridge.source_index_batch.side_effect = incomplete
    with pytest.raises(FileNotFoundError, match="E楼"):
        service.run("2026-10", log=lambda _: None)
    bitable.list_fields.assert_not_called()
    bitable.upload_attachment_bytes.assert_not_called()
    assert not module.completed(service.config, "2026-10")


def test_schema_conflict_preserves_fields(clients):
    service, _, bitable, _ = clients
    bitable.list_fields.side_effect = None
    bitable.list_fields.return_value = [{"field_name": "PUE确认表", "type": 1}]
    with pytest.raises(ValueError, match="PUE确认表"):
        service.ensure_schema()
    bitable._post_json.assert_not_called()


def test_monthly_preflight_requests_only_missing_buildings_and_families(clients):
    from unittest.mock import call

    service, bridge, bitable, _ = clients
    sources = bridge.source_index_batch.side_effect

    def missing(queries):
        results = sources(queries)
        for i, query in enumerate(queries):
            if (query['building'], query['source_family']) in {
                ('A楼', 'pue_confirmation_family'), ('E楼', 'utility_feeder_deviation_family'),
            }:
                results[i]['entries'] = []
        return results

    bridge.source_index_batch.side_effect = missing
    with pytest.raises(FileNotFoundError, match="未齐"):
        service.source_paths("2026-10", request_missing=True)
    assert bridge.refresh_latest_source_cache.call_args_list == [
        call(source_family="pue_confirmation_family", buildings=["A楼"], target_bucket_key="2026-10"),
        call(source_family="utility_feeder_deviation_family", buildings=["E楼"], target_bucket_key="2026-10"),
    ]
    bitable.create_record.assert_not_called()


def test_unconfirmed_create_does_not_mark_month_complete(clients):
    service, _, bitable, rows = clients
    create = bitable.create_record.side_effect

    def lost_create(table, fields, **kw):
        if fields["楼栋"] != "E楼":
            create(table, fields, **kw)

    bitable.create_record.side_effect = lost_create
    with pytest.raises(RuntimeError, match="回查未确认: E楼"):
        service.run("2026-10", log=lambda _: None)
    assert not module.completed(service.config, "2026-10")
    bitable.create_record.side_effect = create
    assert service.run("2026-10", log=lambda _: None)["updated_buildings"] == 1
    assert len(rows) == 5


def test_timeout_replays_same_create_after_restart(clients):
    service, bridge, bitable, rows = clients
    create = bitable.create_record.side_effect
    applied = {}

    def create_once(table, fields, *, client_token):
        if client_token in applied:
            assert fields == applied[client_token]
            return
        applied[client_token] = fields
        create(table, fields, client_token=client_token)
        if len(applied) == 1:
            raise TimeoutError("response lost after commit")

    bitable.create_record.side_effect = create_once
    with pytest.raises(TimeoutError):
        service.run("2026-10", log=lambda _: None)
    bitable.list_records.side_effect = [[], *([rows] * 2)]
    restarted = module.EnergyReportUploadService(service.config, internal=bridge, bitable=bitable)
    restarted.run("2026-10", log=lambda _: None)
    assert len(rows) == len(applied) == 5
    assert bitable.upload_attachment_bytes.call_count == 10


def test_truncated_zip_blocks_all_writes(clients):
    service, _, bitable, _ = clients
    paths = service.source_paths("2026-10")
    paths["E楼", "utility_feeder_deviation_family"].write_bytes(b"PK\x03\x04truncated")
    with pytest.raises(module.EnergyReportSourcePending, match="未写入飞书"):
        service.run("2026-10", log=lambda _: None, manifest=module.source_manifest(paths))
    bitable.list_fields.assert_not_called()


def test_corrupt_workbook_xml_blocks_upload_and_forces_targeted_repair(clients):
    from zipfile import ZipFile

    service, bridge, bitable, rows = clients
    paths = service.source_paths("2026-10")
    with ZipFile(paths["E楼", "utility_feeder_deviation_family"], "w") as archive:
        archive.writestr("xl/workbook.xml", "<workbook><not-closed")
    with pytest.raises(module.EnergyReportSourcePending, match="未写入飞书"):
        service.run("2026-10", log=lambda _: None, manifest=module.source_manifest(paths))
    bitable.list_fields.assert_not_called()
    bitable.upload_attachment_bytes.assert_not_called()
    assert rows == []
    bridge.refresh_latest_source_cache.assert_called_once_with(
        source_family="utility_feeder_deviation_family", buildings=["E楼"], target_bucket_key="2026-10", force_refresh=True,
    )


def test_zero_byte_ready_source_forces_repair_in_scheduler_preflight(clients):
    service, bridge, bitable, _ = clients
    reply = bridge.source_index_batch.side_effect

    def empty_source(queries):
        result = reply(queries)
        path = Path(service.config["shared_bridge"]["external_root_dir"]) / result[0]["entries"][0]["relative_path"]
        path.write_bytes(b"")
        return result

    bridge.source_index_batch.side_effect = empty_source
    with pytest.raises(FileNotFoundError, match="未齐"):
        service.source_paths("2026-10", request_missing=True)
    bridge.refresh_latest_source_cache.assert_called_once_with(
        source_family="pue_confirmation_family", buildings=["A楼"], target_bucket_key="2026-10", force_refresh=True,
    )
    bitable.list_fields.assert_not_called()


def test_pending_month_survives_rollover_and_is_removed_only_after_completion(tmp_path):
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    assert module.pending_periods(config, datetime(2026, 11, 30, 20)) == ("2026-10",)
    assert module.pending_periods(config, datetime(2026, 12, 1, 9, 4, 59)) == ("2026-10",)
    assert module.pending_periods(config, datetime(2026, 12, 1, 9, 5)) == ("2026-10", "2026-11")
    module.save_cached_json(module._completion_path(config, "2026-11"), {"period": "2026-11", "table_id": module.TABLE_ID})
    assert module.pending_periods(config, datetime(2027, 2, 1, 9, 5)) == ("2026-10", "2026-12", "2027-01")
    assert module.enqueue_period(config, "2026-09", datetime(2027, 2, 1, 9))
    assert not module.enqueue_period(config, "2026-11", datetime(2027, 2, 1, 9))
    assert module.pending_periods(config, datetime(2027, 2, 1, 9))[0] == "2026-09"
    with pytest.raises(ValueError, match="已结束"):
        module.enqueue_period(config, "2027-02", datetime(2027, 2, 1, 9))
