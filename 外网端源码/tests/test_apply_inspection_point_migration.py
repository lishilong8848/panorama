from copy import deepcopy
import json

import pytest

from app.modules.inspection_data.service import point_settings
from app.modules.inspection_data.service.inspection_data_upload_service import _record_ids_sha256
from scripts import apply_inspection_point_migration as migration
from test_inspection_point_migration_preview import _inputs


class FakeBitable:
    app_token = "mock"
    LIST_FIELD_URL = "mock://{app_token}/{table_id}"

    def __init__(self, live):
        self.rows = {table: deepcopy(live[building[0]]["config"])
                     for building, table in migration.POINT_CONFIG_TABLES.items()}
        self.rows.update({scope: deepcopy(rows["source"]) for scope, rows in live.items()})
        for scope in ("D", "E"):
            for i, row in enumerate(self.rows[scope]):
                row.setdefault("record_id", f"source_{i}")
            if any(row["fields"].get("记录类型") == "设备巡检" for row in self.rows[scope]):
                continue
            for row in self.rows[scope]:
                row["fields"]["上传日期"] = 123
            ids = {row["record_id"] for row in self.rows[scope]} | {"marker"}
            marker = {
                "building": f"{scope}楼", "status": "completed", "upload_time": 123,
                "device_count": 1, "point_count": len(ids) - 1, "record_ids_sha256": _record_ids_sha256(ids),
            }
            self.rows[scope].append({"record_id": "marker", "fields": {
                "楼栋": f"{scope}楼", "上传日期": 123, "记录类型": "设备巡检",
                "备注": migration.INSPECTION_BATCH_PREFIX + json.dumps(marker),
            }})
        self.fields = {table: {**migration._FIELD_TYPES, "名称": 1}
                       for table in migration.POINT_CONFIG_TABLES.values()}
        self.writes = []
        self.break_create = False
        self.ignore_update = False

    def list_records(self, table, *, page_size, max_records):
        assert page_size == 500 and max_records == 0
        return deepcopy(self.rows[table])

    def list_fields(self, table):
        return [{"field_name": field, "type": kind, "is_primary": field == "名称"}
                for field, kind in self.fields[table].items()]

    def list_record_ids(self, table, *, page_size, require_complete):
        assert page_size == 500 and require_complete
        return [row["record_id"] for row in self.rows[table]]

    def _post_json(self, url, payload):
        table = url.rsplit("/", 1)[-1]
        field = payload["field_name"]
        assert payload["type"] == 1 and field not in self.fields[table]
        self.fields[table][field] = 1
        self.writes.append(("create_field", table))

    def batch_update_records(self, table, updates):
        self.writes.append(("update", table))
        if self.ignore_update:
            return
        for update in updates:
            row = next(row for row in self.rows[table] if row["record_id"] == update["record_id"])
            row["fields"].update(update["fields"])

    def create_record(self, table, fields, *, client_token):
        self.writes.append(("create", table))
        self.rows[table].append({"record_id": client_token, "fields": deepcopy(fields)})
        if self.break_create:
            raise RuntimeError("response lost after successful create")


@pytest.fixture
def inputs(monkeypatch, tmp_path):
    plan, live, _ = _inputs(monkeypatch)
    plan["D"]["manual_states"].append({
        "scope": "D", "profession": "暖通", "point_id": "local_" + "d" * 32,
        "device": "manual", "name": "check",
        "target": {"workbook": "表", "sheet": "工作表", "cell": "C6",
                   "round_cells": {"白1": "C6", "白2": "C7", "夜1": "C8", "夜2": "C9"}},
    })
    config = {"deployment": {"role_mode": "external"}, "paths": {"runtime_state_root": str(tmp_path)}}
    client = FakeBitable(live)
    local = migration._normalized_settings(migration._config_rows(client), {}, {})
    local["E楼"]["electrical_inspection_family"]["baseline"]["1_0"].update(value="12", sampleTime="08:00")
    point_settings._save_settings(config, local)
    revision = point_settings.current_point_settings_revision(config)

    def run(*, apply=True, expected_revision=revision):
        return migration.migrate(config, plan, expected_revision=expected_revision,
                                 expected_settings_path=point_settings._path(config), client=client,
                                 source_tables={"D楼": "D", "E楼": "E"}, apply=apply)

    return config, client, run, revision


def test_preview_then_first_write_and_idempotent_rerun(inputs):
    config, client, run, revision = inputs
    e_table = migration.POINT_CONFIG_TABLES["E楼"]
    client.rows[e_table][0]["fields"]["附件"] = [{"file_token": "mock_attachment"}]
    original = deepcopy(client.rows[e_table][0]["fields"])
    assert run(apply=False)["revision"] == revision
    assert not client.writes
    result = run()
    assert result["revision"] > revision
    assert result["buildings"]["D"] == {"updates": 1, "creates": 1, "add_fields": [migration.SOURCE_FIELD]}
    assert {k: client.rows[e_table][0]["fields"][k] for k in original} == original
    e_settings = point_settings.load_point_settings(config)["E楼"]
    assert any(group["status_rules"]["1_0"]["normal"] == ["ok"]
               for group in e_settings.values() if "1_0" in group.get("status_rules", {}))
    assert e_settings["electrical_inspection_family"]["baseline"]["1_0"]["value"] == "12"
    assert e_settings["electrical_inspection_family"]["baseline"]["1_0"]["sampleTime"] == "08:00"
    client.writes.clear()
    assert run(expected_revision=result["revision"])["revision"] == result["revision"]
    assert not client.writes


def test_readback_failure_does_not_advance_local_revision(inputs):
    config, client, run, revision = inputs
    client.ignore_update = True
    before = point_settings._path(config).read_bytes()
    with pytest.raises(RuntimeError, match="回读不一致"):
        run()
    assert point_settings.current_point_settings_revision(config) == revision
    assert point_settings._path(config).read_bytes() == before


def test_rerun_after_interrupted_create_skips_already_applied_rows(inputs):
    config, client, run, revision = inputs
    client.break_create = True
    with pytest.raises(RuntimeError, match="response lost"):
        run()
    assert point_settings.current_point_settings_revision(config) == revision
    client.break_create = False
    client.writes.clear()
    result = run()
    assert result["revision"] > revision
    assert client.writes == [("update", migration.POINT_CONFIG_TABLES["E楼"])]


def test_pending_sync_and_revision_and_wrong_role_block_writes(inputs):
    config, client, run, revision = inputs
    with pytest.raises(ValueError, match="修订号"):
        run(expected_revision=revision + 1)
    point_settings._write_pending(config, {"D楼"})
    with pytest.raises(ValueError, match="未同步"):
        run()
    config["deployment"]["role_mode"] = "internal"
    with pytest.raises(ValueError, match="外网端"):
        run()
    assert not client.writes


def test_incomplete_batch_is_rejected_before_writes(inputs):
    _, client, run, _ = inputs
    client.rows["D"].pop(0)
    with pytest.raises(ValueError, match="没有完整的巡检采集批次"):
        run()
    assert not client.writes


def _set_batch_upload_time(rows, upload_time):
    prefix = migration.INSPECTION_BATCH_PREFIX
    for row in rows:
        fields = row["fields"]
        fields["上传日期"] = upload_time
        remark = fields.get("备注", "")
        if remark.startswith(prefix):
            marker = json.loads(remark[len(prefix):])
            marker["upload_time"] = upload_time
            fields["备注"] = prefix + json.dumps(marker)


def test_independently_completed_building_batches_can_have_different_upload_times(inputs):
    _, client, run, _ = inputs
    _set_batch_upload_time(client.rows["E"], 456)

    result = run()

    assert result["applied"] is True
    assert result["buildings"]["D"]["updates"] == result["buildings"]["E"]["updates"] == 1


def test_building_batch_changed_after_preflight_is_rejected(inputs, monkeypatch):
    _, client, run, _ = inputs
    original = client.list_records
    e_reads = 0

    def list_records(table, **kwargs):
        nonlocal e_reads
        if table == "E":
            e_reads += 1
            if e_reads == 2:
                _set_batch_upload_time(client.rows["E"], 456)
        return original(table, **kwargs)

    monkeypatch.setattr(client, "list_records", list_records)
    with pytest.raises(ValueError, match="源表在预检期间已变化"):
        run()
    assert not client.writes


def test_non_migrated_baseline_name_change_is_rejected(inputs):
    _, client, run, _ = inputs
    client.rows[migration.POINT_CONFIG_TABLES["E楼"]][1]["fields"]["测点名称"] = "different name"
    with pytest.raises(ValueError, match="名称或路径"):
        run()
    assert not client.writes


def test_old_status_schema_adds_only_missing_text_fields(inputs):
    _, client, run, revision = inputs
    table = migration.POINT_CONFIG_TABLES["D楼"]
    missing = {"测点类型", *migration.STATUS_FIELDS.values()}
    for field in missing:
        del client.fields[table][field]
    e_table = migration.POINT_CONFIG_TABLES["E楼"]
    client.fields[e_table][migration.SOURCE_FIELD] = 1
    initial_fields = deepcopy(client.fields)
    preview = run(apply=False)
    assert set(preview["buildings"]["D"]["add_fields"]) == missing | {migration.SOURCE_FIELD}
    assert not client.writes
    assert run()["revision"] > revision
    assert client.fields[table] == initial_fields[table] | dict.fromkeys(missing | {migration.SOURCE_FIELD}, 1)
    assert client.fields[e_table] == initial_fields[e_table]


def test_incompatible_existing_type_stops_migration_before_writes(inputs):
    _, client, run, _ = inputs
    table = migration.POINT_CONFIG_TABLES["D楼"]
    client.fields[table]["状态正常值"] = 2
    with pytest.raises(ValueError, match="状态正常值"):
        run()
    assert not client.writes
