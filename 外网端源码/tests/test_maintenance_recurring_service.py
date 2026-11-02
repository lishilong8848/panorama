from __future__ import annotations

import json
import socket
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from handover_log_module.repository.maintenance_management_repository import MaintenanceManagementRow
from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore
from handover_log_module.service.maintenance_management_payload_builder import MaintenanceManagementPayloadBuilder
from handover_log_module.service.maintenance_recurring_service import MaintenanceRecurringService


BUILDINGS = [f"{code}\u697c" for code in "ABCDE"]
BUILDING = BUILDINGS[0]
COLUMNS = {"maintenance_item": "B", "maintenance_party": "C", "completion": "D", "executor": "H"}
SECTION = "Recurring maintenance"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*_args, **_kwargs):
        pytest.fail("Network access is forbidden in recurring-maintenance tests")

    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)


@pytest.fixture
def config(tmp_path):
    return {
        "_global_paths": {"runtime_state_root": str(tmp_path / "runtime")},
        "maintenance_management_section": {
            "enabled": False,
            "sections": {"maintenance_management": SECTION},
            "column_mapping": {"resolve_by_header": False},
        },
    }


@pytest.fixture
def service(config):
    return MaintenanceRecurringService(config)


def values(item="Inspect UPS", **overrides):
    return {"maintenance_item": item, "maintenance_party": "Internal", "completion": "Done", "executor": "Lee", **overrides}


def create(service, item="Inspect UPS", **overrides):
    args = {"values": values(item), "mode": "every_shift", "duty_date": "2026-11-06", "duty_shift": "night"}
    args.update(overrides)
    return service.upsert(BUILDING, **args)


def store_for(config, building=BUILDING):
    return ReviewBuildingDocumentStore(config=config, building=building)


def raw_default(config):
    with store_for(config).connect(read_only=True) as conn:
        row = conn.execute("SELECT * FROM building_defaults WHERE default_key='maintenance_recurring'").fetchone()
        return dict(row) if row is not None else None


def build(builder, **overrides):
    args = {"building": BUILDING, "duty_date": "2026-11-07", "duty_shift": "day", "emit_log": lambda _: None}
    args.update(overrides)
    return builder.build(**args)


def source_row(item, record_id="source-1"):
    return MaintenanceManagementRow(
        record_id=record_id, building_values=[BUILDING], updated_time=datetime(2026, 11, 7, 10),
        item_text=item, specialty_text="", raw_fields={},
    )


def test_restart_persists_multiple_records_and_isolates_all_buildings(config, service):
    records = [create(service), create(service, "Inspect cooling", mode="day_only")]
    for building in BUILDINGS[1:]:
        assert service.list_records(building) == []
        other = service.upsert(building, values=values(), mode="every_shift", duty_date="2026-11-07", duty_shift="day")
        assert other["id"] != records[0]["id"]
        assert service.list_records(building) == [other]
    assert MaintenanceRecurringService(config).list_records(BUILDING) == records
    script = (
        "import json,sys; "
        "from handover_log_module.service.maintenance_recurring_service import MaintenanceRecurringService; "
        "config,building=json.load(sys.stdin); "
        "print(json.dumps(MaintenanceRecurringService(config).list_records(building)))"
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", script], input=json.dumps([config, BUILDING]),
        cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, check=True, timeout=30,
    )
    assert json.loads(result.stdout) == records
    assert all(str(uuid.UUID(record["id"])) == record["id"] for record in records)
    assert all(datetime.strptime(record["created_at"], "%Y-%m-%d %H:%M:%S") for record in records)
    # Returned objects are detached, not a service-global cache.
    records[0]["values"]["executor"] = "Not saved"
    assert service.list_records(BUILDING)[0]["values"]["executor"] == "Lee"


@pytest.mark.parametrize("day,shift,expected", [
    ("2026-11-05", "night", []), ("2026-11-06", "day", []),
    ("2026-11-06", "night", ["Inspect UPS"]),
    ("2026-11-07", "day", ["Inspect UPS", "Inspect cooling"]),
    ("2026-11-07", "night", ["Inspect UPS"]),
    ("2026-11-08", "day", ["Inspect UPS", "Inspect cooling"]),
    ("2026-11-08", "night", ["Inspect UPS"]),
])
def test_start_context_and_weekends_without_weekday_restrictions(service, day, shift, expected):
    first = create(service)
    create(service, "Inspect cooling", mode="day_only")
    rows = service.applicable_rows(BUILDING, day, shift, COLUMNS)
    assert [row["cells"]["B"] for row in rows] == expected
    if rows:
        assert rows[0] == {"row_id": f"maintenance-recurring:{first['id']}", "cells": dict(zip(COLUMNS.values(), values().values()))}


def test_day_only_created_in_day_is_immediately_applicable(service):
    create(service, mode="day_only", duty_shift="day")
    assert len(service.applicable_rows(BUILDING, "2026-11-06", "day", COLUMNS)) == 1
    assert service.applicable_rows(BUILDING, "2026-11-06", "night", COLUMNS) == []


def test_duplicate_reuses_id_and_unchanged_submission_is_idempotent(config, service):
    first = create(service)
    before = raw_default(config)
    assert create(service, "  inSPECT\tU P S\n", duty_date="2026-12-01") == first
    assert raw_default(config) == before
    with pytest.raises(ValueError, match="^revision_conflict$"):
        create(service, mode="day_only")
    changed = create(service, mode="day_only", revision=first["revision"])
    assert changed["id"] == first["id"]
    assert changed["revision"] == 2
    assert len(service.list_records(BUILDING)) == 1


@pytest.mark.parametrize("duty_date,duty_shift", [("2026-12-01", "day"), ("2026-11-01", "day"), ("2026-11-06", "day")])
def test_edit_keeps_original_effective_context_and_bumps_only_its_revision(config, service, duty_date, duty_shift):
    first = create(service)
    second = create(service, "Second item")
    edited = create(service, "Renamed item", record_id=first["id"], revision=1, duty_date=duty_date, duty_shift=duty_shift)
    assert edited["id"] == first["id"]
    assert edited["revision"] == 2
    assert (edited["from_date"], edited["from_shift"]) == ("2026-11-06", "night")
    assert edited["created_at"] == first["created_at"]
    assert service.list_records(BUILDING)[1] == second
    assert raw_default(config)["revision"] == 3
    assert service.applicable_rows(BUILDING, "2026-11-06", "day", COLUMNS) == []


@pytest.mark.parametrize("revision", [None, 0, 2, "1", True])
def test_edit_delete_stale_or_missing_revision_cannot_change_data(config, service, revision):
    first = create(service)
    before = raw_default(config)
    with pytest.raises(ValueError, match="^revision_conflict$"):
        create(service, "Changed", record_id=first["id"], revision=revision)
    with pytest.raises(ValueError, match="^revision_conflict$"):
        service.delete(BUILDING, record_id=first["id"], revision=revision)
    assert raw_default(config) == before


def test_real_stale_revision_after_edit_and_deleted_missing_record(config, service):
    first = create(service)
    changed = create(service, "Changed", record_id=first["id"], revision=1)
    with pytest.raises(ValueError, match="^revision_conflict$"):
        create(service, "Changed again", record_id=first["id"], revision=1)
    with pytest.raises(ValueError, match="^revision_conflict$"):
        service.delete(BUILDING, record_id=first["id"], revision=1)
    service.delete(BUILDING, record_id=first["id"], revision=changed["revision"])
    with pytest.raises(KeyError):
        create(service, record_id=first["id"], revision=2)
    with pytest.raises(KeyError):
        service.delete(BUILDING, record_id=first["id"], revision=2)
    assert raw_default(config)["revision"] == 3
    assert json.loads(raw_default(config)["value_json"]) == []


def test_conflicting_rename_rejected_atomically(config, service):
    first = create(service)
    create(service, "Second")
    before = raw_default(config)
    with pytest.raises(ValueError, match="^duplicate_item$"):
        create(service, " s e c o n d ", record_id=first["id"], revision=1)
    assert raw_default(config) == before


def test_delete_stops_future_rows_without_mutating_stored_documents_or_schema(config, service):
    store = store_for(config)
    store.ensure_ready()
    store.set_default("unrelated_default", {"keep": True})
    session = {"session_id": "past", "building": BUILDING, "duty_date": "2026-11-06", "duty_shift": "night", "revision": 1}
    store.upsert_imported_document(session=session, document={"sections": [{"name": "Past", "rows": ["unchanged"]}]}, imported_from_excel=False)
    with store.connect(read_only=True) as conn:
        schema_before = [tuple(row) for row in conn.execute("SELECT name,sql FROM sqlite_master ORDER BY name")]
        documents_before = [tuple(row) for row in conn.execute("SELECT * FROM review_documents")]
    first = create(service)
    materialized = service.applicable_rows(BUILDING, "2026-11-07", "day", COLUMNS)
    service.delete(BUILDING, record_id=first["id"], revision=1)
    assert service.applicable_rows(BUILDING, "2026-12-01", "day", COLUMNS) == []
    assert len(materialized) == 1
    assert store.get_default("unrelated_default") == {"keep": True}
    with store.connect(read_only=True) as conn:
        assert [tuple(row) for row in conn.execute("SELECT name,sql FROM sqlite_master ORDER BY name")] == schema_before
        assert [tuple(row) for row in conn.execute("SELECT * FROM review_documents")] == documents_before


@pytest.mark.parametrize("bad", ["{broken", "{}", "null", '[{"id":"not-valid"}]', "[null]"])
def test_corruption_is_not_replaced_by_empty_defaults(config, service, bad):
    store = store_for(config)
    store.ensure_ready()
    with store.connect() as conn:
        conn.execute("INSERT INTO building_defaults VALUES(?,?,?,?)", ("maintenance_recurring", bad, 7, "unchanged"))
    before = raw_default(config)
    actions = [
        lambda: service.list_records(BUILDING), lambda: create(service),
        lambda: service.delete(BUILDING, record_id="missing", revision=1),
        lambda: service.applicable_rows(BUILDING, "2026-11-07", "day", COLUMNS),
        lambda: build(MaintenanceManagementPayloadBuilder(config), require_source_success=True),
    ]
    for action in actions:
        with pytest.raises(ValueError, match="^maintenance_recurring_corrupt$"):
            action()
        assert raw_default(config) == before


@pytest.mark.parametrize("damage", ["duplicate_id", "duplicate_item", "mode", "revision", "date", "timestamp", "values", "version"])
def test_structurally_corrupt_records_are_rejected(config, service, damage):
    first = create(service)
    records = [deepcopy(first)]
    if damage in {"duplicate_id", "duplicate_item"}:
        records.append(deepcopy(first))
        if damage == "duplicate_item":
            records[1]["id"] = str(uuid.uuid4())
        else:
            records[1]["values"]["maintenance_item"] = "Other"
    elif damage == "mode":
        records[0]["mode"] = ["day_only"]
    elif damage == "revision":
        records[0]["revision"] = True
    elif damage == "date":
        records[0]["from_date"] = "2026-02-30"
    elif damage == "timestamp":
        records[0]["created_at"] = "not a timestamp"
    elif damage == "values":
        records[0]["values"]["completion"] = 12
    with store_for(config).connect() as conn:
        conn.execute("UPDATE building_defaults SET value_json=?,revision=? WHERE default_key=?", (json.dumps(records), 0 if damage == "version" else 1, "maintenance_recurring"))
    before = raw_default(config)
    with pytest.raises(ValueError, match="^maintenance_recurring_corrupt$"):
        create(service, "New")
    assert raw_default(config) == before


def test_record_limit_allows_existing_edits_and_deleting_then_creating(service):
    for index in range(200):
        create(service, f"Item {index}")
    first = service.list_records(BUILDING)[0]
    assert create(service, "Item 0") == first
    with pytest.raises(ValueError, match="^maintenance_recurring_limit$"):
        create(service, "Item 200")
    edited = create(service, "Edited", record_id=first["id"], revision=1)
    service.delete(BUILDING, record_id=edited["id"], revision=2)
    create(service, "Item 200")
    assert len(service.list_records(BUILDING)) == 200


@pytest.mark.parametrize("invalid", [
    {}, values(" \t\n"), values(completion=None), values(executor=1),
    values("x" * 2001), values(completion="x" * 2001), values(executor="bad\x00text"),
    {**values(), "unexpected": "value"},
])
def test_invalid_values_rejected_before_writes(config, service, invalid):
    with pytest.raises(ValueError):
        create(service, values=invalid)
    assert not store_for(config).db_path.exists()


@pytest.mark.parametrize("overrides", [
    {"mode": "weekly"}, {"mode": None}, {"duty_date": "2026-02-30"}, {"duty_date": "20261107"},
    {"duty_shift": "morning"}, {"duty_shift": None}, {"record_id": 1}, {"revision": 1},
])
def test_invalid_modes_context_and_creation_revision(service, overrides):
    with pytest.raises(ValueError):
        create(service, **overrides)
    assert service.list_records(BUILDING) == []


def test_a_to_e_guard_is_explicit_even_if_shared_building_helper_supports_h(service, monkeypatch):
    monkeypatch.setattr("handover_log_module.service.maintenance_recurring_service.building_code_from_name", lambda _: "H")
    with pytest.raises(ValueError, match="^invalid_building$"):
        service.list_records("H\u697c")


@pytest.mark.parametrize("mapping", [{}, {**COLUMNS, "executor": "B"}, {**COLUMNS, "executor": "XFE"}, {**COLUMNS, "executor": "1"}])
def test_invalid_column_mapping_is_an_error_not_empty_rows(service, mapping):
    create(service)
    with pytest.raises(ValueError, match="^invalid_column_mapping$"):
        service.applicable_rows(BUILDING, "2026-11-07", "day", mapping)


def test_sqlite_transaction_prevents_lost_updates_without_process_local_lock(config, service, monkeypatch):
    store_for(config).ensure_ready()
    original_store = MaintenanceRecurringService._store

    def unlocked(self, building):
        store = original_store(self, building)
        store._write_lock = nullcontext()
        return store

    monkeypatch.setattr(MaintenanceRecurringService, "_store", unlocked)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda index: create(MaintenanceRecurringService(config), f"Item {index}"), range(24)))
    assert len(service.list_records(BUILDING)) == 24
    assert raw_default(config)["revision"] == 24
    with ThreadPoolExecutor(max_workers=8) as pool:
        duplicates = list(pool.map(lambda _: create(MaintenanceRecurringService(config), "Same item"), range(8)))
    assert len({r["id"] for r in duplicates}) == 1
    assert len(service.list_records(BUILDING)) == 25
    assert raw_default(config)["revision"] == 25


def test_mutations_use_begin_immediate_and_failed_write_rolls_back(config, service, monkeypatch):
    create(service)
    before = raw_default(config)
    original_connect = ReviewBuildingDocumentStore.connect
    statements = []

    @contextmanager
    def traced(self, **kwargs):
        with original_connect(self, **kwargs) as conn:
            conn.set_trace_callback(statements.append)
            yield conn

    monkeypatch.setattr(ReviewBuildingDocumentStore, "connect", traced)
    original_write = service._write

    def fail_after_write(conn, records, version):
        original_write(conn, records, version)
        raise OSError("simulated disk failure")

    monkeypatch.setattr(service, "_write", fail_after_write)
    with pytest.raises(OSError, match="simulated disk failure"):
        create(service, "Other")
    assert "BEGIN IMMEDIATE" in statements
    assert "ROLLBACK" in statements
    assert raw_default(config) == before


@pytest.mark.parametrize("preloaded", [False, True])
def test_builder_actual_source_wins_and_recurring_has_stable_row_id(config, service, monkeypatch, preloaded):
    config["maintenance_management_section"]["enabled"] = True
    config["maintenance_management_section"]["column_mapping"]["fallback_cols"] = {
        "maintenance_item": "E", "maintenance_party": "F", "completion": "G", "executor": "J",
    }
    create(service, "ins pect UPS")
    recurring = create(service, "Cooling")
    builder = MaintenanceManagementPayloadBuilder(config)
    rows = [source_row("Inspect UPS"), source_row("INSPECT\tUPS", "duplicate-source")]
    fetch = Mock(return_value=(rows, builder.repo.get_config()))
    monkeypatch.setattr(builder.repo, "list_current_shift_rows", fetch)
    args = {"preloaded_engineers": []}
    if preloaded:
        args["preloaded_rows_by_building"] = {BUILDING: rows}
    result = build(builder, **args)
    assert [r["cells"]["E"] for r in result[SECTION]] == ["Inspect UPS", "Cooling"]
    assert result[SECTION][0]["cells"]["J"] == "/"
    assert "row_id" not in result[SECTION][0]
    assert result[SECTION][1]["row_id"] == f"maintenance-recurring:{recurring['id']}"
    assert result[SECTION][1]["cells"]["J"] == "Lee"
    assert build(builder, **args) == result
    assert fetch.call_count == (0 if preloaded else 2)


@pytest.mark.parametrize("strict", [False, True])
def test_disabled_source_still_merges_recurrence_without_fetch(config, service, monkeypatch, strict):
    create(service)
    builder = MaintenanceManagementPayloadBuilder(config)
    fetch = Mock(side_effect=AssertionError("disabled source must not fetch"))
    monkeypatch.setattr(builder.repo, "list_current_shift_rows", fetch)
    assert len(build(builder, require_source_success=strict)[SECTION]) == 1
    fetch.assert_not_called()


def test_feishu_read_failure_generation_works_but_strict_refresh_preserves_content(config, service, monkeypatch):
    create(service)
    config["maintenance_management_section"]["enabled"] = True
    builder = MaintenanceManagementPayloadBuilder(config)
    monkeypatch.setattr(builder.repo, "list_current_shift_rows", Mock(side_effect=RuntimeError("offline mock")))
    original_document = {SECTION: [{"cells": {"B": "Actual source record"}}]}
    current_document = deepcopy(original_document)
    with pytest.raises(ValueError, match="\u7ef4\u62a4\u6570\u636e\u6e90\u8bfb\u53d6\u5931\u8d25\uff0c\u5df2\u4fdd\u7559\u5f53\u524d\u5185\u5bb9"):
        current_document.update(build(builder, require_source_success=True))
    assert current_document == original_document
    assert [r["cells"]["B"] for r in build(builder)[SECTION]] == ["Inspect UPS"]


def test_strict_failure_precedes_recurring_read_or_template_mapping(config, monkeypatch):
    config["maintenance_management_section"]["enabled"] = True
    builder = MaintenanceManagementPayloadBuilder(config)
    monkeypatch.setattr(builder, "_build_source", Mock(return_value={}))
    forbidden = Mock(side_effect=AssertionError("must fail before recurring read/template open"))
    monkeypatch.setattr(MaintenanceRecurringService, "list_records", forbidden)
    monkeypatch.setattr(builder, "_resolve_section_column_mapping", forbidden)
    with pytest.raises(ValueError, match="\u7ef4\u62a4\u6570\u636e\u6e90\u8bfb\u53d6\u5931\u8d25"):
        build(builder, require_source_success=True)
    forbidden.assert_not_called()


def test_strict_successful_empty_source_allows_recurring_rows(config, service):
    config["maintenance_management_section"]["enabled"] = True
    create(service)
    builder = MaintenanceManagementPayloadBuilder(config)
    result = build(builder, require_source_success=True, preloaded_rows_by_building={}, preloaded_engineers=[])
    assert len(result[SECTION]) == 1


@pytest.mark.parametrize("existing_store", [False, True])
def test_no_rules_fast_path_avoids_database_initialization_or_extra_template_open(config, monkeypatch, existing_store):
    store = store_for(config)
    if existing_store:
        store.ensure_ready()
    builder = MaintenanceManagementPayloadBuilder(config)
    source = {SECTION: []}
    monkeypatch.setattr(builder, "_build_source", Mock(return_value=source))
    forbidden = Mock(side_effect=AssertionError("read-only/no-rules path must not initialize or reopen template"))
    monkeypatch.setattr(ReviewBuildingDocumentStore, "ensure_ready", forbidden)
    monkeypatch.setattr(builder, "_resolve_section_column_mapping", forbidden)
    assert build(builder) is source
    assert store.db_path.exists() == existing_store
    forbidden.assert_not_called()


def test_local_store_failure_preserves_normal_rows_and_strict_refresh_fails(config, monkeypatch):
    builder = MaintenanceManagementPayloadBuilder(config)
    source = {SECTION: [{"cells": {"B": "Normal maintenance"}}]}
    monkeypatch.setattr(builder, "_build_source", Mock(return_value=source))
    monkeypatch.setattr(MaintenanceRecurringService, "list_records", Mock(side_effect=OSError("unreadable local store")))
    logs = []
    assert build(builder, emit_log=logs.append) == source
    assert any("unreadable local store" in message for message in logs)
    with pytest.raises(OSError, match="unreadable local store"):
        build(builder, require_source_success=True)
