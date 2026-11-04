from __future__ import annotations

import json
import socket
from datetime import datetime, time

import pytest
from openpyxl import Workbook

from handover_log_module.service.chiller_mode_focus_state_service import ChillerModeFocusStateService
from handover_log_module.service.chiller_mode_upload_service import ChillerModeUploadService
from handover_log_module.service.hvac_bitable_sync_service import HvacBitableSyncService


@pytest.fixture(autouse=True)
def _deny_network(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("network access is forbidden in these tests")

    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)


class FakeClient:
    def __init__(self, *, fail_batch: int = 0) -> None:
        self.timeout = 30
        self.request_retry_count = 1
        self.fail_batch = fail_batch
        self.batch_number = 0
        self.events: list[tuple[str, list[str] | int, int, int]] = []
        self.created_rows: list[dict] = []

    def list_fields(self, **_kwargs):
        return [{"field_name": name} for name in ("楼栋", "所属控制器", "采集点", "数据", "冷机模式")]

    def list_record_ids(self, **_kwargs):
        self.events.append(("list", 0, self.timeout, self.request_retry_count))
        return ["old-1", "old-2"]

    def batch_create_records(self, *, fields_list, **_kwargs):
        self.batch_number += 1
        self.events.append(
            ("create", len(fields_list), self.timeout, self.request_retry_count)
        )
        if self.batch_number == self.fail_batch:
            raise TimeoutError("read timed out")
        self.created_rows.extend(fields_list)
        start = (self.batch_number - 1) * 2
        return [
            {
                "data": {
                    "records": [
                        {"record_id": f"new-{start + index + 1}"}
                        for index in range(len(fields_list))
                    ]
                }
            }
        ]

    def batch_delete_records(self, *, record_ids, **_kwargs):
        normalized = list(record_ids)
        self.events.append(
            ("delete", normalized, self.timeout, self.request_retry_count)
        )
        return len(normalized)


def _target() -> dict:
    return {
        "page_size": 500,
        "delete_batch_size": 500,
        "create_batch_size": 2,
        "create_timeout_sec": 60,
        "create_retry_count": 3,
        "replace_existing": True,
    }


def test_chiller_safe_replace_creates_all_rows_before_deleting_old_records():
    service = ChillerModeUploadService({})
    client = FakeClient()

    deleted, created = service._replace_target_records_safely(
        client=client,
        table_id="table",
        rows=[{"value": index} for index in range(3)],
        target=_target(),
        list_field_names=["楼栋"],
        emit_log=lambda _text: None,
    )

    assert (deleted, created) == (2, 3)
    assert [event[0] for event in client.events] == ["list", "create", "create", "delete"]
    assert client.events[-1][1] == ["old-1", "old-2"]
    assert all(
        event[2:] == (60, 3)
        for event in client.events
        if event[0] in {"create", "delete"}
    )
    assert client.timeout == 30
    assert client.request_retry_count == 1


def _payload(*, building="A楼", rows=None):
    return {
        "schema_version": 1,
        "source_family": "chiller_mode_switch_family",
        "building": building,
        "sheets": [{"name": "首表", "rows": rows if rows is not None else [
            [None, None, "控制器A", "1#冷机模式", 1],
        ]}],
    }


def _write_json(path, payload, *, bom=False):
    path.write_text(
        json.dumps(payload, ensure_ascii=False, default=lambda value: value.isoformat()),
        encoding="utf-8-sig" if bom else "utf-8",
    )


def _write_workbook(path, sheets):
    workbook = Workbook()
    for index, source in enumerate(sheets):
        sheet = workbook.active if index == 0 else workbook.create_sheet()
        sheet.title = source["name"]
        for row in source["rows"]:
            sheet.append(row)
    workbook.save(path)
    workbook.close()


def _parse(service, path, building="A楼"):
    return service._parse_source_file(
        building=building,
        file_path=str(path),
        cfg=service._normalize_cfg(),
        emit_log=lambda _text: None,
    )


@pytest.mark.parametrize("suffix", [".json", ".JSON"])
def test_json_and_equivalent_xlsx_produce_same_first_sheet_rows(tmp_path, suffix):
    timestamp = datetime(2026, 11, 4, 8, 30, 15)
    rows = [
        [timestamp, None, "所属控制器", "采集点", "数据"],
        [timestamp, time(8, 30), "控制器A", "1#冷机模式", "1"],
        [timestamp, None, None, "温度", 23.5],
        [None, None, "控制器B", "数值零", 0],
        [None],
        [None, None, None, "文本数值", "1,234.5"],
        [None, None, None, "负数", "-0.25"],
        [None, None, None, "空字符串", ""],
        [None, None, None, "空值", None],
        [None, None, None, "时间值", time(12, 30)],
        [None, None, None, "日期时间值", timestamp],
        [None, None, None, "布尔值", True],
        [None, None, None, "非数值", "离线"],
        [None, None, None, "", 19],
    ]
    payload = _payload(rows=rows)
    payload["sheets"].append({"name": "其他表", "rows": [[None, None, "其他", "冷机模式", 4]]})
    source = tmp_path / f"source{suffix}"
    excel = tmp_path / "source.xlsx"
    _write_json(source, payload, bom=True)
    _write_workbook(excel, payload["sheets"])
    service = ChillerModeUploadService({})

    json_result = _parse(service, source)
    excel_result = _parse(service, excel)

    assert {key: value for key, value in json_result.items() if key != "file_path"} == {
        key: value for key, value in excel_result.items() if key != "file_path"
    }
    assert json_result["rows"] == [
        {"楼栋": "A楼", "所属控制器": "控制器A", "采集点": "1#冷机模式", "数据": 1, "冷机模式": "制冷"},
        {"楼栋": "A楼", "所属控制器": "控制器A", "采集点": "温度", "数据": 23.5},
        {"楼栋": "A楼", "所属控制器": "控制器B", "采集点": "数值零", "数据": 0},
        {"楼栋": "A楼", "所属控制器": "控制器B", "采集点": "文本数值", "数据": 1234.5},
        {"楼栋": "A楼", "所属控制器": "控制器B", "采集点": "负数", "数据": -0.25},
    ]
    assert json_result["mode_count"] == 1
    assert json_result["skipped_rows"] == 8


def test_json_source_rows_preserve_time_strings_and_empty_values(tmp_path):
    rows = [
        ["2026-11-04T08:30:15", "08:30:00", "控制器", "冷机模式", 0],
        [None, "", None, "空值", None],
    ]
    source = tmp_path / "source.json"
    _write_json(source, _payload(rows=rows))

    assert ChillerModeUploadService._json_source_rows(source, building="A楼") == rows


def test_json_and_xlsx_use_same_custom_fields_and_mode_mapping(tmp_path):
    payload = _payload()
    source = tmp_path / "source.json"
    excel = tmp_path / "source.xlsx"
    _write_json(source, payload)
    _write_workbook(excel, payload["sheets"])
    service = ChillerModeUploadService({
        "chiller_mode_upload": {
            "fields": {"building": "机楼", "chiller_mode": "运行模式"},
            "mode_value_map": {"1": "自定义模式"},
        },
    })

    json_result = _parse(service, source)
    excel_result = _parse(service, excel)

    assert json_result["rows"] == excel_result["rows"] == [{
        "机楼": "A楼",
        "所属控制器": "控制器A",
        "采集点": "1#冷机模式",
        "数据": 1,
        "运行模式": "自定义模式",
    }]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda data: data.pop("schema_version"), "版本"),
        (lambda data: data.update(schema_version=True), "版本"),
        (lambda data: data.update(schema_version=1.0), "版本"),
        (lambda data: data.update(schema_version=2), "版本"),
        (lambda data: data.update(schema_version="1"), "版本"),
        (lambda data: data.pop("source_family"), "来源"),
        (lambda data: data.update(source_family="other_family"), "来源"),
        (lambda data: data.update(source_family=" chiller_mode_switch_family"), "来源"),
        (lambda data: data.pop("building"), "楼栋"),
        (lambda data: data.update(building="B楼"), "楼栋"),
        (lambda data: data.update(building=True), "楼栋"),
        (lambda data: data.update(building=" A楼"), "楼栋"),
        (lambda data: data.update(sheets={}), "工作表"),
        (lambda data: data.update(sheets=[]), "工作表"),
        (lambda data: data.update(sheets=[None]), "工作表"),
        (lambda data: data["sheets"][0].pop("name"), "工作表"),
        (lambda data: data["sheets"][0].update(name=" "), "工作表"),
        (lambda data: data["sheets"][0].pop("rows"), "工作表"),
        (lambda data: data["sheets"][0].update(rows={}), "工作表"),
        (lambda data: data["sheets"][0].update(rows=[None]), "工作表"),
        (lambda data: data["sheets"][0]["rows"].append([{}, None]), "工作表"),
        (lambda data: data["sheets"][0]["rows"].append([[1]]), "工作表"),
        (lambda data: data["sheets"].append({"name": "第二表", "rows": [None]}), "工作表"),
    ],
)
def test_invalid_json_metadata_or_structure_is_rejected(tmp_path, change, message):
    payload = _payload()
    change(payload)
    source = tmp_path / "bad.json"
    _write_json(source, payload)

    with pytest.raises(ValueError, match=message):
        _parse(ChillerModeUploadService({}), source)


@pytest.mark.parametrize(
    "contents",
    [
        b"{",
        b"[]",
        b"\xff",
        b'{"source_family": NaN}',
        b'{"source_family": Infinity}',
        b'{"schema_version": 2, "schema_version": 1}',
    ],
)
def test_invalid_json_document_is_rejected(tmp_path, contents):
    source = tmp_path / "bad.json"
    source.write_bytes(contents)

    with pytest.raises((ValueError, UnicodeError)):
        _parse(ChillerModeUploadService({}), source)


def test_duplicate_source_family_is_rejected_even_if_last_value_is_correct(tmp_path):
    source = tmp_path / "duplicate.json"
    content = json.dumps(_payload(), ensure_ascii=False)
    content = content.replace('"source_family":', '"source_family": "wrong", "source_family":', 1)
    source.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match="重复字段"):
        _parse(ChillerModeUploadService({}), source)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_numbers_are_rejected_even_in_unused_columns(tmp_path, value):
    payload = _payload()
    payload["sheets"][0]["rows"][0][0] = value
    source = tmp_path / "bad.json"
    _write_json(source, payload)

    with pytest.raises(ValueError):
        _parse(ChillerModeUploadService({}), source)


def test_overflowing_json_number_is_rejected(tmp_path):
    source = tmp_path / "bad.json"
    content = json.dumps(_payload(), ensure_ascii=False).replace('"控制器A"', '1e999', 1)
    source.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError):
        _parse(ChillerModeUploadService({}), source)


@pytest.mark.parametrize(
    "invalid_case",
    ["family", "building", "schema", "structure", "empty", "nonnumeric", "huge_number", "syntax", "duplicate"],
)
def test_one_invalid_json_prevents_any_target_operation(tmp_path, monkeypatch, invalid_case):
    good = tmp_path / "good.xlsx"
    bad = tmp_path / "bad.json"
    _write_workbook(good, _payload()["sheets"])
    payload = _payload(building="B楼")
    if invalid_case == "family":
        payload["source_family"] = "other_family"
    elif invalid_case == "building":
        payload["building"] = "A楼"
    elif invalid_case == "schema":
        payload["schema_version"] = 2
    elif invalid_case == "structure":
        payload["sheets"][0]["rows"].append([{}, None])
    elif invalid_case == "empty":
        payload["sheets"][0]["rows"] = [[None, None, "控制器", "采集点", None]]
    elif invalid_case == "nonnumeric":
        payload["sheets"][0]["rows"][0][4] = "离线"
    elif invalid_case == "huge_number":
        payload["sheets"][0]["rows"][0][4] = 10**400
    if invalid_case == "syntax":
        bad.write_text("{", encoding="utf-8")
    elif invalid_case == "duplicate":
        content = json.dumps(payload, ensure_ascii=False)
        content = content.replace('"source_family":', '"source_family": "wrong", "source_family":', 1)
        bad.write_text(content, encoding="utf-8")
    else:
        _write_json(bad, payload)
    service = ChillerModeUploadService({})
    calls = []

    def forbidden_client(*_args, **_kwargs):
        calls.append("client")
        raise AssertionError("invalid source must not access target")

    monkeypatch.setattr(service, "_new_client", forbidden_client)
    result = service.continue_from_source_units(
        source_units=[
            {"building": "A楼", "file_path": str(good)},
            {"building": "B楼", "file_path": str(bad)},
        ],
        target_descriptor={"target_kind": "base_token_pair", "table_id": "table"},
        emit_log=lambda _text: None,
    )

    assert result["status"] == "failed"
    assert result["deleted_count"] == result["created_count"] == 0
    assert len(result["parsed_files"]) == 1
    assert len(result["failed_files"]) == 1
    assert calls == []


def test_valid_json_and_legacy_xlsx_are_uploaded_together_locally(tmp_path, monkeypatch):
    source = tmp_path / "source.json"
    legacy = tmp_path / "legacy.xlsx"
    _write_json(source, _payload())
    _write_workbook(legacy, _payload(building="B楼")["sheets"])
    service = ChillerModeUploadService({})
    client = FakeClient()
    monkeypatch.setattr(service, "_new_client", lambda *_args, **_kwargs: client)
    monkeypatch.setattr(ChillerModeFocusStateService, "record_rows", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(HvacBitableSyncService, "safe_sync_after_chiller_upload", lambda *_args, **_kwargs: {})

    result = service.continue_from_source_units(
        source_units=[
            {"building": "A楼", "file_path": str(source)},
            {"building": "B楼", "file_path": str(legacy)},
        ],
        target_descriptor={"target_kind": "base_token_pair", "table_id": "table"},
        emit_log=lambda _text: None,
    )

    assert result["status"] == "ok"
    assert (result["deleted_count"], result["created_count"]) == (2, 2)
    assert [row["楼栋"] for row in client.created_rows] == ["A楼", "B楼"]
    assert [row["冷机模式"] for row in client.created_rows] == ["制冷", "制冷"]
    assert [event[0] for event in client.events] == ["list", "create", "delete"]


def test_chiller_safe_replace_keeps_old_records_when_a_create_batch_fails():
    service = ChillerModeUploadService({})
    client = FakeClient(fail_batch=2)
    logs: list[str] = []

    with pytest.raises(TimeoutError, match="read timed out"):
        service._replace_target_records_safely(
            client=client,
            table_id="table",
            rows=[{"value": index} for index in range(3)],
            target=_target(),
            list_field_names=["楼栋"],
            emit_log=logs.append,
        )

    delete_events = [event for event in client.events if event[0] == "delete"]
    assert len(delete_events) == 1
    assert delete_events[0][1] == ["new-1", "new-2"]
    assert "old-1" not in delete_events[0][1]
    assert "old-2" not in delete_events[0][1]
    assert any("旧记录保留=2" in text for text in logs)
    assert client.timeout == 30
    assert client.request_retry_count == 1
