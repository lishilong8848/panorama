from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from openpyxl import Workbook

from app.modules.temperature_humidity_upload.service.temperature_humidity_upload_service import (
    SOURCE_FAMILY,
    TemperatureHumidityUploadService,
)


def _write_source_file(
    path: Path,
    *,
    temperature: float,
    humidity: float,
    invalid_status: bool = False,
    temperature_label: str = "温度",
    humidity_label: str = "湿度",
) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "sheet1"
    sheet.merge_cells("C3:C4")
    sheet["C3"] = "B-144变电所P1_TH-01"
    sheet["D3"] = temperature_label
    sheet["E3"] = temperature
    sheet["D4"] = humidity_label
    sheet["E4"] = humidity
    sheet["D6"] = "B-111-CRAH-01_运行状态"
    sheet["E6"] = 1
    sheet["D7"] = "B-111-CRAH-02_运行状态"
    sheet["E7"] = 0
    if invalid_status:
        sheet["D8"] = "B-111-CRAH-03_运行状态"
        sheet["E8"] = 2
    workbook.save(path)
    workbook.close()


class FakeInternalClient:
    def __init__(
        self,
        files: dict[str, Path],
        *,
        entry_overrides: dict[str, dict] | None = None,
    ):
        self.files = files
        self.entry_overrides = entry_overrides or {}
        self.refresh_calls = []

    def source_index_batch(self, queries, *, default_limit=50):
        output = []
        for index, query in enumerate(queries):
            building = query["building"]
            path = self.files.get(building)
            entries = []
            if path is not None:
                entry = {
                    "entry_id": f"entry-{building}",
                    "source_family": query["source_family"],
                    "building": building,
                    "bucket_kind": "daily",
                    "bucket_key": query["bucket_or_date"],
                    "duty_date": query["bucket_or_date"],
                    "status": "ready",
                    "file_verified": True,
                    "file_path": str(path),
                    "downloaded_at": "2026-07-18 09:20:00",
                }
                entry.update(self.entry_overrides.get(building, {}))
                entries.append(entry)
            output.append({"index": index, "ok": True, "entries": entries})
        return output

    def refresh_latest_source_cache(self, *, source_family, buildings, target_bucket_key=""):
        self.refresh_calls.append((source_family, list(buildings), target_bucket_key))
        return {"ok": True, "accepted_count": len(buildings)}


class FakeBitableClient:
    def __init__(self, *, fail_create: bool = False):
        self.fail_create = fail_create
        self.create_failed = False
        self.created = []
        self.deleted = []
        self.events = []
        self.next_record_number = 1
        self.records = {
            "rec-old-1": {
                "楼栋": "A楼",
                "位置": "旧温湿度位置",
                "温度": 19.5,
                "湿度": 45.0,
            }
        }

    def list_fields(self, table_id, page_size=500):
        return [
            {"field_name": "楼栋", "type": 3},
            {"field_name": "位置", "type": 1},
            {"field_name": "温度", "type": 2},
            {"field_name": "湿度", "type": 2},
            {"field_name": "运行状态", "type": 3},
        ]

    def list_records(self, table_id, page_size=500, max_records=0, *, view_id="", filter_formula="", field_names=None):
        return [
            {"record_id": record_id, "fields": dict(fields)}
            for record_id, fields in self.records.items()
        ]

    def batch_create_records(self, table_id, fields_list, batch_size=200, progress_callback=None):
        self.events.append("create")
        if self.fail_create and not self.create_failed:
            self.create_failed = True
            raise RuntimeError("create failed")
        for fields in fields_list:
            record_id = f"rec-new-{self.next_record_number}"
            self.next_record_number += 1
            self.records[record_id] = dict(fields)
            self.created.append(dict(fields))
        return [{"code": 0}]

    def batch_delete_records(self, table_id, record_ids, batch_size=500, progress_callback=None):
        self.events.append("delete")
        deleted_count = 0
        for record_id in record_ids:
            if record_id in self.records:
                self.records.pop(record_id)
                deleted_count += 1
            self.deleted.append(record_id)
        return deleted_count


def _runtime_config(buildings: list[str], *, shared_root: Path | None = None) -> dict:
    config = {
        "temperature_humidity_upload": {
            "enabled": True,
            "buildings": buildings,
            "trigger_missing_download": True,
            "wait_source_timeout_sec": 0,
            "target": {
                "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
                "table_id": "tblfnTbEWK9607zV",
                "fields": {
                    "building": "楼栋",
                    "position": "位置",
                    "temperature": "温度",
                    "humidity": "湿度",
                    "running_status": "运行状态",
                },
            },
        }
    }
    if shared_root is not None:
        config["shared_bridge"] = {
            "root_dir": str(shared_root),
            "external_root_dir": str(shared_root),
        }
    return config


def test_parse_workbook_merges_temperature_humidity_and_maps_binary_status(tmp_path):
    source = tmp_path / "source.xlsx"
    _write_source_file(source, temperature=27.41, humidity=52.79)

    result = TemperatureHumidityUploadService.parse_workbook(source, building="A楼")

    assert result["temperature_location_count"] == 1
    assert result["status_location_count"] == 2
    assert result["unparsed_status_count"] == 0
    assert result["rows"][0] == {
        "building": "A楼",
        "position": "B-144变电所P1_TH-01",
        "temperature": 27.41,
        "humidity": 52.79,
    }
    assert result["rows"][1]["running_status"] == "开启"
    assert result["rows"][2]["running_status"] == "关闭"


def test_parse_workbook_accepts_labels_containing_temperature_or_humidity(tmp_path):
    source = tmp_path / "source-with-prefixed-labels.xlsx"
    _write_source_file(
        source,
        temperature=28.88,
        humidity=61.25,
        temperature_label="室内温度值",
        humidity_label="相对湿度",
    )

    result = TemperatureHumidityUploadService.parse_workbook(source, building="C楼")

    assert result["temperature_location_count"] == 1
    assert result["rows"][0] == {
        "building": "C楼",
        "position": "B-144变电所P1_TH-01",
        "temperature": 28.88,
        "humidity": 61.25,
    }


def test_parse_workbook_rejects_non_binary_running_status(tmp_path):
    source = tmp_path / "invalid-status.xlsx"
    _write_source_file(
        source,
        temperature=27.41,
        humidity=52.79,
        invalid_status=True,
    )

    with pytest.raises(ValueError, match="运行状态必须为0或1"):
        TemperatureHumidityUploadService.parse_workbook(source, building="A楼")


def test_parse_workbook_does_not_reuse_temperature_position_across_status_row(tmp_path):
    source = tmp_path / "broken-temperature-group.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet["C3"] = "B-144变电所P1_TH-01"
    sheet["D3"] = "温度"
    sheet["E3"] = 27.41
    sheet["D4"] = "B-111-CRAH-01_运行状态"
    sheet["E4"] = 1
    sheet["D5"] = "湿度"
    sheet["E5"] = 52.79
    workbook.save(source)
    workbook.close()

    with pytest.raises(ValueError, match="缺少位置"):
        TemperatureHumidityUploadService.parse_workbook(source, building="A楼")


def test_run_validates_then_clears_old_records_before_uploading_snapshot(tmp_path):
    files = {}
    for index, building in enumerate(("A楼", "B楼"), start=1):
        path = tmp_path / f"{building}.xlsx"
        _write_source_file(path, temperature=20 + index, humidity=50 + index)
        files[building] = path
    internal = FakeInternalClient(files)
    bitable = FakeBitableClient()

    result = TemperatureHumidityUploadService(
        _runtime_config(["A楼", "B楼"]),
        internal_client=internal,
        bitable_client=bitable,
    ).run(source_date="2026-07-18")

    assert result["status"] == "success"
    assert result["uploaded_count"] == 6
    assert result["temperature_location_count"] == 2
    assert result["status_location_count"] == 4
    assert result["unparsed_status_count"] == 0
    assert bitable.events == ["delete", "create"]
    assert bitable.deleted == ["rec-old-1"]
    temperature_row = next(row for row in bitable.created if row.get("温度") == 21.0)
    assert temperature_row == {
        "楼栋": "A楼",
        "位置": "B-144变电所P1_TH-01",
        "温度": 21.0,
        "湿度": 51.0,
    }
    opened = next(row for row in bitable.created if row.get("位置") == "B-111-CRAH-01_运行状态")
    assert opened["运行状态"] == "开启"


def test_create_failure_restores_old_record_snapshot(tmp_path):
    source = tmp_path / "A楼.xlsx"
    _write_source_file(source, temperature=27.0, humidity=50.0)
    bitable = FakeBitableClient(fail_create=True)

    with pytest.raises(RuntimeError, match="create failed"):
        TemperatureHumidityUploadService(
            _runtime_config(["A楼"]),
            internal_client=FakeInternalClient({"A楼": source}),
            bitable_client=bitable,
        ).run(source_date="2026-07-18")

    assert bitable.events == ["delete", "create", "create"]
    assert bitable.deleted == ["rec-old-1"]
    assert list(bitable.records.values()) == [
        {
            "楼栋": "A楼",
            "位置": "旧温湿度位置",
            "温度": 19.5,
            "湿度": 45.0,
        }
    ]


def test_missing_source_requests_internal_refresh_without_deleting_old_records():
    internal = FakeInternalClient({})
    bitable = FakeBitableClient()
    source_date = datetime.now().strftime("%Y-%m-%d")

    with pytest.raises(RuntimeError, match="B楼"):
        TemperatureHumidityUploadService(
            _runtime_config(["A楼", "B楼"]),
            internal_client=internal,
            bitable_client=bitable,
        ).run(source_date=source_date)

    assert internal.refresh_calls == [
        ("air_conditioner_temperature_humidity_family", ["A楼", "B楼"], source_date)
    ]
    assert bitable.events == []
    assert bitable.deleted == []


@pytest.mark.parametrize(
    "entry_override",
    [
        {"source_family": "branch_power_family"},
        {"bucket_key": "2026-07-17", "duty_date": "2026-07-17"},
        {"bucket_kind": "latest"},
    ],
)
def test_wrong_source_index_entry_is_rejected_without_deleting_old_records(
    tmp_path,
    entry_override,
):
    source = tmp_path / "A楼.xlsx"
    _write_source_file(source, temperature=27.0, humidity=50.0)
    bitable = FakeBitableClient()
    source_date = datetime.now().strftime("%Y-%m-%d")
    internal = FakeInternalClient(
        {"A楼": source},
        entry_overrides={"A楼": entry_override},
    )

    with pytest.raises(RuntimeError, match="A楼"):
        TemperatureHumidityUploadService(
            _runtime_config(["A楼"]),
            internal_client=internal,
            bitable_client=bitable,
        ).run(source_date=source_date)

    assert internal.refresh_calls == [
        ("air_conditioner_temperature_humidity_family", ["A楼"], source_date)
    ]
    assert bitable.events == []


def test_internal_absolute_path_is_remapped_through_external_shared_root(tmp_path):
    shared_root = tmp_path / "external-share"
    relative_path = Path(
        "空调温湿度源文件",
        "202607",
        "20260718--整日",
        "20260718--整日--空调温湿度源文件--A楼.xlsx",
    )
    mapped_source = shared_root / relative_path
    mapped_source.parent.mkdir(parents=True, exist_ok=True)
    _write_source_file(mapped_source, temperature=27.0, humidity=50.0)
    internal = FakeInternalClient(
        {"A楼": Path(r"D:\share\internal-only\A楼.xlsx")},
        entry_overrides={"A楼": {"relative_path": relative_path.as_posix()}},
    )
    bitable = FakeBitableClient()

    result = TemperatureHumidityUploadService(
        _runtime_config(["A楼"], shared_root=shared_root),
        internal_client=internal,
        bitable_client=bitable,
    ).run(source_date="2026-07-18")

    assert result["status"] == "success"
    assert result["source_files"]["A楼"] == str(mapped_source)
    assert result["uploaded_count"] == 3


def _json_payload(*, building="A楼"):
    return {
        "schema_version": 1,
        "business_date": "2026-07-18",
        "query_end": "2026-07-18 10:00:00",
        "source_family": SOURCE_FAMILY,
        "building": building,
        "sheets": [
            {
                "name": "sheet1",
                "rows": [
                    [None, None, "机房一", "室内温度值", "26.5℃"],
                    [None, None, None, "相对湿度", "52.5%"],
                    [],
                    [None, None, None, "空调一运行状态", "1"],
                    [None, None, None, "空调二运行状态", 0],
                    [None, None, "机房二", "温度", "1,234.5"],
                    [None, None, None, "湿度", 40.5],
                ],
            },
            {
                "name": "ignored",
                "rows": [[None, None, None, "无效的运行状态", 2]],
            },
        ],
    }


def _write_json(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


@pytest.mark.parametrize("excel_suffix", [".xlsx", ".xlsm"])
def test_json_and_excel_parse_the_same_first_sheet(tmp_path, excel_suffix):
    payload = _json_payload()
    json_path = tmp_path / "source.JSON"
    excel_path = tmp_path / f"source{excel_suffix}"
    _write_json(json_path, payload)

    workbook = Workbook()
    for index, source_sheet in enumerate(payload["sheets"]):
        sheet = workbook.active if index == 0 else workbook.create_sheet()
        sheet.title = source_sheet["name"]
        for row in source_sheet["rows"]:
            sheet.append(row)
        if index == 0:
            sheet.merge_cells("C1:C2")
            sheet.merge_cells("C6:C7")
    workbook.save(excel_path)
    workbook.close()

    json_result = TemperatureHumidityUploadService.parse_workbook(json_path, building="A楼")
    excel_result = TemperatureHumidityUploadService.parse_workbook(excel_path, building="A楼")

    assert {key: value for key, value in json_result.items() if key != "file_path"} == {
        key: value for key, value in excel_result.items() if key != "file_path"
    }
    assert json_result["sheet_name"] == "sheet1"
    assert json_result["temperature_location_count"] == 2
    assert json_result["status_location_count"] == 2
    assert [item.get("running_status") for item in json_result["rows"][2:]] == ["开启", "关闭"]


@pytest.mark.parametrize("excel_suffix", [".xlsx", ".xlsm"])
def test_source_index_selects_json_and_legacy_excel(tmp_path, excel_suffix):
    json_path = tmp_path / "A楼.json"
    excel_path = tmp_path / f"B楼{excel_suffix}"
    _write_json(json_path, _json_payload())
    _write_source_file(excel_path, temperature=27.0, humidity=50.0)
    service = TemperatureHumidityUploadService(_runtime_config(["A楼", "B楼"]))

    selected, missing, errors = service._query_source_files(
        FakeInternalClient({"A楼": json_path, "B楼": excel_path}),
        source_family=SOURCE_FAMILY,
        source_date="2026-07-18",
        buildings=["A楼", "B楼"],
    )

    assert selected == {"A楼": json_path, "B楼": excel_path}
    assert missing == []
    assert errors == []


def test_source_index_rejects_json_under_another_configured_family(tmp_path):
    json_path = tmp_path / "A楼.json"
    excel_path = tmp_path / "B楼.xlsx"
    _write_json(json_path, _json_payload())
    _write_source_file(excel_path, temperature=27.0, humidity=50.0)
    service = TemperatureHumidityUploadService(_runtime_config(["A楼", "B楼"]))

    selected, missing, errors = service._query_source_files(
        FakeInternalClient({"A楼": json_path, "B楼": excel_path}),
        source_family="branch_power_family",
        source_date="2026-07-18",
        buildings=["A楼", "B楼"],
    )

    assert selected == {"B楼": excel_path}
    assert missing == ["A楼"]
    assert errors == []


def test_json_relative_path_is_remapped_through_shared_root(tmp_path):
    shared_root = tmp_path / "external-share"
    relative_path = Path("空调温湿度源文件", "20260718", "A楼.json")
    mapped_source = shared_root / relative_path
    mapped_source.parent.mkdir(parents=True, exist_ok=True)
    _write_json(mapped_source, _json_payload())
    internal = FakeInternalClient(
        {"A楼": Path(r"D:\share\internal-only\A楼.json")},
        entry_overrides={"A楼": {"relative_path": relative_path.as_posix()}},
    )
    service = TemperatureHumidityUploadService(_runtime_config(["A楼"], shared_root=shared_root))

    selected, missing, errors = service._query_source_files(
        internal, source_family=SOURCE_FAMILY, source_date="2026-07-18", buildings=["A楼"]
    )

    assert selected == {"A楼": mapped_source}
    assert missing == []
    assert errors == []
    assert service.parse_workbook(selected["A楼"], building="A楼")["temperature_location_count"] == 2


def test_json_with_utf8_bom_is_supported(tmp_path):
    source = tmp_path / "bom.json"
    source.write_text(json.dumps(_json_payload(), ensure_ascii=False), encoding="utf-8-sig")

    result = TemperatureHumidityUploadService.parse_workbook(source, building="A楼")

    assert result["temperature_location_count"] == 2
    assert result["status_location_count"] == 2


def test_run_accepts_mixed_json_and_excel_with_fake_clients(tmp_path):
    json_path = tmp_path / "A楼.json"
    excel_path = tmp_path / "B楼.xlsx"
    _write_json(json_path, _json_payload())
    _write_source_file(excel_path, temperature=27.0, humidity=50.0)
    target = FakeBitableClient()

    result = TemperatureHumidityUploadService(
        _runtime_config(["A楼", "B楼"]),
        internal_client=FakeInternalClient({"A楼": json_path, "B楼": excel_path}),
        bitable_client=target,
    ).run(source_date="2026-07-18")

    assert result["source_files"] == {"A楼": str(json_path), "B楼": str(excel_path)}
    assert result["temperature_location_count"] == 3
    assert result["status_location_count"] == 4
    assert result["uploaded_count"] == 7
    assert any(row.get("楼栋") == "A楼" and row.get("温度") == 26.5 for row in target.created)
    assert any(row.get("楼栋") == "B楼" and row.get("温度") == 27.0 for row in target.created)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda data: data.update(schema_version=2), "版本"),
        (lambda data: data.pop("source_family"), "来源"),
        (lambda data: data.update(source_family="branch_power_family"), "来源"),
        (lambda data: data.update(source_family=None), "来源"),
        (lambda data: data.update(source_family=f" {SOURCE_FAMILY}"), "来源"),
        (lambda data: data.pop("building"), "楼栋"),
        (lambda data: data.update(building="B楼"), "楼栋"),
        (lambda data: data.update(building=True), "楼栋"),
        (lambda data: data.update(building=" A楼"), "楼栋"),
        (lambda data: data.update(sheets={}), "工作表"),
        (lambda data: data.update(sheets=[]), "工作表"),
        (lambda data: data.update(sheets=[None]), "工作表"),
        (lambda data: data["sheets"][0].pop("name"), "工作表"),
        (lambda data: data["sheets"][0].update(name=""), "工作表"),
        (lambda data: data["sheets"][0].pop("rows"), "工作表"),
        (lambda data: data["sheets"][0].update(rows={}), "工作表"),
        (lambda data: data["sheets"][0].update(rows=[{}]), "工作表"),
        (lambda data: data["sheets"][0].update(rows=[[[1]]]), "工作表"),
        (lambda data: data["sheets"][0].update(rows=[[float("inf")]]), "格式无效"),
        (lambda data: data["sheets"][1].update(rows=[None]), "工作表"),
    ],
)
def test_invalid_json_metadata_and_structure_are_rejected(tmp_path, change, message):
    payload = _json_payload()
    change(payload)
    source = tmp_path / "invalid.json"
    _write_json(source, payload)

    with pytest.raises(ValueError, match=message):
        TemperatureHumidityUploadService.parse_workbook(source, building="A楼")


@pytest.mark.parametrize(
    ("contents", "message"),
    [
        (b"{", "格式无效"),
        (b"[]", "顶层"),
        (b"\xff", "格式无效"),
        (b'{"source_family": NaN}', "无效数字"),
    ],
)
def test_invalid_json_document_is_rejected(tmp_path, contents, message):
    source = tmp_path / "invalid.json"
    source.write_bytes(contents)

    with pytest.raises(ValueError, match=message):
        TemperatureHumidityUploadService.parse_workbook(source, building="A楼")


def test_duplicate_source_family_is_rejected_even_when_last_value_matches(tmp_path):
    source = tmp_path / "duplicate.json"
    content = json.dumps(_json_payload(), ensure_ascii=False)
    content = content.replace('"source_family":', '"source_family": "wrong", "source_family":', 1)
    source.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match="重复字段"):
        TemperatureHumidityUploadService.parse_workbook(source, building="A楼")


def test_nonfinite_json_number_is_rejected(tmp_path):
    source = tmp_path / "overflow.json"
    content = json.dumps(_json_payload(), ensure_ascii=False)
    source.write_text(content.replace('"26.5℃"', "1e999", 1), encoding="utf-8")

    with pytest.raises(ValueError, match="工作表"):
        TemperatureHumidityUploadService.parse_workbook(source, building="A楼")


def test_json_integer_outside_float_range_uses_numeric_error_protection(tmp_path):
    payload = _json_payload()
    payload["sheets"][0]["rows"][0][4] = 10**400
    source = tmp_path / "huge-number.json"
    _write_json(source, payload)

    with pytest.raises(ValueError, match="不是有效数字"):
        TemperatureHumidityUploadService.parse_workbook(source, building="A楼")


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([[None, None, "机房", "温度", "not a number"]], "不是有效数字"),
        ([[None, None, "机房", "温度", True]], "不是有效数字"),
        ([[None, None, "机房", "温度", 25]], "未成对温湿度"),
        ([[None, None, None, "温度", 25]], "缺少位置"),
        ([[None, None, None, "空调运行状态", 2]], "运行状态必须为0或1"),
        ([[None, None, None, "空调运行状态", True]], "运行状态必须为0或1"),
        (
            [
                [None, None, "机房", "温度", 25],
                [None, None, None, "空调运行状态", 1],
                [None, None, None, "湿度", 50],
            ],
            "缺少位置",
        ),
        ([[]], "未解析到可上传数据"),
    ],
)
def test_json_uses_existing_row_error_protection(tmp_path, rows, message):
    payload = _json_payload()
    payload["sheets"][0]["rows"] = rows
    source = tmp_path / "invalid-row.json"
    excel_source = tmp_path / "invalid-row.xlsx"
    _write_json(source, payload)
    workbook = Workbook()
    for row in rows:
        workbook.active.append(row)
    workbook.save(excel_source)
    workbook.close()

    with pytest.raises(ValueError, match=message) as json_error:
        TemperatureHumidityUploadService.parse_workbook(source, building="A楼")
    with pytest.raises(ValueError, match=message) as excel_error:
        TemperatureHumidityUploadService.parse_workbook(excel_source, building="A楼")
    assert str(json_error.value) == str(excel_error.value)


@pytest.mark.parametrize("invalid_case", ["building", "family", "row", "syntax", "date", "version", "query_date"])
def test_invalid_json_stops_before_any_target_access(tmp_path, invalid_case):
    good_source = tmp_path / "A楼.json"
    bad_source = tmp_path / "B楼.json"
    _write_json(good_source, _json_payload())
    bad_payload = _json_payload(building="B楼")
    if invalid_case == "building":
        bad_payload["building"] = "A楼"
    elif invalid_case == "family":
        bad_payload["source_family"] = "branch_power_family"
    elif invalid_case == "row":
        bad_payload["sheets"][0]["rows"][3][4] = 2
    elif invalid_case == "date":
        bad_payload["business_date"] = "2026-07-17"
    elif invalid_case == "version":
        bad_payload["schema_version"] = 2
    elif invalid_case == "query_date":
        bad_payload["query_end"] = "2026-07-19 10:00:00"
    if invalid_case == "syntax":
        bad_source.write_text("{", encoding="utf-8")
    else:
        _write_json(bad_source, bad_payload)

    class NoTargetAccess:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected target access: {name}")

    service = TemperatureHumidityUploadService(
        _runtime_config(["A楼", "B楼"]),
        internal_client=FakeInternalClient({"A楼": good_source, "B楼": bad_source}),
        bitable_client=NoTargetAccess(),
    )

    with pytest.raises(ValueError):
        service.run(source_date="2026-07-18")
