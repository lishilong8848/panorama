from __future__ import annotations

import json
import socket
from datetime import datetime, timedelta
from pathlib import Path

import openpyxl
import pytest

from app.modules.report_pipeline.core.metrics_rules import BASE_RULES
from pipeline_utils import load_calc_module


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    def deny(*_args, **_kwargs):
        pytest.fail("月报本地测试不得访问网络")

    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket.socket, "connect_ex", deny)


@pytest.fixture
def calc():
    return load_calc_module()


def _payload(business_date="2026-10-31", building="A楼"):
    start = datetime.strptime(business_date, "%Y-%m-%d")
    headers = [
        start - timedelta(minutes=10),
        *[start + timedelta(hours=hour) for hour in range(25)],
        start + timedelta(days=1, minutes=10),
    ]
    rows = [
        ["月报"],
        [None, None, None, None, *[value.isoformat() for value in headers], "最大值", "最小值", "平均值"],
        [],
    ]
    for index, (metric, _method) in enumerate(BASE_RULES):
        rows.append([None, "类型" if index == 0 else None, "分类" if index == 0 else None,
                     metric, 99999, 100, *([110] * 23), 130, 99999, 99999, 1, 99999])
    rows.append([None, None, None, "总_电度", 99999, 500, *([515] * 23), 535, 99999, 99999, 1, 99999])
    return {
        "schema_version": 1,
        "source_family": "monthly_report_family",
        "building": building,
        "business_date": business_date,
        "sheets": [{"name": "月报", "rows": rows}, {"name": "其他", "rows": [["不参与计算"]]}],
    }


def _write_json(path: Path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _write_xlsx(path: Path, payload):
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    for row_number, row in enumerate(payload["sheets"][0]["rows"], 1):
        for col_number, value in enumerate(row, 1):
            if row_number == 2 and isinstance(value, str):
                try:
                    value = datetime.fromisoformat(value)
                except ValueError:
                    pass
            if value is not None:
                worksheet.cell(row_number, col_number, value)
    workbook.create_sheet("其他").append(["不参与计算"])
    workbook.save(path)
    workbook.close()
    return path


@pytest.mark.parametrize("business_date", ["2026-08-24", "2026-10-31", "2026-12-31", "2024-02-29"])
@pytest.mark.parametrize("building", ["A楼", "B楼"])
def test_json_and_xlsx_calculate_identically_across_boundaries(tmp_path, calc, business_date, building):
    payload = _payload(business_date, building)
    json_path = _write_json(tmp_path / "source.json", payload)
    xlsx_path = _write_xlsx(tmp_path / "source.xlsx", payload)

    json_result = calc.calculate_monthly_report(str(json_path), building, business_date)
    xlsx_result = calc.calculate_monthly_report(str(xlsx_path), building, business_date)

    json_data = json_result.to_dict()
    xlsx_data = xlsx_result.to_dict()
    json_data.pop("source_file")
    xlsx_data.pop("source_file")
    assert json_data == xlsx_data
    assert json_result.month == business_date[:7]
    assert json_result.missing_metrics == []
    assert json_result.values["IT负载率"] == 110.4
    assert json_result.values["市电总用电量"] == 30
    assert json_result.values["冷却塔功率"] == 2760
    assert json_result.values["光伏发电量"] == (35 if building == "A楼" else 30)
    assert json_result.values["PUE"] == (0.833 if building == "A楼" else 1.0)


def test_json_uses_embedded_building_and_date_when_not_supplied(tmp_path, calc):
    path = _write_json(tmp_path / "unrelated_filename.json", _payload("2026-10-31", "B楼"))
    result = calc.calculate_monthly_report(str(path))
    assert (result.building, result.month, result.values["PUE"]) == ("B楼", "2026-10", 1.0)


def test_json_and_xlsx_match_when_all_time_columns_are_within_the_day(tmp_path, calc):
    payload = _payload()
    rows = payload["sheets"][0]["rows"]
    rows[1] = rows[1][:4] + rows[1][5:-4] + rows[1][-3:]
    for index in range(3, len(rows)):
        values = rows[index][5:-4]
        rows[index] = rows[index][:4] + values + [max(values), min(values), sum(values) / len(values)]
    json_path = _write_json(tmp_path / "source.json", payload)
    xlsx_path = _write_xlsx(tmp_path / "source.xlsx", payload)

    json_result = calc.calculate_monthly_report(str(json_path), "A楼", "2026-10-31")
    xlsx_result = calc.calculate_monthly_report(str(xlsx_path), "A楼", "2026-10-31")
    assert json_result.values == xlsx_result.values
    assert json_result.values["IT负载率"] == 110.4
    assert json_result.values["冷却塔功率"] == 2760


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda p: p.pop("schema_version"), "schema_version"),
        (lambda p: p.update(schema_version=True), "schema_version"),
        (lambda p: p.update(schema_version=1.0), "schema_version"),
        (lambda p: p.update(schema_version=2), "schema_version"),
        (lambda p: p.update(source_family="another_family"), "来源"),
        (lambda p: p.pop("source_family"), "来源"),
        (lambda p: p.update(building=""), "楼栋"),
        (lambda p: p.update(building=" A楼"), "楼栋"),
        (lambda p: p.update(business_date="2026-2-01"), "业务日期"),
        (lambda p: p.update(business_date="2026-02-30"), "业务日期"),
        (lambda p: p.pop("business_date"), "业务日期"),
        (lambda p: p.update(sheets=[]), "工作表"),
        (lambda p: p["sheets"][0].update(name=""), "工作表"),
        (lambda p: p["sheets"][0].update(rows=[]), "工作表"),
        (lambda p: p["sheets"][0].update(rows=[{}]), "一维数组"),
        (lambda p: p["sheets"][0]["rows"][0].append({}), "非标量"),
        (lambda p: p["sheets"][1].update(rows=[None]), "一维数组"),
        (lambda p: p["sheets"][0]["rows"][3].__setitem__(5, float("inf")), "无效数字"),
    ],
)
def test_json_rejects_invalid_contract(tmp_path, calc, mutate, message):
    payload = _payload()
    mutate(payload)
    path = _write_json(tmp_path / "source.json", payload)
    with pytest.raises(ValueError, match=message):
        calc.calculate_monthly_report(str(path), "A楼", "2026-10-31")


def test_json_rejects_duplicate_keys_and_invalid_encoding(tmp_path, calc):
    path = tmp_path / "source.json"
    path.write_text('{"source_family":"bad","source_family":"monthly_report_family"}', encoding="utf-8")
    with pytest.raises(ValueError, match="重复字段"):
        calc.calculate_monthly_report(str(path))
    path.write_bytes(b"\xff")
    with pytest.raises(ValueError, match="格式无效"):
        calc.calculate_monthly_report(str(path))


@pytest.mark.parametrize(
    ("expected_building", "expected_date", "message"),
    [("B楼", "2026-10-31", "楼栋"), ("A楼", "2026-11-01", "日期"),
     ("A楼", "2026-2-01", "日期"), ("", "2026-10-31", "楼栋")],
)
def test_json_rejects_expected_building_or_date_mismatch(tmp_path, calc, expected_building, expected_date, message):
    path = _write_json(tmp_path / "source.json", _payload())
    with pytest.raises(ValueError, match=message):
        calc.calculate_monthly_report(str(path), expected_building, expected_date)


@pytest.mark.parametrize(
    ("col", "message"),
    [(5, "业务日00:00"), (29, "次日00:00")],
)
def test_json_rejects_missing_boundary(tmp_path, calc, col, message):
    payload = _payload()
    payload["sheets"][0]["rows"][1][col] = "2026-10-31T01:00:00"
    path = _write_json(tmp_path / "source.json", payload)
    with pytest.raises(ValueError, match=message):
        calc.calculate_monthly_report(str(path), "A楼", "2026-10-31")


@pytest.mark.parametrize("business_date", ["2026-10-31", "2026-12-31", "2024-02-29"])
@pytest.mark.parametrize("hours", [(1,), (12,), (23,), (1, 12, 23)])
def test_json_rejects_missing_interior_hours(tmp_path, calc, business_date, hours):
    payload = _payload(business_date)
    rows = payload["sheets"][0]["rows"]
    for col in sorted((5 + hour for hour in hours), reverse=True):
        for row in [rows[1], *rows[3:]]:
            row.pop(col)
    path = _write_json(tmp_path / "source.json", payload)
    with pytest.raises(ValueError, match="缺少业务日整点") as error:
        calc.calculate_monthly_report(str(path), "A楼", business_date)
    start = datetime.strptime(business_date, "%Y-%m-%d")
    for hour in hours:
        assert (start + timedelta(hours=hour)).strftime("%Y-%m-%d %H:%M") in str(error.value)


def test_json_extra_non_hour_column_cannot_replace_missing_hour(tmp_path, calc):
    payload = _payload()
    payload["sheets"][0]["rows"][1][17] = "2026-10-31T12:30:00"
    path = _write_json(tmp_path / "source.json", payload)
    with pytest.raises(ValueError, match="2026-10-31 12:00"):
        calc.calculate_monthly_report(str(path), "A楼", "2026-10-31")


def test_complete_json_keeps_extra_in_day_column_behavior(tmp_path, calc):
    payload = _payload()
    rows = payload["sheets"][0]["rows"]
    rows[1].insert(18, "2026-10-31T12:30:00")
    for row in rows[3:]:
        row.insert(18, 110)
    json_path = _write_json(tmp_path / "source.json", payload)
    xlsx_path = _write_xlsx(tmp_path / "source.xlsx", payload)
    json_result = calc.calculate_monthly_report(str(json_path), "A楼", "2026-10-31")
    xlsx_result = calc.calculate_monthly_report(str(xlsx_path), "A楼", "2026-10-31")
    assert json_result.values == xlsx_result.values
    assert json_result.values["冷却塔功率"] == 2870


def test_excel_still_accepts_missing_interior_hour(tmp_path, calc):
    payload = _payload()
    rows = payload["sheets"][0]["rows"]
    for row in [rows[1], *rows[3:]]:
        row.pop(17)
    path = _write_xlsx(tmp_path / "source.xlsx", payload)
    result = calc.calculate_monthly_report(str(path), "A楼", "2026-10-31")
    assert result.values["市电总用电量"] == 30


@pytest.mark.parametrize("entrypoint", ["items", "mapping"])
def test_missing_hour_blocks_batch_save_and_upload(tmp_path, calc, monkeypatch, entrypoint):
    good = _write_json(tmp_path / "good.json", _payload())
    payload = _payload(building="B楼")
    for row in [payload["sheets"][0]["rows"][1], *payload["sheets"][0]["rows"][3:]]:
        row.pop(17)
    bad = _write_json(tmp_path / "bad.json", payload)
    calls = []
    monkeypatch.setattr(calc, "save_results", lambda *_a, **_k: calls.append("save"))
    monkeypatch.setattr(calc, "upload_results_to_feishu", lambda *_a, **_k: calls.append("upload"))
    with pytest.raises(ValueError, match="2026-10-31 12:00"):
        if entrypoint == "items":
            calc.run_with_explicit_file_items(
                {}, [{"building": "A楼", "file_path": str(good), "upload_date": "2026-10-31"},
                     {"building": "B楼", "file_path": str(bad), "upload_date": "2026-10-31"}],
                upload=True, save_json=True, emit_log=lambda _line: None,
            )
        else:
            calc.run_with_explicit_files(
                {"input": {"buildings": ["A楼", "B楼"]}}, {"A楼": str(good), "B楼": str(bad)},
                upload=True, save_json=True,
            )
    assert calls == []


@pytest.mark.parametrize(
    "case",
    ["unknown_header", "duplicate_header", "duplicate_metric", "missing_value", "nan_value", "stats"],
)
def test_json_rejects_incomplete_calculation_data(tmp_path, calc, case):
    payload = _payload()
    rows = payload["sheets"][0]["rows"]
    if case == "unknown_header":
        rows[1][6] = "not a date"
    elif case == "duplicate_header":
        rows[1][6] = rows[1][5]
    elif case == "duplicate_metric":
        rows.append(rows[3][:])
    elif case == "missing_value":
        rows[3][7] = None
    elif case == "nan_value":
        rows[3][6] = "NaN"
    else:
        rows[1][-1] = None
    path = _write_json(tmp_path / "source.json", payload)
    with pytest.raises(ValueError):
        calc.calculate_monthly_report(str(path), "A楼", "2026-10-31")


def test_json_rejects_numeric_overflow_during_calculation(tmp_path, calc):
    payload = _payload()
    row = next(row for row in payload["sheets"][0]["rows"][3:] if row[3] == "市电总用电量")
    row[5], row[29] = -1e308, 1e308
    path = _write_json(tmp_path / "source.json", payload)
    with pytest.raises(ValueError, match="计算结果无效"):
        calc.calculate_monthly_report(str(path), "A楼", "2026-10-31")


def test_legacy_xlsx_without_date_still_reads_and_keeps_missing_metric_behavior(tmp_path, calc):
    payload = _payload()
    payload["sheets"][0]["rows"].pop(3)
    path = _write_xlsx(tmp_path / "A楼_legacy.xlsx", payload)
    result = calc.calculate_monthly_report(str(path))
    assert result.building == "A楼"
    assert result.month == "2026-10"
    assert "IT负载率" in result.missing_metrics
    assert result.values["IT负载率"] == 0


def test_json_keeps_existing_missing_metric_behavior_for_building_specific_sources(tmp_path, calc):
    payload = _payload()
    payload["sheets"][0]["rows"].pop(3)
    path = _write_json(tmp_path / "source.json", payload)
    result = calc.calculate_monthly_report(str(path), "A楼", "2026-10-31")
    assert "IT负载率" in result.missing_metrics
    assert result.values["IT负载率"] == 0


def test_legacy_xlsx_file_items_still_work_without_network(tmp_path, calc):
    path = _write_xlsx(tmp_path / "source.xlsx", _payload())
    result = calc.run_with_explicit_file_items(
        {}, [{"building": "A楼", "file_path": str(path), "upload_date": "2026-10-31"}],
        upload=False, emit_log=lambda _line: None,
    )
    assert len(result) == 1
    assert result[0].values["市电总用电量"] == 30


def test_local_file_items_parse_json_and_mixed_batch_stops_before_save_or_upload(tmp_path, calc, monkeypatch):
    good = _write_json(tmp_path / "good.json", _payload())
    bad_payload = _payload()
    bad_payload["business_date"] = "2026-10-30"
    bad = _write_json(tmp_path / "bad.json", bad_payload)
    calls = []
    monkeypatch.setattr(calc, "save_results", lambda *_a, **_k: calls.append("save"))
    monkeypatch.setattr(calc, "upload_results_to_feishu", lambda *_a, **_k: calls.append("upload"))
    item = {"building": "A楼", "file_path": str(good), "upload_date": "2026-10-31"}
    result = calc.run_with_explicit_file_items({}, [item], upload=False, emit_log=lambda _line: None)
    assert len(result) == 1
    assert result[0].month == "2026-10"
    with pytest.raises(ValueError, match="日期"):
        calc.run_with_explicit_file_items(
            {}, [item, {**item, "file_path": str(bad)}], upload=False, save_json=True,
            emit_log=lambda _line: None,
        )
    assert calls == []
    calc.run_with_explicit_file_items({}, [item], upload=True, save_json=False, emit_log=lambda _line: None)
    assert calls == ["upload"]


def test_explicit_mapping_accepts_json_for_upload(tmp_path, calc, monkeypatch):
    path = _write_json(tmp_path / "source.json", _payload())
    calls = []
    monkeypatch.setattr(calc, "upload_results_to_feishu", lambda *_a, **_k: calls.append("upload"))
    config = {"input": {"buildings": ["A楼"]}}
    result = calc.run_with_explicit_files(config, {"A楼": str(path)}, upload=False)
    assert len(result) == 1
    assert result[0].building == "A楼"
    calc.run_with_explicit_files(config, {"A楼": str(path)}, upload=True)
    assert calls == ["upload"]


def test_local_directory_run_finds_api_json_with_legacy_glob_setting(tmp_path, calc):
    _write_json(tmp_path / "A楼_20261031.json", _payload())
    _write_xlsx(tmp_path / "B楼_old.xlsx", _payload(building="B楼"))
    config = {"input": {"excel_dir": str(tmp_path), "buildings": ["A楼", "B楼"],
                        "file_glob_template": "{building}_*.xlsx"}}
    results = calc.run_with_config(config)
    assert [item.building for item in results] == ["A楼"]
    assert results[0].month == "2026-10"


def test_local_directory_run_does_not_treat_old_excel_as_api_data(tmp_path, calc):
    _write_xlsx(tmp_path / "A楼_old.xlsx", _payload())
    config = {"input": {"excel_dir": str(tmp_path), "buildings": ["A楼"]}}
    with pytest.raises(FileNotFoundError, match="没有可用的 JSON"):
        calc.run_with_config(config)
