from __future__ import annotations

import json

import pytest

from handover_log_module.repository.excel_reader import load_rows


def _source(tmp_path, *, building="A楼", family="handover_log_family"):
    path = tmp_path / "source.json"
    path.write_text(json.dumps({
        "schema_version": 1,
        "source_family": family,
        "building": building,
        "sheets": [{"name": "sheet1", "rows": [
            [], [None, None, None, None, "2026-11-04 09:00:00"], [],
            [None, "A-202", "A-202-TH-01", "温度", "24.65"],
            [None, None, None, "湿度", "52.73"],
        ]}],
    }, ensure_ascii=False), encoding="utf-8")
    return path


def test_handover_json_rows_preserve_values_and_forward_fill(tmp_path):
    rows = load_rows(str(_source(tmp_path)), {}, {}, expected_building="A楼", expected_source_family="handover_log_family")
    assert [(row.b_text, row.c_text, row.d_name, row.value) for row in rows] == [
        ("A-202", "A-202-TH-01", "温度", 24.65),
        ("A-202", "A-202-TH-01", "湿度", 52.73),
    ]


def test_handover_json_rejects_wrong_building_or_source(tmp_path):
    path = _source(tmp_path)
    with pytest.raises(ValueError, match="楼栋不匹配"):
        load_rows(str(path), {}, {}, expected_building="B楼")
    with pytest.raises(ValueError, match="来源不匹配"):
        load_rows(str(path), {}, {}, expected_source_family="handover_capacity_report_family")


def test_handover_json_rejects_duplicate_keys_and_nonfinite_values(tmp_path):
    path = tmp_path / "source.json"
    path.write_text('{"schema_version":1,"schema_version":1}', encoding="utf-8")
    with pytest.raises(ValueError, match="重复字段"):
        load_rows(str(path), {}, {})
    path.write_text('{"schema_version":1,"source_family":"handover_log_family","building":"A楼","sheets":[{"name":"sheet1","rows":[[NaN]]}]}', encoding="utf-8")
    with pytest.raises(ValueError, match="无效数字"):
        load_rows(str(path), {}, {})
