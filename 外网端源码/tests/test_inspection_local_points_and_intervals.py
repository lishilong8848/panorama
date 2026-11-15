import json
from unittest.mock import Mock

import pytest

from app.config.inspection_data_config import INSPECTION_ROUND_FIELDS, INSPECTION_SOURCE_CONTRACTS
from app.modules.inspection_data.service import point_settings
from app.modules.inspection_data.service.inspection_data_upload_service import InspectionDataUploadService, field_definitions
from app.modules.inspection_data.service.point_config_bitable import (
    POINT_CONFIG_TABLES, _expected_rows, read_point_config_bitable,
)
from app.modules.inspection_data.service.point_correction import corrected_samples
from app.modules.inspection_data.service.point_status import (
    FIXED_VALUE_FIELD, INTERVAL_FIELD, SOURCE_FIELD, normalize_status_rules, status_fields,
)


FAMILY = next(iter(INSPECTION_SOURCE_CONTRACTS))
POINT = "10_20"
FIXED_STATUS, FIXED_VALUE, MANUAL = ["local_" + digit * 32 for digit in "abc"]
EXTENSIONS = (SOURCE_FIELD, FIXED_VALUE_FIELD, INTERVAL_FIELD)


def make_group():
    rule = normalize_status_rules({
        FIXED_STATUS: {"type": "状态", "source_kind": "fixed_status", "fixed_value": "△"},
        FIXED_VALUE: {"type": "直接填充", "source_kind": "fixed_value", "fixed_value": "0.0"},
        MANUAL: {"type": "状态", "source_kind": "manual_status"},
        POINT: {"type": "状态", "normal": ["online"], "intervals": [
            {"min": "1.00", "max": "10.0", "status": "normal"},
        ]},
    }, {FIXED_STATUS, FIXED_VALUE, MANUAL, POINT})
    return {
        "baseline": {POINT: {"name": "采集", "path": ""}}, "remove": [],
        "points": {FIXED_STATUS: "固定状态项", FIXED_VALUE: "固定数值项", MANUAL: "手工项"},
        "status_rules": rule,
        "targets": {
            point: {"workbook": "D楼电气", "sheet": "设备", "cell": "",
                    "round_cells": {key: f"{col}{row}" for key, col in zip(INSPECTION_ROUND_FIELDS, "BCDE")}}
            for row, point in enumerate((FIXED_STATUS, FIXED_VALUE, MANUAL), 6)
        },
    }


def test_fixed_and_manual_rows_use_local_keys_not_collection_ids():
    group = make_group()
    group["remove"] = ["local_" + "d" * 32]
    query = point_settings.query_rules({"D楼": {FAMILY: group}})
    assert query == {"D楼": {FAMILY: {"baseline": [POINT], "add": [], "remove": []}}}
    rows = {row["测点ID"]: row for row in InspectionDataUploadService._local_point_rows("D楼", "电气", group)}
    assert rows[FIXED_STATUS]["状态判定"] == "△"
    assert rows[FIXED_VALUE]["测点值"] == "0"
    assert rows[FIXED_VALUE]["测点原始值"] == ""
    assert rows[MANUAL]["状态判定"] == "待核对"
    assert rows[MANUAL]["数据完整性"] == "缺少源数据"
    assert all(row["记录类型"] == "测点明细" and "采样时间" not in row for row in rows.values())
    assert {field: rows[MANUAL][field] for field in INSPECTION_ROUND_FIELDS.values()} == dict(zip(
        INSPECTION_ROUND_FIELDS.values(), ["B8", "C8", "D8", "E8"],
    ))


def test_interval_applies_to_corrected_value_and_preserves_exact_matches():
    rule = make_group()["status_rules"][POINT]
    for reading, expected in (("online", "√"), (1, "√"), (10, "√"), (10.01, "待核对"),
                              ("", "待核对"), ("nan", "待核对"), (None, "待核对")):
        assert status_fields(rule, reading)["状态判定"] == expected
    entry = {"value_time": "2026-11-15 10:00:00", "point_ids": [POINT], "building": "D楼",
             "report_name": "测试", "rows": [["位置", "设备", "测点", "12"]]}
    corrections = corrected_samples(entry, {POINT: [{"min": "12", "max": "12", "value": "10"}]})
    row, = InspectionDataUploadService._point_detail_rows(entry, "电气", {}, status_rules={POINT: rule}, corrections=corrections)
    assert (row["测点原始值"], row["测点修正值"], row["状态判定"]) == ("12", "10", "√")
    assert json.loads(row[INTERVAL_FIELD]) == [{"min": "1", "max": "10", "status": "normal"}]


@pytest.mark.parametrize("additional,exact", [
    ([{"min": "10", "max": "11", "status": "abnormal"}], []),
    ([], ["8.0"]),
])
def test_conflicting_status_ranges_rejected(additional, exact):
    rule = {"type": "状态", "abnormal": exact, "intervals": [
        {"min": "1", "max": "10", "status": "normal"}, *additional,
    ]}
    with pytest.raises(ValueError, match="重叠|冲突"):
        normalize_status_rules({POINT: rule}, {POINT})


def test_config_round_trip_preserves_rules_and_no_confusion_with_sample_values():
    group = make_group()
    fixtures = {}
    for building, table in POINT_CONFIG_TABLES.items():
        groups = {family: group for family in INSPECTION_SOURCE_CONTRACTS}
        rows = _expected_rows(groups, optional_fields=EXTENSIONS)
        fixtures[table] = [{"fields": fields, "record_id": f"{building}_{i}"}
                           for i, fields in enumerate(rows.values())]
    client = Mock()
    client.list_records.side_effect = lambda table, **_: fixtures[table]
    restored = read_point_config_bitable({}, client=client)["D楼"][FAMILY]
    assert restored["targets"] == group["targets"]
    assert restored["status_rules"] == group["status_rules"]
    schema = {d["field_name"]: d["type"] for d in field_definitions()}
    assert all(schema.get(field) == 1 for field in EXTENSIONS)
