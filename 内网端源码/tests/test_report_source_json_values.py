import json

import pytest

from app.shared.utils.report_source_json import validate_report_source_json


@pytest.mark.parametrize(("family", "value_col"), [
    ("handover_log_family", 4), ("branch_power_family", 3),
])
def test_only_actual_time_column_values_count(tmp_path, family, value_col):
    header = [None] * value_col + ["2026-11-04 08:00:00", "最大值"]
    row = ["设备"] * value_col + [None, 123]
    payload = {"schema_version": 1, "source_family": family, "building": "A楼",
               "business_date": "2026-11-04", "query_start": "2026-11-04 08:00:00",
               "query_end": "2026-11-04 09:00:00", "sheets": [
                   {"name": "sheet1", "rows": [[], header, [], row]}]}
    path = tmp_path / "source.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ValueError, match="没有有效测点值"):
        validate_report_source_json(path)
    row[value_col] = 0
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validate_report_source_json(path)
