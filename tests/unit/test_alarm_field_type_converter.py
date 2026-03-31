from __future__ import annotations

from app.modules.alarm_export.core.field_type_converter import (
    build_field_meta_map,
    convert_alarm_row_by_field_meta,
)


def test_convert_alarm_row_by_field_meta_handles_number_percent():
    table_fields = [
        {"field_name": "触发值", "type": 2, "property": {}},
        {"field_name": "告警内容", "type": 1, "property": {}},
    ]
    meta_map = build_field_meta_map(table_fields)
    row, stats = convert_alarm_row_by_field_meta(
        {"触发值": "45%", "告警内容": "abc"},
        meta_map,
    )
    assert row["触发值"] == 45
    assert row["告警内容"] == "abc"
    assert stats["nullified_fields"] == 0


def test_convert_alarm_row_by_field_meta_nullifies_invalid_number():
    table_fields = [{"field_name": "触发值", "type": 2, "property": {}}]
    meta_map = build_field_meta_map(table_fields)
    row, stats = convert_alarm_row_by_field_meta({"触发值": "N/A"}, meta_map)
    assert "触发值" not in row
    assert stats["nullified_fields"] == 1

