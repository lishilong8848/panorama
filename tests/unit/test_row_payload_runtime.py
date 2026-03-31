from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from app.modules.sheet_import.service.row_payload_runtime import (
    prepare_row_payloads_for_table,
    prepare_rows_for_table,
)


@dataclass
class _RowPayload:
    row_index: int
    fields: Dict[str, Any]


def test_prepare_row_payloads_for_table_basic() -> None:
    raw_rows = [_RowPayload(row_index=4, fields={"名称": "A", "忽略列": "x"})]
    table_fields = [{"field_name": "名称", "type": 1, "property": {}}]

    rows, stats = prepare_row_payloads_for_table(
        raw_rows=raw_rows,
        table_fields=table_fields,
        tz_offset_hours=8,
        normalize_field_name=lambda s: str(s).strip(),
        convert_value_for_field=lambda value, _meta, _tz: (value, True),
        row_payload_factory=lambda row_index, fields: _RowPayload(row_index=row_index, fields=fields),
    )

    assert len(rows) == 1
    assert rows[0].fields["名称"] == "A"
    assert stats["skipped_missing_fields"] == 1


def test_prepare_rows_for_table_wrapper() -> None:
    raw_rows: List[Dict[str, Any]] = [{"名称": "A"}]
    table_fields = [{"field_name": "名称", "type": 1, "property": {}}]

    def _prepare_row_payloads_for_table(**kwargs: Any):
        payloads = kwargs["raw_rows"]
        return payloads, {"dropped_rows": 0, "skipped_missing_fields": 0, "skipped_unsupported_values": 0, "skipped_invalid_values": 0}

    rows, stats = prepare_rows_for_table(
        raw_rows=raw_rows,
        table_fields=table_fields,
        tz_offset_hours=8,
        prepare_row_payloads_for_table=_prepare_row_payloads_for_table,
        row_payload_factory=lambda row_index, fields: _RowPayload(row_index=row_index, fields=fields),
    )

    assert rows == [{"名称": "A"}]
    assert stats["dropped_rows"] == 0
