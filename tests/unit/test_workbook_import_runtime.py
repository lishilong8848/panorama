from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import openpyxl
import pytest

from app.modules.sheet_import.service.workbook_import_runtime import (
    import_workbook_sheets_to_feishu,
)


@dataclass
class _RowPayload:
    row_index: int
    fields: Dict[str, Any] = field(default_factory=dict)


class _Client:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.created: List[Dict[str, Any]] = []

    def clear_table(self, **_kwargs: Any) -> int:
        return 0

    def list_fields(self, **_kwargs: Any) -> List[Dict[str, Any]]:
        return [{"field_name": "名称", "type": 1}]

    def batch_create_records(self, **kwargs: Any) -> None:
        self.created.extend(kwargs.get("fields_list", []))


def _base_config(enabled: bool = True) -> Dict[str, Any]:
    return {
        "feishu": {
            "app_id": "app_id",
            "app_secret": "app_secret",
            "date_tz_offset_hours": 8,
            "request_retry_count": 1,
            "request_retry_interval_sec": 1,
        },
        "feishu_sheet_import": {
            "enabled": enabled,
            "app_token": "app_token",
            "clear_before_upload": True,
            "continue_on_sheet_error": True,
            "timeout": 30,
            "list_page_size": 200,
            "delete_batch_size": 200,
            "create_batch_size": 100,
            "sheet_rules": [{"sheet_name": "S1", "table_id": "tbl1", "header_row": 1}],
        },
    }


def test_import_workbook_disabled() -> None:
    with pytest.raises(ValueError):
        import_workbook_sheets_to_feishu(
            config=_base_config(enabled=False),
            xlsx_path="x.xlsx",
            client_factory=lambda **kwargs: _Client(**kwargs),
            normalize_sheet_rules=lambda rules: rules,
            parse_image_import_config=lambda _cfg: {"enabled": False},
            build_explicit_image_mapping=lambda _rules: {},
            extract_rows_with_row_index=lambda **_kwargs: [],
            prepare_row_payloads_for_table=lambda **_kwargs: ([], {"dropped_rows": 0, "skipped_missing_fields": 0, "skipped_unsupported_values": 0, "skipped_invalid_values": 0}),
            apply_sheet_images_to_row_payloads=lambda **_kwargs: {"detected_images": 0, "uploaded_images": 0, "rows_with_images": 0, "missing_mapping_count": 0, "orphan_row_images": 0},
            emit_log=lambda _msg: None,
        )


def test_import_workbook_success(tmp_path: Path) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "S1"
    xlsx = tmp_path / "s1.xlsx"
    wb.save(xlsx)
    wb.close()

    clients: List[_Client] = []

    def _factory(**kwargs: Any) -> _Client:
        c = _Client(**kwargs)
        clients.append(c)
        return c

    result = import_workbook_sheets_to_feishu(
        config=_base_config(enabled=True),
        xlsx_path=str(xlsx),
        client_factory=_factory,
        normalize_sheet_rules=lambda rules: rules,
        parse_image_import_config=lambda _cfg: {"enabled": False},
        build_explicit_image_mapping=lambda _rules: {},
        extract_rows_with_row_index=lambda **_kwargs: [_RowPayload(row_index=2, fields={"名称": "ok"})],
        prepare_row_payloads_for_table=lambda **_kwargs: (
            [_RowPayload(row_index=2, fields={"名称": "ok"})],
            {"dropped_rows": 0, "skipped_missing_fields": 0, "skipped_unsupported_values": 0, "skipped_invalid_values": 0},
        ),
        apply_sheet_images_to_row_payloads=lambda **_kwargs: {
            "detected_images": 0,
            "uploaded_images": 0,
            "rows_with_images": 0,
            "missing_mapping_count": 0,
            "orphan_row_images": 0,
        },
        emit_log=lambda _msg: None,
    )

    assert result["success_count"] == 1
    assert result["failed_count"] == 0
    assert clients and clients[0].created == [{"名称": "ok"}]
