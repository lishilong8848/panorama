from __future__ import annotations

import pytest

from app.modules.sheet_import.core.sheet_rules import normalize_sheet_rules


def test_normalize_sheet_rules_from_dict() -> None:
    raw = {
        "0.重点推动": {"table_id": "tbl1", "header_row": 1},
        "1.楼内闭环": {"table_id": "tbl2", "header_row": 2},
    }
    rules = normalize_sheet_rules(raw)
    assert len(rules) == 2
    assert rules[0]["sheet_name"] == "0.重点推动"
    assert rules[1]["header_row"] == 2


def test_normalize_sheet_rules_invalid_header_row() -> None:
    with pytest.raises(ValueError):
        normalize_sheet_rules([{"sheet_name": "A", "table_id": "tbl", "header_row": 0}])


def test_normalize_sheet_rules_duplicate_sheet() -> None:
    with pytest.raises(ValueError):
        normalize_sheet_rules(
            [
                {"sheet_name": "A", "table_id": "tbl1", "header_row": 1},
                {"sheet_name": "A", "table_id": "tbl2", "header_row": 1},
            ]
        )
