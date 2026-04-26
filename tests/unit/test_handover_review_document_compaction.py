from __future__ import annotations

from handover_log_module.service.review_document_state_service import ReviewDocumentStateService
from handover_log_module.service.review_document_writer import ReviewDocumentWriter


def _document_with_blank_and_content_rows():
    return {
        "title": "交接班日志",
        "sections": [
            {
                "name": "本班完成工作",
                "columns": [
                    {"key": "B", "label": "内容", "source_cols": ["B"]},
                    {"key": "D", "label": "状态", "source_cols": ["D"]},
                ],
                "rows": [
                    {"row_id": "blank-1", "cells": {"B": "", "D": ""}, "is_placeholder_row": True},
                    {"row_id": "content-1", "cells": {"B": "EA118机房E楼灭火器维护", "D": "已完成"}},
                    {"row_id": "blank-2", "cells": {"B": " ", "D": ""}, "is_placeholder_row": True},
                ],
            }
        ],
    }


def test_compact_section_blank_rows_removes_empty_rows_when_section_has_content() -> None:
    compacted = ReviewDocumentStateService.compact_section_blank_rows(_document_with_blank_and_content_rows())

    rows = compacted["sections"][0]["rows"]
    assert len(rows) == 1
    assert rows[0]["row_id"] == "content-1"
    assert rows[0]["is_placeholder_row"] is False


def test_compact_section_blank_rows_keeps_single_placeholder_when_all_rows_empty() -> None:
    document = _document_with_blank_and_content_rows()
    document["sections"][0]["rows"] = [
        {"row_id": "blank-1", "cells": {"B": "", "D": ""}},
        {"row_id": "blank-2", "cells": {"B": " ", "D": ""}},
    ]

    compacted = ReviewDocumentStateService.compact_section_blank_rows(document)

    rows = compacted["sections"][0]["rows"]
    assert len(rows) == 1
    assert rows[0]["is_placeholder_row"] is True
    assert rows[0]["cells"] == {"B": "", "D": ""}


def test_writer_drops_blank_section_rows_before_excel_payload() -> None:
    writer = ReviewDocumentWriter({})

    payloads = writer._category_payloads_from_document(_document_with_blank_and_content_rows())

    assert payloads["本班完成工作"] == [
        {"cells": {"B": "EA118机房E楼灭火器维护", "C": "", "D": "已完成", "E": "", "F": "", "G": "", "H": "", "I": ""}}
    ]


def test_writer_uses_empty_payload_for_all_blank_section_rows() -> None:
    writer = ReviewDocumentWriter({})
    document = _document_with_blank_and_content_rows()
    document["sections"][0]["rows"] = [
        {"row_id": "blank-1", "cells": {"B": "", "D": ""}},
        {"row_id": "blank-2", "cells": {"B": " ", "D": ""}},
    ]

    payloads = writer._category_payloads_from_document(document)

    assert payloads["本班完成工作"] == []
