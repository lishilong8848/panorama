from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pytest

from app.modules.sheet_import.service.image_upload_runtime import (
    apply_sheet_images_to_row_payloads,
    parse_image_import_config,
)


@dataclass
class _Placement:
    row_index: int
    column_index: int
    file_name: str
    mime_type: str
    content: bytes


@dataclass
class _RowPayload:
    row_index: int
    fields: Dict[str, Any] = field(default_factory=dict)


class _Client:
    timeout = 30

    def upload_attachment_bytes(self, **_kwargs: Any) -> str:
        return "file_token_1"


def test_parse_image_import_config_defaults() -> None:
    cfg = parse_image_import_config({"timeout": 20})
    assert cfg["enabled"] is False
    assert cfg["upload_timeout"] == 20


def test_parse_image_import_config_invalid_mode() -> None:
    with pytest.raises(ValueError):
        parse_image_import_config(
            {
                "timeout": 20,
                "image_import": {
                    "mapping_mode": "bad_mode",
                },
            }
        )


def test_apply_sheet_images_to_row_payloads_disabled() -> None:
    stats = apply_sheet_images_to_row_payloads(
        ws=None,
        sheet_name="S1",
        header_row=1,
        row_payloads=[],
        table_fields=[],
        image_cfg={"enabled": False},
        explicit_map_by_sheet={},
        client=_Client(),
        build_raw_header_name_by_column=lambda **_kwargs: {},
        extract_sheet_images_by_anchor=lambda **_kwargs: [],
        resolve_attachment_target_field=lambda **_kwargs: (None, "x"),
        select_tokens_by_strategy=lambda tokens, _strategy: tokens,
    )
    assert stats["detected_images"] == 0
    assert stats["uploaded_images"] == 0


def test_apply_sheet_images_to_row_payloads_success() -> None:
    row_payloads = [_RowPayload(row_index=4)]
    table_fields = [{"field_name": "问题图片图片", "type": 17}]
    placements = [
        _Placement(
            row_index=4,
            column_index=8,
            file_name="a.png",
            mime_type="image/png",
            content=b"png",
        )
    ]

    def _resolve_attachment_target_field(**_kwargs: Any) -> tuple[Optional[str], str]:
        return "问题图片图片", ""

    stats = apply_sheet_images_to_row_payloads(
        ws=None,
        sheet_name="S1",
        header_row=1,
        row_payloads=row_payloads,
        table_fields=table_fields,
        image_cfg={
            "enabled": True,
            "mapping_mode": "explicit_then_auto",
            "missing_attachment_field_strategy": "fail_sheet",
            "upload_timeout": 30,
            "multi_image_strategy": "all",
        },
        explicit_map_by_sheet={},
        client=_Client(),
        build_raw_header_name_by_column=lambda **_kwargs: {8: "问题图片"},
        extract_sheet_images_by_anchor=lambda **_kwargs: placements,
        resolve_attachment_target_field=_resolve_attachment_target_field,
        select_tokens_by_strategy=lambda tokens, _strategy: tokens,
    )

    assert stats["detected_images"] == 1
    assert stats["uploaded_images"] == 1
    assert row_payloads[0].fields["问题图片图片"] == [{"file_token": "file_token_1"}]
