from __future__ import annotations

from app.modules.sheet_import.core.image_anchor_mapper import (
    auto_pick_attachment_field,
    build_explicit_image_mapping,
    resolve_attachment_target_field,
    select_tokens_by_strategy,
)


def test_build_explicit_image_mapping_basic() -> None:
    raw = [
        {
            "sheet_name": "0.重点推动",
            "mappings": [
                {"source_column": "上传图片/附件（如有）", "target_field": "上传图片|附件（如有）图片"},
            ],
        }
    ]
    mapping = build_explicit_image_mapping(raw)
    assert mapping
    sheet = next(iter(mapping.values()))
    assert "上传图片/附件（如有）" in sheet["exact"]


def test_auto_pick_attachment_field_exact_and_suffix() -> None:
    fields = ["上传图片|附件（如有）图片", "闭环照片图片"]
    target, reason = auto_pick_attachment_field("闭环照片", fields)
    assert target == "闭环照片图片"
    assert reason == ""


def test_resolve_attachment_target_field_explicit_then_auto() -> None:
    explicit_map = build_explicit_image_mapping(
        {
            "0.重点推动": {
                "column_to_field": {"问题图片": "问题图片图片"},
            }
        }
    )
    target, reason = resolve_attachment_target_field(
        sheet_name="0.重点推动",
        source_column="问题图片",
        attachment_field_names=["问题图片图片"],
        explicit_map_by_sheet=explicit_map,
        mapping_mode="explicit_then_auto",
    )
    assert target == "问题图片图片"
    assert reason == ""


def test_select_tokens_by_strategy() -> None:
    tokens = ["a", "b", "c"]
    assert select_tokens_by_strategy(tokens, "all") == ["a", "b", "c"]
    assert select_tokens_by_strategy(tokens, "first") == ["a"]
    assert select_tokens_by_strategy(tokens, "last") == ["c"]
