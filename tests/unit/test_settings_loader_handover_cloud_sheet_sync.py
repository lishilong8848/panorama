from __future__ import annotations

import copy

from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.settings_loader import validate_settings


def test_validate_settings_backfills_blank_handover_cloud_sheet_names() -> None:
    cfg = copy.deepcopy(DEFAULT_CONFIG_V3)
    sheet_names = cfg["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]
    sheet_names["A楼"] = ""
    sheet_names["B楼"] = "   "
    sheet_names.pop("C楼", None)

    normalized = validate_settings(cfg)

    normalized_names = normalized["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]
    assert normalized_names["A楼"] == "A楼"
    assert normalized_names["B楼"] == "B楼"
    assert normalized_names["C楼"] == "C楼"
    assert normalized_names["D楼"] == "D楼"
    assert normalized_names["E楼"] == "E楼"


def test_validate_settings_replaces_invalid_handover_cloud_sheet_names_block() -> None:
    cfg = copy.deepcopy(DEFAULT_CONFIG_V3)
    cfg["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"] = []

    normalized = validate_settings(cfg)

    assert normalized["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"] == {
        "A楼": "A楼",
        "B楼": "B楼",
        "C楼": "C楼",
        "D楼": "D楼",
        "E楼": "E楼",
    }

