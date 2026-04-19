from __future__ import annotations

import pytest

from app.config.config_adapter import ensure_v3_config
from app.config.settings_loader import _validate_handover_review_ui


def test_review_ui_fixed_cells_accept_pair_entries() -> None:
    cfg = ensure_v3_config({})
    cfg["features"]["handover_log"]["review_ui"]["fixed_cells"]["metrics_summary"] = [
        {"label_cell": "A6", "value_cell": "B6"},
        "B15",
    ]

    _validate_handover_review_ui(cfg)


def test_review_ui_fixed_cells_reject_missing_value_cell() -> None:
    cfg = ensure_v3_config({})
    cfg["features"]["handover_log"]["review_ui"]["fixed_cells"]["metrics_summary"] = [
        {"label_cell": "A6"},
    ]

    with pytest.raises(ValueError, match="value_cell"):
        _validate_handover_review_ui(cfg)


def test_review_ui_fixed_cells_reject_invalid_pair_cell_name() -> None:
    cfg = ensure_v3_config({})
    cfg["features"]["handover_log"]["review_ui"]["fixed_cells"]["metrics_summary"] = [
        {"label_cell": "A6", "value_cell": "B0"},
    ]

    with pytest.raises(ValueError, match="value_cell"):
        _validate_handover_review_ui(cfg)


def test_review_ui_allows_legacy_autosave_debounce_ms_zero() -> None:
    cfg = ensure_v3_config({})
    cfg["features"]["handover_log"]["review_ui"]["autosave_debounce_ms"] = 0

    _validate_handover_review_ui(cfg)
