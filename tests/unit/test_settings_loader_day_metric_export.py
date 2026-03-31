from __future__ import annotations

import pytest

from app.config.config_adapter import ensure_v3_config
from app.config.settings_loader import _validate_handover_day_metric_export


def test_day_metric_export_position_code_default_is_accepted() -> None:
    cfg = ensure_v3_config({})

    _validate_handover_day_metric_export(cfg)


def test_day_metric_export_position_code_cannot_be_blank() -> None:
    cfg = ensure_v3_config({})
    cfg["features"]["handover_log"]["day_metric_export"]["fields"]["position_code"] = ""

    with pytest.raises(ValueError, match="position_code"):
        _validate_handover_day_metric_export(cfg)
