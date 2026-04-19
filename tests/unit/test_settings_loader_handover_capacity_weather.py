from __future__ import annotations

import copy

from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.settings_loader import validate_settings


def test_validate_settings_accepts_default_handover_capacity_weather_config() -> None:
    cfg = copy.deepcopy(DEFAULT_CONFIG_V3)

    normalized = validate_settings(cfg)

    weather = normalized["features"]["handover_log"]["capacity_report"]["weather"]
    assert weather["provider"] == "seniverse"
    assert weather["location"] == "崇川区"
    assert weather["auth_mode"] == "signed"


def test_validate_settings_rejects_missing_seniverse_private_key() -> None:
    cfg = copy.deepcopy(DEFAULT_CONFIG_V3)
    cfg["features"]["handover_log"]["capacity_report"]["weather"]["seniverse_private_key"] = ""

    try:
        validate_settings(cfg)
    except ValueError as exc:
        assert "seniverse_private_key" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_validate_settings_accepts_weather_fallback_locations() -> None:
    cfg = copy.deepcopy(DEFAULT_CONFIG_V3)
    cfg["features"]["handover_log"]["capacity_report"]["weather"]["fallback_locations"] = ["南通", "nantong"]

    validate_settings(cfg)
