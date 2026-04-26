from __future__ import annotations

from app.config.config_compat_cleanup import sanitize_wet_bulb_collection_config


def test_sanitize_wet_bulb_collection_removes_retired_enabled_switch() -> None:
    sanitized = sanitize_wet_bulb_collection_config(
        {
            "enabled": False,
            "manual_button_enabled": False,
            "scheduler": {"enabled": True},
            "source": {"switch_to_internal_before_download": True, "reuse_handover_download": True},
            "target": {"base_url": "https://example.invalid", "table_id": "tbl"},
        }
    )

    assert "enabled" not in sanitized
    assert "manual_button_enabled" not in sanitized
    assert sanitized["scheduler"]["enabled"] is True
    assert "switch_to_internal_before_download" not in sanitized["source"]
    assert sanitized["source"]["reuse_handover_download"] is True
    assert "base_url" not in sanitized["target"]
    assert sanitized["target"]["table_id"] == "tbl"
