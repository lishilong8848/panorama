from app.config.secret_masking import mask_settings, merge_masked_values


def test_mask_settings_masks_seniverse_private_key() -> None:
    cfg = {
        "features": {
            "handover_log": {
                "capacity_report": {
                    "weather": {
                        "seniverse_public_key": "public-key",
                        "seniverse_private_key": "SwWfUI324UukiuVs2",
                    }
                }
            }
        }
    }

    masked = mask_settings(cfg)

    assert masked["features"]["handover_log"]["capacity_report"]["weather"]["seniverse_public_key"] == "public-key"
    assert masked["features"]["handover_log"]["capacity_report"]["weather"]["seniverse_private_key"] != "SwWfUI324UukiuVs2"
    assert "*" in masked["features"]["handover_log"]["capacity_report"]["weather"]["seniverse_private_key"]


def test_merge_masked_values_preserves_existing_seniverse_private_key() -> None:
    old_cfg = {
        "features": {
            "handover_log": {
                "capacity_report": {
                    "weather": {
                        "seniverse_private_key": "SwWfUI324UukiuVs2",
                    }
                }
            }
        }
    }
    masked_value = mask_settings(old_cfg)["features"]["handover_log"]["capacity_report"]["weather"]["seniverse_private_key"]
    new_cfg = {
        "features": {
            "handover_log": {
                "capacity_report": {
                    "weather": {
                        "seniverse_private_key": masked_value,
                    }
                }
            }
        }
    }

    merged = merge_masked_values(new_cfg, old_cfg)

    assert merged["features"]["handover_log"]["capacity_report"]["weather"]["seniverse_private_key"] == "SwWfUI324UukiuVs2"
