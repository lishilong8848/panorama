from __future__ import annotations

from app.config.settings_loader import validate_settings


def test_validate_settings_accepts_single_business_root_contract() -> None:
    normalized = validate_settings(
        {
            "version": 3,
            "common": {
                "paths": {
                    "business_root_dir": r"D:\QLDownload",
                }
            },
        }
    )

    assert normalized["common"]["paths"] == {
        "business_root_dir": r"D:\QLDownload",
        "runtime_state_root": ".runtime",
    }
    assert "output_dir" not in normalized["features"]["handover_log"]["template"]


def test_validate_settings_compatibly_migrates_legacy_paths_to_business_root() -> None:
    normalized = validate_settings(
        {
            "version": 3,
            "common": {
                "paths": {
                    "download_save_dir": r"D:\LegacyRoot",
                    "excel_dir": r"D:\LegacyExcel",
                    "runtime_state_root": r"D:\LegacyRuntime",
                }
            },
            "features": {
                "handover_log": {
                    "template": {
                        "output_dir": r"D:\LegacyRoot\交接班日志输出",
                    }
                }
            },
        }
    )

    assert normalized["common"]["paths"] == {
        "business_root_dir": r"D:\LegacyRoot",
        "runtime_state_root": r"D:\LegacyRuntime",
    }
    assert "output_dir" not in normalized["features"]["handover_log"]["template"]


def test_validate_settings_accepts_legacy_shared_bridge_root_for_internal_role() -> None:
    normalized = validate_settings(
        {
            "version": 3,
            "common": {
                "deployment": {"role_mode": "internal"},
                "paths": {"business_root_dir": r"D:\QLDownload"},
                "shared_bridge": {
                    "enabled": True,
                    "root_dir": r"D:\share",
                },
            },
            "features": {},
        }
    )

    assert normalized["common"]["shared_bridge"]["root_dir"] == r"D:\share"
    assert normalized["common"]["shared_bridge"]["internal_root_dir"] == r"D:\share"
    assert normalized["common"]["shared_bridge"]["external_root_dir"] == r"D:\share"


def test_validate_settings_accepts_legacy_shared_bridge_root_for_external_role() -> None:
    normalized = validate_settings(
        {
            "version": 3,
            "common": {
                "deployment": {"role_mode": "external"},
                "paths": {"business_root_dir": r"D:\QLDownload"},
                "shared_bridge": {
                    "enabled": True,
                    "root_dir": r"\\172.16.1.2\share",
                },
            },
            "features": {},
        }
    )

    assert normalized["common"]["shared_bridge"]["root_dir"] == r"\\172.16.1.2\share"
    assert normalized["common"]["shared_bridge"]["internal_root_dir"] == r"\\172.16.1.2\share"
    assert normalized["common"]["shared_bridge"]["external_root_dir"] == r"\\172.16.1.2\share"

