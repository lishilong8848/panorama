from __future__ import annotations

import pytest

from app.config.settings_loader import validate_settings


def test_validate_settings_allows_internal_role_without_remote_updater_fields() -> None:
    normalized = validate_settings(
        {
            "version": 3,
            "common": {
                "deployment": {"role_mode": "internal"},
                "paths": {"business_root_dir": r"D:\QJPT"},
                "shared_bridge": {"enabled": True, "root_dir": r"D:\QJPT_Shared"},
                "updater": {
                    "gitee_repo": "",
                    "gitee_branch": "",
                    "gitee_subdir": "",
                    "gitee_manifest_path": "",
                },
            },
            "features": {},
        }
    )

    assert normalized["common"]["deployment"]["role_mode"] == "internal"
    assert normalized["common"]["shared_bridge"]["root_dir"] == r"D:\QJPT_Shared"



def test_validate_settings_still_requires_remote_updater_fields_for_external_role() -> None:
    with pytest.raises(ValueError, match=r"common\.updater\.gitee_repo"):
        validate_settings(
            {
                "version": 3,
                "common": {
                    "deployment": {"role_mode": "external"},
                    "paths": {"business_root_dir": r"D:\QJPT"},
                    "shared_bridge": {"enabled": True, "root_dir": r"D:\QJPT_Shared"},
                    "updater": {
                        "gitee_repo": "",
                        "gitee_branch": "",
                        "gitee_subdir": "",
                        "gitee_manifest_path": "",
                    },
                },
                "features": {},
            }
        )
