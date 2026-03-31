from __future__ import annotations

import copy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config.config_merge_guard import (  # noqa: E402
    ConfigValueLossError,
    build_repaired_user_config,
    merge_user_config_payload,
)
from app.config.config_schema_v3 import DEFAULT_CONFIG_V3  # noqa: E402


def _build_user_config() -> dict:
    cfg = copy.deepcopy(DEFAULT_CONFIG_V3)
    cfg["common"]["paths"]["business_root_dir"] = r"D:\UserData\QJPT"
    cfg["common"]["feishu_auth"]["app_id"] = "cli_app_id"
    cfg["common"]["feishu_auth"]["app_secret"] = "cli_secret"
    cfg["features"]["monthly_report"]["buildings"] = ["A楼", "B楼"]
    cfg["features"]["monthly_report"]["sites"] = [
        {
            "building": "A楼",
            "enabled": True,
            "host": "10.0.0.10",
            "username": "user-a",
            "password": "pwd-a",
            "url": "",
        }
    ]
    cfg["features"]["sheet_import"]["sheet_rules"] = [
        {"sheet_name": "Sheet1", "table_id": "tbl_1", "header_row": 1}
    ]
    return cfg


def test_merge_user_config_payload_preserves_key_user_values_when_new_payload_is_defaultish() -> None:
    old_cfg = _build_user_config()
    new_cfg = copy.deepcopy(DEFAULT_CONFIG_V3)

    result = merge_user_config_payload(new_cfg, old_cfg, force_overwrite=True)

    assert result.merged["common"]["paths"]["business_root_dir"] == r"D:\UserData\QJPT"
    assert result.merged["features"]["monthly_report"]["buildings"] == ["A楼", "B楼"]
    assert result.merged["features"]["monthly_report"]["sites"][0]["host"] == "10.0.0.10"
    assert result.merged["features"]["sheet_import"]["sheet_rules"][0]["table_id"] == "tbl_1"
    assert "features.monthly_report.buildings" in result.suspicious_loss_paths


def test_merge_user_config_payload_rejects_massive_value_loss_by_default() -> None:
    old_cfg = _build_user_config()
    new_cfg = copy.deepcopy(DEFAULT_CONFIG_V3)

    try:
        merge_user_config_payload(new_cfg, old_cfg)
    except ConfigValueLossError as exc:
        assert "检测到可能会丢失用户配置" in str(exc)
        assert len(exc.suspicious_paths) >= 5
    else:
        raise AssertionError("expected ConfigValueLossError")


def test_build_repaired_user_config_uses_old_config_as_value_source() -> None:
    source_old_cfg = _build_user_config()
    target_cfg = copy.deepcopy(DEFAULT_CONFIG_V3)
    target_cfg["common"]["paths"]["business_root_dir"] = r"D:\QLDownload"
    target_cfg["features"]["monthly_report"]["buildings"] = []
    target_cfg["features"]["monthly_report"]["sites"] = []
    target_cfg["features"]["sheet_import"] = {}

    result = build_repaired_user_config(source_old_cfg, target_cfg)

    assert result.merged["common"]["paths"]["business_root_dir"] == r"D:\UserData\QJPT"
    assert result.merged["features"]["monthly_report"]["sites"][0]["username"] == "user-a"
    assert result.merged["features"]["sheet_import"]["sheet_rules"][0]["sheet_name"] == "Sheet1"
