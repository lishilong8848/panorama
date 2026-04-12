from __future__ import annotations

import json

import pytest

from app.config.settings_loader import load_settings, validate_settings


def _base_v3_config(*, role_mode: str) -> dict:
    return {
        "version": 3,
        "common": {
            "deployment": {"role_mode": role_mode},
            "paths": {"business_root_dir": r"D:\QLDownload"},
            "shared_bridge": {
                "enabled": True,
                "root_dir": r"D:\share" if role_mode == "internal" else r"\\172.16.1.2\share",
            },
        },
        "features": {},
    }


def test_validate_settings_ignores_deprecated_common_alarm_db_for_external_role() -> None:
    cfg = _base_v3_config(role_mode="external")
    cfg["common"]["alarm_db"] = {
        "port": 0,
        "user": "",
        "password": "",
        "database": "",
    }

    normalized = validate_settings(cfg)

    assert normalized["common"]["deployment"]["role_mode"] == "external"
    assert "alarm_db" not in normalized["common"]


def test_validate_settings_ignores_deprecated_common_alarm_db_for_internal_role() -> None:
    cfg = _base_v3_config(role_mode="internal")
    cfg["common"]["alarm_db"] = {
        "port": 3306,
        "user": "",
        "password": "",
        "database": "",
    }

    normalized = validate_settings(cfg)

    assert normalized["common"]["deployment"]["role_mode"] == "internal"
    assert "alarm_db" not in normalized["common"]


def test_load_settings_auto_removes_deprecated_alarm_db_from_file(tmp_path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    payload = _base_v3_config(role_mode="internal")
    payload["common"]["alarm_db"] = {
        "port": 3306,
        "user": "root",
        "password": "secret",
        "database": "alarm_db",
    }
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    normalized = load_settings(config_path)
    saved = json.loads(config_path.read_text(encoding="utf-8-sig"))

    assert "alarm_db" not in normalized["common"]
    assert "alarm_db" not in saved["common"]


def test_validate_settings_rejects_partial_alarm_export_target() -> None:
    cfg = _base_v3_config(role_mode="external")
    cfg["features"]["alarm_export"] = {
        "feishu": {
            "app_token": "alarm-app-token",
            "table_id": "",
            "page_size": 500,
            "delete_batch_size": 500,
            "create_batch_size": 200,
        }
    }

    with pytest.raises(ValueError, match="app_token 与 table_id"):
        validate_settings(cfg)


def test_validate_settings_rejects_invalid_alarm_export_batch_size() -> None:
    cfg = _base_v3_config(role_mode="external")
    cfg["features"]["alarm_export"] = {
        "feishu": {
            "app_token": "alarm-app-token",
            "table_id": "alarm-table-id",
            "page_size": 0,
            "delete_batch_size": 500,
            "create_batch_size": 200,
        }
    }

    with pytest.raises(ValueError, match="features.alarm_export.feishu.page_size"):
        validate_settings(cfg)
