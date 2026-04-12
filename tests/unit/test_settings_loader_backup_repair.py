from __future__ import annotations

import json

from app.config.config_adapter import ensure_v3_config
from app.config.settings_loader import _repair_critical_settings_from_backups
from app.config.settings_loader import repair_day_metric_related_settings


def _write_json(path, payload) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def test_repair_critical_settings_from_backups_restores_missing_feishu_and_day_metric(tmp_path) -> None:
    config_path = tmp_path / "settings.json"
    current = ensure_v3_config({})
    current["common"]["feishu_auth"]["app_id"] = ""
    current["common"]["feishu_auth"]["app_secret"] = ""
    _write_json(config_path, current)

    backup = ensure_v3_config({})
    backup["common"]["feishu_auth"]["app_id"] = "cli_backup"
    backup["common"]["feishu_auth"]["app_secret"] = "secret_backup"
    backup["features"]["day_metric_upload"]["scheduler"]["enabled"] = True
    backup["features"]["day_metric_upload"]["target"]["source"]["app_token"] = "custom_app_token"
    backup["features"]["day_metric_upload"]["target"]["source"]["table_id"] = "custom_table"
    _write_json(tmp_path / "settings.backup.20260411-030000.json", backup)

    repaired, notes = _repair_critical_settings_from_backups(current, config_path)

    assert repaired["common"]["feishu_auth"]["app_id"] == "cli_backup"
    assert repaired["common"]["feishu_auth"]["app_secret"] == "secret_backup"
    assert repaired["features"]["day_metric_upload"]["scheduler"]["enabled"] is True
    assert repaired["features"]["day_metric_upload"]["target"]["source"]["app_token"] == "custom_app_token"
    assert repaired["features"]["day_metric_upload"]["target"]["source"]["table_id"] == "custom_table"
    assert any("飞书应用凭据" in item for item in notes)
    assert any("12项独立上传配置" in item for item in notes)


def test_repair_critical_settings_from_backups_keeps_existing_valid_values(tmp_path) -> None:
    config_path = tmp_path / "settings.json"
    current = ensure_v3_config({})
    current["common"]["feishu_auth"]["app_id"] = "cli_current"
    current["common"]["feishu_auth"]["app_secret"] = "secret_current"
    current["features"]["day_metric_upload"]["scheduler"]["enabled"] = True
    current["features"]["day_metric_upload"]["target"]["source"]["app_token"] = "current_app_token"
    current["features"]["day_metric_upload"]["target"]["source"]["table_id"] = "current_table"
    _write_json(config_path, current)

    backup = ensure_v3_config({})
    backup["common"]["feishu_auth"]["app_id"] = "cli_backup"
    backup["common"]["feishu_auth"]["app_secret"] = "secret_backup"
    backup["features"]["day_metric_upload"]["scheduler"]["enabled"] = False
    backup["features"]["day_metric_upload"]["target"]["source"]["app_token"] = "backup_app_token"
    backup["features"]["day_metric_upload"]["target"]["source"]["table_id"] = "backup_table"
    _write_json(tmp_path / "settings.backup.20260411-040000.json", backup)

    repaired, notes = _repair_critical_settings_from_backups(current, config_path)

    assert repaired["common"]["feishu_auth"]["app_id"] == "cli_current"
    assert repaired["common"]["feishu_auth"]["app_secret"] == "secret_current"
    assert repaired["features"]["day_metric_upload"]["scheduler"]["enabled"] is True
    assert repaired["features"]["day_metric_upload"]["target"]["source"]["app_token"] == "current_app_token"
    assert repaired["features"]["day_metric_upload"]["target"]["source"]["table_id"] == "current_table"
    assert notes == []


def test_repair_critical_settings_from_backups_restores_notify_keyword_for_same_webhook(tmp_path) -> None:
    config_path = tmp_path / "settings.json"
    current = ensure_v3_config({})
    current["common"]["notify"]["feishu_webhook_url"] = "https://example.test/hook"
    current["common"]["notify"]["keyword"] = "事件"
    _write_json(config_path, current)

    backup = ensure_v3_config({})
    backup["common"]["notify"]["feishu_webhook_url"] = "https://example.test/hook"
    backup["common"]["notify"]["keyword"] = "全景平台"
    _write_json(tmp_path / "settings.backup.20260411-050000.json", backup)

    repaired, notes = _repair_critical_settings_from_backups(current, config_path)

    assert repaired["common"]["notify"]["keyword"] == "全景平台"
    assert any("Webhook告警配置" in item for item in notes)


def test_repair_critical_settings_uses_legacy_feishu_fields_without_backup(tmp_path) -> None:
    config_path = tmp_path / "settings.json"
    current = ensure_v3_config({})
    current["common"]["feishu_auth"]["app_id"] = ""
    current["common"]["feishu_auth"]["app_secret"] = ""
    current["feishu"] = {
        "app_id": "cli_legacy",
        "app_secret": "secret_legacy",
        "request_retry_count": 5,
        "request_retry_interval_sec": 1.5,
        "timeout": 45,
    }
    _write_json(config_path, current)

    repaired, notes = _repair_critical_settings_from_backups(current, config_path)

    assert repaired["common"]["feishu_auth"]["app_id"] == "cli_legacy"
    assert repaired["common"]["feishu_auth"]["app_secret"] == "secret_legacy"
    assert repaired["common"]["feishu_auth"]["request_retry_count"] == 5
    assert repaired["common"]["feishu_auth"]["request_retry_interval_sec"] == 1.5
    assert repaired["common"]["feishu_auth"]["timeout"] == 45
    assert notes == [] or any("飞书应用凭据 <- 当前配置兼容字段(feishu)" == item for item in notes)


def test_repair_critical_settings_falls_back_to_template_when_backup_missing(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "settings.json"
    current = ensure_v3_config({})
    current["common"]["feishu_auth"]["app_id"] = ""
    current["common"]["feishu_auth"]["app_secret"] = ""
    _write_json(config_path, current)

    template = ensure_v3_config({})
    template["common"]["feishu_auth"]["app_id"] = "cli_template"
    template["common"]["feishu_auth"]["app_secret"] = "secret_template"

    monkeypatch.setattr(
        "app.config.settings_loader._load_template_settings_v3",
        lambda: (template, "表格计算配置.template.json"),
    )

    repaired, notes = _repair_critical_settings_from_backups(current, config_path)

    assert repaired["common"]["feishu_auth"]["app_id"] == "cli_template"
    assert repaired["common"]["feishu_auth"]["app_secret"] == "secret_template"
    assert any("飞书应用凭据 <- 表格计算配置.template.json" == item for item in notes)


def test_repair_day_metric_related_settings_restores_day_metric_profile_from_backup(tmp_path) -> None:
    config_path = tmp_path / "settings.json"
    current = ensure_v3_config({})
    current["common"]["feishu_auth"]["app_id"] = "cli_current"
    current["common"]["feishu_auth"]["app_secret"] = "secret_current"
    current["features"]["day_metric_upload"]["target"]["source"]["app_token"] = "current_app_token"
    current["features"]["day_metric_upload"]["target"]["source"]["table_id"] = "current_table"
    current["features"]["day_metric_upload"]["target"]["types"][0]["name"] = "当前值"
    _write_json(config_path, current)

    backup = ensure_v3_config({})
    backup["common"]["feishu_auth"]["app_id"] = "cli_current"
    backup["common"]["feishu_auth"]["app_secret"] = "secret_current"
    backup["features"]["day_metric_upload"]["target"]["source"]["app_token"] = "backup_app_token"
    backup["features"]["day_metric_upload"]["target"]["source"]["table_id"] = "backup_table"
    backup["features"]["day_metric_upload"]["target"]["types"][0]["name"] = "备份值"
    _write_json(tmp_path / "settings.backup.20260411-060000.json", backup)

    from app.config import settings_loader

    original_candidates = settings_loader._day_metric_repair_baseline_candidates
    settings_loader._day_metric_repair_baseline_candidates = lambda _config_path=None: []  # type: ignore[assignment]
    try:
        repaired, notes, changed = repair_day_metric_related_settings(current, config_path)
    finally:
        settings_loader._day_metric_repair_baseline_candidates = original_candidates  # type: ignore[assignment]

    assert changed is True
    assert repaired["features"]["day_metric_upload"]["target"]["source"]["app_token"] == "backup_app_token"
    assert repaired["features"]["day_metric_upload"]["target"]["source"]["table_id"] == "backup_table"
    assert repaired["features"]["day_metric_upload"]["target"]["types"][0]["name"] == "备份值"
    assert any("12项配置轮廓 <- settings.backup.20260411-060000.json" == item for item in notes)


def test_repair_day_metric_related_settings_prefers_fixed_baseline(tmp_path) -> None:
    config_path = tmp_path / "settings.json"
    current = ensure_v3_config({})
    current["common"]["feishu_auth"]["app_id"] = "cli_current"
    current["common"]["feishu_auth"]["app_secret"] = "secret_current"
    current["features"]["day_metric_upload"]["target"]["source"]["app_token"] = "current_app_token"
    current["features"]["day_metric_upload"]["target"]["source"]["table_id"] = "current_table"
    current["features"]["day_metric_upload"]["target"]["types"][0]["name"] = "当前值"
    _write_json(config_path, current)

    # 普通备份（应被固定修复基线覆盖）
    backup = ensure_v3_config({})
    backup["features"]["day_metric_upload"]["target"]["source"]["app_token"] = "backup_app_token"
    backup["features"]["day_metric_upload"]["target"]["source"]["table_id"] = "backup_table"
    backup["features"]["day_metric_upload"]["target"]["types"][0]["name"] = "普通备份值"
    _write_json(tmp_path / "settings.backup.20260411-060000.json", backup)

    # 固定修复基线
    baseline_name = "表格计算配置.backup.20260409-145808.json"
    baseline = ensure_v3_config({})
    baseline["features"]["day_metric_upload"]["target"]["source"]["app_token"] = "baseline_app_token"
    baseline["features"]["day_metric_upload"]["target"]["source"]["table_id"] = "baseline_table"
    baseline["features"]["day_metric_upload"]["target"]["types"][0]["name"] = "固定基线值"
    baseline_path = tmp_path / baseline_name
    _write_json(baseline_path, baseline)

    repaired, notes, changed = repair_day_metric_related_settings(current, config_path)

    assert changed is True
    assert repaired["features"]["day_metric_upload"]["target"]["source"]["app_token"] == "baseline_app_token"
    assert repaired["features"]["day_metric_upload"]["target"]["source"]["table_id"] == "baseline_table"
    assert repaired["features"]["day_metric_upload"]["target"]["types"][0]["name"] == "固定基线值"
    assert any(f"12项配置轮廓 <- {baseline_name}" == item for item in notes)
