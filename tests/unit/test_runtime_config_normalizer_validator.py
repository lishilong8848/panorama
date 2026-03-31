from __future__ import annotations

from datetime import datetime

from app.config.config_adapter import ensure_v3_config
from app.modules.report_pipeline.service.download_runtime_utils import extract_site_host
from app.modules.report_pipeline.service.runtime_config_defaults import (
    default_performance_config,
    default_resume_config,
)
from app.modules.report_pipeline.service.runtime_config_normalizer import normalize_runtime_config
from app.modules.report_pipeline.service.runtime_config_validator import validate_runtime_config


def _base_config() -> dict:
    return {
        "input": {
            "excel_dir": r"D:\QLDownload",
            "buildings": ["A楼"],
        },
        "download": {
            "save_dir": r"D:\QLDownload",
            "run_subdir_mode": "timestamp",
            "run_subdir_prefix": "run_",
            "time_range_mode": "yesterday_to_today_start",
            "custom_window_mode": "absolute",
            "daily_custom_window": {"start_time": "08:00:00", "end_time": "17:00:00", "cross_day": False},
            "start_time": "2026-03-01 00:00:00",
            "end_time": "2026-03-02 00:00:00",
            "max_retries": 2,
            "retry_wait_sec": 2,
            "site_start_delay_sec": 1,
            "only_process_downloaded_this_run": True,
            "sites": [
                {
                    "building": "A楼",
                    "enabled": True,
                    "host": "http://192.168.1.10/page/main/main.html",
                    "username": "admin",
                    "password": "pwd",
                }
            ],
            "browser_headless": True,
            "browser_channel": "chrome",
            "playwright_browsers_path": "",
            "resume": default_resume_config(),
            "performance": default_performance_config(),
        },
        "network": {
            "internal_ssid": "inner",
            "external_ssid": "outer",
            "switch_timeout_sec": 30,
            "retry_count": 3,
            "retry_interval_sec": 2,
            "require_saved_profiles": True,
            "switch_back_to_original": False,
        },
        "notify": {
            "enable_webhook": False,
            "feishu_webhook_url": "",
            "keyword": "事件",
            "timeout": 10,
            "on_download_failure": True,
            "on_wifi_failure": True,
            "on_upload_failure": True,
        },
        "feishu": {
            "enable_upload": True,
        },
    }


def _parse_hms(value: str, field_name: str) -> tuple[int, int, int]:
    try:
        dt = datetime.strptime(str(value), "%H:%M:%S")
        return dt.hour, dt.minute, dt.second
    except Exception as exc:  # noqa: BLE001
        raise ValueError(field_name) from exc


def test_normalize_runtime_config_fills_network_defaults() -> None:
    cfg = _base_config()
    cfg["network"].pop("scan_attempts", None)
    cfg["network"].pop("scan_wait_sec", None)
    out = normalize_runtime_config(cfg, extract_site_host=extract_site_host)
    assert out["network"]["scan_attempts"] == 3
    assert out["network"]["scan_wait_sec"] == 2
    assert out["download"]["sites"][0]["host"] == "192.168.1.10"


def test_ensure_v3_config_migrates_legacy_alarm_db_to_common_alarm_db() -> None:
    legacy_cfg = {
        "alarm_bitable_export": {
            "db": {
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "password": "secret",
                "database": "alarm_db",
            }
        }
    }
    out = ensure_v3_config(legacy_cfg)
    assert out["common"]["alarm_db"]["port"] == 3306
    assert out["common"]["alarm_db"]["user"] == "root"
    assert out["common"]["alarm_db"]["database"] == "alarm_db"


def test_validate_runtime_config_rejects_invalid_hard_recovery_step() -> None:
    cfg = normalize_runtime_config(_base_config(), extract_site_host=extract_site_host)
    cfg["network"]["hard_recovery_steps"] = ["bad_step"]
    try:
        validate_runtime_config(cfg, extract_site_host=extract_site_host, parse_hms_text=_parse_hms)
    except ValueError as exc:
        assert "hard_recovery_steps" in str(exc)
    else:
        raise AssertionError("expected ValueError")
