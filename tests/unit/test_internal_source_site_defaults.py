from __future__ import annotations

from app.config.config_adapter import adapt_runtime_config, ensure_v3_config
from app.modules.report_pipeline.service.download_runtime_utils import extract_site_host
from app.modules.report_pipeline.service.runtime_config_validator import validate_runtime_config


def _parse_hms(value: str, field_name: str) -> tuple[int, int, int]:
    parts = str(value).split(":")
    if len(parts) != 3:
        raise ValueError(field_name)
    return int(parts[0]), int(parts[1]), int(parts[2])


def test_default_internal_source_sites_start_disabled() -> None:
    cfg = ensure_v3_config({})
    sites = cfg["common"]["internal_source_sites"]
    assert [site["building"] for site in sites] == ["A楼", "B楼", "C楼", "D楼", "E楼"]
    assert all(site["enabled"] is False for site in sites)


def test_incomplete_internal_source_sites_do_not_block_runtime_validation() -> None:
    cfg = ensure_v3_config(
        {
            "common": {
                "deployment": {"role_mode": "internal"},
                "shared_bridge": {"enabled": True, "root_dir": r"D:\QLDownloa\share"},
                "internal_source_sites": [
                    {"building": "A楼", "enabled": True, "host": "", "username": "", "password": ""},
                    {"building": "B楼", "enabled": True, "host": "192.168.1.11", "username": "", "password": ""},
                ],
            },
            "features": {},
        }
    )
    runtime = adapt_runtime_config(cfg)
    assert runtime["download"]["sites"][0]["enabled"] is False
    assert runtime["download"]["sites"][1]["enabled"] is False
    validate_runtime_config(runtime, extract_site_host=extract_site_host, parse_hms_text=_parse_hms)


def test_blank_common_internal_source_sites_fall_back_to_feature_sites() -> None:
    cfg = ensure_v3_config(
        {
            "common": {
                "deployment": {"role_mode": "internal"},
                "shared_bridge": {"enabled": True, "root_dir": r"D:\QLDownloa\share"},
                "internal_source_sites": [
                    {"building": "A楼", "enabled": False, "host": "", "username": "", "password": ""},
                    {"building": "B楼", "enabled": False, "host": "", "username": "", "password": ""},
                ],
            },
            "features": {
                "monthly_report": {
                    "sites": [
                        {"building": "A楼", "enabled": True, "host": "192.168.210.50", "username": "admin", "password": "pw-a"},
                        {"building": "B楼", "enabled": True, "host": "192.168.220.50", "username": "admin", "password": "pw-b"},
                    ],
                },
            },
        }
    )

    runtime = adapt_runtime_config(cfg)

    assert runtime["internal_source_sites"][0]["enabled"] is True
    assert runtime["internal_source_sites"][0]["host"] == "192.168.210.50"
    assert runtime["download"]["sites"][1]["enabled"] is True
    assert runtime["download"]["sites"][1]["host"] == "192.168.220.50"
