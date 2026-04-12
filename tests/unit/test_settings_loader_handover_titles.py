from __future__ import annotations

import copy
import json

from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.settings_loader import load_settings, validate_settings


def test_validate_settings_normalizes_handover_template_title_config() -> None:
    cfg = copy.deepcopy(DEFAULT_CONFIG_V3)
    template = cfg["features"]["handover_log"]["template"]
    template["apply_building_title"] = False
    template["title_cell"] = "B2"
    template["building_title_pattern"] = "错误模板"
    template["building_title_map"] = {"A楼": "错误标题"}

    normalized = validate_settings(cfg)
    normalized_template = normalized["features"]["handover_log"]["template"]

    assert normalized_template["apply_building_title"] is True
    assert normalized_template["title_cell"] == "A1"
    assert normalized_template["building_title_pattern"] == "EA118机房{building_code}栋数据中心交接班日志"
    assert normalized_template["building_title_map"]["A楼"] == "EA118机房A栋数据中心交接班日志"
    assert normalized_template["building_title_map"]["E楼"] == "EA118机房E栋数据中心交接班日志"


def test_load_settings_auto_rewrites_noncanonical_handover_title_config(tmp_path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    payload = copy.deepcopy(DEFAULT_CONFIG_V3)
    template = payload["features"]["handover_log"]["template"]
    template["apply_building_title"] = False
    template["title_cell"] = "C3"
    template["building_title_pattern"] = "错误模板"
    template["building_title_map"] = {"A楼": "错误标题"}
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    normalized = load_settings(config_path)
    saved = json.loads(config_path.read_text(encoding="utf-8-sig"))

    assert normalized["features"]["handover_log"]["template"]["title_cell"] == "A1"
    assert saved["features"]["handover_log"]["template"]["title_cell"] == "A1"
    assert saved["features"]["handover_log"]["template"]["building_title_map"]["B楼"] == "EA118机房B栋数据中心交接班日志"
