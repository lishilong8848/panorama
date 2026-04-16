from __future__ import annotations

from handover_log_module.core.cell_rule_compiler import build_effective_handover_config, normalize_row


def test_normalize_row_expands_chiller_mode_keyword_to_running_mode_variant() -> None:
    row = normalize_row({"id": "chiller_mode_1", "d_keywords": ["1号冷机模式"]})

    assert row["d_keywords"] == ["1号冷机模式", "1号冷机运行模式"]


def test_normalize_row_expands_chiller_running_mode_keyword_to_plain_variant() -> None:
    row = normalize_row({"id": "chiller_mode_2", "d_keywords": ["2号冷机运行模式"]})

    assert row["d_keywords"] == ["2号冷机运行模式", "2号冷机模式"]


def test_normalize_row_keeps_non_chiller_keywords_unchanged() -> None:
    row = normalize_row({"id": "wet_bulb", "d_keywords": ["室外湿球温度"]})

    assert row["d_keywords"] == ["室外湿球温度"]


def test_build_effective_handover_config_compiles_chiller_mode_with_both_keywords() -> None:
    cfg = {
        "sites": [{"building": "A楼"}],
        "cell_rules": {
            "default_rows": [
                {
                    "id": "chiller_mode_1",
                    "enabled": True,
                    "target_cell": "",
                    "rule_type": "direct",
                    "d_keywords": ["1号冷机模式"],
                }
            ],
            "building_rows": {},
        },
    }

    effective = build_effective_handover_config(cfg, "A楼", ["A楼"])

    assert effective["rules"]["chiller_mode_1"]["d_match"] == ["1号冷机模式", "1号冷机运行模式"]
