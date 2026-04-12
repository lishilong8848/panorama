from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.handover_segment_store import (
    handover_building_segment_path,
    handover_common_segment_path,
    read_segment_document,
)
from app.config.settings_loader import (
    get_handover_common_segment,
    get_handover_building_segment,
    load_settings,
    save_handover_common_segment,
    save_handover_building_segment,
    save_settings,
)


def _write_default_config(path: Path) -> None:
    payload = copy.deepcopy(DEFAULT_CONFIG_V3)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def test_load_settings_migrates_handover_segments_on_first_run(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)

    normalized = load_settings(config_path)

    common_doc = read_segment_document(handover_common_segment_path(config_path))
    a_doc = read_segment_document(handover_building_segment_path(config_path, "A"))

    assert common_doc["revision"] == 1
    assert a_doc["revision"] == 1
    assert common_doc["data"]["cloud_sheet_sync"]["sheet_names"] == {}
    assert (
        a_doc["data"]["cloud_sheet_sync"]["sheet_names"]["A楼"]
        == normalized["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"]
    )
    assert list(tmp_path.glob("表格计算配置.pre_handover_segments.*.json"))


def test_save_handover_building_segment_keeps_other_buildings_unchanged(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    current = load_settings(config_path)

    old_a_doc = get_handover_building_segment("A", config_path)
    a_payload = copy.deepcopy(old_a_doc["data"])
    a_payload["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-独立Sheet"

    saved, next_doc, _ = save_handover_building_segment(
        "A",
        a_payload,
        base_revision=old_a_doc["revision"],
        config_path=config_path,
    )

    assert next_doc["revision"] == old_a_doc["revision"] + 1
    assert saved["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] == "A楼-独立Sheet"
    assert (
        saved["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["B楼"]
        == current["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["B楼"]
    )


def test_save_handover_building_segment_rejects_stale_revision(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    load_settings(config_path)

    old_a_doc = get_handover_building_segment("A", config_path)
    a_payload = copy.deepcopy(old_a_doc["data"])
    a_payload["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-第一次保存"
    save_handover_building_segment(
        "A",
        a_payload,
        base_revision=old_a_doc["revision"],
        config_path=config_path,
    )

    stale_payload = copy.deepcopy(old_a_doc["data"])
    stale_payload["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-旧快照"
    with pytest.raises(ValueError, match="当前楼配置已被其他人修改"):
        save_handover_building_segment(
            "A",
            stale_payload,
            base_revision=old_a_doc["revision"],
            config_path=config_path,
        )


def test_save_handover_common_segment_rejects_stale_revision(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    load_settings(config_path)

    old_common_doc = get_handover_common_segment(config_path)
    common_payload = copy.deepcopy(old_common_doc["data"])
    common_payload["cloud_sheet_sync"]["root_wiki_url"] = "https://example.com/wiki/common-first"
    save_handover_common_segment(
        common_payload,
        base_revision=old_common_doc["revision"],
        config_path=config_path,
    )

    stale_payload = copy.deepcopy(old_common_doc["data"])
    stale_payload["cloud_sheet_sync"]["root_wiki_url"] = "https://example.com/wiki/common-stale"
    with pytest.raises(ValueError, match="公共配置已被其他人修改"):
        save_handover_common_segment(
            stale_payload,
            base_revision=old_common_doc["revision"],
            config_path=config_path,
        )


def test_save_settings_preserves_segment_backed_handover_values(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    current = load_settings(config_path)

    a_doc = get_handover_building_segment("A", config_path)
    a_payload = copy.deepcopy(a_doc["data"])
    a_payload["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-段配置真值"
    save_handover_building_segment(
        "A",
        a_payload,
        base_revision=a_doc["revision"],
        config_path=config_path,
    )

    stale_payload = copy.deepcopy(current)
    stale_payload["common"]["paths"]["business_root_dir"] = r"D:\SegmentedRoot"
    stale_payload["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-旧页面错误值"

    saved = save_settings(stale_payload, config_path)

    assert saved["common"]["paths"]["business_root_dir"] == r"D:\SegmentedRoot"
    assert saved["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] == "A楼-段配置真值"


def test_load_settings_prefers_segment_truth_after_root_config_is_stale(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    load_settings(config_path)

    a_doc = get_handover_building_segment("A", config_path)
    a_payload = copy.deepcopy(a_doc["data"])
    a_payload["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-分段真值"
    save_handover_building_segment(
        "A",
        a_payload,
        base_revision=a_doc["revision"],
        config_path=config_path,
    )

    root_payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    root_payload["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-聚合旧值"
    config_path.write_text(json.dumps(root_payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    reloaded = load_settings(config_path)

    assert reloaded["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] == "A楼-分段真值"
