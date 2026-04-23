from __future__ import annotations

import copy
import json
import threading
from pathlib import Path

import pytest

from app.config import settings_loader
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
from app.modules.scheduler.api._config_persistence import (
    persist_scheduler_toggle,
    save_scheduler_config_snapshot,
)


class _SchedulerConfigContainer:
    def __init__(self, config_path: Path, config: dict) -> None:
        self.config_path = config_path
        self.config = copy.deepcopy(config)
        self.logs: list[str] = []
        self.toggles: list[dict] = []

    def reload_config(self, config: dict) -> None:
        self.config = copy.deepcopy(config)

    def add_system_log(self, message: str) -> None:
        self.logs.append(str(message))

    def record_external_scheduler_toggle(self, **kwargs) -> None:
        self.toggles.append(dict(kwargs))


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


def test_scheduler_config_snapshot_updates_handover_common_segment(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    current = load_settings(config_path)
    container = _SchedulerConfigContainer(config_path, current)

    merged = copy.deepcopy(current)
    scheduler = merged["features"]["handover_log"]["scheduler"]
    scheduler["morning_time"] = "08:05:00"
    scheduler["afternoon_time"] = "16:35:00"

    saved = save_scheduler_config_snapshot(
        container,
        merged,
        path=("features", "handover_log", "scheduler"),
    )

    common_doc = get_handover_common_segment(config_path)
    assert common_doc["data"]["scheduler"]["morning_time"] == "08:05:00"
    assert common_doc["data"]["scheduler"]["afternoon_time"] == "16:35:00"
    assert saved["features"]["handover_log"]["scheduler"]["morning_time"] == "08:05:00"
    assert load_settings(config_path)["features"]["handover_log"]["scheduler"]["afternoon_time"] == "16:35:00"


def test_scheduler_toggle_updates_handover_common_segment(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    current = load_settings(config_path)
    container = _SchedulerConfigContainer(config_path, current)

    persist_scheduler_toggle(
        container,
        path=("features", "handover_log", "scheduler"),
        auto_start_in_gui=True,
    )

    common_doc = get_handover_common_segment(config_path)
    assert common_doc["data"]["scheduler"]["auto_start_in_gui"] is True
    assert common_doc["data"]["scheduler"]["enabled"] is True
    assert container.config["features"]["handover_log"]["scheduler"]["auto_start_in_gui"] is True


def test_monthly_report_scheduler_config_snapshot_updates_handover_common_segment(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    current = load_settings(config_path)
    container = _SchedulerConfigContainer(config_path, current)

    merged = copy.deepcopy(current)
    scheduler = merged["features"]["handover_log"]["monthly_event_report"]["scheduler"]
    scheduler["run_time"] = "09:15:00"

    save_scheduler_config_snapshot(
        container,
        merged,
        path=("features", "handover_log", "monthly_event_report", "scheduler"),
    )

    common_doc = get_handover_common_segment(config_path)
    assert common_doc["data"]["monthly_event_report"]["scheduler"]["run_time"] == "09:15:00"
    assert (
        load_settings(config_path)["features"]["handover_log"]["monthly_event_report"]["scheduler"]["run_time"]
        == "09:15:00"
    )


def test_save_settings_does_not_rewrite_existing_user_filled_values(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    payload = copy.deepcopy(DEFAULT_CONFIG_V3)
    payload["common"]["paths"]["business_root_dir"] = r"D:\用户业务目录"
    template = payload["features"]["handover_log"]["template"]
    template["apply_building_title"] = False
    template["title_cell"] = "C3"
    template["building_title_pattern"] = "用户自定义标题"
    template["building_title_map"] = {
        "A楼": "A楼自定义标题",
        "B楼": "B楼自定义标题",
    }
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    saved = save_settings(copy.deepcopy(payload), config_path)

    assert saved["common"]["paths"]["business_root_dir"] == r"D:\用户业务目录"
    assert saved["features"]["handover_log"]["template"]["title_cell"] == "C3"
    assert saved["features"]["handover_log"]["template"]["building_title_pattern"] == "用户自定义标题"
    assert saved["features"]["handover_log"]["template"]["building_title_map"]["A楼"] == "A楼自定义标题"


def test_save_handover_segment_refresh_does_not_rewrite_unrelated_config_values(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    payload = copy.deepcopy(DEFAULT_CONFIG_V3)
    payload["common"]["paths"]["business_root_dir"] = r"D:\用户业务目录"
    payload["features"]["handover_log"]["template"]["title_cell"] = "C3"
    payload["features"]["handover_log"]["template"]["building_title_pattern"] = "用户自定义标题"
    payload["features"]["handover_log"]["template"]["building_title_map"] = {"A楼": "A楼自定义标题"}
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    load_settings(config_path)
    a_doc = get_handover_building_segment("A", config_path)
    a_payload = copy.deepcopy(a_doc["data"])
    a_payload["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-分段保存后新值"

    save_handover_building_segment(
        "A",
        a_payload,
        base_revision=a_doc["revision"],
        config_path=config_path,
    )

    saved = json.loads(config_path.read_text(encoding="utf-8-sig"))
    assert saved["common"]["paths"]["business_root_dir"] == r"D:\用户业务目录"
    assert saved["features"]["handover_log"]["template"]["title_cell"] == "C3"
    assert saved["features"]["handover_log"]["template"]["building_title_pattern"] == "用户自定义标题"
    assert saved["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] == "A楼-分段保存后新值"


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


def test_save_handover_building_segments_can_commit_concurrently(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    load_settings(config_path)

    a_doc = get_handover_building_segment("A", config_path)
    b_doc = get_handover_building_segment("B", config_path)
    a_payload = copy.deepcopy(a_doc["data"])
    b_payload = copy.deepcopy(b_doc["data"])
    a_payload["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-并发保存"
    b_payload["cloud_sheet_sync"]["sheet_names"]["B楼"] = "B楼-并发保存"

    barrier = threading.Barrier(2, timeout=5)
    original_refresh = settings_loader._refresh_handover_aggregate_view

    def _refresh_with_barrier(config_path_arg=None):
        barrier.wait()
        return original_refresh(config_path_arg)

    monkeypatch.setattr(settings_loader, "_refresh_handover_aggregate_view", _refresh_with_barrier)

    errors: list[BaseException] = []

    def _run_save(building_code: str, payload: dict[str, object], revision: int) -> None:
        try:
            save_handover_building_segment(
                building_code,
                payload,
                base_revision=revision,
                config_path=config_path,
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread_a = threading.Thread(target=_run_save, args=("A", a_payload, a_doc["revision"]), daemon=True)
    thread_b = threading.Thread(target=_run_save, args=("B", b_payload, b_doc["revision"]), daemon=True)
    thread_a.start()
    thread_b.start()
    thread_a.join(timeout=10)
    thread_b.join(timeout=10)

    assert not thread_a.is_alive()
    assert not thread_b.is_alive()
    assert not errors

    reloaded = load_settings(config_path)
    assert reloaded["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] == "A楼-并发保存"
    assert reloaded["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["B楼"] == "B楼-并发保存"


def test_save_handover_common_and_building_segments_can_commit_concurrently(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    load_settings(config_path)

    common_doc = get_handover_common_segment(config_path)
    b_doc = get_handover_building_segment("B", config_path)
    common_payload = copy.deepcopy(common_doc["data"])
    b_payload = copy.deepcopy(b_doc["data"])
    common_payload["cloud_sheet_sync"]["root_wiki_url"] = "https://example.com/wiki/concurrent-common"
    b_payload["cloud_sheet_sync"]["sheet_names"]["B楼"] = "B楼-并发保存-common"

    barrier = threading.Barrier(2, timeout=5)
    original_refresh = settings_loader._refresh_handover_aggregate_view

    def _refresh_with_barrier(config_path_arg=None):
        barrier.wait()
        return original_refresh(config_path_arg)

    monkeypatch.setattr(settings_loader, "_refresh_handover_aggregate_view", _refresh_with_barrier)

    errors: list[BaseException] = []

    def _run_common() -> None:
        try:
            save_handover_common_segment(
                common_payload,
                base_revision=common_doc["revision"],
                config_path=config_path,
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def _run_building() -> None:
        try:
            save_handover_building_segment(
                "B",
                b_payload,
                base_revision=b_doc["revision"],
                config_path=config_path,
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread_common = threading.Thread(target=_run_common, daemon=True)
    thread_b = threading.Thread(target=_run_building, daemon=True)
    thread_common.start()
    thread_b.start()
    thread_common.join(timeout=10)
    thread_b.join(timeout=10)

    assert not thread_common.is_alive()
    assert not thread_b.is_alive()
    assert not errors

    reloaded = load_settings(config_path)
    assert reloaded["features"]["handover_log"]["cloud_sheet_sync"]["root_wiki_url"] == "https://example.com/wiki/concurrent-common"
    assert reloaded["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["B楼"] == "B楼-并发保存-common"
