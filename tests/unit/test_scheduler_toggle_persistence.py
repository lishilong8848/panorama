from __future__ import annotations

import copy
import json
from pathlib import Path

from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.handover_segment_store import handover_common_segment_path, read_segment_document
from app.config.settings_loader import load_settings
from app.modules.scheduler.api._config_persistence import (
    persist_scheduler_toggle,
    save_handover_common_scheduler_patch,
)


class _FakeContainer:
    def __init__(self, config: dict, config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        self.reloaded = None
        self.recorded_toggles = []
        self.logs = []

    def reload_config(self, saved: dict) -> None:
        self.config = saved
        self.reloaded = saved

    def record_external_scheduler_toggle(self, *, path, auto_start_in_gui, source="") -> None:  # noqa: ANN001
        self.recorded_toggles.append(
            {
                "path": tuple(path),
                "auto_start_in_gui": bool(auto_start_in_gui),
                "source": source,
            }
        )

    def add_system_log(self, text: str) -> None:
        self.logs.append(text)


def _write_default_config(path: Path) -> None:
    payload = copy.deepcopy(DEFAULT_CONFIG_V3)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def test_persist_scheduler_toggle_start_enables_auto_start_and_enabled(monkeypatch, tmp_path: Path) -> None:
    saved_payload = {}

    def _fake_save_settings(merged, config_path):  # noqa: ANN001
        saved_payload["config"] = merged
        saved_payload["config_path"] = config_path
        return merged

    monkeypatch.setattr("app.modules.scheduler.api._config_persistence.save_settings", _fake_save_settings)
    container = _FakeContainer({"features": {"day_metric_upload": {"scheduler": {"enabled": False, "auto_start_in_gui": False}}}}, tmp_path / "config.json")

    persist_scheduler_toggle(
        container,
        path=("features", "day_metric_upload", "scheduler"),
        auto_start_in_gui=True,
    )

    scheduler_cfg = saved_payload["config"]["features"]["day_metric_upload"]["scheduler"]
    assert scheduler_cfg["enabled"] is True
    assert scheduler_cfg["auto_start_in_gui"] is True
    assert container.reloaded is not None
    assert container.recorded_toggles == [
        {
            "path": ("features", "day_metric_upload", "scheduler"),
            "auto_start_in_gui": True,
            "source": "调度开关",
        }
    ]


def test_persist_scheduler_toggle_stop_disables_auto_start_without_forcing_enabled_false(monkeypatch, tmp_path: Path) -> None:
    saved_payload = {}

    def _fake_save_settings(merged, config_path):  # noqa: ANN001
        saved_payload["config"] = merged
        saved_payload["config_path"] = config_path
        return merged

    monkeypatch.setattr("app.modules.scheduler.api._config_persistence.save_settings", _fake_save_settings)
    container = _FakeContainer({"common": {"scheduler": {"enabled": True, "auto_start_in_gui": True}}}, tmp_path / "config.json")

    persist_scheduler_toggle(
        container,
        path=("common", "scheduler"),
        auto_start_in_gui=False,
    )

    scheduler_cfg = saved_payload["config"]["common"]["scheduler"]
    assert scheduler_cfg["enabled"] is True
    assert scheduler_cfg["auto_start_in_gui"] is False
    assert container.reloaded is not None
    assert container.recorded_toggles == [
        {
            "path": ("common", "scheduler"),
            "auto_start_in_gui": False,
            "source": "调度开关",
        }
    ]


def test_handover_scheduler_patch_updates_common_segment_and_aggregate(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    config = load_settings(config_path)
    container = _FakeContainer(config, config_path)

    result = save_handover_common_scheduler_patch(
        container,
        path=("features", "handover_log", "scheduler"),
        scheduler_patch={"morning_time": "06:31:00"},
        source="test",
    )

    common_doc = read_segment_document(handover_common_segment_path(config_path))
    aggregate = json.loads(config_path.read_text(encoding="utf-8-sig"))
    assert result["changed"] is True
    assert result["scheduler_config"]["morning_time"] == "06:31:00"
    assert common_doc["data"]["scheduler"]["morning_time"] == "06:31:00"
    assert aggregate["features"]["handover_log"]["scheduler"]["morning_time"] == "06:31:00"
    assert any("已更新交接班公共分段调度" in item for item in container.logs)


def test_persist_handover_scheduler_toggle_writes_common_segment(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    config = load_settings(config_path)
    container = _FakeContainer(config, config_path)

    persist_scheduler_toggle(
        container,
        path=("features", "handover_log", "scheduler"),
        auto_start_in_gui=True,
    )

    common_doc = read_segment_document(handover_common_segment_path(config_path))
    aggregate = json.loads(config_path.read_text(encoding="utf-8-sig"))
    assert common_doc["data"]["scheduler"]["enabled"] is True
    assert common_doc["data"]["scheduler"]["auto_start_in_gui"] is True
    assert aggregate["features"]["handover_log"]["scheduler"]["enabled"] is True
    assert aggregate["features"]["handover_log"]["scheduler"]["auto_start_in_gui"] is True
    assert container.reloaded is not None
    assert container.recorded_toggles == [
        {
            "path": ("features", "handover_log", "scheduler"),
            "auto_start_in_gui": True,
            "source": "调度开关",
        }
    ]
