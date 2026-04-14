from __future__ import annotations

from pathlib import Path

from app.modules.scheduler.api._config_persistence import persist_scheduler_toggle


class _FakeContainer:
    def __init__(self, config: dict, config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        self.reloaded = None

    def reload_config(self, saved: dict) -> None:
        self.config = saved
        self.reloaded = saved


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
