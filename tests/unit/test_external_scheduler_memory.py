from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.bootstrap.container import AppContainer
from app.config.config_adapter import adapt_runtime_config


class _FakeJobService:
    def update_log_buffer_size(self, _size: int) -> None:
        return None

    def configure_task_engine(self, **_kwargs) -> None:
        return None

    def has_incomplete_jobs(self) -> bool:
        return False

    def has_running_jobs(self) -> bool:
        return False

    def set_global_log_sink(self, _sink) -> None:  # noqa: ANN001
        return None


def _build_base_config(runtime_state_root: Path) -> dict:
    return {
        "common": {
            "paths": {"runtime_state_root": str(runtime_state_root)},
            "scheduler": {"auto_start_in_gui": True, "enabled": True},
        },
        "features": {
            "handover_log": {
                "scheduler": {"auto_start_in_gui": False, "enabled": True},
                "monthly_change_report": {"scheduler": {"auto_start_in_gui": True, "enabled": True}},
                "monthly_event_report": {"scheduler": {"auto_start_in_gui": False, "enabled": True}},
            },
            "wet_bulb_collection": {"scheduler": {"auto_start_in_gui": True, "enabled": True}},
            "day_metric_upload": {"scheduler": {"auto_start_in_gui": False, "enabled": True}},
            "alarm_export": {"scheduler": {"auto_start_in_gui": True, "enabled": True}},
        },
    }


def _build_container(tmp_path: Path) -> AppContainer:
    runtime_state_root = tmp_path / "runtime_state"
    runtime_state_root.mkdir(parents=True, exist_ok=True)
    frontend_root = tmp_path / "frontend"
    frontend_root.mkdir(parents=True, exist_ok=True)
    config = _build_base_config(runtime_state_root)
    return AppContainer(
        config=config,
        runtime_config=adapt_runtime_config(config),
        config_path=tmp_path / "config.json",
        frontend_mode="source",
        frontend_root=frontend_root,
        frontend_assets_dir=frontend_root,
        job_service=_FakeJobService(),
    )


def _memory_file(container: AppContainer) -> Path:
    return container._external_scheduler_autostart_path()


def _read_memory_file(container: AppContainer) -> dict:
    path = _memory_file(container)
    return json.loads(path.read_text(encoding="utf-8"))


def test_external_scheduler_memory_falls_back_to_config_when_file_missing(tmp_path: Path) -> None:
    container = _build_container(tmp_path)

    resolved = container.resolve_external_scheduler_autostart_state(force_refresh=True)

    assert resolved["ok"] is True
    assert resolved["memory_source"] == "config_fallback"
    assert resolved["changed"] is True
    assert resolved["states"] == {
        "auto_flow": True,
        "handover": False,
        "wet_bulb_collection": True,
        "day_metric_upload": False,
        "alarm_event_upload": True,
        "monthly_change_report": True,
        "monthly_event_report": False,
    }
    payload = _read_memory_file(container)
    assert payload["source"] == "config_fallback"
    assert payload["states"] == resolved["states"]


def test_external_scheduler_memory_repairs_legacy_exit_snapshot_once(tmp_path: Path) -> None:
    container = _build_container(tmp_path)
    path = _memory_file(container)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "role_mode": "external",
                "updated_at": "2026-04-16 00:00:00",
                "source": "用户退出当前系统",
                "states": {
                    "auto_flow": False,
                    "handover": False,
                    "wet_bulb_collection": False,
                    "day_metric_upload": False,
                    "alarm_event_upload": False,
                    "monthly_change_report": False,
                    "monthly_event_report": False,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    resolved = container.resolve_external_scheduler_autostart_state(force_refresh=True)

    assert resolved["ok"] is True
    assert resolved["memory_source"] == "legacy_repair"
    assert resolved["changed"] is True
    assert resolved["states"]["auto_flow"] is True
    assert resolved["states"]["alarm_event_upload"] is True
    payload = _read_memory_file(container)
    assert payload["source"] == "legacy_repair"
    assert payload["states"] == resolved["states"]


def test_exit_does_not_override_existing_external_scheduler_memory(tmp_path: Path) -> None:
    container = _build_container(tmp_path)
    persisted = container.persist_external_scheduler_autostart_state(
        source="调度开关",
        states={
            "auto_flow": False,
            "handover": True,
            "wet_bulb_collection": False,
            "day_metric_upload": True,
            "alarm_event_upload": False,
            "monthly_change_report": True,
            "monthly_event_report": False,
        },
    )
    assert persisted["ok"] is True

    before = _read_memory_file(container)
    on_exit = container.persist_external_scheduler_autostart_state_on_exit(source="退出当前系统")
    after = _read_memory_file(container)

    assert on_exit["ok"] is True
    assert on_exit["skipped"] is True
    assert on_exit["reason"] == "exit_does_not_override_memory"
    assert before == after
    assert after["source"] == "调度开关"


def test_record_external_scheduler_toggle_updates_target_key_only(tmp_path: Path) -> None:
    container = _build_container(tmp_path)
    container.resolve_external_scheduler_autostart_state(force_refresh=True)

    update = container.record_external_scheduler_toggle(
        path=("features", "day_metric_upload", "scheduler"),
        auto_start_in_gui=True,
        source="调度开关",
    )
    assert update["ok"] is True

    resolved = container.resolve_external_scheduler_autostart_state(force_refresh=True)
    assert resolved["states"]["day_metric_upload"] is True
    assert resolved["states"]["auto_flow"] is True
    assert resolved["states"]["handover"] is False


def test_external_scheduler_memory_updates_runtime_scheduler_paths(tmp_path: Path) -> None:
    container = _build_container(tmp_path)
    container.persist_external_scheduler_autostart_state(
        source="调度开关",
        states={
            "auto_flow": False,
            "handover": True,
            "wet_bulb_collection": True,
            "day_metric_upload": False,
            "alarm_event_upload": False,
            "monthly_change_report": False,
            "monthly_event_report": False,
        },
    )

    result = container.apply_external_scheduler_autostart_state(source="test")

    assert result["ok"] is True
    assert container.runtime_config["handover_log"]["scheduler"]["auto_start_in_gui"] is True
    assert container.runtime_config["wet_bulb_collection"]["scheduler"]["auto_start_in_gui"] is True


def test_refresh_single_scheduler_runtime_rebuilds_only_target_scheduler(tmp_path: Path) -> None:
    container = _build_container(tmp_path)
    container.scheduler = container._build_scheduler()
    container.handover_scheduler_manager = container._build_handover_scheduler_manager()
    original_auto_flow = container.scheduler
    original_handover = container.handover_scheduler_manager
    updated = _build_base_config(tmp_path / "runtime_state")
    updated["features"]["handover_log"]["scheduler"]["morning_time"] = "08:30:00"

    result = container.refresh_single_scheduler_runtime("handover", updated, restart_running=False)

    assert result["ok"] is True
    assert result["scheduler_key"] == "handover"
    assert container.scheduler is original_auto_flow
    assert container.handover_scheduler_manager is not original_handover
    assert container.runtime_config["handover_log"]["scheduler"]["morning_time"] == "08:30:00"


def test_refresh_single_scheduler_runtime_restarts_only_running_target(tmp_path: Path) -> None:
    container = _build_container(tmp_path)
    container.scheduler = container._build_scheduler()
    container.handover_scheduler_manager = container._build_handover_scheduler_manager()
    container.handover_scheduler_manager.start()
    original_auto_flow = container.scheduler
    original_handover = container.handover_scheduler_manager
    updated = _build_base_config(tmp_path / "runtime_state")
    updated["features"]["handover_log"]["scheduler"]["afternoon_time"] = "17:05:00"

    result = container.refresh_single_scheduler_runtime("handover", updated, restart_running=True)

    assert result["ok"] is True
    assert result["was_running"] is True
    assert result["running"] is True
    assert result["restarted"] is True
    assert container.scheduler is original_auto_flow
    assert container.handover_scheduler_manager is not original_handover
    assert container.runtime_config["handover_log"]["scheduler"]["afternoon_time"] == "17:05:00"
    container.handover_scheduler_manager.stop()


def test_refresh_single_scheduler_runtime_restores_previous_scheduler_when_rebuild_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = _build_container(tmp_path)
    container.handover_scheduler_manager = container._build_handover_scheduler_manager()
    container.handover_scheduler_manager.start()
    original_scheduler = container.handover_scheduler_manager
    original_config = json.loads(json.dumps(container.config, ensure_ascii=False))
    updated = _build_base_config(tmp_path / "runtime_state")
    updated["features"]["handover_log"]["scheduler"]["morning_time"] = "09:45:00"

    def _boom():
        raise RuntimeError("rebuild failed")

    monkeypatch.setattr(container, "_build_handover_scheduler_manager", _boom)

    with pytest.raises(RuntimeError, match="rebuild failed"):
        container.refresh_single_scheduler_runtime("handover", updated, restart_running=True)

    assert container.handover_scheduler_manager is original_scheduler
    assert container.handover_scheduler_manager.is_running() is True
    assert container.config == original_config
    container.handover_scheduler_manager.stop()
