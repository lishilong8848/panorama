from __future__ import annotations

import copy
from pathlib import Path

from app.bootstrap.container import AppContainer
from app.config.config_adapter import adapt_runtime_config
from app.config.config_schema_v3 import DEFAULT_CONFIG_V3


class _RecordingJobService:
    def __init__(self) -> None:
        self.log_buffer_sizes = []
        self.configured_runtime_configs = []
        self.global_log_sink = None

    def update_log_buffer_size(self, value: int) -> None:
        self.log_buffer_sizes.append(value)

    def configure_task_engine(self, *, runtime_config, app_dir, config_snapshot_getter, current_ssid_getter=None) -> None:
        self.configured_runtime_configs.append(runtime_config)

    def set_global_log_sink(self, sink) -> None:
        self.global_log_sink = sink


class _NoReloadService:
    def __init__(self) -> None:
        self.stop_calls = 0
        self.start_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1

    def start(self) -> None:
        self.start_calls += 1

    def is_running(self) -> bool:
        return False

    def get_deployment_snapshot(self) -> dict:
        return {"role_mode": "external"}


class _FakeRuntimeService(_NoReloadService):
    def __init__(
        self,
        *,
        role_mode: str = "external",
        enabled: bool = False,
        running: bool = False,
        runtime_snapshot: dict | None = None,
    ) -> None:
        super().__init__()
        self.enabled = enabled
        self.auto_start_in_gui = False
        self._running = running
        self._role_mode = role_mode
        self._runtime_snapshot = runtime_snapshot or {
            "enabled": enabled,
            "running": running,
            "status": "已停止",
            "next_run_time": "",
            "slots": {},
        }

    def stop(self) -> dict:
        self.stop_calls += 1
        self._running = False
        return {"stopped": True, "running": False, "reason": "stopped"}

    def start(self) -> dict:
        self.start_calls += 1
        self._running = True
        return {"started": True, "running": True, "reason": "started"}

    def is_running(self) -> bool:
        return self._running

    def get_deployment_snapshot(self) -> dict:
        return {
            "role_mode": self._role_mode,
            "node_label": "内网端" if self._role_mode == "internal" else "外网端",
        }

    def get_runtime_snapshot(self) -> dict:
        return {
            **self._runtime_snapshot,
            "enabled": self._runtime_snapshot.get("enabled", self.enabled),
            "running": self._running,
        }

    def status_text(self) -> str:
        return "运行中" if self._running else "已停止"

    def next_run_text(self) -> str:
        return ""


def test_apply_config_snapshot_light_does_not_rebuild_runtime_services(tmp_path: Path) -> None:
    config = copy.deepcopy(DEFAULT_CONFIG_V3)
    runtime_config = adapt_runtime_config(config)
    job_service = _RecordingJobService()
    container = AppContainer(
        config=config,
        runtime_config=runtime_config,
        config_path=tmp_path / "表格计算配置.json",
        frontend_mode="source",
        frontend_root=tmp_path,
        frontend_assets_dir=tmp_path,
        job_service=job_service,
    )
    services = [_NoReloadService() for _ in range(9)]
    (
        container.scheduler,
        container.handover_scheduler_manager,
        container.wet_bulb_collection_scheduler,
        container.day_metric_upload_scheduler,
        container.alarm_event_upload_scheduler,
        container.monthly_change_report_scheduler,
        container.monthly_event_report_scheduler,
        container.updater_service,
        container.shared_bridge_service,
    ) = services
    container.alert_log_uploader = _NoReloadService()

    updated = copy.deepcopy(config)
    updated["features"]["handover_log"]["cloud_sheet_sync"]["root_wiki_url"] = "https://example.com/handover"

    container.apply_config_snapshot(updated, mode="light")

    assert container.config["features"]["handover_log"]["cloud_sheet_sync"]["root_wiki_url"] == "https://example.com/handover"
    assert container.wifi_service is not None
    assert job_service.log_buffer_sizes
    assert job_service.configured_runtime_configs
    assert all(service.stop_calls == 0 for service in services)
    assert all(service.start_calls == 0 for service in services)
    assert container.alert_log_uploader.stop_calls == 0
    assert container.alert_log_uploader.start_calls == 0


def test_deployment_snapshot_prefers_configured_role_when_shared_bridge_runtime_is_stale(tmp_path: Path) -> None:
    config = copy.deepcopy(DEFAULT_CONFIG_V3)
    config["common"]["deployment"]["role_mode"] = "external"
    config["common"]["deployment"]["last_started_role_mode"] = "external"
    config["common"]["deployment"]["node_label"] = "外网端"
    runtime_config = adapt_runtime_config(config)
    container = AppContainer(
        config=config,
        runtime_config=runtime_config,
        config_path=tmp_path / "表格计算配置.json",
        frontend_mode="source",
        frontend_root=tmp_path,
        frontend_assets_dir=tmp_path,
        job_service=_RecordingJobService(),
    )
    container.shared_bridge_service = _FakeRuntimeService(role_mode="internal")
    container.runtime_services_armed = True

    snapshot = container.deployment_snapshot()

    assert snapshot["role_mode"] == "external"
    assert snapshot["last_started_role_mode"] == "external"
    assert snapshot["node_label"] == "外网端"


def test_start_role_runtime_services_rebuilds_shared_bridge_when_role_changes(tmp_path: Path) -> None:
    config = copy.deepcopy(DEFAULT_CONFIG_V3)
    config["common"]["deployment"]["role_mode"] = "external"
    config["common"]["deployment"]["last_started_role_mode"] = "external"
    config["common"]["deployment"]["node_label"] = "外网端"
    runtime_config = adapt_runtime_config(config)
    container = AppContainer(
        config=config,
        runtime_config=runtime_config,
        config_path=tmp_path / "表格计算配置.json",
        frontend_mode="source",
        frontend_root=tmp_path,
        frontend_assets_dir=tmp_path,
        job_service=_RecordingJobService(),
    )
    stale_bridge = _FakeRuntimeService(role_mode="internal")
    rebuilt_bridge = _FakeRuntimeService(role_mode="external")
    disabled_runtime = _FakeRuntimeService(enabled=False)
    disabled_handover_runtime = _FakeRuntimeService(
        enabled=False,
        runtime_snapshot={"enabled": False, "running": False, "status": "已禁用", "slots": {}},
    )
    container.scheduler = disabled_runtime
    container.handover_scheduler_manager = disabled_handover_runtime
    container.wet_bulb_collection_scheduler = disabled_runtime
    container.day_metric_upload_scheduler = disabled_runtime
    container.alarm_event_upload_scheduler = disabled_runtime
    container.monthly_change_report_scheduler = disabled_runtime
    container.monthly_event_report_scheduler = disabled_runtime
    container.updater_service = _FakeRuntimeService(enabled=False)
    container.alert_log_uploader = _FakeRuntimeService(enabled=False)
    container.shared_bridge_service = stale_bridge
    container.apply_external_scheduler_autostart_state = lambda source="": {  # noqa: ARG005
        "ok": True,
        "states": {},
        "changed": False,
    }
    container._build_shared_bridge_service = lambda: rebuilt_bridge  # type: ignore[method-assign]

    result = container.start_role_runtime_services(source="test_activate")

    assert result["ok"] is True
    assert result["role_mode"] == "external"
    assert container.runtime_services_armed is True
    assert stale_bridge.stop_calls == 1
    assert container.shared_bridge_service is rebuilt_bridge
    assert rebuilt_bridge.start_calls == 1
