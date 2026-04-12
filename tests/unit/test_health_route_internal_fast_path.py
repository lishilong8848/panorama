from pathlib import Path
import sys
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.modules.report_pipeline.api import routes


class _FakeWetBulbCollectionService:
    def __init__(self, _runtime_cfg):
        pass

    def build_target_descriptor(self, force_refresh=False):
        _ = force_refresh
        return {}


class _ExplodingWetBulbCollectionService:
    def __init__(self, _runtime_cfg):
        pass

    def build_target_descriptor(self, force_refresh=False):
        _ = force_refresh
        raise AssertionError("internal health should not build wet bulb target preview")


def _explode(*_args, **_kwargs):
    raise AssertionError("should not be called for internal health")


class _ExplodingDayMetricBitableExportService:
    def __init__(self, _cfg):
        pass

    def build_target_descriptor(self, force_refresh=False):
        _ = force_refresh
        raise AssertionError("internal health should not build day metric target preview")


class _ExplodingSharedSourceCacheService:
    def __init__(self, *, runtime_config, store, download_browser_pool=None, emit_log=None):
        _ = runtime_config, store, download_browser_pool, emit_log

    def get_alarm_event_upload_target_preview(self, force_refresh=False):
        _ = force_refresh
        raise AssertionError("internal health should not build alarm target preview")


def test_internal_health_skips_external_handover_runtime_context(monkeypatch, tmp_path):
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)
    monkeypatch.setattr(routes, "WetBulbCollectionService", _FakeWetBulbCollectionService)
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _ExplodingDayMetricBitableExportService)
    monkeypatch.setattr(routes, "EventFollowupCacheStore", _explode)
    monkeypatch.setattr(routes, "load_handover_config", _explode)
    monkeypatch.setattr(routes, "ReviewSessionService", _explode)
    monkeypatch.setattr(routes, "ReviewFollowupTriggerService", _explode)
    monkeypatch.setattr(routes, "_build_handover_review_access", _explode)

    container = SimpleNamespace(
        version="web-3.0.0",
        config={"version": 3},
        config_path=tmp_path / "config.json",
        runtime_config={
            "paths": {"runtime_state_root": str(tmp_path / ".runtime")},
            "scheduler": {},
            "download": {"resume": {}},
            "handover_log": {"template": {}, "event_sections": {"cache": {"state_file": "handover_cache.json"}}},
            "network": {},
            "wet_bulb_collection": {},
            "shared_bridge": {},
        },
        scheduler=None,
        frontend_mode="source",
        frontend_root=str(tmp_path / "frontend"),
        frontend_assets_dir=str(tmp_path / "frontend"),
        job_service=SimpleNamespace(
            active_job_id=lambda: "",
            active_job_ids=lambda include_waiting=True: [],
            job_counts=lambda: {"queued": 0, "running": 0, "finished": 0, "failed": 0},
        ),
        updater_snapshot=lambda: {},
        handover_scheduler_status=lambda: {"enabled": False, "running": False, "status": "未初始化", "slots": {}, "state_paths": {}},
        wet_bulb_collection_scheduler_status=lambda: {
            "enabled": False,
            "running": False,
            "status": "未初始化",
            "next_run_time": "",
            "last_check_at": "",
            "last_decision": "",
            "last_trigger_at": "",
            "last_trigger_result": "",
            "state_path": "",
            "state_exists": False,
        },
        scheduler_executor_name=lambda: "-",
        is_scheduler_executor_bound=lambda: False,
        handover_scheduler_executor_name=lambda: "-",
        is_handover_scheduler_executor_bound=lambda: False,
        wet_bulb_collection_scheduler_executor_name=lambda: "-",
        is_wet_bulb_collection_scheduler_executor_bound=lambda: False,
        deployment_snapshot=lambda: {"role_mode": "internal", "node_id": "internal-node", "node_label": "内网端"},
        shared_bridge_snapshot=lambda: {
            "enabled": True,
            "role_mode": "internal",
            "root_dir": "D:/share",
            "internal_download_pool": {"page_slots": []},
            "internal_source_cache": {},
        },
        system_logs=[],
        get_system_log_entries=lambda **_kwargs: [],
        system_log_next_offset=lambda: 0,
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)), url=SimpleNamespace(hostname="127.0.0.1", port=18765))

    payload = routes.health(request)

    assert payload["ok"] is True
    assert payload["deployment"]["role_mode"] == "internal"
    assert payload["handover"]["review_status"]["batch_key"] == ""
    assert payload["handover"]["review_status"]["followup_progress"]["status"] == "idle"
    assert payload["handover"]["review_links"] == []
    assert payload["handover"]["review_base_url_effective"] == ""
    assert payload["handover"]["event_sections"]["pending_count"] == 0


def test_internal_health_requests_shared_bridge_light_snapshot(monkeypatch, tmp_path):
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)
    monkeypatch.setattr(routes, "WetBulbCollectionService", _FakeWetBulbCollectionService)
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _ExplodingDayMetricBitableExportService)

    shared_bridge_modes = []

    class _Container(SimpleNamespace):
        def shared_bridge_snapshot(self, mode="external_full"):
            shared_bridge_modes.append(mode)
            return {
                "enabled": True,
                "role_mode": "internal",
                "root_dir": "D:/share",
                "internal_download_pool": {"page_slots": []},
                "internal_source_cache": {},
            }

    container = _Container(
        version="web-3.0.0",
        config={"version": 3},
        config_path=tmp_path / "config.json",
        runtime_config={
            "paths": {"runtime_state_root": str(tmp_path / ".runtime")},
            "scheduler": {},
            "download": {"resume": {}},
            "handover_log": {"template": {}, "event_sections": {"cache": {"state_file": "handover_cache.json"}}},
            "network": {},
            "wet_bulb_collection": {},
            "shared_bridge": {},
        },
        scheduler=None,
        frontend_mode="source",
        frontend_root=str(tmp_path / "frontend"),
        frontend_assets_dir=str(tmp_path / "frontend"),
        job_service=SimpleNamespace(
            active_job_id=lambda: "",
            active_job_ids=lambda include_waiting=True: [],
            job_counts=lambda: {"queued": 0, "running": 0, "finished": 0, "failed": 0},
        ),
        updater_snapshot=lambda: {},
        handover_scheduler_status=lambda: {"enabled": False, "running": False, "status": "未初始化", "slots": {}, "state_paths": {}},
        wet_bulb_collection_scheduler_status=lambda: {
            "enabled": False,
            "running": False,
            "status": "未初始化",
            "next_run_time": "",
            "last_check_at": "",
            "last_decision": "",
            "last_trigger_at": "",
            "last_trigger_result": "",
            "state_path": "",
            "state_exists": False,
        },
        scheduler_executor_name=lambda: "-",
        is_scheduler_executor_bound=lambda: False,
        handover_scheduler_executor_name=lambda: "-",
        is_handover_scheduler_executor_bound=lambda: False,
        wet_bulb_collection_scheduler_executor_name=lambda: "-",
        is_wet_bulb_collection_scheduler_executor_bound=lambda: False,
        deployment_snapshot=lambda: {"role_mode": "internal", "node_id": "internal-node", "node_label": "内网端"},
        system_logs=[],
        get_system_log_entries=lambda **_kwargs: [],
        system_log_next_offset=lambda: 0,
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)), url=SimpleNamespace(hostname="127.0.0.1", port=18765))

    payload = routes.health(request)

    assert payload["ok"] is True
    assert shared_bridge_modes == ["internal_light"]


def test_internal_health_skips_live_wifi_probe(monkeypatch, tmp_path):
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)
    monkeypatch.setattr(routes, "WetBulbCollectionService", _FakeWetBulbCollectionService)
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _ExplodingDayMetricBitableExportService)

    class _WifiService:
        def current_ssid(self):
            raise AssertionError("internal health should not probe current_ssid")

        def current_interface_name(self):
            raise AssertionError("internal health should not probe current_interface_name")

        def visible_targets(self):
            raise AssertionError("internal health should not probe visible_targets")

        def get_last_switch_report(self):
            return {"result": "idle", "error_type": "", "error": ""}

    container = SimpleNamespace(
        version="web-3.0.0",
        config={"version": 3},
        config_path=tmp_path / "config.json",
        runtime_config={
            "paths": {"runtime_state_root": str(tmp_path / ".runtime")},
            "scheduler": {},
            "download": {"resume": {}},
            "handover_log": {"template": {}, "event_sections": {"cache": {"state_file": "handover_cache.json"}}},
            "network": {},
            "wet_bulb_collection": {},
            "shared_bridge": {},
        },
        scheduler=None,
        frontend_mode="source",
        frontend_root=str(tmp_path / "frontend"),
        frontend_assets_dir=str(tmp_path / "frontend"),
        wifi_service=_WifiService(),
        job_service=SimpleNamespace(
            active_job_id=lambda: "",
            active_job_ids=lambda include_waiting=True: [],
            job_counts=lambda: {"queued": 0, "running": 0, "finished": 0, "failed": 0},
        ),
        updater_snapshot=lambda: {},
        handover_scheduler_status=lambda: {"enabled": False, "running": False, "status": "未初始化", "slots": {}, "state_paths": {}},
        wet_bulb_collection_scheduler_status=lambda: {
            "enabled": False,
            "running": False,
            "status": "未初始化",
            "next_run_time": "",
            "last_check_at": "",
            "last_decision": "",
            "last_trigger_at": "",
            "last_trigger_result": "",
            "state_path": "",
            "state_exists": False,
        },
        scheduler_executor_name=lambda: "-",
        is_scheduler_executor_bound=lambda: False,
        handover_scheduler_executor_name=lambda: "-",
        is_handover_scheduler_executor_bound=lambda: False,
        wet_bulb_collection_scheduler_executor_name=lambda: "-",
        is_wet_bulb_collection_scheduler_executor_bound=lambda: False,
        deployment_snapshot=lambda: {"role_mode": "internal", "node_id": "internal-node", "node_label": "内网端"},
        shared_bridge_snapshot=lambda mode="external_full": {
            "enabled": True,
            "role_mode": "internal",
            "root_dir": "D:/share",
            "internal_download_pool": {"page_slots": []},
            "internal_source_cache": {},
        },
        system_logs=[],
        get_system_log_entries=lambda **_kwargs: [],
        system_log_next_offset=lambda: 0,
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)), url=SimpleNamespace(hostname="127.0.0.1", port=18765))

    payload = routes.health(request)

    assert payload["ok"] is True
    assert payload["network"]["current_ssid"] is None
    assert payload["network"]["interface_name"] == ""


def test_internal_health_skips_wet_bulb_target_preview(monkeypatch, tmp_path):
    monkeypatch.setattr(routes, "get_app_dir", lambda: tmp_path)
    monkeypatch.setattr(routes, "WetBulbCollectionService", _ExplodingWetBulbCollectionService)
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _ExplodingDayMetricBitableExportService)
    monkeypatch.setattr(routes, "SharedSourceCacheService", _ExplodingSharedSourceCacheService)

    container = SimpleNamespace(
        version="web-3.0.0",
        config={"version": 3},
        config_path=tmp_path / "config.json",
        runtime_config={
            "paths": {"runtime_state_root": str(tmp_path / ".runtime")},
            "scheduler": {},
            "download": {"resume": {}},
            "handover_log": {"template": {}, "event_sections": {"cache": {"state_file": "handover_cache.json"}}},
            "network": {},
            "wet_bulb_collection": {},
            "shared_bridge": {},
        },
        scheduler=None,
        frontend_mode="source",
        frontend_root=str(tmp_path / "frontend"),
        frontend_assets_dir=str(tmp_path / "frontend"),
        job_service=SimpleNamespace(
            active_job_id=lambda: "",
            active_job_ids=lambda include_waiting=True: [],
            job_counts=lambda: {"queued": 0, "running": 0, "finished": 0, "failed": 0},
        ),
        updater_snapshot=lambda: {},
        handover_scheduler_status=lambda: {"enabled": False, "running": False, "status": "未初始化", "slots": {}, "state_paths": {}},
        wet_bulb_collection_scheduler_status=lambda: {
            "enabled": False,
            "running": False,
            "status": "未初始化",
            "next_run_time": "",
            "last_check_at": "",
            "last_decision": "",
            "last_trigger_at": "",
            "last_trigger_result": "",
            "state_path": "",
            "state_exists": False,
        },
        scheduler_executor_name=lambda: "-",
        is_scheduler_executor_bound=lambda: False,
        handover_scheduler_executor_name=lambda: "-",
        is_handover_scheduler_executor_bound=lambda: False,
        wet_bulb_collection_scheduler_executor_name=lambda: "-",
        is_wet_bulb_collection_scheduler_executor_bound=lambda: False,
        deployment_snapshot=lambda: {"role_mode": "internal", "node_id": "internal-node", "node_label": "内网端"},
        shared_bridge_snapshot=lambda mode="external_full": {
            "enabled": True,
            "role_mode": "internal",
            "root_dir": "D:/share",
            "internal_download_pool": {"page_slots": []},
            "internal_source_cache": {},
        },
        system_logs=[],
        get_system_log_entries=lambda **_kwargs: [],
        system_log_next_offset=lambda: 0,
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)), url=SimpleNamespace(hostname="127.0.0.1", port=18765))

    payload = routes.health(request)

    assert payload["ok"] is True
    assert payload["wet_bulb_collection"]["target_preview"] == {}
    assert payload["alarm_event_upload"]["target_preview"] == {}
