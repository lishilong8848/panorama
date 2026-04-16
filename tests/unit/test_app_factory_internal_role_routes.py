from pathlib import Path
import sys

from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.bootstrap import app_factory


class _FakeJobService:
    def __init__(self):
        self.shutdown_calls = 0

    def active_job_id(self):
        return ""

    def active_job_ids(self, include_waiting=True):
        return []

    def job_counts(self):
        return {"queued": 0, "running": 0, "finished": 0, "failed": 0}

    def shutdown_task_engine(self):
        self.shutdown_calls += 1
        return None


class _FakeContainer:
    def __init__(self, *, frontend_root: Path, role_mode: str = "internal"):
        shared_bridge_root = str((frontend_root / "shared").as_posix())
        self.config = {
            "common": {
                "console": {},
                "deployment": {
                    "role_mode": role_mode,
                    "last_started_role_mode": "",
                },
                "shared_bridge": {
                    "enabled": True,
                    "root_dir": shared_bridge_root,
                    "internal_root_dir": shared_bridge_root,
                    "external_root_dir": shared_bridge_root,
                },
            }
        }
        self.runtime_config = {}
        self.config_path = frontend_root / "config.json"
        self.frontend_mode = "source"
        self.frontend_root = frontend_root
        self.frontend_assets_dir = frontend_root
        self.job_service = _FakeJobService()
        self.scheduler = None
        self.handover_scheduler_manager = None
        self.wet_bulb_collection_scheduler = None
        self.day_metric_upload_scheduler = None
        self.alarm_event_upload_scheduler = None
        self.monthly_change_report_scheduler = None
        self.monthly_event_report_scheduler = None
        self.updater_service = None
        self.alert_log_uploader = None
        self.shared_bridge_service = None
        self.version = "web-3.0.0"
        self.runtime_services_armed = False
        self.runtime_service_start_calls = []
        self.runtime_service_stop_calls = []
        self.role_mode = role_mode
        self.startup_handoff = {
            "active": False,
            "mode": "",
            "target_role_mode": "",
            "requested_at": "",
            "reason": "",
            "nonce": "",
        }
        self.startup_handoff_cleared = 0
        self.updater_restart_callback = None
        self.system_log_next_offset = lambda: 0

    def add_system_log(self, *_args, **_kwargs):
        return None

    def set_scheduler_callback(self, *_args, **_kwargs):
        return None

    def set_handover_scheduler_callback(self, *_args, **_kwargs):
        return None

    def set_wet_bulb_collection_scheduler_callback(self, *_args, **_kwargs):
        return None

    def set_updater_restart_callback(self, callback, *_args, **_kwargs):
        self.updater_restart_callback = callback
        return None

    def scheduler_executor_name(self):
        return "-"

    def is_scheduler_executor_bound(self):
        return False

    def handover_scheduler_executor_name(self):
        return "-"

    def is_handover_scheduler_executor_bound(self):
        return False

    def wet_bulb_collection_scheduler_executor_name(self):
        return "-"

    def is_wet_bulb_collection_scheduler_executor_bound(self):
        return False

    def day_metric_upload_scheduler_executor_name(self):
        return "-"

    def is_day_metric_upload_scheduler_executor_bound(self):
        return False

    def alarm_event_upload_scheduler_executor_name(self):
        return "-"

    def is_alarm_event_upload_scheduler_executor_bound(self):
        return False

    def monthly_change_report_scheduler_executor_name(self):
        return "-"

    def is_monthly_change_report_scheduler_executor_bound(self):
        return False

    def monthly_event_report_scheduler_executor_name(self):
        return "-"

    def is_monthly_event_report_scheduler_executor_bound(self):
        return False

    def deployment_snapshot(self):
        deployment = self.config.get("common", {}).get("deployment", {})
        role_mode = str(deployment.get("role_mode") or self.role_mode or "").strip().lower()
        return {
            "role_mode": role_mode,
            "last_started_role_mode": str(deployment.get("last_started_role_mode") or "").strip().lower(),
            "node_id": "internal-node",
            "node_label": str(deployment.get("node_label") or ("内网端" if role_mode == "internal" else "外网端")),
        }

    def reload_config(self, saved):
        self.config = saved
        self.runtime_config = saved.get("common", {})
        self.role_mode = str(saved.get("common", {}).get("deployment", {}).get("role_mode") or "").strip().lower()

    def get_startup_role_handoff(self):
        return dict(self.startup_handoff)

    def clear_startup_role_handoff(self):
        self.startup_handoff = {
            "active": False,
            "mode": "",
            "target_role_mode": "",
            "requested_at": "",
            "reason": "",
            "nonce": "",
        }
        self.startup_handoff_cleared += 1

    def write_startup_role_handoff(self, *, target_role_mode: str, source: str, reason: str = "", source_startup_time: str = ""):
        self.startup_handoff = {
            "active": True,
            "mode": "startup_role_resume",
            "target_role_mode": str(target_role_mode or "").strip().lower(),
            "requested_at": "2026-04-08 18:00:00",
            "reason": str(reason or "").strip(),
            "source": str(source or "").strip(),
            "source_startup_time": str(source_startup_time or "").strip(),
            "nonce": "updater-handoff",
        }
        return dict(self.startup_handoff)

    def shared_bridge_snapshot(self):
        bridge = self.config.get("common", {}).get("shared_bridge", {})
        return {
            "enabled": True,
            "root_dir": str(bridge.get("root_dir", "") or "").strip(),
            "db_status": "ok",
        }

    def start_role_runtime_services(self, source="启动确认"):
        self.runtime_services_armed = True
        self.runtime_service_start_calls.append(str(source or ""))
        return {"ok": True, "armed": True, "role_mode": self.role_mode}

    def stop_role_runtime_services(self, source="退出当前系统"):
        self.runtime_services_armed = False
        self.runtime_service_stop_calls.append(str(source or ""))
        return {"ok": True, "armed": False, "role_mode": self.role_mode, "cancelled_jobs": []}

    def handover_scheduler_status(self):
        return {
            "enabled": True,
            "running": False,
            "status": "未启动",
            "slots": {},
            "state_paths": {},
        }

    def start_handover_scheduler(self, source="手动"):
        return {"ok": True, "running": True, "source": source}

    def stop_day_metric_upload_scheduler(self, source="关闭自动"):
        return {"ok": True, "source": source}

    def stop_alarm_event_upload_scheduler(self, source="关闭自动"):
        return {"ok": True, "source": source}

    def stop_monthly_change_report_scheduler(self, source="关闭自动"):
        return {"ok": True, "source": source}

    def stop_monthly_event_report_scheduler(self, source="关闭自动"):
        return {"ok": True, "source": source}


def _build_app(monkeypatch, tmp_path: Path, *, role_mode: str = "internal"):
    frontend_root = tmp_path / "frontend"
    frontend_root.mkdir()
    (frontend_root / "index.html").write_text(
        """<!doctype html><html><head></head><body><div id=\"app\"></div><script type=\"module\" src=\"/assets/app.js\"></script></body></html>""",
        encoding="utf-8",
    )
    (frontend_root / "app.js").write_text("export const ok = true;", encoding="utf-8")
    container = _FakeContainer(frontend_root=frontend_root, role_mode=role_mode)
    monkeypatch.setattr(app_factory, "build_container", lambda: container)
    monkeypatch.setattr(app_factory, "_is_loopback_client", lambda _host: True)
    monkeypatch.setattr(app_factory, "save_settings", lambda settings, _path: settings)
    return app_factory.create_app(enable_lifespan=False)


def _build_app_with_lifespan(monkeypatch, tmp_path: Path, *, role_mode: str = "internal"):
    frontend_root = tmp_path / "frontend"
    frontend_root.mkdir()
    (frontend_root / "index.html").write_text(
        """<!doctype html><html><head></head><body><div id=\"app\"></div><script type=\"module\" src=\"/assets/app.js\"></script></body></html>""",
        encoding="utf-8",
    )
    (frontend_root / "app.js").write_text("export const ok = true;", encoding="utf-8")
    container = _FakeContainer(frontend_root=frontend_root, role_mode=role_mode)
    monkeypatch.setattr(app_factory, "build_container", lambda: container)
    monkeypatch.setattr(app_factory, "_is_loopback_client", lambda _host: True)
    monkeypatch.setattr(app_factory, "save_settings", lambda settings, _path: settings)
    return app_factory.create_app(enable_lifespan=True)


def test_internal_role_blocks_business_job_routes(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path)
    app.state.runtime_services_activated = True
    app.state.startup_role_confirmed = True
    client = TestClient(app)

    response = client.post("/api/jobs/auto-once", json={})

    assert response.status_code == 409
    assert response.json()["detail"] == "当前为内网端，本地管理页不提供该业务入口，请在外网端发起。"


def test_role_selection_mode_blocks_business_api_until_activation(monkeypatch, tmp_path):
    client = TestClient(_build_app(monkeypatch, tmp_path, role_mode="external"))

    response = client.post("/api/jobs/auto-once", json={})

    assert response.status_code == 409
    assert response.json()["detail"] == "当前未进入内网端或外网端，请先在角色选择页进入系统。"
    bootstrap = client.get("/api/health/bootstrap")
    assert bootstrap.status_code == 200


def test_lan_console_client_can_call_scheduler_api_after_activation(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path, role_mode="external")
    app.state.runtime_services_activated = True
    app.state.startup_role_confirmed = True
    monkeypatch.setattr(app_factory, "_is_loopback_client", lambda _host: False)
    monkeypatch.setattr(app_factory, "_is_lan_console_client", lambda _request: True)
    monkeypatch.setattr("app.modules.scheduler.api._config_persistence.save_settings", lambda settings, _path: settings)
    client = TestClient(app)

    response = client.post("/api/scheduler/handover/start", json={})

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True


def test_internal_role_still_allows_bridge_health(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path)
    app.state.runtime_services_activated = True
    app.state.startup_role_confirmed = True
    expected_root = app.state.container.config["common"]["shared_bridge"]["root_dir"]
    client = TestClient(app)

    response = client.get("/api/bridge/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["deployment"]["role_mode"] == "internal"
    assert payload["shared_bridge"]["root_dir"] == expected_root


def test_internal_role_does_not_mount_network_routes(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path)
    app.state.runtime_services_activated = True
    app.state.startup_role_confirmed = True
    client = TestClient(app)

    response = client.get("/api/network/status")

    assert response.status_code == 404


def test_startup_runtime_requires_explicit_activation(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path)
    container = app.state.container

    with TestClient(app) as client:
        assert container.runtime_services_armed is False
        assert app.state.startup_role_confirmed is False

        response = client.post("/api/runtime/activate-startup", json={"source": "test_activate"})

        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["activated"] is True
        assert payload["already_active"] is False
        assert payload["role_mode"] == "internal"
        assert container.runtime_services_armed is True
        assert container.runtime_service_start_calls == ["test_activate"]
        assert app.state.startup_role_confirmed is True

        second = client.post("/api/runtime/activate-startup", json={"source": "test_activate_again"})
        assert second.status_code == 200
        second_payload = second.json()
        assert second_payload["ok"] is True
        assert second_payload["already_active"] is True
        assert container.runtime_service_start_calls == ["test_activate"]
        assert app.state.startup_role_confirmed is True


def test_startup_runtime_clears_handoff_after_restart_resume(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path, role_mode="external")
    container = app.state.container
    container.startup_handoff = {
        "active": True,
        "mode": "startup_role_resume",
        "target_role_mode": "external",
        "requested_at": "2026-04-03 08:35:00",
        "reason": "role_mode_switch",
        "nonce": "handoff-123",
    }

    with TestClient(app) as client:
        response = client.post(
            "/api/runtime/activate-startup",
            json={
                "source": "startup_role_resume_after_restart",
                "startup_handoff_nonce": "handoff-123",
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert container.startup_handoff_cleared == 1
        assert container.startup_handoff["active"] is False


def test_startup_runtime_rejects_missing_role(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path, role_mode="")
    container = app.state.container

    with TestClient(app) as client:
        response = client.post("/api/runtime/activate-startup", json={"source": "test_activate"})

        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is False
        assert payload["activated"] is False
        assert payload["role_mode"] == ""
        assert container.runtime_services_armed is False
        assert container.runtime_service_start_calls == []
        assert app.state.startup_role_confirmed is False


def test_updater_restart_callback_writes_role_handoff_for_external(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path, role_mode="external")
    container = app.state.container
    exit_calls = []

    def _fake_exit(code):
        exit_calls.append(code)

    class _ImmediateThread:
        def __init__(self, target=None, **_kwargs):
            self._target = target

        def start(self):
            if callable(self._target):
                self._target()

    monkeypatch.setattr(app_factory.os, "_exit", _fake_exit)
    monkeypatch.setattr(app_factory.time, "sleep", lambda _secs: None)
    monkeypatch.setattr(app_factory.threading, "Thread", _ImmediateThread)
    monkeypatch.setenv("QJPT_RESTART_EXIT_CODE", "194")

    callback = container.updater_restart_callback
    assert callable(callback)

    ok, detail = callback({"reason": "updated_restart_scheduled"})

    assert ok is True
    assert detail == "same_console_restart_scheduled"
    assert container.startup_handoff["active"] is True
    assert container.startup_handoff["target_role_mode"] == "external"
    assert container.startup_handoff["source"] == "updater_restart"
    assert exit_calls == [194]


def test_lifespan_exposes_saved_role_as_restorable_without_reconfiguration(monkeypatch, tmp_path):
    app = _build_app_with_lifespan(monkeypatch, tmp_path, role_mode="external")
    container = app.state.container

    with TestClient(app) as client:
        response = client.get("/api/health/bootstrap")

        assert response.status_code == 200
        payload = response.json()
        assert container.runtime_services_armed is False
        assert container.runtime_service_start_calls == []
        assert app.state.startup_role_confirmed is False
        assert payload["runtime_activated"] is False
        assert payload["startup_role_confirmed"] is True
        assert payload["role_selection_required"] is False


def test_activate_startup_with_role_saves_role_and_last_started(monkeypatch, tmp_path):
    app = _build_app_with_lifespan(monkeypatch, tmp_path, role_mode="")
    container = app.state.container
    external_root = str((tmp_path / "external-share").as_posix())

    with TestClient(app) as client:
        response = client.post(
            "/api/runtime/activate-startup",
            json={
                "source": "test_role_select",
                "role_mode": "external",
                "shared_bridge": {
                    "root_dir": external_root,
                    "external_root_dir": external_root,
                    "poll_interval_sec": 3,
                },
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["activated"] is True
        assert payload["role_mode"] == "external"
        assert payload["saved_role"]["role_mode"] == "external"
        deployment = container.config["common"]["deployment"]
        assert deployment["role_mode"] == "external"
        assert deployment["last_started_role_mode"] == "external"
        assert deployment["node_label"] == "外网端"
        bridge = container.config["common"]["shared_bridge"]
        assert bridge["root_dir"] == external_root
        assert bridge["external_root_dir"] == external_root
        assert bridge["poll_interval_sec"] == 3
        assert container.runtime_service_start_calls == ["test_role_select"]
        assert app.state.startup_role_confirmed is True


def test_activate_startup_switches_role_without_process_restart(monkeypatch, tmp_path):
    app = _build_app_with_lifespan(monkeypatch, tmp_path, role_mode="internal")
    container = app.state.container
    external_root = str((tmp_path / "external-share").as_posix())

    with TestClient(app) as client:
        first = client.post("/api/runtime/activate-startup", json={"source": "initial"})
        assert first.status_code == 200
        assert app.state.runtime_services_activated is True

        response = client.post(
            "/api/runtime/activate-startup",
            json={
                "source": "switch_to_external",
                "role_mode": "external",
                "shared_bridge": {
                    "root_dir": external_root,
                    "external_root_dir": external_root,
                },
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["role_mode"] == "external"
        assert container.runtime_service_stop_calls == ["switch_to_external-切换角色前停止当前系统"]
        assert container.runtime_service_start_calls == ["initial", "switch_to_external"]
        assert container.config["common"]["deployment"]["role_mode"] == "external"
        assert container.config["common"]["deployment"]["last_started_role_mode"] == "external"


def test_exit_current_runtime_stops_role_services_and_returns_to_selector(monkeypatch, tmp_path):
    app = _build_app_with_lifespan(monkeypatch, tmp_path, role_mode="external")
    container = app.state.container

    with TestClient(app) as client:
        activate = client.post("/api/runtime/activate-startup", json={"source": "test_activate"})
        assert activate.status_code == 200
        assert app.state.runtime_services_activated is True
        response = client.post("/api/runtime/exit-current", json={"source": "test_exit"})

        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["deactivated"] is True
        assert container.runtime_services_armed is False
        assert container.runtime_service_stop_calls == ["test_exit"]
        assert app.state.runtime_services_activated is False
        assert app.state.startup_role_confirmed is False
        assert app.state.startup_role_user_exited is True

        bootstrap = client.get("/api/health/bootstrap").json()
        assert bootstrap["role_selection_required"] is True
        assert bootstrap["startup_role_user_exited"] is True


def test_app_factory_routes_do_not_leave_mock_val_ser_fields(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path)
    bad_fields = []

    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        for label, field in (("response", route.response_field), ("body", route.body_field)):
            if field is None:
                continue
            adapter = getattr(field, "_type_adapter", None)
            validator = getattr(adapter, "validator", None)
            if "_mock_val_ser" in type(validator).__module__:
                bad_fields.append((route.path, route.name, label))

    assert bad_fields == []
