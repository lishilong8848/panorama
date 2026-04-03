from pathlib import Path
import sys

from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.bootstrap import app_factory


class _FakeJobService:
    def active_job_id(self):
        return ""

    def active_job_ids(self, include_waiting=True):
        return []

    def job_counts(self):
        return {"queued": 0, "running": 0, "finished": 0, "failed": 0}

    def shutdown_task_engine(self):
        return None


class _FakeContainer:
    def __init__(self, *, frontend_root: Path, role_mode: str = "internal"):
        self.config = {
            "common": {
                "console": {},
                "deployment": {
                    "role_mode": role_mode,
                    "last_started_role_mode": "",
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
        self.updater_service = None
        self.alert_log_uploader = None
        self.shared_bridge_service = None
        self.version = "web-3.0.0"
        self.runtime_services_armed = False
        self.runtime_service_start_calls = []
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
        self.system_log_next_offset = lambda: 0

    def add_system_log(self, *_args, **_kwargs):
        return None

    def set_scheduler_callback(self, *_args, **_kwargs):
        return None

    def set_handover_scheduler_callback(self, *_args, **_kwargs):
        return None

    def set_wet_bulb_collection_scheduler_callback(self, *_args, **_kwargs):
        return None

    def set_updater_restart_callback(self, *_args, **_kwargs):
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

    def deployment_snapshot(self):
        return {"role_mode": self.role_mode, "node_id": "internal-node", "node_label": "内网端"}

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

    def shared_bridge_snapshot(self):
        return {"enabled": True, "root_dir": "D:/QJPT_Shared", "db_status": "ok"}

    def start_role_runtime_services(self, source="启动确认"):
        self.runtime_services_armed = True
        self.runtime_service_start_calls.append(str(source or ""))
        return {"ok": True, "armed": True, "role_mode": self.role_mode}


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
    client = TestClient(_build_app(monkeypatch, tmp_path))

    response = client.post("/api/jobs/auto-once", json={})

    assert response.status_code == 409
    assert response.json()["detail"] == "当前为内网端，本地管理页不提供该业务入口，请在外网端发起。"


def test_internal_role_still_allows_bridge_health(monkeypatch, tmp_path):
    client = TestClient(_build_app(monkeypatch, tmp_path))

    response = client.get("/api/bridge/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["deployment"]["role_mode"] == "internal"
    assert payload["shared_bridge"]["root_dir"] == "D:/QJPT_Shared"


def test_internal_role_does_not_mount_network_routes(monkeypatch, tmp_path):
    client = TestClient(_build_app(monkeypatch, tmp_path))

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


def test_lifespan_keeps_saved_role_unactivated_until_user_confirms(monkeypatch, tmp_path):
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
        assert payload["role_selection_required"] is True


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
