from pathlib import Path
import sys

from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.bootstrap import app_factory
from app.modules.handover_review.api import routes as handover_review_routes


class _FakeJobService:
    def active_job_id(self):
        return ""


class _FakeContainer:
    def __init__(self, *, frontend_mode: str, frontend_root: Path, frontend_assets_dir: Path):
        self.config = {"common": {"console": {}}}
        self.runtime_config = {}
        self.config_path = frontend_root / "config.json"
        self.frontend_mode = frontend_mode
        self.frontend_root = frontend_root
        self.frontend_assets_dir = frontend_assets_dir
        self.job_service = _FakeJobService()
        self.scheduler = None
        self.handover_scheduler_manager = None
        self.wet_bulb_collection_scheduler = None
        self.updater_service = None
        self.alert_log_uploader = None
        self.shared_bridge_service = None
        self.version = "web-3.0.0"
        self._role_mode = "external"

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

    def deployment_snapshot(self) -> dict:
        return {"role_mode": self._role_mode}


def _build_app(monkeypatch, tmp_path: Path, *, frontend_mode: str):
    frontend_root = tmp_path / "frontend"
    frontend_root.mkdir()
    (frontend_root / "index.html").write_text(
        """<!doctype html><html><head><link rel="stylesheet" href="/assets/style.css" /></head><body><div id="app"></div><script src="/assets/vue.global.prod.js"></script><script type="module" src="/assets/app.js"></script></body></html>""",
        encoding="utf-8",
    )
    (frontend_root / "app.js").write_text("export const ok = true;", encoding="utf-8")
    (frontend_root / "config_helpers.js").write_text("export const ok = true;", encoding="utf-8")
    (frontend_root / "style.css").write_text("body{background:#fff;}", encoding="utf-8")
    (frontend_root / "vue.global.prod.js").write_text("window.Vue = {};", encoding="utf-8")
    container = _FakeContainer(
        frontend_mode=frontend_mode,
        frontend_root=frontend_root,
        frontend_assets_dir=frontend_root,
    )
    monkeypatch.setattr(app_factory, "build_container", lambda: container)
    monkeypatch.setattr(app_factory, "_is_loopback_client", lambda _host: True)
    return app_factory.create_app(enable_lifespan=False)


def test_source_mode_root_and_assets_are_no_cache(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path, frontend_mode="source")
    client = TestClient(app)

    root_resp = client.get("/")
    asset_prefix = app.state.source_frontend_asset_prefix
    assert f"{asset_prefix}/app.js" in root_resp.text
    assert f"{asset_prefix}/style.css" in root_resp.text
    asset_resp = client.get(f"{asset_prefix}/config_helpers.js")

    expected = "no-store, no-cache, must-revalidate, max-age=0"
    assert root_resp.headers["cache-control"] == expected
    assert root_resp.headers["pragma"] == "no-cache"
    assert root_resp.headers["expires"] == "0"
    assert asset_resp.headers["cache-control"] == expected
    assert asset_resp.headers["pragma"] == "no-cache"
    assert asset_resp.headers["expires"] == "0"


def test_non_source_mode_does_not_force_no_cache(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path, frontend_mode="dist")
    client = TestClient(app)

    root_resp = client.get("/")
    asset_resp = client.get("/assets/config_helpers.js")

    assert root_resp.headers.get("cache-control", "") != "no-store, no-cache, must-revalidate, max-age=0"
    assert asset_resp.headers.get("cache-control", "") != "no-store, no-cache, must-revalidate, max-age=0"
    assert "/assets-src/" not in root_resp.text


def test_source_mode_handover_review_page_uses_same_versioned_asset_prefix(monkeypatch, tmp_path):
    app = _build_app(monkeypatch, tmp_path, frontend_mode="source")
    monkeypatch.setattr(handover_review_routes, "_build_review_services", lambda _container: (object(), None, None, None))
    monkeypatch.setattr(handover_review_routes, "_resolve_building_or_404", lambda _service, _building_code: "A楼")
    client = TestClient(app)

    review_resp = client.get("/handover/review/a")

    expected = "no-store, no-cache, must-revalidate, max-age=0"
    assert review_resp.headers["cache-control"] == expected
    assert f"{app.state.source_frontend_asset_prefix}/app.js" in review_resp.text


def test_assets_src_prefix_is_whitelisted_for_external_review_access():
    assert app_factory._is_externally_allowed_path("/assets-src/20260325230000/app.js") is True
