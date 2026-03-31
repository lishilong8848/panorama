from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.modules.report_pipeline.api import routes


def _base_config(public_base_url: str = ""):
    return {
        "common": {"console": {"port": 18765}},
        "features": {
            "handover_log": {
                "review_ui": {
                    "public_base_url": public_base_url,
                    "buildings": [
                        {"code": "a", "name": "A楼"},
                        {"code": "b", "name": "B楼"},
                    ],
                }
            }
        },
    }


def _make_container(public_base_url: str = ""):
    logs: list[str] = []
    config = _base_config(public_base_url)
    container = SimpleNamespace()
    container.config = config
    container.runtime_config = config
    container.config_path = PROJECT_ROOT / "dummy.json"
    container.version = "3.0.0"
    container._logs = logs
    container.add_system_log = logs.append

    def _reload(settings):
        container.config = settings
        container.runtime_config = settings

    container.reload_config = _reload
    return container


def _make_request(container, hostname: str = "127.0.0.1", port: int = 18765):
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(container=container)),
        url=SimpleNamespace(hostname=hostname, port=port),
    )


def test_review_access_manual_snapshot_is_immediately_effective_and_not_probed(monkeypatch, tmp_path):
    container = _make_container(public_base_url="http://192.168.31.10:18765")
    monkeypatch.setattr(routes, "_resolve_review_access_state_path", lambda _container: tmp_path / "handover_review_access_state.json")
    monkeypatch.setattr(
        routes,
        "_probe_review_base_urls_cached",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("manual snapshot should not probe")),
    )

    result = routes._build_handover_review_access(container, _make_request(container))

    assert result["configured"] is True
    assert result["review_base_url_effective"] == "http://192.168.31.10:18765"
    assert result["review_base_url_effective_source"] == "manual"
    assert result["review_base_url_status"] == "manual_ok"
    assert result["review_links"][0]["url"] == "http://192.168.31.10:18765/handover/review/a"


def test_review_access_uses_persisted_auto_snapshot_without_reprobe(monkeypatch, tmp_path):
    container = _make_container()
    state_path = tmp_path / "handover_review_access_state.json"
    monkeypatch.setattr(routes, "_resolve_review_access_state_path", lambda _container: state_path)
    routes._save_review_access_state(
        container,
        {
            "configured": True,
            "effective_base_url": "http://192.168.1.20:18765",
            "effective_source": "auto",
            "candidates": ["http://192.168.1.20:18765"],
            "validated_candidates": [{"base_url": "http://192.168.1.20:18765", "ok": True, "probes": []}],
            "candidate_results": [{"base_url": "http://192.168.1.20:18765", "ok": True, "probes": []}],
            "status": "auto_ok",
            "error": "",
            "configured_at": "2026-03-25 20:00:00",
            "last_probe_at": "2026-03-25 20:00:00",
        },
    )
    monkeypatch.setattr(
        routes,
        "_probe_review_base_urls_cached",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("persisted snapshot should not probe")),
    )

    result = routes._build_handover_review_access(container, _make_request(container))

    assert result["configured"] is True
    assert result["review_base_url_effective"] == "http://192.168.1.20:18765"
    assert result["review_base_url_effective_source"] == "auto"
    assert result["review_base_url_status"] == "auto_ok"
    assert result["review_base_url_configured_at"] == "2026-03-25 20:00:00"


def test_put_config_persists_manual_snapshot_and_invalidates_probe_cache(monkeypatch, tmp_path):
    state_path = tmp_path / "handover_review_access_state.json"
    monkeypatch.setattr(routes, "_resolve_review_access_state_path", lambda _container: state_path)
    routes._review_base_probe_cache[(('http://192.168.1.20:18765',), (("a", "A楼", "/api/handover/review/a"),))] = {
        "checked_at": 1.0,
        "result": [],
    }

    container = _make_container()
    payload = _base_config(public_base_url="http://192.168.31.10:18765")
    request = _make_request(container)

    monkeypatch.setattr(
        routes,
        "merge_user_config_payload",
        lambda incoming, _current, clear_paths=None, force_overwrite=False: SimpleNamespace(
            merged=incoming,
            suspicious_loss_paths=[],
        ),
    )
    monkeypatch.setattr(routes, "save_settings", lambda settings, _path: settings)

    response = routes.put_config(payload, request)
    persisted = json.loads(state_path.read_text(encoding="utf-8"))

    assert response["handover_review_access"]["review_base_url_effective"] == "http://192.168.31.10:18765"
    assert persisted["configured"] is True
    assert persisted["effective_source"] == "manual"
    assert routes._review_base_probe_cache == {}


def test_schedule_handover_review_access_startup_probe_skips_when_state_already_configured(monkeypatch, tmp_path):
    container = _make_container()
    state_path = tmp_path / "handover_review_access_state.json"
    monkeypatch.setattr(routes, "_resolve_review_access_state_path", lambda _container: state_path)
    routes._save_review_access_state(
        container,
        {
            "configured": True,
            "effective_base_url": "http://192.168.1.20:18765",
            "effective_source": "auto",
            "status": "auto_ok",
            "configured_at": "2026-03-25 20:00:00",
            "last_probe_at": "2026-03-25 20:00:00",
        },
    )

    started_targets = []

    class _FakeThread:
        def __init__(self, *, target, name, daemon):
            started_targets.append(target)

        def start(self):
            raise AssertionError("configured machine should not schedule startup probe")

    monkeypatch.setattr(routes.threading, "Thread", _FakeThread)

    routes.schedule_handover_review_access_startup_probe(container, delay_sec=0)
    routes.schedule_handover_review_access_startup_probe(container, delay_sec=0)

    assert started_targets == []
    assert any("启动不再自动探测" in item for item in container._logs)


def test_reprobe_route_runs_probe_and_persists_snapshot(monkeypatch, tmp_path):
    container = _make_container()
    state_path = tmp_path / "handover_review_access_state.json"
    monkeypatch.setattr(routes, "_resolve_review_access_state_path", lambda _container: state_path)
    monkeypatch.setattr(routes, "_detect_lan_ipv4s", lambda request_host="": ["192.168.31.10"])
    monkeypatch.setattr(
        routes,
        "_probe_review_base_urls_cached",
        lambda base_urls, probe_targets, timeout_sec=1.5: [
            {
                "base_url": "http://192.168.31.10:18765",
                "ok": True,
                "error": "",
                "probes": [
                    {"code": "a", "name": "A楼", "path": "/api/handover/review/a", "ok": True, "error": ""},
                    {"code": "b", "name": "B楼", "path": "/api/handover/review/b", "ok": True, "error": ""},
                ],
            }
        ],
    )

    response = routes.reprobe_handover_review_access(_make_request(container))
    persisted = json.loads(state_path.read_text(encoding="utf-8"))

    assert response["ok"] is True
    assert response["handover_review_access"]["review_base_url_effective"] == "http://192.168.31.10:18765"
    assert response["handover_review_access"]["review_base_url_effective_source"] == "auto"
    assert persisted["configured"] is True
    assert persisted["effective_source"] == "auto"
