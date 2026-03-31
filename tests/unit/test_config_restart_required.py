from __future__ import annotations

from types import SimpleNamespace

from app.modules.report_pipeline.api import routes


def _make_config(
    *,
    role_mode: str = "",
    bridge_enabled: bool = False,
    bridge_root_dir: str = "",
    bridge_internal_root_dir: str = "",
    bridge_external_root_dir: str = "",
):
    return {
        "version": 3,
        "common": {
            "deployment": {
                "role_mode": role_mode,
                "node_id": "",
                "node_label": "",
            },
            "shared_bridge": {
                "enabled": bridge_enabled,
                "root_dir": bridge_root_dir,
                "internal_root_dir": bridge_internal_root_dir,
                "external_root_dir": bridge_external_root_dir,
                "poll_interval_sec": 2,
                "heartbeat_interval_sec": 5,
                "claim_lease_sec": 30,
                "stale_task_timeout_sec": 1800,
                "artifact_retention_days": 7,
                "sqlite_busy_timeout_ms": 5000,
            },
        },
        "features": {
            "handover_log": {
                "review_ui": {
                    "public_base_url": "",
                    "buildings": [{"code": "a", "name": "A楼"}],
                }
            }
        },
    }


def _make_request(container):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def _make_container(current_config):
    container = SimpleNamespace()
    container.config = current_config
    container.runtime_config = current_config
    container.config_path = "dummy.json"
    container.logs = []
    container.add_system_log = container.logs.append

    def _reload(saved):
        container.config = saved
        container.runtime_config = saved

    container.reload_config = _reload
    return container


def test_put_config_marks_restart_required_when_role_mode_changes(monkeypatch) -> None:
    current = _make_config(role_mode="", bridge_enabled=False, bridge_root_dir="")
    incoming = _make_config(role_mode="external", bridge_enabled=True, bridge_root_dir="D:/QJPT_Shared")
    container = _make_container(current)

    monkeypatch.setattr(
        routes,
        "merge_user_config_payload",
        lambda payload, _current, clear_paths=None, force_overwrite=False: SimpleNamespace(merged=payload),
    )
    monkeypatch.setattr(routes, "save_settings", lambda settings, _path: settings)
    monkeypatch.setattr(routes, "_materialize_review_access_snapshot", lambda _container: {"configured": False})
    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None)

    response = routes.put_config(incoming, _make_request(container))

    assert response["ok"] is True
    assert response["restart_required"] is True


def test_put_config_marks_restart_required_when_shared_bridge_changes(monkeypatch) -> None:
    current = _make_config(role_mode="external", bridge_enabled=True, bridge_root_dir="D:/QJPT_Shared")
    incoming = _make_config(role_mode="external", bridge_enabled=True, bridge_root_dir="E:/QJPT_Shared")
    container = _make_container(current)

    monkeypatch.setattr(
        routes,
        "merge_user_config_payload",
        lambda payload, _current, clear_paths=None, force_overwrite=False: SimpleNamespace(merged=payload),
    )
    monkeypatch.setattr(routes, "save_settings", lambda settings, _path: settings)
    monkeypatch.setattr(routes, "_materialize_review_access_snapshot", lambda _container: {"configured": False})
    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None)

    response = routes.put_config(incoming, _make_request(container))

    assert response["ok"] is True
    assert response["restart_required"] is True


def test_put_config_keeps_restart_required_false_when_role_signature_unchanged(monkeypatch) -> None:
    current = _make_config(role_mode="", bridge_enabled=False, bridge_root_dir="")
    incoming = _make_config(role_mode="", bridge_enabled=False, bridge_root_dir="")
    container = _make_container(current)

    monkeypatch.setattr(
        routes,
        "merge_user_config_payload",
        lambda payload, _current, clear_paths=None, force_overwrite=False: SimpleNamespace(merged=payload),
    )
    monkeypatch.setattr(routes, "save_settings", lambda settings, _path: settings)
    monkeypatch.setattr(routes, "_materialize_review_access_snapshot", lambda _container: {"configured": False})
    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None)

    response = routes.put_config(incoming, _make_request(container))

    assert response["ok"] is True
    assert response["restart_required"] is False


def test_put_config_marks_restart_required_when_external_role_root_switches_to_role_specific_path(monkeypatch) -> None:
    current = _make_config(
        role_mode="external",
        bridge_enabled=True,
        bridge_root_dir="D:/legacy-shared",
        bridge_external_root_dir="D:/legacy-shared",
    )
    incoming = _make_config(
        role_mode="external",
        bridge_enabled=True,
        bridge_root_dir="D:/legacy-shared",
        bridge_external_root_dir="\\\\172.16.1.2\\share",
    )
    container = _make_container(current)

    monkeypatch.setattr(
        routes,
        "merge_user_config_payload",
        lambda payload, _current, clear_paths=None, force_overwrite=False: SimpleNamespace(merged=payload),
    )
    monkeypatch.setattr(routes, "save_settings", lambda settings, _path: settings)
    monkeypatch.setattr(routes, "_materialize_review_access_snapshot", lambda _container: {"configured": False})
    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None)

    response = routes.put_config(incoming, _make_request(container))

    assert response["ok"] is True
    assert response["restart_required"] is True
