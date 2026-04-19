from __future__ import annotations

import contextlib
from types import SimpleNamespace

from app.modules.updater.api import routes


class _FakeJobService:
    def __init__(self):
        self.guards = []

    @contextlib.contextmanager
    def resource_guard(self, *, name, resource_keys=None, timeout_sec=None):  # noqa: ANN001
        self.guards.append({"name": name, "resource_keys": list(resource_keys or []), "timeout_sec": timeout_sec})
        yield


class _FakeUpdaterService:
    def check_now(self):
        return {"last_result": "checked"}

    def apply_now(self, *, mode="normal", queue_if_busy=False):  # noqa: ANN001
        return {"last_result": "updated", "queue_status": "none", "mode": mode, "queued": queue_if_busy}

    def restart_now(self):
        return {"last_result": "updated_restart_scheduled"}

    def submit_internal_peer_command(self, *, action):  # noqa: ANN001
        return {
            "accepted": True,
            "already_pending": False,
            "action": action,
            "message": "已下发内网端检查更新命令，等待内网端执行。",
            "command": {
                "command_id": "cmd-1",
                "action": action,
                "status": "pending",
            },
        }


class _FakeContainer:
    def __init__(self, *, role_mode: str = "external"):
        self.job_service = _FakeJobService()
        self.runtime_config = {"updater": {"enabled": True}}
        self.config = {
            "common": {"deployment": {"role_mode": role_mode}},
            "deployment": {"role_mode": role_mode},
        }
        self._updater = _FakeUpdaterService()
        self.logs = []

    def ensure_updater_service(self):
        return self._updater

    def updater_snapshot(self):
        return {
            "running": False,
            "last_check_at": "",
            "last_result": "",
            "last_error": "",
            "local_version": "",
            "remote_version": "",
            "update_mode": "git_pull",
            "app_root_dir": r"D:\QJPT_V3",
            "persistent_user_data_dir": r"D:\QJPT_V3\user_data",
            "git_available": True,
            "git_repo_detected": True,
            "branch": "master",
            "local_commit": "1111111222222333333",
            "remote_commit": "9999999aaaaaaa",
            "worktree_dirty": False,
            "dirty_files": [],
            "source_kind": "git_remote",
            "source_label": "Git 仓库更新源",
            "local_release_revision": 0,
            "remote_release_revision": 0,
            "update_available": False,
            "force_apply_available": False,
            "restart_required": False,
            "dependency_sync_status": "idle",
            "dependency_sync_error": "",
            "dependency_sync_at": "",
            "queued_apply": {},
            "state_path": "",
            "mirror_ready": True,
            "mirror_version": "V3.61.20260328",
            "mirror_manifest_path": r"D:\QJPT_Shared\updater\approved\latest_patch.json",
            "last_publish_at": "2026-03-28 12:00:00",
            "last_publish_error": "",
            "internal_peer": {
                "available": True,
                "online": True,
                "update_available": False,
                "command": {
                    "active": False,
                },
            },
        }

    def deployment_snapshot(self):
        return {"role_mode": self.config["common"]["deployment"]["role_mode"]}

    def add_system_log(self, text):
        self.logs.append(text)


def _fake_request(*, role_mode: str = "external"):
    container = _FakeContainer(role_mode=role_mode)
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_updater_apply_route_uses_global_guard() -> None:
    request = _fake_request()

    payload = routes.updater_apply(request, {"mode": "normal", "queue_if_busy": True})

    assert payload["ok"] is True
    guard = request.app.state.container.job_service.guards[0]
    assert guard["resource_keys"] == ["updater:global"]


def test_updater_check_route_uses_global_guard() -> None:
    request = _fake_request()

    payload = routes.updater_check(request)

    assert payload["ok"] is True
    guard = request.app.state.container.job_service.guards[0]
    assert guard["resource_keys"] == ["updater:global"]


def test_updater_status_exposes_source_and_mirror_fields() -> None:
    request = _fake_request()

    payload = routes.updater_status(request)

    assert payload["ok"] is True
    runtime = payload["runtime"]
    assert runtime["source_kind"] == "git_remote"
    assert runtime["source_label"] == "Git 仓库更新源"
    assert runtime["update_mode"] == "git_pull"
    assert runtime["branch"] == "master"
    assert runtime["git_available"] is True
    assert runtime["mirror_ready"] is True
    assert runtime["mirror_version"] == "V3.61.20260328"
    assert runtime["internal_peer"]["available"] is True


def test_updater_status_prefers_runtime_disabled_state() -> None:
    request = _fake_request()
    request.app.state.container.updater_snapshot = lambda: {
        "enabled": False,
        "disabled_reason": "source_python_run",
        "last_result": "disabled",
    }

    payload = routes.updater_status(request)

    assert payload["ok"] is True
    assert payload["runtime"]["enabled"] is False
    assert payload["runtime"]["disabled_reason"] == "source_python_run"
    assert payload["runtime"]["last_result"] == "disabled"


def test_updater_internal_peer_check_route_uses_global_guard() -> None:
    request = _fake_request()

    payload = routes.updater_internal_peer_check(request)

    assert payload["ok"] is True
    assert payload["result"]["accepted"] is True
    guard = request.app.state.container.job_service.guards[0]
    assert guard["resource_keys"] == ["updater:global"]


def test_updater_internal_peer_check_forbidden_for_non_external_role() -> None:
    request = _fake_request(role_mode="internal")

    try:
        routes.updater_internal_peer_check(request)
    except Exception as exc:  # noqa: BLE001
        assert isinstance(exc, routes.HTTPException)
        assert exc.status_code == 403
        assert "仅外网端可下发内网更新命令" in str(exc.detail)
    else:
        raise AssertionError("expected HTTPException 403")


def test_updater_internal_peer_check_allows_v3_common_deployment_role() -> None:
    request = _fake_request(role_mode="external")
    request.app.state.container.config = {"common": {"deployment": {"role_mode": "external"}}}

    payload = routes.updater_internal_peer_check(request)

    assert payload["ok"] is True
    assert payload["result"]["accepted"] is True
