from __future__ import annotations

import contextlib
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.modules.report_pipeline.api import routes


class _FakeJobService:
    def __init__(self, *, busy: bool = False) -> None:
        self.busy = busy
        self.guards = []

    @contextlib.contextmanager
    def resource_guard(self, *, name, resource_keys=None, timeout_sec=None):  # noqa: ANN001
        self.guards.append(
            {
                "name": name,
                "resource_keys": list(resource_keys or []),
                "timeout_sec": timeout_sec,
            }
        )
        yield

    def has_incomplete_jobs(self) -> bool:
        return self.busy


class _FakeContainer:
    def __init__(self, *, busy: bool = False, restart_ok: bool = True) -> None:
        self.job_service = _FakeJobService(busy=busy)
        self.logs = []
        self.restart_calls = []
        self._restart_ok = restart_ok

    def request_app_restart(self, context):
        self.restart_calls.append(dict(context or {}))
        if self._restart_ok:
            return True, "restart_scheduled"
        return False, "restart_failed"

    def add_system_log(self, text) -> None:
        self.logs.append(str(text))


def _make_request(container):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_restart_app_route_triggers_restart_and_uses_global_guard() -> None:
    container = _FakeContainer()

    payload = routes.restart_app(
        _make_request(container),
        {"source": "startup_role_picker", "reason": "role_switch"},
    )

    assert payload["ok"] is True
    assert payload["result"]["last_result"] == "restart_scheduled"
    assert container.restart_calls == [{"source": "startup_role_picker", "reason": "role_switch"}]
    assert container.job_service.guards[0]["resource_keys"] == ["updater:global"]
    assert any("source=startup_role_picker" in line for line in container.logs)


def test_restart_app_route_rejects_when_jobs_are_running() -> None:
    container = _FakeContainer(busy=True)

    with pytest.raises(HTTPException) as exc_info:
        routes.restart_app(_make_request(container), {"source": "startup_role_picker"})

    assert exc_info.value.status_code == 409
    assert "当前仍有任务在运行，请等待全部任务结束后再重启程序" == str(exc_info.value.detail)


def test_restart_app_route_raises_http_400_when_restart_callback_fails() -> None:
    container = _FakeContainer(restart_ok=False)

    with pytest.raises(HTTPException) as exc_info:
        routes.restart_app(_make_request(container), {"source": "startup_role_picker"})

    assert exc_info.value.status_code == 400
    assert "触发程序重启失败" in str(exc_info.value.detail)
