from pathlib import Path
import sys
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.modules.report_pipeline.api import routes


class _FakeOrchestrator:
    def __init__(self, _config):
        pass

    def list_pending_resume_runs(self):
        return [{"run_id": "resume-1"}]


def _build_request(
    *,
    role_mode: str,
    node_id: str,
    node_label: str,
    startup_role_confirmed: bool,
    last_started_role_mode: str = "",
    activation_phase: str = "idle",
    startup_handoff: dict | None = None,
):
    container = SimpleNamespace(
        version="web-3.0.0",
        frontend_mode="source",
        runtime_config={"common": {"console": {"port": 18765}}},
        deployment_snapshot=lambda: {
            "role_mode": role_mode,
            "last_started_role_mode": last_started_role_mode,
            "node_id": node_id,
            "node_label": node_label,
        },
        job_service=SimpleNamespace(
            active_job_id=lambda: "job-123" if role_mode else "",
            active_job_ids=lambda include_waiting=True: ["job-123"] if role_mode else [],
            job_counts=lambda: {"queued": 0, "running": 1 if role_mode else 0, "finished": 0, "failed": 0},
        ),
        system_log_next_offset=lambda: 42,
        get_startup_role_handoff=lambda: dict(startup_handoff or {}),
    )
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                started_at="2026-03-30 12:00:00",
                runtime_services_activated=False,
                runtime_activation_phase=activation_phase,
                runtime_activation_error="",
                startup_role_confirmed=startup_role_confirmed,
            )
        )
    )


def test_health_bootstrap_requires_role_confirmation_for_each_new_process(monkeypatch):
    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestrator)
    request = _build_request(
        role_mode="internal",
        last_started_role_mode="internal",
        node_id="internal-node",
        node_label="内网端",
        startup_role_confirmed=False,
    )

    payload = routes.health_bootstrap(request)

    assert payload["deployment"]["role_mode"] == "internal"
    assert payload["deployment"]["node_label"] == "内网端"
    assert payload["startup_role_confirmed"] is False
    assert payload["role_selection_required"] is True
    assert payload["startup_handoff"]["active"] is False
    assert payload["runtime_activated"] is False
    assert payload["activation_phase"] == "idle"
    assert payload["activation_error"] == ""


def test_health_bootstrap_skips_selector_after_current_process_is_confirmed(monkeypatch):
    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestrator)
    request = _build_request(
        role_mode="external",
        last_started_role_mode="external",
        node_id="external-node",
        node_label="外网端",
        startup_role_confirmed=True,
    )

    payload = routes.health_bootstrap(request)

    assert payload["deployment"]["role_mode"] == "external"
    assert payload["deployment"]["node_label"] == "外网端"
    assert payload["startup_role_confirmed"] is True
    assert payload["role_selection_required"] is False


def test_health_bootstrap_requires_selector_after_auto_start_failure(monkeypatch):
    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestrator)
    request = _build_request(
        role_mode="external",
        last_started_role_mode="external",
        node_id="external-node",
        node_label="外网端",
        startup_role_confirmed=False,
        activation_phase="failed",
    )

    payload = routes.health_bootstrap(request)

    assert payload["startup_role_confirmed"] is False
    assert payload["role_selection_required"] is True


def test_health_bootstrap_exposes_active_startup_handoff(monkeypatch):
    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestrator)
    request = _build_request(
        role_mode="external",
        last_started_role_mode="internal",
        node_id="external-node",
        node_label="外网端",
        startup_role_confirmed=False,
        startup_handoff={
            "active": True,
            "mode": "startup_role_resume",
            "target_role_mode": "external",
            "requested_at": "2026-04-03 08:30:00",
            "reason": "role_mode_switch",
            "nonce": "handoff-123",
        },
    )

    payload = routes.health_bootstrap(request)

    assert payload["startup_handoff"]["active"] is True
    assert payload["startup_handoff"]["mode"] == "startup_role_resume"
    assert payload["startup_handoff"]["target_role_mode"] == "external"
    assert payload["startup_handoff"]["requested_at"] == "2026-04-03 08:30:00"
    assert payload["startup_handoff"]["reason"] == "role_mode_switch"
    assert payload["startup_handoff"]["nonce"] == "handoff-123"
    assert payload["activation_phase"] == "idle"


def test_health_bootstrap_sanitizes_legacy_role(monkeypatch):
    monkeypatch.setattr(routes, "OrchestratorService", _FakeOrchestrator)
    request = _build_request(
        role_mode="switching",
        node_id="legacy-node",
        node_label="旧角色",
        startup_role_confirmed=False,
    )

    payload = routes.health_bootstrap(request)

    assert payload["deployment"]["role_mode"] == ""
    assert payload["deployment"]["node_id"] == "legacy-node"
    assert payload["deployment"]["node_label"] == "旧角色"
    assert payload["startup_role_confirmed"] is False
    assert payload["role_selection_required"] is True
