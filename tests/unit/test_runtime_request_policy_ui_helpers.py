from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
HELPER_PATH = PROJECT_ROOT / "web" / "frontend" / "src" / "runtime_request_policy_ui_helpers.js"
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "runtime_request_policy_ui_helpers"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _run_node_script(work_dir: Path, content: str) -> dict:
    script_path = work_dir / "runtime_request_policy_test.mjs"
    script_path.write_text(content, encoding="utf-8")
    result = subprocess.run(
        ["node", str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_external_summary_polls_status_page_after_backend_ready_even_if_loading_visible(work_dir: Path) -> None:
    script = f"""
import {{ createRuntimeRequestPolicyUiHelpers }} from {json.dumps(HELPER_PATH.as_uri())};

const ref = (value) => ({{ value }});
const computed = (getter) => ({{
  get value() {{
    return getter();
  }}
}});

const health = {{
  runtime_activated: true,
  startup_role_confirmed: true,
  role_selection_required: false,
  startup_role_user_exited: false,
  activation_phase: "activating",
  deployment: {{ role_mode: "external" }},
}};

const helpers = createRuntimeRequestPolicyUiHelpers({{
  computed,
  startupRoleSelectorHandled: ref(true),
  updaterUiOverlayVisible: ref(false),
  updaterAwaitingRestartRecovery: ref(false),
  startupRoleSelectorVisible: ref(false),
  startupRoleLoadingVisible: ref(true),
  startupRoleActivationInFlight: ref(true),
  bootstrapReady: ref(true),
  health,
  currentView: ref("status"),
  deploymentRoleMode: ref("external"),
  fullHealthLoaded: ref(true),
  bridgeTasksEnabled: ref(false),
  dashboardActiveModule: ref("auto_flow"),
  activeConfigTab: ref(""),
}});

console.log(JSON.stringify({{
  paused: helpers.shouldPauseRuntimeRequests.value,
  runtimeReady: helpers.runtimeRequestsReady.value,
  externalSummary: helpers.shouldPollExternalDashboardSummary.value,
  internalStatus: helpers.shouldPollInternalRuntimeStatus.value,
}}));
"""

    payload = _run_node_script(work_dir, script)

    assert payload["paused"] is False
    assert payload["runtimeReady"] is True
    assert payload["externalSummary"] is True
    assert payload["internalStatus"] is False
