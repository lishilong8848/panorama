from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_STATE_PATH = PROJECT_ROOT / "web" / "frontend" / "src" / "app_state.js"
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "app_state_can_run"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _run_node_script(work_dir: Path, content: str) -> dict:
    script_path = work_dir / "app_state_can_run_test.mjs"
    script_path.write_text(content, encoding="utf-8")
    result = subprocess.run(
        ["node", str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_can_run_stays_true_while_updater_overview_is_pending_backend(work_dir: Path) -> None:
    script = f"""
import {{ createAppState }} from {json.dumps(APP_STATE_PATH.as_uri())};

const reactive = (value) => value;
const ref = (value) => ({{ value }});
const computed = (getter) => ({{
  get value() {{
    return getter();
  }}
}});

const appState = createAppState({{ reactive, ref, computed }});

const before = appState.canRun.value;
appState.health.updater.display_overview = {{
  tone: "neutral",
  status_text: "等待后端更新状态",
  business_actions: {{
    allowed: false,
    reason_code: "pending_backend",
    disabled_reason: "等待后端更新状态。",
    status_text: "等待后端更新状态",
  }},
}};
const pendingBackend = appState.canRun.value;
appState.health.updater.display_overview = {{
  tone: "warning",
  status_text: "等待重启生效",
  business_actions: {{
    allowed: false,
    reason_code: "restart_required",
    disabled_reason: "更新已完成，需先重启生效。",
    status_text: "等待重启生效",
  }},
}};
const restartRequired = appState.canRun.value;

console.log(JSON.stringify({{
  before,
  pendingBackend,
  restartRequired,
}}));
"""

    payload = _run_node_script(work_dir, script)

    assert payload["before"] is True
    assert payload["pendingBackend"] is True
    assert payload["restartRequired"] is False
