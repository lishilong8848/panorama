from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_STATE_PATH = PROJECT_ROOT / "web" / "frontend" / "src" / "app_state.js"
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "internal_download_pool_realtime_overview"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _run_node_script(work_dir: Path, content: str) -> dict:
    script_path = work_dir / "internal_download_pool_realtime_overview.mjs"
    script_path.write_text(content, encoding="utf-8")
    result = subprocess.run(
        ["node", str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_internal_download_pool_overview_always_exposes_all_five_buildings(work_dir: Path) -> None:
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
appState.health.deployment.role_mode = "internal";
appState.health.shared_bridge.internal_download_pool = {{
  enabled: true,
  browser_ready: true,
  active_buildings: [],
  page_slots: [
    {{
      building: "C楼",
      page_ready: true,
      in_use: false,
      login_state: "ready",
      last_result: "ready"
    }}
  ]
}};
appState.internalRuntimeSummary.value = {{
  pool: appState.health.shared_bridge.internal_download_pool,
  source_cache: {{}},
}};

const overview = appState.internalDownloadPoolOverview.value;
console.log(JSON.stringify({{
  buildings: overview.slots.map((slot) => slot.building),
  stateTexts: overview.slots.map((slot) => slot.stateText),
  loginTexts: overview.slots.map((slot) => slot.loginText)
}}));
"""

    payload = _run_node_script(work_dir, script)

    assert payload["buildings"] == ["A楼", "B楼", "C楼", "D楼", "E楼"]
    assert payload["stateTexts"][2] == "待命"
    assert payload["loginTexts"][2] == "已登录"
    assert payload["stateTexts"][0] == "未建页"
    assert payload["loginTexts"][0] == "待初始化"
