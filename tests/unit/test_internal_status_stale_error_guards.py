from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_STATE_PATH = PROJECT_ROOT / "web" / "frontend" / "src" / "app_state.js"
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "internal_status_stale_error_guards"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _run_node_script(work_dir: Path, content: str) -> dict:
    script_path = work_dir / "internal_status_stale_error_guards.mjs"
    script_path.write_text(content, encoding="utf-8")
    result = subprocess.run(
        ["node", str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_internal_status_view_models_ignore_stale_last_error_after_success(work_dir: Path) -> None:
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
  last_error: "Page.wait_for_selector: Timeout 20000ms exceeded.",
  active_buildings: [],
  page_slots: [
    {{ building: "A楼", login_state: "ready", page_ready: true, in_use: false }},
    {{ building: "B楼", login_state: "ready", page_ready: true, in_use: false }},
    {{ building: "C楼", login_state: "ready", page_ready: true, in_use: false }},
    {{ building: "D楼", login_state: "ready", page_ready: true, in_use: false }},
    {{ building: "E楼", login_state: "ready", page_ready: true, in_use: false }},
  ],
}};
appState.health.shared_bridge.internal_source_cache = {{
  enabled: true,
  scheduler_running: true,
  current_hour_bucket: "2026-04-05 11",
  last_run_at: "2026-04-05 11:56:00",
  last_success_at: "2026-04-05 11:58:00",
  last_error: "旧错误",
  current_hour_refresh: {{
    running: false,
    last_run_at: "2026-04-05 11:56:00",
    last_success_at: "2026-04-05 11:58:00",
    last_error: "旧错误",
    failed_buildings: [],
    blocked_buildings: [],
    running_buildings: [],
    completed_buildings: ["A楼", "B楼", "C楼", "D楼", "E楼"],
  }},
  handover_log_family: {{
    current_bucket: "2026-04-05 11",
    last_success_at: "2026-04-05 11:58:00",
    failed_buildings: [],
    blocked_buildings: [],
    buildings: [
      {{ building: "A楼", status: "ready", ready: true }},
      {{ building: "B楼", status: "ready", ready: true }},
      {{ building: "C楼", status: "ready", ready: true }},
      {{ building: "D楼", status: "ready", ready: true }},
      {{ building: "E楼", status: "ready", ready: true }},
    ],
  }},
  monthly_report_family: {{
    current_bucket: "2026-04-05 11",
    last_success_at: "2026-04-05 11:58:00",
    failed_buildings: [],
    blocked_buildings: [],
    buildings: [
      {{ building: "A楼", status: "ready", ready: true }},
      {{ building: "B楼", status: "ready", ready: true }},
      {{ building: "C楼", status: "ready", ready: true }},
      {{ building: "D楼", status: "ready", ready: true }},
      {{ building: "E楼", status: "ready", ready: true }},
    ],
  }},
  alarm_event_family: {{
    current_bucket: "2026-04-05 08",
    last_success_at: "2026-04-05 08:05:00",
    failed_buildings: [],
    blocked_buildings: [],
    buildings: [
      {{ building: "A楼", status: "ready", ready: true }},
      {{ building: "B楼", status: "ready", ready: true }},
      {{ building: "C楼", status: "ready", ready: true }},
      {{ building: "D楼", status: "ready", ready: true }},
      {{ building: "E楼", status: "ready", ready: true }},
    ],
  }},
}};

console.log(JSON.stringify({{
  sourceCacheTone: appState.internalSourceCacheOverview.value.tone,
  sourceCacheStatus: appState.internalSourceCacheOverview.value.statusText,
  currentHourTone: appState.currentHourRefreshOverview.value.tone,
  currentHourStatus: appState.currentHourRefreshOverview.value.statusText,
  currentHourLastError: appState.currentHourRefreshOverview.value.lastError,
}}));
"""

    payload = _run_node_script(work_dir, script)

    assert payload["sourceCacheTone"] == "success"
    assert payload["sourceCacheStatus"] == "本轮共享文件已全部就绪"
    assert payload["currentHourTone"] == "success"
    assert payload["currentHourStatus"] == "最近一轮已完成"
    assert payload["currentHourLastError"] == ""

