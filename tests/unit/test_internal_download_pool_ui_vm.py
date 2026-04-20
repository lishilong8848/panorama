from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_STATE_PATH = PROJECT_ROOT / "web" / "frontend" / "src" / "app_state.js"
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "internal_download_pool_ui_vm"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _run_node_script(work_dir: Path, content: str) -> dict:
    script_path = work_dir / "internal_download_pool_vm_test.mjs"
    script_path.write_text(content, encoding="utf-8")
    result = subprocess.run(
        ["node", str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_internal_download_pool_overview_simplifies_browser_errors(work_dir: Path) -> None:
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
appState.health.shared_bridge.enabled = true;
appState.health.shared_bridge.internal_download_pool = {{
  enabled: true,
  browser_ready: true,
  last_error: "Page.goto: net::ERR_EMPTY_RESPONSE at http://192.168.231.53/page/main/main.html",
  active_buildings: [],
  page_slots: [
    {{
      building: "A楼",
      page_ready: true,
      in_use: false,
      last_used_at: "",
      last_login_at: "",
      last_result: "failed",
      last_error: "Page.goto: net::ERR_EMPTY_RESPONSE at http://192.168.210.50/page/main/main.html",
      login_error: "Page.goto: net::ERR_EMPTY_RESPONSE at http://192.168.210.50/page/main/main.html",
      login_state: "failed"
    }}
  ]
}};
appState.internalRuntimeSummary.value = {{
  pool: appState.health.shared_bridge.internal_download_pool,
  source_cache: {{}},
}};

const overview = appState.internalDownloadPoolOverview.value;
const target = overview.slots.find((item) => item.building === "A楼");
console.log(JSON.stringify({{
  errorText: overview.errorText,
  slotLoginText: target.loginText,
  slotDetailText: target.detailText
}}));
"""

    payload = _run_node_script(work_dir, script)

    assert payload["errorText"] == "页面无响应，请检查楼栋页面服务或网络"
    assert payload["slotLoginText"] == "登录失败"
    assert payload["slotDetailText"] == "页面无响应，请检查楼栋页面服务或网络"


def test_internal_download_pool_overview_shows_suspended_recovery_state(work_dir: Path) -> None:
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
appState.health.shared_bridge.enabled = true;
appState.health.shared_bridge.internal_download_pool = {{
  enabled: true,
  browser_ready: true,
  last_error: "",
  active_buildings: [],
  page_slots: [
    {{
      building: "C楼",
      page_ready: true,
      in_use: false,
      last_used_at: "",
      last_login_at: "",
      last_result: "failed",
      last_error: "页面无响应，请检查楼栋页面服务或网络",
      login_error: "页面无响应，请检查楼栋页面服务或网络",
      login_state: "failed",
      suspended: true,
      suspend_reason: "C楼 页面无响应: 页面无响应，请检查楼栋页面服务或网络",
      failure_kind: "page_unreachable",
      recovery_attempts: 3,
      next_probe_at: "2026-03-31 23:00:00"
    }}
  ]
}};
appState.internalRuntimeSummary.value = {{
  pool: appState.health.shared_bridge.internal_download_pool,
  source_cache: {{}},
}};

const overview = appState.internalDownloadPoolOverview.value;
const target = overview.slots.find((item) => item.building === "C楼");
console.log(JSON.stringify({{
  slotStateText: target.stateText,
  slotLoginText: target.loginText,
  slotDetailText: target.detailText
}}));
"""

    payload = _run_node_script(work_dir, script)

    assert payload["slotStateText"] == "已暂停等待恢复"
    assert payload["slotLoginText"] == "页面异常"
    assert "下次自动检测：2026-03-31 23:00:00" in payload["slotDetailText"]
