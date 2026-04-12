from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_STATE_PATH = PROJECT_ROOT / "web" / "frontend" / "src" / "app_state.js"
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "external_internal_alert_status_ui_vm"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _run_node_script(work_dir: Path, content: str) -> dict:
    script_path = work_dir / "external_internal_alert_status_vm_test.mjs"
    script_path.write_text(content, encoding="utf-8")
    result = subprocess.run(
        ["node", str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_external_internal_alert_overview_is_alarm_driven(work_dir: Path) -> None:
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
appState.health.deployment.role_mode = 'external';
appState.health.shared_bridge.internal_alert_status = {{
  buildings: [
    {{
      building: 'A楼',
      status: 'problem',
      summary: 'A楼 登录失败，等待内网恢复',
      detail: '页面无响应，请检查楼栋页面服务或网络',
      last_problem_at: '2026-04-01 09:10:00',
      last_recovered_at: '',
      active_count: 1,
    }},
    {{
      building: 'B楼',
      status: 'normal',
      summary: '正常',
      detail: '',
      last_problem_at: '',
      last_recovered_at: '2026-04-01 09:20:00',
      active_count: 0,
    }}
  ],
  active_count: 1,
  last_notified_at: '2026-04-01 09:21:00',
}};

const overview = appState.externalInternalAlertOverview.value;
console.log(JSON.stringify({{
  statusText: overview.statusText,
  summaryText: overview.summaryText,
  firstBuilding: overview.buildings[0],
  secondBuilding: overview.buildings[1],
  fifthBuilding: overview.buildings[4],
}}));
"""
    payload = _run_node_script(work_dir, script)

    assert payload['statusText'] == '存在异常楼栋'
    assert payload['summaryText'] == '当前有 1 个楼栋存在未恢复的内网告警。'
    assert payload['firstBuilding']['building'] == 'A楼'
    assert payload['firstBuilding']['statusText'] == '异常'
    assert payload['firstBuilding']['summaryText'] == 'A楼 登录失败，等待内网恢复'
    assert payload['secondBuilding']['building'] == 'B楼'
    assert payload['secondBuilding']['statusText'] == '正常'
    assert payload['secondBuilding']['summaryText'] == '已恢复正常'
    assert payload['fifthBuilding']['building'] == 'E楼'
    assert payload['fifthBuilding']['statusText'] == '正常'
