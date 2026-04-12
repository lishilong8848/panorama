from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_STATE_PATH = PROJECT_ROOT / "web" / "frontend" / "src" / "app_state.js"
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "internal_status_realtime_file_overview"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _run_node_script(work_dir: Path, content: str) -> dict:
    script_path = work_dir / "internal_status_realtime_file_overview.mjs"
    script_path.write_text(content, encoding="utf-8")
    result = subprocess.run(
        ["node", str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_internal_realtime_source_families_always_expose_all_five_buildings(work_dir: Path) -> None:
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
appState.health.shared_bridge.internal_source_cache = {{
  enabled: true,
  scheduler_running: true,
  current_hour_bucket: "2026-04-01 09",
  handover_log_family: {{
    current_bucket: "2026-04-01 09",
    last_success_at: "2026-04-01 09:10:00",
    buildings: [
      {{
        building: "C楼",
        bucket_key: "2026-04-01 09",
        status: "ready",
        ready: true,
        downloaded_at: "2026-04-01 09:10:00",
        relative_path: "交接班日志源文件/202604/20260401--09/C楼.xlsx",
      }},
    ],
  }},
  monthly_report_family: {{
    current_bucket: "2026-04-01 09",
    last_success_at: "2026-04-01 09:11:00",
    buildings: [
      {{
        building: "A楼",
        bucket_key: "2026-04-01 09",
        status: "ready",
        ready: true,
        downloaded_at: "2026-04-01 09:11:00",
        relative_path: "全景平台月报源文件/202604/20260401--09/A楼.xlsx",
      }},
    ],
  }},
  alarm_event_family: {{
    current_bucket: "2026-04-01 08",
    last_success_at: "2026-04-01 08:05:00",
    buildings: [
      {{
        building: "E楼",
        bucket_key: "2026-04-01 08",
        status: "ready",
        ready: true,
        downloaded_at: "2026-04-01 08:05:00",
        relative_path: "告警信息源文件/202604/20260401--08/E楼.json",
      }},
    ],
  }},
}};

const families = appState.internalRealtimeSourceFamilies.value;
console.log(JSON.stringify({{
  familyTitles: families.map((item) => item.title),
  handoverBuildings: families[0].buildings.map((item) => item.building),
  monthlyBuildings: families[1].buildings.map((item) => item.building),
  alarmBuildings: families[2].buildings.map((item) => item.building),
  handoverStatusC: families[0].buildings[2].stateText,
  handoverStatusA: families[0].buildings[0].stateText,
  monthlyStatusA: families[1].buildings[0].stateText,
  alarmStatusE: families[2].buildings[4].stateText,
  alarmStatusA: families[2].buildings[0].stateText,
}}));
"""

    payload = _run_node_script(work_dir, script)

    assert payload["familyTitles"] == ["交接班日志源文件", "全景平台月报源文件", "告警信息源文件"]
    assert payload["handoverBuildings"] == ["A楼", "B楼", "C楼", "D楼", "E楼"]
    assert payload["monthlyBuildings"] == ["A楼", "B楼", "C楼", "D楼", "E楼"]
    assert payload["alarmBuildings"] == ["A楼", "B楼", "C楼", "D楼", "E楼"]
    assert payload["handoverStatusC"] == "已就绪"
    assert payload["handoverStatusA"] == "等待中"
    assert payload["monthlyStatusA"] == "已就绪"
    assert payload["alarmStatusE"] == "已就绪"
    assert payload["alarmStatusA"] == "等待中"
