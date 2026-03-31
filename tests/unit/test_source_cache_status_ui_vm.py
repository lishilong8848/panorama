from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_STATE_PATH = PROJECT_ROOT / "web" / "frontend" / "src" / "app_state.js"
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "source_cache_status_ui_vm"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _run_node_script(work_dir: Path, content: str) -> dict:
    script_path = work_dir / "source_cache_vm_test.mjs"
    script_path.write_text(content, encoding="utf-8")
    result = subprocess.run(
        ["node", str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_source_cache_status_view_models_show_latest_selection_and_fallback(work_dir: Path) -> None:
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

appState.health.deployment.role_mode = "external";
appState.health.shared_bridge.internal_source_cache = {{
  enabled: true,
  scheduler_running: true,
  current_hour_bucket: "2026-03-31 09",
  last_error: "",
  handover_log_family: {{
    latest_selection: {{
      best_bucket_key: "2026-03-31 09",
      fallback_buildings: ["B楼"],
      missing_buildings: [],
      stale_buildings: [],
      can_proceed: true,
      buildings: [
        {{
          building: "A楼",
          bucket_key: "2026-03-31 09",
          status: "ready",
          using_fallback: false,
          version_gap: 0,
          downloaded_at: "2026-03-31 09:01:00",
          resolved_file_path: "\\\\\\\\172.16.1.2\\\\share\\\\交接班日志源文件\\\\202603\\\\20260331--09\\\\A楼.xlsx"
        }},
        {{
          building: "B楼",
          bucket_key: "2026-03-31 08",
          status: "ready",
          using_fallback: true,
          version_gap: 1,
          downloaded_at: "2026-03-31 08:58:00",
          resolved_file_path: "\\\\\\\\172.16.1.2\\\\share\\\\交接班日志源文件\\\\202603\\\\20260331--08\\\\B楼.xlsx"
        }}
      ]
    }}
  }},
  monthly_report_family: {{
    latest_selection: {{
      best_bucket_key: "2026-03-31 09",
      fallback_buildings: [],
      missing_buildings: [],
      stale_buildings: ["E楼"],
      can_proceed: false,
      buildings: [
        {{
          building: "A楼",
          bucket_key: "2026-03-31 09",
          status: "ready",
          using_fallback: false,
          version_gap: 0,
          downloaded_at: "2026-03-31 09:02:00",
          resolved_file_path: "\\\\\\\\172.16.1.2\\\\share\\\\全景平台月报源文件\\\\202603\\\\20260331--09\\\\A楼.xlsx"
        }},
        {{
          building: "E楼",
          bucket_key: "2026-03-31 05",
          status: "stale",
          using_fallback: false,
          version_gap: 4,
          downloaded_at: "2026-03-31 05:05:00",
          resolved_file_path: "\\\\\\\\172.16.1.2\\\\share\\\\全景平台月报源文件\\\\202603\\\\20260331--05\\\\E楼.xlsx"
        }}
      ]
    }}
  }}
}};

const externalOverview = appState.sharedSourceCacheReadinessOverview.value;

appState.health.deployment.role_mode = "";
const emptyRoleOverview = appState.sharedSourceCacheReadinessOverview.value;

console.log(JSON.stringify({{
  external: {{
    statusText: externalOverview.statusText,
    summaryText: externalOverview.summaryText,
    referenceBucketKey: externalOverview.referenceBucketKey,
    canProceedLatest: externalOverview.canProceedLatest,
    firstFamilyStatus: externalOverview.families[0].statusText,
    firstFamilyFallbackState: externalOverview.families[0].buildings[1].stateText,
    firstFamilyFallbackGap: externalOverview.families[0].buildings[1].versionGap,
    firstFamilyFallbackPath: externalOverview.families[0].buildings[1].resolvedFilePath,
    secondFamilyStatus: externalOverview.families[1].statusText,
    secondFamilyStaleState: externalOverview.families[1].buildings[1].stateText,
    secondFamilyStaleGap: externalOverview.families[1].buildings[1].versionGap
  }},
  emptyRole: {{
    statusText: emptyRoleOverview.statusText,
    familyCount: emptyRoleOverview.families.length
  }}
}}));
"""

    payload = _run_node_script(work_dir, script)

    assert payload["external"]["statusText"] == "等待共享文件就绪"
    assert payload["external"]["summaryText"] == "部分楼栋共享文件版本过旧，等待更新后会自动重试默认入口。"
    assert payload["external"]["referenceBucketKey"] == "2026-03-31 09"
    assert payload["external"]["canProceedLatest"] is False
    assert payload["external"]["firstFamilyStatus"] == "已允许回退"
    assert payload["external"]["firstFamilyFallbackState"] == "使用上一版共享文件"
    assert payload["external"]["firstFamilyFallbackGap"] == 1
    assert payload["external"]["firstFamilyFallbackPath"].startswith("\\\\172.16.1.2\\share\\")
    assert payload["external"]["secondFamilyStatus"] == "存在过旧楼栋"
    assert payload["external"]["secondFamilyStaleState"] == "版本过旧，等待更新"
    assert payload["external"]["secondFamilyStaleGap"] == 4
    assert payload["emptyRole"]["statusText"] == "当前角色未使用共享缓存"
    assert payload["emptyRole"]["familyCount"] == 0
