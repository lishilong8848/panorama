from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_STATE_PATH = PROJECT_ROOT / "web" / "frontend" / "src" / "app_state.js"
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "alarm_event_readiness_ui_vm"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _run_node_script(work_dir: Path, content: str) -> dict:
    script_path = work_dir / "alarm_event_readiness_ui_vm_test.mjs"
    script_path.write_text(content, encoding="utf-8")
    result = subprocess.run(
        ["node", str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_external_alarm_family_readiness_prefers_today_latest_else_yesterday_fallback(work_dir: Path) -> None:
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
  handover_log_family: {{
    latest_selection: {{
      best_bucket_key: "2026-04-03 09",
      best_bucket_age_hours: 0.5,
      is_best_bucket_too_old: false,
      can_proceed: true,
      fallback_buildings: [],
      missing_buildings: [],
      stale_buildings: [],
      buildings: [
        {{ building: "A楼", status: "ready", bucket_key: "2026-04-03 09", using_fallback: false, version_gap: 0, downloaded_at: "2026-04-03 09:05:00" }},
        {{ building: "B楼", status: "ready", bucket_key: "2026-04-03 09", using_fallback: false, version_gap: 0, downloaded_at: "2026-04-03 09:05:00" }},
        {{ building: "C楼", status: "ready", bucket_key: "2026-04-03 09", using_fallback: false, version_gap: 0, downloaded_at: "2026-04-03 09:05:00" }},
        {{ building: "D楼", status: "ready", bucket_key: "2026-04-03 09", using_fallback: false, version_gap: 0, downloaded_at: "2026-04-03 09:05:00" }},
        {{ building: "E楼", status: "ready", bucket_key: "2026-04-03 09", using_fallback: false, version_gap: 0, downloaded_at: "2026-04-03 09:05:00" }},
      ],
    }},
  }},
  monthly_report_family: {{
    latest_selection: {{
      best_bucket_key: "2026-04-03 09",
      best_bucket_age_hours: 0.5,
      is_best_bucket_too_old: false,
      can_proceed: true,
      fallback_buildings: [],
      missing_buildings: [],
      stale_buildings: [],
      buildings: [
        {{ building: "A楼", status: "ready", bucket_key: "2026-04-03 09", using_fallback: false, version_gap: 0, downloaded_at: "2026-04-03 09:05:00" }},
        {{ building: "B楼", status: "ready", bucket_key: "2026-04-03 09", using_fallback: false, version_gap: 0, downloaded_at: "2026-04-03 09:05:00" }},
        {{ building: "C楼", status: "ready", bucket_key: "2026-04-03 09", using_fallback: false, version_gap: 0, downloaded_at: "2026-04-03 09:05:00" }},
        {{ building: "D楼", status: "ready", bucket_key: "2026-04-03 09", using_fallback: false, version_gap: 0, downloaded_at: "2026-04-03 09:05:00" }},
        {{ building: "E楼", status: "ready", bucket_key: "2026-04-03 09", using_fallback: false, version_gap: 0, downloaded_at: "2026-04-03 09:05:00" }},
      ],
    }},
  }},
  alarm_event_family: {{
    selection_policy: "today_latest_else_yesterday_fallback",
    selection_reference_date: "2026-04-03",
    used_previous_day_fallback: ["B楼"],
    missing_today_buildings: ["B楼", "D楼"],
    missing_both_days_buildings: ["D楼"],
    buildings: [
      {{ building: "A楼", status: "ready", bucket_key: "2026-04-03 09:30:00", downloaded_at: "2026-04-03 09:30:00", selected_downloaded_at: "2026-04-03 09:30:00", source_kind: "manual", selection_scope: "today", relative_path: "alarm/A.json", resolved_file_path: "C:/share/alarm/A.json" }},
      {{ building: "B楼", status: "ready", bucket_key: "2026-04-02 16", downloaded_at: "2026-04-02 16:05:00", selected_downloaded_at: "2026-04-02 16:05:00", source_kind: "latest", selection_scope: "yesterday_fallback", relative_path: "alarm/B.json", resolved_file_path: "C:/share/alarm/B.json" }},
      {{ building: "C楼", status: "consumed", bucket_key: "2026-04-03 09", downloaded_at: "2026-04-03 09:10:00", selected_downloaded_at: "2026-04-03 09:10:00", source_kind: "latest", selection_scope: "today", relative_path: "alarm/C.json" }},
      {{ building: "D楼", status: "waiting", bucket_key: "", downloaded_at: "", selected_downloaded_at: "", source_kind: "", selection_scope: "missing", relative_path: "" }},
      {{ building: "E楼", status: "ready", bucket_key: "2026-04-03 07", downloaded_at: "2026-04-03 07:00:00", selected_downloaded_at: "2026-04-03 07:00:00", source_kind: "latest", selection_scope: "today", relative_path: "alarm/E.json", resolved_file_path: "C:/share/alarm/E.json" }},
    ],
    external_upload: {{
      running: false,
      started_at: "",
      current_mode: "",
      current_scope: "",
      last_run_at: "2026-04-03 10:00:00",
      last_success_at: "2026-04-03 10:00:00",
      last_error: "",
      uploaded_record_count: 88,
      uploaded_file_count: 3,
      consumed_count: 3,
    }},
  }},
}};
const sourceCache = appState.health.shared_bridge.internal_source_cache;
appState.health.dashboard_display.shared_source_cache_overview = {{
  reason_code: "ready",
  tone: "success",
  status_text: "共享文件已就绪",
  summary_text: "共享文件已就绪，默认入口可继续执行。",
  reference_bucket_key: "2026-04-03 09",
  can_proceed_latest: true,
  families: [
    {{ key: "handover_log_family", ...sourceCache.handover_log_family.latest_selection }},
    {{ key: "monthly_report_family", ...sourceCache.monthly_report_family.latest_selection }},
    {{ key: "alarm_event_family", ...sourceCache.alarm_event_family }},
  ],
}};

const overview = appState.sharedSourceCacheReadinessOverview.value;
const alarmFamily = overview.families[2];
console.log(JSON.stringify({{
  familyTitles: overview.families.map((item) => item.title),
  canProceedLatest: overview.canProceedLatest,
  referenceBucketKey: overview.referenceBucketKey,
  alarmStatusText: alarmFamily.statusText,
  alarmSummaryText: alarmFamily.summaryText,
  selectionReferenceDate: alarmFamily.selectionReferenceDate,
  fallbackBuildings: alarmFamily.usedPreviousDayFallback,
  missingBothDaysBuildings: alarmFamily.missingBothDaysBuildings,
  alarmAState: alarmFamily.buildings[0].stateText,
  alarmASource: alarmFamily.buildings[0].sourceKindText,
  alarmASelection: alarmFamily.buildings[0].selectionScopeText,
  alarmBSelection: alarmFamily.buildings[1].selectionScopeText,
  alarmCState: alarmFamily.buildings[2].stateText,
  alarmDState: alarmFamily.buildings[3].stateText,
}}));
"""

    payload = _run_node_script(work_dir, script)

    assert payload["familyTitles"] == ["交接班日志源文件", "全景平台月报源文件", "告警信息源文件"]
    assert payload["canProceedLatest"] is True
    assert payload["referenceBucketKey"] == "2026-04-03 09"
    assert payload["alarmStatusText"] == "存在缺失楼栋"
    assert "当天最新一份" in payload["alarmSummaryText"]
    assert payload["selectionReferenceDate"] == "2026-04-03"
    assert payload["fallbackBuildings"] == ["B楼"]
    assert payload["missingBothDaysBuildings"] == ["D楼"]
    assert payload["alarmAState"] == "已就绪"
    assert payload["alarmASource"] == "手动"
    assert payload["alarmASelection"] == "今天最新"
    assert payload["alarmBSelection"] == "昨天回退"
    assert payload["alarmCState"] == "已消费"
    assert payload["alarmDState"] == "今天和昨天都缺文件"
