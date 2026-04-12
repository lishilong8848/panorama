from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_STATE_PATH = PROJECT_ROOT / "web" / "frontend" / "src" / "app_state.js"
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "handover_review_board_ui_vm"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _run_node_script(work_dir: Path, content: str) -> dict:
    script_path = work_dir / "handover_review_board_ui_vm_test.mjs"
    script_path.write_text(content, encoding="utf-8")
    result = subprocess.run(
        ["node", str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_handover_review_board_rows_are_stable_with_partial_external_payload(work_dir: Path) -> None:
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
appState.health.handover.review_status.buildings = [
  {{
    building: "E楼",
    has_session: true,
    confirmed: false,
    cloud_sheet_sync: {{ status: "pending_upload" }},
  }},
  {{
    building: "A楼",
    has_session: true,
    confirmed: true,
    cloud_sheet_sync: {{ status: "success", spreadsheet_url: "https://cloud.example.com/a" }},
  }},
  {{
    building: "D楼",
    has_session: false,
    confirmed: false,
  }},
];
appState.health.handover.review_links = [
  {{ building: "C楼", code: "c", url: "http://192.168.0.3:18765/handover/review/c" }},
  {{ building: "A楼", code: "a", url: "http://192.168.0.1:18765/handover/review/a" }},
  {{ building: "F楼", code: "f", url: "http://192.168.0.6:18765/handover/review/f" }},
];

const rows = appState.handoverReviewBoardRows.value;
console.log(JSON.stringify({{
  buildings: rows.map((row) => row.building),
  texts: rows.map((row) => row.text),
  hasUrls: rows.map((row) => row.hasUrl),
  cloudSyncTexts: rows.map((row) => row.cloudSheetSyncText),
  firstUrl: rows[0]?.url || "",
  firstCloudUrl: rows[0]?.cloudSheetUrl || "",
  extraBuildingText: rows[5]?.text || "",
}}));
"""

    payload = _run_node_script(work_dir, script)

    assert payload["buildings"] == ["A楼", "B楼", "C楼", "D楼", "E楼", "F楼"]
    assert payload["texts"] == ["已确认", "未生成", "可访问", "未生成", "待确认", "可访问"]
    assert payload["hasUrls"] == [True, False, True, False, False, True]
    assert payload["cloudSyncTexts"] == [
        "云表已同步",
        "云表未执行",
        "云表未执行",
        "云表未执行",
        "云表待最终上传",
        "云表未执行",
    ]
    assert payload["firstUrl"] == "http://192.168.0.1:18765/handover/review/a"
    assert payload["firstCloudUrl"] == "https://cloud.example.com/a"
    assert payload["extraBuildingText"] == "可访问"
