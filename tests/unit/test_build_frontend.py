from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_build_frontend_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "scripts" / "build_frontend.py"
    spec = importlib.util.spec_from_file_location("build_frontend_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_build_frontend_assets_copies_all_src_js_and_css(tmp_path: Path) -> None:
    module = _load_build_frontend_module()
    project_dir = tmp_path / "project"
    src_dir = project_dir / "web" / "frontend" / "src"
    dist_assets = project_dir / "web" / "frontend" / "dist" / "assets"
    legacy_assets = project_dir / "web_frontend" / "dist" / "assets"

    src_dir.mkdir(parents=True, exist_ok=True)
    legacy_assets.mkdir(parents=True, exist_ok=True)

    (src_dir / "index.html").write_text("<!doctype html>", encoding="utf-8")
    (src_dir / "app.js").write_text("import './extra.js';", encoding="utf-8")
    (src_dir / "extra.js").write_text("export const ok = true;", encoding="utf-8")
    (src_dir / "dashboard_wet_bulb_collection_actions.js").write_text("export const wet = true;", encoding="utf-8")
    (src_dir / "dashboard_template_sections").mkdir(parents=True, exist_ok=True)
    (src_dir / "dashboard_template_sections" / "dashboard_handover_log_section.js").write_text(
        "export const tpl = true;",
        encoding="utf-8",
    )
    (src_dir / "style.css").write_text("body { color: red; }", encoding="utf-8")
    (src_dir / "vue.global.prod.js").write_text("window.Vue = {};", encoding="utf-8")

    module.build_frontend_assets(project_dir)

    assert (dist_assets / "app.js").exists()
    assert (dist_assets / "extra.js").exists()
    assert (dist_assets / "dashboard_wet_bulb_collection_actions.js").exists()
    assert (dist_assets / "dashboard_template_sections" / "dashboard_handover_log_section.js").exists()
    assert (dist_assets / "style.css").exists()
    assert (dist_assets / "vue.global.prod.js").exists()
    assert (legacy_assets / "dashboard_wet_bulb_collection_actions.js").exists()
    assert (legacy_assets / "dashboard_template_sections" / "dashboard_handover_log_section.js").exists()


def test_scheduler_ui_falls_back_to_raw_runtime_times() -> None:
    project_root = Path(__file__).resolve().parents[2]
    source = (project_root / "web" / "frontend" / "src" / "scheduler_ui_helpers.js").read_text(encoding="utf-8")

    assert "next_run_text: \"next_run_time\"" in source
    assert "last_trigger_text: \"last_trigger_at\"" in source


def test_auto_flow_scheduler_card_shows_next_run_time() -> None:
    project_root = Path(__file__).resolve().parents[2]
    source = (
        project_root
        / "web"
        / "frontend"
        / "src"
        / "dashboard_template_sections"
        / "dashboard_auto_flow_section.js"
    ).read_text(encoding="utf-8")

    assert "下次执行" in source
    assert "getSchedulerDisplayText('scheduler', 'next_run_text', '-')" in source


def test_wet_bulb_scheduler_action_merges_display_snapshot() -> None:
    project_root = Path(__file__).resolve().parents[2]
    source = (
        project_root / "web" / "frontend" / "src" / "dashboard_wet_bulb_collection_actions.js"
    ).read_text(encoding="utf-8")

    assert "display: data.display && typeof data.display === \"object\"" in source


def test_handover_scheduler_time_inputs_auto_save_on_change() -> None:
    project_root = Path(__file__).resolve().parents[2]
    handover_section = (
        project_root
        / "web"
        / "frontend"
        / "src"
        / "dashboard_template_sections"
        / "dashboard_handover_log_section.js"
    ).read_text(encoding="utf-8")

    assert 'type="time"' in handover_section
    assert 'step="1"' in handover_section
    assert 'v-model="config.handover_log.scheduler.morning_time"' in handover_section
    assert 'v-model="config.handover_log.scheduler.afternoon_time"' in handover_section
    assert 'type="text" inputmode="numeric" placeholder="HH:MM:SS"' not in handover_section
    assert 'saveHandoverSchedulerQuickConfig({ morning_time: $event.target.value })' in handover_section
    assert 'saveHandoverSchedulerQuickConfig({ afternoon_time: $event.target.value })' in handover_section
    assert '@click="saveHandoverSchedulerQuickConfig()"' not in handover_section
    assert "保存时间" not in handover_section
    assert "修改后立即生效" in handover_section


def test_scheduler_time_inputs_pass_current_dom_value_to_quick_save() -> None:
    project_root = Path(__file__).resolve().parents[2]
    alarm_section = (
        project_root
        / "web"
        / "frontend"
        / "src"
        / "dashboard_template_sections"
        / "dashboard_alarm_event_upload_section.js"
    ).read_text(encoding="utf-8")
    monthly_section = (
        project_root
        / "web"
        / "frontend"
        / "src"
        / "dashboard_template_sections"
        / "dashboard_monthly_event_report_section.js"
    ).read_text(encoding="utf-8")

    assert "saveAlarmEventUploadSchedulerQuickConfig({ run_time: $event.target.value })" in alarm_section
    assert "saveMonthlyEventReportSchedulerQuickConfig({ run_time: $event.target.value })" in monthly_section
    assert "saveMonthlyChangeReportSchedulerQuickConfig({ run_time: $event.target.value })" in monthly_section


def test_scheduler_quick_saves_queue_latest_payload() -> None:
    project_root = Path(__file__).resolve().parents[2]
    action_guard_source = (project_root / "web" / "frontend" / "src" / "action_guard.js").read_text(encoding="utf-8")
    scheduler_actions = (
        project_root / "web" / "frontend" / "src" / "dashboard_scheduler_actions.js"
    ).read_text(encoding="utf-8")
    wet_bulb_actions = (
        project_root / "web" / "frontend" / "src" / "dashboard_wet_bulb_collection_actions.js"
    ).read_text(encoding="utf-8")
    monthly_actions = (
        project_root / "web" / "frontend" / "src" / "dashboard_monthly_event_report_actions.js"
    ).read_text(encoding="utf-8")

    assert "if (options.queueLatest)" in action_guard_source
    assert "queuedTaskMap" in action_guard_source
    assert "{ cooldownMs: 0, queueLatest: true }" in scheduler_actions
    assert "{ cooldownMs: 0, queueLatest: true }" in wet_bulb_actions
    assert "{ cooldownMs: 0, queueLatest: true }" in monthly_actions


def test_handover_review_recipients_use_local_draft_building_switch() -> None:
    project_root = Path(__file__).resolve().parents[2]
    handover_tab = (
        project_root / "web" / "frontend" / "src" / "app_config_feature_handover_tab.js"
    ).read_text(encoding="utf-8")
    save_helpers = (
        project_root / "web" / "frontend" / "src" / "config_save_ui_helpers.js"
    ).read_text(encoding="utf-8")
    runtime_actions = (
        project_root / "web" / "frontend" / "src" / "runtime_health_config_actions.js"
    ).read_text(encoding="utf-8")

    assert "onHandoverReviewRecipientBuildingChange($event.target.value)" in handover_tab
    assert "onHandoverReviewRecipientBuildingChange(nextBuilding)" in save_helpers
    assert "collectDirtyHandoverReviewRecipientBuildings()" in save_helpers
    assert "lastSavedHandoverReviewRecipientSignatures, buildingText" in save_helpers
    assert "lastSavedHandoverBuildingMetaSignatures, buildingText" in save_helpers
    assert "allowSkip: false" in save_helpers
    assert "preserveDraftOnConflict: true" in save_helpers
    assert "skipSingleFlight: true" in save_helpers
    assert "baseRevision" in runtime_actions
    assert "handoverBuildingSegmentRevisions" in runtime_actions


def test_handover_review_link_send_only_checks_unsaved_config_inside_handover_config_tab() -> None:
    project_root = Path(__file__).resolve().parents[2]
    save_helpers = (
        project_root / "web" / "frontend" / "src" / "config_save_ui_helpers.js"
    ).read_text(encoding="utf-8")

    send_body = save_helpers[save_helpers.index("async function sendHandoverReviewLink") :]
    send_body = send_body[: send_body.index("async function onHandoverConfigBuildingChange")]
    assert "const shouldCheckPendingConfig =" in send_body
    assert 'String(currentView?.value || "").trim() === "config"' in send_body
    assert 'String(activeConfigTab?.value || "").trim() === "feature_handover"' in send_body
    assert "if (shouldCheckPendingConfig && hasPendingHandoverConfigChanges(targetBuilding))" in send_body
    assert 'reason: "action_unavailable"' in send_body


def test_handover_review_capacity_image_send_is_sync_without_job_polling() -> None:
    project_root = Path(__file__).resolve().parents[2]
    source = (
        project_root / "web" / "frontend" / "src" / "handover_review_action_helpers.js"
    ).read_text(encoding="utf-8")

    send_body = source[source.index("async function sendCurrentCapacityImage") :]
    assert "正在生成并发送容量表图片..." in send_body
    assert "审核文本和容量表图片发送成功" in send_body
    assert "job_id" not in send_body
    assert "waitForBackgroundJob" not in send_body
    assert "部分收件人发送失败" not in send_body


def test_handover_review_110kv_dirty_only_marks_actual_changes() -> None:
    project_root = Path(__file__).resolve().parents[2]
    source = (
        project_root / "web" / "frontend" / "src" / "handover_review_app.js"
    ).read_text(encoding="utf-8")

    update_body = source[source.index("async function updateSubstation110kvCell") :]
    update_body = update_body[: update_body.index("function valuesAfterRowLabel")]
    paste_body = source[source.index("async function pasteSubstation110kvTable") :]
    paste_body = paste_body[: paste_body.index("function updateCoolingPumpPressure")]

    assert 'if (!currentRow || String(currentRow[fieldKey] ?? "") === nextValue) return;' in update_body
    assert 'if (!row || String(row[fieldKey] ?? "") === nextValue) return;' in update_body
    assert "let recognized = false;" in paste_body
    assert "let rowChanged = false;" in paste_body
    assert "if (!rowChanged) return row;" in paste_body
    assert 'statusText.value = recognized ? "110KV变电站内容无变化" : "未识别到110KV变电站表格行";' in paste_body


def test_handover_review_110kv_remote_revision_refreshes_local_block() -> None:
    project_root = Path(__file__).resolve().parents[2]
    source = (
        project_root / "web" / "frontend" / "src" / "handover_review_app.js"
    ).read_text(encoding="utf-8")

    shared_body = source[source.index("function applySharedBlockPayload") :]
    shared_body = shared_body[: shared_body.index("function buildSubstation110kvPayload")]
    save_body = source[source.index("async function saveSubstation110kvIfNeeded") :]
    save_body = save_body[: save_body.index("async function saveDocument")]
    mounted_body = source[source.index("onMounted(async () =>") :]
    mounted_body = mounted_body[: mounted_body.index("return {")]

    assert "const serverRevisionChanged = hasIncomingSubstation && incomingRevision !== currentRevision;" in shared_body
    assert "const preserveLocalRows = Boolean(substation110kvDirty.value && incomingLock.client_holds_lock);" in shared_body
    assert "rows: currentBlock.rows," in shared_body
    assert "if (!substation110kvDirty.value || serverRevisionChanged)" in shared_body
    assert "substation110kvDirty.value = false;" in shared_body
    assert "pollIntervalMs.value = 1000;" in shared_body
    assert "const saved = await flushSubstation110kvAutoSave();" in save_body
    assert "await releaseSubstation110kvLock();" in save_body
    assert 'window.addEventListener("storage", handleReviewStatusBroadcast);' in mounted_body
    assert 'window.removeEventListener("storage", handleReviewStatusBroadcast);' in mounted_body


def test_handover_review_110kv_auto_saves_as_official_data() -> None:
    project_root = Path(__file__).resolve().parents[2]
    source = (
        project_root / "web" / "frontend" / "src" / "handover_review_app.js"
    ).read_text(encoding="utf-8")
    api_source = (project_root / "web" / "frontend" / "src" / "api_client.js").read_text(encoding="utf-8")

    assert "saveHandoverReview110kvDraftApi" not in source
    assert "/shared-blocks/110kv/draft" not in api_source
    assert "function scheduleSubstation110kvAutoSave()" in source
    assert "async function flushSubstation110kvAutoSave()" in source
    assert "saveHandoverReview110kvApi(buildingCode" in source
    assert "scheduleSubstation110kvAutoSave();" in source[source.index("function markSubstation110kvDirty") :]
    assert "await flushSubstation110kvAutoSave();" in source[source.index("async function saveSubstation110kvIfNeeded") :]
    assert "shared_block_drafts" not in source


def test_runtime_time_normalizer_accepts_single_digit_hour() -> None:
    project_root = Path(__file__).resolve().parents[2]
    source = (project_root / "web" / "frontend" / "src" / "config_date_utils.js").read_text(encoding="utf-8")

    assert r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$" in source
    assert "padStart(2, \"0\")" in source
