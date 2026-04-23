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


def test_handover_scheduler_time_inputs_use_explicit_save() -> None:
    project_root = Path(__file__).resolve().parents[2]
    handover_section = (
        project_root
        / "web"
        / "frontend"
        / "src"
        / "dashboard_template_sections"
        / "dashboard_handover_log_section.js"
    ).read_text(encoding="utf-8")

    assert 'type="text" inputmode="numeric" placeholder="HH:MM:SS"' in handover_section
    assert '@change="saveHandoverSchedulerQuickConfig' not in handover_section
    assert '@click="saveHandoverSchedulerQuickConfig()"' in handover_section
    assert "保存时间" in handover_section


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


def test_runtime_time_normalizer_accepts_single_digit_hour() -> None:
    project_root = Path(__file__).resolve().parents[2]
    source = (project_root / "web" / "frontend" / "src" / "config_date_utils.js").read_text(encoding="utf-8")

    assert r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$" in source
    assert "padStart(2, \"0\")" in source
