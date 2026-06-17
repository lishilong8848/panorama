from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CAPACITY_DELIVERY_SOURCE = ROOT / "handover_log_module" / "service" / "capacity_report_image_delivery_service.py"
SUMMARY_SOURCE = ROOT / "handover_log_module" / "service" / "handover_summary_message_service.py"
ROUTES_SOURCE = ROOT / "app" / "modules" / "handover_review" / "api" / "routes.py"
REVIEW_LINK_SOURCE = ROOT / "handover_log_module" / "service" / "review_link_delivery_service.py"
ORCHESTRATOR_SOURCE = ROOT / "handover_log_module" / "service" / "handover_orchestrator.py"
REVIEW_FOLLOWUP_SOURCE = ROOT / "handover_log_module" / "service" / "review_followup_trigger_service.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_capacity_image_delivery_does_not_write_review_link_delivery_state():
    source = _read(CAPACITY_DELIVERY_SOURCE)

    assert "update_capacity_image_delivery" in source
    assert "update_review_link_delivery" not in source
    assert "_persist_review_delivery" not in source


def test_capacity_image_delivery_blocks_mixed_review_link_text():
    source = _read(CAPACITY_DELIVERY_SOURCE)

    assert "审核链接：" in source
    assert "/handover/review/" in source
    assert "交接班文本异常包含审核页链接，已阻止发送" in source


def test_capacity_image_delivery_rejects_blank_or_tiny_rendered_images():
    source = _read(CAPACITY_DELIVERY_SOURCE)

    assert "_validate_rendered_image_content(output_path)" in source
    assert "容量表截图为空白图片" in source
    assert "容量表截图尺寸异常" in source
    assert "容量表截图有效内容过少" in source


def test_handover_summary_requires_current_and_next_people():
    source = _read(SUMMARY_SOURCE)

    assert "if not current_people or not next_people:" in source
    assert "人员信息不完整，跳过发送" in source
    assert "return \"\"" in source


def test_capacity_image_send_requires_short_lived_server_token():
    source = _read(ROUTES_SOURCE)

    assert "capacity-image/prepare" in source
    assert "_issue_capacity_image_send_token" in source
    assert "_verify_capacity_image_send_token" in source
    assert "send_token" in source
    assert "缺少容量表图片发送确认令牌" in source
    assert "容量表图片发送确认令牌已过期" in source


def test_review_link_pending_dispatch_covers_blank_latest_batch_but_not_regenerate_skip():
    source = _read(REVIEW_LINK_SOURCE)
    orchestrator_source = _read(ORCHESTRATOR_SOURCE)

    assert 'status in {"", "success", "partial_failed", "failed", "pending_access", "disabled", "unconfigured"}' in source
    assert 'status == "skipped_auto_disabled" or source == "auto_disabled"' in source
    assert "session_batch_key != latest_batch_key" in source
    assert '"status": "skipped_auto_disabled"' in orchestrator_source
    assert '"source": "auto_disabled"' in orchestrator_source


def test_handover_existing_file_generation_preseeds_shared_outdoor_temperature():
    source = _read(ORCHESTRATOR_SOURCE)

    assert "def _persist_shared_outdoor_temperature_cells(" in source
    assert "def _merge_shared_outdoor_cells_into_prebuilt(" in source
    assert '[{"building": building, "file_path": data_file} for building, data_file in normalized_files]' in source
    assert "self._persist_shared_outdoor_temperature_cells(" in source
    assert "self._merge_shared_outdoor_cells_into_prebuilt(prebuilt_fixed, shared_outdoor_cells)" in source
    assert "fixed_cell_values=fixed_cell_values" in source


def test_station_h_patrol_cells_use_two_current_duty_people():
    source = _read(REVIEW_FOLLOWUP_SOURCE)

    assert 'current_second = current_names[1] if len(current_names) >= 2 else ""' in source
    assert '"H15": current_first' in source
    assert '"H16": current_second' in source
    assert '"H16": current_first' not in source
