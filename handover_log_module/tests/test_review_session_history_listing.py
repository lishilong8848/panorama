from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook

from app.config.config_adapter import ensure_v3_config
from handover_log_module.service.review_session_service import ReviewSessionService


def _build_service(tmp_path: Path) -> ReviewSessionService:
    cfg = ensure_v3_config({})
    handover_cfg = cfg.setdefault("features", {}).setdefault("handover_log", {})
    handover_cfg.setdefault("event_sections", {}).setdefault("cache", {})["state_file"] = "review_cloud_sync_state.json"
    handover_cfg.setdefault("template", {})["output_dir"] = str(tmp_path / "outputs")
    handover_cfg["_global_paths"] = {"runtime_state_root": str(tmp_path)}
    return ReviewSessionService(handover_cfg)


def _create_output_file(path: Path, *, shift_text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "交接班日志"
    worksheet["F2"] = shift_text
    workbook.save(path)
    workbook.close()


def _register_session(
    service: ReviewSessionService,
    *,
    duty_date: str,
    duty_shift: str,
    output_file: Path,
    cloud_status: str,
    spreadsheet_url: str,
) -> None:
    registered = service.register_generated_output(
        building="A楼",
        duty_date=duty_date,
        duty_shift=duty_shift,
        data_file="",
        output_file=str(output_file),
        source_mode="from_download",
    )
    service.update_cloud_sheet_sync(
        session_id=registered["session_id"],
        cloud_sheet_sync={
            "attempted": cloud_status in {"success", "failed"},
            "success": cloud_status == "success",
            "status": cloud_status,
            "spreadsheet_url": spreadsheet_url,
            "sheet_title": "A楼",
        },
    )


def test_list_building_cloud_history_sessions_filters_successful_cloud_outputs_and_limits_to_10(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _build_service(tmp_path)
    monkeypatch.setattr(service, "_is_legacy_test_output_file", lambda _output: False)
    output_dir = tmp_path / "outputs"

    successful_rows = [
        ("2026-03-24", "night"),
        ("2026-03-24", "day"),
        ("2026-03-23", "night"),
        ("2026-03-23", "day"),
        ("2026-03-22", "night"),
        ("2026-03-22", "day"),
        ("2026-03-21", "night"),
        ("2026-03-21", "day"),
        ("2026-03-20", "night"),
        ("2026-03-20", "day"),
        ("2026-03-19", "night"),
        ("2026-03-19", "day"),
    ]

    for duty_date, duty_shift in successful_rows:
        output_file = output_dir / f"A楼_{duty_date}_{duty_shift}.xlsx"
        _create_output_file(output_file, shift_text="夜班" if duty_shift == "night" else "白班")
        _register_session(
            service,
            duty_date=duty_date,
            duty_shift=duty_shift,
            output_file=output_file,
            cloud_status="success",
            spreadsheet_url=f"https://example.com/{duty_date}/{duty_shift}",
        )

    failed_file = output_dir / "A楼_2026-03-18_day.xlsx"
    _create_output_file(failed_file, shift_text="白班")
    _register_session(
        service,
        duty_date="2026-03-18",
        duty_shift="day",
        output_file=failed_file,
        cloud_status="failed",
        spreadsheet_url="https://example.com/2026-03-18/day",
    )

    no_url_file = output_dir / "A楼_2026-03-17_night.xlsx"
    _create_output_file(no_url_file, shift_text="夜班")
    _register_session(
        service,
        duty_date="2026-03-17",
        duty_shift="night",
        output_file=no_url_file,
        cloud_status="success",
        spreadsheet_url="",
    )

    missing_output_file = output_dir / "A楼_2026-03-16_day.xlsx"
    _register_session(
        service,
        duty_date="2026-03-16",
        duty_shift="day",
        output_file=missing_output_file,
        cloud_status="success",
        spreadsheet_url="https://example.com/2026-03-16/day",
    )

    items = service.list_building_cloud_history_sessions("A楼", limit=10)

    assert [item["session_id"] for item in items] == [
        "A楼|2026-03-24|night",
        "A楼|2026-03-24|day",
        "A楼|2026-03-23|night",
        "A楼|2026-03-23|day",
        "A楼|2026-03-22|night",
        "A楼|2026-03-22|day",
        "A楼|2026-03-21|night",
        "A楼|2026-03-21|day",
        "A楼|2026-03-20|night",
        "A楼|2026-03-20|day",
    ]
    assert all(item["cloud_sheet_sync"]["status"] == "success" for item in items)
    assert all(item["cloud_sheet_sync"]["spreadsheet_url"] for item in items)
