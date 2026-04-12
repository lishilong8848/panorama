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


def test_update_cloud_sheet_sync_persists_to_session_and_batch_status(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    registered = service.register_generated_output(
        building="A楼",
        duty_date="2026-03-22",
        duty_shift="day",
        data_file=r"D:\handover\A楼源数据.xlsx",
        output_file=r"D:\handover\A楼交接班.xlsx",
        source_mode="from_file",
    )

    updated = service.update_cloud_sheet_sync(
        session_id=registered["session_id"],
        cloud_sheet_sync={
            "attempted": True,
            "success": True,
            "status": "success",
            "spreadsheet_token": "sheet_token_1",
            "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
            "sheet_title": "A楼",
            "error": "",
            "synced_row_count": 59,
            "synced_column_count": 9,
            "synced_merges": [
                {
                    "start_row_index": 22,
                    "end_row_index": 23,
                    "start_column_index": 0,
                    "end_column_index": 2,
                }
            ],
            "dynamic_merge_signature": "merge-signature-1",
        },
    )

    assert updated["cloud_sheet_sync"]["attempted"] is True
    assert updated["cloud_sheet_sync"]["success"] is True
    assert updated["cloud_sheet_sync"]["status"] == "success"
    assert updated["cloud_sheet_sync"]["spreadsheet_url"] == "https://vnet.feishu.cn/wiki/wiki_token_1"
    assert updated["cloud_sheet_sync"]["synced_row_count"] == 59
    assert updated["cloud_sheet_sync"]["synced_column_count"] == 9
    assert len(updated["cloud_sheet_sync"]["synced_merges"]) == 1
    assert updated["cloud_sheet_sync"]["dynamic_merge_signature"] == "merge-signature-1"
    assert updated["updated_at"]

    latest_batch = service.get_latest_batch_status()
    row = next(item for item in latest_batch["buildings"] if item["building"] == "A楼")
    assert row["has_session"] is True
    assert row["cloud_sheet_sync"]["status"] == "success"
    assert row["cloud_sheet_sync"]["sheet_title"] == "A楼"
    assert row["cloud_sheet_sync"]["synced_row_count"] == 59
    assert row["cloud_sheet_sync"]["dynamic_merge_signature"] == "merge-signature-1"


def test_register_generated_output_persists_source_file_cache(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    cached_path = tmp_path / "handover" / "source_files" / "2026-03-24_day" / "A楼" / "A楼_2026-03-24_day" / "source.xlsx"
    cached_path.parent.mkdir(parents=True, exist_ok=True)
    cached_path.write_bytes(b"source")

    registered = service.register_generated_output(
        building="A楼",
        duty_date="2026-03-24",
        duty_shift="day",
        data_file=str(cached_path),
        output_file=r"D:\handover\A楼交接班.xlsx",
        source_mode="from_file",
        source_file_cache={
            "managed": True,
            "stored_path": str(cached_path),
            "original_name": "A楼.xlsx",
            "stored_at": "2026-03-24 09:00:00",
            "cleanup_status": "active",
            "cleanup_at": "",
        },
    )

    assert registered["data_file"] == str(cached_path)
    assert registered["source_file_cache"]["managed"] is True
    assert registered["source_file_cache"]["stored_path"] == str(cached_path)
    assert registered["source_file_cache"]["original_name"] == "A楼.xlsx"


def test_load_state_marks_missing_managed_source_file_cache(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    state = service._review_state_store.load_state()
    missing_path = tmp_path / "handover" / "source_files" / "2026-03-24_day" / "A楼" / "A楼_2026-03-24_day" / "source.xlsx"
    state["review_sessions"] = {
        "A楼|2026-03-24|day": {
            "session_id": "A楼|2026-03-24|day",
            "building": "A楼",
            "building_code": "a",
            "duty_date": "2026-03-24",
            "duty_shift": "day",
            "batch_key": "2026-03-24|day",
            "output_file": r"D:\handover\A楼交接班.xlsx",
            "data_file": str(missing_path),
            "source_mode": "from_file",
            "revision": 1,
            "confirmed": False,
            "confirmed_at": "",
            "confirmed_by": "",
            "updated_at": "2026-03-24 09:00:00",
            "day_metric_export": {},
            "cloud_sheet_sync": {},
            "source_file_cache": {
                "managed": True,
                "stored_path": str(missing_path),
                "original_name": "A楼.xlsx",
                "stored_at": "2026-03-24 09:00:00",
                "cleanup_status": "active",
                "cleanup_at": "",
            },
            "source_data_attachment_export": {},
        }
    }
    service._review_state_store.save_state(state)

    session = service.get_session_by_id("A楼|2026-03-24|day")

    assert session is not None
    assert session["source_file_cache"]["managed"] is True
    assert session["source_file_cache"]["cleanup_status"] == "missing"
    assert session["source_file_cache"]["cleanup_at"]


def test_load_state_filters_pytest_legacy_handover_outputs(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    state = service._review_state_store.load_state()
    state["review_sessions"] = {
        "C楼|2026-03-22|day": {
            "session_id": "C楼|2026-03-22|day",
            "building": "C楼",
            "building_code": "c",
            "duty_date": "2026-03-22",
            "duty_shift": "day",
            "batch_key": "2026-03-22|day",
            "output_file": r"C:\Users\tester\AppData\Local\Temp\pytest-of-user\pytest-93\output\C楼_20260322_handover.xlsx",
            "data_file": r"C:\Users\tester\AppData\Local\Temp\handover_from_file_xxx\input.xlsx",
            "source_mode": "from_file",
            "revision": 1,
            "confirmed": False,
            "confirmed_at": "",
            "confirmed_by": "",
            "updated_at": "2026-03-22 12:00:00",
            "day_metric_export": {},
            "cloud_sheet_sync": {},
            "source_data_attachment_export": {},
        }
    }
    state["review_latest_by_building"] = {"C楼": "C楼|2026-03-22|day"}
    service._review_state_store.save_state(state)

    latest_batch = service.get_latest_batch_status()

    assert latest_batch["batch_key"] == ""
    assert latest_batch["confirmed_count"] == 0
    assert all(not bool(item["has_session"]) for item in latest_batch["buildings"])

    reloaded = service._review_state_store.load_state()
    assert reloaded["review_sessions"] == {}
    assert reloaded["review_latest_by_building"] == {}


def test_load_state_rebuilds_latest_by_building_from_valid_sessions(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    state = service._review_state_store.load_state()
    state["review_sessions"] = {
        "C楼|2026-03-22|night": {
            "session_id": "C楼|2026-03-22|night",
            "building": "C楼",
            "building_code": "c",
            "duty_date": "2026-03-22",
            "duty_shift": "night",
            "batch_key": "2026-03-22|night",
            "output_file": r"D:\QLDownload\交接班日志输出\C楼_20260322_交接班日志.xlsx",
            "data_file": r"D:\QLDownload\源数据\C楼.xlsx",
            "source_mode": "from_download",
            "revision": 1,
            "confirmed": False,
            "confirmed_at": "",
            "confirmed_by": "",
            "updated_at": "2026-03-22 02:44:02",
            "day_metric_export": {},
            "cloud_sheet_sync": {},
            "source_data_attachment_export": {},
        },
        "C楼|2026-03-22|day": {
            "session_id": "C楼|2026-03-22|day",
            "building": "C楼",
            "building_code": "c",
            "duty_date": "2026-03-22",
            "duty_shift": "day",
            "batch_key": "2026-03-22|day",
            "output_file": r"C:\Users\tester\AppData\Local\Temp\pytest-of-user\pytest-93\output\C楼_20260322_handover.xlsx",
            "data_file": r"C:\Users\tester\AppData\Local\Temp\handover_from_file_xxx\input.xlsx",
            "source_mode": "from_file",
            "revision": 1,
            "confirmed": False,
            "confirmed_at": "",
            "confirmed_by": "",
            "updated_at": "2026-03-22 12:00:00",
            "day_metric_export": {},
            "cloud_sheet_sync": {},
            "source_data_attachment_export": {},
        },
    }
    state["review_latest_by_building"] = {"C楼": "C楼|2026-03-22|day"}
    service._review_state_store.save_state(state)

    latest = service.get_latest_session("C楼")

    assert latest is not None
    assert latest["session_id"] == "C楼|2026-03-22|night"
    assert latest["output_file"] == r"D:\QLDownload\交接班日志输出\C楼_20260322_交接班日志.xlsx"

    reloaded = service._review_state_store.load_state()
    assert reloaded["review_latest_by_building"]["C楼"] == "C楼|2026-03-22|night"
    assert "C楼|2026-03-22|day" not in reloaded["review_sessions"]


def test_get_latest_session_recovers_newer_formal_output_file(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    output_dir = tmp_path / "outputs"
    night_file = output_dir / "C楼_20260322_交接班日志.xlsx"
    day_file = output_dir / "C楼_20260322_交接班日志_2.xlsx"
    _create_output_file(night_file, shift_text="夜班")
    _create_output_file(day_file, shift_text="白班")

    service.register_generated_output(
        building="C楼",
        duty_date="2026-03-22",
        duty_shift="night",
        data_file="",
        output_file=str(night_file),
        source_mode="from_download",
    )

    latest = service.get_latest_session("C楼")

    assert latest is not None
    assert latest["session_id"] == "C楼|2026-03-22|day"
    assert latest["output_file"] == str(day_file)
    assert latest["source_mode"] == "recovered_from_output"

    latest_again = service.get_latest_session("C楼")
    assert latest_again is not None
    assert latest_again["session_id"] == "C楼|2026-03-22|day"
    assert latest_again["output_file"] == str(day_file)


def test_get_latest_session_recovery_degrades_gracefully_when_state_save_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _build_service(tmp_path)
    output_dir = tmp_path / "outputs"
    day_file = output_dir / "C楼_20260322_交接班日志.xlsx"
    _create_output_file(day_file, shift_text="白班")

    def _raise_register(**_kwargs):  # noqa: ANN003
        raise PermissionError("locked")

    monkeypatch.setattr(service, "register_generated_output", _raise_register)

    latest = service.get_latest_session("C楼")

    assert latest is not None
    assert latest["session_id"] == "C楼|2026-03-22|day"
    assert latest["output_file"] == str(day_file)
    assert latest["source_mode"] == "recovered_from_output"


def test_get_batch_status_and_session_follow_requested_duty_context(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    output_dir = tmp_path / "outputs"
    night_file = output_dir / "A楼_20260323_交接班日志.xlsx"
    day_file = output_dir / "A楼_20260324_交接班日志.xlsx"
    _create_output_file(night_file, shift_text="夜班")
    _create_output_file(day_file, shift_text="白班")

    service.register_generated_output(
        building="A楼",
        duty_date="2026-03-23",
        duty_shift="night",
        data_file="",
        output_file=str(night_file),
        source_mode="from_download",
    )
    service.register_generated_output(
        building="A楼",
        duty_date="2026-03-24",
        duty_shift="day",
        data_file="",
        output_file=str(day_file),
        source_mode="from_download",
    )

    requested = service.get_session_for_building_duty("A楼", "2026-03-24", "day")
    batch_status = service.get_batch_status_for_duty("2026-03-24", "day")

    assert requested is not None
    assert requested["session_id"] == "A楼|2026-03-24|day"
    assert batch_status["batch_key"] == "2026-03-24|day"
    assert batch_status["duty_date"] == "2026-03-24"
    assert batch_status["duty_shift"] == "day"


def test_list_building_sessions_returns_existing_outputs_latest_first(tmp_path: Path, monkeypatch) -> None:
    service = _build_service(tmp_path)
    monkeypatch.setattr(service, "_is_legacy_test_output_file", lambda _output: False)
    output_dir = tmp_path / "outputs"
    latest_file = output_dir / "A楼_20260323_交接班日志.xlsx"
    history_file = output_dir / "A楼_20260322_交接班日志.xlsx"
    missing_file = output_dir / "A楼_20260321_交接班日志.xlsx"
    _create_output_file(latest_file, shift_text="夜班")
    _create_output_file(history_file, shift_text="白班")

    service.register_generated_output(
        building="A楼",
        duty_date="2026-03-23",
        duty_shift="night",
        data_file="",
        output_file=str(latest_file),
        source_mode="from_download",
    )
    service.register_generated_output(
        building="A楼",
        duty_date="2026-03-22",
        duty_shift="day",
        data_file="",
        output_file=str(history_file),
        source_mode="from_download",
    )
    service.register_generated_output(
        building="A楼",
        duty_date="2026-03-21",
        duty_shift="night",
        data_file="",
        output_file=str(missing_file),
        source_mode="from_download",
    )

    items = service.list_building_sessions("A楼")

    assert [item["session_id"] for item in items] == [
        "A楼|2026-03-23|night",
        "A楼|2026-03-22|day",
    ]
    assert service.get_latest_session_id("A楼") == "A楼|2026-03-23|night"


def test_touch_session_after_history_save_only_resets_cloud_sync(tmp_path: Path, monkeypatch) -> None:
    service = _build_service(tmp_path)
    monkeypatch.setattr(service, "_is_legacy_test_output_file", lambda _output: False)
    output_file = tmp_path / "outputs" / "A楼_20260322_交接班日志.xlsx"
    _create_output_file(output_file, shift_text="白班")

    registered = service.register_generated_output(
        building="A楼",
        duty_date="2026-03-22",
        duty_shift="day",
        data_file=r"D:\handover\A楼源数据.xlsx",
        output_file=str(output_file),
        source_mode="from_file",
    )
    service.mark_confirmed(
        building="A楼",
        session_id=registered["session_id"],
        base_revision=int(registered.get("revision", 0)),
        confirmed=True,
        confirmed_by="tester",
    )
    service.update_source_data_attachment_export(
        session_id=registered["session_id"],
        source_data_attachment_export={
            "status": "success",
            "uploaded_count": 1,
            "uploaded_revision": 1,
            "reason": "",
        },
    )
    service.update_cloud_sheet_sync(
        session_id=registered["session_id"],
        cloud_sheet_sync={
            "attempted": True,
            "success": True,
            "status": "success",
            "synced_revision": 1,
            "last_attempt_revision": 1,
            "sheet_title": "A楼",
        },
    )
    current = service.get_session_by_id(registered["session_id"])
    assert current is not None

    updated, batch_status = service.touch_session_after_history_save(
        building="A楼",
        session_id=registered["session_id"],
        base_revision=int(current["revision"]),
    )

    assert updated["revision"] == int(current["revision"]) + 1
    assert updated["confirmed"] is True
    assert updated["confirmed_by"] == "tester"
    assert updated["source_data_attachment_export"]["status"] == "success"
    assert updated["cloud_sheet_sync"]["status"] == "pending_upload"
    assert updated["cloud_sheet_sync"]["attempted"] is False
    assert updated["cloud_sheet_sync"]["success"] is False
    assert updated["cloud_sheet_sync"]["last_attempt_revision"] == 1
    assert batch_status["batch_key"] == "2026-03-22|day"


def test_touch_session_after_save_keeps_attachment_state_after_first_full_cloud_sync(tmp_path: Path, monkeypatch) -> None:
    service = _build_service(tmp_path)
    monkeypatch.setattr(service, "_is_legacy_test_output_file", lambda _output: False)
    output_file = tmp_path / "outputs" / "A楼_20260322_交接班日志.xlsx"
    _create_output_file(output_file, shift_text="白班")

    registered = service.register_generated_output(
        building="A楼",
        duty_date="2026-03-22",
        duty_shift="day",
        data_file=r"D:\handover\A楼源数据.xlsx",
        output_file=str(output_file),
        source_mode="from_file",
    )
    service.register_cloud_batch(
        batch_key="2026-03-22|day",
        duty_date="2026-03-22",
        duty_shift="day",
        cloud_batch={
            "status": "prepared",
            "spreadsheet_token": "sheet_token_1",
            "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
            "spreadsheet_title": "日报云文档",
            "first_full_cloud_sync_completed": True,
            "first_full_cloud_sync_at": "2026-03-22 10:00:00",
        },
    )
    service.update_source_data_attachment_export(
        session_id=registered["session_id"],
        source_data_attachment_export={
            "status": "success",
            "uploaded_count": 1,
            "uploaded_revision": 1,
            "reason": "",
            "uploaded_at": "2026-03-22 10:00:00",
        },
    )
    service.update_cloud_sheet_sync(
        session_id=registered["session_id"],
        cloud_sheet_sync={
            "attempted": True,
            "success": True,
            "status": "success",
            "synced_revision": 1,
            "last_attempt_revision": 1,
            "sheet_title": "A楼",
        },
    )
    current = service.get_session_by_id(registered["session_id"])
    assert current is not None

    updated, batch_status = service.touch_session_after_save(
        building="A楼",
        session_id=registered["session_id"],
        base_revision=int(current["revision"]),
    )

    assert updated["confirmed"] is False
    assert updated["revision"] == int(current["revision"]) + 1
    assert updated["source_data_attachment_export"]["status"] == "success"
    assert updated["source_data_attachment_export"]["uploaded_revision"] == 1
    assert updated["source_data_attachment_export"]["frozen_after_first_full_cloud_sync"] is True
    assert updated["cloud_sheet_sync"]["status"] == "pending_upload"
    assert updated["cloud_sheet_sync"]["attempted"] is False
    assert batch_status["batch_key"] == "2026-03-22|day"
