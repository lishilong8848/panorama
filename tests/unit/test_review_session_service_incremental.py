from __future__ import annotations

import pytest

from handover_log_module.service.review_session_service import ReviewSessionService


def _service(tmp_path):
    return ReviewSessionService(
        {
            "_global_paths": {
                "runtime_state_root": str(tmp_path / ".runtime"),
            }
        }
    )


def test_register_generated_output_does_not_use_full_state_rewrite(tmp_path, monkeypatch) -> None:
    service = _service(tmp_path)

    def _boom(_payload):  # noqa: ANN001
        raise AssertionError("save_state should not be used on hot path")

    monkeypatch.setattr(service._review_state_store, "save_state", _boom)

    session = service.register_generated_output(
        building="A楼",
        duty_date="2026-04-15",
        duty_shift="day",
        data_file="demo.xlsx",
        output_file="output.xlsx",
        source_mode="generated",
    )

    assert session["session_id"] == "A楼|2026-04-15|day"


def test_mark_confirmed_does_not_use_full_state_rewrite(tmp_path, monkeypatch) -> None:
    service = _service(tmp_path)
    session = service.register_generated_output(
        building="A楼",
        duty_date="2026-04-15",
        duty_shift="day",
        data_file="demo.xlsx",
        output_file="output.xlsx",
        source_mode="generated",
    )

    def _boom(_payload):  # noqa: ANN001
        raise AssertionError("save_state should not be used on hot path")

    monkeypatch.setattr(service._review_state_store, "save_state", _boom)

    updated, batch_status = service.mark_confirmed(
        building="A楼",
        session_id=session["session_id"],
        confirmed=True,
        base_revision=int(session["revision"]),
        confirmed_by="tester",
    )

    assert updated["confirmed"] is True
    assert batch_status["batch_key"] == "2026-04-15|day"


def test_load_state_repairs_missing_source_cache_without_full_state_rewrite(tmp_path, monkeypatch) -> None:
    service = _service(tmp_path)
    state = service._review_state_store.load_state()
    missing_path = tmp_path / "handover" / "source_files" / "2026-04-15_day" / "A楼" / "source.xlsx"
    state["review_sessions"] = {
        "A楼|2026-04-15|day": {
            "session_id": "A楼|2026-04-15|day",
            "building": "A楼",
            "building_code": "a",
            "duty_date": "2026-04-15",
            "duty_shift": "day",
            "batch_key": "2026-04-15|day",
            "output_file": r"D:\handover\A楼交接班.xlsx",
            "data_file": str(missing_path),
            "source_mode": "from_file",
            "revision": 1,
            "confirmed": False,
            "confirmed_at": "",
            "confirmed_by": "",
            "updated_at": "2026-04-15 09:00:00",
            "day_metric_export": {},
            "cloud_sheet_sync": {},
            "source_file_cache": {
                "managed": True,
                "stored_path": str(missing_path),
                "original_name": "A楼.xlsx",
                "stored_at": "2026-04-15 09:00:00",
                "cleanup_status": "active",
                "cleanup_at": "",
            },
            "source_data_attachment_export": {},
        }
    }
    service._review_state_store.save_state(state)

    def _boom(_payload):  # noqa: ANN001
        raise AssertionError("save_state should not be used in _load_state repairs")

    monkeypatch.setattr(service._review_state_store, "save_state", _boom)

    session = service.get_session_by_id("A楼|2026-04-15|day")

    assert session is not None
    assert session["source_file_cache"]["cleanup_status"] == "missing"
    assert session["source_file_cache"]["cleanup_at"]


def test_load_state_repairs_legacy_sessions_without_full_state_rewrite(tmp_path, monkeypatch) -> None:
    service = _service(tmp_path)
    state = service._review_state_store.load_state()
    state["review_sessions"] = {
        "C楼|2026-04-15|night": {
            "session_id": "C楼|2026-04-15|night",
            "building": "C楼",
            "building_code": "c",
            "duty_date": "2026-04-15",
            "duty_shift": "night",
            "batch_key": "2026-04-15|night",
            "output_file": r"D:\QLDownload\交接班日志输出\C楼_20260415_交接班日志.xlsx",
            "data_file": r"D:\QLDownload\源数据\C楼.xlsx",
            "source_mode": "from_download",
            "revision": 1,
            "confirmed": False,
            "confirmed_at": "",
            "confirmed_by": "",
            "updated_at": "2026-04-15 20:00:00",
            "day_metric_export": {},
            "cloud_sheet_sync": {},
            "source_data_attachment_export": {},
        },
        "C楼|2026-04-15|day": {
            "session_id": "C楼|2026-04-15|day",
            "building": "C楼",
            "building_code": "c",
            "duty_date": "2026-04-15",
            "duty_shift": "day",
            "batch_key": "2026-04-15|day",
            "output_file": r"C:\Users\tester\AppData\Local\Temp\pytest-of-user\pytest-93\output\C楼_20260415_handover.xlsx",
            "data_file": r"C:\Users\tester\AppData\Local\Temp\handover_from_file_xxx\input.xlsx",
            "source_mode": "from_file",
            "revision": 1,
            "confirmed": False,
            "confirmed_at": "",
            "confirmed_by": "",
            "updated_at": "2026-04-15 09:00:00",
            "day_metric_export": {},
            "cloud_sheet_sync": {},
            "source_data_attachment_export": {},
        },
    }
    state["review_latest_by_building"] = {"C楼": "C楼|2026-04-15|day"}
    state["review_latest_batch_key"] = "2026-04-15|day"
    service._review_state_store.save_state(state)

    def _boom(_payload):  # noqa: ANN001
        raise AssertionError("save_state should not be used in _load_state repairs")

    monkeypatch.setattr(service._review_state_store, "save_state", _boom)

    repaired_state = service._load_state()

    assert repaired_state["review_latest_by_building"]["C楼"] == "C楼|2026-04-15|night"
    assert repaired_state["review_latest_batch_key"] == "2026-04-15|night"
    assert "C楼|2026-04-15|day" not in repaired_state["review_sessions"]


def test_list_building_sessions_does_not_trigger_output_recovery(tmp_path, monkeypatch) -> None:
    service = _service(tmp_path)
    output_file = tmp_path / "outputs" / "A楼_latest.xlsx"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_bytes(b"demo")

    state = service._review_state_store.load_state()
    state["review_sessions"] = {
        "A楼|2026-04-15|day": {
            "session_id": "A楼|2026-04-15|day",
            "building": "A楼",
            "building_code": "a",
            "duty_date": "2026-04-15",
            "duty_shift": "day",
            "batch_key": "2026-04-15|day",
            "output_file": str(output_file),
            "data_file": "",
            "source_mode": "generated",
            "revision": 2,
            "confirmed": False,
            "confirmed_at": "",
            "confirmed_by": "",
            "updated_at": "2026-04-15 09:00:00",
            "cloud_sheet_sync": {},
            "source_data_attachment_export": {},
        }
    }
    state["review_latest_by_building"] = {}
    service._review_state_store.save_state(state)
    monkeypatch.setattr(service, "_is_legacy_test_output_file", lambda _path: False)

    def _boom(_building):  # noqa: ANN001
        raise AssertionError("history/list hot path should not trigger output recovery")

    monkeypatch.setattr(service, "_recover_latest_session_from_output_file", _boom)

    sessions = service.list_building_sessions("A楼")

    assert [item["session_id"] for item in sessions] == ["A楼|2026-04-15|day"]
