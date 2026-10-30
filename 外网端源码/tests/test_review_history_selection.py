from types import SimpleNamespace
from unittest.mock import Mock

from app.modules.handover_review.api import routes


def test_history_date_includes_records_without_download_files():
    session = {"session_id": "A楼|2026-03-01|day", "building": "A楼", "duty_date": "2026-03-01", "duty_shift": "day"}
    service = SimpleNamespace(
        get_latest_session_id_fast=lambda _b: "A楼|2026-10-27|day",
        list_building_sessions=lambda _b: [session],
        list_building_generated_file_history_sessions_fast=Mock(side_effect=AssertionError("not file-only")),
    )
    history = routes._build_history_payload(service, building="A楼", selected_session_id=session["session_id"], history_date="2026-03-01")
    assert history["sessions"][0]["session_id"] == session["session_id"]
    assert history["selected_is_latest"] is False
    assert history["sessions"][0]["has_output_file"] is False


def test_history_init_is_deduplicated_and_only_parses_known_local_file(monkeypatch, tmp_path):
    file = tmp_path / "old.xlsx"
    file.write_bytes(b"fixture")
    queued = []
    executor = SimpleNamespace(submit=lambda fn: queued.append(fn))
    session = {"session_id": "A楼|2026-03-01|day", "output_file": str(file)}
    parser = SimpleNamespace(parse=Mock(return_value={"sections": []}))
    store = SimpleNamespace(upsert_imported_document=Mock())
    container = SimpleNamespace(add_system_log=Mock())
    service = SimpleNamespace(recover_history_session=Mock(return_value=session))
    monkeypatch.setattr(routes, "_REVIEW_HISTORY_IMPORTS", {})
    monkeypatch.setattr(routes, "_REVIEW_HISTORY_IMPORT_EXECUTOR", executor)
    monkeypatch.setattr(routes, "_handover_cfg", lambda _c: {})
    monkeypatch.setattr(routes, "ReviewBuildingDocumentStore", lambda **_kw: store)
    for _ in range(10):
        status = routes._schedule_history_document_import(container, service, parser, building="A楼", duty_date="2026-03-01", duty_shift="day")
        assert status["pending"]
    assert len(queued) == 1
    queued[0]()
    parser.parse.assert_called_once_with(str(file))
    store.upsert_imported_document.assert_called_once()


def test_history_init_never_opens_unc(monkeypatch):
    queued = []
    monkeypatch.setattr(routes, "_REVIEW_HISTORY_IMPORTS", {})
    monkeypatch.setattr(routes, "_REVIEW_HISTORY_IMPORT_EXECUTOR", SimpleNamespace(submit=lambda fn: queued.append(fn)))
    parser = SimpleNamespace(parse=Mock())
    session = {"output_file": r"\\server\share\log.xlsx"}
    container = SimpleNamespace(add_system_log=Mock())
    routes._schedule_history_document_import(container, None, parser, building="A楼", duty_date="2026-03-01", duty_shift="day", session=session)
    queued[0]()
    parser.parse.assert_not_called()


def test_h_history_does_not_fetch_current_roster(monkeypatch):
    selection = {"current_people": ["历史值班"], "next_people": ["历史接班"], "long_day_people": [], "training_rows": []}
    selection_service = SimpleNamespace(get_selection=lambda *_a: selection, resolve_selection=lambda **_kw: selection)
    roster = Mock(side_effect=AssertionError("history must not query roster"))
    focus = Mock(return_value={"rows": [], "image_status": {"available": True}})
    monkeypatch.setattr(routes, "_handover_cfg", lambda _c: {})
    monkeypatch.setattr(routes, "_is_current_handover_duty_context", lambda **_kw: False)
    monkeypatch.setattr(routes, "_build_station_h_review_selection_service", lambda _c: selection_service)
    monkeypatch.setattr(routes, "_station_h_roster_candidate_people", roster)
    monkeypatch.setattr(routes, "_schedule_station_h_roster_refresh", roster)
    monkeypatch.setattr(routes, "_build_station_h_duty_focus_service", lambda _c: SimpleNamespace(build_status=focus))
    payload = routes._station_h_status_payload(object(), duty_date="2026-03-01", duty_shift="night")
    assert payload["historical"] is True
    assert payload["selection"]["current_people"] == ["历史值班"]
    assert focus.call_args.kwargs["historical"] is True
    roster.assert_not_called()
