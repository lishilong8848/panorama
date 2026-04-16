from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi import BackgroundTasks, HTTPException

from app.modules.handover_review.api import routes


class _DummyJobService:
    @contextmanager
    def resource_guard(self, **_kwargs):
        yield


def _fake_request():
    container = SimpleNamespace(
        add_system_log=lambda *_args, **_kwargs: None,
        config={},
        config_path="config.json",
        reload_config=lambda _cfg: None,
        job_service=_DummyJobService(),
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def _background_tasks():
    return BackgroundTasks()


def test_handover_review_data_prefers_session_id_and_returns_history(monkeypatch):
    captured = {}

    class _Parser:
        config = {}

        def parse(self, _output_file):
            return {"fixed_blocks": [], "sections": [], "footer_blocks": []}

    class _Service:
        def get_batch_status(self, batch_key):
            return {"batch_key": batch_key, "duty_date": "2026-03-22", "duty_shift": "day"}

    class _DocumentState:
        def load_document(self, session):
            return (
                {"fixed_blocks": [], "sections": [], "footer_blocks": []},
                {
                    **session,
                    "excel_sync": {"status": "synced", "synced_revision": 1, "pending_revision": 0, "error": ""},
                },
            )

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), _Parser(), None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: _DocumentState())
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")

    def _fake_loader(_service, **kwargs):
        captured.update(kwargs)
        return {
            "session_id": "A楼|2026-03-22|day",
            "building": "A楼",
            "output_file": "demo.xlsx",
            "batch_key": "2026-03-22|day",
        }

    monkeypatch.setattr(routes, "_load_target_session_or_404", _fake_loader)
    monkeypatch.setattr(
        routes,
        "_build_history_payload",
        lambda _service, **_kwargs: {
            "latest_session_id": "A楼|2026-03-23|night",
            "selected_session_id": "A楼|2026-03-22|day",
            "selected_is_latest": False,
            "sessions": [{"session_id": "A楼|2026-03-22|day"}],
        },
    )

    payload = routes.handover_review_data(
        "a",
        _fake_request(),
        duty_date="2026-03-24",
        duty_shift="night",
        session_id="A楼|2026-03-22|day",
    )

    assert captured == {
        "building": "A楼",
        "duty_date": "2026-03-24",
        "duty_shift": "night",
        "session_id": "A楼|2026-03-22|day",
    }
    assert payload["history"]["selected_session_id"] == "A楼|2026-03-22|day"
    assert payload["history"]["selected_is_latest"] is False


def test_handover_review_save_history_skips_default_persistence(monkeypatch):
    persisted_default_calls = []
    touched_history_calls = []

    class _Service:
        def get_latest_session_id(self, building):
            assert building == "A楼"
            return "A楼|2026-03-23|night"

        def touch_session_after_history_save(self, *, building, session_id, base_revision):
            touched_history_calls.append((building, session_id, base_revision))
            return (
                {
                    "session_id": session_id,
                    "building": building,
                    "revision": base_revision + 1,
                    "updated_at": "2026-03-23 21:00:00",
                    "output_file": "history.xlsx",
                    "batch_key": "2026-03-22|day",
                },
                {"batch_key": "2026-03-22|day"},
            )

        def touch_session_after_save(self, **_kwargs):
            raise AssertionError("latest save branch should not run")

    class _DocumentState:
        def ensure_document_for_session(self, _session):
            return None

        def save_document(self, *, session, document, base_revision, dirty_regions):
            return (
                {"session_id": session["session_id"], "revision": base_revision + 1, "document": document},
                {"session_id": session["session_id"], "revision": base_revision},
            )

        def restore_document(self, *, building, previous):
            raise AssertionError("history save should not restore document")

        def enqueue_excel_sync(self, session, *, target_revision):
            return {
                "status": "pending",
                "synced_revision": target_revision - 1,
                "pending_revision": target_revision,
                "error": "",
                "updated_at": "",
            }

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: _DocumentState())
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(routes, "_ensure_session_lock_held_or_409", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **_kwargs: {
            "session_id": "A楼|2026-03-22|day",
            "building": "A楼",
            "revision": 4,
            "output_file": "history.xlsx",
            "batch_key": "2026-03-22|day",
        },
    )
    monkeypatch.setattr(
        routes,
        "_persist_review_defaults",
        lambda *_args, **_kwargs: persisted_default_calls.append(True),
    )
    monkeypatch.setattr(
        routes,
        "_build_history_payload",
        lambda _service, **_kwargs: {
            "latest_session_id": "A楼|2026-03-23|night",
            "selected_session_id": "A楼|2026-03-22|day",
            "selected_is_latest": False,
            "sessions": [],
        },
    )

    payload = routes.handover_review_save(
        "a",
        _fake_request(),
        _background_tasks(),
        {
            "session_id": "A楼|2026-03-22|day",
            "base_revision": 4,
            "document": {"fixed_blocks": [], "sections": [], "footer_blocks": []},
        },
    )

    assert persisted_default_calls == []
    assert touched_history_calls == [("A楼", "A楼|2026-03-22|day", 4)]
    assert payload["history"]["selected_is_latest"] is False


def test_handover_review_save_latest_passes_dirty_regions_and_returns_save_profile(monkeypatch):
    persisted_defaults_calls = []
    touched_latest_calls = []

    class _Service:
        def get_latest_session_id(self, building):
            assert building == "A楼"
            return "A楼|2026-03-23|night"

        def touch_session_after_save(self, *, building, session_id, base_revision):
            touched_latest_calls.append((building, session_id, base_revision))
            return (
                {
                    "session_id": session_id,
                    "building": building,
                    "revision": base_revision + 1,
                    "updated_at": "2026-03-23 22:00:00",
                    "output_file": "latest.xlsx",
                    "batch_key": "2026-03-23|night",
                },
                {"batch_key": "2026-03-23|night"},
            )

        def touch_session_after_history_save(self, **_kwargs):
            raise AssertionError("history save branch should not run")

    class _DocumentState:
        def ensure_document_for_session(self, _session):
            return None

        def save_document(self, *, session, document, base_revision, dirty_regions):
            return (
                {"session_id": session["session_id"], "revision": base_revision + 1, "document": document},
                {"session_id": session["session_id"], "revision": base_revision},
            )

        def restore_document(self, *, building, previous):
            raise AssertionError("latest save should not restore document")

        def enqueue_excel_sync(self, session, *, target_revision):
            return {
                "status": "pending",
                "synced_revision": target_revision - 1,
                "pending_revision": target_revision,
                "error": "",
                "updated_at": "",
            }

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: _DocumentState())
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(routes, "_ensure_session_lock_held_or_409", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **_kwargs: {
            "session_id": "A楼|2026-03-23|night",
            "building": "A楼",
            "revision": 7,
            "output_file": "latest.xlsx",
            "batch_key": "2026-03-23|night",
        },
    )
    monkeypatch.setattr(
        routes,
        "_persist_review_defaults",
        lambda *_args, **kwargs: persisted_defaults_calls.append(kwargs.get("dirty_regions")) or {
            "footer_inventory_rows": 1,
            "cabinet_power_fields": 4,
            "config_updated": False,
            "config_sync_required": True,
            "config_building_code": "A",
            "config_data": {"review_ui": {"cabinet_power_defaults_by_building": {"A楼": {"cells": {"B13": "10"}}}}},
        },
    )
    monkeypatch.setattr(
        routes,
        "_build_history_payload",
        lambda _service, **_kwargs: {
            "latest_session_id": "A楼|2026-03-23|night",
            "selected_session_id": "A楼|2026-03-23|night",
            "selected_is_latest": True,
            "sessions": [],
        },
    )

    payload = routes.handover_review_save(
        "a",
        _fake_request(),
        _background_tasks(),
        {
            "session_id": "A楼|2026-03-23|night",
            "base_revision": 7,
            "document": {"fixed_blocks": [], "sections": [], "footer_blocks": []},
            "dirty_regions": {"fixed_blocks": True, "sections": False, "footer_inventory": False},
        },
    )

    assert persisted_defaults_calls == [{"fixed_blocks": True, "sections": False, "footer_inventory": False}]
    assert touched_latest_calls == [("A楼", "A楼|2026-03-23|night", 7)]
    assert payload["history"]["selected_is_latest"] is True
    assert {"write_ms", "defaults_ms", "session_ms", "total_ms"}.issubset(payload["save_profile"].keys())
    assert all(isinstance(payload["save_profile"][key], int) for key in {"write_ms", "defaults_ms", "session_ms", "total_ms"})
    assert payload["save_profile"]["defaults_config_async"] is True
    assert payload["save_profile"]["defaults_config_status"] == "queued"


def test_handover_review_update_cloud_sync_uses_history_session(monkeypatch):
    followup_calls = []

    class _Service:
        def get_batch_status(self, batch_key):
            return {"batch_key": batch_key}

        def get_session_concurrency(self, **_kwargs):
            return {"client_holds_lock": True}

    class _Followup:
        def force_update_cloud_sheet_for_session(self, session_id, emit_log):
            followup_calls.append(session_id)
            emit_log(f"cloud-update:{session_id}")
            return {
                "status": "ok",
                "session": {
                    "session_id": session_id,
                    "building": "A楼",
                    "batch_key": "2026-03-22|day",
                },
                "batch_status": {"batch_key": "2026-03-22|day"},
                "cloud_sheet_sync": {"status": "ok"},
            }

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, None, _Followup()))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **_kwargs: {
            "session_id": "A楼|2026-03-22|day",
            "building": "A楼",
            "batch_key": "2026-03-22|day",
            "output_file": "history.xlsx",
        },
    )
    monkeypatch.setattr(
        routes,
        "_build_history_payload",
        lambda _service, **_kwargs: {
            "latest_session_id": "A楼|2026-03-23|night",
            "selected_session_id": "A楼|2026-03-22|day",
            "selected_is_latest": False,
            "sessions": [],
        },
    )

    payload = routes.handover_review_update_cloud_sync(
        "a",
        _fake_request(),
        {"session_id": "A楼|2026-03-22|day", "client_id": "review-a001"},
    )

    assert followup_calls == ["A楼|2026-03-22|day"]
    assert payload["ok"] is True
    assert payload["cloud_sheet_sync"]["status"] == "ok"


def test_persist_review_defaults_writes_back_building_segment_for_latest_session(monkeypatch):
    captured = {}

    class _DocumentState:
        def persist_defaults_from_document(self, *, building, document, dirty_regions):
            assert building == "A楼"
            assert dirty_regions == {"fixed_blocks": True, "sections": False, "footer_inventory": True}
            return {
                "footer_inventory_rows": 1,
                "cabinet_power_fields": 4,
                "defaults_updated": True,
            }

    container = SimpleNamespace(
        config={},
        config_path="config.json",
        reload_config=lambda cfg: captured.setdefault("reloaded", cfg),
    )
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: _DocumentState())
    monkeypatch.setattr(
        routes,
        "get_handover_building_segment",
        lambda building_code, config_path: {
            "revision": 5,
            "data": {"review_ui": {}},
        },
    )

    result = routes._persist_review_defaults(
        container,
        building="A楼",
        document={
            "fixed_blocks": [
                {
                    "fields": [
                        {"cell": "B13", "value": "10"},
                        {"cell": "D13", "value": "11"},
                        {"cell": "F13", "value": "12"},
                        {"cell": "H13", "value": "13"},
                    ]
                }
            ],
            "sections": [],
            "footer_blocks": [
                {
                    "type": "inventory_table",
                    "rows": [
                        {
                            "cells": {
                                "B": "对讲机",
                                "C": "值班室",
                                "E": "5",
                                "F": "否",
                                "G": "无",
                                "H": "不应写回",
                            }
                        }
                    ],
                }
            ],
        },
        dirty_regions={"fixed_blocks": True, "sections": False, "footer_inventory": True},
    )

    assert result["defaults_updated"] is True
    assert result["config_updated"] is False
    assert result["config_sync_required"] is True
    assert result["config_building_code"] == "A"
    review_ui = result["config_data"]["review_ui"]
    assert review_ui["cabinet_power_defaults_by_building"]["A楼"]["cells"] == {
        "B13": "10",
        "D13": "11",
        "F13": "12",
        "H13": "13",
    }
    assert review_ui["footer_inventory_defaults_by_building"]["A楼"]["rows"][0]["cells"] == {
        "B": "对讲机",
        "C": "值班室",
        "E": "5",
        "F": "否",
        "G": "无",
    }
    assert "reloaded" not in captured


def test_persist_review_defaults_skips_building_segment_write_when_only_sections_dirty(monkeypatch):
    class _DocumentState:
        def persist_defaults_from_document(self, *, building, document, dirty_regions):
            assert building == "A楼"
            assert dirty_regions == {"fixed_blocks": False, "sections": True, "footer_inventory": False}
            return {
                "footer_inventory_rows": 0,
                "cabinet_power_fields": 0,
                "defaults_updated": False,
            }

    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: _DocumentState())
    monkeypatch.setattr(
        routes,
        "save_handover_building_segment",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not write building segment")),
    )

    result = routes._persist_review_defaults(
        SimpleNamespace(config={}, config_path="config.json", reload_config=lambda _cfg: None),
        building="A楼",
        document={"fixed_blocks": [], "sections": [], "footer_blocks": []},
        dirty_regions={"fixed_blocks": False, "sections": True, "footer_inventory": False},
    )

    assert result["defaults_updated"] is False
    assert result["config_updated"] is False


def test_handover_review_status_returns_lightweight_payload(monkeypatch):
    class _Parser:
        config = {"review_ui": {"poll_interval_sec": 9}}

    class _Service:
        def get_batch_status(self, batch_key):
            return {"batch_key": batch_key}

    class _DocumentState:
        def attach_excel_sync(self, session):
            return {**session, "excel_sync": {"status": "synced"}}

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), _Parser(), None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: _DocumentState())
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **_kwargs: {
            "session_id": "A楼|2026-03-23|night",
            "building": "A楼",
            "revision": 8,
            "batch_key": "2026-03-23|night",
        },
    )
    monkeypatch.setattr(routes, "_get_session_concurrency_safe", lambda *_args, **_kwargs: {"client_holds_lock": True})
    monkeypatch.setattr(routes, "_build_history_payload_safe", lambda *_args, **_kwargs: {"sessions": [], "selected_session_id": "A楼|2026-03-23|night"})

    payload = routes.handover_review_status("a", _fake_request(), client_id="review-a001")

    assert payload["ok"] is True
    assert payload["building"] == "A楼"
    assert "document" not in payload
    assert payload["review_ui"]["poll_interval_sec"] == 9
    assert payload["session"]["excel_sync"]["status"] == "synced"


def test_handover_review_save_requires_active_lock(monkeypatch):
    class _Service:
        def get_session_concurrency(self, **_kwargs):
            return {
                "client_holds_lock": False,
                "active_editor": {"client_id": "review-other"},
                "current_revision": 3,
            }

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **_kwargs: {
            "session_id": "A楼|2026-03-23|night",
            "building": "A楼",
            "revision": 3,
            "batch_key": "2026-03-23|night",
            "output_file": "latest.xlsx",
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_save(
            "a",
            _fake_request(),
            _background_tasks(),
            {
                "session_id": "A楼|2026-03-23|night",
                "base_revision": 3,
                "client_id": "review-b001",
                "document": {"fixed_blocks": [], "sections": [], "footer_blocks": []},
            },
        )

    assert exc_info.value.status_code == 409
    assert "其他终端编辑" in str(exc_info.value.detail)


def test_handover_review_confirm_requires_active_lock(monkeypatch):
    class _Service:
        def get_session_by_id(self, session_id):
            return {
                "session_id": session_id,
                "building": "A楼",
                "revision": 3,
                "batch_key": "2026-03-23|night",
                "output_file": "latest.xlsx",
            }

        def get_latest_session_id(self, _building):
            return "A楼|2026-03-23|night"

        def get_session_concurrency(self, **_kwargs):
            return {"client_holds_lock": False}

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **_kwargs: {
            "session_id": "A楼|2026-03-23|night",
            "building": "A楼",
            "revision": 3,
            "batch_key": "2026-03-23|night",
            "output_file": "latest.xlsx",
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_confirm(
            "a",
            _fake_request(),
            {
                "session_id": "A楼|2026-03-23|night",
                "base_revision": 3,
                "client_id": "review-b001",
            },
        )

    assert exc_info.value.status_code == 409
    assert "其他终端编辑" in str(exc_info.value.detail)


def test_handover_review_unconfirm_requires_active_lock(monkeypatch):
    class _Service:
        def get_session_by_id(self, session_id):
            return {
                "session_id": session_id,
                "building": "A楼",
                "revision": 4,
                "confirmed": True,
                "batch_key": "2026-03-23|night",
                "output_file": "latest.xlsx",
            }

        def get_latest_session_id(self, _building):
            return "A楼|2026-03-23|night"

        def get_session_concurrency(self, **_kwargs):
            return {"client_holds_lock": False}

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **_kwargs: {
            "session_id": "A楼|2026-03-23|night",
            "building": "A楼",
            "revision": 4,
            "confirmed": True,
            "batch_key": "2026-03-23|night",
            "output_file": "latest.xlsx",
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_unconfirm(
            "a",
            _fake_request(),
            {
                "session_id": "A楼|2026-03-23|night",
                "base_revision": 4,
                "client_id": "review-b001",
            },
        )

    assert exc_info.value.status_code == 409
    assert "其他终端编辑" in str(exc_info.value.detail)


def test_handover_review_cloud_sync_update_requires_active_lock(monkeypatch):
    class _Service:
        def get_session_concurrency(self, **_kwargs):
            return {"client_holds_lock": False}

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, None, SimpleNamespace()))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **_kwargs: {
            "session_id": "A楼|2026-03-22|day",
            "building": "A楼",
            "batch_key": "2026-03-22|day",
            "output_file": "history.xlsx",
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_update_cloud_sync(
            "a",
            _fake_request(),
            {
                "session_id": "A楼|2026-03-22|day",
                "client_id": "review-b001",
            },
        )

    assert exc_info.value.status_code == 409
    assert "其他终端编辑" in str(exc_info.value.detail)
