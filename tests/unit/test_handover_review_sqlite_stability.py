from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from fastapi import BackgroundTasks

from app.modules.handover_review.api import routes
from handover_log_module.service.review_document_state_service import ReviewDocumentStateService


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


def test_load_target_session_allows_missing_output_file_when_session_exists():
    class _Service:
        def get_session_by_id(self, session_id):
            return {
                "session_id": session_id,
                "building": "A楼",
                "output_file": "missing.xlsx",
            }

    session = routes._load_target_session_or_404(
        _Service(),
        building="A楼",
        session_id="A楼|2026-04-15|day",
    )

    assert session["output_file"] == "missing.xlsx"


def test_handover_review_save_returns_ok_when_enqueue_excel_sync_fails(monkeypatch):
    class _Service:
        def get_latest_session_id(self, building):
            assert building == "A楼"
            return "A楼|2026-04-15|day"

        def touch_session_after_save(self, *, building, session_id, base_revision):
            return (
                {
                    "session_id": session_id,
                    "building": building,
                    "revision": base_revision + 1,
                    "updated_at": "2026-04-15 10:00:00",
                    "output_file": "latest.xlsx",
                    "batch_key": "2026-04-15|day",
                },
                {"batch_key": "2026-04-15|day"},
            )

    class _DocumentState:
        def ensure_document_for_session(self, _session):
            return None

        def save_document(self, *, session, document, base_revision, dirty_regions, ensure_ready=True):
            return (
                {"session_id": session["session_id"], "revision": base_revision + 1, "document": document},
                {"session_id": session["session_id"], "revision": base_revision},
            )

        def restore_document(self, *, building, previous):
            raise AssertionError("should not restore on enqueue failure")

        def enqueue_excel_sync(self, session, *, target_revision):
            raise RuntimeError("queue offline")

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_args, **_kwargs: _DocumentState())
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(routes, "_ensure_session_lock_held_or_409", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **_kwargs: {
            "session_id": "A楼|2026-04-15|day",
            "building": "A楼",
            "revision": 3,
            "output_file": "latest.xlsx",
            "batch_key": "2026-04-15|day",
        },
    )
    monkeypatch.setattr(
        routes,
        "_persist_review_defaults",
        lambda *_args, **_kwargs: {"footer_inventory_rows": 0, "cabinet_power_fields": 0, "defaults_updated": False},
    )
    monkeypatch.setattr(
        routes,
        "_queue_capacity_overlay_after_review_save",
        lambda *, saved_session, **_kwargs: (saved_session, False),
    )
    monkeypatch.setattr(routes, "_build_history_payload_safe", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(routes, "_get_session_concurrency_safe", lambda *_args, **_kwargs: {})

    payload = routes.handover_review_save(
        "a",
        _fake_request(),
        _background_tasks(),
        {
            "session_id": "A楼|2026-04-15|day",
            "base_revision": 3,
            "document": {"fixed_blocks": [], "sections": [], "footer_blocks": []},
        },
    )

    assert payload["ok"] is True
    assert payload["revision"] == 4
    assert payload["session"]["excel_sync"]["status"] == "failed"
    assert "后台Excel同步排队失败" in payload["session"]["excel_sync"]["error"]
    assert payload["save_profile"]["queued_excel_sync"] is False


def test_load_review_document_cached_reuses_latest_snapshot(tmp_path: Path):
    output_file = tmp_path / "latest.xlsx"
    output_file.write_text("ok", encoding="utf-8")
    session = {
        "session_id": "A楼|2026-04-17|day",
        "building": "A楼",
        "revision": 2,
        "output_file": str(output_file),
    }

    class _DocumentState(routes.ReviewDocumentStateService):
        def __init__(self):
            self.calls = 0

        def load_document(self, session):
            self.calls += 1
            return (
                {"fixed_blocks": [{"cell": "A1", "value": "cached"}], "sections": [], "footer_blocks": []},
                dict(session),
            )

        def attach_excel_sync(self, session):
            payload = dict(session)
            payload["excel_sync"] = {"status": "synced"}
            return payload

    document_state = _DocumentState()
    with routes._REVIEW_DOCUMENT_CACHE_GUARD:
        routes._REVIEW_DOCUMENT_CACHE.clear()

    first_document, _first_session = routes._load_review_document_cached(document_state, session)
    second_document, second_session = routes._load_review_document_cached(document_state, session)

    assert document_state.calls == 1
    assert first_document == second_document
    assert second_session["excel_sync"]["status"] == "synced"


def test_review_document_state_keeps_sqlite_document_when_excel_fingerprint_changes(tmp_path: Path):
    output_file = tmp_path / "handover.xlsx"
    output_file.write_text("initial", encoding="utf-8")
    config = {"_global_paths": {"runtime_state_root": str(tmp_path / "runtime")}}
    session = {
        "session_id": "A楼|2026-04-29|day",
        "building": "A楼",
        "duty_date": "2026-04-29",
        "duty_shift": "day",
        "batch_key": "2026-04-29|day",
        "revision": 1,
        "output_file": str(output_file),
    }

    class _Parser:
        def __init__(self):
            self.calls = 0

        def parse(self, _path):
            self.calls += 1
            return {
                "fixed_blocks": [{"fields": [{"cell": "A1", "value": "sqlite-source"}]}],
                "sections": [],
                "footer_blocks": [],
            }

    parser = _Parser()
    service = ReviewDocumentStateService(config, parser=parser, emit_log=lambda *_args: None)

    first = service.ensure_document_for_session(session)
    output_file.write_text("changed by excel sync", encoding="utf-8")
    second = service.ensure_document_for_session(session)

    assert parser.calls == 1
    assert second["revision"] == first["revision"]
    assert second["document"]["fixed_blocks"][0]["fields"][0]["value"] == "sqlite-source"
