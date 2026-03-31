from contextlib import contextmanager
from types import SimpleNamespace

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


def test_handover_review_data_prefers_session_id_and_returns_history(monkeypatch):
    captured = {}

    class _Parser:
        config = {}

        def parse(self, _output_file):
            return {"fixed_blocks": [], "sections": [], "footer_blocks": []}

    class _Service:
        def get_batch_status(self, batch_key):
            return {"batch_key": batch_key, "duty_date": "2026-03-22", "duty_shift": "day"}

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), _Parser(), None, None))
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
    writer_calls = []
    persisted_default_calls = []
    touched_history_calls = []

    class _Writer:
        def write(self, *, output_file, document):
            writer_calls.append((output_file, document))

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

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, _Writer(), None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
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
        {
            "session_id": "A楼|2026-03-22|day",
            "base_revision": 4,
            "document": {"fixed_blocks": [], "sections": [], "footer_blocks": []},
        },
    )

    assert writer_calls == [("history.xlsx", {"fixed_blocks": [], "sections": [], "footer_blocks": []})]
    assert persisted_default_calls == []
    assert touched_history_calls == [("A楼", "A楼|2026-03-22|day", 4)]
    assert payload["history"]["selected_is_latest"] is False


def test_handover_review_update_cloud_sync_uses_history_session(monkeypatch):
    followup_calls = []

    class _Service:
        def get_batch_status(self, batch_key):
            return {"batch_key": batch_key}

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
        {"session_id": "A楼|2026-03-22|day"},
    )

    assert followup_calls == ["A楼|2026-03-22|day"]
    assert payload["ok"] is True
    assert payload["cloud_sheet_sync"]["status"] == "ok"
