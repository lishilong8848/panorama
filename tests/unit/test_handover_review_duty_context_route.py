from types import SimpleNamespace

from app.modules.handover_review.api import routes


def _fake_request():
    container = SimpleNamespace(add_system_log=lambda *_args, **_kwargs: None)
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_handover_review_data_loads_requested_duty_context(monkeypatch):
    captured = {}

    class _Parser:
        config = {}

        def parse(self, _output_file):
            return {"fixed_blocks": [], "sections": [], "footer_blocks": []}

    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")

    def _fake_loader(_service, **kwargs):
        captured.update(kwargs)
        return {
            "session_id": "A楼|2026-03-24|day",
            "building": "A楼",
            "output_file": "demo.xlsx",
            "batch_key": "2026-03-24|day",
            "duty_date": "2026-03-24",
            "duty_shift": "day",
        }

    monkeypatch.setattr(routes, "_load_target_session_or_404", _fake_loader)

    class _Service:
        def get_batch_status(self, batch_key):
            return {"batch_key": batch_key, "duty_date": "2026-03-24", "duty_shift": "day", "buildings": []}

        def get_latest_session_id(self, building):
            assert building == "A楼"
            return "A楼|2026-03-24|day"

        def list_building_cloud_history_sessions(self, building, *, limit=10):
            assert building == "A楼"
            assert limit == 10
            return [
                {
                    "session_id": "A楼|2026-03-24|day",
                    "building": "A楼",
                    "duty_date": "2026-03-24",
                    "duty_shift": "day",
                    "revision": 1,
                    "confirmed": False,
                    "updated_at": "2026-03-24 08:00:00",
                    "output_file": "demo.xlsx",
                }
            ]

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), _Parser(), None, None))

    class _DocumentState:
        def load_document(self, session):
            hydrated = dict(session)
            hydrated["excel_sync"] = {"status": "synced"}
            return (
                {"fixed_blocks": [], "sections": [], "footer_blocks": []},
                hydrated,
            )

    monkeypatch.setattr(
        routes,
        "_build_review_document_state_service",
        lambda _container, **_kwargs: _DocumentState(),
    )

    payload = routes.handover_review_data(
        "a",
        _fake_request(),
        duty_date="2026-03-24",
        duty_shift="day",
    )

    assert captured == {"building": "A楼", "duty_date": "2026-03-24", "duty_shift": "day", "session_id": ""}
    assert payload["session"]["session_id"] == "A楼|2026-03-24|day"
    assert payload["batch_status"]["batch_key"] == "2026-03-24|day"
    assert payload["history"]["selected_is_latest"] is True
    assert payload["history"]["selected_in_history_list"] is True
    assert payload["history"]["history_limit"] == 10
    assert payload["history"]["history_rule"] == "cloud_success_only"
    assert payload["display_state"]["actions"]["download"]["allowed"] is True
    assert payload["display_state"]["actions"]["confirm"]["allowed"] is True
    assert payload["display_state"]["history_hint"].startswith("仅显示最近 10 条已成功上云")
    assert payload["display_state"]["download_state"]["status"] == "ready"
    assert payload["display_state"]["confirm_state"]["status"] == "pending_confirm"


def test_review_display_disables_confirm_while_cloud_sheet_uploading():
    display = routes._build_review_display_state(
        building="A楼",
        session={
            "session_id": "A楼|2026-03-24|day",
            "building": "A楼",
            "batch_key": "2026-03-24|day",
            "duty_date": "2026-03-24",
            "duty_shift": "day",
            "revision": 2,
            "confirmed": True,
            "output_file": "demo.xlsx",
            "cloud_sheet_sync": {"status": "uploading"},
        },
        batch_status={"batch_key": "2026-03-24|day", "all_confirmed": True},
        concurrency={},
        history={"latest_session_id": "A楼|2026-03-24|day", "selected_is_latest": True},
        defaults_sync={},
        save_status={},
        latest_session_id="A楼|2026-03-24|day",
        client_session_id="A楼|2026-03-24|day",
        client_revision=2,
    )

    assert display["cloud_sheet"]["status"] == "uploading"
    assert display["actions"]["confirm"]["allowed"] is False
    assert "云文档上传中" in display["actions"]["confirm"]["disabled_reason"]
