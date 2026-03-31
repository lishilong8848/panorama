from app.modules.handover_review.api import routes


class _Service:
    def __init__(self, sessions, session_map=None):
        self._sessions = sessions
        self._session_map = session_map or {}

    def get_latest_session_id(self, building):
        assert building == "A楼"
        return "A楼|2026-03-24|night"

    def list_building_cloud_history_sessions(self, building, *, limit=10):
        assert building == "A楼"
        assert limit == 10
        return self._sessions[:limit]

    def get_session_by_id(self, session_id):
        return self._session_map.get(session_id)


def test_build_history_payload_marks_selected_latest_when_not_in_history_list():
    payload = routes._build_history_payload(
        _Service(
            [
                {
                    "session_id": "A楼|2026-03-23|night",
                    "building": "A楼",
                    "duty_date": "2026-03-23",
                    "duty_shift": "night",
                    "revision": 2,
                    "confirmed": True,
                    "updated_at": "2026-03-23 20:00:00",
                    "output_file": "A楼_20260323.xlsx",
                }
            ],
            session_map={
                "A楼|2026-03-24|night": {
                    "session_id": "A楼|2026-03-24|night",
                    "building": "A楼",
                    "duty_date": "2026-03-24",
                    "duty_shift": "night",
                    "output_file": "missing.xlsx",
                    "cloud_sheet_sync": {"status": "pending_upload", "spreadsheet_url": ""},
                }
            },
        ),
        building="A楼",
        selected_session_id="A楼|2026-03-24|night",
    )

    assert payload["latest_session_id"] == "A楼|2026-03-24|night"
    assert payload["selected_session_id"] == "A楼|2026-03-24|night"
    assert payload["selected_is_latest"] is True
    assert payload["selected_in_history_list"] is False
    assert payload["selected_history_excluded_reason"] == "not_cloud_success"
    assert payload["history_limit"] == 10
    assert payload["history_rule"] == "cloud_success_only"
    assert [item["session_id"] for item in payload["sessions"]] == ["A楼|2026-03-23|night"]


def test_build_history_payload_marks_selected_in_history_list_when_present():
    payload = routes._build_history_payload(
        _Service(
            [
                {
                    "session_id": "A楼|2026-03-24|night",
                    "building": "A楼",
                    "duty_date": "2026-03-24",
                    "duty_shift": "night",
                    "revision": 3,
                    "confirmed": True,
                    "updated_at": "2026-03-24 08:00:00",
                    "output_file": "A楼_20260324.xlsx",
                }
            ],
            session_map={
                "A楼|2026-03-24|night": {
                    "session_id": "A楼|2026-03-24|night",
                    "building": "A楼",
                    "duty_date": "2026-03-24",
                    "duty_shift": "night",
                    "output_file": "A楼_20260324.xlsx",
                    "cloud_sheet_sync": {"status": "success", "spreadsheet_url": "https://example.com/latest"},
                }
            },
        ),
        building="A楼",
        selected_session_id="A楼|2026-03-24|night",
    )

    assert payload["selected_is_latest"] is True
    assert payload["selected_in_history_list"] is True
    assert payload["selected_history_excluded_reason"] == ""
    assert payload["sessions"][0]["label"] == "最新 2026-03-24 / 夜班"
