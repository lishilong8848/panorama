from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.modules.handover_review.api import routes


class _BrokenService:
    def get_latest_session_id(self, building):
        assert building == "A楼"
        return "A楼|2026-03-25|night"

    def list_building_cloud_history_sessions(self, building, *, limit=10):
        raise RuntimeError("boom")


def test_build_history_payload_safe_degrades_when_history_listing_fails():
    logs = []

    payload = routes._build_history_payload_safe(
        _BrokenService(),
        building="A楼",
        selected_session_id="A楼|2026-03-25|night",
        emit_log=logs.append,
    )

    assert payload["latest_session_id"] == "A楼|2026-03-25|night"
    assert payload["selected_session_id"] == "A楼|2026-03-25|night"
    assert payload["selected_is_latest"] is True
    assert payload["selected_in_history_list"] is False
    assert payload["history_limit"] == 10
    assert payload["history_rule"] == "cloud_success_only"
    assert payload["sessions"] == []
    assert payload["degraded"] is True
    assert payload["error"] == "history_unavailable"
    assert logs
    assert "已降级" in logs[0]
