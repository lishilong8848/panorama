from app.core.app_state import AppStateRepository
from app.modules.handover_review.api import routes as review_routes
from handover_log_module.repository.shift_roster_repository import ShiftRosterRepository
from handover_log_module.service.station_h_review_selection_service import (
    STATION_H_REVIEW_NAMESPACE,
    StationHReviewSelectionService,
    station_h_build_batch_key,
)


def _service(tmp_path):
    repository = AppStateRepository(app_dir=tmp_path)
    repository.ensure_ready()
    return StationHReviewSelectionService({}, app_state_repository=repository), repository


def test_save_selection_filters_long_day_people_from_duty_fields(tmp_path):
    service, _repository = _service(tmp_path)

    saved = service.save_selection(
        duty_date="2026-06-09",
        duty_shift="day",
        current_people="张宇航 梅冰冰",
        next_people="马进宇 张岳军",
        long_day_people="梅冰冰 马进宇 李苏琪",
    )

    assert saved["current_people"] == ["张宇航"]
    assert saved["next_people"] == ["张岳军"]
    assert saved["long_day_people"] == ["梅冰冰", "马进宇", "李苏琪"]
    assert saved["current_people_text"] == "张宇航"
    assert saved["next_people_text"] == "张岳军"


def test_get_selection_sanitizes_legacy_dirty_payload(tmp_path):
    service, repository = _service(tmp_path)
    key = station_h_build_batch_key("2026-06-09", "night")
    repository.put_runtime_kv(
        STATION_H_REVIEW_NAMESPACE,
        key,
        {
            "duty_date": "2026-06-09",
            "duty_shift": "night",
            "batch_key": key,
            "current_people": ["祁金鹰", "高荣"],
            "next_people_text": "李苏琪 张宇航",
            "long_day_people_text": "高荣 李苏琪",
            "source": "manual",
        },
    )

    selection = service.get_selection("2026-06-09", "night")

    assert selection["current_people"] == ["祁金鹰"]
    assert selection["next_people"] == ["张宇航"]
    assert selection["long_day_people"] == ["高荣", "李苏琪"]
    assert selection["current_people_text"] == "祁金鹰"
    assert selection["next_people_text"] == "张宇航"


def test_duty_focus_is_persisted_and_preserved_when_only_people_are_saved(tmp_path):
    service, _repository = _service(tmp_path)
    focus = {
        "date_text": "2026-08-12",
        "shift": "day",
        "rows": [{"building": "A楼", "modes": {"1": "制冷"}, "change_note": "2#→1#"}],
        "checks": {"11": "√", "19": "27.7℃/25.8℃"},
        "signatures": {"handover": {"selection_id": "table:record"}},
    }

    service.save_selection(
        duty_date="2026-08-12",
        duty_shift="day",
        current_people="张宇航",
        next_people="张岳军",
        long_day_people="李苏琪",
        duty_focus=focus,
    )
    service.save_selection(
        duty_date="2026-08-12",
        duty_shift="day",
        current_people="祁金鹰",
        next_people="张岳军",
        long_day_people="李苏琪",
    )

    loaded = service.get_selection("2026-08-12", "day")
    assert loaded["current_people"] == ["祁金鹰"]
    assert loaded["duty_focus"] == focus


def test_shift_roster_cache_only_does_not_call_feishu(tmp_path, monkeypatch):
    repository = ShiftRosterRepository(
        {"_global_paths": {"runtime_state_root": str(tmp_path)}}
    )

    def _unexpected_remote_call(_cfg):
        raise AssertionError("cache-only status lookup must not create a Feishu client")

    monkeypatch.setattr(repository, "_new_client", _unexpected_remote_call)

    assignment = repository.query_assignment(
        building="H楼",
        duty_date="2026-08-12",
        duty_shift="day",
        emit_log=lambda _message: None,
        allow_remote=False,
    )

    assert assignment.source_records == 0
    assert assignment.current_people == ""
    assert assignment.next_people == ""


def test_station_h_status_returns_pending_without_remote_lookup(monkeypatch):
    class _SelectionService:
        @staticmethod
        def get_selection(_duty_date, _duty_shift):
            return None

        @staticmethod
        def resolve_selection(*, duty_date, duty_shift):
            return {
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "current_people": [],
                "next_people": [],
                "long_day_people": [],
                "source": "",
            }

    observed = {}
    monkeypatch.setattr(review_routes, "_handover_cfg", lambda _container: {})
    monkeypatch.setattr(
        review_routes,
        "_build_station_h_review_selection_service",
        lambda _container: _SelectionService(),
    )

    def _cache_only_candidates(
        _container,
        *,
        handover_cfg,
        duty_date,
        duty_shift,
        allow_remote,
    ):
        observed["allow_remote"] = allow_remote
        return [], [], [], "", 0

    monkeypatch.setattr(
        review_routes,
        "_station_h_roster_candidate_people",
        _cache_only_candidates,
    )
    monkeypatch.setattr(
        review_routes,
        "_schedule_station_h_roster_refresh",
        lambda *_args, **_kwargs: True,
    )

    payload = review_routes._station_h_status_payload(
        object(),
        duty_date="2026-08-12",
        duty_shift="day",
    )

    assert observed["allow_remote"] is False
    assert payload["ok"] is True
    assert payload["candidate_source"]["refresh_pending"] is True
    assert payload["candidate_source"]["refreshing"] is True
    assert payload["candidate_source"]["retry_after_ms"] == 4000


def test_station_h_roster_background_refresh_is_deduplicated(monkeypatch):
    submitted = []

    class _Executor:
        @staticmethod
        def submit(callback):
            submitted.append(callback)
            return object()

    monkeypatch.setattr(review_routes, "_STATION_H_ROSTER_REFRESH_EXECUTOR", _Executor())
    with review_routes._STATION_H_ROSTER_REFRESH_LOCK:
        review_routes._STATION_H_ROSTER_REFRESH_INFLIGHT.clear()
        review_routes._STATION_H_ROSTER_REFRESH_LAST_STARTED.clear()
        review_routes._STATION_H_ROSTER_REFRESH_ERRORS.clear()

    first = review_routes._schedule_station_h_roster_refresh(
        object(),
        handover_cfg={},
        duty_date="2026-08-12",
        duty_shift="day",
    )
    second = review_routes._schedule_station_h_roster_refresh(
        object(),
        handover_cfg={},
        duty_date="2026-08-12",
        duty_shift="day",
    )

    assert first is True
    assert second is True
    assert len(submitted) == 1

    with review_routes._STATION_H_ROSTER_REFRESH_LOCK:
        review_routes._STATION_H_ROSTER_REFRESH_INFLIGHT.clear()
        review_routes._STATION_H_ROSTER_REFRESH_LAST_STARTED.clear()
        review_routes._STATION_H_ROSTER_REFRESH_ERRORS.clear()
