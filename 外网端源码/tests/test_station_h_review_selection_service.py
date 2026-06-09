from app.core.app_state import AppStateRepository
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
