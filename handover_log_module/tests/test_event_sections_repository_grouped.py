from __future__ import annotations

from datetime import datetime

from handover_log_module.repository.event_sections_repository import EventSectionsRepository


def _event_sections_cfg() -> dict:
    return {
        "enabled": True,
        "source": {"app_token": "app", "table_id": "tbl", "page_size": 500, "max_records": 5000},
        "fields": {
            "building": "building",
            "exclude_checked": "exclude",
            "final_status": "final_status",
            "event_time": "event_time",
            "event_level": "event_level",
            "description": "description",
            "to_maint": "to_maint",
            "maint_done_time": "maint_done_time",
            "event_done_time": "event_done_time",
        },
        "duty_window": {
            "day_start": "09:00:00",
            "day_end": "18:00:00",
            "night_start": "18:00:00",
            "night_end_next_day": "09:00:00",
        },
        "cache": {"enabled": True, "max_pending": 20000, "max_last_query_ids": 5000},
    }


class _RepoWithFakeClient(EventSectionsRepository):
    def __init__(self, cfg: dict, rows: list[dict]) -> None:
        super().__init__({"event_sections": cfg})
        self._rows = rows

    def _new_client(self, cfg):  # noqa: ANN001, ARG002
        class _Client:
            def __init__(self, rows: list[dict]) -> None:
                self._rows = rows

            def list_records(self, **_kwargs):
                return list(self._rows)

        return _Client(self._rows)


def test_load_current_shift_events_grouped_buckets_rows_by_building() -> None:
    repo = _RepoWithFakeClient(
        _event_sections_cfg(),
        [
            {
                "record_id": "cur-a",
                "fields": {
                    "building": "A楼",
                    "event_time": "2026-03-14 10:00:00",
                    "event_level": "一级",
                    "description": "cur-a",
                    "event_done_time": "",
                },
            },
            {
                "record_id": "history-a",
                "fields": {
                    "building": "A楼",
                    "event_time": "2026-03-14 08:20:00",
                    "event_level": "一级",
                    "description": "history-a",
                    "event_done_time": "",
                },
            },
            {
                "record_id": "ongoing-c",
                "fields": {
                    "building": "C楼",
                    "event_time": "2026-03-14 07:50:00",
                    "event_level": "一级",
                    "description": "ongoing-c",
                    "final_status": "事件闭环转检修中",
                    "event_done_time": "",
                },
            },
        ],
    )

    grouped = repo.load_current_shift_events_grouped(
        buildings=["A楼", "C楼"],
        duty_date="2026-03-14",
        duty_shift="day",
        now=datetime(2026, 3, 14, 15, 0, 0),
        emit_log=lambda *_args: None,
    )

    assert [row.record_id for row in grouped["A楼"].current_rows] == ["cur-a"]
    assert [row.record_id for row in grouped["A楼"].historical_open_rows] == ["history-a"]
    assert [row.record_id for row in grouped["C楼"].outside_shift_ongoing_rows] == ["ongoing-c"]
    assert grouped["A楼"].shift_start == datetime(2026, 3, 14, 9, 0, 0)
    assert grouped["A楼"].shift_end == datetime(2026, 3, 14, 18, 0, 0)


def test_load_current_shift_events_grouped_does_not_cross_assign_other_buildings() -> None:
    repo = _RepoWithFakeClient(
        _event_sections_cfg(),
        [
            {
                "record_id": "a-only",
                "fields": {
                    "building": "A楼",
                    "event_time": "2026-03-14 10:00:00",
                    "event_level": "一级",
                    "description": "a-only",
                    "event_done_time": "",
                },
            },
            {
                "record_id": "b-only",
                "fields": {
                    "building": "B楼",
                    "event_time": "2026-03-14 10:10:00",
                    "event_level": "一级",
                    "description": "b-only",
                    "event_done_time": "",
                },
            },
        ],
    )

    grouped = repo.load_current_shift_events_grouped(
        buildings=["A楼", "C楼"],
        duty_date="2026-03-14",
        duty_shift="day",
        now=datetime(2026, 3, 14, 15, 0, 0),
        emit_log=lambda *_args: None,
    )

    assert [row.record_id for row in grouped["A楼"].current_rows] == ["a-only"]
    assert grouped["C楼"].current_rows == []
    assert grouped["C楼"].historical_open_rows == []
    assert grouped["C楼"].outside_shift_ongoing_rows == []
