from __future__ import annotations

from datetime import datetime

from handover_log_module.repository.event_sections_repository import (
    EventRow,
    EventSectionQueryResult,
    EventSectionsRepository,
)
from handover_log_module.service.event_category_payload_builder import EventCategoryPayloadBuilder


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
        "sections": {"new_event": "new", "history_followup": "history"},
        "column_mapping": {
            "resolve_by_header": False,
            "fallback_cols": {
                "event_level": "B",
                "event_time": "C",
                "description": "D",
                "work_window": "E",
                "progress": "F",
                "follower": "G",
            },
        },
        "progress_text": {"done": "已完成", "todo": "未完成"},
        "cache": {"enabled": True, "max_pending": 20000, "max_last_query_ids": 5000},
    }


def _row(
    *,
    record_id: str,
    event_time: datetime,
    event_done_time: datetime | None,
    final_status_text: str = "",
) -> EventRow:
    return EventRow(
        record_id=record_id,
        event_time=event_time,
        event_level="一级",
        description=f"desc-{record_id}",
        building_text="A楼",
        final_status_text=final_status_text,
        excluded_checked=False,
        to_maint=False,
        maint_done_time=None,
        event_done_time=event_done_time,
        raw_fields={},
    )


class _FakeEventRepo:
    def __init__(self, query_result: EventSectionQueryResult) -> None:
        self._query_result = query_result

    def load_current_shift_events(self, **_kwargs) -> EventSectionQueryResult:
        return self._query_result

    @staticmethod
    def get_progress_text(row: EventRow, progress_cfg: dict) -> str:
        done_text = str(progress_cfg.get("done", "已完成")).strip() or "已完成"
        todo_text = str(progress_cfg.get("todo", "未完成")).strip() or "未完成"
        return todo_text if row.event_done_time is None else done_text

    def get_record_by_id(self, *, record_id: str) -> EventRow | None:  # noqa: ARG002
        return None


class _FakeCacheStore:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self._rows = list(rows or [])
        self.list_calls = 0
        self.update_calls: list[dict] = []

    def list_pending_for_building(self, building: str) -> list[dict]:
        self.list_calls += 1
        return list(self._rows)

    def update_building_pending(
        self,
        *,
        building: str,
        pending_rows: list[dict],
        max_pending: int,
        last_query_record_ids: list[str],
        max_last_query_ids: int,
    ) -> None:
        self.update_calls.append(
            {
                "building": building,
                "pending_rows": list(pending_rows),
                "max_pending": max_pending,
                "last_query_record_ids": list(last_query_record_ids),
                "max_last_query_ids": max_last_query_ids,
            }
        )


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


def test_event_sections_repository_builds_historical_open_rows() -> None:
    cfg = _event_sections_cfg()
    rows = [
        {
            "record_id": "cur",
            "fields": {
                "building": "A楼",
                "event_time": "2026-03-14 10:00:00",
                "event_level": "一级",
                "description": "cur",
                "event_done_time": "",
            },
        },
        {
            "record_id": "old-open",
            "fields": {
                "building": "A楼",
                "event_time": "2026-03-14 08:10:00",
                "event_level": "一级",
                "description": "old-open",
                "event_done_time": "",
            },
        },
        {
            "record_id": "old-closed",
            "fields": {
                "building": "A楼",
                "event_time": "2026-03-14 08:15:00",
                "event_level": "一级",
                "description": "old-closed",
                "event_done_time": "2026-03-14 14:00:00",
            },
        },
    ]
    repo = _RepoWithFakeClient(cfg, rows)

    result = repo.load_current_shift_events(
        building="A楼",
        duty_date="2026-03-14",
        duty_shift="day",
        now=datetime(2026, 3, 14, 15, 0, 0),
        emit_log=lambda *_args: None,
    )

    assert [row.record_id for row in result.current_rows] == ["cur"]
    assert [row.record_id for row in result.historical_open_rows] == ["old-open"]
    assert result.shift_start == datetime(2026, 3, 14, 9, 0, 0)
    assert result.shift_end == datetime(2026, 3, 14, 18, 0, 0)


def test_event_category_builder_current_duty_uses_shift_end_for_new_event_progress() -> None:
    cfg = _event_sections_cfg()
    query_result = EventSectionQueryResult(
        current_rows=[
            _row(
                record_id="done",
                event_time=datetime(2026, 3, 14, 10, 0, 0),
                event_done_time=datetime(2026, 3, 14, 18, 0, 0),
            ),
            _row(
                record_id="todo",
                event_time=datetime(2026, 3, 14, 11, 0, 0),
                event_done_time=datetime(2026, 3, 14, 18, 1, 0),
            ),
        ],
        outside_shift_ongoing_rows=[],
        historical_open_rows=[],
        shift_start=datetime(2026, 3, 14, 9, 0, 0),
        shift_end=datetime(2026, 3, 14, 18, 0, 0),
        cfg=cfg,
    )
    cache_store = _FakeCacheStore()
    builder = EventCategoryPayloadBuilder(
        {"event_sections": cfg},
        repository=_FakeEventRepo(query_result),
        cache_store=cache_store,
    )

    payload = builder.build(
        building="A楼",
        duty_date="2026-03-14",
        duty_shift="day",
        follower_text="张三",
        is_current_duty_context=True,
        emit_log=lambda *_args: None,
    )

    assert payload["new"] == [
        {"cells": {"B": "一级", "C": "2026-03-14 10:00:00", "D": "desc-done", "E": "/", "F": "已完成", "G": "张三"}},
        {"cells": {"B": "一级", "C": "2026-03-14 11:00:00", "D": "desc-todo", "E": "/", "F": "未完成", "G": "张三"}},
    ]
    assert payload["history"] == []
    assert cache_store.list_calls == 1
    assert len(cache_store.update_calls) == 1
    assert [row["record_id"] for row in cache_store.update_calls[0]["pending_rows"]] == ["todo"]


def test_event_category_builder_historical_duty_skips_cache_and_uses_historical_open_rows() -> None:
    cfg = _event_sections_cfg()
    query_result = EventSectionQueryResult(
        current_rows=[
            _row(
                record_id="new-todo",
                event_time=datetime(2026, 3, 13, 10, 0, 0),
                event_done_time=None,
            )
        ],
        outside_shift_ongoing_rows=[],
        historical_open_rows=[
            _row(
                record_id="history-open",
                event_time=datetime(2026, 3, 13, 8, 0, 0),
                event_done_time=None,
            )
        ],
        shift_start=datetime(2026, 3, 13, 9, 0, 0),
        shift_end=datetime(2026, 3, 13, 18, 0, 0),
        cfg=cfg,
    )
    cache_store = _FakeCacheStore(rows=[{"record_id": "cached"}])
    builder = EventCategoryPayloadBuilder(
        {"event_sections": cfg},
        repository=_FakeEventRepo(query_result),
        cache_store=cache_store,
    )

    payload = builder.build(
        building="A楼",
        duty_date="2026-03-13",
        duty_shift="day",
        follower_text="李四",
        is_current_duty_context=False,
        emit_log=lambda *_args: None,
    )

    assert payload["new"] == [
        {"cells": {"B": "一级", "C": "2026-03-13 10:00:00", "D": "desc-new-todo", "E": "/", "F": "未完成", "G": "李四"}}
    ]
    assert payload["history"] == [
        {"cells": {"B": "一级", "C": "2026-03-13 08:00:00", "D": "desc-history-open", "E": "/", "F": "未完成", "G": "李四"}}
    ]
    assert cache_store.list_calls == 0
    assert cache_store.update_calls == []
