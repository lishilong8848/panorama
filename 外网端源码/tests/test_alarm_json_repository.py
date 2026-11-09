from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

from handover_log_module.repository.alarm_json_repository import AlarmJsonRepository
from handover_log_module.service.handover_orchestrator import HandoverOrchestrator


def _write_alarm_json(
    path: Path,
    *,
    building: str,
    query_start: str,
    query_end: str,
    rows: list[dict] | None = None,
    bucket_kind: str = "handover_window",
) -> None:
    payload_rows = rows or []
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source_family": "alarm_event_family",
                "building": building,
                "bucket_kind": bucket_kind,
                "bucket_key": "test",
                "generated_at": query_end,
                "query_start": query_start,
                "query_end": query_end,
                "row_count": len(payload_rows),
                "count_summary": {},
                "rows": payload_rows,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


class AlarmJsonRepositoryTests(unittest.TestCase):
    def test_query_uses_handover_window_snapshot_for_night_shift(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fresh = root / "fresh.json"
            _write_alarm_json(
                fresh,
                building="A楼",
                query_start="2026-04-01 08:05:00",
                query_end="2026-05-31 09:05:00",
                rows=[
                    {
                        "event_time": "2026-05-31 07:00:00",
                        "is_recover": "0",
                        "accept_content": "测试告警",
                    }
                ],
            )
            repo = AlarmJsonRepository(
                {
                    "_deployment_role_mode": "external",
                    "_shared_bridge": {"root_dir": str(root)},
                    "sites": [{"building": "A楼"}],
                }
            )
            repo._http_alarm_window_entries = lambda *, buildings, duty_date, duty_shift: [  # type: ignore[method-assign]
                {
                    "building": "A楼",
                    "bucket_kind": "handover_window",
                    "bucket_key": "2026-05-31 08",
                    "downloaded_at": "2026-05-31 08:05:00",
                    "file_path": str(fresh),
                },
            ]

            summary = repo.query_alarm_summary(
                building="A楼",
                start_time="2026-05-30 18:00:00",
                end_time="2026-05-31 09:00:00",
                emit_log=lambda _line: None,
            )

            self.assertEqual(summary.query_end, "2026-05-31 09:05:00")
            self.assertEqual(summary.total_count, 1)
            self.assertEqual(summary.unrecovered_count, 1)

    def test_ensure_window_coverage_requests_exact_window_task(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.calls: list[dict] = []

            def create_alarm_event_window_query_task(self, **kwargs):
                self.calls.append(kwargs)
                return {"task_id": "task-1", "status": "success"}

            def get_task(self, task_id: str):
                return {"task_id": task_id, "status": "success"}

        fake = FakeClient()
        repo = AlarmJsonRepository(
            {
                "_deployment_role_mode": "external",
                "_shared_bridge": {"root_dir": "D:\\share"},
                "sites": [{"building": "A楼"}, {"building": "B楼"}],
            }
        )
        repo._http_client = lambda: fake  # type: ignore[method-assign]
        repo._http_alarm_window_entries = lambda *, buildings, duty_date, duty_shift: []  # type: ignore[method-assign]

        snapshot = repo.ensure_window_coverage(
            buildings=["A楼", "B楼"],
            start_time="2026-05-30 18:00:00",
            end_time="2026-05-31 09:00:00",
            duty_date="2026-05-30",
            duty_shift="night",
            emit_log=lambda _line: None,
        )

        self.assertEqual(snapshot["selection_policy"], "handover_window_exact")
        self.assertEqual(len(fake.calls), 1)
        self.assertEqual(fake.calls[0]["buildings"], ["A楼", "B楼"])
        self.assertEqual(fake.calls[0]["query_start"], "2026-05-30 18:00:00")
        self.assertEqual(fake.calls[0]["query_end"], "2026-05-31 09:00:00")
        self.assertEqual(fake.calls[0]["duty_date"], "2026-05-30")
        self.assertEqual(fake.calls[0]["duty_shift"], "night")

    def test_handover_alarm_window_ignores_legacy_shift_window_config(self) -> None:
        service = HandoverOrchestrator(
            {
                "download": {
                    "shift_windows": {
                        "day": {"start": "08:00:00", "end": "17:00:00"},
                        "night": {"start": "17:00:00", "end_next_day": "08:00:00"},
                    }
                }
            }
        )

        day_window = service._build_alarm_duty_window(duty_date="2026-05-31", duty_shift="day")
        night_window = service._build_alarm_duty_window(duty_date="2026-05-31", duty_shift="night")

        self.assertEqual(day_window.start_time, "2026-05-31 09:00:00")
        self.assertEqual(day_window.end_time, "2026-05-31 18:00:00")
        self.assertEqual(night_window.start_time, "2026-05-31 18:00:00")
        self.assertEqual(night_window.end_time, "2026-06-01 09:00:00")

    def test_early_generation_caps_window_and_reloads_replaced_file(self) -> None:
        class Clock(datetime):
            current = datetime(2026, 11, 9, 16, 5)

            @classmethod
            def now(cls, tz=None):
                return cls.current

        start, end = "2026-11-09 09:00:00", "2026-11-09 18:00:00"
        with tempfile.TemporaryDirectory() as tmp, patch(
            "handover_log_module.repository.alarm_json_repository.datetime", Clock
        ):
            path = Path(tmp) / "window.json"
            _write_alarm_json(path, building="A楼", query_start=start, query_end="2026-11-09 15:00:00")
            repo = AlarmJsonRepository({})
            repo.build_window_selection_snapshot = Mock(return_value={
                "duty_date": "2026-11-09", "duty_shift": "day",
                "selected_by_building": {"A楼": {"file_path": str(path), "source_kind": "handover_window"}},
            })

            def refresh(**kwargs):
                self.assertEqual(kwargs["end_dt"], datetime(2026, 11, 9, 16, 5))
                _write_alarm_json(path, building="A楼", query_start=start, query_end="2026-11-09 16:05:00", rows=[
                    {"event_time": "2026-11-09 16:04:00", "is_recover": "0"},
                    {"event_time": "2026-11-09 17:00:00", "is_recover": "0"},
                ])
                return True

            repo._request_alarm_refresh_for_window = Mock(side_effect=refresh)
            cache = {}
            snapshot = repo.ensure_window_coverage(buildings=["A楼"], start_time=start, end_time=end,
                                                   emit_log=lambda _: None, document_cache=cache)
            Clock.current = datetime(2026, 11, 9, 16, 6)
            summary = repo.query_alarm_summary(building="A楼", start_time=start, end_time=end,
                                               emit_log=lambda _: None, selection_snapshot=snapshot, document_cache=cache)
            self.assertEqual(summary.total_count, 1)
            self.assertFalse(summary.coverage_ok)
            repo._request_alarm_refresh_for_window.assert_called_once()

    def test_unavailable_batch_does_not_resubmit_for_each_building(self) -> None:
        repo = AlarmJsonRepository({})
        repo.build_window_selection_snapshot = Mock(return_value={"selected_by_building": {}})
        repo._request_alarm_refresh_for_window = Mock(return_value=False)
        start, end = "2026-05-31 09:00:00", "2026-05-31 18:00:00"
        snapshot = repo.ensure_window_coverage(buildings=["A楼", "B楼"], start_time=start, end_time=end, emit_log=lambda _: None)
        for building in ("A楼", "B楼"):
            with self.assertRaisesRegex(RuntimeError, "未找到覆盖"):
                repo.query_alarm_summary(building=building, start_time=start, end_time=end,
                                         emit_log=lambda _: None, selection_snapshot=snapshot)
        repo._request_alarm_refresh_for_window.assert_called_once()

    def test_previous_shift_does_not_reuse_current_shift_snapshot(self) -> None:
        repo = AlarmJsonRepository({})
        repo.build_window_selection_snapshot = Mock(return_value={"selected_by_building": {}})
        repo._request_alarm_refresh_for_window = Mock(return_value=False)
        snapshot = {"duty_date": "2026-05-31", "duty_shift": "day",
                    "selected_by_building": {"A楼": {"file_path": "unrelated-day.json"}}}
        with self.assertRaisesRegex(RuntimeError, "未找到覆盖"):
            repo.query_alarm_summary(building="A楼", start_time="2026-05-30 18:00:00", end_time="2026-05-31 09:00:00",
                                     emit_log=lambda _: None, selection_snapshot=snapshot)
        repo.build_window_selection_snapshot.assert_called_once_with(buildings=["A楼"], duty_date="2026-05-30", duty_shift="night")


if __name__ == "__main__":
    unittest.main()
