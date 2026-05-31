from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
