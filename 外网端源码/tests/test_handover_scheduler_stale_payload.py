import unittest
from datetime import datetime

from app.worker.task_handlers import _stale_scheduled_handover_payload


class HandoverSchedulerStalePayloadTest(unittest.TestCase):
    def _config(self):
        return {
            "handover_log": {
                "scheduler": {
                    "morning_time": "02:00:00",
                    "afternoon_time": "12:00:00",
                }
            }
        }

    def test_morning_previous_night_payload_is_stale_after_day_generation_time(self):
        payload = {
            "scheduler_slot": "morning",
            "duty_date": "2026-06-03",
            "duty_shift": "night",
        }
        result = _stale_scheduled_handover_payload(
            self._config(),
            payload,
            now=datetime(2026, 6, 4, 12, 7, 46),
        )
        self.assertTrue(result["stale"])
        self.assertEqual(result["stale_after"], "2026-06-04 12:00:00")

    def test_morning_previous_night_payload_can_run_before_day_generation_time(self):
        payload = {
            "scheduler_slot": "morning",
            "duty_date": "2026-06-03",
            "duty_shift": "night",
        }
        result = _stale_scheduled_handover_payload(
            self._config(),
            payload,
            now=datetime(2026, 6, 4, 11, 59, 59),
        )
        self.assertFalse(result["stale"])

    def test_afternoon_day_payload_is_stale_after_next_morning_generation_time(self):
        payload = {
            "scheduler_slot": "afternoon",
            "duty_date": "2026-06-04",
            "duty_shift": "day",
        }
        result = _stale_scheduled_handover_payload(
            self._config(),
            payload,
            now=datetime(2026, 6, 5, 2, 0, 0),
        )
        self.assertTrue(result["stale"])
        self.assertEqual(result["stale_after"], "2026-06-05 02:00:00")

    def test_manual_payload_without_scheduler_slot_is_not_stale(self):
        payload = {
            "duty_date": "2026-06-03",
            "duty_shift": "night",
        }
        result = _stale_scheduled_handover_payload(
            self._config(),
            payload,
            now=datetime(2026, 6, 4, 12, 7, 46),
        )
        self.assertFalse(result["stale"])


if __name__ == "__main__":
    unittest.main()
