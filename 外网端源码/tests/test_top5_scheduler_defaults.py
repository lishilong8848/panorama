from __future__ import annotations

import unittest
from datetime import datetime

from app.bootstrap.app_factory import _previous_calendar_year_month
from app.bootstrap.container import _normalize_top5_scheduler_cfg
from app.modules.scheduler.api._config_persistence import _handover_common_scheduler_path


class Top5SchedulerDefaultsTest(unittest.TestCase):
    def test_scheduled_report_targets_previous_calendar_month(self) -> None:
        self.assertEqual(
            _previous_calendar_year_month(datetime(2026, 8, 3, 9, 30)),
            ("2026", 7),
        )
        self.assertEqual(
            _previous_calendar_year_month(datetime(2026, 1, 3, 9, 30)),
            ("2025", 12),
        )

    def test_new_defaults_run_on_third_at_nine_thirty(self) -> None:
        cfg = _normalize_top5_scheduler_cfg({})
        self.assertTrue(cfg["enabled"])
        self.assertTrue(cfg["auto_start_in_gui"])
        self.assertEqual(cfg["day_of_month"], 3)
        self.assertEqual(cfg["run_time"], "09:30:00")
        self.assertFalse(cfg["catch_up_if_missed"])

    def test_startup_catch_up_is_always_disabled(self) -> None:
        cfg = _normalize_top5_scheduler_cfg({"catch_up_if_missed": True})
        self.assertFalse(cfg["catch_up_if_missed"])

    def test_legacy_three_oclock_default_is_migrated(self) -> None:
        cfg = _normalize_top5_scheduler_cfg(
            {
                "enabled": True,
                "auto_start_in_gui": False,
                "day_of_month": 3,
                "run_time": "03:00:00",
            }
        )
        self.assertTrue(cfg["auto_start_in_gui"])
        self.assertEqual(cfg["run_time"], "09:30:00")

    def test_custom_schedule_is_not_overwritten(self) -> None:
        cfg = _normalize_top5_scheduler_cfg(
            {
                "enabled": True,
                "auto_start_in_gui": False,
                "day_of_month": 5,
                "run_time": "10:15:00",
            }
        )
        self.assertFalse(cfg["auto_start_in_gui"])
        self.assertEqual(cfg["day_of_month"], 5)
        self.assertEqual(cfg["run_time"], "10:15:00")

    def test_scheduler_config_uses_handover_common_segment(self) -> None:
        self.assertEqual(
            _handover_common_scheduler_path(
                ("features", "handover_log", "top5_power_report", "scheduler")
            ),
            ("top5_power_report", "scheduler"),
        )


if __name__ == "__main__":
    unittest.main()
