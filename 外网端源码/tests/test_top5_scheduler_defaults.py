from __future__ import annotations

import unittest

from app.bootstrap.container import _normalize_top5_scheduler_cfg


class Top5SchedulerDefaultsTest(unittest.TestCase):
    def test_new_defaults_run_on_third_at_nine_thirty(self) -> None:
        cfg = _normalize_top5_scheduler_cfg({})
        self.assertTrue(cfg["enabled"])
        self.assertTrue(cfg["auto_start_in_gui"])
        self.assertEqual(cfg["day_of_month"], 3)
        self.assertEqual(cfg["run_time"], "09:30:00")
        self.assertTrue(cfg["catch_up_if_missed"])

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


if __name__ == "__main__":
    unittest.main()
