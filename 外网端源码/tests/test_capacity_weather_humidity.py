import unittest

from handover_log_module.service.capacity_report_common import (
    build_common_capacity_cell_values,
    calculate_relative_humidity_from_dry_wet,
    format_relative_humidity_from_dry_wet,
)


class CapacityWeatherHumidityTests(unittest.TestCase):
    def test_relative_humidity_calculated_from_dry_and_wet_bulb(self):
        humidity = calculate_relative_humidity_from_dry_wet("30.0℃", "24.0℃")

        self.assertIsNotNone(humidity)
        self.assertGreater(humidity, 55)
        self.assertLess(humidity, 75)
        self.assertTrue(format_relative_humidity_from_dry_wet("30.0℃", "24.0℃").endswith("%"))

    def test_common_capacity_values_fill_x2_when_weather_humidity_missing(self):
        values = build_common_capacity_cell_values(
            {
                "duty_shift": "day",
                "handover_cells": {"B7": "30.0", "D7": "24.0"},
                "outdoor_handover_cells": {"B7": "30.0", "D7": "24.0"},
                "weather_text": "多云",
                "weather_humidity": "",
            }
        )

        self.assertEqual(values.get("L2"), "多云")
        self.assertEqual(values.get("R2"), "30.0")
        self.assertEqual(values.get("AB2"), "24.0")
        self.assertRegex(values.get("X2", ""), r"^\d+(?:\.\d+)?%$")

    def test_existing_weather_humidity_is_preserved(self):
        values = build_common_capacity_cell_values(
            {
                "duty_shift": "night",
                "handover_cells": {"B7": "30.0", "D7": "24.0"},
                "outdoor_handover_cells": {"B7": "30.0", "D7": "24.0"},
                "weather_humidity": "88%",
            }
        )

        self.assertEqual(values.get("X2"), "88%")


if __name__ == "__main__":
    unittest.main()
