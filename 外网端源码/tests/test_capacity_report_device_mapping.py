import unittest

from openpyxl import Workbook

from handover_log_module.core.models import RawRow
from handover_log_module.service.capacity_report_common import (
    build_capacity_cells_with_config,
    build_capacity_template_snapshot,
)


def _raw_row(*, row: int, c_text: str, d_name: str, value: str) -> RawRow:
    return RawRow(
        row_index=row,
        b_text="南通阿里/B楼/三层/变电所M4 B-317",
        c_text=c_text,
        d_name=d_name,
        e_raw=value,
        value=float(value),
    )


class CapacityReportDeviceMappingTests(unittest.TestCase):
    def _snapshot(self):
        workbook = Workbook()
        sheet = workbook.active
        sheet["O68"] = "HVDC"
        sheet["R68"] = "输出功率kw"
        sheet["S68"] = "直流列头柜"
        sheet["O69"] = "-317-HVDC-111"
        sheet["S69"] = "-301-RPP-DC-1"
        return workbook, build_capacity_template_snapshot(sheet, "B楼")

    def test_table_headers_are_not_collected_as_device_identifiers(self):
        workbook, snapshot = self._snapshot()
        try:
            self.assertEqual([item["row"] for item in snapshot["hvdc_entries"]], [69])
            self.assertEqual([item["row"] for item in snapshot["rpp_entries"]], [69])
            self.assertEqual(snapshot["hvdc_entries"][0]["identifier"], "B-317-HVDC-111")
            self.assertEqual(snapshot["rpp_entries"][0]["identifier"], "B-301-RPP-DC-1")
        finally:
            workbook.close()

    def test_missing_hvdc_power_is_left_blank_instead_of_written_as_zero(self):
        workbook, snapshot = self._snapshot()
        try:
            values = build_capacity_cells_with_config(
                {
                    "building": "B楼",
                    "capacity_rows": [
                        _raw_row(row=1, c_text="B-317-HVDC-112", d_name="直流屏_电池组电压_V", value="269.90"),
                        _raw_row(row=2, c_text="B-317-HVDC-112", d_name="直流屏_直流电压_V", value="269.80"),
                    ],
                    "template_snapshot": snapshot,
                    "running_units": {},
                }
            )

            self.assertNotIn("O68", values)
            self.assertNotIn("R68", values)
            self.assertNotIn("S68", values)
            self.assertEqual(values["P69"], "269.90")
            self.assertEqual(values["Q69"], "269.80")
            self.assertNotIn("R69", values)
        finally:
            workbook.close()

    def test_hvdc_power_is_written_when_the_source_metric_exists(self):
        workbook, snapshot = self._snapshot()
        try:
            values = build_capacity_cells_with_config(
                {
                    "building": "B楼",
                    "capacity_rows": [
                        _raw_row(row=1, c_text="B-317-HVDC-112", d_name="直流屏_电池组电压_V", value="269.90"),
                        _raw_row(row=2, c_text="B-317-HVDC-112", d_name="直流屏_直流电压_V", value="269.80"),
                        _raw_row(row=3, c_text="B-317-HVDC-112", d_name="直流总功率_KW", value="63.40"),
                    ],
                    "template_snapshot": snapshot,
                    "running_units": {},
                }
            )

            self.assertEqual(values["R69"], "63.40")
        finally:
            workbook.close()

    def test_b417_transformer_temperature_uses_101_102_source_numbering(self):
        workbook = Workbook()
        sheet = workbook.active
        sheet["B127"] = "-417-TR201"
        sheet["B132"] = "-417-TR202"
        snapshot = build_capacity_template_snapshot(sheet, "B楼")
        try:
            values = build_capacity_cells_with_config(
                {
                    "building": "B楼",
                    "capacity_rows": [
                        _raw_row(row=1, c_text="B-417-TR-101_变压器温控仪", d_name="B_温度_℃", value="67.80"),
                        _raw_row(row=2, c_text="B-417-TR-102_变压器温控仪", d_name="B_温度_℃", value="66.80"),
                    ],
                    "template_snapshot": snapshot,
                    "running_units": {},
                }
            )

            self.assertEqual(values["D127"], "67.80")
            self.assertEqual(values["D132"], "66.80")
        finally:
            workbook.close()


if __name__ == "__main__":
    unittest.main()
