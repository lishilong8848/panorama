from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import openpyxl

from handover_log_module.service.top5_power_report_service import Top5PowerReportService


BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"]


def _code(building: str) -> str:
    return building[:1]


def _write_capacity_source(path: Path, building: str) -> None:
    code = _code(building)
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append([None] * 11)
    sheet.append([None, None, None, None, "00:00", "01:00", "02:00", "03:00", "最大值", "最小值", "平均值"])

    for index in range(5):
        prefix = f"{code}-{200 + index}"
        suffix = "101" if index % 2 == 0 else "201"
        sheet.append([
            None,
            f"{building}/变电所/{prefix}/{prefix}-TRB-{suffix}",
            f"{prefix}-TRB-{suffix}_总进线柜",
            "总_有功功率_KW",
            90 + index,
            91 + index,
            92 + index,
            93 + index,
            100 + index,
            90 + index,
            95 + index,
        ])

    for index in range(5):
        prefix = f"{code}-{300 + index}"
        sheet.append([None, f"{building}/变电所/{prefix}", f"{prefix}-HVDC-{100 + index}", "直流电压_V", 270, 270, 270, 270, 270, 270, 270])
        sheet.append([None, None, None, "直流总功率_KW", 80 + index, 81 + index, 82 + index, 83 + index, 110 + index, 80 + index, 88 + index])

    for index in range(5):
        prefix = f"{code}-{400 + index}"
        sheet.append([None, f"{building}/UPS/{prefix}-UPS-{100 + index}", f"{prefix}-UPS-{100 + index}_UPS", "电池_正极电压_V", 260, 260, 260, 260, 260, 260, 260])
        sheet.append([None, None, None, "UPS_输出总有功功率_KW", 10 + index, 11 + index, 12 + index, 13 + index, 30 + index, 10 + index, 15 + index])

    workbook.save(path)


def _write_branch_source(path: Path, building: str) -> None:
    code = _code(building)
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append([None, None, None, "00:00", "01:00", "02:00"])
    sheet.append([None] * 6)
    sheet.append(["设备名称", "设备名称", "测点", None, None, None])
    for index in range(5):
        room = f"{code}-{500 + index}包间"
        cabinet = f"{code}-{500 + index}-{chr(65 + index)}列-AC010"
        base = 20 + index
        sheet.append([room, cabinet, "1_支路功率_KW", base, base + 1, base + 2])
        sheet.append([room, cabinet, "2_支路功率_KW", 1, 1, 1])
    workbook.save(path)


def _entry(building: str, path: Path) -> dict:
    return {"building": building, "file_path": str(path), "bucket_key": "2026-05-01"}


class Top5PowerReportServiceTest(unittest.TestCase):
    def test_extract_capacity_records_normalizes_top5(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source = temp_path / "capacity_A.xlsx"
            _write_capacity_source(source, "A楼")

            groups = Top5PowerReportService.extract_capacity_records(source, building="A楼")

            self.assertEqual(groups.transformers[0].identifier, "A-204-A变压器容量")
            self.assertEqual(groups.transformers[0].power_kw, 104)
            self.assertEqual(groups.hvdcs[0].identifier, "A-304-HVDC-104")
            self.assertEqual(groups.upss[0].identifier, "A-404-UPS-104_UPS")

    def test_extract_branch_records_aggregates_hourly_column_power(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source = temp_path / "branch_A.xlsx"
            _write_branch_source(source, "A楼")

            records = Top5PowerReportService.extract_branch_records(source, building="A楼")

            self.assertEqual(records[0].identifier, "A-504-E列功率和")
            self.assertEqual(records[0].power_kw, 27)
            self.assertEqual(len(records), 5)

    def test_run_writes_summary_and_source_sheets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            capacity_entries = []
            branch_entries = []
            for building in BUILDINGS:
                capacity_path = temp_path / f"capacity_{_code(building)}.xlsx"
                branch_path = temp_path / f"branch_{_code(building)}.xlsx"
                _write_capacity_source(capacity_path, building)
                _write_branch_source(branch_path, building)
                capacity_entries.append(_entry(building, capacity_path))
                branch_entries.append(_entry(building, branch_path))

            service = Top5PowerReportService(
                {
                    "handover_log": {
                        "top5_power_report": {
                            "template": {
                                "source_path": str(temp_path / "missing_template.xlsx"),
                                "output_dir": str(temp_path / "out"),
                            }
                        }
                    }
                }
            )

            result = service.run(capacity_entries=capacity_entries, branch_entries=branch_entries, emit_log=lambda _: None)

            output_path = Path(result["output_file"])
            self.assertTrue(output_path.exists())
            workbook = openpyxl.load_workbook(output_path, data_only=True)
            try:
                self.assertEqual(workbook.sheetnames[0], "汇总信息表")
                self.assertEqual(len(workbook.sheetnames), 11)
                summary = workbook["汇总信息表"]
                self.assertEqual(summary["A3"].value, 1)
                self.assertEqual(summary["B3"].value, "A")
                self.assertTrue(summary["E23"].value.startswith("E-"))
                self.assertIn("容量_A", workbook.sheetnames)
                self.assertIn("支路功率_E", workbook.sheetnames)
            finally:
                workbook.close()


if __name__ == "__main__":
    unittest.main()
