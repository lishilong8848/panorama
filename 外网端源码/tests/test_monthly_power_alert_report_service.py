from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import Mock

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from handover_log_module.service.monthly_power_alert_report_service import MonthlyPowerAlertReportService
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient


class _FakeBitableClient:
    def __init__(self, records_by_table):
        self.records_by_table = records_by_table
        self.read_calls = []

    def list_records(self, table_id, **kwargs):  # noqa: ANN001, ARG002
        self.read_calls.append({"table_id": table_id, **kwargs})
        records = list(self.records_by_table.get(table_id, []))
        limit = kwargs.get("max_records", 0)
        return records[:limit] if limit else records


def _record(fields):
    return {"record_id": "rec_test", "fields": dict(fields)}


class MonthlyPowerAlertReportServiceTest(unittest.TestCase):
    def test_run_generates_monthly_report_and_dedupes_cabinet_daily_duration(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = {
                "common": {"feishu_auth": {"app_id": "cli_test", "app_secret": "sec_test"}},
                "handover_log": {
                    "top5_power_report": {
                        "monthly_power_alert_report": {
                            "output_dir": tmpdir,
                            "file_name_pattern": "monthly_alert_{year}{month2}.xlsx",
                            "max_records": 1,
                        }
                    }
                },
                "features": {
                    "branch_power_upload": {
                        "power_alert_sync": {
                            "app_token": "app_test",
                            "tables": {
                                "row_line": {"table_id": "tbl_row", "view_id": "vew3F3KIqS"},
                                "line_head": {"table_id": "tbl_line", "view_id": "vewkVrqxiG"},
                                "cabinet": {"table_id": "tbl_cabinet", "view_id": "vewnybKvrN"},
                                "branch": {"table_id": "tbl_branch", "view_id": "vewjUPAvZP"},
                            },
                        }
                    }
                },
            }
            records_by_table = {
                "tbl_row": [
                    _record({"数据时间": "2026/5/1", "楼栋": "C楼", "房间": "C-301.EA118", "机列": "F列", "功率": "220.5KW", "次数": 1, "时长": "2h"}),
                ],
                "tbl_line": [
                    _record(
                        {
                            "数据时间": "2026/5/1",
                            "楼栋": "A楼",
                            "房间": "A-401.EA118",
                            "机列": "H列-DC003",
                            "功率": "107.591kw",
                            "对侧机列": "H列-AC003",
                            "对侧机列最大功率": "97.322kw",
                            "次数": 1,
                            "时长": "2h",
                        }
                    ),
                ],
                "tbl_cabinet": [
                    _record(
                        {
                            "数据时间": "2026/5/1",
                            "楼栋": "B楼",
                            "房间": "B-201包间",
                            "机柜号": "C列C02",
                            "机柜功率": "19.5kw",
                            "PDU编号": "C02-A2",
                            "电流值": "8.1",
                            "是否负载不均匀": "均匀",
                            "次数": 1,
                            "时长": "5h",
                        }
                    ),
                    _record(
                        {
                            "数据时间": "2026/5/1",
                            "楼栋": "B楼",
                            "房间": "B-201包间",
                            "机柜号": "C列C02",
                            "机柜功率": "19.5kw",
                            "PDU编号": "C02-B2",
                            "电流值": "7.9",
                            "是否负载不均匀": "均匀",
                            "次数": 1,
                            "时长": "5h",
                        }
                    ),
                    _record(
                        {
                            "数据时间": "2026/5/2",
                            "楼栋": "B楼",
                            "房间": "B-201包间",
                            "机柜号": "C列C02",
                            "机柜功率": "20.25kw",
                            "PDU编号": "C02-A2",
                            "电流值": "8.3",
                            "是否负载不均匀": "均匀",
                            "次数": 1,
                            "时长": "2h",
                        }
                    ),
                ],
                "tbl_branch": [
                    _record({"数据时间": "2026/4/30", "楼栋": "A楼", "房间": "A-301包间", "支路编号": "C列-AC002 #20", "PDU编号": "C02-A2", "支路功率": "99", "时长": "24h"}),
                    _record(
                        {
                            "数据时间": "2026/5/1",
                            "楼栋": "A楼",
                            "房间": "A-301包间",
                            "支路编号": "C列-AC002 #20",
                            "PDU编号": "C02-A2",
                            "支路功率": "8.28",
                            "对侧PDU编号": "C02-B2",
                            "对侧支路功率": "2.2",
                            "时长": "3h",
                        }
                    ),
                ],
            }
            service = MonthlyPowerAlertReportService(cfg)
            client = _FakeBitableClient(records_by_table)
            service._client = lambda _power_cfg, _emit_log: client  # noqa: SLF001

            result = service.run(year="2026", month=5, emit_log=lambda _msg: None)

            self.assertEqual(4, len(client.read_calls))
            expected_views = {table["table_id"]: table["view_id"] for table in service._power_cfg()["tables"].values()}
            for call in client.read_calls:
                self.assertEqual(expected_views[call["table_id"]], call["view_id"])
                self.assertEqual(0, call["max_records"])
            self.assertEqual({"row_line": 1, "line_head": 1, "cabinet": 1, "branch": 1}, result["row_counts"])
            self.assertEqual(2, result["source_record_counts"]["branch"])
            output_path = Path(result["output_file"])
            self.assertTrue(output_path.exists())
            workbook = openpyxl.load_workbook(output_path, data_only=True)
            try:
                self.assertEqual(
                    [
                        "机列超215功率统计",
                        "列头柜超107.5功率统计",
                        "机柜超18KW统计",
                        "单支路超6.25KW功率",
                        "说明",
                        "机列超功率附表",
                        "列头柜超功率附表",
                        "机柜超功率附表",
                    ],
                    workbook.sheetnames,
                )
                cabinet = workbook["机柜超18KW统计"]
                self.assertEqual("EA118", cabinet["C3"].value)
                self.assertEqual("7h", cabinet["L3"].value)
                self.assertEqual("C02-A2", cabinet["H3"].value)
                self.assertEqual("C02-B2", cabinet["H4"].value)
                branch = workbook["单支路超6.25KW功率"]
                self.assertEqual("EA118", branch["C3"].value)
                self.assertEqual("C列-AC002 #20", branch["F3"].value)
                self.assertEqual("8.28", branch["H3"].value)
                self.assertEqual("3h", branch["K3"].value)
                self.assertEqual(3, branch.max_row)
            finally:
                workbook.close()

    def test_read_follows_all_pages_before_filtering_by_data_time(self):
        service = MonthlyPowerAlertReportService({})
        client = object.__new__(FeishuBitableClient)
        client.app_token = "app_test"
        client._get_json = Mock(side_effect=[
            {"data": {"items": [_record({
                "数据时间": "2026/8/31", "创建时间": "2026/9/1",
                "楼栋": "B楼", "房间": "B-302包间", "支路编号": "G列-DC011 #38",
                "PDU编号": "H01-A2", "支路功率": "99", "时长": "24h",
            })], "has_more": True, "page_token": "page_2"}},
            {"data": {"items": [_record({
                "数据时间": "2026/09/30 23:00:00", "创建时间": "2026/10/7",
                "楼栋": "B楼", "房间": "B-302包间", "支路编号": "G列-DC011 #38",
                "PDU编号": "H01-A2", "支路功率": "7.214", "时长": "8h",
            })], "has_more": False}},
        ])

        records = service._list_table_records(
            client, {"table_id": "tbl_branch", "view_id": "vewjUPAvZP"},
            {"page_size": 1, "max_records": 1}, emit_log=lambda _msg: None,
        )
        self.assertEqual(2, len(records))
        self.assertEqual(2, client._get_json.call_count)
        for call in client._get_json.call_args_list:
            self.assertEqual("vewjUPAvZP", call.kwargs["params"]["view_id"])
        self.assertEqual("page_2", client._get_json.call_args_list[1].kwargs["params"]["page_token"])
        items = service._aggregate_branch(records, year="2026", month=9)
        self.assertEqual(1, len(items))
        self.assertEqual(7.214, items[0].max_value)
        self.assertEqual(8, items[0].duration_hours)

    def test_default_views_match_requested_views(self):
        tables = MonthlyPowerAlertReportService({})._power_cfg()["tables"]
        self.assertEqual("vewjUPAvZP", tables["branch"]["view_id"])
        self.assertEqual("vewnybKvrN", tables["cabinet"]["view_id"])
        self.assertEqual("vewkVrqxiG", tables["line_head"]["view_id"])
        self.assertEqual("vew3F3KIqS", tables["row_line"]["view_id"])


if __name__ == "__main__":
    unittest.main()
