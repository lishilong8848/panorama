from __future__ import annotations

import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

from handover_log_module.service.branch_power_upload_service import BranchPowerUploadService
from handover_log_module.service.power_alert_sync_service import PowerAlertSyncService, _PowerAlertTable


class _MainTableClient:
    def __init__(self, *, old_ids=None, failure="") -> None:
        self.created: list[dict] = []
        self.ids: list[str] = list(old_ids or [])
        self.events: list[str] = []
        self.failure = failure
        self.serial = 0

    def clear_table(self, _table_id, **_kwargs) -> int:
        return 0

    def list_record_ids(self, _table_id, **_kwargs):
        self.events.append("list")
        if self.failure == "list":
            raise RuntimeError("mock incomplete pagination")
        if self.failure == "external" and self.serial and "external-1" not in self.ids:
            self.ids.append("external-1")
        if self.failure == "not_visible" and self.serial:
            return [item for item in self.ids if not item.startswith("new-")]
        return self.ids[:]

    def batch_delete_records(self, _table_id, record_ids, **_kwargs):
        self.events.append("delete")
        if self.failure == "delete":
            raise RuntimeError("mock deletion failed")
        if self.failure == "partial_delete":
            return 0
        self.ids = [item for item in self.ids if item not in record_ids]
        if self.failure == "external_after_delete":
            self.ids.append("external-after-delete")
        return len(record_ids)

    def batch_create_records(self, _table_id, fields_list, **_kwargs):
        self.events.append("create")
        self.created = [dict(item) for item in fields_list]
        self.serial += 1
        ids = [f"new-{self.serial}-{i}" for i in range(len(fields_list))]
        self.ids.extend(ids)
        if self.failure == "create":
            raise RuntimeError("mock create failed after partial upload")
        if self.failure == "response":
            return [{"data": {"records": []}}]
        return [{"data": {"records": [{"record_id": item} for item in ids]}}]


class _AlertTableClient:
    def __init__(self, *, fail_create: bool = False, verify_mismatches: int = 0) -> None:
        self.records = [
            {
                "record_id": "old-1",
                "fields": {"数据时间": "2026/08/03", "支路编号": "旧记录"},
            }
        ]
        self.fail_create = fail_create
        self.verify_mismatches = verify_mismatches
        self.list_calls = 0
        self.delete_calls = 0
        self.create_calls = 0

    def list_fields(self, _table_id, **_kwargs):
        return [
            {
                "field_name": name,
                "type": 2 if name == "序号" else 1,
                "property": {},
            }
            for name in PowerAlertSyncService.TARGET_FIELDS["branch"]
        ]

    def list_records(self, _table_id=None, **_kwargs):
        self.list_calls += 1
        if self.list_calls > 1 and self.verify_mismatches > 0:
            self.verify_mismatches -= 1
            return []
        return [dict(item) for item in self.records]

    def batch_create_records(self, *, table_id, fields_list, batch_size):
        del table_id, batch_size
        self.create_calls += 1
        if self.fail_create:
            raise RuntimeError("mock create failed")
        for index, fields in enumerate(fields_list, start=1):
            self.records.append({"record_id": f"new-{index}", "fields": dict(fields)})

    def batch_delete_records(self, *, table_id, record_ids, batch_size):
        del table_id, batch_size
        self.delete_calls += 1
        targets = set(record_ids)
        before = len(self.records)
        self.records = [item for item in self.records if item.get("record_id") not in targets]
        return before - len(self.records)


def _daily_rows(service: BranchPowerUploadService, business_date: str) -> dict[str, list[dict]]:
    day = datetime.strptime(business_date, "%Y-%m-%d")
    output: dict[str, list[dict]] = {}
    for hour in range(24):
        bucket = day.replace(hour=hour).strftime("%Y-%m-%d %H")
        output[bucket] = [
            {
                service.FIELD_BUILDING: "A楼",
                service.FIELD_ROOM: "A-301包间",
                service.FIELD_ROW: "A-301-C列-DC010",
                service.FIELD_BRANCH_ID: "C03-A1",
                service.FIELD_PDU_ID: "3",
                f"功率-{hour}:00": 7.2,
                f"电流-{hour}:00": 3.1,
                f"开关状态-{hour}:00": "合闸",
            }
        ]
    return output


def _branch_success(*, generated: int = 1) -> dict:
    return {
        "ok": True,
        "status": "success",
        "targets": {
            "branch": {
                "generated": generated,
                "created": generated,
                "verified": True,
                "verified_count": generated,
                "dry_run": False,
            }
        },
    }


def _full_cabinet_success() -> dict:
    return {
        "ok": True,
        "status": "success",
        "targets": {
            "cabinet": {},
            "line_head": {},
            "row_line": {},
        },
    }


class BranchPowerDailyAlertSyncTests(unittest.TestCase):
    def _run_daily_upload(self, branch_result: dict, *, upload_main_table: bool = True, main_client=None):
        service = BranchPowerUploadService({})
        main_client = main_client or _MainTableClient()
        branch_sync = Mock()
        branch_sync.sync.side_effect = AssertionError("automatic flow must not reread Feishu main table")
        branch_sync.sync_from_source_records.return_value = branch_result
        full_sync = Mock()
        full_sync.sync_from_source_units.return_value = _full_cabinet_success()

        with (
            patch.object(
                service,
                "_normalize_range_source_bundles",
                return_value=[SimpleNamespace(building="A楼", full_cabinet_power_file="full_cabinet.xlsx")],
            ),
            patch.object(
                service,
                "_parse_bundle_rows_by_hour",
                return_value=(_daily_rows(service, "2026-08-03"), {}),
            ),
            patch.object(service, "_client", return_value=main_client),
            patch.object(service, "_target_fields_from_bitable", return_value=service._upload_fields()),
            patch(
                "handover_log_module.service.branch_power_upload_service.PowerAlertSyncService",
                return_value=branch_sync,
            ),
            patch(
                "handover_log_module.service.branch_power_upload_service.FullCabinetPowerStatsSyncService",
                return_value=full_sync,
            ),
        ):
            result = service.upload_day_from_source_files(
                business_date="2026-08-03",
                source_units=[{"building": "A楼"}],
                upload_main_table=upload_main_table,
                emit_log=lambda _line: None,
            )
        return result, main_client, branch_sync, full_sync

    def test_main_table_create_then_delete_only_old_ids(self) -> None:
        client = _MainTableClient(old_ids=["old-1", "old-2"])
        result, _, _, _ = self._run_daily_upload(_branch_success(), main_client=client)
        self.assertEqual(client.events, ["list", "create", "list", "delete", "list"])
        self.assertEqual(client.ids, ["new-1-0"])
        self.assertEqual(result["deleted"], 2)

    def test_main_table_does_not_delete_records_created_after_snapshot(self) -> None:
        client = _MainTableClient(old_ids=["old-1"], failure="external")
        with patch("handover_log_module.service.branch_power_upload_service.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "并发写入，旧记录未删除"):
                self._run_daily_upload(_branch_success(), main_client=client)
        self.assertEqual(client.ids, ["old-1", "new-1-0", "external-1"])

    def test_main_table_failed_create_or_unconfirmed_new_preserves_old(self) -> None:
        for failure in ("list", "create", "response", "not_visible"):
            client = _MainTableClient(old_ids=["old-1"], failure=failure)
            with self.subTest(failure=failure), patch("handover_log_module.service.branch_power_upload_service.time.sleep"):
                with self.assertRaises(RuntimeError):
                    self._run_daily_upload(_branch_success(), main_client=client)
            self.assertIn("old-1", client.ids)
            self.assertNotIn("delete", client.events)

    def test_main_table_incomplete_delete_is_reported(self) -> None:
        client = _MainTableClient(old_ids=["old-1"], failure="partial_delete")
        with patch("handover_log_module.service.branch_power_upload_service.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "替换后校验不完整"):
                self._run_daily_upload(_branch_success(), main_client=client)
        self.assertEqual(client.ids, ["old-1", "new-1-0"])

    def test_main_table_reports_external_write_after_deleting_old(self) -> None:
        client = _MainTableClient(old_ids=["old-1"], failure="external_after_delete")
        with self.assertRaisesRegex(RuntimeError, "冲突记录已保留"):
            self._run_daily_upload(_branch_success(), main_client=client)
        self.assertEqual(client.ids, ["new-1-0", "external-after-delete"])

    def test_main_table_retry_cleans_old_and_partial_new_after_failure(self) -> None:
        client = _MainTableClient(old_ids=["old-1"], failure="create")
        with self.assertRaises(RuntimeError):
            self._run_daily_upload(_branch_success(), main_client=client)
        self.assertEqual(client.ids, ["old-1", "new-1-0"])
        client.failure = ""
        self._run_daily_upload(_branch_success(), main_client=client)
        self.assertEqual(client.ids, ["new-2-0"])

    def test_main_table_delete_failure_keeps_new_and_retry_cleans_up(self) -> None:
        client = _MainTableClient(old_ids=["old-1"], failure="delete")
        with self.assertRaisesRegex(RuntimeError, "deletion failed"):
            self._run_daily_upload(_branch_success(), main_client=client)
        self.assertEqual(client.ids, ["old-1", "new-1-0"])
        client.failure = ""
        self._run_daily_upload(_branch_success(), main_client=client)
        self.assertEqual(client.ids, ["new-2-0"])

    def test_automatic_upload_uses_local_records_for_branch_alerts(self) -> None:
        result, main_client, branch_sync, _full_sync = self._run_daily_upload(_branch_success())

        branch_sync.sync.assert_not_called()
        branch_sync.sync_from_source_records.assert_called_once()
        call = branch_sync.sync_from_source_records.call_args.kwargs
        self.assertEqual(call["report_date"], "2026-08-03")
        self.assertEqual(call["only_keys"], ["branch"])
        self.assertEqual(call["source_records"], main_client.created)
        self.assertEqual(result["status"], "success")

    def test_manual_statistics_rerun_uses_the_same_local_record_path(self) -> None:
        result, main_client, branch_sync, full_sync = self._run_daily_upload(
            _branch_success(),
            upload_main_table=False,
        )

        branch_sync.sync.assert_not_called()
        branch_sync.sync_from_source_records.assert_called_once()
        call = branch_sync.sync_from_source_records.call_args.kwargs
        self.assertEqual(call["report_date"], "2026-08-03")
        self.assertEqual(len(call["source_records"]), 1)
        self.assertEqual(call["source_records"][0]["功率-23:00"], 7.2)
        self.assertEqual(main_client.created, [])
        full_sync.sync_from_source_units.assert_called_once()
        self.assertEqual(full_sync.sync_from_source_units.call_args.kwargs["report_date"], "2026-08-03")
        self.assertFalse(result["upload_main_table"])

    def test_branch_alert_failure_propagates_after_main_upload(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "单支路超6.25KW同步失败"):
            self._run_daily_upload(
                {
                    "ok": False,
                    "status": "failed",
                    "error": "Data not ready",
                }
            )

    def test_incomplete_branch_result_is_rejected(self) -> None:
        incomplete = _branch_success(generated=1)
        incomplete["targets"]["branch"]["verified_count"] = 0

        with self.assertRaisesRegex(RuntimeError, "单支路超6.25KW同步结果不完整"):
            self._run_daily_upload(incomplete)


class PowerAlertTargetVerificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = PowerAlertSyncService({})
        self.table = _PowerAlertTable(
            key="branch",
            name="单支路超6.25KW功率",
            table_id="tbl-branch",
            view_id="",
            threshold=6.25,
            app_token="app-token",
        )
        self.row = {
            "序号": 1,
            "数据时间": "2026/08/03",
            "机房": "EA118",
            "楼栋": "A楼",
            "房间": "A-301包间",
            "PDU编号": "C03-A1",
            "支路号": "3",
            "支路编号": "C列-DC010 #3",
            "支路功率": "7.2",
            "对侧PDU编号": "C03-B1",
            "对侧支路功率": "1.1",
            "采集时间点": "10:00",
            "时长": "1h",
            "备注": None,
        }

    def _replace(self, client: _AlertTableClient, rows: list[dict]):
        return self.service._replace_target_rows(
            client=client,
            table=self.table,
            rows=rows,
            report_date="2026/08/03",
            dry_run=False,
            page_size=500,
            batch_size=200,
            verify_retry_count=3,
            verify_retry_interval_sec=0,
            emit_log=lambda _line: None,
        )

    def test_replace_verifies_same_date_record_count_with_retry(self) -> None:
        client = _AlertTableClient(verify_mismatches=1)

        result = self._replace(client, [self.row])

        self.assertTrue(result["verified"])
        self.assertEqual(result["verified_count"], 1)
        self.assertEqual(result["verify_attempts"], 2)
        self.assertEqual(client.delete_calls, 1)

    def test_create_failure_keeps_existing_same_date_records(self) -> None:
        client = _AlertTableClient(fail_create=True)

        with self.assertRaisesRegex(RuntimeError, "mock create failed"):
            self._replace(client, [self.row])

        self.assertEqual(client.delete_calls, 0)
        self.assertEqual([item["record_id"] for item in client.records], ["old-1"])

    def test_zero_generated_rows_clears_old_records_and_verifies_zero(self) -> None:
        client = _AlertTableClient()

        result = self._replace(client, [])

        self.assertEqual(result["generated"], 0)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["verified_count"], 0)
        self.assertEqual(client.create_calls, 0)
        self.assertEqual(client.records, [])


if __name__ == "__main__":
    unittest.main()
