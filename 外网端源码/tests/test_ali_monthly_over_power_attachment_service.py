from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path

from handover_log_module.service.ali_monthly_over_power_attachment_service import (
    AliMonthlyOverPowerAttachmentService,
)


class FakeAliMonthlyOverPowerAttachmentService(AliMonthlyOverPowerAttachmentService):
    def __init__(self, runtime_config, records):
        super().__init__(runtime_config)
        self.records = records
        self.downloaded_urls: list[str] = []

    def _list_records(self, cfg, *, emit_log=None):  # noqa: ANN001
        return list(self.records)

    def _token(self, *, force_refresh: bool = False) -> str:
        return "fake-token"

    def _download_attachment(self, url: str, save_path: Path, token: str) -> None:
        self.downloaded_urls.append(url)
        save_path.write_bytes(f"downloaded:{url}".encode("utf-8"))


def _config(output_dir: Path) -> dict:
    return {
        "common": {
            "feishu_auth": {
                "app_id": "cli_xxx",
                "app_secret": "secret_xxx",
            }
        },
        "handover_log": {
            "top5_power_report": {
                "over_power_attachment": {
                    "enabled": True,
                    "app_token": "app_token",
                    "table_id": "table_id",
                    "view_id": "view_id",
                    "output_dir": str(output_dir),
                    "zip_file_name_pattern": "月度超功率附件_{year}{month}_{timestamp}.zip",
                }
            }
        },
    }


class AliMonthlyOverPowerAttachmentServiceTest(unittest.TestCase):
    def test_filters_target_month_and_excludes_top5_attachments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "out"
            records = [
                {
                    "fields": {
                        "年度": "2026年",
                        "月份": "5月",
                        "附件": [
                            {"name": "5月超功率明细.xlsx", "url": "https://example.test/a"},
                            {"name": "5月超功耗统计.xlsx", "tmp_url": "https://example.test/b"},
                            {"name": "5月TOP5超功率.xlsx", "url": "https://example.test/top5"},
                        ],
                    }
                },
                {
                    "fields": {
                        "年度": "2026",
                        "月份": "6",
                        "附件": [{"name": "6月超功率明细.xlsx", "url": "https://example.test/c"}],
                    }
                },
            ]
            service = FakeAliMonthlyOverPowerAttachmentService(_config(output_dir), records)

            result = service.run(year="2026", month=5, emit_log=lambda _: None)

            self.assertEqual(result["downloaded_count"], 2)
            self.assertEqual(service.downloaded_urls, ["https://example.test/a", "https://example.test/b"])
            zip_path = Path(result["zip_file"])
            self.assertTrue(zip_path.exists())
            with zipfile.ZipFile(zip_path) as archive:
                self.assertEqual(sorted(archive.namelist()), ["5月超功率明细.xlsx", "5月超功耗统计.xlsx"])

    def test_missing_table_config_fails_before_request(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cfg = _config(Path(temp_dir))
            cfg["handover_log"]["top5_power_report"]["over_power_attachment"]["table_id"] = ""
            service = FakeAliMonthlyOverPowerAttachmentService(cfg, [])

            with self.assertRaisesRegex(ValueError, "table_id"):
                service.run(year="2026", month=5, emit_log=lambda _: None)


if __name__ == "__main__":
    unittest.main()
