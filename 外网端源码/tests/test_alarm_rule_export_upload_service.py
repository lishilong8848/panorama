from __future__ import annotations

import pytest

from app.modules.alarm_rule_export_upload.service.alarm_rule_export_upload_service import (
    AlarmRuleExportUploadService,
)


class FakeInternalClient:
    def __init__(self, files):
        self.files = files
        self.downloads = []

    def list_alarm_rule_export_files(self, *, period="", building=""):
        return {"ok": True, "period": period, "files": list(self.files), "count": len(self.files)}

    def download_alarm_rule_export_file(self, *, period, building, file_name):
        self.downloads.append((period, building, file_name))
        return f"{building}-{file_name}".encode("utf-8"), file_name, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class FakeBitableClient:
    def __init__(self):
        self.uploads = []
        self.created = []
        self.deleted = []

    def list_records(self, table_id, page_size=500, max_records=0, *, view_id="", filter_formula="", field_names=None):
        return [
            {"record_id": "rec-old-a", "fields": {"楼栋": "A楼", "月份": "2026-06"}},
            {"record_id": "rec-old-a-prev", "fields": {"楼栋": "A楼", "月份": "2026-05"}},
            {"record_id": "rec-old-c", "fields": {"楼栋": "C楼", "月份": "2026-06"}},
        ]

    def upload_attachment_bytes(self, *, file_name, content, mime_type="application/octet-stream", timeout=None):
        token = f"token-{file_name}"
        self.uploads.append({"file_name": file_name, "content": content, "mime_type": mime_type, "token": token})
        return token

    def batch_create_records(self, table_id, fields_list, batch_size=200, progress_callback=None):
        self.created.extend(fields_list)
        return [{"code": 0}]

    def batch_delete_records(self, table_id, record_ids, batch_size=500, progress_callback=None):
        self.deleted.extend(record_ids)
        return len(record_ids)


def _runtime_config(buildings):
    return {
        "feishu": {"app_id": "cli_xxx", "app_secret": "secret", "timeout": 1},
        "alarm_rule_export_upload": {
            "enabled": True,
            "buildings": buildings,
            "target": {
                "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
                "table_id": "tblNyGBGSCnWhWyL",
                "fields": {"building": "楼栋", "month": "月份", "attachment": "附件"},
            },
        },
    }


def test_alarm_rule_export_upload_replaces_same_building_and_month_only():
    internal = FakeInternalClient(
        [
            {
                "building": "A楼",
                "period": "2026-06",
                "file_name": "A楼_告警规则_202606.xlsx",
                "file_exists": True,
                "size_bytes": 100,
                "downloaded_at": "2026-06-01 04:00:00",
            },
            {
                "building": "B楼",
                "period": "2026-06",
                "file_name": "B楼_告警规则_202606.xlsx",
                "file_exists": True,
                "size_bytes": 100,
                "downloaded_at": "2026-06-01 04:00:00",
            },
        ]
    )
    bitable = FakeBitableClient()
    result = AlarmRuleExportUploadService(
        _runtime_config(["A楼", "B楼"]),
        internal_client=internal,
        bitable_client=bitable,
    ).run(period="2026-06")

    assert result["status"] == "success"
    assert result["uploaded_count"] == 2
    assert result["month_value"] == "2026-06"
    assert bitable.deleted == ["rec-old-a"]
    assert [row["楼栋"] for row in bitable.created] == ["A楼", "B楼"]
    assert [row["月份"] for row in bitable.created] == ["2026-06", "2026-06"]
    assert all(row["附件"][0]["file_token"].startswith("token-") for row in bitable.created)


def test_alarm_rule_export_upload_fails_when_building_file_missing():
    internal = FakeInternalClient(
        [
            {
                "building": "A楼",
                "period": "2026-06",
                "file_name": "A楼_告警规则_202606.xlsx",
                "file_exists": True,
                "size_bytes": 100,
            }
        ]
    )
    with pytest.raises(RuntimeError, match="B楼"):
        AlarmRuleExportUploadService(
            _runtime_config(["A楼", "B楼"]),
            internal_client=internal,
            bitable_client=FakeBitableClient(),
        ).run(period="2026-06")
