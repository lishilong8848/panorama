from __future__ import annotations

import json

from app.modules.alarm_rule_export.service.alarm_rule_export_service import (
    SiteConfig,
    _completed_state_records,
    _load_export_state,
    _pending_state_records,
    _upsert_export_state_record,
    _default_download_root,
    _filename_has_period_date,
    list_alarm_rule_export_files,
    resolve_alarm_rule_export_file,
    build_default_args,
)


def test_state_records_are_scoped_by_period(tmp_path):
    state_file = tmp_path / "export_records.json"
    state_file.write_text(
        json.dumps(
            {
                "version": 1,
                "records": [
                    {
                        "building": "A楼",
                        "file_name": "old.xlsx",
                        "status": "downloaded",
                    },
                    {
                        "building": "A楼",
                        "period": "2026-05",
                        "file_name": "may.xlsx",
                        "status": "downloaded",
                    },
                    {
                        "building": "A楼",
                        "period": "2026-06",
                        "file_name": "june.xlsx",
                        "status": "created",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    args = build_default_args(state_file=str(state_file), period="2026-06")
    site = SiteConfig(building="A楼", host="127.0.0.1", username="u", password="p")

    state = _load_export_state(args)

    migrated = [item for item in state["records"] if item["file_name"] == "old.xlsx"][0]
    assert migrated["period"] == "2026-06"
    assert [item["file_name"] for item in _completed_state_records(args, site)] == ["old.xlsx"]
    assert [item["file_name"] for item in _pending_state_records(args, site)] == ["june.xlsx"]


def test_upsert_uses_building_period_and_exact_file_name(tmp_path):
    state_file = tmp_path / "export_records.json"
    args = build_default_args(state_file=str(state_file), period="2026-06")
    site = SiteConfig(building="B楼", host="127.0.0.1", username="u", password="p")

    _upsert_export_state_record(
        args,
        site,
        {"file_name": "B楼_202606_001.xlsx", "file_prefix": "B楼_202606"},
        "created",
    )
    _upsert_export_state_record(
        args,
        site,
        {"file_name": "B楼_202606_001.xlsx", "file_prefix": "B楼_202606"},
        "generating",
    )

    records = json.loads(state_file.read_text(encoding="utf-8"))["records"]
    assert len(records) == 1
    assert records[0]["building"] == "B楼"
    assert records[0]["period"] == "2026-06"
    assert records[0]["file_name"] == "B楼_202606_001.xlsx"
    assert records[0]["status"] == "generating"


def test_downloaded_record_takes_precedence_for_same_month(tmp_path):
    state_file = tmp_path / "export_records.json"
    state_file.write_text(
        json.dumps(
            {
                "version": 2,
                "records": [
                    {
                        "building": "C楼",
                        "period": "2026-06",
                        "file_name": "done.xlsx",
                        "status": "downloaded",
                    },
                    {
                        "building": "C楼",
                        "period": "2026-06",
                        "file_name": "old_pending.xlsx",
                        "status": "created",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    args = build_default_args(state_file=str(state_file), period="2026-06")
    site = SiteConfig(building="C楼", host="127.0.0.1", username="u", password="p")

    assert [item["file_name"] for item in _completed_state_records(args, site)] == ["done.xlsx"]
    assert [item["file_name"] for item in _pending_state_records(args, site)] == ["old_pending.xlsx"]


def test_default_download_root_prefers_shared_bridge_root():
    root = _default_download_root(
        {
            "shared_bridge": {"root_dir": r"D:\share"},
            "paths": {"business_root_dir": r"D:\QLDownload"},
        }
    )

    assert str(root) == r"D:\share"


def test_default_download_root_reads_common_shared_bridge_root():
    root = _default_download_root(
        {
            "common": {
                "shared_bridge": {"root_dir": r"D:\share"},
                "paths": {"business_root_dir": r"D:\QLDownload"},
            }
        }
    )

    assert str(root) == r"D:\share"


def test_filename_has_period_date_supports_legacy_names():
    assert _filename_has_period_date("A楼_202606_告警规则.xlsx", "2026-06")
    assert _filename_has_period_date("A楼_20260613_告警规则.xlsx", "2026-06")
    assert _filename_has_period_date("A楼-2026-06-13-告警规则.xlsx", "2026-06")
    assert _filename_has_period_date("A楼_2026年6月13日_告警规则.xlsx", "2026-06")
    assert _filename_has_period_date("E楼_001_南通阿里保税A区E楼_0612085913-20260612085913", "2026-06")
    assert _filename_has_period_date("E楼_告警规则_0531164438-20260531164438", "2026-05")
    assert not _filename_has_period_date("E楼_告警规则_0531164438-20260531164438", "2026-06")
    assert not _filename_has_period_date("A楼_20260531_告警规则.xlsx", "2026-06")
    assert not _filename_has_period_date("A楼_无日期_告警规则.xlsx", "2026-06")


def test_list_and_resolve_downloaded_files_by_exact_name(tmp_path):
    shared_root = tmp_path / "share"
    target_dir = shared_root / "告警规则导出" / "2026-06" / "A楼"
    target_dir.mkdir(parents=True)
    exported_file = target_dir / "A楼_202606_001_alarm.xlsx"
    exported_file.write_text("ok", encoding="utf-8")
    state_file = tmp_path / "export_records.json"
    state_file.write_text(
        json.dumps(
            {
                "version": 2,
                "records": [
                    {
                        "building": "A楼",
                        "period": "2026-06",
                        "file_name": exported_file.name,
                        "status": "downloaded",
                        "downloaded_path": str(exported_file),
                        "downloaded_at": "2026-06-01T03:40:00",
                    },
                    {
                        "building": "A楼",
                        "period": "2026-06",
                        "file_name": "A楼_pending.xlsx",
                        "status": "created",
                        "downloaded_path": str(target_dir / "A楼_pending.xlsx"),
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    config = {"shared_bridge": {"root_dir": str(shared_root)}}

    listing = list_alarm_rule_export_files(
        config=config,
        period="2026-06",
        building="A楼",
        state_file=state_file,
    )
    resolved_path, metadata = resolve_alarm_rule_export_file(
        config=config,
        period="2026-06",
        building="A楼",
        file_name=exported_file.name,
        state_file=state_file,
    )

    assert listing["count"] == 1
    assert listing["files"][0]["file_name"] == exported_file.name
    assert listing["files"][0]["relative_path"].endswith("告警规则导出/2026-06/A楼/A楼_202606_001_alarm.xlsx")
    assert resolved_path == exported_file
    assert metadata["file_exists"] is True
