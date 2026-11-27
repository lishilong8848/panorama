from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from unittest.mock import Mock

import 下载动环表格 as pipeline
from app.modules.shared_bridge.service.report_missing_notice import ReportMissingNoticeOutbox


def test_monthly_download_uses_api_and_keeps_business_date(tmp_path, monkeypatch):
    calls = []

    def query(site, **kwargs):
        calls.append(kwargs)
        return {
            "schema_version": 1, "source_family": "monthly_report_family", "building": site["building"],
            "business_date": kwargs["business_date"], "query_start": kwargs["query_start"],
            "query_end": kwargs["query_end"], "requested_point_count": 1, "returned_point_count": 1,
            "sheets": [{"name": "sheet1", "rows": [[], [None, None, None, None, "2026-11-03 00:00:00"], [], [None, "楼栋", "设备", "电度", "12"]]}],
        }

    monkeypatch.setattr(pipeline, "query_report_json", query)
    task = pipeline.DownloadTask(
        date_text="2026-11-03", start_time="2026-11-02 23:50:00", end_time="2026-11-04 01:20:00",
        save_dir=str(tmp_path), site={"building": "A楼"},
    )
    pairs = asyncio.run(pipeline._run_download_tasks_by_building({}, [task], "月报", "完成", "失败"))
    outcome = pairs[0][1]
    assert outcome.success
    assert Path(outcome.file_path).suffix == ".json"
    assert json.loads(Path(outcome.file_path).read_text(encoding="utf-8"))["business_date"] == "2026-11-03"
    assert calls[0]["source_family"] == "monthly_report_family"


def test_scheduled_monthly_api_starts_all_five_buildings(monkeypatch, tmp_path):
    barrier = threading.Barrier(5, timeout=5)
    def query(site, **kwargs):
        barrier.wait()
        return {
            "schema_version": 1, "source_family": "monthly_report_family", "building": site["building"],
            "business_date": kwargs["business_date"], "query_start": kwargs["query_start"], "query_end": kwargs["query_end"],
            "sheets": [{"name": "sheet1", "rows": [[], [None, None, None, None, "2026-11-03 00:00:00"], [],
                                                        [None, "楼栋", "设备", "电度", 1]]}],
        }
    monkeypatch.setattr(pipeline, "query_report_json", query)
    tasks = [pipeline.DownloadTask(date_text="2026-11-03", start_time="2026-11-02 23:50:00",
                                   end_time="2026-11-04 01:20:00", save_dir=str(tmp_path),
                                   site={"building": building}) for building in ("A楼", "B楼", "C楼", "D楼", "E楼")]
    results = asyncio.run(pipeline._run_download_tasks_by_building({}, tasks, "月报", "完成", "失败"))
    assert len(results) == 5 and all(outcome.success for _, outcome in results)


def test_monthly_empty_day_alert_is_deduped(tmp_path, monkeypatch):
    config = {"paths": {"runtime_state_root": str(tmp_path / "state")}}
    task = pipeline.DownloadTask(date_text="2026-11-03", start_time="2026-11-02 23:50:00",
                                 end_time="2026-11-04 01:20:00", save_dir=str(tmp_path), site={"building": "A楼"})
    monkeypatch.setattr(pipeline, "query_report_json", Mock(side_effect=pipeline.ReportNoSamples("月报无实测")))
    outbox = ReportMissingNoticeOutbox(config)
    for _ in range(2):
        results = asyncio.run(pipeline._run_download_tasks_by_building(config, [task], "月报", "完成", "失败"))
        assert not results[0][1].success
    notice = outbox.claim()
    assert "窗口：2026-11-03" in notice["message"]
    assert outbox.acknowledge(notice["notice_id"], notice["lease_token"])
    assert outbox.claim() is None and not list(tmp_path.glob("*.json"))
