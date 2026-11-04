from __future__ import annotations

import json
import threading
from pathlib import Path

from handover_log_module.service.handover_download_service import HandoverDownloadService


def _config():
    return {
        "download": {"template_name": "交接班日志（李世龙）", "scale_label": "5分钟", "max_retries": 1},
        "sites": [{"building": "A楼", "enabled": True, "host": "192.168.1.2", "username": "u", "password": "p"}],
        "capacity_report": {"download": {"template_name": "交接班容量报表", "scale_label": "5分钟"}},
    }


def _payload(site, **kwargs):
    return {
        "schema_version": 1, "source_family": kwargs["source_family"], "building": site["building"],
        "business_date": kwargs.get("business_date", ""), "query_start": kwargs["query_start"],
        "query_end": kwargs["query_end"], "requested_point_count": 1, "returned_point_count": 1,
        "sheets": [{"name": "sheet1", "rows": [[], [None, None, None, None, "2026-11-04 09:00:00"], [], [None, "房间", "设备", "温度", "1"]]}],
    }


def test_handover_and_capacity_use_api_json(tmp_path, monkeypatch):
    calls = []

    def query(site, **kwargs):
        calls.append(kwargs)
        return _payload(site, **kwargs)

    monkeypatch.setattr("handover_log_module.service.handover_download_service.query_report_json", query)
    service = HandoverDownloadService(_config(), business_root_override=tmp_path)
    common = dict(buildings=["A楼"], start_time="2026-11-04 08:40:00", end_time="2026-11-04 09:00:00", reuse_cached=False, emit_log=lambda _: None)
    log_result = service.run(**common)
    capacity_result = service.run_capacity_only(**common)
    assert not log_result["failed"] and not capacity_result["failed"]
    assert [item["source_family"] for item in calls] == ["handover_log_family", "handover_capacity_report_family"]
    assert Path(log_result["success_files"][0]["file_path"]).suffix == ".json"
    assert json.loads(Path(capacity_result["success_files"][0]["file_path"]).read_text(encoding="utf-8"))["building"] == "A楼"


def test_branch_api_retains_business_date_and_sheet(tmp_path, monkeypatch):
    calls = []
    def query(site, **kwargs):
        calls.append(kwargs)
        return _payload(site, **kwargs)

    monkeypatch.setattr("handover_log_module.service.handover_download_service.query_report_json", query)
    service = HandoverDownloadService(_config(), business_root_override=tmp_path)
    result = service.run_branch_power_only(
        buildings=["A楼"], start_time="2026-11-02 23:50:00", end_time="2026-11-04 02:20:00",
        business_date="2026-11-03", reuse_cached=False, emit_log=lambda _: None,
    )
    assert not result["failed"]
    assert calls[0]["source_family"] == "branch_power_family"
    assert calls[0]["sheet_name"] == "支路功率"
    assert calls[0]["business_date"] == "2026-11-03"


def test_manual_api_fetch_starts_all_buildings_together(tmp_path, monkeypatch):
    barrier = threading.Barrier(5, timeout=5)
    def query(site, **kwargs):
        barrier.wait()
        return _payload(site, **kwargs)
    monkeypatch.setattr("handover_log_module.service.handover_download_service.query_report_json", query)
    config = _config()
    config["sites"] = [{"building": f"{letter}楼", "enabled": True} for letter in "ABCDE"]
    service = HandoverDownloadService(config, business_root_override=tmp_path)
    result = service.run(buildings=[f"{letter}楼" for letter in "ABCDE"],
                         start_time="2026-11-04 08:40:00", end_time="2026-11-04 09:00:00",
                         reuse_cached=False, emit_log=lambda _: None)
    assert not result["failed"] and len(result["success_files"]) == 5
