from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import json

import openpyxl
import pytest

from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService
from app.modules.internal_bridge_http.service.internal_bridge_http_runner import InternalBridgeHttpTaskRunner
from app.modules.shared_bridge.service.shared_source_cache_service import SharedSourceCacheService
from handover_log_module.service import handover_download_service as download


FAMILY = "handover_capacity_report_family"
BUCKET = "2026-11-26 09"


def workbook(path, *, building="C楼", stamp="2026-11-26 09:05:00", value=0):
    path.parent.mkdir(parents=True, exist_ok=True)
    book = openpyxl.Workbook()
    ws = book.active
    ws.title = "sheet1"
    ws["E2"] = stamp
    ws.append([])
    ws.append([None, f"南通/{building}/配电室", "TR-1", "B相温度", value])
    book.save(path)
    book.close()
    return path


@pytest.fixture
def cache(tmp_path):
    root = tmp_path / "share"
    root.mkdir()
    store = SharedBridgeStore(root)
    store.ensure_ready()
    return SharedSourceCacheService(runtime_config={
        "deployment": {"role_mode": "internal"},
        "shared_bridge": {"enabled": True, "root_dir": str(root)},
        "internal_source_cache": {"enabled": True},
    }, store=store, emit_log=lambda _: None)


@pytest.mark.parametrize("family", ["handover_log_family", FAMILY])
def test_manual_excel_is_registered_as_validated_json_with_shift(cache, family, monkeypatch):
    monkeypatch.setattr("app.modules.shared_bridge.service.shared_source_cache_service._now_dt",
                        lambda: datetime(2026, 11, 26, 10))
    path = cache._source_target_path(source_family=family, building="C楼", bucket_kind="latest",
                                     bucket_key=BUCKET, duty_date="", duty_shift="", source_path=Path("f.xlsx"))
    workbook(path)
    kwargs = dict(source_family=family, building="C楼", bucket_kind="latest", bucket_key=BUCKET)
    row = cache.recover_handover_excel(**kwargs)
    assert path.exists() and row["file_path"].endswith(".json")
    assert (row["duty_date"], row["duty_shift"]) == ("2026-11-26", "day")
    data = json.loads(Path(row["file_path"]).read_text(encoding="utf-8"))
    assert data["building"] == "C楼" and data["source_family"] == family
    assert data["sheets"][0]["rows"][3][4] == 0
    assert cache._existing_latest_refresh_result(source_family=family, building="C楼", bucket_key=BUCKET)["reason"] == "already_ready"
    restored = cache.recover_handover_excel(source_family=family, building="C楼", bucket_kind="date",
                                             bucket_key="2026-11-26", duty_date="2026-11-26", duty_shift="day")
    assert restored["bucket_kind"] == "date" and restored["duty_shift"] == "day"


@pytest.mark.parametrize("building,stamp", [("B楼", "2026-11-26 09:05:00"), ("C楼", "2026-11-25 09:05:00")])
def test_renaming_wrong_file_is_not_enough(cache, building, stamp):
    path = cache._source_target_path(source_family=FAMILY, building="C楼", bucket_kind="latest",
                                     bucket_key=BUCKET, duty_date="", duty_shift="", source_path=Path("f.xlsx"))
    workbook(path, building=building, stamp=stamp)
    assert cache.recover_handover_excel(source_family=FAMILY, building="C楼", bucket_kind="latest", bucket_key=BUCKET) is None
    assert not path.with_suffix(".json").exists()


@pytest.mark.parametrize("value", ["--", "无权限", "NaN"])
def test_excel_fallback_rejects_sheet_without_a_real_sample(tmp_path, value):
    raw = workbook(tmp_path / "vendor.xlsx", value=value)
    with pytest.raises(ValueError, match="没有有效测点读数"):
        download.handover_excel_payload(raw, family=FAMILY, building="C楼")


def test_excel_cannot_register_a_later_timestamp_hidden_after_header_gap(cache):
    path = cache._source_target_path(source_family=FAMILY, building="C楼", bucket_kind="latest",
                                     bucket_key=BUCKET, duty_date="", duty_shift="", source_path=Path("f.xlsx"))
    workbook(path)
    book = openpyxl.load_workbook(path)
    book.active["G2"] = "2026-11-26 10:05:00"
    book.active["G4"] = 77
    book.save(path)
    book.close()
    assert cache.recover_handover_excel(
        source_family=FAMILY, building="C楼", bucket_kind="latest", bucket_key=BUCKET) is None
    assert not path.with_suffix(".json").exists()


def test_http_index_recovers_manual_excel_when_requested_by_external(cache):
    path = cache._source_target_path(source_family=FAMILY, building="C楼", bucket_kind="latest",
                                     bucket_key=BUCKET, duty_date="", duty_shift="", source_path=Path("f.xlsx"))
    workbook(path)
    runner = InternalBridgeHttpTaskRunner(runtime_service=SimpleNamespace(
        shared_bridge_root=str(cache.shared_root), _source_cache_service=cache))
    rows = runner._recover_source_index_from_existing_files_if_needed(
        [], source_family=FAMILY, building="C楼", bucket_kind="latest", bucket_key=BUCKET)
    assert len(rows) == 1
    assert rows[0]["relative_path"].endswith("C楼.json") and rows[0]["duty_shift"] == "day"


def test_browser_reuses_exact_query_only_for_failed_floor(monkeypatch, tmp_path):
    config = {"download": {"template_name": "交接班容量报表", "scale_label": "5分钟", "report_kind": "capacity"},
              "sites": [{"building": b, "enabled": True} for b in ("A楼", "C楼")]}
    service = download.HandoverDownloadService(config, business_root_override=tmp_path)
    success = {"building": "A楼", "success": True, "file_path": "A.json"}
    monkeypatch.setattr(service, "_run_report_api", lambda **_: {"success_files": [success], "failed": [
        {"building": "C楼", "error": "缺少3个测点"}]})
    raw = workbook(tmp_path / "source.xlsx")
    export = Mock(return_value=[{"building": "C楼", "success": True, "file_path": str(raw)}])
    monkeypatch.setattr(download.DownloadGateway, "download_handover_xlsx_batch", export)
    result = service.run(buildings=["A楼", "C楼"], start_time="2026-11-26 08:50:00", end_time="2026-11-26 09:10:00")
    assert not result["failed"] and result["success_files"][0] == success
    assert export.call_args.kwargs["buildings"] == ["C楼"]
    assert export.call_args.kwargs["start_time"] == result["start_time"]
    assert export.call_args.kwargs["end_time"] == result["end_time"]
    assert export.call_args.kwargs["template_name"] == "交接班容量报表"


def test_single_building_refresh_marks_ready_only_after_excel_fallback(cache, monkeypatch, tmp_path):
    now = datetime(2026, 11, 26, 9, 15)
    module = "app.modules.shared_bridge.service.shared_source_cache_service"
    monkeypatch.setattr(f"{module}._now_dt", lambda: now)
    monkeypatch.setattr(f"{module}.load_handover_config", lambda _: {
        "sites": [{"building": "C楼", "enabled": True}],
        "download": {"max_retries": 1, "scale_label": "5分钟"},
        "capacity_report": {"download": {"template_name": "交接班容量报表"}},
    })
    monkeypatch.setattr(download, "query_report_json", Mock(side_effect=ValueError("配置606，返回603")))
    raw = workbook(tmp_path / "export.xlsx")
    export = Mock(return_value=[{"building": "C楼", "success": True, "file_path": str(raw)}])
    monkeypatch.setattr(download.DownloadGateway, "download_handover_xlsx_batch", export)
    messages = []
    cache.emit_log = messages.append
    started = cache.start_building_latest_refresh(source_family=FAMILY, building="C楼", target_bucket_key=BUCKET)
    assert started["accepted"]
    with cache._lock:
        thread = cache._building_latest_refresh_threads.get((FAMILY, "C楼", "latest", BUCKET))
    if thread is not None:
        thread.join(15)
        assert not thread.is_alive()
    row = cache._get_ready_entry(source_family=FAMILY, building="C楼", bucket_kind="latest", bucket_key=BUCKET)
    path = cache._resolve_entry_file_path(row)
    assert path and path.with_suffix(".xlsx").is_file()
    assert json.loads(path.read_text(encoding="utf-8"))["source_format"] == "xlsx"
    export.assert_called_once()
    assert any("Excel兜底已通过源表校验" in msg for msg in messages)


def test_partial_api_result_with_filled_ids_does_not_open_browser(monkeypatch, tmp_path):
    svc = download.HandoverDownloadService({
        "download": {"report_kind": "capacity", "template_name": "交接班容量报表", "scale_label": "5分钟"},
        "sites": [{"building": "C楼", "enabled": True}],
    }, business_root_override=tmp_path)
    def collect(site, **args):
        return {
            "schema_version": 1, "source_family": FAMILY, "building": site["building"], "business_date": "",
            "query_start": args["query_start"], "query_end": args["query_end"],
            "requested_point_count": 2, "returned_point_count": 2, "api_returned_point_count": 1,
            "zero_filled_point_ids": ["1_3"], "sheets": [{"name":"sheet1","rows":[[],
                ["", "", "", "", "2026-11-26 09:05:00"], [],
                ["", "C楼", "TR-1", "温度", 25], ["", "C楼", "未返回测点", "1_3", 0]]}],
        }
    monkeypatch.setattr(download, "query_report_json", collect)
    def unexpected(*args, **kwargs):
        pytest.fail("补齐后的合法 API 结果不应触发浏览器")
    monkeypatch.setattr(download.DownloadGateway, "download_handover_xlsx_batch", unexpected)
    result = svc.run(buildings=["C楼"], start_time="2026-11-26 08:55:00", end_time="2026-11-26 09:15:00")
    assert result["failed"] == []
    doc = json.loads(Path(result["success_files"][0]["file_path"]).read_text(encoding="utf-8"))
    assert (doc["api_returned_point_count"], doc["zero_filled_point_ids"]) == (1, ["1_3"])


def test_failed_api_and_wrong_building_fallback_return_both_causes(monkeypatch, tmp_path):
    svc = download.HandoverDownloadService({
        "download": {"report_kind":"capacity", "template_name":"交接班容量报表", "scale_label":"5分钟", "max_retries":1},
        "sites":[{"building":"C楼","enabled":True}],
    }, business_root_override=tmp_path)
    monkeypatch.setattr(download, "query_report_json", Mock(side_effect=TimeoutError("接口超时")))
    wrong = workbook(tmp_path / "wrong.xlsx", building="B楼")
    monkeypatch.setattr(download.DownloadGateway,"download_handover_xlsx_batch",Mock(return_value=[
        {"building":"C楼","success":True,"file_path":str(wrong)}]))
    result = svc.run(buildings=["C楼"], start_time="2026-11-26 08:55:00", end_time="2026-11-26 09:15:00")
    assert not result["success_files"]
    assert "API: 接口超时" in result["failed"][0]["error"]
    assert "Excel:" in result["failed"][0]["error"] and "楼栋不匹配" in result["failed"][0]["error"]


def test_night_bridge_failure_reports_the_only_incomplete_building(monkeypatch):
    module = "app.modules.shared_bridge.service.shared_bridge_runtime_service"
    succeeded = [{"building": f"{b}楼", "file_path": f"{b}.json"} for b in "ABCDE"]
    collected = {"handover": {"success_files": succeeded}, "capacity": {
        "success_files": [row for row in succeeded if row["building"] != "C楼"],
        "failed": [{"building": "C楼", "error": "报表接口返回测点不完整"}],
    }}
    monkeypatch.setattr(f"{module}.load_handover_config", lambda _: {})
    monkeypatch.setattr(f"{module}.HandoverDownloadService", lambda *a, **k: SimpleNamespace(
        ensure_internal_ready=Mock(), run_with_capacity_report=Mock(return_value=collected)))
    bridge = SimpleNamespace(_store=Mock(), runtime_config={}, _source_cache_service=None,
                             _internal_download_pool=None, _stage_claim_token=Mock(return_value="claim"),
                             _bridge_emit=Mock(return_value=Mock()), current_source_cache_bucket=Mock(),
                             _fail_bound_job=Mock(), _request_runtime_status_refresh=Mock())
    SharedBridgeRuntimeService._run_handover_internal_download(bridge, {
        "task_id": "test-night", "request": {"duty_date": "2026-11-25", "duty_shift": "night"},
    })
    stage = bridge._store.complete_stage.call_args.kwargs
    assert stage["next_task_status"] == "failed"
    assert stage["stage_error"] == "C楼/容量表：报表接口返回测点不完整"
    assert len(stage["stage_result"]["capacity_files"]) == 4
