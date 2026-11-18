import copy
import json
import time
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest
from app.modules.inspection_data.service import inspection_data_upload_service as module
from app.modules.inspection_data.service.inspection_data_upload_service import InspectionDataUploadService
from test_inspection_data_service import inspection_datasets, Cloud


@pytest.fixture(autouse=True)
def isolate(monkeypatch):
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", Mock(side_effect=AssertionError("real network forbidden")))
    monkeypatch.setattr(module, "require_feishu_auth_settings", Mock(side_effect=AssertionError("real auth forbidden")))
    monkeypatch.setattr(module, "FeishuBitableClient", Mock(side_effect=AssertionError("real Feishu forbidden")))
    monkeypatch.setattr(module, "load_point_settings", lambda config: {})
    monkeypatch.setattr(module, "refresh_cloud_point_settings", lambda *a, **k: ({"D楼": {}, "E楼": {}}, 1, {}))
    monkeypatch.setattr(module, "report_operating_controls", lambda *a: {})
    monkeypatch.setattr(module, "handover_inspection_rows", lambda *a, **k: {})


def docs(tmp_path):
    now = datetime.now().replace(microsecond=0)
    result = list(inspection_datasets(tmp_path)["D楼"].values())
    for doc in result:
        doc.update(query_start=(now-timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S"),
                   query_end=now.strftime("%Y-%m-%d %H:%M:%S"), value_time=now.strftime("%Y-%m-%d %H:%M:%S"),
                   bucket_key=module.inspection_hour_bucket(now), collection_started_at=int(time.time()*1000))
    return result


class Internal:
    def __init__(self, documents, collect=True):
        self.documents = documents
        self.refreshes = []
        self.collect = collect
    def refresh_latest_source_cache(self, **kwargs):
        self.refreshes.append(kwargs)
        if self.collect:
            for doc in self.documents:
                if doc["source_family"] == kwargs["source_family"]:
                    doc["collection_started_at"] = int(time.time()*1000)
        return {"ok": True, "accepted": True}
    def get_latest_inspection_points(self, **kwargs):
        return {"ok": True, "datasets": copy.deepcopy(self.documents)}


@pytest.mark.parametrize("stale_kind", ["prior_collection", "old_sample", "missing_collection"])
def test_same_bucket_cached_or_old_sample_never_satisfies_live_request(tmp_path, stale_kind):
    documents = docs(tmp_path)
    floor = int(time.time()*1000)-1000
    if stale_kind == "prior_collection":
        for doc in documents: doc["collection_started_at"] -= 5000
    elif stale_kind == "missing_collection":
        for doc in documents: doc.pop("collection_started_at")
    else:
        for doc in documents: doc["value_time"] = doc["query_start"]
    internal = Internal(documents, collect=False)
    service = InspectionDataUploadService({"inspection_data_upload": {"wait_source_timeout_sec": 0}},
                                        internal_client=internal, bitable_client=Cloud())
    with pytest.raises(RuntimeError, match="未齐或过期"):
        service._sources(datetime.now().strftime("%Y-%m-%d"), lambda _: None, lambda: None,
                         point_settings={"D楼": {}, "E楼": {}}, buildings=["D楼"], scoped_building="D楼",
                         not_before=floor, force_refresh=True)
    assert len(internal.refreshes) == 2
    assert all(r["force_refresh"] is True and r["buildings"] == ["D楼"] for r in internal.refreshes)


def test_real_service_mock_network_upload_receipt_matches_verified_complete_batch(tmp_path, monkeypatch):
    documents = docs(tmp_path)
    internal = Internal(documents)
    cloud = Cloud()
    service = InspectionDataUploadService({"inspection_data_upload": {"wait_source_timeout_sec": 0}},
                                        internal_client=internal, bitable_client=cloud)
    monkeypatch.setattr(module, "BACKUP_ROOT", tmp_path/"backups")
    floor = int(time.time()*1000)-2000
    result = service.run(building="D楼", force_refresh=True, not_before=floor)
    assert result["status"] == "success" and result["buildings"] == ["D楼"]
    assert floor <= result["source_time"] <= result["uploaded_at"]
    real_times = [row["fields"]["源文件时间"] for row in cloud.rows[module.DEFAULT_CONFIG["tables"]["D楼"]]
                  if row["fields"].get("源文件时间") is not None]
    assert min(real_times) == result["source_time"]
    item = result["results"]["D楼"]
    assert service.read_completed_batch(building="D楼", upload_time=item["upload_time"]) == item["batch_completion"]
    assert item["uploaded_count"] == item["batch_completion"]["device_count"] + item["batch_completion"]["point_count"]
    assert not any(event[1] == module.DEFAULT_CONFIG["tables"]["E楼"] for event in cloud.events)


def test_stale_real_supplement_blocks_before_table_writes(tmp_path, monkeypatch):
    documents = docs(tmp_path)
    cloud = Cloud()
    service = InspectionDataUploadService({"inspection_data_upload": {"wait_source_timeout_sec": 0}},
                                         internal_client=Internal(documents), bitable_client=cloud)
    floor = int(time.time()*1000)-2000
    old = {"记录类型": "测点明细", "测点来源": "collected", "源文件时间": floor-1, "采样时间": floor+1000}
    monkeypatch.setattr(module, "handover_inspection_rows", lambda *a, **k: {"D楼": [old]})
    with pytest.raises(ValueError, match="采样时间"):
        service.run(building="D楼", force_refresh=True, not_before=floor)
    assert cloud.events == []


def test_force_refresh_does_not_forward_lighthouse_key_to_internal_bridge(monkeypatch):
    from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient
    import urllib.request
    requests = []
    class Response:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self): return b'{"ok":true}'
    def open_request(request, **kwargs):
        requests.append(request)
        return Response()
    monkeypatch.setattr(urllib.request, "urlopen", open_request)
    client = InternalBridgeHttpClient.from_runtime_config({"internal_bridge_http": {"base_url": "http://isolated.invalid"},
        "inspection_data_upload": {"lighthouse_bridge": {"token": "isolated-token"}}})
    client.refresh_latest_source_cache(source_family="electrical_inspection_family", buildings=["D楼"], force_refresh=True)
    client.refresh_latest_source_cache(source_family="electrical_inspection_family", buildings=["D楼"])
    client.refresh_latest_source_cache(source_family="pue_confirmation_family", buildings=["D楼"], force_refresh=True)
    assert json.loads(requests[0].data)["force_refresh"] is True
    assert all(request.get_header("Authorization") is None for request in requests)


def test_verified_upload_receipt_is_saved_before_later_local_failure(tmp_path, monkeypatch):
    from app.modules.inspection_data.service import live_requests as live
    import uuid
    documents = docs(tmp_path)
    cloud = Cloud()
    config = {"paths": {"runtime_state_root": str(tmp_path)}, "inspection_data_upload": {
        "wait_source_timeout_sec": 0, "lighthouse_bridge": {"enabled": True}}}
    service = InspectionDataUploadService(config, internal_client=Internal(documents), bitable_client=cloud)
    monkeypatch.setattr(module, "BACKUP_ROOT", tmp_path/"backups")
    original_write = module.atomic_write_text
    def failing_local_checkpoint(path, content):
        import json
        if json.loads(content).get("status") == "completed":
            raise OSError("local disk checkpoint failed")
        return original_write(path, content)
    monkeypatch.setattr(module, "atomic_write_text", failing_local_checkpoint)
    monkeypatch.setattr(module, "InspectionDataUploadService", lambda config: service)
    rid, floor = uuid.uuid4().hex, int(time.time()*1000)-2000
    store = live.LiveRequestStore(config)
    store.submit(rid, "D楼", floor)
    batch = store.next_batch("D楼")
    with pytest.raises(RuntimeError):
        live.run_live_upload(config, {"live_batch_id": batch["batch_id"]}, lambda _: None)
    receipt = store.get(rid)
    assert receipt["ok"] is True and receipt["state"] == "uploaded"
    table_rows = cloud.rows[module.DEFAULT_CONFIG["tables"]["D楼"]]
    upload_time = table_rows[0]["fields"]["上传日期"]
    assert service.read_completed_batch(building="D楼", upload_time=upload_time) is not None
    events = copy.deepcopy(cloud.events)
    replay, _ = store.submit(rid, "D楼", floor)
    assert replay == receipt
    assert live.run_live_upload(config, {"live_batch_id": batch["batch_id"]}, lambda _: None)["status"] == "skipped"
    assert cloud.events == events


def test_old_auxiliary_mode_keeps_its_original_time_without_blocking_live_batch(tmp_path, monkeypatch):
    from app.modules.inspection_data.service.handover_inspection_inputs import _point_row, MODE_SOURCE
    documents = docs(tmp_path)
    cloud = Cloud()
    service = InspectionDataUploadService({"inspection_data_upload": {"wait_source_timeout_sec": 0}},
                                         internal_client=Internal(documents), bitable_client=cloud)
    old = datetime.now(module.LOCAL_TZ)-timedelta(minutes=18)
    auxiliary = _point_row("D楼", "K7", MODE_SOURCE, "1号制冷单元", "模式辅助", "/", "3", old, "旧缓存原采样")
    old_time = auxiliary["采样时间"]
    auxiliary["源文件时间"] = old_time
    monkeypatch.setattr(module, "handover_inspection_rows", lambda *a, **k: {"D楼": [auxiliary]})
    monkeypatch.setattr(module, "BACKUP_ROOT", tmp_path/"backups")
    floor = int(time.time()*1000)-2000
    result = service.run(building="D楼", force_refresh=True, not_before=floor)
    assert result["status"] == "success" and result["source_time"] >= floor
    actual = [row["fields"] for row in cloud.rows[module.DEFAULT_CONFIG["tables"]["D楼"]]]
    saved = next(row for row in actual if row.get("测点来源") == MODE_SOURCE)
    assert saved["源文件时间"] == saved["采样时间"] == old_time < floor
    sampled_source_times = [row["源文件时间"] for row in actual if row.get("测点来源") != MODE_SOURCE]
    assert result["source_time"] == min(sampled_source_times)


def test_fresh_direct_mode_frequency_and_compressor_current_keep_real_readings():
    from app.modules.inspection_data.service.chiller_operating_points import operation_rows
    from app.modules.inspection_data.service.handover_inspection_inputs import _stamp
    sampled = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    document = {"building": "D楼", "report_name": "暖通巡检表", "value_time": sampled,
                "point_ids": ["7_0_1", "7_0_2", "7_0_3"],
                "rows": [["冷站", "1号制冷单元", "压缩机电流", "0.00"],
                         ["冷站", "1号制冷单元", "模式反馈", "3"],
                         ["冷站", "1号二次泵", "频率反馈", "15.50"]]}
    points = InspectionDataUploadService._point_detail_rows(document, "暖通", {})
    assert points[0]["测点原始值"] == "0.00" and points[0]["测点值"] == "0.00"
    assert points[0]["采样时间"] == int(_stamp(sampled).timestamp()*1000)
    derived = operation_rows("D楼", sampled, points,
        {"unit_modes": {"1": "7_0_2"}, "secondary_frequencies": {"1": "7_0_3"}})
    marks = {row["目标单元格"]: row["测点值"] for row in derived}
    assert marks["K9"] == "√" and marks["K26"] == "√"
    assert all(row["采样时间"] == points[0]["采样时间"] for row in derived)
    # Mode is auxiliary: mode=plate-exchanger does not rewrite compressor current=0.
    assert points[0]["测点原始值"] == "0.00"


def test_old_cached_mode_and_frequency_are_not_relabelled_as_current(monkeypatch):
    from app.modules.inspection_data.service import chiller_operating_points as operating
    from app.modules.inspection_data.service.handover_inspection_inputs import _stamp
    current = datetime.now(module.LOCAL_TZ).replace(microsecond=0)
    older = current-timedelta(minutes=5)
    def cached(*args, **kwargs):
        if kwargs.get("family") == operating.CAPACITY_FAMILY:
            return {"D楼": {(("D楼", "冷冻二次泵", "1"), older): {"30"}}}
        return {"D楼": {("mode", 1, older): {"3"}}}
    monkeypatch.setattr(operating, "_source_observations", cached)
    controls = operating.report_operating_controls(None, {}, {"D楼": {"value_time": current.isoformat()}}, lambda _: None)
    assert controls == {}


def test_live_source_file_clock_uses_actual_sample_not_query_end_or_upload_time(tmp_path, monkeypatch):
    documents = docs(tmp_path)
    query_end = datetime.strptime(documents[0]["query_end"], "%Y-%m-%d %H:%M:%S")
    samples = [query_end-timedelta(minutes=4), query_end-timedelta(minutes=2)]
    for doc, sampled in zip(documents, samples): doc["value_time"] = sampled.strftime("%Y-%m-%d %H:%M:%S")
    floor = int((query_end-timedelta(minutes=5)).replace(tzinfo=module.LOCAL_TZ).timestamp()*1000)
    expected = int(samples[0].replace(tzinfo=module.LOCAL_TZ).timestamp()*1000)
    cloud = Cloud()
    service = InspectionDataUploadService({"inspection_data_upload": {"wait_source_timeout_sec": 0}},
                                         internal_client=Internal(documents), bitable_client=cloud)
    monkeypatch.setattr(module, "BACKUP_ROOT", tmp_path/"backups")
    result = service.run(building="D楼", force_refresh=True, not_before=floor)
    assert result["source_time"] == expected < int(query_end.replace(tzinfo=module.LOCAL_TZ).timestamp()*1000)
    rows = [row["fields"] for row in cloud.rows[module.DEFAULT_CONFIG["tables"]["D楼"]]]
    assert min(row["源文件时间"] for row in rows) == expected
    assert all(row["源文件时间"] == row["采样时间"] for row in rows if row["记录类型"] == "测点明细")
    assert result["uploaded_at"] >= max(row["源文件时间"] for row in rows)


def test_future_real_row_cannot_hide_behind_older_minimum_time():
    current = datetime.now().replace(microsecond=0)
    source = {"D楼": {"hvac": {"value_time": current.strftime("%Y-%m-%d %H:%M:%S")}}}
    now = int(time.time()*1000)
    with pytest.raises(ValueError, match="采样时间"):
        InspectionDataUploadService._minimum_live_source_time(source, {"D楼": {"rows": [
            {"记录类型": "测点明细", "源文件时间": now+60000}]}}, now-5000)
