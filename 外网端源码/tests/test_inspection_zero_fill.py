import copy
import socket
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.modules.inspection_data.service import inspection_data_upload_service as upload, point_settings
from app.modules.inspection_data.service.chiller_operating_points import HVAC_FAMILY, OPERATING_SOURCE
from app.modules.scheduler.api import inspection_data_upload_routes as routes
from test_inspection_data_service import Cloud, inspection_datasets


REAL, STATE, FREQ, UNKNOWN = IDS = ["7_0_1", "7_0_2", "7_0_3", "7_0_4"]
DATE, BUCKET = "2026-10-27", "2026-10-27 10"


@pytest.fixture
def scenario(tmp_path, monkeypatch):
    forbidden = Mock(side_effect=AssertionError("real network forbidden"))
    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    docs = inspection_datasets(tmp_path)
    doc = docs["D楼"][HVAC_FAMILY]
    room, device = doc["rows"][0][:2]
    doc.update(point_ids=IDS[:], requested_point_ids=IDS[:], zero_filled_point_ids=IDS[1:],
               rows=[[room, device, "启停状态", "0"], [room, device, "运行状态", "0"],
                     ["", "", "", "0"], ["", "", "", "0"]])
    rule = {"type": "状态", "not_running": ["0"]}
    group = {
        "baseline": {STATE: {"name": "已核验启停", "path": "核验路径"}, FREQ: {"name": "1#二次泵频率反馈", "path": "泵路径"}},
        "points": {UNKNOWN: "备注不是名称"},
        "targets": {pid: {"cell": cell} for pid, cell in zip(IDS, ("A4", "B4", "C4", "D4"))},
        "status_rules": {REAL: rule, STATE: {"type": "状态", "intervals": [{"min": "0", "max": "0", "status": "normal"}]},
                         FREQ: {"type": "直接填充"}},
        "correction_rules": {pid: [{"min": "0", "max": "0", "value": "15"}] for pid in IDS[1:]},
        "operating_rules": {"unit_modes": {"1": STATE}, "secondary_frequencies": {"1": FREQ}},
    }
    settings = {"D楼": {HVAC_FAMILY: group}, "E楼": {}}
    bridge = Mock()
    bridge.get_inspection_snapshot.return_value = {"ok": True, "bucket_key": BUCKET, "datasets": list(docs["D楼"].values())}
    bridge.get_latest_inspection_points.return_value = bridge.get_inspection_snapshot.return_value
    cloud = Cloud()
    service = upload.InspectionDataUploadService({}, internal_client=bridge, bitable_client=cloud)
    monkeypatch.setattr(upload, "load_point_settings", lambda *_: settings)
    monkeypatch.setattr(upload, "refresh_cloud_point_settings", lambda *a, **kw: (settings, 1, {}))
    monkeypatch.setattr(upload, "report_operating_controls", lambda *_: {})
    monkeypatch.setattr(upload, "handover_inspection_rows", lambda *a, **kw: {})
    monkeypatch.setattr(upload, "BACKUP_ROOT", tmp_path / "backups")
    monkeypatch.setattr(routes.InternalBridgeHttpClient, "from_runtime_config", lambda *_: bridge)
    monkeypatch.setattr(routes, "verified_review_point_settings", lambda *_: settings)
    return doc, service, cloud, docs


@pytest.mark.parametrize("actual,device_state,point_state", [("0", "关闭", "△"), ("1", "开启", "待核对")])
def test_only_real_sample_controls_device_and_placeholder_is_not_corrected_or_used_for_chillers(
        scenario, actual, device_state, point_state):
    doc, service, cloud, _ = scenario
    doc["rows"][0][3] = actual
    result = service.run(source_date=DATE, building="D楼")
    assert result["results"]["D楼"]["sources"][HVAC_FAMILY]["zero_filled_point_ids"] == IDS[1:]
    rows = [r["fields"] for r in cloud.rows[upload.DEFAULT_CONFIG["tables"]["D楼"]]]
    points = {r["测点ID"]: r for r in rows if r.get("测点ID") in IDS}
    assert points[REAL]["测点原始值"] == actual and points[REAL]["状态判定"] == point_state
    for pid in IDS[1:]:
        assert points[pid]["测点原始值"] == "" and "测点修正值" not in points[pid]
        assert points[pid]["数据完整性"] == "缺少源数据" and "采样时间" not in points[pid]
    assert points[STATE]["状态判定"] == "待核对" and points[STATE]["测点值"] == ""
    assert (points[STATE]["测点名称"], points[STATE]["测点路径"]) == ("已核验启停", "核验路径")
    assert points[FREQ]["测点值"] == "0"
    assert all(points[UNKNOWN][key] == "" for key in ("测点值", "测点名称", "测点路径", "设备编号", "位置"))
    assert points[UNKNOWN]["测点备注"] == "备注不是名称"
    marks = {r["目标单元格"]: r["测点值"] for r in rows if r.get("测点来源") == OPERATING_SOURCE}
    assert all(marks[cell] == "" for cell in ("B4", "K4", "K7", "K26", "K24"))
    device = next(r for r in rows if r["记录类型"] == "设备巡检" and r["专业"] == "暖通"
                  and r["设备编号"] == doc["rows"][0][1] and r["位置"] == doc["rows"][0][0])
    assert device["启停状态"] == device_state and "运行状态" not in device
    assert result["results"]["D楼"]["sources"][HVAC_FAMILY]["valid_points"] == 1


def test_only_real_added_points_can_enable_unmapped_upload(scenario):
    doc, service, cloud, _ = scenario
    doc["rows"][0][:3] = ["D楼", "未匹配设备", "未匹配信号"]
    doc["point_overrides"] = {"add": [FREQ]}
    with pytest.raises(ValueError, match="没有可识别的有效测点"):
        service.run(source_date=DATE, building="D楼")
    assert not cloud.events
    doc["point_overrides"] = {"add": [REAL]}
    assert service.run(source_date=DATE, building="D楼", dry_run=True)["status"] == "success"


def test_missing_samples_keep_source_identity_and_are_summarized_separately(scenario):
    doc, service, cloud, _ = scenario
    room, device = doc["rows"][0][:2]
    doc.update(zero_filled_point_ids=[UNKNOWN], zero_filled_sample_point_ids=[STATE, FREQ])
    doc["rows"][2][:3] = ["", "", "频率反馈"]
    summary = [{"point_id": pid, "device": device, "metric": metric, "count": 1}
               for pid, metric in ((STATE, "运行状态"), (FREQ, "频率反馈"))]
    logs = []
    dry_run = service.run(source_date=DATE, building="D楼", dry_run=True, log=logs.append)
    assert not cloud.events
    result = service.run(source_date=DATE, building="D楼")
    for response in (dry_run, result):
        source = response["results"]["D楼"]["sources"][HVAC_FAMILY]
        assert source["zero_filled_point_ids"] == [UNKNOWN]
        assert source["zero_filled_sample_point_ids"] == [STATE, FREQ]
        assert response["missing_sample_points"] == source["missing_sample_points"] == summary
        assert source["valid_points"] == 1
    assert any("整行缺失=1，采样值缺失=2" in message for message in logs)
    rows = [r["fields"] for r in cloud.rows[upload.DEFAULT_CONFIG["tables"]["D楼"]]]
    points = {r["测点ID"]: r for r in rows if r.get("测点ID") in IDS}
    assert points[REAL]["测点原始值"] == "0" and "采样时间" in points[REAL]
    for item in summary:
        row = points[item["point_id"]]
        assert row["测点路径"] == f"{room} / {device} / {item['metric']}"
        assert row["测点原始值"] == "" and row["数据完整性"] == "缺少源数据"
        assert "采样时间" not in row and "测点修正值" not in row
    assert points[STATE]["测点值"] == "" and points[STATE]["状态判定"] == "待核对"
    assert points[FREQ]["测点值"] == "0"


def test_missing_sample_cannot_certify_an_all_invalid_dataset(scenario):
    doc, service, cloud, _ = scenario
    doc.update(zero_filled_point_ids=[FREQ, UNKNOWN], zero_filled_sample_point_ids=[STATE])
    doc["rows"][0][3] = "timed out"
    with pytest.raises(ValueError, match="缺少有效实测"):
        service.run(source_date=DATE, building="D楼")
    assert not cloud.events


def test_missing_ups_sample_does_not_transfer_the_next_reading_to_the_previous_ups(scenario):
    _, service, cloud, docs = scenario
    doc = docs["D楼"]["electrical_inspection_family"]
    room, first_ups, second_ups = "电气系统西区", "D-144-UPS-101", "D-144-UPS-201"
    doc.update(point_ids=IDS[:3], requested_point_ids=IDS[:3], zero_filled_sample_point_ids=[STATE], rows=[
        [room, first_ups, "L1负载率(%)", "3"],
        ["", second_ups, "L1负载率(%)", "0"],
        ["", "", "L2负载率(%)", "8"],
    ])
    result = service.run(source_date=DATE, building="D楼")
    assert {"point_id": STATE, "device": second_ups, "metric": "L1负载率(%)", "count": 1} in result["missing_sample_points"]
    rows = [r["fields"] for r in cloud.rows[upload.DEFAULT_CONFIG["tables"]["D楼"]]]
    ups = {r["设备编号"]: r for r in rows if r["记录类型"] == "设备巡检" and r["设备编号"] in {first_ups, second_ups}}
    assert ups[first_ups]["L1负载率(%)"] == 3 and "L2负载率(%)" not in ups[first_ups]
    assert ups[second_ups]["L2负载率(%)"] == 8 and "L1负载率(%)" not in ups[second_ups]


@pytest.mark.parametrize("invalid", [
    {"zero_filled_point_ids": IDS}, {"zero_filled_point_ids": [STATE, STATE]},
    {"zero_filled_point_ids": ["7_0_9"]}, {"zero_filled_point_ids": STATE},
    {"requested_point_ids": IDS + [REAL]}, {"requested_point_ids": IDS[:-1]},
    {"point_ids": [REAL, STATE, STATE, UNKNOWN]}, {"requested_point_ids": IDS + ["7_0_8"]},
    {"point_ids": IDS[:-1]}, {"value_time": "2026-10-27 00:00:00"},
    {"zero_filled_sample_point_ids": [STATE]}, {"zero_filled_sample_point_ids": [REAL, REAL]},
    {"zero_filled_sample_point_ids": ["7_0_9"]}, {"zero_filled_sample_point_ids": REAL},
])
def test_corrupt_transport_is_rejected_without_repair_or_writes(scenario, invalid):
    doc, service, cloud, _ = scenario
    doc.update(invalid)
    original = copy.deepcopy(doc)
    with pytest.raises(ValueError):
        service.run(source_date=DATE, building="D楼")
    assert doc == original and not cloud.events


def test_no_legacy_partial_no_overwrite_and_no_placeholder_only_sample(scenario):
    doc, service, cloud, _ = scenario
    doc["rows"][1][3] = "1"
    with pytest.raises(ValueError, match="补零"):
        service.run(source_date=DATE, building="D楼")
    doc["rows"][1][3], doc["rows"][0][3] = "0", "--"
    with pytest.raises(ValueError, match="补零"):
        service.run(source_date=DATE, building="D楼")
    del doc["zero_filled_point_ids"]
    doc["point_ids"].pop()
    doc["rows"].pop()
    with pytest.raises(ValueError, match="未返回全部请求测点"):
        service.run(source_date=DATE, building="D楼")
    assert not cloud.events


def test_latest_points_never_show_placeholder_as_sample(scenario):
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=SimpleNamespace(runtime_config={}))))
    payload = routes.latest_building_inspection_points("d", request)
    points = {row["id"]: row for row in payload["datasets"][HVAC_FAMILY]["points"]}
    assert points[REAL]["value"] == "0.00" and points[REAL]["raw_value"] == "0"
    for pid in IDS[1:]:
        assert points[pid]["value"] == points[pid]["raw_value"] == "" and points[pid]["corrected_value"] is None
    assert points[STATE]["status"] == "待核对" and points[STATE]["name"] == "已核验启停"
    assert points[UNKNOWN]["name"] == points[UNKNOWN]["path"] == ""


@pytest.mark.parametrize("rule", [None, {"type": "直接填充"}])
def test_discrete_missing_state_is_not_reported_as_zero_without_state_rule(scenario, rule):
    doc, _, _, _ = scenario
    discrete = "55_0_444_2_600_0"
    doc["point_ids"][1] = doc["requested_point_ids"][1] = doc["zero_filled_point_ids"][0] = discrete
    rows = upload.InspectionDataUploadService._point_detail_rows(
        doc, "暖通", {}, status_rules={discrete: rule} if rule else {}, for_upload=True,
    )
    row = next(row for row in rows if row["测点ID"] == discrete)
    assert (row["测点值"], row["测点原始值"], row["状态判定"]) == ("", "", "待核对")


def test_initial_baseline_needs_complete_sources(scenario, tmp_path):
    partial, _, _, docs = scenario
    all_docs = [d for groups in docs.values() for d in groups.values()]
    for doc in all_docs:
        if doc is not partial:
            doc.update(point_ids=[REAL], requested_point_ids=[REAL])
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    with pytest.raises(ValueError, match="基线包含缺失"):
        point_settings.initialize_point_baseline(config, all_docs)
    assert point_settings.load_point_settings(config) is None
