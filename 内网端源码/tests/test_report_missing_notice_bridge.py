import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config.inspection_data_config import FAMILY_HVAC_INSPECTION, INSPECTION_REPORT_TEMPLATES
from app.modules.internal_bridge_http.api.routes import router
from app.modules.shared_bridge.service import report_missing_notice as notices
from app.modules.shared_bridge.service.report_api_transform import ReportNoSamples
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_source_cache_service import SharedSourceCacheService
from handover_log_module.service import handover_download_service as download


FAMILY = "handover_log_family"
BUCKET = "2026-11-26 09"
START = f"{BUCKET}:00:00"
END = f"{BUCKET}:05:00"
CLAIM = "/api/internal-bridge/report-missing-notices/claim"
ACK = "/api/internal-bridge/report-missing-notices/ack"


@pytest.fixture
def bridge(tmp_path):
    root = tmp_path / "shared"
    config = {
        "deployment": {"role_mode": "internal"},
        "paths": {"runtime_state_root": str(tmp_path / "local")},
        "shared_bridge": {"enabled": True, "root_dir": str(root)},
        "internal_bridge_http": {"enabled": True, "allowed_client_ips": ["testclient"]},
        "inspection_data": {"enabled": True},
        "internal_source_sites": [{"building": "D楼", "enabled": True}, {"building": "E楼", "enabled": True}],
    }
    store = SharedBridgeStore(root)
    store.ensure_ready()
    cache = SharedSourceCacheService(runtime_config=config, store=store)
    app = FastAPI()
    app.include_router(router)
    app.state.container = SimpleNamespace(runtime_config=config, config={})
    return cache, TestClient(app), notices.ReportMissingNoticeOutbox(config)


def publish(cache, tmp_path, *, family=FAMILY, building="D楼", bucket=BUCKET, status="ready", **metadata):
    document = {
        "schema_version": 1, "source_family": family, "building": building, "bucket_key": bucket,
        "query_start": START, "query_end": END, "business_date": bucket[:10],
        "sheets": [{"name": "test", "rows": [[], [START, END], [], [1, 0]]}],
        **metadata,
    }
    if family in INSPECTION_REPORT_TEMPLATES:
        document.update(report_name=INSPECTION_REPORT_TEMPLATES[family], rows=[{"point_id": "1_2"}])
    source = tmp_path / "report.json"
    source.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
    return cache._store_entry(
        source_family=family, building=building, bucket_kind="daily" if len(bucket) == 10 else "latest", bucket_key=bucket,
        duty_date=bucket[:10], duty_shift="", source_path=source, status=status,
    )


def enqueue_empty(cache, *, family=FAMILY, building="D楼", bucket=BUCKET):
    notices.enqueue_empty_report_notice(
        cache.runtime_config, family=family, report_label="交接班日志", building=building, window=bucket,
    )


def test_only_empty_reports_enqueue_and_http_ack_dedupes(bridge):
    cache, client, _ = bridge
    enqueue_empty(cache)
    notice = client.post(CLAIM).json()["notice"]
    assert "所有测点均无有效实测" in notice["message"]
    ack = {key: notice[key] for key in ("notice_id", "lease_token")}
    assert client.post(ACK, json={**ack, "lease_token": "0" * 32}).json() == {"ok": False}
    assert client.post(CLAIM).json() == {"notice": None}
    assert client.post(ACK, json=ack).json() == {"ok": True}
    assert client.post(ACK, json=ack).json() == {"ok": True}

    enqueue_empty(cache)
    assert client.post(CLAIM).json() == {"notice": None}
    enqueue_empty(cache, building="E楼")
    enqueue_empty(cache, bucket="2026-11-26 10")
    distinct = [client.post(CLAIM).json()["notice"]["notice_id"] for _ in range(2)]
    assert len({notice["notice_id"], *distinct}) == 3


def test_unconfirmed_empty_notice_is_retried(bridge, monkeypatch):
    cache, client, outbox = bridge
    monkeypatch.setattr(notices.time, "time", lambda: 1000)
    enqueue_empty(cache)
    with ThreadPoolExecutor(2) as pool:
        claims = list(pool.map(lambda _: outbox.claim(), range(2)))
    assert sum(claim is not None for claim in claims) == 1
    first = next(claim for claim in claims if claim)
    assert "所有测点均无有效实测" in first["message"]
    monkeypatch.setattr(notices.time, "time", lambda: 1301)
    retry = client.post(CLAIM).json()["notice"]
    assert retry["notice_id"] == first["notice_id"]
    assert retry["lease_token"] != first["lease_token"]
    assert not outbox.acknowledge(first["notice_id"], first["lease_token"])


def test_outbox_failure_does_not_replace_original_no_data_error(bridge, monkeypatch, caplog):
    cache, _, _ = bridge
    monkeypatch.setattr(notices.ReportMissingNoticeOutbox, "enqueue_many", Mock(side_effect=OSError("secret-test")))
    enqueue_empty(cache)
    assert "整表无数据提醒入队失败" in caplog.text
    assert "secret-test" not in caplog.text


def test_inspection_notifies_only_when_the_selected_report_is_empty(bridge, monkeypatch):
    from app.config.inspection_data_config import inspection_hour_bucket

    cache, client, _ = bridge
    bucket = inspection_hour_bucket()
    run = Mock(side_effect=[TimeoutError("offline"), ReportNoSamples("巡检接口未返回有效测点"),
                            ReportNoSamples("巡检接口未返回有效测点")])
    monkeypatch.setattr(download.HandoverDownloadService, "run_inspection_data_only", run)
    fill = lambda: cache.fill_hvac_inspection_latest(building="D楼", bucket_key=bucket, emit_log=lambda _: None,
                                                     force_refresh=True)
    with pytest.raises(TimeoutError):
        fill()
    assert client.post(CLAIM).json()["notice"] is None
    for _ in range(2):
        with pytest.raises(ReportNoSamples):
            fill()
    notice = client.post(CLAIM).json()["notice"]
    assert "暖通巡检表" in notice["message"] and "所有测点均无有效实测" in notice["message"]
    assert client.post(ACK, json=notice).json() == {"ok": True}
    assert client.post(CLAIM).json()["notice"] is None


def test_expired_notices_are_not_replayed_after_outage(bridge, monkeypatch):
    _, client, outbox = bridge
    expired_id, fresh_id = "1" * 64, "2" * 64
    monkeypatch.setattr(notices.time, "time", lambda: 1000)
    outbox.enqueue(expired_id, "expired")
    monkeypatch.setattr(notices.time, "time", lambda: 1001 + notices._PENDING_TTL)
    outbox.enqueue(fresh_id, "fresh")
    assert client.post(CLAIM).json()["notice"]["notice_id"] == fresh_id
    outbox.enqueue(expired_id, "same gap again")
    assert client.post(CLAIM).json() == {"notice": None}


def test_partial_reports_keep_full_json_without_any_notice(tmp_path, bridge):
    cache, client, _ = bridge
    gaps = [{"point_id": f"40_0_109_1_{i:04d}_0", "device": "E-402-C列A路-DC015",
             "metric": "27_支路功率_KW", "count": 1} for i in range(5088)]
    options = {"family": "branch_power_family", "building": "E楼", "bucket": "2026-11-26",
               "report_name": "列头柜支路电流"}
    entry = publish(cache, tmp_path, **options, missing_sample_points=gaps)
    assert len(json.loads(Path(entry["file_path"]).read_text(encoding="utf-8"))["missing_sample_points"]) == 5088
    publish(cache, tmp_path, family=FAMILY_HVAC_INSPECTION, zero_filled_sample_point_ids=["4_5"])
    assert client.post(CLAIM).json() == {"notice": None}


def test_old_partial_backlog_is_not_delivered_after_update(bridge):
    cache, client, outbox = bridge
    head = "【报表缺测提醒】共享源文件已就绪\n报表：支路功率源文件\n"
    outbox.enqueue_many([
        ("1" * 64, head + "测点清单 1/318\n缺读数：1_2"),
        ("2" * 64, head + "测点清单 132/318\n缺读数：2_3"),
        ("3" * 64, head + "测点清单 1/1\n缺读数：3_4"),
        ("4" * 64, head + "缺读数：4_5"),
    ])
    enqueue_empty(cache)
    notice = client.post(CLAIM).json()["notice"]
    assert "所有测点均无有效实测" in notice["message"]
    assert client.post(ACK, json=notice).json() == {"ok": True}
    assert client.post(CLAIM).json() == {"notice": None}


def test_branch_retries_before_sending_empty_notice_and_ignores_timeouts(tmp_path, bridge, monkeypatch):
    cache, client, _ = bridge
    svc = download.HandoverDownloadService({
        "_global_paths": cache.runtime_config["paths"], "sites": [{"building": "E楼"}],
        "branch_power": {"download": {"max_retries": 2, "retry_wait_sec": 0}},
    }, business_root_override=tmp_path)
    query = Mock(side_effect=TimeoutError("offline"))
    monkeypatch.setattr(download, "query_report_json", query)
    def collect():
        return svc.run_branch_power_only(
            buildings=["E楼"], start_time="2026-11-25 23:50:00", end_time="2026-11-27 02:20:00",
            business_date="2026-11-26", reuse_cached=False, emit_log=lambda _: None,
        )
    assert collect()["failed"]
    assert client.post(CLAIM).json() == {"notice": None}
    query.reset_mock()
    query.side_effect = ReportNoSamples("报表没有可用的实际采样")
    assert collect()["failed"] and query.call_count == 2
    notice = client.post(CLAIM).json()["notice"]
    assert "支路功率" in notice["message"] and "窗口：2026-11-26" in notice["message"]
    assert client.post(ACK, json=notice).json() == {"ok": True}
    assert collect()["failed"]
    assert client.post(CLAIM).json() == {"notice": None}


def test_notice_routes_reject_unlisted_peers(bridge):
    cache, client, _ = bridge
    cache.runtime_config["internal_bridge_http"]["allowed_client_ips"] = ["192.168.1.1"]
    client.app.state.container.runtime_config = cache.runtime_config
    assert client.post(CLAIM).status_code == 403
    assert client.post(ACK, json={}).status_code == 403
