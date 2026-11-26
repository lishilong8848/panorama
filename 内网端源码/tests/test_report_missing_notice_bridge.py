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
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_source_cache_service import SharedSourceCacheService


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
        "query_start": START, "query_end": END,
        "sheets": [{"name": "test", "rows": [[], [START, END], [], [1, 0]]}],
        **metadata,
    }
    if family in INSPECTION_REPORT_TEMPLATES:
        document.update(report_name=INSPECTION_REPORT_TEMPLATES[family], rows=[{"point_id": "1_2"}])
    source = tmp_path / "report.json"
    source.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
    return cache._store_entry(
        source_family=family, building=building, bucket_kind="latest", bucket_key=bucket,
        duty_date=bucket[:10], duty_shift="", source_path=source, status=status,
    )


def test_only_ready_publications_enqueue_and_http_ack_dedupes(tmp_path, bridge, monkeypatch):
    cache, client, _ = bridge
    missing = {
        "zero_filled_point_ids": [f"1_{i}" for i in range(5)],
        "missing_sample_points": [{"point_id": "", "device": "机柜A", "metric": "功率", "count": 3}],
    }
    publish(cache, tmp_path)
    publish(cache, tmp_path, status="refreshing", **missing)
    with monkeypatch.context() as patch:
        patch.setattr(cache, "_cache_file_compat", Mock(side_effect=OSError("not published")))
        with pytest.raises(OSError):
            publish(cache, tmp_path, **missing)
    assert client.post(CLAIM).json() == {"notice": None}

    ready = publish(cache, tmp_path, **missing)
    assert Path(ready["file_path"]).is_file()
    notice = client.post(CLAIM).json()["notice"]
    assert "未返回测点：5 个" in notice["message"]
    assert "未返回：1_4" in notice["message"]
    assert "机柜A / 功率 (3条)" in notice["message"]
    assert len(notice["message"]) < 1400
    ack = {key: notice[key] for key in ("notice_id", "lease_token")}
    assert client.post(ACK, json={**ack, "lease_token": "0" * 32}).json() == {"ok": False}
    assert client.post(CLAIM).json() == {"notice": None}
    assert client.post(ACK, json=ack).json() == {"ok": True}
    assert client.post(ACK, json=ack).json() == {"ok": True}

    reordered = {
        "zero_filled_point_ids": list(reversed(missing["zero_filled_point_ids"])),
        "missing_sample_points": [
            {**missing["missing_sample_points"][0], "row_number": 88, "first_sample_missing": True, "zero_filled": True},
        ],
    }
    publish(cache, tmp_path, **reordered)
    assert client.post(CLAIM).json() == {"notice": None}
    publish(cache, tmp_path, zero_filled_point_ids=["9_9"])
    changed_id = client.post(CLAIM).json()["notice"]["notice_id"]
    reordered["missing_sample_points"][0]["count"] = 4
    publish(cache, tmp_path, **reordered)
    changed_count = client.post(CLAIM).json()["notice"]["notice_id"]
    publish(cache, tmp_path, building="E楼", **missing)
    publish(cache, tmp_path, bucket="2026-11-26 10", **missing)
    distinct = [client.post(CLAIM).json()["notice"]["notice_id"] for _ in range(2)]
    assert len({notice["notice_id"], changed_id, changed_count, *distinct}) == 5


def test_inspection_uses_same_queue_and_unconfirmed_claim_is_retried(tmp_path, bridge, monkeypatch):
    cache, client, outbox = bridge
    monkeypatch.setattr(notices.time, "time", lambda: 1000)
    publish(cache, tmp_path, family=FAMILY_HVAC_INSPECTION, zero_filled_sample_point_ids=["4_5"])
    with ThreadPoolExecutor(2) as pool:
        claims = list(pool.map(lambda _: outbox.claim(), range(2)))
    assert sum(claim is not None for claim in claims) == 1
    first = next(claim for claim in claims if claim)
    assert "暖通巡检表" in first["message"]
    assert "缺读数测点：1 个" in first["message"]
    assert "缺读数：4_5" in first["message"]
    monkeypatch.setattr(notices.time, "time", lambda: 1301)
    retry = client.post(CLAIM).json()["notice"]
    assert retry["notice_id"] == first["notice_id"]
    assert retry["lease_token"] != first["lease_token"]
    assert not outbox.acknowledge(first["notice_id"], first["lease_token"])


def test_outbox_failure_never_invalidates_published_file(tmp_path, bridge, monkeypatch, caplog):
    cache, _, _ = bridge
    monkeypatch.setattr(notices.ReportMissingNoticeOutbox, "enqueue_many", Mock(side_effect=OSError("secret-test")))
    entry = publish(cache, tmp_path, zero_filled_point_ids=["1_2"])
    assert Path(entry["file_path"]).is_file()
    assert "报表缺测提醒入队失败" in caplog.text
    assert "secret-test" not in caplog.text


def test_capacity_notice_uses_only_exact_known_c_building_point_labels(tmp_path, bridge):
    cache, client, _ = bridge
    publish(
        cache, tmp_path, family="handover_capacity_report_family", building="C楼",
        zero_filled_point_ids=["25_0_220_1_601_0", "25_0_221_1_601_0"],
    )
    message = client.post(CLAIM).json()["notice"]["message"]
    assert "25_0_220_1_601_0 / 电池_正极电压_V" in message
    assert "25_0_221_1_601_0 /" not in message


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


def test_long_missing_list_is_split_without_omissions_or_replay(tmp_path, bridge):
    cache, client, _ = bridge
    ids = [f"25_0_220_1_{index:04d}_0" for index in range(160)]
    publish(cache, tmp_path, zero_filled_point_ids=ids)
    messages = []
    while notice := client.post(CLAIM).json()["notice"]:
        messages.append(notice["message"])
        assert len(notice["message"]) < 1400
        assert client.post(ACK, json=notice).json() == {"ok": True}
    assert len(messages) > 1
    assert set(ids) == {line.removeprefix("未返回：") for message in messages for line in message.splitlines()
                        if line.startswith("未返回：")}
    publish(cache, tmp_path, zero_filled_point_ids=list(reversed(ids)))
    assert client.post(CLAIM).json() == {"notice": None}


def test_notice_routes_reject_unlisted_peers(bridge):
    cache, client, _ = bridge
    cache.runtime_config["internal_bridge_http"]["allowed_client_ips"] = ["192.168.1.1"]
    client.app.state.container.runtime_config = cache.runtime_config
    assert client.post(CLAIM).status_code == 403
    assert client.post(ACK, json={}).status_code == 403
