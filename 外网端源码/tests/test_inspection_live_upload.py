from __future__ import annotations

import copy
import json
import threading
import time
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.modules.inspection_data.service import live_requests as live
from app.modules.scheduler.api.inspection_live_routes import router
from app.modules.inspection_data.service import inspection_data_upload_service as upload


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    import urllib.request
    import requests
    monkeypatch.setattr(urllib.request, "urlopen", Mock(side_effect=AssertionError("real network forbidden")))
    monkeypatch.setattr(requests.Session, "request", Mock(side_effect=AssertionError("real network forbidden")))
    monkeypatch.setattr(upload, "FeishuBitableClient", Mock(side_effect=AssertionError("real Feishu forbidden")))


@pytest.fixture
def config(tmp_path):
    return {"paths": {"runtime_state_root": str(tmp_path)}, "inspection_data_upload": {
        "enabled": True, "lighthouse_bridge": {"enabled": True}}}


def submit(store, building="D楼", floor=None):
    floor = int(time.time()*1000)-1000 if floor is None else floor
    rid = uuid.uuid4().hex
    store.submit(rid, building, floor)
    return rid, floor


def proof(batch, floor, building="D楼"):
    return {"status": "success", "live_batch_id": batch, "buildings": [building],
            "source_time": floor, "uploaded_at": int(time.time()*1000), "results": {building: {
                "uploaded_count": 3, "upload_time": 100, "batch_completion": {
                    "status": "completed", "building": building, "upload_time": 100,
                    "device_count": 1, "point_count": 2}}}}


@pytest.fixture
def api(config):
    app = FastAPI()
    container = SimpleNamespace(runtime_config=config, dispatch_inspection_live_requests=Mock(),
                                _configured_deployment_snapshot=lambda: {"role_mode": "external"})
    app.state.container = container
    app.include_router(router)
    with TestClient(app, base_url="http://192.168.1.5", client=("192.168.1.6", 5000)) as client:
        yield client, container


def body():
    return {"request_id": uuid.uuid4().hex, "building": "D楼", "not_before": int(time.time()*1000)-1000}


BASE = "/api/scheduler/inspection-data-upload/live"


@pytest.mark.parametrize("enabled,host,peer,headers", [
    (False, "192.168.1.5", "192.168.1.6", {}),
    (True, "192.168.1.5", "8.8.8.8", {}),
    (True, "public.example", "127.0.0.1", {}),
    (True, "192.168.1.5", "127.0.0.1", {"X-Forwarded-For": "8.8.8.8"}),
    (True, "192.168.1.5", "192.168.1.6", {"Origin": "http://unexpected.example"}),
])
def test_disabled_public_proxy_and_browser_requests_cannot_collect(api, config, enabled, host, peer, headers):
    client, container = api
    config["inspection_data_upload"]["lighthouse_bridge"] = {"enabled": enabled}
    request = body()
    with TestClient(client.app, base_url=f"http://{host}", client=(peer, 5000)) as caller:
        for response in (caller.post(BASE, json=request, headers=headers),
                         caller.get(BASE + "/" + request["request_id"], headers=headers)):
            assert response.status_code == 403
            assert response.json()["source_time"] is None
    container.dispatch_inspection_live_requests.assert_not_called()


def test_api_idempotency_conflict_persistence_and_safe_unavailable(api, config):
    client, container = api
    request = body()
    first = client.post(BASE, json=request)
    second = client.post(BASE, json=request)
    assert first.status_code == 202 and second.status_code == 200
    assert first.json() == second.json()
    assert set(first.json()) == {"ok", "request_id", "building", "state", "source_time", "uploaded_at", "job_id", "error"}
    assert client.get(BASE+"/"+request["request_id"]).json() == first.json()
    assert live.LiveRequestStore(config).get(request["request_id"]) == first.json()
    assert client.post(BASE, json={**request, "building": "E楼"}).status_code == 409
    container.dispatch_inspection_live_requests.side_effect = RuntimeError("secret URL")
    assert client.post(BASE, json=body()).json()["state"] == "pending"


@pytest.mark.parametrize("patch", [{"not_before": True}, {"not_before": 1}, {"not_before": 99999999999999},
    {"building": "A楼"}, {"request_id": "bad"}, {"extra": 1}])
def test_invalid_requests_rejected_without_queue(api, patch):
    client, container = api
    response = client.post(BASE, json={**body(), **patch})
    assert response.status_code == 400
    container.dispatch_inspection_live_requests.assert_not_called()


def test_time_boundary_replay_and_failed_same_id_recovery(config):
    store = live.LiveRequestStore(config)
    rid = uuid.uuid4().hex
    floor = 1000000
    store.submit(rid, "D楼", floor, now_ms=floor+live.MAX_REQUEST_AGE_MS)
    with pytest.raises(ValueError):
        store.submit(uuid.uuid4().hex, "D楼", floor, now_ms=floor+live.MAX_REQUEST_AGE_MS+1)
    batch = store.next_batch("D楼")
    for _ in range(3):
        store.begin(batch["batch_id"])
        store.fail(batch["batch_id"])
    assert store.get(rid)["state"] == "failed"
    state, created = store.submit(rid, "D楼", floor, now_ms=floor+9999999)
    assert not created and state["state"] == "pending"
    assert store.next_batch("D楼")["batch_id"] == batch["batch_id"]
    assert store.next_batch("D楼")["not_before"] == floor
    with pytest.raises(live.LiveRequestConflict):
        store.submit(rid, "D楼", floor+1, now_ms=floor+9999999)


def test_pending_coalesces_maximum_floor_running_gets_followup_and_other_building(config):
    store = live.LiveRequestStore(config)
    old, floor = submit(store)
    batch = store.next_batch("D楼")
    newer, newer_floor = submit(store, floor=floor+1)
    assert store.next_batch("D楼")["not_before"] == newer_floor
    store.begin(batch["batch_id"])
    late, late_floor = submit(store, floor=floor+2)
    other, _ = submit(store, building="E楼")
    assert store.next_batch("E楼")["building"] == "E楼"
    store.complete(batch["batch_id"], proof(batch["batch_id"], newer_floor))
    assert all(store.get(rid)["state"] == "uploaded" for rid in (old, newer))
    assert store.get(late)["state"] == store.get(other)["state"] == "pending"
    following = store.next_batch("D楼")
    assert following["not_before"] == late_floor and following["batch_id"] != batch["batch_id"]


@pytest.mark.parametrize("mutation", ["old_time", "wrong_batch", "partial_count", "wrong_building", "no_marker", "future_source"])
def test_only_complete_own_batch_with_actual_fresh_sample_can_ack(config, mutation):
    store = live.LiveRequestStore(config)
    rid, floor = submit(store)
    batch = store.next_batch("D楼")
    result = proof(batch["batch_id"], floor)
    if mutation == "old_time": result["source_time"] = floor-1
    if mutation == "wrong_batch": result["live_batch_id"] = "old-running-job"
    if mutation == "partial_count": result["results"]["D楼"]["uploaded_count"] = 2
    if mutation == "wrong_building": result["buildings"] = ["E楼"]
    if mutation == "no_marker": result["results"]["D楼"]["batch_completion"] = {}
    if mutation == "future_source": result["source_time"] = result["uploaded_at"]+1
    with pytest.raises(ValueError): store.complete(batch["batch_id"], result)
    assert store.get(rid)["state"] != "uploaded"


class Queue:
    def __init__(self): self.jobs = []
    def find_active_job_by_dedupe_key(self, key):
        return next((j for j in self.jobs if j["dedupe_key"] == key and j["status"] in live.ACTIVE_JOB_STATES), None)
    def start_worker_job(self, **kwargs):
        job = {**kwargs, "job_id": uuid.uuid4().hex, "status": "queued", "result": None}
        self.jobs.append(job)
        return SimpleNamespace(job_id=job["job_id"])
    def get_job(self, job_id): return next(j for j in self.jobs if j["job_id"] == job_id)


def test_queue_restart_failure_recovery_and_legacy_upload_is_not_adopted(config):
    store = live.LiveRequestStore(config)
    rid, floor = submit(store)
    queue = Queue()
    queue.jobs.append({"job_id": "legacy", "dedupe_key": "inspection_data_upload", "status": "running"})
    container = SimpleNamespace(runtime_config=config, job_service=queue,
                                _configured_deployment_snapshot=lambda: {"role_mode": "external"})
    live.dispatch_live_requests(container)
    job = queue.jobs[1]
    assert job["resource_keys"] == ["inspection_data_upload:D"]
    assert store.get(rid)["job_id"] == job["job_id"] != "legacy"
    batch_id = job["worker_payload"]["live_batch_id"]
    store.begin(batch_id)
    job["status"] = "interrupted"
    live.dispatch_live_requests(container)
    assert store.get(rid)["state"] == "pending"
    with store.connect() as db: db.execute("UPDATE batches SET retry_at=0")
    live.dispatch_live_requests(container)
    assert len(queue.jobs) == 3
    latest = queue.jobs[-1]
    latest.update(status="success", result=proof(batch_id, floor))
    live.dispatch_live_requests(container)
    assert live.LiveRequestStore(config).get(rid)["state"] == "uploaded"


def test_worker_failure_safe_and_persistent_then_success(config, monkeypatch):
    store = live.LiveRequestStore(config)
    rid, floor = submit(store)
    batch = store.next_batch("D楼")
    fake = Mock()
    monkeypatch.setattr(upload, "InspectionDataUploadService", lambda config: fake)
    fake.run.side_effect = RuntimeError("https://internal secret test-isolated-secret")
    with pytest.raises(RuntimeError) as error:
        live.run_live_upload(config, {"live_batch_id": batch["batch_id"]}, lambda _: None)
    assert "secret" not in str(error.value)
    assert store.get(rid)["state"] == "pending"
    fake.run.side_effect = None
    fake.run.return_value = proof(batch["batch_id"], floor)
    live.run_live_upload(config, {"live_batch_id": batch["batch_id"]}, lambda _: None)
    assert store.get(rid)["state"] == "uploaded"
    assert fake.run.call_args.kwargs["force_refresh"] is True
    assert fake.run.call_args.kwargs["not_before"] == floor


def test_oldest_real_source_time_excludes_all_auxiliary_rows():
    stamp = int((datetime.now(upload.LOCAL_TZ)-timedelta(minutes=2)).timestamp()*1000)
    text = datetime.fromtimestamp(stamp/1000, upload.LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    base = int(datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=upload.LOCAL_TZ).timestamp()*1000)
    sources = {"D楼": {"hvac": {"value_time": text}}}
    rows = [{"记录类型": "测点明细", "测点来源": "collected", "源文件时间": base-1000,
             "采样时间": base, "上传日期": base+99999}]
    for source in {"manual_status", "fixed_value", "fixed_status", "inspection_operating",
                   "inspection_handover_mode", "inspection_handover_pressure",
                   "derived_operating", "HANDOVER_MODE", "HANDOVER_PRESSURE"}:
        rows.append({"记录类型": "测点明细", "测点来源": source,
                     "源文件时间": base-999999, "采样时间": base-999999})
    assert upload.InspectionDataUploadService._minimum_live_source_time(sources, {"D楼": {"rows": rows}}, base-2000) == base-1000
    with pytest.raises(ValueError):
        upload.InspectionDataUploadService._minimum_live_source_time(sources, {"D楼": {"rows": rows}}, base)


def test_legacy_bridge_settings_keep_enabled_flag_without_key():
    from app.config.config_adapter import ensure_v3_config, adapt_runtime_config, sync_runtime_back_to_v3
    original = {"common": {}, "features": {"inspection_data_upload": {"lighthouse_bridge": {
        "enabled": True, "token": "test-isolated-secret"}}}}
    cfg = ensure_v3_config(original)
    runtime = adapt_runtime_config(cfg)
    assert runtime["inspection_data_upload"]["lighthouse_bridge"] == {"enabled": True}
    assert sync_runtime_back_to_v3(cfg, runtime)["features"]["inspection_data_upload"]["lighthouse_bridge"] == {"enabled": True}


def test_public_exposure_excludes_live_api_and_settings():
    from app.bootstrap.app_factory import _is_externally_allowed_path
    assert not _is_externally_allowed_path(BASE)
    assert not _is_externally_allowed_path(BASE + "/" + uuid.uuid4().hex)
    assert not _is_externally_allowed_path("/api/config")
    assert not _is_externally_allowed_path(BASE + "-settings")


def test_lan_console_saves_enable_flag_without_key(api):
    client, container = api
    endpoint = BASE + "-settings"
    headers = {"X-Inspection-Console": "1"}
    update = Mock(return_value={"features": {"inspection_data_upload": {
        "lighthouse_bridge": {"enabled": True}}}})
    container.ensure_config_runtime_service = lambda: SimpleNamespace(update_config_patch=update)

    def apply(saved, **_):
        container.runtime_config["inspection_data_upload"] = saved["features"]["inspection_data_upload"]
    container.apply_config_snapshot = apply
    assert client.get(endpoint).status_code == 403
    assert client.get(endpoint + "?reveal=true", headers=headers).json() == {"enabled": True}
    assert client.put(endpoint, json={"enabled": True, "token": "legacy"}, headers=headers).status_code == 400
    response = client.put(endpoint, json={"enabled": True}, headers=headers)
    assert response.json() == {"enabled": True}
    assert "no-store" in response.headers["cache-control"]
    assert update.call_args.args[0]["features"]["inspection_data_upload"]["lighthouse_bridge"] == {"enabled": True}


def test_valid_failed_query_ok_true_and_same_post_restores_original_batch_after_age_limit(api, config, monkeypatch):
    client, container = api
    request = body()
    assert client.post(BASE, json=request).status_code == 202
    store = live.LiveRequestStore(config)
    batch = store.next_batch("D楼")
    for _ in range(3):
        store.begin(batch["batch_id"])
        store.fail(batch["batch_id"])
    failed = client.get(BASE+"/"+request["request_id"]).json()
    assert failed["ok"] is True and failed["state"] == "failed"
    assert "原 request_id" in failed["error"] and "新的请求" not in failed["error"]
    container.dispatch_inspection_live_requests.reset_mock()
    monkeypatch.setattr(live.time, "time", lambda: (request["not_before"]+live.MAX_REQUEST_AGE_MS+99999)/1000)
    restored = client.post(BASE, json=request)
    assert restored.status_code == 200 and restored.json()["ok"] is True
    assert restored.json()["state"] == "pending"
    original = store.next_batch("D楼")
    assert original["batch_id"] == batch["batch_id"] and original["not_before"] == request["not_before"]
    assert client.get(BASE+"/"+request["request_id"]).status_code == 200
    with store.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 1


def test_uploaded_post_replays_original_receipt_without_new_batch_or_upload(config):
    store = live.LiveRequestStore(config)
    rid, floor = submit(store)
    batch = store.next_batch("D楼")
    store.complete(batch["batch_id"], proof(batch["batch_id"], floor))
    receipt = store.get(rid)
    queue = Queue()
    container = SimpleNamespace(runtime_config=config, job_service=queue,
                                _configured_deployment_snapshot=lambda: {"role_mode": "external"})
    replay, created = store.submit(rid, "D楼", floor, now_ms=floor+99999999)
    live.dispatch_live_requests(container)
    assert not created and replay == receipt and replay["ok"] is True
    assert queue.jobs == [] and store.next_batch("D楼") is None
    assert store.get(rid) == receipt


def test_transient_get_error_keeps_persisted_request_and_original_threshold(api, config, monkeypatch):
    client, _ = api
    request = body()
    client.post(BASE, json=request)
    store = live.LiveRequestStore(config)
    before = store.get(request["request_id"])
    with monkeypatch.context() as temporary:
        temporary.setattr(live.LiveRequestStore, "get", Mock(side_effect=OSError("do not expose internal path")))
        response = client.get(BASE+"/"+request["request_id"])
        assert response.status_code == 503 and response.json()["ok"] is False
        assert "internal path" not in response.text
    assert client.get(BASE+"/"+request["request_id"]).json() == before
    with store.connect() as db:
        assert db.execute("SELECT not_before FROM requests WHERE request_id=?", (request["request_id"],)).fetchone()[0] == request["not_before"]


def test_simultaneous_same_id_is_one_durable_request(config):
    from concurrent.futures import ThreadPoolExecutor
    store = live.LiveRequestStore(config)
    rid, floor = uuid.uuid4().hex, int(time.time()*1000)-1000
    with ThreadPoolExecutor(max_workers=4) as workers:
        results = list(workers.map(lambda _: live.LiveRequestStore(config).submit(rid, "D楼", floor), range(8)))
    assert sum(created for _, created in results) == 1
    with store.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 1
