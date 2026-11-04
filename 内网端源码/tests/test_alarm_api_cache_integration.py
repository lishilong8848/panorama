from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

from app.modules.shared_bridge.service import alarm_api_session_adapter as adapter
from app.modules.shared_bridge.service import api_request_capacity as capacity
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_source_cache_service import FAMILY_ALARM_EVENT, SharedSourceCacheService


SITE = {"building": "A楼", "enabled": True, "host": "https://fake.invalid", "username": "fake", "password": "fake"}
START, END = "2026-11-03 08:00:00", "2026-11-03 20:00:00"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    original = socket.socket.connect

    def connect(sock, address):
        if isinstance(address, tuple) and address[0] in {"127.0.0.1", "::1"}:
            return original(sock, address)
        raise AssertionError("external network forbidden")

    monkeypatch.setattr(socket.socket, "connect", connect)


@pytest.fixture
def cache(tmp_path):
    root = tmp_path / "shared"
    root.mkdir()
    store = SharedBridgeStore(root)
    store.ensure_ready()
    return SharedSourceCacheService(runtime_config={
        "deployment": {"role_mode": "internal"},
        "shared_bridge": {"enabled": True, "root_dir": str(root)},
        "internal_source_cache": {"enabled": True},
        "internal_source_sites": [SITE],
    }, store=store, emit_log=lambda _: None, download_browser_pool=None)


class Response:
    status_code = 200

    def __init__(self, data):
        self.data = data

    def json(self):
        return self.data


class FakeBms:
    instances = []

    def __init__(self, site):
        self.site = site
        self.base_url = "https://fake.invalid"
        self.logged_in = False
        self.closed = False
        self.calls = []
        self.instances.append(self)

    def login(self):
        self.logged_in = True

    def request(self, method, path, **kwargs):
        assert self.logged_in and not self.closed
        self.calls.append((method, path, kwargs))
        if path.endswith("/count"):
            return Response({"error_code": "00", "data": [{"event_level": 1, "count": 1}]})
        return Response({"error_code": "00", "data": {"page": {}, "event_list": [
            {"event_level": 1, "content": "测试告警", "event_source": "机房/设备"}
        ]}})

    def close(self):
        self.closed = True


@pytest.mark.parametrize("mode", ["latest", "manual", "handover_window"])
def test_real_adapter_paths_store_ready_without_browser(cache, monkeypatch, mode):
    FakeBms.instances = []
    monkeypatch.setattr(adapter, "BmsReportApiSession", FakeBms)
    calls = []
    original = capacity.api_request_slot

    @contextmanager
    def slot(**kwargs):
        with original(**kwargs):
            calls.append("acquired")
            yield

    monkeypatch.setattr(capacity, "api_request_slot", slot)
    common = {"building": "A楼", "emit_log": lambda _: None}
    if mode == "handover_window":
        entry = cache.fill_alarm_event_handover_window(**common, duty_date="2026-11-03", duty_shift="DAY", query_start=START, query_end=END)
    else:
        entry = getattr(cache, "fill_alarm_event_" + mode)(**common, bucket_key="2026-11-04 08")
    document = json.loads(Path(entry["file_path"]).read_text(encoding="utf-8"))
    assert calls == ["acquired"]
    assert len(FakeBms.instances) == 1 and FakeBms.instances[0].site == SITE
    assert FakeBms.instances[0].closed and FakeBms.instances[0].logged_in
    assert [item[1] for item in FakeBms.instances[0].calls] == ["/api/v2/tsdb/status/event", "/api/v2/tsdb/status/event/count"]
    assert document["row_count"] == entry["metadata"]["row_count"] == 1
    assert document["bucket_kind"] == ("handover_window" if mode == "handover_window" else "latest")
    assert not list(cache._tmp_root.rglob("alarm-*"))
    if mode == "manual":
        assert entry["metadata"]["manual"] is True
    if mode == "handover_window":
        assert (document["query_start"], document["query_end"]) == (START, END)
        assert (entry["duty_date"], entry["duty_shift"]) == ("2026-11-03", "day")
        assert entry["metadata"]["purpose"] == "handover_alarm_window"
        assert (entry["metadata"]["query_start"], entry["metadata"]["query_end"]) == (START, END)
        filters = FakeBms.instances[0].calls[0][2]["json"]["where"]["and"][0]["and"]
        assert [item["value"] for item in filters[-2:]] == [int(time.mktime(time.strptime(value, "%Y-%m-%d %H:%M:%S"))) for value in (START, END)]


@pytest.mark.parametrize("failure", ["no_file", "partial", "mismatch", "window", "exception"])
def test_incomplete_or_failed_export_never_registered(cache, monkeypatch, failure):
    async def export(_site, **kwargs):
        path = kwargs["output_path"]
        payload = {"query_start": START, "query_end": END, "row_count": 0, "count_summary": {}}
        document = {**payload, "schema_version": 1, "source_family": FAMILY_ALARM_EVENT,
                    "building": "A楼", "bucket_kind": "handover_window", "bucket_key": "2026-11-03", "rows": []}
        if failure == "exception":
            path.write_text("partial", encoding="utf-8")
            raise RuntimeError("fake failure")
        if failure == "partial":
            path.write_text("{", encoding="utf-8")
        elif failure != "no_file":
            if failure == "mismatch":
                document["row_count"] = 1
            if failure == "window":
                payload["query_start"] = document["query_start"] = "2026-11-03 09:00:00"
            path.write_text(json.dumps(document), encoding="utf-8")
        return payload

    monkeypatch.setattr(adapter, "export_alarm_event_json", export)
    with pytest.raises((OSError, ValueError, RuntimeError)):
        cache.fill_alarm_event_handover_window(building="A楼", duty_date="2026-11-03", duty_shift="day", query_start=START, query_end=END, emit_log=lambda _: None)
    assert not cache.store.list_source_cache_entries(source_family=FAMILY_ALARM_EVENT, status="ready", limit=20)
    assert not list(cache._tmp_root.rglob("alarm-*"))


@pytest.mark.parametrize("phase", ["slot", "export", "timeout"])
def test_stop_or_timeout_cancels_without_ready_and_releases_slot(cache, monkeypatch, phase):
    slots = threading.BoundedSemaphore(1)
    monkeypatch.setattr(capacity, "_API_SLOTS", slots)
    entered, cleaned = threading.Event(), threading.Event()

    async def export(_site, **kwargs):
        kwargs["output_path"].write_text("partial", encoding="utf-8")
        entered.set()
        try:
            await asyncio.sleep(10)
        finally:
            cleaned.set()

    monkeypatch.setattr(adapter, "export_alarm_event_json", export)
    if phase == "slot":
        slots.acquire()
    if phase == "timeout":
        cache.history_fill_timeout_sec = 0.3
    errors = []

    def run():
        try:
            cache.fill_alarm_event_latest(building="A楼", bucket_key="2026-11-04 08", emit_log=lambda _: None)
        except Exception as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=run)
    thread.start()
    try:
        if phase == "slot":
            time.sleep(0.1)
        else:
            assert entered.wait(3)
        if phase != "timeout":
            cache._stop_event.set()
        thread.join(4)
        assert not thread.is_alive() and errors
        assert ("超时" if phase == "timeout" else "取消") in errors[0]
        assert phase == "slot" or cleaned.is_set()
        assert not cache.store.list_source_cache_entries(source_family=FAMILY_ALARM_EVENT, status="ready", limit=20)
        assert not list(cache._tmp_root.rglob("alarm-*"))
    finally:
        if phase == "slot":
            slots.release()
        thread.join(4)
    assert slots.acquire(timeout=1)
    slots.release()


def test_manual_orchestration_without_browser_pool(cache, monkeypatch):
    FakeBms.instances = []
    monkeypatch.setattr(adapter, "BmsReportApiSession", FakeBms)
    result = cache._run_manual_alarm_refresh_impl()
    assert result["completed_buildings"] == ["A楼"]
    assert result["failed_buildings"] == result["blocked_buildings"] == []
    assert result["total_row_count"] == 1
    assert FakeBms.instances[0].closed


def test_automatic_orchestration_without_browser_pool(cache, monkeypatch):
    FakeBms.instances = []
    monkeypatch.setattr(adapter, "BmsReportApiSession", FakeBms)
    result = cache._run_latest_source_steps_by_building(
        steps=[(FAMILY_ALARM_EVENT, "2026-11-04 08", cache.fill_alarm_event_latest)],
        force_retry_failed=True,
    )
    assert result["completed_units"] == [f"A楼/{FAMILY_ALARM_EVENT}"]
    assert result["failed_units"] == result["blocked_units"] == []
    assert FakeBms.instances[0].closed


def test_concurrent_exports_obey_shared_capacity_and_use_unique_files(cache, monkeypatch):
    monkeypatch.setattr(capacity, "_API_SLOTS", threading.BoundedSemaphore(1))
    paths, active = [], []
    guard = threading.Lock()
    first_entered, release = threading.Event(), threading.Event()

    async def export(_site, **kwargs):
        path = kwargs["output_path"]
        with guard:
            paths.append(path)
            active.append(len(paths))
            assert len(active) == 1
        first_entered.set()
        try:
            while not release.is_set():
                await asyncio.sleep(0.01)
            payload = {"query_start": START, "query_end": END, "row_count": 0, "count_summary": {}}
            document = {**payload, "schema_version": 1, "source_family": FAMILY_ALARM_EVENT,
                        "building": "A楼", "bucket_kind": "latest", "bucket_key": "2026-11-04 08", "rows": []}
            path.write_text(json.dumps(document), encoding="utf-8")
            return payload
        finally:
            with guard:
                active.pop()

    monkeypatch.setattr(adapter, "export_alarm_event_json", export)
    monkeypatch.setattr(cache, "_store_entry", lambda **kwargs: {"path": kwargs["source_path"]})
    errors = []

    def run():
        try:
            cache.fill_alarm_event_latest(building="A楼", bucket_key="2026-11-04 08", emit_log=lambda _: None)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(2)]
    for thread in threads:
        thread.start()
    try:
        assert first_entered.wait(3)
        time.sleep(0.1)
        assert len(paths) == 1
    finally:
        release.set()
        for thread in threads:
            thread.join(4)
    assert not errors and all(not thread.is_alive() for thread in threads)
    assert len(paths) == 2 and paths[0] != paths[1]
    assert all(not path.exists() for path in paths)
