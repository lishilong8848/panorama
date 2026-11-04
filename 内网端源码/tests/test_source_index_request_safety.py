import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.modules.internal_bridge_http.api import routes
from app.modules.internal_bridge_http.service.internal_bridge_http_runner import InternalBridgeHttpTaskRunner


def test_cancelled_http_request_holds_slot_until_worker_finishes(monkeypatch):
    executor = ThreadPoolExecutor(max_workers=1)
    monkeypatch.setattr(routes, "_SOURCE_INDEX_EXECUTOR", executor)
    semaphore = threading.BoundedSemaphore(1)
    monkeypatch.setattr(routes, "_SOURCE_INDEX_REQUEST_SEMAPHORE", semaphore)
    entered, release = threading.Event(), threading.Event()
    def slow():
        entered.set()
        release.wait(3)
        return {"ok": True}
    async def run():
        first = asyncio.create_task(routes._run_source_index_query("test", slow))
        while not entered.is_set():
            await asyncio.sleep(0.01)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = await routes._run_source_index_query("test", lambda: pytest.fail("over capacity"))
        assert second["status"] == "busy"
        release.set()
    try:
        asyncio.run(run())
    finally:
        release.set()
        executor.shutdown(wait=True)
    assert semaphore.acquire(blocking=False)
    semaphore.release()


def test_missing_index_write_is_deferred_and_restored_file_not_invalidated(monkeypatch, tmp_path):
    store = SimpleNamespace(update_source_cache_entry_status=Mock())
    runner = InternalBridgeHttpTaskRunner(runtime_service=SimpleNamespace(shared_bridge_root=str(tmp_path)))
    runner._store = store
    missing = tmp_path / "file.xlsx"
    entry = {"entry_id": "entry1", "file_path": str(missing), "status": "ready", "updated_at": "2026-09-23 00:00:00"}
    monkeypatch.setattr(runner, "_get_store", lambda: store)
    monkeypatch.setattr(runner, "_list_source_cache_entries_fast", lambda *a, **kw: [entry])
    monkeypatch.setattr(runner, "_merge_main_source_cache_entries", lambda entries, **kw: entries)
    monkeypatch.setattr(runner, "_start_source_index_recovery_if_needed", lambda *a, **kw: None)
    assert runner.list_source_index() == []
    store.update_source_cache_entry_status.assert_not_called()
    runner._flush_missing_source_entries()
    assert store.update_source_cache_entry_status.call_args.kwargs["expected_updated_at"] == entry["updated_at"]
    runner._mark_source_index_entry_missing(entry, reason="missing", detail="test")
    missing.touch()
    runner._flush_missing_source_entries()
    assert store.update_source_cache_entry_status.call_count == 1


def test_deferred_missing_update_keeps_new_file_registered_in_same_second(monkeypatch, tmp_path):
    from app.modules.shared_bridge.service import shared_bridge_store
    monkeypatch.setattr(shared_bridge_store, "_now_text", lambda: "2026-09-23 00:00:00")
    store = shared_bridge_store.SharedBridgeStore(tmp_path / "index")
    store.ensure_ready()
    fields = dict(source_family="test", building="A楼", bucket_kind="day",
                  bucket_key="2026-09-22", status="ready")
    store.upsert_source_cache_entry(**fields, relative_path="old.xlsx", file_hash="old")
    entry = store.list_source_cache_entries()[0]
    entry["file_path"] = str(tmp_path / "old.xlsx")
    runner = InternalBridgeHttpTaskRunner(runtime_service=SimpleNamespace(shared_bridge_root=str(tmp_path)))
    runner._store = store
    runner._mark_source_index_entry_missing(entry, reason="missing", detail="test")
    (tmp_path / "new.xlsx").touch()
    store.upsert_source_cache_entry(**fields, relative_path="new.xlsx", file_hash="new")
    runner._flush_missing_source_entries()
    latest = store.list_source_cache_entries()[0]
    assert latest["relative_path"] == "new.xlsx"
    assert latest["status"] == "ready"


@pytest.mark.parametrize(("family", "suffix", "accepted"), [
    ("monthly_report_family", ".xlsx", False),
    ("monthly_report_family", ".json", True),
    ("handover_log_family", ".xlsx", False),
    ("top5_monthly_report_family", ".xlsx", True),
    ("pue_confirmation_family", ".xlsx", True),
    ("utility_feeder_deviation_family", ".xlsx", True),
])
def test_index_returns_only_current_source_format(monkeypatch, tmp_path, family, suffix, accepted):
    source = tmp_path / ("source" + suffix)
    source.write_bytes(b"source")
    entry = {"source_family": family, "file_path": str(source), "status": "ready"}
    runner = InternalBridgeHttpTaskRunner(runtime_service=SimpleNamespace(shared_bridge_root=str(tmp_path)))
    monkeypatch.setattr(runner, "_get_store", lambda: object())
    monkeypatch.setattr(runner, "_list_source_cache_entries_fast", lambda *a, **kw: [entry])
    monkeypatch.setattr(runner, "_merge_main_source_cache_entries", lambda entries, **kw: entries)
    recovery = Mock()
    monkeypatch.setattr(runner, "_start_source_index_recovery_if_needed", recovery)

    rows = runner.list_source_index(source_family=family, building="A楼")
    assert bool(rows) is accepted
    assert recovery.call_args.args[0] == rows
    assert source.read_bytes() == b"source"


@pytest.mark.parametrize("case", ["empty", "wrong_building", "wrong_date", "valid"])
def test_background_recovery_validates_json_before_ready(tmp_path, case):
    path = tmp_path / "source.json"
    payload = {"schema_version": 1, "source_family": "monthly_report_family", "building": "A楼",
               "business_date": "2026-10-29", "query_start": "2026-10-29 00:00:00",
               "query_end": "2026-10-30 01:00:00", "sheets": [
                   {"name": "sheet1", "rows": [[], [None, None, None, None, "2026-10-29 00:00:00"],
                                                 [], [None, "房间", "设备", "电度", 0]]}]}
    if case == "wrong_building":
        payload["building"] = "B楼"
    if case == "wrong_date":
        payload["business_date"] = "2026-10-28"
    path.write_text("" if case == "empty" else json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    index = SimpleNamespace(upsert_entry=Mock(side_effect=lambda entry: entry))
    cache = SimpleNamespace(
        _candidate_existing_latest_paths=lambda **kw: [{"relative_path": "source.json", "bucket_kind": "daily",
                                                        "bucket_key": "2026-10-29", "duty_date": "2026-10-29"}],
        _resolve_relative_path_under_shared_root=lambda _: path,
        store=SimpleNamespace(_source_cache_index_store=index),
    )
    runner = InternalBridgeHttpTaskRunner(runtime_service=SimpleNamespace())
    result = runner._recover_exact_existing_source_file_to_index(
        main_cache=cache, source_family="monthly_report_family", building="A楼", bucket_key="2026-10-29",
    )
    assert bool(result) is (case == "valid")
    assert index.upsert_entry.call_count == (1 if case == "valid" else 0)
