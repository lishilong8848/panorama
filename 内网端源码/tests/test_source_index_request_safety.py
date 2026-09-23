import asyncio
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
