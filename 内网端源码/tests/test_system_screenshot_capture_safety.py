import asyncio
import concurrent.futures
import os
import threading
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.modules.system_screenshot_capture.service import system_screenshot_capture_service as module


def config_for(tmp_path):
    return {"system_screenshot_capture": {"state_file": str(tmp_path / "capture.json"), "download_root": str(tmp_path)}}


def test_overlapping_force_request_joins_same_batch_and_cancel_is_scoped(tmp_path, monkeypatch):
    entered = threading.Event()
    completed = threading.Event()
    def capture(**kwargs):
        entered.set()
        assert kwargs["cancel_event"].wait(3)
        completed.set()
        raise asyncio.CancelledError()
    monkeypatch.setattr(module, "run_system_screenshot_capture", capture)
    container = SimpleNamespace(_system_screenshot_capture_run_lock=threading.Lock())
    first = module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), force=True)
    assert entered.wait(1)
    try:
        second = module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), force=True)
        assert second["status"] == "running"
        assert second["batch_id"] == first["batch_id"]
        assert second["started_at"] == first["started_at"]
        assert module.cancel_system_screenshot_capture(container, "other-batch")["status"] == "skipped"
    finally:
        module.cancel_system_screenshot_capture(container, first["batch_id"])
        assert completed.wait(1)
    assert container._system_screenshot_capture_run_lock.acquire(timeout=1)
    container._system_screenshot_capture_run_lock.release()


def test_retry_reuses_persisted_batch_identity(tmp_path, monkeypatch):
    captured_ids = []
    def capture(**kwargs):
        captured_ids.append(kwargs["batch_id"])
        return {"status": "success"}
    monkeypatch.setattr(module, "run_system_screenshot_capture", capture)
    container = SimpleNamespace(_system_screenshot_capture_run_lock=threading.Lock())
    first = module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), force=True, wait=True, request_id="request-1")
    container = SimpleNamespace(_system_screenshot_capture_run_lock=threading.Lock())
    second = module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), force=True, wait=True, request_id="request-1")
    assert first["batch_id"] == second["batch_id"]
    assert captured_ids[0] == captured_ids[1]
    third = module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), force=True, wait=True, request_id="request-2")
    assert third["batch_id"] != first["batch_id"]


def test_pool_submits_one_target_at_a_time(tmp_path, monkeypatch):
    targets = module._normalize_targets()[:3]
    lengths = []
    owners = []
    async def capture_targets(**kwargs):
        lengths.append(len(kwargs["building_targets"]))
        return [{"target_key": kwargs["building_targets"][0].key}]
    monkeypatch.setattr(module, "_capture_building_targets", capture_targets)
    async def run():
        tasks = []
        def submit(building, runner, *, owner):
            owners.append(owner)
            future = concurrent.futures.Future()
            async def complete():
                future.set_result(await runner(object()))
            tasks.append(asyncio.create_task(complete()))
            return future
        monkeypatch.setattr(module, "get_internal_download_browser_pool", lambda: SimpleNamespace(submit_building_job=submit, is_running=lambda: True))
        records = await module._capture_with_browser_pool(
            plan_by_building={"A楼": targets}, site_by_building={"A楼": object()},
            args=SimpleNamespace(cancel_event=threading.Event()),
            state_path=tmp_path / "state.json", download_root_path=tmp_path,
            output_dir=tmp_path, compact_date="20261006", hour_text="", emit_log=lambda _: None,
        )
        await asyncio.gather(*tasks)
        return records
    assert len(asyncio.run(run())) == 3
    assert lengths == [1, 1, 1]
    assert owners == ["system_screenshot_capture"] * 3


def test_failed_capture_releases_batch_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "run_system_screenshot_capture", Mock(side_effect=RuntimeError("capture failed")))
    container = SimpleNamespace(_system_screenshot_capture_run_lock=threading.Lock())
    with pytest.raises(RuntimeError, match="capture failed"):
        module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), wait=True)
    assert not container._system_screenshot_capture_run_lock.locked()
    assert module._load_state(tmp_path / "capture.json")["capture_batch"]["status"] == "failed"


def test_force_retry_only_captures_unfinished_targets(tmp_path, monkeypatch):
    targets = module._normalize_targets()[:2]
    path = tmp_path / "first.png"
    path.write_bytes(b"test-png")
    module._upsert_record(tmp_path / "capture.json", {
        "capture_date": "2026-10-06", "site_building": "A楼",
        "target_key": targets[0].key, "file_path": str(path),
        "status": "captured", "capture_batch_id": "batch-1",
    })
    monkeypatch.setattr(module, "_select_sites", lambda *_: [SimpleNamespace(building="A楼")])
    captured = []
    async def fill(**kwargs):
        for target in kwargs["plan_by_building"]["A楼"]:
            captured.append(target.key)
            module._upsert_record(tmp_path / "capture.json", {
                "capture_date": "2026-10-06", "site_building": "A楼",
                "target_key": target.key, "file_path": str(path),
                "status": "captured", "capture_batch_id": "batch-1",
            })
        return []
    monkeypatch.setattr(module, "_capture_with_browser_pool", fill)
    config = config_for(tmp_path)
    config["system_screenshot_capture"]["targets"] = [dict(key=item.key, label=item.label, span_id=item.span_id) for item in targets]
    result = asyncio.run(module.run_system_screenshot_capture_async(
        config=config, capture_date="2026-10-06", force=True, batch_id="batch-1",
    ))
    assert result["status"] == "success"
    assert captured == [targets[1].key]


def test_new_daily_capture_is_newer_than_old_hourly_record(tmp_path):
    path = tmp_path / "capture.png"
    path.write_bytes(b"test-png")
    modified = datetime(2026, 10, 6, 8, 0).timestamp()
    os.utime(path, (modified, modified))
    shared = {"capture_date": "2026-10-06", "site_building": "A楼", "target_key": "power_distribution", "file_path": str(path), "status": "captured"}
    module._upsert_record(tmp_path / "capture.json", {**shared, "capture_hour": "", "captured_at": "2026-10-06 10:00:00", "capture_batch_id": "new-batch"})
    module._upsert_record(tmp_path / "capture.json", {**shared, "capture_hour": "09", "captured_at": "2026-10-06 09:00:00", "capture_batch_id": "old-batch"})
    files = module.list_system_screenshot_files(config=config_for(tmp_path), capture_date="2026-10-06")["files"]
    assert len(files) == 1
    assert files[0]["capture_batch_id"] == "new-batch"


def test_state_write_failure_releases_capture_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "_save_capture_batch", Mock(side_effect=OSError("disk full")))
    container = SimpleNamespace(_system_screenshot_capture_run_lock=threading.Lock())
    with pytest.raises(OSError, match="disk full"):
        module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path))
    assert not container._system_screenshot_capture_run_lock.locked()
