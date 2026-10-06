from __future__ import annotations

import asyncio
import pytest
from types import SimpleNamespace
from unittest.mock import Mock, AsyncMock
from app.shared.runtime.building_browser_locks import acquire_building_browser_lock, release_building_browser_lock
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService

from app.modules.shared_bridge.service.internal_download_browser_pool import InternalDownloadBrowserPool


def test_stop_finishes_cleanup_after_probe_cancellation() -> None:
    async def _run() -> None:
        pool = InternalDownloadBrowserPool({})
        pool._health_probe_task = asyncio.create_task(asyncio.sleep(60))
        pool._recovery_probe_task = asyncio.create_task(asyncio.sleep(60))

        await pool._async_stop()

        assert pool._health_probe_task is None
        assert pool._recovery_probe_task is None

    asyncio.run(_run())


def test_stop_keeps_thread_reference_when_cleanup_times_out() -> None:
    class _Loop:
        def call_soon_threadsafe(self, callback):
            callback()

        def stop(self):
            return None

    class _Thread:
        def join(self, timeout=None):
            return None

        def is_alive(self):
            return True

    pool = InternalDownloadBrowserPool({})
    loop = _Loop()
    thread = _Thread()
    pool._loop = loop  # type: ignore[assignment]
    pool._thread = thread  # type: ignore[assignment]

    result = pool.stop()

    assert result == {"stopped": False, "running": True, "reason": "cleanup_timeout"}
    assert pool._loop is loop
    assert pool._thread is thread


@pytest.mark.parametrize("wait_on_local", [False, True])
def test_cancel_waiter_does_not_leak_or_release_another_owner(wait_on_local):
    async def run():
        pool = InternalDownloadBrowserPool({})
        pool._locks["A楼"] = asyncio.Lock()
        if wait_on_local:
            await pool._locks["A楼"].acquire()
        else:
            assert acquire_building_browser_lock("A楼", owner="other", timeout_sec=0)
        async def work():
            async with pool._building_job_lock("A楼", "test"):
                pytest.fail("must remain waiting")
        task = asyncio.create_task(work())
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        if wait_on_local:
            pool._locks["A楼"].release()
        else:
            assert not acquire_building_browser_lock("A楼", owner="test", timeout_sec=0)
            release_building_browser_lock("A楼")
        assert acquire_building_browser_lock("A楼", owner="next", timeout_sec=0)
        release_building_browser_lock("A楼")
        assert not pool._job_tasks
    asyncio.run(run())


def test_screenshot_selector_failure_does_not_rebuild_download_browser(monkeypatch):
    async def run():
        pool = InternalDownloadBrowserPool({})
        pool._locks["A楼"] = asyncio.Lock()
        monkeypatch.setattr(pool, "_mark_slot_recycle_pending_if_needed", Mock())
        monkeypatch.setattr(pool, "_recycle_slot_if_needed", AsyncMock())
        monkeypatch.setattr(pool, "_ensure_page", AsyncMock(return_value=object()))
        monkeypatch.setattr(pool, "_fail_if_login_not_ready", AsyncMock())
        monkeypatch.setattr(pool, "_ensure_logged_in", AsyncMock())
        rebuild = AsyncMock()
        monkeypatch.setattr(pool, "_rebuild_slot_after_job_failure", rebuild)
        async def fail(page):
            raise RuntimeError("未找到系统入口")
        with pytest.raises(RuntimeError, match="未找到系统入口"):
            await pool._run_building_job("A楼", fail, owner="system_screenshot_capture")
        rebuild.assert_not_called()
        assert not pool._job_tasks
        assert not pool._slot_snapshot("A楼")["in_use"]
    asyncio.run(run())


def test_pool_shutdown_cancels_jobs_and_releases_resource():
    async def run():
        pool = InternalDownloadBrowserPool({})
        pool._locks["A楼"] = asyncio.Lock()
        entered = asyncio.Event()
        async def work():
            async with pool._building_job_lock("A楼", "test"):
                entered.set()
                await asyncio.sleep(60)
        task = asyncio.create_task(work())
        await entered.wait()
        await pool._async_stop()
        assert task.cancelled()
        assert acquire_building_browser_lock("A楼", owner="next", timeout_sec=0)
        release_building_browser_lock("A楼")
    asyncio.run(run())


def test_runtime_keeps_pool_registered_until_cleanup_finishes(monkeypatch):
    from app.modules.shared_bridge.service import shared_bridge_runtime_service as module
    pool = SimpleNamespace(stop=Mock(return_value={"running": True, "reason": "cleanup_timeout"}))
    service = SharedBridgeRuntimeService.__new__(SharedBridgeRuntimeService)
    service._internal_download_pool = pool
    clear = Mock()
    monkeypatch.setattr(module, "clear_internal_download_browser_pool", clear)
    assert not service._stop_internal_download_pool()
    assert service._internal_download_pool is pool
    clear.assert_not_called()
    pool.stop.return_value = {"running": False, "stopped": True}
    assert service._stop_internal_download_pool()
    clear.assert_called_once_with(pool)
    assert service._internal_download_pool is None


def test_repeated_stop_does_not_interrupt_async_cleanup():
    calls = []
    loop = SimpleNamespace(stop=lambda: None, call_soon_threadsafe=lambda callback: calls.append(callback))
    pool = InternalDownloadBrowserPool({})
    pool._loop = loop
    pool._thread = SimpleNamespace(is_alive=lambda: True, join=lambda **kwargs: None)
    assert pool.stop()["reason"] == "cleanup_timeout"
    assert pool.stop()["reason"] == "cleanup_timeout"
    assert len(calls) == 1


def test_source_download_overtakes_waiting_screenshot():
    async def run():
        pool = InternalDownloadBrowserPool({})
        pool._locks["A楼"] = asyncio.Lock()
        assert acquire_building_browser_lock("A楼", owner="busy", timeout_sec=0)
        order = []
        async def work(owner):
            async with pool._building_job_lock("A楼", owner):
                order.append(owner)
        screenshot = asyncio.create_task(work("system_screenshot_capture"))
        download = asyncio.create_task(work("source_file_download"))
        await asyncio.sleep(0)
        release_building_browser_lock("A楼")
        await asyncio.wait_for(asyncio.gather(screenshot, download), timeout=2)
        assert order == ["source_file_download", "system_screenshot_capture"]
        assert not pool._source_job_waiters
        assert not pool._job_tasks
    asyncio.run(run())
