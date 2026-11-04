import asyncio
from datetime import datetime
from types import SimpleNamespace

from app.modules.shared_bridge.api.routes import bridge_internal_runtime_status, bridge_internal_runtime_status_building
from app.modules.shared_bridge.service.internal_download_browser_pool import InternalDownloadBrowserPool
from app.shared.runtime.building_browser_locks import building_browser_lock_snapshot


def test_runtime_status_uses_memory_without_live_or_database_reads():
    def forbidden(*_args, **_kwargs):
        raise AssertionError("request path must not perform live or database reads")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    coordinator = SimpleNamespace(
        peek_scope_snapshot=lambda key: {"updated_at": now, "payload": {"marker": "cached"}} if key == "internal_runtime_summary" else None,
        read_scope_snapshot=forbidden,
        request_internal_runtime_refresh=forbidden,
    )
    container = SimpleNamespace(deployment_snapshot=lambda: {"role_mode": "internal"},
                                shared_bridge_service=SimpleNamespace(get_health_snapshot=forbidden),
                                runtime_status_coordinator=coordinator)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))
    result = asyncio.run(bridge_internal_runtime_status(request))
    assert result["summary"]["marker"] == "cached"
    assert result["summary"]["snapshot_stale"] is False


def test_missing_building_snapshot_reports_waiting_without_live_reads():
    refreshes = []
    coordinator = SimpleNamespace(peek_building_snapshot=lambda _building: None,
                                  request_internal_runtime_refresh=lambda **kw: refreshes.append(kw))
    container = SimpleNamespace(deployment_snapshot=lambda: {"role_mode": "internal"},
                                runtime_status_coordinator=coordinator)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))
    result = asyncio.run(bridge_internal_runtime_status_building("a", request))
    assert result["status"]["reason_code"] == "snapshot_pending"
    assert result["status"]["snapshot_stale"] is True
    assert len(refreshes) == 1


def test_page_lock_wait_never_holds_global_browser_lock():
    async def scenario():
        pool = InternalDownloadBrowserPool({})
        page_lock = asyncio.Lock()
        pool._locks["A楼"] = page_lock
        await page_lock.acquire()

        async def run():
            async with pool._building_job_lock("A楼", "source_file_download"):
                raise AssertionError("page lock should remain busy")

        task = asyncio.create_task(run())
        try:
            await asyncio.sleep(0.05)
            assert "A楼" not in building_browser_lock_snapshot()["locked_buildings"]
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            page_lock.release()
        assert pool._source_job_waiters.get("A楼", 0) == 0

    asyncio.run(scenario())
