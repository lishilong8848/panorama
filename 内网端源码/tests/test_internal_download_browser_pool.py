from __future__ import annotations

import asyncio

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
