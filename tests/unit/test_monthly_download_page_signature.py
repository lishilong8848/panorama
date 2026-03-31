from __future__ import annotations

import asyncio

import 下载动环表格 as monthly_module


def test_monthly_download_retry_accepts_injected_page(monkeypatch) -> None:
    marker = object()
    captured: dict[str, object] = {}

    async def _fake_runtime(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr(monthly_module, "download_site_with_retry_runtime", _fake_runtime)

    result = asyncio.run(
        monthly_module._download_site_with_retry(  # noqa: SLF001
            context=None,
            download_cfg={},
            perf_cfg={},
            site={"building": "A楼"},
            start_time="2026-03-30 00:00:00",
            end_time="2026-03-30 01:00:00",
            page=marker,
        )
    )

    assert result == "ok"
    assert captured["page"] is marker

