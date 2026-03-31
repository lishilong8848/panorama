from __future__ import annotations

import asyncio
from dataclasses import dataclass

from app.modules.report_pipeline.service.download_retry_utils import retry_failed_download_tasks


@dataclass
class _Task:
    date_text: str
    start_time: str
    end_time: str
    save_dir: str
    site: dict
    attempt_round: str = "first_pass"


@dataclass
class _Outcome:
    success: bool


def test_retry_failed_download_tasks_stops_when_retry_disabled() -> None:
    cfg = {"download": {"performance": {"retry_failed_after_all_done": False, "retry_failed_max_rounds": 1}}}
    called = {"n": 0}

    async def _fake_runner(**kwargs):
        called["n"] += 1
        return []

    out = asyncio.run(
        retry_failed_download_tasks(
            config=cfg,
            failed_tasks=[_Task("2026-03-01", "s", "e", "d", {"building": "A楼"})],
            source_name="x",
            run_download_tasks_by_building=_fake_runner,
        )
    )
    assert out == []
    assert called["n"] == 0


def test_retry_failed_download_tasks_runs_until_success() -> None:
    cfg = {"download": {"performance": {"retry_failed_after_all_done": True, "retry_failed_max_rounds": 2}}}
    calls = {"n": 0}

    async def _fake_runner(**kwargs):
        calls["n"] += 1
        task = kwargs["download_tasks"][0]
        if calls["n"] == 1:
            return [(task, _Outcome(success=False))]
        return [(task, _Outcome(success=True))]

    out = asyncio.run(
        retry_failed_download_tasks(
            config=cfg,
            failed_tasks=[_Task("2026-03-01", "s", "e", "d", {"building": "A楼"})],
            source_name="x",
            run_download_tasks_by_building=_fake_runner,
        )
    )
    assert len(out) == 2
    assert calls["n"] == 2
    assert out[-1][1].success is True
