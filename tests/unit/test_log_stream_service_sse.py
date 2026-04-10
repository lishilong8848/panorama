from __future__ import annotations

import asyncio

from app.modules.websocket.service.log_stream_service import LogStreamService, SystemLogStreamService
from app.modules.report_pipeline.service.job_service import TaskEngineUnavailableError


class _FakeJobService:
    def __init__(self) -> None:
        self.calls = 0

    def get_logs(self, job_id: str, offset: int = 0, *, after_event_id: int | None = None, limit: int = 1000):  # noqa: ANN001
        self.calls += 1
        cursor = int(after_event_id or 0)
        events = []
        if cursor < 2:
            events.append(
                {
                    "event_id": 2,
                    "stage_id": "main",
                    "stream": "stdout",
                    "event_type": "stage_status",
                    "level": "info",
                    "created_at": "2026-03-26 10:00:00",
                    "payload": {"status": "running"},
                }
            )
        if cursor < 3:
            events.append(
                {
                    "event_id": 3,
                    "stage_id": "main",
                    "stream": "stdout",
                    "event_type": "result",
                    "level": "info",
                    "created_at": "2026-03-26 10:00:01",
                    "payload": {"ok": True},
                }
            )
        return {
            "job_id": job_id,
            "status": "success",
            "events": [item for item in events if int(item["event_id"]) > cursor],
            "last_event_id": 3,
            "lines": [],
        }


class _FakeRequest:
    def __init__(self) -> None:
        self._calls = 0

    async def is_disconnected(self) -> bool:
        self._calls += 1
        return self._calls > 1


class _FakeContainer:
    def __init__(self) -> None:
        self.system_log_entries = [
            {"id": 1, "line": "one", "level": "info", "source": "system"},
            {"id": 2, "line": "two", "level": "warning", "source": "system"},
            {"id": 3, "line": "three", "level": "error", "source": "system"},
        ]


def _consume_stream(coro):
    async def _runner():
        chunks = []
        async for item in coro:
            chunks.append(item)
        return chunks

    return asyncio.run(_runner())


def test_job_log_stream_replays_from_last_event_id() -> None:
    service = LogStreamService(_FakeJobService())
    chunks = _consume_stream(service.stream("job-1", last_event_id=1))
    joined = "".join(chunks)
    assert "id: 2" in joined
    assert "id: 3" in joined
    assert 'event: stage_status' in joined
    assert 'event: result' in joined
    assert 'event: done' in joined


def test_job_log_stream_emits_done_for_interrupted_status() -> None:
    class _InterruptedJobService(_FakeJobService):
        def get_logs(self, job_id: str, offset: int = 0, *, after_event_id: int | None = None, limit: int = 1000):  # noqa: ANN001
            return {
                "job_id": job_id,
                "status": "interrupted",
                "events": [],
                "last_event_id": 9,
                "lines": [],
            }

    service = LogStreamService(_InterruptedJobService())
    chunks = _consume_stream(service.stream("job-interrupted", last_event_id=9))
    joined = "".join(chunks)
    assert "event: done" in joined
    assert '"status": "interrupted"' in joined or '"status":"interrupted"' in joined


def test_job_log_stream_emits_error_when_task_engine_temporarily_unavailable() -> None:
    class _UnavailableJobService:
        def get_logs(self, job_id: str, offset: int = 0, *, after_event_id: int | None = None, limit: int = 1000):  # noqa: ANN001
            raise TaskEngineUnavailableError("任务状态存储暂时不可用，请稍后重试")

    service = LogStreamService(_UnavailableJobService())
    chunks = _consume_stream(service.stream("job-unavailable", last_event_id=0))
    joined = "".join(chunks)
    assert "event: error" in joined
    assert "任务状态存储暂时不可用" in joined
    assert "event: done" in joined
    assert '"status": "unavailable"' in joined or '"status":"unavailable"' in joined


def test_system_log_stream_replays_from_last_event_id() -> None:
    service = SystemLogStreamService(_FakeContainer())
    chunks = _consume_stream(service.stream(request=_FakeRequest(), last_event_id=1))
    joined = "".join(chunks)
    assert "id: 1" not in joined
    assert "id: 2" in joined
    assert "id: 3" in joined
    assert 'event: log' in joined
