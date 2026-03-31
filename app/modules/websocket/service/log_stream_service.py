from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator


_TERMINAL_JOB_STATUSES = {
    "success",
    "failed",
    "cancelled",
    "partial_failed",
    "interrupted",
    "blocked_precondition",
}


def _sse(event_id: int, event: str, payload: dict) -> str:
    parts = [f"id: {int(event_id or 0)}", f"event: {event}", f"data: {json.dumps(payload, ensure_ascii=False)}", ""]
    return "\n".join(parts) + "\n"


class LogStreamService:
    def __init__(self, job_service) -> None:
        self.job_service = job_service

    async def stream(self, job_id: str, *, last_event_id: int = 0) -> AsyncIterator[str]:
        cursor = max(0, int(last_event_id or 0))
        while True:
            payload = self.job_service.get_logs(job_id, after_event_id=cursor, limit=1000)
            events = list(payload.get("events") or [])
            for item in events:
                event_id = int(item.get("event_id") or 0)
                cursor = max(cursor, event_id)
                event_payload = {
                    "job_id": job_id,
                    "stage_id": str(item.get("stage_id", "") or ""),
                    "level": str(item.get("level", "info") or "info"),
                    "stream": str(item.get("stream", "job") or "job"),
                    "event_type": str(item.get("event_type", "log") or "log"),
                    "created_at": str(item.get("created_at", "") or ""),
                    "payload": item.get("payload") or {},
                }
                yield _sse(event_id, str(item.get("event_type", "log") or "log"), event_payload)

            status = str(payload.get("status", "") or "")
            if status in _TERMINAL_JOB_STATUSES:
                done_id = max(cursor, int(payload.get("last_event_id") or 0))
                yield _sse(done_id, "done", {"job_id": job_id, "status": status, "last_event_id": done_id})
                break
            await asyncio.sleep(0.5)


class SystemLogStreamService:
    def __init__(self, container) -> None:
        self.container = container

    async def stream(self, request, *, last_event_id: int = 0) -> AsyncIterator[str]:
        cursor = max(0, int(last_event_id or 0))
        while True:
            entries = list(getattr(self.container, "system_log_entries", []))
            if entries:
                oldest_id = int(entries[0].get("id", 0) or 0)
                if cursor < oldest_id - 1:
                    cursor = oldest_id - 1
            for entry in entries:
                entry_id = int(entry.get("id", 0) or 0)
                if entry_id <= cursor:
                    continue
                cursor = entry_id
                payload = {
                    "line": str(entry.get("line", "") or ""),
                    "level": str(entry.get("level", "") or ""),
                    "source": str(entry.get("source", "") or ""),
                    "id": entry_id,
                }
                yield _sse(entry_id, "log", payload)

            if await request.is_disconnected():
                break
            await asyncio.sleep(0.5)
