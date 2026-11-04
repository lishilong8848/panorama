from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit
from uuid import uuid4

from app.modules.shared_bridge.service.alarm_event_page_export_service import (
    stream_alarm_event_json_document,
)
from app.modules.shared_bridge.service.report_api_session import BmsReportApiSession, ReportApiError


async def _run_blocking(function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # A requests call cannot be interrupted; do not close its session while it runs.
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:  # noqa: BLE001
                break
        if not task.cancelled():
            task.exception()
        raise


class _AsyncResponse:
    def __init__(self, response: Any) -> None:
        self._response = response
        self.ok = True  # BmsReportApiSession.request rejects non-2xx responses.

    async def json(self) -> Any:
        return self._response.json()

    async def text(self) -> str:
        return self._response.text


class _AsyncRequestContext:
    def __init__(self, session: BmsReportApiSession) -> None:
        self._session = session

    async def post(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        fail_on_status_code: bool | None = None,  # noqa: ARG002
    ) -> _AsyncResponse:
        parsed = urlsplit(url)
        base = urlsplit(self._session.base_url)
        if (parsed.scheme, parsed.netloc) != (base.scheme, base.netloc) or parsed.query or parsed.fragment:
            raise ReportApiError("动环接口地址无效")
        response = await _run_blocking(
            self._session.request, "POST", parsed.path, headers=headers, json=data
        )
        return _AsyncResponse(response)


async def export_alarm_event_json(
    site: dict[str, Any],
    *,
    output_path: Path,
    source_family: str,
    building: str,
    bucket_kind: str,
    bucket_key: str,
    now: datetime | None = None,
    query_start: Any = None,
    query_end: Any = None,
    emit_log: Callable[[str], None] | None = None,
    log_prefix: str = "",
) -> dict[str, Any]:
    """Export with a private, automatically authenticated BMS HTTP session."""
    output_path = Path(output_path)
    stage_path = output_path.with_name(f".{output_path.name}.{uuid4().hex}.stage")
    temp_path = stage_path.with_name(f"{stage_path.name}.tmp")
    session = BmsReportApiSession(site)
    try:
        await _run_blocking(session.login)
        completion_log: str | None = None

        def forward_log(message: str) -> None:
            nonlocal completion_log
            message = message.replace(str(stage_path), str(output_path))
            if message.startswith(f"{log_prefix}告警 API 拉取完成:"):
                completion_log = message
            elif emit_log is not None:
                emit_log(message)

        result = await stream_alarm_event_json_document(
            _AsyncRequestContext(session),
            base_url=session.base_url,
            output_path=stage_path,
            source_family=source_family,
            building=building,
            bucket_kind=bucket_kind,
            bucket_key=bucket_key,
            now=now,
            query_start=query_start,
            query_end=query_end,
            emit_log=forward_log if emit_log is not None else None,
            log_prefix=log_prefix,
        )
        stage_path.replace(output_path)
        if completion_log is not None and emit_log is not None:
            emit_log(completion_log)
        return result
    finally:
        try:
            session.close()
        finally:
            temp_path.unlink(missing_ok=True)
            stage_path.unlink(missing_ok=True)
