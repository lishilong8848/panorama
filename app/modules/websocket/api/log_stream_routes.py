from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.modules.report_pipeline.service.job_service import TaskEngineUnavailableError
from app.modules.websocket.service.log_stream_service import LogStreamService, SystemLogStreamService


router = APIRouter(tags=["logs"])


@router.get("/api/jobs/{job_id}/logs")
async def job_logs(request: Request, job_id: str) -> StreamingResponse:
    container = request.app.state.container
    try:
        container.job_service.get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TaskEngineUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    service = LogStreamService(container.job_service)
    last_event_id = request.headers.get("Last-Event-ID") or request.query_params.get("last_event_id") or "0"
    return StreamingResponse(service.stream(job_id, last_event_id=int(str(last_event_id or "0") or "0")), media_type="text/event-stream")


@router.get("/api/logs/system")
async def system_logs(request: Request, offset: int = 0) -> StreamingResponse:
    container = request.app.state.container
    service = SystemLogStreamService(container)
    last_event_id = request.headers.get("Last-Event-ID") or request.query_params.get("last_event_id")
    cursor = int(str(last_event_id or offset or 0) or "0")
    return StreamingResponse(service.stream(request=request, last_event_id=cursor), media_type="text/event-stream")
