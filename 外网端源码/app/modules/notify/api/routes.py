from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from app.modules.notify.service.webhook_notify_service import WebhookNotifyService


router = APIRouter(prefix="/api/notify", tags=["notify"])


@router.post("/test")
def notify_test(request: Request) -> dict:
    container = request.app.state.container
    service = WebhookNotifyService(container.runtime_config)
    try:
        service.send_failure(stage="测试告警", detail="手动触发测试告警", emit_log=container.add_system_log)
        return {"ok": True}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
