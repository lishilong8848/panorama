from __future__ import annotations

from fastapi import APIRouter, Request


router = APIRouter(prefix="/api/feishu", tags=["feishu"])


@router.get("/status")
def feishu_status(request: Request) -> dict:
    container = request.app.state.container
    runtime_cfg = container.runtime_config
    feishu_cfg = runtime_cfg.get("feishu", {}) if isinstance(runtime_cfg, dict) else {}
    return {
        "enable_upload": bool(feishu_cfg.get("enable_upload", False)),
        "calc_table_id": str(feishu_cfg.get("calc_table_id", "") or ""),
        "attachment_table_id": str(feishu_cfg.get("attachment_table_id", "") or ""),
    }
