from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/api/user", tags=["user"])


@router.get("/status")
def user_status() -> dict:
    return {"enabled": False, "message": "用户模块预留"}
