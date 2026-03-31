from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/api/ocr", tags=["ocr"])


@router.get("/status")
def ocr_status() -> dict:
    return {"enabled": False, "message": "OCR模块预留"}
