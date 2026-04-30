from __future__ import annotations

from typing import Any, Dict

from app.modules.feishu.repository.feishu_api_repository import FeishuApiRepository


class AttachmentUploadService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._repo = FeishuApiRepository(config)

    def import_sheet_workbook(self, xlsx_path: str) -> Dict[str, Any]:
        return self._repo.import_workbook_sheets(xlsx_path)
