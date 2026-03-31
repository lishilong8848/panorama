from __future__ import annotations

from typing import Any, Dict, List

from app.modules.feishu.repository.feishu_api_repository import FeishuApiRepository


class BitableUploadService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._repo = FeishuApiRepository(config)

    def upload_file_items(self, file_items: List[Dict[str, str]]) -> Any:
        return self._repo.upload_results(file_items)
