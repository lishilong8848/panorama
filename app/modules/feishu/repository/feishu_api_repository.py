from __future__ import annotations

import copy
from typing import Any, Dict, List

from pipeline_utils import load_calc_module


class FeishuApiRepository:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = copy.deepcopy(config)
        self._calc_module = load_calc_module()

    def upload_results(self, file_items: List[Dict[str, str]]) -> Any:
        if hasattr(self._calc_module, "run_with_explicit_file_items"):
            return self._calc_module.run_with_explicit_file_items(
                config=self.config,
                file_items=file_items,
                upload=True,
                save_json=False,
            )
        raise RuntimeError("计算脚本缺少 run_with_explicit_file_items 入口")

    def import_workbook_sheets(self, xlsx_path: str) -> Dict[str, Any]:
        if hasattr(self._calc_module, "import_workbook_sheets_to_feishu"):
            return self._calc_module.import_workbook_sheets_to_feishu(self.config, xlsx_path)
        raise RuntimeError("计算脚本缺少 import_workbook_sheets_to_feishu 入口")
