from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Callable, Dict

from app.config.config_adapter import normalize_role_mode
from pipeline_utils import load_calc_module


class CalculationService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = copy.deepcopy(config)

    def _deployment_role_mode(self) -> str:
        deployment_cfg = self.config.get("deployment", {})
        if not isinstance(deployment_cfg, dict):
            return ""
        return normalize_role_mode(deployment_cfg.get("role_mode"))

    def run_manual_upload(
        self,
        building: str,
        file_path: str,
        upload_date: str,
        legacy_switch_external_before_upload: bool,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        calc_module = load_calc_module()

        if bool(legacy_switch_external_before_upload):
            emit_log("[手动补传] 网络切换功能已移除，按当前角色网络直接上传")

        p = Path(file_path)
        results = calc_module.run_with_explicit_file_items(
            config=self.config,
            file_items=[
                {
                    "building": building,
                    "file_path": str(p),
                    "upload_date": str(upload_date or "").strip(),
                }
            ],
            upload=True,
            save_json=False,
            upload_log_feature="手动补传",
        )
        return {"building": building, "upload_date": upload_date, "result_count": len(results)}
