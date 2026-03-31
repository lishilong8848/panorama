from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Callable, Dict

from app.modules.network.service.wifi_switch_service import WifiSwitchService
from pipeline_utils import load_calc_module


class CalculationService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = copy.deepcopy(config)
        self._wifi = WifiSwitchService(self.config)

    def _deployment_role_mode(self) -> str:
        deployment_cfg = self.config.get("deployment", {})
        if not isinstance(deployment_cfg, dict):
            return "switching"
        text = str(deployment_cfg.get("role_mode", "") or "").strip().lower()
        if text == "hybrid":
            return "switching"
        if text in {"switching", "internal", "external"}:
            return text
        return "switching"

    def run_manual_upload(
        self,
        building: str,
        file_path: str,
        upload_date: str,
        legacy_switch_external_before_upload: bool,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        _ = bool(legacy_switch_external_before_upload)
        calc_module = load_calc_module()

        should_switch_external = self._deployment_role_mode() == "switching"
        if should_switch_external:
            external_ssid = str(self.config.get("network", {}).get("external_ssid", "")).strip()
            if external_ssid:
                ok, msg = self._wifi.connect(external_ssid)
                if not ok:
                    emit_log(
                        "[文件流程失败] 功能=手动补传 阶段=WiFi切换(外网) 楼栋="
                        f"{building or '-'} 文件={file_path or '-'} 日期={upload_date or '-'} 错误={msg}"
                    )
                    raise RuntimeError(f"切换外网失败: {msg}")

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
