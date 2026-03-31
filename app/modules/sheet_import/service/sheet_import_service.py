from __future__ import annotations

from typing import Any, Callable, Dict

from app.modules.feishu.service.attachment_upload_service import AttachmentUploadService
from app.modules.network.service.wifi_switch_service import WifiSwitchService


class SheetImportService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self._upload = AttachmentUploadService(config)
        self._wifi = WifiSwitchService(config)

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

    def run(
        self,
        xlsx_path: str,
        legacy_switch_external_before_upload: bool,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        _ = bool(legacy_switch_external_before_upload)
        if not bool(self.config.get("feishu_sheet_import", {}).get("enabled", False)):
            raise RuntimeError("feishu_sheet_import.enabled=false，5Sheet导表已禁用")

        should_switch_external = self._deployment_role_mode() == "switching"
        if should_switch_external:
            external_ssid = str(self.config.get("network", {}).get("external_ssid", "")).strip()
            if external_ssid:
                ok, msg = self._wifi.connect(external_ssid)
                if not ok:
                    emit_log(
                        "[文件流程失败] 功能=5Sheet导表 阶段=WiFi切换(外网) 楼栋=- "
                        f"文件={xlsx_path or '-'} 日期=- 错误={msg}"
                    )
                    raise RuntimeError(f"切换外网失败: {msg}")

        try:
            result = self._upload.import_sheet_workbook(xlsx_path)
        except Exception as exc:  # noqa: BLE001
            emit_log(
                "[文件流程失败] 功能=5Sheet导表 阶段=导表执行 楼栋=- "
                f"文件={xlsx_path or '-'} 日期=- 错误={exc}"
            )
            raise

        success_count = int(result.get("success_count", 0))
        failed_count = int(result.get("failed_count", 0))
        if failed_count > 0:
            for row in result.get("sheet_results", []):
                if bool(row.get("success", False)):
                    continue
                sheet_name = str(row.get("sheet_name", "")).strip() or "-"
                error = str(row.get("error", "")).strip() or "未知错误"
                emit_log(
                    "[文件流程失败] 功能=5Sheet导表 阶段="
                    f"Sheet:{sheet_name} 楼栋=- 文件={xlsx_path or '-'} 日期=- 错误={error}"
                )
        else:
            emit_log(
                "[文件上传成功] 功能=5Sheet导表 阶段=飞书上传完成 楼栋=- "
                f"文件={xlsx_path or '-'} 日期=- 详情=成功Sheet={success_count} 失败Sheet={failed_count}"
            )
        return result
