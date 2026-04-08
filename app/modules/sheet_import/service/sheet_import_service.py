from __future__ import annotations

from typing import Any, Callable, Dict

from app.config.config_adapter import normalize_role_mode
from app.modules.feishu.service.attachment_upload_service import AttachmentUploadService


class SheetImportService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self._upload = AttachmentUploadService(config)

    def _deployment_role_mode(self) -> str:
        deployment_cfg = self.config.get("deployment", {})
        if not isinstance(deployment_cfg, dict):
            return ""
        return normalize_role_mode(deployment_cfg.get("role_mode"))

    def run(
        self,
        xlsx_path: str,
        legacy_switch_external_before_upload: bool,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        if not bool(self.config.get("feishu_sheet_import", {}).get("enabled", False)):
            raise RuntimeError("feishu_sheet_import.enabled=false，5Sheet导表已禁用")

        if bool(legacy_switch_external_before_upload):
            emit_log("[5Sheet导表] 网络切换功能已移除，按当前角色网络直接导表")

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
