from __future__ import annotations

from typing import Any, Callable, Dict

from app.modules.report_pipeline.service.feishu_upload_runtime import (
    insert_calc_fields_to_local_mysql,
)
from pipeline_utils import load_calc_module


class MonthlyMysqlInitialBackupService:
    FIELD_NAMES = ["类型", "分类", "项目", "楼栋", "日期", "计算方式", "用电量"]

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    def run(
        self,
        *,
        emit_log: Callable[[str], None] = print,
        progress_callback: Callable[[Dict[str, Any]], None] | None = None,
        cancel_check: Callable[[], None] | None = None,
    ) -> Dict[str, Any]:
        feishu_cfg = self.config.get("feishu", {})
        if not isinstance(feishu_cfg, dict):
            raise ValueError("配置错误: feishu 缺失")
        mysql_cfg = feishu_cfg.get("local_mysql", {})
        if not isinstance(mysql_cfg, dict) or not bool(mysql_cfg.get("enabled", False)):
            raise ValueError("配置错误: feishu.local_mysql 未启用")
        if callable(cancel_check):
            cancel_check()
        emit_log("[首次备份多维记录] 正在检查本地MySQL连接和表结构")
        insert_calc_fields_to_local_mysql(mysql_cfg=mysql_cfg, fields_list=[])

        calc_module = load_calc_module()
        client = calc_module.FeishuBitableClient(
            app_id=str(feishu_cfg.get("app_id", "") or "").strip(),
            app_secret=str(feishu_cfg.get("app_secret", "") or "").strip(),
            app_token=str(feishu_cfg.get("app_token", "") or "").strip(),
            calc_table_id=str(feishu_cfg.get("calc_table_id", "") or "").strip(),
            attachment_table_id=str(feishu_cfg.get("attachment_table_id", "") or "").strip(),
            date_field_mode=str(feishu_cfg.get("date_field_mode", "timestamp") or "timestamp").strip(),
            date_field_day=int(feishu_cfg.get("date_field_day", 1) or 1),
            date_tz_offset_hours=int(feishu_cfg.get("date_tz_offset_hours", 8) or 8),
            timeout=int(feishu_cfg.get("timeout", 30) or 30),
            request_retry_count=int(feishu_cfg.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(feishu_cfg.get("request_retry_interval_sec", 1) or 1),
            emit_log=emit_log,
        )
        table_id = str(feishu_cfg.get("calc_table_id", "") or "").strip()
        if not table_id:
            raise ValueError("配置错误: feishu.calc_table_id 不能为空")

        def _progress(payload: Dict[str, Any]) -> None:
            if callable(progress_callback):
                progress_callback(payload)

        last_logged = 0

        def _read_progress(fetched: int, total: int) -> None:
            nonlocal last_logged
            if callable(cancel_check):
                cancel_check()
            percent = min(80, int(fetched * 80 / total)) if total > 0 else 5
            _progress(
                {
                    "phase": "reading",
                    "progress": percent,
                    "fetched_records": fetched,
                    "total_records": total,
                    "written_records": 0,
                    "message": f"正在读取飞书记录 {fetched}/{total or '?'}",
                }
            )
            if fetched == total or fetched - last_logged >= 500:
                last_logged = fetched
                emit_log(f"[首次备份多维记录] 已读取 {fetched}/{total or '?'} 条")

        emit_log(f"[首次备份多维记录] 开始读取飞书多维表: table={table_id}")
        _progress(
            {
                "phase": "reading",
                "progress": 0,
                "fetched_records": 0,
                "total_records": 0,
                "written_records": 0,
                "message": "正在读取飞书记录",
            }
        )
        records = client.list_records(
            table_id=table_id,
            page_size=500,
            max_records=0,
            field_names=self.FIELD_NAMES,
            progress_callback=_read_progress,
        )
        if callable(cancel_check):
            cancel_check()
        fields_list = [
            item.get("fields", {})
            for item in records
            if isinstance(item, dict) and isinstance(item.get("fields", {}), dict)
        ]
        total = len(fields_list)
        _progress(
            {
                "phase": "writing",
                "progress": 85,
                "fetched_records": total,
                "total_records": total,
                "written_records": 0,
                "message": f"正在批量写入 MySQL，共 {total} 条",
            }
        )
        emit_log(f"[首次备份多维记录] 飞书读取完成，开始一次性批量写入MySQL: count={total}")
        written = insert_calc_fields_to_local_mysql(
            mysql_cfg=mysql_cfg,
            fields_list=fields_list,
            require_building=False,
        )
        result = {
            "status": "success",
            "fetched_records": total,
            "total_records": total,
            "written_records": written,
        }
        _progress(
            {
                "phase": "completed",
                "progress": 100,
                **result,
                "message": f"备份完成，共写入 {written} 条",
            }
        )
        emit_log(f"[首次备份多维记录] 完成: fetched={total}, written={written}")
        return result
