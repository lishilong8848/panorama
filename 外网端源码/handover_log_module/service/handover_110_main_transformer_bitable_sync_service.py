from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from handover_log_module.repository.excel_reader import load_workbook_quietly
from handover_log_module.service.handover_110_station_upload_service import _resolve_runtime_file_path


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


class Handover110MainTransformerBitableSyncService:
    """Upload the 110kV main transformer handover values to the fixed Bitable table."""

    LOG_PREFIX = "[交接班][110主变多维]"
    TRANSFORMER_COLUMNS = (9, 10, 11, 12)  # I:L
    GIS_TRANSFORMER_COLUMNS = (7, 8, 9, 10)  # G:J
    TRANSFORMER_NAMES = ("1号主变", "2号主变", "3号主变", "4号主变")
    LINE_NAMES = ("阿开线", "阿开线", "阿家线", "阿家线")
    REQUIRED_FIELDS = (
        "日期",
        "班次",
        "主变名称",
        "所属线路",
        "油温",
        "档位",
        "负载（KW）",
        "电流（A）",
        "负载率%",
        "GIS压力是否正常",
    )

    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "target": {
                "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
                "table_id": "tbl8Ni54taYGeWAa",
                "page_size": 500,
                "max_records": 5000,
                "delete_batch_size": 200,
                "create_batch_size": 200,
            },
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("station_110_transformer_bitable", {})
        if not isinstance(raw, dict):
            raw = {}
        cfg = _deep_merge(self._defaults(), raw)
        target = cfg.get("target", {}) if isinstance(cfg.get("target", {}), dict) else {}
        defaults = self._defaults()["target"]
        for key in ("app_token", "table_id"):
            target[key] = str(target.get(key, defaults[key]) or defaults[key]).strip()
        for key in ("page_size", "max_records", "delete_batch_size", "create_batch_size"):
            target[key] = max(1, int(target.get(key, defaults[key]) or defaults[key]))
        cfg["target"] = target
        cfg["enabled"] = bool(cfg.get("enabled", True))
        return cfg

    def _new_client(self, cfg: Dict[str, Any]) -> FeishuBitableClient:
        global_feishu = require_feishu_auth_settings(self.handover_cfg)
        target = cfg.get("target", {})
        app_token = str(target.get("app_token", "") or "").strip()
        table_id = str(target.get("table_id", "") or "").strip()
        if not app_token or not table_id:
            raise ValueError("110主变多维表配置缺失: app_token/table_id")
        return FeishuBitableClient(
            app_id=str(global_feishu.get("app_id", "") or "").strip(),
            app_secret=str(global_feishu.get("app_secret", "") or "").strip(),
            app_token=app_token,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
            emit_log=None,
        )

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _number(value: Any) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value or "").strip().replace(",", "").replace("%", "")
        if not text or text in {"/", "-", "N/A"}:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    @staticmethod
    def _text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value).strip()

    @classmethod
    def _format_percent(cls, value: Any) -> str:
        number = cls._number(value)
        if number is None:
            text = cls._text(value)
            return text if text.endswith("%") else ""
        if abs(number) <= 1:
            number *= 100
        return f"{number:.2f}%"

    @classmethod
    def _pressure_threshold(cls, label: Any) -> float:
        text = str(label or "").strip()
        if "断路器" in text:
            return 0.55
        if "PT" in text.upper() or "压变" in text:
            return 0.45
        return 0.35

    @classmethod
    def _gis_statuses(cls, worksheet: Any) -> List[str]:
        statuses: List[str] = []
        for col in cls.GIS_TRANSFORMER_COLUMNS:
            ok = True
            seen = False
            for row_idx in range(132, 138):
                label = worksheet.cell(row=row_idx, column=6).value
                value = cls._number(worksheet.cell(row=row_idx, column=col).value)
                if value is None:
                    continue
                seen = True
                if value < cls._pressure_threshold(label):
                    ok = False
                    break
            statuses.append("正常" if seen and ok else "异常")
        return statuses

    @classmethod
    def parse_workbook(cls, source_file: str | Path) -> List[Dict[str, Any]]:
        path = Path(source_file)
        if not path.exists():
            raise FileNotFoundError(f"110站上传文件不存在: {path}")
        workbook = load_workbook_quietly(path, data_only=True)
        try:
            if not workbook.worksheets:
                raise ValueError("110站文件缺少工作表")
            worksheet = workbook.worksheets[0]
            gis_statuses = cls._gis_statuses(worksheet)
            rows: List[Dict[str, Any]] = []
            for index, column in enumerate(cls.TRANSFORMER_COLUMNS):
                current_a = cls._number(worksheet.cell(row=31, column=column).value)
                max_load_mw = cls._number(worksheet.cell(row=34, column=column).value)
                load_rate = cls._format_percent(worksheet.cell(row=35, column=column).value)
                oil_temp = cls._text(worksheet.cell(row=39, column=column).value)
                tap_position = cls._text(worksheet.cell(row=41, column=column).value)
                if current_a is None or max_load_mw is None or not load_rate:
                    raise ValueError(f"{cls.TRANSFORMER_NAMES[index]} 主变关键数据缺失")
                rows.append(
                    {
                        "transformer_name": cls.TRANSFORMER_NAMES[index],
                        "line_name": cls.LINE_NAMES[index],
                        "oil_temp": oil_temp,
                        "tap_position": tap_position,
                        "load_kw": round(max_load_mw * 1000),
                        "current_a": round(current_a, 2),
                        "load_rate": load_rate,
                        "gis_status": gis_statuses[index],
                    }
                )
            if len(rows) != 4:
                raise ValueError(f"110主变数据不完整: {len(rows)}/4")
            return rows
        finally:
            workbook.close()

    @staticmethod
    def _normalize_select_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            for key in ("name", "text", "value"):
                text = str(value.get(key, "") or "").strip()
                if text:
                    return text
            return ""
        if isinstance(value, list):
            for item in value:
                text = Handover110MainTransformerBitableSyncService._normalize_select_text(item)
                if text:
                    return text
            return ""
        return str(value).strip()

    @staticmethod
    def _normalize_date_to_midnight_ms(value: Any) -> int | None:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            number = int(value)
            if number <= 0:
                return None
            if number < 10**11:
                number *= 1000
            dt = datetime.fromtimestamp(number / 1000)
            return int(datetime(dt.year, dt.month, dt.day, 0, 0, 0).timestamp() * 1000)
        text = str(value or "").strip().replace("/", "-")
        try:
            dt = datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return None
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _shift_text(duty_shift: str) -> str:
        return "白班" if str(duty_shift or "").strip().lower() == "day" else "夜班"

    @staticmethod
    def _date_ms(duty_date: str) -> int:
        return date_text_to_timestamp_ms(str(duty_date or "").strip(), default_day=1, tz_offset_hours=8)

    def _existing_record_ids(
        self,
        *,
        client: FeishuBitableClient,
        table_id: str,
        duty_date: str,
        duty_shift: str,
        cfg: Dict[str, Any],
    ) -> List[str]:
        target = cfg.get("target", {})
        target_date_ms = self._date_ms(duty_date)
        target_shift = self._shift_text(duty_shift)
        records = client.list_records(
            table_id=table_id,
            page_size=int(target.get("page_size", 500) or 500),
            max_records=int(target.get("max_records", 5000) or 5000),
            field_names=["日期", "班次", "主变名称"],
        )
        matched: List[str] = []
        for item in records:
            if not isinstance(item, dict):
                continue
            record_id = str(item.get("record_id", "") or "").strip()
            fields = item.get("fields", {}) if isinstance(item.get("fields", {}), dict) else {}
            if not record_id:
                continue
            date_ms = self._normalize_date_to_midnight_ms(fields.get("日期"))
            shift_text = self._normalize_select_text(fields.get("班次"))
            transformer = self._normalize_select_text(fields.get("主变名称"))
            if date_ms == target_date_ms and shift_text == target_shift and transformer in self.TRANSFORMER_NAMES:
                matched.append(record_id)
        return matched

    def _validate_fields(self, client: FeishuBitableClient, table_id: str) -> None:
        fields_meta = client.list_fields(table_id=table_id)
        names = {
            str(item.get("field_name", "") or item.get("name", "") or "").strip()
            for item in fields_meta
            if isinstance(item, dict)
        }
        missing = [name for name in self.REQUIRED_FIELDS if name not in names]
        if missing:
            raise ValueError(f"110主变多维表字段缺失: {', '.join(missing)}")

    def _payload_fields(self, *, duty_date: str, duty_shift: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        date_ms = self._date_ms(duty_date)
        shift_text = self._shift_text(duty_shift)
        payloads: List[Dict[str, Any]] = []
        for row in rows:
            payloads.append(
                {
                    "日期": date_ms,
                    "班次": shift_text,
                    "主变名称": str(row.get("transformer_name", "") or "").strip(),
                    "所属线路": str(row.get("line_name", "") or "").strip(),
                    "油温": str(row.get("oil_temp", "") or "").strip(),
                    "档位": str(row.get("tap_position", "") or "").strip(),
                    "负载（KW）": row.get("load_kw"),
                    "电流（A）": row.get("current_a"),
                    "负载率%": str(row.get("load_rate", "") or "").strip(),
                    "GIS压力是否正常": str(row.get("gis_status", "") or "").strip(),
                }
            )
        return payloads

    def sync_from_upload_state(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        upload_state: Dict[str, Any],
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        cfg = self._normalize_cfg()
        if not cfg.get("enabled", True):
            return {"status": "skipped", "reason": "disabled", "updated_at": self._now_text()}
        target = cfg.get("target", {})
        table_id = str(target.get("table_id", "") or "").strip()
        state = upload_state if isinstance(upload_state, dict) else {}
        stored_path_text = str(state.get("stored_path", "") or "").strip()
        if not stored_path_text or str(state.get("status", "")).strip().lower() not in {"success", "parsed"}:
            return {
                "status": "skipped",
                "reason": "no_success_upload",
                "error": str(state.get("error", "") or "").strip(),
                "updated_at": self._now_text(),
            }
        source_file = _resolve_runtime_file_path(self.handover_cfg, stored_path_text)
        rows = self.parse_workbook(source_file)
        if len(rows) != 4:
            raise ValueError(f"110主变数据不完整: {len(rows)}/4")
        payloads = self._payload_fields(duty_date=duty_date, duty_shift=duty_shift, rows=rows)
        if len(payloads) != 4 or any(not row.get("主变名称") for row in payloads):
            raise ValueError("110主变多维写入 payload 不完整")

        client = self._new_client(cfg)
        self._validate_fields(client, table_id)
        old_ids = self._existing_record_ids(
            client=client,
            table_id=table_id,
            duty_date=duty_date,
            duty_shift=duty_shift,
            cfg=cfg,
        )
        responses = client.batch_create_records(
            table_id=table_id,
            fields_list=payloads,
            batch_size=int(target.get("create_batch_size", 200) or 200),
        )
        deleted = 0
        if old_ids:
            deleted = client.batch_delete_records(
                table_id=table_id,
                record_ids=old_ids,
                batch_size=int(target.get("delete_batch_size", 200) or 200),
            )
        created = len(payloads)
        emit_log(
            f"{self.LOG_PREFIX} 写入完成 duty={duty_date}|{duty_shift}, "
            f"deleted={deleted}, created={created}"
        )
        return {
            "status": "success",
            "duty_date": str(duty_date or "").strip(),
            "duty_shift": str(duty_shift or "").strip().lower(),
            "table_id": table_id,
            "source_file": str(source_file),
            "deleted_records": deleted,
            "created_records": created,
            "response_count": len(responses),
            "updated_at": self._now_text(),
            "error": "",
        }
