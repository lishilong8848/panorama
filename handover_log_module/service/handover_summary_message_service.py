from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from handover_log_module.core.shift_window import build_duty_window
from handover_log_module.repository.excel_reader import load_workbook_quietly


CONTACT_APP_TOKEN = "G7oUwGdwaiTmimk8i2ecGTWOn4d"
CONTACT_TABLE_ID = "tblATnYxuleJpyVl"
CONTACT_NAME_FIELD = "姓名"
CONTACT_PHONE_FIELD = "联系方式"

DUTY_PHONE_BY_BUILDING = {
    "A楼": "18114204727",
    "B楼": "18114375165",
    "C楼": "18094363218",
    "D楼": "18051309945",
    "E楼": "18100640527",
}


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _field_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("text", "name", "value", "label"):
            text = _text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        return "、".join(item for item in (_field_text(part) for part in value) if item)
    return _text(value)


def _formula_literal(value: Any) -> str:
    text = _text(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def _split_people(value: Any) -> List[str]:
    items = re.split(r"[、,，/；;\s]+", _text(value))
    output: List[str] = []
    for item in items:
        name = _text(item)
        if name and name not in output:
            output.append(name)
    return output


def _format_number_text(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    try:
        number = float(text.replace(",", ""))
    except ValueError:
        return text
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}".rstrip("0").rstrip(".")


def _append_unit(value: Any, unit: str) -> str:
    text = _format_number_text(value)
    if not text:
        return ""
    if unit.lower() in text.lower():
        return text
    return f"{text}{unit}"


def _format_duty_time(start_text: str, end_text: str) -> str:
    def _parse(value: str) -> datetime | None:
        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(value, pattern)
            except ValueError:
                continue
        return None

    def _fmt(dt: datetime | None, fallback: str) -> str:
        if dt is None:
            return fallback
        return f"{dt.year}年{dt.month:02d}月{dt.day:02d}日{dt.hour}:{dt.minute:02d}"

    return f"{_fmt(_parse(start_text), start_text)}-{_fmt(_parse(end_text), end_text)}"


def _cell(ws: Any, cell_name: str) -> str:
    return _text(ws[str(cell_name).strip().upper()].value)


class HandoverSummaryMessageService:
    def __init__(self, handover_cfg: Dict[str, Any], *, config_path: str | Path | None = None) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}
        self.config_path = Path(config_path) if config_path else None
        self._contact_cache: Dict[str, str] = {}

    def build_for_session(
        self,
        session: Dict[str, Any],
        *,
        emit_log: Callable[[str], None] = print,
    ) -> str:
        payload = session if isinstance(session, dict) else {}
        building = _text(payload.get("building"))
        duty_date = _text(payload.get("duty_date"))
        duty_shift = _text(payload.get("duty_shift")).lower()
        output_file = _text(payload.get("output_file"))
        if not building or not duty_date or duty_shift not in {"day", "night"} or not output_file:
            return ""
        output_path = Path(output_file)
        if not output_path.exists():
            return ""

        output = self._read_output_context(output_path)
        current_people = _text(output.get("current_people"))
        next_people = _text(output.get("next_people"))
        current_names = _split_people(current_people)
        next_names = _split_people(next_people)
        phone_map = self._lookup_contact_phones(current_names + [name for name in next_names if name not in current_names], emit_log=emit_log)

        title = (_text(output.get("title")) or self._fallback_title(building, duty_shift)).strip("【】")
        lines = [
            f"【{title}】",
            f"【值班时间】{self._build_duty_time(duty_date=duty_date, duty_shift=duty_shift)}",
            f"【交班人员】{current_people}",
            f"【联系方式】{self._phones_for_names(current_names, phone_map)}",
            f"【接班人员】{next_people}",
            f"【联系方式】{self._phones_for_names(next_names, phone_map)}",
            f"【值班手机】{DUTY_PHONE_BY_BUILDING.get(building, '')}",
            "【交接内容】",
        ]
        lines.extend(self._build_handover_content_lines(building=building, output=output, session=payload))

        work_items = [item for item in (output.get("work_items", []) if isinstance(output.get("work_items", []), list) else []) if _text(item)]
        lines.append("")
        lines.append("【本班完成工作】")
        lines.extend([f"{idx}、{item}" for idx, item in enumerate(work_items, 1)] or ["1、值班巡检"])
        lines.extend(
            [
                "",
                "【重点关注项】",
                "1、盯屏时，关注运行冷塔的液位，如液位较低，及时查看冷塔补水是否正常，防止冷却水量不足；",
                "2、当BMS系统出现包间温湿度预警告警时，运维人员需及时前往包间空调间进行调整空调参数，控制包间温湿度值在正常范围之内；",
            ]
        )
        it_load = _append_unit(output.get("it_load"), "KW")
        total_load = _append_unit(output.get("total_load"), "KW")
        pue = _format_number_text(output.get("pue"))
        if it_load:
            lines.append(f"{building}IT负载功率:{it_load}")
        if total_load:
            lines.append(f"实时总负荷:{total_load}")
        if pue:
            lines.append(f"PUE:{pue}")
        return "\n".join(lines).strip()

    @staticmethod
    def _fallback_title(building: str, duty_shift: str) -> str:
        shift_text = "白班" if duty_shift == "day" else "夜班" if duty_shift == "night" else duty_shift
        return f"{building}世纪互联 {shift_text}".strip()

    def _build_duty_time(self, *, duty_date: str, duty_shift: str) -> str:
        download_cfg = self.handover_cfg.get("download", {})
        shift_windows = download_cfg.get("shift_windows", {}) if isinstance(download_cfg, dict) else {}
        try:
            window = build_duty_window(
                duty_date=duty_date,
                duty_shift=duty_shift,
                shift_windows=shift_windows if isinstance(shift_windows, dict) else {},
            )
            return _format_duty_time(window.start_time, window.end_time)
        except Exception:  # noqa: BLE001
            return duty_date

    def _read_output_context(self, output_path: Path) -> Dict[str, Any]:
        workbook = load_workbook_quietly(output_path, data_only=True)
        try:
            template_cfg = self.handover_cfg.get("template", {}) if isinstance(self.handover_cfg.get("template", {}), dict) else {}
            sheet_name = _text(template_cfg.get("sheet_name"))
            ws = workbook[sheet_name] if sheet_name and sheet_name in workbook.sheetnames else workbook.active
            planned = _cell(ws, "B13")
            powered = _cell(ws, "D13")
            return {
                "title": _cell(ws, "A1"),
                "current_people": _cell(ws, "C3"),
                "next_people": _cell(ws, "G3"),
                "pue": _cell(ws, "B6"),
                "total_load": _cell(ws, "D6"),
                "it_load": _cell(ws, "F6"),
                "planned_cabinets": planned,
                "powered_cabinets": powered,
                "unpowered_cabinets": self._calc_unpowered_cabinets(planned, powered),
                "work_items": self._extract_work_items(ws),
            }
        finally:
            workbook.close()

    @staticmethod
    def _calc_unpowered_cabinets(planned: Any, powered: Any) -> str:
        try:
            return str(max(0, int(float(_text(planned).replace(",", ""))) - int(float(_text(powered).replace(",", "")))))
        except Exception:  # noqa: BLE001
            return ""

    def _extract_work_items(self, ws: Any) -> List[str]:
        try:
            from handover_log_module.core.section_layout import build_section_logical_columns, parse_category_sections
        except Exception:  # noqa: BLE001
            return []
        items: List[str] = []
        for section in parse_category_sections(ws):
            section_name = _text(getattr(section, "name", ""))
            if not section_name or "事件" in section_name:
                continue
            columns = build_section_logical_columns(ws, section)
            for row_idx in range(section.template_data_row, section.end_row + 1):
                parts: List[str] = []
                for column in columns:
                    value = _text(ws[f"{column.key}{row_idx}"].value)
                    if not value or value in {"/", "无", "None"} or re.fullmatch(r"\d+(?:\.0)?", value):
                        continue
                    if value not in parts:
                        parts.append(value)
                item = "，".join(parts).strip("，")
                if item and item not in items:
                    items.append(item)
        return items

    @staticmethod
    def _normalize_running_units(raw: Any) -> Dict[str, List[Dict[str, Any]]]:
        output: Dict[str, List[Dict[str, Any]]] = {"west": [], "east": []}
        payload = raw if isinstance(raw, dict) else {}
        for zone in ("west", "east"):
            rows = payload.get(zone, [])
            if not isinstance(rows, list):
                continue
            for item in rows:
                if not isinstance(item, dict):
                    continue
                try:
                    unit = int(item.get("unit", 0) or 0)
                except Exception:  # noqa: BLE001
                    unit = 0
                if unit <= 0:
                    continue
                output[zone].append({"unit": unit, "mode_text": _text(item.get("mode_text"))})
            output[zone].sort(key=lambda row: int(row.get("unit", 0) or 0))
        return output

    @staticmethod
    def _fallback_cooling_line(*, zone: str, running_units: Dict[str, List[Dict[str, Any]]]) -> str:
        zone_name = "A区" if zone == "west" else "B区"
        active_units = list((running_units or {}).get(zone, []))
        running_count = len(active_units)
        backup_count = max(0, 3 - running_count)
        parts = [
            f"冷冻站{zone_name}3套制冷单元{running_count}用{backup_count}备",
            "群控模式为开启状态",
            "备用机组与备用二次泵状态正常可用",
        ]
        for item in active_units:
            unit = int(item.get("unit", 0) or 0)
            mode_text = _text(item.get("mode_text"))
            parts.append(f"{unit}#制冷单元{mode_text}模式运行正常" if mode_text else f"{unit}#制冷单元运行正常")
        return "，".join(parts).rstrip("，") + "；"

    @staticmethod
    def _running_units_from_capacity_file(capacity_output_file: str) -> Dict[str, List[Dict[str, Any]]]:
        output: Dict[str, List[Dict[str, Any]]] = {"west": [], "east": []}
        path = Path(_text(capacity_output_file))
        if not _text(capacity_output_file) or not path.exists() or not path.is_file():
            return output
        workbook = load_workbook_quietly(path, data_only=True)
        try:
            ws = workbook.active
            for zone, cells in {"west": ["D23", "D33"], "east": ["Q23", "Q33"]}.items():
                for cell_name in cells:
                    text = _cell(ws, cell_name)
                    match = re.search(r"(\d+)\s*[#号]?\s*制冷单元\s*[→:：-]?\s*([\u4e00-\u9fff]*)", text)
                    if not match:
                        continue
                    output[zone].append({"unit": int(match.group(1)), "mode_text": _text(match.group(2))})
        finally:
            workbook.close()
        for zone in ("west", "east"):
            output[zone].sort(key=lambda row: int(row.get("unit", 0) or 0))
        return output

    def _cooling_content_lines(self, session: Dict[str, Any]) -> List[str]:
        summary = session.get("capacity_cooling_summary", {}) if isinstance(session, dict) else {}
        lines_payload = summary.get("lines", {}) if isinstance(summary, dict) else {}
        west_line = _text(lines_payload.get("west")) if isinstance(lines_payload, dict) else ""
        east_line = _text(lines_payload.get("east")) if isinstance(lines_payload, dict) else ""
        if west_line or east_line:
            return [
                west_line or self._fallback_cooling_line(zone="west", running_units={"west": [], "east": []}),
                east_line or self._fallback_cooling_line(zone="east", running_units={"west": [], "east": []}),
            ]
        running_units = self._normalize_running_units(session.get("capacity_running_units", {}) if isinstance(session, dict) else {})
        if not running_units.get("west") and not running_units.get("east"):
            running_units = self._running_units_from_capacity_file(_text(session.get("capacity_output_file")) if isinstance(session, dict) else "")
        return [
            self._fallback_cooling_line(zone="west", running_units=running_units),
            self._fallback_cooling_line(zone="east", running_units=running_units),
        ]

    def _build_handover_content_lines(self, *, building: str, output: Dict[str, Any], session: Dict[str, Any] | None = None) -> List[str]:
        powered = _format_number_text(output.get("powered_cabinets"))
        unpowered = _format_number_text(output.get("unpowered_cabinets"))
        cabinet_text = (
            f"{building}已上电机柜{powered}个，未上电机柜{unpowered}个；"
            if powered and unpowered
            else f"{building}机柜上电信息以审核表为准；"
        )
        code_match = re.search(r"([A-Za-z])", building)
        code = code_match.group(1).upper() if code_match else ""
        switchgear = f"{code}-144、{code}-120变电所" if code else "变电所"
        cooling_lines = self._cooling_content_lines(session if isinstance(session, dict) else {})
        return [
            f"1、{building}机房楼由双路市电带载运行，{switchgear}内10KV中压母联开关为“热备用”状态，投退方式为“自投自复”状态，综保状态正常；",
            f"2、{cooling_lines[0]}",
            f"3、{cooling_lines[1]}",
            f"4、{cabinet_text}",
            "5、变更，事件周汇总，当日有I2及以上告警需填写并发送H楼；",
            "6、每晚24点进行超功率机柜统计，每月进行超功率汇总统计；",
            "7、室外温湿度变化较大，注意多观察机房温湿度，及时调整空调及恒湿机，及时调整运行模式；",
        ]

    def _build_contact_client(self) -> FeishuBitableClient:
        global_feishu = require_feishu_auth_settings(self.handover_cfg, config_path=self.config_path)
        return FeishuBitableClient(
            app_id=str(global_feishu.get("app_id", "") or "").strip(),
            app_secret=str(global_feishu.get("app_secret", "") or "").strip(),
            app_token=CONTACT_APP_TOKEN,
            calc_table_id=CONTACT_TABLE_ID,
            attachment_table_id=CONTACT_TABLE_ID,
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
        )

    def _lookup_contact_phones(self, names: List[str], *, emit_log: Callable[[str], None]) -> Dict[str, str]:
        output: Dict[str, str] = {}
        missing_names = [name for name in names if name and name not in self._contact_cache]
        client: FeishuBitableClient | None = None
        for name in missing_names:
            try:
                if client is None:
                    client = self._build_contact_client()
                records = client.list_records(
                    table_id=CONTACT_TABLE_ID,
                    page_size=20,
                    max_records=20,
                    filter_formula=f"CurrentValue.[{CONTACT_NAME_FIELD}]={_formula_literal(name)}",
                )
                phone = ""
                for record in records:
                    fields = record.get("fields", {}) if isinstance(record, dict) else {}
                    if isinstance(fields, dict):
                        phone = _field_text(fields.get(CONTACT_PHONE_FIELD))
                    if phone:
                        break
                self._contact_cache[name] = phone
            except Exception as exc:  # noqa: BLE001
                self._contact_cache[name] = ""
                emit_log(f"[交接班][审核文本] 联系方式查询失败 name={name}, error={exc}")
        for name in names:
            output[name] = self._contact_cache.get(name, "")
        return output

    @staticmethod
    def _phones_for_names(names: List[str], phone_map: Dict[str, str]) -> str:
        return "、".join(phone_map.get(name, "") for name in names if phone_map.get(name, ""))
