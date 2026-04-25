from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from handover_log_module.core.chiller_mode_resolver import (
    DEFAULT_EAST_KEYS,
    DEFAULT_VALUE_MAP,
    DEFAULT_WEST_KEYS,
)
from handover_log_module.core.shift_window import build_duty_window
from handover_log_module.repository.excel_reader import load_workbook_quietly
from handover_log_module.service.capacity_report_common import CapacitySourceQuery
from handover_log_module.service.handover_extract_service import HandoverExtractService


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

RUNNING_MODE_TEXTS = {"制冷", "预冷", "板换"}
TOWER_LEVEL_ALIASES = ["冷却塔液位", "冷塔液位", "冷却塔水位", "冷塔水位"]
TANK_TEMP_ALIASES = ["蓄冷罐温度", "蓄水罐温度", "补水罐温度", "蓄冷罐后备温度"]
TANK_LEVEL_ALIASES = ["水池液位", "蓄水罐液位", "补水罐液位", "蓄冷罐液位"]
DEFAULT_EVENT_SECTION_NAMES = {"新事件处理", "历史事件跟进"}


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _field_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "label"):
            text = _text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts = [_field_text(item) for item in value]
        return "、".join(part for part in parts if part)
    return _text(value)


def _formula_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE()" if value else "FALSE()"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(int(value)) if isinstance(value, int) else str(value)
    text = _text(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def _split_people(value: Any) -> List[str]:
    text = _text(value)
    if not text:
        return []
    items = re.split(r"[、,，/；;\s]+", text)
    output: List[str] = []
    for item in items:
        name = _text(item)
        if name and name not in output:
            output.append(name)
    return output


def _extract_building_code(building: str) -> str:
    match = re.search(r"([A-Za-z])", _text(building))
    return match.group(1).upper() if match else ""


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


def _format_with_unit(value: Any, unit: str) -> str:
    text = _format_number_text(value)
    if not text:
        return ""
    if text.endswith(unit):
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
    value = ws[str(cell_name).strip().upper()].value
    return _text(value)


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
        source = self._read_source_context(
            building=building,
            data_file=_text(payload.get("data_file")),
            emit_log=emit_log,
        )
        capacity = self._read_capacity_output_context(_text(payload.get("capacity_output_file")))

        current_people = _text(output.get("current_people"))
        next_people = _text(output.get("next_people"))
        current_names = _split_people(current_people)
        next_names = _split_people(next_people)
        all_names = current_names + [name for name in next_names if name not in current_names]
        phone_map = self._lookup_contact_phones(all_names, emit_log=emit_log)

        title = _text(output.get("title")) or self._fallback_title(building, duty_shift)
        title = title.strip("【】")
        duty_time = self._build_duty_time(duty_date=duty_date, duty_shift=duty_shift)

        lines = [
            f"【{title}】",
            f"【值班时间】{duty_time}",
            f"【交班人员】{current_people}",
            f"【联系方式】{self._phones_for_names(current_names, phone_map)}",
            f"【接班人员】{next_people}",
            f"【联系方式】{self._phones_for_names(next_names, phone_map)}",
            f"【值班手机】{DUTY_PHONE_BY_BUILDING.get(building, '')}",
            "【交接内容】",
        ]
        lines.extend(self._build_handover_content_lines(building=building, output=output, source=source, capacity=capacity))

        work_items = list(output.get("work_items", [])) if isinstance(output.get("work_items", []), list) else []
        lines.append("")
        lines.append("【本班完成工作】")
        if work_items:
            lines.extend(f"{idx}、{item}" for idx, item in enumerate(work_items, 1))
        else:
            lines.append("1、值班巡检")

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
        return "\n".join(lines)

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
            sheet_name = _text(self.handover_cfg.get("template", {}).get("sheet_name") if isinstance(self.handover_cfg.get("template", {}), dict) else "")
            ws = workbook[sheet_name] if sheet_name and sheet_name in workbook.sheetnames else workbook.active
            planned = _cell(ws, "B13")
            powered = _cell(ws, "D13")
            return {
                "title": _cell(ws, "A1"),
                "shift_text": _cell(ws, "F2"),
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

    def _event_section_names(self) -> set[str]:
        names = set(DEFAULT_EVENT_SECTION_NAMES)
        raw = self.handover_cfg.get("event_sections", {})
        if isinstance(raw, dict):
            for key in ("category_names", "event_category_names", "sections"):
                value = raw.get(key)
                if isinstance(value, list):
                    names.update(_text(item) for item in value if _text(item))
        return names

    def _extract_work_items(self, ws: Any) -> List[str]:
        from handover_log_module.core.section_layout import build_section_logical_columns, parse_category_sections

        event_sections = self._event_section_names()
        items: List[str] = []
        for section in parse_category_sections(ws):
            section_name = _text(section.name)
            if not section_name:
                continue
            if section_name in event_sections or "事件" in section_name:
                continue
            columns = build_section_logical_columns(ws, section)
            for row_idx in range(section.template_data_row, section.end_row + 1):
                parts: List[str] = []
                for column in columns:
                    value = _text(ws[f"{column.key}{row_idx}"].value)
                    if not value or value in {"/", "无", "None"}:
                        continue
                    if re.fullmatch(r"\d+(?:\.0)?", value):
                        continue
                    if value not in parts:
                        parts.append(value)
                item = "，".join(parts).strip("，")
                if item and item not in items:
                    items.append(item)
        return items

    def _read_source_context(
        self,
        *,
        building: str,
        data_file: str,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        source_path = Path(data_file) if data_file else None
        if source_path is None or not source_path.exists():
            return {"rows": [], "running_units": {}}
        try:
            extracted = HandoverExtractService(self.handover_cfg).extract(building=building, data_file=str(source_path))
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][审核链接摘要] 源文件提取失败 building={building}, file={source_path}, error={exc}")
            return {"rows": [], "running_units": {}}
        hits = extracted.get("hits", {}) if isinstance(extracted, dict) else {}
        effective_config = extracted.get("effective_config", {}) if isinstance(extracted, dict) else {}
        return {
            "building": building,
            "rows": extracted.get("rows", []) if isinstance(extracted.get("rows", []), list) else [],
            "running_units": self._resolve_running_units(hits if isinstance(hits, dict) else {}, effective_config),
        }

    def _resolve_running_units(self, hits: Dict[str, Any], effective_config: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        chiller_cfg = effective_config.get("chiller_mode", {}) if isinstance(effective_config.get("chiller_mode", {}), dict) else {}
        value_map = chiller_cfg.get("value_map", DEFAULT_VALUE_MAP)
        if not isinstance(value_map, dict):
            value_map = DEFAULT_VALUE_MAP
        value_map = {str(key).strip(): str(value).strip() for key, value in value_map.items() if str(key).strip()}
        west_keys = chiller_cfg.get("west_keys", DEFAULT_WEST_KEYS)
        east_keys = chiller_cfg.get("east_keys", DEFAULT_EAST_KEYS)
        zone_keys = {
            "west": [str(item).strip() for item in west_keys if _text(item)],
            "east": [str(item).strip() for item in east_keys if _text(item)],
        }
        output: Dict[str, List[Dict[str, Any]]] = {"west": [], "east": []}
        for zone, keys in zone_keys.items():
            for key in keys:
                match = re.search(r"([1-6])$", key)
                unit = int(match.group(1)) if match else 0
                hit = hits.get(key)
                raw_value = getattr(hit, "value", None) if hit is not None else None
                mode_text = self._mode_text(raw_value, value_map)
                if unit <= 0 or mode_text not in RUNNING_MODE_TEXTS:
                    continue
                output[zone].append({"unit": unit, "mode_text": mode_text})
        output["west"].sort(key=lambda item: int(item.get("unit", 0) or 0))
        output["east"].sort(key=lambda item: int(item.get("unit", 0) or 0))
        return output

    @staticmethod
    def _mode_text(value: Any, value_map: Dict[str, str]) -> str:
        raw = _text(value)
        if not raw:
            return ""
        if raw in value_map:
            return _text(value_map.get(raw))
        try:
            number = float(raw)
        except ValueError:
            number = None
        if number is not None and int(number) == number:
            key = str(int(number))
            if key in value_map:
                return _text(value_map.get(key))
        lowered = raw.casefold()
        for text in value_map.values():
            if _text(text).casefold() == lowered:
                return _text(text)
        return raw

    @staticmethod
    def _read_capacity_output_context(capacity_output_file: str) -> Dict[str, Any]:
        path = Path(capacity_output_file) if capacity_output_file else None
        if path is None or not path.exists():
            return {}
        workbook = load_workbook_quietly(path, data_only=True, read_only=True)
        try:
            ws = workbook[workbook.sheetnames[0]]
            return {
                "titles": {
                    "west": [_cell(ws, "D23"), _cell(ws, "D33")],
                    "east": [_cell(ws, "Q23"), _cell(ws, "Q33")],
                },
                "tank_level": {"west": _cell(ws, "AC27"), "east": _cell(ws, "AC28")},
                "secondary_redundancy": {"west": _cell(ws, "D49"), "east": _cell(ws, "Q49")},
            }
        finally:
            workbook.close()

    def _fallback_running_units_from_capacity(self, capacity: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        output: Dict[str, List[Dict[str, Any]]] = {"west": [], "east": []}
        titles = capacity.get("titles", {}) if isinstance(capacity, dict) else {}
        for zone in ("west", "east"):
            for title in titles.get(zone, []) if isinstance(titles.get(zone, []), list) else []:
                text = _text(title)
                match = re.search(r"([1-6])\D*制冷单元[→:：-]*\s*([^，；\s]+)", text)
                if not match:
                    continue
                mode_text = _text(match.group(2))
                if mode_text in RUNNING_MODE_TEXTS:
                    output[zone].append({"unit": int(match.group(1)), "mode_text": mode_text})
        return output

    def _build_handover_content_lines(
        self,
        *,
        building: str,
        output: Dict[str, Any],
        source: Dict[str, Any],
        capacity: Dict[str, Any],
    ) -> List[str]:
        running_units = source.get("running_units", {}) if isinstance(source.get("running_units", {}), dict) else {}
        if not running_units.get("west") and not running_units.get("east"):
            running_units = self._fallback_running_units_from_capacity(capacity)
        planned = _format_number_text(output.get("planned_cabinets"))
        powered = _format_number_text(output.get("powered_cabinets"))
        unpowered = _format_number_text(output.get("unpowered_cabinets"))
        cabinet_text = (
            f"{building}已上电机柜{powered}个，未上电机柜{unpowered}个；"
            if powered and unpowered
            else f"{building}机柜上电信息以审核表为准；"
        )
        if planned and powered and not unpowered:
            cabinet_text = f"{building}规划机柜{planned}个，已上电机柜{powered}个；"
        code = _extract_building_code(building)
        switchgear = f"{code}-144、{code}-120变电所" if code else "变电所"
        return [
            f"1、{building}机房楼由双路市电带载运行，{switchgear}内10KV中压母联开关为“热备用”状态，投退方式为“自投自复”状态，综保状态正常；",
            f"2、{self._build_chiller_zone_line('A', 'west', running_units, source, capacity)}",
            f"3、{self._build_chiller_zone_line('B', 'east', running_units, source, capacity)}",
            f"4、{cabinet_text}",
            "5、变更，事件周汇总，当日有I2及以上告警需填写并发送H楼；",
            "6、每晚24点进行超功率机柜统计，每月进行超功率汇总统计；",
            "7、室外温湿度变化较大，注意多观察机房温湿度，及时调整空调及恒湿机，及时调整运行模式；",
        ]

    def _build_chiller_zone_line(
        self,
        zone_label: str,
        zone_key: str,
        running_units: Dict[str, List[Dict[str, Any]]],
        source: Dict[str, Any],
        capacity: Dict[str, Any],
    ) -> str:
        units = list(running_units.get(zone_key, [])) if isinstance(running_units.get(zone_key, []), list) else []
        running_count = len(units)
        backup_count = max(0, 3 - running_count)
        parts = [
            f"冷冻站{zone_label}区3套制冷单元{running_count}用{backup_count}备",
            "群控模式为开启状态",
            "备用机组与备用二次泵状态正常可用",
        ]
        query = CapacitySourceQuery(
            source.get("rows", []) if isinstance(source.get("rows", []), list) else [],
            building=_text(source.get("building")),
        )
        for item in units:
            unit = int(item.get("unit", 0) or 0)
            mode_text = _text(item.get("mode_text"))
            if unit <= 0 or not mode_text:
                continue
            unit_text = f"{unit}#制冷单元{mode_text}模式运行正常"
            tower_level = query.first_text_by_d_aliases(TOWER_LEVEL_ALIASES, zone=zone_key, unit=unit, allow_global=False)
            if tower_level:
                unit_text += f"，{unit}#冷却塔液位{_format_with_unit(tower_level, 'm')}正常"
            parts.append(unit_text)
        if units:
            pump_units = "、".join(f"{int(item.get('unit', 0) or 0)}#" for item in units if int(item.get("unit", 0) or 0) > 0)
            if pump_units:
                parts.append(f"{pump_units}二次泵运行正常")
        tank_temp = query.first_text_by_d_aliases(TANK_TEMP_ALIASES, zone=zone_key, allow_global=True)
        tank_level = query.first_text_by_d_aliases(TANK_LEVEL_ALIASES, zone=zone_key, allow_global=True)
        if not tank_level and isinstance(capacity.get("tank_level", {}), dict):
            tank_level = _text(capacity.get("tank_level", {}).get(zone_key))
        tank_parts: List[str] = []
        if tank_temp:
            tank_parts.append(f"蓄冷罐后备温度{_format_with_unit(tank_temp, '℃')}正常")
        if tank_level:
            tank_parts.append(f"液位{_format_with_unit(tank_level, 'm')}正常")
        if tank_parts:
            parts.append("、".join(tank_parts))
        return "，".join(parts) + "；"

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
                formula = f"CurrentValue.[{CONTACT_NAME_FIELD}]={_formula_literal(name)}"
                records = client.list_records(
                    table_id=CONTACT_TABLE_ID,
                    page_size=20,
                    max_records=20,
                    filter_formula=formula,
                )
                phone = ""
                for record in records:
                    fields = record.get("fields", {}) if isinstance(record, dict) else {}
                    if not isinstance(fields, dict):
                        continue
                    phone = _field_text(fields.get(CONTACT_PHONE_FIELD))
                    if phone:
                        break
                self._contact_cache[name] = phone
                if not phone:
                    emit_log(f"[交接班][审核链接摘要] 未找到联系方式 name={name}")
            except Exception as exc:  # noqa: BLE001
                self._contact_cache[name] = ""
                emit_log(f"[交接班][审核链接摘要] 联系方式查询失败 name={name}, error={exc}")
        for name in names:
            output[name] = self._contact_cache.get(name, "")
        return output

    @staticmethod
    def _phones_for_names(names: List[str], phone_map: Dict[str, str]) -> str:
        return "、".join(phone_map.get(name, "") for name in names if phone_map.get(name, ""))
