from __future__ import annotations

import argparse
import calendar
import csv
import os
import re
import shutil
import sys
from collections import Counter, defaultdict
from copy import copy
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter


FEISHU_TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
FEISHU_API = "https://open.feishu.cn/open-apis"

BUILDINGS = ["A", "B", "C", "D", "E"]
INCLUDED_ALARM_LEVELS = {"紧急", "严重", "重要", "次要"}
EXPECTED_DETAIL_COLUMNS = {
    "变更": "P",
    "维护": "Q",
    "检修": "R",
    "设备轮巡": "S",
    "设备调整": "T",
    "低负荷": "U",
    "上下电": "V",
}
EXPECTED_ALIASES = {
    "设备检修": "检修",
    "设备轮询": "设备轮巡",
}
UNEXPECTED_TYPES = ["设备故障", "人为误操作", "通讯故障", "超电", "环境参数异常", "未知原因"]
UNEXPECTED_COLUMNS = {
    "设备故障": "X",
    "人为误操作": "Y",
    "通讯故障": "Z",
    "超电": "AA",
    "环境参数异常": "AB",
    "未知原因": "AC",
}
BATTERY_MEASUREMENT_RE = re.compile(
    r"^(?:(?:\d+_)|最高|最低)?(?P<metric>电池内阻|电池电压|电池温度)(?:值)?"
    r"(?P<unit>_uΩ|_V|_℃)?[:：]\s*过(?P<direction>高|低)报警$"
)
CURRENT_VALUE_RE = re.compile(
    r"[，,；;、]?\s*(?:当前值|触发值)\s*(?:为)?\s*[:：]?\s*[-+]?\d+(?:\.\d+)?\s*"
    r"(?:KW|KVA|A|V|%|℃|度)?",
    flags=re.I,
)

ALIASES = {
    "level": ["等级", "告警等级"],
    "alarm": ["告警内容", "告警名称", "告警标题", "告警"],
    "location": ["位置", "设备位置", "告警位置", "区域位置"],
    "object": ["监控对象", "设备名称", "对象", "告警对象"],
    "start_time": ["产生时间", "发生时间", "告警产生时间", "告警时间"],
    "accept_time": ["受理时间", "处理时间", "响应时间"],
    "process_status": ["处理状态"],
    "handler": ["受理人", "处理人"],
    "accept_desc": ["受理描述", "处理描述"],
    "recover_time": ["恢复时间", "告警恢复时间"],
    "recover_status": ["恢复状态"],
    "suggestion": ["处理建议"],
    "alarm_type": ["告警类型", "类型"],
    "trigger_value": ["触发值", "当前值"],
    "alarm_class": ["告警分类", "分类"],
    "confirm_time": ["确认时间"],
    "confirm_user": ["确认人"],
    "confirm_desc": ["确认描述"],
    "building": ["楼栋", "机楼", "楼宇"],
    "room": ["房间", "机房", "区域", "房间位置"],
}


@dataclass
class AlarmRecord:
    raw: dict[str, Any]
    building: str
    source_category: str = ""
    level: str = ""
    alarm: str = ""
    location_parts: list[str] | None = None
    object_name: str = ""
    start_time: datetime | None = None
    accept_time: datetime | None = None
    process_status: str = ""
    handler: str = ""
    accept_desc: str = ""
    recover_time: datetime | None = None
    recover_status: str = ""
    suggestion: str = ""
    alarm_type: str = ""
    trigger_value: str = ""
    alarm_class: str = ""
    confirm_time: datetime | None = None
    confirm_user: str = ""
    confirm_desc: str = ""

    @property
    def category(self) -> str:
        return normalize_category(
            extract_bracket_type(self.confirm_desc) or extract_bracket_type(self.accept_desc) or self.source_category
        )

    @property
    def is_recovered(self) -> bool:
        return self.recover_status == "已恢复" or self.recover_time is not None

    @property
    def summary_text(self) -> str:
        return top_summary_display(self.object_name, self.alarm, self.confirm_desc or self.accept_desc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate EA118 monthly alarm analysis workbook.")
    parser.add_argument("--template", required=True, help="Template workbook, e.g. March alarm analysis xlsx.")
    parser.add_argument("--month", required=True, help="Month to include, YYYY-MM.")
    parser.add_argument("--out", required=True, help="Output workbook path.")
    parser.add_argument("--feishu-url", help="Feishu wiki/bitable URL.")
    parser.add_argument("--source-export", help="Optional exported xlsx/csv source. Used instead of Feishu API.")
    parser.add_argument("--report-date", help="Date written to 原始数据!A4:A8, YYYY-MM-DD. Default: third day of next month.")
    parser.add_argument("--as-of", help="Cutoff for unrecovered >48h, YYYY-MM-DD or YYYY-MM-DD HH:MM:SS. Default: now.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    month_start, month_end = month_bounds(args.month)
    report_date = parse_cli_dt(args.report_date) if args.report_date else default_report_date(month_start)
    as_of = parse_cli_dt(args.as_of) if args.as_of else datetime.now()

    generate_alarm_report(
        template=Path(args.template),
        month=args.month,
        out=Path(args.out),
        feishu_url=args.feishu_url,
        source_export=Path(args.source_export) if args.source_export else None,
        report_date=report_date,
        as_of=as_of,
    )
    print(f"Wrote {args.out}")
    return 0


def generate_alarm_report(
    template: Path,
    month: str,
    out: Path,
    feishu_url: str | None = None,
    source_export: Path | None = None,
    report_date: datetime | None = None,
    as_of: datetime | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    month_start, month_end = month_bounds(month)
    report_date = report_date or default_report_date(month_start)
    as_of = as_of or datetime.now()

    if source_export:
        records = load_export_records(source_export)
    else:
        if not feishu_url:
            raise RuntimeError("Either feishu_url or source_export is required.")
        records = fetch_feishu_records(feishu_url, session=session)

    all_alarms = [to_alarm_record(r) for r in records]
    candidate_alarms = [
        r
        for r in all_alarms
        if r.building in BUILDINGS and r.start_time and month_start <= r.start_time < month_end
    ]
    excluded_level_counts = Counter(normalize_alarm_level(r.level) for r in candidate_alarms if is_excluded_alarm_level(r))
    excluded_non_target_level_count = sum(excluded_level_counts.values())
    alarms = [r for r in candidate_alarms if is_included_alarm_level(r)]
    alarms.sort(key=lambda r: r.start_time or datetime.min, reverse=True)

    if not alarms:
        if excluded_non_target_level_count:
            raise RuntimeError(f"{month} 没有可分析告警，已排除非目标等级记录 {excluded_non_target_level_count} 条。")
        raise RuntimeError(f"No alarm records found for {month}.")

    out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(template, out)

    wb = load_workbook(out)
    fill_detail_sheets(wb, alarms)
    ensure_summary_layout(wb)
    fill_summary_sheet_from_workbook(wb, report_date, as_of)
    clean_summary_unused_area(wb)
    force_recalculate(wb)
    wb.save(out)
    wb.close()
    return {
        "output_path": out,
        "alarm_count": len(alarms),
        "record_count": len(records),
        "candidate_count": len(candidate_alarms),
        "excluded_non_target_level_count": excluded_non_target_level_count,
        "excluded_level_counts": dict(excluded_level_counts),
        "excluded_warning_count": excluded_level_counts.get("预警", 0),
    }


def month_bounds(month: str) -> tuple[datetime, datetime]:
    year, mon = map(int, month.split("-"))
    return datetime(year, mon, 1), datetime(year + (mon == 12), 1 if mon == 12 else mon + 1, 1)


def default_report_date(month_start: datetime) -> datetime:
    year, mon = month_start.year, month_start.month
    last = calendar.monthrange(year, mon)[1]
    return month_start.replace(day=last) + timedelta(days=3)


def parse_cli_dt(value: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported date/time: {value}")


def feishu_session() -> requests.Session:
    app_id = os.environ.get("FEISHU_APP_ID")
    app_secret = os.environ.get("FEISHU_APP_SECRET")
    if not app_id or not app_secret:
        raise RuntimeError("Missing FEISHU_APP_ID or FEISHU_APP_SECRET.")

    session = requests.Session()
    resp = session.post(FEISHU_TOKEN_URL, json={"app_id": app_id, "app_secret": app_secret}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"Failed to get tenant token: {data}")
    session.headers.update({"Authorization": f"Bearer {data['tenant_access_token']}"})
    return session


def fetch_feishu_records(feishu_url: str, session: requests.Session | None = None) -> list[dict[str, Any]]:
    parsed = urlparse(feishu_url)
    wiki_token = parsed.path.rstrip("/").split("/")[-1]
    query = parse_qs(parsed.query)
    table_id = one(query, "table")
    view_id = one(query, "view")
    if not wiki_token or not table_id:
        raise RuntimeError("The Feishu URL must include a wiki token and table=... parameter.")

    session = session or feishu_session()
    node_resp = session.get(f"{FEISHU_API}/wiki/v2/spaces/get_node", params={"token": wiki_token}, timeout=30)
    if node_resp.status_code != 200 or node_resp.json().get("code") != 0:
        raise RuntimeError(
            "Cannot resolve the Feishu wiki node. Add the app behind FEISHU_APP_ID as a collaborator "
            f"with read permission, then retry. Response: {node_resp.text[:500]}"
        )
    node = node_resp.json()["data"]["node"]
    app_token = node.get("obj_token")
    if not app_token:
        raise RuntimeError(f"Wiki node did not expose a bitable app token: {node}")

    records: list[dict[str, Any]] = []
    page_token = None
    while True:
        params = {"page_size": 500}
        if view_id:
            params["view_id"] = view_id
        if page_token:
            params["page_token"] = page_token
        resp = session.get(
            f"{FEISHU_API}/bitable/v1/apps/{app_token}/tables/{table_id}/records",
            params=params,
            timeout=60,
        )
        if resp.status_code != 200 or resp.json().get("code") != 0:
            raise RuntimeError(
                "Cannot read Feishu bitable records. Confirm the app has bitable read permission "
                f"and access to this base/table. Response: {resp.text[:500]}"
            )
        payload = resp.json()["data"]
        for item in payload.get("items", []):
            fields = {k: flatten_feishu_value(v) for k, v in item.get("fields", {}).items()}
            if has_non_empty_source_data(fields):
                records.append(fields)
        if not payload.get("has_more"):
            break
        page_token = payload.get("page_token")
    return records


def has_non_empty_source_data(fields: dict[str, Any]) -> bool:
    if "分类" in fields:
        return is_non_empty(fields.get("分类"))
    return True


def is_non_empty(value: Any) -> bool:
    if value in (None, ""):
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def one(query: dict[str, list[str]], key: str) -> str:
    return query.get(key, [""])[0]


def flatten_feishu_value(value: Any) -> Any:
    if isinstance(value, list):
        parts = [flatten_feishu_value(v) for v in value]
        return "\n".join(str(v) for v in parts if v not in (None, ""))
    if isinstance(value, dict):
        for key in ("text", "name", "en_name", "value"):
            if key in value:
                return flatten_feishu_value(value[key])
        return " ".join(str(flatten_feishu_value(v)) for v in value.values() if v not in (None, ""))
    return value


def load_export_records(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [dict(row) for row in csv.DictReader(f)]
    if suffix not in (".xlsx", ".xlsm"):
        raise RuntimeError(f"Unsupported source export: {path}")

    wb = load_workbook(path, data_only=True)
    rows: list[dict[str, Any]] = []
    skip_sheets = {"数据分析", "原始数据"}
    for ws in wb.worksheets:
        if ws.title in skip_sheets or ws.max_row < 2:
            continue
        headers = [normalize_header(ws.cell(1, c).value) for c in range(1, ws.max_column + 1)]
        if "告警内容" not in headers or "产生时间" not in headers:
            continue
        for r in range(2, ws.max_row + 1):
            if ws.cell(r, 1).value in (None, ""):
                continue
            record: dict[str, Any] = {}
            for c, header in enumerate(headers, start=1):
                if header:
                    record[header] = ws.cell(r, c).value
            if ws.title in BUILDINGS:
                record.setdefault("楼栋", ws.title)
            rows.append(record)
    return rows


def normalize_header(value: Any) -> str:
    return str(value or "").replace("\n", "").strip()


def to_alarm_record(raw: dict[str, Any]) -> AlarmRecord:
    record = {normalize_header(k): v for k, v in raw.items()}
    building = infer_building(record)
    return AlarmRecord(
        raw=record,
        building=building,
        source_category=str(record.get("分类") or "").strip(),
        level=str_value(record, "level"),
        alarm=str_value(record, "alarm"),
        location_parts=extract_location_parts(record, building),
        object_name=str_value(record, "object"),
        start_time=parse_dt(value_for(record, "start_time")),
        accept_time=parse_dt(value_for(record, "accept_time")),
        process_status=str_value(record, "process_status"),
        handler=str_value(record, "handler"),
        accept_desc=str_value(record, "accept_desc"),
        recover_time=parse_dt(value_for(record, "recover_time")),
        recover_status=str_value(record, "recover_status"),
        suggestion=str_value(record, "suggestion"),
        alarm_type=str_value(record, "alarm_type"),
        trigger_value=str_value(record, "trigger_value"),
        alarm_class=str_value(record, "alarm_class"),
        confirm_time=parse_dt(value_for(record, "confirm_time")),
        confirm_user=str_value(record, "confirm_user"),
        confirm_desc=str_value(record, "confirm_desc"),
    )


def value_for(record: dict[str, Any], canonical: str) -> Any:
    for name in ALIASES[canonical]:
        if name in record and record[name] not in (None, ""):
            return record[name]
    return None


def str_value(record: dict[str, Any], canonical: str) -> str:
    value = value_for(record, canonical)
    if value is None:
        return ""
    if isinstance(value, datetime):
        return format_dt(value)
    return str(value).strip()


def normalize_alarm_level(level: str) -> str:
    return re.sub(r"\s+", "", str(level or "")).strip()


def is_included_alarm_level(alarm: AlarmRecord) -> bool:
    return normalize_alarm_level(alarm.level) in INCLUDED_ALARM_LEVELS


def is_excluded_alarm_level(alarm: AlarmRecord) -> bool:
    return not is_included_alarm_level(alarm)


def parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, (int, float)):
        # Feishu date fields commonly come back as milliseconds.
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000
        return datetime.fromtimestamp(raw)
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def format_dt(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else ""


def infer_building(record: dict[str, Any]) -> str:
    text = " ".join(str(v) for v in record.values() if v is not None)
    direct = str(value_for(record, "building") or "").upper()
    for building in BUILDINGS:
        if direct in (building, f"{building}楼"):
            return building
    for building in BUILDINGS:
        if re.search(rf"(?<![A-Z0-9]){building}楼", text, flags=re.I) or re.search(rf"(?<![A-Z0-9]){building}-", text, flags=re.I):
            return building
    return ""


def extract_location_parts(record: dict[str, Any], building: str) -> list[str]:
    candidates = []
    for name in ("位置1", "位置2", "位置3", "位置4", "位置5", "一级位置", "二级位置", "三级位置", "四级位置", "五级位置"):
        if name in record and record[name] not in (None, ""):
            candidates.append(str(record[name]).strip())
    if candidates:
        return candidates

    value = value_for(record, "location")
    if value:
        text = str(value).strip()
        for sep in (">", "/", "\\", "|"):
            if sep in text:
                parts = [p.strip() for p in text.split(sep) if p.strip()]
                return parts
        return [text]

    parts = []
    if building:
        parts.append(f"{building}楼")
    room = str_value(record, "room")
    if room and room not in parts:
        parts.append(room)
    return parts


def fill_detail_sheets(wb, alarms: list[AlarmRecord]) -> None:
    by_building: dict[str, list[AlarmRecord]] = defaultdict(list)
    for alarm in alarms:
        by_building[alarm.building].append(alarm)

    for building in BUILDINGS:
        ws = wb[building]
        template = capture_row_template(ws, 2, ws.max_column)
        if ws.max_row > 1:
            ws.delete_rows(2, ws.max_row - 1)
        for idx, alarm in enumerate(by_building.get(building, []), start=1):
            row = idx + 1
            apply_row_template(ws, row, template)
            values, duration_col = detail_row_values(alarm, idx, building, ws.max_column)
            for col, value in values.items():
                ws.cell(row, col).value = value
            ws.cell(row, duration_col).value = duration_formula(row, building)


def capture_row_template(ws, row: int, max_col: int) -> dict[str, Any]:
    return {
        "height": ws.row_dimensions[row].height,
        "cells": [
            {
                "style": copy(ws.cell(row, col)._style),
            }
            for col in range(1, max_col + 1)
        ],
    }


def apply_row_template(ws, row: int, template: dict[str, Any]) -> None:
    ws.row_dimensions[row].height = template["height"]
    for col, cell_template in enumerate(template["cells"], start=1):
        cell = ws.cell(row, col)
        cell._style = copy(cell_template["style"])


def detail_row_values(alarm: AlarmRecord, seq: int, building: str, max_col: int) -> tuple[dict[int, Any], int]:
    parts = (alarm.location_parts or [])[:]
    accept_desc = alarm.accept_desc or (f"【{alarm.source_category}】" if alarm.source_category else "")
    confirm_desc = alarm.confirm_desc or accept_desc
    if building == "E":
        parts = pad(parts, 4)
        values = {
            1: seq,
            2: alarm.level,
            3: alarm.alarm,
            4: parts[0],
            5: parts[1],
            6: parts[2],
            7: parts[3],
            8: alarm.object_name,
            9: format_dt(alarm.start_time),
            10: format_dt(alarm.accept_time),
            11: alarm.process_status,
            12: alarm.handler,
            13: accept_desc,
            14: format_dt(alarm.recover_time),
            15: alarm.recover_status or ("已恢复" if alarm.recover_time else "未恢复"),
            16: alarm.suggestion,
            17: alarm.alarm_type,
            18: alarm.trigger_value,
            19: alarm.alarm_class,
            20: format_dt(alarm.confirm_time),
            21: alarm.confirm_user,
            22: confirm_desc,
        }
        return values, 29

    parts = pad(parts, 5)
    values = {
        1: seq,
        2: alarm.level,
        3: alarm.alarm,
        4: parts[0],
        5: parts[1],
        6: parts[2],
        7: parts[3],
        8: parts[4],
        9: alarm.object_name,
        10: format_dt(alarm.start_time),
        11: format_dt(alarm.accept_time),
        12: alarm.process_status,
        13: alarm.handler,
        14: accept_desc,
        15: format_dt(alarm.recover_time),
        16: alarm.recover_status or ("已恢复" if alarm.recover_time else "未恢复"),
        17: alarm.suggestion,
        18: alarm.alarm_type,
        19: alarm.trigger_value,
        20: alarm.alarm_class,
        21: format_dt(alarm.confirm_time),
        22: alarm.confirm_user,
        23: confirm_desc,
    }
    return values, 31 if building == "B" else 29


def pad(values: list[str], length: int) -> list[str]:
    return (values + [""] * length)[:length]


def duration_formula(row: int, building: str) -> str:
    if building == "E":
        return f"=N{row}-I{row}"
    return f"=O{row}-J{row}"


RECOVERED_STATUS = "\u5df2\u6062\u590d"
SUMMARY_SHEET = "\u539f\u59cb\u6570\u636e"
DETAIL_CONFIG = {
    "A": {"desc": 14, "recover_status": 16, "start": 10, "recover_time": 15, "object": 9, "alarm": 3, "confirm_desc": 23},
    "B": {"desc": 14, "recover_status": 16, "start": 10, "recover_time": 15, "object": 9, "alarm": 3, "confirm_desc": 23},
    "C": {"desc": 14, "recover_status": 16, "start": 10, "recover_time": 15, "object": 9, "alarm": 3, "confirm_desc": 23},
    "D": {"desc": 14, "recover_status": 16, "start": 10, "recover_time": 15, "object": 9, "alarm": 3, "confirm_desc": 23},
    "E": {"desc": 13, "recover_status": 15, "start": 9, "recover_time": 14, "object": 8, "alarm": 3, "confirm_desc": 22},
}
TOP_COLUMNS = [("AD", "AE", "AF"), ("AG", "AH", "AI"), ("AJ", "AK", "AL")]


def ensure_summary_layout(wb) -> None:
    ws = wb[SUMMARY_SHEET]
    for merged_range in list(ws.merged_cells.ranges):
        ws.unmerge_cells(str(merged_range))

    header_to_col = {
        normalize_header(ws.cell(3, col).value): col
        for col in range(1, ws.max_column + 1)
        if normalize_header(ws.cell(3, col).value)
    }
    if "低负荷" not in header_to_col:
        insert_at = header_to_col.get("上下电", 21)
        ws.insert_cols(insert_at)
        for row in range(1, ws.max_row + 1):
            copy_cell_style(ws.cell(row, insert_at + 1), ws.cell(row, insert_at))
        ws.column_dimensions[get_column_letter(insert_at)].width = ws.column_dimensions[
            get_column_letter(insert_at + 1)
        ].width

    set_summary_headers(ws)
    update_analysis_formulas(wb)


def copy_cell_style(src, dst) -> None:
    if src.has_style:
        dst._style = copy(src._style)
    dst.number_format = src.number_format
    dst.font = copy(src.font)
    dst.fill = copy(src.fill)
    dst.border = copy(src.border)
    dst.alignment = copy(src.alignment)
    dst.protection = copy(src.protection)


def set_summary_headers(ws) -> None:
    merge_ranges = [
        "F1:N1",
        "F2:N2",
        "O1:V1",
        "O2:V2",
        "W1:AC1",
        "W2:AC2",
        "AD1:AF1",
        "AG1:AI1",
        "AJ1:AL1",
    ]
    for cell_range in merge_ranges:
        ws.merge_cells(cell_range)

    ws["O1"] = "预期内告警数量"
    ws["W1"] = "预期外告警数量"
    for cell in ("AD1", "AG1", "AJ1"):
        ws[cell] = "预期外告警排序和明细"
    for cell in ("AE2", "AH2", "AK2"):
        ws[cell] = "包含告警标题、告警原因、当前状态、异常备注。"

    headers = {
        "O3": "预期内总数",
        "P3": "变更",
        "Q3": "维护",
        "R3": "检修",
        "S3": "设备轮巡",
        "T3": "设备调整",
        "U3": "低负荷",
        "V3": "上下电",
        "W3": "预期外总数",
        "X3": "设备故障",
        "Y3": "人为误操作",
        "Z3": "通讯故障",
        "AA3": "超电",
        "AB3": "环境参数\n异常",
        "AC3": "未知原因",
        "AD3": "预期外告警数第一告警类型",
        "AE3": "预期外告警数量第一告警内容",
        "AF3": "预期外告警数量第一告警数量",
        "AG3": "预期外告警数第二告警类型",
        "AH3": "预期外告警数量第二告警内容",
        "AI3": "预期外告警数量第二告警数量",
        "AJ3": "预期外告警数第三告警类型",
        "AK3": "预期外告警数量第三告警内容",
        "AL3": "预期外告警数量第三告警数量",
    }
    for cell, value in headers.items():
        ws[cell] = value


def update_analysis_formulas(wb) -> None:
    ws = wb["数据分析"]
    for row, building in enumerate(BUILDINGS, start=4):
        status_col = "O" if building == "E" else "P"
        ws[f"B{row}"] = f"=COUNTA('{building}'!$A:$A)-1"
        ws[f"C{row}"] = f'=COUNTIF(\'{building}\'!${status_col}:${status_col},"已恢复")'
        ws[f"D{row}"] = f"=B{row}-C{row}"
        ws[f"E{row}"] = f"=IFERROR(D{row}/B{row},0)"
    ws["B3"] = "=SUM(B4:B8)"
    ws["C3"] = "=SUM(C4:C8)"
    ws["D3"] = "=SUM(D4:D8)"
    ws["E3"] = "=IFERROR(D3/B3,0)"

    for row in range(27, 32):
        source_row = row - 23
        ws[f"B{row}"] = f"=原始数据!AF{source_row}"
        ws[f"C{row}"] = f"=原始数据!AD{source_row}"
        ws[f"D{row}"] = f"=原始数据!AI{source_row}"
        ws[f"E{row}"] = f"=原始数据!AG{source_row}"
        ws[f"F{row}"] = f"=原始数据!AL{source_row}"
        ws[f"G{row}"] = f"=原始数据!AJ{source_row}"
        ws[f"H{row}"] = f"=B{row}/(B{row}+D{row}+F{row})"


def clean_summary_unused_area(wb) -> None:
    ws = wb[SUMMARY_SHEET]
    if ws.max_row > 8:
        ws.delete_rows(9, ws.max_row - 8)


def fill_summary_sheet_from_workbook(wb, report_date: datetime, as_of: datetime) -> None:
    ws = wb[SUMMARY_SHEET]
    for row, building in enumerate(BUILDINGS, start=4):
        detail_rows = read_detail_rows(wb[building], building)
        counts = summarize_detail_rows(detail_rows, as_of)
        values = {
            "A": report_date,
            "B": "EA118",
            "C": building,
            "D": counts["total"],
            "E": counts["recovered"],
            "F": counts["duration"]["0-5"],
            "G": counts["duration"]["5-30"],
            "H": counts["duration"]["30-60"],
            "I": counts["duration"]["1-6h"],
            "J": counts["duration"]["6-48h"],
            "K": counts["duration"][">48h"],
            "L": counts["unrecovered_total"],
            "M": format_counter(counts["unexpected_0_5"], parens="full"),
            "N": format_counter(counts["unrecovered_by_type"], parens="ascii"),
            "O": counts["total"] - counts["unexpected_total"],
            "W": counts["unexpected_total"],
        }
        for category, col in EXPECTED_DETAIL_COLUMNS.items():
            values[col] = counts["expected_detail"][category]
        for category, col in UNEXPECTED_COLUMNS.items():
            values[col] = counts["unexpected_detail"][category]

        top = counts["top_unexpected"]
        for offset, (type_col, text_col, count_col) in enumerate(TOP_COLUMNS):
            if offset < len(top):
                (category, text), count = top[offset]
                values[type_col] = display_category(category)
                values[text_col] = text
                values[count_col] = count
            else:
                values[type_col] = ""
                values[text_col] = ""
                values[count_col] = 0

        for col, value in values.items():
            ws[f"{col}{row}"].value = value


def read_detail_rows(ws, building: str) -> list[dict[str, Any]]:
    cfg = DETAIL_CONFIG[building]
    rows: list[dict[str, Any]] = []
    for row in range(2, ws.max_row + 1):
        if ws.cell(row, 1).value in (None, ""):
            continue
        accept_desc = string_cell(ws, row, cfg["desc"])
        confirm_desc = string_cell(ws, row, cfg["confirm_desc"])
        rows.append(
            {
                "category": normalize_summary_category(extract_bracket_type(confirm_desc) or extract_bracket_type(accept_desc)),
                "accept_desc": accept_desc,
                "confirm_desc": confirm_desc,
                "recovered": string_cell(ws, row, cfg["recover_status"]) == RECOVERED_STATUS,
                "start": parse_dt(ws.cell(row, cfg["start"]).value),
                "recover_time": parse_dt(ws.cell(row, cfg["recover_time"]).value),
                "object": string_cell(ws, row, cfg["object"]),
                "alarm": string_cell(ws, row, cfg["alarm"]),
            }
        )
    return rows


def string_cell(ws, row: int, col: int) -> str:
    value = ws.cell(row, col).value
    return "" if value is None else str(value).strip()


def summarize_detail_rows(rows: list[dict[str, Any]], as_of: datetime) -> dict[str, Any]:
    duration = Counter()
    expected_detail = Counter()
    unexpected_detail = Counter()
    unexpected_0_5 = Counter()
    unrecovered_by_type = Counter()
    top_unexpected = Counter()
    top_unexpected_display: dict[tuple[str, str], str] = {}
    recovered = 0
    unrecovered_total = 0

    for row in rows:
        category = row["category"]
        is_unexpected = category in UNEXPECTED_TYPES
        if is_unexpected:
            unexpected_detail[category] += 1
            key_text = top_summary_key(row["object"], row["alarm"], row["confirm_desc"] or row["accept_desc"])
            top_key = (category, key_text)
            top_unexpected[top_key] += 1
            top_unexpected_display.setdefault(
                top_key,
                top_summary_display(row["object"], row["alarm"], row["confirm_desc"] or row["accept_desc"]),
            )
        else:
            expected_detail[EXPECTED_ALIASES.get(category, category)] += 1

        bucket = recovery_bucket(row["start"], row["recover_time"])
        if row["recovered"]:
            recovered += 1
            if bucket:
                duration[bucket] += 1
            if is_unexpected and bucket == "0-5":
                unexpected_0_5[category] += 1
        else:
            unrecovered_total += 1
            if category:
                unrecovered_by_type[EXPECTED_ALIASES.get(category, category)] += 1

    return {
        "total": len(rows),
        "recovered": recovered,
        "unrecovered_total": unrecovered_total,
        "duration": duration,
        "expected_detail": expected_detail,
        "unexpected_total": sum(unexpected_detail.values()),
        "unexpected_detail": unexpected_detail,
        "unexpected_0_5": unexpected_0_5,
        "unrecovered_by_type": unrecovered_by_type,
        "top_unexpected": [((category, top_unexpected_display.get((category, key), key)), count) for (category, key), count in top_unexpected.most_common(3)],
    }


def normalize_summary_category(category: str) -> str:
    clean = normalize_category(category)
    if clean in {"PDU\u6545\u969c\u9884\u8b66", "\u8bbe\u5907\u5f02\u5e38"}:
        return "\u8bbe\u5907\u6545\u969c"
    if clean == "\u7ef4\u4fdd":
        return "\u7ef4\u62a4"
    if clean == "\u901a\u4fe1\u6545\u969c":
        return "\u901a\u8baf\u6545\u969c"
    return clean


def top_summary_text(row: dict[str, Any]) -> str:
    return top_summary_display(row["object"], row["alarm"], row["confirm_desc"] or row["accept_desc"])


def top_summary_key(object_name: str, alarm: str, desc: str = "") -> str:
    key, _ = top_summary_group(object_name, alarm, desc)
    return key


def top_summary_display(object_name: str, alarm: str, desc: str) -> str:
    _, label = top_summary_group(object_name, alarm, desc)
    return label


def top_summary_group(object_name: str, alarm: str, desc: str = "") -> tuple[str, str]:
    clean_desc = normalize_top_desc(strip_bracket_prefix(desc))
    alarm_key, alarm_label, family = normalize_top_alarm_parts(alarm, clean_desc)
    object_key, object_label = normalize_top_object(object_name, family)
    key = "\0".join((normalize_top_key_part(object_key), normalize_top_key_part(alarm_key)))
    label = join_top_summary(object_label, alarm_label, clean_desc)
    return key, label


def normalize_top_key_part(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "").strip()).upper()


def normalize_top_alarm_parts(text: str, desc: str = "") -> tuple[str, str, str]:
    clean = normalize_top_alarm_text(text)
    if not clean:
        return "", "", "default"

    if "整流模块故障" in desc or ("整流模块" in clean and "故障" in clean):
        return "整流模块故障状态: 告警", "整流模块故障状态: 告警", "rectifier"

    generator = re.search(r"(\d+)#机", clean)
    if generator and "通讯故障" in desc:
        label = f"{generator.group(1)}#柴发不在自动状态等多个告警"
        return label, label, "generator"

    battery = BATTERY_MEASUREMENT_RE.match(clean)
    if battery:
        metric = battery.group("metric")
        unit = battery.group("unit") or ""
        direction = battery.group("direction")
        family_direction = "" if metric == "电池内阻" else direction
        family = f"battery:{metric}:{family_direction}"
        if metric == "电池温度":
            label = f"多个电池温度过{direction}报警"
        else:
            label = f"{metric}{unit}: 过{direction}报警"
        return family, label, family

    return clean, clean, "default"


def normalize_top_alarm_text(text: str) -> str:
    clean = str(text or "").strip()
    if not clean:
        return ""
    clean = CURRENT_VALUE_RE.sub("", clean)
    clean = re.sub(r"\s+", " ", clean).strip(" ，,；;、")
    return clean


def normalize_top_desc(text: str) -> str:
    clean = str(text or "").strip()
    if not clean:
        return ""
    clean = re.sub(r"\s+", " ", clean)
    clean = CURRENT_VALUE_RE.sub("", clean)
    return clean.strip(" ，,；;、")


def normalize_top_object(object_name: str, family: str) -> tuple[str, str]:
    clean = str(object_name or "").strip()
    if family == "generator":
        return clean, ""
    if family.startswith("battery:电池温度:"):
        return clean, clean
    if family.startswith("battery:"):
        return normalize_battery_object(clean)
    return clean, clean


def normalize_battery_object(object_name: str) -> tuple[str, str]:
    clean = object_name.strip()
    if re.search(r"_电池组\d+$", clean):
        clean = re.sub(r"_电池组\d+$", "", clean)
    elif re.search(r"电池组\d+$", clean):
        clean = re.sub(r"电池组\d+$", "电池组", clean)
    else:
        clean = re.sub(r"-\d+组电池监控$", "", clean)
        clean = re.sub(r"-\d+组$", "", clean)
    return clean, clean


def join_top_summary(object_name: str, alarm: str, desc: str) -> str:
    head = f"{object_name}{alarm}".strip()
    if desc:
        return f"{head}\uff0c{desc}".strip("\uff0c")
    return head


def fill_summary_sheet(wb, alarms: list[AlarmRecord], report_date: datetime, as_of: datetime) -> None:
    ws = wb["原始数据"]
    by_building: dict[str, list[AlarmRecord]] = defaultdict(list)
    for alarm in alarms:
        by_building[alarm.building].append(alarm)

    for row, building in enumerate(BUILDINGS, start=4):
        rows = by_building.get(building, [])
        counts = summarize_building(rows, as_of)
        values = {
            "A": report_date,
            "B": "EA118",
            "C": building,
            "D": len(rows),
            "E": counts["recovered"],
            "F": counts["duration"]["0-5"],
            "G": counts["duration"]["5-30"],
            "H": counts["duration"]["30-60"],
            "I": counts["duration"]["1-6h"],
            "J": counts["duration"]["6-48h"],
            "K": counts["duration"][">48h"],
            "L": counts["unrecovered_48h"],
            "M": format_counter(counts["unexpected_0_5"], parens="full"),
            "N": format_counter(counts["unrecovered_by_type"], parens="ascii"),
            "O": counts["expected_total"],
            "W": counts["unexpected_total"],
        }
        for category, col in EXPECTED_DETAIL_COLUMNS.items():
            values[col] = counts["expected_detail"][category]
        for category, col in UNEXPECTED_COLUMNS.items():
            values[col] = counts["unexpected_detail"][category]

        top = counts["top_unexpected"]
        for offset, item in enumerate(top[:3]):
            type_col, text_col, count_col = TOP_COLUMNS[offset]
            values[type_col] = display_category(item[0][0])
            values[text_col] = item[0][1]
            values[count_col] = item[1]
        for offset in range(len(top), 3):
            type_col, text_col, count_col = TOP_COLUMNS[offset]
            values[type_col] = ""
            values[text_col] = ""
            values[count_col] = 0

        for col, value in values.items():
            ws[f"{col}{row}"].value = value


def summarize_building(rows: list[AlarmRecord], as_of: datetime) -> dict[str, Any]:
    duration = Counter()
    expected_detail = Counter()
    unexpected_detail = Counter()
    unexpected_0_5 = Counter()
    unrecovered_by_type = Counter()
    top_unexpected = Counter()
    top_unexpected_display: dict[tuple[str, str], str] = {}
    recovered = 0
    unrecovered_48h = 0
    expected_total = 0
    unexpected_total = 0

    for row in rows:
        category = row.category
        if category in UNEXPECTED_TYPES:
            unexpected_total += 1
            unexpected_detail[category] += 1
            key_text = top_summary_key(row.object_name, row.alarm, row.confirm_desc or row.accept_desc)
            top_key = (category, key_text)
            top_unexpected[top_key] += 1
            top_unexpected_display.setdefault(top_key, row.summary_text)
        else:
            expected_total += 1
            expected_detail[EXPECTED_ALIASES.get(category, category)] += 1

        if row.is_recovered:
            recovered += 1
            bucket = recovery_bucket(row.start_time, row.recover_time)
            if bucket:
                duration[bucket] += 1
            if category in UNEXPECTED_TYPES and bucket == "0-5":
                unexpected_0_5[category] += 1
        else:
            if row.start_time and as_of - row.start_time >= timedelta(hours=48):
                unrecovered_48h += 1
            if category:
                unrecovered_by_type[EXPECTED_ALIASES.get(category, category)] += 1

    return {
        "duration": duration,
        "recovered": recovered,
        "unrecovered_48h": unrecovered_48h,
        "expected_total": expected_total,
        "unexpected_total": unexpected_total,
        "expected_detail": expected_detail,
        "unexpected_detail": unexpected_detail,
        "unexpected_0_5": unexpected_0_5,
        "unrecovered_by_type": unrecovered_by_type,
        "top_unexpected": [((category, top_unexpected_display.get((category, key), key)), count) for (category, key), count in top_unexpected.most_common(3)],
    }


def recovery_bucket(start: datetime | None, end: datetime | None) -> str:
    if not start or not end:
        return ""
    seconds = (end - start).total_seconds()
    if seconds < 0:
        return ""
    if seconds <= 310:
        return "0-5"
    minutes = seconds // 60
    if minutes <= 30:
        return "5-30"
    if minutes <= 60:
        return "30-60"
    if minutes <= 6 * 60:
        return "1-6h"
    if minutes <= 48 * 60:
        return "6-48h"
    return ">48h"


def extract_bracket_type(text: str) -> str:
    match = re.search(r"【([^】]+)】", text or "")
    return match.group(1).strip() if match else ""


def strip_bracket_prefix(text: str) -> str:
    return re.sub(r"^\s*【[^】]+】", "", text or "").strip()


def normalize_category(category: str) -> str:
    clean = (category or "").replace("\n", "").replace(" ", "").strip()
    return EXPECTED_ALIASES.get(clean, clean)


def display_category(category: str) -> str:
    if category == "环境参数异常":
        return "环境参数\n异常"
    return category


def display_counter_category(category: str) -> str:
    return "环境参数异常" if category == "环境参数异常" else category


def format_counter(counter: Counter, parens: str) -> str:
    if not counter:
        return ""
    if parens == "full":
        ordered_keys = [key for key in UNEXPECTED_TYPES if counter.get(key)]
        ordered_keys.extend(key for key, _ in counter.most_common() if key not in ordered_keys)
        return "\n".join(f"【{display_counter_category(k)}】（{counter[k]}）" for k in ordered_keys)
    return "\n".join(f"【{display_counter_category(k)}】({v})" for k, v in counter.items())


def force_recalculate(wb) -> None:
    try:
        wb.calculation.fullCalcOnLoad = True
        wb.calculation.forceFullCalc = True
        wb.calculation.calcMode = "auto"
    except Exception:
        pass


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
