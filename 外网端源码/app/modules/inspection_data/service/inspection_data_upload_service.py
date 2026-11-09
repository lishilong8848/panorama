from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone, time as dt_time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable

from openpyxl import load_workbook

from app.config.inspection_data_config import INSPECTION_DATA_UPLOAD_DEFAULTS as DEFAULT_CONFIG
from app.config.inspection_data_config import INSPECTION_ROUND_FIELDS
from app.config.inspection_data_config import INSPECTION_SOURCE_CONTRACTS as SOURCE_FAMILIES
from app.config.inspection_data_config import inspection_hour_bucket
from app.modules.inspection_data.service.point_correction import CORRECTED_VALUE_FIELD, corrected_samples
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient
from app.modules.inspection_data.service.point_settings import (
    load_or_restore_point_settings, load_point_settings, query_rules, retry_pending_point_config, sync_point_settings,
)
from app.shared.utils.atomic_file import atomic_write_text


CATALOG_PATH = Path(__file__).resolve().parents[1] / "data" / "inspection_catalog.json"
BACKUP_ROOT = Path(__file__).resolve().parents[4] / ".runtime" / "inspection_data" / "backups"
LOCAL_TZ = timezone(timedelta(hours=8))
ELECTRICAL_POINT_ALIASES = {
    "电压(kV)": {"平均_线电压_kV"},
    "电流(A)": {"平均_电流_A"},
    "输入电压(V)": {"主路输入UV线电压"},
    "L1温度(℃)": {"A相温度"},
    "L2温度(℃)": {"B相温度"},
    "L3温度(℃)": {"C相温度"},
    "L1负载率(%)": {"A_输出负载_%"},
    "L2负载率(%)": {"B_输出负载_%"},
    "L3负载率(%)": {"C_输出负载_%"},
    "输出电压(V)": {"直流电压_V", "直流屏_直流电压_V"},
    "输出电流(A)": {"负载总电流_A", "直流屏_负载总电流_A"},
    "功率因数": {"系统功率因数"},
}
DIESEL_PARALLEL_FIELDS = {
    "电池电压_V": "蓄电池电压(V)",
    "冷却液温度_℃": "冷却液水温(℃)",
}
POINT_DETAIL_TYPES = {"记录类型": 3, "测点ID": 1, "测点名称": 1, "测点路径": 1, "测点值": 1, "测点原始值": 1,
                      "测点类型": 1, "状态正常值": 1, "状态异常值": 1, "状态未运行值": 1, "状态判定": 1,
                      "采样时间": 5, "测点备注": 1}
POINT_TARGET_FIELDS = {"workbook": "目标巡检表", "sheet": "目标工作表", "cell": "目标单元格"}
POINT_DETAIL_TYPES.update(dict.fromkeys(POINT_TARGET_FIELDS.values(), 1))
POINT_DETAIL_TYPES.update(dict.fromkeys(INSPECTION_ROUND_FIELDS.values(), 1))
POINT_DETAIL_TYPES[CORRECTED_VALUE_FIELD] = 1
INSPECTION_BATCH_PREFIX = "INSPECTION_BATCH_V1:"


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _key(value: Any) -> str:
    return re.sub(r"\s+", "", _text(value)).replace("（", "(").replace("）", ")")


def _record_identity(fields: dict, keys: list[str]) -> tuple:
    def field_value(value):
        if isinstance(value, list) and all(isinstance(part, dict) and "text" in part for part in value):
            return "".join(str(part["text"]) for part in value) or None
        if isinstance(value, (dict, list)):
            raise ValueError("飞书返回了无法识别的巡检字段值")
        return None if value == "" else value

    return tuple(field_value(fields.get(key)) for key in keys)


def _comparison_record_identity(fields: dict, keys: list[str], numeric_fields: set[str]) -> tuple:
    values = _record_identity(fields, keys)
    result = []
    for field, value in zip(keys, values):
        if field not in numeric_fields or value is None:
            result.append(value)
            continue
        number = None
        if not isinstance(value, bool) and isinstance(value, (str, int, float, Decimal)):
            text = str(value).strip()
            if re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", text):
                try:
                    candidate = Decimal(text)
                    if candidate.is_finite():
                        number = candidate
                except InvalidOperation:
                    pass
        result.append(("number", number) if number is not None else ("invalid", value))
    return tuple(result)


def _field_value_format(fields: dict, name: str) -> str:
    if name not in fields:
        return "字段缺失"
    value = fields[name]
    if value is None:
        return "null"
    if isinstance(value, str):
        return "空字符串" if value == "" else "字符串"
    if isinstance(value, bool):
        return "布尔值"
    if isinstance(value, int):
        return "整数"
    if isinstance(value, float):
        return "小数"
    if isinstance(value, list):
        if not value:
            return "空列表"
        if all(isinstance(part, dict) and "text" in part for part in value):
            return "富文本片段列表"
        return "其他列表"
    if isinstance(value, dict):
        return "对象"
    return "其他类型"


def _record_mismatch_diagnostic(expected_rows: list[dict], actual_rows: list[dict], fields: list[str],
                                numeric_fields: set[str]) -> str:
    # Compare field distributions, not row positions: the API does not guarantee readback order.
    unsupported = object()

    def distribution(rows: list[dict], field: str):
        counts = Counter()
        formats = defaultdict(set)
        for row in rows:
            try:
                value = _comparison_record_identity(row, [field], numeric_fields)[0]
                hash(value)
            except (TypeError, ValueError):
                value = unsupported
            counts[value] += 1
            formats[value].add(_field_value_format(row, field))
        return counts, formats

    differences = []
    for field in fields:
        expected, expected_formats = distribution(expected_rows, field)
        actual, actual_formats = distribution(actual_rows, field)
        missing, extra = expected - actual, actual - expected
        if not missing and not extra:
            continue
        expected_types = sorted({kind for value in missing for kind in expected_formats[value]})
        actual_types = sorted({kind for value in extra for kind in actual_formats[value]})
        safe_name = re.sub(r"[\x00-\x1f\x7f]", "?", field)[:80]
        differences.append(f"{safe_name}: 期望格式={','.join(expected_types) or '无'}; 实际格式={','.join(actual_types) or '无'}")
        if len(differences) == 3:
            break
    return "；".join(differences) if differences else "字段组合不一致，单字段分布相同"


def _record_ids_sha256(record_ids: set[str]) -> str:
    encoded = json.dumps(sorted(record_ids), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_inspection_batch(records: list[dict], *, building: str, upload_time: int) -> dict | None:
    """Validate one complete table snapshot against an explicitly expected batch."""
    if building not in {"D楼", "E楼"} or type(upload_time) is not int or upload_time <= 0 or not isinstance(records, list):
        return None
    ids: set[str] = set()
    counts = Counter()
    markers = []
    try:
        for record in records:
            if not isinstance(record, dict) or not isinstance(record.get("fields"), dict):
                return None
            rid = record.get("record_id")
            if not isinstance(rid, str) or not rid or rid in ids:
                return None
            ids.add(rid)
            fields = record["fields"]
            row_building, row_type, row_time, remark = _record_identity(fields, ["楼栋", "记录类型", "上传日期", "备注"])
            if row_building != building or row_type not in {"设备巡检", "测点明细"} or type(row_time) is not int or row_time != upload_time:
                return None
            counts[row_type] += 1
            if isinstance(remark, str) and remark.startswith(INSPECTION_BATCH_PREFIX):
                if row_type != "设备巡检":
                    return None
                markers.append(json.loads(remark[len(INSPECTION_BATCH_PREFIX):]))
    except (ValueError, TypeError):
        return None
    if len(markers) != 1 or not isinstance(markers[0], dict):
        return None
    marker = markers[0]
    if (set(marker) != {"status", "building", "upload_time", "device_count", "point_count", "record_ids_sha256"}
            or marker["status"] != "completed" or marker["building"] != building
            or type(marker["upload_time"]) is not int or marker["upload_time"] != upload_time
            or type(marker["device_count"]) is not int or marker["device_count"] != counts["设备巡检"]
            or type(marker["point_count"]) is not int or marker["point_count"] != counts["测点明细"]
            or not counts["设备巡检"] or marker["record_ids_sha256"] != _record_ids_sha256(ids)):
        return None
    return marker


def load_catalog() -> list[dict[str, Any]]:
    return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))["devices"]


def field_definitions() -> list[dict[str, Any]]:
    definitions = {
        name: {"field_name": name, "type": kind}
        for name, kind in {
            "设备编号": 1, "楼栋": 3, "专业": 3, "位置": 1, "巡检表单": 1,
            "上传日期": 5, "源文件时间": 5, "源文件": 1, "启停状态": 3,
            "数据完整性": 3, "缺少源数据": 1, "备注": 1,
            **POINT_DETAIL_TYPES,
        }.items()
    }
    options = {"楼栋": ["D楼", "E楼"], "专业": ["暖通", "电气"],
               "启停状态": ["开启", "关闭"], "数据完整性": ["完整", "缺少源数据"],
               "记录类型": ["设备巡检", "测点明细"]}
    for name, values in options.items():
        definitions[name]["property"] = {"options": [{"name": v} for v in values]}
    for name in ("上传日期", "源文件时间", "采样时间"):
        definitions[name]["property"] = {"date_formatter": "yyyy/MM/dd HH:mm", "auto_fill": False}
    for device in load_catalog():
        for point in device["points"]:
            definitions.setdefault(point["field"], {"field_name": point["field"], "type": point["type"]})
    return list(definitions.values())


def _point_aliases(point: dict[str, Any]) -> set[str]:
    field, label = _key(point["field"]), _key(point["label"])
    phase = field[:2] if field.startswith(("L1", "L2", "L3")) else ""
    # A phase-less temperature must never select one of three phase readings.
    aliases = {field, field.split("(", 1)[0]}
    if not phase:
        aliases.update((label, label.split("(", 1)[0]))
    else:
        aliases.update((phase + label, phase + label.split("(", 1)[0]))
    return aliases


def _numeric(value: Any, field: str) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    text = _text(value)
    unit = field.rsplit("(", 1)[-1].rstrip(")") if "(" in field else ""
    if unit and text.lower().endswith(unit.lower()):
        text = text[:-len(unit)].strip()
    try:
        result = float(text.replace(",", ""))
    except (ValueError, TypeError):
        return None
    return result if math.isfinite(result) else None


def _on_off(value: Any) -> str:
    # Only explicit on/off points use this mapping, never alarm or switch health.
    text = _text(value)
    if text in {"1", "1.0", "开启", "启动", "运行"}:
        return "开启"
    if text in {"0", "0.0", "关闭", "停止", "未运行", "△", "Δ", "▲"}:
        return "关闭"
    return ""


def _round_measurement(value: float) -> float:
    if abs(value) >= 1e16:
        return value
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _device_aliases(device: dict[str, Any]) -> set[str]:
    name = _key(device["device"])
    names = {name}
    # The room and CRAH number are both printed in the form, not inferred from order.
    match = re.fullmatch(r"CRAH(\d+)", name)
    room = re.search(r"\((\d{3})\)", _key(device["position"]))
    if match and room:
        names.add(f"{room[1]}-CRAH-{match[1]}")
    prefix = device["building"][0] + "-"
    if device["discipline"] == "电气" and name == "直流屏":
        room = {"电气系统西区": "144", "电气系统东区": "120"}.get(_key(device["position"]))
        if room:
            names.add(f"{prefix}{room}-AD-001")
    return names | {n[len(prefix):] if n.startswith(prefix) else prefix + n for n in names}


def _electrical_source_device_key(name: str) -> str:
    # A feeder row can mention its downstream transformer after the underscore.
    # Only the leading identifier owns the readings in that row.
    match = re.match(r"^([DE]-\d{3}-(?:AH|TR|UPS|HVDC|SVG)-\d{3})(?:_|$)", name)
    if match:
        return match[1]
    screen = re.fullmatch(r"([DE]-(?:120|144))-AD-?001_直流屏", name)
    return f"{screen[1]}-AD-001" if screen else ""


def _first_value_column(sheet) -> tuple[int, str]:
    candidates = []
    extra_values = False
    for row_number, row in enumerate(sheet.iter_rows(min_row=1, max_row=40, min_col=5, max_col=128, values_only=True), 1):
        dates = []
        for column, value in enumerate(row, 5):
            extra_values = extra_values or (column > 5 and value is not None)
            if isinstance(value, (datetime, dt_time)) or re.fullmatch(
                r"(?:\d{4}[-/]\d{1,2}[-/]\d{1,2}[ T\n]+)?\d{1,2}:\d{2}(?::\d{2})?", _text(value)
            ):
                dates.append((column, _text(value)))
        if dates:
            candidates.append((len(dates), -row_number, dates[0]))
    if candidates:
        return max(candidates, key=lambda item: item[:2])[2]
    if extra_values:
        raise ValueError("巡检源文件有多列数值但没有可识别的时间表头，无法确定第一个时刻；未修改多维表")
    return 5, ""


class InspectionDataUploadService:
    def __init__(self, runtime_config: dict[str, Any], *, internal_client=None, bitable_client=None):
        self.runtime_config = runtime_config
        raw = runtime_config.get("inspection_data_upload", {})
        self.cfg = {**copy.deepcopy(DEFAULT_CONFIG), **raw}
        self.cfg.pop("source_family", None)
        self.cfg["source_families"] = copy.deepcopy(DEFAULT_CONFIG["source_families"])
        self._internal = internal_client
        self._bitable = bitable_client

    @staticmethod
    def normalize_source_date(value: Any = None) -> str:
        if not value:
            return datetime.now().strftime("%Y-%m-%d")
        return datetime.strptime(str(value), "%Y-%m-%d").strftime("%Y-%m-%d")

    def make_bitable_client(self):
        if self._bitable is not None:
            return self._bitable
        auth = require_feishu_auth_settings(self.runtime_config)
        return FeishuBitableClient(
            **auth, app_token=self.cfg["app_token"], calc_table_id="", attachment_table_id="",
            date_text_to_timestamp_ms_fn=lambda **_: 0,
            canonical_metric_name_fn=lambda value: str(value), dimension_mapping={},
        )

    def read_completed_batch(self, *, building: str, upload_time: int) -> dict | None:
        if building not in {"D楼", "E楼"} or building not in self.cfg["tables"]:
            return None
        client = self.make_bitable_client()
        table = self.cfg["tables"][building]
        first = client.list_records(table, page_size=500, max_records=0)
        marker = validate_inspection_batch(first, building=building, upload_time=upload_time)
        if marker is None:
            return None
        # Copy before the second read: some clients return mutable shared objects.
        first_snapshot = json.dumps(sorted(first, key=lambda row: row["record_id"]), ensure_ascii=False, sort_keys=True)
        second = client.list_records(table, page_size=500, max_records=0)
        if (validate_inspection_batch(second, building=building, upload_time=upload_time) != marker
                or json.dumps(sorted(second, key=lambda row: row["record_id"]), ensure_ascii=False, sort_keys=True) != first_snapshot):
            return None
        return marker

    def ensure_schema(self, *, apply: bool = False) -> dict[str, Any]:
        client = self.make_bitable_client()
        report = {}
        for building in self.cfg["buildings"]:
            table = self.cfg["tables"][building]
            existing = {item["field_name"]: item for item in client.list_fields(table)}
            conflicts = [d["field_name"] for d in field_definitions()
                         if d["field_name"] in existing and existing[d["field_name"]]["type"] != d["type"]]
            if conflicts:
                raise ValueError(f"{building} 巡检字段类型不符，未修改已有字段: {','.join(conflicts)}")
            missing = [d for d in field_definitions() if d["field_name"] not in existing]
            if apply:
                for definition in missing:
                    url = client.LIST_FIELD_URL.format(app_token=client.app_token, table_id=table)
                    client._post_json(url, definition)
                actual = {d["field_name"]: d["type"] for d in client.list_fields(table)}
                if any(actual.get(d["field_name"]) != d["type"] for d in field_definitions()):
                    raise RuntimeError(f"{building} 巡检字段创建后复核失败")
            report[building] = {"table_id": table, "missing_fields": [d["field_name"] for d in missing], "applied": apply}
        return report

    @staticmethod
    def parse_workbook(file_path: Path | str, *, building: str, catalog=None) -> dict[str, Any]:
        # Offline comparison helper. Uploads use the internal API instead.
        workbook = load_workbook(file_path, read_only=True, data_only=True)
        try:
            sheet = workbook.worksheets[0]
            sheet.reset_dimensions()
            value_column, value_time = _first_value_column(sheet)
            samples = ([*row[:3], row[-1]] for row in sheet.iter_rows(min_col=2, max_col=value_column, values_only=True))
            result = InspectionDataUploadService.parse_samples(samples, building=building, catalog=catalog, value_time=value_time)
            return {**result, "value_column": value_column}
        finally:
            workbook.close()

    @staticmethod
    def parse_samples(samples, *, building: str, catalog=None, value_time: str, allow_unmapped: bool = False,
                      matched_rows: dict[tuple[int, str], list[int]] | None = None,
                      corrections: dict[int, str] | None = None) -> dict[str, Any]:
        devices = [d for d in (load_catalog() if catalog is None else catalog) if d["building"] == building]
        if not devices:
            raise ValueError(f"不支持的巡检楼栋: {building}")
        by_device = defaultdict(list)
        for i, device in enumerate(devices):
            for name in _device_aliases(device):
                by_device[name].append(i)
        readings: dict[int, dict[str, Any]] = defaultdict(dict)
        corrected_fields = set()
        corrections = corrections or {}
        diagnostics: list[str] = []
        valid_count = 0
        position = device_name = ""
        for row_number, values in enumerate(samples, 1):
            if row_number > 100000:
                raise ValueError("巡检源文件超过100000行，停止解析，未修改多维表")
            b, c, d, raw = values
            if row_number - 1 in corrections:
                raw = corrections[row_number - 1]
            explicit = set(re.findall(r"(?<![A-Za-z0-9])([A-E])(?:楼|栋|-\d{3})", _text(b) + " " + _text(c)))
            if explicit and explicit != {building[0]}:
                raise ValueError(f"{building} 巡检源文件出现其他楼栋标识，第{row_number}行，未修改多维表")
            if _text(b):
                if _key(b) != _key(position):
                    device_name = ""
                position = _text(b)
            if _text(c):
                device_name = _text(c)
            if not _text(d) or not device_name:
                continue
            if _key(c) in {"设备", "设备编号", "监控对象"} and _key(d) in {"点位", "测点", "监控指标", "指标"}:
                continue
            metric = _key(d)
            source_device = _key(device_name)
            parallel_point = (re.fullmatch(r"([1-9]\d*)#_(电池电压_V|冷却液温度_℃)", metric)
                              if source_device in {"柴发并机系统", f"{building[0]}-178-柴发并机系统"} else None)
            tank_point = (re.fullmatch(r"([1-9]\d*)#油箱体积", metric)
                          if source_device in {"燃油自控系统", f"{building[0]}-178-燃油自控系统"} else None)
            diesel_point = parallel_point or tank_point
            if diesel_point:
                number = diesel_point[1]
                metric = DIESEL_PARALLEL_FIELDS[parallel_point[2]] if parallel_point else "柴油液位(cm)"
                candidates = [
                    i for i in by_device.get(f"{number}#柴发", [])
                    if devices[i]["discipline"] == "电气" and devices[i]["sheet"].strip() == "柴发巡检表"
                ]
            else:
                candidates = list(set(by_device.get(_key(device_name), [])))
            if not candidates and not diesel_point:
                candidates = list({
                    i for i in by_device.get(_electrical_source_device_key(_key(device_name)), [])
                    if devices[i]["discipline"] == "电气"
                })
            if len(candidates) > 1:
                candidates = [i for i in candidates if _key(devices[i]["position"]) == _key(position)]
            if len(candidates) != 1:
                diagnostics.append(f"第{row_number}行设备未唯一匹配: {position}/{device_name}/{_text(d)}")
                continue
            i = candidates[0]
            if metric == "启停状态":
                field, value = "启停状态", _on_off(raw)
            else:
                electrical = devices[i]["discipline"] == "电气"
                points = [
                    p for p in devices[i]["points"]
                    if metric in _point_aliases(p)
                    or (electrical and metric in ELECTRICAL_POINT_ALIASES.get(p["field"], ()))
                ]
                if len(points) != 1:
                    diagnostics.append(f"第{row_number}行点位未唯一匹配: {device_name}/{_text(d)}")
                    continue
                p = points[0]
                field = p["field"]
                value = _numeric(raw, field) if p["type"] == 2 else _text(raw)
            if matched_rows is not None:
                matched_rows.setdefault((i, field), []).append(row_number - 1)
            if parallel_point and field == "冷却液水温(℃)" and value == 3276.4:
                diagnostics.append(f"第{row_number}行柴发冷却液温度源值异常: {number}#柴发/{raw}")
                continue
            if tank_point and value is not None:
                # The source gives tank volume; the agreed liquid-level scale is 10:1.
                value = float(Decimal(str(value)).scaleb(-1))
            if value is None or value == "":
                diagnostics.append(f"第{row_number}行空值或无效数值: {device_name}/{field}")
                continue
            if field in readings[i] and readings[i][field] != value:
                raise ValueError(f"{building} 巡检源文件重复点位数值冲突: {device_name}/{field}，未修改多维表")
            readings[i][field] = value
            if row_number - 1 in corrections:
                corrected_fields.add((i, field))
            if field == "运行状态" and _on_off(raw):
                readings[i]["启停状态"] = _on_off(raw)
            valid_count += 1
        if not valid_count and not allow_unmapped:
            raise ValueError(f"{building} 巡检源文件没有可识别的有效测点，保留目标表原数据")
        rows = []
        for i, device in enumerate(devices):
            values = {field: _round_measurement(value) if isinstance(value, float) and (i, field) not in corrected_fields else value
                      for field, value in readings.get(i, {}).items()}
            missing = [p["field"] for p in device["points"] if p["field"] not in values]
            if "启停状态" not in values:
                missing.append("启停状态")
            rows.append({"设备编号": device["device"], "楼栋": building, "专业": device["discipline"],
                         "位置": device["position"], "巡检表单": device["sheet"], **values,
                         "数据完整性": "缺少源数据" if missing else "完整", "缺少源数据": "、".join(missing)})
        return {"rows": rows, "valid_points": valid_count, "matched_devices": len(readings),
                "missing_devices": len(devices) - len(readings), "diagnostics": diagnostics,
                "value_time": value_time}

    @staticmethod
    def _validate_dataset(doc: dict, date: str, bucket: str) -> datetime:
        key = (doc.get("building"), doc.get("source_family"))
        family = SOURCE_FAMILIES.get(key[1])
        if (doc.get("schema_version") != 1 or key[0] not in {"D楼", "E楼"} or not family
                or doc.get("report_name") != family["report_name"] or doc.get("bucket_key") != bucket
                or bucket[:10] != date):
            raise ValueError("巡检接口楼栋、专业、时段或版本不匹配；未清空多维表")
        try:
            start, end, sampled = (
                datetime.strptime(doc.get(name, ""), "%Y-%m-%d %H:%M:%S")
                for name in ("query_start", "query_end", "value_time")
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("巡检接口查询时间无效；未清空多维表") from exc
        if end - start != timedelta(minutes=20) or inspection_hour_bucket(end) != bucket or not start <= sampled <= end:
            raise ValueError("巡检接口查询窗口不匹配；未清空多维表")
        rows = doc.get("rows")
        if (not isinstance(rows, list) or not 0 < len(rows) <= 10000
                or any(not isinstance(row, list) or len(row) != 4
                       or any(value is not None and not isinstance(value, str) for value in row) for row in rows)):
            raise ValueError("巡检接口测点结构无效；未清空多维表")
        if "point_ids" in doc and (
            not isinstance(doc["point_ids"], list) or len(doc["point_ids"]) != len(rows)
            or any(not isinstance(point_id, str) or not point_id for point_id in doc["point_ids"])
            or len(set(doc["point_ids"])) != len(rows)
        ):
            raise ValueError("巡检接口测点ID与结果行不对应；未清空多维表")
        if "requested_point_ids" in doc:
            requested = doc["requested_point_ids"]
            if (not isinstance(requested, list) or any(not isinstance(i, str) or not i for i in requested)
                    or set(requested) != set(doc.get("point_ids", []))):
                raise ValueError("巡检接口未返回全部请求测点；未清空多维表")
        return end

    @staticmethod
    def _point_detail_rows(entry: dict, discipline: str, notes: dict, targets: dict | None = None,
                           status_rules: dict | None = None, corrections: dict[int, str] | None = None) -> list[dict]:
        from app.modules.inspection_data.service.point_status import status_fields
        rows = []
        position = device = ""
        sample_time = int(datetime.strptime(entry["value_time"], "%Y-%m-%d %H:%M:%S")
                          .replace(tzinfo=LOCAL_TZ).timestamp() * 1000)
        ids = entry.get("point_ids", [])
        corrections = corrections or {}
        for i, (place, name, metric, raw) in enumerate(entry["rows"]):
            if _text(place) and _key(place) != _key(position):
                position, device = _text(place), ""
            if _text(name):
                device = _text(name)
            if not device or not _text(metric):
                if ids:
                    raise ValueError(f"巡检接口测点 {ids[i]} 缺少设备或名称；未清空多维表")
                continue
            corrected = corrections.get(i)
            numeric = _numeric(raw, "")
            value = f"{_round_measurement(numeric):.2f}" if numeric is not None else _text(raw)
            if corrected is not None:
                value = corrected
            point_id = ids[i] if ids else ""
            target = (targets or {}).get(point_id, {})
            rows.append({
                "记录类型": "测点明细", "楼栋": entry["building"], "专业": discipline, "位置": position,
                "设备编号": device, "巡检表单": entry["report_name"], "测点名称": _text(metric),
                "测点ID": point_id, "测点路径": " / ".join(filter(None, (position, device, _text(metric)))),
                "测点值": value,
                "测点原始值": "" if raw is None else str(raw),
                **({CORRECTED_VALUE_FIELD: corrected} if corrected is not None else {}),
                **status_fields((status_rules or {}).get(point_id), corrected if corrected is not None else raw),
                "测点备注": notes.get(point_id, ""),
                **{field: target[key] for key, field in POINT_TARGET_FIELDS.items() if key in target},
                **{field: target.get("round_cells", {}).get(key, "")
                   for key, field in INSPECTION_ROUND_FIELDS.items() if "round_cells" in target},
                "采样时间": sample_time, "数据完整性": "完整" if _text(raw) else "缺少源数据",
            })
        return rows

    def _sources(self, source_date: str, log, cancel, source_bucket_key: str = "", point_settings=None) -> dict:
        internal = self._internal or InternalBridgeHttpClient.from_runtime_config(self.runtime_config)
        if internal is None:
            raise ValueError("巡检数据处理需要配置内网HTTP桥接地址")
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        requested_bucket = source_bucket_key or (inspection_hour_bucket(now) if source_date == today else "")
        deadline = time.monotonic() + (max(0, float(self.cfg["wait_source_timeout_sec"])) if source_date == today else 0)
        required = [(building, family) for building in self.cfg["buildings"] for family in SOURCE_FAMILIES]
        retry_dispatch_at = {}
        last_diagnostic = ""
        last_error_log = -60.0
        pending_sync = source_date == today and point_settings is not None
        rules = query_rules(point_settings)
        while True:
            cancel()
            if load_point_settings(self.runtime_config) != point_settings:
                raise RuntimeError("巡检测点设置已变更，请重新运行；未清空多维表")
            try:
                if pending_sync:
                    sync_point_settings(self.runtime_config, point_settings, bridge=internal)
                    pending_sync = False
                payload = internal.get_inspection_snapshot(source_date=source_date, bucket_key=requested_bucket)
            except Exception as exc:
                if not self._retryable_source_error(exc):
                    raise
                payload = {}
                if time.monotonic() - last_error_log >= 60:
                    log(f"[巡检数据处理] 内网暂不可用，保留原数据并等待: {exc}")
                    last_error_log = time.monotonic()
            selected = {b: {} for b in self.cfg["buildings"]}
            bucket = payload.get("bucket_key", "")
            if payload.get("ok"):
                if requested_bucket and bucket != requested_bucket:
                    raise ValueError("巡检接口响应时段与请求不一致；未清空多维表")
                if not isinstance(payload.get("datasets"), list):
                    raise ValueError("巡检接口返回格式无效；未清空多维表")
                seen = set()
                for doc in payload["datasets"]:
                    if not isinstance(doc, dict):
                        raise ValueError("巡检接口测点响应无效；未清空多维表")
                    captured = self._validate_dataset(doc, source_date, bucket)
                    building, family = doc["building"], doc["source_family"]
                    if (building, family) in seen:
                        raise ValueError("巡检接口返回重复楼栋和专业；未清空多维表")
                    seen.add((building, family))
                    if source_date == today and point_settings is not None:
                        expected = rules.get(building, {}).get(family, {})
                        if doc.get("point_overrides", {}) != expected:
                            continue
                        if expected:
                            ids = set(doc.get("point_ids", []))
                            added, removed = set(expected.get("add", ())), set(expected.get("remove", ()))
                            baseline = expected.get("baseline")
                            if (not doc.get("requested_point_ids") or not added <= ids or ids & removed
                                    or (baseline is not None and ids != (set(baseline) | added) - removed)):
                                raise ValueError("巡检接口返回的测点与已保存清单不符；未清空多维表")
                    age = (datetime.now() - captured).total_seconds()
                    if source_date == today and (age < -300 or age > max(1, int(self.cfg["max_source_age_minutes"])) * 60):
                        continue
                    selected[building][family] = doc
            missing = [(b, family) for b, family in required if family not in selected[b]]
            if not missing:
                return selected
            missing_labels = ','.join(f"{b}/{SOURCE_FAMILIES[family]['label']}" for b, family in missing)
            if time.monotonic() >= deadline or datetime.now().strftime("%Y-%m-%d") != source_date:
                raise RuntimeError(f"{source_date} 巡检接口数据未齐或过期: {missing_labels}；未清空多维表")
            if requested_bucket != inspection_hour_bucket(datetime.now()):
                raise RuntimeError(f"巡检时段已结束: {requested_bucket}；未清空多维表")
            for family in SOURCE_FAMILIES if not pending_sync else ():
                tick = time.monotonic()
                buildings = [b for b, f in missing if f == family and tick >= retry_dispatch_at.get((b, f), 0)]
                if not buildings:
                    continue
                cancel()
                try:
                    internal.refresh_latest_source_cache(source_family=family, buildings=buildings,
                                                         target_bucket_key=requested_bucket)
                    # A queued acknowledgement does not prove that the download started.
                    retry_dispatch_at.update({(b, family): time.monotonic() + 60 for b in buildings})
                except Exception as exc:
                    if not self._retryable_source_error(exc):
                        raise
            message = f"[巡检数据处理] 等待内网接口数据: date={source_date}, sources={missing_labels}"
            if message != last_diagnostic:
                log(message)
                last_diagnostic = message
            wake = time.monotonic() + min(max(1, float(self.cfg["wait_source_poll_sec"])), max(0, deadline-time.monotonic()))
            while time.monotonic() < wake:
                cancel()
                time.sleep(max(0, min(0.25, wake-time.monotonic())))

    @staticmethod
    def _retryable_source_error(exc):
        return InternalBridgeHttpClient._is_retryable_error(exc) or any(
            reason in str(exc) for reason in ("WinError 10054", "HTTP 返回 502", "HTTP 返回 503", "HTTP 返回 504")
        )

    def run(self, *, log: Callable[[str], None] | None = None, check_cancel=None,
            source_date=None, source_bucket_key: str = "", dry_run: bool = False) -> dict[str, Any]:
        log = log or (lambda _: None)
        cancel = check_cancel or (lambda: None)
        if not self.cfg["enabled"]:
            return {"status": "skipped", "reason": "disabled"}
        if set(self.cfg["buildings"]) != {"D楼", "E楼"} or len(self.cfg["buildings"]) != 2:
            raise ValueError("巡检数据处理仅支持D楼、E楼，必须同时预检")
        if len(set(self.cfg["tables"][b] for b in self.cfg["buildings"])) != 2:
            raise ValueError("D/E楼巡检目标表不能相同")
        date = self.normalize_source_date(source_date)
        if not source_bucket_key and date == datetime.now().strftime("%Y-%m-%d"):
            source_bucket_key = inspection_hour_bucket(datetime.now())

        def require_current_bucket():
            if source_bucket_key and (
                source_bucket_key[:10] != date or source_bucket_key != inspection_hour_bucket(datetime.now())
            ):
                raise RuntimeError(f"巡检时段已结束或不匹配: {source_bucket_key}；未清空多维表")

        require_current_bucket()
        point_settings = load_or_restore_point_settings(self.runtime_config)
        if not dry_run:
            try:
                retry_pending_point_config(self.runtime_config)
            except Exception as exc:
                log(f"[巡检数据处理] 飞书测点配置待同步: {exc}")
        sources = self._sources(date, log, cancel, source_bucket_key, point_settings)
        parsed = {}
        catalog = load_catalog()
        for building in self.cfg["buildings"]:
            combined = {"rows": [], "valid_points": 0, "matched_devices": 0,
                        "missing_devices": 0, "diagnostics": [], "sources": {}, "point_count": 0}
            for family, contract in SOURCE_FAMILIES.items():
                cancel()
                entry = sources[building][family]
                group = (point_settings or {}).get(building, {}).get(family, {})
                corrections = corrected_samples(entry, group.get("correction_rules", {}))
                point_rows = self._point_detail_rows(entry, contract["discipline"], group.get("points", {}),
                                                     group.get("targets", {}), group.get("status_rules", {}), corrections)
                added_ids = set(entry.get("point_overrides", {}).get("add", []))
                p = self.parse_samples(
                    entry["rows"], building=building, value_time=entry["value_time"], corrections=corrections,
                    catalog=[d for d in catalog if d["discipline"] == contract["discipline"]],
                    allow_unmapped=bool(entry.get("requested_point_ids")
                                        and any(r["测点ID"] in added_ids and r["测点值"] for r in point_rows)),
                )
                source_time = int(datetime.strptime(entry["query_end"], "%Y-%m-%d %H:%M:%S")
                                  .replace(tzinfo=LOCAL_TZ).timestamp() * 1000)
                source_name = f"巡检接口/{entry['report_name']}"
                combined["rows"].extend({"记录类型": "设备巡检", **r, "源文件时间": source_time, "源文件": source_name}
                                        for r in [*p["rows"], *point_rows])
                combined["point_count"] += len(point_rows)
                for key in ("valid_points", "matched_devices", "missing_devices"):
                    combined[key] += p[key]
                combined["diagnostics"].extend(f"{contract['label']}: {item}" for item in p["diagnostics"])
                combined["sources"][family] = {
                    **contract, "source_family": family, "source_file": source_name,
                    "source_time": entry["query_end"], "bucket_kind": "latest", "bucket_key": entry["bucket_key"],
                    **{key: value for key, value in p.items() if key != "rows"},
                }
                log(f"[巡检数据处理] {building}/{contract['report_name']} 设备={len(p['rows'])}, 有效测点={p['valid_points']}, "
                    f"数值修正={len(corrections)}, 缺数据设备={p['missing_devices']}, 首个时刻={p['value_time']}")
                for item in p["diagnostics"][:10]:
                    log(f"[巡检数据处理] {building}/{contract['label']} {item}")
            parsed[building] = combined
        source_metadata = {"source_families": copy.deepcopy(DEFAULT_CONFIG["source_families"]),
                           "source_file_count": sum(len(p["sources"]) for p in parsed.values())}
        if dry_run:
            return {"status": "success", "dry_run": True, "source_date": date,
                    **source_metadata,
                    "buildings": list(parsed), "uploaded_count": 0,
                    "planned_count": sum(len(p["rows"]) for p in parsed.values()),
                    "results": {b: {k: v for k, v in p.items() if k != "rows"} for b, p in parsed.items()}}
        client = self.make_bitable_client()
        # Preflight both tables before the first destructive operation.
        schema = self.ensure_schema()
        missing = {field for item in schema.values() for field in item["missing_fields"]}
        if missing - POINT_DETAIL_TYPES.keys():
            raise ValueError("巡检目标表字段未创建完整，请先执行字段初始化；未清空多维表")
        if missing:
            self.ensure_schema(apply=True)
        numeric_fields = {item["field_name"] for item in field_definitions() if item["type"] == 2}
        replacements = {}
        occupied_times = set()
        # Validate old IDs and preserve both snapshots before clearing either table.
        for building in parsed:
            cancel()
            table = self.cfg["tables"][building]
            old = client.list_records(table, page_size=500, max_records=0)
            occupied_times.update(value for row in old if isinstance(value := row.get("fields", {}).get("上传日期"), int))
            ids = [_text(r.get("record_id")) for r in old]
            if any(not rid for rid in ids) or len(ids) != len(set(ids)):
                raise RuntimeError(f"{building} 旧记录ID不完整，未清空目标表")
            backup = BACKUP_ROOT / f"{table}.json"
            snapshot = json.loads(backup.read_text(encoding="utf-8")) if backup.exists() else {}
            # A retry must not replace the pre-failure snapshot with a half-written table.
            if snapshot.get("status") != "pending":
                snapshot = {"building": building, "status": "pending", "saved_at": datetime.now().isoformat(), "records": old}
                atomic_write_text(backup, json.dumps(snapshot, ensure_ascii=False))
            replacements[building] = (table, ids, backup, snapshot)
        upload_time = int(datetime.now(LOCAL_TZ).timestamp() * 1000)
        while upload_time in occupied_times:
            upload_time += 1

        def require_unchanged():
            cancel()
            require_current_bucket()
            if load_point_settings(self.runtime_config) != point_settings:
                raise RuntimeError("巡检测点设置已变更，请重新运行；未清空多维表")

        staged = {}
        attempted = []
        try:
            for building, parsed_building in parsed.items():
                require_unchanged()
                table, ids, _, _ = replacements[building]
                rows = [{**r, "上传日期": upload_time} for r in parsed_building["rows"]]
                attempted.append(building)
                responses = client.batch_create_records(table, rows, batch_size=200)
                created = [r.get("record_id") for response in responses for r in response.get("data", {}).get("records", [])]
                if len(created) != len(rows) or not all(created) or len(set(created)) != len(created):
                    raise RuntimeError("新记录创建返回数量不完整")
                actual = client.list_records(table, page_size=500, max_records=0)
                created_ids = set(created)
                if created_ids & set(ids) or {r.get("record_id") for r in actual} != created_ids | set(ids):
                    raise RuntimeError("上传后回查数量或记录ID不一致")
                fields = sorted({name for row in rows for name in row})
                expected = Counter(_comparison_record_identity(r, fields, numeric_fields) for r in rows)
                actual_rows = [r.get("fields", {}) for r in actual if r.get("record_id") in created_ids]
                try:
                    observed = Counter(_comparison_record_identity(r, fields, numeric_fields) for r in actual_rows)
                except (TypeError, ValueError):
                    observed = None
                if observed != expected:
                    try:
                        diagnostic = _record_mismatch_diagnostic(rows, actual_rows, fields, numeric_fields)
                    except Exception:  # noqa: BLE001
                        diagnostic = "字段格式诊断不可用"
                    log(f"[巡检数据处理] {building} 上传回查字段差异: {diagnostic}（字段值已隐藏）")
                    raise RuntimeError(f"上传后回查设备、位置或上传批次不一致；{diagnostic}（字段值已隐藏）")
                staged[building] = created_ids
            require_unchanged()
        except Exception as exc:
            # A failed batch may have committed part of its rows before returning an error.
            for building in attempted:
                table, ids, _, _ = replacements[building]
                try:
                    actual = client.list_records(table, page_size=500, max_records=0, field_names=["上传日期", "楼栋"])
                    old_ids = set(ids)
                    new_ids = [r["record_id"] for r in actual if r.get("record_id") and r["record_id"] not in old_ids
                               and r.get("fields", {}).get("上传日期") == upload_time
                               and _text(r.get("fields", {}).get("楼栋")) == building]
                    if new_ids and client.batch_delete_records(table, new_ids, batch_size=200) != len(new_ids):
                        raise RuntimeError("未清理全部暂存记录")
                except Exception as cleanup_exc:
                    log(f"[巡检数据处理] {building} 暂存记录待清理（旧记录未删）: {cleanup_exc}")
            raise RuntimeError(f"巡检新批次未通过校验，未删除旧记录: {exc}") from exc

        result = {}
        for building, parsed_building in parsed.items():
            table, ids, backup, snapshot = replacements[building]
            rows = parsed_building["rows"]
            carrier_id = ""
            marker_text = ""
            marker_write_attempted = False
            try:
                require_unchanged()
                if ids and client.batch_delete_records(table, ids, batch_size=200) != len(ids):
                    raise RuntimeError("旧记录未全部清理")
                actual = client.list_records(table, page_size=500, max_records=0)
                if {r.get("record_id") for r in actual} != staged[building]:
                    raise RuntimeError("清理旧记录后回查不一致")
                expected_rows = [{**row, "上传日期": upload_time} for row in rows]
                fields = sorted({name for row in expected_rows for name in row})
                if Counter(_comparison_record_identity(row, fields, numeric_fields) for row in expected_rows) != Counter(
                    _comparison_record_identity(row.get("fields", {}), fields, numeric_fields) for row in actual
                ):
                    raise RuntimeError("清理旧记录后数据回查不一致")
                devices = [r for r in actual if _record_identity(r.get("fields", {}), ["记录类型"])[0] == "设备巡检"]
                if not devices:
                    raise RuntimeError("巡检批次缺少设备记录")
                empty_device_ids = []
                for device in devices:
                    try:
                        remark = _record_identity(device.get("fields", {}), ["备注"])[0]
                    except ValueError:
                        continue
                    if remark is None:
                        empty_device_ids.append(device["record_id"])
                if not empty_device_ids:
                    raise RuntimeError("巡检设备记录备注均非空，无法写入完成标记")
                carrier_id = min(empty_device_ids)
                counts = Counter(row["记录类型"] for row in rows)
                marker = {"status": "completed", "building": building, "upload_time": upload_time,
                          "device_count": counts["设备巡检"], "point_count": counts["测点明细"],
                          "record_ids_sha256": _record_ids_sha256(staged[building])}
                marker_text = INSPECTION_BATCH_PREFIX + json.dumps(marker, ensure_ascii=False, separators=(",", ":"))
                require_unchanged()
                current = client.list_records(table, page_size=500, max_records=0)
                if {r.get("record_id") for r in current} != staged[building]:
                    raise RuntimeError("写入完成标记前记录已变化")
                carrier = next(r for r in current if r.get("record_id") == carrier_id)
                if _record_identity(carrier.get("fields", {}), ["备注"])[0] is not None:
                    raise RuntimeError("写入完成标记前设备备注已变化")
                marker_write_attempted = True
                client.update_record(table, carrier_id, {"备注": marker_text})
                if self.read_completed_batch(building=building, upload_time=upload_time) != marker:
                    raise RuntimeError("完成标记回查不一致")
                require_unchanged()
                snapshot["status"] = "completed"
                atomic_write_text(backup, json.dumps(snapshot, ensure_ascii=False))
            except Exception as exc:
                if marker_write_attempted and carrier_id and marker_text:
                    try:
                        current = client.list_records(table, page_size=500, max_records=0)
                        carrier = next((r for r in current if r.get("record_id") == carrier_id), None)
                        if carrier and _record_identity(carrier.get("fields", {}), ["备注"])[0] == marker_text:
                            client.update_record(table, carrier_id, {"备注": ""})
                    except Exception as cleanup_exc:
                        log(f"[巡检数据处理] {building} 完成标记状态未确认: {cleanup_exc}")
                raise RuntimeError(f"{building} 巡检旧记录清理未完成，新批次已核验并保留: {exc}；可重跑清理") from exc
            result[building] = {"table_id": table, "uploaded_count": len(rows), "deleted_count": len(ids),
                                "upload_time": upload_time, "batch_completion": marker,
                                "missing_devices": parsed_building["missing_devices"],
                                "point_count": parsed_building["point_count"],
                                "unmatched_or_invalid_rows": len(parsed_building["diagnostics"]),
                                "sources": parsed_building["sources"]}
            log(f"[巡检数据处理] {building} 上传完成: records={len(rows)}, deleted={len(ids)}")
        return {"status": "success", "source_date": date, "buildings": list(result), "results": result,
                **source_metadata,
                "uploaded_count": sum(v["uploaded_count"] for v in result.values())}
