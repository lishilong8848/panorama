from __future__ import annotations

import json
import re

from app.modules.inspection_data.service.point_correction import _decimal, _decimal_text


STATUS_FIELDS = {
    "normal": "状态正常值",
    "abnormal": "状态异常值",
    "not_running": "状态未运行值",
}
STATUS_SYMBOLS = {"normal": "√", "abnormal": "×", "not_running": "△"}
INTERVAL_FIELD = "状态区间规则"
SOURCE_FIELD = "测点来源"
FIXED_VALUE_FIELD = "固定值"
LOCAL_POINT_ID = re.compile(r"local_[0-9a-f]{32}")
LOCAL_SOURCES = {"fixed_status", "fixed_value", "manual_status"}


def _intervals(raw, exact):
    if not isinstance(raw, list) or len(raw) > 50:
        raise ValueError("每个测点最多设置50条状态区间")
    parsed = []
    for row in raw:
        if (not isinstance(row, dict) or set(row) != {"min", "max", "status"}
                or not isinstance(row["status"], str) or row["status"] not in STATUS_SYMBOLS):
            raise ValueError("状态区间格式无效")
        lower, upper = _decimal(row["min"]), _decimal(row["max"])
        if lower is None or upper is None or lower > upper:
            raise ValueError("状态区间需填写有效的最小值和最大值")
        parsed.append((lower, upper, row["status"]))
    parsed.sort()
    if any(a[1] >= b[0] for a, b in zip(parsed, parsed[1:])):
        raise ValueError("状态区间包含端点，不能重叠")
    for status, values in exact.items():
        for value in values:
            number = _decimal(value, reading=True)
            if number is not None and any(lo <= number <= hi and target != status for lo, hi, target in parsed):
                raise ValueError("状态精确值与区间映射冲突")
    return [{"min": _decimal_text(lo), "max": _decimal_text(hi), "status": status} for lo, hi, status in parsed]


def normalize_status_rules(rules, allowed_ids):
    if not isinstance(rules, dict) or set(rules) - set(allowed_ids):
        raise ValueError("状态规则必须对应已启用测点")
    normalized = {}
    for point_id, rule in rules.items():
        if not isinstance(rule, dict) or set(rule) - {"type", "intervals", "source_kind", "fixed_value", *STATUS_FIELDS}:
            raise ValueError("测点状态规则格式无效")
        kind = rule.get("type", "")
        if kind not in ("状态", "直接填充", "数值"):
            raise ValueError("目标单元格类型必须为运行状态或直接填充")
        values = {}
        seen = set()
        for key in STATUS_FIELDS:
            items = rule.get(key, [])
            if (not isinstance(items, list) or len(items) > 50
                    or any(not isinstance(item, str) or not item or len(item) > 200 for item in items)):
                raise ValueError("状态原始值必须为非空字符串数组")
            if len(set(items)) != len(items) or seen.intersection(items):
                raise ValueError("同一测点的状态原始值不能重复或重叠")
            seen.update(items)
            values[key] = list(items)
        intervals = _intervals(rule.get("intervals", []), values)
        source = rule.get("source_kind", "collected")
        is_local = bool(LOCAL_POINT_ID.fullmatch(point_id))
        if not isinstance(source, str) or (source not in LOCAL_SOURCES if is_local else source != "collected"):
            raise ValueError("测点来源与内部编号不一致")
        if kind in ("直接填充", "数值") and (seen or intervals):
            raise ValueError("直接填充不能设置状态映射")
        if is_local:
            if seen or intervals or kind != ("直接填充" if source == "fixed_value" else "状态"):
                raise ValueError("固定或手工巡检项不能设置采集值映射")
            values["source_kind"] = source
            if source == "fixed_status" and rule.get("fixed_value") in STATUS_SYMBOLS.values():
                values["fixed_value"] = rule["fixed_value"]
            elif source == "fixed_value" and (number := _decimal(rule.get("fixed_value"))) is not None:
                text = _decimal_text(number)
                if abs(number) > 10**15 or len(text.replace(".", "").lstrip("-0").rstrip("0")) > 15:
                    raise ValueError("固定数值最多支持15位有效数字，且绝对值不能超过10的15次方")
                values["fixed_value"] = text
            elif source != "manual_status" or rule.get("fixed_value") not in (None, ""):
                raise ValueError("请填写有效的固定值或固定状态")
        elif rule.get("fixed_value") not in (None, ""):
            raise ValueError("采集点不能设置固定值")
        if intervals:
            values["intervals"] = intervals
        normalized[point_id] = {"type": kind, **values}
    return normalized


def status_fields(rule, raw):
    if not rule:
        return {}
    result = {"测点类型": rule["type"], **{
        field: json.dumps(rule.get(key, []), ensure_ascii=False, separators=(",", ":"))
        for key, field in STATUS_FIELDS.items()
    }}
    source = rule.get("source_kind", "collected")
    if source != "collected":
        result[SOURCE_FIELD] = source
        if "fixed_value" in rule:
            result[FIXED_VALUE_FIELD] = rule["fixed_value"]
    if rule.get("intervals"):
        result[INTERVAL_FIELD] = json.dumps(rule["intervals"], ensure_ascii=False, separators=(",", ":"))
    if rule["type"] == "状态":
        status = "待核对"
        if source == "fixed_status":
            status = rule["fixed_value"]
        elif source == "collected" and raw is not None and str(raw).strip():
            status = next((symbol for key, symbol in STATUS_SYMBOLS.items()
                           if str(raw) in rule.get(key, [])), status)
            value = _decimal(raw, reading=True)
            if status == "待核对" and value is not None:
                for item in rule.get("intervals", ()):
                    if _decimal(item["min"]) <= value <= _decimal(item["max"]):
                        status = STATUS_SYMBOLS[item["status"]]
                        break
        result["状态判定"] = status
    return result
