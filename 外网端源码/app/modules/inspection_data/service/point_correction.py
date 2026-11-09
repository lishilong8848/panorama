from __future__ import annotations

import re
from decimal import Decimal


CORRECTION_RULE_FIELD = "数值修正规则"
CORRECTED_VALUE_FIELD = "测点修正值"
_DECIMAL = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)")
_READING = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]{1,3})?")


def _decimal(value, *, reading=False):
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        return None
    text = str(value).strip()
    if reading and re.fullmatch(r"[+-]?[0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?", text):
        text = text.replace(",", "")
    if len(text) > 48 or not (_READING if reading else _DECIMAL).fullmatch(text):
        return None
    return Decimal(text)


def _decimal_text(value):
    if not value:
        return "0"
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def normalize_correction_rules(rules, allowed_ids):
    if not isinstance(rules, dict) or set(rules) - set(allowed_ids):
        raise ValueError("数值修正规则必须对应已启用测点")
    result = {}
    for point_id, intervals in rules.items():
        if not isinstance(intervals, list) or len(intervals) > 20:
            raise ValueError("每个测点最多设置20条数值修正规则")
        parsed = []
        for interval in intervals:
            if not isinstance(interval, dict) or set(interval) != {"min", "max", "value"}:
                raise ValueError("请填写数值修正的最小值、最大值和修正值")
            lower, upper, replacement = (_decimal(interval[key]) for key in ("min", "max", "value"))
            if lower is None or upper is None or replacement is None or lower > upper:
                raise ValueError("数值修正必须为有效数字，最小值不能大于最大值")
            significant = _decimal_text(replacement).replace(".", "").lstrip("-0").rstrip("0")
            if len(significant) > 15 or abs(replacement) > 10**15:
                raise ValueError("修正值最多支持15位有效数字，且绝对值不能超过10的15次方")
            parsed.append((lower, upper, replacement))
        parsed.sort()
        if any(left[1] >= right[0] for left, right in zip(parsed, parsed[1:])):
            raise ValueError("同一测点的修正范围包含端点，不能重叠")
        if parsed:
            result[point_id] = [{"min": _decimal_text(lower), "max": _decimal_text(upper),
                                 "value": _decimal_text(replacement)} for lower, upper, replacement in parsed]
    return result


def corrected_samples(entry, rules):
    """Return only matched replacements. Preserve the source rows for audit."""
    if not rules:
        return {}
    corrections = {}
    for index, point_id in enumerate(entry.get("point_ids", ())):
        intervals = rules.get(point_id)
        if not intervals:
            continue
        observed = _decimal(entry["rows"][index][3], reading=True)
        if observed is None:
            continue
        for interval in intervals:
            if Decimal(interval["min"]) <= observed <= Decimal(interval["max"]):
                corrections[index] = interval["value"]
                break
    return corrections
