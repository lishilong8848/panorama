from __future__ import annotations

import json


STATUS_FIELDS = {
    "normal": "状态正常值",
    "abnormal": "状态异常值",
    "not_running": "状态未运行值",
}
STATUS_SYMBOLS = {"normal": "√", "abnormal": "×", "not_running": "△"}


def normalize_status_rules(rules, allowed_ids):
    if not isinstance(rules, dict) or set(rules) - set(allowed_ids):
        raise ValueError("状态规则必须对应已启用测点")
    normalized = {}
    for point_id, rule in rules.items():
        if not isinstance(rule, dict) or set(rule) - {"type", *STATUS_FIELDS}:
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
        if kind in ("直接填充", "数值") and seen:
            raise ValueError("直接填充不能设置状态映射")
        normalized[point_id] = {"type": kind, **values}
    return normalized


def status_fields(rule, raw):
    if not rule:
        return {}
    result = {"测点类型": rule["type"], **{
        field: json.dumps(rule.get(key, []), ensure_ascii=False, separators=(",", ":"))
        for key, field in STATUS_FIELDS.items()
    }}
    if rule["type"] == "状态":
        result["状态判定"] = next((symbol for key, symbol in STATUS_SYMBOLS.items()
                                  if raw is not None and str(raw) in rule.get(key, [])), "待核对")
    return result
