from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

from app.modules.inspection_data.service.handover_inspection_inputs import (
    MODE_ROWS, WORKBOOKS, _point_row, _source_observations, _stamp,
)
from handover_log_module.core.chiller_mode_resolver import DEFAULT_VALUE_MAP, _normalize_mode_code


OPERATING_SOURCE = "inspection_operating"
OPERATING_ROLE_FIELD = "冷机表联动控制"
HVAC_FAMILY = "hvac_inspection_family"
CONTROL_GROUPS = ("unit_modes", "secondary_frequencies")
UNIT_COLUMNS = "BCDEFG"
SECONDARY_COLUMNS = "KLMNOP"
CHILLER_NUMERIC_ROWS = (4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15)
CAPACITY_FAMILY = "handover_capacity_report_family"
D_PUMP_NAME = re.compile(r"([1-6])#(冷冻泵|冷却泵|二次泵)频率反馈(?:\(主\))?")
E_LOCAL_PUMP_NAME = re.compile(r"E-(124|150)-DDC-10([1-3])_(一次冷冻泵|冷却泵)频率反馈")
E_SECONDARY_PUMP_NAME = re.compile(r"E-(124|150)-DDC-105_冷冻水二次泵频率反馈SCHWP-([1-6])")
PUMP_NAMES = {"冷冻泵": "冷冻一次泵", "一次冷冻泵": "冷冻一次泵", "冷却泵": "冷却泵", "二次泵": "冷冻二次泵"}


def normalize_operating_rules(building, family, rules, active_ids):
    if not isinstance(rules, dict) or set(rules) - set(CONTROL_GROUPS):
        raise ValueError("冷机表联动配置无效")
    if not rules:
        return {}
    if building not in WORKBOOKS or family != HVAC_FAMILY:
        raise ValueError("冷机表联动仅支持 D/E 楼暖通测点")
    output = {}
    used = set()
    for name in CONTROL_GROUPS:
        group = rules.get(name, {})
        if not isinstance(group, dict):
            raise ValueError("冷机表联动的测点清单无效")
        normalized = {}
        for number, point_id in group.items():
            if (not isinstance(number, str) or not re.fullmatch(r"[1-6]", number)
                    or not isinstance(point_id, str) or not re.fullmatch(r"[0-9]+(?:_[0-9]+){1,15}", point_id)
                    or point_id not in active_ids or point_id in used):
                raise ValueError("冷机表联动须选用本楼启用中的动环测点，每个测点只绑定一台设备")
            normalized[number] = point_id
            used.add(point_id)
        output[name] = normalized
    return output if used else {}


def _frequency_running(value):
    try:
        number = Decimal(value.strip().removesuffix("Hz").strip())
    except (AttributeError, InvalidOperation):
        return None
    if not number.is_finite():
        return None
    return number > 10


def pump_identity(name):
    if match := D_PUMP_NAME.fullmatch(name):
        return "D楼", PUMP_NAMES[match[2]], match[1]
    if match := E_LOCAL_PUMP_NAME.fullmatch(name):
        return "E楼", PUMP_NAMES[match[3]], str(int(match[2]) + (3 if match[1] == "124" else 0))
    if match := E_SECONDARY_PUMP_NAME.fullmatch(name):
        if (int(match[2]) >= 4) == (match[1] == "124"):
            return "E楼", "冷冻二次泵", match[2]
    return None


def pump_observations(rows, *, start, end):
    if len(rows) < 4:
        return {}
    columns = {i: stamp for i, label in enumerate(rows[1][4:], 4)
               if (stamp := _stamp(label)) is not None and start <= stamp <= end}
    output = {}
    controller = ""
    for row in rows[3:]:
        if len(row) <= 4:
            continue
        if row[1]:
            controller = ""
        if row[2]:
            controller = str(row[2]).strip()
        name = str(row[3] or "").strip()
        if (identity := pump_identity(name)) is None:
            continue
        building, equipment, number = identity
        room = "124" if int(number) >= 4 else "150"
        if building == "D楼":
            index = 105 if equipment == "冷冻二次泵" else 100 + (int(number) - 1) % 3 + 1
            if not re.search(rf"(?<![A-Z])D-{room}-BAS-{index}(?:[^0-9]|$)", controller):
                continue
        elif controller != name.split("_", 1)[0]:
            continue
        for i, stamp in columns.items():
            if i < len(row):
                value = row[i]
                output.setdefault((identity, stamp), set()).add("" if value is None else str(value).strip())
    return output


def report_operating_controls(bridge, config, hvac, log):
    modes = _source_observations(bridge, config, hvac, log)
    pumps = _source_observations(bridge, config, hvac, log, family=CAPACITY_FAMILY, extract=pump_observations)
    controls = {}
    for building, doc in hvac.items():
        stamp = _stamp(doc["value_time"])
        control = {"modes": {}, "pumps": {}}
        for (number, when), values in modes.get(building, {}).items():
            if when == stamp and len(values) == 1:
                control["modes"][str(number)] = next(iter(values))
        for ((scope, pump, number), when), values in pumps.get(building, {}).items():
            if scope == building and when == stamp and len(values) == 1:
                control["pumps"][pump, number] = next(iter(values))
        if control["modes"] or control["pumps"]:
            controls[building] = control
    return controls


def operation_rows(building, sampled_at, points, rules, reports=None):
    rules = rules or {}
    reports = reports or {}
    if (sampled := _stamp(sampled_at)) is None:
        return []
    readings, pump_samples = {}, {}
    for point in points:
        if point.get("楼栋") != building:
            continue
        point_id = point["测点ID"]
        raw = point.get("测点原始值", "")
        readings[point_id] = "" if point_id in readings else raw
        identity = pump_identity(point.get("测点名称", ""))
        if identity and identity[0] == building:
            pump_samples.setdefault(identity[1:], set()).add(raw)
    if not (rules or reports or pump_samples):
        return []
    output = []
    for number, left, right in zip("123456", UNIT_COLUMNS, SECONDARY_COLUMNS):
        source = rules.get("unit_modes", {}).get(number)
        raw = readings.get(source, "") or reports.get("modes", {}).get(number, "")
        try:
            mode = DEFAULT_VALUE_MAP.get(_normalize_mode_code(raw, DEFAULT_VALUE_MAP))
        except (OverflowError, ValueError):
            mode = None
        running = mode in MODE_ROWS[building]
        status = "√" if running else "△" if mode == "停机" else ""
        unit_name = f"{number}号制冷单元"
        marks = {f"{right}4": status, f"{right}5": "√"}
        marks.update({f"{right}{row}": ("√" if mode == label else "/") if mode else ""
                      for row, label in enumerate(MODE_ROWS[building], 7)})
        if not running:
            blank = "/" if mode == "停机" else ""
            marks.update({f"{left}{row}": blank for row in CHILLER_NUMERIC_ROWS})
            marks.update({f"{right}{row}": blank for row in range(10, 16)})
            marks[f"{left}4"] = status
        for cell, value in marks.items():
            note = "冷塔远程控制固定填写" if cell == f"{right}5" else (
                f"制冷单元模式 · {source or '本轮动环报表'} · {mode or '待核对'}")
            output.append(_point_row(building, cell, OPERATING_SOURCE, unit_name, note, value, raw, sampled, note))

        for pump, column, state_row, pressure_rows in (
            ("冷冻一次泵", left, 21, (17, 19)),
            ("冷却泵", left, 26, (24, 25)),
            ("冷冻二次泵", right, 26, (24, 25)),
        ):
            source = rules.get("secondary_frequencies", {}).get(number) if pump == "冷冻二次泵" else None
            observed = pump_samples.get((pump, number), set())
            raw = readings.get(source, "") or (next(iter(observed)) if len(observed) == 1 else "")
            if not raw and len(observed) <= 1:
                raw = reports.get("pumps", {}).get((pump, number), "")
            pump_running = _frequency_running(raw)
            status = "√" if pump_running is True else "△" if pump_running is False else ""
            marks = {f"{column}{state_row}": status}
            if pump_running is not True:
                marks.update({f"{column}{row}": "/" if pump_running is False else "" for row in pressure_rows})
            note = f"{pump}频率反馈 · {source or '本轮动环报表'} · {raw or '待核对'}"
            for cell, value in marks.items():
                output.append(_point_row(building, cell, OPERATING_SOURCE, f"{number}号{pump}",
                                         note, value, raw, sampled, note))
    return output
