from __future__ import annotations

import re
from datetime import datetime, time, timedelta, timezone
from hashlib import sha256
from pathlib import Path

from app.config.inspection_data_config import INSPECTION_ROUND_FIELDS
from app.modules.inspection_data.service.point_correction import _decimal, _decimal_text
from app.shared.utils.report_source_json import load_report_source_json
from handover_log_module.core.chiller_mode_resolver import DEFAULT_VALUE_MAP, _normalize_mode_code


TZ = timezone(timedelta(hours=8))
SHEET = "暖通冷机系统巡检表"
WORKBOOKS = {
    "D楼": "21V-JSNTFOC-RM-423 南通数据中心D栋暖通巡检表单(2024.11.30）.xlsx",
    "E楼": "21V-JSNTFOC-RM-423 南通数据中心E栋暖通巡检表单(2024年最新版) (2).xlsx",
}
MODE_ROWS = {"D楼": ("制冷", "预冷", "板换"), "E楼": ("板换", "制冷", "预冷")}
MODE_SOURCE = "inspection_handover_mode"
PRESSURE_SOURCE = "inspection_handover_pressure"
MODE_FAMILY = "chiller_mode_switch_family"
PRESSURE_EDITS = "_inspection_pressure_edits"
PRESSURE_TOUCHED = "_inspection_pressure_touched"
PRESSURE_CONFIRMATION = "inspection_pressure_confirmation"
PRESSURE_LAYOUT = {
    "rows": ("", "冷却泵", "BCDEFG", (24, 25)),
    "primary_rows": ("primary:", "冷冻泵", "BCDEFG", (17, 19)),
    "secondary_rows": ("secondary:", "二次泵", "KLMNOP", (24, 25)),
}
PRESSURE_FIELDS = ("inlet_pressure", "outlet_pressure")
MODE_LABEL = re.compile(r"([1-6])(?:号|#|＃)冷机(?:运行)?模式")
VALID_MODES = set(DEFAULT_VALUE_MAP.values())


def public_review_document(document):
    return {key: value for key, value in (document or {}).items()
            if key not in (PRESSURE_EDITS, PRESSURE_TOUCHED)}


def _stamp(value) -> datetime | None:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.strip().replace("/", "-"))
        except ValueError:
            return None
    else:
        return None
    return result.astimezone(TZ) if result.tzinfo is not None else result.replace(tzinfo=TZ)


def pressure_round(when: datetime) -> tuple[str, str, int] | None:
    """Lighthouse slots start at 09:30, 12:00, 20:00 and 00:00 Beijing time."""
    local = when.astimezone(TZ)
    hour = local.time()
    day = local.date()
    if time(9, 30) <= hour < time(12):
        return day.isoformat(), "day", 1
    if time(12) <= hour < time(18):
        return day.isoformat(), "day", 2
    if hour >= time(20):
        return day.isoformat(), "night", 1
    if hour < time(9):
        return (day - timedelta(days=1)).isoformat(), "night", 2
    return None


def _pressure_text(value) -> str | None:
    if isinstance(value, str):
        value = re.sub(r"\s*mPa$", "", value.strip(), flags=re.IGNORECASE)
    parsed = _decimal(value)
    result = _decimal_text(parsed) if parsed is not None else None
    return result if result is not None and len(result) <= 24 else None


def _pressure_cells(payload) -> dict:
    """Resolve equipment numbering, never infer a pump number from a review row's position."""
    if not isinstance(payload, dict):
        return {}
    result = {}
    duplicates = set()
    for group, (prefix, pump, columns, rows) in PRESSURE_LAYOUT.items():
        records = payload.get(group)
        if not isinstance(records, list):
            continue
        for record in records:
            if not isinstance(record, dict):
                continue
            unit, zone = record.get("unit"), record.get("zone")
            if type(unit) is not int or not 1 <= unit <= 6 or zone not in ("west", "east"):
                continue
            if group != "secondary_rows" and zone != ("west" if unit <= 3 else "east"):
                continue
            if record.get("row_id") != f"{prefix}{zone}:{unit}":
                continue
            for field, row, direction in zip(PRESSURE_FIELDS, rows, ("进水", "出水")):
                address = f"{columns[unit - 1]}{row}"
                if address in result:
                    duplicates.add(address)
                result[address] = (f"{unit}号{pump}", f"{direction}压力(MPa)", zone,
                                   f"{record['row_id']}/{field}", record.get(field))
    return {cell: item for cell, item in result.items() if cell not in duplicates}


def record_pressure_edits(session, previous, document, baseline, *, touched, now=None):
    """Persist provenance with the same document revision as the review edit."""
    document.pop(PRESSURE_EDITS, None)
    when = now or datetime.now(TZ)
    slot = pressure_round(when)
    if session.get("building") not in WORKBOOKS or not slot:
        return
    if slot[:2] != (session.get("duty_date"), session.get("duty_shift")):
        return
    existing = previous.get(PRESSURE_EDITS) or {}
    edits = dict(existing.get("cells", {})) if existing.get("slot") == list(slot) else {}
    if touched:
        old = _pressure_cells(baseline)
        incoming = _pressure_cells(document.get("cooling_pump_pressures") or {})
        for address, (device, metric, zone, field, value) in incoming.items():
            edit = touched.get(field)
            if not isinstance(edit, dict) or edit.get("value") != value:
                continue
            sampled = _stamp(edit.get("at"))
            prior = old.get(address)
            if (sampled is None or sampled > when or pressure_round(sampled) != slot
                    or prior is None or prior[:4] != (device, metric, zone, field)):
                continue
            current = _pressure_text(value)
            if current is None:
                edits.pop(address, None)
                continue
            edits[address] = {"value": current, "edited_at": sampled.isoformat(),
                              "device": device, "metric": metric, "zone": zone}
        for address in edits.keys() - incoming.keys():
            edits.pop(address)
    if edits:
        document[PRESSURE_EDITS] = {"slot": list(slot), "cells": edits}


def _point_row(building, address, source, device, metric, value, raw, sampled, note):
    stable_key = f"{building}/{SHEET}/{address}/{source}"
    return {
        "记录类型": "测点明细", "楼栋": building, "专业": "暖通", "位置": "冷冻站",
        "巡检表单": WORKBOOKS[building], "设备编号": device, "测点名称": metric,
        "测点ID": "local_" + sha256(stable_key.encode()).hexdigest()[:32],
        "测点来源": source, "测点路径": f"冷冻站 / {device} / {metric}",
        "测点值": value, "测点原始值": str(raw), "采样时间": int(sampled.timestamp() * 1000),
        "测点备注": note, "数据完整性": "完整",
        "目标巡检表": WORKBOOKS[building], "目标工作表": SHEET, "目标单元格": address,
        **dict.fromkeys(INSPECTION_ROUND_FIELDS.values(), address),
    }


def pressure_rows(building, session, document, *, now):
    if not session or not session.get("confirmed") or session.get("building") != building:
        return []
    confirmed = _stamp(session.get("pressure_confirmed_at"))
    slot = pressure_round(now)
    if not confirmed or confirmed > now or not slot or pressure_round(confirmed) != slot:
        return []
    if slot[:2] != (session.get("duty_date"), session.get("duty_shift")):
        return []
    evidence = document.get(PRESSURE_EDITS) or {}
    if evidence.get("slot") != list(slot):
        return []
    current = _pressure_cells(document.get("cooling_pump_pressures") or {})
    result = []
    for address, entry in evidence.get("cells", {}).items():
        if address not in current:
            continue
        device, metric, zone, _, raw = current[address]
        value = _pressure_text(raw)
        sampled = _stamp(entry.get("edited_at"))
        if (not sampled or not sampled <= confirmed or pressure_round(sampled) != slot
                or value is None or value != entry.get("value")
                or (device, metric, zone) != (entry.get("device"), entry.get("metric"), entry.get("zone"))):
            continue
        note = f"交接班审核确认｜{slot[0]} {slot[1]}{slot[2]}｜录入 {sampled:%H:%M:%S}｜确认 {confirmed:%H:%M:%S}"
        result.append(_point_row(building, address, PRESSURE_SOURCE, device, metric, value, raw, sampled, note))
    return result


def mode_observations(rows, *, start, end):
    """Use the actual report data columns and keep the newest observation per unit."""
    if len(rows) < 4:
        return {}
    columns = {index: stamp for index, label in enumerate(rows[1][4:], 4)
               if (stamp := _stamp(label)) is not None and start <= stamp <= end}
    observations = {}
    for row in rows[3:]:
        if len(row) <= 4:
            continue
        name = re.sub(r"\s+", "", str(row[3] or ""))
        match = MODE_LABEL.fullmatch(name)
        if match is None:
            continue
        unit = int(match[1])
        for index, stamp in columns.items():
            if index >= len(row):
                continue
            observations.setdefault((unit, stamp), set()).add(row[index])
    return observations


def mode_rows(building, observations, value_map, occupied):
    rows = []
    for unit, column in enumerate("KLMNOP", 1):
        timestamps = [sampled for number, sampled in observations if number == unit]
        if not timestamps or any(f"{column}{line}" in occupied for line in (7, 8, 9)):
            continue
        sampled = max(timestamps)
        readings = observations[unit, sampled]
        try:
            decoded = {value_map.get(_normalize_mode_code(value, value_map)) for value in readings}
        except (OverflowError, ValueError):
            continue
        if len(decoded) != 1 or (mode := decoded.pop()) not in VALID_MODES:
            continue
        for row, label in enumerate(MODE_ROWS[building], 7):
            value = "√" if mode == label else "/"
            rows.append(_point_row(
                building, f"{column}{row}", MODE_SOURCE, f"{unit}号制冷单元",
                {"制冷": "冷机模式", "预冷": "预冷模式", "板换": "自然冷模式"}[label],
                value, "/".join(sorted(map(str, readings))), sampled, f"制冷单元模式切换参数｜{mode}",
            ))
    return rows


def _source_observations(bridge, config, hvac, log, *, family=MODE_FAMILY, extract=mode_observations):
    # Paths are resolved under the external host's configured share, never the inner server's drive.
    shared = config.get("shared_bridge") or {}
    root = shared.get("external_root_dir") or shared.get("root_dir")
    if not root:
        return {}
    if bridge is None:
        from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient

        bridge = InternalBridgeHttpClient.from_runtime_config(config)
    if not hasattr(bridge, "source_index_batch"):
        return {}
    buildings = list(hvac)
    queries = [{"source_family": family, "building": building, "status": "ready", "limit": 6}
               for building in buildings]
    try:
        index_results = bridge.source_index_batch(queries)
    except Exception as exc:
        log(f"[巡检数据处理] {family} 暂不可用: {type(exc).__name__}")
        return {}

    if not isinstance(index_results, list):
        return {}
    try:
        base = Path(root).resolve()
    except (OSError, RuntimeError):
        return {}
    output = {}
    for building, result in zip(buildings, index_results):
        window = hvac[building]
        start, end = _stamp(window["query_start"]), _stamp(window["query_end"])
        target = output.setdefault(building, {})
        if not isinstance(result, dict):
            continue
        entries = result.get("entries")
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if (not isinstance(entry, dict) or entry.get("building") != building
                    or entry.get("source_family") != family):
                continue
            relative = Path(str(entry.get("relative_path") or "").replace("\\", "/"))
            if not relative.parts or relative.anchor or ".." in relative.parts or relative.suffix.lower() != ".json":
                continue
            if any(":" in part for part in relative.parts):
                continue
            try:
                resolved_path = (base / relative).resolve(strict=True)
                if not resolved_path.is_relative_to(base):
                    continue
                doc = load_report_source_json(resolved_path, expected_family=family,
                                              expected_building=building)
                lower, upper = max(start, _stamp(doc["query_start"])), min(end, _stamp(doc["query_end"]))
                for key, values in extract(doc["sheets"][0]["rows"], start=lower, end=upper).items():
                    target.setdefault(key, set()).update(values)
            except (ValueError, OSError) as exc:
                log(f"[巡检数据处理] {building} {family} 源文件不可读: {type(exc).__name__}")
    return output


def record_pressure_confirmation(config, session, document_revision, *, now=None):
    """Record an explicit review confirmation for the document version that was prepared."""
    when = now or datetime.now(TZ)
    building = session.get("building")
    slot = pressure_round(when)
    if (building not in WORKBOOKS or not session.get("confirmed") or not slot
            or slot[:2] != (session.get("duty_date"), session.get("duty_shift"))):
        return False
    from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore

    store = ReviewBuildingDocumentStore(config=config, building=building)
    state = store.get_document(session["session_id"]) or {}
    if state.get("revision") != document_revision or state.get("source_excel_path") != session.get("output_file"):
        return False
    document = state.get("document") or {}
    if not pressure_rows(building, {**session, "pressure_confirmed_at": when.isoformat()}, document, now=when):
        return False
    store.set_default(PRESSURE_CONFIRMATION, {
        "session_id": session["session_id"], "session_revision": session["revision"],
        "document_revision": document_revision, "confirmed_at": when.isoformat(),
    })
    from app.modules.inspection_data.service.point_settings import request_inspection_upload_retrigger

    request_inspection_upload_retrigger(
        {"paths": {"runtime_state_root": str(store.db_path.parent.parent)}},
        reason=f"{building}巡检压力审核确认", force=True,
    )
    return True


def _confirmed_pressure_rows(config, building, now):
    slot = pressure_round(now)
    if not slot:
        return []
    from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore
    from handover_log_module.service.review_session_service import ReviewSessionService

    service = ReviewSessionService(config, initialize_generated_file_index=False)
    session = service.get_latest_session_for_context_fast(building=building, duty_date=slot[0], duty_shift=slot[1])
    if not session or not session.get("confirmed"):
        return []
    store = ReviewBuildingDocumentStore(config=config, building=building)
    receipt = store.get_default(PRESSURE_CONFIRMATION) or {}
    if receipt.get("session_id") != session["session_id"] or receipt.get("session_revision") != session["revision"]:
        return []
    state = store.get_document(session["session_id"])
    if (not state or state.get("source_excel_path") != session.get("output_file")
            or state.get("revision") != receipt.get("document_revision")):
        return []
    latest = service.get_latest_session_for_context_fast(building=building, duty_date=slot[0], duty_shift=slot[1])
    # Recheck confirmation after reading the document. A reopened/changed review is not a receipt.
    if not latest or any(latest.get(key) != session.get(key) for key in
                         ("session_id", "revision", "confirmed", "confirmed_at", "output_file")):
        return []
    return pressure_rows(building, {**session, "pressure_confirmed_at": receipt["confirmed_at"]},
                         state.get("document") or {}, now=now)


def handover_inspection_rows(runtime_config, bridge, hvac, building_rows, log, *, now=None,
                              pressure_failures=None, skip_buildings=()):
    from handover_log_module.api.facade import load_handover_config

    hvac = {building: doc for building, doc in hvac.items() if building not in skip_buildings}
    if not hvac:
        return {}
    current = now or datetime.now(TZ)
    observed = _source_observations(bridge, runtime_config, hvac, log)
    try:
        config = load_handover_config(runtime_config)
    except ValueError as exc:
        log(f"[巡检数据处理] 交接班附加配置不可用: {type(exc).__name__}")
        return {}
    raw_map = (config.get("chiller_mode") or {}).get("value_map", DEFAULT_VALUE_MAP)
    value_map = {str(key): str(value) for key, value in raw_map.items()}
    result = {}
    for building, rows in building_rows.items():
        if building in skip_buildings:
            continue
        taken = set()
        for row in rows:
            if (row.get("专业") != "暖通" or row.get("目标巡检表") != WORKBOOKS[building]
                    or str(row.get("目标工作表", "")).strip() != SHEET or row.get("测点值") in ("", None)):
                continue
            taken.update(str(row.get(field) or "").strip()
                         for field in ("目标单元格", *INSPECTION_ROUND_FIELDS.values()))
        extra = mode_rows(building, observed.get(building, {}), value_map, taken)
        end = _stamp(hvac[building]["query_end"])
        # Older snapshots may have no fields for the active round. The consumer independently
        # filters the supplemental row by its actual edit timestamp and the sealed batch time.
        if ((config.get("_global_paths") or {}).get("runtime_state_root") and end
                and end.date() == current.astimezone(TZ).date()):
            try:
                extra.extend(row for row in _confirmed_pressure_rows(config, building, current)
                             if row["目标单元格"] not in taken)
            except Exception as exc:
                if pressure_failures is not None:
                    pressure_failures.append(building)
                log(f"[巡检数据处理] {building} 同轮审核压力暂不可用: {type(exc).__name__}")
        result[building] = extra
    return result
