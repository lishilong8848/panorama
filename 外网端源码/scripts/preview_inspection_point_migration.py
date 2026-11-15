"""Offline migration preview. No network, settings writes, or apply mode."""
import argparse
import copy
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config.inspection_data_config import INSPECTION_ROUND_FIELDS, INSPECTION_SOURCE_CONTRACTS
from app.modules.inspection_data.service.point_config_bitable import (
    POINT_CONFIG_TABLES, _different, _expected_rows, _index, _text, read_point_config_bitable,
)
from app.modules.inspection_data.service.point_settings import _normalize_targets
from app.modules.inspection_data.service.point_status import LOCAL_POINT_ID, SOURCE_FIELD, normalize_status_rules


TARGET_FIELDS = {"workbook": "目标巡检表", "sheet": "目标工作表", "cell": "目标单元格"}
ROUND_LABELS = dict(zip(INSPECTION_ROUND_FIELDS, ("白1", "白2", "夜1", "夜2")))
POSITION_FIELDS = set(TARGET_FIELDS.values()) | set(INSPECTION_ROUND_FIELDS.values())
FAMILY = {contract["discipline"]: family for family, contract in INSPECTION_SOURCE_CONTRACTS.items()}


def _as_target(fields):
    target = {key: _text(fields.get(field)) for key, field in TARGET_FIELDS.items()}
    rounds = {key: _text(fields.get(field)) for key, field in INSPECTION_ROUND_FIELDS.items()}
    if any(rounds.values()):
        target["round_cells"] = rounds
    return target if any(target.values()) else {}


def _slots(target):
    cells = set(target.get("round_cells", {}).values()) - {""}
    if not cells and target.get("cell"):
        cells.add(target["cell"])
    return {(target.get("workbook"), target.get("sheet"), cell.strip().upper()) for cell in cells}


def _contains_device(text, device):
    return bool(device and re.search(r"(?<![A-Za-z0-9])" + re.escape(device) + r"(?![A-Za-z0-9])", text, re.I))


def _view(settings, local):
    for building, groups in local.items():
        for family, group in groups.items():
            current = settings.setdefault(building, {}).setdefault(family, {})
            for field, value in group.items():
                if isinstance(value, dict):
                    merged = current.setdefault(field, {})
                    for key, item in value.items():
                        merged.setdefault(key, copy.deepcopy(item))
                elif field not in current:
                    current[field] = copy.deepcopy(value)
                elif field in {"remove", "explicitly_enabled", "explicitly_disabled"}:
                    current[field] = sorted(set(value) | set(current[field]))
    return settings


def _target(kind, item, building):
    point_id = item["point_id"]
    if kind == "backfill":
        proposed = item["fields"]
        if set(proposed) != POSITION_FIELDS or any(not isinstance(value, str) for value in proposed.values()):
            raise ValueError("坐标字段不完整或包含非坐标字段")
        target = _as_target(proposed)
    else:
        target = {**item["target"], "round_cells": {
            key: item["target"]["round_cells"][label] for key, label in ROUND_LABELS.items()
        }}
    return _normalize_targets(building, {point_id}, {point_id: target})[point_id]


def manual_fields(family, profession, point_id, note, target, rule):
    return _expected_rows({family: {
        "points": {point_id: note}, "remove": [],
        "targets": {point_id: target}, "status_rules": {point_id: rule},
    }}, optional_fields=(SOURCE_FIELD,))[profession, point_id]


def preview(plan, live, local, *, resume=True):
    class SnapshotClient:
        def list_records(self, table, **_):
            return live[table_scope[table]]["config"]

    table_scope = {table: building[0] for building, table in POINT_CONFIG_TABLES.items()}
    cloud = read_point_config_bitable({}, client=SnapshotClient())
    merged = _view(copy.deepcopy(cloud), local)
    result = {}
    for scope in ("D", "E"):
        building = f"{scope}楼"
        cfg, _ = _index(live[scope]["config"])
        observed = defaultdict(list)
        for record in live[scope]["source"]:
            fields = record["fields"]
            if _text(fields.get("记录类型")) == "测点明细" and _text(fields.get("楼栋")) == building:
                observed[_text(fields.get("专业")), _text(fields.get("测点ID"))].append(fields)
        occupied = defaultdict(set)
        for settings in (cloud, local):
            for profession, family in FAMILY.items():
                group = settings.get(building, {}).get(family, {})
                for point_id, target in group.get("targets", {}).items():
                    for slot in _slots(target):
                        occupied[slot].add((profession, point_id))
        counts = Counter((item.get("profession"), item.get("point_id"))
                         for name in ("backfill", "manual_states") for item in plan[scope][name])
        manual_counts = Counter(item.get("point_id") for item in plan[scope]["manual_states"])
        proposals = defaultdict(set)
        for kind in ("backfill", "manual_states"):
            for item in plan[scope][kind]:
                try:
                    for slot in _slots(_target(kind, item, building)):
                        proposals[slot].add((item["profession"], item["point_id"]))
                except (KeyError, TypeError, ValueError):
                    continue
        rows = []
        for kind in ("backfill", "manual_states"):
            for item in plan[scope][kind]:
                profession, point_id = key = (item.get("profession"), item.get("point_id"))
                row = {"kind": kind, "profession": profession, "point_id": point_id}
                rows.append(row)
                try:
                    if item.get("scope") != scope or profession not in FAMILY or counts[key] != 1:
                        raise ValueError("楼栋、专业或清单身份冲突")
                    family = FAMILY[profession]
                    current = local.get(building, {}).get(family, {})
                    group = merged[building][family]
                    saved = cfg.get(key, {})
                    fields = saved.get("fields", {})
                    unchanged = False
                    if point_id in current.get("remove", ()) or point_id in current.get("explicitly_disabled", ()):
                        raise ValueError("本机测点已排除或关闭")
                    if kind == "backfill":
                        if not saved or saved.get("record_id") != item.get("record_id") or fields.get("启用") is not True:
                            raise ValueError("飞书记录 ID 不匹配或点位未启用")
                        samples = observed[key]
                        display_device = item.get("display_device") or item["device"]
                        source_device = item.get("source_device") or item.get("device") or display_device
                        row["identity"] = {
                            "display_device": display_device, "source_device": source_device,
                            "expected_name": item["name"],
                            "config_name": _text(fields.get("测点名称")),
                            "config_path": _text(fields.get("测点路径")),
                            "samples": [{"device": _text(sample.get("设备编号")), "name": _text(sample.get("测点名称"))}
                                        for sample in samples],
                        }
                        if (len(samples) != 1 or _text(fields.get("测点名称")) != item["name"]
                                or _text(samples[0].get("测点名称")) != item["name"]
                                or not _contains_device(_text(samples[0].get("设备编号")), source_device)
                                or not _contains_device(_text(fields.get("测点路径")), source_device)):
                            raise ValueError("配置与采集的名称或设备身份不匹配")
                        expected, proposed = item["expected"], item["fields"]
                        if set(expected) != POSITION_FIELDS or any(value != "" for value in expected.values()):
                            raise ValueError("回填仅接受原坐标全部留空的记录")
                        actual = {field: _text(fields.get(field)) for field in POSITION_FIELDS}
                        if actual != expected and actual != proposed:
                            raise ValueError("飞书原坐标已变更")
                        unchanged = actual == proposed
                        patch = {"record_id": saved["record_id"], "expected": expected, "fields": proposed}
                    else:
                        if (not isinstance(point_id, str) or not LOCAL_POINT_ID.fullmatch(point_id)
                                or manual_counts[point_id] != 1
                                or any(pid == point_id and prof != profession for prof, pid in cfg)
                                or (not saved and any(point_id in g.get("points", {}) for g in merged[building].values()))):
                            raise ValueError("手工项 ID 无效、重复或已存在")
                        note = " / ".join(filter(None, (item["device"], item["name"])))
                        if not note or len(note) > 500:
                            raise ValueError("手工项名称无效")
                        rule = normalize_status_rules({point_id: {"type": "状态", "source_kind": "manual_status"}}, {point_id})
                        patch = {"expected_absent": True, "note": note, "status_rule": rule[point_id]}
                    target = _target(kind, item, building)
                    if kind == "manual_states" and saved:
                        if not resume or _different(fields, manual_fields(
                            family, profession, point_id, note, target, rule[point_id],
                        )):
                            raise ValueError("手工项 ID 已存在且与计划不一致")
                        unchanged = True
                    previous = current.get("targets", {}).get(point_id)
                    if previous and _normalize_targets(building, {point_id}, {point_id: previous})[point_id] != target:
                        raise ValueError("本机已有不同坐标")
                    if any((occupied[slot] | proposals[slot]) - {key} for slot in _slots(target)):
                        raise ValueError("目标格与现有配置或其他清单项冲突")
                    group.setdefault("targets", {})[point_id] = target
                    if kind == "manual_states":
                        group.setdefault("points", {})[point_id] = note
                        group.setdefault("status_rules", {}).update(rule)
                    else:
                        group.setdefault("points", {}).setdefault(point_id, "")
                    row.update(status="unchanged" if unchanged else "ready",
                               family=family, target=target, **patch)
                except (ValueError, KeyError, TypeError) as exc:
                    row.update(status="skip", reason=str(exc))
        result[scope] = {"counts": dict(Counter(f"{row['kind']}:{row['status']}" for row in rows)), "operations": rows}
    return {"buildings": result, "reference_settings_not_for_apply": merged}


def simulate(plan, live, local):
    """Plan and replay changes on an isolated snapshot, without a write client."""
    result = preview(plan, live, local)
    staged = copy.deepcopy(live)
    for scope, building_result in result["buildings"].items():
        rows, _ = _index(staged[scope]["config"])
        updates, creates = {}, {}
        for op in building_result["operations"]:
            if op["status"] != "ready":
                continue
            key = (op["profession"], op["point_id"])
            if op["kind"] == "backfill":
                row = rows[key]
                if row["record_id"] != op["record_id"] or any(
                    _text(row["fields"].get(field)) != value for field, value in op["expected"].items()
                ):
                    raise ValueError("模拟前发现巡检原坐标已变更")
                updates[key] = op["fields"]
                row["fields"].update(op["fields"])
            else:
                point_id = op["point_id"]
                patch = manual_fields(op["family"], *key, op["note"], op["target"], op["status_rule"])
                if key in rows:
                    raise ValueError("模拟前发现手工项已存在")
                creates[key] = patch
                staged[scope]["config"].append({"record_id": "simulation_" + point_id, "fields": dict(patch)})
        readback, _ = _index(staged[scope]["config"])
        original, _ = _index(live[scope]["config"])
        if readback.keys() != original.keys() | creates.keys() or any(
            readback[key] != {**row, "fields": {**row["fields"], **updates.get(key, {})}}
            for key, row in original.items()
        ) or any(readback[key]["fields"] != patch for key, patch in creates.items()):
            raise ValueError("模拟回读不一致")
        building_result["simulated_readback"] = {"updated": len(updates), "created": len(creates)}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("plan", "live", "local", "output"):
        parser.add_argument(f"--{name}", required=True, type=Path)
    parser.add_argument("--simulate", action="store_true", help="Replay the incremental patch in memory only.")
    args = parser.parse_args()
    inputs = {name: getattr(args, name).read_bytes() for name in ("plan", "live", "local")}
    decoded = {name: json.loads(raw) for name, raw in inputs.items()}
    local = decoded["local"]
    run = simulate if args.simulate else preview
    result = run(decoded["plan"], decoded["live"], local.get("settings", local))
    result["input_sha256"] = {name: hashlib.sha256(raw).hexdigest() for name, raw in inputs.items()}
    result["local_revision"] = local.get("revision")
    result["preview_only"] = True
    # Exclusive creation prevents overwriting a runtime settings file or any previous report.
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
    print(json.dumps({scope: data["counts"] for scope, data in result["buildings"].items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
