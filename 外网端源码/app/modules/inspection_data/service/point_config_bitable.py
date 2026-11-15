from __future__ import annotations

import json

from app.config.inspection_data_config import INSPECTION_ROUND_FIELDS, INSPECTION_SOURCE_CONTRACTS
from app.modules.inspection_data.service.chiller_operating_points import CONTROL_GROUPS, OPERATING_ROLE_FIELD
from app.modules.inspection_data.service.point_correction import CORRECTION_RULE_FIELD, normalize_correction_rules
from app.modules.inspection_data.service.point_status import (
    FIXED_VALUE_FIELD, INTERVAL_FIELD, LOCAL_POINT_ID, SOURCE_FIELD, STATUS_FIELDS, normalize_status_rules, status_fields,
)


POINT_CONFIG_TABLES = {"D楼": "tblU0q6LvNjpMIN1", "E楼": "tblrUhk1NAmTg1a2"}
EXPLICIT_ENABLE_FIELD = "用户主动启用"
_FIELD_TYPES = {
    "专业": 1, "测点ID": 1, "测点名称": 1, "测点路径": 1, "基线测点": 7, "启用": 7,
    "测点备注": 1, "目标巡检表": 1, "目标工作表": 1, "目标单元格": 1,
    **dict.fromkeys(INSPECTION_ROUND_FIELDS.values(), 1),
    "测点类型": 1, **dict.fromkeys(STATUS_FIELDS.values(), 1),
}


def _text(value):
    if isinstance(value, list):
        return "".join(str(item.get("text", "")) for item in value if isinstance(item, dict))
    return str(value or "")


def _index(records):
    rows, blank_ids = {}, []
    record_ids = set()
    for row in records:
        fields = row.get("fields", {})
        record_id = row.get("record_id")
        key = _text(fields.get("专业")), _text(fields.get("测点ID"))
        if not record_id or record_id in record_ids:
            raise ValueError("飞书巡检配置表记录 ID 缺失或重复")
        record_ids.add(record_id)
        if not any(fields.values()):
            blank_ids.append(record_id)
            continue
        if not all(key) or key in rows:
            raise ValueError("飞书巡检配置表存在无标识或重复的记录，未写入")
        rows[key] = row
    return rows, blank_ids


_OPTIONAL_FIELDS = (SOURCE_FIELD, FIXED_VALUE_FIELD, INTERVAL_FIELD)


def _expected_rows(groups, *, include_explicit_enable=False, include_corrections=False, include_operating_roles=False, optional_fields=()):
    desired = {}
    for family, group in groups.items():
        discipline = INSPECTION_SOURCE_CONTRACTS[family]["discipline"]
        baseline, points, remove = group.get("baseline", {}), group["points"], set(group["remove"])
        roles = {point_id: f"{role}:{number}" for role, controls in group.get("operating_rules", {}).items()
                 for number, point_id in controls.items()}
        for point_id in sorted(set(baseline) | set(points) | remove):
            point = baseline.get(point_id, {})
            target = group.get("targets", {}).get(point_id, {})
            desired[discipline, point_id] = {
                "专业": discipline, "测点ID": point_id,
                "测点名称": points.get(point_id, "") if LOCAL_POINT_ID.fullmatch(point_id) else point.get("name", ""),
                "测点路径": point.get("path", ""), "基线测点": point_id in baseline,
                "启用": point_id not in remove, "测点备注": points.get(point_id, ""),
                "目标巡检表": target.get("workbook", ""), "目标工作表": target.get("sheet", ""),
                "目标单元格": target.get("cell", ""),
            }
            if include_explicit_enable:
                if point_id in group.get("explicitly_enabled", ()) and point_id not in remove:
                    desired[discipline, point_id][EXPLICIT_ENABLE_FIELD] = True
                elif point_id in remove or point_id in group.get("explicitly_disabled", ()):
                    desired[discipline, point_id][EXPLICIT_ENABLE_FIELD] = False
            if include_operating_roles:
                desired[discipline, point_id][OPERATING_ROLE_FIELD] = roles.get(point_id, "")
            status = group.get("status_rules", {}).get(point_id, {})
            metadata = status_fields(status, None)
            desired[discipline, point_id].update({field: metadata.get(field, "") for field in optional_fields})
            if include_corrections:
                rules = group.get("correction_rules", {}).get(point_id)
                desired[discipline, point_id][CORRECTION_RULE_FIELD] = json.dumps(rules, separators=(",", ":")) if rules else ""
            desired[discipline, point_id].update({"测点类型": status.get("type", ""), **{
                field: json.dumps(status.get(key, []), ensure_ascii=False, separators=(",", ":")) if status else ""
                for key, field in STATUS_FIELDS.items()
            }})
            if "round_cells" in target:
                desired[discipline, point_id].update({field: target["round_cells"].get(key, "")
                                                       for key, field in INSPECTION_ROUND_FIELDS.items()})
            elif point_id in remove or (point_id in points and not target):
                desired[discipline, point_id].update(dict.fromkeys(INSPECTION_ROUND_FIELDS.values(), ""))
    return desired


def _different(observed, desired):
    return {field: value for field, value in desired.items()
            if value != (observed.get(field) is True if isinstance(value, bool) else _text(observed.get(field)))}


def point_config_client(config):
    from app.modules.inspection_data.service.inspection_data_upload_service import InspectionDataUploadService

    return InspectionDataUploadService(config).make_bitable_client()


def _record_snapshot(rows):
    return json.dumps(sorted(rows, key=lambda row: row["record_id"]), ensure_ascii=False, sort_keys=True)


def read_building_point_records(client, building):
    return client.list_records(POINT_CONFIG_TABLES[building], max_records=0, require_complete=True)


def read_point_config_bitable(config, *, client=None, buildings=None, snapshots=None):
    client = client or point_config_client(config)
    families = {contract["discipline"]: family for family, contract in INSPECTION_SOURCE_CONTRACTS.items()}
    settings = {}
    requested = tuple(POINT_CONFIG_TABLES if buildings is None else buildings)
    for building in requested:
        if snapshots is not None:
            rows = snapshots[building]
        elif buildings is None:
            rows = client.list_records(POINT_CONFIG_TABLES[building], max_records=0)
        else:
            rows = read_building_point_records(client, building)
        current, _ = _index(rows)
        groups = {}
        for (discipline, point_id), row in current.items():
            if discipline not in families:
                raise ValueError(f"{building} 飞书巡检测点配置专业无效")
            fields = row["fields"]
            if LOCAL_POINT_ID.fullmatch(point_id):
                if fields.get("启用") is not True:
                    continue
                if fields.get("基线测点") is True:
                    raise ValueError(f"{building} 本地巡检项不能标为采集基线")
            group = groups.setdefault(families[discipline], {"points": {}, "remove": [], "baseline": {}})
            if fields.get("基线测点") is True:
                group["baseline"][point_id] = {"name": _text(fields.get("测点名称")), "path": _text(fields.get("测点路径"))}
            if fields.get("启用") is not True:
                group["remove"].append(point_id)
                continue
            if fields.get(EXPLICIT_ENABLE_FIELD) is True:
                group.setdefault("explicitly_enabled", []).append(point_id)
            if role_text := _text(fields.get(OPERATING_ROLE_FIELD)):
                role, _, number = role_text.partition(":")
                if role not in CONTROL_GROUPS or number not in "123456" or len(number) != 1:
                    raise ValueError(f"{building} 冷机表联动控制字段无效")
                roles = group.setdefault("operating_rules", {}).setdefault(role, {})
                if number in roles:
                    raise ValueError(f"{building} 冷机表联动控制重复")
                roles[number] = point_id
            target = {key: _text(fields.get(field)) for key, field in
                      (("workbook", "目标巡检表"), ("sheet", "目标工作表"), ("cell", "目标单元格"))}
            rounds = {key: _text(fields.get(field)) for key, field in INSPECTION_ROUND_FIELDS.items()}
            if any(rounds.values()):
                target["round_cells"] = rounds
            note = _text(fields.get("测点备注"))
            kind = _text(fields.get("测点类型"))
            encoded = {key: _text(fields.get(field)) for key, field in STATUS_FIELDS.items()}
            if kind or any(encoded.values()):
                try:
                    rule = {"type": kind, **{key: json.loads(value) if value else [] for key, value in encoded.items()}}
                    if source := _text(fields.get(SOURCE_FIELD)):
                        rule["source_kind"] = source
                    if fixed := _text(fields.get(FIXED_VALUE_FIELD)):
                        rule["fixed_value"] = fixed
                    if intervals := _text(fields.get(INTERVAL_FIELD)):
                        rule["intervals"] = json.loads(intervals)
                    normalized = normalize_status_rules({point_id: rule}, {point_id})
                except (ValueError, TypeError, json.JSONDecodeError) as exc:
                    raise ValueError(f"{building} 测点 {point_id} 状态规则无效") from exc
                group.setdefault("status_rules", {}).update(normalized)
            correction = _text(fields.get(CORRECTION_RULE_FIELD))
            if correction:
                try:
                    rules = normalize_correction_rules({point_id: json.loads(correction)}, {point_id})
                except (ValueError, TypeError) as exc:
                    raise ValueError(f"{building} 测点 {point_id} 数值修正规则无效") from exc
                group.setdefault("correction_rules", {}).update(rules)
            if point_id not in group["baseline"] or note or any(target.values()):
                group["points"][point_id] = note
            if any(target.values()):
                group.setdefault("targets", {})[point_id] = target
        if groups:
            settings[building] = groups
    if (settings or buildings is not None) and (set(settings) != set(requested) or any(
            set(groups) != set(INSPECTION_SOURCE_CONTRACTS) or any(not group["baseline"] for group in groups.values())
            for groups in settings.values())):
        raise ValueError("飞书巡检测点基线不完整，未恢复配置")
    return settings


def sync_point_config_bitable(config, settings, *, buildings=None, initialize=False, client=None,
                              prior_settings=None, snapshots=None):
    client = client or point_config_client(config)
    guarded = snapshots is not None
    requested = tuple(POINT_CONFIG_TABLES if buildings is None else buildings)
    plans = {}
    for building in requested:
        table = POINT_CONFIG_TABLES[building]
        definitions = client.list_fields(table)
        existing = {f["field_name"]: f["type"] for f in definitions}
        explicit_type = existing.get(EXPLICIT_ENABLE_FIELD)
        if explicit_type is not None and explicit_type != 7:
            raise ValueError(f"{building} 飞书配置表的{EXPLICIT_ENABLE_FIELD}必须是复选框")
        if explicit_type is None and any(group.get("explicitly_enabled") or group.get("explicitly_disabled")
                                         for group in settings.get(building, {}).values()):
            raise ValueError(f"{building} 飞书配置表缺少复选框字段{EXPLICIT_ENABLE_FIELD}，主动启用待同步")
        primary = next((f["field_name"] for f in definitions if f.get("is_primary") and f["type"] == 1), "")
        missing = {k: t for k, t in _FIELD_TYPES.items() if k not in existing}
        has_operating = any(group.get("operating_rules") for group in settings.get(building, {}).values())
        operating_type = existing.get(OPERATING_ROLE_FIELD)
        if operating_type not in (None, 1):
            raise ValueError(f"{building} 飞书配置表的{OPERATING_ROLE_FIELD}必须是文本")
        if has_operating and operating_type is None:
            missing[OPERATING_ROLE_FIELD] = 1
        has_corrections = any(group.get("correction_rules") for group in settings.get(building, {}).values())
        correction_type = existing.get(CORRECTION_RULE_FIELD)
        if correction_type not in (None, 1):
            raise ValueError(f"{building} 飞书配置表的{CORRECTION_RULE_FIELD}必须是文本")
        if has_corrections and correction_type is None:
            missing[CORRECTION_RULE_FIELD] = 1
        if not primary or any(existing.get(k, t) != t for k, t in _FIELD_TYPES.items()) or (missing and not initialize):
            raise ValueError(f"{building} 飞书巡检测点配置表字段不匹配")
        optional_fields = {field for field in _OPTIONAL_FIELDS if field in existing}
        for group in settings.get(building, {}).values():
            for rule in group.get("status_rules", {}).values():
                optional_fields.update(set(status_fields(rule, None)) & set(_OPTIONAL_FIELDS))
        if any(existing.get(field, 1) != 1 for field in optional_fields):
            raise ValueError(f"{building} 飞书巡检配置扩展字段必须是文本")
        missing.update({field: 1 for field in optional_fields if field not in existing})
        if guarded:
            current, blank = _index(snapshots[building])
        else:
            current, blank = _index(client.list_records(table, max_records=0))
        desired = _expected_rows(settings.get(building, {}), include_explicit_enable=explicit_type == 7,
                                 include_corrections=has_corrections or correction_type == 1,
                                 include_operating_roles=has_operating or operating_type == 1,
                                 optional_fields=sorted(optional_fields))
        if not desired:
            raise ValueError(f"{building} 没有可保存的测点配置")
        previous_rows = (_expected_rows(prior_settings[building], include_explicit_enable=explicit_type == 7,
                                        include_corrections=has_corrections or correction_type == 1,
                                        include_operating_roles=has_operating or operating_type == 1,
                                        optional_fields=sorted(optional_fields)) if guarded else {})
        plans[building] = (table, primary, missing, current, blank, desired, previous_rows)

    result = {}
    for building, (table, primary, missing, current, blank, desired, previous_rows) in plans.items():
        if guarded and _record_snapshot(read_building_point_records(client, building)) != _record_snapshot(snapshots[building]):
            from app.modules.inspection_data.service.point_settings import PointSettingsConflict

            raise PointSettingsConflict(f"{building}飞书测点配置已变更，请重新加载后再保存")
        for field, field_type in missing.items():
            url = client.LIST_FIELD_URL.format(app_token=client.app_token, table_id=table)
            client._post_json(url, {"field_name": field, "type": field_type})
        creates, updates, verify = [], [], {}
        for key, fields in desired.items():
            payload = {primary: f"{key[0]} / {key[1]}", **fields}
            previous = current.get(key)
            if previous:
                if guarded:
                    before = previous_rows.get(key, {})
                    # Clear a formerly managed value when the new configuration omits it.
                    cleared = {field: False if isinstance(value, bool) else ""
                               for field, value in before.items() if field not in fields}
                    delta = {field: value for field, value in {**cleared, **fields}.items()
                             if value != before.get(field, False if isinstance(value, bool) else "")}
                    changes = _different(previous["fields"], delta)
                else:
                    changes = _different(previous["fields"], payload)
                if changes:
                    updates.append({"record_id": previous["record_id"], "fields": changes})
                    verify[key] = changes
            elif blank and not guarded:
                updates.append({"record_id": blank.pop(0), "fields": payload})
            else:
                creates.append(payload)
                verify[key] = fields
        for key in (previous_rows.keys() if guarded else current.keys()) - desired.keys():
            previous = current[key]
            if previous["fields"].get("启用") is True:
                updates.append({"record_id": previous["record_id"], "fields": {"启用": False}})
                verify[key] = {"启用": False}
        if creates:
            client.batch_create_records(table, creates)
        if updates:
            client.batch_update_records(table, updates)
        if guarded:
            rows = read_building_point_records(client, building)
            actual = _index(rows)[0]
            if any(key not in actual or _different(actual[key]["fields"], row) for key, row in verify.items()):
                raise RuntimeError(f"{building} 飞书巡检测点配置写入后回查不一致")
            result[building] = rows
            continue
        actual = _index(client.list_records(table, max_records=0))[0] if creates or updates else current
        if (any(key not in actual or _different(actual[key]["fields"], row) for key, row in desired.items())
                or any(actual[key]["fields"].get("启用") is True for key in actual.keys() - desired.keys())):
            raise RuntimeError(f"{building} 飞书巡检测点配置写入后回查不一致")
        result[building] = {"table_id": table, "count": len(desired), "created": len(creates), "updated": len(updates)}
    return result
