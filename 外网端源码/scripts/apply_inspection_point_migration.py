"""Maintenance-only incremental inspection mapping migration."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config.config_adapter import adapt_runtime_config
from app.config.runtime_role import forced_role_mode_from_env
from app.config.settings_loader import load_pipeline_config
from app.modules.inspection_data.service.inspection_data_upload_service import (
    INSPECTION_BATCH_PREFIX, InspectionDataUploadService, validate_inspection_batch,
)
from app.modules.inspection_data.service.point_config_bitable import (
    POINT_CONFIG_TABLES, _FIELD_TYPES, _different, _index, _text, read_point_config_bitable,
)
from app.modules.inspection_data.service.point_settings import (
    _normalize_targets, _path, _pending_buildings, _save_settings, _settings_snapshot,
    _settings_write_lock, _validate_effective_count, _validate_local_points, _validate_target_collisions,
)
from app.modules.inspection_data.service.point_status import SOURCE_FIELD, STATUS_FIELDS
from scripts.preview_inspection_point_migration import manual_fields, preview


def _complete_rows(client, table):
    rows = client.list_records(table, page_size=500, max_records=0)
    ids = client.list_record_ids(table, page_size=500, require_complete=True)
    observed = [row.get("record_id") for row in rows]
    if len(observed) != len(set(observed)) or set(observed) != set(ids):
        raise RuntimeError(f"{table} 记录读取不完整或读取期间已变化")
    return sorted(copy.deepcopy(rows), key=lambda row: row["record_id"])


def _config_rows(client):
    return {
        building[0]: _complete_rows(client, table)
        for building, table in POINT_CONFIG_TABLES.items()
    }


def _source_rows(client, table, building):
    rows = _complete_rows(client, table)
    for row in rows:
        remark = _text(row.get("fields", {}).get("备注"))
        if remark.startswith(INSPECTION_BATCH_PREFIX):
            try:
                upload_time = json.loads(remark[len(INSPECTION_BATCH_PREFIX):])["upload_time"]
            except (KeyError, TypeError, ValueError):
                break
            marker = validate_inspection_batch(rows, building=building, upload_time=upload_time)
            if marker:
                return rows, marker
            break
    raise ValueError(f"{building} 没有完整的巡检采集批次：完成标记、数量或记录 ID 摘要校验失败")


def _normalized_settings(rows, local, patches):
    table_scopes = {table: building[0] for building, table in POINT_CONFIG_TABLES.items()}

    class Readback:
        def list_records(self, table, **_kwargs):
            return rows[table_scopes[table]]

    settings = read_point_config_bitable({}, client=Readback())
    if not settings:
        raise ValueError("巡检配置没有有效基线")
    for building, groups in settings.items():
        for family, group in groups.items():
            old = local.get(building, {}).get(family, {})
            old_targets = _normalize_targets(building, set(old.get("targets", {})), old.get("targets", {}))
            if "targets" in group:
                group["targets"] = _normalize_targets(building, group["points"], group["targets"])
            migrated_ids = patches.get((building, family), set())
            if any(group.get("targets", {}).get(pid) != target for pid, target in old_targets.items()
                   if pid not in migrated_ids):
                raise ValueError(f"{building}/{family} 本地已有不同的非迁移坐标")
            for point_id, point in old.get("baseline", {}).items():
                identity = group["baseline"].get(point_id)
                if identity is None or any(point.get(key) != identity.get(key) for key in ("name", "path")):
                    raise ValueError(f"{building}/{family} 本地基线设备名称或路径与飞书不一致：{point_id}")
                extra_keys = set(point) - {"name", "path", "value", "sampleTime"}
                if extra_keys:
                    raise ValueError(f"{building}/{family} 基线点含无法识别的本地缓存字段：{point_id}")
                identity.update({key: point[key] for key in ("value", "sampleTime") if key in point})
            for field in ("points", "status_rules", "correction_rules"):
                if any(group.get(field, {}).get(pid, "" if field == "points" else None) != value
                       for pid, value in old.get(field, {}).items()):
                    raise ValueError(f"{building}/{family} 本地配置与飞书的 {field} 不一致")
            for field in ("remove", "explicitly_enabled"):
                if set(old.get(field, ())) - set(group.get(field, ())):
                    raise ValueError(f"{building}/{family} 本地启用状态与飞书不一致")
            if disabled := old.get("explicitly_disabled"):
                if set(disabled) - set(group["remove"]):
                    raise ValueError(f"{building}/{family} 本地停用状态与飞书不一致")
                group["explicitly_disabled"] = sorted(set(disabled))
            unknown = set(old) - set(group)
            if any(old[key] for key in unknown if key not in {"targets", "explicitly_enabled", "status_rules", "correction_rules"}):
                raise ValueError(f"{building}/{family} 有无法迁移的本地配置字段")
            group["remove"].sort()
            _validate_effective_count(group["baseline"], group["points"], group["remove"])
            _validate_local_points(group["points"], group.get("targets", {}), group.get("status_rules", {}),
                                   group.get("correction_rules", {}))
        if set(local.get(building, {})) - set(groups):
            raise ValueError(f"{building} 存在未识别的本地专业")
        _validate_target_collisions(groups)
    if set(local) - set(settings):
        raise ValueError("存在未识别的本地楼栋")
    return settings


def _table_schema(client, table, *, needs_manual_source):
    fields = client.list_fields(table)
    types = {row["field_name"]: row["type"] for row in fields}
    primary = [row["field_name"] for row in fields if row.get("is_primary") and row["type"] == 1]
    if len(primary) != 1 or primary[0] in _FIELD_TYPES or primary[0] == SOURCE_FIELD:
        raise ValueError(f"{table} 未找到独立的文本主字段")
    additive_text = {"测点类型", *STATUS_FIELDS.values(), SOURCE_FIELD}
    required = {**_FIELD_TYPES, **({SOURCE_FIELD: 1} if needs_manual_source else {})}
    wrong_type = [field for field, kind in required.items() if field in types and types[field] != kind]
    missing = [field for field in required if field not in types]
    if wrong_type or set(missing) - additive_text:
        raise ValueError(f"{table} 必需字段缺失或类型不匹配：{wrong_type or missing}")
    computed = {name for name, kind in types.items() if kind in {19, 20, 1001, 1002, 1003, 1004, 1005}}
    return primary[0], missing, computed


def migrate(config, plan, *, expected_revision, expected_settings_path, client, source_tables, apply=False):
    """Read and validate both buildings under the normal settings write lock."""
    role = config.get("deployment", {}).get("role_mode")
    if role != "external" or forced_role_mode_from_env() not in {"", "external"}:
        raise ValueError("只允许在外网端运行迁移")
    settings_path = _path(config).resolve(strict=True)
    if settings_path != Path(expected_settings_path).resolve(strict=True):
        raise ValueError("运行配置路径与指定的正式快照不一致")
    with _settings_write_lock(config):
        local, revision = _settings_snapshot(config)
        if not local or revision != expected_revision:
            raise ValueError("本地配置修订号不匹配")
        if _pending_buildings(config):
            raise ValueError("存在未同步到飞书的本地配置，禁止迁移")
        if set(plan) != {"D", "E"} or any(not plan[scope]["backfill"] for scope in plan):
            raise ValueError("需要完整的 D/E 双楼迁移清单")
        before = _config_rows(client)
        sources = {scope: _source_rows(client, source_tables[f"{scope}楼"], f"{scope}楼") for scope in before}
        live = {
            scope: {"config": rows, "source": sources[scope][0]}
            for scope, rows in before.items()
        }
        report = preview(plan, live, local, resume=True)["buildings"]
        rejected = [(scope, row["point_id"], row["reason"]) for scope, data in report.items()
                    for row in data["operations"] if row["status"] == "skip"]
        if rejected:
            raise ValueError(f"迁移预检有 {len(rejected)} 条冲突：{rejected[:5]}")
        staged = copy.deepcopy(before)
        changes, patched_points = {}, {}
        for scope, data in report.items():
            building = f"{scope}楼"
            table = POINT_CONFIG_TABLES[building]
            operations = data["operations"]
            primary, add_fields, computed = _table_schema(client, table, needs_manual_source=any(
                op["kind"] == "manual_states" for op in operations))
            rows, _ = _index(staged[scope])
            updates, creates = [], {}
            for op in operations:
                key = op["profession"], op["point_id"]
                patched_points.setdefault((building, op["family"]), set()).add(op["point_id"])
                if op["status"] != "ready":
                    continue
                if op["kind"] == "backfill":
                    updates.append({"record_id": op["record_id"], "fields": op["fields"]})
                    rows[key]["fields"].update(op["fields"])
                else:
                    fields = {primary: f"{key[0]} / {key[1]}", **manual_fields(
                        op["family"], *key, op["note"], op["target"], op["status_rule"],
                    )}
                    creates[key] = fields
                    staged[scope].append({"record_id": "preview_" + key[1], "fields": fields})
            changes[scope] = (table, add_fields, updates, creates, computed)
        next_settings = _normalized_settings(staged, local, patched_points)
        summary = {
            scope: {"updates": len(updates), "creates": len(creates), "add_fields": add_fields}
            for scope, (_, add_fields, updates, creates, _) in changes.items()
        }
        if not apply:
            return {"applied": False, "revision": revision, "buildings": summary}

        # The local lock does not cover a user editing Feishu directly.
        # All external writers must be paused during this maintenance operation.
        if _config_rows(client) != before:
            raise ValueError("飞书配置在预检期间已变化，未执行迁移")
        if any(_source_rows(client, source_tables[f"{scope}楼"], f"{scope}楼") != source
               for scope, source in sources.items()):
            raise ValueError("巡检源表在预检期间已变化，未执行迁移")
        for table, add_fields, updates, creates, _ in changes.values():
            url = client.LIST_FIELD_URL.format(app_token=client.app_token, table_id=table)
            for field in add_fields:
                client._post_json(url, {"field_name": field, "type": 1})
            if add_fields:
                actual_types = {row["field_name"]: row["type"] for row in client.list_fields(table)}
                if any(actual_types.get(field) != 1 for field in add_fields):
                    raise RuntimeError(f"{table} 新增字段尚未回读，本地配置未更新")
            if updates:
                client.batch_update_records(table, updates)
            for key, fields in creates.items():
                token = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{client.app_token}/{table}/{key[0]}/{key[1]}"))
                client.create_record(table, fields, client_token=token)

        actual = _config_rows(client)
        for scope, (_, _, updates, creates, computed) in changes.items():
            original_rows, _ = _index(before[scope])
            expected_rows, expected_blank = _index(staged[scope])
            actual_rows, actual_blank = _index(actual[scope])
            if expected_rows.keys() != actual_rows.keys() or set(expected_blank) != set(actual_blank):
                raise RuntimeError(f"{scope} 楼飞书迁移回读记录集合不一致，本地配置未更新")
            by_record_id = {update["record_id"]: update["fields"] for update in updates}
            for key, row in expected_rows.items():
                observed = actual_rows[key]
                written = creates.get(key, by_record_id.get(row["record_id"], {}))
                if (_different(observed["fields"], written) or
                        (key not in creates and observed["record_id"] != row["record_id"])):
                    raise RuntimeError(f"{scope} 楼飞书迁移回读不一致：{key[1]}，本地配置未更新")
                if key in original_rows:
                    ignored = computed | set(written)
                    def protected(fields):
                        return {k: v for k, v in fields.items() if k not in ignored}
                    if protected(observed["fields"]) != protected(original_rows[key]["fields"]):
                        raise RuntimeError(f"{scope} 楼测点 {key[1]} 未迁移字段已变化，本地配置未更新")
        if _normalized_settings(actual, local, patched_points) != next_settings:
            raise RuntimeError("飞书回读不能复原预期的本地配置")
        if _settings_snapshot(config) != (local, revision) or _pending_buildings(config):
            raise RuntimeError("本地配置在迁移期间已变化，未覆盖本地快照")
        if next_settings != local:
            _save_settings(config, next_settings)
        return {"applied": True, "revision": _settings_snapshot(config)[1], "buildings": summary}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--settings-path", required=True, type=Path)
    parser.add_argument("--expected-revision", required=True, type=int)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--all-writers-stopped", action="store_true")
    args = parser.parse_args()
    if args.apply and not args.all_writers_stopped:
        parser.error("--apply requires --all-writers-stopped")
    plan_bytes = args.plan.read_bytes()
    plan_sha256 = hashlib.sha256(plan_bytes).hexdigest()
    if args.apply and args.expected_plan_sha256 != plan_sha256:
        parser.error("--apply requires the matching SHA-256 from the reviewed plan")
    config_path = args.config.resolve(strict=True)
    if config_path.parent != Path.cwd():
        parser.error("run from the deployment directory containing --config")
    config = adapt_runtime_config(load_pipeline_config(config_path))
    service = InspectionDataUploadService(config)
    result = migrate(
        config, json.loads(plan_bytes), expected_revision=args.expected_revision,
        expected_settings_path=args.settings_path, client=service.make_bitable_client(),
        source_tables=service.cfg["tables"], apply=args.apply,
    )
    print(json.dumps({"plan_sha256": plan_sha256, **result}, ensure_ascii=False))


if __name__ == "__main__":
    main()
