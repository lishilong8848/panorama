from copy import deepcopy

from scripts import preview_inspection_point_migration as migration


def _inputs(monkeypatch):
    monkeypatch.setattr("app.modules.inspection_data.service.point_settings.inspection_target_options",
                        lambda _: {"表": ("工作表",)})
    live = {}
    local = {}
    for scope in "DE":
        building = scope + "楼"
        live[scope] = {"config": [], "source": []}
        local[building] = {}
        for index, (profession, family) in enumerate(migration.FAMILY.items(), 1):
            point = f"{index}_0"
            fields = {
                "专业": profession, "测点ID": point, "测点名称": "温度", "测点路径": f"{scope}-123 / 温度",
                "基线测点": True, "启用": True, "用户主动启用": True, "测点备注": "飞书备注",
                "测点类型": "状态", "状态正常值": '["ok"]', "无关字段": "保留",
            }
            live[scope]["config"].append({"record_id": point, "fields": fields})
            live[scope]["source"].append({"fields": {**fields, "记录类型": "测点明细",
                                                      "设备编号": f"{scope}-123", "楼栋": building}})
            local[building][family] = {"points": {}, "remove": [], "targets": {}}
    plan = {scope: {"backfill": [], "manual_states": []} for scope in "DE"}
    for scope in plan:
        plan[scope]["backfill"].append({
            "scope": scope, "profession": "电气", "point_id": "1_0", "record_id": "1_0",
            "name": "温度", "device": f"{scope}-123", "expected": dict.fromkeys(migration.POSITION_FIELDS, ""),
            "fields": {**dict.fromkeys(migration.POSITION_FIELDS, ""), "目标巡检表": "表",
                       "目标工作表": "工作表", "目标单元格": "B6"},
        })
    return plan, live, local


def test_migration_preserves_existing_fields_and_isolates_building_mismatch(monkeypatch):
    plan, live, local = _inputs(monkeypatch)
    live["D"]["source"][0]["fields"]["设备编号"] = "D-124"
    before = deepcopy((plan, live, local))
    result = migration.preview(plan, live, local)
    assert result["buildings"]["D"]["counts"] == {"backfill:skip": 1}
    assert result["buildings"]["E"]["counts"] == {"backfill:ready": 1}
    e = result["buildings"]["E"]["operations"][0]
    assert set(e["fields"]) == migration.POSITION_FIELDS
    e_group = result["reference_settings_not_for_apply"]["E楼"][e["family"]]
    assert e_group["points"]["1_0"] == "飞书备注"
    assert e_group["status_rules"]["1_0"]["normal"] == ["ok"]
    assert e_group["explicitly_enabled"] == ["1_0"]
    assert (plan, live, local) == before


def test_migration_refuses_cloud_drift_and_target_collisions(monkeypatch):
    plan, live, local = _inputs(monkeypatch)
    live["D"]["config"][0]["fields"]["目标单元格"] = "G8"
    plan["E"]["manual_states"].append({
        "scope": "E", "profession": "暖通", "point_id": "local_" + "a" * 32,
        "device": "设备", "name": "状态",
        "target": {"workbook": "表", "sheet": "工作表", "cell": "B6",
                   "round_cells": {"白1": "B6", "白2": "B7", "夜1": "B8", "夜2": "B9"}},
    })
    result = migration.preview(plan, live, local)
    assert "飞书原坐标已变更" in result["buildings"]["D"]["operations"][0]["reason"]
    assert result["buildings"]["E"]["counts"] == {"backfill:skip": 1, "manual_states:skip": 1}


def test_dual_device_identity_requires_explicit_source_alias(monkeypatch):
    plan, live, local = _inputs(monkeypatch)
    item = plan["D"]["backfill"][0]
    item["device"] = "D-122"
    assert migration.preview(plan, live, local)["buildings"]["D"]["counts"] == {"backfill:skip": 1}
    del item["device"]
    item.update(display_device="D-122", source_device="D-123")
    confirmed = migration.preview(plan, live, local)["buildings"]["D"]["operations"][0]
    assert confirmed["status"] == "ready"
    assert confirmed["identity"]["display_device"] == "D-122"
    assert confirmed["identity"]["source_device"] == "D-123"
    live["D"]["config"][0]["fields"]["测点路径"] = "D-122 / 温度"
    assert migration.preview(plan, live, local)["buildings"]["D"]["counts"] == {"backfill:skip": 1}


def test_simulated_apply_updates_only_coordinates_and_adds_manual_status(monkeypatch):
    plan, live, local = _inputs(monkeypatch)
    manual_id = "local_" + "c" * 32
    plan["D"]["manual_states"].append({
        "scope": "D", "profession": "暖通", "point_id": manual_id,
        "device": "设备", "name": "状态",
        "target": {"workbook": "表", "sheet": "工作表", "cell": "C6",
                   "round_cells": {"白1": "C6", "白2": "C7", "夜1": "C8", "夜2": "C9"}},
    })
    before = deepcopy((plan, live, local))
    result = migration.simulate(plan, live, local)
    assert result["buildings"]["D"]["simulated_readback"] == {"updated": 1, "created": 1}
    assert result["buildings"]["E"]["simulated_readback"] == {"updated": 1, "created": 0}
    assert (plan, live, local) == before
