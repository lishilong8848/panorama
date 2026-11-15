from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.modules.inspection_data.service import point_config_bitable as cloud
from app.modules.inspection_data.service import point_settings as settings
from app.modules.scheduler.api.inspection_data_upload_routes import router


D, E = "D楼", "E楼"
ELECTRICAL, HVAC = "electrical_inspection_family", "hvac_inspection_family"
BASE_POINT, EXTRA_POINT = "7_0_1", "7_0_2"


class FakeConfigBitable:
    app_token = "isolated"
    LIST_FIELD_URL = "{app_token}/{table_id}"

    def __init__(self):
        self.rows = {
            table: [
                {"record_id": f"{table}-{index}", "fields": {
                    "专业": discipline, "测点ID": BASE_POINT, "基线测点": True,
                    "启用": True, "测点名称": f"{building}{discipline}基线", "测点路径": "源路径",
                }}
                for index, discipline in enumerate(("电气", "暖通"))
            ]
            for building, table in cloud.POINT_CONFIG_TABLES.items()
        }
        self.columns = dict(cloud._FIELD_TYPES, 主键=1, 用户主动启用=7)
        self.fail_read = set()
        self.read = []
        self.writes = []
        self.before_list_fields = None
        self.fail_update = False

    def list_records(self, table, *, max_records, require_complete=False):
        assert max_records == 0
        self.read.append(table)
        if table in self.fail_read:
            raise RuntimeError("配置表读取超时")
        return deepcopy(self.rows[table])

    def list_fields(self, table):
        if self.before_list_fields is not None:
            self.before_list_fields()
        return [{"field_name": field, "type": kind, "is_primary": field == "主键"}
                for field, kind in self.columns.items()]

    def batch_create_records(self, table, payload):
        self.writes.append((table, deepcopy(payload)))
        for row in payload:
            self.rows[table].append({"record_id": f"{table}-{len(self.rows[table])}", "fields": deepcopy(row)})

    def batch_update_records(self, table, payload):
        if self.fail_update:
            raise RuntimeError("写入超时")
        self.writes.append((table, deepcopy(payload)))
        for item in payload:
            row = next(row for row in self.rows[table] if row["record_id"] == item["record_id"])
            row["fields"].update(deepcopy(item["fields"]))

    def fields(self, building, discipline="电气", point_id=BASE_POINT):
        return next(row["fields"] for row in self.rows[cloud.POINT_CONFIG_TABLES[building]]
                    if row["fields"]["专业"] == discipline and row["fields"]["测点ID"] == point_id)


@pytest.fixture
def setup(tmp_path, monkeypatch):
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    client = FakeConfigBitable()
    bridge = Mock()
    bridge.put_inspection_point_rules.side_effect = lambda rules, revision: {
        "ok": True, "point_overrides": rules, "revision": revision,
    }
    monkeypatch.setattr(cloud, "point_config_client", lambda _: client)
    monkeypatch.setattr(settings.InternalBridgeHttpClient, "from_runtime_config", lambda _: bridge)
    return config, client, bridge


def target(building, cell):
    workbook, sheets = next((book, sheets) for book, sheets in settings.inspection_target_options(building).items()
                            if "柴发巡检表" in sheets)
    return {"workbook": workbook, "sheet": "柴发巡检表", "cell": cell}


def change_on_cloud(client, building, point_id, cell):
    wanted = target(building, cell)
    table = cloud.POINT_CONFIG_TABLES[building]
    client.rows[table].append({"record_id": f"new-{building}", "fields": {
        "专业": "电气", "测点ID": point_id, "启用": True, "测点备注": "从飞书新增",
        "目标巡检表": wanted["workbook"], "目标工作表": wanted["sheet"], "目标单元格": cell,
    }})
    return wanted


def test_cloud_edit_wins_over_local_and_schedules_verified_full_rules(setup):
    config, client, bridge = setup
    settings._save_settings(config, {D: {ELECTRICAL: {"points": {"9_99": "旧本地"}, "remove": []}}})
    expected_target = change_on_cloud(client, E, EXTRA_POINT, "B6")
    result, revision, errors = settings.refresh_cloud_point_settings(config)
    assert not errors
    assert result[D][ELECTRICAL]["points"] == {}
    assert result[E][ELECTRICAL]["targets"][EXTRA_POINT] == expected_target
    assert client.writes == []
    assert settings.read_inspection_upload_retrigger(config)["revision"] == revision

    # A worker forwards the same cloud snapshot to the old full-replacement bridge.
    settings.sync_point_settings(config, result)
    rules = bridge.put_inspection_point_rules.call_args.args[0]
    assert rules[D][ELECTRICAL]["add"] == []
    assert rules[E][ELECTRICAL]["add"] == [EXTRA_POINT]
    assert settings.refresh_cloud_point_settings(config)[1] == revision


def test_save_writes_only_changed_cloud_fields_and_never_touches_other_building(setup):
    config, client, bridge = setup
    before, _, _ = settings.refresh_cloud_point_settings(config)
    client.fields(D, "暖通")["测点备注"] = "多维表直接修改"
    client.fields(D)["人工列"] = "保留"
    client.read.clear()
    updated = settings.save_point_group(
        config, building=D, family=ELECTRICAL, points={BASE_POINT: "审核页编辑"}, remove=[],
        expected_group=before[D][ELECTRICAL],
    )
    table = cloud.POINT_CONFIG_TABLES[D]
    assert updated["synced"] and updated["cloud_synced"]
    assert client.writes == [(table, [{"record_id": f"{table}-0", "fields": {"测点备注": "审核页编辑"}}])]
    assert set(client.read) == {table}
    assert updated["settings"][D][HVAC]["points"][BASE_POINT] == "多维表直接修改"
    assert client.fields(D)["人工列"] == "保留"
    assert bridge.put_inspection_point_rules.call_args.args[0][E] == settings.query_rules(before)[E]


def test_pending_local_and_conflicting_cloud_edit_cannot_overwrite(setup):
    config, client, _ = setup
    before, _, _ = settings.refresh_cloud_point_settings(config)
    client.before_list_fields = lambda: client.fields(D).update({"测点备注": "并发飞书修改"})
    with pytest.raises(settings.PointSettingsConflict):
        settings.save_point_group(
            config, building=D, family=ELECTRICAL, points={BASE_POINT: "旧编辑"}, remove=[],
            expected_group=before[D][ELECTRICAL],
        )
    client.before_list_fields = None
    assert client.writes == []
    fresh, _, _ = settings.refresh_cloud_point_settings(config)
    assert fresh[D][ELECTRICAL]["points"][BASE_POINT] == "并发飞书修改"
    client.fail_update = True
    with pytest.raises(settings.PointConfigUnavailable):
        settings.save_point_group(
            config, building=D, family=ELECTRICAL, points={BASE_POINT: "写入失败"}, remove=[],
            expected_group=fresh[D][ELECTRICAL],
        )
    assert settings.load_point_settings(config)[D] == fresh[D]
    assert client.writes == []


def test_one_building_failure_does_not_apply_legacy_cache_or_block_other_page(setup):
    config, client, bridge = setup
    d_table = cloud.POINT_CONFIG_TABLES[D]
    client.fail_read.add(d_table)
    settings._save_settings(config, {D: {ELECTRICAL: {"points": {"9_99": "旧本地"}, "remove": []}}})
    app = FastAPI()
    app.include_router(router)
    app.state.container = SimpleNamespace(runtime_config=config,
                                          _configured_deployment_snapshot=lambda: {"role_mode": "external"},
                                          trigger_inspection_data_upload_now=lambda: {"ok": True})
    url = "/api/scheduler/inspection-data-upload/points"
    with TestClient(app) as api:
        assert api.get(f"{url}/d").status_code == 503
        expected = api.get(f"{url}/e").json()["settings"][ELECTRICAL]
        result = api.put(f"{url}/e", json={"family": ELECTRICAL, "expected_group": expected,
                                         "points": {BASE_POINT: "E楼更新"}, "remove": []})
        assert result.status_code == 200
        assert not result.json()["synced"]
        assert result.json()["cloud_synced"]
    assert client.fields(D).get("测点备注") is None
    assert client.fields(E)["测点备注"] == "E楼更新"
    bridge.put_inspection_point_rules.assert_not_called()


def test_background_polls_each_building_and_dispatches_fresh_revision(setup, monkeypatch):
    from app.bootstrap.container import AppContainer

    config, client, _ = setup
    cached, revision, _ = settings.refresh_cloud_point_settings(config)
    settings.record_inspection_upload_revision(config, revision)
    assert settings.ack_inspection_upload_retrigger(config, revision)
    change_on_cloud(client, E, EXTRA_POINT, "B6")
    d_table = cloud.POINT_CONFIG_TABLES[D]
    client.fail_read.add(d_table)
    client.timeout = 30
    monkeypatch.setattr(cloud, "point_config_client", lambda _: client)
    monotonic = Mock(side_effect=[0.0, 0.0, 20.0, 20.0])
    monkeypatch.setattr("time.monotonic", monotonic)
    dispatch = Mock(side_effect=lambda: settings.read_inspection_upload_retrigger(config) or {})
    container = SimpleNamespace(runtime_config=config, add_system_log=Mock(),
                                _configured_deployment_snapshot=lambda: {"role_mode": "external"},
                                dispatch_pending_inspection_retrigger=dispatch)
    watchdog = AppContainer._build_inspection_retrigger_watchdog(container)
    assert watchdog._dispatch() == {}
    marker = watchdog._dispatch()
    current = settings.load_point_settings(config)
    assert marker["revision"] > revision
    assert current[D] == cached[D]
    assert EXTRA_POINT in current[E][ELECTRICAL]["points"]
    assert not client.writes
