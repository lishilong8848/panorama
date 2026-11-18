from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from app.config.config_adapter import ensure_v3_config
from handover_log_module.repository.exercise_management_repository import ExerciseManagementRepository
from handover_log_module.service.exercise_management_payload_builder import ExerciseManagementPayloadBuilder


def _record(record_id, when, *, building="C楼", team="B班", scenario="失电演练"):
    return {"record_id": record_id, "fields": {
        "楼栋": building, "班组": team, "发生时间": when, "演练场景": scenario,
    }}


@pytest.mark.parametrize("preload", [False, True])
def test_exercise_uses_same_building_team_and_shift_in_both_paths(monkeypatch, preload):
    tz = timezone(timedelta(hours=8))
    next_morning = int(datetime(2026, 11, 18, 7, 59, tzinfo=tz).timestamp() * 1000)
    records = [
        _record("before", "2026-11-17 16:59:59"),
        _record("starts", "2026-11-17 17:00:00"),
        _record("other_team", "2026-11-17 17:10:00", team="D班"),
        _record("other_building", "2026-11-17 17:10:00", building="D楼"),
        _record("missing_team", "2026-11-17 17:20:00", team=None),
        _record("bad_time", 10**20),
        _record("next_morning", next_morning, team=" b班 "),
        _record("next_morning", next_morning, team="B班"),
        _record("next_shift", "2026-11-18 08:00:00"),
    ]
    config = {}
    repo = ExerciseManagementRepository(config)
    client = Mock()
    client.list_records.return_value = records
    monkeypatch.setattr(repo, "_new_client", lambda _: client)
    monkeypatch.setattr(repo, "_load_field_option_maps", lambda **_: {})
    builder = ExerciseManagementPayloadBuilder(config, repository=repo)
    monkeypatch.setattr(builder, "_resolve_section_column_mapping", lambda **_: {
        "演练管理": {"exercise_type": "B", "exercise_item": "C", "completion": "D", "executor": "H"},
    })
    duty = {"duty_date": "2026-11-17", "duty_shift": "night"}
    emit = Mock()
    grouped = repo.list_current_shift_rows_grouped(buildings=["C楼", "D楼"], emit_log=emit, **duty)[0] if preload else None
    params = dict(building="C楼", executor_text="值班人", preloaded_rows_by_building=grouped, emit_log=emit, **duty)
    rows = builder.build(current_team="B班", **params)["演练管理"]
    assert [row["cells"]["C"] for row in rows] == ["失电演练", "失电演练"]
    assert all(row["cells"]["H"] == "值班人" for row in rows)
    assert builder.build(current_team="", **params) == {}
    client.list_records.assert_called_once_with(
        table_id="tblo1UuKuBfWQNEm", view_id="vewqNLAo7q",
        page_size=500, max_records=5000, field_names=["楼栋", "班组", "发生时间", "演练场景"],
    )


def test_legacy_exercise_source_migrates_without_enabling_disabled_section():
    raw = {"version": 3, "common": {}, "features": {"handover_log": {"exercise_management_section": {
        "enabled": False,
        "source": {"table_id": "tblBrALE11XCicNN", "max_records": 200},
        "fields": {"building": "机楼", "start_time": "演练开始时间", "project": "告警描述"},
    }}}}
    section = ensure_v3_config(raw)["features"]["handover_log"]["exercise_management_section"]
    assert not section["enabled"]
    assert section["source"]["table_id"] == "tblo1UuKuBfWQNEm"
    assert section["source"]["view_id"] == "vewqNLAo7q"
    assert section["source"]["max_records"] == 200
    assert section["fields"] == {"building": "楼栋", "team": "班组", "start_time": "发生时间", "project": "演练场景"}
