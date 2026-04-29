from __future__ import annotations

from handover_log_module.repository.shift_roster_repository import ShiftRosterRepository


def test_list_engineer_directory_uses_target_duty_marker(monkeypatch) -> None:
    repo = ShiftRosterRepository({})
    captured: dict = {}

    def _fake_load_records_from_source(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(repo, "_load_records_from_source", _fake_load_records_from_source)

    rows = repo.list_engineer_directory(
        duty_date="2026-03-14",
        duty_shift="day",
        emit_log=lambda *_args: None,
    )

    assert rows == []
    assert captured["stage"] == "工程师目录"
    assert captured["duty_date"] == "2026-03-14"
    assert captured["duty_shift"] == "day"


def test_query_long_day_cell_values_grouped_loads_records_once(monkeypatch) -> None:
    repo = ShiftRosterRepository({})
    captured: dict = {"calls": 0}

    def _fake_load_records_from_source(**kwargs):
        captured["calls"] += 1
        captured["kwargs"] = kwargs
        return [
            {"fields": {"排班日期": "2026-03-14", "机楼": "A楼", "班次": "长白", "值班人员（实际）": "张三"}},
            {"fields": {"排班日期": "2026-03-14", "机楼": "B楼", "班次": "长白", "值班人员（实际）": "李四"}},
        ]

    monkeypatch.setattr(repo, "_load_records_from_source", _fake_load_records_from_source)

    grouped = repo.query_long_day_cell_values_grouped(
        buildings=["A楼", "B楼"],
        duty_date="2026-03-14",
        duty_shift="day",
        emit_log=lambda *_args: None,
    )

    assert captured["calls"] == 1
    assert captured["kwargs"]["stage"] == "长白岗查询"
    assert grouped["A楼"]["B4"] == "长白岗：张三"
    assert grouped["B楼"]["B4"] == "长白岗：李四"


def test_query_long_day_cell_values_uses_slash_when_old_config_rest_text_is_rest(monkeypatch) -> None:
    repo = ShiftRosterRepository({"shift_roster": {"long_day": {"rest_text": "休息"}}})
    monkeypatch.setattr(repo, "_load_records_from_source", lambda **_kwargs: [])
    logs = []

    grouped = repo.query_long_day_cell_values_grouped(
        buildings=["A楼"],
        duty_date="2026-03-14",
        duty_shift="day",
        emit_log=logs.append,
    )

    assert grouped["A楼"]["B4"] == "长白岗：/"
    assert any("fallback=/" in line for line in logs)
