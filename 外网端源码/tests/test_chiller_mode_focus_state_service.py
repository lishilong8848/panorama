from __future__ import annotations

from app.core.app_state import AppStateRepository
from handover_log_module.service.chiller_mode_focus_state_service import (
    ChillerModeFocusStateService,
)


def _repository(tmp_path):
    repository = AppStateRepository(
        runtime_config={"paths": {"runtime_state_root": str(tmp_path / "runtime")}},
        app_dir=tmp_path,
    )
    repository.ensure_ready()
    return repository


def _modes(*running):
    active = {int(unit) for unit in running}
    return {
        str(unit): "制冷" if unit in active else "停机"
        for unit in range(1, 7)
    }


def test_pending_started_unit_is_cancelled_when_it_stops_before_rotation(tmp_path):
    repository = _repository(tmp_path)
    service = ChillerModeFocusStateService({}, app_state_repository=repository)

    service.record_modes({"A楼": _modes(1, 2)}, observed_at="2026-08-12 10:00:00")
    pending = service.record_modes({"A楼": _modes(1, 2, 3)}, observed_at="2026-08-12 10:10:00")
    cancelled = service.record_modes({"A楼": _modes(1, 2)}, observed_at="2026-08-12 10:20:00")

    assert pending["buildings"]["A楼"]["change_note"] == "无"
    assert pending["buildings"]["A楼"]["pending_by_zone"]["west"]["started"] == [3]
    assert cancelled["buildings"]["A楼"]["change_note"] == "无"
    assert cancelled["buildings"]["A楼"]["pending_by_zone"] == {}


def test_rotation_is_confirmed_only_after_original_unit_stops_and_survives_restart(tmp_path):
    repository = _repository(tmp_path)
    service = ChillerModeFocusStateService({}, app_state_repository=repository)

    service.record_modes({"A楼": _modes(1, 2)}, observed_at="2026-08-12 10:00:00")
    service.record_modes({"A楼": _modes(1, 2, 3)}, observed_at="2026-08-12 10:10:00")
    confirmed = service.record_modes({"A楼": _modes(2, 3)}, observed_at="2026-08-12 10:20:00")

    assert confirmed["buildings"]["A楼"]["change_note"] == "1#→3#"
    assert confirmed["buildings"]["A楼"]["pending_by_zone"] == {}
    restarted = ChillerModeFocusStateService({}, app_state_repository=repository)
    persisted = restarted.get_batch_state("2026-08-12|day")
    assert persisted["buildings"]["A楼"]["change_note"] == "1#→3#"


def test_rotation_note_uses_only_units_that_actually_stop_in_each_zone(tmp_path):
    repository = _repository(tmp_path)
    service = ChillerModeFocusStateService({}, app_state_repository=repository)

    service.record_modes({"A楼": _modes(4, 5)}, observed_at="2026-08-12 10:00:00")
    service.record_modes({"A楼": _modes(4, 5, 6)}, observed_at="2026-08-12 10:10:00")
    confirmed = service.record_modes({"A楼": _modes(4, 6)}, observed_at="2026-08-12 10:20:00")

    assert confirmed["buildings"]["A楼"]["change_note"] == "5#→6#"


def test_rotation_note_groups_units_only_when_they_all_actually_stop(tmp_path):
    repository = _repository(tmp_path)
    service = ChillerModeFocusStateService({}, app_state_repository=repository)

    service.record_modes({"A楼": _modes(1, 2)}, observed_at="2026-08-12 10:00:00")
    service.record_modes({"A楼": _modes(1, 2, 3)}, observed_at="2026-08-12 10:10:00")
    confirmed = service.record_modes({"A楼": _modes(3)}, observed_at="2026-08-12 10:20:00")

    assert confirmed["buildings"]["A楼"]["change_note"] == "1、2#→3#"


def test_direct_rotation_is_zone_scoped_and_incomplete_rows_do_not_change_state(tmp_path):
    repository = _repository(tmp_path)
    service = ChillerModeFocusStateService({}, app_state_repository=repository)

    first = service.record_modes({"A楼": _modes(1, 4)}, observed_at="2026-08-12 11:00:00")
    second = service.record_modes({"A楼": _modes(2, 4)}, observed_at="2026-08-12 11:10:00")
    incomplete = service.record_modes(
        {"A楼": {"1": "停机", "2": "停机"}},
        observed_at="2026-08-12 11:20:00",
    )

    assert first["buildings"]["A楼"]["change_note"] == "无"
    assert second["buildings"]["A楼"]["change_note"] == "1#→2#"
    assert "1#→4#" not in second["buildings"]["A楼"]["change_note"]
    assert incomplete["buildings"]["A楼"]["modes"] == _modes(2, 4)
    assert incomplete["incomplete_buildings"] == ["A楼"]


def test_extract_modes_uses_mode_text_and_numeric_fallback():
    rows = [
        {"楼栋": "A楼", "采集点": "1号冷机模式", "数据": 1, "冷机状态": "制冷"},
        {"楼栋": "A楼", "采集点": "2号冷机运行模式", "数据": 4},
        {"楼栋": "B楼", "采集点": "6#制冷单元模式", "数据": 3},
        {"楼栋": "B楼", "采集点": "非模式点", "数据": 1},
    ]

    parsed = ChillerModeFocusStateService.extract_modes(rows)

    assert parsed == {
        "A楼": {"1": "制冷", "2": "停机"},
        "B楼": {"6": "板换"},
    }
