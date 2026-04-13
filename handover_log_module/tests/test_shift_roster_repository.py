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
