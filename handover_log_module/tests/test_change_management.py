from __future__ import annotations

from datetime import datetime
from pathlib import Path

import openpyxl

from handover_log_module.repository.change_management_repository import ChangeManagementRow
from handover_log_module.service.change_management_payload_builder import ChangeManagementPayloadBuilder


def _build_change_template(path: Path) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "交接班日志"
    ws["A35"] = "变更管理"
    ws["A36"] = "序号"
    ws["B36"] = "变更等级"
    ws["D36"] = "描述"
    ws["E36"] = "作业时间段"
    ws["H36"] = "执行人"
    ws["A37"] = 1
    ws["B37"] = "/"
    ws["D37"] = "/"
    ws["E37"] = "/"
    ws["H37"] = "/"
    wb.save(path)
    wb.close()


class _FakeChangeRepo:
    def __init__(self, cfg, rows):
        self._cfg = cfg
        self._rows = rows

    def list_current_shift_rows(self, **_kwargs):
        return list(self._rows), self._cfg


class _FakeShiftRosterRepo:
    def __init__(self, rows):
        self._rows = rows

    def list_engineer_directory(self, **_kwargs):
        return list(self._rows)


def test_change_management_builder_skips_blank_description_rows(tmp_path: Path) -> None:
    template_path = tmp_path / "change_template.xlsx"
    _build_change_template(template_path)
    cfg = {
        "enabled": True,
        "sections": {"change_management": "变更管理"},
        "column_mapping": {
            "resolve_by_header": True,
            "header_alias": {
                "change_level": ["变更等级"],
                "work_window": ["作业时间段"],
                "description": ["描述"],
                "executor": ["执行人"],
            },
            "fallback_cols": {
                "change_level": "B",
                "work_window": "E",
                "description": "D",
                "executor": "H",
            },
        },
        "work_window_text": {
            "day_anchor": "08:00:00",
            "day_default_end": "18:30:00",
            "night_anchor": "18:00:00",
            "night_default_end_next_day": "08:00:00",
        },
    }
    rows = [
        ChangeManagementRow(
            record_id="rec-blank",
            building_values=["A楼"],
            updated_time=datetime(2026, 3, 14, 9, 0, 0),
            change_level="低",
            process_updates_text="2026-03-14 09:10:00",
            description="",
            specialty_text="电气",
            raw_fields={},
        ),
        ChangeManagementRow(
            record_id="rec-keep",
            building_values=["A楼"],
            updated_time=datetime(2026, 3, 14, 10, 0, 0),
            change_level="低",
            process_updates_text="2026-03-14 10:10:00",
            description="变更描述",
            specialty_text="电气",
            raw_fields={},
        ),
    ]
    builder = ChangeManagementPayloadBuilder(
        {"template": {"source_path": str(template_path), "sheet_name": "交接班日志"}},
        repository=_FakeChangeRepo(cfg, rows),
        shift_roster_repo=_FakeShiftRosterRepo(
            [{"building": "A楼", "specialty": "电气", "supervisor": "汪根尚"}]
        ),
    )

    payload = builder.build(
        building="A楼",
        duty_date="2026-03-14",
        duty_shift="day",
        emit_log=lambda *_args: None,
    )

    assert payload["变更管理"] == [
        {"cells": {"B": "低", "D": "变更描述", "E": "10:10-18:30", "H": "汪根尚"}}
    ]


def test_change_management_builder_normalizes_specialty_for_executor_match(tmp_path: Path) -> None:
    template_path = tmp_path / "change_template_specialty.xlsx"
    _build_change_template(template_path)
    cfg = {
        "enabled": True,
        "sections": {"change_management": "变更管理"},
        "column_mapping": {
            "resolve_by_header": True,
            "header_alias": {
                "change_level": ["变更等级"],
                "work_window": ["作业时间段"],
                "description": ["描述"],
                "executor": ["执行人"],
            },
            "fallback_cols": {
                "change_level": "B",
                "work_window": "E",
                "description": "D",
                "executor": "H",
            },
        },
        "work_window_text": {
            "day_anchor": "08:00:00",
            "day_default_end": "18:30:00",
            "night_anchor": "18:00:00",
            "night_default_end_next_day": "08:00:00",
        },
    }
    rows = [
        ChangeManagementRow(
            record_id="rec-normalized",
            building_values=["A楼"],
            updated_time=datetime(2026, 3, 14, 9, 0, 0),
            change_level="低",
            process_updates_text="2026-03-14 09:10:00",
            description="消防系统变更",
            specialty_text="消防值守",
            raw_fields={},
        ),
    ]
    builder = ChangeManagementPayloadBuilder(
        {"template": {"source_path": str(template_path), "sheet_name": "交接班日志"}},
        repository=_FakeChangeRepo(cfg, rows),
        shift_roster_repo=_FakeShiftRosterRepo(
            [{"building": "A楼", "specialty": "消防", "supervisor": "高荣"}]
        ),
    )

    payload = builder.build(
        building="A楼",
        duty_date="2026-03-14",
        duty_shift="day",
        emit_log=lambda *_args: None,
    )

    assert payload["变更管理"] == [
        {"cells": {"B": "低", "D": "消防系统变更", "E": "09:10-18:30", "H": "高荣"}}
    ]
