from __future__ import annotations

from types import SimpleNamespace

from handover_log_module.repository import change_management_repository as change_repo_module
from handover_log_module.repository import maintenance_management_repository as maintenance_repo_module
from handover_log_module.repository import other_important_work_repository as other_work_repo_module


class _FakeClient:
    def __init__(self, records: list[dict]) -> None:
        self._records = records

    def list_fields(self, **_: object) -> list[dict]:
        return []

    def list_records(self, **_: object) -> list[dict]:
        return list(self._records)


def _stub_duty_window(*_: object, **__: object) -> SimpleNamespace:
    return SimpleNamespace(
        start_time="2026-04-10 08:00:00",
        end_time="2026-04-10 18:00:00",
    )


def test_change_management_uses_shifted_overlap_and_monthly_field_fallback(monkeypatch) -> None:
    records = [
        {
            "record_id": "keep-1",
            "fields": {
                "楼栋": ["A楼"],
                "开始": "2026-04-10 08:30:00",
                "结束": "2026-04-10 09:30:00",
                "阿里-变更等级": "一级",
                "过程更新时间": "09:00-09:30",
                "名称": "变更一",
                "专业": "暖通",
            },
        },
        {
            "record_id": "drop-1",
            "fields": {
                "楼栋": ["A楼"],
                "开始": "2026-04-10 07:00:00",
                "结束": "2026-04-10 09:00:00",
                "阿里-变更等级": "一级",
                "过程更新时间": "08:00-09:00",
                "名称": "变更二",
                "专业": "暖通",
            },
        },
        {
            "record_id": "keep-2",
            "fields": {
                "楼栋": ["A楼"],
                "开始": "2026-04-10 19:00:00",
                "结束": "",
                "阿里-变更等级": "一级",
                "过程更新时间": "19:00-持续中",
                "名称": "变更三",
                "专业": "暖通",
            },
        },
    ]
    repo = change_repo_module.ChangeManagementRepository(
        {
            "change_management_section": {
                "fields": {
                    "start_time": "",
                    "end_time": "",
                },
                "monthly_report_fields": {
                    "start_time": "开始",
                    "end_time": "结束",
                },
            }
        }
    )
    monkeypatch.setattr(change_repo_module, "build_duty_window", _stub_duty_window)
    monkeypatch.setattr(repo, "_new_client", lambda *_args, **_kwargs: _FakeClient(records))
    monkeypatch.setattr(repo, "_load_field_option_maps", lambda **_kwargs: {})

    rows, _cfg = repo.list_current_shift_rows(
        building="A楼",
        duty_date="2026-04-10",
        duty_shift="day",
        emit_log=lambda *_args: None,
    )

    assert [row.record_id for row in rows] == ["keep-1", "keep-2"]


def test_maintenance_management_history_allows_missing_end_time(monkeypatch) -> None:
    records = [
        {
            "record_id": "history-open",
            "fields": {
                "楼栋": ["A楼"],
                "实际开始时间": "2026-04-10 10:00:00",
                "实际结束时间": "",
                "名称": "维保项",
                "专业": "暖通",
            },
        }
    ]
    repo = maintenance_repo_module.MaintenanceManagementRepository({})
    monkeypatch.setattr(maintenance_repo_module, "build_duty_window", _stub_duty_window)
    monkeypatch.setattr(repo, "_new_client", lambda *_args, **_kwargs: _FakeClient(records))
    monkeypatch.setattr(repo, "_load_field_option_maps", lambda **_kwargs: {})

    rows, _cfg = repo.list_current_shift_rows(
        building="A楼",
        duty_date="2026-04-01",
        duty_shift="day",
        emit_log=lambda *_args: None,
    )

    assert [row.record_id for row in rows] == ["history-open"]


def test_other_important_work_uses_shifted_overlap_and_keeps_location_prefix(monkeypatch) -> None:
    records = [
        {
            "record_id": "keep-1",
            "fields": {
                "楼栋": ["A楼"],
                "实际开始时间": "2026-04-10 08:30:00",
                "实际结束时间": "2026-04-10 09:30:00",
                "位置": "A区机房",
                "内容": "调整冷机模式",
                "进度": "完成",
                "专业": "暖通",
            },
        },
        {
            "record_id": "drop-1",
            "fields": {
                "楼栋": ["A楼"],
                "实际开始时间": "2026-04-10 07:00:00",
                "实际结束时间": "2026-04-10 09:00:00",
                "位置": "A区机房",
                "内容": "边界不命中",
                "进度": "完成",
                "专业": "暖通",
            },
        },
        {
            "record_id": "keep-2",
            "fields": {
                "楼栋": ["A楼"],
                "实际开始时间": "2026-04-10 18:30:00",
                "实际结束时间": "",
                "位置": "A区机房",
                "内容": "持续中调整",
                "进度": "进行中",
                "专业": "暖通",
            },
        },
    ]
    repo = other_work_repo_module.OtherImportantWorkRepository(
        {
            "other_important_work_section": {
                "order": ["device_adjustment"],
            }
        }
    )
    monkeypatch.setattr(other_work_repo_module, "build_duty_window", _stub_duty_window)
    monkeypatch.setattr(repo, "_new_client", lambda *_args, **_kwargs: _FakeClient(records))
    monkeypatch.setattr(repo, "_load_field_option_maps", lambda **_kwargs: {})

    rows, _cfg = repo.list_current_shift_rows(
        building="A楼",
        duty_date="2026-04-01",
        duty_shift="day",
        emit_log=lambda *_args: None,
    )

    assert [row.record_id for row in rows] == ["keep-1", "keep-2"]
    assert rows[0].description_text == "A区机房 调整冷机模式"
