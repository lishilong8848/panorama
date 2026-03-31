from __future__ import annotations

from handover_log_module.repository.change_management_repository import ChangeManagementRepository


def _change_cfg() -> dict:
    return {
        "enabled": True,
        "source": {"app_token": "app", "table_id": "tbl", "page_size": 500, "max_records": 5000},
        "fields": {
            "building": "building",
            "updated_time": "updated_time",
            "change_level": "change_level",
            "process_updates": "process_updates",
            "description": "description",
            "specialty": "specialty",
        },
    }


class _RepoWithFakeClient(ChangeManagementRepository):
    def __init__(self, cfg: dict, records: list[dict]) -> None:
        super().__init__({"change_management_section": cfg})
        self._records = records

    def _new_client(self, cfg):  # noqa: ANN001, ARG002
        class _Client:
            def __init__(self, records: list[dict]) -> None:
                self._records = records

            def list_fields(self, **_kwargs):
                return []

            def list_records(self, **_kwargs):
                return list(self._records)

        return _Client(self._records)


def test_list_current_shift_rows_grouped_assigns_only_exact_single_buildings() -> None:
    repo = _RepoWithFakeClient(
        _change_cfg(),
        [
            {
                "record_id": "rec-a",
                "fields": {
                    "building": "A楼",
                    "updated_time": "2026-03-14 10:00:00",
                    "change_level": "L1",
                    "process_updates": "2026-03-14 11:00:00",
                    "description": "change-a",
                    "specialty": "电气",
                },
            },
            {
                "record_id": "rec-ac",
                "fields": {
                    "building": ["A楼", "C楼"],
                    "updated_time": "2026-03-14 10:05:00",
                    "change_level": "L1",
                    "process_updates": "2026-03-14 11:05:00",
                    "description": "change-ac",
                    "specialty": "电气",
                },
            },
            {
                "record_id": "rec-c",
                "fields": {
                    "building": "C楼",
                    "updated_time": "2026-03-14 10:10:00",
                    "change_level": "L2",
                    "process_updates": "2026-03-14 11:10:00",
                    "description": "change-c",
                    "specialty": "暖通",
                },
            },
        ],
    )

    grouped, _ = repo.list_current_shift_rows_grouped(
        buildings=["A楼", "C楼"],
        duty_date="2026-03-14",
        duty_shift="day",
        emit_log=lambda *_args: None,
    )

    assert [row.record_id for row in grouped["A楼"]] == ["rec-a"]
    assert [row.record_id for row in grouped["C楼"]] == ["rec-c"]


def test_list_current_shift_rows_grouped_skips_unselected_buildings() -> None:
    repo = _RepoWithFakeClient(
        _change_cfg(),
        [
            {
                "record_id": "rec-b",
                "fields": {
                    "building": "B楼",
                    "updated_time": "2026-03-14 10:00:00",
                    "change_level": "L1",
                    "process_updates": "2026-03-14 11:00:00",
                    "description": "change-b",
                    "specialty": "电气",
                },
            }
        ],
    )

    grouped, _ = repo.list_current_shift_rows_grouped(
        buildings=["A楼", "C楼"],
        duty_date="2026-03-14",
        duty_shift="day",
        emit_log=lambda *_args: None,
    )

    assert grouped["A楼"] == []
    assert grouped["C楼"] == []
