from __future__ import annotations

from datetime import datetime

from handover_log_module.repository.maintenance_management_repository import (
    MaintenanceManagementRepository,
    MaintenanceManagementRow,
)
from handover_log_module.service.maintenance_management_payload_builder import (
    MaintenanceManagementPayloadBuilder,
)


class FakeClient:
    def list_fields(self, **_kwargs):
        return [
            {
                "field_name": "楼栋",
                "property": {
                    "options": [
                        {"id": "opt-a", "name": "A楼"},
                        {"id": "opt-b", "name": "B楼"},
                        {"id": "opt-d", "name": "D楼"},
                    ]
                },
            },
            {
                "field_name": "专业",
                "property": {
                    "type": {
                        "ui_property": {
                            "options": [
                                {"id": "opt-fire", "name": "消防"},
                                {"id": "opt-hvac", "name": "暖通"},
                                {"id": "opt-electric", "name": "电气"},
                            ]
                        }
                    }
                },
            },
        ]

    def list_records(self, **_kwargs):
        return [
            {
                "record_id": "rec-c",
                "fields": {
                    "楼栋": None,
                    "楼栋-L": ["A楼"],
                    "实际开始时间": "2026-07-20 10:00:00",
                    "实际结束时间": None,
                    "名称": "EA118机房C栋火灾报警系统维护",
                    "专业": "opt-fire",
                    "专业-L": "旧专业",
                },
            },
            {
                "record_id": "rec-b",
                "fields": {
                    "楼栋": ["opt-b"],
                    "楼栋-L": ["D楼"],
                    "实际开始时间": "2026-07-20 10:05:00",
                    "实际结束时间": None,
                    "名称": "EA118机房B楼精密空调维护",
                    "专业": {"id": "opt-hvac"},
                    "专业-L": "电气",
                },
            },
            {
                "record_id": "rec-110",
                "fields": {
                    "楼栋": None,
                    "楼栋-L": ["E楼"],
                    "实际开始时间": "2026-07-20 10:10:00",
                    "实际结束时间": None,
                    "名称": "EA118-110KV变电站月度维护",
                    "专业": "opt-electric",
                    "专业-L": "旧专业",
                },
            },
            {
                "record_id": "rec-ae",
                "fields": {
                    "楼栋": None,
                    "楼栋-L": ["B楼"],
                    "实际开始时间": "2026-07-20 10:15:00",
                    "实际结束时间": None,
                    "名称": "A楼与E栋消防系统联合维护",
                    "专业": "opt-fire",
                    "专业-L": "旧专业",
                },
            },
            {
                "record_id": "rec-multi",
                "fields": {
                    "楼栋": ["opt-a", {"id": "opt-d"}],
                    "楼栋-L": ["B楼"],
                    "实际开始时间": "2026-07-20 10:20:00",
                    "实际结束时间": None,
                    "名称": "联合供配电维护",
                    "专业": "opt-electric",
                    "专业-L": "旧专业",
                },
            },
        ]


def test_blank_building_is_inferred_from_maintenance_item_without_matching_ea118():
    repository = MaintenanceManagementRepository(
        {
            "download": {
                "shift_windows": {
                    "day": {"start": "08:00:00", "end": "17:00:00"},
                    "night": {"start": "17:00:00", "end_next_day": "08:00:00"},
                }
            }
        }
    )
    repository._new_client = lambda _cfg: FakeClient()  # type: ignore[method-assign]
    logs: list[str] = []

    grouped, _ = repository.list_current_shift_rows_grouped(
        buildings=["A楼", "B楼", "C楼", "D楼", "E楼"],
        duty_date="2026-07-20",
        duty_shift="day",
        emit_log=logs.append,
    )

    assert [row.record_id for row in grouped["A楼"]] == ["rec-ae", "rec-multi"]
    assert [row.record_id for row in grouped["B楼"]] == ["rec-b"]
    assert [row.record_id for row in grouped["C楼"]] == ["rec-c"]
    assert [row.record_id for row in grouped["D楼"]] == ["rec-multi"]
    assert [row.record_id for row in grouped["E楼"]] == ["rec-ae"]
    assert grouped["B楼"][0].specialty_text == "暖通"
    assert grouped["D楼"][0].specialty_text == "电气"
    assert all(row.record_id != "rec-110" for rows in grouped.values() for row in rows)
    assert any("building_inferred_from_item=2" in line for line in logs)
    assert any("blank_building_unresolved=1" in line for line in logs)


def test_l_field_names_are_migrated_to_current_fields():
    repository = MaintenanceManagementRepository(
        {
            "maintenance_management_section": {
                "fields": {
                    "building": "楼栋-L",
                    "specialty": "专业-L",
                }
            }
        }
    )

    fields = repository.get_config()["fields"]

    assert fields["building"] == "楼栋"
    assert fields["specialty"] == "专业"


def test_multi_building_maintenance_is_rendered_on_each_matching_review_page():
    handover_cfg = {
        "maintenance_management_section": {
            "column_mapping": {
                "resolve_by_header": False,
            }
        }
    }
    repository = MaintenanceManagementRepository(handover_cfg)
    builder = MaintenanceManagementPayloadBuilder(handover_cfg, repository=repository)
    shared_row = MaintenanceManagementRow(
        record_id="rec-shared",
        building_values=["A楼", "D楼"],
        updated_time=datetime(2026, 7, 20, 10, 0, 0),
        item_text="供配电联合维护",
        specialty_text="电气",
        raw_fields={},
    )
    rows_by_building = {
        "A楼": [shared_row],
        "B楼": [],
        "C楼": [],
        "D楼": [shared_row],
        "E楼": [],
    }

    for building in ("A楼", "D楼"):
        payload = builder.build(
            building=building,
            duty_date="2026-07-20",
            duty_shift="day",
            preloaded_rows_by_building=rows_by_building,
            preloaded_engineers=[],
            emit_log=lambda _message: None,
        )

        assert [row["cells"]["B"] for row in payload["维护管理"]] == ["供配电联合维护"]


def test_option_map_cache_is_isolated_by_requested_fields():
    repository = MaintenanceManagementRepository({})
    client = FakeClient()

    stale_fields = repository._load_field_option_maps(
        client=client,
        table_id="tbl-maintenance",
        target_fields=["楼栋-L", "专业-L"],
        emit_log=lambda _message: None,
    )
    current_fields = repository._load_field_option_maps(
        client=client,
        table_id="tbl-maintenance",
        target_fields=["楼栋", "专业"],
        emit_log=lambda _message: None,
    )

    assert stale_fields == {"楼栋-L": {}, "专业-L": {}}
    assert current_fields["楼栋"]["opt-a"] == "A楼"
    assert current_fields["专业"]["opt-electric"] == "电气"
