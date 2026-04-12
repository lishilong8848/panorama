from __future__ import annotations

import copy

from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.settings_loader import validate_settings


def test_validate_settings_backfills_handover_shift_interval_fields() -> None:
    cfg = copy.deepcopy(DEFAULT_CONFIG_V3)
    change_fields = cfg["features"]["handover_log"]["change_management_section"]["fields"]
    maintenance_fields = cfg["features"]["handover_log"]["maintenance_management_section"]["fields"]
    other_sources = cfg["features"]["handover_log"]["other_important_work_section"]["sources"]

    change_fields.pop("start_time", None)
    change_fields.pop("end_time", None)
    maintenance_fields.pop("start_time", None)
    for source_cfg in other_sources.values():
        source_cfg["fields"].pop("actual_start_time", None)

    normalized = validate_settings(cfg)

    normalized_change_fields = normalized["features"]["handover_log"]["change_management_section"]["fields"]
    normalized_maintenance_fields = normalized["features"]["handover_log"]["maintenance_management_section"]["fields"]
    normalized_other_sources = normalized["features"]["handover_log"]["other_important_work_section"]["sources"]

    assert normalized_change_fields["start_time"] == "变更开始时间"
    assert normalized_change_fields["end_time"] == "变更结束时间"
    assert normalized_maintenance_fields["start_time"] == "实际开始时间"
    assert normalized_other_sources["power_notice"]["fields"]["actual_start_time"] == "实际开始时间"
    assert normalized_other_sources["device_adjustment"]["fields"]["actual_start_time"] == "实际开始时间"
