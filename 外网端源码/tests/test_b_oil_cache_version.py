from types import SimpleNamespace

import pytest

from handover_log_module.core.oil_tank_calibration import B_OIL_CALIBRATION_VERSION
from handover_log_module.service.handover_capacity_oil_cache_service import HandoverCapacityOilCacheService
from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService


@pytest.mark.parametrize("building,version,expected", [
    ("B楼", "", {"first": "13910", "second": "12500"}),
    ("B楼", B_OIL_CALIBRATION_VERSION, {"first": "13730", "second": "12330"}),
    ("A楼", "", {"first": "13730", "second": "12330"}),
    ("C楼", "", {"first": "13730", "second": "12330"}),
    ("D楼", "", {"first": "13730", "second": "12330"}),
    ("E楼", "", {"first": "13730", "second": "12330"}),
])
def test_previous_cache_only_reused_with_matching_calculation_version(building, version, expected):
    service = HandoverCapacityReportService.__new__(HandoverCapacityReportService)
    service._oil_cache_service = SimpleNamespace(load_previous_values=lambda **_: {
        "first": "13730", "second": "12330", "calculation_version": version,
    })
    service._review_session_service = SimpleNamespace(get_latest_session_for_context=lambda **_: {})
    previous, _ = service._load_previous_capacity_display_oil_values(
        building=building, duty_date="2026-10-23", duty_shift="day",
        current_display_values={"first": "13910", "second": "12500"}, emit_log=lambda _: None,
    )
    assert previous == expected


@pytest.mark.parametrize("building,version", [("A楼", ""), ("B楼", B_OIL_CALIBRATION_VERSION)])
def test_new_cache_preserves_version_across_service_instances(tmp_path, building, version):
    config = {"paths": {"runtime_state_root": str(tmp_path)}}
    cache = HandoverCapacityOilCacheService(config)
    cache.save_current_values(
        building=building, duty_date="2026-10-22", duty_shift="night", first="13910", second="12500",
    )
    assert (tmp_path / "scheduler_runtime_state.sqlite3").is_file()
    restored = HandoverCapacityOilCacheService(config)
    assert restored.load_previous_values(building=building, duty_date="2026-10-23", duty_shift="day") == {
        "first": "13910", "second": "12500", "calculation_version": version,
    }
