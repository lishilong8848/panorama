import json
from decimal import Decimal

import pytest

from app.modules.shared_bridge.service.shared_source_cache_service import SharedSourceCacheService


@pytest.mark.parametrize(
    "value",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        Decimal("NaN"),
        Decimal("Infinity"),
        "nan",
        "NaN",
        "inf",
        "-Infinity",
    ],
)
def test_alarm_non_finite_values_are_normalized_to_blank(value):
    assert SharedSourceCacheService._normalize_alarm_cell_text(value) == ""
    assert SharedSourceCacheService._coerce_alarm_number_field(value) == ""


def test_alarm_finite_number_remains_json_compliant():
    value = SharedSourceCacheService._coerce_alarm_number_field("12.35")

    assert value == 12.35
    json.dumps({"触发值": value}, ensure_ascii=False, allow_nan=False)


def test_alarm_upload_preflight_rejects_non_json_safe_value():
    with pytest.raises(RuntimeError, match="未执行清表"):
        SharedSourceCacheService._validate_alarm_upload_records_json([{"触发值": float("nan")}])

