from __future__ import annotations

from handover_log_module.core.models import RawRow
from handover_log_module.core.selectors import compute_metric_hits


def test_d_column_rules_are_fuzzy_and_case_insensitive() -> None:
    rows = [
        RawRow(
            row_index=4,
            b_text="path",
            c_text="point",
            d_name="A-Building/Zone/UPS-LoadRate",
            e_raw=12.3,
            value=12.3,
            b_norm="",
            c_norm="",
        )
    ]

    rules = {
        # d_equals 也应按包含匹配，不区分大小写
        "m1": {"d_equals": "ups-load", "agg": "first"},
        # d_contains 大小写不敏感
        "m2": {"d_contains": "building/zone", "agg": "first"},
        # d_match 列表任一项模糊匹配
        "m3": {"d_match": ["other", "loadrate"], "agg": "first"},
        # d_regex 大小写不敏感
        "m4": {"d_regex": ".*ups-loadrate$", "agg": "first"},
        # group_contains 大小写不敏感
        "m5": {"d_regex": ".*loadrate", "group_contains": "UPS", "agg": "first"},
    }

    hits, missing = compute_metric_hits(rows=rows, rules=rules)
    assert not missing
    assert set(hits.keys()) == {"m1", "m2", "m3", "m4", "m5"}
