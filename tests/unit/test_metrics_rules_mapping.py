from app.modules.report_pipeline.core.metrics_rules import (
    FEISHU_DIMENSION_MAPPING,
    canonical_metric_name,
)


def test_other_precision_air_metric_alias_maps_to_same_canonical() -> None:
    assert canonical_metric_name("其他精密空调用电量") == "其他精密空调总用电量"
    assert canonical_metric_name("其他精密空调总用电量") == "其他精密空调总用电量"
    assert canonical_metric_name("其他机密空调总用电量") == "其他精密空调总用电量"
    assert canonical_metric_name("其他包间精密空调总用电量") == "其他精密空调总用电量"


def test_other_precision_air_metric_feishu_dimension_mapping_is_stable() -> None:
    expected = ("用电量拆分", "末端空调用电", "配电室空调")
    assert FEISHU_DIMENSION_MAPPING["其他精密空调总用电量"] == expected
    assert FEISHU_DIMENSION_MAPPING["其他精密空调用电量"] == expected
    assert FEISHU_DIMENSION_MAPPING["其他机密空调总用电量"] == expected
    assert FEISHU_DIMENSION_MAPPING["其他包间精密空调总用电量"] == expected
