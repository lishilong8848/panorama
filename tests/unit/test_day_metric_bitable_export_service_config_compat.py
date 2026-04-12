from __future__ import annotations

from app.config.config_adapter import ensure_v3_config
from handover_log_module.service.day_metric_bitable_export_service import DayMetricBitableExportService


def test_day_metric_bitable_export_service_supports_v3_feishu_and_feature_config() -> None:
    cfg = ensure_v3_config({})
    cfg["common"]["feishu_auth"]["app_id"] = "cli_test"
    cfg["common"]["feishu_auth"]["app_secret"] = "secret_test"
    cfg["features"]["day_metric_upload"]["target"]["source"]["app_token"] = "app_token_test"
    cfg["features"]["day_metric_upload"]["target"]["source"]["table_id"] = "table_test"

    service = DayMetricBitableExportService(cfg)

    assert service._global_feishu_cfg()["app_id"] == "cli_test"
    assert service._global_feishu_cfg()["app_secret"] == "secret_test"
    assert service._runtime_day_metric_cfg()["target"]["source"]["app_token"] == "app_token_test"
    assert service._runtime_day_metric_cfg()["target"]["source"]["table_id"] == "table_test"


def test_day_metric_bitable_export_service_supports_v3_handover_template_sheet_name() -> None:
    cfg = ensure_v3_config({})
    cfg["features"]["handover_log"]["template"]["sheet_name"] = "交接班记录"

    service = DayMetricBitableExportService(cfg)

    assert service._sheet_name() == "交接班记录"
