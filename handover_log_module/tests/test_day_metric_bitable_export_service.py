from __future__ import annotations

from handover_log_module.core.models import MetricHit
from handover_log_module.service.day_metric_bitable_export_service import DayMetricBitableExportService


class _FakeClient:
    def __init__(self, records):
        self.records = list(records)
        self.deleted_calls = []
        self.created_calls = []

    def list_records(self, *, table_id, page_size, max_records):  # noqa: ANN001
        return list(self.records)

    def batch_delete_records(self, *, table_id, record_ids, batch_size):  # noqa: ANN001
        self.deleted_calls.append(
            {
                "table_id": table_id,
                "record_ids": list(record_ids),
                "batch_size": batch_size,
            }
        )
        return len(record_ids)

    def batch_create_records(self, *, table_id, fields_list, batch_size):  # noqa: ANN001
        self.created_calls.append(
            {
                "table_id": table_id,
                "fields_list": list(fields_list),
                "batch_size": batch_size,
            }
        )
        return {"created": len(fields_list)}


def _cfg():
    return {
        "day_metric_export": {
            "enabled": True,
            "only_day_shift": True,
            "source": {
                "app_token": "app",
                "table_id": "tbl_demo",
                "page_size": 500,
                "max_records": 5000,
                "delete_batch_size": 200,
                "create_batch_size": 200,
            },
            "fields": {
                "type": "类型",
                "building": "楼栋",
                "date": "日期",
                "value": "数值",
            },
            "types": [
                {"name": "总负荷（KW）", "source": "cell", "cell": "D6"},
                {"name": "IT总负荷（KW）", "source": "cell", "cell": "F6"},
            ],
        }
    }


def test_normalize_cfg_adds_position_code_default() -> None:
    service = DayMetricBitableExportService(_cfg())

    cfg = service._normalize_cfg()

    assert cfg["fields"]["position_code"] == "位置/编号"


def test_serialize_metric_origin_context_maps_metric_and_target_cell() -> None:
    service = DayMetricBitableExportService(_cfg())

    context = service.serialize_metric_origin_context(
        hits={
            "city_power": MetricHit(
                metric_key="city_power",
                row_index=12,
                d_name="总负荷",
                value=11.0,
                b_norm="A-401",
                c_norm="",
                b_text="A-401",
                c_text="",
            ),
            "cold_temp_max": MetricHit(
                metric_key="cold_temp_max",
                row_index=4,
                d_name="冷通道最高温度",
                value=31.2,
                b_norm="E-301",
                c_norm="C3-2",
                b_text="E-301",
                c_text="C3-2",
            ),
        },
        effective_config={
            "cell_mapping": {
                "city_power": "D6",
                "cold_temp_max": "H8",
            }
        },
    )

    assert context["by_metric_id"]["cold_temp_max"]["b_norm"] == "E-301"
    assert context["by_metric_id"]["cold_temp_max"]["c_norm"] == "C3-2"
    assert context["by_target_cell"]["D6"]["metric_key"] == "city_power"
    assert context["by_target_cell"]["D6"]["b_norm"] == "A-401"


def test_prepare_records_writes_position_code_for_metric_and_cell() -> None:
    cfg = _cfg()
    cfg["day_metric_export"]["types"] = [
        {"name": "冷通道最高温度（℃）", "source": "metric", "metric_id": "cold_temp_max"},
        {"name": "总负荷（KW）", "source": "cell", "cell": "D6"},
        {"name": "UPS负载率（MAX）", "source": "cell_percent", "cell": "D10"},
    ]
    service = DayMetricBitableExportService(cfg)

    records, _preview = service._prepare_records(
        cfg=service._normalize_cfg(),
        building="A楼",
        duty_date="2026-03-24",
        cell_values={"D6": 11, "D10": "55%"},
        resolved_values_by_id={"cold_temp_max": 31.2},
        metric_origin_context={
            "by_metric_id": {
                "cold_temp_max": {"b_norm": "E-301", "c_norm": "C3-2"},
            },
            "by_target_cell": {
                "D6": {"metric_key": "city_power", "b_norm": "A-401", "c_norm": "", "c_text": "1#总配"},
                "D10": {"metric_key": "ups_load_max", "b_norm": "", "c_norm": "", "c_text": "UPS-3"},
            },
        },
    )

    assert records[0]["位置/编号"] == "E-301 C3-2"
    assert records[1]["位置/编号"] == "1#总配"
    assert records[2]["位置/编号"] == "UPS-3"


def test_list_existing_records_for_unit_filters_building_date_and_type(monkeypatch) -> None:
    service = DayMetricBitableExportService(_cfg())
    target_ms = service._midnight_timestamp_ms("2026-03-24")
    other_ms = service._midnight_timestamp_ms("2026-03-23")
    fake_client = _FakeClient(
        [
            {"record_id": "rec_1", "fields": {"楼栋": "A楼", "日期": target_ms, "类型": "总负荷（KW）"}},
            {"record_id": "rec_2", "fields": {"楼栋": "A楼", "日期": target_ms, "类型": "不相关类型"}},
            {"record_id": "rec_3", "fields": {"楼栋": "B楼", "日期": target_ms, "类型": "总负荷（KW）"}},
            {"record_id": "rec_4", "fields": {"楼栋": "A楼", "日期": other_ms, "类型": "IT总负荷（KW）"}},
        ]
    )
    monkeypatch.setattr(service, "_new_client", lambda _cfg: fake_client)

    matched = service.list_existing_records_for_unit(building="A楼", duty_date="2026-03-24")

    assert [item["record_id"] for item in matched] == ["rec_1"]


def test_rewrite_from_output_file_deletes_then_recreates(monkeypatch) -> None:
    service = DayMetricBitableExportService(_cfg())
    target_ms = service._midnight_timestamp_ms("2026-03-24")
    fake_client = _FakeClient(
        [
            {"record_id": "rec_1", "fields": {"楼栋": "A楼", "日期": target_ms, "类型": "总负荷（KW）"}},
            {"record_id": "rec_2", "fields": {"楼栋": "A楼", "日期": target_ms, "类型": "IT总负荷（KW）"}},
        ]
    )
    monkeypatch.setattr(service, "_new_client", lambda _cfg: fake_client)
    monkeypatch.setattr(service, "_load_workbook_cell_values", lambda output_file, cfg: {"D6": 11, "F6": 22})  # noqa: ARG005
    monkeypatch.setattr(
        service,
        "_prepare_records",
        lambda **kwargs: (  # noqa: ARG005
            [
                {"楼栋": "A楼", "日期": target_ms, "类型": "总负荷（KW）", "数值": 11, "位置/编号": "A-401"},
                {"楼栋": "A楼", "日期": target_ms, "类型": "IT总负荷（KW）", "数值": 22, "位置/编号": ""},
            ],
            [{"楼栋": "A楼", "日期": target_ms, "类型": "总负荷（KW）", "数值": 11, "位置/编号": "A-401"}],
        ),
    )

    result = service.rewrite_from_output_file(
        building="A楼",
        duty_date="2026-03-24",
        duty_shift="day",
        output_file="demo.xlsx",
        metric_values_by_id={},
        metric_origin_context={},
        emit_log=lambda *_args: None,
    )

    assert result["status"] == "ok"
    assert result["deleted_records"] == 2
    assert result["created_records"] == 2
    assert fake_client.deleted_calls[0]["record_ids"] == ["rec_1", "rec_2"]
    assert len(fake_client.created_calls[0]["fields_list"]) == 2
