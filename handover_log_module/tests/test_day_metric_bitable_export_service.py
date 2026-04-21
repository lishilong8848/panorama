from __future__ import annotations

from handover_log_module.core.models import MetricHit
from handover_log_module.service.day_metric_bitable_export_service import DayMetricBitableExportService


class _FakeClient:
    def __init__(self, records):
        self.records = list(records)
        self.deleted_calls = []
        self.created_calls = []
        self.updated_calls = []
        self.list_calls = []

    def list_records(self, *, table_id, page_size, max_records, filter_formula=""):  # noqa: ANN001
        self.list_calls.append(
            {
                "table_id": table_id,
                "page_size": page_size,
                "max_records": max_records,
                "filter_formula": filter_formula,
            }
        )
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

    def batch_update_records(self, *, table_id, records, batch_size):  # noqa: ANN001
        self.updated_calls.append(
            {
                "table_id": table_id,
                "records": list(records),
                "batch_size": batch_size,
            }
        )
        return {"updated": len(records)}


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
    service = DayMetricBitableExportService(cfg)

    records, _preview = service._prepare_records(
        cfg=service._normalize_cfg(),
        building="A楼",
        duty_date="2026-03-24",
        cell_values={},
        resolved_values_by_id={"cold_temp_max": 31.2, "city_power": 11, "ups_load_max": 55},
        metric_origin_context={
            "by_metric_id": {
                "cold_temp_max": {"b_norm": "E-301", "c_norm": "C3-2"},
                "city_power": {"metric_key": "city_power", "b_norm": "A-401", "c_norm": "", "c_text": "1#总配"},
                "ups_load_max": {"metric_key": "ups_load_max", "b_norm": "", "c_norm": "", "c_text": "UPS-3"},
            },
        },
    )

    by_type = {item["类型"]: item for item in records}
    assert by_type["冷通道最高温度（℃）"]["数值"] == 31.2
    assert by_type["冷通道最高温度（℃）"]["位置/编号"] == "E-301 C3-2"
    assert by_type["总负荷（KW）"]["数值"] == 11
    assert by_type["总负荷（KW）"]["位置/编号"] == "pue能耗数据计算"
    assert by_type["UPS负载率（MAX）"]["数值"] == 55
    assert by_type["UPS负载率（MAX）"]["位置/编号"] == "UPS-3"


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
    monkeypatch.setattr(service, "_resolve_target", lambda _cfg: {"app_token": "app", "table_id": "tbl_demo"})
    monkeypatch.setattr(service, "_new_client", lambda _cfg, **kwargs: fake_client)

    matched = service.list_existing_records_for_unit(building="A楼", duty_date="2026-03-24")

    assert [item["record_id"] for item in matched] == ["rec_1"]
    assert "CurrentValue.[日期]>=" in fake_client.list_calls[0]["filter_formula"]
    assert "CurrentValue.[日期]<" in fake_client.list_calls[0]["filter_formula"]
    assert 'TODATE("2026-03-24")' in fake_client.list_calls[0]["filter_formula"]
    assert 'TODATE("2026-03-25")' in fake_client.list_calls[0]["filter_formula"]


def test_rewrite_from_output_file_upserts_existing_records(monkeypatch) -> None:
    service = DayMetricBitableExportService(_cfg())
    target_ms = service._midnight_timestamp_ms("2026-03-24")
    fake_client = _FakeClient(
        [
            {"record_id": "rec_1", "fields": {"楼栋": "A楼", "日期": target_ms, "类型": "总负荷（KW）"}},
            {"record_id": "rec_2", "fields": {"楼栋": "A楼", "日期": target_ms, "类型": "IT总负荷（KW）"}},
        ]
    )
    monkeypatch.setattr(service, "_resolve_target", lambda _cfg: {"app_token": "app", "table_id": "tbl_demo"})
    monkeypatch.setattr(service, "_new_client", lambda _cfg, **kwargs: fake_client)
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
        metric_values_by_id={"city_power": 11, "it_power": 22},
        metric_origin_context={},
        emit_log=lambda *_args: None,
    )

    assert result["status"] == "ok"
    assert result["deleted_records"] == 0
    assert result["updated_records"] == 2
    assert result["created_records"] == 0
    assert fake_client.deleted_calls == []
    assert fake_client.created_calls == []
    assert [item["record_id"] for item in fake_client.updated_calls[0]["records"]] == ["rec_1", "rec_2"]
    assert "CurrentValue.[楼栋]" in fake_client.list_calls[0]["filter_formula"]
    assert "CurrentValue.[日期]>=" in fake_client.list_calls[0]["filter_formula"]
    assert "CurrentValue.[日期]<" in fake_client.list_calls[0]["filter_formula"]
    assert 'TODATE("2026-03-24")' in fake_client.list_calls[0]["filter_formula"]


def test_rewrite_from_output_file_falls_back_to_building_scope_when_exact_filter_misses(monkeypatch) -> None:
    service = DayMetricBitableExportService(_cfg())
    target_ms = service._midnight_timestamp_ms("2026-03-24")
    fake_client = _FakeClient([])

    def _list_records(*, table_id, page_size, max_records, filter_formula=""):  # noqa: ANN001
        fake_client.list_calls.append(
            {
                "table_id": table_id,
                "page_size": page_size,
                "max_records": max_records,
                "filter_formula": filter_formula,
            }
        )
        if "CurrentValue.[日期]>=" in filter_formula:
            return []
        return [
            {"record_id": "rec_1", "fields": {"楼栋": "A楼", "日期": target_ms + 12 * 60 * 60 * 1000, "类型": "总负荷（KW）"}},
            {"record_id": "rec_2", "fields": {"楼栋": "A楼", "日期": target_ms + 12 * 60 * 60 * 1000, "类型": "IT总负荷（KW）"}},
        ]

    fake_client.list_records = _list_records  # type: ignore[method-assign]
    monkeypatch.setattr(service, "_resolve_target", lambda _cfg: {"app_token": "app", "table_id": "tbl_demo"})
    monkeypatch.setattr(service, "_new_client", lambda _cfg, **kwargs: fake_client)
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
    logs: list[str] = []

    result = service.rewrite_from_output_file(
        building="A楼",
        duty_date="2026-03-24",
        duty_shift="day",
        output_file="demo.xlsx",
        metric_values_by_id={"city_power": 11, "it_power": 22},
        metric_origin_context={},
        emit_log=logs.append,
    )

    assert result["status"] == "ok"
    assert result["updated_records"] == 2
    assert len(fake_client.list_calls) == 2
    assert "CurrentValue.[日期]>=" in fake_client.list_calls[0]["filter_formula"]
    assert 'TODATE("2026-03-24")' in fake_client.list_calls[0]["filter_formula"]
    assert "CurrentValue.[日期]" not in fake_client.list_calls[1]["filter_formula"]
    assert any("范围过滤匹配旧记录" in line for line in logs)
