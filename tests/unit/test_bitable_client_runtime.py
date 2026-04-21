from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient


@dataclass
class _Record:
    type_name: str
    category_name: str
    item_name: str
    month: str
    value: float

    def to_feishu_fields(
        self,
        *,
        date_value: Any,
        type_name: str,
        category_name: str,
        item_name: str,
    ) -> Dict[str, Any]:
        return {
            "类型": type_name,
            "分类": category_name,
            "项目": item_name,
            "日期": date_value,
            "值": self.value,
        }


class _ClientForTest(FeishuBitableClient):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.created: List[Dict[str, Any]] = []

    def batch_create_records(self, table_id: str, fields_list: List[Dict[str, Any]], batch_size: int = 200):  # type: ignore[override]
        self.created.extend(fields_list)
        return [{"data": {"items": []}}]


class _RequestClient(FeishuBitableClient):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.requests: List[Dict[str, Any]] = []

    def _request_json_with_auth_retry(self, method: str, url: str, **kwargs: Any) -> Dict[str, Any]:  # type: ignore[override]
        self.requests.append({"method": method, "url": url, "kwargs": kwargs})
        return {"code": 0, "data": {"records": []}}


def _new_client(date_mode: str = "timestamp") -> _ClientForTest:
    return _ClientForTest(
        app_id="app_id",
        app_secret="app_secret",
        app_token="app_token",
        calc_table_id="tbl_calc",
        attachment_table_id="tbl_attach",
        date_field_mode=date_mode,
        date_field_day=1,
        date_tz_offset_hours=8,
        timeout=30,
        request_retry_count=0,
        request_retry_interval_sec=0,
        date_text_to_timestamp_ms_fn=lambda **_kwargs: 1234567890000,
        canonical_metric_name_fn=lambda x: str(x),
        dimension_mapping={"PUE": ("用电量拆分", "分析指标", "PUE")},
    )


def test_to_feishu_date_text_mode() -> None:
    c = _new_client(date_mode="text")
    assert c._to_feishu_date("2026-03-08") == "2026-03-08"


def test_upload_calc_records_mapping_and_date_override() -> None:
    c = _new_client(date_mode="timestamp")
    records = [
        _Record(
            type_name="原类型",
            category_name="原分类",
            item_name="PUE",
            month="2026-03-01",
            value=1.234,
        )
    ]
    c.upload_calc_records(records, skip_zero_records=False, date_override="2026-03-08")
    assert len(c.created) == 1
    assert c.created[0]["类型"] == "用电量拆分"
    assert c.created[0]["项目"] == "PUE"
    assert c.created[0]["日期"] == 1234567890000


def test_update_record_uses_put_record_endpoint() -> None:
    c = _RequestClient(
        app_id="app_id",
        app_secret="app_secret",
        app_token="app_token",
        calc_table_id="tbl_calc",
        attachment_table_id="tbl_attach",
        date_field_mode="timestamp",
        date_field_day=1,
        date_tz_offset_hours=8,
        timeout=30,
        request_retry_count=0,
        request_retry_interval_sec=0,
        date_text_to_timestamp_ms_fn=lambda **_kwargs: 1234567890000,
        canonical_metric_name_fn=lambda x: str(x),
        dimension_mapping={},
    )

    c.update_record(table_id="tbl_calc", record_id="rec_1", fields={"A": 1})

    assert c.requests[0]["method"] == "PUT"
    assert c.requests[0]["url"].endswith("/tables/tbl_calc/records/rec_1")
    assert c.requests[0]["kwargs"]["payload"] == {"fields": {"A": 1}}


def test_batch_update_records_sends_chunks() -> None:
    c = _RequestClient(
        app_id="app_id",
        app_secret="app_secret",
        app_token="app_token",
        calc_table_id="tbl_calc",
        attachment_table_id="tbl_attach",
        date_field_mode="timestamp",
        date_field_day=1,
        date_tz_offset_hours=8,
        timeout=30,
        request_retry_count=0,
        request_retry_interval_sec=0,
        date_text_to_timestamp_ms_fn=lambda **_kwargs: 1234567890000,
        canonical_metric_name_fn=lambda x: str(x),
        dimension_mapping={},
    )

    c.batch_update_records(
        table_id="tbl_calc",
        records=[
            {"record_id": "rec_1", "fields": {"A": 1}},
            {"record_id": "rec_2", "fields": {"A": 2}},
        ],
        batch_size=1,
    )

    assert len(c.requests) == 2
    assert all(item["method"] == "POST" for item in c.requests)
    assert all(item["url"].endswith("/tables/tbl_calc/records/batch_update") for item in c.requests)
    assert c.requests[0]["kwargs"]["payload"] == {
        "records": [{"record_id": "rec_1", "fields": {"A": 1}}]
    }


def test_list_records_sends_filter_formula() -> None:
    c = _RequestClient(
        app_id="app_id",
        app_secret="app_secret",
        app_token="app_token",
        calc_table_id="tbl_calc",
        attachment_table_id="tbl_attach",
        date_field_mode="timestamp",
        date_field_day=1,
        date_tz_offset_hours=8,
        timeout=30,
        request_retry_count=0,
        request_retry_interval_sec=0,
        date_text_to_timestamp_ms_fn=lambda **_kwargs: 1234567890000,
        canonical_metric_name_fn=lambda x: str(x),
        dimension_mapping={},
    )

    c.list_records(table_id="tbl_calc", filter_formula='CurrentValue.[日期]="2026-03-01"')

    assert c.requests[0]["method"] == "GET"
    assert c.requests[0]["kwargs"]["params"]["filter"] == 'CurrentValue.[日期]="2026-03-01"'
