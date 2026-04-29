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
