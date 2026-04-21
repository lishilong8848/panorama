from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import pytest

from app.modules.report_pipeline.service.feishu_upload_runtime import (
    upload_results_to_feishu,
)


@dataclass
class _FakeResult:
    source_file: str
    building: str
    month: str
    values: Dict[str, float]
    records: List[Dict[str, Any]]


class _FakeClient:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.calls: List[tuple] = []
        self.calc_records: List[Dict[str, Any]] = []
        self.attachment_records: List[Dict[str, Any]] = []

    def _to_feishu_date(self, date_text: str) -> str:
        return date_text

    def list_records(
        self,
        table_id: str,
        page_size: int = 500,
        max_records: int = 0,
        *,
        view_id: str = "",
        filter_formula: str = "",
    ) -> List[Dict[str, Any]]:
        self.calls.append(("list_records", table_id, page_size, max_records, view_id, filter_formula))
        if table_id == "calc_table":
            return list(self.calc_records)
        if table_id == "attach_table":
            return list(self.attachment_records)
        return []

    def batch_delete_records(self, table_id: str, record_ids: List[str], batch_size: int = 500) -> int:
        self.calls.append(("batch_delete_records", table_id, list(record_ids), batch_size))
        return len(record_ids)

    def build_calc_record_fields(self, records: List[Dict[str, Any]], **kwargs: Any) -> List[Dict[str, Any]]:
        self.calls.append(("build_calc_record_fields", records, kwargs))
        return [dict(record) for record in records]

    def batch_update_records(self, table_id: str, records: List[Dict[str, Any]], batch_size: int = 200) -> None:
        self.calls.append(("batch_update_records", table_id, list(records), batch_size))

    def batch_create_records(self, table_id: str, fields_list: List[Dict[str, Any]], batch_size: int = 200) -> None:
        self.calls.append(("batch_create_records", table_id, list(fields_list), batch_size))

    def update_record(self, table_id: str, record_id: str, fields: Dict[str, Any]) -> None:
        self.calls.append(("update_record", table_id, record_id, dict(fields)))

    def upload_attachment(self, source_file: str) -> str:
        self.calls.append(("upload_attachment", source_file))
        return "token_1"

    def upload_attachment_record(self, **kwargs: Any) -> None:
        self.calls.append(("upload_attachment_record", kwargs))


def _build_config(enable_upload: bool = True) -> Dict[str, Any]:
    return {
        "feishu": {
            "enable_upload": enable_upload,
            "app_id": "app_id",
            "app_secret": "app_secret",
            "app_token": "app_token",
            "calc_table_id": "calc_table",
            "attachment_table_id": "attach_table",
            "date_field_mode": "timestamp",
            "date_field_day": 1,
            "date_tz_offset_hours": 8,
            "timeout": 30,
            "request_retry_count": 2,
            "request_retry_interval_sec": 1,
            "report_type": "全景平台月报",
            "skip_zero_records": False,
        }
    }


def test_upload_results_to_feishu_skip_when_disabled() -> None:
    logs: List[str] = []

    upload_results_to_feishu(
        results=[],
        config=_build_config(enable_upload=False),
        resolve_upload_date_from_runtime=lambda _cfg: None,
        client_factory=lambda **_: (_ for _ in ()).throw(RuntimeError("should not init client")),
        emit_log=logs.append,
    )

    assert any("上传" in line and "关闭" in line for line in logs)


def test_upload_results_to_feishu_success_flow(tmp_path: Path) -> None:
    logs: List[str] = []
    clients: List[_FakeClient] = []

    def _factory(**kwargs: Any) -> _FakeClient:
        c = _FakeClient(**kwargs)
        c.calc_records = [
            {
                "record_id": "rec_calc_1",
                "fields": {"楼栋": "A楼", "日期": "2026-03-01", "类型": "用电", "分类": "总览", "项目": "PUE"},
            }
        ]
        c.attachment_records = [
            {
                "record_id": "rec_attach_1",
                "fields": {"类型": "全景平台月报", "楼栋": "A楼", "日期": "2026-03-01"},
            }
        ]
        clients.append(c)
        return c

    source_file = tmp_path / "A楼.xlsx"
    source_file.write_bytes(b"fake")
    result = _FakeResult(
        source_file=str(source_file),
        building="A楼",
        month="2026-03-01",
        values={"PUE": 1.23456},
        records=[{"楼栋": "A楼", "日期": "2026-03-01", "类型": "用电", "分类": "总览", "项目": "PUE", "值": 1.23}],
    )

    upload_results_to_feishu(
        results=[result],
        config=_build_config(enable_upload=True),
        resolve_upload_date_from_runtime=lambda _cfg: "2026-03-01",
        client_factory=_factory,
        emit_log=logs.append,
    )

    assert len(clients) == 1
    client_calls = [c[0] for c in clients[0].calls]
    assert client_calls.count("list_records") == 2
    assert client_calls.count("batch_delete_records") == 0
    assert client_calls[-4:] == [
        "build_calc_record_fields",
        "batch_update_records",
        "upload_attachment",
        "update_record",
    ]
    list_calls = [call for call in clients[0].calls if call[0] == "list_records"]
    assert "CurrentValue.[楼栋]" in list_calls[0][5]
    assert "CurrentValue.[日期]" in list_calls[0][5]
    assert "CurrentValue.[类型]" in list_calls[1][5]
    assert any("PUE=1.235" in line for line in logs)
    assert any("开始准备按日期 upsert" in line for line in logs)
    assert any("文件上传成功" in line for line in logs)


def test_upload_results_to_feishu_calc_stage_failure() -> None:
    logs: List[str] = []

    class _FailClient(_FakeClient):
        def batch_create_records(self, table_id: str, fields_list: List[Dict[str, Any]], batch_size: int = 200) -> None:
            raise RuntimeError("calc boom")

    with pytest.raises(RuntimeError):
        upload_results_to_feishu(
            results=[
                _FakeResult(
                    source_file="C:/tmp/A.xlsx",
                    building="A楼",
                    month="2026-03-01",
                    values={"PUE": 1.0},
                    records=[{"楼栋": "A楼", "日期": "2026-03-01", "类型": "用电", "分类": "总览", "项目": "PUE"}],
                )
            ],
            config=_build_config(enable_upload=True),
            resolve_upload_date_from_runtime=lambda _cfg: "2026-03-01",
            client_factory=lambda **kwargs: _FailClient(**kwargs),
            emit_log=logs.append,
        )

    assert any("文件流程失败" in line and "计算记录上传" in line for line in logs)


def test_upload_results_to_feishu_queries_old_records_by_building_and_date(tmp_path: Path) -> None:
    logs: List[str] = []
    clients: List[_FakeClient] = []

    def _factory(**kwargs: Any) -> _FakeClient:
        c = _FakeClient(**kwargs)
        c.calc_records = [
            {"record_id": "rec_calc_a", "fields": {"楼栋": "A楼", "日期": "2026-03-01"}},
            {"record_id": "rec_calc_b", "fields": {"楼栋": "B楼", "日期": "2026-03-01"}},
        ]
        c.attachment_records = [
            {"record_id": "rec_attach_a", "fields": {"类型": "全景平台月报", "楼栋": "A楼", "日期": "2026-03-01"}},
            {"record_id": "rec_attach_b", "fields": {"类型": "全景平台月报", "楼栋": "B楼", "日期": "2026-03-01"}},
        ]
        clients.append(c)
        return c

    source_a = tmp_path / "A楼.xlsx"
    source_b = tmp_path / "B楼.xlsx"
    source_a.write_bytes(b"a")
    source_b.write_bytes(b"b")
    results = [
        _FakeResult(
            source_file=str(source_a),
            building="A楼",
            month="2026-03-01",
            values={"PUE": 1.1},
            records=[{"楼栋": "A楼", "日期": "2026-03-01", "类型": "用电", "分类": "总览", "项目": "PUE"}],
        ),
        _FakeResult(
            source_file=str(source_b),
            building="B楼",
            month="2026-03-01",
            values={"PUE": 1.2},
            records=[{"楼栋": "B楼", "日期": "2026-03-01", "类型": "用电", "分类": "总览", "项目": "PUE"}],
        ),
    ]

    upload_results_to_feishu(
        results=results,
        config=_build_config(enable_upload=True),
        resolve_upload_date_from_runtime=lambda _cfg: "2026-03-01",
        client_factory=_factory,
        emit_log=logs.append,
    )

    assert len(clients) == 1
    list_calls = [call for call in clients[0].calls if call[0] == "list_records"]
    assert len(list_calls) == 4
    assert [call[1] for call in list_calls] == ["calc_table", "attach_table", "calc_table", "attach_table"]
    assert "A楼" in list_calls[0][5]
    assert "A楼" in list_calls[1][5]
    assert "B楼" in list_calls[2][5]
    assert "B楼" in list_calls[3][5]
    delete_calls = [call for call in clients[0].calls if call[0] == "batch_delete_records"]
    assert len(delete_calls) == 0
    assert any("楼栋=A楼" in line and "已按日期读取计算记录" in line for line in logs)
    assert any("楼栋=B楼" in line and "已按日期读取附件记录" in line for line in logs)
