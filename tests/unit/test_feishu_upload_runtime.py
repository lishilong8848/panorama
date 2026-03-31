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

    def upload_calc_records(self, records: List[Dict[str, Any]], **kwargs: Any) -> None:
        self.calls.append(("upload_calc_records", records, kwargs))

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
        clients.append(c)
        return c

    source_file = tmp_path / "A楼.xlsx"
    source_file.write_bytes(b"fake")
    result = _FakeResult(
        source_file=str(source_file),
        building="A楼",
        month="2026-03-01",
        values={"PUE": 1.23456},
        records=[{"k": "v"}],
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
    assert client_calls == [
        "upload_calc_records",
        "upload_attachment",
        "upload_attachment_record",
    ]
    assert any("PUE=1.235" in line for line in logs)
    assert any("文件上传成功" in line for line in logs)


def test_upload_results_to_feishu_calc_stage_failure() -> None:
    logs: List[str] = []

    class _FailClient(_FakeClient):
        def upload_calc_records(self, records: List[Dict[str, Any]], **kwargs: Any) -> None:
            raise RuntimeError("calc boom")

    with pytest.raises(RuntimeError):
        upload_results_to_feishu(
            results=[
                _FakeResult(
                    source_file="C:/tmp/A.xlsx",
                    building="A楼",
                    month="2026-03-01",
                    values={"PUE": 1.0},
                    records=[],
                )
            ],
            config=_build_config(enable_upload=True),
            resolve_upload_date_from_runtime=lambda _cfg: "2026-03-01",
            client_factory=lambda **kwargs: _FailClient(**kwargs),
            emit_log=logs.append,
        )

    assert any("文件流程失败" in line and "计算记录上传" in line for line in logs)
