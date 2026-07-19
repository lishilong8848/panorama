from __future__ import annotations

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from handover_log_module.service.source_data_attachment_bitable_export_service import (
    SourceDataAttachmentBitableExportService,
)


class FakeBitableClient:
    def __init__(self, *, upload_error: Exception | None = None) -> None:
        self.upload_error = upload_error
        self.upload_calls = []
        self.created = []
        self.deleted = []

    def upload_attachment(self, file_path, timeout=None):
        self.upload_calls.append({"file_path": file_path, "timeout": timeout})
        if self.upload_error is not None:
            raise self.upload_error
        return "file-token"

    def batch_create_records(self, table_id, fields_list, batch_size=200, progress_callback=None):
        self.created.extend(fields_list)
        return [{"code": 0}]

    def batch_delete_records(self, table_id, record_ids, batch_size=500, progress_callback=None):
        self.deleted.extend(record_ids)
        return len(record_ids)


def _service_config(upload_timeout_sec=120):
    return {
        "source_data_attachment_export": {
            "source": {
                "upload_timeout_sec": upload_timeout_sec,
            }
        }
    }


def test_upload_attachment_forwards_explicit_timeout(tmp_path, monkeypatch):
    source_file = tmp_path / "source.xlsx"
    source_file.write_bytes(b"xlsx")
    client = object.__new__(FeishuBitableClient)
    captured = {}

    def fake_upload_attachment_bytes(**kwargs):
        captured.update(kwargs)
        return "file-token"

    monkeypatch.setattr(client, "upload_attachment_bytes", fake_upload_attachment_bytes)

    assert client.upload_attachment(str(source_file), timeout=120) == "file-token"
    assert captured["timeout"] == 120
    assert captured["content"] == b"xlsx"


def test_source_attachment_uses_dedicated_upload_timeout(tmp_path, monkeypatch):
    source_file = tmp_path / "source.xlsx"
    source_file.write_bytes(b"xlsx")
    fake_client = FakeBitableClient()
    service = SourceDataAttachmentBitableExportService(_service_config(upload_timeout_sec=150))
    monkeypatch.setattr(service, "_new_client", lambda _cfg: fake_client)

    result = service.run_from_source_file(
        building="C楼",
        duty_date="2026-07-18",
        duty_shift="day",
        data_file=str(source_file),
        existing_records=[],
        emit_log=lambda _message: None,
    )

    assert result["status"] == "ok"
    assert fake_client.upload_calls == [{"file_path": str(source_file), "timeout": 150}]
    assert len(fake_client.created) == 1


def test_source_attachment_upload_failure_preserves_existing_records(tmp_path, monkeypatch):
    source_file = tmp_path / "source.xlsx"
    source_file.write_bytes(b"xlsx")
    fake_client = FakeBitableClient(upload_error=TimeoutError("upload timed out"))
    service = SourceDataAttachmentBitableExportService(_service_config())
    monkeypatch.setattr(service, "_new_client", lambda _cfg: fake_client)

    result = service.run_from_source_file(
        building="E楼",
        duty_date="2026-07-18",
        duty_shift="day",
        data_file=str(source_file),
        existing_records=[
            {
                "record_id": "rec-old",
                "fields": {
                    "类型": "动环数据",
                    "楼栋": "E楼",
                    "日期": 1784304000000,
                    "班次": "白班",
                },
            }
        ],
        emit_log=lambda _message: None,
    )

    assert result["status"] == "failed"
    assert fake_client.created == []
    assert fake_client.deleted == []
