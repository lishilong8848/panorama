from __future__ import annotations

from pathlib import Path

from app.config.config_adapter import ensure_v3_config
from handover_log_module.service.review_session_service import ReviewSessionService
from handover_log_module.service.source_data_attachment_bitable_export_service import (
    SourceDataAttachmentBitableExportService,
)


class _FakeClient:
    def __init__(self) -> None:
        self.deleted: list[dict] = []
        self.created: list[dict] = []
        self.uploaded: list[str] = []

    def list_records(self, **_kwargs):
        return []

    def batch_delete_records(self, table_id: str, record_ids: list[str], batch_size: int = 200) -> int:
        self.deleted.append({"table_id": table_id, "record_ids": list(record_ids), "batch_size": batch_size})
        return len(record_ids)

    def upload_attachment(self, file_path: str) -> str:
        self.uploaded.append(file_path)
        return "file-token-1"

    def batch_create_records(self, table_id: str, fields_list: list[dict], batch_size: int = 1):
        self.created.append({"table_id": table_id, "fields_list": list(fields_list), "batch_size": batch_size})
        return [{"code": 0}]


class _ServiceWithFakeClient(SourceDataAttachmentBitableExportService):
    def __init__(self, cfg: dict, client: _FakeClient) -> None:
        super().__init__(cfg)
        self._client = client

    def _new_client(self, cfg):  # noqa: ANN001, ARG002
        return self._client


def test_build_deferred_state_respects_enabled_and_night_toggle() -> None:
    enabled_cfg = {
        "source_data_attachment_export": {
            "enabled": True,
            "upload_night_shift": False,
        }
    }
    disabled_cfg = {
        "source_data_attachment_export": {
            "enabled": False,
        }
    }
    enabled_service = SourceDataAttachmentBitableExportService(enabled_cfg)
    disabled_service = SourceDataAttachmentBitableExportService(disabled_cfg)

    assert enabled_service.build_deferred_state(duty_shift="day")["status"] == "pending_review"
    assert enabled_service.build_deferred_state(duty_shift="night") == {
        "status": "skipped",
        "reason": "night_shift_disabled",
        "uploaded_count": 0,
        "error": "",
        "uploaded_at": "",
        "uploaded_revision": 0,
    }
    assert disabled_service.build_deferred_state(duty_shift="day") == {
        "status": "skipped",
        "reason": "disabled",
        "uploaded_count": 0,
        "error": "",
        "uploaded_at": "",
        "uploaded_revision": 0,
    }


def test_run_from_source_file_replaces_existing_record_by_strict_tuple(tmp_path: Path) -> None:
    source_file = tmp_path / "source.xlsx"
    source_file.write_bytes(b"demo")
    client = _FakeClient()
    service = _ServiceWithFakeClient(
        {
            "source_data_attachment_export": {
                "enabled": True,
                "source": {
                    "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
                    "table_id": "tblF13MQ10PslIdI",
                    "page_size": 500,
                    "max_records": 5000,
                    "delete_batch_size": 200,
                },
                "fields": {
                    "type": "类型",
                    "building": "楼栋",
                    "date": "日期",
                    "shift": "班次",
                    "attachment": "附件",
                },
                "fixed_values": {
                    "type": "动环数据",
                    "shift_text": {"day": "白班", "night": "夜班"},
                },
                "replace_existing": True,
            }
        },
        client,
    )

    existing_records = [
        {
            "record_id": "rec-match",
            "fields": {
                "类型": "动环数据",
                "楼栋": "A楼",
                "日期": service._midnight_timestamp_ms("2026-03-15"),
                "班次": "白班",
            },
        },
        {
            "record_id": "rec-other-shift",
            "fields": {
                "类型": "动环数据",
                "楼栋": "A楼",
                "日期": service._midnight_timestamp_ms("2026-03-15"),
                "班次": "夜班",
            },
        },
    ]

    result = service.run_from_source_file(
        building="A楼",
        duty_date="2026-03-15",
        duty_shift="day",
        data_file=str(source_file),
        existing_records=existing_records,
        emit_log=lambda *_args: None,
    )

    assert result["status"] == "ok"
    assert result["deleted_record_ids"] == ["rec-match"]
    assert client.deleted == [
        {"table_id": "tblF13MQ10PslIdI", "record_ids": ["rec-match"], "batch_size": 200}
    ]
    assert client.uploaded == [str(source_file)]
    created_row = client.created[0]["fields_list"][0]
    assert created_row["类型"] == "动环数据"
    assert created_row["楼栋"] == "A楼"
    assert created_row["日期"] == service._midnight_timestamp_ms("2026-03-15")
    assert created_row["班次"] == "白班"
    assert created_row["附件"] == [{"file_token": "file-token-1"}]


def test_review_session_service_persists_data_file_and_attachment_state(tmp_path: Path) -> None:
    cfg = ensure_v3_config({})
    cfg.setdefault("features", {}).setdefault("handover_log", {}).setdefault("event_sections", {}).setdefault(
        "cache", {}
    )["state_file"] = "review_test_state.json"
    cfg.setdefault("features", {}).setdefault("handover_log", {})["_global_paths"] = {
        "runtime_state_root": str(tmp_path)
    }
    service = ReviewSessionService(cfg["features"]["handover_log"])

    registered = service.register_generated_output(
        building="A楼",
        duty_date="2026-03-15",
        duty_shift="day",
        data_file=r"D:\QLDownload\A楼源数据.xlsx",
        output_file=r"D:\QLDownload\A楼交接班.xlsx",
        source_mode="from_file",
        day_metric_export={"status": "pending_review", "reason": "await_all_confirmed"},
        source_data_attachment_export={"status": "pending_review", "reason": "await_all_confirmed"},
    )

    assert registered["data_file"] == r"D:\QLDownload\A楼源数据.xlsx"
    assert registered["source_data_attachment_export"]["status"] == "pending_review"

    touched, _batch_status = service.touch_session_after_save(
        building="A楼",
        session_id=registered["session_id"],
        base_revision=registered["revision"],
    )

    assert touched["source_data_attachment_export"]["status"] == "pending_review"
    assert touched["source_data_attachment_export"]["reason"] == "await_all_confirmed"
    assert touched["source_data_attachment_export"]["uploaded_revision"] == 0


def test_run_from_source_file_reports_missing_managed_source_cache(tmp_path: Path) -> None:
    cfg = {
        "_global_paths": {"runtime_state_root": str(tmp_path)},
        "source_data_attachment_export": {
            "enabled": True,
            "source": {
                "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
                "table_id": "tblF13MQ10PslIdI",
            },
        },
    }
    service = SourceDataAttachmentBitableExportService(cfg)
    managed_path = service._source_file_cache_service.build_stored_path(  # noqa: SLF001
        building="A楼",
        duty_date="2026-03-24",
        duty_shift="day",
        session_id="A楼|2026-03-24|day",
        original_name="A楼源数据.xlsx",
    )

    logs: list[str] = []
    result = service.run_from_source_file(
        building="A楼",
        duty_date="2026-03-24",
        duty_shift="day",
        data_file=str(managed_path),
        existing_records=[],
        emit_log=logs.append,
    )

    assert result["status"] == "failed"
    assert result["reason"] == "missing_source_file_cache"
    assert "原因=源文件缓存不存在" in logs[0]
