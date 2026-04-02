from __future__ import annotations

from pathlib import Path

from app.bootstrap.container import AppContainer
from app.modules.report_pipeline.service.job_service import JobService
from app.modules.report_pipeline.service.system_alert_log_upload_service import (
    SystemAlertLogUploadService,
)


class _FakeClient:
    def __init__(self) -> None:
        self.created_calls = []

    def batch_create_records(self, *, table_id, fields_list, batch_size):  # noqa: ANN001
        self.created_calls.append(
            {
                "table_id": table_id,
                "fields_list": list(fields_list),
                "batch_size": batch_size,
            }
        )
        return []


def test_container_keeps_structured_alert_entries() -> None:
    container = AppContainer(
        config={"common": {"console": {"log_buffer_size": 200}}},
        runtime_config={},
        config_path=Path("config.json"),
        frontend_mode="source",
        frontend_root=Path("."),
        frontend_assets_dir=Path("."),
        job_service=JobService(),
    )

    container.add_system_log("[配置] 已保存", write_console=False)
    container.add_system_log(
        r"D:\demo.py:10: UserWarning: Unknown extension is not supported and will be removed",
        source="job",
        write_console=False,
    )
    container.add_system_log("任务执行失败: demo", write_console=False)

    all_entries = container.get_system_log_entries(limit=10)
    alert_entries = container.get_system_log_entries(levels={"warning", "error"}, limit=10)

    assert [item["level"] for item in all_entries] == ["info", "warning", "error"]
    assert [item["source"] for item in alert_entries] == ["python_warning", "system"]
    assert container.system_log_next_offset() == 3


def test_system_alert_log_upload_service_uploads_only_alert_lines(tmp_path: Path, monkeypatch) -> None:
    uploaded_ids = []
    runtime_root = tmp_path / "runtime"
    service = SystemAlertLogUploadService(
        config_getter=lambda: {"common": {"feishu_auth": {"app_id": "app", "app_secret": "secret"}}},
        active_job_id_getter=lambda: "",
        emit_log=lambda _text: None,
        runtime_state_root=str(runtime_root),
        mark_uploaded=lambda ids: uploaded_ids.extend(ids),
    )
    fake_client = _FakeClient()
    monkeypatch.setattr(service, "_build_client", lambda: fake_client)

    service.enqueue_entry(
        {
            "id": 7,
            "timestamp": "2026-03-25 16:18:58",
            "level": "warning",
            "source": "python_warning",
            "line": "[2026-03-25 16:18:58] D:\\demo.py:10: UserWarning: Conditional Formatting extension is not supported and will be removed",
            "uploaded": False,
        }
    )
    service._flush_pending()

    assert fake_client.created_calls[0]["table_id"] == "tblGy6Z1GTxbY1EQ"
    assert fake_client.created_calls[0]["fields_list"] == [
        {
            "日志信息": "[2026-03-25 16:18:58] D:\\demo.py:10: UserWarning: Conditional Formatting extension is not supported and will be removed"
        }
    ]
    assert uploaded_ids == [7]
    snapshot = service.runtime_snapshot()
    assert snapshot["pending_lines"] == 0
    assert snapshot["last_error"] == ""


def test_system_alert_log_upload_service_compacts_uploaded_queue_file(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    service = SystemAlertLogUploadService(
        config_getter=lambda: {"common": {"feishu_auth": {"app_id": "app", "app_secret": "secret"}}},
        active_job_id_getter=lambda: "",
        emit_log=lambda _text: None,
        runtime_state_root=str(runtime_root),
    )

    for index in range(3):
        service.enqueue_entry(
            {
                "id": index + 1,
                "timestamp": "2026-03-25 16:18:58",
                "level": "warning",
                "source": "system",
                "line": f"line-{index + 1}",
            }
        )
    service._state["uploaded_line_count"] = service.COMPACT_UPLOADED_LINES_THRESHOLD
    service._save_state()
    service._queue_path.write_text(
        "\n".join(
            ['{"timestamp":"2026-03-25 16:18:58","line":"historical"}'] * service.COMPACT_UPLOADED_LINES_THRESHOLD
            + ['{"timestamp":"2026-03-25 16:18:58","line":"line-3"}']
        )
        + "\n",
        encoding="utf-8",
    )
    service._compact_queue_file_if_needed()

    assert service._queue_path.read_text(encoding="utf-8").strip().endswith("line-3\"}")
    assert int(service._state.get("uploaded_line_count", 0) or 0) == 0


def test_job_service_global_sink_receives_job_output() -> None:
    service = JobService()
    captured = []
    service.set_global_log_sink(lambda text: captured.append(str(text or "").strip()))

    job = service.start_job(
        name="demo",
        run_func=lambda emit_log: emit_log("D:\\demo.py:10: UserWarning: Unknown extension is not supported and will be removed"),
    )
    service.wait_job(job.job_id, timeout_sec=5)

    assert any("UserWarning:" in item for item in captured)
