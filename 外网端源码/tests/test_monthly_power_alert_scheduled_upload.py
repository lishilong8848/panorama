from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.bootstrap import app_factory
from app.worker import task_handlers


@pytest.mark.parametrize("now, expected", [
    (datetime(2026, 9, 3, 9, 30), ("2026", 8)),
    (datetime(2026, 1, 3, 9, 30), ("2025", 12)),
])
def test_monthly_schedule_submits_two_independent_jobs_for_same_month(monkeypatch, now, expected):
    start = Mock(side_effect=[SimpleNamespace(job_id="top5-job"), SimpleNamespace(job_id="stats-job")])
    container = SimpleNamespace(
        runtime_config={}, job_service=SimpleNamespace(start_worker_job=start), add_system_log=Mock(),
    )
    service = Mock(return_value=SimpleNamespace(all_buildings=lambda: ["A楼", "B楼"]))
    monkeypatch.setattr(app_factory, "Top5PowerReportService", service)
    year, month = app_factory._previous_calendar_year_month(now)

    ok, detail = app_factory._submit_top5_monthly_report_jobs(container, year=year, month=month)

    assert ok is True
    assert "top5-job" in detail and "stats-job" in detail
    top5, stats = [call.kwargs for call in start.call_args_list]
    assert top5["worker_handler"] == "top5_power_report"
    assert stats["worker_handler"] == "monthly_power_alert_report"
    for job in (top5, stats):
        assert (job["worker_payload"]["year"], job["worker_payload"]["month"]) == expected
        assert job["priority"] == "scheduler"
        assert job["submitted_by"] == "scheduler"
    assert top5["resource_keys"] == ["top5_power_report:global"]
    assert stats["resource_keys"] == [f"monthly_power_alert_report:{year}-{month:02d}"]
    assert top5["dedupe_key"] != stats["dedupe_key"]
    assert stats["worker_payload"]["upload_to_bitable"] is True


@pytest.mark.parametrize("failed_handler", ["top5_power_report", "monthly_power_alert_report"])
def test_submission_failure_does_not_prevent_other_report(monkeypatch, failed_handler):
    calls = []

    def start(**kwargs):
        calls.append(kwargs)
        if kwargs["worker_handler"] == failed_handler:
            raise RuntimeError("submit failed")
        return SimpleNamespace(job_id="accepted-job")

    container = SimpleNamespace(
        runtime_config={}, job_service=SimpleNamespace(start_worker_job=start), add_system_log=Mock(),
    )
    monkeypatch.setattr(app_factory, "Top5PowerReportService", Mock(
        return_value=SimpleNamespace(all_buildings=lambda: ["A楼"]),
    ))

    ok, detail = app_factory._submit_top5_monthly_report_jobs(container, year="2026", month=8)

    assert ok is False
    assert len(calls) == 2
    assert "accepted-job" in detail and "submit failed" in detail


def _mock_report_worker(monkeypatch, result):
    generator = Mock()
    generator.run.return_value = result
    uploader = Mock()
    uploader.upload_report.return_value = {"status": "ok", "record_id": "stats-record"}
    notify = Mock()
    monkeypatch.setattr(task_handlers, "MonthlyPowerAlertReportService", Mock(return_value=generator))
    factory = Mock(return_value=uploader)
    monkeypatch.setattr(task_handlers, "Top5PowerReportBitableUploadService", factory)
    monkeypatch.setattr(task_handlers, "WebhookNotifyService", Mock(return_value=notify))
    return generator, factory, uploader, notify


def test_scheduled_worker_uploads_generated_file_and_requested_month(monkeypatch):
    generator, _, uploader, notify = _mock_report_worker(monkeypatch, {
        "status": "ok", "output_file": "stats.xlsx", "year": "2025", "month": "12",
    })

    result = task_handlers.handle_monthly_power_alert_report(
        {}, {"year": "2025", "month": 12, "upload_to_bitable": True}, lambda _line: None,
    )

    assert generator.run.call_args.kwargs["year"] == "2025"
    uploaded = uploader.upload_report.call_args.kwargs
    assert uploaded["file_path"] == "stats.xlsx"
    assert uploaded["year"] == "2025" and uploaded["month"] == 12
    assert uploaded["sub_category"] == "机柜超功耗"
    assert uploaded["report_name"] == "月度超功率统计表"
    assert result["bitable_upload"]["record_id"] == "stats-record"
    notify.send_failure.assert_not_called()


@pytest.mark.parametrize("payload, generated", [
    ({"year": "2026", "month": 8}, {"status": "ok", "output_file": "stats.xlsx"}),
    ({"year": "2026", "month": 8, "upload_to_bitable": True}, {"status": "skipped"}),
])
def test_manual_generation_and_disabled_report_do_not_upload(monkeypatch, payload, generated):
    _, factory, _, _ = _mock_report_worker(monkeypatch, generated)

    task_handlers.handle_monthly_power_alert_report({}, payload, lambda _line: None)

    factory.assert_not_called()


def test_scheduled_upload_failure_is_not_reported_as_success(monkeypatch, tmp_path):
    output_file = tmp_path / "stats.xlsx"
    output_file.touch()
    _, _, uploader, notify = _mock_report_worker(monkeypatch, {
        "status": "ok", "output_file": str(output_file),
    })
    uploader.upload_report.side_effect = RuntimeError("Feishu timeout")

    with pytest.raises(RuntimeError, match="Feishu timeout"):
        task_handlers.handle_monthly_power_alert_report(
            {}, {"year": "2026", "month": 8, "upload_to_bitable": True}, lambda _line: None,
        )

    assert output_file.exists()
    notify.send_failure.assert_called_once()


def test_disabled_upload_is_not_reported_as_uploaded(monkeypatch):
    _, _, uploader, _ = _mock_report_worker(monkeypatch, {"status": "ok", "output_file": "stats.xlsx"})
    uploader.upload_report.return_value = {"status": "skipped", "reason": "disabled"}

    with pytest.raises(RuntimeError, match="未上传多维: disabled"):
        task_handlers.handle_monthly_power_alert_report(
            {}, {"year": "2026", "month": 8, "upload_to_bitable": True}, lambda _line: None,
        )
