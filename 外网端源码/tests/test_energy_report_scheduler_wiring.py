from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

from app.bootstrap.container import AppContainer
from app.modules.report_pipeline.service import energy_report_upload_service as module
from app.worker.task_handlers import HANDLER_REGISTRY


def test_scheduler_does_not_submit_until_files_are_ready(monkeypatch, tmp_path):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 1, 8, 30)

    monkeypatch.setattr("app.bootstrap.container.datetime", Clock)
    monkeypatch.setattr(module, "completed", lambda *_: False)
    monkeypatch.setattr(
        "app.modules.feishu.service.feishu_auth_resolver.require_feishu_auth_settings", lambda _: {},
    )
    probe = Mock(side_effect=FileNotFoundError("E楼文件未齐"))
    monkeypatch.setattr(module.EnergyReportUploadService, "source_paths", probe)
    jobs = SimpleNamespace(start_worker_job=Mock(return_value=SimpleNamespace(job_id="test-id")))
    container = SimpleNamespace(
        _configured_deployment_snapshot=lambda: {"role_mode": "external"},
        runtime_config={"paths": {"runtime_state_root": str(tmp_path)}}, job_service=jobs, add_system_log=Mock(),
    )
    assert AppContainer._submit_energy_report_upload(container, "测试") == (False, "skip:source_not_ready")
    jobs.start_worker_job.assert_not_called()

    probe.side_effect = None
    assert AppContainer._submit_energy_report_upload(container, "测试") == (True, "test-id")
    kwargs = jobs.start_worker_job.call_args.kwargs
    assert kwargs["worker_payload"] == {"period": "2026-10"}
    assert kwargs["worker_handler"] in HANDLER_REGISTRY
    assert kwargs["dedupe_key"] == "energy_report_upload:2026-10"
    probe.assert_called_with("2026-10", request_missing=True, log=container.add_system_log)

    monkeypatch.setattr(module, "completed", lambda *_: True)
    assert not AppContainer._submit_energy_report_upload(container, "测试")[0]
    jobs.start_worker_job.assert_called_once()


def test_older_unready_month_does_not_block_ready_month(monkeypatch, tmp_path):
    monkeypatch.setattr(module, "pending_periods", lambda *_: ("2026-09", "2026-10"))
    monkeypatch.setattr("app.modules.feishu.service.feishu_auth_resolver.require_feishu_auth_settings", lambda _: {})
    probe = Mock(side_effect=[FileNotFoundError("9月未齐"), {}])
    monkeypatch.setattr(module.EnergyReportUploadService, "source_paths", probe)
    jobs = SimpleNamespace(start_worker_job=Mock(return_value=SimpleNamespace(job_id="test-id")))
    container = SimpleNamespace(
        _configured_deployment_snapshot=lambda: {"role_mode": "external"},
        runtime_config={"paths": {"runtime_state_root": str(tmp_path)}}, job_service=jobs, add_system_log=Mock(),
    )
    assert AppContainer._submit_energy_report_upload(container, "测试") == (True, "test-id")
    assert [call.args[0] for call in probe.call_args_list] == ["2026-09", "2026-10"]
    assert jobs.start_worker_job.call_args.kwargs["worker_payload"] == {"period": "2026-10"}
