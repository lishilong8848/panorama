from types import SimpleNamespace

from app.bootstrap.container import AppContainer
from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.config_adapter import adapt_runtime_config


def test_alarm_rule_upload_scheduler_defaults_are_monthly_and_enabled():
    scheduler = DEFAULT_CONFIG_V3["features"]["alarm_rule_export_upload"]["scheduler"]
    assert scheduler["enabled"] is True
    assert scheduler["auto_start_in_gui"] is True
    assert scheduler["day_of_month"] == 3
    assert scheduler["run_time"] == "09:00:00"
    assert scheduler["catch_up_if_missed"] is False


def test_alarm_rule_upload_scheduler_reuses_monthly_facade(monkeypatch):
    captured = {}
    def fake_facade(**kwargs):
        captured.update(kwargs)
        return kwargs
    monkeypatch.setattr("app.bootstrap.container.ApschedulerSchedulerFacade", fake_facade)
    runtime = adapt_runtime_config(DEFAULT_CONFIG_V3)
    fake = SimpleNamespace(
        runtime_config=runtime,
        alarm_rule_export_upload_scheduler_callback=None,
        _alarm_rule_export_upload_scheduler_run_callback=lambda source: (True, source),
        _runtime_state_root_text=lambda: ".runtime",
        add_system_log=lambda line: None,
        _job_busy_for_feature_prefixes=lambda *features: (lambda: False),
        ensure_scheduler_orchestrator=lambda: object(),
    )
    result = AppContainer._build_alarm_rule_export_upload_scheduler(fake)
    assert result["scheduler_key"] == "alarm_rule_export_upload"
    assert result["schedule_kind"] == "monthly"
    assert result["scheduler_cfg"]["day_of_month"] == 3
    assert result["scheduler_cfg"]["run_time"] == "09:00:00"

