from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from handover_log_module.api.facade import load_handover_config
from handover_log_module.service.handover_orchestrator import HandoverOrchestrator


def test_load_handover_config_injects_role_and_shared_bridge() -> None:
    runtime_cfg = {
        "deployment": {"role_mode": "external"},
        "download": {},
        "network": {},
        "handover_log": {},
        "shared_bridge": {"root_dir": "C:/QJPT/share"},
    }

    cfg = load_handover_config(runtime_cfg)

    assert cfg["_deployment_role_mode"] == "external"
    assert cfg["_shared_bridge"]["root_dir"] == "C:/QJPT/share"


def test_external_role_uses_alarm_json_summary() -> None:
    cfg = load_handover_config(
        {
            "deployment": {"role_mode": "external"},
            "download": {},
            "network": {},
            "handover_log": {},
            "shared_bridge": {"root_dir": str(Path("C:/QJPT/share"))},
        }
    )
    orchestrator = HandoverOrchestrator(cfg)
    logs: list[str] = []
    alarm_db_called = {"value": False}
    alarm_json_called = {"value": False}

    def _explode_alarm_db(**_kwargs):  # noqa: ANN003
        alarm_db_called["value"] = True
        raise AssertionError("external role should not query alarm db")

    def _query_alarm_json(**_kwargs):  # noqa: ANN003
        alarm_json_called["value"] = True
        return SimpleNamespace(
            total_count=12,
            unrecovered_count=3,
            accept_description="冷机检修中",
            source_kind="manual",
            selection_scope="today",
            selected_downloaded_at="2026-04-03 09:30:00",
            query_start="2026-02-02 00:00:00",
            query_end="2026-04-03 10:00:00",
            coverage_ok=True,
            fallback_used=False,
            error="",
        )

    orchestrator._alarm_json_repo = SimpleNamespace(query_alarm_summary=_query_alarm_json)

    fixed_values, _date_ref, alarm_summary = orchestrator._build_fixed_values_with_alarm(
        building="A楼",
        duty_date="2026-04-01",
        duty_shift="day",
        start_time="2026-04-01 09:00:00",
        end_time="2026-04-01 10:00:00",
        emit_log=logs.append,
        include_roster=False,
    )

    assert alarm_db_called["value"] is False
    assert alarm_json_called["value"] is True
    assert fixed_values["B15"] == "12"
    assert fixed_values["D15"] == "3"
    assert fixed_values["F15"] == "冷机检修中"
    assert alarm_summary["source"] == "alarm_json"
    assert alarm_summary["building"] == "A楼"
    assert alarm_summary["total_count"] == 12
    assert alarm_summary["unrecovered_count"] == 3
    assert alarm_summary["accept_description"] == "冷机检修中"
    assert alarm_summary["coverage_ok"] is True
    assert alarm_summary["fallback_used"] is False
    assert alarm_summary["selection_scope"] == "today"
    assert alarm_summary["source_kind"] == "manual"


def test_external_role_falls_back_to_default_when_alarm_json_fails() -> None:
    cfg = load_handover_config(
        {
            "deployment": {"role_mode": "external"},
            "download": {},
            "network": {},
            "handover_log": {},
        }
    )
    orchestrator = HandoverOrchestrator(cfg)
    logs: list[str] = []

    def _explode_alarm_json(**_kwargs):  # noqa: ANN003
        raise RuntimeError("coverage 不足")

    orchestrator._alarm_json_repo = SimpleNamespace(query_alarm_summary=_explode_alarm_json)

    fixed_values, _date_ref, alarm_summary = orchestrator._build_fixed_values_with_alarm(
        building="A楼",
        duty_date="2026-04-01",
        duty_shift="day",
        start_time="2026-04-01 09:00:00",
        end_time="2026-04-01 10:00:00",
        emit_log=logs.append,
        include_roster=False,
    )

    assert fixed_values["B15"] == "0"
    assert fixed_values["D15"] == "0"
    assert fixed_values["F15"] == "/"
    assert alarm_summary["source"] == "alarm_json"
    assert alarm_summary["coverage_ok"] is False
    assert alarm_summary["fallback_used"] is True
    assert alarm_summary["error"] == "coverage 不足"
    assert any("按兜底填充" in item for item in logs)


def test_internal_role_uses_alarm_json_summary() -> None:
    cfg = load_handover_config(
        {
            "deployment": {"role_mode": "internal"},
            "download": {},
            "network": {},
            "handover_log": {},
            "shared_bridge": {"root_dir": str(Path("C:/QJPT/share"))},
        }
    )
    orchestrator = HandoverOrchestrator(cfg)
    logs: list[str] = []
    called = {"value": False}

    def _query_alarm_summary(**_kwargs):  # noqa: ANN003
        called["value"] = True
        return SimpleNamespace(
            total_count=12,
            unrecovered_count=3,
            accept_description="已接警",
            source_kind="latest",
            selection_scope="today",
            selected_downloaded_at="2026-04-01 09:30:00",
            query_start="2026-02-01 00:00:00",
            query_end="2026-04-01 10:00:00",
            coverage_ok=True,
            fallback_used=False,
            error="",
        )

    orchestrator._alarm_json_repo = SimpleNamespace(query_alarm_summary=_query_alarm_summary)

    fixed_values, _date_ref, alarm_summary = orchestrator._build_fixed_values_with_alarm(
        building="A楼",
        duty_date="2026-04-01",
        duty_shift="day",
        start_time="2026-04-01 09:00:00",
        end_time="2026-04-01 10:00:00",
        emit_log=logs.append,
        include_roster=False,
    )

    assert called["value"] is True
    assert fixed_values["B15"] == "12"
    assert fixed_values["D15"] == "3"
    assert fixed_values["F15"] == "已接警"
    assert alarm_summary["source"] == "alarm_json"
    assert alarm_summary["coverage_ok"] is True
    assert alarm_summary["fallback_used"] is False
    assert not any("按兜底填充" in item for item in logs)
