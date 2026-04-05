from __future__ import annotations

from pathlib import Path

import openpyxl

from app.config.config_adapter import ensure_v3_config
from handover_log_module.service.handover_orchestrator import HandoverOrchestrator


def _build_orchestrator(tmp_path: Path, *, role_mode: str = "internal") -> HandoverOrchestrator:
    cfg = ensure_v3_config({})
    handover_cfg = cfg.setdefault("features", {}).setdefault("handover_log", {})
    handover_cfg.setdefault("event_sections", {}).setdefault("cache", {})["state_file"] = "orchestrator_source_cache_state.json"
    handover_cfg["_global_paths"] = {"runtime_state_root": str(tmp_path)}
    handover_cfg["_deployment_role_mode"] = role_mode
    return HandoverOrchestrator(handover_cfg)


def _build_workbook(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = "handover"
    worksheet["A1"] = "source"
    workbook.save(path)
    workbook.close()


def test_run_from_existing_file_registers_managed_source_file_cache(tmp_path: Path, monkeypatch) -> None:
    orchestrator = _build_orchestrator(tmp_path)
    input_file = tmp_path / "temp" / "input.xlsx"
    _build_workbook(input_file)
    output_file = tmp_path / "outputs" / "A楼_20260324_交接班日志.xlsx"

    monkeypatch.setattr(
        orchestrator._extract_service,
        "extract",
        lambda **_kwargs: {"hits": {}, "effective_config": {}},
    )

    def _fake_fill(**_kwargs):
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_bytes(b"filled")
        return {
            "output_file": str(output_file),
            "fills": {},
            "missing_metric_to_cell": {},
            "resolved_values_by_id": {},
        }

    monkeypatch.setattr(orchestrator._fill_service, "fill", _fake_fill)
    monkeypatch.setattr(orchestrator, "_build_shift_roster_fixed_values", lambda **_kwargs: {})
    monkeypatch.setattr(
        orchestrator._day_metric_export_service,
        "build_deferred_state",
        lambda **_kwargs: {
            "status": "pending_review",
            "reason": "await_all_confirmed",
            "uploaded_count": 0,
            "error": "",
            "uploaded_at": "",
            "uploaded_revision": 0,
            "metric_values_by_id": {},
        },
    )
    monkeypatch.setattr(
        orchestrator._source_data_attachment_export_service,
        "build_deferred_state",
        lambda **_kwargs: {
            "status": "pending_review",
            "reason": "await_all_confirmed",
            "uploaded_count": 0,
            "error": "",
            "uploaded_at": "",
            "uploaded_revision": 0,
        },
    )

    summary = orchestrator.run_from_existing_file(
        building="A楼",
        data_file=str(input_file),
        duty_date="2026-03-24",
        duty_shift="day",
        fixed_cell_values={},
        category_payloads={},
        source_mode="from_file",
        emit_log=lambda *_args: None,
    )

    row = summary["results"][0]
    session = row["review_session"]
    stored_path = Path(session["data_file"])
    assert stored_path.exists()
    assert stored_path != input_file
    assert session["source_file_cache"]["managed"] is True
    assert session["source_file_cache"]["stored_path"] == str(stored_path)

    input_file.unlink()
    saved_state = orchestrator._review_session_service._cache_store.load_state()  # noqa: SLF001
    saved_session = saved_state["review_sessions"][session["session_id"]]
    assert saved_session["data_file"] == str(stored_path)
    assert Path(saved_session["data_file"]).exists()


def test_run_from_existing_file_builds_metric_origin_context_for_deferred_state(tmp_path: Path, monkeypatch) -> None:
    orchestrator = _build_orchestrator(tmp_path)
    input_file = tmp_path / "temp" / "input.xlsx"
    _build_workbook(input_file)
    output_file = tmp_path / "outputs" / "A楼_20260324_交接班日志.xlsx"
    captured: dict = {}

    monkeypatch.setattr(
        orchestrator._extract_service,
        "extract",
        lambda **_kwargs: {
            "hits": {
                "city_power": {
                    "row_index": 12,
                    "b_norm": "A-401",
                    "c_norm": "",
                    "b_text": "A-401",
                    "c_text": "",
                }
            },
            "effective_config": {"cell_mapping": {"city_power": "D6"}},
        },
    )

    def _fake_fill(**_kwargs):
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_bytes(b"filled")
        return {
            "output_file": str(output_file),
            "fills": {},
            "missing_metric_to_cell": {},
            "resolved_values_by_id": {"city_power": 11},
        }

    monkeypatch.setattr(orchestrator._fill_service, "fill", _fake_fill)
    monkeypatch.setattr(orchestrator, "_build_shift_roster_fixed_values", lambda **_kwargs: {})

    def _fake_build_deferred_state(**kwargs):
        captured.update(kwargs)
        return {
            "status": "pending_review",
            "reason": "await_all_confirmed",
            "uploaded_count": 0,
            "error": "",
            "uploaded_at": "",
            "uploaded_revision": 0,
            "metric_values_by_id": kwargs.get("resolved_values_by_id", {}),
            "metric_origin_context": kwargs.get("metric_origin_context", {}),
        }

    monkeypatch.setattr(orchestrator._day_metric_export_service, "build_deferred_state", _fake_build_deferred_state)
    monkeypatch.setattr(
        orchestrator._source_data_attachment_export_service,
        "build_deferred_state",
        lambda **_kwargs: {
            "status": "pending_review",
            "reason": "await_all_confirmed",
            "uploaded_count": 0,
            "error": "",
            "uploaded_at": "",
            "uploaded_revision": 0,
        },
    )

    orchestrator.run_from_existing_file(
        building="A楼",
        data_file=str(input_file),
        duty_date="2026-03-24",
        duty_shift="day",
        fixed_cell_values={},
        category_payloads={},
        source_mode="from_file",
        emit_log=lambda *_args: None,
    )

    assert captured["resolved_values_by_id"] == {"city_power": 11}
    assert captured["metric_origin_context"]["by_metric_id"]["city_power"]["b_norm"] == "A-401"
    assert captured["metric_origin_context"]["by_target_cell"]["D6"]["metric_key"] == "city_power"


def test_run_from_existing_file_external_keeps_shared_source_path(tmp_path: Path, monkeypatch) -> None:
    orchestrator = _build_orchestrator(tmp_path, role_mode="external")
    input_file = tmp_path / "shared" / "A楼_source.xlsx"
    _build_workbook(input_file)
    output_file = tmp_path / "outputs" / "A楼_20260324_交接班日志.xlsx"

    monkeypatch.setattr(
        orchestrator._extract_service,
        "extract",
        lambda **_kwargs: {"hits": {}, "effective_config": {}},
    )

    def _fake_fill(**_kwargs):
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_bytes(b"filled")
        return {
            "output_file": str(output_file),
            "fills": {},
            "missing_metric_to_cell": {},
            "resolved_values_by_id": {},
        }

    monkeypatch.setattr(orchestrator._fill_service, "fill", _fake_fill)
    monkeypatch.setattr(orchestrator, "_build_shift_roster_fixed_values", lambda **_kwargs: {})
    monkeypatch.setattr(
        orchestrator._day_metric_export_service,
        "build_deferred_state",
        lambda **_kwargs: {
            "status": "pending_review",
            "reason": "await_all_confirmed",
            "uploaded_count": 0,
            "error": "",
            "uploaded_at": "",
            "uploaded_revision": 0,
            "metric_values_by_id": {},
        },
    )
    monkeypatch.setattr(
        orchestrator._source_data_attachment_export_service,
        "build_deferred_state",
        lambda **_kwargs: {
            "status": "pending_review",
            "reason": "await_all_confirmed",
            "uploaded_count": 0,
            "error": "",
            "uploaded_at": "",
            "uploaded_revision": 0,
        },
    )

    summary = orchestrator.run_from_existing_file(
        building="A楼",
        data_file=str(input_file),
        duty_date="2026-03-24",
        duty_shift="day",
        fixed_cell_values={},
        category_payloads={},
        source_mode="from_file",
        emit_log=lambda *_args: None,
    )

    row = summary["results"][0]
    session = row["review_session"]
    assert session["data_file"] == str(input_file)
    assert session["source_file_cache"]["managed"] is False
    assert session["source_file_cache"]["stored_path"] == ""
