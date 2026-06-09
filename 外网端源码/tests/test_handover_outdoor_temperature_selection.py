from handover_log_module.service.handover_orchestrator import HandoverOrchestrator


def _build_orchestrator() -> HandoverOrchestrator:
    orchestrator = HandoverOrchestrator({})
    orchestrator._send_outdoor_temperature_anomaly_alerts = lambda **kwargs: None  # noqa: SLF001
    return orchestrator


def test_outdoor_temperature_skips_invalid_a_and_uses_b():
    orchestrator = _build_orchestrator()
    values = {
        "A楼": {"B7": "25.0", "D7": "26.0"},
        "B楼": {"B7": "27.0", "D7": "24.0"},
    }
    orchestrator._extract_outdoor_temperature_cells_from_source = (  # noqa: SLF001
        lambda *, building, data_file, emit_log: values.get(building, {})
    )

    result = orchestrator._resolve_shared_outdoor_temperature_cells(  # noqa: SLF001
        [{"building": "A楼", "file_path": "a.xlsx"}, {"building": "B楼", "file_path": "b.xlsx"}],
        duty_date="2026-06-09",
        duty_shift="day",
        emit_log=lambda text: None,
    )

    assert result["B7"] == "27.0"
    assert result["D7"] == "24.0"
    assert result["_selected_building"] == "B楼"


def test_outdoor_temperature_all_invalid_returns_blank():
    orchestrator = _build_orchestrator()
    values = {
        "A楼": {"B7": "25.0", "D7": "26.0"},
        "B楼": {"B7": "", "D7": "24.0"},
        "C楼": {"B7": "abc", "D7": "24.0"},
    }
    orchestrator._extract_outdoor_temperature_cells_from_source = (  # noqa: SLF001
        lambda *, building, data_file, emit_log: values.get(building, {})
    )

    result = orchestrator._resolve_shared_outdoor_temperature_cells(  # noqa: SLF001
        [
            {"building": "A楼", "file_path": "a.xlsx"},
            {"building": "B楼", "file_path": "b.xlsx"},
            {"building": "C楼", "file_path": "c.xlsx"},
        ],
        duty_date="2026-06-09",
        duty_shift="day",
        emit_log=lambda text: None,
    )

    assert result["B7"] == ""
    assert result["D7"] == ""
    assert result["_selected_building"] == ""
