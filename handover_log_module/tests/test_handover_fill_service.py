from __future__ import annotations

from datetime import datetime
from pathlib import Path

from handover_log_module.service import handover_fill_service as module


def _effective_config() -> dict:
    return {
        "template": {
            "sheet_name": "handover",
            "title_cell": "A1",
            "apply_building_title": False,
        },
        "cell_mapping": {"city_power": "D6"},
        "format_templates": {"city_power": "{value}"},
        "missing_policy": "blank",
        "download": {"time_format": "%Y-%m-%d %H:%M:%S"},
    }


def test_fill_can_skip_output_file_generation(monkeypatch) -> None:
    monkeypatch.setattr(module, "build_cell_value_map", lambda **kwargs: {"D6": "11"})  # noqa: ARG005
    monkeypatch.setattr(module, "build_resolved_value_context", lambda **kwargs: {"city_power": 11})  # noqa: ARG005
    monkeypatch.setattr(module, "missing_metrics_for_cells", lambda **kwargs: {})  # noqa: ARG005
    monkeypatch.setattr(module, "build_metric_text", lambda **kwargs: "11")  # noqa: ARG005

    copy_calls: list[dict] = []
    monkeypatch.setattr(module, "copy_template_and_fill", lambda **kwargs: copy_calls.append(kwargs) or Path("ignored.xlsx"))

    service = module.HandoverFillService({"template": {"sheet_name": "handover"}})
    cabinet_calls: list[dict] = []
    footer_calls: list[dict] = []
    monkeypatch.setattr(
        service,
        "_apply_cabinet_power_defaults",
        lambda **kwargs: cabinet_calls.append(kwargs),  # noqa: ARG005
    )
    monkeypatch.setattr(
        service,
        "_apply_footer_inventory_defaults",
        lambda **kwargs: footer_calls.append(kwargs),  # noqa: ARG005
    )

    result = service.fill(
        building="A",
        data_file="input.xlsx",
        hits={},
        effective_config=_effective_config(),
        date_ref_override=datetime(2026, 3, 26),
        write_output_file=False,
        emit_log=lambda *_args: None,
    )

    assert result["output_file"] == ""
    assert result["final_cell_values"] == {"D6": "11"}
    assert result["resolved_values_by_id"] == {"city_power": 11}
    assert copy_calls == []
    assert cabinet_calls == []
    assert footer_calls == []


def test_fill_still_writes_output_file_by_default(monkeypatch) -> None:
    monkeypatch.setattr(module, "build_cell_value_map", lambda **kwargs: {"D6": "11"})  # noqa: ARG005
    monkeypatch.setattr(module, "build_resolved_value_context", lambda **kwargs: {"city_power": 11})  # noqa: ARG005
    monkeypatch.setattr(module, "missing_metrics_for_cells", lambda **kwargs: {})  # noqa: ARG005
    monkeypatch.setattr(module, "build_metric_text", lambda **kwargs: "11")  # noqa: ARG005

    copy_calls: list[dict] = []
    monkeypatch.setattr(
        module,
        "copy_template_and_fill",
        lambda **kwargs: copy_calls.append(kwargs) or Path("D:/output/demo.xlsx"),
    )

    service = module.HandoverFillService({"template": {"sheet_name": "handover"}})
    cabinet_calls: list[dict] = []
    footer_calls: list[dict] = []
    monkeypatch.setattr(
        service,
        "_apply_cabinet_power_defaults",
        lambda **kwargs: cabinet_calls.append(kwargs),  # noqa: ARG005
    )
    monkeypatch.setattr(
        service,
        "_apply_footer_inventory_defaults",
        lambda **kwargs: footer_calls.append(kwargs),  # noqa: ARG005
    )

    result = service.fill(
        building="A",
        data_file="input.xlsx",
        hits={},
        effective_config=_effective_config(),
        date_ref_override=datetime(2026, 3, 26),
        emit_log=lambda *_args: None,
    )

    assert Path(result["output_file"]) == Path("D:/output/demo.xlsx")
    assert len(copy_calls) == 1
    assert len(cabinet_calls) == 1
    assert len(footer_calls) == 1
