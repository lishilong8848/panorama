from __future__ import annotations

from typing import Any, Callable, Dict, List


def load_handover_config(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    from handover_log_module.api.facade import load_handover_config as _impl

    return _impl(config)


def run_from_download(
    config: Dict[str, Any],
    buildings: List[str] | None = None,
    end_time: str | None = None,
    duty_date: str | None = None,
    duty_shift: str | None = None,
    emit_log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    from handover_log_module.api.facade import run_from_download as _impl

    return _impl(
        config=config,
        buildings=buildings,
        end_time=end_time,
        duty_date=duty_date,
        duty_shift=duty_shift,
        emit_log=emit_log,
    )


def run_from_existing_file(
    config: Dict[str, Any],
    building: str,
    data_file: str,
    capacity_source_file: str | None = None,
    end_time: str | None = None,
    duty_date: str | None = None,
    duty_shift: str | None = None,
    emit_log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    from handover_log_module.api.facade import run_from_existing_file as _impl

    return _impl(
        config=config,
        building=building,
        data_file=data_file,
        capacity_source_file=capacity_source_file,
        end_time=end_time,
        duty_date=duty_date,
        duty_shift=duty_shift,
        emit_log=emit_log,
    )


def run_from_existing_files(
    config: Dict[str, Any],
    building_files: List[tuple[str, str]],
    capacity_building_files: List[tuple[str, str]] | None = None,
    configured_buildings: List[str] | None = None,
    end_time: str | None = None,
    duty_date: str | None = None,
    duty_shift: str | None = None,
    emit_log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    from handover_log_module.api.facade import run_from_existing_files as _impl

    return _impl(
        config=config,
        building_files=building_files,
        capacity_building_files=capacity_building_files,
        configured_buildings=configured_buildings,
        end_time=end_time,
        duty_date=duty_date,
        duty_shift=duty_shift,
        emit_log=emit_log,
    )


__all__ = [
    "load_handover_config",
    "run_from_download",
    "run_from_existing_file",
    "run_from_existing_files",
]
