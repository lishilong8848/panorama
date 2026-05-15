from __future__ import annotations

from typing import Any, Dict


FORCED_FIXED_CELL_VALUES: Dict[str, str] = {}

DEFAULT_FIXED_CELL_VALUES: Dict[str, str] = {}


def normalize_cell_name(value: Any) -> str:
    return str(value or "").strip().upper()


def forced_fixed_cell_value(cell_name: Any) -> str | None:
    return FORCED_FIXED_CELL_VALUES.get(normalize_cell_name(cell_name))


def default_fixed_cell_value(cell_name: Any) -> str | None:
    return DEFAULT_FIXED_CELL_VALUES.get(normalize_cell_name(cell_name))


def apply_forced_fixed_cell_values(values: Dict[str, Any] | None) -> Dict[str, str]:
    output: Dict[str, str] = {}
    if isinstance(values, dict):
        for raw_cell, raw_value in values.items():
            cell = normalize_cell_name(raw_cell)
            if not cell:
                continue
            output[cell] = "" if raw_value is None else str(raw_value)
    output.update(FORCED_FIXED_CELL_VALUES)
    for cell, default_value in DEFAULT_FIXED_CELL_VALUES.items():
        if not str(output.get(cell, "") or "").strip():
            output[cell] = default_value
    return output
