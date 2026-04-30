from __future__ import annotations

from typing import Any, Dict

from handover_log_module.service.capacity_report_common import build_capacity_cells_with_config


def build_capacity_cells(context: Dict[str, Any]) -> Dict[str, str]:
    return build_capacity_cells_with_config(context)
