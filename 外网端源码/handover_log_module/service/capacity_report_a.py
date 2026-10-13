from __future__ import annotations

import re
from typing import Any, Dict

from handover_log_module.core.normalizers import format_number
from handover_log_module.service.capacity_report_common import (
    CapacitySourceQuery,
    _emit_capacity_report_log,
    build_capacity_cells_with_config,
)
from handover_log_module.service.capacity_room_inputs_service import CapacityRoomInputsService


def build_capacity_cells(context: Dict[str, Any]) -> Dict[str, str]:
    values = build_capacity_cells_with_config(context)
    snapshot = context.get("template_snapshot")
    entries = snapshot.get("a_line_head_entries", []) if isinstance(snapshot, dict) else []
    if not entries:
        return values
    query = CapacitySourceQuery(context.get("capacity_rows", []))
    missing = []
    for entry in entries:
        identifier = entry["identifier"]
        row = query.first_row_by_d_aliases(
            ["进线_功率"], c_regexes=[rf"^{re.escape(identifier)}$"],
        )
        values[f"E{entry['row']}"] = format_number(row.value) if row is not None else ""
        if not values[f"E{entry['row']}"]:
            missing.append(identifier)
    for room, target_row in CapacityRoomInputsService.A_POTENTIAL_ROOM_ROWS:
        room_pattern = re.compile(rf"(?<![A-Za-z0-9])A-{room}(?!\d)", re.IGNORECASE)
        room_query = CapacitySourceQuery(row for row in query.rows if room_pattern.search(row.b_text))
        row = room_query.first_row_by_d_aliases(
            ["总有功功率", "总_有功功率"], c_contains=["CRAHB-A_电量仪"],
        )
        values[f"N{target_row}"] = format_number(row.value) if row is not None else ""
        if not values[f"N{target_row}"]:
            missing.append(f"挖潜机房{room}/CRAHB-A_电量仪")
    if missing:
        _emit_capacity_report_log(context, f"[交接班][容量报表][挖潜机房] 源数据缺失或非数值，留空: {','.join(missing)}")
    return values
