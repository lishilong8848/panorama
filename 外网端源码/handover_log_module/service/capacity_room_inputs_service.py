from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from pipeline_utils import get_app_dir

from handover_log_module.repository.excel_reader import load_workbook_quietly


class CapacityRoomInputsService:
    DEFAULTS_KEY = "capacity_room_inputs"

    OTHER_BUILDING_ROWS = (69, 79, 89, 103, 117, 127)
    E_BUILDING_ROWS = (69, 89, 109, 129, 149, 169)
    ROOM_NAMES = ("M1", "M2", "M3", "M4", "M5", "M6")
    TEMPLATE_BY_FAMILY = {
        "other": "其他楼交接班容量报表空模板.xlsx",
        "e": "E楼交接班容量报表空模板.xlsx",
    }

    @classmethod
    def rows_for_building(cls, building: str) -> tuple[int, ...]:
        return cls.E_BUILDING_ROWS if str(building or "").strip() == "E楼" else cls.OTHER_BUILDING_ROWS

    @classmethod
    def template_family_for_building(cls, building: str) -> str:
        return "e" if str(building or "").strip() == "E楼" else "other"

    @classmethod
    def tracked_cells(cls) -> List[str]:
        cells: List[str] = []
        for row in sorted(set(cls.OTHER_BUILDING_ROWS + cls.E_BUILDING_ROWS)):
            cells.extend([f"Z{row}", f"AA{row}", f"AC{row}"])
        return cells

    @classmethod
    def row_specs_for_building(cls, building: str) -> List[Dict[str, Any]]:
        rows = cls.rows_for_building(building)
        specs: List[Dict[str, Any]] = []
        for index, row in enumerate(rows):
            room = cls.ROOM_NAMES[index] if index < len(cls.ROOM_NAMES) else f"M{index + 1}"
            specs.append(
                {
                    "room": room,
                    "label": f"{room}包间",
                    "row": row,
                    "total_cell": f"Z{row}",
                    "powered_cell": f"AA{row}",
                    "aircon_cell": f"AC{row}",
                }
            )
        return specs

    @staticmethod
    def _text(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @classmethod
    def _template_path_for_building(cls, building: str) -> Path:
        family = cls.template_family_for_building(building)
        return get_app_dir() / cls.TEMPLATE_BY_FAMILY[family]

    @classmethod
    def template_cells_for_building(cls, building: str) -> Dict[str, str]:
        template_path = cls._template_path_for_building(building)
        if not template_path.exists():
            return {}
        workbook = load_workbook_quietly(template_path, data_only=False)
        try:
            sheet = workbook["本班组"] if "本班组" in workbook.sheetnames else workbook.active
            cells: Dict[str, str] = {}
            for spec in cls.row_specs_for_building(building):
                for cell_key in ("total_cell", "powered_cell", "aircon_cell"):
                    cell_name = str(spec.get(cell_key, "") or "").strip().upper()
                    if not cell_name:
                        continue
                    value = sheet[cell_name].value
                    cells[cell_name] = "" if value is None else str(value).strip()
            return cells
        finally:
            workbook.close()

    @classmethod
    def cells_from_payload(cls, payload: Any, *, building: str) -> Dict[str, str]:
        source = payload if isinstance(payload, dict) else {}
        rows = source.get("rows", []) if isinstance(source, dict) else []
        by_room: Dict[str, Dict[str, Any]] = {}
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                room = cls._text(row.get("room")).upper()
                if room:
                    by_room[room] = row
        cells: Dict[str, str] = {}
        for spec in cls.row_specs_for_building(building):
            room = cls._text(spec.get("room")).upper()
            row = by_room.get(room, {})
            cells[str(spec["total_cell"]).upper()] = cls._text(row.get("total_cabinets"))
            cells[str(spec["powered_cell"]).upper()] = cls._text(row.get("powered_cabinets"))
            cells[str(spec["aircon_cell"]).upper()] = cls._text(row.get("aircon_started"))
        return cells

    @classmethod
    def payload_from_cells(cls, cells: Any, *, building: str) -> Dict[str, Any]:
        source = cells if isinstance(cells, dict) else {}
        rows: List[Dict[str, Any]] = []
        for spec in cls.row_specs_for_building(building):
            total_cell = str(spec["total_cell"]).upper()
            powered_cell = str(spec["powered_cell"]).upper()
            aircon_cell = str(spec["aircon_cell"]).upper()
            rows.append(
                {
                    "room": spec["room"],
                    "label": spec["label"],
                    "row": spec["row"],
                    "total_cell": total_cell,
                    "powered_cell": powered_cell,
                    "aircon_cell": aircon_cell,
                    "total_cabinets": cls._text(source.get(total_cell)),
                    "powered_cabinets": cls._text(source.get(powered_cell)),
                    "aircon_started": cls._text(source.get(aircon_cell)),
                }
            )
        return {
            "title": "M1-M6包间机柜与空调启动台数",
            "rows": rows,
        }

    @classmethod
    def normalize_payload(cls, payload: Any, *, building: str) -> Dict[str, Any]:
        if isinstance(payload, dict):
            return cls.payload_from_cells(cls.cells_from_payload(payload, building=building), building=building)
        return cls.payload_from_cells({}, building=building)

    @classmethod
    def extract_cells_from_document(cls, document: Dict[str, Any], *, building: str = "") -> Dict[str, str]:
        if not isinstance(document, dict):
            return {}
        payload = document.get("capacity_room_inputs", {})
        if not isinstance(payload, dict):
            return {}
        if building:
            return cls.cells_from_payload(payload, building=building)
        cells: Dict[str, str] = {}
        rows = payload.get("rows", [])
        if not isinstance(rows, list):
            return cells
        for row in rows:
            if not isinstance(row, dict):
                continue
            for source_key, value_key in (
                ("total_cell", "total_cabinets"),
                ("powered_cell", "powered_cabinets"),
                ("aircon_cell", "aircon_started"),
            ):
                cell_name = cls._text(row.get(source_key)).upper()
                if cell_name:
                    cells[cell_name] = cls._text(row.get(value_key))
        return cells

