from __future__ import annotations

import re
from typing import Any, Dict


HANDOVER_TITLE_CELL = "A1"
HANDOVER_BUILDING_TITLE_PATTERN = "EA118机房{building_code}栋数据中心交接班日志"
HANDOVER_BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")


def extract_building_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = text.upper()
    for pattern in (
        r"机房\s*([A-E])\s*栋",
        r"([A-E])\s*(?=楼|栋)",
        r"\b([A-E])\b",
    ):
        matched = re.search(pattern, normalized)
        if matched:
            return matched.group(1).upper()
    return ""


def build_handover_building_title(building: Any) -> str:
    building_code = extract_building_code(building)
    if not building_code:
        return ""
    return HANDOVER_BUILDING_TITLE_PATTERN.format(building_code=building_code)


def canonical_handover_building_title_map() -> Dict[str, str]:
    return {
        building: build_handover_building_title(building)
        for building in HANDOVER_BUILDINGS
    }
