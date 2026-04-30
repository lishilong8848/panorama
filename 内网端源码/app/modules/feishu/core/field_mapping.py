from __future__ import annotations

from typing import Any, Dict


def get_calc_table_id(config: Dict[str, Any]) -> str:
    return str(config.get("feishu", {}).get("calc_table_id", "")).strip()


def get_attachment_table_id(config: Dict[str, Any]) -> str:
    return str(config.get("feishu", {}).get("attachment_table_id", "")).strip()
