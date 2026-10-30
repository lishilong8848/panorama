from __future__ import annotations

import copy
from typing import Any, Dict


FAMILY_ELECTRICAL_INSPECTION = "electrical_inspection_family"
FAMILY_HVAC_INSPECTION = "hvac_inspection_family"
INSPECTION_REPORT_TEMPLATES = {
    FAMILY_ELECTRICAL_INSPECTION: "电气巡检表",
    FAMILY_HVAC_INSPECTION: "暖通巡检表",
}
INSPECTION_FAMILY_LABELS = {
    FAMILY_ELECTRICAL_INSPECTION: "电气巡检源文件",
    FAMILY_HVAC_INSPECTION: "暖通巡检源文件",
}


def normalize_inspection_data_config(value: Any) -> Dict[str, Any]:
    config = copy.deepcopy(value) if isinstance(value, dict) else {}
    config["enabled"] = config.get("enabled") is True
    download = config.get("download", {})
    download = download if isinstance(download, dict) else {}
    # A legacy combined template must never select either independent report.
    download.pop("template_name", None)
    download["template_names"] = dict(INSPECTION_REPORT_TEMPLATES)
    config["download"] = download
    return config
