from __future__ import annotations

import copy
import re
from datetime import datetime
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

INSPECTION_POINT_ID = re.compile(r"[0-9]+(?:_[0-9]+){1,15}")


class InspectionPointRevisionConflict(ValueError):
    pass


def normalize_inspection_point_overrides(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict) or set(value) - {"D楼", "E楼"}:
        raise ValueError("巡检测点配置只支持 D楼、E楼")
    result = {}
    for building, groups in value.items():
        if not isinstance(groups, dict) or set(groups) - set(INSPECTION_REPORT_TEMPLATES):
            raise ValueError("巡检测点配置只支持电气和暖通")
        for family, rules in groups.items():
            if not isinstance(rules, dict) or set(rules) - {"add", "remove", "baseline"}:
                raise ValueError("巡检测点配置必须为基线、新增和移除列表")
            normalized = {}
            actions = ("add", "remove", "baseline") if "baseline" in rules else ("add", "remove")
            for action in actions:
                ids = rules.get(action, [])
                if (not isinstance(ids, list) or len(ids) > (2000 if action == "baseline" else 1000)
                        or any(not isinstance(i, str) or len(i.strip()) > 100 or not INSPECTION_POINT_ID.fullmatch(i.strip()) for i in ids)):
                    raise ValueError(f"{building} {INSPECTION_REPORT_TEMPLATES[family]} 测点ID格式无效")
                normalized[action] = sorted(set(i.strip() for i in ids))
            if set(normalized["add"]) & set(normalized["remove"]):
                raise ValueError("同一测点不能同时新增和移除")
            if "baseline" in normalized and (not normalized["baseline"] or not 1 <= len(
                    (set(normalized["baseline"]) | set(normalized["add"])) - set(normalized["remove"])) <= 2000):
                raise ValueError("每个楼栋专业必须保留 1–2000 个测点")
            if any(normalized.values()):
                result.setdefault(building, {})[family] = normalized
    return result


def inspection_hour_bucket(when: datetime | None = None) -> str:
    current = when or datetime.now()
    return current.replace(hour=current.hour - current.hour % 2).strftime("%Y-%m-%d %H")


def normalize_inspection_data_config(value: Any) -> Dict[str, Any]:
    config = copy.deepcopy(value) if isinstance(value, dict) else {}
    config["enabled"] = config.get("enabled") is True
    if "point_overrides" in config:
        config["point_overrides"] = normalize_inspection_point_overrides(config["point_overrides"])
    download = config.get("download", {})
    download = download if isinstance(download, dict) else {}
    # A legacy combined template must never select either independent report.
    download.pop("template_name", None)
    download["template_names"] = dict(INSPECTION_REPORT_TEMPLATES)
    config["download"] = download
    return config
