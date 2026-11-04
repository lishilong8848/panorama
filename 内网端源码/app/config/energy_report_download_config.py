from datetime import datetime, timedelta


FAMILY_PUE_CONFIRMATION = "pue_confirmation_family"
FAMILY_UTILITY_FEEDER_DEVIATION = "utility_feeder_deviation_family"

ENERGY_REPORT_LABELS = {
    FAMILY_PUE_CONFIRMATION: "PUE确认表源文件",
    FAMILY_UTILITY_FEEDER_DEVIATION: "市电及馈线偏差（无市电进线）源文件",
}
PUE_CONFIRMATION_TEMPLATES = {
    "A楼": "A楼PUE确认报表（导这个）",
    "B楼": "B楼PUE确认表（导这个）",
    "C楼": "C楼PUE确认表",
    "D楼": "D楼PUE确认表",
    "E楼": "E楼PUE确认表",
}
FEEDER_DEVIATION_TEMPLATES = {
    building: "市电及馈线偏差（无市电进线)" if building == "E楼" else "市电及馈线偏差（无市电进线）"
    for building in PUE_CONFIRMATION_TEMPLATES
}
ENERGY_REPORT_TEMPLATES = frozenset(PUE_CONFIRMATION_TEMPLATES.values()) | frozenset(FEEDER_DEVIATION_TEMPLATES.values())


def energy_report_period(source_family: str, value: str = "", *, now: datetime | None = None) -> str:
    if source_family not in ENERGY_REPORT_LABELS:
        raise ValueError("不支持的能耗报表类型")
    fmt = "%Y-%m"
    if value:
        parsed = datetime.strptime(value, fmt)
        if parsed.strftime(fmt) != value:
            raise ValueError(f"无效报表日期: {value}")
        return value
    boundary = (now or datetime.now()).replace(day=1)
    return (boundary - timedelta(days=1)).strftime(fmt)
