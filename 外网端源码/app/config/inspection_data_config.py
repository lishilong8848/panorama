from __future__ import annotations

import copy
import re
from typing import Any


INSPECTION_SOURCE_CONTRACTS = {
    "electrical_inspection_family": {
        "discipline": "电气", "report_name": "电气巡检表", "label": "电气巡检源文件",
    },
    "hvac_inspection_family": {
        "discipline": "暖通", "report_name": "暖通巡检表", "label": "暖通巡检源文件",
    },
}


INSPECTION_DATA_UPLOAD_DEFAULTS = {
    "enabled": True,
    "source_families": {contract["discipline"]: family for family, contract in INSPECTION_SOURCE_CONTRACTS.items()},
    "buildings": ["D楼", "E楼"],
    "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
    "tables": {"D楼": "tblDG0Z3fyKCpZbg", "E楼": "tblbkTs8tfW869ry"},
    "wait_source_timeout_sec": 600,
    "wait_source_poll_sec": 5,
    "max_source_age_minutes": 120,
    "scheduler": {
        "enabled": False,
        "auto_start_in_gui": False,
        "run_times": [],
        "check_interval_sec": 30,
        "catch_up_if_missed": False,
        "retry_failed_in_same_period": True,
        "state_file": "inspection_data_upload_scheduler_state.json",
    },
}


def normalize_inspection_scheduler(value: Any) -> dict:
    if not isinstance(value, dict):
        raise ValueError("inspection_data_upload.scheduler 必须是对象")
    cfg = {**copy.deepcopy(INSPECTION_DATA_UPLOAD_DEFAULTS["scheduler"]), **value}
    times = cfg["run_times"]
    if not isinstance(times, list) or any(
        not isinstance(item, str)
        or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d", item)
        for item in times
    ):
        raise ValueError("run_times 必须是 HH:MM:SS 时间数组")
    cfg["run_times"] = sorted(set(times))
    for key in ("enabled", "auto_start_in_gui", "catch_up_if_missed", "retry_failed_in_same_period"):
        if not isinstance(cfg[key], bool):
            raise ValueError(f"{key} 必须是布尔值")
    if not cfg["enabled"] or not cfg["run_times"]:
        cfg["auto_start_in_gui"] = False
    interval = cfg["check_interval_sec"]
    if isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0:
        raise ValueError("check_interval_sec 必须是正整数")
    if not isinstance(cfg["state_file"], str) or not cfg["state_file"].strip():
        raise ValueError("state_file 不能为空")
    return cfg
