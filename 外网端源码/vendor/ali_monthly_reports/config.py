from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_ALI_ALARM_SOURCE_URL = (
    "https://vnet.feishu.cn/wiki/SOwsw315aiBJjgkl48ccoxIPntc?table=tblD7hi70s6U6rlU&view=vewG7OKFEg"
)
DEFAULT_ALI_STAFF_SOURCE_URL = (
    "https://vnet.feishu.cn/wiki/AdQmwOqwei6Xr4kag3pcp1Flnmg?table=tblOu75UtJTDNHIM&view=vewrQcypV4"
)
DEFAULT_HIGH_POWER_TABLE = {
    "name": "高功率TOP5报表",
    "table_id": "tblkh6YCMYtS8nHa",
    "view_id": "vewrHJHl3v",
}


@dataclass
class ReportConfig:
    download_folder: str = "downloads"
    feishu_app_id: str = ""
    feishu_app_secret: str = ""
    feishu_app_token: str = ""
    ali_alarm_source_url: str = DEFAULT_ALI_ALARM_SOURCE_URL
    ali_alarm_template_path: str = "resources/EA118机房2026年3月告警分析表.xlsx"
    ali_staff_source_url: str = DEFAULT_ALI_STAFF_SOURCE_URL
    high_power_table_id: str = DEFAULT_HIGH_POWER_TABLE["table_id"]
    high_power_view_id: str = DEFAULT_HIGH_POWER_TABLE["view_id"]

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "ReportConfig":
        return cls(
            download_folder=str(data.get("download_folder", "downloads")).strip() or "downloads",
            feishu_app_id=str(data.get("feishu_app_id", "")).strip(),
            feishu_app_secret=str(data.get("feishu_app_secret", "")).strip(),
            feishu_app_token=str(data.get("feishu_app_token", "")).strip(),
            ali_alarm_source_url=str(data.get("ali_alarm_source_url", DEFAULT_ALI_ALARM_SOURCE_URL)).strip(),
            ali_alarm_template_path=str(
                data.get("ali_alarm_template_path", "resources/EA118机房2026年3月告警分析表.xlsx")
            ).strip(),
            ali_staff_source_url=str(data.get("ali_staff_source_url", DEFAULT_ALI_STAFF_SOURCE_URL)).strip(),
            high_power_table_id=str(data.get("high_power_table_id", DEFAULT_HIGH_POWER_TABLE["table_id"])).strip(),
            high_power_view_id=str(data.get("high_power_view_id", DEFAULT_HIGH_POWER_TABLE["view_id"])).strip(),
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "ReportConfig":
        with Path(path).open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            raise ValueError("配置文件必须是 JSON object")
        return cls.from_mapping(data)

    @classmethod
    def from_env(cls) -> "ReportConfig":
        return cls.from_mapping(
            {
                "download_folder": os.getenv("DOWNLOAD_FOLDER", "downloads"),
                "feishu_app_id": os.getenv("FEISHU_APP_ID", ""),
                "feishu_app_secret": os.getenv("FEISHU_APP_SECRET", ""),
                "feishu_app_token": os.getenv("FEISHU_APP_TOKEN", ""),
                "ali_alarm_source_url": os.getenv("ALI_ALARM_SOURCE_URL", DEFAULT_ALI_ALARM_SOURCE_URL),
                "ali_alarm_template_path": os.getenv(
                    "ALI_ALARM_TEMPLATE_PATH",
                    "resources/EA118机房2026年3月告警分析表.xlsx",
                ),
                "ali_staff_source_url": os.getenv("ALI_STAFF_SOURCE_URL", DEFAULT_ALI_STAFF_SOURCE_URL),
                "high_power_table_id": os.getenv("HIGH_POWER_TABLE_ID", DEFAULT_HIGH_POWER_TABLE["table_id"]),
                "high_power_view_id": os.getenv("HIGH_POWER_VIEW_ID", DEFAULT_HIGH_POWER_TABLE["view_id"]),
            }
        )

    @property
    def high_power_table_config(self) -> dict[str, str]:
        return {
            "name": DEFAULT_HIGH_POWER_TABLE["name"],
            "table_id": self.high_power_table_id,
            "view_id": self.high_power_view_id,
        }
