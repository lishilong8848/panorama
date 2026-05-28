#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standard HVAC running-data processor for Feishu Bitable.

This module only contains pure HVAC transformation helpers. Feishu API access
is provided by the project-level Feishu app client.
"""

from __future__ import annotations

import json
import hashlib
import re
import time
from collections.abc import Iterable
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_CONFIG: dict[str, Any] = {
    "identity": "bot",
    "base_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
    "source": {
        "table_id": "tblkvVCNRbtMmjQg",
        "all_view_id": "vewtnp2Ay9",
        "running_view_id": "vewSen1ncq",
    },
    "target": {
        "table_id": "tblxOyKdyyiMTdhR",
        "view_id": "vewyJLUSVm",
    },
    "weather": {
        "enabled": True,
        "latitude": 31.94,
        "longitude": 120.98,
        "timezone": "Asia/Shanghai",
        "summary_hours": 8,
        "past_days": 2,
        "forecast_days": 2,
        "temperature_trend_threshold_c": 0.5,
        "precipitation_threshold_mm": 0.1,
        "precipitation_probability_threshold": 50,
        "timeout_seconds": 15,
        "warnings": {
            "enabled": True,
            "provider": "nmc",
            "station_id": "VHAmf"
        },
    },
    "notifications": {
        "mode_switch_alerts": {
            "enabled": False,
            "chat_id": "oc_9961bb057de8bd715447559c5e63c4f2",
            "identity": "bot",
            "max_items": 10,
        },
    },
}


F_TEXT = "文本"
F_BUILDING = "楼栋"
F_CONTROLLER = "所属控制器"
F_POINT = "采集点"
F_DATA = "数据"
F_STATUS = "冷机状态"
F_CREATED = "创建时间"
F_REMARK = "备注"
F_PLATE_DIFF = "板换冷冻供回水温度差值"
F_CHILLER_TOWER_DIFF = "冷机冷冻回水与下塔温度差值"
F_UNIT = "制冷单元编号"

T_UNIT = "制冷单元-运行中"
T_MODE = "冷机运行模式"
T_UPDATED = "更新时间"
T_MODE_SWITCH_HINT = "模式切换提示"
T_WEATHER_SUMMARY = "8h内温度趋势"
T_WEATHER_WARNING = "气象预警"
T_LEGACY_TEMP_TREND = "2h内温度趋势"
T_WET_BULB = "湿球温度"
T_TOWER_FREQ = "冷塔频率"
T_TOWER = "冷塔出水温度"
T_PLATE_DIFF = "板换冷冻水供回水温差"
T_CHILLER_TOWER_DIFF = "冷机冷冻回水与下塔温度差值"

SUPPLY_FIELDS = [
    "二次泵末端冷冻水供水温度-1",
    "二次泵末端冷冻水供水温度-2",
    "二次泵末端冷冻水供水温度-3",
    "二次泵末端冷冻水供水温度-4",
    "二次泵末端冷冻水供水温度-最高",
]
RETURN_FIELDS = [
    "二次泵末端冷冻水回水温度-1",
    "二次泵末端冷冻水回水温度-2",
    "二次泵末端冷冻水回水温度-3",
    "二次泵末端冷冻水回水温度-4",
    "二次泵末端冷冻水回水温度-最高",
]

SOURCE_FIELDS = [
    F_TEXT,
    F_BUILDING,
    F_CONTROLLER,
    F_POINT,
    F_DATA,
    F_STATUS,
    F_CREATED,
    F_REMARK,
    F_PLATE_DIFF,
    F_CHILLER_TOWER_DIFF,
    F_UNIT,
]
TARGET_FIELDS = [
    F_TEXT,
    F_BUILDING,
    T_UNIT,
    T_MODE,
    T_MODE_SWITCH_HINT,
    T_WEATHER_SUMMARY,
    T_WEATHER_WARNING,
    T_WET_BULB,
    T_TOWER_FREQ,
    T_TOWER,
    T_PLATE_DIFF,
    T_CHILLER_TOWER_DIFF,
    *SUPPLY_FIELDS,
    *RETURN_FIELDS,
]
TARGET_READ_FIELDS = [*TARGET_FIELDS, T_UPDATED]

MODE_BY_NUMBER = {1: "制冷", 2: "预冷", 3: "板换", 4: "停机"}
RUNNING_MODES = {"制冷", "预冷", "板换"}
BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"]
UNITS = [f"{i}号制冷单元" for i in range(1, 7)]


def scalar(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else ""
    return "" if value is None else value


def fmt_num(value: Any) -> str | None:
    if value in ("", None):
        return None
    number = float(value)
    if abs(number) < 0.005:
        number = 0.0
    text = f"{number:.2f}".rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def max_fmt(values: list[Any]) -> str | None:
    nums = [float(v) for v in values if v not in ("", None)]
    return fmt_num(max(nums)) if nums else None


def as_float(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def max_float(values: Iterable[Any]) -> float | None:
    nums = [num for value in values if (num := as_float(value)) is not None]
    return max(nums) if nums else None


def min_float(values: Iterable[Any]) -> float | None:
    nums = [num for value in values if (num := as_float(value)) is not None]
    return min(nums) if nums else None


def parse_local_datetime(value: Any) -> datetime | None:
    value = scalar(value)
    if value in ("", None):
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000
        return datetime.fromtimestamp(timestamp)
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


WEATHER_CODE_TEXT = {
    0: "晴",
    1: "晴间多云",
    2: "多云",
    3: "阴",
    45: "雾",
    48: "雾",
    51: "小毛毛雨",
    53: "毛毛雨",
    55: "较强毛毛雨",
    56: "冻毛毛雨",
    57: "冻毛毛雨",
    61: "小雨",
    63: "中雨",
    65: "大雨",
    66: "冻雨",
    67: "冻雨",
    71: "小雪",
    73: "中雪",
    75: "大雪",
    77: "雪粒",
    80: "阵雨",
    81: "较强阵雨",
    82: "强阵雨",
    85: "阵雪",
    86: "强阵雪",
    95: "雷阵雨",
    96: "雷阵雨伴冰雹",
    99: "强雷阵雨伴冰雹",
}


class WeatherSummaryProvider:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.error: str | None = None
        self.warning_error: str | None = None
        self._forecast: list[dict[str, Any]] | None = None
        self._warning_summary: str | None = None
        self._warning_text: str | None = None
        self._run_time = datetime.now()
        self.nearest_window_count = 0

    def summary_for(self, updated_at: Any) -> str | None:
        if not self.config.get("enabled", True):
            return "天气查询未启用"
        updated_time = parse_local_datetime(updated_at) or self._run_time
        summary_hours = int(self.config.get("summary_hours", 8))
        window = self.forecast_window(updated_time, summary_hours)
        if not window:
            return None
        weather_text = self.describe_window(window, summary_hours)
        if not weather_text:
            return None
        return weather_text

    def forecast_window(self, start_time: datetime, hours: int) -> list[dict[str, Any]]:
        forecast = self.forecast()
        if not forecast:
            return []
        start_hour = start_time.replace(minute=0, second=0, microsecond=0)
        end_time = start_hour + timedelta(hours=hours)
        window = [item for item in forecast if start_hour <= item["time"] <= end_time]
        if window:
            return window

        first_time = forecast[0]["time"]
        last_time = forecast[-1]["time"]
        if start_hour < first_time:
            fallback_start = first_time
        elif start_hour > last_time:
            fallback_start = max(first_time, last_time - timedelta(hours=hours))
        else:
            fallback_start = min(forecast, key=lambda item: abs(item["time"] - start_hour))["time"]
        fallback_end = fallback_start + timedelta(hours=hours)
        self.nearest_window_count += 1
        return [item for item in forecast if fallback_start <= item["time"] <= fallback_end]

    def describe_window(self, window: list[dict[str, Any]], hours: int) -> str | None:
        ordered_window = sorted(
            [item for item in window if item.get("time") is not None],
            key=lambda item: item["time"],
        )
        temperatures = [float(item["temperature"]) for item in ordered_window if item.get("temperature") is not None]
        if not temperatures:
            return None
        first_temp = temperatures[0]
        last_temp = temperatures[-1]
        min_temp = min(temperatures)
        max_temp = max(temperatures)
        delta = last_temp - first_temp
        threshold = float(self.config.get("temperature_trend_threshold_c", 0.5))
        step_tolerance = float(self.config.get("temperature_trend_step_tolerance_c", 0.2))
        adjacent_deltas = [temperatures[index + 1] - temperatures[index] for index in range(len(temperatures) - 1)]
        mostly_non_decreasing = all(item >= -step_tolerance for item in adjacent_deltas)
        mostly_non_increasing = all(item <= step_tolerance for item in adjacent_deltas)
        if max_temp - min_temp < threshold:
            trend = "气温基本平稳"
        elif delta >= threshold and mostly_non_decreasing:
            trend = "气温逐步上升"
        elif delta <= -threshold and mostly_non_increasing:
            trend = "气温逐步下降"
        elif delta >= threshold:
            trend = "气温整体上升"
        elif delta <= -threshold:
            trend = "气温整体下降"
        else:
            trend = "气温小幅波动"

        weather = self.weather_text(ordered_window)
        rain = self.rain_text(ordered_window)
        return (
            f"未来{hours}h内{trend}，"
            f"最低{fmt_temp(min_temp)}℃，最高{fmt_temp(max_temp)}℃，"
            f"{weather}，{rain}"
        )

    def weather_text(self, window: list[dict[str, Any]]) -> str:
        codes: list[int] = []
        for item in window:
            try:
                if item.get("weather_code") not in ("", None):
                    codes.append(int(float(item.get("weather_code"))))
            except Exception:  # noqa: BLE001
                pass
        if not codes:
            return "天气情况未知"
        severe_order = [99, 96, 95, 82, 86, 75, 67, 65, 63, 81, 80, 61, 53, 51, 3, 2, 1, 0]
        code_set = set(codes)
        selected = next((code for code in severe_order if code in code_set), codes[0])
        return f"天气以{WEATHER_CODE_TEXT.get(int(selected), '未知天气')}为主"

    def rain_text(self, window: list[dict[str, Any]]) -> str:
        precipitation: list[float] = []
        probabilities: list[int] = []
        for item in window:
            try:
                precipitation.append(float(item.get("precipitation") or 0))
            except Exception:  # noqa: BLE001
                precipitation.append(0.0)
            try:
                probabilities.append(int(float(item.get("precipitation_probability") or 0)))
            except Exception:  # noqa: BLE001
                probabilities.append(0)
        total_precipitation = sum(precipitation)
        max_probability = max(probabilities) if probabilities else 0
        precipitation_threshold = float(self.config.get("precipitation_threshold_mm", 0.1))
        probability_threshold = int(self.config.get("precipitation_probability_threshold", 50))
        if total_precipitation >= precipitation_threshold:
            return f"有降雨，累计约{fmt_temp(total_precipitation)}mm"
        if max_probability >= probability_threshold:
            return f"有降雨可能，最高概率{max_probability}%"
        return "无降雨"

    def forecast(self) -> list[dict[str, Any]]:
        if self._forecast is not None:
            return self._forecast
        try:
            self._forecast = self.fetch_forecast()
        except Exception as exc:  # noqa: BLE001 - report and fall back to explicit "未知".
            self.error = str(exc)
            self._forecast = []
        return self._forecast

    def fetch_forecast(self) -> list[dict[str, Any]]:
        summary_hours = int(self.config.get("summary_hours", 8))
        past_days = int(self.config.get("past_days", 2))
        params_dict = {
            "latitude": self.config.get("latitude", 31.94),
            "longitude": self.config.get("longitude", 120.98),
            "hourly": "temperature_2m,precipitation,precipitation_probability,weather_code",
            "timezone": self.config.get("timezone", "Asia/Shanghai"),
        }
        if past_days > 0:
            params_dict["past_days"] = past_days
            params_dict["forecast_days"] = int(self.config.get("forecast_days", 2))
        else:
            params_dict["forecast_hours"] = int(self.config.get("forecast_hours", max(summary_hours + 6, 12)))
        params = urlencode(params_dict)
        url = f"https://api.open-meteo.com/v1/forecast?{params}"
        timeout = float(self.config.get("timeout_seconds", 15))
        with urlopen(url, timeout=timeout) as response:
            data = json.load(response)
        hourly = data.get("hourly") or {}
        times = hourly.get("time") or []
        temperatures = hourly.get("temperature_2m") or []
        precipitation = hourly.get("precipitation") or []
        probabilities = hourly.get("precipitation_probability") or []
        weather_codes = hourly.get("weather_code") or []
        forecast: list[dict[str, Any]] = []
        for index, (time_text, temperature) in enumerate(zip(times, temperatures)):
            if temperature in ("", None):
                continue
            forecast.append(
                {
                    "time": datetime.fromisoformat(str(time_text)),
                    "temperature": float(temperature),
                    "precipitation": precipitation[index] if index < len(precipitation) else None,
                    "precipitation_probability": probabilities[index] if index < len(probabilities) else None,
                    "weather_code": weather_codes[index] if index < len(weather_codes) else None,
                }
            )
        return forecast

    def warning_summary(self) -> str:
        if self._warning_summary is not None:
            return self._warning_summary
        warning_config = self.config.get("warnings") or {}
        if not warning_config.get("enabled", False):
            self._warning_summary = ""
            return self._warning_summary
        provider = str(warning_config.get("provider", "nmc")).lower()
        try:
            if provider != "nmc":
                raise ValueError(f"不支持的气象预警源: {provider}")
            alarms = self.fetch_nmc_alarms(str(warning_config.get("station_id", "") or "VHAmf"))
            if not alarms:
                self._warning_summary = ""
            else:
                titles = [str(alarm.get("title") or alarm.get("name") or alarm.get("type") or "气象预警") for alarm in alarms[:2]]
                suffix = "等" if len(alarms) > 2 else ""
                self._warning_summary = f"有{'、'.join(titles)}{suffix}"
        except Exception as exc:  # noqa: BLE001 - keep weather summary writable even if warning source fails.
            self.warning_error = str(exc)
            self._warning_summary = ""
        return self._warning_summary

    def warning_text(self) -> str | None:
        if self._warning_text is not None:
            return self._warning_text
        if not self.config.get("enabled", True):
            self._warning_text = "天气查询未启用"
            return self._warning_text

        warning_config = self.config.get("warnings") or {}
        if not warning_config.get("enabled", False):
            self._warning_text = "气象预警未启用"
            return self._warning_text

        official_warning = self.warning_summary()
        if official_warning:
            self._warning_text = official_warning
            return self._warning_text

        if self.warning_error:
            self._warning_text = "气象预警查询失败"
        else:
            self._warning_text = "无气象预警"
        return self._warning_text

    def fetch_nmc_alarms(self, station_id: str) -> list[dict[str, Any]]:
        if not station_id:
            raise ValueError("未配置中央气象台城市站点 ID")
        params = urlencode({"stationid": station_id})
        url = f"https://www.nmc.cn/rest/weather?{params}"
        timeout = float(self.config.get("timeout_seconds", 15))
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.nmc.cn/",
            },
        )
        with urlopen(request, timeout=timeout) as response:
            data = json.load(response)
        if data.get("code") not in (0, "0", None):
            raise RuntimeError(f"中央气象台接口返回异常: {data.get('msg') or data.get('code')}")
        warn = (((data.get("data") or {}).get("real") or {}).get("warn") or {})
        alert = str(warn.get("alert") or "").strip()
        if not alert or alert == "9999":
            return []
        return [
            {
                "title": alert,
                "type": str(warn.get("signaltype") or "").strip(),
                "level": str(warn.get("signallevel") or "").strip(),
                "description": str(warn.get("issuecontent") or "").strip(),
                "url": warn.get("url"),
            }
        ]


def fmt_temp(value: Any) -> str:
    text = fmt_num(value)
    return text if text is not None else "未知"


def merge_config(path: Path | None) -> dict[str, Any]:
    config = json.loads(json.dumps(DEFAULT_CONFIG, ensure_ascii=False))
    if not path:
        return config
    with path.open("r", encoding="utf-8-sig") as f:
        user_config = json.load(f)

    def deep_update(dst: dict[str, Any], src: dict[str, Any]) -> dict[str, Any]:
        for key, value in src.items():
            if isinstance(value, dict) and isinstance(dst.get(key), dict):
                deep_update(dst[key], value)
            else:
                dst[key] = value
        return dst

    return deep_update(config, user_config)


def normalize_records(block: dict[str, Any]) -> list[dict[str, Any]]:
    raw_rows = block.get("data", [])
    field_names = block.get("fields") or block.get("field_id_list") or []
    record_ids = block.get("record_id_list") or []
    rows = []
    for index, row in enumerate(raw_rows):
        if isinstance(row, dict):
            rows.append(row)
        else:
            rows.append(
                {
                    "record_id": record_ids[index] if index < len(record_ids) else None,
                    "fields": dict(zip(field_names, row)),
                }
            )
    return rows


def unit_name(num: int) -> str:
    return f"{num}号制冷单元"


def unit_num(unit: str) -> int | None:
    match = re.search(r"([1-6])号", unit)
    return int(match.group(1)) if match else None


def unit_group(unit: str) -> str:
    num = unit_num(unit)
    if num is None:
        return ""
    return "150" if num <= 3 else "124"


def unit_from_chinese_number(text: str) -> str | None:
    mapping = {
        "一": 1,
        "二": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
    }
    for ch, num in mapping.items():
        if f"{ch}号冷机单元控制器" in text:
            return unit_name(num)
    return None


def unit_from_controller(controller: str) -> str | None:
    if not controller:
        return None
    if controller == "负载率":
        return None
    if "群控管理器" in controller:
        return None

    chinese = unit_from_chinese_number(controller)
    if chinese:
        return chinese

    match = re.search(r"-(124|150)-(?:DDC|BAS|GW)-(\d{3})", controller)
    if not match:
        return None
    system, suffix_text = match.groups()
    suffix = int(suffix_text)
    if suffix == 100:
        return "全局"
    if suffix not in (101, 102, 103):
        return None
    if system == "124":
        return unit_name(suffix - 97)
    return unit_name(suffix - 100)


def unit_from_point(point: str) -> str | None:
    match = re.search(r"(?<!\d)([1-6])(?:号|#)", point)
    if match:
        return unit_name(int(match.group(1)))
    return None


def derive_unit(controller: str, point: str) -> str | None:
    controller_unit = unit_from_controller(controller)
    if controller_unit:
        return controller_unit
    return unit_from_point(point)


def is_mode_point(point: str) -> bool:
    return re.search(r"(?<!\d)[1-6]号冷机(?:运行)?模式", point) is not None


def mode_from_number(value: Any) -> str | None:
    try:
        return MODE_BY_NUMBER.get(int(float(value)))
    except (TypeError, ValueError):
        return None


def is_tower_point(point: str) -> bool:
    return any(key in point for key in ["冷塔出口温度", "冷塔出水温度", "冷却塔出口温度", "冷却塔出水温度"])


def is_wet_bulb_point(point: str) -> bool:
    return "湿球" in point


def is_tower_frequency_point(point: str) -> bool:
    tower_name = "冷塔" in point or "冷却塔" in point
    fan_name = "风机" in point or "风扇" in point
    frequency_name = "频率" in point or "变频" in point
    return tower_name and fan_name and frequency_name


def is_secondary_supply_point(point: str) -> bool:
    if "板" in point:
        return False
    return "冷冻水供水温度" in point and (
        "二次" in point or "末端" in point or re.search(r"^[CDE]-(124|150)", point) is not None
    )


def is_secondary_return_point(point: str) -> bool:
    if "板" in point:
        return False
    if re.search(r"冷冻水回水管0?[1-4]水道温度", point):
        return True
    return "冷冻水回水温度" in point and (
        "二次" in point or "末端" in point or re.search(r"^[CDE]-(124|150)", point) is not None
    )


def is_plate_inlet(point: str) -> bool:
    if "板" not in point:
        return False
    return any(key in point for key in ["进口", "进水", "回水"])


def is_plate_outlet(point: str) -> bool:
    if "板" not in point:
        return False
    return any(key in point for key in ["出口", "出水", "供水"])


def point_index(point: str) -> int | None:
    patterns = [
        r"_(\d)$",
        r"回水管0?([1-4])水道温度",
        r"温度\s*_?\s*([1-4])",
    ]
    for pattern in patterns:
        match = re.search(pattern, point)
        if match:
            num = int(match.group(1))
            if 1 <= num <= 4:
                return num
    return None


def group_from_controller_or_point(controller: str, point: str) -> str:
    for source in (controller, point):
        if "-150-" in source or "C-150" in source or "D-150" in source or "E-150" in source:
            return "150"
        if "-124-" in source or "C-124" in source or "D-124" in source or "E-124" in source:
            return "124"
    return ""


@dataclass
class SourceDerived:
    source_patches: dict[str, dict[str, Any]] = field(default_factory=dict)
    modes: dict[tuple[str, str], str] = field(default_factory=dict)
    unit_metrics: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    wet_bulb: dict[tuple[str, str], Any] = field(default_factory=dict)
    tower_frequency: dict[tuple[str, str], list[Any]] = field(default_factory=lambda: defaultdict(list))
    secondary_supply: dict[tuple[str, str], dict[int, Any]] = field(default_factory=lambda: defaultdict(dict))
    secondary_return: dict[tuple[str, str], dict[int, Any]] = field(default_factory=lambda: defaultdict(dict))


def add_patch(patches: dict[str, dict[str, Any]], record_id: str | None, field_name: str, value: Any) -> None:
    if not record_id:
        return
    patches.setdefault(record_id, {})[field_name] = value


def derive_source(records: list[dict[str, Any]]) -> SourceDerived:
    derived = SourceDerived()

    # 1. Unit number, remarks, and mode source rows.
    mode_values: dict[tuple[str, str], set[str]] = defaultdict(set)
    for record in records:
        rid = record.get("record_id")
        fields = record["fields"]
        building = scalar(fields.get(F_BUILDING))
        controller = str(scalar(fields.get(F_CONTROLLER)))
        point = str(scalar(fields.get(F_POINT)))
        data = fields.get(F_DATA)

        unit = derive_unit(controller, point)
        if unit and scalar(fields.get(F_UNIT)) != unit:
            add_patch(derived.source_patches, rid, F_UNIT, unit)
        unit_for_record = unit or str(scalar(fields.get(F_UNIT)))

        remark = ""
        if is_tower_point(point):
            remark = "冷塔出水温度"
        elif is_secondary_supply_point(point):
            remark = "二次泵末端冷冻供水温度"
        if remark and scalar(fields.get(F_REMARK)) != remark:
            add_patch(derived.source_patches, rid, F_REMARK, remark)

        if building and unit_for_record and is_mode_point(point):
            mode = mode_from_number(data)
            if mode:
                mode_values[(building, unit_for_record)].add(mode)

    conflicts = {key: values for key, values in mode_values.items() if len(values) > 1}
    if conflicts:
        pretty = {f"{k[0]}|{k[1]}": sorted(v) for k, v in conflicts.items()}
        raise RuntimeError(f"源表冷机模式存在冲突: {json.dumps(pretty, ensure_ascii=False)}")
    derived.modes = {key: next(iter(values)) for key, values in mode_values.items()}

    # 2. Propagate mode/status to source rows.
    for record in records:
        rid = record.get("record_id")
        fields = record["fields"]
        building = scalar(fields.get(F_BUILDING))
        unit = scalar(fields.get(F_UNIT)) or scalar(derived.source_patches.get(rid, {}).get(F_UNIT))
        if not building or not unit or unit == "全局":
            continue
        mode = derived.modes.get((building, unit))
        if mode and scalar(fields.get(F_STATUS)) != mode:
            add_patch(derived.source_patches, rid, F_STATUS, mode)

    # 3. Secondary water supply/return groups.
    for record in records:
        fields = record["fields"]
        building = scalar(fields.get(F_BUILDING))
        controller = str(scalar(fields.get(F_CONTROLLER)))
        point = str(scalar(fields.get(F_POINT)))
        data = fields.get(F_DATA)
        if not building or data is None:
            continue
        group = group_from_controller_or_point(controller, point)
        idx = point_index(point)
        if not group or not idx:
            continue
        if is_secondary_supply_point(point):
            derived.secondary_supply[(building, group)][idx] = data
        elif is_secondary_return_point(point):
            derived.secondary_return[(building, group)][idx] = data

    # 4. Wet-bulb temperature per building + 124/150 group.
    for record in records:
        fields = record["fields"]
        building = scalar(fields.get(F_BUILDING))
        controller = str(scalar(fields.get(F_CONTROLLER)))
        point = str(scalar(fields.get(F_POINT)))
        data = fields.get(F_DATA)
        if not building or data is None or not is_wet_bulb_point(point):
            continue
        group = group_from_controller_or_point(controller, point)
        if group:
            derived.wet_bulb[(building, group)] = data
        derived.wet_bulb.setdefault((building, ""), data)

    # 5. Plate/tower metrics and tower fan frequency per building + unit.
    unit_points: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "inlet": None,
            "outlet": None,
            "tower_values": [],
            "inlet_rows": [],
            "outlet_rows": [],
            "tower_rows": [],
        }
    )
    for record in records:
        rid = record.get("record_id")
        fields = record["fields"]
        building = scalar(fields.get(F_BUILDING))
        controller = str(scalar(fields.get(F_CONTROLLER)))
        point = str(scalar(fields.get(F_POINT)))
        data = fields.get(F_DATA)
        unit = scalar(fields.get(F_UNIT)) or scalar(derived.source_patches.get(rid, {}).get(F_UNIT))
        if not building or not unit or unit == "全局" or data is None:
            continue

        key = (building, unit)
        if is_plate_inlet(point):
            unit_points[key]["inlet"] = data
            unit_points[key]["inlet_rows"].append(record)
        elif is_plate_outlet(point):
            unit_points[key]["outlet"] = data
            unit_points[key]["outlet_rows"].append(record)
        elif is_tower_point(point):
            unit_points[key]["tower_values"].append(data)
            unit_points[key]["tower_rows"].append(record)
        if is_tower_frequency_point(point):
            derived.tower_frequency[key].append(data)

    for key, values in unit_points.items():
        inlet = values["inlet"]
        outlet = values["outlet"]
        tower = min_float(values["tower_values"])
        if inlet is None or outlet is None:
            continue
        plate_diff = fmt_num(float(inlet) - float(outlet))
        tower_diff = fmt_num(float(inlet) - float(tower)) if tower is not None else None
        derived.unit_metrics[key] = {
            "inlet": inlet,
            "outlet": outlet,
            "tower": tower,
            "plate_diff": plate_diff,
            "tower_diff": tower_diff,
        }
        for row in values["inlet_rows"] + values["outlet_rows"]:
            if scalar(row["fields"].get(F_PLATE_DIFF)) != plate_diff:
                add_patch(derived.source_patches, row.get("record_id"), F_PLATE_DIFF, plate_diff)
        if tower_diff is not None:
            for row in values["inlet_rows"] + values["tower_rows"]:
                if scalar(row["fields"].get(F_CHILLER_TOWER_DIFF)) != tower_diff:
                    add_patch(derived.source_patches, row.get("record_id"), F_CHILLER_TOWER_DIFF, tower_diff)

    return derived


def apply_source_patches(client: Any, table_id: str, patches: dict[str, dict[str, Any]]) -> int:
    grouped: dict[str, list[str]] = defaultdict(list)
    payload_by_key: dict[str, dict[str, Any]] = {}
    for record_id, patch in patches.items():
        key = json.dumps(patch, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        payload_by_key[key] = patch
        grouped[key].append(record_id)

    updated = 0
    for key, record_ids in grouped.items():
        updated += client.batch_update(table_id, record_ids, payload_by_key[key])
    return updated


def select_payload(name: str, options: list[str]) -> dict[str, Any]:
    hues = ["Blue", "Green", "Orange", "Purple", "Wathet", "Carmine", "Yellow", "Gray"]
    return {
        "type": "select",
        "name": name,
        "multiple": False,
        "options": [
            {"name": option, "hue": hues[index % len(hues)], "lightness": "Lighter"}
            for index, option in enumerate(options)
        ],
    }


def ensure_target_fields(client: Any, target_table_id: str) -> dict[str, Any]:
    fields = {item["name"]: item for item in client.field_list(target_table_id)}
    changes = {"created": [], "updated": []}

    def ensure_text(name: str) -> None:
        nonlocal fields
        if name in fields:
            return
        client.field_create(target_table_id, {"type": "text", "name": name, "style": {"type": "plain"}})
        changes["created"].append(name)
        fields = {item["name"]: item for item in client.field_list(target_table_id)}

    def ensure_plain_text_field(name: str) -> None:
        nonlocal fields
        payload = {"type": "text", "name": name, "style": {"type": "plain"}}
        field = fields.get(name)
        if field:
            if field.get("type") != "text":
                client.field_update(target_table_id, field["id"], payload)
                changes["updated"].append(name)
                fields = {item["name"]: item for item in client.field_list(target_table_id)}
            return
        client.field_create(target_table_id, payload)
        changes["created"].append(name)
        fields = {item["name"]: item for item in client.field_list(target_table_id)}

    def ensure_weather_summary_text() -> None:
        nonlocal fields
        payload = {"type": "text", "name": T_WEATHER_SUMMARY, "style": {"type": "plain"}}
        field = fields.get(T_WEATHER_SUMMARY)
        if field:
            if field.get("type") != "text":
                client.field_update(target_table_id, field["id"], payload)
                changes["updated"].append(T_WEATHER_SUMMARY)
                fields = {item["name"]: item for item in client.field_list(target_table_id)}
            return
        legacy_field = fields.get(T_LEGACY_TEMP_TREND)
        if legacy_field:
            client.field_update(target_table_id, legacy_field["id"], payload)
            changes["updated"].append(f"{T_LEGACY_TEMP_TREND} -> {T_WEATHER_SUMMARY}")
            fields = {item["name"]: item for item in client.field_list(target_table_id)}
            return
        ensure_plain_text_field(T_WEATHER_SUMMARY)

    # Rename the original single combined supply field if the table still uses it.
    old_supply = "二次泵末端冷冻水供水温度"
    if SUPPLY_FIELDS[0] not in fields and old_supply in fields:
        field_id = fields[old_supply]["id"]
        client.field_update(
            target_table_id,
            field_id,
            {"type": "text", "name": SUPPLY_FIELDS[0], "style": {"type": "plain"}},
        )
        changes["updated"].append(f"{old_supply} -> {SUPPLY_FIELDS[0]}")
        fields = {item["name"]: item for item in client.field_list(target_table_id)}

    ensure_weather_summary_text()
    ensure_plain_text_field(T_WEATHER_WARNING)

    for name in [
        T_MODE_SWITCH_HINT,
        T_WET_BULB,
        T_TOWER_FREQ,
        T_TOWER,
        T_PLATE_DIFF,
        T_CHILLER_TOWER_DIFF,
        *SUPPLY_FIELDS,
        *RETURN_FIELDS,
    ]:
        ensure_text(name)

    select_specs = {
        F_BUILDING: BUILDINGS,
        T_UNIT: UNITS,
        T_MODE: ["制冷", "预冷", "板换", "停机"],
    }
    for field_name, options in select_specs.items():
        field = fields.get(field_name)
        if not field:
            client.field_create(target_table_id, select_payload(field_name, options))
            changes["created"].append(field_name)
            fields = {item["name"]: item for item in client.field_list(target_table_id)}
            continue
        if field.get("type") == "select":
            existing = {option.get("name") for option in field.get("options") or []}
            if any(option not in existing for option in options):
                client.field_update(target_table_id, field["id"], select_payload(field_name, options))
                changes["updated"].append(field_name)
                fields = {item["name"]: item for item in client.field_list(target_table_id)}

    return changes


def steps_text(steps: list[str]) -> str:
    return "；".join(f"{index}. {step}" for index, step in enumerate(steps, 1))


BACKUP_SYNC_STEP = "模式切换时，备用机组需要切换至相同模式，防止故障切机后模式不一致"


SWITCH_STEPS = {
    ("制冷", "预冷"): [
        "检查制冷单元处于可用状态、无告警，IT包间温湿度在正常范围内",
        "将冷机冷冻出水温度设低至15至16℃，建立冷冻水供回水温差，防止进入再循环",
        "在系统设置界面，将模式切换为预冷模式（江森系统模式设定禁止打到强制）",
        "切换后冷机出水温度达到15至16℃且冷机电流稳定、冷冻水供回水温差大于2℃后，再按0.5℃幅度上调至17至18℃",
    ],
    ("板换", "预冷"): [
        "检查制冷单元处于可用状态、无告警，IT包间温湿度在正常范围内",
        "将冷机冷冻出水温度设低至15至16℃，防止冷机负载低进入再循环",
        "预冷模式下冷塔风扇参考点温度设置22.5℃，切机时快速提高冷机冷却水进水温度",
        "在系统设置界面，将模式切换为预冷模式（江森系统模式设定禁止打到强制）",
        "冷机开机运行后出水温度达到15至16℃且冷机电流稳定、冷冻水供回水温差大于2℃后，再按0.5℃幅度上调至17至18℃",
        "冷却泵频率根据冷机冷凝器供回水温差5℃进行调节",
    ],
    ("预冷", "制冷"): [
        "检查制冷单元处于可用状态、无告警，IT包间温湿度在正常范围内",
        "在系统设置界面，将模式切换为冷机制冷模式（江森系统模式设定禁止打到强制）",
        "切换过程中查看冷机电流百分比＞30%",
    ],
    ("预冷", "板换"): [
        "检查制冷单元处于可用状态、无告警，IT包间温湿度在正常范围内",
        "确认板换冷冻出水温度设定值21至21.5℃，冷塔低频率运行",
        "将冷机冷冻出水温度设低至15至16℃，防止切换过程中二次泵供水出现高温；调整过程中查看冷机电流百分比＞30%",
        "将冷塔风扇频率手动强制最高频率运行45至48Hz，使冷塔下塔出水温度＜18.5℃",
        "在系统设置界面，将模式切换为板换模式（江森系统模式设定禁止打到强制）",
        "待末端供水温度稳定＜18℃后，将冷塔风扇恢复自动，冷却泵频率根据冷冻水供水温度进行调节",
    ],
}


def needs_backup_sync(building: str) -> bool:
    return building in {"C楼", "D楼", "E楼"}


def switch_steps(building: str, from_mode: str, to_mode: str) -> list[str]:
    steps = list(SWITCH_STEPS[(from_mode, to_mode)])
    if needs_backup_sync(building):
        steps.append(BACKUP_SYNC_STEP)
    return steps


def mode_switch_hint(
    building: str,
    mode: str,
    wet_bulb: Any,
    metrics: dict[str, Any],
    secondary_supply_values: list[Any],
    tower_frequency_values: list[Any],
) -> str:
    wet = as_float(wet_bulb)
    inlet = as_float(metrics.get("inlet"))
    outlet = as_float(metrics.get("outlet"))
    tower = as_float(metrics.get("tower"))
    plate_diff = as_float(metrics.get("plate_diff"))
    tower_diff = as_float(metrics.get("tower_diff"))
    secondary_supply_max = max_float(secondary_supply_values)
    tower_frequency_max = max_float(tower_frequency_values)

    if mode == "制冷" and wet is not None and tower_diff is not None and wet < 19 and tower_diff > 1.5:
        return (
            f"当前湿球温度{fmt_temp(wet)}℃＜19℃，且（冷冻回水温度{fmt_temp(inlet)}℃-"
            f"冷却塔最低出水温度{fmt_temp(tower)}℃）={fmt_temp(tower_diff)}℃＞1.5℃，"
            f"具备冷机制冷模式切换为预冷模式条件。切换操作要求：{steps_text(switch_steps(building, mode, '预冷'))}"
        )

    if mode == "板换" and wet is not None and secondary_supply_max is not None and wet > 14 and secondary_supply_max > 18.5:
        return (
            f"当前湿球温度{fmt_temp(wet)}℃＞14℃，且二次泵末端冷冻供水温度最高"
            f"{fmt_temp(secondary_supply_max)}℃＞18.5℃，具备板换模式切换为预冷模式条件。"
            f"切换操作要求：{steps_text(switch_steps(building, mode, '预冷'))}"
        )

    if mode == "预冷" and wet is not None and plate_diff is not None and wet > 18 and plate_diff < 0.5:
        return (
            f"当前湿球温度{fmt_temp(wet)}℃＞18℃，且（板换冷冻进水温度{fmt_temp(inlet)}℃-"
            f"板换冷冻出水温度{fmt_temp(outlet)}℃）={fmt_temp(plate_diff)}℃＜0.5℃，"
            f"具备预冷模式切换为冷机制冷模式条件。切换操作要求：{steps_text(switch_steps(building, mode, '制冷'))}"
        )

    if (
        mode == "预冷"
        and wet is not None
        and outlet is not None
        and tower_frequency_max is not None
        and wet < 14
        and 21 <= outlet <= 21.5
        and tower_frequency_max <= 5
    ):
        return (
            f"当前湿球温度{fmt_temp(wet)}℃＜14℃，板换冷冻出水温度{fmt_temp(outlet)}℃在21至21.5℃范围内，"
            f"冷塔频率最高{fmt_temp(tower_frequency_max)}Hz为低频运行，具备预冷模式切换为板换模式可测条件；"
            f"板换冷冻出水温度设定值仍需现场确认。切换操作要求：{steps_text(switch_steps(building, mode, '板换'))}"
        )

    return ""


def mode_switch_target(hint: str) -> str:
    if "冷机制冷模式切换为预冷模式" in hint:
        return "制冷→预冷"
    if "板换模式切换为预冷模式" in hint:
        return "板换→预冷"
    if "预冷模式切换为冷机制冷模式" in hint:
        return "预冷→制冷"
    if "预冷模式切换为板换模式" in hint:
        return "预冷→板换"
    return "模式切换"


def target_modes(target: str) -> tuple[str, str] | None:
    if target == "制冷→预冷":
        return ("制冷", "预冷")
    if target == "板换→预冷":
        return ("板换", "预冷")
    if target == "预冷→制冷":
        return ("预冷", "制冷")
    if target == "预冷→板换":
        return ("预冷", "板换")
    return None


def operation_steps_for_alert(row: dict[str, Any], target: str) -> list[str]:
    modes = target_modes(target)
    if modes is None:
        return ["按《制冷模式手动切换标准操作流程》执行现场确认和切换。"]
    return switch_steps(str(row.get(F_BUILDING) or ""), modes[0], modes[1])


def condition_summary(row: dict[str, Any], target: str) -> str:
    if target == "制冷→预冷":
        return (
            f"湿球 {row.get(T_WET_BULB) or '未知'}℃ ＜ 19℃；"
            f"冷冻回水与冷塔最低出水温差 {row.get(T_CHILLER_TOWER_DIFF) or '未知'}℃ ＞ 1.5℃"
        )
    if target == "板换→预冷":
        return (
            f"湿球 {row.get(T_WET_BULB) or '未知'}℃ ＞ 14℃；"
            f"二次泵末端冷冻供水最高 {row.get(SUPPLY_FIELDS[4]) or '未知'}℃ ＞ 18.5℃"
        )
    if target == "预冷→制冷":
        return (
            f"湿球 {row.get(T_WET_BULB) or '未知'}℃ ＞ 18℃；"
            f"板换冷冻供回水温差 {row.get(T_PLATE_DIFF) or '未知'}℃ ＜ 0.5℃"
        )
    if target == "预冷→板换":
        return (
            f"湿球 {row.get(T_WET_BULB) or '未知'}℃ ＜ 14℃；"
            f"板换冷冻出水当前值已达可测条件，设定值需现场确认；冷塔频率 {row.get(T_TOWER_FREQ) or '未知'}Hz"
        )
    return row.get(T_MODE_SWITCH_HINT) or ""


def card_text(content: str) -> dict[str, str]:
    return {"tag": "lark_md", "content": content}


def card_div(content: str) -> dict[str, Any]:
    return {"tag": "div", "text": card_text(content)}


def card_field(title: str, value: str, color: str = "grey") -> dict[str, Any]:
    return {
        "is_short": True,
        "text": card_text(f"**{title}**\n<font color=\"{color}\">{value}</font>"),
    }


def numbered_steps(steps: list[str]) -> str:
    return "\n".join(f"{index}. {step}" for index, step in enumerate(steps, 1))


def build_mode_switch_alert_card(rows: list[dict[str, Any]], max_items: int = 10) -> dict[str, Any]:
    alert_rows = [row for row in rows if row.get(T_MODE_SWITCH_HINT)]
    elements: list[dict[str, Any]] = [
        card_div("🚨 **<font color=\"red\">模式切换条件已触发</font>**"),
        {
            "tag": "div",
            "fields": [
                card_field("命中条数", f"{len(alert_rows)} 条", "red"),
                card_field("生成时间", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "green"),
            ],
        },
        card_div("**处理建议**\n<font color=\"orange\">请结合现场状态、告警和IT包间温湿度复核后执行，禁止直接强制切换。</font>"),
        {"tag": "hr"},
    ]
    for index, row in enumerate(alert_rows[:max_items], 1):
        target = mode_switch_target(str(row.get(T_MODE_SWITCH_HINT) or ""))
        steps = operation_steps_for_alert(row, target)
        elements.extend(
            [
                card_div(f"🔴 **{index}. <font color=\"red\">{row[F_TEXT]}</font>｜{target}**"),
                {
                    "tag": "div",
                    "fields": [
                        card_field("当前模式", str(row.get(T_MODE) or "未知"), "blue"),
                        card_field("目标方向", target, "red"),
                    ],
                },
                card_div(f"**关键条件**\n<font color=\"red\">{condition_summary(row, target)}</font>"),
                card_div(f"**操作要点**\n<font color=\"grey\">{numbered_steps(steps)}</font>"),
                {"tag": "hr"},
            ]
        )
    remaining = len(alert_rows) - max_items
    if remaining > 0:
        elements.append(card_div(f"另有 <font color=\"orange\">{remaining}</font> 条命中记录未展开，请查看目标表 `{T_MODE_SWITCH_HINT}` 字段。"))
    return {
        "config": {"wide_screen_mode": True, "enable_forward": True},
        "header": {
            "template": "red",
            "title": {
                "tag": "plain_text",
                "content": f"暖通模式切换提醒｜命中 {len(alert_rows)} 条",
            },
        },
        "elements": elements,
    }


def send_mode_switch_alerts(client: Any, rows: list[dict[str, Any]], notify_config: dict[str, Any]) -> dict[str, Any]:
    alert_rows = [row for row in rows if row.get(T_MODE_SWITCH_HINT)]
    if not alert_rows:
        return {"sent": False, "reason": "no_alerts", "count": 0}
    chat_id = str(notify_config.get("chat_id") or "").strip()
    if not chat_id:
        return {"sent": False, "reason": "missing_chat_id", "count": len(alert_rows)}
    identity = str(notify_config.get("identity") or client.identity or "bot")
    max_items = int(notify_config.get("max_items", 10))
    content = build_mode_switch_alert_card(alert_rows, max_items=max_items)
    fingerprint = hashlib.sha1(
        json.dumps(
            [[row.get(F_TEXT), row.get(T_MODE), row.get(T_MODE_SWITCH_HINT)] for row in alert_rows],
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:12]
    idempotency_key = f"hvac-mode-switch-{datetime.now().strftime('%Y%m%d%H%M')}-{fingerprint}"
    result = client.send_interactive_message(chat_id, content, identity, idempotency_key)
    data = result.get("data") if isinstance(result.get("data"), dict) else result
    return {
        "sent": True,
        "count": len(alert_rows),
        "chat_id": chat_id,
        "identity": identity,
        "msg_type": "interactive",
        "message_id": data.get("message_id") if isinstance(data, dict) else None,
        "idempotency_key": idempotency_key,
    }


def target_row(building: str, unit: str, mode: str, derived: SourceDerived) -> dict[str, Any]:
    key = (building, unit)
    metrics = derived.unit_metrics.get(key, {})
    group = unit_group(unit)
    supply_values = [derived.secondary_supply.get((building, group), {}).get(i) for i in range(1, 5)]
    return_values = [derived.secondary_return.get((building, group), {}).get(i) for i in range(1, 5)]
    wet_bulb = derived.wet_bulb.get((building, group), derived.wet_bulb.get((building, "")))
    tower_frequency_values = derived.tower_frequency.get(key, [])

    row = {
        F_TEXT: f"{building}-{unit}",
        F_BUILDING: building,
        T_UNIT: unit,
        T_MODE: mode,
        T_MODE_SWITCH_HINT: mode_switch_hint(building, mode, wet_bulb, metrics, supply_values, tower_frequency_values),
        T_WEATHER_SUMMARY: "",
        T_WEATHER_WARNING: "",
        T_WET_BULB: fmt_num(wet_bulb),
        T_TOWER_FREQ: max_fmt(tower_frequency_values),
        T_TOWER: fmt_num(metrics.get("tower")),
        T_PLATE_DIFF: None if mode == "制冷" else metrics.get("plate_diff"),
        T_CHILLER_TOWER_DIFF: metrics.get("tower_diff"),
    }
    for index, field_name in enumerate(SUPPLY_FIELDS[:4]):
        row[field_name] = fmt_num(supply_values[index])
    row[SUPPLY_FIELDS[4]] = max_fmt(supply_values)
    for index, field_name in enumerate(RETURN_FIELDS[:4]):
        row[field_name] = fmt_num(return_values[index])
    row[RETURN_FIELDS[4]] = max_fmt(return_values)
    return row


def build_target_rows(derived: SourceDerived) -> list[dict[str, Any]]:
    rows = []
    for (building, unit), mode in sorted(derived.modes.items()):
        if unit == "全局" or mode not in RUNNING_MODES:
            continue
        rows.append(target_row(building, unit, mode, derived))
    return rows


def upsert_target_rows(
    client: Any,
    target_table_id: str,
    target_view_id: str,
    target_rows: list[dict[str, Any]],
    weather_provider: WeatherSummaryProvider,
) -> dict[str, Any]:
    existing = client.record_list(target_table_id, target_view_id, TARGET_READ_FIELDS)
    by_key = {}
    record_by_key = {}
    blank_ids = []
    for record in existing:
        fields = record["fields"]
        key = scalar(fields.get(F_TEXT))
        if key:
            by_key[key] = record.get("record_id")
            record_by_key[key] = record
        elif record.get("record_id"):
            blank_ids.append(record["record_id"])

    planned_keys = {row.get(F_TEXT) for row in target_rows if row.get(F_TEXT)}
    stale_ids = [
        record_id
        for key, record_id in by_key.items()
        if key not in planned_keys and re.fullmatch(r"[A-E]楼-\d号制冷单元", str(key or ""))
    ]
    stale_deleted = client.batch_delete(target_table_id, stale_ids) if stale_ids else 0

    updated = 0
    unchanged = 0
    reused_blank = 0
    to_create: list[dict[str, Any]] = []

    weather_summary = weather_provider.summary_for(None)
    weather_warning = weather_provider.warning_text()

    def apply_weather_fields(row: dict[str, Any]) -> None:
        if weather_summary is None:
            row.pop(T_WEATHER_SUMMARY, None)
        else:
            row[T_WEATHER_SUMMARY] = weather_summary
        if weather_warning is None:
            row.pop(T_WEATHER_WARNING, None)
        else:
            row[T_WEATHER_WARNING] = weather_warning

    for row in target_rows:
        key = row[F_TEXT]
        apply_weather_fields(row)
        if key in by_key:
            current_fields = record_by_key[key]["fields"]
            patch = {}
            for field_name, value in row.items():
                expected = "" if value is None else value
                if scalar(current_fields.get(field_name)) != expected:
                    patch[field_name] = value if value != "" else None
            if patch:
                updated += client.batch_update(target_table_id, [by_key[key]], patch)
            else:
                unchanged += 1
        elif blank_ids:
            patch = {field_name: (value if value != "" else None) for field_name, value in row.items()}
            reused_blank += client.batch_update(target_table_id, [blank_ids.pop(0)], patch)
        else:
            to_create.append(row)

    created = 0
    if to_create:
        create_rows = []
        for row in to_create:
            create_rows.append([row.get(field_name) if row.get(field_name) not in ("", None) else None for field_name in TARGET_FIELDS])
        created = client.batch_create(target_table_id, TARGET_FIELDS, create_rows)

    return {
        "existing_before": len(existing),
        "updated_existing": updated,
        "unchanged_existing": unchanged,
        "reused_blank": reused_blank,
        "created": created,
        "deleted_stale": stale_deleted,
    }


def verify_source(records: list[dict[str, Any]], derived: SourceDerived) -> list[dict[str, Any]]:
    by_id = {record["record_id"]: record["fields"] for record in records}
    mismatches = []
    for record_id, patch in derived.source_patches.items():
        fields = by_id.get(record_id, {})
        for field_name, expected in patch.items():
            actual = scalar(fields.get(field_name))
            expected_text = "" if expected is None else expected
            if actual != expected_text:
                mismatches.append(
                    {
                        "record_id": record_id,
                        "field": field_name,
                        "expected": expected_text,
                        "actual": actual,
                    }
                )
    return mismatches

def verify_target(records: list[dict[str, Any]], target_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key = {scalar(record["fields"].get(F_TEXT)): record["fields"] for record in records if scalar(record["fields"].get(F_TEXT))}
    expected_keys = {row[F_TEXT] for row in target_rows if row.get(F_TEXT)}
    mismatches = []
    for row in target_rows:
        key = row[F_TEXT]
        fields = by_key.get(key)
        if not fields:
            mismatches.append({"key": key, "field": F_TEXT, "expected": "record exists", "actual": "missing"})
            continue
        for field_name, expected in row.items():
            actual = scalar(fields.get(field_name))
            expected_text = "" if expected is None else expected
            if actual != expected_text:
                mismatches.append(
                    {
                        "key": key,
                        "field": field_name,
                        "expected": expected_text,
                        "actual": actual,
                    }
                )
    for key in sorted(by_key):
        if key not in expected_keys and re.fullmatch(r"[A-E]楼-\d号制冷单元", str(key or "")):
            mismatches.append({"key": key, "field": F_TEXT, "expected": "not present", "actual": "stale record exists"})
    return mismatches
