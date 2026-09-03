from __future__ import annotations

import copy
import json
import math
import threading
import time
from datetime import datetime, timedelta
from typing import Any

from handover_log_module.core.normalizers import to_float
from handover_log_module.repository.review_session_state_store import ReviewSessionStateStore
from handover_log_module.service.capacity_report_common import calculate_relative_humidity_from_dry_wet
from handover_log_module.vendor import hvac_bitable_sync as hvac


_WEATHER_CACHE: dict[str, Any] = {}
# ponytail: one weather refresh per process; use per-location locks if serving multiple sites.
_WEATHER_LOCK = threading.Lock()


def _temperature(value: Any) -> float | None:
    text = str(value if value is not None else "").strip().removesuffix("℃").removesuffix("°C").strip()
    number = to_float(text)
    return number if number is not None and math.isfinite(number) else None


def _weather_forecast(config: dict[str, Any]) -> dict[str, Any]:
    global _WEATHER_CACHE
    key = json.dumps(config, sort_keys=True, ensure_ascii=False)
    cached = _WEATHER_CACHE
    if cached.get("key") == key and time.monotonic() < cached["expires_at"]:
        return cached
    if not _WEATHER_LOCK.acquire(blocking=False):
        return {"items": [], "updated_at": None, "error": "weather_refreshing"}
    try:
        cached = _WEATHER_CACHE
        if cached.get("key") == key and time.monotonic() < cached["expires_at"]:
            return cached
        try:
            items = hvac.WeatherSummaryProvider(config).fetch_forecast()
            error = None if items else "weather_unavailable"
        except Exception:  # External errors must not expose URLs, paths or credentials.
            items, error = [], "weather_unavailable"
        cached = {
            "key": key, "items": items, "error": error,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S") if items else None,
            "expires_at": time.monotonic() + (60 if error else 300),
        }
        _WEATHER_CACHE = cached
        return cached
    finally:
        _WEATHER_LOCK.release()


class HandoverEnvironmentService:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    def read(self, *, duty_date: str, duty_shift: str) -> dict[str, Any]:
        start = datetime.strptime(duty_date, "%Y-%m-%d").replace(hour=9 if duty_shift == "day" else 18)
        end = start + timedelta(hours=9 if duty_shift == "day" else 15)
        batch_key = f"{duty_date}|{duty_shift}"
        warnings: list[str] = []
        block: dict[str, Any] = {}
        try:
            store = ReviewSessionStateStore(global_paths=self.config.get("_global_paths", {}))
            if store.db_path.is_file():
                block = store.get_shared_block(batch_key=batch_key, block_id="outdoor_temperature")
        except Exception:
            warnings.append("temperature_store_unavailable")

        payload = block.get("payload") if isinstance(block.get("payload"), dict) else {}
        cells = payload.get("cells") if isinstance(payload.get("cells"), dict) else {}
        dry, wet = _temperature(cells.get("B7")), _temperature(cells.get("D7"))
        humidity = None
        if dry is None or wet is None:
            warnings.append("temperature_missing_or_invalid")
        elif wet > dry:
            warnings.append("wet_bulb_exceeds_dry_bulb")
        else:
            try:
                calculated = calculate_relative_humidity_from_dry_wet(dry, wet)
                if calculated is not None and math.isfinite(calculated):
                    humidity = round(calculated, 2)
            except (ValueError, OverflowError, ZeroDivisionError):
                pass
            if humidity is None:
                warnings.append("humidity_calculation_failed")

        upload_cfg = self.config.get("_global_chiller_mode_upload") or {}
        hvac_cfg = upload_cfg.get("hvac_bitable_sync") or {}
        weather_cfg = copy.deepcopy(hvac.DEFAULT_CONFIG["weather"])
        weather_cfg.update(hvac_cfg.get("weather") or {})
        # Bound provider work and keep the hourly timestamps in the same timezone as duty dates.
        weather_cfg.update(timeout_seconds=5, timezone="Asia/Shanghai", past_days=2, forecast_days=2)
        weather, weather_updated_at = None, None
        if not weather_cfg.get("enabled", True):
            warnings.append("weather_disabled")
        else:
            forecast = _weather_forecast(weather_cfg)
            weather_updated_at = forecast["updated_at"]
            if forecast["error"]:
                warnings.append(forecast["error"])
            else:
                window = [item for item in forecast["items"] if start <= item["time"] < end]
                expected = {start + timedelta(hours=i) for i in range(int((end - start).total_seconds() / 3600))}
                available = {item["time"] for item in window if item.get("weather_code") in hvac.WEATHER_CODE_TEXT}
                if expected <= available:
                    text = hvac.WeatherSummaryProvider(weather_cfg).weather_text(window)
                    weather = text.removeprefix("天气以").removesuffix("为主")
                else:
                    warnings.append("weather_window_unavailable")

        data = {
            "weather": weather,
            "dry_bulb_temperature": dry,
            "wet_bulb_temperature": wet,
            "relative_humidity": humidity,
        }
        available_count = sum(value is not None for value in data.values())
        return {
            "ok": True,
            "status": "success" if available_count == 4 else ("partial" if available_count else "unavailable"),
            "duty_date": duty_date, "duty_shift": duty_shift, "batch_key": batch_key,
            "timezone": "Asia/Shanghai",
            "data": data,
            "units": {"dry_bulb_temperature": "℃", "wet_bulb_temperature": "℃", "relative_humidity": "%"},
            "sources": {
                "temperature": {
                    "type": "handover_shared_block",
                    "updated_at": block.get("updated_at") or None,
                    "revision": int(block.get("revision") or 0),
                },
                "weather": {
                    "type": "open_meteo_hourly_shift_summary",
                    "fetched_at": weather_updated_at,
                    "window_start": start.strftime("%Y-%m-%d %H:%M:%S"),
                    "window_end": end.strftime("%Y-%m-%d %H:%M:%S"),
                },
                "humidity": {"type": "calculated_from_dry_wet", "pressure_hpa": 1013.25},
            },
            "warnings": warnings,
        }
