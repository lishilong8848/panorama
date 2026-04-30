from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from app.shared.utils.atomic_file import atomic_write_text
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class HandoverCapacityOilCacheService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config if isinstance(config, dict) else {}

    def _state_path(self) -> Path:
        runtime_root = resolve_runtime_state_root(runtime_config=self.config)
        capacity_cfg = self.config.get("capacity_report", {}) if isinstance(self.config.get("capacity_report", {}), dict) else {}
        oil_cfg = capacity_cfg.get("oil_cache", {}) if isinstance(capacity_cfg.get("oil_cache", {}), dict) else {}
        state_file = str(oil_cfg.get("state_file", "handover_capacity_oil_cache.json") or "").strip() or "handover_capacity_oil_cache.json"
        return runtime_root / state_file

    def _load(self) -> Dict[str, Any]:
        path = self._state_path()
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}
        return payload if isinstance(payload, dict) else {}

    def _save(self, payload: Dict[str, Any]) -> None:
        atomic_write_text(
            self._state_path(),
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _key(*, building: str, duty_date: str, duty_shift: str) -> str:
        return f"{str(building or '').strip()}|{str(duty_date or '').strip()}|{str(duty_shift or '').strip().lower()}"

    @staticmethod
    def _previous_shift(*, duty_date: str, duty_shift: str) -> tuple[str, str]:
        duty_date_text = str(duty_date or "").strip()
        shift_text = str(duty_shift or "").strip().lower()
        duty_day = datetime.strptime(duty_date_text, "%Y-%m-%d")
        if shift_text == "day":
            return (duty_day - timedelta(days=1)).strftime("%Y-%m-%d"), "night"
        return duty_day.strftime("%Y-%m-%d"), "day"

    def load_previous_values(self, *, building: str, duty_date: str, duty_shift: str) -> Dict[str, str]:
        previous_date, previous_shift = self._previous_shift(duty_date=duty_date, duty_shift=duty_shift)
        payload = self._load()
        row = payload.get(self._key(building=building, duty_date=previous_date, duty_shift=previous_shift), {})
        if not isinstance(row, dict):
            return {"first": "", "second": ""}
        return {
            "first": str(row.get("first", "") or "").strip(),
            "second": str(row.get("second", "") or "").strip(),
        }

    def save_current_values(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        first: str,
        second: str,
    ) -> None:
        payload = self._load()
        payload[self._key(building=building, duty_date=duty_date, duty_shift=duty_shift)] = {
            "building": str(building or "").strip(),
            "duty_date": str(duty_date or "").strip(),
            "duty_shift": str(duty_shift or "").strip().lower(),
            "first": str(first or "").strip(),
            "second": str(second or "").strip(),
            "updated_at": _now_text(),
        }
        self._save(payload)
