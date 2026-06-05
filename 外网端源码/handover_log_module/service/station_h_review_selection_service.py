from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from app.core.app_state import AppStateRepository
from pipeline_utils import get_app_dir


STATION_H_REVIEW_NAMESPACE = "station_h_review"

STATION_H_LONG_DAY_ROLE_BY_NAME = {
    "梅冰冰": "设施运维经理",
    "马进宇": "设施运维副经理",
    "曹李培": "综合管理",
    "王庆华": "暖通主管",
    "周海祥": "电气主管",
    "曹毅": "弱电主管",
    "明志勇": "安全&消防工程师",
    "高荣": "消防主管",
}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def station_h_build_batch_key(duty_date: str, duty_shift: str) -> str:
    return f"{str(duty_date or '').strip()}|{str(duty_shift or '').strip().lower()}"


def _parse_duty_date(duty_date: str) -> datetime | None:
    try:
        return datetime.strptime(str(duty_date or "").strip(), "%Y-%m-%d")
    except Exception:
        return None


def _normalize_duty_shift(duty_shift: str) -> str:
    text = str(duty_shift or "").strip().lower()
    return text if text in {"day", "night"} else ""


def split_station_h_people(value: Any) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        parts: list[str] = []
        for item in value:
            parts.extend(split_station_h_people(item))
    else:
        raw = str(value or "").strip()
        if "：" in raw:
            raw = raw.split("：", 1)[1]
        elif ":" in raw:
            raw = raw.split(":", 1)[1]
        parts = [item.strip() for item in re.split(r"[、,/，；;\s]+", raw) if item.strip()]
    seen: set[str] = set()
    output: list[str] = []
    for name in parts:
        key = re.sub(r"\s+", "", str(name or "").strip())
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(str(name or "").strip())
    return output


def join_station_h_people(names: Sequence[Any]) -> str:
    return "、".join(split_station_h_people(list(names)))


def station_h_long_day_text(names: Sequence[Any] | Any) -> str:
    selected = set(split_station_h_people(names))
    ordered = [name for name in STATION_H_LONG_DAY_ROLE_BY_NAME if name in selected]
    return f"常白岗：{' '.join(ordered) if ordered else '/'}"


def _runtime_config_from_handover_config(config: Dict[str, Any]) -> Dict[str, Any]:
    payload = config if isinstance(config, dict) else {}
    paths = payload.get("paths", {}) if isinstance(payload.get("paths", {}), dict) else {}
    runtime_state_root = str(paths.get("runtime_state_root", "") or "").strip()
    global_paths = payload.get("_global_paths", {}) if isinstance(payload.get("_global_paths", {}), dict) else {}
    if not runtime_state_root:
        runtime_state_root = str(global_paths.get("runtime_state_root", "") or "").strip()
    return {"paths": {"runtime_state_root": runtime_state_root}} if runtime_state_root else {}


class StationHReviewSelectionService:
    """Persist H-building manual review selections in the local app state DB."""

    def __init__(
        self,
        config: Dict[str, Any] | None = None,
        *,
        app_state_repository: AppStateRepository | None = None,
        app_dir: Path | None = None,
    ) -> None:
        self.config = config if isinstance(config, dict) else {}
        self._repository = app_state_repository or self._build_repository(app_dir=app_dir)

    def _build_repository(self, *, app_dir: Path | None = None) -> AppStateRepository | None:
        try:
            repository = AppStateRepository(
                runtime_config=_runtime_config_from_handover_config(self.config),
                app_dir=app_dir or get_app_dir(),
            )
            repository.ensure_ready()
            return repository
        except Exception:
            return None

    def _get(self, duty_date: str, duty_shift: str) -> Dict[str, Any] | None:
        repository = self._repository
        if repository is None:
            return None
        key = station_h_build_batch_key(duty_date, duty_shift)
        if "|" not in key or key.endswith("|"):
            return None
        try:
            payload = repository.get_runtime_kv(STATION_H_REVIEW_NAMESPACE, key)
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def get_selection(self, duty_date: str, duty_shift: str) -> Dict[str, Any] | None:
        payload = self._get(duty_date, duty_shift)
        return self._normalize_payload(payload) if isinstance(payload, dict) else None

    def save_selection(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        current_people: Any,
        next_people: Any,
        long_day_people: Any,
        source: str = "manual",
    ) -> Dict[str, Any]:
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = _normalize_duty_shift(duty_shift)
        if not _parse_duty_date(duty_date_text) or not duty_shift_text:
            raise ValueError("H楼审核页日期或班次无效")
        payload = {
            "duty_date": duty_date_text,
            "duty_shift": duty_shift_text,
            "batch_key": station_h_build_batch_key(duty_date_text, duty_shift_text),
            "current_people": split_station_h_people(current_people),
            "next_people": split_station_h_people(next_people),
            "long_day_people": self._normalize_long_day_people(long_day_people),
            "source": str(source or "manual").strip() or "manual",
            "updated_at": _now_text(),
        }
        payload["current_people_text"] = join_station_h_people(payload["current_people"])
        payload["next_people_text"] = join_station_h_people(payload["next_people"])
        payload["long_day_people_text"] = join_station_h_people(payload["long_day_people"])
        repository = self._repository
        if repository is None:
            raise RuntimeError("H楼审核页本地状态库不可用")
        repository.put_runtime_kv(STATION_H_REVIEW_NAMESPACE, payload["batch_key"], payload)
        return dict(payload)

    def resolve_selection(
        self,
        *,
        duty_date: str,
        duty_shift: str,
    ) -> Dict[str, Any]:
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = _normalize_duty_shift(duty_shift)
        exact = self.get_selection(duty_date_text, duty_shift_text)
        if exact is not None:
            exact["resolved_source"] = "current"
            return exact

        current_people: list[str] = []
        next_people: list[str] = []
        people_source = ""
        for candidate in self._same_team_candidates(duty_date_text, duty_shift_text):
            saved = self.get_selection(candidate["duty_date"], candidate["duty_shift"])
            if not saved:
                continue
            candidate_current = split_station_h_people(saved.get("current_people", []))
            candidate_next = split_station_h_people(saved.get("next_people", []))
            if candidate_current or candidate_next:
                current_people = candidate_current
                next_people = candidate_next
                people_source = f"rotation:{saved.get('batch_key', '')}"
                break

        long_day_people: list[str] = []
        long_day_source = ""
        for candidate in self._previous_shift_candidates(duty_date_text, duty_shift_text):
            saved = self.get_selection(candidate["duty_date"], candidate["duty_shift"])
            if not saved:
                continue
            long_day_people = self._normalize_long_day_people(saved.get("long_day_people", []))
            long_day_source = f"previous:{saved.get('batch_key', '')}"
            break

        return self._normalize_payload(
            {
                "duty_date": duty_date_text,
                "duty_shift": duty_shift_text,
                "batch_key": station_h_build_batch_key(duty_date_text, duty_shift_text),
                "current_people": current_people,
                "next_people": next_people,
                "long_day_people": long_day_people,
                "source": "fallback",
                "resolved_source": "fallback",
                "people_source": people_source,
                "long_day_source": long_day_source,
                "updated_at": "",
            }
        )

    def _normalize_payload(self, payload: Dict[str, Any] | None) -> Dict[str, Any]:
        raw = payload if isinstance(payload, dict) else {}
        duty_date = str(raw.get("duty_date", "") or "").strip()
        duty_shift = _normalize_duty_shift(str(raw.get("duty_shift", "") or "").strip())
        current_people = split_station_h_people(raw.get("current_people", raw.get("current_people_text", "")))
        next_people = split_station_h_people(raw.get("next_people", raw.get("next_people_text", "")))
        long_day_people = self._normalize_long_day_people(raw.get("long_day_people", raw.get("long_day_people_text", "")))
        return {
            **raw,
            "duty_date": duty_date,
            "duty_shift": duty_shift,
            "batch_key": str(raw.get("batch_key", "") or "").strip() or station_h_build_batch_key(duty_date, duty_shift),
            "current_people": current_people,
            "next_people": next_people,
            "long_day_people": long_day_people,
            "current_people_text": join_station_h_people(current_people),
            "next_people_text": join_station_h_people(next_people),
            "long_day_people_text": join_station_h_people(long_day_people),
            "updated_at": str(raw.get("updated_at", "") or "").strip(),
            "source": str(raw.get("source", "") or "").strip(),
            "resolved_source": str(raw.get("resolved_source", "") or "").strip(),
            "people_source": str(raw.get("people_source", "") or "").strip(),
            "long_day_source": str(raw.get("long_day_source", "") or "").strip(),
        }

    @staticmethod
    def _normalize_long_day_people(value: Any) -> List[str]:
        selected = set(split_station_h_people(value))
        return [name for name in STATION_H_LONG_DAY_ROLE_BY_NAME if name in selected]

    @staticmethod
    def _context(duty_day: datetime, duty_shift: str) -> Dict[str, str]:
        return {"duty_date": duty_day.strftime("%Y-%m-%d"), "duty_shift": duty_shift}

    def _same_team_candidates(self, duty_date: str, duty_shift: str, *, cycles: int = 4) -> Iterable[Dict[str, str]]:
        duty_day = _parse_duty_date(duty_date)
        shift = _normalize_duty_shift(duty_shift)
        if duty_day is None or not shift:
            return []
        candidates: list[dict[str, str]] = []
        for offset in range(0, max(1, int(cycles or 1))):
            base = offset * 4
            if shift == "day":
                candidates.append(self._context(duty_day - timedelta(days=3 + base), "night"))
                candidates.append(self._context(duty_day - timedelta(days=4 + base), "day"))
            else:
                candidates.append(self._context(duty_day - timedelta(days=1 + base), "day"))
                candidates.append(self._context(duty_day - timedelta(days=4 + base), "night"))
        return candidates

    def _previous_shift_candidates(self, duty_date: str, duty_shift: str, *, limit: int = 8) -> Iterable[Dict[str, str]]:
        duty_day = _parse_duty_date(duty_date)
        shift = _normalize_duty_shift(duty_shift)
        if duty_day is None or not shift:
            return []
        candidates: list[dict[str, str]] = []
        cursor_day = duty_day
        cursor_shift = shift
        for _idx in range(max(1, int(limit or 1))):
            if cursor_shift == "night":
                cursor_shift = "day"
            else:
                cursor_day = cursor_day - timedelta(days=1)
                cursor_shift = "night"
            candidates.append(self._context(cursor_day, cursor_shift))
        return candidates

