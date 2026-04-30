from __future__ import annotations

from typing import Any, Iterable


_SPECIALTY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("电气", ("配电", "变电", "电气")),
    ("消防", ("消防",)),
    ("弱电", ("弱电",)),
    ("暖通", ("暖通",)),
)
_SHARED_ENGINEER_SPECIALTIES = {"消防"}


def normalize_specialty_text(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for canonical, keywords in _SPECIALTY_RULES:
        for keyword in keywords:
            if keyword and keyword in text:
                return canonical
    return text


def specialty_matches(left: str | None, right: str | None) -> bool:
    return normalize_specialty_text(left) == normalize_specialty_text(right)


def normalize_specialty_iter(values: Iterable[str | None]) -> list[str]:
    output: list[str] = []
    for value in values:
        normalized = normalize_specialty_text(value)
        if normalized:
            output.append(normalized)
    return output


def pick_engineer_supervisor(
    engineers: Iterable[dict[str, Any]],
    *,
    building: str | None,
    specialty_text: str | None,
) -> str:
    specialty = normalize_specialty_text(specialty_text)
    current_building = str(building or "").strip()
    if not specialty or not current_building:
        return ""

    shared_candidates: list[str] = []
    for row in engineers:
        if not isinstance(row, dict):
            continue
        if normalize_specialty_text(row.get("specialty", "")) != specialty:
            continue
        supervisor = str(row.get("supervisor", "") or "").strip()
        if not supervisor:
            continue
        row_building = str(row.get("building", "") or "").strip()
        if row_building == current_building:
            return supervisor
        if specialty in _SHARED_ENGINEER_SPECIALTIES:
            shared_candidates.append(supervisor)

    return shared_candidates[0] if shared_candidates else ""
