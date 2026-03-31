from __future__ import annotations

from typing import Iterable


_SPECIALTY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("电气", ("配电", "变电", "电气")),
    ("消防", ("消防",)),
    ("弱电", ("弱电",)),
    ("暖通", ("暖通",)),
)


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
