from __future__ import annotations


def non_empty(value: str, default: str = "") -> str:
    v = str(value).strip()
    return v if v else default
