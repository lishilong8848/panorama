from __future__ import annotations

import math
import re
from typing import Any, Optional


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def extract_by_regex(text: str, pattern: str) -> str:
    if not pattern:
        return ""
    matches = re.findall(pattern, text or "")
    if not matches:
        return ""
    value = matches[-1]
    if isinstance(value, tuple):
        return "".join(str(x) for x in value)
    return str(value)


def normalize_b(raw_text: str, regex_pattern: str) -> str:
    return extract_by_regex(raw_text or "", regex_pattern)


def normalize_c(raw_text: str, regex_pattern: str) -> str:
    text = raw_text or ""
    extracted = extract_by_regex(text, regex_pattern)
    normalized = _normalize_c_channel(extracted)
    if normalized:
        return normalized
    return _normalize_c_channel(text)


def _normalize_c_channel(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""

    direct = re.search(r"([A-Za-z]\d-\d{1,2})", raw)
    if direct:
        return direct.group(1).upper()

    # 兼容: 冷通道C3-TH_01 / C3-TH-01 / C3 TH 01 -> C3-01（TH后严格两位数字）
    th_style = re.search(r"([A-Za-z]\d)\s*[-_ ]?\s*TH\s*[-_ ]?\s*(\d{2})", raw, flags=re.IGNORECASE)
    if th_style:
        return f"{th_style.group(1).upper()}-{th_style.group(2).zfill(2)}"

    # 兜底: C3_1 / C3 1 / C3-1 -> C3-1
    generic = re.search(r"([A-Za-z]\d)\s*[-_ ](\d{1,2})", raw)
    if generic:
        return f"{generic.group(1).upper()}-{int(generic.group(2))}"
    return ""


def format_number(value: Optional[float], max_decimals: int = 2) -> str:
    if value is None:
        return ""
    rounded = round(float(value), int(max_decimals))
    text = f"{rounded:.{int(max_decimals)}f}".rstrip("0").rstrip(".")
    if text == "-0":
        return "0"
    return text
