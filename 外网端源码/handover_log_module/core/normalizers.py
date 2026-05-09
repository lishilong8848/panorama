from __future__ import annotations

import math
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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


def _decimal_from_value(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        decimal_value = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None
    if not decimal_value.is_finite():
        return None
    return decimal_value


def _format_decimal(decimal_value: Decimal, max_decimals: int = 2, *, force_decimals: bool = False) -> str:
    places = max(0, int(max_decimals))
    quant = Decimal("1").scaleb(-places)
    rounded = decimal_value.quantize(quant, rounding=ROUND_HALF_UP)
    if rounded == 0:
        rounded = abs(rounded)
    if not force_decimals and rounded == rounded.to_integral_value():
        return str(int(rounded))
    return f"{rounded:.{places}f}"


def format_extracted_value(value: Any, max_decimals: int = 2) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).strip()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        decimal_value = _decimal_from_value(value)
        if decimal_value is None:
            return str(value).strip()
        return _format_decimal(
            decimal_value,
            max_decimals,
            force_decimals=decimal_value != decimal_value.to_integral_value(),
        )

    text = str(value).strip()
    if not text:
        return ""
    normalized = text.replace(",", "")
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", normalized):
        decimal_value = _decimal_from_value(normalized)
        if decimal_value is not None:
            return _format_decimal(decimal_value, max_decimals, force_decimals="." in normalized)
    return text


def format_number(value: Optional[float], max_decimals: int = 2) -> str:
    if value is None:
        return ""
    decimal_value = _decimal_from_value(value)
    if decimal_value is None:
        return ""
    return _format_decimal(
        decimal_value,
        max_decimals,
        force_decimals=decimal_value != decimal_value.to_integral_value(),
    )
