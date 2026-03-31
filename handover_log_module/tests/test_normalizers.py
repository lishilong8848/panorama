from __future__ import annotations

from handover_log_module.core.normalizers import format_number, normalize_b, normalize_c, to_float


def test_to_float_basic() -> None:
    assert to_float("12.5") == 12.5
    assert to_float(3) == 3.0
    assert to_float("abc") is None


def test_normalize_b_and_c() -> None:
    b = "南通阿里保税A区B楼/B楼三层/包间M1 E-301"
    c = "冷通道 C3-2 温湿度点位"
    c_th = "冷通道C3-TH_01"
    c_th_dash = "冷通道C3-TH-01"
    assert normalize_b(b, r"([A-Za-z]-\d{3})") == "E-301"
    assert normalize_c(c, r"([A-Za-z]\d-\d)") == "C3-2"
    assert normalize_c(c_th, r"([A-Za-z]\d-TH_\d{2})") == "C3-01"
    assert normalize_c(c_th_dash, r"([A-Za-z]\d-TH-\d{2})") == "C3-01"


def test_format_number() -> None:
    assert format_number(25.17) == "25.17"
    assert format_number(16.6) == "16.6"
    assert format_number(9.50) == "9.5"
    assert format_number(None) == ""
