from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import main


def test_browser_url_prefers_localhost_when_bound_to_all_interfaces(monkeypatch) -> None:
    monkeypatch.setattr(main, "_detect_lan_ipv4s", lambda: ["192.168.1.20"])

    local_url, lan_url, browser_url = main._resolve_browser_host("0.0.0.0", 18765)

    assert local_url == "http://127.0.0.1:18765/"
    assert lan_url == "http://192.168.1.20:18765/"
    assert browser_url == "http://127.0.0.1:18765/"
