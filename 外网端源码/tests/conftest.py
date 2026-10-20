from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def isolate_feishu_config_source(monkeypatch):
    monkeypatch.setattr("app.modules.feishu.service.feishu_auth_resolver._config_getter", None)
