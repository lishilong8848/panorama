from __future__ import annotations

from typing import Any, Dict

from app.config.settings_loader import validate_settings


AppSettings = Dict[str, Any]


def validate_app_settings(settings: AppSettings) -> AppSettings:
    return validate_settings(settings)
