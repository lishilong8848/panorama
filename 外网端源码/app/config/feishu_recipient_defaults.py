from __future__ import annotations

from typing import Any

from app.config.feishu_app_migration import CURRENT_FEISHU_APP_ID


PUBLIC_ACCOUNT_APP_ID = CURRENT_FEISHU_APP_ID
# These public accounts were explicitly supplied by the user for this application.
PUBLIC_ACCOUNT_IDS = {
    "A": ("ou_3e0a9399a037276f8449cc778f18bb8a", "ou_822759df685e2807d538fb9cf5465465"),
    "B": ("ou_160e991b0fd811cda7260de056d8a363", "ou_ac4e1a554a6cabab4d8eea7e3956bec3"),
    "C": ("ou_99391b00c1406917930483d8500af400", "ou_f2879fa04399d43bff26f92616678b6d"),
    "D": ("ou_b9c26b4da3721c2de11ee175c707e756", "ou_18396043dc393fafd3b7f43f0e284396"),
    "E": ("ou_f0fe7137818e8b422b0594e51eab13f3", "ou_fb53efe356466339f4f3cc22f2c7f9fb"),
    "H": ("", "ou_3e7a45be43f809cd54e277243e9821ab"),
    "110": ("ou_913ff495ae8f5a0a55ae5670fd148bbd", "ou_ca8103bc15b06b448ab94958d5cfef43"),
}


def migrate_public_recipient_ids(config: dict[str, Any]) -> None:
    if config.get("common", {}).get("feishu_auth", {}).get("app_id") != PUBLIC_ACCOUNT_APP_ID:
        return
    replacements = {old: new for old, new in PUBLIC_ACCOUNT_IDS.values() if old}

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if key in {"open_id", "receive_id"} and isinstance(item, str) and item in replacements:
                    value[key] = replacements[item]
                elif isinstance(item, (dict, list)):
                    walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(config)
