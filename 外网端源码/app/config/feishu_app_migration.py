from __future__ import annotations

from typing import Any

CURRENT_FEISHU_APP_ID = "cli_aa42dc770a381cb3"


FEISHU_APPLICATION_CONFIG_VERSION = 1


def migrate_feishu_application(config: Any) -> bool:
    if not isinstance(config, dict):
        return False
    common = config.get("common")
    common = common if isinstance(common, dict) else {}
    auth = common.get("feishu_auth")
    if not isinstance(auth, dict) or not auth.get("app_id"):
        legacy = next(
            (item for item in (common.get("feishu"), config.get("feishu"))
             if isinstance(item, dict) and item.get("app_id")),
            None,
        )
        auth = {
            key: legacy[key]
            for key in ("app_id", "app_secret", "timeout", "request_retry_count", "request_retry_interval_sec")
            if key in legacy
        } if isinstance(legacy, dict) else None
    if not isinstance(auth, dict) or not auth.get("app_id"):
        return False
    changed = auth.get("application_config_version") != FEISHU_APPLICATION_CONFIG_VERSION
    if changed and auth.get("app_id") != CURRENT_FEISHU_APP_ID:
        # Secrets belong to an application; the new secret must be supplied in configuration.
        auth.update(app_id=CURRENT_FEISHU_APP_ID, app_secret="")
    auth["application_config_version"] = FEISHU_APPLICATION_CONFIG_VERSION
    common["feishu_auth"] = auth
    config["common"] = common

    def discard_separate_credentials(value: Any) -> None:
        nonlocal changed
        if isinstance(value, dict):
            if value is not auth:
                changed = changed or "app_id" in value or "app_secret" in value
                value.pop("app_id", None)
                value.pop("app_secret", None)
            for child in value.values():
                discard_separate_credentials(child)
        elif isinstance(value, list):
            for child in value:
                discard_separate_credentials(child)

    discard_separate_credentials(config)
    return changed
