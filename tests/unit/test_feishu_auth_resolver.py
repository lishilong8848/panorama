from __future__ import annotations

from pathlib import Path

from app.modules.feishu.service import feishu_auth_resolver as resolver_module


def test_resolve_feishu_auth_settings_prefers_common_auth() -> None:
    resolved = resolver_module.resolve_feishu_auth_settings(
        {
            "common": {
                "feishu_auth": {
                    "app_id": "app-id",
                    "app_secret": "app-secret",
                    "timeout": 45,
                    "request_retry_count": 5,
                    "request_retry_interval_sec": 4,
                }
            },
            "feishu": {"app_id": "", "app_secret": ""},
        }
    )

    assert resolved["app_id"] == "app-id"
    assert resolved["app_secret"] == "app-secret"
    assert resolved["timeout"] == 45
    assert resolved["request_retry_count"] == 5
    assert resolved["request_retry_interval_sec"] == 4.0


def test_resolve_feishu_auth_settings_falls_back_to_disk(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(resolver_module, "resolve_config_path", lambda _path=None: config_path)

    import app.config.settings_loader as settings_loader

    monkeypatch.setattr(
        settings_loader,
        "load_settings",
        lambda _path: {
            "common": {
                "feishu_auth": {
                    "app_id": "disk-app",
                    "app_secret": "disk-secret",
                    "timeout": 33,
                    "request_retry_count": 6,
                    "request_retry_interval_sec": 7,
                }
            }
        },
    )

    resolved = resolver_module.resolve_feishu_auth_settings({}, config_path=config_path)

    assert resolved["app_id"] == "disk-app"
    assert resolved["app_secret"] == "disk-secret"
    assert resolved["timeout"] == 33
    assert resolved["request_retry_count"] == 6
    assert resolved["request_retry_interval_sec"] == 7.0
