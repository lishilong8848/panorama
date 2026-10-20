from __future__ import annotations

import copy
import json

import pytest

from app.config import feishu_app_migration as migration
from app.config import settings_loader
from app.config.config_adapter import adapt_runtime_config, ensure_v3_config
from app.config.config_merge_guard import merge_user_config_payload
from app.modules.feishu.service.feishu_auth_resolver import resolve_feishu_auth_settings
from app.modules.feishu.service.feishu_auth_resolver import configure_feishu_auth_source, require_feishu_auth_settings
from app.modules.feishu.service.feishu_token_manager import FeishuTokenManager
from scripts.check_feishu_table_permissions import collect_targets


OLD_ID = "cli_previous_test"


def test_upgrade_does_not_reuse_a_different_app_secret():
    config = {"common": {"feishu_auth": {"app_id": OLD_ID, "app_secret": "old-test-secret", "timeout": 17}},
              "features": {"custom": {"enabled": False, "directory": "local-only"}},
              "jobs": [{"_global_feishu": {"app_id": OLD_ID, "app_secret": "old-test-secret"}}]}
    expected = copy.deepcopy(config)
    expected["common"]["feishu_auth"].update(
        app_id=migration.CURRENT_FEISHU_APP_ID, app_secret="", application_config_version=1,
    )
    expected["jobs"][0]["_global_feishu"] = {}
    assert migration.migrate_feishu_application(config)
    assert config == expected
    assert not migration.migrate_feishu_application(config)


@pytest.mark.parametrize("version", [None, 1])
def test_current_application_uses_only_the_secret_supplied_in_config(version):
    auth = {"app_id": migration.CURRENT_FEISHU_APP_ID, "app_secret": "config-only-secret"}
    if version is not None:
        auth["application_config_version"] = version
    config = {"common": {"feishu_auth": auth}}
    migration.migrate_feishu_application(config)
    assert require_feishu_auth_settings(config)["app_secret"] == "config-only-secret"
    assert not hasattr(migration, "_CURRENT_FEISHU_APP_SECRET")


def test_missing_config_secret_does_not_fallback_to_legacy_credentials():
    config = {"common": {"feishu_auth": {"app_id": migration.CURRENT_FEISHU_APP_ID, "app_secret": ""}},
              "feishu": {"app_id": OLD_ID, "app_secret": "old-test-secret"}}
    migration.migrate_feishu_application(config)
    with pytest.raises(ValueError, match="飞书配置缺失"):
        require_feishu_auth_settings(config)


def test_config_center_changes_are_not_reset_by_migration_or_adapter():
    config = {"version": 3, "common": {"feishu_auth": {
        "app_id": "cli_future", "app_secret": "future-secret", "application_config_version": 1,
    }}, "features": {}}
    assert not migration.migrate_feishu_application(config)
    assert ensure_v3_config(config)["common"]["feishu_auth"]["app_id"] == "cli_future"
    assert adapt_runtime_config(config)["feishu"]["app_id"] == "cli_future"
    assert require_feishu_auth_settings(config)["app_secret"] == "future-secret"


def test_legacy_settings_move_credentials_to_config_center():
    config = {"feishu": {"app_id": OLD_ID, "app_secret": "old-test-secret", "calc_table_id": "tblKeep"}}
    assert migration.migrate_feishu_application(config)
    assert config["common"]["feishu_auth"]["app_id"] == migration.CURRENT_FEISHU_APP_ID
    assert config["feishu"]["calc_table_id"] == "tblKeep"
    assert "app_id" not in config["feishu"]


@pytest.mark.parametrize("config", [
    {"feishu": {"app_id": OLD_ID, "app_secret": "old-test-secret"}},
    {"version": 3, "common": {"feishu": {"app_id": OLD_ID, "app_secret": "old-test-secret"}}, "features": {}},
    {"common": {"feishu_auth": {"app_id": OLD_ID, "app_secret": "old-test-secret"}}},
])
def test_legacy_shapes_require_a_configured_secret_for_the_new_application(tmp_path, config):
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    runtime = adapt_runtime_config(settings_loader.load_pipeline_config(path))
    assert runtime["feishu"]["app_id"] == migration.CURRENT_FEISHU_APP_ID
    assert runtime["feishu"]["app_secret"] == ""
    restarted = adapt_runtime_config(settings_loader.load_pipeline_config(path))
    assert restarted["feishu"] == runtime["feishu"]


def test_standalone_calculator_also_migrates_and_tracks_config_center(tmp_path):
    from app.modules.report_pipeline.service.calc_io_runtime import load_config

    path = tmp_path / "calculator.json"
    path.write_text(json.dumps({"feishu": {"app_id": OLD_ID, "app_secret": "old-test-secret"}}), encoding="utf-8")
    runtime = load_config(path, resolve_pipeline_config_path=lambda value: value)
    assert runtime["feishu"]["app_id"] == migration.CURRENT_FEISHU_APP_ID
    assert runtime["feishu"]["app_secret"] == ""
    cfg = settings_loader.load_pipeline_config(path)
    cfg["common"]["feishu_auth"].update(app_id="cli_later", app_secret="later")
    settings_loader.write_settings_atomically(cfg, path, backup=False)
    assert require_feishu_auth_settings(runtime)["app_id"] == "cli_later"


def test_auth_save_waits_for_older_writes_before_publishing(tmp_path):
    import threading
    from app.config.config_runtime_service import ConfigRuntimeService

    path = tmp_path / "settings.json"
    original = {"version": 3, "common": {"feishu_auth": {
        "app_id": "cli_before", "app_secret": "before", "application_config_version": 1,
    }}, "features": {}}
    path.write_text(json.dumps(original), encoding="utf-8")
    entered, release, completed = threading.Event(), threading.Event(), threading.Event()
    writes, errors = [], []

    def save(config, target, **kwargs):
        if config.get("older_write"):
            entered.set()
            assert release.wait(5)
        settings_loader.write_settings_atomically(config, target, backup=False)
        writes.append(config["common"]["feishu_auth"]["app_id"])
        return config

    service = ConfigRuntimeService(config_path=path, repository=None, save_settings_func=save, debounce_sec=0.05)
    service.configure(original)
    newer = copy.deepcopy(original)
    newer["common"]["feishu_auth"].update(app_id="cli_after", app_secret="after")

    def update():
        try:
            service.apply_config_snapshot(newer, source="auth", persist_json=True)
        except Exception as exc:
            errors.append(exc)
        finally:
            completed.set()

    thread = threading.Thread(target=update)
    try:
        service.apply_config_snapshot({**original, "older_write": True}, source="other", persist_json=True)
        assert entered.wait(2)
        thread.start()
        assert not completed.wait(0.1)
        assert service.get_config()["common"]["feishu_auth"]["app_id"] == "cli_before"
        assert settings_loader.load_pipeline_config(path)["common"]["feishu_auth"]["app_id"] == "cli_before"
        release.set()
        assert completed.wait(5)
        assert not errors
        assert service.flush(2)
        assert writes == ["cli_before", "cli_after"]
        assert service.get_config()["common"]["feishu_auth"]["app_id"] == "cli_after"
        assert settings_loader.load_pipeline_config(path)["common"]["feishu_auth"]["app_id"] == "cli_after"
    finally:
        release.set()
        if thread.ident is not None:
            thread.join(5)
        service.shutdown()


def test_auth_save_failure_does_not_publish_new_credentials(tmp_path):
    from app.config.config_runtime_service import ConfigRuntimeService
    original = {"version": 3, "common": {"feishu_auth": {"app_id": "cli_before", "app_secret": "before"}}, "features": {}}

    def fail(*args, **kwargs):
        raise OSError("disk unavailable")

    service = ConfigRuntimeService(config_path=tmp_path / "settings.json", repository=None, save_settings_func=fail)
    service.configure(original)
    try:
        updated = copy.deepcopy(original)
        updated["common"]["feishu_auth"]["app_secret"] = "after"
        with pytest.raises(OSError, match="disk unavailable"):
            service.apply_config_snapshot(updated, source="auth", persist_json=True)
        assert service.get_config() == original
        assert service.flush(1)
    finally:
        service.shutdown()


def test_existing_user_config_is_persisted_once_with_backup(tmp_path):
    path = tmp_path / "settings.json"
    original = {"common": {"feishu_auth": {"app_id": OLD_ID, "app_secret": "old-test-secret"}}, "other": ["keep"]}
    path.write_text(json.dumps(original), encoding="utf-8")
    loaded = settings_loader.load_pipeline_config(path)
    assert loaded["common"]["feishu_auth"] == {
        "app_id": migration.CURRENT_FEISHU_APP_ID, "app_secret": "", "application_config_version": 1,
    }
    assert loaded["other"] == ["keep"]
    assert json.loads(path.read_text(encoding="utf-8-sig")) == loaded
    backups = list(tmp_path.glob("settings.backup.*.json"))
    assert len(backups) == 1
    assert json.loads(backups[0].read_text()) == original
    before = path.stat().st_mtime_ns
    settings_loader.load_pipeline_config(path)
    assert path.stat().st_mtime_ns == before
    loaded["common"]["feishu_auth"].update(app_id="cli_future", app_secret="future-secret")
    settings_loader.write_settings_atomically(loaded, path, backup=False)
    restarted = settings_loader.load_pipeline_config(path)
    assert restarted["common"]["feishu_auth"]["app_id"] == "cli_future"
    assert restarted["common"]["feishu_auth"]["app_secret"] == "future-secret"


def test_adapter_merge_and_stale_runtime_payloads_cannot_restore_old_app():
    old = {"version": 3, "common": {"feishu_auth": {"app_id": OLD_ID, "app_secret": "old-test-secret"}}, "features": {}}
    updated = copy.deepcopy(old)
    migration.migrate_feishu_application(updated)
    adapted = ensure_v3_config(updated)
    assert adapted["common"]["feishu_auth"]["app_id"] == migration.CURRENT_FEISHU_APP_ID
    runtime = adapt_runtime_config(updated)
    assert runtime["feishu"]["app_secret"] == ""
    merged = merge_user_config_payload(adapted, updated).merged
    assert merged["common"]["feishu_auth"] == adapted["common"]["feishu_auth"]
    auth = resolve_feishu_auth_settings({"common": adapted["common"], "feishu": old["common"]["feishu_auth"]})
    assert (auth["app_id"], auth["app_secret"]) == (migration.CURRENT_FEISHU_APP_ID, "")


def test_token_manager_follows_global_config_changes_and_invalidation(monkeypatch):
    seen = []
    manager = FeishuTokenManager()
    config = {"common": {"feishu_auth": {"app_id": migration.CURRENT_FEISHU_APP_ID, "app_secret": "new-test-secret"}}}
    configure_feishu_auth_source(lambda: config)
    monkeypatch.setattr(manager, "_fetch_token", lambda **kwargs: seen.append(kwargs) or "fake-token")
    manager.get_token(app_id=OLD_ID, app_secret="old-test-secret", timeout=2)
    manager.get_token(app_id=migration.CURRENT_FEISHU_APP_ID, app_secret="new-test-secret", timeout=2)
    assert seen == [{"app_id": migration.CURRENT_FEISHU_APP_ID, "app_secret": "new-test-secret", "timeout": 2}]
    manager.invalidate(app_id=OLD_ID, app_secret="old-test-secret")
    manager.get_token(app_id=OLD_ID, app_secret="old-test-secret", timeout=2)
    assert len(seen) == 2
    config["common"]["feishu_auth"] = {"app_id": "cli_future", "app_secret": "future-secret"}
    manager.get_token(app_id=OLD_ID, app_secret="old-test-secret", timeout=2)
    assert seen[-1]["app_id"] == "cli_future"
    assert seen[-1]["app_secret"] == "future-secret"
    config["common"]["feishu_auth"] = {}
    with pytest.raises(ValueError, match="飞书配置缺失"):
        manager.get_token(app_id=OLD_ID, app_secret="old-test-secret", timeout=2)


def test_permission_inventory_inherits_source_tokens_and_deduplicates_tables():
    config = {"roster": {"source": {"app_token": "base-roster", "table_id": "tblRoster"},
                         "long_day": {"source": {"app_token": "", "table_id": "tblLongDay"}}},
              "upload": {"app_token": "base-power", "calc_table_id": "tblCalc", "attachment_table_id": "tblAttachment"},
              "screenshots": {"app_token": "base-power", "targets": [{"table_id": "tblScreenshot"}, {"table_id": "tblScreenshot"}]}}
    targets = collect_targets(config)
    pairs = {(row["configured_token"], row["table_id"]) for row in targets}
    assert ("base-roster", "tblLongDay") in pairs
    assert ("base-power", "tblAttachment") in pairs
    assert sum(row["table_id"] == "tblScreenshot" for row in targets) == 1


def test_explicit_app_change_back_to_default_is_not_lost_by_merge_guard():
    old = {"version": 3, "common": {"feishu_auth": {
        "app_id": "cli_future", "app_secret": "future-secret", "application_config_version": 1,
    }}, "features": {}}
    new = copy.deepcopy(old)
    new["common"]["feishu_auth"].update(app_id=migration.CURRENT_FEISHU_APP_ID, app_secret="new-test-secret")
    merged = merge_user_config_payload(new, old).merged
    assert merged["common"]["feishu_auth"]["app_id"] == migration.CURRENT_FEISHU_APP_ID
    assert merged["common"]["feishu_auth"]["app_secret"] == "new-test-secret"


@pytest.mark.parametrize("kind", ["bitable", "sheets", "im"])
def test_existing_clients_follow_app_and_secret_changes(monkeypatch, kind):
    from app.modules.feishu.service import feishu_token_manager as token_module
    from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
    from app.modules.feishu.service.sheets_client_runtime import FeishuSheetsClientRuntime
    from app.modules.feishu.service.im_file_message_client import FeishuImFileMessageClient

    config = {"common": {"feishu_auth": {"app_id": "cli_before", "app_secret": "before-secret"}}}
    configure_feishu_auth_source(lambda: config)
    seen = []
    monkeypatch.setattr(token_module.feishu_token_manager, "_fetch_token", lambda **kwargs: seen.append(kwargs) or kwargs["app_id"])
    monkeypatch.setattr(token_module.feishu_token_manager, "_entries", {})
    if kind == "bitable":
        client = FeishuBitableClient(
            "ignored", "ignored", "base", "calc", "attachment",
            date_text_to_timestamp_ms_fn=lambda **kwargs: 0,
            canonical_metric_name_fn=str, dimension_mapping={},
        )
    else:
        cls = FeishuSheetsClientRuntime if kind == "sheets" else FeishuImFileMessageClient
        client = cls(app_id="ignored", app_secret="ignored")
    assert client.refresh_token(force=False) == "cli_before"
    config["common"]["feishu_auth"] = {"app_id": "cli_after", "app_secret": "after-secret"}
    assert client.refresh_token(force=False) == "cli_after"
    assert (client.app_id, client.app_secret) == ("cli_after", "after-secret")
    assert [(row["app_id"], row["app_secret"]) for row in seen] == [
        ("cli_before", "before-secret"), ("cli_after", "after-secret"),
    ]


def test_target_preview_cache_follows_the_configured_application(monkeypatch):
    import time
    from app.modules.feishu.service import bitable_target_resolver as module
    config = {"common": {"feishu_auth": {"app_id": "cli_before", "app_secret": "before-secret"}}}
    configure_feishu_auth_source(lambda: config)
    resolver = module.BitableTargetResolver(app_id="ignored", app_secret="ignored")
    cache = {}
    for name in ("before", "after"):
        key = ("base", "tblTest", f"cli_{name}", f"{name}-secret:{module._TOKEN_PAIR_PREVIEW_CACHE_VERSION}")
        cache[key] = {"expires_at": time.time() + 60, "preview": {"application": name}}
    monkeypatch.setattr(module, "_TOKEN_PAIR_PREVIEW_CACHE", cache)
    assert resolver.resolve_token_pair_preview(configured_app_token="base", table_id="tblTest")["application"] == "before"
    config["common"]["feishu_auth"] = {"app_id": "cli_after", "app_secret": "after-secret"}
    assert resolver.resolve_token_pair_preview(configured_app_token="base", table_id="tblTest")["application"] == "after"


def test_worker_reads_config_center_credentials_instead_of_queued_snapshot(tmp_path, monkeypatch):
    path = tmp_path / "worker-settings.json"
    config = {"common": {"feishu_auth": {
        "app_id": "cli_live", "app_secret": "live-secret", "application_config_version": 1,
    }}}
    path.write_text(json.dumps(config), encoding="utf-8")
    configure_feishu_auth_source(lambda: settings_loader.load_pipeline_config(path))
    manager = FeishuTokenManager()
    seen = []
    monkeypatch.setattr(manager, "_fetch_token", lambda **kwargs: seen.append(kwargs) or kwargs["app_id"])
    assert manager.get_token(app_id=OLD_ID, app_secret="queued-secret", timeout=2) == "cli_live"
    config["common"]["feishu_auth"].update(app_id="cli_updated", app_secret="updated-secret")
    settings_loader.write_settings_atomically(config, path, backup=False)
    assert manager.get_token(app_id=OLD_ID, app_secret="queued-secret", timeout=2) == "cli_updated"
    assert [row["app_secret"] for row in seen] == ["live-secret", "updated-secret"]
