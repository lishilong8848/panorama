from __future__ import annotations

import copy
from pathlib import Path

from app.config.config_adapter import adapt_runtime_config
from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.settings_loader import load_settings, validate_settings, write_settings_atomically
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from scripts.dev_instance import build_instance_config, prepare_instance


def test_build_instance_config_overrides_role_port_and_paths(tmp_path: Path) -> None:
    base_cfg = copy.deepcopy(DEFAULT_CONFIG_V3)
    cfg = build_instance_config(
        base_cfg,
        role_mode="internal",
        port=18766,
        instance_root=tmp_path / "internal",
        shared_root=tmp_path / "shared",
    )

    common = cfg["common"]
    assert common["deployment"]["role_mode"] == "internal"
    assert common["console"]["port"] == 18766
    assert common["shared_bridge"]["enabled"] is True
    assert common["shared_bridge"]["root_dir"] == str(tmp_path / "shared")
    assert common["paths"]["business_root_dir"] == str(tmp_path / "internal" / "business")
    assert common["paths"]["runtime_state_root"] == str(tmp_path / "internal" / ".runtime")


def test_prepare_instance_writes_isolated_config(tmp_path: Path) -> None:
    base_config_path = tmp_path / "base.json"
    write_settings_atomically(validate_settings(copy.deepcopy(DEFAULT_CONFIG_V3)), path=base_config_path)

    target = prepare_instance(
        role_mode="external",
        port=18767,
        base_config_path=str(base_config_path),
        instance_root=tmp_path / "external",
        shared_root=tmp_path / "shared",
    )

    saved = load_settings(target)
    assert target == tmp_path / "external" / "表格计算配置.json"
    assert saved["common"]["deployment"]["role_mode"] == "external"
    assert saved["common"]["console"]["port"] == 18767
    assert saved["common"]["paths"]["runtime_state_root"] == str(tmp_path / "external" / ".runtime")


def test_runtime_state_root_flows_into_runtime_config_and_workspace(tmp_path: Path) -> None:
    cfg = copy.deepcopy(DEFAULT_CONFIG_V3)
    cfg["common"]["paths"]["runtime_state_root"] = str(tmp_path / "custom_runtime")

    runtime_cfg = adapt_runtime_config(cfg)

    assert runtime_cfg["paths"]["runtime_state_root"] == str(tmp_path / "custom_runtime")
    assert resolve_runtime_state_root(runtime_config=runtime_cfg, app_dir=tmp_path / "app") == tmp_path / "custom_runtime"
