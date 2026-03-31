from __future__ import annotations

from typing import Any, Callable, Dict

from app.config.config_adapter import normalize_role_mode
from app.modules.report_pipeline.service.runtime_config_defaults import (
    ensure_performance_config,
    ensure_resume_config,
)


def normalize_runtime_config(config: Dict[str, Any], *, extract_site_host: Callable[[Any], str]) -> Dict[str, Any]:
    if isinstance(config, dict) and isinstance(config.get("common"), dict) and isinstance(config.get("features"), dict):
        from app.config.config_adapter import adapt_runtime_config

        config = adapt_runtime_config(config)

    if "input" not in config or not isinstance(config["input"], dict):
        raise ValueError("配置错误: 缺少 input 对象，请在 JSON 中配置。")
    if "download" not in config or not isinstance(config["download"], dict):
        raise ValueError("配置错误: 缺少 download 对象，请在 JSON 中配置。")
    if "network" not in config or not isinstance(config["network"], dict):
        raise ValueError("配置错误: 缺少 network 对象，请在 JSON 中配置。")
    if "notify" not in config or not isinstance(config["notify"], dict):
        raise ValueError("配置错误: 缺少 notify 对象，请在 JSON 中配置。")
    if "feishu" not in config or not isinstance(config["feishu"], dict):
        raise ValueError("配置错误: 缺少 feishu 对象，请在 JSON 中配置。")
    if "paths" not in config or not isinstance(config["paths"], dict):
        config["paths"] = {}

    input_cfg = config["input"]
    if not str(input_cfg.get("file_glob_template", "")).strip():
        input_cfg["file_glob_template"] = "{building}_*.xlsx"

    download_cfg = config["download"]
    ensure_resume_config(download_cfg)
    ensure_performance_config(download_cfg)

    custom_window_mode = str(download_cfg.get("custom_window_mode", "absolute")).strip().lower()
    if custom_window_mode not in {"absolute", "daily_relative"}:
        custom_window_mode = "absolute"
    download_cfg["custom_window_mode"] = custom_window_mode

    daily_cfg = download_cfg.get("daily_custom_window")
    if not isinstance(daily_cfg, dict):
        daily_cfg = {}
        download_cfg["daily_custom_window"] = daily_cfg
    if not str(daily_cfg.get("start_time", "")).strip():
        daily_cfg["start_time"] = "08:00:00"
    if not str(daily_cfg.get("end_time", "")).strip():
        daily_cfg["end_time"] = "17:00:00"
    if "cross_day" not in daily_cfg:
        daily_cfg["cross_day"] = False
    else:
        daily_cfg["cross_day"] = bool(daily_cfg["cross_day"])

    network_cfg = config["network"]
    network_cfg.setdefault("connect_poll_interval_sec", 1)
    deployment_cfg = config.get("deployment", {})
    if not isinstance(deployment_cfg, dict):
        deployment_cfg = {}
    role_mode = normalize_role_mode(deployment_cfg.get("role_mode"))
    network_cfg.pop("enable_auto_switch_wifi", None)
    network_cfg["enable_auto_switch_wifi"] = False
    network_cfg["role_mode"] = role_mode
    network_cfg.setdefault("fail_fast_on_netsh_error", True)
    network_cfg.setdefault("scan_before_connect", True)
    network_cfg.setdefault("scan_attempts", 3)
    network_cfg.setdefault("scan_wait_sec", 2)
    network_cfg.setdefault("strict_target_visible_before_connect", True)
    network_cfg.setdefault("connect_with_ssid_param", True)
    network_cfg.setdefault("preferred_interface", "")
    network_cfg.setdefault("auto_disconnect_before_connect", True)
    network_cfg.setdefault("hard_recovery_enabled", True)
    network_cfg.setdefault("hard_recovery_after_scan_failures", 2)
    network_cfg.setdefault("hard_recovery_steps", ["toggle_adapter", "restart_wlansvc"])
    network_cfg.setdefault("hard_recovery_cooldown_sec", 20)
    network_cfg.setdefault("require_admin_for_hard_recovery", True)
    network_cfg.setdefault("internal_profile_name", "")
    network_cfg.setdefault("external_profile_name", "")
    network_cfg.setdefault("post_switch_stabilize_sec", 3)
    network_cfg.setdefault("post_switch_probe_enabled", False)
    network_cfg.setdefault("post_switch_probe_internal_host", "")
    network_cfg.setdefault("post_switch_probe_internal_port", 80)
    network_cfg.setdefault("post_switch_probe_external_host", "open.feishu.cn")
    network_cfg.setdefault("post_switch_probe_external_port", 443)
    network_cfg.setdefault("post_switch_probe_timeout_sec", 2)
    network_cfg.setdefault("post_switch_probe_retries", 3)
    network_cfg.setdefault("post_switch_probe_interval_sec", 1)
    network_cfg.setdefault("internal_probe_timeout_ms", 1200)
    network_cfg.setdefault("internal_probe_count", 1)
    network_cfg.setdefault("internal_probe_parallelism", 5)
    network_cfg.setdefault("internal_probe_cache_ttl_sec", 2)
    network_cfg.setdefault("external_probe_cache_ttl_sec", 2)

    sites = download_cfg.get("sites")
    if isinstance(sites, list):
        for site in sites:
            if not isinstance(site, dict):
                continue
            host = extract_site_host(site.get("host", "")) or extract_site_host(site.get("url", ""))
            if host:
                site["host"] = host

    paths_cfg = config["paths"]
    paths_cfg.setdefault("runtime_state_root", ".runtime")
    return config
