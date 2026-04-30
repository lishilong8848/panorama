from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, List

def validate_runtime_config(
    config: Dict[str, Any],
    *,
    extract_site_host: Callable[[Any], str],
    parse_hms_text: Callable[[Any, str], tuple[int, int, int]],
) -> None:
    def _require_keys(section: Dict[str, Any], keys: List[str], section_name: str) -> None:
        missing = [k for k in keys if k not in section]
        if missing:
            raise ValueError(f"配置错误: {section_name} 缺少字段 {missing}")

    input_cfg = config["input"]
    download_cfg = config["download"]
    network_cfg = config.get("network", {})
    notify_cfg = config["notify"]
    feishu_cfg = config["feishu"]
    paths_cfg = config.get("paths", {})

    _require_keys(input_cfg, ["excel_dir", "buildings"], "input")
    _require_keys(
        download_cfg,
        [
            "save_dir",
            "run_subdir_mode",
            "run_subdir_prefix",
            "time_range_mode",
            "custom_window_mode",
            "daily_custom_window",
            "start_time",
            "end_time",
            "max_retries",
            "retry_wait_sec",
            "site_start_delay_sec",
            "only_process_downloaded_this_run",
            "sites",
            "browser_headless",
            "browser_channel",
            "playwright_browsers_path",
            "resume",
            "performance",
        ],
        "download",
    )
    if not isinstance(network_cfg, dict):
        raise ValueError("配置错误: network 必须是对象")
    _require_keys(
        notify_cfg,
        [
            "enable_webhook",
            "feishu_webhook_url",
            "keyword",
            "timeout",
            "on_download_failure",
            "on_wifi_failure",
            "on_upload_failure",
        ],
        "notify",
    )
    _require_keys(feishu_cfg, ["enable_upload"], "feishu")

    if not isinstance(paths_cfg, dict):
        raise ValueError("配置错误: paths 必须是对象")
    if not str(paths_cfg.get("runtime_state_root", "")).strip():
        raise ValueError("配置错误: paths.runtime_state_root 不能为空")

    sites = download_cfg["sites"]
    if not isinstance(sites, list):
        raise ValueError("配置错误: download.sites 必须是数组")
    for site in sites:
        if not isinstance(site, dict):
            raise ValueError("配置错误: download.sites 每项必须是对象")
        _require_keys(site, ["building", "enabled", "username", "password"], "download.sites[]")
        building = str(site["building"]).strip()
        enabled = bool(site["enabled"])
        if not building:
            raise ValueError("配置错误: download.sites 存在空 building")
        if enabled:
            host = extract_site_host(site.get("host", "")) or extract_site_host(site.get("url", ""))
            if not host:
                raise ValueError(f"配置错误: {building} 已启用但 host/url 为空")
            site["host"] = host
            if not str(site["username"]).strip():
                raise ValueError(f"配置错误: {building} 已启用但 username 为空")
            if not str(site["password"]).strip():
                raise ValueError(f"配置错误: {building} 已启用但 password 为空")

    resume_cfg = download_cfg["resume"]
    if not isinstance(resume_cfg, dict):
        raise ValueError("配置错误: download.resume 必须是对象")
    if int(resume_cfg["retention_days"]) <= 0:
        raise ValueError("配置错误: download.resume.retention_days 必须大于0")
    if int(resume_cfg["auto_continue_poll_sec"]) <= 0:
        raise ValueError("配置错误: download.resume.auto_continue_poll_sec 必须大于0")
    if int(resume_cfg["gc_every_n_items"]) <= 0:
        raise ValueError("配置错误: download.resume.gc_every_n_items 必须大于0")
    if int(resume_cfg["upload_chunk_threshold"]) <= 0:
        raise ValueError("配置错误: download.resume.upload_chunk_threshold 必须大于0")
    if int(resume_cfg["upload_chunk_size"]) <= 0:
        raise ValueError("配置错误: download.resume.upload_chunk_size 必须大于0")
    if not str(resume_cfg.get("root_dir", "")).strip():
        raise ValueError("配置错误: download.resume.root_dir 不能为空")
    if not str(resume_cfg.get("index_file", "")).strip():
        raise ValueError("配置错误: download.resume.index_file 不能为空")

    if float(network_cfg.get("connect_poll_interval_sec", 1)) <= 0:
        raise ValueError("配置错误: network.connect_poll_interval_sec 必须大于0")
    if int(network_cfg.get("scan_attempts", 3)) <= 0:
        raise ValueError("配置错误: network.scan_attempts 必须大于0")
    if int(network_cfg.get("scan_wait_sec", 2)) <= 0:
        raise ValueError("配置错误: network.scan_wait_sec 必须大于0")
    if int(network_cfg.get("hard_recovery_after_scan_failures", 2)) <= 0:
        raise ValueError("配置错误: network.hard_recovery_after_scan_failures 必须大于0")
    if int(network_cfg.get("hard_recovery_cooldown_sec", 20)) < 0:
        raise ValueError("配置错误: network.hard_recovery_cooldown_sec 必须大于等于0")
    if float(network_cfg.get("post_switch_stabilize_sec", 0)) < 0:
        raise ValueError("配置错误: network.post_switch_stabilize_sec 必须大于等于0")
    if float(network_cfg.get("post_switch_probe_timeout_sec", 0)) <= 0:
        raise ValueError("配置错误: network.post_switch_probe_timeout_sec 必须大于0")
    if int(network_cfg.get("post_switch_probe_retries", 0)) <= 0:
        raise ValueError("配置错误: network.post_switch_probe_retries 必须大于0")
    if float(network_cfg.get("post_switch_probe_interval_sec", 0)) <= 0:
        raise ValueError("配置错误: network.post_switch_probe_interval_sec 必须大于0")
    if int(network_cfg.get("post_switch_probe_internal_port", 0)) <= 0:
        raise ValueError("配置错误: network.post_switch_probe_internal_port 必须大于0")
    if int(network_cfg.get("post_switch_probe_external_port", 0)) <= 0:
        raise ValueError("配置错误: network.post_switch_probe_external_port 必须大于0")
    hard_steps = network_cfg.get("hard_recovery_steps", [])
    if not isinstance(hard_steps, list):
        raise ValueError("配置错误: network.hard_recovery_steps 必须是数组")
    allowed_steps = {"toggle_adapter", "restart_wlansvc"}
    invalid = [str(x) for x in hard_steps if str(x) not in allowed_steps]
    if invalid:
        raise ValueError(f"配置错误: network.hard_recovery_steps 仅支持 {sorted(allowed_steps)}，当前 {invalid}")

    perf_cfg = download_cfg["performance"]
    if not isinstance(perf_cfg, dict):
        raise ValueError("配置错误: download.performance 必须是对象")
    if int(perf_cfg["query_result_timeout_ms"]) <= 0:
        raise ValueError("配置错误: download.performance.query_result_timeout_ms 必须大于0")
    if int(perf_cfg["login_fill_timeout_ms"]) <= 0:
        raise ValueError("配置错误: download.performance.login_fill_timeout_ms 必须大于0")
    if int(perf_cfg["start_end_visible_timeout_ms"]) <= 0:
        raise ValueError("配置错误: download.performance.start_end_visible_timeout_ms 必须大于0")
    if int(perf_cfg["page_refresh_retry_count"]) < 0:
        raise ValueError("配置错误: download.performance.page_refresh_retry_count 必须大于等于0")
    if int(perf_cfg["retry_failed_max_rounds"]) < 0:
        raise ValueError("配置错误: download.performance.retry_failed_max_rounds 必须大于等于0")

    mode = str(download_cfg["time_range_mode"]).strip()
    if mode not in {"yesterday_to_today_start", "last_month_to_this_month_start", "custom"}:
        raise ValueError(
            "配置错误: download.time_range_mode 仅支持 yesterday_to_today_start、last_month_to_this_month_start 或 custom"
        )
    if mode != "custom":
        return

    custom_mode = str(download_cfg.get("custom_window_mode", "absolute")).strip().lower()
    if custom_mode not in {"absolute", "daily_relative"}:
        raise ValueError("配置错误: download.custom_window_mode 仅支持 absolute 或 daily_relative")
    if custom_mode == "absolute":
        start_time = str(download_cfg.get("start_time", "")).strip()
        end_time = str(download_cfg.get("end_time", "")).strip()
        if not start_time or not end_time:
            raise ValueError(
                "当 download.time_range_mode=custom 且 custom_window_mode=absolute 时，必须配置 start_time 和 end_time"
            )
        time_format = "%Y-%m-%d %H:%M:%S"
        try:
            start_dt = datetime.strptime(start_time, time_format)
        except ValueError as exc:
            raise ValueError(f"配置错误: download.start_time 格式错误，必须为 {time_format}") from exc
        try:
            end_dt = datetime.strptime(end_time, time_format)
        except ValueError as exc:
            raise ValueError(f"配置错误: download.end_time 格式错误，必须为 {time_format}") from exc
        if start_dt >= end_dt:
            raise ValueError("配置错误: download.start_time 必须早于 download.end_time")
        now = datetime.now()
        if start_dt > now or end_dt > now:
            raise ValueError("配置错误: custom absolute 时间区间不能超过当前时间")
        return

    daily_cfg = download_cfg.get("daily_custom_window")
    if not isinstance(daily_cfg, dict):
        raise ValueError("配置错误: download.daily_custom_window 必须是对象")
    start_hms = str(daily_cfg.get("start_time", "")).strip()
    end_hms = str(daily_cfg.get("end_time", "")).strip()
    if not start_hms or not end_hms:
        raise ValueError(
            "当 download.time_range_mode=custom 且 custom_window_mode=daily_relative 时，必须配置 daily_custom_window.start_time 和 end_time"
        )
    start_h, start_m, start_s = parse_hms_text(start_hms, "download.daily_custom_window.start_time")
    end_h, end_m, end_s = parse_hms_text(end_hms, "download.daily_custom_window.end_time")
    start_seconds = start_h * 3600 + start_m * 60 + start_s
    end_seconds = end_h * 3600 + end_m * 60 + end_s
    cross_day = bool(daily_cfg.get("cross_day", False))
    if cross_day:
        if end_seconds > start_seconds:
            raise ValueError(
                "配置错误: 当 daily_custom_window.cross_day=true 时，end_time 应小于或等于 start_time（跨天区间）"
            )
        if end_seconds == start_seconds:
            raise ValueError("配置错误: daily_custom_window 跨天区间不能是 24 小时整")
    else:
        if end_seconds <= start_seconds:
            raise ValueError("配置错误: 当 daily_custom_window.cross_day=false 时，end_time 必须晚于 start_time")
