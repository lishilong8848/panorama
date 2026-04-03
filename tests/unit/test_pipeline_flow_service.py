from __future__ import annotations

from app.modules.report_pipeline.service.pipeline_flow_service import run_pipeline_with_time_windows


class _Wifi:
    def get_current_ssid(self):
        return "EL-BG"


async def _empty_async_result(**_kwargs):
    return []


def test_pipeline_flow_returns_when_internal_switch_fail():
    cfg = {
        "download": {
            "resume": {"gc_every_n_items": 5, "upload_chunk_threshold": 20, "upload_chunk_size": 5},
            "browser_channel": "chrome",
            "only_process_downloaded_this_run": True,
            "sites": [],
        },
        "network": {
            "require_saved_profiles": True,
            "enable_auto_switch_wifi": True,
            "internal_ssid": "inner",
            "external_ssid": "outer",
            "external_profile_name": "",
            "internal_profile_name": "",
            "switch_back_to_original": False,
        },
        "feishu": {"enable_upload": True},
    }

    out = run_pipeline_with_time_windows(
        config=cfg,
        time_windows=[{"date": "2026-03-08", "start_time": "2026-03-08 00:00:00", "end_time": "2026-03-09 00:00:00"}],
        normalize_runtime_config=lambda c: c,
        validate_runtime_config=lambda _c: None,
        configure_playwright_environment=lambda _c: "",
        load_calc_module=lambda: type("M", (), {"run_with_explicit_files": object(), "run_with_explicit_file_items": object()})(),
        resolve_run_save_dir=lambda _d: "D:\\QLDownload\\run_x",
        build_checkpoint=lambda **_k: {"file_items": [], "run_id": "r1"},
        save_checkpoint_and_index=lambda _c, cp: cp,
        sync_summary_from_checkpoint=lambda _s, _cp: None,
        build_wifi_switcher=lambda _n, log_cb=None: _Wifi(),
        try_switch_wifi=lambda **_k: (False, "switch fail", False),
        build_time_window_download_tasks=lambda **_k: ({}, []),
        run_download_tasks_by_building=lambda **_k: [],
        retry_failed_download_tasks=lambda **_k: [],
        collect_first_pass_results=lambda _p: ({}, []),
        merge_retry_results=lambda a, _b: a,
        apply_download_outcomes=lambda **_k: None,
        flush_pending_notify_events=lambda **_k: None,
        notify_event=lambda *_a, **_k: None,
        log_file_failure=lambda **_k: None,
        upload_retryable_items=lambda **_k: {},
        task_factory=object,
        emit_log=lambda _m: None,
    )
    assert "切换内网失败" in out.get("error", "")


def test_pipeline_flow_suppresses_fixed_network_skip_logs():
    cfg = {
        "download": {
            "resume": {"gc_every_n_items": 5, "upload_chunk_threshold": 20, "upload_chunk_size": 5},
            "browser_channel": "chrome",
            "only_process_downloaded_this_run": True,
            "sites": [],
        },
        "network": {
            "require_saved_profiles": True,
            "enable_auto_switch_wifi": False,
            "internal_ssid": "inner",
            "external_ssid": "outer",
            "external_profile_name": "",
            "internal_profile_name": "",
            "switch_back_to_original": True,
        },
        "feishu": {"enable_upload": False},
    }
    logs = []

    run_pipeline_with_time_windows(
        config=cfg,
        time_windows=[{"date": "2026-03-08", "start_time": "2026-03-08 00:00:00", "end_time": "2026-03-09 00:00:00"}],
        normalize_runtime_config=lambda c: c,
        validate_runtime_config=lambda _c: None,
        configure_playwright_environment=lambda _c: "",
        load_calc_module=lambda: type("M", (), {"run_with_explicit_files": object(), "run_with_explicit_file_items": object()})(),
        resolve_run_save_dir=lambda _d: "D:\\QLDownload\\run_x",
        build_checkpoint=lambda **_k: {"file_items": [], "run_id": "r1"},
        save_checkpoint_and_index=lambda _c, cp: cp,
        sync_summary_from_checkpoint=lambda _s, _cp: None,
        build_wifi_switcher=lambda _n, log_cb=None: _Wifi(),
        try_switch_wifi=lambda **_k: (True, "skip", True),
        build_time_window_download_tasks=lambda **_k: ({}, []),
        run_download_tasks_by_building=_empty_async_result,
        retry_failed_download_tasks=_empty_async_result,
        collect_first_pass_results=lambda _p: ({}, []),
        merge_retry_results=lambda a, _b: a,
        apply_download_outcomes=lambda **_k: None,
        flush_pending_notify_events=lambda **_k: None,
        notify_event=lambda *_a, **_k: None,
        log_file_failure=lambda **_k: None,
        upload_retryable_items=lambda **_k: {},
        task_factory=object,
        emit_log=logs.append,
    )

    assert all("SSID" not in message for message in logs)
    assert all("切换到内网" not in message for message in logs)
    assert all("切换到外网" not in message for message in logs)
