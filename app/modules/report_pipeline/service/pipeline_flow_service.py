from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, Type


def run_pipeline_with_time_windows(
    config: Dict[str, Any],
    time_windows: List[Dict[str, str]],
    *,
    source_name: str = "自动流程",
    force_explicit_files: bool = False,
    normalize_runtime_config: Callable[[Dict[str, Any]], Dict[str, Any]],
    validate_runtime_config: Callable[[Dict[str, Any]], None],
    configure_playwright_environment: Callable[[Dict[str, Any]], str],
    load_calc_module: Callable[[], Any],
    resolve_run_save_dir: Callable[[Dict[str, Any]], str],
    build_checkpoint: Callable[..., Dict[str, Any]],
    save_checkpoint_and_index: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]],
    sync_summary_from_checkpoint: Callable[[Dict[str, Any], Dict[str, Any]], None],
    build_wifi_switcher: Callable[..., Any],
    try_switch_wifi: Callable[..., tuple[bool, str, bool]],
    build_time_window_download_tasks: Callable[..., tuple[Dict[str, Dict[str, Any]], List[Any]]],
    run_download_tasks_by_building: Callable[..., Any],
    retry_failed_download_tasks: Callable[..., Any],
    collect_first_pass_results: Callable[..., Any],
    merge_retry_results: Callable[..., Any],
    apply_download_outcomes: Callable[..., None],
    flush_pending_notify_events: Callable[..., None],
    notify_event: Callable[..., None],
    log_file_failure: Callable[..., None],
    upload_retryable_items: Callable[..., Dict[str, Any]],
    task_factory: Type[Any],
    emit_log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    config = normalize_runtime_config(config)
    validate_runtime_config(config)
    download_cfg = config["download"]
    resume_cfg = download_cfg["resume"]

    browser_channel = str(download_cfg["browser_channel"]).strip()
    browsers_path = configure_playwright_environment(config)
    if browsers_path:
        emit_log(f"[Playwright] 使用浏览器目录: {browsers_path}")
    elif browser_channel:
        emit_log(f"[Playwright] 未内置 ms-playwright 目录，使用系统浏览器 channel={browser_channel}")
    else:
        emit_log("[Playwright] 未内置 ms-playwright 目录，将尝试 Playwright 默认浏览器配置")

    calc_module = load_calc_module()
    if not hasattr(calc_module, "run_with_explicit_files"):
        raise RuntimeError("计算脚本缺少 run_with_explicit_files 入口，请先升级表格计算模块代码")
    if not hasattr(calc_module, "run_with_explicit_file_items"):
        raise RuntimeError("计算脚本缺少 run_with_explicit_file_items 入口，请先升级表格计算模块代码")

    network_cfg = config["network"]
    feishu_cfg = config["feishu"]
    run_save_dir = resolve_run_save_dir(download_cfg)
    download_cfg["save_dir"] = run_save_dir
    emit_log(f"[{source_name}] 本次运行目录: {run_save_dir}")

    summary: Dict[str, Any] = {
        "source": source_name,
        "run_save_dir": run_save_dir,
        "selected_dates": [str(item.get("date", "")).strip() for item in time_windows],
        "processed_dates": 0,
        "date_results": [],
        "file_items": [],
        "success_dates": [],
        "failed_dates": [],
        "pending_resume": False,
        "resume_run_id": "",
        "pending_upload_count": 0,
        "upload_failed_count": 0,
    }

    checkpoint = build_checkpoint(
        source_name=source_name,
        run_save_dir=run_save_dir,
        selected_dates=summary["selected_dates"],
    )
    checkpoint = save_checkpoint_and_index(config, checkpoint)
    sync_summary_from_checkpoint(summary, checkpoint)

    wifi = build_wifi_switcher(network_cfg, log_cb=emit_log)
    require_saved = bool(network_cfg["require_saved_profiles"])
    original_ssid = wifi.get_current_ssid()
    pending_notify_events: List[Any] = []
    enable_auto_switch_wifi = bool(network_cfg.get("enable_auto_switch_wifi", True))
    if enable_auto_switch_wifi:
        emit_log(f"[网络] 当前SSID: {original_ssid}")

    internal_ssid = str(network_cfg["internal_ssid"]).strip()
    external_ssid = str(network_cfg["external_ssid"]).strip()
    external_profile_name = str(network_cfg.get("external_profile_name", "") or "").strip() or None
    switch_back = bool(network_cfg["switch_back_to_original"])

    ok, msg, skipped = try_switch_wifi(
        wifi=wifi,
        network_cfg=network_cfg,
        enable_auto_switch_wifi=enable_auto_switch_wifi,
        target_ssid=internal_ssid,
        require_saved_profile=require_saved,
        profile_name=str(network_cfg.get("internal_profile_name", "") or "").strip() or None,
    )
    if skipped:
        pass
    elif not ok:
        emit_log(f"[网络] 切换内网失败: {msg}")
        log_file_failure(
            feature=source_name,
            stage="WiFi切换(内网)",
            building="-",
            file_path="-",
            upload_date="-",
            error=msg,
        )
        notify_event(
            config,
            stage="WiFi切换(内网)",
            detail=msg,
            toggle_key="on_wifi_failure",
            wifi=wifi,
            external_ssid=external_ssid,
            pending_events=pending_notify_events,
        )
        checkpoint["stage"] = "wait_external_upload"
        checkpoint["last_error"] = f"切换内网失败: {msg}"
        checkpoint = save_checkpoint_and_index(config, checkpoint)
        sync_summary_from_checkpoint(summary, checkpoint)
        summary["error"] = f"切换内网失败: {msg}"
        flush_pending_notify_events(
            config=config,
            wifi=wifi,
            external_ssid=external_ssid,
            external_profile_name=external_profile_name,
            require_saved_profile=require_saved,
            enable_auto_switch_wifi=enable_auto_switch_wifi,
            pending_events=pending_notify_events,
        )
        return summary
    else:
        emit_log(f"[网络] {msg}")

    enabled_sites = [site for site in download_cfg["sites"] if bool(site["enabled"])]
    date_result_by_date, all_download_tasks = build_time_window_download_tasks(
        time_windows=time_windows,
        enabled_sites=enabled_sites,
        run_save_dir=run_save_dir,
        config=config,
        task_factory=task_factory,
        emit_log=emit_log,
    )

    first_pass_pairs = asyncio.run(
        run_download_tasks_by_building(
            config=config,
            download_tasks=all_download_tasks,
            feature=source_name,
            success_stage="内网下载",
            failure_stage="内网下载",
        )
    )
    final_outcome_by_key, failed_for_retry = collect_first_pass_results(first_pass_pairs)

    retry_pairs = asyncio.run(
        retry_failed_download_tasks(
            config=config,
            failed_tasks=failed_for_retry,
            source_name=source_name,
        )
    )
    final_outcome_by_key = merge_retry_results(final_outcome_by_key, retry_pairs)

    def _notify_download_failure(date_text: str, building: str, error: str) -> None:
        notify_event(
            config,
            stage="内网下载",
            building=building,
            detail=f"日期={date_text}; {error}",
            toggle_key="on_download_failure",
            wifi=wifi,
            external_ssid=external_ssid,
            pending_events=pending_notify_events,
        )

    apply_download_outcomes(
        final_outcome_by_key=final_outcome_by_key,
        date_result_by_date=date_result_by_date,
        summary=summary,
        checkpoint=checkpoint,
        notify_failure=_notify_download_failure,
    )
    checkpoint["stage"] = "download_done"
    checkpoint["date_results"] = summary["date_results"]
    checkpoint["last_error"] = ""
    checkpoint = save_checkpoint_and_index(config, checkpoint)
    sync_summary_from_checkpoint(summary, checkpoint)

    if not checkpoint["file_items"]:
        emit_log("[下载] 所有日期均未下载到有效文件，本次任务结束")
        checkpoint["stage"] = "completed"
        checkpoint["last_error"] = ""
        checkpoint = save_checkpoint_and_index(config, checkpoint)
        sync_summary_from_checkpoint(summary, checkpoint)
        flush_pending_notify_events(
            config=config,
            wifi=wifi,
            external_ssid=external_ssid,
            external_profile_name=external_profile_name,
            require_saved_profile=require_saved,
            enable_auto_switch_wifi=enable_auto_switch_wifi,
            pending_events=pending_notify_events,
        )
        if switch_back and original_ssid and enable_auto_switch_wifi:
            wifi.connect(original_ssid, require_saved_profile=require_saved)
        return summary

    need_upload = bool(feishu_cfg["enable_upload"])
    if need_upload:
        ok, msg, skipped = try_switch_wifi(
            wifi=wifi,
            network_cfg=network_cfg,
            enable_auto_switch_wifi=enable_auto_switch_wifi,
            target_ssid=external_ssid,
            require_saved_profile=require_saved,
            profile_name=str(network_cfg.get("external_profile_name", "") or "").strip() or None,
        )
        if skipped:
            pass
        elif not ok:
            emit_log(f"[网络] 切换外网失败: {msg}")
            log_file_failure(
                feature=source_name,
                stage="WiFi切换(外网)",
                building="-",
                file_path="-",
                upload_date="-",
                error=msg,
            )
            notify_event(
                config,
                stage="WiFi切换(外网)",
                detail=msg,
                toggle_key="on_wifi_failure",
                wifi=wifi,
                external_ssid=external_ssid,
                pending_events=pending_notify_events,
            )
            checkpoint["stage"] = "wait_external_upload"
            checkpoint["last_error"] = f"切换外网失败: {msg}"
            checkpoint = save_checkpoint_and_index(config, checkpoint)
            sync_summary_from_checkpoint(summary, checkpoint)
            summary["error"] = f"切换外网失败: {msg}"
            flush_pending_notify_events(
                config=config,
                wifi=wifi,
                external_ssid=external_ssid,
                external_profile_name=external_profile_name,
                require_saved_profile=require_saved,
                enable_auto_switch_wifi=enable_auto_switch_wifi,
                pending_events=pending_notify_events,
            )
            if switch_back and original_ssid and enable_auto_switch_wifi:
                wifi.connect(original_ssid, require_saved_profile=require_saved)
            return summary
        else:
            emit_log(f"[网络] {msg}")

    flush_pending_notify_events(
        config=config,
        wifi=wifi,
        external_ssid=external_ssid,
        external_profile_name=external_profile_name,
        require_saved_profile=require_saved,
        enable_auto_switch_wifi=enable_auto_switch_wifi,
        pending_events=pending_notify_events,
    )

    try:
        only_this_run = bool(download_cfg["only_process_downloaded_this_run"]) or force_explicit_files
        if only_this_run and need_upload:
            checkpoint["stage"] = "uploading"
            checkpoint["last_error"] = ""
            checkpoint = save_checkpoint_and_index(config, checkpoint)
            sync_summary_from_checkpoint(summary, checkpoint)

            upload_result = upload_retryable_items(
                config=config,
                calc_module=calc_module,
                checkpoint=checkpoint,
                gc_every_n_items=int(resume_cfg["gc_every_n_items"]),
                upload_chunk_threshold=int(resume_cfg["upload_chunk_threshold"]),
                upload_chunk_size=int(resume_cfg["upload_chunk_size"]),
            )
            sync_summary_from_checkpoint(summary, checkpoint)
            summary["upload_processed_count"] = upload_result["processed_count"]
            summary["upload_success_count"] = upload_result["success_count"]
            summary["upload_failed_count"] = upload_result["failed_count"]
            summary["upload_failure_items"] = upload_result["failure_items"]

            if int(summary.get("pending_upload_count", 0)) > 0:
                checkpoint["stage"] = "completed_with_failures"
                checkpoint["last_error"] = f"上传存在失败项: {summary['pending_upload_count']}"
            elif int(summary.get("file_missing_count", 0)) > 0:
                checkpoint["stage"] = "completed_with_failures"
                checkpoint["last_error"] = f"存在缺失文件: {summary['file_missing_count']}"
            else:
                checkpoint["stage"] = "completed"
                checkpoint["last_error"] = ""
            checkpoint = save_checkpoint_and_index(config, checkpoint)
            sync_summary_from_checkpoint(summary, checkpoint)

            if upload_result["failed_count"] > 0:
                detail = f"run_id={checkpoint['run_id']}; 上传失败{upload_result['failed_count']}项，可继续续传"
                emit_log(f"[上传] {detail}")
                notify_event(
                    config,
                    stage="计算上传",
                    detail=detail,
                    toggle_key="on_upload_failure",
                    wifi=wifi,
                    external_ssid=external_ssid,
                    pending_events=pending_notify_events,
                )
            emit_log(f"[{source_name}] 本次任务处理完成")
        elif only_this_run:
            calc_module.run_with_explicit_file_items(
                config=config,
                file_items=summary["file_items"],
                upload=False,
                save_json=False,
            )
            checkpoint["stage"] = "completed"
            checkpoint["last_error"] = ""
            checkpoint = save_checkpoint_and_index(config, checkpoint)
            sync_summary_from_checkpoint(summary, checkpoint)
            emit_log(f"[{source_name}] 本次任务处理完成")
        else:
            config["input"]["excel_dir"] = run_save_dir
            results = calc_module.run_with_config(config)
            if results and need_upload:
                calc_module.upload_results_to_feishu(results, config)
            checkpoint["stage"] = "completed"
            checkpoint["last_error"] = ""
            checkpoint = save_checkpoint_and_index(config, checkpoint)
            sync_summary_from_checkpoint(summary, checkpoint)
            emit_log(f"[{source_name}] 本次任务处理完成")
    except Exception as exc:  # noqa: BLE001
        detail = str(exc)
        summary["error"] = detail
        emit_log(f"[上传] 计算/上传阶段失败: {detail}")
        log_file_failure(
            feature=source_name,
            stage="计算上传",
            building="-",
            file_path="-",
            upload_date="-",
            error=detail,
        )
        checkpoint["stage"] = "wait_external_upload"
        checkpoint["last_error"] = detail
        checkpoint = save_checkpoint_and_index(config, checkpoint)
        sync_summary_from_checkpoint(summary, checkpoint)
        notify_event(
            config,
            stage="计算上传",
            detail=detail,
            toggle_key="on_upload_failure",
            wifi=wifi,
            external_ssid=external_ssid,
            pending_events=pending_notify_events,
        )
    finally:
        flush_pending_notify_events(
            config=config,
            wifi=wifi,
            external_ssid=external_ssid,
            external_profile_name=external_profile_name,
            require_saved_profile=require_saved,
            enable_auto_switch_wifi=enable_auto_switch_wifi,
            pending_events=pending_notify_events,
        )
        if switch_back and original_ssid and enable_auto_switch_wifi:
            current = wifi.get_current_ssid()
            if current != original_ssid:
                ok, msg = wifi.connect(original_ssid, require_saved_profile=require_saved)
                emit_log(f"[网络] 恢复原SSID: {'成功' if ok else '失败'} - {msg}")
    return summary
