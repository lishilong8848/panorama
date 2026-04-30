from __future__ import annotations

import copy
from typing import Any, Callable, Dict


def run_resume_upload(
    config: Dict[str, Any],
    run_id: str | None = None,
    auto_trigger: bool = False,
    *,
    normalize_runtime_config: Callable[[Dict[str, Any]], Dict[str, Any]],
    validate_runtime_config: Callable[[Dict[str, Any]], None],
    ensure_resume_config: Callable[[Dict[str, Any]], Dict[str, Any]],
    load_pending_checkpoint: Callable[[Dict[str, Any], str | None], Dict[str, Any] | None],
    sync_summary_from_checkpoint: Callable[[Dict[str, Any], Dict[str, Any]], None],
    save_checkpoint_and_index: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]],
    load_calc_module: Callable[[], Any],
    build_wifi_switcher: Callable[..., Any],
    try_switch_wifi: Callable[..., tuple[bool, str, bool]],
    upload_retryable_items: Callable[..., Dict[str, Any]],
    notify_event: Callable[..., None],
    log_file_failure: Callable[..., None],
    emit_log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    cfg = normalize_runtime_config(copy.deepcopy(config))
    validate_runtime_config(cfg)

    download_cfg = cfg["download"]
    feishu_cfg = cfg["feishu"]
    network_cfg = cfg["network"]
    resume_cfg = ensure_resume_config(download_cfg if isinstance(download_cfg, dict) else {})

    summary: Dict[str, Any] = {
        "resumed": False,
        "auto_trigger": bool(auto_trigger),
        "run_id": str(run_id or "").strip(),
        "pending_resume": False,
        "pending_upload_count": 0,
        "upload_failed_count": 0,
        "uploaded_count": 0,
        "error": "",
    }

    if not bool(resume_cfg["enabled"]):
        summary["error"] = "download.resume.enabled=false，续传已禁用"
        return summary

    checkpoint = load_pending_checkpoint(cfg, run_id=run_id)
    if checkpoint is None:
        summary["error"] = "没有待续传任务"
        return summary

    summary["run_id"] = str(checkpoint.get("run_id", "")).strip()
    summary["run_save_dir"] = str(checkpoint.get("run_save_dir", "")).strip()
    summary["selected_dates"] = list(checkpoint.get("selected_dates", []))
    sync_summary_from_checkpoint(summary, checkpoint)

    if int(summary.get("pending_upload_count", 0)) <= 0:
        checkpoint["stage"] = "completed"
        checkpoint["last_error"] = ""
        checkpoint = save_checkpoint_and_index(cfg, checkpoint)
        sync_summary_from_checkpoint(summary, checkpoint)
        summary["resumed"] = True
        return summary

    if not bool(feishu_cfg.get("enable_upload", False)):
        summary["error"] = "feishu.enable_upload=false，未执行续传上传"
        return summary

    calc_module = load_calc_module()
    if not hasattr(calc_module, "run_with_explicit_file_items"):
        raise RuntimeError("计算脚本缺少 run_with_explicit_file_items 入口，请先升级表格计算模块代码")

    wifi = build_wifi_switcher(network_cfg, log_cb=emit_log) if bool(network_cfg) else None
    require_saved = bool(network_cfg.get("require_saved_profiles", False))
    external_ssid = str(network_cfg.get("external_ssid", "")).strip()
    enable_auto_switch_wifi = False
    current_ssid = wifi.get_current_ssid() if wifi is not None else ""

    if wifi is not None and external_ssid and current_ssid != external_ssid:
        ok, msg, skipped = try_switch_wifi(
            wifi=wifi,
            network_cfg=network_cfg,
            enable_auto_switch_wifi=enable_auto_switch_wifi,
            target_ssid=external_ssid,
            require_saved_profile=require_saved,
            profile_name=str(network_cfg.get("external_profile_name", "") or "").strip() or None,
        )
        if skipped:
            emit_log("[网络] 当前角色固定网络，续传阶段按当前网络继续执行")
        elif not ok:
            checkpoint["stage"] = "wait_external_upload"
            checkpoint["last_error"] = f"切换外网失败: {msg}"
            checkpoint = save_checkpoint_and_index(cfg, checkpoint)
            sync_summary_from_checkpoint(summary, checkpoint)
            summary["pending_resume"] = True
            summary["error"] = f"切换外网失败，等待网络就绪后继续续传: {msg}"
            emit_log(f"[续传] {summary['error']}")
            log_file_failure(
                feature="断点续传",
                stage="网络准备(外网)",
                building="-",
                file_path="-",
                upload_date="-",
                error=msg,
            )
            return summary
        else:
            emit_log(f"[续传] 已切换外网: {msg}")

    checkpoint["stage"] = "uploading"
    checkpoint["last_error"] = ""
    checkpoint = save_checkpoint_and_index(cfg, checkpoint)
    sync_summary_from_checkpoint(summary, checkpoint)
    emit_log(f"[续传] 使用历史下载文件继续上传，未重新下载。run_id={summary['run_id']}")

    upload_result = upload_retryable_items(
        config=cfg,
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
    if upload_result["failed_count"] > 0:
        notify_event(
            cfg,
            stage="计算上传",
            detail=f"run_id={summary['run_id']}; 续传失败 {upload_result['failed_count']} 项，可再次续传。",
            toggle_key="on_upload_failure",
            wifi=wifi,
            external_ssid=external_ssid,
        )

    if int(summary.get("pending_upload_count", 0)) > 0:
        checkpoint["stage"] = "completed_with_failures"
        checkpoint["last_error"] = f"续传后仍有失败项: {summary['pending_upload_count']}"
        summary["pending_resume"] = True
    elif int(summary.get("file_missing_count", 0)) > 0:
        checkpoint["stage"] = "completed_with_failures"
        checkpoint["last_error"] = f"续传完成，但存在缺失文件: {summary['file_missing_count']}"
        summary["pending_resume"] = False
    else:
        checkpoint["stage"] = "completed"
        checkpoint["last_error"] = ""
        summary["pending_resume"] = False

    checkpoint = save_checkpoint_and_index(cfg, checkpoint)
    sync_summary_from_checkpoint(summary, checkpoint)
    summary["resumed"] = True
    emit_log(
        f"[续传] 本次成功 {summary.get('upload_success_count', 0)} 项，"
        f"失败 {summary.get('upload_failed_count', 0)} 项。"
    )
    return summary
