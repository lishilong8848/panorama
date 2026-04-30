from __future__ import annotations

import copy
import gc
import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

from app.modules.report_pipeline.core.time_window_policy import (
    build_time_range as build_time_range_policy,
    combine_date_hms as combine_date_hms_policy,
    normalize_selected_dates as normalize_selected_dates_policy,
    parse_hms_text as parse_hms_text_policy,
)
from app.modules.report_pipeline.service.resume_checkpoint_store import (
    build_checkpoint as build_checkpoint_store,
    checkpoint_path as checkpoint_path_store,
    cleanup_resume_index as cleanup_resume_index_store,
    collect_retryable_file_items as collect_retryable_file_items_store,
    list_pending_upload_runs_internal as list_pending_upload_runs_internal_store,
    load_checkpoint_by_path as load_checkpoint_by_path_store,
    load_pending_checkpoint as load_pending_checkpoint_store,
    load_resume_index as load_resume_index_store,
    new_run_id as new_run_id_store,
    normalize_checkpoint as normalize_checkpoint_store,
    now_text as now_text_store,
    parse_time_or_none as parse_time_or_none_store,
    refresh_checkpoint_summary as refresh_checkpoint_summary_store,
    resolve_resume_index_path as resolve_resume_index_path_store,
    resolve_resume_root_dir as resolve_resume_root_dir_store,
    safe_load_json as safe_load_json_store,
    safe_save_json as safe_save_json_store,
    save_checkpoint_and_index as save_checkpoint_and_index_store,
    save_resume_index as save_resume_index_store,
    sync_summary_from_checkpoint as sync_summary_from_checkpoint_store,
)
from app.modules.report_pipeline.service.download_runtime_utils import (
    extract_site_host as extract_site_host_runtime,
    group_download_tasks_by_building as group_download_tasks_by_building_runtime,
    is_retryable_download_timeout as is_retryable_download_timeout_runtime,
    get_multi_date_max as get_multi_date_max_runtime,
    resolve_run_save_dir as resolve_run_save_dir_runtime,
    resolve_site_urls as resolve_site_urls_runtime,
)
from app.modules.report_pipeline.service.download_task_executor import (
    run_download_tasks_by_building as run_download_tasks_by_building_runtime,
)
from app.modules.report_pipeline.service.download_site_worker import (
    DownloadOutcome,
    download_site_with_retry as download_site_with_retry_runtime,
)
from app.modules.report_pipeline.service.resume_upload_runner import (
    upload_retryable_items as upload_retryable_items_runtime,
)
from app.modules.report_pipeline.service.pipeline_flow_service import (
    run_pipeline_with_time_windows as run_pipeline_with_time_windows_runtime,
)
from app.modules.report_pipeline.service.pipeline_notify_runtime import (
    PendingNotifyEvent,
    flush_pending_notify_events as flush_pending_notify_events_runtime,
    notify_event as notify_event_runtime,
)
from app.modules.report_pipeline.service.resume_flow_service import (
    run_resume_upload as run_resume_upload_runtime,
)
from app.modules.report_pipeline.service.pipeline_window_builder import (
    build_time_window_download_tasks as build_time_window_download_tasks_runtime,
)
from app.modules.report_pipeline.service.download_result_utils import (
    apply_download_outcomes as apply_download_outcomes_runtime,
    collect_first_pass_results as collect_first_pass_results_runtime,
    merge_retry_results as merge_retry_results_runtime,
)
from app.modules.report_pipeline.service.download_retry_utils import (
    retry_failed_download_tasks as retry_failed_download_tasks_runtime,
)
from app.modules.report_pipeline.service.runtime_config_defaults import (
    ensure_performance_config as ensure_performance_config_runtime,
    ensure_resume_config as ensure_resume_config_runtime,
)
from app.modules.report_pipeline.service.runtime_config_normalizer import (
    normalize_runtime_config as normalize_runtime_config_runtime,
)
from app.modules.report_pipeline.service.runtime_config_validator import (
    validate_runtime_config as validate_runtime_config_runtime,
)
from app.modules.report_pipeline.service.wifi_factory import (
    build_wifi_switcher as build_wifi_switcher_runtime,
    try_switch_wifi as try_switch_wifi_runtime,
)
from pipeline_utils import (
    build_event_text,
    configure_playwright_environment,
    get_app_dir,
    get_last_month_window,
    load_calc_module,
    load_pipeline_config,
    send_feishu_webhook,
)
@dataclass
class DownloadTask:
    date_text: str
    start_time: str
    end_time: str
    save_dir: str
    site: Dict[str, Any]
    attempt_round: str = "first_pass"


def _norm_log_value(value: Any, default: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else default


def _log_file_success(
    *,
    feature: str,
    stage: str,
    building: str | None = None,
    file_path: str | None = None,
    upload_date: str | None = None,
    detail: str = "",
) -> None:
    print(
        f"[文件上传成功] 功能={_norm_log_value(feature)} 阶段={_norm_log_value(stage)} "
        f"楼栋={_norm_log_value(building)} 文件={_norm_log_value(file_path)} "
        f"日期={_norm_log_value(upload_date)} 详情={_norm_log_value(detail)}"
    )


def _log_file_failure(
    *,
    feature: str,
    stage: str,
    building: str | None = None,
    file_path: str | None = None,
    upload_date: str | None = None,
    error: str = "",
) -> None:
    print(
        f"[文件流程失败] 功能={_norm_log_value(feature)} 阶段={_norm_log_value(stage)} "
        f"楼栋={_norm_log_value(building)} 文件={_norm_log_value(file_path)} "
        f"日期={_norm_log_value(upload_date)} 错误={_norm_log_value(error)}"
    )


def _extract_site_host(raw_value: Any) -> str:
    return extract_site_host_runtime(raw_value)


def _parse_hms_text(value: Any, field_name: str) -> tuple[int, int, int]:
    return parse_hms_text_policy(value, field_name)


def _combine_date_hms(day: date, hms_text: str, field_name: str) -> datetime:
    return combine_date_hms_policy(day, hms_text, field_name)


def _notify_event(
    config: Dict[str, Any],
    stage: str,
    detail: str,
    building: str | None = None,
    toggle_key: str | None = None,
    wifi: Any | None = None,
    external_ssid: str | None = None,
    pending_events: List[PendingNotifyEvent] | None = None,
) -> None:
    notify_event_runtime(
        config=config,
        stage=stage,
        detail=detail,
        building=building,
        toggle_key=toggle_key,
        wifi=wifi,
        external_ssid=external_ssid,
        pending_events=pending_events,
        build_event_text=build_event_text,
        send_feishu_webhook=send_feishu_webhook,
        emit_log=print,
    )


def _flush_pending_notify_events(
    config: Dict[str, Any],
    wifi: Any,
    external_ssid: str,
    external_profile_name: str | None,
    require_saved_profile: bool,
    enable_auto_switch_wifi: bool,
    pending_events: List[PendingNotifyEvent],
) -> None:
    flush_pending_notify_events_runtime(
        config=config,
        wifi=wifi,
        external_ssid=external_ssid,
        external_profile_name=external_profile_name,
        require_saved_profile=require_saved_profile,
        enable_auto_switch_wifi=enable_auto_switch_wifi,
        pending_events=pending_events,
        try_switch_wifi=try_switch_wifi_runtime,
        notify_event=_notify_event,
        emit_log=print,
    )


def _ensure_resume_config(download_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return ensure_resume_config_runtime(download_cfg)


def _ensure_performance_config(download_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return ensure_performance_config_runtime(download_cfg)


def _now_text() -> str:
    return now_text_store()


def _new_run_id() -> str:
    return new_run_id_store()


def _parse_time_or_none(value: str) -> datetime | None:
    return parse_time_or_none_store(value)


def _safe_load_json(path: Path, default_obj: Dict[str, Any]) -> Dict[str, Any]:
    return safe_load_json_store(path, default_obj)


def _safe_save_json(path: Path, data: Dict[str, Any]) -> None:
    safe_save_json_store(path, data)


def _runtime_state_root(config: Dict[str, Any] | None = None) -> str:
    cfg = config if isinstance(config, dict) else {}
    paths_cfg = cfg.get("paths", {})
    if not isinstance(paths_cfg, dict):
        return ""
    return str(paths_cfg.get("runtime_state_root", "")).strip()


def _resume_root_dir(config: Dict[str, Any] | None = None) -> str:
    cfg = config if isinstance(config, dict) else {}
    download_cfg = cfg.get("download", {})
    if not isinstance(download_cfg, dict):
        return ""
    resume_cfg = _ensure_resume_config(download_cfg)
    return str(resume_cfg.get("root_dir", "")).strip()


def _resume_index_file(config: Dict[str, Any] | None = None) -> str:
    cfg = config if isinstance(config, dict) else {}
    download_cfg = cfg.get("download", {})
    if not isinstance(download_cfg, dict):
        return ""
    resume_cfg = _ensure_resume_config(download_cfg)
    return str(resume_cfg.get("index_file", "")).strip()


def _resolve_resume_root_dir(config: Dict[str, Any] | None = None) -> Path:
    return resolve_resume_root_dir_store(
        app_dir=get_app_dir(),
        root_dir=_resume_root_dir(config),
        runtime_state_root=_runtime_state_root(config),
    )


def _resolve_resume_index_path(config: Dict[str, Any] | None = None) -> Path:
    return resolve_resume_index_path_store(
        app_dir=get_app_dir(),
        root_dir=_resume_root_dir(config),
        index_file=_resume_index_file(config),
        runtime_state_root=_runtime_state_root(config),
    )


def _checkpoint_path(run_save_dir: str) -> Path:
    return checkpoint_path_store(run_save_dir)


def _refresh_checkpoint_summary(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    return refresh_checkpoint_summary_store(checkpoint)


def _normalize_checkpoint(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    return normalize_checkpoint_store(checkpoint)


def _load_resume_index(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return load_resume_index_store(
        app_dir=get_app_dir(),
        root_dir=_resume_root_dir(config),
        index_file=_resume_index_file(config),
        runtime_state_root=_runtime_state_root(config),
    )


def _save_resume_index(index_obj: Dict[str, Any], config: Dict[str, Any] | None = None) -> None:
    save_resume_index_store(
        index_obj,
        app_dir=get_app_dir(),
        root_dir=_resume_root_dir(config),
        index_file=_resume_index_file(config),
        runtime_state_root=_runtime_state_root(config),
    )


def _cleanup_resume_index(retention_days: int, config: Dict[str, Any] | None = None) -> None:
    cleanup_resume_index_store(
        retention_days,
        app_dir=get_app_dir(),
        root_dir=_resume_root_dir(config),
        index_file=_resume_index_file(config),
        runtime_state_root=_runtime_state_root(config),
    )


def _save_checkpoint_and_index(config: Dict[str, Any], checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    download_cfg = config.get("download", {})
    resume_cfg = _ensure_resume_config(download_cfg if isinstance(download_cfg, dict) else {})
    return save_checkpoint_and_index_store(
        checkpoint,
        retention_days=int(resume_cfg.get("retention_days", 7)),
        app_dir=get_app_dir(),
        root_dir=_resume_root_dir(config),
        index_file=_resume_index_file(config),
        runtime_state_root=_runtime_state_root(config),
    )


def _load_checkpoint_by_path(checkpoint_path: Path) -> Dict[str, Any]:
    return load_checkpoint_by_path_store(checkpoint_path)


def _list_pending_upload_runs_internal(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    download_cfg = config.get("download", {})
    resume_cfg = _ensure_resume_config(download_cfg if isinstance(download_cfg, dict) else {})
    if not bool(resume_cfg.get("enabled", True)):
        return []
    return list_pending_upload_runs_internal_store(
        retention_days=int(resume_cfg.get("retention_days", 7)),
        app_dir=get_app_dir(),
        root_dir=_resume_root_dir(config),
        index_file=_resume_index_file(config),
        runtime_state_root=_runtime_state_root(config),
    )


def list_pending_upload_runs(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    cfg = _normalize_runtime_config(copy.deepcopy(config))
    _validate_runtime_config(cfg)
    return _list_pending_upload_runs_internal(cfg)


def delete_pending_upload_run(config: Dict[str, Any], run_id: str) -> Dict[str, Any]:
    cfg = _normalize_runtime_config(copy.deepcopy(config))
    _validate_runtime_config(cfg)

    run_text = str(run_id or "").strip()
    if not run_text:
        raise ValueError("run_id 不能为空")

    download_cfg = cfg.get("download", {})
    resume_cfg = _ensure_resume_config(download_cfg if isinstance(download_cfg, dict) else {})
    _cleanup_resume_index(int(resume_cfg.get("retention_days", 7)), cfg)

    index_obj = _load_resume_index(cfg)
    items = index_obj.get("items")
    if not isinstance(items, list):
        items = []

    target_item: Dict[str, Any] | None = None
    kept_items: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        item_run_id = str(item.get("run_id", "")).strip()
        checkpoint_path = Path(str(item.get("checkpoint_path", "")).strip())
        if checkpoint_path.exists():
            try:
                checkpoint_obj = _load_checkpoint_by_path(checkpoint_path)
                item_run_id = str(checkpoint_obj.get("run_id", "")).strip() or item_run_id
            except Exception:
                pass

        if target_item is None and item_run_id == run_text:
            target_item = item
            continue
        kept_items.append(item)

    if target_item is None:
        return {
            "ok": False,
            "deleted": False,
            "run_id": run_text,
            "message": f"未找到续传任务: {run_text}",
        }

    index_obj["items"] = kept_items
    _save_resume_index(index_obj, cfg)

    checkpoint_path_text = str(target_item.get("checkpoint_path", "")).strip()
    run_save_dir = str(target_item.get("run_save_dir", "")).strip()
    if not checkpoint_path_text and run_save_dir:
        checkpoint_path_text = str(_checkpoint_path(run_save_dir))
    checkpoint_path = Path(checkpoint_path_text) if checkpoint_path_text else None

    checkpoint_deleted = False
    checkpoint_delete_error = ""
    if checkpoint_path and checkpoint_path.exists():
        try:
            checkpoint_path.unlink()
            checkpoint_deleted = True
        except Exception as exc:  # noqa: BLE001
            checkpoint_delete_error = str(exc)

    return {
        "ok": True,
        "deleted": True,
        "run_id": run_text,
        "run_save_dir": run_save_dir,
        "checkpoint_path": checkpoint_path_text,
        "checkpoint_deleted": checkpoint_deleted,
        "checkpoint_delete_error": checkpoint_delete_error,
        "message": f"已删除续传任务: {run_text}",
    }


def _load_pending_checkpoint(config: Dict[str, Any], run_id: str | None = None) -> Dict[str, Any] | None:
    download_cfg = config.get("download", {})
    resume_cfg = _ensure_resume_config(download_cfg if isinstance(download_cfg, dict) else {})
    if not bool(resume_cfg.get("enabled", True)):
        return None
    return load_pending_checkpoint_store(
        run_id=run_id,
        retention_days=int(resume_cfg.get("retention_days", 7)),
        app_dir=get_app_dir(),
        root_dir=_resume_root_dir(config),
        index_file=_resume_index_file(config),
        runtime_state_root=_runtime_state_root(config),
    )


def _collect_retryable_file_items(checkpoint: Dict[str, Any]) -> List[Dict[str, Any]]:
    return collect_retryable_file_items_store(checkpoint)


def _normalize_runtime_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return normalize_runtime_config_runtime(config, extract_site_host=_extract_site_host)


def _validate_runtime_config(config: Dict[str, Any]) -> None:
    validate_runtime_config_runtime(
        config,
        extract_site_host=_extract_site_host,
        parse_hms_text=_parse_hms_text,
    )


def _resolve_site_urls(site: Dict[str, Any]) -> List[str]:
    return resolve_site_urls_runtime(site)


def _resolve_run_save_dir(download_cfg: Dict[str, Any]) -> str:
    return resolve_run_save_dir_runtime(download_cfg)


def _build_time_range(download_cfg: Dict[str, Any]) -> tuple[str, str]:
    return build_time_range_policy(download_cfg, get_last_month_window=get_last_month_window, emit_log=print)


def _normalize_selected_dates(selected_dates: List[str], max_dates_per_run: int) -> List[str]:
    return normalize_selected_dates_policy(selected_dates, max_dates_per_run)


def _get_multi_date_max(download_cfg: Dict[str, Any]) -> int:
    return get_multi_date_max_runtime(download_cfg, default_value=31)


def _is_retryable_download_timeout(error_text: str) -> bool:
    return is_retryable_download_timeout_runtime(error_text)


async def _download_site_with_retry(
    context,
    download_cfg: Dict[str, Any],
    perf_cfg: Dict[str, Any],
    site: Dict[str, Any],
    start_time: str,
    end_time: str,
    page=None,
) -> DownloadOutcome:
    return await download_site_with_retry_runtime(
        context=context,
        download_cfg=download_cfg,
        perf_cfg=perf_cfg,
        site=site,
        start_time=start_time,
        end_time=end_time,
        page=page,
        resolve_site_urls=_resolve_site_urls,
        is_retryable_download_timeout=_is_retryable_download_timeout,
        emit_log=print,
    )


def _group_download_tasks_by_building(download_tasks: List[DownloadTask]) -> List[Tuple[str, List[DownloadTask]]]:
    grouped = group_download_tasks_by_building_runtime(download_tasks)
    return [(str(building), list(task_items)) for building, task_items in grouped]


async def _run_download_tasks_by_building(
    config: Dict[str, Any],
    download_tasks: List[DownloadTask],
    feature: str,
    success_stage: str,
    failure_stage: str,
    success_detail_prefix: str = "下载成功 URL=",
) -> List[Tuple[DownloadTask, DownloadOutcome]]:
    pairs = await run_download_tasks_by_building_runtime(
        config=config,
        download_tasks=download_tasks,
        feature=feature,
        success_stage=success_stage,
        failure_stage=failure_stage,
        success_detail_prefix=success_detail_prefix,
        group_download_tasks_by_building=_group_download_tasks_by_building,
        download_site_with_retry=_download_site_with_retry,
        log_file_success=_log_file_success,
        log_file_failure=_log_file_failure,
    )
    return [(task, outcome) for task, outcome in pairs]


async def _retry_failed_download_tasks(
    config: Dict[str, Any],
    failed_tasks: List[DownloadTask],
    source_name: str,
) -> List[Tuple[DownloadTask, DownloadOutcome]]:
    results = await retry_failed_download_tasks_runtime(
        config=config,
        failed_tasks=failed_tasks,
        source_name=source_name,
        run_download_tasks_by_building=_run_download_tasks_by_building,
    )
    return [(task, outcome) for task, outcome in results]


def _build_checkpoint(
    source_name: str,
    run_save_dir: str,
    selected_dates: List[str],
    run_id: str | None = None,
) -> Dict[str, Any]:
    return build_checkpoint_store(
        source_name=source_name,
        run_save_dir=run_save_dir,
        selected_dates=selected_dates,
        run_id=run_id,
    )


def _sync_summary_from_checkpoint(summary: Dict[str, Any], checkpoint: Dict[str, Any]) -> None:
    sync_summary_from_checkpoint_store(summary, checkpoint)


def _upload_retryable_items(
    config: Dict[str, Any],
    calc_module,
    checkpoint: Dict[str, Any],
    gc_every_n_items: int,
    upload_chunk_threshold: int,
    upload_chunk_size: int,
) -> Dict[str, Any]:
    return upload_retryable_items_runtime(
        config=config,
        calc_module=calc_module,
        checkpoint=checkpoint,
        gc_every_n_items=gc_every_n_items,
        upload_chunk_threshold=upload_chunk_threshold,
        upload_chunk_size=upload_chunk_size,
        collect_retryable_file_items=_collect_retryable_file_items,
        now_text=_now_text,
        save_checkpoint_and_index=_save_checkpoint_and_index,
        log_file_failure=_log_file_failure,
        refresh_checkpoint_summary=_refresh_checkpoint_summary,
        gc_collect=gc.collect,
    )


def _run_pipeline_with_time_windows(
    config: Dict[str, Any],
    time_windows: List[Dict[str, str]],
    source_name: str = "自动流程",
    force_explicit_files: bool = False,
) -> Dict[str, Any]:
    return run_pipeline_with_time_windows_runtime(
        config=config,
        time_windows=time_windows,
        source_name=source_name,
        force_explicit_files=force_explicit_files,
        normalize_runtime_config=_normalize_runtime_config,
        validate_runtime_config=_validate_runtime_config,
        configure_playwright_environment=configure_playwright_environment,
        load_calc_module=load_calc_module,
        resolve_run_save_dir=_resolve_run_save_dir,
        build_checkpoint=_build_checkpoint,
        save_checkpoint_and_index=_save_checkpoint_and_index,
        sync_summary_from_checkpoint=_sync_summary_from_checkpoint,
        build_wifi_switcher=build_wifi_switcher_runtime,
        try_switch_wifi=try_switch_wifi_runtime,
        build_time_window_download_tasks=build_time_window_download_tasks_runtime,
        run_download_tasks_by_building=_run_download_tasks_by_building,
        retry_failed_download_tasks=_retry_failed_download_tasks,
        collect_first_pass_results=collect_first_pass_results_runtime,
        merge_retry_results=merge_retry_results_runtime,
        apply_download_outcomes=apply_download_outcomes_runtime,
        flush_pending_notify_events=_flush_pending_notify_events,
        notify_event=_notify_event,
        log_file_failure=_log_file_failure,
        upload_retryable_items=_upload_retryable_items,
        task_factory=DownloadTask,
        emit_log=print,
    )


class _BridgeCalcModuleNoop:
    def run_with_explicit_files(self, **_kwargs) -> Dict[str, Any]:
        return {}

    def run_with_explicit_file_items(self, **_kwargs) -> Dict[str, Any]:
        return {}


def _run_download_only_with_time_windows(
    config: Dict[str, Any],
    time_windows: List[Dict[str, str]],
    source_name: str = "共享桥接月报下载",
    force_explicit_files: bool = False,
) -> Dict[str, Any]:
    cfg = copy.deepcopy(config)
    feishu_cfg = cfg.get("feishu", {})
    if not isinstance(feishu_cfg, dict):
        feishu_cfg = {}
        cfg["feishu"] = feishu_cfg
    feishu_cfg["enable_upload"] = False
    return run_pipeline_with_time_windows_runtime(
        config=cfg,
        time_windows=time_windows,
        source_name=source_name,
        force_explicit_files=force_explicit_files,
        normalize_runtime_config=_normalize_runtime_config,
        validate_runtime_config=_validate_runtime_config,
        configure_playwright_environment=configure_playwright_environment,
        load_calc_module=lambda: _BridgeCalcModuleNoop(),
        resolve_run_save_dir=_resolve_run_save_dir,
        build_checkpoint=_build_checkpoint,
        save_checkpoint_and_index=_save_checkpoint_and_index,
        sync_summary_from_checkpoint=_sync_summary_from_checkpoint,
        build_wifi_switcher=build_wifi_switcher_runtime,
        try_switch_wifi=try_switch_wifi_runtime,
        build_time_window_download_tasks=build_time_window_download_tasks_runtime,
        run_download_tasks_by_building=_run_download_tasks_by_building,
        retry_failed_download_tasks=_retry_failed_download_tasks,
        collect_first_pass_results=collect_first_pass_results_runtime,
        merge_retry_results=merge_retry_results_runtime,
        apply_download_outcomes=apply_download_outcomes_runtime,
        flush_pending_notify_events=_flush_pending_notify_events,
        notify_event=_notify_event,
        log_file_failure=_log_file_failure,
        upload_retryable_items=_upload_retryable_items,
        task_factory=DownloadTask,
        emit_log=print,
    )


def run_resume_upload(config: Dict[str, Any], run_id: str | None = None, auto_trigger: bool = False) -> Dict[str, Any]:
    return run_resume_upload_runtime(
        config=config,
        run_id=run_id,
        auto_trigger=auto_trigger,
        normalize_runtime_config=_normalize_runtime_config,
        validate_runtime_config=_validate_runtime_config,
        ensure_resume_config=_ensure_resume_config,
        load_pending_checkpoint=_load_pending_checkpoint,
        sync_summary_from_checkpoint=_sync_summary_from_checkpoint,
        save_checkpoint_and_index=_save_checkpoint_and_index,
        load_calc_module=load_calc_module,
        build_wifi_switcher=build_wifi_switcher_runtime,
        try_switch_wifi=try_switch_wifi_runtime,
        upload_retryable_items=_upload_retryable_items,
        notify_event=_notify_event,
        log_file_failure=_log_file_failure,
        emit_log=print,
    )


def run_with_selected_dates(config: Dict[str, Any], selected_dates: List[str]) -> Dict[str, Any]:
    cfg = copy.deepcopy(config)
    cfg = _normalize_runtime_config(cfg)
    _validate_runtime_config(cfg)

    download_cfg = cfg["download"]
    max_dates_per_run = _get_multi_date_max(download_cfg)
    normalized_dates = _normalize_selected_dates(selected_dates, max_dates_per_run=max_dates_per_run)
    windows: List[Dict[str, str]] = []
    for day_text in normalized_dates:
        start = datetime.strptime(day_text, "%Y-%m-%d")
        end = start + timedelta(days=1)
        windows.append(
            {
                "date": day_text,
                "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    print(f"[多日期] 已选择 {len(windows)} 天: {', '.join(normalized_dates)}")
    return _run_pipeline_with_time_windows(
        cfg,
        windows,
        source_name="多日期手动流程",
        force_explicit_files=True,
    )


def run_download_only_auto_once(
    config: Dict[str, Any],
    *,
    source_name: str = "共享桥接月报下载",
) -> Dict[str, Any]:
    cfg = copy.deepcopy(config)
    cfg = _normalize_runtime_config(cfg)
    _validate_runtime_config(cfg)
    download_cfg = cfg["download"]
    start_time, end_time = _build_time_range(download_cfg)
    date_text = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
    return _run_download_only_with_time_windows(
        cfg,
        [
            {
                "date": date_text,
                "start_time": start_time,
                "end_time": end_time,
            }
        ],
        source_name=source_name,
    )


def run_download_only_with_selected_dates(
    config: Dict[str, Any],
    *,
    selected_dates: List[str],
    source_name: str = "共享桥接月报多日期下载",
) -> Dict[str, Any]:
    cfg = copy.deepcopy(config)
    cfg = _normalize_runtime_config(cfg)
    _validate_runtime_config(cfg)

    download_cfg = cfg["download"]
    max_dates_per_run = _get_multi_date_max(download_cfg)
    normalized_dates = _normalize_selected_dates(selected_dates, max_dates_per_run=max_dates_per_run)
    windows: List[Dict[str, str]] = []
    for day_text in normalized_dates:
        start = datetime.strptime(day_text, "%Y-%m-%d")
        end = start + timedelta(days=1)
        windows.append(
            {
                "date": day_text,
                "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    print(f"[共享桥接-多日期] 已选择 {len(windows)} 天: {', '.join(normalized_dates)}")
    return _run_download_only_with_time_windows(
        cfg,
        windows,
        source_name=source_name,
        force_explicit_files=True,
    )


def main() -> None:
    config = load_pipeline_config()
    config = _normalize_runtime_config(config)
    _validate_runtime_config(config)
    download_cfg = config["download"]
    start_time, end_time = _build_time_range(download_cfg)
    date_text = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
    _run_pipeline_with_time_windows(
        config,
        [
            {
                "date": date_text,
                "start_time": start_time,
                "end_time": end_time,
            }
        ],
        source_name="自动流程",
    )


if __name__ == "__main__":
    main()


