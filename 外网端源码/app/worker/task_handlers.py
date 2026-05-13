from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Tuple

from app.modules.notify.service.webhook_notify_service import WebhookNotifyService
from app.modules.report_pipeline.service.calculation_service import CalculationService
from app.modules.report_pipeline.service.monthly_cache_continue_service import run_monthly_from_file_items
from app.modules.report_pipeline.service.orchestrator_service import OrchestratorService
from app.modules.sheet_import.service.sheet_import_service import SheetImportService
from app.modules.shared_bridge.service import shared_bridge_runtime_service as shared_bridge_runtime_module
from app.shared.utils.runtime_temp_workspace import cleanup_runtime_temp_dir
from handover_log_module.service.day_metric_standalone_upload_service import DayMetricStandaloneUploadService
from handover_log_module.api.facade import load_handover_config
from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore
from handover_log_module.service.handover_xlsx_write_queue_service import HandoverXlsxWriteQueueService
from handover_log_module.service.branch_power_upload_service import BranchPowerUploadService
from handover_log_module.service.review_document_state_service import ReviewDocumentStateService
from handover_log_module.service.review_session_service import ReviewSessionService
from handover_log_module.service.wet_bulb_collection_service import WetBulbCollectionService
from pipeline_utils import get_app_dir


def _cleanup_temp_dir(config: Dict[str, Any], payload: Dict[str, Any]) -> None:
    cleanup_dir = str(payload.get("cleanup_dir", "") or "").strip()
    if not cleanup_dir:
        return
    cleanup_runtime_temp_dir(Path(cleanup_dir), runtime_config=config, app_dir=get_app_dir())


def _review_routes():
    from app.modules.handover_review.api import routes as review_routes

    return review_routes


def _review_container(config: Dict[str, Any], emit_log: Callable[[str], None]):
    return SimpleNamespace(
        runtime_config=config,
        add_system_log=emit_log,
        config=config,
        config_path="",
        reload_config=lambda _cfg: None,
    )


def handle_handover_from_download(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        if bool(payload.get("skip_if_batch_fully_generated_and_sent", False)):
            duty_date = str(payload.get("duty_date", "") or "").strip()
            duty_shift = str(payload.get("duty_shift", "") or "").strip().lower()
            if duty_date and duty_shift:
                building_list = [
                    str(item or "").strip()
                    for item in (payload.get("buildings") if isinstance(payload.get("buildings"), list) else [])
                    if str(item or "").strip()
                ]
                try:
                    review_service = ReviewSessionService(load_handover_config(config))
                    completion = review_service.batch_generation_and_review_links_completed(
                        duty_date=duty_date,
                        duty_shift=duty_shift,
                        buildings=building_list,
                    )
                    if bool(completion.get("complete", False)):
                        emit_log(
                            "[交接班调度] 后台任务恢复前检测到本班已全量完成，跳过执行: "
                            f"duty_date={duty_date}, duty_shift={duty_shift}, "
                            f"buildings={','.join(completion.get('target_buildings', []) or building_list)}"
                        )
                        return {
                            "status": "skipped",
                            "reason": "batch_fully_generated_and_review_links_sent",
                            "duty_date": duty_date,
                            "duty_shift": duty_shift,
                            "buildings": completion.get("target_buildings", []) or building_list,
                            "completion": completion,
                        }
                except Exception as exc:  # noqa: BLE001
                    emit_log(f"[交接班调度] 检查本班全量完成状态失败，继续执行: {exc}")
        orchestrator = OrchestratorService(config)
        if str(payload.get("resume_kind", "") or "").strip() == "shared_bridge_handover":
            raw_items = list(payload.get("building_files") or [])
            building_files: List[Tuple[str, str]] = []
            for item in raw_items:
                if not isinstance(item, dict):
                    continue
                building = str(item.get("building", "") or "").strip()
                file_path = str(item.get("file_path", "") or "").strip()
                if building and file_path:
                    building_files.append((building, file_path))
            raw_capacity_items = list(payload.get("capacity_building_files") or [])
            capacity_building_files: List[Tuple[str, str]] = []
            for item in raw_capacity_items:
                if not isinstance(item, dict):
                    continue
                building = str(item.get("building", "") or "").strip()
                file_path = str(item.get("file_path", "") or "").strip()
                if building and file_path:
                    capacity_building_files.append((building, file_path))
            return orchestrator.run_handover_from_files(
                building_files=building_files,
                capacity_building_files=capacity_building_files,
                end_time=payload.get("end_time"),
                duty_date=payload.get("duty_date"),
                duty_shift=payload.get("duty_shift"),
                emit_log=emit_log,
            )
        result = orchestrator.run_handover_from_download(
            buildings=payload.get("buildings"),
            end_time=payload.get("end_time"),
            duty_date=payload.get("duty_date"),
            duty_shift=payload.get("duty_shift"),
            emit_log=emit_log,
        )
        failure_summary = orchestrator.build_handover_download_failure_summary(result)
        if failure_summary:
            emit_log(
                "[交接班下载] 失败汇总告警: "
                f"buildings={str(failure_summary.get('building', '') or '-').strip() or '-'}, "
                f"detail={str(failure_summary.get('detail', '') or '-').strip() or '-'}"
            )
            notify.send_failure(
                stage="交接班日志（内网下载）",
                detail=str(failure_summary.get("detail", "") or "").strip() or "交接班内网下载存在失败楼栋",
                building=str(failure_summary.get("building", "") or "").strip() or None,
                emit_log=emit_log,
            )
        return result
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(stage="交接班日志（内网下载）", detail=str(exc), emit_log=emit_log)
        raise


def handle_day_metric_from_download(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        if str(payload.get("resume_kind", "") or "").strip() == "shared_bridge_day_metric":
            service = DayMetricStandaloneUploadService(config)
            return service.continue_from_source_files(
                selected_dates=list(payload.get("selected_dates") or []),
                buildings=list(payload.get("buildings") or []),
                source_units=list(payload.get("source_units") or []),
                building_scope=str(payload.get("building_scope", "") or "").strip() or "all_enabled",
                building=str(payload.get("building", "") or "").strip() or None,
                emit_log=emit_log,
            )
        orchestrator = OrchestratorService(config)
        return orchestrator.run_day_metric_from_download(
            selected_dates=list(payload.get("selected_dates") or []),
            building_scope=str(payload.get("building_scope", "") or "").strip(),
            building=str(payload.get("building", "") or "").strip() or None,
            emit_log=emit_log,
        )
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(
            stage="12项独立上传（内网下载）",
            detail=str(exc),
            building=str(payload.get("building", "") or "").strip() or None,
            emit_log=emit_log,
        )
        raise


def handle_branch_power_from_download(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    service = BranchPowerUploadService(config)
    source_units = [item for item in list(payload.get("source_units") or []) if isinstance(item, dict)]
    target_business_date = str(
        payload.get("target_business_date", "")
        or payload.get("business_date", "")
        or ""
    ).strip()
    if not target_business_date:
        target_business_date = str(payload.get("target_bucket_key", "") or payload.get("bucket_key", "") or "").strip()
    if not target_business_date:
        raise RuntimeError("支路信息整日恢复任务缺少 target_business_date")
    target_business_date = target_business_date.replace("/", "-")[:10]
    try:
        target_business_date = datetime.strptime(target_business_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise RuntimeError("支路信息整日恢复任务 target_business_date 格式必须为 YYYY-MM-DD") from exc
    if not source_units:
        raise RuntimeError("支路信息整日恢复任务缺少共享源文件")
    return service.upload_day_from_source_files(
        business_date=target_business_date,
        source_units=source_units,
        emit_log=emit_log,
    )


def handle_wet_bulb_collection_run(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        if str(payload.get("resume_kind", "") or "").strip() == "shared_bridge_wet_bulb":
            service = WetBulbCollectionService(config)
            return service.continue_from_source_units(
                source_units=list(payload.get("source_units") or []),
                emit_log=emit_log,
            )
        orchestrator = OrchestratorService(config)
        source = str(payload.get("source", "") or "").strip() or "湿球温度定时采集"
        return orchestrator.run_wet_bulb_collection(emit_log=emit_log, source=source)
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(stage="湿球温度定时采集", detail=str(exc), emit_log=emit_log)
        raise


def handle_auto_once(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    source = str(payload.get("source", "") or "").strip() or "立即执行自动流程"
    try:
        if str(payload.get("resume_kind", "") or "").strip() == "shared_bridge_monthly_auto_once":
            return run_monthly_from_file_items(
                config,
                file_items=list(payload.get("file_items") or []),
                emit_log=emit_log,
                source_label=source,
            )
        orchestrator = OrchestratorService(config)
        return orchestrator.run_auto_once(emit_log, source=source)
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(stage=source, detail=str(exc), emit_log=emit_log)
        raise


def handle_multi_date(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    selected_dates = [str(item or "").strip() for item in list(payload.get("selected_dates") or []) if str(item or "").strip()]
    try:
        if str(payload.get("resume_kind", "") or "").strip() == "shared_bridge_monthly_multi_date":
            return run_monthly_from_file_items(
                config,
                file_items=list(payload.get("file_items") or []),
                emit_log=emit_log,
                source_label=str(payload.get("source_label", "") or "月报历史共享文件").strip() or "月报历史共享文件",
            )
        orchestrator = OrchestratorService(config)
        return orchestrator.run_multi_date(selected_dates, emit_log)
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(stage="多日期自动流程", detail=str(exc), emit_log=emit_log)
        raise


def handle_alarm_event_upload(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    mode = str(payload.get("mode", "") or "").strip().lower() or "full"
    building = str(payload.get("building", "") or "").strip()
    runtime = shared_bridge_runtime_module.SharedBridgeRuntimeService(
        runtime_config=config,
        app_version="worker",
        emit_log=emit_log,
    )
    try:
        if mode == "single_building":
            result = runtime.upload_alarm_event_source_cache_single_building_to_bitable(
                building=building,
                emit_log=emit_log,
            )
        else:
            result = runtime.upload_alarm_event_source_cache_full_to_bitable(emit_log=emit_log)
        accepted = bool(result.get("accepted"))
        reason = str(result.get("reason", "") or "").strip().lower()
        if not accepted:
            raise RuntimeError(str(result.get("error", "") or "").strip() or "告警信息上传失败")
        if reason == "partial_completed":
            failed_entries = ", ".join(
                str(item or "").strip()
                for item in (result.get("failed_entries", []) if isinstance(result.get("failed_entries", []), list) else [])
                if str(item or "").strip()
            )
            raise RuntimeError(f"存在失败楼栋，请查看日志{f'：{failed_entries}' if failed_entries else ''}")
        return result
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(
            stage="告警信息上传",
            detail=str(exc),
            building=building or None,
            emit_log=emit_log,
        )
        raise
    finally:
        try:
            runtime.stop()
        except Exception:
            pass


def handle_resume_upload(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        orchestrator = OrchestratorService(config)
        return orchestrator.run_resume_upload(
            emit_log=emit_log,
            run_id=str(payload.get("run_id", "") or "").strip() or None,
            auto_trigger=bool(payload.get("auto_trigger", False)),
        )
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(stage="断点续传", detail=str(exc), emit_log=emit_log)
        raise


def handle_manual_upload(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        service = CalculationService(config)
        return service.run_manual_upload(
            building=str(payload.get("building", "") or "").strip(),
            file_path=str(payload.get("file_path", "") or "").strip(),
            upload_date=str(payload.get("upload_date", "") or "").strip(),
            switch_external_before_upload=bool(payload.get("switch_external_before_upload", False)),
            emit_log=emit_log,
        )
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(
            stage="手动补传",
            detail=str(exc),
            building=str(payload.get("building", "") or "").strip() or None,
            emit_log=emit_log,
        )
        raise
    finally:
        _cleanup_temp_dir(config, payload)


def handle_handover_from_file(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        orchestrator = OrchestratorService(config)
        return orchestrator.run_handover_from_file(
            building=str(payload.get("building", "") or "").strip(),
            file_path=str(payload.get("file_path", "") or "").strip(),
            capacity_source_file=str(payload.get("capacity_source_file", "") or "").strip() or None,
            end_time=str(payload.get("end_time", "") or "").strip() or None,
            duty_date=str(payload.get("duty_date", "") or "").strip() or None,
            duty_shift=str(payload.get("duty_shift", "") or "").strip().lower() or None,
            emit_log=emit_log,
        )
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(
            stage="交接班日志（已有文件）",
            detail=str(exc),
            building=str(payload.get("building", "") or "").strip() or None,
            emit_log=emit_log,
        )
        raise
    finally:
        _cleanup_temp_dir(config, payload)


def handle_handover_from_files(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    raw_items = list(payload.get("building_files") or [])
    building_files: List[Tuple[str, str]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        building = str(item.get("building", "") or "").strip()
        file_path = str(item.get("file_path", "") or "").strip()
        if building and file_path:
            building_files.append((building, file_path))
    raw_capacity_items = list(payload.get("capacity_building_files") or [])
    capacity_building_files: List[Tuple[str, str]] = []
    for item in raw_capacity_items:
        if not isinstance(item, dict):
            continue
        building = str(item.get("building", "") or "").strip()
        file_path = str(item.get("file_path", "") or "").strip()
        if building and file_path:
            capacity_building_files.append((building, file_path))
    try:
        orchestrator = OrchestratorService(config)
        return orchestrator.run_handover_from_files(
            building_files=building_files,
            capacity_building_files=capacity_building_files,
            end_time=str(payload.get("end_time", "") or "").strip() or None,
            duty_date=str(payload.get("duty_date", "") or "").strip() or None,
            duty_shift=str(payload.get("duty_shift", "") or "").strip().lower() or None,
            emit_log=emit_log,
        )
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(
            stage="交接班日志（已有文件批量）",
            detail=str(exc),
            building=",".join([item[0] for item in building_files]) or None,
            emit_log=emit_log,
        )
        raise
    finally:
        _cleanup_temp_dir(config, payload)


def handle_handover_review_regenerate(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    def _text(value: Any) -> str:
        return str(value or "").strip()

    building = _text(payload.get("building"))
    session_id = _text(payload.get("session_id"))
    duty_date = _text(payload.get("duty_date"))
    duty_shift = _text(payload.get("duty_shift")).lower()
    data_file = _text(payload.get("data_file"))
    capacity_source_file = _text(payload.get("capacity_source_file"))
    client_id = _text(payload.get("client_id"))
    job_id = _text(payload.get("job_id"))
    if runtime is not None:
        runtime.raise_if_cancelled()
    if not building or not session_id or not duty_date or duty_shift not in {"day", "night"}:
        raise RuntimeError("重新生成参数不完整")
    if not data_file or not Path(data_file).exists():
        raise RuntimeError(f"交接班源文件不存在: {data_file or '-'}")
    if not capacity_source_file or not Path(capacity_source_file).exists():
        raise RuntimeError(f"交接班容量报表源文件不存在: {capacity_source_file or '-'}")

    handover_cfg = load_handover_config(config)
    review_service = ReviewSessionService(handover_cfg)
    document_state = ReviewDocumentStateService(handover_cfg, emit_log=emit_log)
    queue_service = HandoverXlsxWriteQueueService(
        handover_cfg,
        review_service=review_service,
        document_state=document_state,
        emit_log=emit_log,
    )
    emit_log(
        "[交接班][审核重生成] 开始 "
        f"building={building}, session_id={session_id}, duty={duty_date}/{duty_shift}, "
        f"source={data_file}, capacity_source={capacity_source_file}"
    )
    queue_service.wait_for_barrier(
        building=building,
        session_id=session_id,
        reason="review_regenerate_before",
        timeout_sec=120,
    )
    if runtime is not None:
        runtime.raise_if_cancelled()

    result = OrchestratorService(config).run_handover_from_files(
        building_files=[(building, data_file)],
        capacity_building_files=[(building, capacity_source_file)],
        end_time=None,
        duty_date=duty_date,
        duty_shift=duty_shift,
        auto_send_review_link=False,
        emit_log=emit_log,
    )
    rows = result.get("results", []) if isinstance(result.get("results", []), list) else []
    row = next((item for item in rows if isinstance(item, dict) and _text(item.get("building")) == building), None)
    if not isinstance(row, dict) or not bool(row.get("success", False)):
        errors = row.get("errors", []) if isinstance(row, dict) and isinstance(row.get("errors", []), list) else []
        error_text = "; ".join([_text(item) for item in errors if _text(item)]) or _text(result.get("errors")) or "重新生成失败"
        raise RuntimeError(error_text)
    review_session = row.get("review_session", {}) if isinstance(row.get("review_session", {}), dict) else {}
    regenerated_session_id = _text(review_session.get("session_id")) or session_id
    latest_session = review_service.get_session_by_id(regenerated_session_id)
    if not isinstance(latest_session, dict):
        raise RuntimeError("重新生成后未找到审核会话")
    if runtime is not None:
        runtime.raise_if_cancelled()

    ReviewBuildingDocumentStore(config=handover_cfg, building=building).delete_document(regenerated_session_id)
    document_state.ensure_document_for_session(latest_session)
    queue_service.enqueue_capacity_overlay_sync(latest_session, client_id=client_id)
    barrier = queue_service.wait_for_barrier(
        building=building,
        session_id=regenerated_session_id,
        reason="review_regenerate_capacity_overlay",
        timeout_sec=120,
    )
    if _text(barrier.get("status")).lower() != "success":
        raise RuntimeError(_text(barrier.get("error")) or "重新生成后容量表补写失败")
    latest_session = review_service.mark_manual_regenerated(
        building=building,
        duty_date=duty_date,
        duty_shift=duty_shift,
        job_id=job_id,
        client_id=client_id,
    )
    emit_log(
        "[交接班][审核重生成] 完成 "
        f"building={building}, session_id={regenerated_session_id}, "
        f"output={latest_session.get('output_file', '-')}, capacity={latest_session.get('capacity_output_file', '-')}"
    )
    return {
        "ok": True,
        "status": "success",
        "building": building,
        "session_id": regenerated_session_id,
        "revision": int(latest_session.get("revision", 0) or 0),
        "output_file": _text(latest_session.get("output_file")),
        "capacity_output_file": _text(latest_session.get("capacity_output_file")),
        "manual_regenerated": True,
        "result": result,
    }


def handle_day_metric_from_file(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        orchestrator = OrchestratorService(config)
        return orchestrator.run_day_metric_from_file(
            building=str(payload.get("building", "") or "").strip(),
            duty_date=str(payload.get("duty_date", "") or "").strip(),
            file_path=str(payload.get("file_path", "") or "").strip(),
            emit_log=emit_log,
        )
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(
            stage="12项独立上传（本地补录）",
            detail=str(exc),
            building=str(payload.get("building", "") or "").strip() or None,
            emit_log=emit_log,
        )
        raise


def handle_sheet_import(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        service = SheetImportService(config)
        result = service.run(
            str(payload.get("xlsx_path", "") or "").strip(),
            bool(payload.get("switch_external_before_upload", False)),
            emit_log,
        )
        if int(result.get("failed_count", 0)) > 0:
            notify.send_failure(stage="5Sheet导表", detail="存在失败Sheet，详情请查看日志", emit_log=emit_log)
        return result
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(stage="5Sheet导表", detail=str(exc), emit_log=emit_log)
        raise
    finally:
        _cleanup_temp_dir(config, payload)


def handle_day_metric_retry_unit(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        if runtime is not None:
            runtime.raise_if_cancelled()
        orchestrator = OrchestratorService(config)
        return orchestrator.retry_day_metric_unit(
            mode=str(payload.get("mode", "") or "").strip().lower(),
            duty_date=str(payload.get("duty_date", "") or "").strip(),
            building=str(payload.get("building", "") or "").strip(),
            source_file=str(payload.get("source_file", "") or "").strip() or None,
            stage=str(payload.get("stage", "") or "").strip().lower() or None,
            emit_log=emit_log,
        )
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(
            stage="day_metric_retry_unit",
            detail=str(exc),
            building=str(payload.get("building", "") or "").strip() or None,
            emit_log=emit_log,
        )
        raise


def handle_day_metric_retry_failed(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        if runtime is not None:
            runtime.raise_if_cancelled()
        orchestrator = OrchestratorService(config)
        return orchestrator.retry_day_metric_failed(
            mode=str(payload.get("mode", "") or "").strip().lower() or None,
            emit_log=emit_log,
        )
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(stage="day_metric_retry_failed", detail=str(exc), emit_log=emit_log)
        raise


def handle_handover_followup_continue(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    notify = WebhookNotifyService(config)
    try:
        if runtime is not None:
            runtime.raise_if_cancelled()
        orchestrator = OrchestratorService(config)
        return orchestrator.run_handover_followup_continue(
            batch_key=str(payload.get("batch_key", "") or "").strip(),
            building=str(payload.get("building", "") or "").strip(),
            session_id=str(payload.get("session_id", "") or "").strip(),
            emit_log=emit_log,
        )
    except Exception as exc:  # noqa: BLE001
        notify.send_failure(stage="handover_followup_continue", detail=str(exc), emit_log=emit_log)
        raise


def handle_handover_confirm_all(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    routes = _review_routes()
    container = _review_container(config, emit_log)
    batch_key = str(payload.get("batch_key", "") or "").strip()
    if runtime is not None:
        runtime.raise_if_cancelled()
    service, _, _, followup = routes._build_review_services(container)
    emit_log(f"[交接班][审核一键全确认] 开始 batch={batch_key}")
    updated_sessions, batch_status = service.confirm_all_in_batch(batch_key=batch_key)
    emit_log(
        f"[交接班][审核一键全确认] batch={batch_key}, sessions={len(updated_sessions)}, all_confirmed={bool(batch_status.get('all_confirmed', False))}"
    )
    try:
        followup_result = routes._start_handover_followup_job_after_confirm(
            container,
            batch_key=batch_key,
            submitted_by="confirm_all",
        )
    except Exception as exc:  # noqa: BLE001
        emit_log(f"[交接班][确认后上传] 任务提交失败 batch={batch_key}, error={exc}")
        followup_result = routes._build_followup_failure_result(followup, batch_key=batch_key, error=str(exc))
    emit_log(
        f"[交接班][确认后上传] batch={batch_key}, status={followup_result.get('status', '-')}, "
        f"uploaded={len(followup_result.get('uploaded_buildings', []))}, failed={len(followup_result.get('failed_buildings', []))}, "
        f"cloud_status={followup_result.get('cloud_sheet_sync', {}).get('status', '-')}"
    )
    refreshed_batch_status = routes._attach_followup_progress(followup, service.get_batch_status(batch_key))
    refreshed_sessions = service.list_batch_sessions(batch_key)
    return {
        "ok": True,
        "updated_sessions": refreshed_sessions or updated_sessions,
        "batch_status": refreshed_batch_status or routes._attach_followup_progress(followup, batch_status),
        "followup_result": followup_result,
    }


def handle_daily_report_auth_open(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    duty_date = str(payload.get("duty_date", "") or "").strip()
    duty_shift = str(payload.get("duty_shift", "") or "").strip().lower()
    if runtime is not None:
        runtime.raise_if_cancelled()
    emit_log("[交接班][日报截图] 单截图公开页面模式，无需初始化飞书截图登录态")
    return {
        "ok": True,
        "status": "skipped",
        "message": "单截图公开页面模式，无需初始化飞书截图登录态",
        "profile_dir": "",
        "duty_date": duty_date,
        "duty_shift": duty_shift,
    }


def handle_daily_report_screenshot_test(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    routes = _review_routes()
    container = _review_container(config, emit_log)
    duty_date = str(payload.get("duty_date", "") or "").strip()
    duty_shift = str(payload.get("duty_shift", "") or "").strip().lower()
    batch_key = f"{duty_date}|{duty_shift}"
    if runtime is not None:
        runtime.raise_if_cancelled()
    review_service, _state_service, asset_service, screenshot_service = routes._build_daily_report_services(container)
    cloud_batch = review_service.get_cloud_batch(batch_key) or {}
    spreadsheet_url = str(cloud_batch.get("spreadsheet_url", "")).strip() if isinstance(cloud_batch, dict) else ""
    summary_result = screenshot_service.capture_daily_report_page(
        duty_date=duty_date,
        duty_shift=duty_shift,
        emit_log=emit_log,
    )
    overall_status = "ok" if str(summary_result.get("status", "")).strip().lower() in {"ok", "skipped"} else "failed"
    return {
        "ok": overall_status != "failed",
        "status": overall_status,
        "batch_key": batch_key,
        "spreadsheet_url": spreadsheet_url,
        "summary_sheet_image": summary_result,
        "capture_assets": asset_service.get_capture_assets_context(duty_date=duty_date, duty_shift=duty_shift),
    }


def handle_daily_report_recapture(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    routes = _review_routes()
    container = _review_container(config, emit_log)
    duty_date = str(payload.get("duty_date", "") or "").strip()
    duty_shift = str(payload.get("duty_shift", "") or "").strip().lower()
    target = str(payload.get("target", "") or "").strip().lower()
    if runtime is not None:
        runtime.raise_if_cancelled()
    review_service, state_service, asset_service, screenshot_service = routes._build_daily_report_services(container)
    try:
        result = routes._daily_report_capture_result_payload(
            screenshot_service.capture_daily_report_page(
                duty_date=duty_date,
                duty_shift=duty_shift,
                emit_log=emit_log,
            )
        )
    except Exception as exc:  # noqa: BLE001
        result = routes._daily_report_capture_result_payload(fallback_stage="unknown", fallback_detail=str(exc))
        emit_log(
            f"[交接班][日报截图] 失败 batch={duty_date}|{duty_shift}, target={target}, "
            f"stage={result['stage']}, status={result['status']}, error={result['error_detail'] or result['error']}"
        )
    if str(result.get("status", "")).strip().lower() == "ok":
        routes._touch_daily_report_asset_rewrite_state(state_service, duty_date=duty_date, duty_shift=duty_shift)
    context = routes._build_daily_report_context_payload(
        review_service=review_service,
        state_service=state_service,
        asset_service=asset_service,
        screenshot_service=screenshot_service,
        duty_date=duty_date,
        duty_shift=duty_shift,
    )
    return {
        "ok": str(result.get("status", "")).strip().lower() == "ok",
        "target": target,
        "result": result,
        "capture_assets": context.get("capture_assets", {}),
        "daily_report_record_export": context.get("daily_report_record_export", {}),
    }


def handle_daily_report_record_rewrite(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    routes = _review_routes()
    container = _review_container(config, emit_log)
    duty_date = str(payload.get("duty_date", "") or "").strip()
    duty_shift = str(payload.get("duty_shift", "") or "").strip().lower()
    if runtime is not None:
        runtime.raise_if_cancelled()
    review_service, state_service, asset_service, screenshot_service = routes._build_daily_report_services(container)
    followup = routes.ReviewFollowupTriggerService(routes._handover_cfg(container))
    logged_failure = False
    try:
        result = followup.rewrite_daily_report_record(duty_date=duty_date, duty_shift=duty_shift, emit_log=emit_log)
    except Exception as exc:  # noqa: BLE001
        failure = routes._daily_report_failure_payload(
            fallback_error_code=str(getattr(exc, "error_code", "") or "daily_report_export_failed"),
            fallback_detail=str(getattr(exc, "error_detail", "") or str(exc)),
        )
        emit_log(
            f"[交接班][日报多维] 失败 batch={duty_date}|{duty_shift}, "
            f"code={failure['error_code'] or '-'}, error={failure['error_detail'] or failure['error']}"
        )
        logged_failure = True
        result = state_service.update_export_state(
            duty_date=duty_date,
            duty_shift=duty_shift,
            daily_report_record_export={
                **state_service.get_export_state(duty_date=duty_date, duty_shift=duty_shift),
                "status": "failed",
                "error": failure["error"],
                "error_code": failure["error_code"],
                "error_detail": failure["error_detail"],
            },
        )
    failure = routes._daily_report_failure_payload(result)
    if not logged_failure and str(result.get("status", "")).strip().lower() != "success" and (failure["error"] or failure["error_detail"]):
        emit_log(
            f"[交接班][日报多维] 失败 batch={duty_date}|{duty_shift}, "
            f"code={failure['error_code'] or '-'}, error={failure['error_detail'] or failure['error']}"
        )
    context = routes._build_daily_report_context_payload(
        review_service=review_service,
        state_service=state_service,
        asset_service=asset_service,
        screenshot_service=screenshot_service,
        duty_date=duty_date,
        duty_shift=duty_shift,
    )
    return {
        "ok": str(result.get("status", "")).strip().lower() == "success",
        "error": failure["error"],
        "error_code": failure["error_code"],
        "error_detail": failure["error_detail"],
        "daily_report_record_export": context.get("daily_report_record_export", {}),
        "capture_assets": context.get("capture_assets", {}),
    }


def handle_handover_cloud_retry_single(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    routes = _review_routes()
    container = _review_container(config, emit_log)
    building = str(payload.get("building", "") or "").strip()
    session_id = str(payload.get("session_id", "") or "").strip()
    target_batch_key = str(payload.get("batch_key", "") or "").strip()
    if runtime is not None:
        runtime.raise_if_cancelled()
    service, _, _, followup = routes._build_review_services(container)
    if session_id:
        result = followup.retry_cloud_sheet_for_session(session_id, emit_log=emit_log)
    else:
        result = followup.retry_cloud_sheet_for_building(building, emit_log=emit_log)
    session = result.get("session") if isinstance(result.get("session"), dict) else {}
    batch_status = result.get("batch_status") if isinstance(result.get("batch_status"), dict) else service.get_batch_status(target_batch_key)
    emit_log(f"[交接班][云表重试] building={building}, batch={session.get('batch_key', '-')}, status={result.get('status', '-')}")
    return {
        "ok": str(result.get("status", "")).strip().lower() in {"ok", "success"},
        "session": session,
        "batch_status": batch_status,
        "cloud_sheet_sync": result.get("cloud_sheet_sync", {}),
        "status": result.get("status", ""),
        "history": routes._build_history_payload_safe(
            service,
            building=building,
            selected_session_id=str(session.get("session_id", "")).strip(),
            emit_log=emit_log,
        ),
    }


def handle_handover_cloud_retry_batch(
    config: Dict[str, Any],
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    routes = _review_routes()
    container = _review_container(config, emit_log)
    batch_key = str(payload.get("batch_key", "") or "").strip()
    if runtime is not None:
        runtime.raise_if_cancelled()
    service, _, _, followup = routes._build_review_services(container)
    result = followup.retry_failed_cloud_sheet_in_batch(batch_key, emit_log=emit_log)
    emit_log(f"[交接班][云表批量重试] batch={batch_key}, status={result.get('status', '-')}")
    batch_status = result.get("batch_status") if isinstance(result.get("batch_status"), dict) else service.get_batch_status(batch_key)
    updated_sessions = result.get("updated_sessions") if isinstance(result.get("updated_sessions"), list) else service.list_batch_sessions(batch_key)
    status = str(result.get("status", "")).strip().lower()
    return {
        "ok": status != "blocked",
        "status": result.get("status", ""),
        "batch_key": batch_key,
        "batch_status": batch_status,
        "updated_sessions": updated_sessions,
        "cloud_sheet_sync": result.get("cloud_sheet_sync", {}),
    }


def handle_test_echo_payload(
    config: Dict[str, Any],  # noqa: ARG001
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
) -> Dict[str, Any]:
    emit_log(f"[worker-test] payload={payload}")
    return {"echo": payload, "status": "ok"}


def handle_test_sleep(
    config: Dict[str, Any],  # noqa: ARG001
    payload: Dict[str, Any],
    emit_log: Callable[[str], None],
    runtime: Any = None,  # noqa: ANN401
) -> Dict[str, Any]:
    total_seconds = max(1.0, float(payload.get("seconds", 3) or 3))
    deadline = time.monotonic() + total_seconds
    while time.monotonic() < deadline:
        if runtime is not None:
            runtime.raise_if_cancelled()
        emit_log("[worker-test] sleep_tick")
        time.sleep(0.2)
    return {"status": "slept", "seconds": total_seconds}


HANDLER_REGISTRY: Dict[str, Callable[[Dict[str, Any], Dict[str, Any], Callable[[str], None]], Dict[str, Any]]] = {
    "handover_from_download": handle_handover_from_download,
    "day_metric_from_download": handle_day_metric_from_download,
    "branch_power_from_download": handle_branch_power_from_download,
    "wet_bulb_collection_run": handle_wet_bulb_collection_run,
    "auto_once": handle_auto_once,
    "multi_date": handle_multi_date,
    "alarm_event_upload": handle_alarm_event_upload,
    "resume_upload": handle_resume_upload,
    "manual_upload": handle_manual_upload,
    "handover_from_file": handle_handover_from_file,
    "handover_from_files": handle_handover_from_files,
    "handover_review_regenerate": handle_handover_review_regenerate,
    "day_metric_from_file": handle_day_metric_from_file,
    "sheet_import": handle_sheet_import,
    "day_metric_retry_unit": handle_day_metric_retry_unit,
    "day_metric_retry_failed": handle_day_metric_retry_failed,
    "handover_followup_continue": handle_handover_followup_continue,
    "handover_confirm_all": handle_handover_confirm_all,
    "daily_report_auth_open": handle_daily_report_auth_open,
    "daily_report_screenshot_test": handle_daily_report_screenshot_test,
    "daily_report_recapture": handle_daily_report_recapture,
    "daily_report_record_rewrite": handle_daily_report_record_rewrite,
    "handover_cloud_retry_single": handle_handover_cloud_retry_single,
    "handover_cloud_retry_batch": handle_handover_cloud_retry_batch,
    "test_echo_payload": handle_test_echo_payload,
    "test_sleep": handle_test_sleep,
}
