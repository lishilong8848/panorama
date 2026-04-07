from __future__ import annotations

import copy
import time
from datetime import datetime
from typing import Any, Callable, Dict, List

from app.modules.report_pipeline.core.entities import JobResultV2, PipelinePhaseResult
from handover_log_module.api.facade import load_handover_config
from handover_log_module.api.facade import run_from_download, run_from_existing_file, run_from_existing_files
from handover_log_module.service.day_metric_standalone_upload_service import DayMetricStandaloneUploadService
from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService
from handover_log_module.service.wet_bulb_collection_service import WetBulbCollectionService
from pipeline_utils import load_download_module


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class OrchestratorService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = copy.deepcopy(config)

    @staticmethod
    def build_handover_download_failure_summary(result: Dict[str, Any]) -> Dict[str, str] | None:
        if not isinstance(result, dict):
            return None
        failed_count = int(result.get("failed_count", 0) or 0)
        if failed_count <= 0:
            return None

        success_count = int(result.get("success_count", 0) or 0)
        duty_date = str(result.get("duty_date", "") or "").strip() or "-"
        duty_shift = str(result.get("duty_shift", "") or "").strip() or "-"
        rows = result.get("results", [])
        failed_rows = [row for row in rows if isinstance(row, dict) and not bool(row.get("success", False))]

        buildings: List[str] = []
        detail_parts: List[str] = []
        for row in failed_rows:
            building = str(row.get("building", "") or "").strip() or "-"
            if building not in buildings:
                buildings.append(building)
            failed_step = str(row.get("failed_step", "") or "").strip()
            errors = row.get("errors", [])
            first_error = str(errors[0] if isinstance(errors, list) and errors else row.get("error", "")).strip() or "-"
            first_error = " ".join(first_error.split())
            if len(first_error) > 160:
                first_error = f"{first_error[:157]}..."
            if failed_step:
                detail_parts.append(f"{building}[{failed_step}: {first_error}]")
            else:
                detail_parts.append(f"{building}[{first_error}]")

        buildings_text = ",".join(buildings) if buildings else "-"
        detail_lines = [
            f"duty_date={duty_date}",
            f"duty_shift={duty_shift}",
            f"success={success_count}",
            f"failed={failed_count}",
            f"failed_buildings={buildings_text}",
        ]
        if detail_parts:
            detail_lines.append("details=" + "; ".join(detail_parts))
        return {
            "building": buildings_text,
            "detail": ", ".join(detail_lines),
        }

    def run_auto_once(self, emit_log: Callable[[str], None], source: str = "立即执行自动流程") -> Dict[str, Any]:
        started_at = _now_text()
        started_perf = time.perf_counter()
        phase_started_at = _now_text()
        phase_started_perf = time.perf_counter()

        module = load_download_module()
        emit_log(f"[{source}] 开始执行")
        module.main()
        emit_log(f"[{source}] 执行完成")

        phase = PipelinePhaseResult(
            phase="自动流程执行",
            status="success",
            started_at=phase_started_at,
            finished_at=_now_text(),
            duration_ms=int((time.perf_counter() - phase_started_perf) * 1000),
            message="内网下载 + 外网上传流程执行完成",
        )
        return JobResultV2(
            status="ok",
            summary="ok",
            phase_results=[phase],
            payload={
                "source": source,
                "duration_ms": int((time.perf_counter() - started_perf) * 1000),
                "started_at": started_at,
                "finished_at": _now_text(),
            },
        ).to_dict()

    def run_multi_date(self, selected_dates: List[str], emit_log: Callable[[str], None]) -> Dict[str, Any]:
        started_at = _now_text()
        started_perf = time.perf_counter()
        phase_started_at = _now_text()
        phase_started_perf = time.perf_counter()

        module = load_download_module()
        if not hasattr(module, "run_with_selected_dates"):
            raise RuntimeError("下载脚本缺少 run_with_selected_dates 入口")
        emit_log(f"[多日期自动流程] 开始执行，日期={','.join(sorted(set(selected_dates)))}")
        result = module.run_with_selected_dates(config=self.config, selected_dates=selected_dates)
        emit_log(
            f"[多日期自动流程] 完成：成功日期={len(result.get('success_dates', []))}，"
            f"失败日期={len(result.get('failed_dates', []))}，下载文件={len(result.get('file_items', []))}，"
            f"待续传={int(result.get('pending_upload_count', 0))}"
        )

        phase = PipelinePhaseResult(
            phase="多日期自动流程",
            status="success",
            started_at=phase_started_at,
            finished_at=_now_text(),
            duration_ms=int((time.perf_counter() - phase_started_perf) * 1000),
            message=(
                f"成功日期={len(result.get('success_dates', []))}, "
                f"失败日期={len(result.get('failed_dates', []))}, "
                f"下载文件={len(result.get('file_items', []))}, "
                f"待续传={int(result.get('pending_upload_count', 0))}"
            ),
        )
        return JobResultV2(
            status=str(result.get("status", "ok")),
            summary="ok",
            phase_results=[phase],
            payload={
                **result,
                "duration_ms": int((time.perf_counter() - started_perf) * 1000),
                "started_at": started_at,
                "finished_at": _now_text(),
            },
            retryable=bool(result.get("pending_upload_count", 0)),
        ).to_dict()

    def list_pending_resume_runs(self) -> List[Dict[str, Any]]:
        module = load_download_module()
        if not hasattr(module, "list_pending_upload_runs"):
            raise RuntimeError("下载脚本缺少 list_pending_upload_runs 入口")
        runs = module.list_pending_upload_runs(config=self.config)
        if not isinstance(runs, list):
            return []
        return runs

    def delete_resume_run(self, run_id: str) -> Dict[str, Any]:
        module = load_download_module()
        if not hasattr(module, "delete_pending_upload_run"):
            raise RuntimeError("下载脚本缺少 delete_pending_upload_run 入口")
        return module.delete_pending_upload_run(config=self.config, run_id=run_id)

    def run_resume_upload(
        self,
        emit_log: Callable[[str], None],
        run_id: str | None = None,
        auto_trigger: bool = False,
    ) -> Dict[str, Any]:
        module = load_download_module()
        if not hasattr(module, "run_resume_upload"):
            raise RuntimeError("下载脚本缺少 run_resume_upload 入口")
        emit_log(
            f"[续传] 开始执行，run_id={str(run_id or '').strip() or '-'}，"
            f"{'自动触发' if auto_trigger else '手动触发'}"
        )
        result = module.run_resume_upload(config=self.config, run_id=run_id, auto_trigger=auto_trigger)
        emit_log(
            f"[续传] 完成：pending={int(result.get('pending_upload_count', 0))}，"
            f"success={int(result.get('upload_success_count', 0))}，"
            f"failed={int(result.get('upload_failed_count', 0))}"
        )
        return result

    def run_handover_from_file(
        self,
        *,
        building: str,
        file_path: str,
        capacity_source_file: str | None = None,
        end_time: str | None,
        duty_date: str | None,
        duty_shift: str | None,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        emit_log(f"[交接班日志] 开始执行（已有文件） building={building}, file={file_path}")
        result = run_from_existing_file(
            config=self.config,
            building=building,
            data_file=file_path,
            capacity_source_file=capacity_source_file,
            end_time=end_time,
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )
        emit_log(
            f"[交接班日志] 执行完成（已有文件）: success={int(result.get('success_count', 0))}, "
            f"failed={int(result.get('failed_count', 0))}"
        )
        return result

    def run_handover_from_files(
        self,
        *,
        building_files: List[tuple[str, str]],
        capacity_building_files: List[tuple[str, str]] | None = None,
        end_time: str | None,
        duty_date: str | None,
        duty_shift: str | None,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        selected_buildings = [str(building or "").strip() for building, _ in building_files if str(building or "").strip()]
        configured_buildings = [
            str(item or "").strip()
            for item in (self.config.get("input", {}) or {}).get("buildings", [])
            if str(item or "").strip()
        ]
        emit_log(
            "[交接班日志] 开始执行（已有文件批量）: "
            f"selected={','.join(selected_buildings) or '-'}, "
            f"duty_date={str(duty_date or '-').strip() or '-'}, duty_shift={str(duty_shift or '-').strip() or '-'}"
        )
        result = run_from_existing_files(
            config=self.config,
            building_files=building_files,
            capacity_building_files=capacity_building_files,
            configured_buildings=configured_buildings,
            end_time=end_time,
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )
        emit_log(
            "[交接班日志] 执行完成（已有文件批量）: "
            f"success={int(result.get('success_count', 0))}, "
            f"failed={int(result.get('failed_count', 0))}, "
            f"selected={','.join(result.get('selected_buildings', []) or []) or '-'}, "
            f"skipped={','.join(result.get('skipped_buildings', []) or []) or '-'}"
        )
        return result

    def run_handover_from_download(
        self,
        *,
        buildings: List[str] | None,
        end_time: str | None,
        duty_date: str | None,
        duty_shift: str | None,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        show_buildings = ",".join(buildings or []) or "按交接班配置启用楼栋"
        emit_log(
            f"[交接班日志] 开始执行（内网下载） buildings={show_buildings}, "
            f"duty_date={str(duty_date or '-').strip() or '-'}, duty_shift={str(duty_shift or '-').strip() or '-'}"
        )
        result = run_from_download(
            config=self.config,
            buildings=buildings,
            end_time=end_time,
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )
        emit_log(
            f"[交接班日志] 执行完成（内网下载）: success={int(result.get('success_count', 0))}, "
            f"failed={int(result.get('failed_count', 0))}"
        )
        return result

    def run_day_metric_from_download(
        self,
        *,
        selected_dates: List[str],
        building_scope: str,
        building: str | None,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        emit_log(
            "[12项独立上传] 提交执行（内网下载）: "
            f"dates={','.join(selected_dates)}, scope={building_scope}, building={str(building or '').strip() or '-'}"
        )
        service = DayMetricStandaloneUploadService(self.config)
        return service.run_from_download(
            selected_dates=selected_dates,
            building_scope=building_scope,
            building=building,
            emit_log=emit_log,
        )

    def run_day_metric_from_file(
        self,
        *,
        building: str,
        duty_date: str,
        file_path: str,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        emit_log(
            "[12项独立上传] 提交执行（本地补录）: "
            f"duty_date={duty_date}, building={building}, file={file_path}"
        )
        service = DayMetricStandaloneUploadService(self.config)
        return service.run_from_file(
            building=building,
            duty_date=duty_date,
            file_path=file_path,
            emit_log=emit_log,
        )

    def retry_day_metric_unit(
        self,
        *,
        mode: str,
        duty_date: str,
        building: str,
        source_file: str | None,
        stage: str | None,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        emit_log(
            "[12项独立上传] 提交单元重试: "
            f"mode={mode}, duty_date={duty_date}, building={building}, stage={str(stage or '-').strip() or '-'}"
        )
        service = DayMetricStandaloneUploadService(self.config)
        return service.retry_unit(
            mode=mode,
            duty_date=duty_date,
            building=building,
            source_file=source_file,
            stage=stage,
            emit_log=emit_log,
        )

    def retry_day_metric_failed(
        self,
        *,
        mode: str | None,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        emit_log(
            "[12项独立上传] 提交全部失败单元重试: "
            f"mode={str(mode or 'from_download').strip() or 'from_download'}"
        )
        service = DayMetricStandaloneUploadService(self.config)
        return service.retry_failed(mode=mode, emit_log=emit_log)

    def run_handover_followup_continue(
        self,
        *,
        batch_key: str,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        target_batch = str(batch_key or "").strip()
        emit_log(f"[交接班][继续后续上传] 提交执行: batch={target_batch}")
        handover_cfg = load_handover_config(self.config)
        service = ReviewFollowupTriggerService(handover_cfg)
        result = service.continue_batch(batch_key=target_batch, emit_log=emit_log)
        emit_log(
            "[交接班][继续后续上传] 执行完成: "
            f"batch={target_batch}, status={str(result.get('status', '')).strip() or '-'}"
        )
        return result

    def run_wet_bulb_collection(
        self,
        emit_log: Callable[[str], None],
        source: str = "湿球温度定时采集",
    ) -> Dict[str, Any]:
        started_at = _now_text()
        started_perf = time.perf_counter()
        phase_started_at = _now_text()
        phase_started_perf = time.perf_counter()

        emit_log(f"[{source}] 开始执行")
        service = WetBulbCollectionService(self.config)
        result = service.run(emit_log=emit_log)
        result_status = str(result.get("status", "ok")).strip() or "ok"
        uploaded_count = len(result.get("uploaded_buildings", []))
        failed_count = len(result.get("failed_buildings", []))
        skipped_count = len(result.get("skipped_buildings", []))
        emit_log(
            f"[{source}] 执行完成: "
            f"status={result_status}, "
            f"uploaded={uploaded_count}, "
            f"failed={failed_count}, "
            f"skipped={skipped_count}"
        )

        phase = PipelinePhaseResult(
            phase="湿球温度定时采集",
            status="success" if result_status in {"ok", "skipped"} else result_status,
            started_at=phase_started_at,
            finished_at=_now_text(),
            duration_ms=int((time.perf_counter() - phase_started_perf) * 1000),
            message=(
                f"uploaded={uploaded_count}, "
                f"failed={failed_count}, "
                f"skipped={skipped_count}"
            ),
        )
        if result_status not in {"ok", "partial_failed", "failed", "skipped"}:
            result_status = "failed"
        return JobResultV2(
            status=result_status,
            summary="ok" if result_status == "ok" else result_status,
            phase_results=[phase],
            payload={
                **result,
                "source": source,
                "duration_ms": int((time.perf_counter() - started_perf) * 1000),
                "started_at": started_at,
                "finished_at": _now_text(),
            },
            retryable=bool(result.get("failed_buildings")),
        ).to_dict()
