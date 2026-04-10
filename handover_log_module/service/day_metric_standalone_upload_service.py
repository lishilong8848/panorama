from __future__ import annotations

import copy
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.config.config_compat_cleanup import sanitize_day_metric_upload_config
from app.modules.notify.service.webhook_notify_service import WebhookNotifyService
from app.shared.utils.atomic_file import atomic_write_text
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from handover_log_module.api.facade import load_handover_config
from handover_log_module.service.day_metric_bitable_export_service import DayMetricBitableExportService
from handover_log_module.service.handover_download_service import HandoverDownloadService
from handover_log_module.service.handover_extract_service import HandoverExtractService
from handover_log_module.service.handover_fill_service import HandoverFillService
from handover_log_module.service.source_data_attachment_bitable_export_service import (
    SourceDataAttachmentBitableExportService,
)
from pipeline_utils import get_app_dir


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    output = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(output.get(key), dict):
            output[key] = _deep_merge(output[key], value)
        else:
            output[key] = copy.deepcopy(value)
    return output


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return int(default)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class DayMetricStandaloneUploadService:
    FAILED_UNITS_STATE_FILE = "day_metric_failed_units.json"

    def __init__(self, config: Dict[str, Any], download_browser_pool: Any | None = None) -> None:
        self.runtime_config = copy.deepcopy(config if isinstance(config, dict) else {})
        self.handover_cfg = load_handover_config(self.runtime_config)
        self._notify = WebhookNotifyService(self.runtime_config)
        self._cfg = self._normalize_cfg()
        self._download_browser_pool = download_browser_pool

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "behavior": {
                "basic_retry_attempts": 3,
                "basic_retry_backoff_sec": 2,
                "network_retry_attempts": 5,
                "network_retry_backoff_sec": 2,
                "alert_after_attempts": 5,
            },
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = sanitize_day_metric_upload_config(self.runtime_config.get("day_metric_upload", {}))
        cfg = _deep_merge(self._defaults(), raw)
        behavior = cfg.get("behavior", {})
        if not isinstance(behavior, dict):
            behavior = {}
        behavior["basic_retry_attempts"] = max(0, _safe_int(behavior.get("basic_retry_attempts", 3), 3))
        behavior["basic_retry_backoff_sec"] = max(0, _safe_int(behavior.get("basic_retry_backoff_sec", 2), 2))
        behavior["network_retry_attempts"] = max(1, _safe_int(behavior.get("network_retry_attempts", 5), 5))
        behavior["network_retry_backoff_sec"] = max(0, _safe_int(behavior.get("network_retry_backoff_sec", 2), 2))
        behavior["alert_after_attempts"] = max(1, _safe_int(behavior.get("alert_after_attempts", 5), 5))
        cfg["behavior"] = behavior
        return cfg

    def _runtime_state_root(self) -> Path:
        return resolve_runtime_state_root(runtime_config=self.runtime_config, app_dir=get_app_dir())

    def _failed_units_state_path(self) -> Path:
        return self._runtime_state_root() / self.FAILED_UNITS_STATE_FILE

    def _load_failed_units_state(self) -> Dict[str, Any]:
        path = self._failed_units_state_path()
        if not path.exists():
            return {"updated_at": "", "units": []}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {"updated_at": "", "units": []}
        units = payload.get("units", []) if isinstance(payload, dict) else []
        return {
            "updated_at": str(payload.get("updated_at", "") or "").strip() if isinstance(payload, dict) else "",
            "units": [copy.deepcopy(item) for item in units if isinstance(item, dict)],
        }

    def _save_failed_units_state(self, units: List[Dict[str, Any]]) -> None:
        path = self._failed_units_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": _now_text(),
            "units": [copy.deepcopy(item) for item in units if isinstance(item, dict)],
        }
        atomic_write_text(
            path,
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _sync_failed_units_state(self, result: Dict[str, Any]) -> None:
        units: List[Dict[str, Any]] = []
        rows = result.get("results", []) if isinstance(result, dict) else []
        for date_row in rows if isinstance(rows, list) else []:
            duty_date = str(date_row.get("duty_date", "") or "").strip()
            buildings = date_row.get("buildings", [])
            for item in buildings if isinstance(buildings, list) else []:
                if str(item.get("status", "")).strip().lower() != "failed":
                    continue
                units.append(
                    {
                        "mode": "from_download",
                        "duty_date": duty_date,
                        "building": str(item.get("building", "") or "").strip(),
                        "stage": str(item.get("stage", "") or "").strip().lower(),
                        "attempts": max(1, _safe_int(item.get("attempts", 1), 1)),
                        "last_error": str(item.get("error", "") or "").strip(),
                        "source_file": str(item.get("source_file", "") or "").strip(),
                        "network_mode": str(item.get("network_mode", "") or "").strip(),
                        "network_side": str(item.get("network_side", "") or "").strip(),
                        "failed_at": str(item.get("failed_at", "") or "").strip() or _now_text(),
                        "retryable": bool(item.get("retryable", False)),
                        "retry_source": str(item.get("retry_source", "") or "").strip(),
                    }
                )
        self._save_failed_units_state(units)

    def _network_auto_switch_enabled(self) -> bool:
        return False

    def _network_mode_text(self) -> str:
        return "current_network"

    def _network_retry_attempts(self) -> int:
        return int(self._cfg.get("behavior", {}).get("network_retry_attempts", 5) or 5)

    def _network_retry_backoff_sec(self) -> int:
        return int(self._cfg.get("behavior", {}).get("network_retry_backoff_sec", 2) or 2)

    def _alert_after_attempts(self) -> int:
        return int(self._cfg.get("behavior", {}).get("alert_after_attempts", 5) or 5)

    def _source_reuse_enabled(self) -> bool:
        return True

    def _selected_buildings(self, *, building_scope: str, building: str | None) -> List[str]:
        if str(building_scope or "").strip() == "single":
            picked = str(building or "").strip()
            if not picked:
                raise ValueError("single 模式下 building 不能为空")
            return [picked]
        configured = self.runtime_config.get("input", {}).get("buildings", [])
        output = [str(item or "").strip() for item in configured if str(item or "").strip()]
        if not output:
            raise ValueError("未配置可用楼栋")
        return output

    @staticmethod
    def _time_window_for_date(duty_date: str) -> tuple[str, str]:
        date_text = str(duty_date or "").strip()
        return f"{date_text} 12:00:00", f"{date_text} 12:20:00"

    def _empty_result_row(self, *, mode: str, duty_date: str, building: str, stage: str) -> Dict[str, Any]:
        retry_source = "persisted_state" if mode == "from_download" else "runtime_file"
        return {
            "mode": mode,
            "duty_date": str(duty_date or "").strip(),
            "building": str(building or "").strip(),
            "status": "failed",
            "stage": str(stage or "").strip().lower() or "download",
            "network_mode": self._network_mode_text(),
            "network_side": "",
            "deleted_records": 0,
            "created_records": 0,
            "source_file": "",
            "output_file": "",
            "error": "",
            "attempts": 0,
            "retryable": True,
            "retry_source": retry_source,
            "failed_at": "",
        }

    def _notify_retry_exhausted(
        self,
        *,
        category: str,
        mode: str,
        duty_date: str,
        building: str,
        stage: str,
        attempts: int,
        error: str,
        network_side: str,
        emit_log: Callable[[str], None],
    ) -> None:
        if str(mode or "").strip().lower() == "from_download":
            return
        if attempts < self._alert_after_attempts():
            return
        detail = (
            f"12项独立上传重试耗尽: mode={mode}, duty_date={duty_date}, building={building}, "
            f"stage={stage}, attempts={attempts}, network_side={network_side or '-'}, error={error}"
        )
        self._notify.send_failure(
            stage="12项独立上传",
            detail=detail,
            building=building,
            emit_log=emit_log,
            category=category,
        )

    def _notify_batch_stage_summary(self, *, result: Dict[str, Any], emit_log: Callable[[str], None]) -> None:
        grouped_rows = result.get("results", []) if isinstance(result.get("results", []), list) else []
        failed_rows: List[Dict[str, Any]] = []
        for date_row in grouped_rows:
            buildings = date_row.get("buildings", []) if isinstance(date_row, dict) else []
            for row in buildings if isinstance(buildings, list) else []:
                if not isinstance(row, dict):
                    continue
                if str(row.get("status", "")).strip().lower() != "failed":
                    continue
                failed_rows.append(row)
        if not failed_rows:
            return

        stage_groups: Dict[str, List[Dict[str, Any]]] = {"download": [], "upload": []}
        for row in failed_rows:
            stage = str(row.get("stage", "") or "").strip().lower()
            if stage == "download":
                stage_groups["download"].append(row)
            elif stage in {"attachment", "extract", "upload"}:
                stage_groups["upload"].append(row)

        for stage_name, rows in stage_groups.items():
            if not rows:
                continue
            units = []
            for item in rows:
                duty_date = str(item.get("duty_date", "") or "").strip()
                building = str(item.get("building", "") or "").strip()
                if duty_date or building:
                    units.append(f"{duty_date}|{building}".strip("|"))
            preview = units[:20]
            extra_count = max(0, len(units) - len(preview))
            unit_text = ",".join(preview) if preview else "-"
            if extra_count > 0:
                unit_text = f"{unit_text} ... +{extra_count}"
            attempts = max(_safe_int(item.get("attempts", 0), 0) for item in rows)
            network_sides = sorted(
                {
                    str(item.get("network_side", "") or "").strip()
                    for item in rows
                    if str(item.get("network_side", "") or "").strip()
                }
            )
            network_side_text = ",".join(network_sides) if network_sides else "-"
            last_error = str(rows[-1].get("error", "") or "").strip() or "unknown_error"
            detail = (
                f"12项独立上传批量失败摘要: mode=from_download, stage={stage_name}, failed_units={len(rows)}, "
                f"units={unit_text}, attempts={attempts}, network_side={network_side_text}, last_error={last_error}"
            )
            self._notify.send_failure(
                stage="12项独立上传",
                detail=detail,
                building="",
                emit_log=emit_log,
                category="download" if stage_name == "download" else "upload",
            )

    def _new_download_service(self) -> HandoverDownloadService:
        if self._download_browser_pool is None:
            return HandoverDownloadService(self.handover_cfg)
        try:
            return HandoverDownloadService(
                self.handover_cfg,
                download_browser_pool=self._download_browser_pool,
            )
        except TypeError as exc:
            # Compatibility fallback: some test doubles / legacy constructors
            # still only accept (cfg), without download_browser_pool.
            if "download_browser_pool" not in str(exc):
                raise
            return HandoverDownloadService(self.handover_cfg)

    def _new_extract_service(self) -> HandoverExtractService:
        return HandoverExtractService(self.handover_cfg)

    def _new_fill_service(self) -> HandoverFillService:
        return HandoverFillService(self.handover_cfg)

    def _new_attachment_service(self) -> SourceDataAttachmentBitableExportService:
        try:
            return SourceDataAttachmentBitableExportService(
                self.handover_cfg,
                log_prefix="[12项独立上传][源数据附件]",
            )
        except TypeError as exc:
            # Compatibility fallback: test doubles / legacy constructors may
            # still only accept (cfg) and do not support log_prefix.
            if "log_prefix" not in str(exc):
                raise
            return SourceDataAttachmentBitableExportService(self.handover_cfg)

    def _new_export_service(self) -> DayMetricBitableExportService:
        return DayMetricBitableExportService(self.runtime_config)

    def _sleep_between_attempts(self) -> None:
        backoff = self._network_retry_backoff_sec()
        if backoff > 0:
            time.sleep(backoff)

    def _run_download_stage_for_date(
        self,
        *,
        download_service: HandoverDownloadService,
        mode: str,
        duty_date: str,
        buildings: List[str],
        emit_log: Callable[[str], None],
    ) -> tuple[Dict[str, str], Dict[str, Dict[str, Any]]]:
        remaining = [str(item or "").strip() for item in buildings if str(item or "").strip()]
        success_files: Dict[str, str] = {}
        failures: Dict[str, Dict[str, Any]] = {}
        max_attempts = self._network_retry_attempts()

        emit_log(
            f"[12项独立上传] 开始下载阶段: duty_date={duty_date}, "
            f"buildings={','.join(remaining) or '-'}, max_attempts={max_attempts}"
        )
        for attempt in range(1, max_attempts + 1):
            if not remaining:
                break
            ensure_error = ""
            try:
                download_service.ensure_internal_ready(emit_log=emit_log)
            except Exception as exc:  # noqa: BLE001
                ensure_error = str(exc)
                for building in remaining:
                    row = self._empty_result_row(mode=mode, duty_date=duty_date, building=building, stage="download")
                    row["error"] = ensure_error
                    row["attempts"] = attempt
                    row["failed_at"] = _now_text()
                    row["network_side"] = "internal"
                    failures[building] = row
                if attempt >= max_attempts:
                    for building in remaining:
                        self._notify_retry_exhausted(
                            category="wifi",
                            mode=mode,
                            duty_date=duty_date,
                            building=building,
                            stage="download",
                            attempts=attempt,
                            error=ensure_error,
                            network_side="internal",
                            emit_log=emit_log,
                        )
                    break
                self._sleep_between_attempts()
                continue

            try:
                start_time, end_time = self._time_window_for_date(duty_date)
                batch = download_service.run(
                    buildings=remaining,
                    start_time=start_time,
                    end_time=end_time,
                    duty_date=duty_date,
                    duty_shift="day",
                    switch_network=False,
                    reuse_cached=self._source_reuse_enabled(),
                    emit_log=emit_log,
                )
            except Exception as exc:  # noqa: BLE001
                error_text = str(exc)
                for building in remaining:
                    row = self._empty_result_row(mode=mode, duty_date=duty_date, building=building, stage="download")
                    row["error"] = error_text
                    row["attempts"] = attempt
                    row["failed_at"] = _now_text()
                    row["network_side"] = "internal"
                    failures[building] = row
                if attempt >= max_attempts:
                    for building in remaining:
                        self._notify_retry_exhausted(
                            category="download",
                            mode=mode,
                            duty_date=duty_date,
                            building=building,
                            stage="download",
                            attempts=attempt,
                            error=error_text,
                            network_side="internal",
                            emit_log=emit_log,
                        )
                    break
                self._sleep_between_attempts()
                continue

            failed_map: Dict[str, str] = {}
            for item in batch.get("failed", []) if isinstance(batch.get("failed", []), list) else []:
                building = str(item.get("building", "") or "").strip()
                if not building:
                    continue
                failed_map[building] = str(item.get("error", "") or "download_failed").strip() or "download_failed"
            next_remaining: List[str] = []
            for item in batch.get("success_files", []) if isinstance(batch.get("success_files", []), list) else []:
                building = str(item.get("building", "") or "").strip()
                file_path = str(item.get("file_path", "") or "").strip()
                if building and file_path:
                    success_files[building] = file_path
            for building in remaining:
                if building in success_files:
                    continue
                row = self._empty_result_row(mode=mode, duty_date=duty_date, building=building, stage="download")
                row["attempts"] = attempt
                row["failed_at"] = _now_text()
                row["network_side"] = "internal"
                row["error"] = failed_map.get(building, "download_failed")
                failures[building] = row
                next_remaining.append(building)
            remaining = next_remaining
            if remaining and attempt < max_attempts:
                self._sleep_between_attempts()

        if remaining:
            for building in remaining:
                row = failures.get(building) or self._empty_result_row(
                    mode=mode,
                    duty_date=duty_date,
                    building=building,
                    stage="download",
                )
                failures[building] = row
                self._notify_retry_exhausted(
                    category="download",
                    mode=mode,
                    duty_date=duty_date,
                    building=building,
                    stage="download",
                    attempts=max(1, _safe_int(row.get("attempts", 1), 1)),
                    error=str(row.get("error", "") or "download_failed"),
                    network_side="internal",
                    emit_log=emit_log,
                )
            emit_log(
                f"[12项独立上传] 下载阶段完成，仍有楼栋失败: "
                f"duty_date={duty_date}, failed={','.join(remaining) or '-'}"
            )
        else:
            emit_log(
                f"[12项独立上传] 下载阶段完成: "
                f"duty_date={duty_date}, success={','.join(sorted(success_files)) or '-'}"
            )
        return success_files, failures

    def _run_attachment_stage(
        self,
        *,
        mode: str,
        duty_date: str,
        building: str,
        source_file: str,
        emit_log: Callable[[str], None],
    ) -> tuple[bool, Dict[str, Any], int]:
        service = self._new_attachment_service()
        download_service = self._new_download_service()
        max_attempts = self._network_retry_attempts()
        last_result: Dict[str, Any] = {}

        for attempt in range(1, max_attempts + 1):
            try:
                download_service.ensure_external_ready(emit_log=emit_log)
            except Exception as exc:  # noqa: BLE001
                error_text = str(exc)
                if attempt >= max_attempts:
                    self._notify_retry_exhausted(
                        category="wifi",
                        mode=mode,
                        duty_date=duty_date,
                        building=building,
                        stage="attachment",
                        attempts=attempt,
                        error=error_text,
                        network_side="external",
                        emit_log=emit_log,
                    )
                    return False, {"status": "failed", "error": error_text}, attempt
                self._sleep_between_attempts()
                continue

            last_result = service.run_from_source_file(
                building=building,
                duty_date=duty_date,
                duty_shift="day",
                data_file=source_file,
                emit_log=emit_log,
            )
            if str(last_result.get("status", "")).strip().lower() in {"ok", "skipped"}:
                return True, last_result, attempt
            error_text = str(last_result.get("error", "") or last_result.get("reason", "") or "attachment_failed").strip()
            if attempt >= max_attempts:
                self._notify_retry_exhausted(
                    category="upload",
                    mode=mode,
                    duty_date=duty_date,
                    building=building,
                    stage="attachment",
                    attempts=attempt,
                    error=error_text,
                    network_side="external",
                    emit_log=emit_log,
                )
                return False, last_result, attempt
            self._sleep_between_attempts()
        return False, last_result, max_attempts

    def _run_upload_stage(
        self,
        *,
        mode: str,
        duty_date: str,
        building: str,
        final_cell_values: Dict[str, Any],
        resolved_values_by_id: Dict[str, Any],
        metric_origin_context: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> tuple[bool, Dict[str, Any], int]:
        service = self._new_export_service()
        download_service = self._new_download_service()
        max_attempts = self._network_retry_attempts()
        last_result: Dict[str, Any] = {}

        for attempt in range(1, max_attempts + 1):
            try:
                download_service.ensure_external_ready(emit_log=emit_log)
            except Exception as exc:  # noqa: BLE001
                error_text = str(exc)
                if attempt >= max_attempts:
                    self._notify_retry_exhausted(
                        category="wifi",
                        mode=mode,
                        duty_date=duty_date,
                        building=building,
                        stage="upload",
                        attempts=attempt,
                        error=error_text,
                        network_side="external",
                        emit_log=emit_log,
                    )
                    return False, {"status": "failed", "error": error_text}, attempt
                self._sleep_between_attempts()
                continue

            last_result = service.run(
                building=building,
                duty_date=duty_date,
                duty_shift="day",
                filled_cell_values=final_cell_values,
                resolved_values_by_id=resolved_values_by_id,
                metric_origin_context=metric_origin_context,
                emit_log=emit_log,
            )
            if str(last_result.get("status", "")).strip().lower() in {"ok", "skipped"}:
                return True, last_result, attempt
            error_text = str(last_result.get("error", "") or "upload_failed").strip() or "upload_failed"
            if attempt >= max_attempts:
                self._notify_retry_exhausted(
                    category="upload",
                    mode=mode,
                    duty_date=duty_date,
                    building=building,
                    stage="upload",
                    attempts=attempt,
                    error=error_text,
                    network_side="external",
                    emit_log=emit_log,
                )
                return False, last_result, attempt
            self._sleep_between_attempts()
        return False, last_result, max_attempts

    def _process_source_file_unit(
        self,
        *,
        mode: str,
        duty_date: str,
        building: str,
        source_file: str,
        emit_log: Callable[[str], None],
        start_stage: str = "attachment",
    ) -> Dict[str, Any]:
        row = self._empty_result_row(mode=mode, duty_date=duty_date, building=building, stage=start_stage)
        row["source_file"] = str(source_file or "").strip()
        row["retryable"] = True
        row["network_side"] = "external" if start_stage in {"attachment", "upload"} else ""

        normalized_stage = str(start_stage or "attachment").strip().lower() or "attachment"
        if normalized_stage not in {"attachment", "extract", "upload"}:
            normalized_stage = "attachment"

        emit_log(
            f"[12项独立上传] 单元开始: duty_date={duty_date}, building={building}, "
            f"start_stage={normalized_stage}, source_file={source_file}"
        )
        if normalized_stage == "attachment":
            emit_log(f"[12项独立上传] 附件阶段开始: duty_date={duty_date}, building={building}")
            attachment_ok, attachment_result, attachment_attempts = self._run_attachment_stage(
                mode=mode,
                duty_date=duty_date,
                building=building,
                source_file=source_file,
                emit_log=emit_log,
            )
            row["attempts"] = max(row["attempts"], attachment_attempts)
            if not attachment_ok:
                row["status"] = "failed"
                row["stage"] = "attachment"
                row["error"] = str(
                    attachment_result.get("error", "") or attachment_result.get("reason", "") or "attachment_failed"
                ).strip() or "attachment_failed"
                row["failed_at"] = _now_text()
                emit_log(
                    f"[12项独立上传] 附件阶段失败: duty_date={duty_date}, building={building}, error={row['error']}"
                )
                return row

        emit_log(f"[12项独立上传] 提取填充开始: duty_date={duty_date}, building={building}")
        try:
            extract_service = self._new_extract_service()
            fill_service = self._new_fill_service()
            extract_result = extract_service.extract(building=building, data_file=source_file)
            fill_result = fill_service.fill(
                building=building,
                data_file=source_file,
                hits=extract_result.get("hits", {}),
                effective_config=extract_result.get("effective_config", {}),
                date_ref_override=datetime.strptime(str(duty_date or "").strip(), "%Y-%m-%d"),
                write_output_file=False,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            row["status"] = "failed"
            row["stage"] = "extract"
            row["error"] = str(exc)
            row["failed_at"] = _now_text()
            row["retryable"] = bool(Path(source_file).exists()) if mode == "from_file" else True
            row["network_side"] = ""
            emit_log(
                f"[12项独立上传] 提取填充失败: duty_date={duty_date}, building={building}, error={row['error']}"
            )
            return row

        resolved_count = 0
        if isinstance(fill_result, dict):
            resolved_values = fill_result.get("resolved_values_by_id", {})
            if isinstance(resolved_values, dict):
                resolved_count = len(resolved_values)
        emit_log(
            f"[12项独立上传] 提取填充完成: duty_date={duty_date}, building={building}, metrics={resolved_count}"
        )

        emit_log(f"[12项独立上传] 写入阶段开始: duty_date={duty_date}, building={building}")
        upload_ok, upload_result, upload_attempts = self._run_upload_stage(
            mode=mode,
            duty_date=duty_date,
            building=building,
            final_cell_values=fill_result.get("final_cell_values", {}) if isinstance(fill_result, dict) else {},
            resolved_values_by_id=fill_result.get("resolved_values_by_id", {}) if isinstance(fill_result, dict) else {},
            metric_origin_context=self._new_export_service().serialize_metric_origin_context(
                hits=extract_result.get("hits", {}) if isinstance(extract_result, dict) else {},
                effective_config=extract_result.get("effective_config", {}) if isinstance(extract_result, dict) else {},
            ),
            emit_log=emit_log,
        )
        row["attempts"] = max(row["attempts"], upload_attempts)
        row["stage"] = "upload"
        row["network_side"] = "external"
        if not upload_ok:
            row["status"] = "failed"
            row["error"] = str(upload_result.get("error", "") or "upload_failed").strip() or "upload_failed"
            row["failed_at"] = _now_text()
            emit_log(
                f"[12项独立上传] 写入阶段失败: duty_date={duty_date}, building={building}, error={row['error']}"
            )
            return row

        row["status"] = "ok"
        row["deleted_records"] = int(upload_result.get("deleted_records", 0) or 0)
        row["created_records"] = int(upload_result.get("created_records", 0) or 0)
        row["error"] = ""
        row["failed_at"] = ""
        row["retryable"] = False
        emit_log(
            f"[12项独立上传] 单元完成: duty_date={duty_date}, building={building}, "
            f"deleted={row['deleted_records']}, created={row['created_records']}"
        )
        return row

    @staticmethod
    def _group_rows_by_date(selected_dates: List[str], rows_by_key: Dict[tuple[str, str], Dict[str, Any]], buildings: List[str]) -> List[Dict[str, Any]]:
        grouped: List[Dict[str, Any]] = []
        for duty_date in selected_dates:
            date_rows: List[Dict[str, Any]] = []
            for building in buildings:
                row = rows_by_key.get((duty_date, building))
                if row is not None:
                    date_rows.append(copy.deepcopy(row))
            grouped.append({"duty_date": duty_date, "buildings": date_rows})
        return grouped

    @staticmethod
    def _summarize_result_rows(grouped_rows: List[Dict[str, Any]]) -> Dict[str, int]:
        success_units = 0
        failed_units = 0
        skipped_units = 0
        total_deleted_records = 0
        total_created_records = 0
        total_units = 0
        for date_row in grouped_rows:
            buildings = date_row.get("buildings", [])
            for row in buildings if isinstance(buildings, list) else []:
                total_units += 1
                status = str(row.get("status", "")).strip().lower()
                if status == "ok":
                    success_units += 1
                elif status == "skipped":
                    skipped_units += 1
                else:
                    failed_units += 1
                total_deleted_records += int(row.get("deleted_records", 0) or 0)
                total_created_records += int(row.get("created_records", 0) or 0)
        return {
            "total_units": total_units,
            "success_units": success_units,
            "failed_units": failed_units,
            "skipped_units": skipped_units,
            "total_deleted_records": total_deleted_records,
            "total_created_records": total_created_records,
        }

    @staticmethod
    def _status_from_summary(summary: Dict[str, int]) -> str:
        failed_units = int(summary.get("failed_units", 0) or 0)
        success_units = int(summary.get("success_units", 0) or 0)
        skipped_units = int(summary.get("skipped_units", 0) or 0)
        if failed_units <= 0:
            return "ok"
        if success_units > 0 or skipped_units > 0:
            return "partial_failed"
        return "failed"

    def _download_success_row(self, *, duty_date: str, building: str, source_file: str) -> Dict[str, Any]:
        row = self._empty_result_row(
            mode="from_download",
            duty_date=duty_date,
            building=building,
            stage="download",
        )
        row["status"] = "ok"
        row["stage"] = "download"
        row["source_file"] = str(source_file or "").strip()
        row["attempts"] = 1
        row["retryable"] = False
        row["network_side"] = "internal"
        row["failed_at"] = ""
        return row

    def _build_batch_result(
        self,
        *,
        mode: str,
        selected_dates: List[str],
        buildings: List[str],
        rows_by_key: Dict[tuple[str, str], Dict[str, Any]],
        building_scope: str,
        building: str | None,
        auto_switch_enabled: bool,
    ) -> Dict[str, Any]:
        grouped_rows = self._group_rows_by_date(selected_dates, rows_by_key, buildings)
        summary = self._summarize_result_rows(grouped_rows)
        return {
            "status": self._status_from_summary(summary),
            "mode": mode,
            "duty_shift": "day",
            "selected_dates": list(selected_dates),
            "selected_buildings": list(buildings),
            "building_scope": str(building_scope or "").strip(),
            "building": str(building or "").strip(),
            "network_switch_followed_global_setting": False,
            "network_auto_switch_enabled": auto_switch_enabled,
            "results": grouped_rows,
            **summary,
        }

    def run_download_only(
        self,
        *,
        selected_dates: List[str],
        building_scope: str,
        building: str | None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        buildings = self._selected_buildings(building_scope=building_scope, building=building)
        download_service = self._new_download_service()
        auto_switch_enabled = self._network_auto_switch_enabled()
        rows_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
        downloaded_files: List[Dict[str, str]] = []

        emit_log(
            f"[12项独立上传] 开始下载批次: dates={','.join(selected_dates)}, "
            f"buildings={','.join(buildings)}, network_mode={self._network_mode_text()}"
        )
        if auto_switch_enabled:
            try:
                emit_log("[12项独立上传] 下载阶段准备内网环境")
                download_service.prepare_internal_for_batch_download(emit_log=emit_log)
            except Exception as exc:  # noqa: BLE001
                emit_log(f"[12项独立上传] 准备内网环境失败: {exc}")

        for duty_date in selected_dates:
            success_files, failures = self._run_download_stage_for_date(
                download_service=download_service,
                mode="from_download",
                duty_date=duty_date,
                buildings=buildings,
                emit_log=emit_log,
            )
            for building_name, file_path in success_files.items():
                rows_by_key[(duty_date, building_name)] = self._download_success_row(
                    duty_date=duty_date,
                    building=building_name,
                    source_file=file_path,
                )
                downloaded_files.append(
                    {
                        "duty_date": duty_date,
                        "building": building_name,
                        "source_file": str(file_path or "").strip(),
                    }
                )
            for building_name, row in failures.items():
                rows_by_key[(duty_date, building_name)] = row

        result = self._build_batch_result(
            mode="from_download",
            selected_dates=selected_dates,
            buildings=buildings,
            rows_by_key=rows_by_key,
            building_scope=building_scope,
            building=building,
            auto_switch_enabled=auto_switch_enabled,
        )
        result["downloaded_files"] = downloaded_files
        result["downloaded_file_count"] = len(downloaded_files)
        emit_log(
            f"[12项独立上传] 下载批次完成: downloaded={len(downloaded_files)}, "
            f"failed={int(result.get('failed_units', 0) or 0)}"
        )
        return result

    def continue_from_source_files(
        self,
        *,
        selected_dates: List[str],
        buildings: List[str],
        source_units: List[Dict[str, Any]],
        building_scope: str,
        building: str | None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        auto_switch_enabled = self._network_auto_switch_enabled()
        rows_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}

        emit_log(
            f"[12项独立上传] 开始上传阶段: units={len(source_units)}, network_mode={self._network_mode_text()}"
        )
        if auto_switch_enabled:
            try:
                emit_log("[12项独立上传] 上传阶段准备外网环境")
                self._new_download_service().ensure_external_ready(emit_log=emit_log)
            except Exception as exc:  # noqa: BLE001
                error_text = str(exc)
                emit_log(f"[12项独立上传] 准备外网环境失败: {error_text}")
                for duty_date in selected_dates:
                    for building_name in buildings:
                        row = self._empty_result_row(
                            mode="from_download",
                            duty_date=duty_date,
                            building=building_name,
                            stage="upload",
                        )
                        row["status"] = "failed"
                        row["error"] = error_text
                        row["attempts"] = 1
                        row["failed_at"] = _now_text()
                        row["network_side"] = "external"
                        rows_by_key[(duty_date, building_name)] = row
                result = self._build_batch_result(
                    mode="from_download",
                    selected_dates=selected_dates,
                    buildings=buildings,
                    rows_by_key=rows_by_key,
                    building_scope=building_scope,
                    building=building,
                    auto_switch_enabled=auto_switch_enabled,
                )
                emit_log(
                    f"[12项独立上传] 上传阶段完成: total={int(result.get('total_units', 0) or 0)}, "
                    f"failed={int(result.get('failed_units', 0) or 0)}"
                )
                return result

        for item in source_units:
            if not isinstance(item, dict):
                continue
            duty_date = str(item.get("duty_date", "") or "").strip()
            building_name = str(item.get("building", "") or "").strip()
            source_file = str(item.get("source_file", "") or "").strip()
            if not duty_date or not building_name:
                continue
            if not source_file or not Path(source_file).exists():
                row = self._empty_result_row(
                    mode="from_download",
                    duty_date=duty_date,
                    building=building_name,
                    stage="upload",
                )
                row["status"] = "failed"
                row["error"] = "源数据文件不存在"
                row["attempts"] = 1
                row["failed_at"] = _now_text()
                row["network_side"] = "external"
                rows_by_key[(duty_date, building_name)] = row
                emit_log(
                    f"[12项独立上传] 单元跳过: duty_date={duty_date}, building={building_name}, error={row['error']}"
                )
                continue
            row = self._process_source_file_unit(
                mode="from_download",
                duty_date=duty_date,
                building=building_name,
                source_file=source_file,
                emit_log=emit_log,
            )
            rows_by_key[(duty_date, building_name)] = row
            emit_log(
                f"[12项独立上传] 单元结果: duty_date={duty_date}, building={building_name}, "
                f"status={str(row.get('status', '') or '-').strip()}, "
                f"stage={str(row.get('stage', '') or '-').strip()}"
            )

        result = self._build_batch_result(
            mode="from_download",
            selected_dates=selected_dates,
            buildings=buildings,
            rows_by_key=rows_by_key,
            building_scope=building_scope,
            building=building,
            auto_switch_enabled=auto_switch_enabled,
        )
        emit_log(
            f"[12项独立上传] 上传阶段完成: total={int(result.get('total_units', 0) or 0)}, "
            f"success={int(result.get('success_units', 0) or 0)}, "
            f"failed={int(result.get('failed_units', 0) or 0)}, "
            f"skipped={int(result.get('skipped_units', 0) or 0)}"
        )
        return result

    def merge_bridge_results(
        self,
        *,
        internal_result: Dict[str, Any],
        external_result: Dict[str, Any],
        selected_dates: List[str],
        buildings: List[str],
        building_scope: str,
        building: str | None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        rows_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
        auto_switch_enabled = self._network_auto_switch_enabled()

        for date_row in internal_result.get("results", []) if isinstance(internal_result.get("results", []), list) else []:
            if not isinstance(date_row, dict):
                continue
            duty_date = str(date_row.get("duty_date", "") or "").strip()
            for row in date_row.get("buildings", []) if isinstance(date_row.get("buildings", []), list) else []:
                if not isinstance(row, dict):
                    continue
                building_name = str(row.get("building", "") or "").strip()
                if not duty_date or not building_name:
                    continue
                if str(row.get("status", "")).strip().lower() == "failed":
                    rows_by_key[(duty_date, building_name)] = copy.deepcopy(row)

        for date_row in external_result.get("results", []) if isinstance(external_result.get("results", []), list) else []:
            if not isinstance(date_row, dict):
                continue
            duty_date = str(date_row.get("duty_date", "") or "").strip()
            for row in date_row.get("buildings", []) if isinstance(date_row.get("buildings", []), list) else []:
                if not isinstance(row, dict):
                    continue
                building_name = str(row.get("building", "") or "").strip()
                if not duty_date or not building_name:
                    continue
                rows_by_key[(duty_date, building_name)] = copy.deepcopy(row)

        for duty_date in selected_dates:
            for building_name in buildings:
                if (duty_date, building_name) in rows_by_key:
                    continue
                row = self._empty_result_row(
                    mode="from_download",
                    duty_date=duty_date,
                    building=building_name,
                    stage="download",
                )
                row["error"] = "bridge_result_incomplete"
                row["attempts"] = 1
                row["failed_at"] = _now_text()
                rows_by_key[(duty_date, building_name)] = row

        result = self._build_batch_result(
            mode="from_download",
            selected_dates=selected_dates,
            buildings=buildings,
            rows_by_key=rows_by_key,
            building_scope=building_scope,
            building=building,
            auto_switch_enabled=auto_switch_enabled,
        )
        self._notify_batch_stage_summary(result=result, emit_log=emit_log)
        self._sync_failed_units_state(result)
        return result

    def run_from_download(
        self,
        *,
        selected_dates: List[str],
        building_scope: str,
        building: str | None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        internal_result = self.run_download_only(
            selected_dates=selected_dates,
            building_scope=building_scope,
            building=building,
            emit_log=emit_log,
        )
        buildings = (
            list(internal_result.get("selected_buildings", []))
            if isinstance(internal_result.get("selected_buildings", []), list)
            else []
        )
        external_result = self.continue_from_source_files(
            selected_dates=selected_dates,
            buildings=buildings,
            source_units=(
                list(internal_result.get("downloaded_files", []))
                if isinstance(internal_result.get("downloaded_files", []), list)
                else []
            ),
            building_scope=building_scope,
            building=building,
            emit_log=emit_log,
        )
        result = self.merge_bridge_results(
            internal_result=internal_result,
            external_result=external_result,
            selected_dates=selected_dates,
            buildings=buildings,
            building_scope=building_scope,
            building=building,
            emit_log=emit_log,
        )
        result["internal"] = internal_result
        result["external"] = external_result
        emit_log(
            f"[12项独立上传] 桥接批次完成: total={int(result.get('total_units', 0) or 0)}, "
            f"success={int(result.get('success_units', 0) or 0)}, "
            f"failed={int(result.get('failed_units', 0) or 0)}"
        )
        return result

    def run_from_file(
        self,
        *,
        building: str,
        duty_date: str,
        file_path: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        source_path = str(file_path or "").strip()
        if not source_path or not Path(source_path).exists():
            row = self._empty_result_row(
                mode="from_file",
                duty_date=duty_date,
                building=building,
                stage="attachment",
            )
            row["status"] = "failed"
            row["error"] = "源数据文件不存在"
            row["retryable"] = False
            row["retry_source"] = ""
            row["failed_at"] = _now_text()
        else:
            row = self._process_source_file_unit(
                mode="from_file",
                duty_date=duty_date,
                building=building,
                source_file=source_path,
                emit_log=emit_log,
            )
        grouped_rows = [{"duty_date": duty_date, "buildings": [row]}]
        summary = self._summarize_result_rows(grouped_rows)
        return {
            "status": self._status_from_summary(summary),
            "mode": "from_file",
            "duty_shift": "day",
            "selected_dates": [duty_date],
            "building_scope": "single",
            "building": building,
            "network_switch_followed_global_setting": False,
            "network_auto_switch_enabled": self._network_auto_switch_enabled(),
            "results": grouped_rows,
            **summary,
        }

    def retry_unit(
        self,
        *,
        mode: str,
        duty_date: str,
        building: str,
        source_file: str | None = None,
        stage: str | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        normalized_mode = str(mode or "").strip().lower() or "from_download"
        duty_date_text = str(duty_date or "").strip()
        building_text = str(building or "").strip()
        if normalized_mode not in {"from_download", "from_file"}:
            raise ValueError(f"retry_unit 不支持的 mode: {mode}")
        if not duty_date_text or not building_text:
            raise ValueError("retry_unit 需要 duty_date 和 building")

        if normalized_mode == "from_file":
            source_path = str(source_file or "").strip()
            if not source_path or not Path(source_path).exists():
                row = self._empty_result_row(
                    mode="from_file",
                    duty_date=duty_date_text,
                    building=building_text,
                    stage=str(stage or "attachment").strip().lower() or "attachment",
                )
                row["status"] = "failed"
                row["error"] = "源数据文件不存在"
                row["retryable"] = False
                row["retry_source"] = ""
                row["failed_at"] = _now_text()
                grouped_rows = [{"duty_date": duty_date_text, "buildings": [row]}]
                summary = self._summarize_result_rows(grouped_rows)
                return {
                    "status": self._status_from_summary(summary),
                    "mode": "from_file",
                    "duty_shift": "day",
                    "selected_dates": [duty_date_text],
                    "building_scope": "single",
                    "building": building_text,
                    "network_switch_followed_global_setting": False,
                    "network_auto_switch_enabled": self._network_auto_switch_enabled(),
                    "results": grouped_rows,
                    **summary,
                }
            return self.run_from_file(
                building=building_text,
                duty_date=duty_date_text,
                file_path=source_path,
                emit_log=emit_log,
            )

        state = self._load_failed_units_state()
        matched = None
        for item in state.get("units", []):
            if str(item.get("mode", "")).strip().lower() != "from_download":
                continue
            if str(item.get("duty_date", "")).strip() != duty_date_text:
                continue
            if str(item.get("building", "")).strip() != building_text:
                continue
            matched = copy.deepcopy(item)
            break
        if matched is None:
            matched = {
                "mode": "from_download",
                "duty_date": duty_date_text,
                "building": building_text,
                "stage": str(stage or "download").strip().lower() or "download",
                "source_file": str(source_file or "").strip(),
            }

        source_path = str(source_file or matched.get("source_file", "")).strip()
        current_stage = str(matched.get("stage", stage or "download") or "download").strip().lower() or "download"
        if current_stage in {"attachment", "upload"} and source_path and Path(source_path).exists():
            row = self._process_source_file_unit(
                mode="from_download",
                duty_date=duty_date_text,
                building=building_text,
                source_file=source_path,
                emit_log=emit_log,
                start_stage=current_stage,
            )
        else:
            rerun_result = self.run_from_download(
                selected_dates=[duty_date_text],
                building_scope="single",
                building=building_text,
                emit_log=emit_log,
            )
            row = rerun_result["results"][0]["buildings"][0]

        grouped_rows = [{"duty_date": duty_date_text, "buildings": [row]}]
        summary = self._summarize_result_rows(grouped_rows)
        result = {
            "status": self._status_from_summary(summary),
            "mode": "from_download",
            "duty_shift": "day",
            "selected_dates": [duty_date_text],
            "building_scope": "single",
            "building": building_text,
            "network_switch_followed_global_setting": False,
            "network_auto_switch_enabled": self._network_auto_switch_enabled(),
            "results": grouped_rows,
            **summary,
        }
        existing_units = [
            item
            for item in state.get("units", [])
            if not (
                str(item.get("mode", "")).strip().lower() == "from_download"
                and str(item.get("duty_date", "")).strip() == duty_date_text
                and str(item.get("building", "")).strip() == building_text
            )
        ]
        if str(row.get("status", "")).strip().lower() == "failed":
            existing_units.append(
                {
                    "mode": "from_download",
                    "duty_date": duty_date_text,
                    "building": building_text,
                    "stage": str(row.get("stage", "")).strip().lower(),
                    "attempts": int(row.get("attempts", 0) or 0),
                    "last_error": str(row.get("error", "")).strip(),
                    "source_file": str(row.get("source_file", "")).strip(),
                    "network_mode": str(row.get("network_mode", "")).strip(),
                    "network_side": str(row.get("network_side", "")).strip(),
                    "failed_at": str(row.get("failed_at", "")).strip() or _now_text(),
                    "retryable": bool(row.get("retryable", False)),
                    "retry_source": str(row.get("retry_source", "")).strip(),
                }
            )
        self._save_failed_units_state(existing_units)
        return result

    def retry_failed(
        self,
        *,
        mode: str | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        normalized_mode = str(mode or "from_download").strip().lower() or "from_download"
        if normalized_mode != "from_download":
            return {
                "status": "skipped",
                "mode": normalized_mode,
                "duty_shift": "day",
                "selected_dates": [],
                "building_scope": "single",
                "building": "",
                "network_switch_followed_global_setting": False,
                "network_auto_switch_enabled": self._network_auto_switch_enabled(),
                "results": [],
                "total_units": 0,
                "success_units": 0,
                "failed_units": 0,
                "skipped_units": 0,
                "total_deleted_records": 0,
                "total_created_records": 0,
            }

        state = self._load_failed_units_state()
        units = [copy.deepcopy(item) for item in state.get("units", []) if isinstance(item, dict)]
        if not units:
            return {
                "status": "skipped",
                "mode": "from_download",
                "duty_shift": "day",
                "selected_dates": [],
                "building_scope": "single",
                "building": "",
                "network_switch_followed_global_setting": False,
                "network_auto_switch_enabled": self._network_auto_switch_enabled(),
                "results": [],
                "total_units": 0,
                "success_units": 0,
                "failed_units": 0,
                "skipped_units": 0,
                "total_deleted_records": 0,
                "total_created_records": 0,
            }

        rows_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
        ordered_dates: List[str] = []
        buildings_in_order: List[str] = []
        remaining_units: List[Dict[str, Any]] = []
        for item in units:
            duty_date_text = str(item.get("duty_date", "")).strip()
            building_text = str(item.get("building", "")).strip()
            if duty_date_text and duty_date_text not in ordered_dates:
                ordered_dates.append(duty_date_text)
            if building_text and building_text not in buildings_in_order:
                buildings_in_order.append(building_text)
            result = self.retry_unit(
                mode="from_download",
                duty_date=duty_date_text,
                building=building_text,
                source_file=str(item.get("source_file", "")).strip(),
                stage=str(item.get("stage", "")).strip().lower(),
                emit_log=emit_log,
            )
            row = result.get("results", [{}])[0].get("buildings", [{}])[0]
            rows_by_key[(duty_date_text, building_text)] = copy.deepcopy(row)
            if str(row.get("status", "")).strip().lower() == "failed":
                remaining_units.append(
                    {
                        "mode": "from_download",
                        "duty_date": duty_date_text,
                        "building": building_text,
                        "stage": str(row.get("stage", "")).strip().lower(),
                        "attempts": int(row.get("attempts", 0) or 0),
                        "last_error": str(row.get("error", "")).strip(),
                        "source_file": str(row.get("source_file", "")).strip(),
                        "network_mode": str(row.get("network_mode", "")).strip(),
                        "network_side": str(row.get("network_side", "")).strip(),
                        "failed_at": str(row.get("failed_at", "")).strip() or _now_text(),
                        "retryable": bool(row.get("retryable", False)),
                        "retry_source": str(row.get("retry_source", "")).strip(),
                    }
                )
        self._save_failed_units_state(remaining_units)
        grouped_rows = self._group_rows_by_date(ordered_dates, rows_by_key, buildings_in_order)
        summary = self._summarize_result_rows(grouped_rows)
        return {
            "status": self._status_from_summary(summary),
            "mode": "from_download",
            "duty_shift": "day",
            "selected_dates": ordered_dates,
            "building_scope": "single",
            "building": "",
            "network_switch_followed_global_setting": False,
            "network_auto_switch_enabled": self._network_auto_switch_enabled(),
            "results": grouped_rows,
            **summary,
        }
