from __future__ import annotations

import copy
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List

from app.config.config_compat_cleanup import sanitize_alarm_export_config
from app.modules.alarm_export.core.field_type_converter import (
    build_field_meta_map,
    convert_alarm_row_by_field_meta,
)
from app.modules.alarm_export.core.transformer import transform_row_to_feishu_fields
from app.modules.alarm_export.repository.alarm_event_repository import AlarmEventRepository
from app.modules.alarm_export.service.alarm_export_resume_store import AlarmExportResumeStore
from app.modules.feishu.service.bitable_target_resolver import BitableTargetResolver, build_bitable_url
from app.modules.report_pipeline.core.entities import AlarmExportSummary, PipelinePhaseResult
from app.shared.logging import build_failure_line, build_success_line
from pipeline_utils import get_app_dir, load_calc_module


class AlarmExportService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = copy.deepcopy(config)

    @staticmethod
    def _build_default_config() -> Dict[str, Any]:
        return {
            "enabled": True,
            "run_with_scheduler": True,
            "snapshot_mode": "clear_and_rebuild",
            "window_days": 35,
            "window_end_time": "00:00:00",
            "skip_levels": [5],
            "alarm_category_default": "真实告警",
            "feishu": {
                "app_token": "SOwsw315aiBJjgkl48ccoxIPntc",
                "table_id": "tblD7hi70s6U6rlU",
                "base_url": "",
                "wiki_url": "",
                "clear_before_upload": True,
                "list_page_size": 500,
                "delete_batch_size": 500,
                "create_batch_size": 200,
                "timeout": 30,
            },
            "db": {
                "port": 3306,
                "user": "root",
                "password": "123456",
                "database": "e_event",
                "table_pattern": "event_{year}_{month:02d}",
                "charset": "utf8mb4",
                "connect_timeout_sec": 5,
                "read_timeout_sec": 20,
                "write_timeout_sec": 20,
                "time_field_mode": "auto",
            },
            "fields": {
                "event_level": "event_level",
                "content": "content",
                "event_source": "event_source",
                "event_time": "event_time",
                "accept_time": "accept_time",
                "process_status": "process_status",
                "accept_user": "accept_user",
                "accept_description": "accept_description",
                "recover_time": "recover_time",
                "recover_status": "is_recover",
                "process_suggestion": "process_suggestion",
                "alarm_type": "alarm_type",
                "trigger_value": "trigger_value",
                "confirm_time": "confirm_time",
                "confirm_user": "confirm_user",
                "confirm_description": "confirm_description",
            },
            "level_mapping": {
                "1": "紧急",
                "2": "严重",
                "3": "重要",
                "4": "次要",
            },
            "resume": {
                "enabled": True,
                "root_dir": "alarm_export_resume",
                "state_file": "alarm_export_resume_state.json",
                "max_retry": 3,
                "retry_interval_sec": 2,
                "reuse_extracted_rows": True,
            },
            "test_db": {
                "enabled": False,
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "password": "123456",
                "database": "e_event",
                "table_mode": "fixed",
                "fixed_table": "event_2026_02",
                "building_label": "测试楼栋",
                "time_field_mode": "auto",
            },
        }

    def _normalize_export_config(self) -> Dict[str, Any]:
        defaults = self._build_default_config()
        raw = sanitize_alarm_export_config(self.config.get("alarm_bitable_export", {}))

        merged = copy.deepcopy(defaults)
        for key in (
            "enabled",
            "run_with_scheduler",
            "snapshot_mode",
            "window_days",
            "window_end_time",
            "skip_levels",
            "alarm_category_default",
        ):
            if key in raw:
                merged[key] = copy.deepcopy(raw[key])

        for section in ("feishu", "db", "fields", "level_mapping", "resume", "test_db"):
            section_raw = raw.get(section, {})
            if isinstance(section_raw, dict):
                merged[section].update(section_raw)

        merged["snapshot_mode"] = str(merged.get("snapshot_mode", "clear_and_rebuild")).strip() or "clear_and_rebuild"
        if not isinstance(merged.get("skip_levels"), list):
            merged["skip_levels"] = [5]

        feishu_cfg = merged["feishu"]
        feishu_cfg["clear_before_upload"] = bool(feishu_cfg.get("clear_before_upload", True))
        feishu_cfg["app_token"] = str(feishu_cfg.get("app_token", "") or "").strip()
        feishu_cfg["table_id"] = str(feishu_cfg.get("table_id", "") or "").strip()
        feishu_cfg["base_url"] = str(feishu_cfg.get("base_url", "") or "").strip()
        feishu_cfg["wiki_url"] = str(feishu_cfg.get("wiki_url", "") or "").strip()
        feishu_cfg["list_page_size"] = max(1, int(feishu_cfg.get("list_page_size", 500)))
        feishu_cfg["delete_batch_size"] = max(1, int(feishu_cfg.get("delete_batch_size", 500)))
        feishu_cfg["create_batch_size"] = max(1, int(feishu_cfg.get("create_batch_size", 200)))
        feishu_cfg["timeout"] = max(1, int(feishu_cfg.get("timeout", 30)))

        resume_cfg = merged["resume"]
        resume_cfg["enabled"] = bool(resume_cfg.get("enabled", True))
        resume_cfg["root_dir"] = (
            str(resume_cfg.get("root_dir", "alarm_export_resume")).strip()
            or "alarm_export_resume"
        )
        resume_cfg["state_file"] = (
            str(resume_cfg.get("state_file", "alarm_export_resume_state.json")).strip()
            or "alarm_export_resume_state.json"
        )
        resume_cfg["max_retry"] = max(1, int(resume_cfg.get("max_retry", 3)))
        resume_cfg["retry_interval_sec"] = max(0, int(resume_cfg.get("retry_interval_sec", 2)))
        resume_cfg["reuse_extracted_rows"] = bool(resume_cfg.get("reuse_extracted_rows", True))

        test_db = merged["test_db"]
        test_db["enabled"] = bool(test_db.get("enabled", False))
        test_db["port"] = max(1, int(test_db.get("port", 3306)))
        test_db["table_mode"] = str(test_db.get("table_mode", "fixed")).strip().lower() or "fixed"
        return merged

    @staticmethod
    def _build_previous_and_current_month_window(now: datetime | None = None) -> tuple[datetime, datetime]:
        end_dt = now or datetime.now()
        if end_dt.month == 1:
            start_dt = datetime(end_dt.year - 1, 12, 1, 0, 0, 0)
        else:
            start_dt = datetime(end_dt.year, end_dt.month - 1, 1, 0, 0, 0)
        return start_dt, end_dt

    def _resolve_feishu_target(self, export_cfg: Dict[str, Any]) -> Dict[str, str]:
        feishu_cfg = export_cfg["feishu"]
        global_feishu = self.config.get("feishu", {})
        app_id = str(global_feishu.get("app_id", "")).strip()
        app_secret = str(global_feishu.get("app_secret", "")).strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: feishu.app_id / feishu.app_secret")

        return BitableTargetResolver(
            app_id=app_id,
            app_secret=app_secret,
            timeout=int(feishu_cfg.get("timeout", 30)),
            request_retry_count=int(global_feishu.get("request_retry_count", 3)),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2)),
        ).resolve(feishu_cfg)

    def _new_feishu_client(
        self,
        export_cfg: Dict[str, Any],
        *,
        resolved_target: Dict[str, str] | None = None,
    ):
        feishu_cfg = export_cfg["feishu"]
        global_feishu = self.config.get("feishu", {})
        app_id = str(global_feishu.get("app_id", "")).strip()
        app_secret = str(global_feishu.get("app_secret", "")).strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: feishu.app_id / feishu.app_secret")

        calc_module = load_calc_module()
        client_cls = getattr(calc_module, "FeishuBitableClient", None)
        if client_cls is None:
            raise RuntimeError("计算脚本缺少 FeishuBitableClient，无法执行告警多维上传")

        target = resolved_target if isinstance(resolved_target, dict) else self._resolve_feishu_target(export_cfg)

        return client_cls(
            app_id=app_id,
            app_secret=app_secret,
            app_token=target["app_token"],
            calc_table_id=target["table_id"],
            attachment_table_id=target["table_id"],
            timeout=int(feishu_cfg.get("timeout", 30)),
            request_retry_count=int(global_feishu.get("request_retry_count", 3)),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2)),
        )

    @staticmethod
    def _build_window_scope_key(end_dt: datetime) -> str:
        return end_dt.strftime("%Y-%m")

    @staticmethod
    def _resume_matches_window(
        state: Dict[str, Any],
        *,
        mode: str,
        window_mode: str,
        window_scope_key: str,
    ) -> bool:
        return (
            str(state.get("mode", "")).strip() == mode
            and str(state.get("window_mode", "")).strip() == window_mode
            and str(state.get("window_scope_key", "")).strip() == window_scope_key
            and str(state.get("status", "")).strip() in {"prepared", "uploading", "failed"}
        )

    @staticmethod
    def _append_phase(
        phase_results: List[PipelinePhaseResult],
        *,
        phase: str,
        status: str,
        started_at: datetime,
        started_perf: float,
        message: str,
        error: str = "",
        metadata: Dict[str, Any] | None = None,
    ) -> None:
        phase_results.append(
            PipelinePhaseResult(
                phase=phase,
                status=status,
                started_at=started_at.strftime("%Y-%m-%d %H:%M:%S"),
                finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                duration_ms=int((time.perf_counter() - started_perf) * 1000),
                message=message,
                error=error,
                metadata=dict(metadata or {}),
            )
        )

    def _build_resume_store(self, export_cfg: Dict[str, Any]) -> AlarmExportResumeStore:
        resume_cfg = export_cfg.get("resume", {})
        runtime_paths = self.config.get("paths", {})
        runtime_state_root = str(runtime_paths.get("runtime_state_root", "")).strip() if isinstance(runtime_paths, dict) else ""
        return AlarmExportResumeStore(
            app_dir=get_app_dir(),
            root_dir=str(resume_cfg.get("root_dir", "alarm_export_resume")),
            state_file=str(resume_cfg.get("state_file", "alarm_export_resume_state.json")),
            runtime_state_root=runtime_state_root,
        )

    @staticmethod
    def _clear_feishu_table_with_progress(
        client: Any,
        *,
        table_id: str,
        feishu_cfg: Dict[str, Any],
        emit_log: Callable[[str], None],
        source: str,
    ) -> int:
        emit_log(f"[{source}] 正在清空目标表旧记录...")
        last_logged_deleted = -1
        last_logged_total = -1

        def _progress_callback(deleted: int, total: int) -> None:
            nonlocal last_logged_deleted, last_logged_total
            normalized_deleted = max(0, int(deleted or 0))
            normalized_total = max(0, int(total or 0))
            if normalized_total <= 0:
                return
            if (
                normalized_deleted == last_logged_deleted
                and normalized_total == last_logged_total
            ):
                return
            last_logged_deleted = normalized_deleted
            last_logged_total = normalized_total
            emit_log(f"[{source}] 清空旧记录进度: {normalized_deleted}/{normalized_total}")

        clear_kwargs = {
            "table_id": table_id,
            "list_page_size": int(feishu_cfg.get("list_page_size", 500)),
            "delete_batch_size": int(feishu_cfg.get("delete_batch_size", 500)),
        }
        try:
            cleared_count = int(
                client.clear_table(
                    **clear_kwargs,
                    progress_callback=_progress_callback,
                )
            )
        except TypeError:
            cleared_count = int(client.clear_table(**clear_kwargs))
        emit_log(f"[{source}] 已清空旧记录: {cleared_count}")
        return cleared_count

    @staticmethod
    def _build_target_descriptor(resolved_target: Dict[str, str]) -> Dict[str, str]:
        table_id = str(resolved_target.get("table_id", "")).strip()
        return {
            "resolved_from": str(resolved_target.get("resolved_from", "")).strip(),
            "source_url": str(resolved_target.get("source_url", "")).strip(),
            "app_token": str(resolved_target.get("app_token", "")).strip(),
            "table_id": table_id,
            "bitable_url": str(resolved_target.get("bitable_url", "")).strip()
            or build_bitable_url(
                str(resolved_target.get("app_token", "")).strip(),
                table_id,
            ),
            "wiki_node_token": str(resolved_target.get("wiki_node_token", "")).strip(),
            "wiki_obj_type": str(resolved_target.get("wiki_obj_type", "")).strip(),
        }

    def query_rows_for_bridge(
        self,
        emit_log: Callable[[str], None] = print,
        source: str = "告警多维上传",
    ) -> Dict[str, Any]:
        export_cfg = self._normalize_export_config()
        if not bool(export_cfg.get("enabled", True)):
            emit_log(f"[{source}] 已禁用，跳过执行")
            return {"status": "skipped", "prepared_rows": []}

        phase_results: List[PipelinePhaseResult] = []
        run_started_at = datetime.now()
        run_started_perf = time.perf_counter()
        mode = "test_db" if bool(export_cfg.get("test_db", {}).get("enabled", False)) else "prod_sites"
        window_mode = "previous_and_current_month_to_now"
        start_dt, end_dt = self._build_previous_and_current_month_window()
        window_scope_key = self._build_window_scope_key(end_dt)
        window_start_text = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        window_end_text = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        date_tag = f"{start_dt:%Y-%m-%d}~{end_dt:%Y-%m-%d}"
        fields_cfg = export_cfg.get("fields", {})
        level_mapping = export_cfg.get("level_mapping", {})
        skip_levels = export_cfg.get("skip_levels", [5])
        alarm_category_default = str(export_cfg.get("alarm_category_default", "真实告警")).strip() or "真实告警"
        snapshot_mode = str(export_cfg.get("snapshot_mode", "clear_and_rebuild")).strip()
        network_cfg = self.config.get("network", {})
        internal_ssid = str(network_cfg.get("internal_ssid", "")).strip()

        emit_log(f"[{source}] 导出模式: {mode}")
        emit_log(f"[{source}] 查询窗口: {window_start_text} ~ {window_end_text} (window_mode={window_mode})")

        if snapshot_mode != "clear_and_rebuild":
            raise ValueError(f"不支持的 snapshot_mode: {snapshot_mode}")
        if mode != "test_db":
            self._switch_wifi_if_needed(
                target_ssid=internal_ssid,
                stage="数据库查询前",
                emit_log=emit_log,
            )

        query_started_at = datetime.now()
        query_started_perf = time.perf_counter()
        repository = AlarmEventRepository(config=self.config, export_cfg=export_cfg)
        query_result = repository.query_events(start_dt=start_dt, end_dt=end_dt, emit_log=emit_log)
        raw_rows = query_result.get("rows", [])
        failed_buildings = query_result.get("failed_buildings", [])
        succeeded_buildings = query_result.get("succeeded_buildings", [])
        raw_count = len(raw_rows)
        skipped_rows = 0
        prepared_rows: List[Dict[str, Any]] = []

        for item in raw_rows:
            if not isinstance(item, dict):
                continue
            building = str(item.get("building", "")).strip()
            row = item.get("row", {})
            if not isinstance(row, dict):
                continue
            fields = transform_row_to_feishu_fields(
                row=row,
                building=building,
                fields_cfg=fields_cfg,
                level_mapping=level_mapping,
                skip_levels=skip_levels,
                alarm_category_default=alarm_category_default,
            )
            if fields is None:
                skipped_rows += 1
                continue
            prepared_rows.append(fields)

        self._append_phase(
            phase_results,
            phase="数据库查询与转换",
            status="success",
            started_at=query_started_at,
            started_perf=query_started_perf,
            message=(
                f"原始记录={raw_count}, 过滤记录={skipped_rows}, 待转换={len(prepared_rows)}, "
                f"楼栋成功={len(succeeded_buildings)}, 楼栋失败={len(failed_buildings)}"
            ),
        )
        emit_log(
            f"[{source}] 数据汇总: 楼栋成功={len(succeeded_buildings)}, 楼栋失败={len(failed_buildings)}, "
            f"原始记录={raw_count}, 过滤记录={skipped_rows}, 待上传={len(prepared_rows)}"
        )

        if not prepared_rows and failed_buildings:
            stage = "数据库查询(测试库)" if mode == "test_db" else "数据库查询"
            for item in failed_buildings:
                emit_log(
                    build_failure_line(
                        feature="告警多维上传",
                        stage=stage,
                        building=str(item.get("building", "")).strip() or "-",
                        upload_date=date_tag,
                        error=str(item.get("error", "")).strip() or "未知错误",
                    )
                )
            raise RuntimeError(
                "告警多维上传失败: 所有楼栋查询失败或无有效记录 -> "
                + "; ".join(f"{x.get('building', '-')}: {x.get('error', '-')}" for x in failed_buildings)
            )

        return {
            "status": "ok" if not failed_buildings else "partial_failed",
            "mode": mode,
            "window_mode": window_mode,
            "window_scope_key": window_scope_key,
            "window_start": window_start_text,
            "window_end": window_end_text,
            "date_tag": date_tag,
            "snapshot_mode": snapshot_mode,
            "raw_count": raw_count,
            "skipped_count": skipped_rows,
            "prepared_row_count": len(prepared_rows),
            "prepared_rows": prepared_rows,
            "success_buildings": list(succeeded_buildings),
            "failed_buildings": list(failed_buildings),
            "phase_results": [item.to_dict() for item in phase_results],
            "duration_ms": int((time.perf_counter() - run_started_perf) * 1000),
            "started_at": run_started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "retryable": bool(failed_buildings),
        }

    def continue_from_bridge_rows(
        self,
        *,
        bridge_payload: Dict[str, Any],
        emit_log: Callable[[str], None] = print,
        source: str = "告警多维上传",
    ) -> Dict[str, Any]:
        export_cfg = self._normalize_export_config()
        if not bool(export_cfg.get("enabled", True)):
            emit_log(f"[{source}] 已禁用，跳过执行")
            return {"status": "skipped"}

        bridge_data = dict(bridge_payload or {})
        prepared_rows = [
            item for item in bridge_data.get("prepared_rows", [])
            if isinstance(item, dict)
        ]
        mode = str(bridge_data.get("mode", "")).strip() or "prod_sites"
        window_mode = str(bridge_data.get("window_mode", "")).strip() or "previous_and_current_month_to_now"
        window_scope_key = str(bridge_data.get("window_scope_key", "")).strip()
        window_start_text = str(bridge_data.get("window_start", "")).strip()
        window_end_text = str(bridge_data.get("window_end", "")).strip()
        date_tag = str(bridge_data.get("date_tag", "")).strip()
        snapshot_mode = str(bridge_data.get("snapshot_mode", "clear_and_rebuild")).strip() or "clear_and_rebuild"
        raw_count = int(bridge_data.get("raw_count", 0) or 0)
        skipped_rows = int(bridge_data.get("skipped_count", 0) or 0)
        succeeded_buildings = [
            str(item or "").strip()
            for item in bridge_data.get("success_buildings", [])
            if str(item or "").strip()
        ]
        failed_buildings = [
            item for item in bridge_data.get("failed_buildings", [])
            if isinstance(item, dict)
        ]

        run_started_at = datetime.now()
        run_started_perf = time.perf_counter()
        phase_results: List[PipelinePhaseResult] = []
        for item in bridge_data.get("phase_results", []):
            if not isinstance(item, dict):
                continue
            phase_results.append(
                PipelinePhaseResult(
                    phase=str(item.get("phase", "数据库查询与转换")),
                    status=str(item.get("status", "success")),
                    started_at=str(item.get("started_at", "")),
                    finished_at=str(item.get("finished_at", "")),
                    duration_ms=int(item.get("duration_ms", 0) or 0),
                    message=str(item.get("message", "")),
                    error=str(item.get("error", "")),
                    metadata=dict(item.get("metadata", {})) if isinstance(item.get("metadata", {}), dict) else {},
                )
            )

        feishu_cfg = export_cfg["feishu"]
        batch_size = int(feishu_cfg.get("create_batch_size", 200))
        network_cfg = self.config.get("network", {})
        external_ssid = str(network_cfg.get("external_ssid", "")).strip()
        resume_cfg = export_cfg.get("resume", {})
        resume_enabled = bool(resume_cfg.get("enabled", True))
        resume_store = self._build_resume_store(export_cfg)
        resume_state: Dict[str, Any] | None = None
        run_id = f"alarm_{uuid.uuid4().hex[:12]}"

        resolved_target = self._resolve_feishu_target(export_cfg)
        client = self._new_feishu_client(export_cfg, resolved_target=resolved_target)
        table_id = str(resolved_target.get("table_id", "")).strip()
        target_descriptor = self._build_target_descriptor(resolved_target)

        typed_rows: List[Dict[str, Any]] = []
        cleared_count = 0
        next_index = 0
        nullified_total = 0
        unsupported_total = 0
        dropped_after_type = 0

        upload_started_at = datetime.now()
        upload_started_perf = time.perf_counter()
        self._switch_wifi_if_needed(
            target_ssid=external_ssid,
            stage="上传前",
            emit_log=emit_log,
        )

        try:
            if prepared_rows:
                meta_started_at = datetime.now()
                meta_started_perf = time.perf_counter()
                try:
                    table_fields = client.list_fields(table_id=table_id, page_size=500)
                except Exception as exc:  # noqa: BLE001
                    emit_log(
                        build_failure_line(
                            feature="告警多维上传",
                            stage="飞书字段元数据加载",
                            upload_date=date_tag,
                            run_id=run_id,
                            error=str(exc),
                        )
                    )
                    raise

                field_meta_map = build_field_meta_map(table_fields)
                emit_log(f"[{source}] 字段元数据加载完成: fields={len(field_meta_map)}")
                for row_fields in prepared_rows:
                    converted, stats = convert_alarm_row_by_field_meta(row_fields, field_meta_map, tz_offset_hours=8)
                    nullified_total += int(stats.get("nullified_fields", 0))
                    unsupported_total += int(stats.get("unsupported_fields", 0))
                    if converted:
                        typed_rows.append(converted)
                    else:
                        dropped_after_type += 1

                self._append_phase(
                    phase_results,
                    phase="飞书字段类型转换",
                    status="success",
                    started_at=meta_started_at,
                    started_perf=meta_started_perf,
                    message=(
                        f"转换后记录={len(typed_rows)}, 置空字段={nullified_total}, "
                        f"不支持字段={unsupported_total}, 丢弃记录={dropped_after_type}"
                    ),
                )
                emit_log(
                    f"[{source}] 类型转换统计: raw={len(prepared_rows)}, typed={len(typed_rows)}, "
                    f"nullified_fields={nullified_total}, unsupported_fields={unsupported_total}, "
                    f"dropped_rows={dropped_after_type}"
                )

                if resume_enabled:
                    rows_path = resume_store.save_rows(run_id=run_id, rows=typed_rows)
                    resume_state = {
                        "run_id": run_id,
                        "status": "prepared",
                        "mode": mode,
                        "window_mode": window_mode,
                        "window_scope_key": window_scope_key,
                        "window_start": window_start_text,
                        "window_end": window_end_text,
                        "date_tag": date_tag,
                        "snapshot_mode": snapshot_mode,
                        "table_id": table_id,
                        "rows_file": str(rows_path),
                        "total_rows": len(typed_rows),
                        "next_index": 0,
                        "batch_size": batch_size,
                        "cleared": False,
                        "cleared_count": 0,
                        "raw_count": raw_count,
                        "skipped_count": skipped_rows,
                        "success_buildings": succeeded_buildings,
                        "failed_buildings": failed_buildings,
                        "last_error": "",
                    }
                    resume_state = resume_store.save_state(resume_state)
                    emit_log(f"[{source}] 已写入断点状态: run_id={run_id}, rows={len(typed_rows)}")

            emit_log(
                f"[{source}] 开始飞书上传: table_id={table_id}, "
                f"clear_before_upload={bool(feishu_cfg.get('clear_before_upload', True))}, "
                f"待上传={len(typed_rows)}, run_id={run_id}"
            )

            if bool(feishu_cfg.get("clear_before_upload", True)):
                cleared_count = self._clear_feishu_table_with_progress(
                    client,
                    table_id=table_id,
                    feishu_cfg=feishu_cfg,
                    emit_log=emit_log,
                    source=source,
                )
                if resume_state:
                    resume_state["cleared"] = True
                    resume_state["cleared_count"] = cleared_count
                    resume_state["status"] = "uploading"
                    resume_state["last_error"] = ""
                    resume_state = resume_store.save_state(resume_state)

            total_rows = len(typed_rows)
            if total_rows:
                if next_index < 0 or next_index > total_rows:
                    next_index = 0
                for start in range(next_index, total_rows, batch_size):
                    end = min(start + batch_size, total_rows)
                    batch = typed_rows[start:end]
                    client.batch_create_records(table_id=table_id, fields_list=batch, batch_size=batch_size)
                    if resume_state:
                        resume_state["next_index"] = end
                        resume_state["status"] = "uploading"
                        resume_state["last_error"] = ""
                        resume_state = resume_store.save_state(resume_state)
                    emit_log(f"[{source}] 上传进度: {end}/{total_rows}")
            else:
                emit_log(f"[{source}] 无待上传记录，已完成清表快照")
        except Exception as exc:  # noqa: BLE001
            if resume_state:
                resume_state["status"] = "failed"
                resume_state["last_error"] = str(exc)
                resume_state = resume_store.save_state(resume_state)
            emit_log(
                build_failure_line(
                    feature="告警多维上传",
                    stage="飞书上传",
                    upload_date=date_tag,
                    run_id=run_id,
                    error=str(exc),
                )
            )
            raise RuntimeError(f"run_id={run_id}; 上传中断，可继续续传。{exc}") from exc

        self._append_phase(
            phase_results,
            phase="飞书清表与写入",
            status="success",
            started_at=upload_started_at,
            started_perf=upload_started_perf,
            message=f"清空={cleared_count}, 上传={len(typed_rows)}",
        )
        if resume_state:
            resume_store.clear(resume_state)

        emit_log(
            build_success_line(
                feature="告警多维上传",
                stage="飞书上传完成",
                upload_date=date_tag,
                run_id=run_id,
                detail=f"模式={mode}, 清空={cleared_count}, 上传={len(typed_rows)}, 失败楼栋={len(failed_buildings)}",
            )
        )

        stage = "数据库查询(测试库)" if mode == "test_db" else "数据库查询"
        for item in failed_buildings:
            emit_log(
                build_failure_line(
                    feature="告警多维上传",
                    stage=stage,
                    building=str(item.get("building", "")).strip() or "-",
                    upload_date=date_tag,
                    run_id=run_id,
                    error=str(item.get("error", "")).strip() or "未知错误",
                )
            )

        summary_model = AlarmExportSummary(
            uploaded_count=len(typed_rows),
            raw_count=raw_count,
            skipped_count=skipped_rows,
            success_buildings=succeeded_buildings,
            failed_buildings=failed_buildings,
        )
        status = "ok" if not failed_buildings else "partial_failed"
        return {
            "status": status,
            "mode": mode,
            "run_id": run_id,
            "window_start": window_start_text,
            "window_end": window_end_text,
            "snapshot_mode": snapshot_mode,
            "cleared_count": cleared_count,
            "uploaded_count": len(typed_rows),
            "raw_count": raw_count,
            "skipped_count": skipped_rows,
            "success_buildings": succeeded_buildings,
            "failed_buildings": failed_buildings,
            "summary_v2": summary_model.to_dict(),
            "phase_results": [x.to_dict() for x in phase_results],
            "duration_ms": int((time.perf_counter() - run_started_perf) * 1000),
            "started_at": run_started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "retryable": bool(failed_buildings),
            "resume_pending": False,
            "target": target_descriptor,
        }

    def _switch_wifi_if_needed(
        self,
        *,
        target_ssid: str,
        stage: str,
        emit_log: Callable[[str], None],
    ) -> None:
        emit_log(f"[告警多维上传] 网络切换功能已移除，按当前网络继续执行: stage={stage}")

    def run(self, emit_log: Callable[[str], None] = print, source: str = "告警多维上传") -> Dict[str, Any]:
        run_started_at = datetime.now()
        run_started_perf = time.perf_counter()
        phase_results: List[PipelinePhaseResult] = []

        export_cfg = self._normalize_export_config()
        if not bool(export_cfg.get("enabled", True)):
            emit_log(f"[{source}] 已禁用，跳过执行")
            return {"status": "skipped", "reason": "disabled"}

        mode = "test_db" if bool(export_cfg.get("test_db", {}).get("enabled", False)) else "prod_sites"
        window_mode = "previous_and_current_month_to_now"
        start_dt, end_dt = self._build_previous_and_current_month_window()
        window_scope_key = self._build_window_scope_key(end_dt)
        window_start_text = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        window_end_text = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        date_tag = f"{start_dt:%Y-%m-%d}~{end_dt:%Y-%m-%d}"

        emit_log(f"[{source}] 导出模式: {mode}")
        emit_log(f"[{source}] 查询窗口: {window_start_text} ~ {window_end_text} (window_mode={window_mode})")

        fields_cfg = export_cfg.get("fields", {})
        level_mapping = export_cfg.get("level_mapping", {})
        skip_levels = export_cfg.get("skip_levels", [5])
        alarm_category_default = str(export_cfg.get("alarm_category_default", "真实告警")).strip() or "真实告警"
        feishu_cfg = export_cfg["feishu"]
        batch_size = int(feishu_cfg.get("create_batch_size", 200))
        snapshot_mode = str(export_cfg.get("snapshot_mode", "clear_and_rebuild")).strip()
        network_cfg = self.config.get("network", {})
        internal_ssid = str(network_cfg.get("internal_ssid", "")).strip()
        external_ssid = str(network_cfg.get("external_ssid", "")).strip()
        if snapshot_mode != "clear_and_rebuild":
            raise ValueError(f"不支持的 snapshot_mode: {snapshot_mode}")

        resume_cfg = export_cfg.get("resume", {})
        resume_enabled = bool(resume_cfg.get("enabled", True))
        runtime_paths = self.config.get("paths", {})
        runtime_state_root = str(runtime_paths.get("runtime_state_root", "")).strip() if isinstance(runtime_paths, dict) else ""
        resume_store = AlarmExportResumeStore(
            app_dir=get_app_dir(),
            root_dir=str(resume_cfg.get("root_dir", "alarm_export_resume")),
            state_file=str(resume_cfg.get("state_file", "alarm_export_resume_state.json")),
            runtime_state_root=runtime_state_root,
        )
        resume_state: Dict[str, Any] | None = None
        run_id = f"alarm_{uuid.uuid4().hex[:12]}"

        raw_count = 0
        skipped_rows = 0
        transformed_rows: List[Dict[str, Any]] = []
        typed_rows: List[Dict[str, Any]] = []
        succeeded_buildings: List[str] = []
        failed_buildings: List[Dict[str, Any]] = []
        cleared_count = 0
        next_index = 0
        nullified_total = 0
        unsupported_total = 0
        dropped_after_type = 0

        stale_resume = False
        if resume_enabled and bool(resume_cfg.get("reuse_extracted_rows", True)):
            loaded = resume_store.load_state()
            if isinstance(loaded, dict):
                if self._resume_matches_window(
                    loaded,
                    mode=mode,
                    window_mode=window_mode,
                    window_scope_key=window_scope_key,
                ):
                    rows_file = str(loaded.get("rows_file", "")).strip()
                    loaded_rows = resume_store.load_rows(rows_file)
                    if loaded_rows:
                        resume_state = loaded
                        run_id = str(loaded.get("run_id", "")).strip() or run_id
                        typed_rows = loaded_rows
                        raw_count = int(loaded.get("raw_count", 0))
                        skipped_rows = int(loaded.get("skipped_count", 0))
                        succeeded_buildings = list(loaded.get("success_buildings", []))
                        failed_buildings = list(loaded.get("failed_buildings", []))
                        cleared_count = int(loaded.get("cleared_count", 0))
                        next_index = max(0, int(loaded.get("next_index", 0)))
                        emit_log(
                            f"[{source}] 发现未完成任务，开始续传: run_id={run_id}, "
                            f"next_index={next_index}, total={len(typed_rows)}"
                        )
                        self._append_phase(
                            phase_results,
                            phase="断点续传加载",
                            status="success",
                            started_at=run_started_at,
                            started_perf=run_started_perf,
                            message=f"run_id={run_id}, 已恢复待上传记录={len(typed_rows)}",
                        )
                    else:
                        stale_resume = True
                else:
                    stale_resume = True
            if stale_resume:
                emit_log(f"[{source}] 发现过期断点状态，已忽略并重建快照")
                resume_store.clear(loaded if isinstance(loaded, dict) else None)

        resolved_target = self._resolve_feishu_target(export_cfg)
        client = self._new_feishu_client(export_cfg, resolved_target=resolved_target)
        table_id = str(resolved_target.get("table_id", "")).strip()
        target_descriptor = {
            "resolved_from": str(resolved_target.get("resolved_from", "")).strip(),
            "source_url": str(resolved_target.get("source_url", "")).strip(),
            "app_token": str(resolved_target.get("app_token", "")).strip(),
            "table_id": table_id,
            "bitable_url": str(resolved_target.get("bitable_url", "")).strip()
            or build_bitable_url(
                str(resolved_target.get("app_token", "")).strip(),
                table_id,
            ),
            "wiki_node_token": str(resolved_target.get("wiki_node_token", "")).strip(),
            "wiki_obj_type": str(resolved_target.get("wiki_obj_type", "")).strip(),
        }

        if not typed_rows:
            if mode != "test_db":
                self._switch_wifi_if_needed(
                    target_ssid=internal_ssid,
                    stage="数据库查询前",
                    emit_log=emit_log,
                )

            query_started_at = datetime.now()
            query_started_perf = time.perf_counter()
            repository = AlarmEventRepository(config=self.config, export_cfg=export_cfg)
            query_result = repository.query_events(start_dt=start_dt, end_dt=end_dt, emit_log=emit_log)
            raw_rows = query_result.get("rows", [])
            failed_buildings = query_result.get("failed_buildings", [])
            succeeded_buildings = query_result.get("succeeded_buildings", [])
            raw_count = len(raw_rows)

            for item in raw_rows:
                if not isinstance(item, dict):
                    continue
                building = str(item.get("building", "")).strip()
                row = item.get("row", {})
                if not isinstance(row, dict):
                    continue
                fields = transform_row_to_feishu_fields(
                    row=row,
                    building=building,
                    fields_cfg=fields_cfg,
                    level_mapping=level_mapping,
                    skip_levels=skip_levels,
                    alarm_category_default=alarm_category_default,
                )
                if fields is None:
                    skipped_rows += 1
                    continue
                transformed_rows.append(fields)

            self._append_phase(
                phase_results,
                phase="数据库查询与转换",
                status="success",
                started_at=query_started_at,
                started_perf=query_started_perf,
                message=(
                    f"原始记录={raw_count}, 过滤记录={skipped_rows}, 待转换={len(transformed_rows)}, "
                    f"楼栋成功={len(succeeded_buildings)}, 楼栋失败={len(failed_buildings)}"
                ),
            )
            emit_log(
                f"[{source}] 数据汇总: 楼栋成功={len(succeeded_buildings)}, 楼栋失败={len(failed_buildings)}, "
                f"原始记录={raw_count}, 过滤记录={skipped_rows}, 待上传={len(transformed_rows)}"
            )

        if not transformed_rows and not typed_rows and failed_buildings:
            stage = "数据库查询(测试库)" if mode == "test_db" else "数据库查询"
            for item in failed_buildings:
                emit_log(
                    build_failure_line(
                        feature="告警多维上传",
                        stage=stage,
                        building=str(item.get("building", "")).strip() or "-",
                        upload_date=date_tag,
                        run_id=run_id,
                        error=str(item.get("error", "")).strip() or "未知错误",
                    )
                )
            raise RuntimeError(
                "告警多维上传失败: 所有楼栋查询失败或无有效记录 -> "
                + "; ".join(f"{x.get('building', '-')}: {x.get('error', '-')}" for x in failed_buildings)
            )

        upload_started_at = datetime.now()
        upload_started_perf = time.perf_counter()
        self._switch_wifi_if_needed(
            target_ssid=external_ssid,
            stage="上传前",
            emit_log=emit_log,
        )

        try:
            if not typed_rows and transformed_rows:
                meta_started_at = datetime.now()
                meta_started_perf = time.perf_counter()
                try:
                    table_fields = client.list_fields(table_id=table_id, page_size=500)
                except Exception as exc:  # noqa: BLE001
                    emit_log(
                        build_failure_line(
                            feature="告警多维上传",
                            stage="飞书字段元数据加载",
                            upload_date=date_tag,
                            run_id=run_id,
                            error=str(exc),
                        )
                    )
                    raise

                field_meta_map = build_field_meta_map(table_fields)
                emit_log(f"[{source}] 字段元数据加载完成: fields={len(field_meta_map)}")

                for row_fields in transformed_rows:
                    converted, stats = convert_alarm_row_by_field_meta(row_fields, field_meta_map, tz_offset_hours=8)
                    nullified_total += int(stats.get("nullified_fields", 0))
                    unsupported_total += int(stats.get("unsupported_fields", 0))
                    if converted:
                        typed_rows.append(converted)
                    else:
                        dropped_after_type += 1

                self._append_phase(
                    phase_results,
                    phase="飞书字段类型转换",
                    status="success",
                    started_at=meta_started_at,
                    started_perf=meta_started_perf,
                    message=(
                        f"转换后记录={len(typed_rows)}, 置空字段={nullified_total}, "
                        f"不支持字段={unsupported_total}, 丢弃记录={dropped_after_type}"
                    ),
                )
                emit_log(
                    f"[{source}] 类型转换统计: raw={len(transformed_rows)}, typed={len(typed_rows)}, "
                    f"nullified_fields={nullified_total}, unsupported_fields={unsupported_total}, "
                    f"dropped_rows={dropped_after_type}"
                )

                if resume_enabled:
                    rows_path = resume_store.save_rows(run_id=run_id, rows=typed_rows)
                    resume_state = {
                        "run_id": run_id,
                        "status": "prepared",
                        "mode": mode,
                        "window_mode": window_mode,
                        "window_scope_key": window_scope_key,
                        "window_start": window_start_text,
                        "window_end": window_end_text,
                        "date_tag": date_tag,
                        "snapshot_mode": snapshot_mode,
                        "table_id": table_id,
                        "rows_file": str(rows_path),
                        "total_rows": len(typed_rows),
                        "next_index": 0,
                        "batch_size": batch_size,
                        "cleared": False,
                        "cleared_count": 0,
                        "raw_count": raw_count,
                        "skipped_count": skipped_rows,
                        "success_buildings": succeeded_buildings,
                        "failed_buildings": failed_buildings,
                        "last_error": "",
                    }
                    resume_state = resume_store.save_state(resume_state)
                    emit_log(f"[{source}] 已写入断点状态: run_id={run_id}, rows={len(typed_rows)}")

            emit_log(
                f"[{source}] 开始飞书上传: table_id={table_id}, "
                f"clear_before_upload={bool(feishu_cfg.get('clear_before_upload', True))}, "
                f"待上传={len(typed_rows)}, run_id={run_id}"
            )

            if bool(feishu_cfg.get("clear_before_upload", True)):
                already_cleared = bool(resume_state and resume_state.get("cleared", False))
                if already_cleared:
                    cleared_count = int(resume_state.get("cleared_count", 0))
                    emit_log(f"[{source}] 续传任务已清空旧记录，跳过重复清表: cleared={cleared_count}")
                else:
                    cleared_count = self._clear_feishu_table_with_progress(
                        client,
                        table_id=table_id,
                        feishu_cfg=feishu_cfg,
                        emit_log=emit_log,
                        source=source,
                    )
                    if resume_state:
                        resume_state["cleared"] = True
                        resume_state["cleared_count"] = cleared_count
                        resume_state["status"] = "uploading"
                        resume_state["last_error"] = ""
                        resume_state = resume_store.save_state(resume_state)

            total_rows = len(typed_rows)
            if total_rows:
                if next_index < 0 or next_index > total_rows:
                    next_index = 0
                if next_index > 0:
                    emit_log(f"[{source}] 从断点继续上传: offset={next_index}/{total_rows}")
                for start in range(next_index, total_rows, batch_size):
                    end = min(start + batch_size, total_rows)
                    batch = typed_rows[start:end]
                    client.batch_create_records(table_id=table_id, fields_list=batch, batch_size=batch_size)
                    if resume_state:
                        resume_state["next_index"] = end
                        resume_state["status"] = "uploading"
                        resume_state["last_error"] = ""
                        resume_state = resume_store.save_state(resume_state)
                    emit_log(f"[{source}] 上传进度: {end}/{total_rows}")
            else:
                emit_log(f"[{source}] 无待上传记录，已完成清表快照")
        except Exception as exc:  # noqa: BLE001
            if resume_state:
                resume_state["status"] = "failed"
                resume_state["last_error"] = str(exc)
                resume_state = resume_store.save_state(resume_state)
            emit_log(
                build_failure_line(
                    feature="告警多维上传",
                    stage="飞书上传",
                    upload_date=date_tag,
                    run_id=run_id,
                    error=str(exc),
                )
            )
            raise RuntimeError(f"run_id={run_id}; 上传中断，可继续续传。{exc}") from exc

        self._append_phase(
            phase_results,
            phase="飞书清表与写入",
            status="success",
            started_at=upload_started_at,
            started_perf=upload_started_perf,
            message=f"清空={cleared_count}, 上传={len(typed_rows)}",
        )

        if resume_state:
            resume_store.clear(resume_state)

        emit_log(
            build_success_line(
                feature="告警多维上传",
                stage="飞书上传完成",
                upload_date=date_tag,
                run_id=run_id,
                detail=f"模式={mode}, 清空={cleared_count}, 上传={len(typed_rows)}, 失败楼栋={len(failed_buildings)}",
            )
        )

        stage = "数据库查询(测试库)" if mode == "test_db" else "数据库查询"
        for item in failed_buildings:
            emit_log(
                build_failure_line(
                    feature="告警多维上传",
                    stage=stage,
                    building=str(item.get("building", "")).strip() or "-",
                    upload_date=date_tag,
                    run_id=run_id,
                    error=str(item.get("error", "")).strip() or "未知错误",
                )
            )

        summary_model = AlarmExportSummary(
            uploaded_count=len(typed_rows),
            raw_count=raw_count,
            skipped_count=skipped_rows,
            success_buildings=succeeded_buildings,
            failed_buildings=failed_buildings,
        )
        status = "ok" if not failed_buildings else "partial_failed"
        return {
            "status": status,
            "mode": mode,
            "run_id": run_id,
            "window_start": window_start_text,
            "window_end": window_end_text,
            "snapshot_mode": snapshot_mode,
            "cleared_count": cleared_count,
            "uploaded_count": len(typed_rows),
            "raw_count": raw_count,
            "skipped_count": skipped_rows,
            "success_buildings": succeeded_buildings,
            "failed_buildings": failed_buildings,
            "summary_v2": summary_model.to_dict(),
            "phase_results": [x.to_dict() for x in phase_results],
            "duration_ms": int((time.perf_counter() - run_started_perf) * 1000),
            "started_at": run_started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "retryable": bool(failed_buildings),
            "resume_pending": False,
            "target": target_descriptor,
        }
