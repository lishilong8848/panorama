from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Callable, Dict, List

from handover_log_module.core.models import BuildingResult, RunSummary
from handover_log_module.core.shift_window import build_duty_window, format_duty_date_text, parse_duty_date
from handover_log_module.repository.alarm_repository import AlarmRepository
from handover_log_module.repository.change_management_repository import ChangeRowsByBuilding
from handover_log_module.repository.event_sections_repository import EventQueryByBuilding
from handover_log_module.repository.exercise_management_repository import ExerciseRowsByBuilding
from handover_log_module.repository.maintenance_management_repository import MaintenanceRowsByBuilding
from handover_log_module.repository.other_important_work_repository import OtherImportantWorkRowsByBuilding
from handover_log_module.repository.shift_roster_repository import (
    ShiftRosterAssignment,
    ShiftRosterRepository,
)
from handover_log_module.service.change_management_payload_builder import ChangeManagementPayloadBuilder
from handover_log_module.service.day_metric_bitable_export_service import DayMetricBitableExportService
from handover_log_module.service.event_category_payload_builder import EventCategoryPayloadBuilder
from handover_log_module.service.exercise_management_payload_builder import ExerciseManagementPayloadBuilder
from handover_log_module.service.handover_download_service import HandoverDownloadService
from handover_log_module.service.handover_cloud_sheet_sync_service import HandoverCloudSheetSyncService
from handover_log_module.service.handover_extract_service import HandoverExtractService
from handover_log_module.service.handover_fill_service import HandoverFillService
from handover_log_module.service.handover_source_file_cache_service import HandoverSourceFileCacheService
from handover_log_module.service.maintenance_management_payload_builder import MaintenanceManagementPayloadBuilder
from handover_log_module.service.other_important_work_payload_builder import OtherImportantWorkPayloadBuilder
from handover_log_module.service.review_session_service import ReviewSessionService
from handover_log_module.service.source_data_attachment_bitable_export_service import (
    SourceDataAttachmentBitableExportService,
)


def _norm(value: Any, default: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else default


def _followup_status_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "pending_review": "待确认后上传",
        "ok": "成功",
        "success": "成功",
        "skipped": "已跳过",
        "failed": "失败",
    }
    return mapping.get(text, text or "-")


def _followup_reason_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "disabled": "配置已禁用",
        "missing_duty_context": "缺少班次上下文",
        "night_shift_disabled": "夜班上传已禁用",
        "await_all_confirmed": "等待五个楼栋全部确认",
        "already_uploaded": "已按当前版本完成上传",
        "missing_source_file": "源数据文件不存在",
        "missing_source_file_cache": "源文件缓存不存在",
        "list_existing_failed": "读取旧记录失败",
        "upload_error": "上传失败",
    }
    return mapping.get(text, text or "-")


@dataclass
class HandoverQueryContext:
    duty_date: str
    duty_shift: str
    target_buildings: List[str]
    roster_assignments: Dict[str, ShiftRosterAssignment] = field(default_factory=dict)
    event_query_by_building: EventQueryByBuilding | None = None
    change_rows_by_building: ChangeRowsByBuilding | None = None
    exercise_rows_by_building: ExerciseRowsByBuilding | None = None
    maintenance_rows_by_building: MaintenanceRowsByBuilding | None = None
    other_important_work_rows_by_building: OtherImportantWorkRowsByBuilding | None = None


class HandoverOrchestrator:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self._download_service = HandoverDownloadService(config)
        self._cloud_sheet_sync_service = HandoverCloudSheetSyncService(config)
        self._extract_service = HandoverExtractService(config)
        self._fill_service = HandoverFillService(config)
        self._alarm_repo = AlarmRepository(config)
        self._shift_roster_repo = ShiftRosterRepository(config)
        self._event_category_builder = EventCategoryPayloadBuilder(config)
        self._change_management_builder = ChangeManagementPayloadBuilder(
            config,
            shift_roster_repo=self._shift_roster_repo,
        )
        self._exercise_management_builder = ExerciseManagementPayloadBuilder(config)
        self._maintenance_management_builder = MaintenanceManagementPayloadBuilder(
            config,
            shift_roster_repo=self._shift_roster_repo,
        )
        self._other_important_work_builder = OtherImportantWorkPayloadBuilder(
            config,
            shift_roster_repo=self._shift_roster_repo,
        )
        self._day_metric_export_service = DayMetricBitableExportService(config)
        self._source_data_attachment_export_service = SourceDataAttachmentBitableExportService(config)
        self._source_file_cache_service = HandoverSourceFileCacheService(config)
        self._review_session_service = ReviewSessionService(config)

    def _deployment_role_mode(self) -> str:
        text = str(self.config.get("_deployment_role_mode", "") or "").strip().lower()
        if text in {"internal", "external"}:
            return text
        return ""

    def _managed_source_cache_service(self) -> HandoverSourceFileCacheService:
        service = getattr(self, "_source_file_cache_service", None)
        if isinstance(service, HandoverSourceFileCacheService):
            return service
        service = HandoverSourceFileCacheService(self.config)
        self._source_file_cache_service = service
        return service

    @staticmethod
    def _assignment_has_people(assignment: ShiftRosterAssignment | None) -> bool:
        if assignment is None:
            return False
        return bool(
            str(assignment.current_people or "").strip()
            or str(assignment.next_people or "").strip()
            or str(assignment.next_first_person or "").strip()
        )

    def _build_alarm_fallback(self) -> Dict[str, str]:
        fixed_cfg = self.config.get("template_fixed_fill", {})
        fail_cfg = fixed_cfg.get("on_alarm_query_fail", {}) if isinstance(fixed_cfg, dict) else {}
        return {
            "total": str(fail_cfg.get("total", "0")).strip() or "0",
            "unrecovered": str(fail_cfg.get("unrecovered", "0")).strip() or "0",
            "accept_desc": str(fail_cfg.get("accept_desc", "/")).strip() or "/",
        }

    def _build_fixed_cell_values(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        alarm_total: str,
        alarm_unrecovered: str,
        alarm_accept_desc: str,
    ) -> Dict[str, str]:
        fixed_cfg = self.config.get("template_fixed_fill", {})
        if not isinstance(fixed_cfg, dict):
            fixed_cfg = {}
        shift_text_cfg = fixed_cfg.get("shift_text", {}) if isinstance(fixed_cfg.get("shift_text", {}), dict) else {}

        date_cell = str(fixed_cfg.get("date_cell", "B2")).strip() or "B2"
        shift_cell = str(fixed_cfg.get("shift_cell", "F2")).strip() or "F2"
        alarm_total_cell = str(fixed_cfg.get("alarm_total_cell", "B15")).strip() or "B15"
        alarm_unrecovered_cell = str(fixed_cfg.get("alarm_unrecovered_cell", "D15")).strip() or "D15"
        alarm_accept_desc_cell = str(fixed_cfg.get("alarm_accept_desc_cell", "F15")).strip() or "F15"

        date_text_format = str(fixed_cfg.get("date_text_format", "{year}年{month}月{day}日")).strip() or "{year}年{month}月{day}日"
        default_shift_text = "白班" if duty_shift == "day" else "夜班"
        shift_text = str(shift_text_cfg.get(duty_shift, default_shift_text)).strip() or default_shift_text
        duty_date_text = format_duty_date_text(duty_date, date_text_format)

        return {
            date_cell: duty_date_text,
            shift_cell: shift_text,
            alarm_total_cell: str(alarm_total).strip(),
            alarm_unrecovered_cell: str(alarm_unrecovered).strip(),
            alarm_accept_desc_cell: str(alarm_accept_desc).strip() or "/",
        }

    @staticmethod
    def _valid_cell(cell: Any) -> str:
        text = str(cell or "").strip().upper()
        if not text:
            return ""
        return text if re.fullmatch(r"[A-Z]+[1-9]\d*", text) else ""

    def _resolve_target_buildings(self, buildings: List[str] | None) -> List[str]:
        if buildings:
            target: List[str] = []
            for item in buildings:
                b = str(item or "").strip()
                if b and b not in target:
                    target.append(b)
            return target

        target: List[str] = []
        sites = self.config.get("sites", [])
        if isinstance(sites, list):
            for site in sites:
                if not isinstance(site, dict):
                    continue
                if not bool(site.get("enabled", False)):
                    continue
                b = str(site.get("building", "")).strip()
                if b and b not in target:
                    target.append(b)
        return target

    def _build_query_context(
        self,
        *,
        buildings: List[str],
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None],
        preloaded_roster_assignments: Dict[str, ShiftRosterAssignment] | None = None,
        prefetch_roster: bool = True,
    ) -> HandoverQueryContext:
        target_buildings = self._resolve_target_buildings(buildings)
        context = HandoverQueryContext(
            duty_date=str(duty_date or "").strip(),
            duty_shift=str(duty_shift or "").strip().lower(),
            target_buildings=target_buildings,
            roster_assignments=dict(preloaded_roster_assignments or {}),
        )
        if not context.duty_date or not context.duty_shift or not target_buildings:
            return context

        if prefetch_roster and not context.roster_assignments:
            try:
                context.roster_assignments = self._shift_roster_repo.query_assignments(
                    buildings=target_buildings,
                    duty_date=context.duty_date,
                    duty_shift=context.duty_shift,
                    emit_log=emit_log,
                )
            except Exception as exc:  # noqa: BLE001
                emit_log(f"[交接班][排班查询] 预取失败，后续按单楼兜底查询: {exc}")
                context.roster_assignments = {}

        if len(target_buildings) <= 1:
            return context

        try:
            context.event_query_by_building = self._event_category_builder.repo.load_current_shift_events_grouped(
                buildings=target_buildings,
                duty_date=context.duty_date,
                duty_shift=context.duty_shift,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][事件分类] 批量预取失败，后续按单楼兜底读取: {exc}")
            context.event_query_by_building = None

        try:
            context.change_rows_by_building, _ = self._change_management_builder.repo.list_current_shift_rows_grouped(
                buildings=target_buildings,
                duty_date=context.duty_date,
                duty_shift=context.duty_shift,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][变更管理] 批量预取失败，后续按单楼兜底读取: {exc}")
            context.change_rows_by_building = None

        try:
            context.exercise_rows_by_building, _ = self._exercise_management_builder.repo.list_current_shift_rows_grouped(
                buildings=target_buildings,
                duty_date=context.duty_date,
                duty_shift=context.duty_shift,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][演练管理] 批量预取失败，后续按单楼兜底读取: {exc}")
            context.exercise_rows_by_building = None

        try:
            context.maintenance_rows_by_building, _ = self._maintenance_management_builder.repo.list_current_shift_rows_grouped(
                buildings=target_buildings,
                duty_date=context.duty_date,
                duty_shift=context.duty_shift,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][维护管理] 批量预取失败，后续按单楼兜底读取: {exc}")
            context.maintenance_rows_by_building = None

        try:
            context.other_important_work_rows_by_building, _ = self._other_important_work_builder.repo.list_current_shift_rows_grouped(
                buildings=target_buildings,
                duty_date=context.duty_date,
                duty_shift=context.duty_shift,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][其他重要工作] 批量预取失败，后续按单楼兜底读取: {exc}")
            context.other_important_work_rows_by_building = None
        return context

    def _build_shift_roster_fixed_values(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None],
        assignment: ShiftRosterAssignment | None = None,
    ) -> Dict[str, str]:
        roster_cfg = self.config.get("shift_roster", {})
        if not isinstance(roster_cfg, dict) or not bool(roster_cfg.get("enabled", True)):
            return {}

        cells_cfg = roster_cfg.get("cells", {})
        if not isinstance(cells_cfg, dict):
            cells_cfg = {}
        current_cell = self._valid_cell(cells_cfg.get("current_people", "C3")) or "C3"
        next_cell = self._valid_cell(cells_cfg.get("next_people", "G3")) or "G3"
        next_first_cells_raw = cells_cfg.get("next_first_person_cells", ["H52", "H53", "H54", "H55"])
        next_first_cells: List[str] = []
        if isinstance(next_first_cells_raw, list):
            for raw in next_first_cells_raw:
                cell = self._valid_cell(raw)
                if cell and cell not in next_first_cells:
                    next_first_cells.append(cell)
        if not next_first_cells:
            next_first_cells = ["H52", "H53", "H54", "H55"]

        try:
            roster = assignment if self._assignment_has_people(assignment) else self._shift_roster_repo.query_assignment(
                building=building,
                duty_date=duty_date,
                duty_shift=duty_shift,
                emit_log=emit_log,
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][排班查询] building={building} 查询失败，按留空继续: {exc}")
            roster = ShiftRosterAssignment("", "", "", "", "", 0)

        fixed: Dict[str, str] = {
            current_cell: str(roster.current_people or "").strip(),
            next_cell: str(roster.next_people or "").strip(),
        }
        first_name = str(roster.next_first_person or "").strip()
        for cell in next_first_cells:
            fixed[cell] = first_name

        emit_log(
            f"[交接班][排班填充] building={building}, duty={duty_date}/{duty_shift}, "
            f"当前班组={'有' if fixed.get(current_cell) else '空'}, 下个班组={'有' if fixed.get(next_cell) else '空'}, "
            f"下班首人={'有' if first_name else '空'}"
        )
        try:
            fixed.update(
                self._shift_roster_repo.query_long_day_cell_values(
                    building=building,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    emit_log=emit_log,
                )
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][长白岗查询] building={building} 查询失败，按留空继续: {exc}")
        return fixed

    @staticmethod
    def _find_cell_value_case_insensitive(values: Dict[str, Any] | None, cell_name: str) -> str:
        if not isinstance(values, dict):
            return ""
        target = str(cell_name or "").strip().upper()
        if not target:
            return ""
        for raw_key, raw_val in values.items():
            key = str(raw_key or "").strip().upper()
            if key == target:
                return str(raw_val or "").strip()
        return ""

    def _build_fixed_values_with_alarm(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        start_time: str,
        end_time: str,
        emit_log: Callable[[str], None],
        roster_assignment: ShiftRosterAssignment | None = None,
        include_roster: bool = True,
    ) -> tuple[Dict[str, str], datetime]:
        fallback = self._build_alarm_fallback()
        total_text = fallback["total"]
        unrecovered_text = fallback["unrecovered"]
        accept_desc_text = fallback["accept_desc"]
        if self._deployment_role_mode() == "external":
            emit_log(
                f"[交接班][告警填充] building={building} 当前为外网端，"
                "已跳过告警数据库查询，按默认值填充"
            )
        else:
            try:
                alarm_summary = self._alarm_repo.query_alarm_summary(
                    building=building,
                    start_time=start_time,
                    end_time=end_time,
                    emit_log=emit_log,
                )
                total_text = str(alarm_summary.total_count)
                unrecovered_text = str(alarm_summary.unrecovered_count)
                accept_desc_text = str(alarm_summary.accept_description or "").strip() or fallback["accept_desc"]
                emit_log(
                    f"[交接班][告警查询] building={building}, total={total_text}, "
                    f"unrecovered={unrecovered_text}, accept_desc={accept_desc_text}"
                )
            except Exception as exc:  # noqa: BLE001
                emit_log(f"[交接班][告警查询] building={building} 查询失败，按兜底填充: {exc}")

        fixed_cell_values = self._build_fixed_cell_values(
            duty_date=duty_date,
            duty_shift=duty_shift,
            alarm_total=total_text,
            alarm_unrecovered=unrecovered_text,
            alarm_accept_desc=accept_desc_text,
        )
        if include_roster:
            fixed_cell_values.update(
                self._build_shift_roster_fixed_values(
                    building=building,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    emit_log=emit_log,
                    assignment=roster_assignment,
                )
            )
        duty_day = parse_duty_date(duty_date)
        date_ref_override = datetime(duty_day.year, duty_day.month, duty_day.day, 0, 0, 0)
        return fixed_cell_values, date_ref_override

    def _infer_duty_by_now(self, now: datetime | None = None) -> tuple[str, str]:
        cursor = now or datetime.now()
        second_of_day = cursor.hour * 3600 + cursor.minute * 60 + cursor.second
        if second_of_day < 9 * 3600:
            day = cursor.date() - timedelta(days=1)
            return day.strftime("%Y-%m-%d"), "night"
        if second_of_day < 18 * 3600:
            return cursor.strftime("%Y-%m-%d"), "day"
        return cursor.strftime("%Y-%m-%d"), "night"

    def _is_current_duty_context(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        now: datetime | None = None,
    ) -> bool:
        current_duty_date, current_duty_shift = self._infer_duty_by_now(now=now)
        return (
            str(duty_date or "").strip() == current_duty_date
            and str(duty_shift or "").strip().lower() == current_duty_shift
        )

    def run_from_existing_file(
        self,
        *,
        building: str,
        data_file: str,
        end_time: str | None = None,
        duty_date: str | None = None,
        duty_shift: str | None = None,
        start_time: str | None = None,
        fixed_cell_values: Dict[str, Any] | None = None,
        date_ref_override: datetime | None = None,
        roster_assignment: ShiftRosterAssignment | None = None,
        category_payloads: Dict[str, Any] | None = None,
        event_query_by_building: EventQueryByBuilding | None = None,
        change_rows_by_building: ChangeRowsByBuilding | None = None,
        exercise_rows_by_building: ExerciseRowsByBuilding | None = None,
        maintenance_rows_by_building: MaintenanceRowsByBuilding | None = None,
        other_important_work_rows_by_building: OtherImportantWorkRowsByBuilding | None = None,
        source_mode: str = "from_file",
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        summary = RunSummary(mode="from_existing_file")
        result = BuildingResult(building=building, data_file=data_file)
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        start_time_text = str(start_time or "").strip()
        end_time_text = str(end_time or "").strip()
        roster_applied = False

        # 无论是否传入 fixed_cell_values，只要班次上下文不完整，都先自动推断，
        # 保证 from-file / from-download / 手动 / 自动路径都能走统一的白班多维上报判定。
        if not duty_date_text or not duty_shift_text:
            inferred_date, inferred_shift = self._infer_duty_by_now()
            duty_date_text = duty_date_text or inferred_date
            duty_shift_text = duty_shift_text or inferred_shift
            emit_log(
                f"[交接班][已有数据表] 未提供完整班次参数，自动推断 duty_date={duty_date_text}, "
                f"duty_shift={duty_shift_text}"
            )

        if fixed_cell_values is None and duty_date_text and duty_shift_text:
            if not start_time_text or not end_time_text:
                download_cfg = self.config.get("download", {})
                shift_windows = {}
                if isinstance(download_cfg, dict):
                    raw_windows = download_cfg.get("shift_windows", {})
                    shift_windows = raw_windows if isinstance(raw_windows, dict) else {}
                duty_window = build_duty_window(
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    shift_windows=shift_windows,
                )
                start_time_text = duty_window.start_time
                end_time_text = duty_window.end_time
            fixed_cell_values, date_ref_override = self._build_fixed_values_with_alarm(
                building=building,
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                start_time=start_time_text,
                end_time=end_time_text,
                emit_log=emit_log,
                roster_assignment=roster_assignment,
            )
            roster_applied = True
            emit_log(
                f"[交接班][已有数据表] 已应用固定单元格填充 duty_date={duty_date_text}, "
                f"duty_shift={duty_shift_text}, start={start_time_text}, end={end_time_text}"
            )
        elif isinstance(fixed_cell_values, dict):
            fixed_cell_values = {
                str(k).strip(): ("" if v is None else str(v).strip())
                for k, v in fixed_cell_values.items()
                if str(k).strip()
            }

        if duty_date_text and duty_shift_text and not roster_applied:
            roster_fixed = self._build_shift_roster_fixed_values(
                building=building,
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                emit_log=emit_log,
                assignment=roster_assignment,
            )
            fixed_cell_values = dict(fixed_cell_values or {})
            fixed_cell_values.update(roster_fixed)
            if date_ref_override is None:
                duty_day = parse_duty_date(duty_date_text)
                date_ref_override = datetime(duty_day.year, duty_day.month, duty_day.day, 0, 0, 0)

        if category_payloads is None and duty_date_text and duty_shift_text:
            roster_cfg = self.config.get("shift_roster", {})
            cells_cfg = roster_cfg.get("cells", {}) if isinstance(roster_cfg, dict) else {}
            current_people_cell = str(cells_cfg.get("current_people", "C3")).strip().upper() or "C3"
            current_people_text = self._find_cell_value_case_insensitive(fixed_cell_values, current_people_cell)
            is_current_duty_context = self._is_current_duty_context(
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
            )
            combined_category_payloads: Dict[str, Any] = {}
            try:
                event_payloads = self._event_category_builder.build(
                    building=building,
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    follower_text=current_people_text,
                    is_current_duty_context=is_current_duty_context,
                    preloaded_query_result_by_building=event_query_by_building,
                    emit_log=emit_log,
                )
                if isinstance(event_payloads, dict):
                    combined_category_payloads.update(event_payloads)
            except Exception as exc:  # noqa: BLE001
                emit_log(f"[交接班][事件分类] 构建失败，按空分类继续: {exc}")
            try:
                change_payloads = self._change_management_builder.build(
                    building=building,
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    preloaded_rows_by_building=change_rows_by_building,
                    emit_log=emit_log,
                )
                if isinstance(change_payloads, dict):
                    combined_category_payloads.update(change_payloads)
            except Exception as exc:  # noqa: BLE001
                emit_log(f"[交接班][变更管理] 构建失败，按空分类继续: {exc}")
            try:
                exercise_payloads = self._exercise_management_builder.build(
                    building=building,
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    executor_text=current_people_text,
                    preloaded_rows_by_building=exercise_rows_by_building,
                    emit_log=emit_log,
                )
                if isinstance(exercise_payloads, dict):
                    combined_category_payloads.update(exercise_payloads)
            except Exception as exc:  # noqa: BLE001
                emit_log(f"[交接班][演练管理] 构建失败，按空分类继续: {exc}")
            try:
                maintenance_payloads = self._maintenance_management_builder.build(
                    building=building,
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    preloaded_rows_by_building=maintenance_rows_by_building,
                    emit_log=emit_log,
                )
                if isinstance(maintenance_payloads, dict):
                    combined_category_payloads.update(maintenance_payloads)
            except Exception as exc:  # noqa: BLE001
                emit_log(f"[交接班][维护管理] 构建失败，按空分类继续: {exc}")
            try:
                other_important_work_payloads = self._other_important_work_builder.build(
                    building=building,
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    preloaded_rows_by_building=other_important_work_rows_by_building,
                    emit_log=emit_log,
                )
                if isinstance(other_important_work_payloads, dict):
                    combined_category_payloads.update(other_important_work_payloads)
            except Exception as exc:  # noqa: BLE001
                emit_log(f"[交接班][其他重要工作] 构建失败，按空分类继续: {exc}")
            category_payloads = combined_category_payloads

        upload_date = _norm(duty_date_text or (end_time_text.split(" ")[0] if end_time_text else ""), "-")
        failed_stage = "数据解析"
        managed_source_file_cache: Dict[str, Any] | None = None
        previous_managed_source_path = ""

        try:
            extracted = self._extract_service.extract(building=building, data_file=data_file)
            failed_stage = "模板填充"
            normalized_category_payloads = category_payloads if isinstance(category_payloads, dict) else {}
            filled = self._fill_service.fill(
                building=building,
                data_file=data_file,
                hits=extracted["hits"],
                effective_config=extracted["effective_config"],
                end_time=end_time_text or None,
                fixed_cell_values=fixed_cell_values,
                date_ref_override=date_ref_override,
                category_payloads=normalized_category_payloads,
                emit_log=emit_log,
            )
            result.output_file = filled["output_file"]
            result.fills = filled["fills"]
            result.missing_metrics = sorted(list(filled["missing_metric_to_cell"].keys()))
            session_day_metric_export = {
                "status": "skipped",
                "reason": "missing_duty_context",
                "uploaded_count": 0,
                "error": "",
                "uploaded_at": "",
                "uploaded_revision": 0,
                "metric_values_by_id": {},
                "metric_origin_context": {"by_metric_id": {}, "by_target_cell": {}},
            }
            session_source_data_attachment_export = {
                "status": "skipped",
                "reason": "missing_duty_context",
                "uploaded_count": 0,
                "error": "",
                "uploaded_at": "",
                "uploaded_revision": 0,
            }
            if duty_date_text and duty_shift_text:
                session_day_metric_export = self._day_metric_export_service.build_deferred_state(
                    duty_shift=duty_shift_text,
                    resolved_values_by_id=filled.get("resolved_values_by_id", {}),
                    metric_origin_context=self._day_metric_export_service.serialize_metric_origin_context(
                        hits=extracted.get("hits", {}),
                        effective_config=extracted.get("effective_config", {}),
                    ),
                )
                session_source_data_attachment_export = (
                    self._source_data_attachment_export_service.build_deferred_state(
                        duty_shift=duty_shift_text,
                    )
                )
                emit_log(
                    "[交接班][白班多维] 延后上传: "
                    f"building={building}, duty_date={duty_date_text}, duty_shift={duty_shift_text}, "
                    f"原因={_followup_reason_text(session_day_metric_export.get('reason'))}"
                )
                emit_log(
                    "[交接班][源数据附件] 延后上传: "
                    f"building={building}, duty_date={duty_date_text}, duty_shift={duty_shift_text}, "
                    f"原因={_followup_reason_text(session_source_data_attachment_export.get('reason'))}"
                )
            else:
                emit_log(
                    "[交接班][白班多维] 跳过: 缺少班次上下文 "
                    f"building={building}, duty_date={duty_date_text or '-'}, duty_shift={duty_shift_text or '-'}"
                )
                emit_log(
                    "[交接班][源数据附件] 跳过: 缺少班次上下文 "
                    f"building={building}, duty_date={duty_date_text or '-'}, duty_shift={duty_shift_text or '-'}"
                )
            result.day_metric_export = {
                "status": str(session_day_metric_export.get("status", "")).strip(),
                "reason": str(session_day_metric_export.get("reason", "")).strip(),
                "uploaded_count": int(session_day_metric_export.get("uploaded_count", 0) or 0),
                "error": str(session_day_metric_export.get("error", "")).strip(),
            }
            review_session: Dict[str, Any] = {}
            if duty_date_text and duty_shift_text and result.output_file:
                if str(source_mode or "").strip().lower() == "from_file":
                    session_id = self._review_session_service.build_session_id(building, duty_date_text, duty_shift_text)
                    previous_session = self._review_session_service.get_session_by_id(session_id)
                    previous_source_cache = (
                        previous_session.get("source_file_cache", {})
                        if isinstance(previous_session, dict) and isinstance(previous_session.get("source_file_cache", {}), dict)
                        else {}
                    )
                    previous_managed_source_path = str(previous_source_cache.get("stored_path", "")).strip()
                    managed_source_file_cache = self._managed_source_cache_service().persist_uploaded_source(
                        source_path=data_file,
                        building=building,
                        duty_date=duty_date_text,
                        duty_shift=duty_shift_text,
                        session_id=session_id,
                        original_name=Path(str(data_file or "").strip()).name,
                        previous_stored_path=previous_managed_source_path,
                        emit_log=emit_log,
                    )
                    result.data_file = str(managed_source_file_cache.get("stored_path", "")).strip() or result.data_file
                try:
                    review_session = self._review_session_service.register_generated_output(
                        building=building,
                        duty_date=duty_date_text,
                        duty_shift=duty_shift_text,
                        data_file=result.data_file,
                        output_file=result.output_file,
                        source_mode=source_mode,
                        day_metric_export=session_day_metric_export,
                        source_file_cache=managed_source_file_cache,
                        source_data_attachment_export=session_source_data_attachment_export,
                    )
                    result.review_session = review_session
                    result.batch_key = str(review_session.get("batch_key", "")).strip()
                    result.confirmed = bool(review_session.get("confirmed", False))
                    result.cloud_sheet_sync = (
                        dict(review_session.get("cloud_sheet_sync", {}))
                        if isinstance(review_session.get("cloud_sheet_sync", {}), dict)
                        else {}
                    )
                    emit_log(
                        "[交接班][审核会话] 已登记 "
                        f"building={building}, session_id={review_session.get('session_id', '-')}, "
                        f"output={result.output_file}"
                    )
                except Exception as exc:  # noqa: BLE001
                    managed_stored_path = (
                        str(managed_source_file_cache.get("stored_path", "")).strip()
                        if isinstance(managed_source_file_cache, dict)
                        else ""
                    )
                    if (
                        managed_stored_path
                        and managed_stored_path != previous_managed_source_path
                    ):
                        self._managed_source_cache_service().remove_managed_source(
                            managed_stored_path,
                            emit_log=emit_log,
                        )
                    result.errors.append(f"审核会话登记失败: {exc}")
                    emit_log(f"[交接班][审核会话] 登记失败 building={building}: {exc}")
            result.success = True
            emit_log(
                "[文件上传成功] 功能=交接班日志 阶段=输出完成 楼栋="
                f"{_norm(building)} 文件={_norm(data_file)} 日期={upload_date} "
                f"详情={_norm(result.output_file)}"
            )
            summary.success_count = 1
        except Exception as exc:  # noqa: BLE001
            result.success = False
            result.errors.append(str(exc))
            summary.failed_count = 1
            summary.errors.append(str(exc))
            emit_log(
                "[文件流程失败] 功能=交接班日志 阶段="
                f"{failed_stage} 楼栋={_norm(building)} 文件={_norm(data_file)} "
                f"日期={upload_date} 错误={_norm(exc)}"
            )

        summary.results.append(result)
        return summary.to_dict()

    def run_from_existing_files(
        self,
        *,
        building_files: List[tuple[str, str]],
        configured_buildings: List[str] | None = None,
        end_time: str | None = None,
        duty_date: str | None = None,
        duty_shift: str | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        summary = RunSummary(mode="from_existing_files")
        selected_buildings: List[str] = []
        normalized_files: List[tuple[str, str]] = []
        for building, data_file in building_files or []:
            building_text = str(building or "").strip()
            data_file_text = str(data_file or "").strip()
            if not building_text or not data_file_text:
                continue
            normalized_files.append((building_text, data_file_text))
            if building_text not in selected_buildings:
                selected_buildings.append(building_text)

        configured = [
            str(item or "").strip()
            for item in (configured_buildings or self._resolve_target_buildings(None))
            if str(item or "").strip()
        ]
        skipped_buildings = [building for building in configured if building not in selected_buildings]
        query_context = HandoverQueryContext(
            duty_date=str(duty_date or "").strip(),
            duty_shift=str(duty_shift or "").strip().lower(),
            target_buildings=selected_buildings,
        )
        if query_context.duty_date and query_context.duty_shift and selected_buildings:
            query_context = self._build_query_context(
                buildings=selected_buildings,
                duty_date=query_context.duty_date,
                duty_shift=query_context.duty_shift,
                emit_log=emit_log,
            )

        for building, data_file in normalized_files:
            try:
                one = self.run_from_existing_file(
                    building=building,
                    data_file=data_file,
                    end_time=end_time,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    roster_assignment=query_context.roster_assignments.get(building),
                    event_query_by_building=query_context.event_query_by_building,
                    change_rows_by_building=query_context.change_rows_by_building,
                    exercise_rows_by_building=query_context.exercise_rows_by_building,
                    maintenance_rows_by_building=query_context.maintenance_rows_by_building,
                    other_important_work_rows_by_building=query_context.other_important_work_rows_by_building,
                    source_mode="from_file",
                    emit_log=emit_log,
                )
            except Exception as exc:  # noqa: BLE001
                result = BuildingResult(building=building, data_file=data_file, success=False, errors=[str(exc)])
                summary.results.append(result)
                summary.failed_count += 1
                summary.errors.append(str(exc))
                emit_log(f"[交接班][已有数据表批量] building={building} 处理失败: {exc}")
                continue

            for row in one.get("results", []):
                result = BuildingResult(
                    building=str(row.get("building", building)),
                    data_file=str(row.get("data_file", data_file)),
                    output_file=str(row.get("output_file", "")),
                    success=bool(row.get("success", False)),
                    fills=[],
                    missing_metrics=list(row.get("missing_metrics", [])),
                    day_metric_export=dict(row.get("day_metric_export", {}))
                    if isinstance(row.get("day_metric_export", {}), dict)
                    else {},
                    cloud_sheet_sync=dict(row.get("cloud_sheet_sync", {}))
                    if isinstance(row.get("cloud_sheet_sync", {}), dict)
                    else {},
                    review_session=dict(row.get("review_session", {}))
                    if isinstance(row.get("review_session", {}), dict)
                    else {},
                    batch_key=str(row.get("batch_key", "")),
                    confirmed=bool(row.get("confirmed", False)),
                    errors=list(row.get("errors", [])),
                )
                summary.results.append(result)
                if result.success:
                    summary.success_count += 1
                else:
                    summary.failed_count += 1

        result_summary = summary.to_dict()
        result_summary["selected_buildings"] = selected_buildings
        result_summary["skipped_buildings"] = skipped_buildings
        return result_summary

    def run_from_download(
        self,
        *,
        buildings: List[str] | None = None,
        end_time: str | None = None,
        duty_date: str | None = None,
        duty_shift: str | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        summary = RunSummary(mode="from_download")
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        switched_external = False
        cloud_batch_meta: Dict[str, Any] | None = None

        roster_prefetch: Dict[str, ShiftRosterAssignment] = {}
        if duty_date_text and duty_shift_text:
            prefetch_buildings = self._resolve_target_buildings(buildings)
            if prefetch_buildings:
                try:
                    roster_prefetch = self._shift_roster_repo.query_assignments(
                        buildings=prefetch_buildings,
                        duty_date=duty_date_text,
                        duty_shift=duty_shift_text,
                        emit_log=emit_log,
                    )
                except Exception as exc:  # noqa: BLE001
                    emit_log(f"[交接班][排班查询] 预取失败，后续按单楼兜底查询: {exc}")

        try:
            download_result = self._download_service.run(
                buildings=buildings,
                end_time=end_time,
                duty_date=duty_date,
                duty_shift=duty_shift,
                emit_log=emit_log,
            )
            summary.start_time = str(download_result.get("start_time", ""))
            summary.end_time = str(download_result.get("end_time", ""))
            duty_date_text = str(download_result.get("duty_date", "")).strip() or duty_date_text
            duty_shift_text = str(download_result.get("duty_shift", "")).strip() or duty_shift_text
            if not duty_date_text or not duty_shift_text:
                inferred_date, inferred_shift = self._infer_duty_by_now()
                duty_date_text = duty_date_text or inferred_date
                duty_shift_text = duty_shift_text or inferred_shift
                emit_log(
                    "[交接班下载] 未返回完整班次上下文，已自动补齐 "
                    f"duty_date={duty_date_text}, duty_shift={duty_shift_text}"
                )
            upload_date = _norm(duty_date_text or (summary.end_time.split(" ")[0] if summary.end_time else ""))

            failed_downloads = download_result.get("failed", [])
            for item in failed_downloads:
                building = _norm(item.get("building", ""))
                error = _norm(item.get("error", "下载失败"))
                result = BuildingResult(building=building, data_file="")
                result.success = False
                result.errors.append(error)
                summary.results.append(result)
                summary.failed_count += 1
                emit_log(
                    "[文件流程失败] 功能=交接班日志 阶段=内网下载 楼栋="
                    f"{building} 文件=- 日期={upload_date} 错误={error}"
                )

            success_items = download_result.get("success_files", [])
            prebuilt_fixed: Dict[str, tuple[Dict[str, str], datetime, ShiftRosterAssignment | None]] = {}
            if duty_date_text and duty_shift_text:
                download_cfg = self.config.get("download", {})
                shift_windows = {}
                if isinstance(download_cfg, dict):
                    raw_windows = download_cfg.get("shift_windows", {})
                    shift_windows = raw_windows if isinstance(raw_windows, dict) else {}
                duty_window = build_duty_window(
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    shift_windows=shift_windows,
                )
                for item in success_items:
                    building = str(item.get("building", "")).strip()
                    if not building:
                        continue
                    assignment = roster_prefetch.get(building)
                    fixed_cell_values, date_ref_override = self._build_fixed_values_with_alarm(
                        building=building,
                        duty_date=duty_date_text,
                        duty_shift=duty_shift_text,
                        start_time=duty_window.start_time,
                        end_time=duty_window.end_time,
                        emit_log=emit_log,
                        roster_assignment=assignment,
                        include_roster=False,
                    )
                    prebuilt_fixed[building] = (fixed_cell_values, date_ref_override, assignment)

            # 内网下载和告警查询完成后优先切回外网，再执行飞书相关填充。
            if success_items:
                try:
                    self._download_service.switch_external_after_download(emit_log)
                    switched_external = True
                except Exception as exc:  # noqa: BLE001
                    emit_log(f"[交接班下载] 切换外网异常: {exc}")

            cloud_sheet_sync_cfg = (
                self.config.get("cloud_sheet_sync", {})
                if isinstance(self.config.get("cloud_sheet_sync", {}), dict)
                else {}
            )
            if success_items and duty_date_text and duty_shift_text and bool(cloud_sheet_sync_cfg.get("enabled", True)):
                default_shift_text = "白班" if duty_shift_text == "day" else "夜班"
                first_building = str(success_items[0].get("building", "")).strip()
                first_fixed_values = prebuilt_fixed.get(first_building, ({}, None, None))[0] if first_building else {}
                shift_text = self._find_cell_value_case_insensitive(first_fixed_values, "F2") or default_shift_text
                duty_date_display = self._find_cell_value_case_insensitive(first_fixed_values, "B2") or format_duty_date_text(
                    duty_date_text
                )
                cloud_batch_meta = self._cloud_sheet_sync_service.prepare_batch_spreadsheet(
                    duty_date=duty_date_text,
                    duty_date_text=duty_date_display,
                    shift_text=shift_text,
                    emit_log=emit_log,
                )
                try:
                    self._review_session_service.register_cloud_batch(
                        batch_key=self._review_session_service.build_batch_key(duty_date_text, duty_shift_text),
                        duty_date=duty_date_text,
                        duty_shift=duty_shift_text,
                        cloud_batch=cloud_batch_meta,
                    )
                except Exception as exc:  # noqa: BLE001
                    emit_log(f"[交接班][云表预建] 批次状态登记失败: {exc}")

            success_buildings = [
                str(item.get("building", "")).strip()
                for item in success_items
                if str(item.get("building", "")).strip()
            ]
            query_context = HandoverQueryContext(
                duty_date=duty_date_text,
                duty_shift=duty_shift_text,
                target_buildings=success_buildings,
                roster_assignments=dict(roster_prefetch),
            )
            if duty_date_text and duty_shift_text and success_buildings:
                query_context = self._build_query_context(
                    buildings=success_buildings,
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    emit_log=emit_log,
                    preloaded_roster_assignments=roster_prefetch,
                    prefetch_roster=False,
                )

            for item in success_items:
                building = str(item.get("building", "")).strip()
                data_file = str(item.get("file_path", "")).strip()
                if not building or not data_file:
                    continue

                assignment = query_context.roster_assignments.get(building) or roster_prefetch.get(building)
                fixed_cell_values: Dict[str, str] | None = None
                date_ref_override: datetime | None = None
                if duty_date_text and duty_shift_text:
                    prebuilt = prebuilt_fixed.get(building)
                    if prebuilt is not None:
                        fixed_cell_values, date_ref_override, assignment = prebuilt
                    else:
                        fixed_cell_values, date_ref_override = self._build_fixed_values_with_alarm(
                            building=building,
                            duty_date=duty_date_text,
                            duty_shift=duty_shift_text,
                            start_time=summary.start_time,
                            end_time=summary.end_time,
                            emit_log=emit_log,
                            roster_assignment=assignment,
                            include_roster=False,
                        )

                one = self.run_from_existing_file(
                    building=building,
                    data_file=data_file,
                    end_time=summary.end_time or end_time,
                    duty_date=duty_date_text or None,
                    duty_shift=duty_shift_text or None,
                    start_time=summary.start_time or None,
                    fixed_cell_values=fixed_cell_values,
                    date_ref_override=date_ref_override,
                    roster_assignment=assignment,
                    category_payloads=None,
                    event_query_by_building=query_context.event_query_by_building,
                    change_rows_by_building=query_context.change_rows_by_building,
                    exercise_rows_by_building=query_context.exercise_rows_by_building,
                    maintenance_rows_by_building=query_context.maintenance_rows_by_building,
                    other_important_work_rows_by_building=query_context.other_important_work_rows_by_building,
                    source_mode="from_download",
                    emit_log=emit_log,
                )
                one_results = one.get("results", [])
                for row in one_results:
                    result = BuildingResult(
                        building=str(row.get("building", building)),
                        data_file=str(row.get("data_file", data_file)),
                        output_file=str(row.get("output_file", "")),
                        success=bool(row.get("success", False)),
                        missing_metrics=list(row.get("missing_metrics", [])),
                        day_metric_export=dict(row.get("day_metric_export", {}))
                        if isinstance(row.get("day_metric_export", {}), dict)
                        else {},
                        cloud_sheet_sync=dict(row.get("cloud_sheet_sync", {}))
                        if isinstance(row.get("cloud_sheet_sync", {}), dict)
                        else {},
                        review_session=dict(row.get("review_session", {}))
                        if isinstance(row.get("review_session", {}), dict)
                        else {},
                        batch_key=str(row.get("batch_key", "")),
                        confirmed=bool(row.get("confirmed", False)),
                        errors=list(row.get("errors", [])),
                    )
                    summary.results.append(result)
                    if result.success:
                        summary.success_count += 1
                    else:
                        summary.failed_count += 1
                    day_export = result.day_metric_export if isinstance(result.day_metric_export, dict) else {}
                    if day_export:
                        emit_log(
                            "[交接班][白班多维] 楼栋="
                            f"{_norm(result.building)} 状态={_followup_status_text(day_export.get('status'))} "
                            f"已上传={int(day_export.get('uploaded_count', 0) or 0)} "
                            f"原因={_followup_reason_text(day_export.get('reason'))}"
                        )
        finally:
            if not switched_external:
                try:
                    self._download_service.switch_external_after_download(emit_log)
                except Exception as exc:  # noqa: BLE001
                    emit_log(f"[交接班下载] 切换外网异常: {exc}")

        result_summary = summary.to_dict()
        if duty_date_text:
            result_summary["duty_date"] = duty_date_text
        if duty_shift_text:
            result_summary["duty_shift"] = duty_shift_text
        return result_summary
