from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from handover_log_module.core.formatter import (
    build_cell_value_map,
    build_resolved_value_context,
    build_metric_text,
    missing_metrics_for_cells,
)
from handover_log_module.core.models import FillValue, MetricHit
from handover_log_module.core.building_title_rules import HANDOVER_TITLE_CELL, build_handover_building_title
from handover_log_module.repository.template_writer import copy_template_and_fill
from handover_log_module.service.cabinet_power_defaults_service import CabinetPowerDefaultsService
from handover_log_module.service.footer_inventory_defaults_service import FooterInventoryDefaultsService
from handover_log_module.service.review_document_state_service import ReviewDocumentStateService


class HandoverFillService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self._cabinet_power_defaults_service = CabinetPowerDefaultsService()
        self._footer_inventory_defaults_service = FooterInventoryDefaultsService()
        self._review_document_state_service = ReviewDocumentStateService(config)

    def _inject_building_title(
        self,
        *,
        building: str,
        template_cfg: Dict[str, Any],
        fixed_cells: Dict[str, str],
    ) -> None:
        title_cell = HANDOVER_TITLE_CELL
        title_text = build_handover_building_title(building)
        if title_text:
            fixed_cells[title_cell] = title_text

    def _apply_footer_inventory_defaults(
        self,
        *,
        building: str,
        template_cfg: Dict[str, Any],
        output_file: str,
        emit_log: Callable[[str], None],
    ) -> None:
        sheet_name = str(template_cfg.get("sheet_name", "")).strip()
        if not sheet_name:
            raise ValueError("template.sheet_name is required")

        applied_rows = self._review_document_state_service.apply_footer_defaults_to_output(
            building=building,
            output_file=output_file,
            emit_log=emit_log,
        )
        if applied_rows is None:
            applied_rows = self._footer_inventory_defaults_service.apply_building_defaults_to_output(
                config=self.config,
                building=building,
                output_file=output_file,
                sheet_name=sheet_name,
                emit_log=emit_log,
            )
        if applied_rows is None:
            emit_log(f"[交接班][工具表默认] 未配置楼栋默认工具表，保留模板内容: building={building}")
            return
        emit_log(
            f"[交接班][工具表默认] 应用楼栋默认工具表: building={building}, rows={applied_rows}, output={output_file}"
        )

    def _apply_cabinet_power_defaults(
        self,
        *,
        building: str,
        template_cfg: Dict[str, Any],
        output_file: str,
        emit_log: Callable[[str], None],
    ) -> None:
        sheet_name = str(template_cfg.get("sheet_name", "")).strip()
        if not sheet_name:
            raise ValueError("template.sheet_name is required")

        applied_fields = self._review_document_state_service.apply_cabinet_defaults_to_output(
            building=building,
            output_file=output_file,
            emit_log=emit_log,
        )
        if applied_fields is None:
            applied_fields = self._cabinet_power_defaults_service.apply_building_defaults_to_output(
                config=self.config,
                building=building,
                output_file=output_file,
                sheet_name=sheet_name,
                emit_log=emit_log,
            )
        if applied_fields is None:
            emit_log(f"[交接班][机柜上下电默认] 未配置楼栋模板默认值，保留模板原值 building={building}")
            return
        emit_log(
            f"[交接班][机柜上下电默认] 应用楼栋模板默认值 building={building}, fields={applied_fields}, output={output_file}"
        )

    def fill(
        self,
        *,
        building: str,
        data_file: str,
        hits: Dict[str, MetricHit],
        effective_config: Dict[str, Any],
        duty_date: str | None = None,
        duty_shift: str | None = None,
        end_time: str | None = None,
        fixed_cell_values: Optional[Dict[str, Any]] = None,
        date_ref_override: datetime | None = None,
        category_payloads: Optional[Dict[str, Any]] = None,
        write_output_file: bool = True,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        template_cfg = effective_config.get("template", {})
        cell_mapping = effective_config.get("cell_mapping", {})
        format_templates = effective_config.get("format_templates", {})
        missing_policy = str(effective_config.get("missing_policy", "blank")).strip().lower()

        if not isinstance(template_cfg, dict):
            raise ValueError("配置错误: handover_log.template 必须是对象")
        if not isinstance(cell_mapping, dict) or not cell_mapping:
            raise ValueError("配置错误: handover_log.cell_mapping 不能为空")
        if not isinstance(format_templates, dict) or not format_templates:
            raise ValueError("配置错误: handover_log.format_templates 不能为空")

        cell_values = build_cell_value_map(
            cell_mapping=cell_mapping,
            templates=format_templates,
            hits=hits,
            effective_config=effective_config,
            missing_policy=missing_policy,
        )
        resolved_values_by_id = build_resolved_value_context(hits=hits, effective_config=effective_config)
        fixed_cells: Dict[str, str] = {}
        if isinstance(fixed_cell_values, dict):
            for raw_cell, raw_value in fixed_cell_values.items():
                cell = str(raw_cell or "").strip()
                if not cell:
                    continue
                if raw_value is None:
                    continue
                fixed_cells[cell] = str(raw_value).strip()
        self._inject_building_title(
            building=building,
            template_cfg=template_cfg,
            fixed_cells=fixed_cells,
        )
        if fixed_cells:
            cell_values.update(fixed_cells)

        missing_map = missing_metrics_for_cells(
            cell_mapping=cell_mapping,
            hits=hits,
            effective_config=effective_config,
        )

        date_ref = date_ref_override if isinstance(date_ref_override, datetime) else datetime.now()
        if end_time and not isinstance(date_ref_override, datetime):
            time_format = str(effective_config.get("download", {}).get("time_format", "%Y-%m-%d %H:%M:%S"))
            date_ref = datetime.strptime(end_time, time_format)

        output_file = ""
        if write_output_file:
            output_path = copy_template_and_fill(
                building=building,
                template_cfg=template_cfg,
                cell_values=cell_values,
                date_ref=date_ref,
                duty_date=str(duty_date or "").strip(),
                duty_shift=str(duty_shift or "").strip(),
                category_payloads=category_payloads,
                emit_log=emit_log,
            )
            output_file = str(output_path)
            self._apply_cabinet_power_defaults(
                building=building,
                template_cfg=template_cfg,
                output_file=output_file,
                emit_log=emit_log,
            )
            self._apply_footer_inventory_defaults(
                building=building,
                template_cfg=template_cfg,
                output_file=output_file,
                emit_log=emit_log,
            )

        fills: List[FillValue] = []
        for metric_key, cell in cell_mapping.items():
            text = build_metric_text(
                metric_key=metric_key,
                hits=hits,
                templates=format_templates,
                effective_config=effective_config,
            )
            if not text and missing_policy == "blank":
                continue
            if not text and missing_policy == "zero":
                text = "0"
            if not text and missing_policy == "na":
                text = "N/A"
            if not text:
                continue
            hit = hits.get(metric_key)
            row_refs: List[int] = []
            if hit is not None:
                row_refs = [hit.row_index]
            fills.append(FillValue(metric_key=metric_key, cell=str(cell), text=text, from_rows=row_refs))
        for cell, text in fixed_cells.items():
            fills.append(FillValue(metric_key=f"fixed:{cell}", cell=cell, text=text, from_rows=[]))

        return {
            "output_file": output_file,
            "fills": fills,
            "missing_metric_to_cell": missing_map,
            "data_file": data_file,
            "resolved_values_by_id": resolved_values_by_id,
            "final_cell_values": dict(cell_values),
        }
