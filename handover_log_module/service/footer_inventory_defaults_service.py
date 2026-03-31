from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, List

import openpyxl

from app.shared.utils.atomic_file import atomic_save_workbook
from handover_log_module.core.footer_layout import (
    FOOTER_GROUP_TITLE_TEXT,
    FOOTER_INVENTORY_COLUMNS,
    find_footer_inventory_layout,
)
from handover_log_module.repository.footer_inventory_writer import write_footer_inventory_table


class FooterInventoryDefaultsService:
    VISIBLE_COLUMNS = ("B", "C", "E", "F", "G", "H")
    DEFAULTS_KEY = "footer_inventory_defaults_by_building"

    @classmethod
    def _blank_cells(cls) -> Dict[str, str]:
        return {column: "" for column in cls.VISIBLE_COLUMNS}

    @classmethod
    def normalize_rows(cls, rows: Any) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        source_rows = rows if isinstance(rows, list) else []
        for raw_row in source_rows:
            cells = raw_row.get("cells", {}) if isinstance(raw_row, dict) else {}
            if not isinstance(cells, dict):
                cells = {}
            normalized_cells = cls._blank_cells()
            for column in cls.VISIBLE_COLUMNS:
                normalized_cells[column] = str(cells.get(column, "") or "").strip()
            normalized.append({"cells": normalized_cells})

        if not normalized:
            return [{"cells": cls._blank_cells()}]

        if not any(any(str(value or "").strip() for value in row["cells"].values()) for row in normalized):
            return [{"cells": cls._blank_cells()}]
        return normalized

    def extract_rows_from_document(self, document: Dict[str, Any]) -> List[Dict[str, Any]]:
        footer_blocks = document.get("footer_blocks", []) if isinstance(document, dict) else []
        if isinstance(footer_blocks, list):
            for block in footer_blocks:
                if not isinstance(block, dict):
                    continue
                if str(block.get("type", "")).strip() != "inventory_table":
                    continue
                return self.normalize_rows(block.get("rows", []))
        return self.normalize_rows([])

    @staticmethod
    def _review_ui_from_v3(cfg: Dict[str, Any], *, create: bool) -> Dict[str, Any]:
        features = cfg.setdefault("features", {}) if create else cfg.get("features", {})
        if not isinstance(features, dict):
            features = {}
            if create:
                cfg["features"] = features
        handover = features.setdefault("handover_log", {}) if create else features.get("handover_log", {})
        if not isinstance(handover, dict):
            handover = {}
            if create:
                features["handover_log"] = handover
        review_ui = handover.setdefault("review_ui", {}) if create else handover.get("review_ui", {})
        if not isinstance(review_ui, dict):
            review_ui = {}
            if create:
                handover["review_ui"] = review_ui
        return review_ui

    @staticmethod
    def _review_ui_from_runtime_root(cfg: Dict[str, Any], *, create: bool) -> Dict[str, Any]:
        handover = cfg.setdefault("handover_log", {}) if create else cfg.get("handover_log", {})
        if not isinstance(handover, dict):
            handover = {}
            if create:
                cfg["handover_log"] = handover
        review_ui = handover.setdefault("review_ui", {}) if create else handover.get("review_ui", {})
        if not isinstance(review_ui, dict):
            review_ui = {}
            if create:
                handover["review_ui"] = review_ui
        return review_ui

    @staticmethod
    def _review_ui_from_handover_cfg(cfg: Dict[str, Any], *, create: bool) -> Dict[str, Any]:
        review_ui = cfg.setdefault("review_ui", {}) if create else cfg.get("review_ui", {})
        if not isinstance(review_ui, dict):
            review_ui = {}
            if create:
                cfg["review_ui"] = review_ui
        return review_ui

    def _review_ui_cfg(self, cfg: Dict[str, Any], *, create: bool = False) -> Dict[str, Any]:
        if not isinstance(cfg, dict):
            return {}
        if isinstance(cfg.get("features"), dict):
            return self._review_ui_from_v3(cfg, create=create)
        if isinstance(cfg.get("handover_log"), dict) and isinstance(cfg.get("download"), dict):
            return self._review_ui_from_runtime_root(cfg, create=create)
        return self._review_ui_from_handover_cfg(cfg, create=create)

    def get_building_defaults(self, config: Dict[str, Any], building: str) -> List[Dict[str, Any]] | None:
        review_ui = self._review_ui_cfg(config, create=False)
        defaults_by_building = review_ui.get(self.DEFAULTS_KEY, {}) if isinstance(review_ui, dict) else {}
        if not isinstance(defaults_by_building, dict):
            return None
        building_name = str(building or "").strip()
        if not building_name:
            return None
        raw = defaults_by_building.get(building_name, {})
        if not isinstance(raw, dict):
            return None
        rows = raw.get("rows", [])
        if not isinstance(rows, list) or not rows:
            return None
        return self.normalize_rows(rows)

    def set_building_defaults(
        self,
        config: Dict[str, Any],
        building: str,
        rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        updated = copy.deepcopy(config if isinstance(config, dict) else {})
        review_ui = self._review_ui_cfg(updated, create=True)
        defaults_by_building = review_ui.get(self.DEFAULTS_KEY, {})
        if not isinstance(defaults_by_building, dict):
            defaults_by_building = {}
        defaults_by_building[str(building or "").strip()] = {"rows": self.normalize_rows(rows)}
        review_ui[self.DEFAULTS_KEY] = defaults_by_building
        return updated

    def build_inventory_block(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        normalized_rows = self.normalize_rows(rows)
        block_rows: List[Dict[str, Any]] = []
        for index, row in enumerate(normalized_rows, start=1):
            cells = row.get("cells", {}) if isinstance(row, dict) else {}
            normalized_cells = self._blank_cells()
            if isinstance(cells, dict):
                for column in self.VISIBLE_COLUMNS:
                    normalized_cells[column] = str(cells.get(column, "") or "").strip()
            block_rows.append(
                {
                    "row_id": f"inventory:config:{index}",
                    "cells": normalized_cells,
                    "is_placeholder_row": not any(normalized_cells.values()),
                }
            )
        return {
            "id": "handover_inventory_table",
            "type": "inventory_table",
            "title": "交接确认",
            "group_title": FOOTER_GROUP_TITLE_TEXT,
            "columns": [dict(item) for item in FOOTER_INVENTORY_COLUMNS],
            "rows": block_rows,
        }

    def apply_building_defaults_to_output(
        self,
        *,
        config: Dict[str, Any],
        building: str,
        output_file: str | Path,
        sheet_name: str,
        emit_log=print,
    ) -> int | None:
        rows = self.get_building_defaults(config, building)
        if rows is None:
            return None

        output_path = Path(str(output_file).strip())
        workbook = openpyxl.load_workbook(output_path)
        try:
            if sheet_name not in workbook.sheetnames:
                raise ValueError(f"交接班模板sheet不存在: {sheet_name}")
            ws = workbook[sheet_name]
            if find_footer_inventory_layout(ws) is None:
                raise ValueError("未找到交接确认区，无法应用楼栋默认工具表")
            write_footer_inventory_table(
                ws=ws,
                inventory_block=self.build_inventory_block(rows),
                emit_log=emit_log,
            )
            atomic_save_workbook(workbook, output_path, temp_suffix=".tmp")
        finally:
            workbook.close()
        return len(rows)
