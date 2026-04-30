from __future__ import annotations

import copy
from typing import Any, Dict

from handover_log_module.core.cell_rule_compiler import build_effective_handover_config, normalize_cell_rules
from handover_log_module.core.models import MetricHit, RawRow
from handover_log_module.core.selectors import compute_metric_hits
from handover_log_module.repository.excel_reader import load_rows


class HandoverExtractService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    def _buildings_for_rules(self) -> list[str]:
        output: list[str] = []

        sites = self.config.get("sites", [])
        if isinstance(sites, list):
            for site in sites:
                if not isinstance(site, dict):
                    continue
                building = str(site.get("building", "")).strip()
                if building and building not in output:
                    output.append(building)

        global_download = self.config.get("_global_download", {})
        if isinstance(global_download, dict):
            global_sites = global_download.get("sites", [])
            if isinstance(global_sites, list):
                for site in global_sites:
                    if not isinstance(site, dict):
                        continue
                    building = str(site.get("building", "")).strip()
                    if building and building not in output:
                        output.append(building)

        return output

    def _effective_config(self, building: str) -> Dict[str, Any]:
        base_cfg = copy.deepcopy(self.config)
        buildings = self._buildings_for_rules()
        if building and building not in buildings:
            buildings.append(building)

        base_cfg["cell_rules"] = normalize_cell_rules(base_cfg, buildings)
        return build_effective_handover_config(base_cfg, building, buildings)

    def extract(
        self,
        *,
        building: str,
        data_file: str,
    ) -> Dict[str, Any]:
        cfg = self._effective_config(building)
        parsing_cfg = cfg.get("parsing", {})
        normalize_cfg = cfg.get("normalize", {})
        rules = cfg.get("rules", {})
        if not isinstance(rules, dict) or not rules:
            raise ValueError("配置错误: handover_log.cell_rules 不能为空或无可用规则")

        rows = load_rows(
            data_file=data_file,
            parsing_cfg=parsing_cfg if isinstance(parsing_cfg, dict) else {},
            normalize_cfg=normalize_cfg if isinstance(normalize_cfg, dict) else {},
        )
        hits, missing = compute_metric_hits(rows=rows, rules=rules)
        return {
            "rows": rows,
            "hits": hits,
            "missing_metrics": missing,
            "effective_config": cfg,
        }
