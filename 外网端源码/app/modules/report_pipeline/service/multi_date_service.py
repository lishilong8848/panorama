from __future__ import annotations

from typing import Any, Callable, Dict, List

from app.modules.report_pipeline.service.orchestrator_service import OrchestratorService


class MultiDateService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._orchestrator = OrchestratorService(config)

    def run(self, dates: List[str], emit_log: Callable[[str], None]) -> Dict[str, Any]:
        return self._orchestrator.run_multi_date(dates, emit_log)
