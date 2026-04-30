from __future__ import annotations

from typing import Any, Callable, Dict

from app.modules.report_pipeline.service.orchestrator_service import OrchestratorService


class DownloaderService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self._orchestrator = OrchestratorService(config)

    def run_auto_once(self, emit_log: Callable[[str], None]) -> Dict[str, Any]:
        return self._orchestrator.run_auto_once(emit_log)
