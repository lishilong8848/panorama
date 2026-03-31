from __future__ import annotations

from types import SimpleNamespace

from app.config import config_adapter
from app.modules.report_pipeline.api import routes as pipeline_routes
from app.modules.sheet_import.api import routes as sheet_import_routes


def test_config_adapter_normalizes_legacy_hybrid_role_to_empty() -> None:
    assert config_adapter._normalize_role_mode("hybrid") == ""
    assert config_adapter._normalize_role_mode("external") == "external"
    assert config_adapter._normalize_role_mode("unknown") == ""


def test_pipeline_route_treats_legacy_hybrid_role_as_unselected() -> None:
    container = SimpleNamespace(deployment_snapshot=lambda: {"role_mode": "hybrid"})

    assert pipeline_routes._deployment_role_mode(container) == ""


def test_sheet_import_route_treats_legacy_hybrid_role_as_unselected() -> None:
    container = SimpleNamespace(deployment_snapshot=lambda: {"role_mode": "hybrid"})

    assert sheet_import_routes._deployment_role_mode(container) == ""
