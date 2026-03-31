from __future__ import annotations

from app.modules.report_pipeline.service.runtime_config_defaults import (
    ensure_performance_config,
    ensure_resume_config,
)


def test_ensure_resume_config_applies_defaults():
    cfg = {}
    out = ensure_resume_config(cfg)
    assert out["enabled"] is True
    assert out["upload_chunk_size"] == 5
    assert cfg["resume"] is out


def test_ensure_performance_config_applies_defaults():
    cfg = {}
    out = ensure_performance_config(cfg)
    assert out["query_result_timeout_ms"] == 10000
    assert out["force_iframe_reopen_each_task"] is True
