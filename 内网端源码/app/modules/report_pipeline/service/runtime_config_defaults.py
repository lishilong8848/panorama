from __future__ import annotations

from typing import Any, Dict


def default_resume_config() -> Dict[str, Any]:
    return {
        "enabled": True,
        "retention_days": 7,
        "auto_continue_when_external": True,
        "auto_continue_poll_sec": 5,
        "gc_every_n_items": 5,
        "upload_chunk_threshold": 20,
        "upload_chunk_size": 5,
        "root_dir": "pipeline_resume",
        "index_file": "index.json",
    }


def default_performance_config() -> Dict[str, Any]:
    return {
        "query_result_timeout_ms": 10000,
        "login_fill_timeout_ms": 5000,
        "start_end_visible_timeout_ms": 3000,
        "force_iframe_reopen_each_task": True,
        "page_refresh_retry_count": 1,
        "retry_failed_after_all_done": True,
        "retry_failed_max_rounds": 1,
    }


def ensure_resume_config(download_cfg: Dict[str, Any]) -> Dict[str, Any]:
    resume_cfg = download_cfg.get("resume")
    if not isinstance(resume_cfg, dict):
        resume_cfg = {}
        download_cfg["resume"] = resume_cfg
    defaults = default_resume_config()
    for key, value in defaults.items():
        resume_cfg.setdefault(key, value)
    return resume_cfg


def ensure_performance_config(download_cfg: Dict[str, Any]) -> Dict[str, Any]:
    perf_cfg = download_cfg.get("performance")
    if not isinstance(perf_cfg, dict):
        perf_cfg = {}
        download_cfg["performance"] = perf_cfg
    defaults = default_performance_config()
    for key, value in defaults.items():
        perf_cfg.setdefault(key, value)
    return perf_cfg
