from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict, List

from pipeline_utils import load_download_module


def resolve_monthly_bridge_root(shared_root_dir: str | Path) -> Path:
    return Path(shared_root_dir) / "artifacts" / "monthly_report"


def resolve_monthly_bridge_resume_root(shared_root_dir: str | Path) -> Path:
    root = resolve_monthly_bridge_root(shared_root_dir) / "resume"
    root.mkdir(parents=True, exist_ok=True)
    return root


def resolve_monthly_bridge_source_root(shared_root_dir: str | Path, task_id: str) -> Path:
    root = resolve_monthly_bridge_root(shared_root_dir) / str(task_id or "").strip() / "source_files"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _clone_monthly_bridge_config(
    runtime_config: Dict[str, Any],
    *,
    shared_root_dir: str | Path,
    task_id: str | None = None,
    disable_upload: bool,
) -> Dict[str, Any]:
    cfg = copy.deepcopy(runtime_config if isinstance(runtime_config, dict) else {})
    download_cfg = cfg.setdefault("download", {})
    if not isinstance(download_cfg, dict):
        download_cfg = {}
        cfg["download"] = download_cfg
    resume_cfg = download_cfg.setdefault("resume", {})
    if not isinstance(resume_cfg, dict):
        resume_cfg = {}
        download_cfg["resume"] = resume_cfg
    feishu_cfg = cfg.setdefault("feishu", {})
    if not isinstance(feishu_cfg, dict):
        feishu_cfg = {}
        cfg["feishu"] = feishu_cfg

    shared_resume_root = resolve_monthly_bridge_resume_root(shared_root_dir)
    download_cfg["resume"] = {**resume_cfg, "enabled": True, "root_dir": str(shared_resume_root)}
    if task_id:
        download_cfg["save_dir"] = str(resolve_monthly_bridge_source_root(shared_root_dir, task_id))
        download_cfg["run_subdir_mode"] = "none"
    if disable_upload:
        feishu_cfg["enable_upload"] = False
    return cfg


def run_bridge_download_only_auto_once(
    runtime_config: Dict[str, Any],
    *,
    shared_root_dir: str | Path,
    task_id: str,
    source_name: str = "共享桥接月报下载",
) -> Dict[str, Any]:
    module = load_download_module()
    if not hasattr(module, "run_download_only_auto_once"):
        raise RuntimeError("下载脚本缺少 run_download_only_auto_once 入口")
    cfg = _clone_monthly_bridge_config(
        runtime_config,
        shared_root_dir=shared_root_dir,
        task_id=task_id,
        disable_upload=True,
    )
    result = module.run_download_only_auto_once(cfg, source_name=source_name)
    if isinstance(result, dict):
        result["bridge_source_root"] = str(resolve_monthly_bridge_source_root(shared_root_dir, task_id))
        result["bridge_resume_root"] = str(resolve_monthly_bridge_resume_root(shared_root_dir))
    return result


def run_bridge_download_only_multi_date(
    runtime_config: Dict[str, Any],
    *,
    shared_root_dir: str | Path,
    task_id: str,
    selected_dates: List[str],
    source_name: str = "共享桥接月报多日期下载",
) -> Dict[str, Any]:
    module = load_download_module()
    if not hasattr(module, "run_download_only_with_selected_dates"):
        raise RuntimeError("下载脚本缺少 run_download_only_with_selected_dates 入口")
    cfg = _clone_monthly_bridge_config(
        runtime_config,
        shared_root_dir=shared_root_dir,
        task_id=task_id,
        disable_upload=True,
    )
    result = module.run_download_only_with_selected_dates(
        cfg,
        selected_dates=selected_dates,
        source_name=source_name,
    )
    if isinstance(result, dict):
        result["bridge_source_root"] = str(resolve_monthly_bridge_source_root(shared_root_dir, task_id))
        result["bridge_resume_root"] = str(resolve_monthly_bridge_resume_root(shared_root_dir))
    return result


def run_bridge_resume_upload(
    runtime_config: Dict[str, Any],
    *,
    shared_root_dir: str | Path,
    run_id: str | None = None,
    auto_trigger: bool = False,
) -> Dict[str, Any]:
    module = load_download_module()
    if not hasattr(module, "run_resume_upload"):
        raise RuntimeError("下载脚本缺少 run_resume_upload 入口")
    cfg = _clone_monthly_bridge_config(
        runtime_config,
        shared_root_dir=shared_root_dir,
        task_id=None,
        disable_upload=False,
    )
    result = module.run_resume_upload(config=cfg, run_id=run_id, auto_trigger=auto_trigger)
    if isinstance(result, dict):
        result["bridge_resume_root"] = str(resolve_monthly_bridge_resume_root(shared_root_dir))
    return result


def list_bridge_pending_resume_runs(
    runtime_config: Dict[str, Any],
    *,
    shared_root_dir: str | Path,
) -> List[Dict[str, Any]]:
    module = load_download_module()
    if not hasattr(module, "list_pending_upload_runs"):
        raise RuntimeError("下载脚本缺少 list_pending_upload_runs 入口")
    cfg = _clone_monthly_bridge_config(
        runtime_config,
        shared_root_dir=shared_root_dir,
        task_id=None,
        disable_upload=False,
    )
    rows = module.list_pending_upload_runs(config=cfg)
    return rows if isinstance(rows, list) else []


def delete_bridge_resume_run(
    runtime_config: Dict[str, Any],
    *,
    shared_root_dir: str | Path,
    run_id: str,
) -> Dict[str, Any]:
    module = load_download_module()
    if not hasattr(module, "delete_pending_upload_run"):
        raise RuntimeError("下载脚本缺少 delete_pending_upload_run 入口")
    cfg = _clone_monthly_bridge_config(
        runtime_config,
        shared_root_dir=shared_root_dir,
        task_id=None,
        disable_upload=False,
    )
    return module.delete_pending_upload_run(config=cfg, run_id=run_id)
