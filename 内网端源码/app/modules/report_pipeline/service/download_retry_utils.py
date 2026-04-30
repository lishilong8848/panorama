from __future__ import annotations

import copy
from typing import Any, Awaitable, Callable, Dict, List, Tuple


def _build_retry_round_tasks(current: List[Any], round_label: str) -> List[Any]:
    out: List[Any] = []
    for item in current:
        task_cls = item.__class__
        out.append(
            task_cls(
                date_text=getattr(item, "date_text", ""),
                start_time=getattr(item, "start_time", ""),
                end_time=getattr(item, "end_time", ""),
                save_dir=getattr(item, "save_dir", ""),
                site=copy.deepcopy(getattr(item, "site", {})),
                attempt_round=round_label,
            )
        )
    return out


async def retry_failed_download_tasks(
    *,
    config: Dict[str, Any],
    failed_tasks: List[Any],
    source_name: str,
    run_download_tasks_by_building: Callable[..., Awaitable[List[Tuple[Any, Any]]]],
) -> List[Tuple[Any, Any]]:
    if not failed_tasks:
        return []

    perf_cfg = config["download"]["performance"]
    if not bool(perf_cfg["retry_failed_after_all_done"]):
        return []

    max_rounds = max(0, int(perf_cfg["retry_failed_max_rounds"]))
    if max_rounds <= 0:
        return []

    results: List[Tuple[Any, Any]] = []
    current = [copy.deepcopy(item) for item in failed_tasks]
    for round_idx in range(max_rounds):
        round_label = f"retry_pass_{round_idx + 1}"
        round_tasks = _build_retry_round_tasks(current, round_label)
        round_results = await run_download_tasks_by_building(
            config=config,
            download_tasks=round_tasks,
            feature=source_name,
            success_stage="内网下载",
            failure_stage="内网下载(二次补下载)",
            success_detail_prefix="二次补下载成功 URL=",
        )
        results.extend(round_results)
        current = [task for task, outcome in round_results if not bool(getattr(outcome, "success", False))]
        if not current:
            break
    return results
