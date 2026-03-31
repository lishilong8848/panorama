from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple


def build_time_window_download_tasks(
    *,
    time_windows: List[Dict[str, str]],
    enabled_sites: List[Dict[str, Any]],
    run_save_dir: str,
    config: Dict[str, Any],
    task_factory: Callable[..., Any],
    emit_log: Callable[[str], None],
) -> Tuple[Dict[str, Dict[str, Any]], List[Any]]:
    date_result_by_date: Dict[str, Dict[str, Any]] = {}
    all_download_tasks: List[Any] = []

    for window in time_windows:
        date_text = str(window.get("date", "")).strip()
        start_time = str(window.get("start_time", "")).strip()
        end_time = str(window.get("end_time", "")).strip()
        if not date_text or not start_time or not end_time:
            continue

        emit_log(f"[时间窗][{date_text}] start={start_time}, end={end_time}")
        config["_runtime"] = {
            "time_range_start": start_time,
            "time_range_end": end_time,
        }
        date_dir = Path(run_save_dir) / date_text.replace("-", "")
        date_dir.mkdir(parents=True, exist_ok=True)

        date_result_by_date[date_text] = {
            "date": date_text,
            "start_time": start_time,
            "end_time": end_time,
            "success_buildings": [],
            "failed_buildings": [],
        }

        for site in enabled_sites:
            all_download_tasks.append(
                task_factory(
                    date_text=date_text,
                    start_time=start_time,
                    end_time=end_time,
                    save_dir=str(date_dir),
                    site=copy.deepcopy(site),
                    attempt_round="first_pass",
                )
            )

    return date_result_by_date, all_download_tasks
