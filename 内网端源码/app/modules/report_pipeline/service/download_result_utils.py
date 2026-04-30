from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple


def collect_first_pass_results(first_pass_pairs: List[Tuple[Any, Any]]) -> tuple[Dict[Tuple[str, str], Tuple[Any, Any]], List[Any]]:
    final_outcome_by_key: Dict[Tuple[str, str], Tuple[Any, Any]] = {}
    failed_for_retry: List[Any] = []
    for task, outcome in first_pass_pairs:
        site = getattr(task, "site", {})
        building = str(getattr(outcome, "building", "") or (site.get("building", "") if isinstance(site, dict) else "")).strip()
        building = building or "-"
        key = (str(getattr(task, "date_text", "")).strip(), building)
        final_outcome_by_key[key] = (task, outcome)
        if not bool(getattr(outcome, "success", False)):
            failed_for_retry.append(task)
    return final_outcome_by_key, failed_for_retry


def merge_retry_results(
    final_outcome_by_key: Dict[Tuple[str, str], Tuple[Any, Any]],
    retry_pairs: List[Tuple[Any, Any]],
) -> Dict[Tuple[str, str], Tuple[Any, Any]]:
    for task, outcome in retry_pairs:
        site = getattr(task, "site", {})
        building = str(getattr(outcome, "building", "") or (site.get("building", "") if isinstance(site, dict) else "")).strip()
        building = building or "-"
        key = (str(getattr(task, "date_text", "")).strip(), building)
        prev = final_outcome_by_key.get(key)
        if prev is None:
            final_outcome_by_key[key] = (task, outcome)
        elif bool(getattr(prev[1], "success", False)):
            continue
        else:
            final_outcome_by_key[key] = (task, outcome)
    return final_outcome_by_key


def apply_download_outcomes(
    *,
    final_outcome_by_key: Dict[Tuple[str, str], Tuple[Any, Any]],
    date_result_by_date: Dict[str, Dict[str, Any]],
    summary: Dict[str, Any],
    checkpoint: Dict[str, Any],
    notify_failure: Callable[[str, str, str], None],
) -> None:
    for (date_text, building), (_task, outcome) in sorted(
        final_outcome_by_key.items(),
        key=lambda x: (x[0][0], x[0][1]),
    ):
        date_result = date_result_by_date.get(date_text)
        if date_result is None:
            continue

        if bool(getattr(outcome, "success", False)):
            success_buildings = date_result.setdefault("success_buildings", [])
            if building not in success_buildings:
                success_buildings.append(building)

            file_path = str(getattr(outcome, "file_path", "")).strip()
            summary.setdefault("file_items", []).append(
                {
                    "building": building,
                    "file_path": file_path,
                    "upload_date": date_text,
                }
            )
            checkpoint.setdefault("file_items", []).append(
                {
                    "building": building,
                    "file_path": file_path,
                    "upload_date": date_text,
                    "status": "pending",
                    "attempts": 0,
                    "last_error": "",
                }
            )
            continue

        error = str(getattr(outcome, "error", "")).strip()
        date_result.setdefault("failed_buildings", []).append({"building": building, "error": error})
        notify_failure(date_text, building, error)

    ordered_dates = sorted(date_result_by_date.keys())
    summary["date_results"] = [date_result_by_date[d] for d in ordered_dates]
    summary["success_dates"] = []
    summary["failed_dates"] = []
    for date_text in ordered_dates:
        one = date_result_by_date[date_text]
        if one.get("success_buildings"):
            summary["success_dates"].append(date_text)
        else:
            summary["failed_dates"].append(date_text)

    summary["processed_dates"] = len(summary["date_results"])
    summary["total_files"] = len(summary.get("file_items", []))
