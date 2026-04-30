from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List


def upload_retryable_items(
    *,
    config: Dict[str, Any],
    calc_module: Any,
    checkpoint: Dict[str, Any],
    gc_every_n_items: int,
    upload_chunk_threshold: int,
    upload_chunk_size: int,
    collect_retryable_file_items: Callable[[Dict[str, Any]], List[Dict[str, Any]]],
    now_text: Callable[[], str],
    save_checkpoint_and_index: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]],
    log_file_failure: Callable[..., None],
    refresh_checkpoint_summary: Callable[[Dict[str, Any]], Dict[str, Any]],
    gc_collect: Callable[[], None],
) -> Dict[str, Any]:
    retryable_rows = collect_retryable_file_items(checkpoint)
    if not retryable_rows:
        return {
            "processed_count": 0,
            "success_count": 0,
            "failed_count": 0,
            "failure_items": [],
        }

    processed_count = 0
    success_count = 0
    failed_count = 0
    failure_items: List[Dict[str, str]] = []

    def _save_progress() -> None:
        checkpoint["updated_at"] = now_text()
        save_checkpoint_and_index(config, checkpoint)

    def _handle_one_item(item: Dict[str, Any]) -> None:
        nonlocal processed_count, success_count, failed_count
        building = str(item.get("building", "")).strip()
        file_path = str(item.get("file_path", "")).strip()
        upload_date = str(item.get("upload_date", "")).strip()
        item["attempts"] = int(item.get("attempts", 0)) + 1

        if not file_path or not Path(file_path).exists():
            err = f"文件不存在: {file_path}"
            item["status"] = "file_missing"
            item["last_error"] = err
            failed_count += 1
            failure_items.append({"building": building, "file_path": file_path, "error": err})
            log_file_failure(
                feature="断点续传",
                stage="文件缺失",
                building=building,
                file_path=file_path,
                upload_date=upload_date,
                error=err,
            )
        else:
            try:
                calc_module.run_with_explicit_file_items(
                    config=config,
                    file_items=[{"building": building, "file_path": file_path, "upload_date": upload_date}],
                    upload=True,
                    save_json=False,
                    upload_log_feature="断点续传",
                )
                item["status"] = "uploaded"
                item["last_error"] = ""
                success_count += 1
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
                item["status"] = "upload_failed"
                item["last_error"] = err
                failed_count += 1
                failure_items.append({"building": building, "file_path": file_path, "error": err})

        processed_count += 1
        _save_progress()
        if processed_count % max(1, gc_every_n_items) == 0:
            gc_collect()

    total_items = len(retryable_rows)
    threshold = max(1, int(upload_chunk_threshold))
    chunk_size = max(1, int(upload_chunk_size))
    if total_items > threshold:
        for i in range(0, total_items, chunk_size):
            chunk = retryable_rows[i : i + chunk_size]
            for item in chunk:
                _handle_one_item(item)
            gc_collect()
    else:
        for item in retryable_rows:
            _handle_one_item(item)

    refresh_checkpoint_summary(checkpoint)
    _save_progress()
    return {
        "processed_count": processed_count,
        "success_count": success_count,
        "failed_count": failed_count,
        "failure_items": failure_items,
    }
