from __future__ import annotations

import copy
from typing import Any, Callable, Dict, List

from pipeline_utils import load_calc_module


def run_monthly_from_file_items(
    runtime_config: Dict[str, Any],
    *,
    file_items: List[Dict[str, Any]],
    emit_log: Callable[[str], None] = print,
    source_label: str = "共享缓存月报",
) -> Dict[str, Any]:
    normalized_items: List[Dict[str, str]] = []
    for item in file_items if isinstance(file_items, list) else []:
        if not isinstance(item, dict):
            continue
        building = str(item.get("building", "") or "").strip()
        file_path = str(item.get("file_path", "") or "").strip()
        upload_date = str(item.get("upload_date", "") or "").strip()
        if not building or not file_path:
            continue
        row = {"building": building, "file_path": file_path}
        if upload_date:
            row["upload_date"] = upload_date
        normalized_items.append(row)
    if not normalized_items:
        raise RuntimeError("共享缓存中没有可继续处理的月报源文件")

    cfg = copy.deepcopy(runtime_config if isinstance(runtime_config, dict) else {})
    calc_module = load_calc_module()
    emit_log(f"[{source_label}] 开始执行: files={len(normalized_items)}")
    results = calc_module.run_with_explicit_file_items(
        config=cfg,
        file_items=normalized_items,
        upload=True,
        save_json=False,
        upload_log_feature=source_label,
    )
    emit_log(f"[{source_label}] 执行完成: result_count={len(results)}")
    return {
        "status": "success",
        "result_count": len(results),
        "file_items": normalized_items,
    }
