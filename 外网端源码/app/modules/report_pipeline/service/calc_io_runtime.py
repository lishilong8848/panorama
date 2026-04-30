from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, List


def resolve_config_path(
    config_path: Path | str | None,
    *,
    resolve_pipeline_config_path: Callable[[Path | str | None], Path],
) -> Path:
    return resolve_pipeline_config_path(config_path)


def load_config(
    config_path: Path | str | None,
    *,
    resolve_pipeline_config_path: Callable[[Path | str | None], Path],
) -> Dict[str, Any]:
    resolved = resolve_config_path(
        config_path,
        resolve_pipeline_config_path=resolve_pipeline_config_path,
    )
    with resolved.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def discover_latest_files(excel_dir: Path, buildings: List[str], file_glob_template: str) -> Dict[str, Path]:
    mapping: Dict[str, Path] = {}
    for building in buildings:
        pattern = file_glob_template.format(building=building)
        candidates = [
            p
            for p in excel_dir.glob(pattern)
            if p.is_file() and p.suffix.lower() == ".xlsx" and not p.name.startswith("~$")
        ]
        if not candidates:
            continue
        candidates.sort(key=lambda p: (p.stat().st_mtime, p.name), reverse=True)
        mapping[building] = candidates[0]
    return mapping


def build_results_from_mapping(
    config: Dict[str, Any],
    building_to_file: Dict[str, Path],
    *,
    calculate_monthly_report: Callable[[str, str | None], Any],
    emit_log: Callable[[str], None] = print,
) -> List[Any]:
    if "input" not in config or not isinstance(config["input"], dict):
        raise ValueError("配置错误: input 缺失，请在 JSON 中配置。")
    input_cfg = config["input"]
    if "buildings" not in input_cfg:
        raise ValueError("配置错误: input.buildings 缺失，请在 JSON 中配置。")
    buildings = input_cfg["buildings"]
    ordered_buildings = [b for b in buildings if b in building_to_file]
    extra_buildings = [b for b in building_to_file.keys() if b not in ordered_buildings]
    ordered_buildings.extend(extra_buildings)

    results: List[Any] = []
    for building in ordered_buildings:
        file_path = building_to_file.get(building)
        if file_path is None:
            continue
        emit_log(f"[{building}] 读取文件: {file_path}")
        result = calculate_monthly_report(str(file_path), building)
        results.append(result)
        emit_log(f"[{building}] 计算完成，缺失指标按0处理: {len(result.missing_metrics)}项")
    return results


def build_results_from_file_items(
    file_items: List[Dict[str, str]],
    *,
    calculate_monthly_report: Callable[[str, str | None], Any],
    emit_log: Callable[[str], None] = print,
) -> List[Any]:
    results: List[Any] = []
    for idx, item in enumerate(file_items, 1):
        if not isinstance(item, dict):
            raise ValueError(f"file_items 第{idx}项必须是对象")
        building = str(item.get("building", "")).strip()
        file_path = str(item.get("file_path", "")).strip()
        if not building:
            raise ValueError(f"file_items 第{idx}项 building 不能为空")
        if not file_path:
            raise ValueError(f"file_items 第{idx}项 file_path 不能为空")
        path_obj = Path(file_path)
        if not path_obj.exists():
            raise FileNotFoundError(f"file_items 第{idx}项文件不存在: {file_path}")
        if path_obj.suffix.lower() != ".xlsx":
            raise ValueError(f"file_items 第{idx}项仅支持 xlsx 文件: {file_path}")

        emit_log(f"[{building}] 读取文件: {file_path}")
        result = calculate_monthly_report(str(path_obj), building)
        results.append(result)
        emit_log(f"[{building}] 计算完成，缺失指标按0处理: {len(result.missing_metrics)}项")
    return results
