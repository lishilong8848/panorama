from __future__ import annotations

import json
import re
from inspect import Parameter, signature
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.report_pipeline.service.source_path_identity import source_file_identity_key


def _call_with_optional_emit_log(fn: Callable[..., Any], *args: Any, emit_log: Callable[[str], None], **kwargs: Any) -> Any:
    try:
        sig = signature(fn)
    except (TypeError, ValueError):
        sig = None
    if sig is None:
        return fn(*args, emit_log=emit_log, **kwargs)
    params = sig.parameters
    supports_emit_log = "emit_log" in params or any(param.kind == Parameter.VAR_KEYWORD for param in params.values())
    if not supports_emit_log:
        return fn(*args, **kwargs)
    try:
        return fn(*args, emit_log=emit_log, **kwargs)
    except TypeError:
        raise


def run_with_config(
    config: Dict[str, Any],
    *,
    discover_latest_files: Callable[[Path, List[str], str], Dict[str, Path]],
    build_results_from_mapping: Callable[[Dict[str, Any], Dict[str, Path]], List[Any]],
    emit_log: Callable[[str], None] = print,
) -> List[Any]:
    if "input" not in config or not isinstance(config["input"], dict):
        raise ValueError("配置错误: input 缺失，请在 JSON 中配置。")
    input_cfg = config["input"]
    required_keys = ["excel_dir", "buildings"]
    missing = [key for key in required_keys if key not in input_cfg]
    if missing:
        raise ValueError(f"配置错误: input 缺少字段 {missing}")

    excel_dir = Path(input_cfg["excel_dir"])
    buildings = input_cfg["buildings"]
    file_glob_template = str(input_cfg.get("file_glob_template", "{building}_*.xlsx")).strip() or "{building}_*.xlsx"

    if not excel_dir.exists():
        raise FileNotFoundError(f"Excel目录不存在: {excel_dir}")

    building_to_file = discover_latest_files(excel_dir, buildings, file_glob_template)
    for building in buildings:
        if building not in building_to_file:
            emit_log(f"[{building}] 未找到 xlsx 文件，已保留楼栋配置并跳过。")
    return build_results_from_mapping(config, building_to_file)


def run_with_explicit_files(
    config: Dict[str, Any],
    building_to_file: Dict[str, str],
    *,
    build_results_from_mapping: Callable[[Dict[str, Any], Dict[str, Path]], List[Any]],
    save_results_fn: Callable[[List[Any], Dict[str, Any]], None],
    upload_results_to_feishu_fn: Callable[..., None],
    upload: bool = True,
    save_json: bool = False,
    upload_log_feature: str = "月报上传",
) -> List[Any]:
    normalized: Dict[str, Path] = {}
    for building, file_path in building_to_file.items():
        if not file_path:
            continue
        path_obj = Path(file_path)
        if not path_obj.exists():
            raise FileNotFoundError(f"[{building}] 文件不存在: {file_path}")
        if path_obj.suffix.lower() != ".xlsx":
            raise ValueError(f"[{building}] 仅支持 xlsx 文件: {file_path}")
        normalized[building] = path_obj

    results = build_results_from_mapping(config, normalized)
    if not results:
        return []

    if save_json:
        save_results_fn(results, config)
    if upload:
        upload_results_to_feishu_fn(results, config, log_feature=upload_log_feature)
    return results


def run_with_explicit_file_items(
    config: Dict[str, Any],
    file_items: List[Dict[str, str]],
    *,
    build_results_from_file_items: Callable[[List[Dict[str, str]]], List[Any]],
    save_results_fn: Callable[[List[Any], Dict[str, Any]], None],
    upload_results_to_feishu_fn: Callable[..., None],
    upload: bool = True,
    save_json: bool = False,
    upload_log_feature: str = "月报上传",
    emit_log: Callable[[str], None] = print,
) -> List[Any]:
    normalized_items: List[Dict[str, str]] = []
    source_date_map: Dict[str, str] = {}
    for idx, item in enumerate(file_items, 1):
        if not isinstance(item, dict):
            raise ValueError(f"file_items 第{idx}项必须是对象")
        building = str(item.get("building", "")).strip()
        file_path = str(item.get("file_path", "")).strip()
        upload_date = str(item.get("upload_date", "")).strip()
        if not building:
            raise ValueError(f"file_items 第{idx}项 building 不能为空")
        if not file_path:
            raise ValueError(f"file_items 第{idx}项 file_path 不能为空")

        path_obj = Path(file_path)
        if not path_obj.exists():
            raise FileNotFoundError(f"file_items 第{idx}项文件不存在: {file_path}")
        if path_obj.suffix.lower() != ".xlsx":
            raise ValueError(f"file_items 第{idx}项仅支持 xlsx 文件: {file_path}")

        normalized_item = {
            "building": building,
            "file_path": str(path_obj),
        }
        normalized_items.append(normalized_item)

        if upload_date:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", upload_date):
                raise ValueError(f"file_items 第{idx}项 upload_date 格式错误，必须为YYYY-MM-DD")
            source_date_map[source_file_identity_key(path_obj)] = upload_date

    results = _call_with_optional_emit_log(
        build_results_from_file_items,
        normalized_items,
        emit_log=emit_log,
    )
    if not results:
        emit_log(f"[{upload_log_feature}] 计算阶段完成: results=0")
        return []
    emit_log(f"[{upload_log_feature}] 计算阶段完成: results={len(results)}")

    if save_json:
        _call_with_optional_emit_log(save_results_fn, results, config, emit_log=emit_log)
    if upload:
        emit_log(f"[{upload_log_feature}] 准备进入飞书上传")
        _call_with_optional_emit_log(
            upload_results_to_feishu_fn,
            results,
            config,
            date_override_by_source=source_date_map if source_date_map else None,
            log_feature=upload_log_feature,
            emit_log=emit_log,
        )
        emit_log(f"[{upload_log_feature}] 飞书上传调用完成")
    return results


def save_results(
    results: List[Any],
    config: Dict[str, Any],
    *,
    emit_log: Callable[[str], None] = print,
) -> None:
    if "output" not in config or not isinstance(config["output"], dict):
        raise ValueError("配置错误: output 缺失，请在 JSON 中配置。")
    output_cfg = config["output"]
    if "save_json" not in output_cfg:
        raise ValueError("配置错误: output.save_json 缺失，请在 JSON 中配置。")
    if not bool(output_cfg["save_json"]):
        return
    if "json_dir" not in output_cfg:
        raise ValueError("配置错误: output.json_dir 缺失，请在 JSON 中配置。")

    output_dir = Path(output_cfg["json_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    merged = []
    used_names: Dict[str, int] = {}
    for result in results:
        data = result.to_dict()
        merged.append(data)
        base_name = f"{result.building}_{result.month}_计算结果"
        seq = used_names.get(base_name, 0) + 1
        used_names[base_name] = seq
        file_name = f"{base_name}.json" if seq == 1 else f"{base_name}_{seq}.json"
        out_path = output_dir / file_name
        out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        emit_log(f"[{result.building}] 已输出: {out_path}")

    merged_path = output_dir / "全部楼栋_计算结果.json"
    merged_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    emit_log(f"[汇总] 已输出: {merged_path}")
