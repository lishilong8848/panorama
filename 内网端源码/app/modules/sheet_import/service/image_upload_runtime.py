from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional


def parse_image_import_config(import_cfg: Dict[str, Any]) -> Dict[str, Any]:
    raw = import_cfg.get("image_import")
    timeout_default = int(import_cfg.get("timeout", 30))
    if raw is None:
        raw = {
            "enabled": False,
            "mapping_mode": "explicit_then_auto",
            "multi_image_strategy": "all",
            "missing_attachment_field_strategy": "fail_sheet",
            "sheet_image_rules": [],
            "upload_timeout": timeout_default,
        }
    if not isinstance(raw, dict):
        raise ValueError("配置错误: image_import 必须是对象")

    cfg = {
        "enabled": bool(raw.get("enabled", False)),
        "mapping_mode": str(raw.get("mapping_mode", "explicit_then_auto")).strip(),
        "multi_image_strategy": str(raw.get("multi_image_strategy", "all")).strip(),
        "missing_attachment_field_strategy": str(raw.get("missing_attachment_field_strategy", "fail_sheet")).strip(),
        "upload_timeout": int(raw.get("upload_timeout", timeout_default)),
        "sheet_image_rules": raw.get("sheet_image_rules", []),
    }
    if cfg["mapping_mode"] not in {"explicit_then_auto", "explicit_only", "auto_only"}:
        raise ValueError("配置错误: image_import.mapping_mode 仅支持 explicit_then_auto/explicit_only/auto_only")
    if cfg["multi_image_strategy"] not in {"all", "first", "last"}:
        raise ValueError("配置错误: image_import.multi_image_strategy 仅支持 all/first/last")
    if cfg["missing_attachment_field_strategy"] not in {"fail_sheet", "skip_and_log"}:
        raise ValueError("配置错误: image_import.missing_attachment_field_strategy 仅支持 fail_sheet/skip_and_log")
    if cfg["upload_timeout"] <= 0:
        raise ValueError("配置错误: image_import.upload_timeout 必须大于0")
    if not isinstance(cfg["sheet_image_rules"], (list, dict)):
        raise ValueError("配置错误: image_import.sheet_image_rules 必须是数组或对象")
    return cfg


def apply_sheet_images_to_row_payloads(
    *,
    ws: Any,
    sheet_name: str,
    header_row: int,
    row_payloads: List[Any],
    table_fields: List[Dict[str, Any]],
    image_cfg: Dict[str, Any],
    explicit_map_by_sheet: Dict[str, Dict[str, Dict[str, str]]],
    client: Any,
    build_raw_header_name_by_column: Callable[..., Dict[int, str]],
    extract_sheet_images_by_anchor: Callable[..., List[Any]],
    resolve_attachment_target_field: Callable[..., tuple[Optional[str], str]],
    select_tokens_by_strategy: Callable[[List[str], str], List[str]],
    log_func: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    stats = {
        "detected_images": 0,
        "uploaded_images": 0,
        "rows_with_images": 0,
        "missing_mapping_count": 0,
        "orphan_row_images": 0,
    }
    if not bool(image_cfg.get("enabled", False)):
        return stats

    attachment_field_names: List[str] = []
    for item in table_fields:
        name = str(item.get("field_name", "")).strip()
        if not name:
            continue
        field_type = int(item.get("type", 0) or 0)
        if field_type == 17:
            attachment_field_names.append(name)

    raw_header_by_col = build_raw_header_name_by_column(ws=ws, header_row=header_row)
    placements = extract_sheet_images_by_anchor(ws=ws, header_row=header_row)
    stats["detected_images"] = len(placements)
    if not placements:
        return stats
    if log_func:
        log_func(f"[5Sheet导表][{sheet_name}] 检测到图片 {len(placements)} 张，开始上传...")

    fail_on_missing = str(image_cfg.get("missing_attachment_field_strategy", "fail_sheet")) == "fail_sheet"
    mapping_mode = str(image_cfg.get("mapping_mode", "explicit_then_auto")).strip()
    upload_timeout = int(image_cfg.get("upload_timeout", getattr(client, "timeout", 30)))
    multi_strategy = str(image_cfg.get("multi_image_strategy", "all")).strip()

    row_map: Dict[int, Any] = {item.row_index: item for item in row_payloads}
    attachment_tokens_by_row: Dict[int, Dict[str, List[str]]] = {}

    total_images = len(placements)
    for image_idx, placement in enumerate(placements, 1):
        if log_func and (image_idx == 1 or image_idx == total_images or image_idx % 20 == 0):
            log_func(f"[5Sheet导表][{sheet_name}] 图片上传进度: {image_idx}/{total_images}")

        row_payload = row_map.get(placement.row_index)
        if row_payload is None:
            stats["orphan_row_images"] += 1
            continue

        source_column = raw_header_by_col.get(placement.column_index, "")
        if not source_column:
            stats["missing_mapping_count"] += 1
            detail = f"未找到图片列表头: sheet={sheet_name}, row={placement.row_index}, col={placement.column_index}"
            if fail_on_missing:
                raise ValueError(detail)
            continue

        target_field, reason = resolve_attachment_target_field(
            sheet_name=sheet_name,
            source_column=source_column,
            attachment_field_names=attachment_field_names,
            explicit_map_by_sheet=explicit_map_by_sheet,
            mapping_mode=mapping_mode,
        )
        if not target_field:
            stats["missing_mapping_count"] += 1
            detail = (
                f"图片列映射失败: sheet={sheet_name}, row={placement.row_index}, col={placement.column_index}, "
                f"source={source_column}, reason={reason}"
            )
            if fail_on_missing:
                raise ValueError(detail)
            continue

        token = client.upload_attachment_bytes(
            file_name=placement.file_name,
            content=placement.content,
            mime_type=placement.mime_type,
            timeout=upload_timeout,
        )
        stats["uploaded_images"] += 1
        row_bucket = attachment_tokens_by_row.setdefault(placement.row_index, {})
        row_bucket.setdefault(target_field, []).append(token)

    for row_index, field_tokens in attachment_tokens_by_row.items():
        row_payload = row_map.get(row_index)
        if row_payload is None:
            continue
        for field_name, token_list in field_tokens.items():
            selected_tokens = select_tokens_by_strategy(token_list, multi_strategy)
            if not selected_tokens:
                continue
            row_payload.fields[field_name] = [{"file_token": token} for token in selected_tokens]

    stats["rows_with_images"] = len(attachment_tokens_by_row)
    if log_func:
        log_func(
            f"[5Sheet导表][{sheet_name}] 图片上传完成: {stats['uploaded_images']}/{stats['detected_images']}，"
            f"涉及{stats['rows_with_images']}行"
        )
    return stats
