from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from app.modules.sheet_import.core.field_value_converter import normalize_field_name

IMAGE_MAPPING_MODE = "explicit_then_auto"


def build_explicit_image_mapping(sheet_image_rules: Any) -> Dict[str, Dict[str, Dict[str, str]]]:
    normalized_rules: List[Dict[str, Any]] = []
    if isinstance(sheet_image_rules, dict):
        for sheet_name, value in sheet_image_rules.items():
            if isinstance(value, dict):
                if "mappings" in value or "column_to_field" in value:
                    normalized_rules.append(
                        {
                            "sheet_name": str(sheet_name).strip(),
                            **value,
                        }
                    )
                else:
                    normalized_rules.append(
                        {
                            "sheet_name": str(sheet_name).strip(),
                            "column_to_field": value,
                        }
                    )
            elif isinstance(value, list):
                normalized_rules.append(
                    {
                        "sheet_name": str(sheet_name).strip(),
                        "mappings": value,
                    }
                )
            else:
                raise ValueError(f"配置错误: image_import.sheet_image_rules[{sheet_name}] 必须是对象或数组")
    elif isinstance(sheet_image_rules, list):
        normalized_rules = sheet_image_rules
    else:
        raise ValueError("配置错误: image_import.sheet_image_rules 必须是数组或对象")

    mapping: Dict[str, Dict[str, Dict[str, str]]] = {}
    for idx, rule in enumerate(normalized_rules, 1):
        if not isinstance(rule, dict):
            raise ValueError(f"配置错误: image_import.sheet_image_rules 第{idx}项必须是对象")
        sheet_name = str(rule.get("sheet_name", "")).strip()
        if not sheet_name:
            raise ValueError(f"配置错误: image_import.sheet_image_rules 第{idx}项缺少 sheet_name")

        items: List[Tuple[str, str]] = []
        if isinstance(rule.get("mappings"), list):
            for j, item in enumerate(rule["mappings"], 1):
                if not isinstance(item, dict):
                    raise ValueError(
                        f"配置错误: image_import.sheet_image_rules 第{idx}项 mappings 第{j}项必须是对象"
                    )
                source = str(item.get("source_column", "")).strip()
                target = str(item.get("target_field", "")).strip()
                if source and target:
                    items.append((source, target))
        if isinstance(rule.get("column_to_field"), dict):
            for source, target in rule["column_to_field"].items():
                source_text = str(source).strip()
                target_text = str(target).strip()
                if source_text and target_text:
                    items.append((source_text, target_text))
        source_single = str(rule.get("source_column", "")).strip()
        target_single = str(rule.get("target_field", "")).strip()
        if source_single and target_single:
            items.append((source_single, target_single))

        sheet_key = normalize_field_name(sheet_name)
        if not sheet_key:
            continue
        if sheet_key not in mapping:
            mapping[sheet_key] = {"exact": {}, "norm": {}}
        for source, target in items:
            mapping[sheet_key]["exact"][source] = target
            mapping[sheet_key]["norm"][normalize_field_name(source)] = target
    return mapping


def auto_pick_attachment_field(source_column: str, attachment_field_names: List[str]) -> Tuple[Optional[str], str]:
    if not attachment_field_names:
        return None, "目标表无附件字段"
    source_text = str(source_column).strip()
    if not source_text:
        return None, "源图片列名为空"

    if source_text in attachment_field_names:
        return source_text, ""
    picture_name = f"{source_text}图片"
    if picture_name in attachment_field_names:
        return picture_name, ""

    source_norm = normalize_field_name(source_text)
    if not source_norm:
        return None, f"源图片列名无法规范化: {source_text}"

    exact_norm_hits: List[str] = []
    prefix_hits: List[str] = []
    for field in attachment_field_names:
        field_norm = normalize_field_name(field)
        if field_norm == source_norm:
            exact_norm_hits.append(field)
            continue
        if field_norm.startswith(source_norm) or source_norm.startswith(field_norm):
            prefix_hits.append(field)

    if len(exact_norm_hits) == 1:
        return exact_norm_hits[0], ""
    if len(exact_norm_hits) > 1:
        return None, f"附件字段匹配不唯一: {exact_norm_hits}"
    if len(prefix_hits) == 1:
        return prefix_hits[0], ""
    if len(prefix_hits) > 1:
        return None, f"附件字段匹配不唯一: {prefix_hits}"
    return None, f"未找到附件字段映射: source={source_text}"


def resolve_attachment_target_field(
    sheet_name: str,
    source_column: str,
    attachment_field_names: List[str],
    explicit_map_by_sheet: Dict[str, Dict[str, Dict[str, str]]],
    mapping_mode: str,
) -> Tuple[Optional[str], str]:
    sheet_key = normalize_field_name(sheet_name)
    explicit = explicit_map_by_sheet.get(sheet_key, {})
    explicit_target: Optional[str] = None
    explicit_reason = ""

    if mapping_mode in {"explicit_then_auto", "explicit_only"}:
        source_text = str(source_column).strip()
        source_norm = normalize_field_name(source_text)
        exact_map = explicit.get("exact", {}) if isinstance(explicit, dict) else {}
        norm_map = explicit.get("norm", {}) if isinstance(explicit, dict) else {}
        if source_text in exact_map:
            explicit_target = str(exact_map[source_text]).strip()
        elif source_norm in norm_map:
            explicit_target = str(norm_map[source_norm]).strip()

        if explicit_target:
            if explicit_target in attachment_field_names:
                return explicit_target, ""
            explicit_reason = f"显式映射的附件字段不存在或非附件字段: {explicit_target}"
        else:
            explicit_reason = f"未配置显式映射: {sheet_name}.{source_column}"

        if mapping_mode == "explicit_only":
            return None, explicit_reason

    if mapping_mode in {"explicit_then_auto", "auto_only"}:
        auto_target, auto_reason = auto_pick_attachment_field(
            source_column=source_column,
            attachment_field_names=attachment_field_names,
        )
        if auto_target:
            return auto_target, ""
        if explicit_reason:
            return None, f"{explicit_reason}; 自动兜底失败: {auto_reason}"
        return None, auto_reason

    return None, f"不支持的图片映射模式: {mapping_mode}"


def select_tokens_by_strategy(tokens: List[str], strategy: str) -> List[str]:
    if not tokens:
        return []
    if strategy == "first":
        return [tokens[0]]
    if strategy == "last":
        return [tokens[-1]]
    return list(tokens)
