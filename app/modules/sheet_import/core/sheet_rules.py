from __future__ import annotations

from typing import Any, Dict, List

from app.modules.sheet_import.core.field_value_converter import normalize_field_name


def get_sheet_rules(config: Dict[str, Any]) -> Any:
    return config.get("feishu_sheet_import", {}).get("sheet_rules", {})


def normalize_sheet_rules(raw_rules: Any) -> List[Dict[str, Any]]:
    if isinstance(raw_rules, list):
        normalized_list = raw_rules
    elif isinstance(raw_rules, dict):
        normalized_list = []
        for sheet_name, rule in raw_rules.items():
            if isinstance(rule, dict):
                normalized_list.append(
                    {
                        "sheet_name": str(sheet_name).strip(),
                        "table_id": rule.get("table_id", ""),
                        "header_row": rule.get("header_row", 1),
                    }
                )
            elif isinstance(rule, str):
                parts = [x.strip() for x in rule.split("|")]
                if len(parts) < 1:
                    raise ValueError(f"配置错误: sheet_rules[{sheet_name}] 字符串格式无效")
                table_id = parts[0]
                header_row = int(parts[1]) if len(parts) >= 2 and parts[1] else 1
                normalized_list.append(
                    {
                        "sheet_name": str(sheet_name).strip(),
                        "table_id": table_id,
                        "header_row": header_row,
                    }
                )
            else:
                raise ValueError(f"配置错误: sheet_rules[{sheet_name}] 必须是对象或字符串")
    else:
        raise ValueError("配置错误: feishu_sheet_import.sheet_rules 必须是非空数组或对象")

    rules: List[Dict[str, Any]] = []
    seen_sheet: set[str] = set()
    for idx, item in enumerate(normalized_list, 1):
        if isinstance(item, dict):
            sheet_name = str(item.get("sheet_name", "")).strip()
            table_id = str(item.get("table_id", "")).strip()
            header_row_raw = item.get("header_row", 1)
        elif isinstance(item, str):
            parts = [x.strip() for x in item.split("|")]
            if len(parts) != 3:
                raise ValueError(f"配置错误: sheet_rules 第{idx}项字符串格式应为 sheet_name|table_id|header_row")
            sheet_name, table_id, header_row_text = parts
            header_row_raw = header_row_text
        else:
            raise ValueError(f"配置错误: sheet_rules 第{idx}项必须是对象或字符串")

        if not sheet_name or not table_id:
            raise ValueError(f"配置错误: sheet_rules 第{idx}项 sheet_name/table_id 不能为空")
        try:
            header_row = int(header_row_raw)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"配置错误: sheet_rules 第{idx}项 header_row 必须是整数") from exc
        if header_row < 1:
            raise ValueError(f"配置错误: sheet_rules 第{idx}项 header_row 必须大于等于1")

        sheet_key = normalize_field_name(sheet_name)
        if sheet_key in seen_sheet:
            raise ValueError(f"配置错误: sheet_rules 存在重复 sheet_name: {sheet_name}")
        seen_sheet.add(sheet_key)

        rules.append(
            {
                "sheet_name": sheet_name,
                "table_id": table_id,
                "header_row": header_row,
            }
        )

    if not rules:
        raise ValueError("配置错误: feishu_sheet_import.sheet_rules 不能为空")
    return rules
