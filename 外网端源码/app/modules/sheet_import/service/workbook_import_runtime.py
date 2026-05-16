from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, Callable, Dict, List

import openpyxl


def import_workbook_sheets_to_feishu(
    *,
    config: Dict[str, Any],
    xlsx_path: str,
    client_factory: Callable[..., Any],
    normalize_sheet_rules: Callable[[Any], List[Dict[str, Any]]],
    parse_image_import_config: Callable[[Dict[str, Any]], Dict[str, Any]],
    build_explicit_image_mapping: Callable[[Any], Dict[str, Dict[str, Dict[str, str]]]],
    extract_rows_with_row_index: Callable[..., List[Any]],
    prepare_row_payloads_for_table: Callable[..., tuple[List[Any], Dict[str, int]]],
    apply_sheet_images_to_row_payloads: Callable[..., Dict[str, int]],
    emit_log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    if "feishu" not in config or not isinstance(config["feishu"], dict):
        raise ValueError("配置错误: feishu 缺失，请在 JSON 中配置。")
    if "feishu_sheet_import" not in config or not isinstance(config["feishu_sheet_import"], dict):
        raise ValueError("配置错误: feishu_sheet_import 缺失，请在 JSON 中配置。")

    feishu_cfg = config["feishu"]
    import_cfg = config["feishu_sheet_import"]
    required_import_keys = [
        "enabled",
        "app_token",
        "clear_before_upload",
        "continue_on_sheet_error",
        "timeout",
        "list_page_size",
        "delete_batch_size",
        "create_batch_size",
        "sheet_rules",
    ]
    missing_import_keys = [key for key in required_import_keys if key not in import_cfg]
    if missing_import_keys:
        raise ValueError(f"配置错误: feishu_sheet_import 缺少字段 {missing_import_keys}")

    if not bool(import_cfg["enabled"]):
        raise ValueError("配置错误: feishu_sheet_import.enabled=false，导表功能已禁用。")

    app_id = str(feishu_cfg["app_id"]).strip()
    app_secret = str(feishu_cfg["app_secret"]).strip()
    import_app_token = str(import_cfg["app_token"]).strip()
    missing_core = [
        k
        for k, v in {"app_id": app_id, "app_secret": app_secret, "import_app_token": import_app_token}.items()
        if not v
    ]
    if missing_core:
        raise ValueError(f"配置错误: 导表飞书参数缺失 {missing_core}")

    path = Path(xlsx_path)
    if not path.exists():
        raise FileNotFoundError(f"导表文件不存在: {xlsx_path}")
    if path.suffix.lower() != ".xlsx":
        raise ValueError(f"仅支持 xlsx 导表文件: {xlsx_path}")

    sheet_rules = normalize_sheet_rules(import_cfg["sheet_rules"])

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Unknown extension is not supported and will be removed",
            category=UserWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message="Conditional Formatting extension is not supported and will be removed",
            category=UserWarning,
        )
        wb = openpyxl.load_workbook(path, data_only=True)

    clear_before_upload = bool(import_cfg["clear_before_upload"])
    continue_on_sheet_error = bool(import_cfg["continue_on_sheet_error"])
    list_page_size = int(import_cfg["list_page_size"])
    delete_batch_size = int(import_cfg["delete_batch_size"])
    create_batch_size = int(import_cfg["create_batch_size"])
    request_retry_count = int(import_cfg.get("request_retry_count", feishu_cfg.get("request_retry_count", 3)))
    request_retry_interval_sec = float(
        import_cfg.get("request_retry_interval_sec", feishu_cfg.get("request_retry_interval_sec", 1))
    )
    tz_offset_hours = int(feishu_cfg.get("date_tz_offset_hours", 8))
    image_cfg = parse_image_import_config(import_cfg)
    explicit_image_map = build_explicit_image_mapping(image_cfg.get("sheet_image_rules", []))

    if list_page_size <= 0:
        raise ValueError("配置错误: feishu_sheet_import.list_page_size 必须大于0")
    if delete_batch_size <= 0:
        raise ValueError("配置错误: feishu_sheet_import.delete_batch_size 必须大于0")
    if create_batch_size <= 0:
        raise ValueError("配置错误: feishu_sheet_import.create_batch_size 必须大于0")
    if request_retry_count < 0:
        raise ValueError("配置错误: feishu_sheet_import.request_retry_count 必须大于等于0")
    if request_retry_interval_sec < 0:
        raise ValueError("配置错误: feishu_sheet_import.request_retry_interval_sec 必须大于等于0")

    client = client_factory(
        app_id=app_id,
        app_secret=app_secret,
        app_token=import_app_token,
        calc_table_id="",
        attachment_table_id="",
        date_field_mode="text",
        date_field_day=1,
        date_tz_offset_hours=8,
        timeout=int(import_cfg["timeout"]),
        request_retry_count=request_retry_count,
        request_retry_interval_sec=request_retry_interval_sec,
    )

    sheet_results: List[Dict[str, Any]] = []
    success_count = 0
    failed_count = 0

    try:
        for rule in sheet_rules:
            sheet_name = str(rule["sheet_name"]).strip()
            table_id = str(rule["table_id"]).strip()
            header_row = int(rule["header_row"])

            result = {
                "sheet_name": sheet_name,
                "table_id": table_id,
                "cleared_count": 0,
                "uploaded_count": 0,
                "detected_images": 0,
                "uploaded_images": 0,
                "rows_with_images": 0,
                "missing_mapping_count": 0,
                "success": False,
                "error": "",
            }

            try:
                if sheet_name not in wb.sheetnames:
                    raise ValueError(f"工作簿中不存在 sheet: {sheet_name}")
                ws = wb[sheet_name]
                table_fields = client.list_fields(table_id=table_id, page_size=list_page_size)
                list_field_names = [
                    str(item.get("field_name", "") or item.get("name", "") or "").strip()
                    for item in table_fields
                    if isinstance(item, dict)
                    and str(item.get("field_name", "") or item.get("name", "") or "").strip()
                ]

                if clear_before_upload:
                    cleared_count = client.clear_table(
                        table_id=table_id,
                        list_page_size=list_page_size,
                        delete_batch_size=delete_batch_size,
                        list_field_names=list_field_names[:1],
                    )
                    result["cleared_count"] = cleared_count
                    emit_log(f"[5Sheet导表][{sheet_name}] 已清空 {cleared_count} 条旧记录")

                raw_row_payloads = extract_rows_with_row_index(ws=ws, header_row=header_row)
                row_payloads, normalize_stats = prepare_row_payloads_for_table(
                    raw_rows=raw_row_payloads,
                    table_fields=table_fields,
                    tz_offset_hours=tz_offset_hours,
                )
                image_stats = apply_sheet_images_to_row_payloads(
                    ws=ws,
                    sheet_name=sheet_name,
                    header_row=header_row,
                    row_payloads=row_payloads,
                    table_fields=table_fields,
                    image_cfg=image_cfg,
                    explicit_map_by_sheet=explicit_image_map,
                    client=client,
                    log_func=emit_log,
                )

                fields_list = [item.fields for item in row_payloads if item.fields]
                if fields_list:
                    client.batch_create_records(table_id=table_id, fields_list=fields_list, batch_size=create_batch_size)
                result["uploaded_count"] = len(fields_list)
                result["detected_images"] = int(image_stats.get("detected_images", 0))
                result["uploaded_images"] = int(image_stats.get("uploaded_images", 0))
                result["rows_with_images"] = int(image_stats.get("rows_with_images", 0))
                result["missing_mapping_count"] = int(image_stats.get("missing_mapping_count", 0))
                if normalize_stats["skipped_missing_fields"] > 0:
                    emit_log(f"[5Sheet导表][{sheet_name}] 已跳过不存在字段值 {normalize_stats['skipped_missing_fields']} 个")
                if normalize_stats["skipped_unsupported_values"] > 0:
                    emit_log(
                        f"[5Sheet导表][{sheet_name}] 已跳过不支持字段值 {normalize_stats['skipped_unsupported_values']} 个"
                    )
                if normalize_stats["skipped_invalid_values"] > 0:
                    emit_log(f"[5Sheet导表][{sheet_name}] 已跳过无效字段值 {normalize_stats['skipped_invalid_values']} 个")
                if normalize_stats["dropped_rows"] > 0:
                    emit_log(f"[5Sheet导表][{sheet_name}] 有 {normalize_stats['dropped_rows']} 行为空或不可写，已忽略")
                if image_stats.get("detected_images", 0) > 0:
                    emit_log(
                        f"[5Sheet导表][{sheet_name}] 图片统计: 检测{image_stats['detected_images']}张, "
                        f"上传{image_stats['uploaded_images']}张, 涉及{image_stats['rows_with_images']}行"
                    )
                if image_stats.get("missing_mapping_count", 0) > 0:
                    emit_log(f"[5Sheet导表][{sheet_name}] 图片映射缺失 {image_stats['missing_mapping_count']} 处")
                if image_stats.get("orphan_row_images", 0) > 0:
                    emit_log(f"[5Sheet导表][{sheet_name}] 图片锚点无对应数据行 {image_stats['orphan_row_images']} 张，已忽略")
                result["success"] = True
                success_count += 1
                emit_log(f"[5Sheet导表][{sheet_name}] 导入成功: {len(fields_list)} 条")
            except Exception as exc:  # noqa: BLE001
                failed_count += 1
                result["error"] = str(exc)
                emit_log(f"[5Sheet导表][{sheet_name}] 导入失败: {exc}")
                if not continue_on_sheet_error:
                    raise
            finally:
                sheet_results.append(result)
    finally:
        wb.close()

    return {
        "success_count": success_count,
        "failed_count": failed_count,
        "sheet_results": sheet_results,
    }
