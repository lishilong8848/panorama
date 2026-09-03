from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from app.modules.report_pipeline.service.local_mysql_persistence import LocalMysqlCalculationWriter


def _text(value: Any) -> str:
    return str(value or "").strip()


def _field_text(value: Any) -> str:
    if isinstance(value, list):
        return ",".join(part for part in (_field_text(item) for item in value) if part)
    if isinstance(value, dict):
        for key in ("text", "name", "value"):
            if key in value:
                return _field_text(value.get(key))
    return _text(value)


def _date_value_for_compare(client: Any, date_text: str) -> Any:
    converter = getattr(client, "_to_feishu_date", None)
    if callable(converter):
        try:
            return converter(date_text)
        except Exception:  # noqa: BLE001
            return date_text
    return date_text


def _date_field_matches(value: Any, *, date_text: str, target_value: Any) -> bool:
    if value is None:
        return False
    value_text = _text(value)
    target_text = _text(target_value)
    if value_text and target_text and value_text == target_text:
        return True
    if value_text and value_text == _text(date_text):
        return True
    target_day = _text(date_text)[:10]
    if isinstance(value, (int, float)) or value_text.isdigit():
        try:
            value_day = datetime.fromtimestamp(
                float(value) / 1000,
                tz=timezone(timedelta(hours=8)),
            ).strftime("%Y-%m-%d")
            return value_day == target_day
        except (OSError, OverflowError, ValueError):
            pass
    # 日期字符串容错: 允许 "YYYY-MM-DD HH:MM:SS" 或 ISO 字符串前缀匹配
    if len(value_text) >= 10 and value_text[:10] == _text(date_text):
        return True
    return False


def _formula_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE()" if value else "FALSE()"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(int(value)) if isinstance(value, int) else str(value)
    text = _text(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def _build_calc_record_filter_formula(*, building: str, date_text: str, target_value: Any) -> str:
    start_date = datetime.strptime(_text(date_text)[:10], "%Y-%m-%d")
    end_date_text = (start_date + timedelta(days=1)).strftime("%Y-%m-%d")
    return (
        f'AND(CurrentValue.[楼栋]={_formula_literal(building)}, '
        f'CurrentValue.[日期]>=TODATE({_formula_literal(start_date.strftime("%Y-%m-%d"))}), '
        f'CurrentValue.[日期]<TODATE({_formula_literal(end_date_text)}))'
    )


def _build_attachment_record_filter_formula(
    *,
    report_type: str,
    building: str,
    date_text: str,
    target_value: Any,
) -> str:
    return (
        f'AND(CurrentValue.[类型]={_formula_literal(report_type)}, '
        f'CurrentValue.[楼栋]={_formula_literal(building)}, '
        f'CurrentValue.[日期]={_formula_literal(target_value or date_text)})'
    )


def _collect_calc_record_ids_for_replace(
    *,
    client: Any,
    calc_table_id: str,
    building: str,
    date_text: str,
    replacement_keys: set[tuple[str, str, str, str]],
) -> List[str]:
    target_date_value = _date_value_for_compare(client, date_text)
    filter_formula = _build_calc_record_filter_formula(
        building=building,
        date_text=date_text,
        target_value=target_date_value,
    )
    try:
        records = client.list_records(
            table_id=calc_table_id,
            page_size=500,
            max_records=0,
            filter_formula=filter_formula,
            field_names=["类型", "分类", "项目", "楼栋", "日期"],
        )
    except Exception:  # noqa: BLE001
        records = client.list_records(
            table_id=calc_table_id,
            page_size=500,
            max_records=0,
            field_names=["类型", "分类", "项目", "楼栋", "日期"],
        )
    record_ids: List[str] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        record_id = _text(item.get("record_id"))
        fields = item.get("fields", {})
        if not record_id or not isinstance(fields, dict):
            continue
        if _text(fields.get("楼栋")) != _text(building):
            continue
        if not _date_field_matches(fields.get("日期"), date_text=date_text, target_value=target_date_value):
            continue
        identity = tuple(_field_text(fields.get(name)) for name in ("类型", "分类", "项目", "楼栋"))
        if identity not in replacement_keys:
            continue
        record_ids.append(record_id)
    return record_ids


def _calc_replacement_keys(client: Any, records: List[Any]) -> set[tuple[str, str, str, str]]:
    canonical_name = getattr(client, "_canonical_metric_name_fn", lambda value: _text(value))
    dimension_mapping = getattr(client, "_dimension_mapping", {})
    keys: set[tuple[str, str, str, str]] = set()
    for record in records:
        item_name = _text(getattr(record, "item_name", ""))
        mapped = dimension_mapping.get(canonical_name(item_name)) if isinstance(dimension_mapping, dict) else None
        if mapped:
            type_name, category_name, mapped_item_name = mapped
        else:
            type_name = getattr(record, "type_name", "")
            category_name = getattr(record, "category_name", "")
            mapped_item_name = item_name
        keys.add(
            (
                _text(type_name),
                _text(category_name),
                _text(mapped_item_name),
                _text(getattr(record, "building", "")),
            )
        )
    return keys


def _collect_attachment_record_ids_for_replace(
    *,
    client: Any,
    attachment_table_id: str,
    report_type: str,
    building: str,
    date_text: str,
) -> List[str]:
    target_date_value = _date_value_for_compare(client, date_text)
    filter_formula = _build_attachment_record_filter_formula(
        report_type=report_type,
        building=building,
        date_text=date_text,
        target_value=target_date_value,
    )
    try:
        records = client.list_records(
            table_id=attachment_table_id,
            page_size=500,
            max_records=500,
            filter_formula=filter_formula,
        )
    except Exception:  # noqa: BLE001
        records = client.list_records(table_id=attachment_table_id, page_size=500, max_records=0)
    record_ids: List[str] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        record_id = _text(item.get("record_id"))
        fields = item.get("fields", {})
        if not record_id or not isinstance(fields, dict):
            continue
        if _text(fields.get("类型")) != _text(report_type):
            continue
        if _text(fields.get("楼栋")) != _text(building):
            continue
        if not _date_field_matches(fields.get("日期"), date_text=date_text, target_value=target_date_value):
            continue
        record_ids.append(record_id)
    return record_ids


def upload_results_to_feishu(
    results: List[Any],
    config: Dict[str, Any],
    *,
    resolve_upload_date_from_runtime: Callable[[Dict[str, Any]], str | None],
    client_factory: Callable[..., Any],
    date_override_by_source: Optional[Dict[str, str]] = None,
    log_feature: str = "月报上传",
    emit_log: Callable[[str], None] = print,
) -> None:
    if "feishu" not in config or not isinstance(config["feishu"], dict):
        raise ValueError("配置错误: feishu 缺失，请在JSON中配置。")
    feishu_cfg = config["feishu"]
    if "enable_upload" not in feishu_cfg:
        raise ValueError("配置错误: feishu.enable_upload 缺失，请在JSON中配置。")
    if not feishu_cfg["enable_upload"]:
        emit_log("[飞书] 已关闭上传。")
        return

    app_id = str(feishu_cfg["app_id"]).strip()
    app_secret = str(feishu_cfg["app_secret"]).strip()
    app_token = str(feishu_cfg["app_token"]).strip()
    calc_table_id = str(feishu_cfg["calc_table_id"]).strip()
    attachment_table_id = str(feishu_cfg["attachment_table_id"]).strip()

    required_values = {
        "app_id": app_id,
        "app_secret": app_secret,
        "app_token": app_token,
        "calc_table_id": calc_table_id,
        "attachment_table_id": attachment_table_id,
    }
    missing_keys = [k for k, v in required_values.items() if not v]
    if missing_keys:
        raise ValueError(f"飞书配置缺失: {missing_keys}")

    request_retry_count = int(feishu_cfg.get("request_retry_count", 3))
    request_retry_interval_sec = float(feishu_cfg.get("request_retry_interval_sec", 1))
    if request_retry_count < 0:
        raise ValueError("配置错误: feishu.request_retry_count 必须大于等于0")
    if request_retry_interval_sec < 0:
        raise ValueError("配置错误: feishu.request_retry_interval_sec 必须大于等于0")

    client = client_factory(
        app_id=app_id,
        app_secret=app_secret,
        app_token=app_token,
        calc_table_id=calc_table_id,
        attachment_table_id=attachment_table_id,
        date_field_mode=str(feishu_cfg["date_field_mode"]).strip(),
        date_field_day=int(feishu_cfg["date_field_day"]),
        date_tz_offset_hours=int(feishu_cfg["date_tz_offset_hours"]),
        timeout=int(feishu_cfg["timeout"]),
        request_retry_count=request_retry_count,
        request_retry_interval_sec=request_retry_interval_sec,
    )
    mysql_writer = LocalMysqlCalculationWriter(feishu_cfg.get("local_mysql", {}))

    report_type = feishu_cfg["report_type"]
    skip_zero_records = bool(feishu_cfg["skip_zero_records"])
    date_override = resolve_upload_date_from_runtime(config)

    normalized_source_dates: Dict[str, str] = {}
    if date_override_by_source:
        for source, day_text in date_override_by_source.items():
            source_path = str(Path(source).resolve())
            normalized_source_dates[source_path] = str(day_text).strip()

    resolved_upload_dates: Dict[str, str] = {}
    for result in results:
        source_key = str(Path(result.source_file).resolve())
        upload_date_text = normalized_source_dates.get(source_key, "") or date_override or result.month
        resolved_upload_dates[source_key] = upload_date_text
    emit_log(f"[飞书上传] 开始准备覆盖查询: results={len(results)}")

    for result in results:
        source_key = str(Path(result.source_file).resolve())
        upload_date_text = resolved_upload_dates.get(source_key, "") or normalized_source_dates.get(source_key, "") or date_override or result.month
        building_text = str(result.building or "-").strip() or "-"
        file_text = str(result.source_file or "-").strip() or "-"
        date_text = str(upload_date_text or "-").strip() or "-"
        pue_value = result.values.get("PUE")
        pue_text = "-" if pue_value is None else f"{float(pue_value):.3f}"
        emit_log(f"[飞书上传] 楼栋={building_text} 日期={date_text} PUE={pue_text}")

        try:
            replacement_keys = _calc_replacement_keys(client, result.records)
            calc_delete_ids = _collect_calc_record_ids_for_replace(
                client=client,
                calc_table_id=calc_table_id,
                building=result.building,
                date_text=upload_date_text,
                replacement_keys=replacement_keys,
            )
            emit_log(
                f"[飞书上传][覆盖] 已读取五字段匹配旧计算记录: 楼栋={building_text}, "
                f"日期={date_text}, keys={len(replacement_keys)}, count={len(calc_delete_ids)}"
            )
        except Exception as exc:  # noqa: BLE001
            mysql_writer.close()
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书旧计算记录覆盖删除 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            attachment_delete_ids = _collect_attachment_record_ids_for_replace(
                client=client,
                attachment_table_id=attachment_table_id,
                report_type=report_type,
                building=result.building,
                date_text=upload_date_text,
            )
            emit_log(
                f"[飞书上传][覆盖] 已读取旧附件记录: 楼栋={building_text}, 日期={date_text}, count={len(attachment_delete_ids)}"
            )
            if attachment_delete_ids:
                deleted_attachment = client.batch_delete_records(
                    table_id=attachment_table_id,
                    record_ids=attachment_delete_ids,
                    batch_size=500,
                )
                emit_log(
                    f"[飞书上传][覆盖] 已删除旧附件记录: 楼栋={building_text}, 日期={date_text}, count={int(deleted_attachment or 0)}"
                )
        except Exception as exc:  # noqa: BLE001
            mysql_writer.close()
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书旧附件记录覆盖删除 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            client.upload_calc_records(
                result.records,
                skip_zero_records=skip_zero_records,
                date_override=upload_date_text,
            )
            deleted_calc = 0
            if calc_delete_ids:
                deleted_calc = client.batch_delete_records(
                    table_id=calc_table_id,
                    record_ids=calc_delete_ids,
                    batch_size=500,
                )
            emit_log(
                f"[飞书上传][覆盖] 新计算记录写入后已删除五字段匹配旧记录: "
                f"楼栋={building_text}, 日期={date_text}, count={int(deleted_calc or 0)}"
            )
        except Exception as exc:  # noqa: BLE001
            mysql_writer.close()
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书计算记录上传 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            persisted_count = mysql_writer.persist(
                result,
                client,
                date_text=upload_date_text,
                skip_zero_records=skip_zero_records,
            )
            if mysql_writer.enabled:
                emit_log(
                    f"[本地MySQL] 批量写入完成: 楼栋={building_text}, 日期={date_text}, count={persisted_count}"
                )
        except Exception as exc:  # noqa: BLE001
            mysql_writer.close()
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=本地MySQL持久化 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            file_token = client.upload_attachment(result.source_file)
        except Exception as exc:  # noqa: BLE001
            mysql_writer.close()
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书附件上传 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            client.upload_attachment_record(
                report_type=report_type,
                building=result.building,
                date_text=upload_date_text,
                attachment_tokens=[file_token],
            )
        except Exception as exc:  # noqa: BLE001
            mysql_writer.close()
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书附件记录写入 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        emit_log(
            f"[文件上传成功] 功能={log_feature} 阶段=飞书上传完成 楼栋={building_text} "
            f"文件={file_text} 日期={date_text} 详情=已按覆盖策略写入新记录"
        )
    mysql_writer.close()
