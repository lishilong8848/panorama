from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


def _text(value: Any) -> str:
    return str(value or "").strip()


def _field_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("name", "text", "value"):
            text = _text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts = [_field_text(item) for item in value]
        return ",".join(part for part in parts if part)
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
    if isinstance(value, (int, float)) and isinstance(target_value, (int, float)):
        return int(value) == int(target_value)
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
    return (
        f'AND(CurrentValue.[楼栋]={_formula_literal(building)}, '
        f'CurrentValue.[日期]={_formula_literal(target_value or date_text)})'
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


def _calc_business_key(fields: Dict[str, Any]) -> tuple[str, str, str]:
    return (
        _field_text(fields.get("类型")),
        _field_text(fields.get("分类")),
        _field_text(fields.get("项目")),
    )


def _list_calc_records_for_upsert(
    *,
    client: Any,
    calc_table_id: str,
    building: str,
    date_text: str,
) -> List[Dict[str, Any]]:
    target_date_value = _date_value_for_compare(client, date_text)
    filter_formula = _build_calc_record_filter_formula(
        building=building,
        date_text=date_text,
        target_value=target_date_value,
    )
    records = client.list_records(
        table_id=calc_table_id,
        page_size=500,
        max_records=500,
        filter_formula=filter_formula,
    )
    matched: List[Dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        record_id = _text(item.get("record_id"))
        fields = item.get("fields", {})
        if not record_id or not isinstance(fields, dict):
            continue
        if _field_text(fields.get("楼栋")) != _text(building):
            continue
        if not _date_field_matches(fields.get("日期"), date_text=date_text, target_value=target_date_value):
            continue
        matched.append(item)
    return matched


def _list_attachment_records_for_upsert(
    *,
    client: Any,
    attachment_table_id: str,
    report_type: str,
    building: str,
    date_text: str,
) -> List[Dict[str, Any]]:
    target_date_value = _date_value_for_compare(client, date_text)
    filter_formula = _build_attachment_record_filter_formula(
        report_type=report_type,
        building=building,
        date_text=date_text,
        target_value=target_date_value,
    )
    records = client.list_records(
        table_id=attachment_table_id,
        page_size=500,
        max_records=500,
        filter_formula=filter_formula,
    )
    matched: List[Dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        record_id = _text(item.get("record_id"))
        fields = item.get("fields", {})
        if not record_id or not isinstance(fields, dict):
            continue
        if _field_text(fields.get("类型")) != _text(report_type):
            continue
        if _field_text(fields.get("楼栋")) != _text(building):
            continue
        if not _date_field_matches(fields.get("日期"), date_text=date_text, target_value=target_date_value):
            continue
        matched.append(item)
    return matched


def _first_record_ids_by_calc_key(records: List[Dict[str, Any]]) -> Dict[tuple[str, str, str], str]:
    out: Dict[tuple[str, str, str], str] = {}
    for item in records:
        if not isinstance(item, dict):
            continue
        record_id = _text(item.get("record_id"))
        fields = item.get("fields", {})
        if not record_id or not isinstance(fields, dict):
            continue
        key = _calc_business_key(fields)
        if all(key) and key not in out:
            out[key] = record_id
    return out


def _build_calc_record_fields_for_upsert(
    *,
    client: Any,
    records: List[Any],
    skip_zero_records: bool,
    date_override: str,
) -> List[Dict[str, Any]]:
    builder = getattr(client, "build_calc_record_fields", None)
    if callable(builder):
        return list(
            builder(
                records,
                skip_zero_records=skip_zero_records,
                date_override=date_override,
            )
            or []
        )
    fields_list: List[Dict[str, Any]] = []
    for record in records:
        if isinstance(record, dict):
            fields_list.append(dict(record))
            continue
        raise AttributeError("client 缺少 build_calc_record_fields，无法执行按键 upsert")
    return fields_list


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
    emit_log(f"[飞书上传] 开始准备按日期 upsert: results={len(results)}")

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
            existing_calc_records = _list_calc_records_for_upsert(
                client=client,
                calc_table_id=calc_table_id,
                building=result.building,
                date_text=upload_date_text,
            )
            emit_log(
                f"[飞书上传][upsert] 已按日期读取计算记录: 楼栋={building_text}, 日期={date_text}, count={len(existing_calc_records)}"
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书计算记录按日期查询 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            existing_attachment_records = _list_attachment_records_for_upsert(
                client=client,
                attachment_table_id=attachment_table_id,
                report_type=report_type,
                building=result.building,
                date_text=upload_date_text,
            )
            emit_log(
                f"[飞书上传][upsert] 已按日期读取附件记录: 楼栋={building_text}, 日期={date_text}, count={len(existing_attachment_records)}"
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书附件记录按日期查询 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            calc_fields = _build_calc_record_fields_for_upsert(
                client=client,
                records=result.records,
                skip_zero_records=skip_zero_records,
                date_override=upload_date_text,
            )
            existing_by_key = _first_record_ids_by_calc_key(existing_calc_records)
            update_payloads: List[Dict[str, Any]] = []
            create_fields: List[Dict[str, Any]] = []
            for fields in calc_fields:
                if not isinstance(fields, dict):
                    continue
                key = _calc_business_key(fields)
                record_id = existing_by_key.get(key) if all(key) else ""
                if record_id:
                    update_payloads.append({"record_id": record_id, "fields": fields})
                else:
                    create_fields.append(fields)
            if update_payloads:
                client.batch_update_records(table_id=calc_table_id, records=update_payloads, batch_size=200)
            if create_fields:
                client.batch_create_records(table_id=calc_table_id, fields_list=create_fields, batch_size=200)
            emit_log(
                f"[飞书上传][upsert] 计算记录完成: 楼栋={building_text}, 日期={date_text}, "
                f"updated={len(update_payloads)}, created={len(create_fields)}"
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书计算记录上传 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            file_token = client.upload_attachment(result.source_file)
        except Exception as exc:  # noqa: BLE001
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书附件上传 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            attachment_fields = {
                "类型": report_type,
                "楼栋": result.building,
                "日期": _date_value_for_compare(client, upload_date_text),
                "附件": [{"file_token": file_token}],
            }
            attachment_record_id = ""
            for item in existing_attachment_records:
                if isinstance(item, dict):
                    attachment_record_id = _text(item.get("record_id"))
                    if attachment_record_id:
                        break
            if attachment_record_id:
                client.update_record(
                    table_id=attachment_table_id,
                    record_id=attachment_record_id,
                    fields=attachment_fields,
                )
                attachment_action = "updated"
            else:
                client.batch_create_records(table_id=attachment_table_id, fields_list=[attachment_fields], batch_size=1)
                attachment_action = "created"
            emit_log(
                f"[飞书上传][upsert] 附件记录完成: 楼栋={building_text}, 日期={date_text}, action={attachment_action}"
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书附件记录写入 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        emit_log(
            f"[文件上传成功] 功能={log_feature} 阶段=飞书上传完成 楼栋={building_text} "
            f"文件={file_text} 日期={date_text} 详情=已按日期 upsert 写入"
        )
